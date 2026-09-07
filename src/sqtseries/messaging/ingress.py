"""Ingress pipeline: ZMQ PULL -> validate -> batched sink (SQLite write).
Batch-first by design: a drained burst is persisted in ONE transaction and
republished in ONE task. The former one-transaction-per-point path was the
ingest bottleneck under concurrent query load (measured 2026-09: 2000 pts/s
pump delivered ~57 pts/s because each point paid a full BEGIN IMMEDIATE +
commit while query threads contended for the GIL). Drain loop pattern per
pyzmq-asyncio-research-2026.md §9: poll once, drain up to N with NOBLOCK,
yield to the loop.
"""

from collections.abc import Callable
from typing import Any

import orjson
import structlog
import zmq
import zmq.asyncio

from ..config import IngestionSettings
from .context import apply_options, socket_options
from .protocol import ProtocolError, parse_ingest

log = structlog.get_logger(__name__)

# Max frames drained per drain_many() call. One drained batch = one sink
# transaction + one publish task, so this bounds per-tick work while keeping
# per-point overhead amortized. 256 points/burst at 2000 pts/s = ~8 batches/s.
DEFAULT_DRAIN_BATCH = 256

# A validated row: (metric, tags, value, timestamp_ns).
Row = tuple[str, dict[str, str] | None, float, int]

# A republish event: (topic_bytes, payload_dict).
Event = tuple[bytes, dict[str, Any]]


class Ingress:
    """Receive, validate, and batch-dispatch measurements from a PULL socket."""

    def __init__(
        self,
        endpoint: str,
        settings: IngestionSettings,
        sink: Callable[[list[Row]], None] | None = None,
        on_publish: Callable[[list[Event]], None] | None = None,
        context: zmq.asyncio.Context | None = None,
    ):
        """
        Args:
            endpoint: ``tcp://127.0.0.1:12501`` (or ipc://).
            sink: callable receiving a list of (metric, tags, value, ts_ns)
                  rows — called ONCE per drained batch; None skips persistence
                  (validation and republish still happen).
            on_publish: callable receiving [(topic_bytes, payload), ...] —
                  called ONCE per drained batch.
        """
        self.endpoint = endpoint
        self.settings = settings
        self.sink = sink
        self.on_publish = on_publish
        self._ctx = context or zmq.asyncio.Context.instance()
        self.socket: zmq.asyncio.Socket | None = None

        self._opts = socket_options(
            hwm=settings.hwm,
            max_msg_size=settings.max_message_size,
        )

        self.recv_count = 0
        self.error_count = 0
        self.invalid_count = 0

    async def start(self) -> None:
        self.socket = self._ctx.socket(zmq.PULL)
        apply_options(self.socket, self._opts)
        self.socket.bind(self.endpoint)
        log.info("ingress listening", endpoint=self.endpoint)

    async def drain_many(self, max_messages: int = DEFAULT_DRAIN_BATCH) -> int:
        """Drain up to ``max_messages`` pending frames; return rows handled.

        NOBLOCK recv loop (never waits): each drained batch is validated and
        then dispatched ONCE — one sink transaction and one publish task per
        burst, instead of per point.
        """
        if self.socket is None:
            raise RuntimeError("ingress not started")
        rows: list[Row] = []
        for _ in range(max_messages):
            try:
                raw = await self.socket.recv(flags=zmq.NOBLOCK)
            except zmq.Again:
                break
            except zmq.ZMQError as exc:
                self.error_count += 1
                log.warning("ingress recv error: %s", exc)
                break
            row = self._parse(raw)
            if row is not None:
                rows.append(row)
        if rows:
            self._dispatch(rows)
        return len(rows)

    async def drain(self) -> None:
        """Drain ALL pending messages (for shutdown/tests)."""
        while await self.drain_many(max_messages=4096):
            pass

    def _parse(self, raw: bytes) -> Row | None:
        """Validate one frame; None (and invalid_count++) if malformed."""
        try:
            msg = orjson.loads(raw)
        except (orjson.JSONDecodeError, ValueError):  # fmt: skip
            self.invalid_count += 1
            return None
        try:
            ingest = parse_ingest(
                msg,
                reject_client_timestamp_skew_s=self.settings.reject_client_timestamp_skew_s,
            )
            metric, tags, value, ts_ns = ingest.to_rows()
        except (ProtocolError, OverflowError):  # fmt: skip
            # Malformed payloads count as invalid. OverflowError guards the
            # (skew-guard-disabled) huge-float timestamp path where
            # ``to_rows`` can't fit ``ts * 1e9`` into an int.
            self.invalid_count += 1
            return None

        self.recv_count += 1
        return metric, tags, value, ts_ns

    def _dispatch(self, rows: list[Row]) -> None:
        """Persist + republish one batch of validated rows."""
        if self.sink is not None:
            self.sink(rows)
        if self.on_publish is not None:
            self.on_publish(
                [
                    (metric.encode(), {"metric": metric, "tags": tags, "value": value})
                    for metric, tags, value, _ts_ns in rows
                ]
            )

    async def stop(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None

    def stats(self) -> dict[str, int]:
        return {
            "recv": self.recv_count,
            "invalid": self.invalid_count,
            "errors": self.error_count,
        }

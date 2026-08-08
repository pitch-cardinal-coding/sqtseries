"""Ingress pipeline: ZMQ PULL -> validate -> write queue -> SQLite.

Republish valid measurements to the PUB/SUB bus for live subscribers.
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

# pubsub topic: "metric" or "metric/tagval" style prefix. Keep simple: metric name.
TOPIC_PREFIX = b""


class Ingress:
    """Receive, validate, and enqueue measurements from a PULL socket."""

    def __init__(
        self,
        endpoint: str,
        settings: IngestionSettings,
        sink: Callable[[Any], None] | None = None,
        on_publish: Callable[[bytes, dict[str, Any]], None] | None = None,
        context: zmq.asyncio.Context | None = None,
    ):
        """
        Args:
            endpoint: ``tcp://127.0.0.1:12501`` (or ipc://).
            sink: callable receiving (metric, tags, value, timestamp_ns) rows.
                  Defaults to a local queue consumed by ``drain``.
            on_publish: callback(bytes_metric, dict) for live subscribers.
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

    async def run_once(self, block: bool = True) -> None:
        """Receive and handle a single message; used by worker loops."""
        if self.socket is None:
            raise RuntimeError("ingress not started")
        try:
            raw = (
                await self.socket.recv()
                if block
                else await self.socket.recv(flags=zmq.NOBLOCK)
            )
        except zmq.Again:
            return
        except zmq.ZMQError as exc:
            self.error_count += 1
            log.warning("ingress recv error: %s", exc)
            return
        self._handle(raw)

    async def drain(self) -> None:
        """Drain ALL pending messages in a tight loop (for shutdown/tests)."""
        while True:
            try:
                raw = await self.socket.recv(flags=zmq.NOBLOCK)
            except zmq.Again:
                return
            except zmq.ZMQError as exc:
                self.error_count += 1
                log.warning("ingress recv error: %s", exc)
                return
            self._handle(raw)

    def _handle(self, raw: bytes) -> None:
        try:
            msg = orjson.loads(raw)
        except orjson.JSONDecodeError, ValueError:
            self.invalid_count += 1
            return
        try:
            ingest = parse_ingest(
                msg,
                reject_client_timestamp_skew_s=self.settings.reject_client_timestamp_skew_s,
            )
        except ProtocolError:
            self.invalid_count += 1
            return

        metric, tags, value, ts_ns = ingest.to_rows()
        self.recv_count += 1
        if self.sink is not None:
            self.sink(metric, tags, value, ts_ns)
        if self.on_publish is not None:
            self.on_publish(
                metric.encode(), {"metric": metric, "tags": tags, "value": value}
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

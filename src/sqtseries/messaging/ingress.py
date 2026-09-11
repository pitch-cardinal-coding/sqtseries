"""Ingress pipeline: ZMQ PULL -> validate -> bounded queue -> persister.

Bounded handler-engine pattern (see markdown/RESEARCHES.md): the ingress
socket is a fast receiver; downstream work (SQLite commit + republish) is
decoupled behind an explicit, bounded buffer. The drain loop NEVER awaits a
sink call — commit time cannot throttle reception (measured 2026-09: awaiting
commits inline capped ingest at ~1,650 pts/s while the pump delivered 10k).
When the queue is full the loop stops reading frames and libzmq backpressure
applies: frames buffer up to RCVHWM (101000), senders block past
that. Bounded memory, zero silent loss.

Drain loop pattern per pyzmq-asyncio-research-2026.md §9: poll once, drain up
to N with NOBLOCK, yield to the loop.
"""

import asyncio
import inspect
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

# Max frames parsed per drain_many() call. Parsed frames go onto a bounded
# queue consumed by the dedicated persister task, so a large parse burst
# costs only queue slots, not a long commit.
DEFAULT_DRAIN_BATCH = 1024

# Sentinel telling the persister to exit after finishing every batch queued
# ahead of it. Compared by identity and never equal to a real batch (which is
# a non-empty list).
_PERSIST_EXIT: Any = None

# Hard bound on shutdown flushing: if the persister cannot finish within
# this budget (DB wedged), shutdown terminates anyway and logs loudly
# instead of hanging forever.
FLUSH_TIMEOUT_S = 60.0

# A validated row: (metric, tags, value, timestamp_ns).
Row = tuple[str, dict[str, str] | None, float, int]

# A republish event: (topic_bytes, payload_dict).
Event = tuple[bytes, dict[str, Any]]


class Ingress:
    """Receive, validate, and batch-persist measurements from a PULL socket."""

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
                  rows — called ONCE per batch by the persister task; None
                  skips persistence (validation and republish still happen).
            on_publish: callable receiving [(topic_bytes, payload), ...] —
                  called ONCE per batch, only after persistence succeeded.
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
        # Bounded hand-off between the drain loop (receiver) and the
        # persister task (consumer). A counting semaphore (one credit per
        # outstanding batch) bounds queued + in-sink batches at
        # ``pending_max``: when the credits are exhausted, drain_many
        # suspends and ZMQ backpressure applies (PUSH senders block at
        # their SNDHWM) — memory stays bounded, nothing is dropped
        # (bounded-queue doctrine: explicit bound, never silent
        # loss). The queue itself is unbounded because the semaphore is
        # the bound (a bare maxsize queue would not count the batch the
        # persister has already taken but not finished).
        self._pending: asyncio.Queue[Any] = asyncio.Queue()
        self._credits = asyncio.Semaphore(settings.pending_max)
        self._persister: asyncio.Task[None] | None = None

        # Batch hand-off bookkeeping (monotonic counters, no emptiness
        # races): _enqueued increments once a parsed batch is ON the queue,
        # _persisted once its _dispatch returned. flush() waits until
        # _persisted catches up to a captured _enqueued. _inflight_puts
        # counts puts in progress (started but not yet on the queue) so
        # drain_inflight can quiesce producers before sending the sentinel.
        self._enqueued = 0
        self._persisted = 0
        self._inflight_puts = 0

        # True while the drain loop is suspended waiting for a credit —
        # surfaced in stats() so operators can SEE backpressure engaging.
        self.backpressured = False

    async def start(self) -> None:
        self.socket = self._ctx.socket(zmq.PULL)
        apply_options(self.socket, self._opts)
        self.socket.bind(self.endpoint)
        self._persister = asyncio.create_task(
            self._persist_loop(), name="ingress-persister"
        )
        log.info("ingress listening", endpoint=self.endpoint)

    async def drain_many(self, max_messages: int = DEFAULT_DRAIN_BATCH) -> int:
        """Parse up to ``max_messages`` pending frames; enqueue as one batch.

        Pure receiver: NOBLOCK recv + validate, then hand the batch to the
        persister task via the bounded queue. NEVER awaits a sink call —
        SQLite commit time cannot throttle reception.

        Returns the number of rows parsed (0 = socket drained dry).
        """
        if self.socket is None:
            raise RuntimeError("ingress not started")
        rows: list[Row] = []
        try:
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
        except asyncio.CancelledError:
            # Cancelled mid-parse (shutdown stops the worker pool first):
            # received frames are already OFF the socket — libzmq will never
            # redeliver them — so enqueue what was parsed before propagating.
            if rows:
                await self._enqueue(rows)
            raise
        if rows:
            await self._enqueue(rows)
        return len(rows)

    async def _enqueue(self, rows: list[Row]) -> None:
        """Put a parsed batch on the queue, cancellation-safe.

        The backpressure point: awaits a credit — suspends while
        ``pending_max`` batches are outstanding (queued or in the sink).
        Once acquired, the credit belongs to this batch until the
        persister finishes dispatching it. Cancellation before the put
        releases the credit; the batch is never orphaned between parse and
        queue (shutdown's drain_inflight relies on this).
        """
        self.backpressured = True
        try:
            await self._credits.acquire()
        finally:
            self.backpressured = False
        self._inflight_puts += 1
        try:
            await self._pending.put(rows)
        except asyncio.CancelledError:
            # Put is on an unbounded queue (never suspends), but if a
            # cancellation lands in this window the credit must go back.
            self._credits.release()
            raise
        finally:
            self._inflight_puts -= 1
            self._enqueued += 1

    async def flush(self) -> None:
        """Wait until every batch parsed so far has been dispatched.

        Non-destructive (the persister keeps running) and idempotent — used
        by tests and any caller that needs persistence observably done.
        """
        # Phase 1: quiesce in-progress puts so the target below includes
        # them (the _enqueued counter increments in the put's done-callback,
        # which can lag the put completing).
        while self._inflight_puts > 0:
            await asyncio.sleep(0.005)
        # Phase 2: wait for the persister to dispatch at least the captured
        # set. _persisted only increments after a dispatch fully returns, so
        # passing this means every captured batch observably landed.
        target = self._enqueued
        while self._persisted < target:
            await asyncio.sleep(0.005)

    async def drain_inflight(self) -> None:
        """Flush everything and stop the persister cleanly (shutdown).

        Waits for in-progress puts to land, then queues the exit sentinel
        BEHIND every real batch, then awaits the persister. A second call
        (double shutdown) is a no-op. Bounded by FLUSH_TIMEOUT_S.
        """
        if self._persister is None or self._persister.done():
            return
        try:
            async with asyncio.timeout(FLUSH_TIMEOUT_S):
                # Stragglers first: shielded puts from cancelled drain
                # workers must land BEFORE the sentinel, or their batches
                # would queue behind it and never persist.
                while self._inflight_puts > 0:
                    await asyncio.sleep(0.005)
                # Awaiting put (not put_nowait): if the queue is full the
                # persister drains it and the sentinel slips in last.
                await self._pending.put(_PERSIST_EXIT)
                await self._persister
        except TimeoutError:
            queued = self._pending.qsize()
            log.error(
                "ingress flush timed out; cancelling persister",
                queued_batches=queued,
            )
            self._persister.cancel()
            await asyncio.gather(self._persister, return_exceptions=True)

    async def drain(self) -> None:
        """Drain ALL pending socket frames (for shutdown/tests)."""
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

    async def _persist_loop(self) -> None:
        """Dedicated consumer: persists + republishes batches off the queue.

        Publish happens only after persistence so live subscribers never see
        a point that later failed to land (atomic-commit doctrine: a failed
        insert rolls back wholly). One bad batch must never kill the
        persister — the pipeline outlives any single failure.
        """
        while True:
            batch = await self._pending.get()
            if batch is _PERSIST_EXIT:
                return
            try:
                await self._dispatch(batch)
            except asyncio.CancelledError:
                raise
            except Exception:
                # _dispatch reports sink failures via the sink's own
                # accounting (persisted/dropped counters).
                log.exception("persist batch failed", batch_size=len(batch))
            finally:
                self._persisted += 1
                # The batch is fully handled (persisted, or its failure
                # accounted by the sink): its credit goes back so the
                # receiver may parse a replacement batch.
                self._credits.release()

    async def _dispatch(self, rows: list[Row]) -> None:
        """Persist + republish one batch of validated rows.

        The sink call may be sync (blocking) or async. Sync sinks run in a
        worker thread so the event loop keeps serving queries while SQLite
        commits; async sinks are awaited directly. Publishing happens only
        after persistence so live subscribers never see a point that later
        failed to land.
        """
        if self.sink is not None:
            if inspect.iscoroutinefunction(self.sink):
                await self.sink(rows)
            else:
                # Sync sink: the blocking BEGIN IMMEDIATE + commit must
                # execute IN the worker thread, not on the event loop.
                await asyncio.to_thread(self.sink, rows)
        if self.on_publish is not None:
            self.on_publish(
                [
                    (metric.encode(), {"metric": metric, "tags": tags, "value": value})
                    for metric, tags, value, _ts_ns in rows
                ]
            )

    async def stop(self) -> None:
        """Close the socket, flush every queued batch, then stop the persister.

        Graceful by contract: stopping must never silently lose batches
        already parsed (drain_inflight does the bounded flush). Idempotent.
        """
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None
        await self.drain_inflight()
        if self._persister is not None:
            # Flush timed out (DB wedged) — tear down so nothing outlives
            # the socket, loudly (drain_inflight already logged).
            if not self._persister.done():
                self._persister.cancel()
            await asyncio.gather(self._persister, return_exceptions=True)
            self._persister = None

    def stats(self) -> dict[str, int]:
        return {
            "recv": self.recv_count,
            "invalid": self.invalid_count,
            "errors": self.error_count,
            "queued_batches": self._pending.qsize(),
            "backpressured": int(self.backpressured),
        }

"""Bounded in-process fan-out hub for live measurements.

Doctrine (bounded queues, loud drops):
  * ``PubSub.publish()`` hands each frame to the hub **in-process** — no
    per-subscriber libzmq pipes, no loopback hop, no transport-internal
    buffering. libzmq's XPUB therefore has exactly the subscribers the
    operator created; fan-out bookkeeping lives here, bounded and observed.
  * Each subscriber gets a small, *weight*-bounded queue (bytes, not count —
    payload blocks, not message count). Overflow drops NEWEST (the queue already
    holds the subscriber's recent history; a slow consumer loses the live
    tail, not backlog) and counts loudly.
  * Slow subscribers never block the publisher: ``deliver`` never awaits a
    subscriber queue; a full queue is a drop + counted warning.
"""

from __future__ import annotations

import asyncio
import contextlib

import structlog

log = structlog.get_logger(__name__)

# Per-subscriber queue cap in BYTES of payload (bounded-queue rule: bound memory, not
# count). 256 KiB ≈ a few thousand typical measurement frames.
DEFAULT_SUBSCRIBER_QUEUE_BYTES = 256 * 1024

# Loud-drop warning cadence: warn on the first drop, then every Nth.
DROP_WARN_EVERY = 1000


def topic_matches(pattern: str, topic: str) -> bool:
    """Subscription matching with boundary awareness.

    ``*`` matches everything. A pattern ending in ``.`` is an explicit
    prefix (``cpu.`` matches ``cpu.load``) — the legacy contract, still
    honoured. A dot-less pattern matches the exact topic or its dot-
    delimited children (``cpu`` matches ``cpu.load``) but NOT siblings
    sharing a prefix (``stress.m1`` does NOT match ``stress.m10`` — the
    old ``str.startswith`` behaviour fanned one topic out to ten under
    stress traffic, a 7x delivery amplification).
    """
    if pattern == "*":
        return True
    if pattern.endswith("."):
        return topic.startswith(pattern)
    if topic == pattern:
        return True
    return topic.startswith(pattern + ".")


class _Subscriber:
    __slots__ = (
        "conn_id",
        "dropped",
        "pending_bytes",
        "queue",
        "queue_bytes",
        "topic",
    )

    def __init__(self, conn_id: str, topic: str, queue_bytes: int) -> None:
        self.conn_id = conn_id
        self.topic = topic
        self.queue_bytes = queue_bytes
        self.queue: asyncio.Queue[tuple[str, bytes] | None] = asyncio.Queue()
        self.pending_bytes = 0
        self.dropped = 0


class FanoutHub:
    """Per-subscriber bounded queues fed directly by ``PubSub.publish()``.

    Lifecycle: ``attach()`` registers a WS connection, ``recv()`` is the
    writer side, ``detach()``/``stop()`` tear down. No ZMQ sockets involved.
    """

    def __init__(
        self,
        pubsub: object,
        *,
        queue_bytes: int = DEFAULT_SUBSCRIBER_QUEUE_BYTES,
    ) -> None:
        self.pubsub = pubsub
        self.queue_bytes = queue_bytes
        self._subs: dict[str, _Subscriber] = {}
        self._sub_seq = 0
        self.published_total = 0
        self.dropped_total = 0

    # -- subscriber management ------------------------------------------------

    def attach(self, topic: str, conn_id: str | None = None) -> str:
        """Register a subscriber for ``topic``; returns its conn_id."""
        if conn_id is None:
            self._sub_seq += 1
            conn_id = f"fanout-{self._sub_seq}"
        self._subs[conn_id] = _Subscriber(conn_id, topic, self.queue_bytes)
        return conn_id

    def detach(self, conn_id: str) -> None:
        """Remove a subscriber; a later recv() returns ``("closed", None)``."""
        sub = self._subs.pop(conn_id, None)
        if sub is not None:
            with contextlib.suppress(asyncio.QueueFull):
                sub.queue.put_nowait(None)

    def subscriber_count(self) -> int:
        return len(self._subs)

    def subscriber_stats(self) -> dict[str, dict[str, int]]:
        return {
            s.conn_id: {
                "topic_len": len(s.topic),
                "queue_depth": s.queue.qsize(),
                "pending_bytes": s.pending_bytes,
                "dropped": s.dropped,
            }
            for s in self._subs.values()
        }

    # -- delivery path (called from PubSub.publish(); never blocks) -----------

    def deliver(self, topic: str, payload: bytes) -> int:
        """Fan one frame out to matching subscribers. Returns delivery count.

        Weight rule: a frame is enqueued when it fits the subscriber's byte
        budget — OR when the queue is empty and the frame alone exceeds the
        budget (an oversized single frame passes through rather than being
        undeliverable). Otherwise the NEWEST frame is dropped and counted
        loudly (drop deliberately, never silently).
        """
        self.published_total += 1
        delivered = 0
        for sub in self._subs.values():
            if not topic_matches(sub.topic, topic):
                continue
            fits = sub.pending_bytes + len(payload) <= sub.queue_bytes
            oversized_single = sub.pending_bytes == 0 and len(payload) > sub.queue_bytes
            if not fits and not oversized_single:
                sub.dropped += 1
                self.dropped_total += 1
                if sub.dropped == 1 or sub.dropped % DROP_WARN_EVERY == 0:
                    log.warning(
                        "exceeded publish hwm for subscriber, dropping message",
                        subscriber=sub.conn_id,
                        topic=topic,
                        dropped=sub.dropped,
                        queue_bytes=sub.queue_bytes,
                        payload_bytes=len(payload),
                    )
                continue
            sub.pending_bytes += len(payload)
            sub.queue.put_nowait((topic, payload))
            delivered += 1
        return delivered

    # -- writer side -----------------------------------------------------------

    async def recv(
        self, conn_id: str, timeout: float | None = None
    ) -> tuple[str, tuple[str, bytes] | None]:
        """Next frame for a subscriber.

        Returns ``("frame", (topic, payload))``, ``("timeout", None)`` when
        ``timeout`` elapsed (caller sends its keepalive), or ``("closed",
        None)`` when the subscriber is gone. Cancellation-safe: the queue
        item is never lost on timeout (asyncio.Queue.get either completes or
        leaves the item for the next getter).
        """
        sub = self._subs.get(conn_id)
        if sub is None:
            return "closed", None
        if timeout is not None:
            try:
                item = await asyncio.wait_for(sub.queue.get(), timeout)
            except TimeoutError:
                return "timeout", None
        else:
            item = await sub.queue.get()
        if item is None:
            # None is the detach()/stop() sentinel, not a frame.
            return "closed", None
        sub.pending_bytes -= len(item[1])
        return "frame", item

    # -- teardown --------------------------------------------------------------

    async def stop(self) -> None:
        """Wake all subscriber queues so recv() callers see ``closed``."""
        for sub in list(self._subs.values()):
            with contextlib.suppress(asyncio.QueueFull):
                sub.queue.put_nowait(None)
        log.info("fanout hub stopped", subscribers=len(self._subs))

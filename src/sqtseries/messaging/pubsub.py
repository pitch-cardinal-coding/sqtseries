"""PUB/SUB live-subscription bus with subscriber tracking via XPUB.
The socket is an XPUB (not plain PUB) so the service receives subscription and
unsubscription events directly. A background reader task decodes those events
and updates the registry, making ZMQ SUB subscriber counts exact at all times.
"""

import asyncio
import contextlib
import time

import structlog
import zmq
import zmq.asyncio

from .protocol import dumps

log = structlog.get_logger(__name__)


class SubscriptionTracker:
    """Track active/lingering topics for a PUB socket."""

    def __init__(self, linger_seconds: float = 30.0):
        self.linger_seconds = linger_seconds
        self._active: set[str] = set()
        self._linger_until: dict[str, float] = {}

    def subscribe(self, topic: str) -> None:
        self._active.add(topic)
        self._linger_until.pop(topic, None)

    def unsubscribe(self, topic: str) -> None:
        self._active.discard(topic)
        self._linger_until[topic] = time.monotonic() + self.linger_seconds

    def cleanup(self) -> None:
        now = time.monotonic()
        expired = [t for t, until in self._linger_until.items() if now >= until]

        for topic in expired:
            del self._linger_until[topic]

    @property
    def active_topics(self) -> set[str]:
        return set(self._active)

    def stats(self) -> dict[str, int]:
        return {"active": len(self._active), "lingering": len(self._linger_until)}


class PubSub:
    """XPUB socket + subscription tracker: publish + know who is subscribed.

    The XPUB socket publishes measurements normally; a background reader task

    decodes ``\\x01topic`` (subscribe) and ``\\x00topic`` (unsubscribe) events

    and updates the optional ``registry`` callback in real time.
    """

    def __init__(
        self,
        endpoint: str,
        *,
        linger_seconds: float = 30.0,
        hwm: int = 10000,
        context: zmq.asyncio.Context | None = None,
        registry: object | None = None,
    ):
        self.endpoint = endpoint
        self.linger_seconds = linger_seconds
        self.hwm = hwm
        self._ctx = context
        self.socket: zmq.asyncio.Socket | None = None
        self.tracker = SubscriptionTracker(linger_seconds)
        # Counts publish attempts, not deliveries: it increments for every
        # publish() call even when libzmq drops the frame for a slow
        # subscriber (per-subscriber HWM drop), so it never reflects
        # per-subscriber receipt.
        self.published = 0
        self._registry = registry
        self._reader_task: asyncio.Task | None = None

    async def start(self) -> None:
        from zmq.asyncio import Context as AContext

        ctx = self._ctx or AContext.instance()
        self.socket = ctx.socket(zmq.XPUB)
        # Emit a subscription event for EVERY join/leave, not just topic-trie
        # transitions. Without this, a second subscriber to an already-known
        # topic generates no event (verified on libzmq 4.3.5: 2 subscribers on
        # "cpu" reported as 1) so per-connection counts would be wrong.

        self.socket.setsockopt(zmq.XPUB_VERBOSER, 1)
        # Unlimited inbound subscriptions; re-subscribe after disconnect

        self.socket.setsockopt(zmq.RCVHWM, 0)
        self.socket.immediate = 1
        self.socket.setsockopt(zmq.LINGER, 500)
        self.socket.setsockopt(zmq.SNDHWM, self.hwm)
        self.socket.bind(self.endpoint)
        self._reader_task = asyncio.create_task(
            self._read_subscriptions(), name="pubsub-xpub-reader"
        )
        log.info("pubsub listening (xpub)", endpoint=self.endpoint)

    async def publish(self, topic: bytes | str, payload: dict) -> None:
        """Publish a measurement to subscribers of ``topic``."""
        if self.socket is None:
            raise RuntimeError("pubsub not started")
        if isinstance(topic, str):
            topic = topic.encode()
        self.tracker.cleanup()
        await self.socket.send_multipart([topic, dumps(payload)])
        self.published += 1

    async def _read_subscriptions(self) -> None:
        """Decode XPUB subscription messages and update registry + tracker."""

        while self.socket is not None:
            try:
                event = await asyncio.wait_for(self._recv_one(), timeout=0.5)
            except TimeoutError:
                continue
            except asyncio.CancelledError:
                return
            if not event:
                # idle tick: sweep expired lingering topics here so _linger_until
                # stays bounded even when no data is ever published (cleanup()
                # would otherwise only run inside publish())
                self.tracker.cleanup()
                continue
            event_type, topic = event
            try:
                topic_str = topic.decode("utf-8")
            except UnicodeDecodeError:
                continue
            if event_type == 1:
                self.tracker.subscribe(topic_str)
                if self._registry is not None:
                    self._registry.register_zmq_sub(topic_str)
            elif event_type == 0:
                self.tracker.unsubscribe(topic_str)
                if self._registry is not None:
                    self._registry.unregister_zmq_sub(topic_str)

    async def _recv_one(self) -> tuple[int, bytes] | None:
        """Read one subscription event from the XPUB socket, non-blocking.

        Returns ``(event_type, topic)`` where ``event_type`` is 1 (subscribe)

        or 0 (unsubscribe), or ``None`` if no event is available.
        """
        if self.socket is None:
            return None
        events = await self.socket.poll(timeout=50, flags=zmq.POLLIN)
        if not events:
            return None
        frame = await self.socket.recv()

        if not frame:
            return None
        event_type = frame[0]
        if event_type not in (0, 1):
            return None
        return int(event_type), bytes(frame[1:])

    async def stop(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None

    def stats(self) -> dict[str, int]:
        stats = {"published": self.published}
        stats.update(self.tracker.stats())
        return stats

"""Stats PUB socket: broadcasts connection and subscription events in JSON."""

import asyncio
import contextlib
import time

import orjson
import structlog
import zmq
import zmq.asyncio

log = structlog.get_logger(__name__)

REPORT_INTERVAL = 10.0


class StatsPublisher:
    """PUB socket that emits ``conn``, ``sub``, and ``report`` events.

    Consumers subscribe with prefix filters (e.g. ``conn``, ``sub``, or ``""``
    for all) using a standard ZMQ SUB socket.
    """

    def __init__(
        self,
        endpoint: str,
        registry: object,
        *,
        report_interval: float = REPORT_INTERVAL,
        context: zmq.asyncio.Context | None = None,
    ):
        self.endpoint = endpoint
        self.registry = registry
        self.report_interval = report_interval
        self._ctx = context
        self.socket: zmq.asyncio.Socket | None = None
        self._task: asyncio.Task | None = None
        self._publish_tasks: set[asyncio.Task] = set()
        self._started_at: float = 0.0
        self._event_hook = None

    async def start(self) -> None:
        from zmq.asyncio import Context as AContext

        ctx = self._ctx or AContext.instance()
        self.socket = ctx.socket(zmq.PUB)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.SNDHWM, 1000)
        self.socket.bind(self.endpoint)
        self._started_at = time.time()
        self._task = asyncio.create_task(self._report_loop(), name="stats-publisher")
        # Hook the registry so stats events are forwarded in real time
        self._event_hook = self._on_registry_event
        self.registry.on_event(self._event_hook)
        log.info("stats publisher started", endpoint=self.endpoint)

    async def publish(self, event_type: str, payload: dict) -> None:
        """Publish a single stats event. Safe to call externally."""
        if self.socket is None:
            return
        frame = orjson.dumps(payload, default=str)
        topic = event_type.encode()
        await self.socket.send_multipart([topic, frame])

    def _on_registry_event(self, event_type: str, payload: dict) -> None:
        """Forward registry events into the stats PUB socket."""
        if self.socket is None:
            return
        task = asyncio.create_task(self.publish(event_type, payload))
        self._publish_tasks.add(task)
        task.add_done_callback(self._publish_tasks.discard)

    async def _report_loop(self) -> None:
        """Publish a ``report`` event every ``report_interval`` seconds."""
        while True:
            await asyncio.sleep(self.report_interval)
            snapshot = self.registry.snapshot()
            report = {
                "uptime_s": round(time.time() - self._started_at, 1),
                "ws_connections": snapshot["ws_connections"],
                "zmq_subscribers": snapshot["zmq_subscribers"],
                "active_topics": snapshot["active_topics"],
            }
            await self._emit_report(report)

    async def _emit_report(self, report: dict) -> None:
        try:
            await self.publish("report", report)
        except Exception:
            log.warning("stats report publish failed", exc_info=True)

    async def stop(self) -> None:
        if self._event_hook is not None:
            # unhook BEFORE draining, so no new per-event tasks are spawned
            self.registry.remove_listener(self._event_hook)
            self._event_hook = None
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._publish_tasks:
            # drain in-flight event-forward tasks (they no-op on a closed socket)
            await asyncio.gather(*self._publish_tasks, return_exceptions=True)
            self._publish_tasks.clear()
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None

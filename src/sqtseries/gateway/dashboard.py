"""Admin dashboard backend: snapshot builder + push WebSocket stream.

``/ws/dashboard`` sends a full snapshot on connect, then live
``conn``/``sub`` registry events plus a 1s counter tick — no polling.
Mirrors the ``/ws/connections`` pattern (bounded queue, listener
unhooked in ``finally``, tick task cancelled on disconnect).
"""

import asyncio
import time
from collections.abc import Callable
from typing import Any

TICK_INTERVAL_S = 1.0
MAX_QUEUE = 500

_LIST_KEYS = ("connections", "subscriptions")


def fallback_snapshot(store: Any, registry: Any) -> dict[str, Any]:
    """Best-effort snapshot when no service stats provider is installed."""
    payload: dict[str, Any] = {
        "type": "snapshot",
        "status": "ok" if store is not None else "degraded",
        "server_time": time.time(),
    }
    if store is not None:
        with _suppress():
            payload["metrics"] = len(store.list_metrics())
        with _suppress():
            payload["series"] = store.series_count()
    if registry is not None:
        snap = registry.snapshot()
        payload["ws_connections"] = snap["ws_connections"]
        payload["zmq_subscribers"] = snap["zmq_subscribers"]
        payload["connections"] = registry.list_connections()
        payload["subscriptions"] = snap["subscriptions"]
    return payload


class _suppress:
    """Tiny context manager: best-effort reads must never break the stream."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, *args: object) -> bool:
        return True


async def dashboard_stream(
    websocket: Any,
    *,
    provider: Callable[[], dict[str, Any]] | None,
    store: Any,
    registry: Any,
) -> None:
    """Serve one ``/ws/dashboard`` connection until it disconnects."""
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    def on_event(event_type: str, payload: dict[str, Any]) -> None:
        loop.call_soon_threadsafe(queue.put_nowait, {"type": event_type, **payload})

    if registry is not None:
        registry.on_event(on_event)
    try:
        # Snapshot/tick may touch SQLite (series counts, watermark): keep
        # them off the event loop like the query path does.
        if provider is not None:
            snap = await asyncio.to_thread(provider)
        else:
            snap = fallback_snapshot(store, registry)
        snap["type"] = "snapshot"
        await websocket.send_json(snap)

        async def ticker() -> None:
            while True:
                await asyncio.sleep(TICK_INTERVAL_S)
                if provider is not None:
                    tick = await asyncio.to_thread(provider)
                else:
                    tick = fallback_snapshot(store, registry)
                tick = {k: v for k, v in tick.items() if k not in _LIST_KEYS}
                tick["type"] = "tick"
                loop.call_soon_threadsafe(queue.put_nowait, tick)

        async def sender() -> None:
            while True:
                item = await queue.get()
                # A stalled client must not grow memory without bound:
                # drop to a bounded backlog instead of accumulating.
                while queue.qsize() > MAX_QUEUE:
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                await websocket.send_json(item)

        async def watch_disconnect() -> None:
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    return

        tick_task = asyncio.create_task(ticker())
        send_task = asyncio.create_task(sender())
        watch_task = asyncio.create_task(watch_disconnect())
        _done, pending = await asyncio.wait(
            {send_task, watch_task}, return_when=asyncio.FIRST_COMPLETED
        )
        tick_task.cancel()
        for task in pending:
            task.cancel()
        await asyncio.gather(tick_task, send_task, watch_task, return_exceptions=True)
    finally:
        if registry is not None:
            registry.remove_listener(on_event)

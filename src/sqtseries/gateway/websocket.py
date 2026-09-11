"""WebSocket subscription handler.

Bounded fan-out: the connection attaches to the shared FanoutHub
(one SUB reader on the XPUB) and receives frames from its own bounded
queue. A slow WS client fills only ITS queue — the hub drops NEWEST frames
for that client, counts loudly, and the publisher never notices.
"""

import asyncio
import contextlib

import structlog
from fastapi import WebSocket, WebSocketDisconnect

from ..messaging.connection_registry import new_connection_id
from ..messaging.fanout import FanoutHub

log = structlog.get_logger(__name__)

# If a frame cannot be flushed to the client within this long, the client is
# effectively dead (not reading); close it instead of holding the connection
# and its subscription hostage. Normal sends are milliseconds.
SEND_TIMEOUT = 30.0

# Keepalive cadence for idle subscriber links (mirrors the old SUB recv
# timeout so clients see the same ping behaviour).
KEEPALIVE_S = 30.0


async def subscribe_and_forward(
    websocket: WebSocket,
    *,
    pubsub: object,
    topic: str = "*",
    registry: object | None = None,
) -> None:
    """Stream live measurements to a WS client via the shared FanoutHub."""
    conn_id = new_connection_id()
    peer = (
        f"{websocket.client.host}:{websocket.client.port}"
        if websocket.client
        else "unknown"
    )
    hub: FanoutHub = getattr(pubsub, "fanout", None)

    if hub is None:
        # The service wiring always provides the fan-out hub; a missing hub
        # means a misconstructed PubSub. Refuse the stream rather than
        # silently degrading to a second SUB socket on the XPUB.
        log.error("websocket subscribe without fanout hub", peer=peer)
        await websocket.close(code=1011, reason="fanout unavailable")
        return

    if registry is not None:
        registry.register_ws(conn_id, peer, topic)

    sub_id = hub.attach(topic, conn_id=conn_id)
    try:

        async def send_loop() -> None:
            while True:
                kind, item = await hub.recv(conn_id, timeout=KEEPALIVE_S)
                if kind == "closed":
                    return
                if kind == "timeout":
                    if not await _send(websocket, '{"type":"ping"}', "keepalive ping"):
                        return
                    continue
                _topic_bytes, payload = item
                # Track activity: receiving data means the connection is alive.
                if registry is not None:
                    registry.touch_ws(conn_id)
                if not await _send(
                    websocket, payload.decode("utf-8", errors="replace"), "frame"
                ):
                    return

        async def watch_disconnect() -> None:
            # Push-only stream: consume inbound frames so a disconnect (or a
            # server-side close during shutdown) surfaces immediately instead
            # of on the next send (up to 30 s later). Incoming client data is
            # intentionally ignored.
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    return
                if registry is not None:
                    registry.touch_ws(conn_id)

        send_task = asyncio.create_task(send_loop())
        watch_task = asyncio.create_task(watch_disconnect())
        done, pending = await asyncio.wait(
            {send_task, watch_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(send_task, watch_task, return_exceptions=True)

        for task in done:
            exc = task.exception()
            if exc is not None and not isinstance(exc, WebSocketDisconnect):
                log.warning("websocket stream ended: %s", exc)
    finally:
        hub.detach(sub_id)
        if registry is not None:
            registry.unregister_ws(conn_id)


async def _send(websocket: WebSocket, text: str, what: str) -> bool:
    """Send one frame; return False if the client is stuck (evict it)."""
    try:
        await asyncio.wait_for(websocket.send_text(text), timeout=SEND_TIMEOUT)
        return True
    except TimeoutError:
        log.warning("websocket slow consumer, closing", what=what)
        with contextlib.suppress(Exception):
            await websocket.close(code=1011, reason="slow consumer")
        return False

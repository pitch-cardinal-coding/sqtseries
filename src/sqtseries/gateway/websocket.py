"""WebSocket subscription handler.
Proxies live measurements from the pubsub XPUB socket to the WebSocket client
and tracks connection state in the registry.
"""

import asyncio
import contextlib

import structlog
import zmq
import zmq.asyncio
from fastapi import WebSocket, WebSocketDisconnect

from ..messaging.connection_registry import new_connection_id

log = structlog.get_logger(__name__)

# If a frame cannot be flushed to the client within this long, the client is
# effectively dead (not reading); close it instead of holding the connection
# and its ZMQ subscription hostage. Normal sends are milliseconds.
SEND_TIMEOUT = 30.0


async def subscribe_and_forward(
    websocket: WebSocket,
    *,
    pubsub: object,
    topic: str = "*",
    registry: object | None = None,
) -> None:
    """Subscribe to the pubsub endpoint and stream frames to the WS client."""

    conn_id = new_connection_id()

    peer = (
        f"{websocket.client.host}:{websocket.client.port}"
        if websocket.client
        else "unknown"
    )
    ctx = zmq.asyncio.Context.instance()
    sock = ctx.socket(zmq.SUB)
    try:
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.RCVHWM, 1000)
        from ..messaging.context import apply_tcp_keepalive

        apply_tcp_keepalive(sock)
        # Subscribe BEFORE connect: the subscription then travels with the
        # connection handshake. Setting it after connect() leaves a (tiny) race
        # where a measurement published between connect completing and the
        # subscription reaching the XPUB is dropped for this subscriber.
        # Subscribe the actual topic prefix (not empty) so XPUB-side counts
        # attribute this client to its topic; "*" still means everything.

        sock.setsockopt(zmq.SUBSCRIBE, b"" if topic == "*" else topic.encode())
        sock.connect(pubsub.endpoint)
        log.info("websocket subscribed", endpoint=pubsub.endpoint)
        if registry is not None:
            registry.register_ws(conn_id, peer, topic)

        async def send_loop() -> None:
            while True:
                # The SUB socket has no RCVTIMEO, so recv never raises
                # zmq.Again; only the wait_for timeout fires (keepalive).

                try:
                    frames = await asyncio.wait_for(sock.recv_multipart(), timeout=30)
                except TimeoutError:
                    if not await _send(websocket, '{"type":"ping"}', "keepalive ping"):
                        return
                    continue
                topic_bytes = frames[0]

                payload = frames[1] if len(frames) > 1 else b"{}"
                # Track activity: receiving data means the connection is alive.

                if registry is not None:
                    registry.touch_ws(conn_id)
                if topic != "*" and not topic_bytes.startswith(topic.encode()):
                    continue
                if not await _send(
                    websocket, payload.decode("utf-8", errors="replace"), "frame"
                ):
                    return

        async def watch_disconnect() -> None:
            # The send loop never reads from the WebSocket, so a client
            # disconnect would otherwise only surface on the next send (up to
            # 30s later, after the keepalive ping timeout). That lingering
            # connection task keeps uvicorn's graceful shutdown waiting. Consume
            # inbound frames in a sibling task so we notice a disconnect (or a
            # server-side close during shutdown) immediately. Incoming client
            # data is intentionally ignored — this stream is push-only.
            # ``receive()`` returns the disconnect as a plain message (only the
            # ``receive_text()`` helpers raise), so check the type explicitly.

            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    return
                # Track activity: any inbound frame means the client is alive.

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
        if registry is not None:
            registry.unregister_ws(conn_id)
        sock.close(linger=0)


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

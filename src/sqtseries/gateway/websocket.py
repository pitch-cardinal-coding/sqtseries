"""WebSocket subscription handler.

Proxies live measurements from the pubsub XPUB socket to the WebSocket client
and tracks connection state in the registry.
"""

import asyncio

import structlog
import zmq
import zmq.asyncio
from fastapi import WebSocket, WebSocketDisconnect

from ..messaging.connection_registry import new_connection_id

log = structlog.get_logger(__name__)


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
        sock.connect(pubsub.endpoint)
        sock.setsockopt(zmq.SUBSCRIBE, b"")
        log.info("websocket subscribed", endpoint=pubsub.endpoint)
        if registry is not None:
            registry.register_ws(conn_id, peer, topic)
        while True:
            try:
                frames = await asyncio.wait_for(sock.recv_multipart(), timeout=30)
            except TimeoutError:
                await websocket.send_text('{"type":"ping"}')
                continue
            except zmq.Again:
                continue
            topic_bytes = frames[0]
            payload = frames[1] if len(frames) > 1 else b"{}"
            if topic != "*" and not topic_bytes.startswith(topic.encode()):
                continue
            await websocket.send_text(payload.decode("utf-8", errors="replace"))
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        log.warning("websocket stream ended: %s", exc)
    finally:
        if registry is not None:
            registry.unregister_ws(conn_id)
        sock.close(linger=0)

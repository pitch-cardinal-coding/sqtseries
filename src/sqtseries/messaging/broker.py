"""Query broker: REP socket serving query/aggregate requests.

The spec targets ROUTER/DEALER for async multi-client; Phase 5 wires the
lifecycle. The broker here provides the REP/ROUTER query responder with a pluggable
handler. For v1 simple REP reply; ROUTER supported when ``use_router=True``.
"""

import asyncio
from collections.abc import Callable
from typing import Any

import structlog
import zmq
import zmq.asyncio

from ..config import QuerySettings
from .protocol import ProtocolError, dumps, loads

log = structlog.get_logger(__name__)


class QueryBroker:
    """Serve query requests over a REP (or ROUTER) socket.

    ``handler(query_dict) -> dict``  (synchronous result) or
    ``handler_async(query_dict) -> awaitable``  (async handler).
    """

    def __init__(
        self,
        endpoint: str,
        settings: QuerySettings,
        handler: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
        context: zmq.asyncio.Context | None = None,
        use_router: bool = False,
        handler_timeout_s: float | None = None,
    ):
        self.endpoint = endpoint
        self.settings = settings
        self.handler = handler
        self.use_router = use_router
        self._ctx = context
        self.handler_timeout_s = handler_timeout_s
        self.socket: zmq.asyncio.Socket | None = None
        self.requests = 0
        self.errors = 0

    async def start(self) -> None:
        from zmq.asyncio import Context as AContext

        ctx = self._ctx or AContext.instance()
        sock_type = zmq.ROUTER if self.use_router else zmq.REP
        self.socket = ctx.socket(sock_type)
        from .context import apply_options, socket_options

        apply_options(self.socket, socket_options(hwm=1000))
        if self.use_router:
            self.socket.router_mandatory = False
        self.socket.bind(self.endpoint)
        log.info("query broker listening", endpoint=self.endpoint)

    async def run_once(self, block: bool = True) -> None:
        if self.socket is None:
            raise RuntimeError("broker not started")
        try:
            if self.use_router:
                frames = await self.socket.recv_multipart(
                    flags=0 if block else zmq.NOBLOCK
                )
                await self._handle_router(frames)
                return
            raw = await self.socket.recv(flags=0 if block else zmq.NOBLOCK)
        except zmq.Again:
            return
        except zmq.ZMQError:
            return
        await self._handle(raw)

    async def _handle(self, raw: bytes) -> None:
        try:
            query = loads(raw)
            result = await self._dispatch(query)
        except TimeoutError:
            self.errors += 1
            result = {
                "status": "error",
                "error": {
                    "code": "QUERY_TIMEOUT",
                    "message": f"query exceeded {self.handler_timeout_s}s",
                },
            }
        except ProtocolError as exc:
            result = {
                "status": "error",
                "error": {"code": "INVALID_REQUEST", "message": str(exc)},
            }
        except Exception as exc:
            self.errors += 1
            log.exception("query handler error")
            result = {
                "status": "error",
                "error": {"code": "INTERNAL_ERROR", "message": str(exc)},
            }
        await self._send(dumps(result))

    async def _handle_router(self, frames: list[bytes]) -> None:
        identity = frames[0]
        if len(frames) < 2:
            await self._send_router(
                identity,
                dumps(
                    {
                        "status": "error",
                        "error": {
                            "code": "INVALID_REQUEST",
                            "message": "missing payload",
                        },
                    }
                ),
            )
            return
        raw = frames[-1]

        try:
            query = loads(raw)
            result = await self._dispatch(query)
        except TimeoutError:
            self.errors += 1
            result = {
                "status": "error",
                "error": {
                    "code": "QUERY_TIMEOUT",
                    "message": f"query exceeded {self.handler_timeout_s}s",
                },
            }
        except ProtocolError as exc:
            result = {
                "status": "error",
                "error": {"code": "INVALID_REQUEST", "message": str(exc)},
            }
        except Exception as exc:
            self.errors += 1
            log.exception("query handler error")
            result = {
                "status": "error",
                "error": {"code": "INTERNAL_ERROR", "message": str(exc)},
            }
        await self._send_router(identity, dumps(result))

    async def _dispatch(self, query: dict[str, Any]) -> dict[str, Any]:
        """Run the handler, honoring ``handler_timeout_s`` when set.

        The default (no timeout) executes synchronously inline, exactly as
        before. With a timeout, the blocking handler moves to a worker
        thread so a slow query never stalls the event loop; if it exceeds
        the budget the client gets a ``QUERY_TIMEOUT`` error instead of
        hanging.
        """
        self.requests += 1
        if self.handler is None:
            raise ProtocolError("no query handler installed")
        if self.handler_timeout_s is None or self.handler_timeout_s <= 0:
            # None or <= 0 disables the budget (runs synchronously inline)
            return self.handler(query)
        return await asyncio.wait_for(
            asyncio.to_thread(self.handler, query), self.handler_timeout_s
        )

    async def _send(self, payload: bytes) -> None:
        if self.socket is None:
            return
        try:
            # Block (wait for HWM capacity) instead of NOBLOCK: a REP socket
            # requires strict recv/send alternation, so dropping a reply on
            # EAGAIN would leave it stuck mid-transaction and wedge the broker
            # for every client until restart.
            await self.socket.send(payload)
        except zmq.ZMQError as exc:
            log.error("query reply send failed: %s", exc)

    async def _send_router(self, identity: bytes, payload: bytes) -> None:
        if self.socket is None:
            return
        try:
            await self.socket.send_multipart(
                [identity, b"", payload], flags=zmq.NOBLOCK
            )
        except zmq.Again:
            log.warning("router reply send would block")
        except zmq.ZMQError as exc:
            log.error("router reply send failed: %s", exc)

    async def stop(self) -> None:
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None

    def stats(self) -> dict[str, int]:
        return {
            "requests": self.requests,
            "errors": self.errors,
        }

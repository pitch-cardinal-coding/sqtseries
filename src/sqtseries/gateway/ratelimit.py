"""Fixed-window rate limiting middleware for the HTTP gateway."""

import time
from collections import defaultdict

from fastapi import Request, Response
from starlette.status import HTTP_429_TOO_MANY_REQUESTS
from starlette.types import ASGIApp, Receive, Scope, Send


class RateLimitMiddleware:
    """Reject requests exceeding ``limit`` per fixed minute window.

    Keyed by the client peer address. The ``X-Forwarded-For`` header is NOT
    trusted (it is client-controlled and would let anyone rotate it to bypass
    the limit); use a trusted proxy's header rewriting if you need it.
    """

    def __init__(self, app: ASGIApp, limit_per_minute: int):
        self.app = app
        self.limit = limit_per_minute
        self._hits: dict[str, tuple[int, int]] = defaultdict(lambda: (0, 0))
        self._max_keys = 10_000

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        client = request.client.host if request.client else "unknown"

        now = int(time.time())
        window = now // 60
        # bound memory: only current-window keys matter; drop older ones when
        # the table grows (blocks a "random X-Forwarded-For"-style memory leak).
        # Within a single window the sweep above clears nothing, so cap the
        # table hard by evicting the oldest entries (FIFO) — extreme IP churn
        # degrades tracking rather than growing memory without bound.
        if len(self._hits) > self._max_keys:
            stale = [k for k, (_, w) in self._hits.items() if w != window]
            for k in stale:
                del self._hits[k]
            excess = len(self._hits) - self._max_keys
            if excess > 0:
                for k in list(self._hits)[:excess]:
                    del self._hits[k]
        count, seen = self._hits[client]
        if seen != window:
            self._hits[client] = (1, window)
        else:
            count += 1
            if count > self.limit:
                response = Response(
                    content=b'{"status":"error","error":{"code":"RATE_LIMITED","message":"too many requests"}}',
                    status_code=HTTP_429_TOO_MANY_REQUESTS,
                    media_type="application/json",
                )
                await response(scope, receive, send)
                return
            self._hits[client] = (count, window)

        await self.app(scope, receive, send)

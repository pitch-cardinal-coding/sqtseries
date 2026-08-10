"""Rate-limiting middleware tests."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from sqtseries.gateway import ratelimit as ratelimit_mod
from sqtseries.gateway.ratelimit import RateLimitMiddleware


def _app(limit: int) -> FastAPI:
    app = FastAPI()

    @app.get("/ping")
    async def ping():
        return {"ok": True}

    app.add_middleware(RateLimitMiddleware, limit_per_minute=limit)
    return app


def test_allows_under_limit():
    c = TestClient(_app(limit=5))
    for _ in range(5):
        assert c.get("/ping").status_code == 200


def test_rejects_over_limit():
    c = TestClient(_app(limit=3))
    for _ in range(3):
        assert c.get("/ping").status_code == 200
    r = c.get("/ping")
    assert r.status_code == 429
    assert "RATE_LIMITED" in r.text


def test_window_resets(monkeypatch):
    """A new 60s window restores the full allowance (fixed-window reset)."""
    clock = {"t": 1_700_000_000.0}
    monkeypatch.setattr(ratelimit_mod.time, "time", lambda: clock["t"])
    c = TestClient(_app(limit=1))
    assert c.get("/ping").status_code == 200
    assert c.get("/ping").status_code == 429
    # advance past the 60s window boundary
    clock["t"] += 61
    assert c.get("/ping").status_code == 200
    assert c.get("/ping").status_code == 429


def test_different_clients_independent():
    c = TestClient(_app(limit=1))
    assert c.get("/ping").status_code == 200
    assert c.get("/ping").status_code == 429


def test_xff_header_does_not_bypass_limit():
    """The X-Forwarded-For header is NOT trusted: a client can't rotate it to
    evade the per-peer limit."""
    c = TestClient(_app(limit=2))
    assert c.get("/ping").status_code == 200
    assert c.get("/ping", headers={"X-Forwarded-For": "10.0.0.99"}).status_code == 200
    assert c.get("/ping", headers={"X-Forwarded-For": "10.0.0.100"}).status_code == 429


async def _hit(mw, host: str) -> None:
    """Drive the middleware once with a fake HTTP scope for ``host``."""

    class _Send:
        def __init__(self):
            self.calls = []

        async def __call__(self, message):
            self.calls.append(message)

    scope = {
        "type": "http",
        "client": (host, 12345),
        "method": "GET",
        "path": "/",
        "headers": [],
        "query_string": b"",
        "scheme": "http",
        "server": ("127.0.0.1", 80),
        "http_version": "1.1",
    }

    async def _receive() -> bytes:
        return b""

    await mw(scope, _receive, _Send())


def test_hits_table_bounded_within_single_window():
    """IP churn inside one 60s window must not grow _hits past the cap.

    The stale sweep only removes keys from *other* windows, so without a hard
    cap a flood of distinct clients within one window grew _hits without
    bound despite _max_keys=10000.
    """
    import asyncio

    from sqtseries.gateway.ratelimit import RateLimitMiddleware

    async def _app(scope, receive, send):
        return None

    mw = RateLimitMiddleware(_app, limit_per_minute=600)

    async def run():
        for i in range(30000):
            host = f"10.{i // 65536}.{(i // 256) % 256}.{i % 256}"
            await _hit(mw, host)
        return len(mw._hits)

    size = asyncio.run(run())
    assert size <= mw._max_keys + 1

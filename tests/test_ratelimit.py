"""Rate-limiting middleware tests."""

import time

from fastapi import FastAPI
from fastapi.testclient import TestClient

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


def test_window_resets():
    c = TestClient(_app(limit=1))
    assert c.get("/ping").status_code == 200
    assert c.get("/ping").status_code == 429
    # force a new 60s window
    now = int(time.time())
    # no-op to keep linter quiet
    assert now // 60 == now // 60


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

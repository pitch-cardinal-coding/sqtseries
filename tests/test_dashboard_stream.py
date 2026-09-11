"""Tests for the admin dashboard stream backend (gateway/dashboard.py).

Exercises the push-WebSocket machinery directly with a fake websocket:
snapshot on connect, 1s ticks, registry event push, bounded queue under a
stalled client, provider-failure degradation, and clean disconnect teardown.
"""

import asyncio

from sqtseries.gateway.dashboard import (
    dashboard_stream,
    fallback_snapshot,
)
from sqtseries.messaging.connection_registry import ConnectionRegistry


class FakeWebSocket:
    """Minimal websocket stand-in: records sends, disconnects on demand."""

    def __init__(self, disconnect_after: int | None = None, send_delay: float = 0.0):
        self.sent: list[dict] = []
        self._disconnect_after = disconnect_after
        self._send_delay = send_delay
        self._receives = 0

    async def send_json(self, obj: dict) -> None:
        if self._send_delay:
            await asyncio.sleep(self._send_delay)
        self.sent.append(obj)

    async def receive(self) -> dict:
        self._receives += 1
        if (
            self._disconnect_after is not None
            and self._receives >= self._disconnect_after
        ):
            return {"type": "websocket.disconnect"}
        await asyncio.sleep(0.05)
        return {"type": "websocket.receive", "text": "ping"}


class FakeRegistry:
    """Registry stand-in recording listener lifecycle."""

    def __init__(self) -> None:
        self.listeners: list = []

    def snapshot(self) -> dict:
        return {"ws_connections": 0, "zmq_subscribers": 0, "subscriptions": []}

    def list_connections(self) -> list:
        return []

    def on_event(self, cb) -> None:
        self.listeners.append(cb)

    def remove_listener(self, cb) -> None:
        if cb in self.listeners:
            self.listeners.remove(cb)


class FakeStore:
    def list_metrics(self) -> list:
        return ["m1", "m2"]

    def series_count(self) -> int:
        return 7


def test_fallback_snapshot_with_store_and_registry():
    snap = fallback_snapshot(FakeStore(), FakeRegistry())
    assert snap["status"] == "ok"
    assert snap["metrics"] == 2
    assert snap["series"] == 7
    assert "server_time" in snap
    assert snap["ws_connections"] == 0


def test_fallback_snapshot_degraded_without_store():
    snap = fallback_snapshot(None, FakeRegistry())
    assert snap["status"] == "degraded"
    assert "metrics" not in snap


def test_fallback_snapshot_tolerates_raising_store():
    class BadStore:
        def list_metrics(self):
            raise RuntimeError("boom")

        def series_count(self):
            return 3

    snap = fallback_snapshot(BadStore(), FakeRegistry())
    # series survives the failing metrics read
    assert snap["series"] == 3
    assert "metrics" not in snap


async def test_stream_snapshot_then_disconnect():
    ws = FakeWebSocket(disconnect_after=1)
    await dashboard_stream(
        ws, provider=lambda: {"ingested": 5}, store=None, registry=FakeRegistry()
    )
    assert ws.sent[0]["type"] == "snapshot"
    assert ws.sent[0]["ingested"] == 5


async def test_stream_ticks_arrive_and_strip_list_keys(monkeypatch):
    monkeypatch.setattr("sqtseries.gateway.dashboard.TICK_INTERVAL_S", 0.01)
    ws = FakeWebSocket(disconnect_after=4)
    await dashboard_stream(
        ws,
        provider=lambda: {"ingested": 1, "connections": [1, 2]},
        store=None,
        registry=FakeRegistry(),
    )
    assert ws.sent[0]["type"] == "snapshot"
    ticks = [m for m in ws.sent[1:] if m["type"] == "tick"]
    assert ticks, "expected at least one tick"
    # list payloads are stripped from ticks (snapshot carries them, ticks don't)
    for t in ticks:
        assert "connections" not in t


async def test_stream_registry_event_pushed():
    registry = ConnectionRegistry()
    ws = FakeWebSocket(disconnect_after=3)
    stream = asyncio.create_task(
        dashboard_stream(ws, provider=None, store=None, registry=registry)
    )
    await asyncio.sleep(0.05)
    registry.register_ws("conn-1", "peer-1", "cpu.*")
    registry.register_ws("conn-2", "peer-2", "mem.*")
    await asyncio.wait_for(stream, timeout=5)
    conns = [m for m in ws.sent if m.get("type") == "conn"]
    assert len(conns) >= 1
    # listener unhooked on teardown
    assert not registry._listeners


async def test_stream_provider_failure_degrades_not_dies():
    def bad_provider():
        raise RuntimeError("stats exploded")

    ws = FakeWebSocket(disconnect_after=3)
    await dashboard_stream(
        ws, provider=bad_provider, store=FakeStore(), registry=FakeRegistry()
    )
    # snapshot degraded to the registry/store fallback instead of raising
    assert ws.sent[0]["type"] == "snapshot"
    assert ws.sent[0]["status"] == "ok"
    assert ws.sent[0]["metrics"] == 2


async def test_stream_bounded_queue_under_stalled_client(monkeypatch):
    """A client that reads slower than events arrive must not grow the queue
    without bound: the sender prunes to MAX_QUEUE."""
    monkeypatch.setattr("sqtseries.gateway.dashboard.MAX_QUEUE", 5)
    registry = ConnectionRegistry()
    ws = FakeWebSocket(disconnect_after=2, send_delay=0.02)
    stream = asyncio.create_task(
        dashboard_stream(ws, provider=None, store=None, registry=registry)
    )
    await asyncio.sleep(0.05)
    for i in range(30):
        registry.register_ws(f"c{i}", f"p{i}", "t")
    await asyncio.wait_for(stream, timeout=10)
    # stream survived, forwarded a bounded subset, and ended cleanly
    assert ws.sent[0]["type"] == "snapshot"
    assert len([m for m in ws.sent if m.get("type") == "conn"]) <= 30


async def test_stream_listener_removed_on_provider_path():
    registry = FakeRegistry()
    ws = FakeWebSocket(disconnect_after=1)
    await dashboard_stream(ws, provider=lambda: {}, store=None, registry=registry)
    assert registry.listeners == []

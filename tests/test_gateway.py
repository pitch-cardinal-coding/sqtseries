"""HTTP gateway tests (FastAPI/TestClient + httpx)."""

import asyncio
import socket
import time
from typing import ClassVar

import httpx
import pytest
from fastapi.testclient import TestClient

from sqtseries.config import Settings
from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema
from sqtseries.gateway import create_app
from sqtseries.messaging import PubSub
from sqtseries.query import TimeSeriesDB
from sqtseries.service import Service


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


pytest.importorskip("fastapi")


def _ws_recv(ws, timeout: float = 5):
    """Receive one WebSocket message with a hard timeout (receive_text blocks)."""
    import threading as _t

    result: dict = {}

    def reader():
        try:
            result["msg"] = ws.receive_text()
        except Exception as exc:  # pragma: no cover
            result["exc"] = exc

    t = _t.Thread(target=reader, daemon=True)
    t.start()
    t.join(timeout)
    if "msg" in result:
        return result["msg"]
    if "exc" in result:
        raise result["exc"]
    raise TimeoutError(f"no ws message within {timeout}s")


@pytest.fixture
def client(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "gateway.sqlite"))
    initialize_schema(eng)
    store = StorageEngine(eng)
    app = create_app(store=store)
    return TestClient(app)


@pytest.fixture
def populated_client(client):
    now = time.time_ns()
    base = now - 10 * 1_000_000_000
    for i in range(10):
        tsdb = client.app.state.tsdb
        tsdb.insert(
            "cpu.usage",
            float(i),
            {"host": "web1"},
            timestamp_ns=base + i * 1_000_000_000,
        )
    return client


class TestWrite:
    def test_write_single(self, client):
        r = client.post(
            "/api/v1/write",
            json={"metric": "cpu.usage", "value": 0.5, "tags": {"host": "web1"}},
        )
        assert r.status_code == 200
        assert r.json()["written"] == 1

    def test_write_batch(self, client):
        r = client.post(
            "/api/v1/write",
            json=[
                {"metric": "a", "value": 1.0},
                {"metric": "a", "value": 2.0, "tags": {"x": "y"}},
            ],
        )
        assert r.status_code == 200
        assert r.json()["written"] == 2

    def test_write_invalid_metric(self, client):
        r = client.post("/api/v1/write", json={"value": 1.0})
        assert r.status_code == 400

    def test_write_bad_body(self, client):
        r = client.post("/api/v1/write", json="nope")
        assert r.status_code == 400


class TestRead:
    def test_read_empty(self, populated_client):
        r = populated_client.get("/api/v1/read", params={"metric": "bogus"})
        assert r.status_code == 200
        assert r.json()["data"] == []

    def test_read_data(self, populated_client):
        r = populated_client.get("/api/v1/read", params={"metric": "cpu.usage"})
        assert r.status_code == 200
        body = r.json()
        assert len(body["data"]) == 10

    def test_read_aggregation(self, populated_client):
        r = populated_client.get(
            "/api/v1/read",
            params={"metric": "cpu.usage", "aggregation": "max"},
        )
        body = r.json()
        assert body["data"][0]["value"] == 9.0

    def test_read_missing_metric_param(self, client):
        r = client.get("/api/v1/read")
        assert r.status_code == 422


class TestAggregate:
    def test_aggregate_multiple(self, populated_client):
        r = populated_client.get(
            "/api/v1/aggregate",
            params={"metric": "cpu.usage", "funcs": "avg,max,min"},
        )
        assert r.status_code == 200
        agg = r.json()["aggregations"]
        assert agg["avg"] == 4.5
        assert agg["max"] == 9.0
        assert agg["min"] == 0.0

    def test_aggregate_bad_func(self, populated_client):
        r = populated_client.get(
            "/api/v1/aggregate", params={"metric": "cpu.usage", "funcs": "bogus"}
        )
        assert r.status_code == 400


class TestHealth:
    def test_health(self, client):
        r = client.get("/api/v1/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_request_id_header(self, client):
        r = client.get("/api/v1/health")
        assert "X-Request-ID" in r.headers


class TestCORS:
    def test_cors_headers(self, client):
        r = client.options(
            "/api/v1/write",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "POST",
            },
        )
        assert "access-control-allow-origin" in r.headers


class TestSecurityHeaders:
    """COMPLIANCE.md Security Headers — every HTTP response carries them."""

    REQUIRED: ClassVar[dict[str, str]] = {
        "x-frame-options": "DENY",
        "x-content-type-options": "nosniff",
        "x-xss-protection": "1; mode=block",
        "referrer-policy": "strict-origin-when-cross-origin",
        "permissions-policy": "geolocation=(), microphone=(), camera=(), payment=(), usb=()",
        "content-security-policy": "default-src 'self'",
    }

    def test_headers_present(self, client):
        r = client.get("/api/v1/health")
        assert r.status_code == 200
        for name, value in self.REQUIRED.items():
            assert r.headers.get(name) == value, f"missing/mismatched {name}"

    def test_headers_on_error_response(self, client):
        """Even a 400 response carries the headers."""
        r = client.post("/api/v1/write", json={"value": 1})
        assert r.status_code == 400
        assert r.headers.get("x-content-type-options") == "nosniff"


class TestStats:
    def test_stats(self, populated_client):
        r = populated_client.get("/api/v1/stats")
        assert r.status_code == 200
        body = r.json()
        assert body["metrics"] == 1
        assert body["series"] == 1


class TestWebSocket:
    def test_ws_subscribe_receives(self, tmp_path):
        import socket as _socket
        import threading

        def free_port():
            s = _socket.socket()
            s.bind(("127.0.0.1", 0))
            p = s.getsockname()[1]
            s.close()
            return p

        port = free_port()
        eng = create_sqlite_engine(str(tmp_path / "ws.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)
        ps = PubSub(f"tcp://127.0.0.1:{port}")
        app = create_app(store=store, pubsub=ps)

        # Run the pubsub on a dedicated thread with its own event loop.
        # The WebSocket handler connects to it over tcp (separate loop is fine
        # because ZMQ sockets are per-context, and the handler uses its own
        # zmq.asyncio.Context.instance()).
        results = {}

        def run_pubsub():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def main():
                await ps.start()
                # publish continuously for 4s so the slow-joining WS SUB
                # eventually catches a message
                end = asyncio.get_running_loop().time() + 4.0
                i = 0
                while asyncio.get_running_loop().time() < end:
                    await ps.publish("cpu", {"value": float(i)})
                    i += 1
                    await asyncio.sleep(0.2)
                await ps.stop()

            try:
                loop.run_until_complete(main())
            except Exception as exc:  # pragma: no cover
                results["error"] = exc
            finally:
                loop.close()

        t = threading.Thread(target=run_pubsub, daemon=True)
        t.start()

        client = TestClient(app)
        received = []
        with client.websocket_connect("/ws/subscribe?metric=cpu") as ws:
            import time as _t

            # collect whatever arrives during the publish window
            deadline = _t.monotonic() + 4
            while _t.monotonic() < deadline:
                try:
                    data = ws.receive_text()
                    received.append(data)
                    if received:
                        # got one; done
                        break
                except Exception:
                    break
        t.join(timeout=5)
        assert len(received) >= 1, f"no messages received (err={results.get('error')})"

    def test_http_write_publishes_to_ws(self, tmp_path):
        """HTTP /api/v1/write must broadcast to live WS subscribers, matching
        the ZMQ ingest path (the camera pump writes over HTTP)."""
        import socket as _socket
        import threading

        def free_port():
            s = _socket.socket()
            s.bind(("127.0.0.1", 0))
            p = s.getsockname()[1]
            s.close()
            return p

        port = free_port()
        eng = create_sqlite_engine(str(tmp_path / "ws_http.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)
        ps = PubSub(f"tcp://127.0.0.1:{port}")
        app = create_app(store=store, pubsub=ps)

        results = {}

        def run_pubsub():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def main():
                await ps.start()
                # keep the XPUB alive for the window; the HTTP writes below
                # drive the publishing
                end = asyncio.get_running_loop().time() + 4.0
                while asyncio.get_running_loop().time() < end:
                    await asyncio.sleep(0.1)
                await ps.stop()

            try:
                loop.run_until_complete(main())
            except Exception as exc:  # pragma: no cover
                results["error"] = exc
            finally:
                loop.close()

        t = threading.Thread(target=run_pubsub, daemon=True)
        t.start()

        client = TestClient(app)
        received = []
        with client.websocket_connect("/ws/subscribe?metric=cpu") as ws:
            # publish repeatedly so the slow-joining SUB eventually catches one
            deadline = time.monotonic() + 4
            i = 0
            while time.monotonic() < deadline and not received:
                r = client.post(
                    "/api/v1/write",
                    json={"metric": "cpu", "value": float(i), "tags": {"host": "web1"}},
                )
                assert r.status_code == 200, r.text
                i += 1
                try:
                    received.append(ws.receive_text())
                except Exception:
                    break
        t.join(timeout=5)
        assert received, (
            f"no frame broadcast for HTTP write (err={results.get('error')})"
        )
        assert '"cpu"' in received[0]
        assert '"web1"' in received[0]

    def test_ws_connections_push(self, tmp_path):
        """/ws/connections pushes a snapshot then one frame per registry
        change (conn/sub), live — not a poll."""
        import json as j

        from sqtseries.messaging import ConnectionRegistry

        registry = ConnectionRegistry()
        eng = create_sqlite_engine(str(tmp_path / "conns.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)
        app = create_app(store=store, registry=registry)
        client = TestClient(app)

        with client.websocket_connect("/ws/connections") as ws:
            snap = j.loads(_ws_recv(ws))
            assert snap["type"] == "snapshot"
            assert snap["connections"] == []

            registry.register_ws("c1", "127.0.0.1:1", "*")
            ev = j.loads(_ws_recv(ws))
            assert ev["type"] == "conn"
            assert ev["id"] == "c1" and ev["connected"] is True

            registry.register_zmq_sub("cpu.")
            ev = j.loads(_ws_recv(ws))
            assert ev["type"] == "sub"
            assert ev["topic"] == "cpu." and ev["subscribers"] == 1

            registry.unregister_ws("c1")
            ev = j.loads(_ws_recv(ws))
            assert ev["type"] == "conn"
            assert ev["id"] == "c1" and ev["connected"] is False
            # arrival + leaving times are emitted on the leave event
            assert isinstance(ev.get("connected_at"), float)
            assert isinstance(ev.get("left_at"), float)
            assert ev["left_at"] >= ev["connected_at"]

            registry.unregister_zmq_sub("cpu.")
            ev = j.loads(_ws_recv(ws))
            assert ev["type"] == "sub" and ev["subscribers"] == 0
            assert isinstance(ev.get("left_at"), float)
            assert ev.get("first_seen") is not None
            assert ev["left_at"] >= ev["first_seen"]

    def test_ws_connections_no_listener_leak(self, tmp_path):
        """Repeated connect/disconnect to /ws/connections must not grow the
        registry's listener list (each handler unhooks its listener)."""
        from sqtseries.messaging import ConnectionRegistry

        registry = ConnectionRegistry()
        eng = create_sqlite_engine(str(tmp_path / "conns_leak.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)
        app = create_app(store=store, registry=registry)
        client = TestClient(app)

        for _ in range(10):
            with client.websocket_connect("/ws/connections") as ws:
                assert _ws_recv(ws)  # snapshot
            assert len(registry._listeners) == 0, len(registry._listeners)


class TestQueryDoesNotBlockLoop:
    """A slow HTTP read/aggregate must run off the event loop so health,
    WS streaming, and other requests stay responsive (found via py-spy)."""

    async def test_slow_aggregate_does_not_stall_health(self, tmp_path):
        ports = {
            name: _free_port()
            for name in ("ingestion", "query", "streaming", "admin", "http", "stats")
        }
        s = Settings(
            database={"path": str(tmp_path / "block.sqlite"), "batch_size": 500},
            ingestion={"port": ports["ingestion"]},
            query={"port": ports["query"]},
            streaming={"port": ports["streaming"]},
            admin={"port": ports["admin"]},
            http={"port": ports["http"]},
            stats={"port": ports["stats"]},
            ports={"auto_detect": False},
        )
        svc = Service(s)
        await svc.start()
        orig = TimeSeriesDB.aggregate

        def slow_aggregate(self, *args, **kwargs):
            time.sleep(1.0)  # simulate a heavy aggregate
            return orig(self, *args, **kwargs)

        TimeSeriesDB.aggregate = slow_aggregate
        try:
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{ports['http']}", timeout=10
            ) as c:
                slow = asyncio.create_task(
                    c.get("/api/v1/aggregate", params={"metric": "x", "funcs": "avg"})
                )
                await asyncio.sleep(0.2)  # let the aggregate enter the slow path
                t0 = time.monotonic()
                health = await c.get("/api/v1/health")
                elapsed = time.monotonic() - t0
                assert health.status_code == 200
                assert elapsed < 0.5, f"health took {elapsed:.2f}s — event loop blocked"
                r = await slow
                assert r.status_code == 200
                assert "aggregations" in r.json()
        finally:
            TimeSeriesDB.aggregate = orig
            await svc.shutdown()

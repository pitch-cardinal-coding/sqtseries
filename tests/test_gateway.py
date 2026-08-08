"""HTTP gateway tests (FastAPI/TestClient + httpx)."""

import asyncio
import time

import pytest
from fastapi.testclient import TestClient

from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema
from sqtseries.gateway import create_app
from sqtseries.messaging import PubSub

pytest.importorskip("fastapi")


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

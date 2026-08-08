"""Tests for documented behaviors that lacked coverage (validated via docs audit).

Each test backs a specific claim made in docs/*.html so a regression would
catch both a code bug and a doc lie.
"""

import asyncio
import json
import os
import time

import pytest

from sqtseries.client import Client
from sqtseries.config import Settings
from sqtseries.service import Service


@pytest.fixture
async def running_service(tmp_path, free_ports):
    s = Settings(
        database={"path": str(tmp_path / "c.sqlite"), "batch_size": 500},
        ingestion={"port": free_ports["ingest"]},
        query={"port": free_ports["query"]},
        streaming={"port": free_ports["streaming"]},
        admin={"port": free_ports["admin"]},
        http={"port": free_ports["http"]},
        stats={"port": free_ports["stats"]},
        ports={"auto_detect": False},
    )
    svc = Service(s)
    await svc.start()
    await svc.pool.stop()
    pump_task = asyncio.create_task(_pump(svc))
    try:
        yield svc, s
    finally:
        pump_task.cancel()
        await asyncio.gather(pump_task, return_exceptions=True)
        await svc.shutdown()


async def _pump(svc):
    while True:
        await svc.ingress.run_once(block=False)
        await svc.broker.run_once(block=False)
        await svc.admin_broker.run_once(block=False)
        await asyncio.sleep(0.005)


@pytest.fixture
def client(running_service):
    _, s = running_service
    c = Client(
        ports={
            "write": s.ingestion.port,
            "query": s.query.port,
            "subscribe": s.streaming.port,
            "admin": s.admin.port,
        }
    )
    yield c
    c.close()


class TestTimeUnits:
    async def test_http_read_uses_seconds(self, running_service):
        """GET /api/v1/read start/end are epoch SECONDS."""
        import httpx

        _, s = running_service
        base = f"http://127.0.0.1:{s.http.port}"
        async with httpx.AsyncClient(base_url=base) as c:
            now = time.time()
            r = await c.post(
                "/api/v1/write",
                json={"metric": "cpu", "value": 1.0, "timestamp": now - 5},
            )
            assert r.status_code == 200
            r = await c.get(
                "/api/v1/read",
                params={"metric": "cpu", "start": now - 10, "end": now},
            )
            assert r.status_code == 200
            data = r.json()["data"]
            assert len(data) == 1
            # returned timestamps are in seconds (1.7e9-ish, not 1.7e18)
            assert data[0]["timestamp"] < 2e9

    async def test_client_query_uses_nanoseconds(self, client):
        """Client query start/end are epoch NANOSECONDS."""
        now_ns = time.time_ns()
        # small buffer: int(epoch_s * 1e9) can round a stored ts a hair above
        # the exact ns boundary (float precision), so end must not equal it
        await asyncio.to_thread(client.write, "cpu", 1.0, timestamp=now_ns / 1e9 - 5)
        await asyncio.to_thread(client.write, "cpu", 2.0, timestamp=now_ns / 1e9 - 4)
        await asyncio.sleep(0.3)
        rows = await asyncio.to_thread(
            client.query, "cpu", start=now_ns - 10 * 1e9, end=now_ns + 1e9
        )
        assert len(rows) == 2
        # returned timestamps are seconds
        assert all(r["timestamp"] < 2e9 for r in rows)


class TestErrorCodes:
    async def test_invalid_query_code(self, client, running_service):
        """Bad aggregation with data -> INVALID_QUERY (not INTERNAL_ERROR)."""
        import orjson
        import zmq

        _, s = running_service
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 100)
        # matches fixture query port
        sock.connect(f"tcp://127.0.0.1:{s.query.port}")
        await asyncio.to_thread(client.write, "cpu", 1.0)
        await asyncio.sleep(0.3)

        def raw_query():
            # blocking recv must run in a thread so the async pump can reply
            sock.send(
                orjson.dumps({"type": "query", "metric": "cpu", "aggregation": "bogus"})
            )
            events = sock.poll(5000)
            assert events & zmq.POLLIN, "no query reply within 5s"
            return orjson.loads(sock.recv())

        reply = await asyncio.to_thread(raw_query)
        sock.close(linger=0)
        ctx.term()
        assert reply["status"] == "error"
        assert reply["error"]["code"] == "INVALID_QUERY"


class TestConfigEnv:
    def test_db_path_env_var(self, tmp_path, monkeypatch):
        db = str(tmp_path / "env.sqlite")
        monkeypatch.setenv("SQT_SERIES_DATABASE__PATH", db)
        s = Settings.load(None)
        assert s.db_path_expanded() == db

    def test_config_file_env_var(self, tmp_path, monkeypatch):
        cfg = tmp_path / "c.toml"
        cfg.write_text("[ingestion]\nport = 15001\n")
        monkeypatch.setenv("SQT_SERIES_CONFIG_FILE", str(cfg))
        s = Settings.load(None)
        assert s.ingestion.port == 15001

    def test_rate_limit_env_var(self, monkeypatch):
        monkeypatch.setenv("SQT_SERIES_HTTP__RATE_LIMIT_PER_MINUTE", "42")
        assert Settings.load(None).http.rate_limit_per_minute == 42


class TestAdminReplyFields:
    async def test_health_reply_fields(self, client):
        h = await asyncio.to_thread(client.admin, "health")
        assert h["status"] == "ok"
        assert h["version"] == "0.1.0"
        assert "uptime" in h
        assert h["database"] is True

    async def test_stats_reply_keys(self, client):
        await asyncio.to_thread(client.write, "cpu", 1.0)
        await asyncio.sleep(0.3)
        st = await asyncio.to_thread(client.admin, "stats")
        for key in (
            "uptime_s",
            "ingested",
            "invalid",
            "queries",
            "published",
            "series",
            "metrics",
            "wal_bytes",
            "checkpoints",
        ):
            assert key in st, f"missing {key}"

    async def test_optimize_reply_field(self, client):
        r = await asyncio.to_thread(client.admin, "optimize")
        assert r["status"] == "ok"
        assert r["optimized"] is True


class TestClientSurface:
    def test_context_manager(self, running_service):
        _, s = running_service
        with Client(
            ports={
                "write": s.ingestion.port,
                "query": s.query.port,
                "subscribe": s.streaming.port,
                "admin": s.admin.port,
            }
        ) as c:
            # no exception
            c.write("cpu", 1.0)

    async def test_series_ids_query(self, running_service):
        """Embedded API can query by series_ids directly."""
        from sqtseries.engine import StorageEngine

        svc, _ = running_service
        store = StorageEngine(svc.engine)
        store.insert_many([("cpu", None, 1.0, time.time_ns())])
        sid = store.series_ids_for_metric("cpu")[0]
        rows = list(svc.ts.query(series_ids=[sid]))
        assert len(rows) == 1


class TestCliRunningState:
    def test_status_shows_running(self, tmp_path, monkeypatch):
        """sqtseries status prints running details when the pid is alive."""
        from click.testing import CliRunner

        import sqtseries.cli as cli_mod
        from sqtseries.cli import main

        monkeypatch.setattr(cli_mod, "_pid_is_sqtseries", lambda pid: True)
        cfg = tmp_path / "c.toml"
        db = tmp_path / "db.sqlite"
        cfg.write_text(f'[database]\npath = "{db}"\n\n[ports]\nauto_detect = false\n')
        rt = tmp_path / "runtime.json"
        rt.write_text(
            json.dumps(
                {
                    # this test process is alive
                    "pid": os.getpid(),
                    "version": "0.1.0",
                    "db_path": str(db),
                    "ports": {"ingest": 12501},
                }
            )
        )
        r = CliRunner().invoke(main, ["--config", str(cfg), "status"])
        assert r.exit_code == 0
        assert "running" in r.output
        assert "12501" in r.output

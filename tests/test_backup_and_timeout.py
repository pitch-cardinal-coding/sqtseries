"""Tests for server-side query timeout (query.timeout_s) and the
automatic backup scheduler (backup.enabled / backup.interval).
"""

import asyncio
import json
import sqlite3
import time
from pathlib import Path

import pytest
import zmq
import zmq.asyncio

from sqtseries.config import QuerySettings, Settings, validate_settings
from sqtseries.engine import (
    BackupManager,
    StorageEngine,
    backup_database,
    create_sqlite_engine,
    initialize_schema,
)
from sqtseries.messaging.broker import QueryBroker

# ---------------------------------------------------------------------------
# Query timeout
# ---------------------------------------------------------------------------


@pytest.fixture
def context():
    ctx = zmq.asyncio.Context()
    yield ctx
    ctx.term()


def free_tcp_port() -> int:
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _run_until_reply(broker, client, timeout_loops=200):
    """Pump the broker until the REQ client gets its reply; return bytes."""

    async def _pump():
        for _ in range(timeout_loops):
            await broker.run_once(block=False)
            if client.getsockopt(zmq.EVENTS) & zmq.POLLIN:
                return await client.recv()
            await asyncio.sleep(0.01)
        return None

    return _pump()


class TestQueryTimeout:
    async def test_slow_handler_times_out(self, context):
        """A handler slower than the budget gets QUERY_TIMEOUT back."""
        port = free_tcp_port()

        def slow(q):
            time.sleep(1.0)
            return {"status": "ok", "data": []}

        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}",
            QuerySettings(),
            handler=slow,
            context=context,
            handler_timeout_s=0.2,
        )
        await broker.start()
        try:
            client = context.socket(zmq.REQ)
            client.setsockopt(zmq.LINGER, 100)
            client.connect(f"tcp://127.0.0.1:{port}")
            client.send(b'{"metric":"cpu"}')
            started = time.monotonic()
            resp = await _run_until_reply(broker, client)
            elapsed = time.monotonic() - started
            client.close(linger=0)

            assert resp is not None
            payload = json.loads(resp)
            assert payload["status"] == "error"
            assert payload["error"]["code"] == "QUERY_TIMEOUT"
            assert elapsed < 0.9  # answered well before the 1s handler done
        finally:
            await broker.stop()

    async def test_fast_handler_with_timeout_succeeds(self, context):
        """Handlers within budget still return normally."""
        port = free_tcp_port()
        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}",
            QuerySettings(),
            handler=lambda q: {"status": "ok", "data": ["yes"]},
            context=context,
            handler_timeout_s=1.0,
        )
        await broker.start()
        try:
            client = context.socket(zmq.REQ)
            client.connect(f"tcp://127.0.0.1:{port}")
            client.send(b'{"metric":"cpu"}')
            resp = await _run_until_reply(broker, client)
            client.close(linger=0)
            assert resp is not None
            assert json.loads(resp) == {"status": "ok", "data": ["yes"]}
        finally:
            await broker.stop()

    async def test_no_timeout_means_synchronous(self, context):
        """handler_timeout_s=None keeps the old inline synchronous path."""
        port = free_tcp_port()
        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}",
            QuerySettings(),
            handler=lambda q: {"status": "ok", "data": []},
            context=context,
        )
        await broker.start()
        try:
            client = context.socket(zmq.REQ)
            client.connect(f"tcp://127.0.0.1:{port}")
            client.send(b'{"metric":"cpu"}')
            resp = await _run_until_reply(broker, client)
            client.close(linger=0)
            assert resp is not None
            assert json.loads(resp)["status"] == "ok"
        finally:
            await broker.stop()

    async def test_service_enforces_configured_timeout(self, tmp_path):
        """End-to-end: a running service replies QUERY_TIMEOUT for a query
        that exceeds query.timeout_s (simulated via a slow handler)."""
        from sqtseries.service import Service

        settings = Settings(
            database={"path": str(tmp_path / "t.sqlite")},
            ingestion={"port": free_tcp_port(), "reject_client_timestamp_skew_s": 0},
            query={"port": free_tcp_port(), "timeout_s": 0.2},
            streaming={"port": free_tcp_port()},
            admin={"port": free_tcp_port()},
            http={"port": free_tcp_port()},
            stats={"port": free_tcp_port()},
            ports={"auto_detect": False},
        )
        svc = Service(settings)
        await svc.start()
        # Stop the worker pool and pump manually, as other tests do, so we
        # can drive the loop deterministically.
        await svc.pool.stop()

        # Replace the query handler with a slow one (as if the DB query were
        # slow): the timeout is applied around the handler call.
        async def pump():
            while True:
                await svc.ingress.drain()
                await svc.broker.run_once(block=False)
                await svc.admin_broker.run_once(block=False)
                await asyncio.sleep(0.005)

        def slow_handler(q):
            time.sleep(1.5)
            return {"status": "ok", "data": []}

        svc.broker.handler = slow_handler
        pump_task = asyncio.create_task(pump())
        try:

            def client_roundtrip():
                ctx = zmq.Context()
                sock = ctx.socket(zmq.REQ)
                sock.setsockopt(zmq.LINGER, 100)
                sock.setsockopt(zmq.RCVTIMEO, 3000)
                sock.connect(f"tcp://127.0.0.1:{settings.query.port}")
                sock.send(b'{"metric":"cpu"}')
                resp = sock.recv()
                sock.close(linger=0)
                ctx.term()
                return resp

            started = time.monotonic()
            resp = await asyncio.to_thread(client_roundtrip)
            payload = json.loads(resp)
            elapsed = time.monotonic() - started

            assert payload["error"]["code"] == "QUERY_TIMEOUT"
            assert elapsed < 1.0  # ~0.2s budget, not the 1.5s handler
        finally:
            pump_task.cancel()
            await asyncio.gather(pump_task, return_exceptions=True)
            await svc.shutdown()


# ---------------------------------------------------------------------------
# Backup manager
# ---------------------------------------------------------------------------


@pytest.fixture
def backup_engine(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "db.sqlite"))
    initialize_schema(eng)
    store = StorageEngine(eng)
    store.insert_many([("cpu.usage", None, 0.7, time.time_ns())])
    yield eng, tmp_path
    eng.dispose()


class TestBackupManager:
    async def test_run_once_creates_snapshot(self, backup_engine):
        eng, tmp_path = backup_engine
        mgr = BackupManager(eng, interval=3600.0, backup_dir=str(tmp_path / "bk"))
        await mgr.run_once()
        assert mgr.backups_created == 1
        assert mgr.runs == 1
        assert mgr.last_backup is not None

        path = Path(mgr.last_backup)
        assert path.exists()
        # valid sqlite file containing the data
        con = sqlite3.connect(path)
        n = con.execute("SELECT count(*) FROM series").fetchone()[0]
        con.close()
        assert n == 1

    async def test_skips_fresh_snapshot(self, backup_engine):
        """A snapshot younger than the interval is not re-created."""
        eng, tmp_path = backup_engine
        backup_database(eng, str(tmp_path / "bk"))  # fresh file
        mgr = BackupManager(eng, interval=3600.0, backup_dir=str(tmp_path / "bk"))
        await mgr.run_once()
        assert mgr.backups_created == 0  # nothing new
        assert mgr.runs == 1  # pass still counted
        # still exactly one backup file
        files = list((tmp_path / "bk").glob("sqtseries-*.db"))
        assert len(files) == 1

    async def test_start_stop_lifecycle(self, backup_engine):
        eng, tmp_path = backup_engine
        mgr = BackupManager(eng, interval=0.05, backup_dir=str(tmp_path / "bk2"))
        await mgr.start()
        await asyncio.sleep(0.15)  # several passes
        assert mgr.runs >= 2
        assert mgr.backups_created >= 1
        await mgr.stop()  # must not raise
        assert mgr._task is None

    def test_is_fresh_helper(self, backup_engine):
        from sqtseries.engine.backup import _is_fresh

        eng, tmp_path = backup_engine
        path = backup_database(eng, str(tmp_path / "bk3"))
        assert _is_fresh(path, 3600.0) is True
        assert _is_fresh(path, 0.0000001) is False
        assert _is_fresh("/nonexistent/backup.db", 3600.0) is False


class TestBackupConfig:
    def test_backup_interval_validated(self):
        ok = Settings(backup={"enabled": True, "interval": "24h"})
        assert validate_settings(ok) == []

        bad = Settings(backup={"enabled": True, "interval": "nope"})
        errs = validate_settings(bad)
        assert any("backup.interval" in e for e in errs)

    def test_disabled_backup_not_validated(self):
        s = Settings(backup={"enabled": False, "interval": "junk"})
        assert validate_settings(s) == []


class TestBackupServiceIntegration:
    async def test_service_creates_backups_on_schedule(self, tmp_path):
        """A running service with backup.enabled makes snapshots and reports
        them in admin stats."""
        from sqtseries.client import Client
        from sqtseries.service import Service

        ports = {
            "ingest": free_tcp_port(),
            "query": free_tcp_port(),
            "streaming": free_tcp_port(),
            "admin": free_tcp_port(),
            "http": free_tcp_port(),
            "stats": free_tcp_port(),
        }
        settings = Settings(
            database={"path": str(tmp_path / "svc.sqlite")},
            ingestion={"port": ports["ingest"]},
            query={"port": ports["query"]},
            streaming={"port": ports["streaming"]},
            admin={"port": ports["admin"]},
            http={"port": ports["http"]},
            stats={"port": ports["stats"]},
            backup={"enabled": True, "interval": "1s", "path": str(tmp_path / "bk")},
            ports={"auto_detect": False},
        )
        svc = Service(settings)
        await svc.start()
        await svc.pool.stop()

        async def pump():
            while True:
                await svc.ingress.drain()
                await svc.broker.run_once(block=False)
                await svc.admin_broker.run_once(block=False)
                await asyncio.sleep(0.005)

        pump_task = asyncio.create_task(pump())
        try:
            await asyncio.sleep(2.0)  # let a pass happen (interval=1s)
            assert svc.backup_manager is not None
            assert svc.backup_manager.backups_created >= 1
            files = list((tmp_path / "bk").glob("sqtseries-*.db"))
            assert len(files) >= 1

            client = Client(
                ports={
                    "write": ports["ingest"],
                    "query": ports["query"],
                    "subscribe": ports["streaming"],
                    "admin": ports["admin"],
                }
            )
            try:
                st = await asyncio.to_thread(client.admin, "stats")
            finally:
                client.close()
            assert st["backup_runs"] >= 1
            assert st["backups_created"] >= 1
            assert st["last_backup"].endswith(".db")
        finally:
            pump_task.cancel()
            await asyncio.gather(pump_task, return_exceptions=True)
            await svc.shutdown()
        # no file left behind mid-write
        assert (tmp_path / "bk").is_dir()

    async def test_disabled_backup_no_manager(self, tmp_path):
        from sqtseries.service import Service

        settings = Settings(
            database={"path": str(tmp_path / "x.sqlite")},
            ingestion={"port": free_tcp_port()},
            query={"port": free_tcp_port()},
            streaming={"port": free_tcp_port()},
            admin={"port": free_tcp_port()},
            http={"port": free_tcp_port()},
            stats={"port": free_tcp_port()},
            backup={"enabled": False},
            ports={"auto_detect": False},
        )
        svc = Service(settings)
        await svc.start()
        try:
            assert svc.backup_manager is None
        finally:
            await svc.shutdown()


class TestZeroTimeoutDisables:
    async def test_zero_timeout_runs_handler(self, context):
        """timeout_s=0 disables the budget (handler runs to completion)."""
        from sqtseries.config import QuerySettings

        port = free_tcp_port()
        broker = QueryBroker(
            f"tcp://127.0.0.1:{port}",
            QuerySettings(),
            handler=lambda q: {"status": "ok", "data": ["done"]},
            context=context,
            handler_timeout_s=0.0,
        )
        await broker.start()
        try:
            client = context.socket(zmq.REQ)
            client.connect(f"tcp://127.0.0.1:{port}")
            client.send(b'{"metric":"cpu"}')
            resp = await _run_until_reply(broker, client)
            client.close(linger=0)
            assert resp is not None
            assert json.loads(resp)["status"] == "ok"
        finally:
            await broker.stop()


class TestAdminKwargs:
    async def test_admin_conncheck_ids(self, tmp_path, free_ports):
        """Client.admin passes extra kwargs into the request (conncheck)."""
        from sqtseries.client import Client
        from sqtseries.service import Service

        settings = Settings(
            database={"path": str(tmp_path / "kw.sqlite")},
            ingestion={"port": free_ports["ingest"]},
            query={"port": free_ports["query"]},
            streaming={"port": free_ports["streaming"]},
            admin={"port": free_ports["admin"]},
            http={"port": free_ports["http"]},
            stats={"port": free_ports["stats"]},
            ports={"auto_detect": False},
        )
        svc = Service(settings)
        await svc.start()
        await svc.pool.stop()

        async def pump():
            while True:
                await svc.ingress.drain()
                await svc.broker.run_once(block=False)
                await svc.admin_broker.run_once(block=False)
                await asyncio.sleep(0.005)

        pump_task = asyncio.create_task(pump())
        c = Client(
            ports={
                "write": free_ports["ingest"],
                "query": free_ports["query"],
                "subscribe": free_ports["streaming"],
                "admin": free_ports["admin"],
            }
        )
        try:
            reply = await asyncio.to_thread(c.admin, "conncheck", ids=["nope"])
            assert reply["status"] == "ok"
            assert reply["present"] == []
        finally:
            c.close()
            pump_task.cancel()
            await asyncio.gather(pump_task, return_exceptions=True)
            await svc.shutdown()

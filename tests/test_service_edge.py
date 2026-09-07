"""Service edge cases: partial-start cleanup, handler NOT_READY, admin errors."""

import asyncio
import sqlite3

import pytest

from sqtseries.config import Settings
from sqtseries.service import Service


@pytest.fixture
def settings(tmp_path, free_ports):
    return Settings(
        database={"path": str(tmp_path / "svc.sqlite"), "batch_size": 500},
        ingestion={"port": free_ports["ingest"]},
        query={"port": free_ports["query"]},
        streaming={"port": free_ports["streaming"]},
        admin={"port": free_ports["admin"]},
        http={"port": free_ports["http"]},
        stats={"port": free_ports["stats"]},
        ports={"auto_detect": False},
    )


class TestPartialStartup:
    async def test_start_failure_cleans_up_and_raises(self, settings, monkeypatch):
        import sqtseries.service as svc_mod

        def failing_create(path, db_settings):
            raise RuntimeError("disk full")

        monkeypatch.setattr(svc_mod, "create_sqlite_engine", failing_create)

        svc = Service(settings)
        with pytest.raises(RuntimeError, match="disk full"):
            await svc.start()
        # shutdown ran during cleanup; second shutdown must be safe
        await svc.shutdown()
        assert not svc.runtime.exists


class TestSinkErrors:
    async def test_sink_insert_failure_swallowed(self, settings):
        svc = Service(settings)
        await svc.start()
        try:
            orig = svc.store.insert_many

            def boom(*a, **k):
                raise sqlite3.OperationalError("locked")

            svc.store.insert_many = boom
            # must not raise
            svc._sink([("cpu", None, 1.0, 1_700_000_000_000_000_000)])
            svc.store.insert_many = orig
        finally:
            await svc.shutdown()

    async def test_publish_failure_swallowed(self, settings, monkeypatch):
        svc = Service(settings)
        await svc.start()
        try:

            async def boom(topic, payload):
                raise RuntimeError("pub broke")

            monkeypatch.setattr(svc.pubsub, "publish", boom)
            svc._on_publish([(b"cpu", {"value": 1.0})])
            # let the publish task run + fail
            await asyncio.sleep(0.1)
        finally:
            await svc.shutdown()


class TestNotReady:
    def test_query_handler_not_ready(self, settings):
        # never started: ts is None
        svc = Service(settings)
        reply = svc._query_handler({"metric": "cpu"})
        assert reply["status"] == "error"
        assert reply["error"]["code"] == "NOT_READY"

    def test_admin_handler_not_ready(self, settings):
        svc = Service(settings)
        reply = svc._admin_handler({"cmd": "health"})
        assert reply["status"] == "error"
        assert reply["error"]["code"] == "NOT_READY"


class TestQueryLimitValidation:
    def test_negative_limit_invalid_query(self, settings, tmp_path):
        from sqtseries.engine import (
            StorageEngine,
            create_sqlite_engine,
            initialize_schema,
        )
        from sqtseries.query import TimeSeriesDB

        eng = create_sqlite_engine(str(tmp_path / "q.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)

        svc = Service(settings)
        svc.store = store
        svc.ts = TimeSeriesDB(store)
        try:
            # a negative limit would silently drop the last row via rows[:-1]

            reply = svc._query_handler({"metric": "m", "limit": -5})
            assert reply["status"] == "error"
            assert reply["error"]["code"] == "INVALID_QUERY"
        finally:
            store.close()


class TestAdminErrors:
    async def test_admin_backup_failure(self, settings, monkeypatch):
        svc = Service(settings)
        await svc.start()
        try:
            import sqtseries.service as svc_mod

            def boom(db, backup_dir, prefix="sqtseries"):
                raise PermissionError("read-only")

            monkeypatch.setattr(svc_mod, "backup_database", boom)
            reply = svc._admin_handler({"cmd": "backup"})
            assert reply["status"] == "error"
            assert reply["error"]["code"] == "BACKUP_FAILED"
        finally:
            await svc.shutdown()

    async def test_admin_vacuum_busy(self, settings):
        svc = Service(settings)
        await svc.start()
        try:
            # hold a read lock, then VACUUM cannot get exclusive access

            with svc.engine.connect() as conn:
                conn.exec_driver_sql("SELECT COUNT(*) FROM series")
                reply = svc._admin_handler({"cmd": "vacuum"})
                assert reply["status"] in ("ok", "error")
                if reply["status"] == "error":
                    assert reply["error"]["code"] == "VACUUM_BUSY"
        finally:
            await svc.shutdown()

    async def test_unknown_admin_command(self, settings):
        svc = Service(settings)
        await svc.start()
        try:
            import pytest as _p

            from sqtseries.messaging import ProtocolError

            with _p.raises(ProtocolError):
                svc._admin_handler({"cmd": "frobnicate"})
        finally:
            await svc.shutdown()

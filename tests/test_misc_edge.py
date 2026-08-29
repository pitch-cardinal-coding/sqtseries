"""Final misc edge cases: module entry, checkpoint failure, client branches."""

import asyncio
import runpy

import pytest

from sqtseries.client import Client


@pytest.fixture
async def running_service(tmp_path, free_ports):
    from sqtseries.config import Settings
    from sqtseries.service import Service

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


def test_python_m_sqtseries_version(monkeypatch):
    import sys

    monkeypatch.setattr(sys, "argv", ["sqtseries", "--version"])
    with pytest.raises(SystemExit):
        runpy.run_module("sqtseries.__main__", run_name="__main__")


class TestCheckpointFailure:
    async def test_checkpoint_failure_does_not_crash(self, tmp_path, monkeypatch):
        from pathlib import Path

        import sqtseries.engine.checkpoint as chk
        from sqtseries.engine import create_sqlite_engine, initialize_schema
        from sqtseries.engine.checkpoint import CheckpointManager

        eng = create_sqlite_engine(str(tmp_path / "cp.sqlite"))
        initialize_schema(eng)
        Path(eng.path + "-wal").write_bytes(b"\x00" * 64)

        def boom(db, mode):
            raise RuntimeError("checkpoint failed")

        monkeypatch.setattr(chk, "wal_checkpoint", boom)
        mgr = CheckpointManager(eng, interval=60.0, max_wal_bytes=0)
        # must not raise
        await mgr._checkpoint_if_needed()
        assert mgr.last_busy == 1
        assert mgr.busy_runs == 0
        eng.dispose()


class TestClientBranches:
    async def test_write_with_timestamp(self, client):
        import time

        await asyncio.to_thread(client.write, "cpu", 1.0, timestamp=time.time())

        await asyncio.sleep(0.3)
        rows = await asyncio.to_thread(client.query, "cpu")
        assert len(rows) == 1

    async def test_query_with_limit_and_order(self, client):
        for i in range(5):
            await asyncio.to_thread(client.write, "cpu", float(i))
        await asyncio.sleep(0.3)
        rows = await asyncio.to_thread(client.query, "cpu", limit=2, order="desc")

        assert len(rows) == 2


class TestStoreSeriesMeta:
    def test_get_series_meta_missing(self, tmp_path):
        from sqtseries.engine import (
            SeriesNotFoundError,
            StorageEngine,
            create_sqlite_engine,
            initialize_schema,
        )

        eng = create_sqlite_engine(str(tmp_path / "s.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)

        with pytest.raises(SeriesNotFoundError):
            store.get_series_meta(12345)
        store.close()

"""Async lifecycle edge cases: cancellation, idempotent stop, drain on shutdown."""

import asyncio

import pytest

from sqtseries.config import Settings
from sqtseries.engine import create_sqlite_engine, initialize_schema
from sqtseries.engine.maintenance import MaintenanceManager
from sqtseries.partition import RetentionManager, RollupManager


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


class TestWorkerPool:
    async def test_stop_cancels_running_tasks(self):
        from sqtseries.messaging import WorkerPool

        started = asyncio.Event()
        stopped = asyncio.Event()

        async def step():
            started.set()
            try:
                while True:
                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                stopped.set()
                raise

        pool = WorkerPool(step, size=2)
        await pool.start()
        await asyncio.wait_for(started.wait(), timeout=2)
        await pool.stop()
        assert stopped.is_set()
        assert pool.running is False
        assert pool._tasks == []

    async def test_step_after_stop_no_work(self):
        from sqtseries.messaging import WorkerPool

        calls = []

        async def step():
            calls.append(1)
            await asyncio.sleep(0)

        pool = WorkerPool(step, size=1)
        await pool.start()
        await asyncio.sleep(0.05)
        await pool.stop()
        n = len(calls)
        await asyncio.sleep(0.05)
        # no more work after stop
        assert len(calls) == n


class TestManagers:
    async def test_managers_stop_idempotent(self, tmp_path):
        eng = create_sqlite_engine(str(tmp_path / "m.sqlite"))
        initialize_schema(eng)
        rollup = RollupManager(eng, interval=60.0)
        maintenance = MaintenanceManager(eng, interval=60.0)
        retention = RetentionManager(eng, object(), ttl="400d", interval=60.0)
        for mgr in (rollup, maintenance, retention):
            await mgr.start()
            await mgr.stop()
            # idempotent
            await mgr.stop()
        eng.dispose()


class TestServiceCleanup:
    async def test_shutdown_drains_publish_tasks(self, settings):
        from sqtseries.service import Service

        svc = Service(settings)
        await svc.start()

        # inject a long-running publish task
        async def slow(topic, payload):
            await asyncio.sleep(0.2)

        svc.pubsub.publish = slow  # type: ignore[method-assign]
        svc._on_publish(b"cpu", {"value": 1.0})
        await asyncio.sleep(0.05)
        assert svc._publish_tasks
        # must await + clear the task
        await svc.shutdown()
        assert not svc._publish_tasks

    async def test_shutdown_twice_safe(self, settings):
        from sqtseries.service import Service

        svc = Service(settings)
        await svc.start()
        await svc.shutdown()
        # second shutdown must not raise
        await svc.shutdown()

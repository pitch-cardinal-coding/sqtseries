"""Async lifecycle edge cases: cancellation, idempotent stop, drain on shutdown."""

import asyncio
import time

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


class TestCleanupNoLingeringTasks:
    async def _pending_task_names(self):
        return {
            t.get_name()
            for t in asyncio.all_tasks(asyncio.get_running_loop())
            if not t.done()
        }

    async def test_pubsub_stop_awaits_reader(self):
        from conftest import free_port

        from sqtseries.messaging.connection_registry import ConnectionRegistry
        from sqtseries.messaging.pubsub import PubSub

        ps = PubSub(f"tcp://127.0.0.1:{free_port()}", registry=ConnectionRegistry())

        await ps.start()
        assert "pubsub-xpub-reader" in await self._pending_task_names()

        await ps.stop()
        # the reader task must be gone, not merely scheduled for cancellation

        assert "pubsub-xpub-reader" not in await self._pending_task_names()

    async def test_stats_publisher_stop_awaits_and_drains(self):
        from conftest import free_port

        from sqtseries.messaging.connection_registry import ConnectionRegistry
        from sqtseries.messaging.stats_publisher import StatsPublisher

        reg = ConnectionRegistry()
        pub = StatsPublisher(f"tcp://127.0.0.1:{free_port()}", registry=reg)

        await pub.start()
        assert "stats-publisher" in await self._pending_task_names()

        # a slow in-flight event-forward task (socket send is fast, so make
        # publish hang so the task is still pending when stop() runs)
        async def slow_publish(event_type, payload):
            await asyncio.sleep(0.2)

        pub.publish = slow_publish  # type: ignore[method-assign]
        pub._on_registry_event("conn", {"id": "x"})
        await asyncio.sleep(0.01)
        assert len(pub._publish_tasks) >= 1
        await pub.stop()
        assert "stats-publisher" not in await self._pending_task_names()

        assert pub._publish_tasks == set()
        assert reg._listeners == []

    async def test_subscription_lingering_bounded_without_publishes(self):
        """_linger_until must not grow forever when no data is ever published."""

        from conftest import free_port

        from sqtseries.messaging.pubsub import PubSub

        ps = PubSub(f"tcp://127.0.0.1:{free_port()}", linger_seconds=1.0)

        await ps.start()
        tracker = ps.tracker
        try:
            # churn unique topics through the tracker (as a real SUB client would)

            for i in range(5000):
                topic = f"churn.{i:06d}"
                tracker.subscribe(topic)
                tracker.unsubscribe(topic)
            assert len(tracker._linger_until) == 5000
            # idle reader loop sweeps expired entries without any publish()

            await asyncio.sleep(2.0)
            assert len(tracker._linger_until) == 0
        finally:
            await ps.stop()


class TestServiceCleanup:
    async def test_shutdown_drains_publish_tasks(self, settings):
        from sqtseries.service import Service

        svc = Service(settings)
        await svc.start()

        # inject a long-running publish task
        async def slow(topic, payload):
            await asyncio.sleep(0.2)

        svc.pubsub.publish = slow  # type: ignore[method-assign]
        svc._on_publish([(b"cpu", {"value": 1.0})])
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

    async def test_restart_no_registry_listener_growth(self, settings):
        """Repeated start/shutdown must not accumulate registry listeners.

        Regression: StatsPublisher registered a new listener on every start()

        and never unhooked it on stop(), so N cycles left N dead listeners

        (keeping N dead publisher objects alive) on the shared registry.

        """
        from sqtseries.service import Service

        svc = Service(settings)
        for _ in range(3):
            await svc.start()
            assert len(svc.connection_registry._listeners) == 1
            await svc.shutdown()
            assert len(svc.connection_registry._listeners) == 0

    async def test_dashboard_streams_close_cleanly(self, settings):
        """Repeated /ws/dashboard cycles must not leak listeners or tasks.

        Each cycle registers a registry listener plus a 1s tick task; both
        must be gone after disconnect, and the registry must show no
        lingering connections.
        """
        import websockets

        from sqtseries.service import Service

        svc = Service(settings)
        await svc.start()
        try:
            port = settings.http.port
            for _ in range(10):
                async with websockets.connect(
                    f"ws://127.0.0.1:{port}/ws/dashboard"
                ) as ws:
                    raw = await ws.recv()
                    assert '"snapshot"' in raw
            # Disconnect cleanup is async server-side: the last close may
            # still be in flight when the loop ends. Settle first.
            deadline = time.monotonic() + 5.0
            while (
                len(svc.connection_registry._listeners) != 1
                and time.monotonic() < deadline
            ):
                await asyncio.sleep(0.05)
            assert svc.connection_registry._listeners != []
            assert len(svc.connection_registry._listeners) == 1
            assert svc.connection_registry.list_connections() == []
        finally:
            await svc.shutdown()

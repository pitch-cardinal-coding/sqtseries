"""Service lifecycle end-to-end tests."""

import asyncio

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
        rollup={"interval": "1m"},
    )


class TestService:
    async def test_start_shutdown(self, settings):
        svc = Service(settings)
        await svc.start()
        assert svc.store is not None
        # runtime file exists
        assert svc.runtime.exists
        # insert data via sink
        import time

        svc._sink([("cpu.usage", None, 0.5, time.time_ns())])
        await svc.shutdown()
        assert not svc.runtime.exists

    async def test_invalid_config_raises(self):
        # duplicate with http port
        s = Settings(ingestion={"port": 12505})
        from sqtseries.service import Service, ServiceError

        with pytest.raises(ServiceError):
            Service(s)

    async def test_query_handler(self, settings):
        svc = Service(settings)
        await svc.start()
        svc._sink([("temp", None, 21.5, 1_700_000_000_000_000_000)])
        result = svc._query_handler({"metric": "temp"})
        assert result["status"] == "ok"
        await svc.shutdown()

    async def test_health(self, settings):
        svc = Service(settings)
        await svc.start()
        health = svc.health()
        assert health["status"] == "ok"
        await svc.shutdown()

    async def test_starts_with_configured_logging(self, settings):
        """Regression: `sqtseries run` configures logging before starting the

        service. The service used plain stdlib loggers with structlog-style

        kwargs, which crashed with TypeError once INFO level was enabled."""

        from sqtseries.config import LoggingSettings
        from sqtseries.logging import configure_logging

        configure_logging(LoggingSettings(level="INFO", format="console"))

        svc = Service(settings)
        await svc.start()
        assert svc.store is not None
        await svc.shutdown()

    async def test_http_gateway_is_served(self, settings):
        """The service must serve the REST + WebSocket gateway on http.port."""

        import httpx

        svc = Service(settings)
        await svc.start()
        try:
            assert svc.http_server is not None
            assert svc.http_server.started
            base = f"http://127.0.0.1:{settings.http.port}"
            async with httpx.AsyncClient(base_url=base) as c:
                r = await c.get("/api/v1/health")
                assert r.status_code == 200
                assert r.json()["status"] == "ok"
                r = await c.post(
                    "/api/v1/write",
                    json={"metric": "cpu", "value": 0.5, "tags": {"host": "a"}},
                )
                assert r.json()["written"] == 1
                await asyncio.sleep(0.3)
                r = await c.get("/api/v1/read", params={"metric": "cpu"})

                assert r.status_code == 200
                assert len(r.json()["data"]) == 1
        finally:
            await svc.shutdown()

    async def test_rollup_manager_wired(self, settings):
        svc = Service(settings)
        await svc.start()
        assert svc.rollup_manager is not None
        assert svc.rollup_manager.interval == 60.0
        assert svc.maintenance_manager is not None
        assert svc.retention_manager is not None
        assert svc.retention_manager.interval == 3600.0
        await svc.shutdown()

    async def test_rollup_serves_aggregates(self, settings):
        """Historical data in a completed hour is answered from the rollup

        fast path after a rollup pass, matching the raw result."""
        from datetime import UTC, datetime

        from sqtseries.partition import rollup_partition

        def ns(dt):
            return int(dt.timestamp() * 1_000_000_000)

        base = ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))

        hour = 3_600_000_000_000

        svc = Service(settings)
        await svc.start()
        try:
            svc._sink(
                [("cpu", None, float(i), base + i * 600_000_000_000) for i in range(6)]
            )
            rollup_partition(svc.engine, "measurements_2026_01", replace=True)

            result = svc.ts.query(
                "cpu",
                start=base,
                end=base + hour,
                aggregation="sum",
            )
            # 0+1+2+3+4+5
            assert result[0][1] == pytest.approx(15.0)

            buckets = svc.ts.query("cpu", aggregation="sum", interval="1h")

            assert buckets[0][1] == pytest.approx(15.0)
        finally:
            await svc.shutdown()

    async def test_sink_accounting_persisted_and_dropped(self, settings, monkeypatch):
        """Accounting identity: recv == persisted + dropped + invalid.

        Successful batches tally into _persisted_count; a failed insert_many
        (SQLite rolls the whole transaction back atomically) lands wholly in
        _dropped_count and is visible in _admin_stats instead of vanishing.
        """
        import time

        svc = Service(settings)
        await svc.start()
        try:
            base = time.time_ns()
            svc._sink([("acct.ok", None, float(i), base + i) for i in range(7)])
            assert svc._persisted_count == 7
            assert svc._dropped_count == 0

            # Force a sink failure exactly where it happens in production:
            # inside insert_many's transaction (disk full / lock timeout).
            def failing_insert_many(rows):
                raise RuntimeError("simulated commit failure")

            monkeypatch.setattr(svc.store, "insert_many", failing_insert_many)
            svc._sink([("acct.fail", None, 1.0, base)])
            assert svc._dropped_count == 1
            assert svc._persisted_count == 7

            # The failure must be visible in the admin stats payload.
            stats = svc._admin_stats()
            assert stats["persisted"] >= 7
            assert stats["dropped"] == 1
        finally:
            await svc.shutdown()

    async def test_shutdown_drains_queued_ingest_frames(self, settings):
        """Frames still sitting in the PULL socket at shutdown must be
        persisted, not dropped by close (libzmq discards undelivered
        messages on close — the ffmpeg-zmq drain-before-finalize lesson).
        Whatever the worker already drained plus what the shutdown sweeps
        drain must account for every frame sent."""
        import time

        import zmq

        svc = Service(settings)
        await svc.start()
        sent = 50
        try:
            ctx = zmq.Context()
            push = ctx.socket(zmq.PUSH)
            push.setsockopt(zmq.LINGER, 1000)
            push.connect(f"tcp://127.0.0.1:{settings.ingestion.port}")
            # The ZMTP handshake is asynchronous: sending (or closing with
            # LINGER 0) before it completes silently discards the frames.
            # Wait for the connection, send, then let the worker fall idle
            # so the frames are still queued when shutdown drains them.
            await asyncio.sleep(0.2)
            base = time.time()
            for i in range(sent):
                push.send_json(
                    {
                        "metric": "drain.test",
                        "value": float(i),
                        "timestamp": base + i * 1e-6,
                    }
                )
            push.close(1000)
            ctx.term()
        finally:
            await svc.shutdown()
        # Every frame must have landed: worker-drained or shutdown-drained.
        assert svc._persisted_count == sent
        assert svc._dropped_count == 0

    async def test_sink_dropped_on_engine_failure(self, settings, monkeypatch):
        """A DB-level failure (the realistic shape: sqlite3.OperationalError
        from disk-full or lock timeout) must count the batch as dropped
        (visible in stats), not lose it silently."""
        import sqlite3
        import time

        svc = Service(settings)
        await svc.start()
        try:

            def failing_begin(*args, **kwargs):
                raise sqlite3.OperationalError("database or disk is full")

            monkeypatch.setattr(svc.store.db, "begin", failing_begin)
            base = time.time_ns()
            svc._sink([("gone.metric", None, 1.0, base)])
            assert svc._dropped_count == 1
            assert svc._persisted_count == 0
            stats = svc._admin_stats()
            assert stats["dropped"] == 1
        finally:
            await svc.shutdown()


class TestDashboardTopics:
    async def test_snapshot_lists_known_topics_with_totals(self, settings):
        """dashboard_snapshot 'topics' covers idle topics (0) with totals."""
        svc = Service(settings)
        await svc.start()
        try:
            reg = svc.connection_registry
            reg.register_zmq_sub("cpu")
            reg.register_zmq_sub("mem")
            reg.unregister_zmq_sub("mem")
            await svc.pubsub.publish("cpu", {"metric": "cpu", "value": 1.0})
            await svc.pubsub.publish("cpu", {"metric": "cpu", "value": 2.0})
            snap = svc.dashboard_snapshot()
            by_topic = {t["topic"]: t for t in snap["topics"]}
            assert by_topic["cpu"]["subscribers"] == 1
            assert by_topic["cpu"]["total"] == 2
            assert by_topic["mem"]["subscribers"] == 0
            assert by_topic["mem"]["total"] == 0
        finally:
            await svc.shutdown()

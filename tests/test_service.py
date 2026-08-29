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

        svc._sink("cpu.usage", None, 0.5, time.time_ns())
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
        svc._sink("temp", None, 21.5, 1_700_000_000_000_000_000)
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
            for i in range(6):
                svc._sink("cpu", None, float(i), base + i * 600_000_000_000)
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

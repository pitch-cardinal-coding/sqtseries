"""Retention policy tests."""

from datetime import UTC, datetime, timedelta

import pytest

from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema
from sqtseries.partition import PartitionManager, RetentionPolicy, parse_ttl


def _ns(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000_000)


@pytest.fixture
def engine(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "ret.sqlite"))
    initialize_schema(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def filled(engine):
    """Create partitions across several months and insert data."""
    pm = PartitionManager(engine)
    store = StorageEngine(engine)
    months = [
        (2025, 11),
        (2025, 12),
        (2026, 1),
        (2026, 2),
    ]
    for y, m in months:
        pm.ensure_partition(y, m)
        dt = datetime(y, m, 15, tzinfo=UTC)
        store.insert_many([("cpu", {"h": "a"}, 1.0, _ns(dt))])
    return engine


class TestParseTtl:
    def test_parses(self):
        assert parse_ttl("30d") == timedelta(days=30)
        assert parse_ttl("12h") == timedelta(hours=12)
        assert parse_ttl("4w") == timedelta(weeks=4)

    def test_invalid(self):
        with pytest.raises(ValueError):
            parse_ttl("bogus")


class TestRetention:
    def test_retains_recent_only(self, filled):
        # now = 2026-02-20, TTL 35d → horizon = 2026-01-16
        #   keep: Jan 31, Feb 28 (and future current-month partition)
        #   drop: Nov 30, Dec 31 (both < Jan 16)
        now = datetime(2026, 2, 20, tzinfo=UTC)
        rp = RetentionPolicy(filled, ttl="35d")
        keep = rp.partitions_to_retain(now)
        assert "measurements_2026_02" in keep
        assert "measurements_2026_01" in keep
        assert "measurements_2025_12" not in keep
        assert "measurements_2025_11" not in keep

    def test_run_drops(self, filled):
        now = datetime(2026, 2, 20, tzinfo=UTC)
        rp = RetentionPolicy(filled, ttl="35d")
        dropped = rp.run(now)
        assert "measurements_2025_12" in dropped
        assert "measurements_2025_11" in dropped
        pm = PartitionManager(filled)
        assert "measurements_2025_12" not in pm.list_partitions()
        assert "measurements_2026_02" in pm.list_partitions()

    def test_no_drop_when_within_ttl(self, filled):
        now = datetime(2026, 2, 20, tzinfo=UTC)
        rp = RetentionPolicy(filled, ttl="400d")
        assert rp.run(now) == []

    def test_drop_removes_rollup_rows(self, engine):
        """Dropping a partition also removes its hours from rollup_hourly."""
        from sqtseries.partition import ensure_rollup_table, rollup_partition

        pm = PartitionManager(engine)
        store = StorageEngine(engine)
        for y, m in [(2025, 11), (2026, 1)]:
            pm.ensure_partition(y, m)
            dt = datetime(y, m, 15, tzinfo=UTC)
            store.insert_many([("cpu", None, 1.0, _ns(dt))])
        ensure_rollup_table(engine)
        rollup_partition(engine, "measurements_2025_11")
        rollup_partition(engine, "measurements_2026_01")

        rp = RetentionPolicy(engine, ttl="35d")
        dropped = rp.run(datetime(2026, 2, 20, tzinfo=UTC))
        assert "measurements_2025_11" in dropped
        assert "measurements_2026_01" not in dropped

        import sqlite3

        con = sqlite3.connect(engine.path)
        try:
            rows = con.execute(
                "SELECT DISTINCT hour_start_ns FROM rollup_hourly ORDER BY 1"
            ).fetchall()
        finally:
            con.close()
        # 2026_01 rolled hours survive
        assert rows
        # no rolled hour falls inside 2025-11
        nov_start = int(datetime(2025, 11, 1, tzinfo=UTC).timestamp() * 1e9)
        dec_start = int(datetime(2025, 12, 1, tzinfo=UTC).timestamp() * 1e9)
        assert all(r[0] < nov_start or r[0] >= dec_start for r in rows)

    def test_drop_rollup_when_no_rollup_table(self, filled):
        """Dropping with no rollup table present must not error."""
        rp = RetentionPolicy(filled, ttl="35d")
        assert rp.run(datetime(2026, 2, 20, tzinfo=UTC))


class TestRetentionManager:
    async def test_manager_drops_and_invalidates(self, engine):
        from sqtseries.partition import RetentionManager

        store = StorageEngine(engine)
        pm = PartitionManager(engine)
        for y, m in [(2025, 11), (2026, 2)]:
            pm.ensure_partition(y, m)
            dt = datetime(y, m, 15, tzinfo=UTC)
            store.insert_many([("cpu", None, 1.0, _ns(dt))])
        store.invalidate_partitions()
        assert len(store._parts_cache if store._parts_cache else []) in (0, 2)

        # Real "now" is far past 2026-02, so use a TTL that retains it:
        # horizon = now - 200d lands in Jan 2026 -> keep 2026_02, drop 2025_11.
        mgr = RetentionManager(engine, store, ttl="200d", interval=3600.0)
        assert mgr.ttl == "200d"
        await mgr.run_once()
        pm = PartitionManager(engine)
        names = pm.list_partitions()
        assert "measurements_2025_11" not in names
        assert "measurements_2026_02" in names
        assert mgr.runs == 1
        assert mgr.dropped_total == 1

    async def test_manager_start_stop(self, engine):
        import asyncio

        from sqtseries.partition import RetentionManager

        store = StorageEngine(engine)
        mgr = RetentionManager(engine, store, ttl="400d", interval=0.05)
        await mgr.start()
        await asyncio.sleep(0.12)
        await mgr.stop()
        # immediate + after one interval
        assert mgr.runs >= 2

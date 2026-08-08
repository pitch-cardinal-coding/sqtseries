"""Partition manager tests."""

import pytest

from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema
from sqtseries.partition import PartitionManager


@pytest.fixture
def engine(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "part.sqlite"))
    initialize_schema(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def manager(engine):
    return PartitionManager(engine)


class TestPartitionManager:
    def test_list_empty_plus_current(self, manager):
        # initialize_schema creates the current month partition
        parts = manager.list_partitions()
        assert len(parts) >= 1

    def test_ensure_creates(self, manager, engine):
        name = manager.ensure_partition(2030, 2)
        assert name == "measurements_2030_02"
        assert name in manager.list_partitions()
        # idempotent
        assert manager.ensure_partition(2030, 2) == name

    def test_rotation_current_month(self, manager):
        import time

        now = time.time_ns()
        name = manager.current_partition(now)
        from datetime import UTC, datetime

        dt_utc = datetime.fromtimestamp(now / 1e9, tz=UTC)
        assert name == f"measurements_{dt_utc.year:04d}_{dt_utc.month:02d}"

    def test_drop_partition(self, manager, engine):
        manager.ensure_partition(2025, 1)
        manager.drop_partition(2025, 1)
        assert "measurements_2025_01" not in manager.list_partitions()

    def test_store_auto_creates_partition(self, engine):
        store = StorageEngine(engine)
        # insert into a future month
        future_ns = int(
            __import__("datetime")
            .datetime(2027, 3, 5, tzinfo=__import__("datetime").timezone.utc)
            .timestamp()
            * 1e9
        )
        store.insert_many([("m", None, 1.0, future_ns)])
        pm = PartitionManager(engine)
        assert "measurements_2027_03" in pm.list_partitions()

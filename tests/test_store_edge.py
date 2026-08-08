"""StorageEngine edge cases: boundaries, odd values, series errors, caches."""

import sqlite3
from datetime import UTC, datetime

import pytest

from sqtseries.engine import (
    SeriesNotFoundError,
    StorageEngine,
    create_sqlite_engine,
    initialize_schema,
)


def _ns(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000_000)


@pytest.fixture
def store(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "s.sqlite"))
    initialize_schema(eng)
    s = StorageEngine(eng)
    yield s
    s.close()


class TestBoundaries:
    def test_month_boundary_split(self, store):
        # Exact integer math: int(timestamp()) is exact for whole seconds.
        boundary = (
            int(datetime(2026, 1, 1, 0, 0, tzinfo=UTC).timestamp()) * 1_000_000_000
        )
        # 2025-12-31 23:59:59.999999999
        dec = boundary - 1
        # 2026-01-01 00:00:00.000000001
        jan = boundary + 1
        store.insert_many([("cpu", None, 1.0, dec), ("cpu", None, 2.0, jan)])
        names = [t for t in store.db.get_table_names() if t.startswith("measurements")]
        assert "measurements_2025_12" in names
        assert "measurements_2026_01" in names

    def test_leap_year_feb29(self, store):
        dt = datetime(2028, 2, 29, 12, 0, tzinfo=UTC)
        store.insert_many([("cpu", None, 1.0, _ns(dt))])
        rows = list(
            store.query_time_range(
                series_ids=store.series_ids_for_metric("cpu"),
                start_ns=_ns(dt) - 1,
                end_ns=_ns(dt) + 1,
            )
        )
        assert len(rows) == 1

    def test_negative_timestamp(self, store):
        store.insert_many([("cpu", None, 1.0, -1000)])
        rows = list(
            store.query_time_range(series_ids=store.series_ids_for_metric("cpu"))
        )
        assert len(rows) == 1


class TestValues:
    def test_nan_value_rejected_at_insert(self, store):
        # SQLite stores NaN as NULL, violating value NOT NULL -> IntegrityError.
        # The service swallows this in _sink (logged); captured as a benign bug.
        import sqlite3

        with pytest.raises(sqlite3.IntegrityError):
            store.insert_many([("cpu", None, float("nan"), 1_700_000_000_000_000_000)])

    def test_inf_value(self, store):
        store.insert_many([("cpu", None, float("inf"), 1_700_000_000_000_000_000)])
        rows = list(
            store.query_time_range(series_ids=store.series_ids_for_metric("cpu"))
        )
        assert rows[0][1] == float("inf")

    def test_empty_insert_returns_zero(self, store):
        assert store.insert_many([]) == 0


class TestSeries:
    def test_resolve_missing_no_create(self, store):
        with store.db.connect() as conn, pytest.raises(SeriesNotFoundError):
            store.resolve_series(conn, "nope", create_if_missing=False)

    def test_get_series_meta_missing(self, store):
        with pytest.raises(SeriesNotFoundError):
            store.get_series_meta(999999)

    def test_series_ids_empty_metric(self, store):
        assert store.series_ids_for_metric("nothing") == []

    def test_list_metrics_unique(self, store):
        store.insert_many(
            [
                ("a", None, 1.0, 1_700_000_000_000_000_000),
                ("a", {"h": "1"}, 1.0, 1_700_000_000_000_000_001),
                ("b", None, 1.0, 1_700_000_000_000_000_002),
            ]
        )
        assert store.list_metrics() == ["a", "b"]


class TestInsertPaths:
    def test_no_auto_create_partition(self, store):
        # inserting into a non-existent partition without auto-create fails
        with pytest.raises(sqlite3.OperationalError):
            store.insert_many(
                [("cpu", None, 1.0, _ns(datetime(2020, 5, 1, tzinfo=UTC)))],
                auto_create_partition=False,
            )

    def test_lru_cache_eviction(self, store):
        small = StorageEngine(store.db, cache_size=3)
        small.insert_many(
            [
                ("m", {"i": str(i)}, 1.0, 1_700_000_000_000_000_000 + i)
                for i in range(10)
            ]
        )
        # resolving the first series again must work (cache re-populates)
        with small.db.connect() as conn:
            sid = small.resolve_series(conn, "m", {"i": "0"})
        assert sid > 0


class TestQueryRange:
    def test_limit(self, store):
        rows = [
            ("cpu", None, float(i), 1_700_000_000_000_000_000 + i * 1_000_000_000)
            for i in range(10)
        ]
        store.insert_many(rows)
        out = list(
            store.query_time_range(
                series_ids=store.series_ids_for_metric("cpu"),
                start_ns=1_700_000_000_000_000_000,
                # base + 9_000_000_999
                end_ns=1_700_000_009_000_000_999,
                limit=3,
                order="asc",
            )
        )
        assert len(out) == 3

    def test_desc_order(self, store):
        rows = [
            ("cpu", None, float(i), 1_700_000_000_000_000_000 + i) for i in range(5)
        ]
        store.insert_many(rows)
        out = list(
            store.query_time_range(
                series_ids=store.series_ids_for_metric("cpu"), order="desc"
            )
        )
        assert out[0][1] == 4.0

    def test_start_gt_end_returns_empty(self, store):
        rows = [("cpu", None, 1.0, 1_700_000_000_000_000_000 + i) for i in range(3)]
        store.insert_many(rows)
        out = list(
            store.query_time_range(
                series_ids=store.series_ids_for_metric("cpu"),
                start_ns=1_700_000_000_000_000_005,
                end_ns=1_700_000_000_000_000_000,
            )
        )
        assert out == []

    def test_partitions_for_range_multi_month(self, store):
        store.insert_many(
            [
                ("cpu", None, 1.0, _ns(datetime(2025, 12, 15, tzinfo=UTC))),
                ("cpu", None, 1.0, _ns(datetime(2026, 1, 15, tzinfo=UTC))),
            ]
        )
        parts = store._partitions_for_range(
            _ns(datetime(2025, 12, 1, tzinfo=UTC)),
            _ns(datetime(2026, 1, 31, tzinfo=UTC)),
        )
        assert "measurements_2025_12" in parts
        assert "measurements_2026_01" in parts

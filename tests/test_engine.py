"""Storage engine tests."""

import time
from datetime import UTC

import pytest

from sqtseries.engine import (
    SeriesNotFoundError,
    StorageEngine,
    create_sqlite_engine,
    initialize_schema,
    quick_check,
    wal_checkpoint,
)


@pytest.fixture
def engine(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "engine.sqlite"))
    initialize_schema(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def store(engine):
    return StorageEngine(engine)


class TestSchema:
    def test_tables_created(self, engine):
        names = set(engine.get_table_names())
        assert "series" in names
        assert any(n.startswith("measurements_") for n in names)

    def test_series_tables_without_rowid(self, engine):
        # measurements partitions must be WITHOUT ROWID
        with engine.connect() as conn:
            row = conn.exec_driver_sql(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name LIKE 'measurements_%'"
            ).first()
            assert "WITHOUT ROWID" in row[0].upper()

    def test_secondary_timestamp_index(self, engine):
        with engine.connect() as conn:
            row = conn.exec_driver_sql(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name LIKE 'ix_measurements%'"
            ).first()
            assert row is not None

    def test_unique_series_index(self, engine):
        with engine.connect() as conn:
            row = conn.exec_driver_sql(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name LIKE 'uq_series%'"
            ).first()
            assert "UNIQUE" in row[0].upper()


class TestSeriesResolution:
    def test_resolve_creates_new(self, store):
        with store.db.connect() as conn:
            sid = store.resolve_series(
                conn, "cpu", {"host": "web1"}, create_if_missing=True
            )
            assert sid >= 1
        # same series resolved again -> same id
        with store.db.connect() as conn:
            sid2 = store.resolve_series(conn, "cpu", {"host": "web1"})
        assert sid2 == sid

    def test_resolve_not_found(self, store):
        with store.db.connect() as conn, pytest.raises(SeriesNotFoundError):
            store.resolve_series(conn, "missing", None, create_if_missing=False)

    def test_resolve_deduplicates(self, store):
        now = time.time_ns()
        store.insert_many([("m", None, 1.0, now)])
        store.insert_many([("m", None, 2.0, now + 1)])
        sids = store.series_ids_for_metric("m")
        # same metric+tags -> one series
        assert len(sids) == 1

    def test_series_meta(self, store):
        now = time.time_ns()
        store.insert_many([("tmp", {"a": "b"}, 1.0, now)])
        sid = store.series_ids_for_metric("tmp")[0]
        metric, tags = store.get_series_meta(sid)
        assert metric == "tmp"
        assert (tags is not None and '"a": "b"' in tags) or '"a":"b"' in tags


class TestInsertion:
    def test_insert_many_returns_count(self, store):
        now = time.time_ns()
        n = store.insert_many([("a", None, 1.0, now), ("b", None, 2.0, now + 1)])
        assert n == 2

    def test_insert_empty(self, store):
        assert store.insert_many([]) == 0

    def test_insert_bulk_partitions(self, store):
        # timestamps across month boundary -> two partitions
        from datetime import datetime

        start = datetime(2026, 1, 29, tzinfo=UTC)
        rows = []
        for i in range(6):
            # +3 days each
            ts = int((start.timestamp() + i * 86400 * 3) * 1e9)
            rows.append(("m", {"i": str(i)}, float(i), ts))
        store.insert_many(rows)
        with store.db.connect() as conn:
            tables = conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE name LIKE 'measurements_%' ORDER BY name"
            ).fetchall()
        names = [t[0] for t in tables]
        assert any(n == "measurements_2026_01" for n in names)
        assert any(n == "measurements_2026_02" for n in names)


class TestQueries:
    def test_range_query(self, store):
        now = time.time_ns()
        store.insert_many(
            [
                ("cpu", None, 0.5, now - 2_000_000_000),
                ("cpu", None, 0.6, now - 1_000_000_000),
                ("cpu", None, 0.7, now),
            ]
        )
        rows = list(
            store.query_time_range(
                metric="cpu", start_ns=now - 3_000_000_000, end_ns=now
            )
        )
        assert len(rows) == 3
        vals = [v for _, v in rows]
        assert vals == sorted(vals)

    def test_range_query_desc(self, store):
        now = time.time_ns()
        store.insert_many(
            [("cpu", None, 0.5, now - 2_000_000_000), ("cpu", None, 0.6, now)]
        )
        rows = list(store.query_time_range(metric="cpu", order="desc"))
        vals = [v for _, v in rows]
        assert vals == sorted(vals, reverse=True)

    def test_range_query_missing_metric(self, store):
        rows = list(store.query_time_range(metric="nope"))
        assert rows == []

    def test_range_query_requires_arg(self, store):
        with pytest.raises(ValueError):
            list(store.query_time_range())


class TestLifecycle:
    def test_quick_check_ok(self, engine):
        assert quick_check(engine) == "ok"

    def test_checkpoint_passive(self, engine):
        result = wal_checkpoint(engine, "PASSIVE")
        assert isinstance(result, str)

    def test_idempotent_schema_init(self, engine):
        # safe to call again
        initialize_schema(engine)
        assert quick_check(engine) == "ok"

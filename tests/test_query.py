"""Query engine tests."""

import time

import pytest

from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema
from sqtseries.query import TimeSeriesDB
from sqtseries.query.agg import (
    aggregate_series,
    downsample,
    gap_fill_linear,
    median,
    p95,
    p99,
)


@pytest.fixture
def tsdb(tmp_path):
    eng = create_sqlite_engine(str(tmp_path / "query.sqlite"))
    initialize_schema(eng)
    store = StorageEngine(eng)
    yield TimeSeriesDB(store)
    store.close()


@pytest.fixture
def populated(tsdb):
    now = time.time_ns()
    base = now - 20 * 1_000_000_000

    for i in range(20):
        tsdb.insert(
            "cpu.usage",
            float(i),
            {"host": "web1"},
            timestamp_ns=base + i * 1_000_000_000,
        )
    return base


class TestRawQuery:
    def test_all_rows(self, tsdb, populated):
        rows = tsdb.query("cpu.usage")
        assert len(rows) == 20

    def test_time_range(self, tsdb, populated):
        rows = tsdb.query(
            "cpu.usage",
            start=populated + 5 * 1_000_000_000,
            end=populated + 9 * 1_000_000_000,
        )
        assert len(rows) == 5
        assert rows[0][1] == 5.0

    def test_no_metric(self, tsdb):
        assert tsdb.query("nope") == []

    def test_order_desc(self, tsdb, populated):
        rows = tsdb.query("cpu.usage", order="desc")
        assert rows[0][1] == 19.0


class TestAggregations:
    def test_aggregate_default_avg(self, tsdb, populated):
        vals = tsdb.aggregate("cpu.usage")
        assert vals["avg"] == pytest.approx(9.5)

    def test_aggregate_multiple(self, tsdb, populated):
        vals = tsdb.aggregate("cpu.usage", funcs=["min", "max", "sum", "count"])

        assert vals["min"] == 0.0
        assert vals["max"] == 19.0
        assert vals["sum"] == 190.0
        assert vals["count"] == 20.0

    def test_whole_window_aggregation(self, tsdb, populated):
        # aggregation without interval collapses to one point
        rows = tsdb.query("cpu.usage", aggregation="max")
        assert len(rows) == 1
        assert rows[0][1] == 19.0


class TestDownsample:
    def test_interval_buckets(self, tsdb, populated):
        rows = tsdb.downsample("cpu.usage", interval="5s")
        # 20 samples at 1s spacing; buckets are epoch-aligned, so count is
        # ceil(span/5s) ± 1 depending on alignment — never more than samples

        assert 1 <= len(rows) <= 20
        # buckets are ascending and non-overlapping
        assert all(rows[i][0] < rows[i + 1][0] for i in range(len(rows) - 1))

    def test_interval_avg(self, tsdb, populated):
        rows = tsdb.query("cpu.usage", aggregation="avg", interval="10s")

        assert 1 <= len(rows) <= 20
        for _, v in rows:
            assert 0.0 <= v <= 19.0


class TestStandalone:
    def test_median(self):
        assert median([1, 2, 3, 4, 5]) == 3
        assert median([1, 2]) == 1.5

    def test_p95_p99(self):
        vals = list(range(100))
        assert p95(vals) == pytest.approx(94.05, abs=1e-6)
        assert p99(vals) == pytest.approx(98.01, abs=1e-6)

    def test_aggregate_functions(self):
        samples = [(0, 1.0), (1, 2.0), (2, 3.0), (3, 4.0)]
        assert aggregate_series(samples, "avg") == 2.5
        assert aggregate_series(samples, "min") == 1.0
        assert aggregate_series(samples, "last") == 4.0

    def test_bad_aggregation(self):
        import pytest as _p

        with _p.raises(ValueError):
            aggregate_series([(0, 1.0)], "bogus")

    def test_gap_fill(self):
        samples = [(0, 0.0), (10, 10.0), (1000, 20.0)]
        filled = gap_fill_linear(samples, max_gap_ns=100)
        # gap of 10ns <= 100 -> insert midpoint (5,5); gap of 990 -> nothing

        assert len(filled) == 4
        assert (5, 5.0) in filled

    def test_downsample_buckets(self):
        samples = [(i * 1_000_000_000, float(i)) for i in range(10)]
        d = downsample(samples, "5s", "sum")
        assert len(d) == 2
        # sum 0..4
        assert d[0][1] == pytest.approx(10.0)


class TestStream:
    def test_query_stream(self, tsdb, populated):
        rows = list(tsdb.query_stream("cpu.usage"))
        assert len(rows) == 20

    def test_missing_series_stream(self, tsdb):
        assert list(tsdb.query_stream("missing")) == []


class TestEdge:
    def test_empty_range(self, tsdb):
        now = time.time_ns()
        tsdb.insert("x", 1.0, None, timestamp_ns=now)
        # query far in the past
        rows = tsdb.query("x", start=now - 1_000_000_000_000, end=now - 500_000_000_000)

        assert rows == []

    def test_limit(self, tsdb, populated):
        rows = tsdb.query("cpu.usage", limit=5)
        assert len(rows) == 5
        # ascending keeps the first 5
        assert rows[0][1] == 0.0

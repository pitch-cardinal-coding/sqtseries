"""Rollup query fast-path tests: equivalence with the raw path + eligibility."""

import time
from datetime import UTC, datetime

import pytest

from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema
from sqtseries.partition import rollup_new_hours
from sqtseries.query import TimeSeriesDB
from sqtseries.query.agg import aggregate_series, downsample

HOUR = 3_600_000_000_000


def _ns(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000_000)


@pytest.fixture
def rollup_env(tmp_path):
    """A DB with data in completed hours 10..18, rolled up to 20:00."""
    eng = create_sqlite_engine(str(tmp_path / "r.sqlite"))
    initialize_schema(eng)
    store = StorageEngine(eng)
    base = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
    rows = []
    t = base
    # hours 10..18, 6 points/hour
    for h in range(9):
        for m in range(6):
            rows.append(("cpu", {"h": "1"}, float(h * 10 + m), t))
            t += 600 * 1_000_000_000
    store.insert_many(rows)
    rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 20, 0, tzinfo=UTC)))
    yield TimeSeriesDB(store), store
    store.close()


def _raw(store, metric, start, end, agg, interval=None, limit=None):
    """The reference raw-path computation (Python over streamed samples)."""
    samples = list(
        store.query_time_range(
            series_ids=store.series_ids_for_metric(metric),
            start_ns=start,
            end_ns=end,
        )
    )
    if agg and interval:
        r = downsample(samples, interval, agg)
    elif agg:
        r = [(samples[0][0], aggregate_series(samples, agg))] if samples else []
    else:
        r = samples
    if limit is not None:
        r = r[:limit]
    return r


class TestEquivalence:
    """Rollup-served results must be identical to the raw path."""

    @pytest.mark.parametrize(
        "func,interval",
        [
            ("avg", None),
            ("sum", None),
            ("min", None),
            ("max", None),
            ("count", None),
            ("avg", "1h"),
            ("sum", "1h"),
            ("max", "1h"),
            ("avg", "2h"),
            ("sum", "2h"),
            ("avg", "6h"),
            ("avg", "1d"),
        ],
    )
    def test_query_matches_raw(self, rollup_env, func, interval):
        tsdb, store = rollup_env
        start = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        end = _ns(datetime(2026, 1, 1, 18, 30, tzinfo=UTC))
        got = tsdb.query(
            "cpu", start=start, end=end, aggregation=func, interval=interval
        )
        exp = _raw(store, "cpu", start, end, func, interval)
        assert len(got) == len(exp)
        for (gts, gv), (ets, ev) in zip(got, exp, strict=True):
            assert gv == pytest.approx(ev, rel=1e-12, abs=1e-9)
            assert gts == ets

    def test_aggregate_matches_raw(self, rollup_env):
        tsdb, store = rollup_env
        start = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        end = _ns(datetime(2026, 1, 1, 18, 30, tzinfo=UTC))
        funcs = ["avg", "sum", "min", "max", "count"]
        got = tsdb.aggregate("cpu", start=start, end=end, funcs=funcs)
        raw = list(
            store.query_time_range(
                series_ids=store.series_ids_for_metric("cpu"),
                start_ns=start,
                end_ns=end,
            )
        )
        for f in funcs:
            assert got[f] == pytest.approx(
                aggregate_series(raw, f), rel=1e-12, abs=1e-9
            )

    def test_window_within_one_hour(self, rollup_env):
        tsdb, store = rollup_env
        start = _ns(datetime(2026, 1, 1, 12, 20, tzinfo=UTC))
        end = _ns(datetime(2026, 1, 1, 12, 50, tzinfo=UTC))
        got = tsdb.query("cpu", start=start, end=end, aggregation="sum")
        exp = _raw(store, "cpu", start, end, "sum")
        assert got[0][1] == pytest.approx(exp[0][1])

    def test_window_crossing_boundary(self, rollup_env):
        tsdb, store = rollup_env
        start = _ns(datetime(2026, 1, 1, 12, 50, tzinfo=UTC))
        end = _ns(datetime(2026, 1, 1, 13, 20, tzinfo=UTC))
        for func in ("avg", "sum"):
            got = tsdb.query("cpu", start=start, end=end, aggregation=func)
            exp = _raw(store, "cpu", start, end, func)
            assert got[0][1] == pytest.approx(exp[0][1])

    def test_empty_window(self, rollup_env):
        tsdb, _ = rollup_env
        start = _ns(datetime(2026, 1, 1, 1, 0, tzinfo=UTC))
        end = _ns(datetime(2026, 1, 1, 2, 0, tzinfo=UTC))
        assert tsdb.query("cpu", start=start, end=end, aggregation="avg") == []
        assert (
            tsdb.query("cpu", start=start, end=end, aggregation="avg", interval="1h")
            == []
        )

    def test_limit(self, rollup_env):
        tsdb, store = rollup_env
        start = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        end = _ns(datetime(2026, 1, 1, 18, 30, tzinfo=UTC))
        got = tsdb.query(
            "cpu", start=start, end=end, aggregation="avg", interval="1h", limit=3
        )
        exp = _raw(store, "cpu", start, end, "avg", "1h", limit=3)
        assert len(got) == 3
        assert [v for _, v in got] == [v for _, v in exp]

    def test_trailing_edge_current_hour(self, tmp_path):
        """Data written into a not-yet-rolled hour is still served correctly."""
        eng = create_sqlite_engine(str(tmp_path / "te.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)
        base = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        rows = [("cpu", None, 1.0, base), ("cpu", None, 2.0, base + 600 * 1e9)]
        store.insert_many(rows)
        rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 20, 0, tzinfo=UTC)))
        # New data in hour 19 (completed but not rolled — written after rollup).
        late = _ns(datetime(2026, 1, 1, 19, 10, tzinfo=UTC))
        store.insert_many(
            [("cpu", None, 50.0, late), ("cpu", None, 60.0, late + 60 * 1e9)]
        )
        tsdb = TimeSeriesDB(store)
        start = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        end = _ns(datetime(2026, 1, 1, 19, 20, tzinfo=UTC))
        got = tsdb.query("cpu", start=start, end=end, aggregation="sum")
        exp = _raw(store, "cpu", start, end, "sum")
        # 3 + 110
        assert got[0][1] == pytest.approx(exp[0][1])
        store.close()


class TestEligibility:
    """Only rollup-expressible queries may use the fast path."""

    @pytest.mark.parametrize("func", ["median", "p95", "p99", "first", "last"])
    def test_non_expressible_funcs_fall_back(self, rollup_env, monkeypatch, func):
        import sqtseries.query.builder as builder

        tsdb, _ = rollup_env
        start = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        end = _ns(datetime(2026, 1, 1, 18, 30, tzinfo=UTC))
        called = []

        def spy(db, sids, s, e, *, bucket_ns):
            called.append(True)
            return []

        monkeypatch.setattr(builder, "query_rollup_partial", spy)
        tsdb.query("cpu", start=start, end=end, aggregation=func)
        assert called == []

    @pytest.mark.parametrize("interval", ["30m", "90m", "1m", "45s"])
    def test_non_aligned_interval_falls_back(self, rollup_env, monkeypatch, interval):
        import sqtseries.query.builder as builder

        tsdb, _ = rollup_env
        start = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        end = _ns(datetime(2026, 1, 1, 18, 30, tzinfo=UTC))
        called = []

        def spy(db, sids, s, e, *, bucket_ns):
            called.append(True)
            return []

        monkeypatch.setattr(builder, "query_rollup_partial", spy)
        tsdb.query("cpu", start=start, end=end, aggregation="avg", interval=interval)
        assert called == []

    def test_no_rollup_data_falls_back(self, tmp_path, monkeypatch):
        import sqtseries.query.builder as builder

        eng = create_sqlite_engine(str(tmp_path / "nr.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)
        tsdb = TimeSeriesDB(store)
        now = time.time_ns()
        store.insert_many([("cpu", None, 1.0, now)])
        called = []

        def spy(db, sids, s, e, *, bucket_ns):
            called.append(True)
            return []

        monkeypatch.setattr(builder, "query_rollup_partial", spy)
        tsdb.query("cpu", aggregation="avg")
        assert called == []

    def test_tail_beyond_watermark_falls_back(self, rollup_env, monkeypatch):
        import sqtseries.query.builder as builder

        tsdb, _ = rollup_env
        # end in the future relative to the 20:00 watermark -> not eligible
        start = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        end = _ns(datetime(2026, 1, 1, 23, 0, tzinfo=UTC))
        called = []

        def spy(db, sids, s, e, *, bucket_ns):
            called.append(True)
            return []

        monkeypatch.setattr(builder, "query_rollup_partial", spy)
        tsdb.query("cpu", start=start, end=end, aggregation="avg")
        assert called == []

    def test_eligible_uses_rollup(self, rollup_env, monkeypatch):
        import sqtseries.query.builder as builder

        tsdb, _ = rollup_env
        start = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        end = _ns(datetime(2026, 1, 1, 18, 30, tzinfo=UTC))
        called = []

        def spy(db, sids, s, e, *, bucket_ns):
            called.append(True)
            return []

        monkeypatch.setattr(builder, "query_rollup_partial", spy)
        tsdb.query("cpu", start=start, end=end, aggregation="avg", interval="1h")
        # fast path used
        assert called


def test_watermark_and_idempotence(tmp_path):
    """rollup_new_hours advances the watermark and is idempotent."""
    from sqtseries.partition import rollup_watermark

    eng = create_sqlite_engine(str(tmp_path / "wm.sqlite"))
    initialize_schema(eng)
    store = StorageEngine(eng)
    base = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
    store.insert_many([("cpu", None, 1.0, base), ("cpu", None, 2.0, base + 10 * 1e9)])
    assert rollup_watermark(eng) == 0
    n = rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC)))
    assert n == 1
    wm = rollup_watermark(eng)
    assert wm == _ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC))
    # idempotent: nothing new
    assert (
        rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 12, 30, tzinfo=UTC))) == 0
    )
    # advancing the clock catches the new completed hour
    store.insert_many([("cpu", None, 3.0, base + 2 * HOUR)])
    assert (
        rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 13, 0, tzinfo=UTC))) == 1
    )
    store.close()

"""Rollup query fast-path tests: equivalence with the raw path + eligibility."""

import time
from datetime import UTC, datetime

import pytest

from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema
from sqtseries.partition import rollup_new_hours
from sqtseries.query import MaxRowsExceededError, TimeSeriesDB
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


class TestBackfillVisibility:
    """A write into an already-rolled hour must stay visible to the fast path.

    ``rollup_new_hours`` only advances forward, so a backfilled row used to be
    stored, reported as written, and then silently omitted from every
    rollup-served answer. The write path rewinds the watermark so the next pass
    re-aggregates the hour; ``INSERT OR REPLACE`` makes that idempotent.
    """

    def test_backfill_is_re_rolled_and_counted(self, tmp_path):
        from sqtseries.partition import rollup_watermark

        eng = create_sqlite_engine(str(tmp_path / "bf.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)
        db = TimeSeriesDB(store)

        base = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        store.insert_many([("cpu", None, 1.0, base), ("cpu", None, 2.0, base + 10)])
        rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC)))
        rolled = rollup_watermark(eng)
        assert rolled == _ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC))

        # Backfill into hour 10, which the rollup has already passed.
        store.insert_many([("cpu", None, 41.0, base + 20)])

        # The watermark is rewound to the backfilled hour, not past it.
        assert rollup_watermark(eng) == base - (base % HOUR)

        # And the next pass re-aggregates that hour.
        assert (
            rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC)))
            == 1
        )

        # The whole window now counts every row, not just the re-rolled ones.
        agg = db.aggregate(
            "cpu",
            base,
            _ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC)),
            funcs=["count", "sum"],
        )
        assert agg["count"] == 3.0
        assert agg["sum"] == pytest.approx(44.0)
        store.close()

    def test_rollup_does_not_clobber_a_concurrent_rewind(self, tmp_path):
        """A rewind landing mid-rollup must survive that rollup.

        ``rollup_new_hours`` reads the watermark, does its work, then writes the
        new one. A backfilled write can rewind in that gap. The watermark is
        read as 12:00 here, so the range being rolled is [12:00, 14:00) and the
        rewind to 10:00 falls OUTSIDE it — hour 10 is not re-aggregated by this
        pass, so advancing past it would drop it forever.
        """
        from sqtseries.partition import rollup as rollup_mod
        from sqtseries.partition import rollup_watermark
        from sqtseries.partition.rollup import META_LAST_HOUR, ROLLUP_META

        eng = create_sqlite_engine(str(tmp_path / "race.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)

        base = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        store.insert_many([("cpu", None, 1.0, base)])
        rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC)))
        assert rollup_watermark(eng) == _ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC))

        rewound_to = base - (base % HOUR)
        real_rollup_partition = rollup_mod.rollup_partition

        def rewind_mid_roll(db, name, **kw):
            # Stands in for a backfilled write arriving between the watermark
            # read and the watermark write.
            with db.begin() as conn:
                conn.exec_driver_sql(
                    f"INSERT OR REPLACE INTO {ROLLUP_META} (k, v) VALUES (?, ?)",  # noqa: S608
                    (META_LAST_HOUR, rewound_to),
                )
            return real_rollup_partition(db, name, **kw)

        rollup_mod.rollup_partition = rewind_mid_roll
        try:
            rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 14, 0, tzinfo=UTC)))
        finally:
            rollup_mod.rollup_partition = real_rollup_partition

        assert rollup_watermark(eng) == rewound_to, (
            "the rollup advanced past a rewind whose hour it never re-aggregated"
        )
        store.close()

    def test_settled_rewind_does_not_pin_the_watermark(self, tmp_path):
        """A rewind already covered by this pass must not pin the watermark.

        When the watermark is rewound before the call, the range rolled is
        [10:00, 14:00) — which INCLUDES the rewound hour, so that hour is
        re-aggregated here. Settling on 10:00 would be re-selecting a range
        that has just been done, and since the watermark is the start of the
        next pass it would never advance: every later pass re-scans the same
        hours forever.
        """
        from sqtseries.partition import rollup_watermark

        eng = create_sqlite_engine(str(tmp_path / "pin.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)

        base = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        store.insert_many([("cpu", None, 1.0, base)])
        rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC)))

        # Backfill into the already-rolled hour 10; the write path rewinds.
        store.insert_many([("cpu", None, 2.0, base + 10)])
        assert rollup_watermark(eng) == base - (base % HOUR)

        rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 14, 0, tzinfo=UTC)))
        assert rollup_watermark(eng) == _ns(datetime(2026, 1, 1, 14, 0, tzinfo=UTC)), (
            "watermark did not advance past a rewind this pass had already "
            "re-aggregated — it will re-scan these hours on every future pass"
        )

        # And it stays advanced (this is the livelock: the old rule pinned it).
        rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 16, 0, tzinfo=UTC)))
        assert rollup_watermark(eng) == _ns(datetime(2026, 1, 1, 16, 0, tzinfo=UTC))
        store.close()

    def test_aggregate_matches_raw_after_backfill(self, tmp_path):
        eng = create_sqlite_engine(str(tmp_path / "eq.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)
        db = TimeSeriesDB(store)

        base = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        store.insert_many([("cpu", None, 1.0, base), ("cpu", None, 2.0, base + 10)])
        rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC)))
        store.insert_many([("cpu", None, 30.0, base + 20)])
        rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC)))

        end = _ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC))
        for func in ("avg", "sum", "min", "max", "count"):
            fast = db.aggregate("cpu", base, end, funcs=[func])[func]
            raw = aggregate_series(
                [
                    (ts, v)
                    for ts, v in store.query_time_range(
                        metric="cpu", start_ns=base, end_ns=end
                    )
                ],
                func,
            )
            assert fast == pytest.approx(raw), func
        store.close()


class TestRollupEdgeRowCap:
    """The fast path's raw edge reads must honour ``max_rows`` like the raw path.

    Both edges used to call ``query_time_range`` with no ``limit``, so a window
    whose *tail* was large materialised every row while the same query without
    an aggregation was correctly refused.
    """

    def test_edge_read_refuses_over_cap(self, tmp_path):
        eng = create_sqlite_engine(str(tmp_path / "cap.sqlite"))
        initialize_schema(eng)
        store = StorageEngine(eng)
        db = TimeSeriesDB(store, max_rows=100)

        base = _ns(datetime(2026, 1, 1, 10, 0, tzinfo=UTC))
        # A few rows in a completed hour, then many inside the current hour.
        store.insert_many([("cpu", None, 1.0, base + i) for i in range(5)])
        rollup_new_hours(eng, now_ns=_ns(datetime(2026, 1, 1, 20, 0, tzinfo=UTC)))

        now = time.time_ns()
        store.insert_many([("cpu", None, 2.0, now - 500 + i) for i in range(300)])

        # Raw read over the same window refuses, and so must the fast path.
        with pytest.raises(MaxRowsExceededError):
            db.query("cpu", start=base, end=now + 1_000_000_000)
        with pytest.raises(MaxRowsExceededError):
            db.aggregate("cpu", base, now + 1_000_000_000, funcs=["avg"])

        # A window inside the rolled region is small enough, and the fast path
        # still answers it.
        rolled_end = _ns(datetime(2026, 1, 1, 12, 0, tzinfo=UTC))
        got = db.aggregate("cpu", base, rolled_end, funcs=["count", "sum"])
        assert got["count"] == 5.0
        assert got["sum"] == pytest.approx(5.0)
        store.close()

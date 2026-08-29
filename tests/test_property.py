"""Property-based tests (Hypothesis): invariants over random data.
Each test builds a fresh database per generated example. Timestamps are
guaranteed unique (the clustered PK is (series_id, timestamp_ns)), so every
insert succeeds and every point round-trips.
"""

import tempfile
from datetime import UTC, datetime

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema
from sqtseries.partition import ensure_rollup_table, rollup_new_hours
from sqtseries.query import TimeSeriesDB

# 2023-11-14 UTC
_BASE = 1_700_000_000_000_000_000

_PROP = settings(max_examples=20, deadline=5000)
_ROLLUP = settings(max_examples=8, deadline=10000)


@st.composite
def points(draw, max_offset: int = 10_000_000):
    """(value, timestamp_offset_ns) pairs with unique offsets.
    Values are bounded to +/-1e6 so summation invariants don't trip on float

    overflow; the goal is to test aggregation logic, not IEEE exoticism.

    """
    n = draw(st.integers(min_value=1, max_value=30))

    offsets = draw(
        st.lists(
            st.integers(min_value=0, max_value=max_offset),
            min_size=n,
            max_size=n,
            unique=True,
        )
    )
    vals = draw(
        st.lists(
            st.floats(
                min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False
            ),
            min_size=n,
            max_size=n,
        )
    )
    return list(zip(vals, offsets, strict=True))


def _fresh_store():
    eng = create_sqlite_engine(tempfile.mkdtemp() + "/prop.sqlite")
    initialize_schema(eng)
    return StorageEngine(eng), eng


def _insert(store, data, spacing_ns=1_000_000):
    rows = [("m", None, v, _BASE + off * spacing_ns) for v, off in data]

    store.insert_many(rows)
    return rows


@_PROP
@given(data=points())
def test_roundtrip_exact(data):
    """Every written (value, ts) pair comes back exactly."""
    store, eng = _fresh_store()
    try:
        rows = _insert(store, data)

        seen = list(store.query_time_range(metric="m"))
        assert len(seen) == len(data)
        expected = sorted((ts, v) for _, _, v, ts in rows)

        for (ets, ev), (gts, gv) in zip(expected, seen, strict=True):
            assert gts == ets
            assert gv == pytest.approx(ev)
    finally:
        eng.dispose()


@_PROP
@given(data=points())
def test_series_identity_stable(data):
    """Same (metric, tags) resolves to the same series_id every time."""

    store, eng = _fresh_store()
    try:
        with store.db.connect() as conn:
            sid1 = store.resolve_series(conn, "m", {"h": "x"})

            sid2 = store.resolve_series(conn, "m", {"h": "x"})

            sid3 = store.resolve_series(conn, "m", {"h": "y"})
        assert sid1 == sid2
        assert sid1 != sid3
    finally:
        eng.dispose()


@_PROP
@given(data=points())
def test_aggregate_count_matches(data):
    store, eng = _fresh_store()
    try:
        _insert(store, data)
        stats = TimeSeriesDB(store).aggregate("m", funcs=["count"])
        assert stats["count"] == len(data)
    finally:
        eng.dispose()


@_PROP
@given(data=points())
def test_aggregate_sum_matches_python(data):
    store, eng = _fresh_store()
    try:
        _insert(store, data)
        expected = sum(v for v, _ in data)

        stats = TimeSeriesDB(store).aggregate("m", funcs=["sum"])
        assert stats["sum"] == pytest.approx(expected, rel=1e-9)
    finally:
        eng.dispose()


@_PROP
@given(data=points())
def test_min_max_bounds(data):
    store, eng = _fresh_store()
    try:
        _insert(store, data)
        vals = [v for v, _ in data]

        stats = TimeSeriesDB(store).aggregate("m", funcs=["min", "max"])

        assert stats["min"] <= min(vals)
        assert stats["max"] >= max(vals)
    finally:
        eng.dispose()


@_PROP
@given(data=points())
def test_downsample_counts_conserve(data):
    """Sum of bucket counts == number of points (buckets partition time)."""

    store, eng = _fresh_store()
    try:
        # 5s apart
        _insert(store, data, spacing_ns=5_000_000_000)
        buckets = TimeSeriesDB(store).query("m", aggregation="count", interval="5s")

        assert sum(int(v) for _, v in buckets) == len(data)
    finally:
        eng.dispose()


@_PROP
@given(data=points())
def test_downsample_sum_conserves(data):
    store, eng = _fresh_store()
    try:
        _insert(store, data, spacing_ns=5_000_000_000)
        total = sum(v for v, _ in data)

        buckets = TimeSeriesDB(store).query("m", aggregation="sum", interval="5s")

        assert sum(v for _, v in buckets) == pytest.approx(total, rel=1e-9)
    finally:
        eng.dispose()


@_PROP
@given(offset=st.integers(min_value=0, max_value=27))
def test_month_crossing_query_complete(offset):
    store, eng = _fresh_store()
    try:
        # ~ Dec 31 2023 23:59:59.999
        boundary = 1_704_067_199_999_999_000
        pts = [(boundary + (i - 5) * 100_000_000, float(i)) for i in range(10)]

        store.insert_many([("m", None, v, ts) for ts, v in pts])
        got = TimeSeriesDB(store).query("m")
        assert len(got) == 10
        for ts, v in pts:
            assert any(gts == ts and gv == pytest.approx(v) for gts, gv in got)
    finally:
        eng.dispose()


@_ROLLUP
@given(data=points(max_offset=2000))
def test_rollup_matches_raw_for_windows(data):
    """After a rollup, wide-window aggregates equal the raw-path answers.

    Offsets are bounded so all data lands before the rollup watermark (the

    fast path needs fully-inside completed hours).
    """
    store, eng = _fresh_store()
    try:
        # 60s apart
        _insert(store, data, spacing_ns=60_000_000_000)
        ensure_rollup_table(eng)
        # _BASE is 2023-11-14 21:13; max offset 2000*60s lands ~Nov 16 06:13,
        # so the rollup frontier must be comfortably after all data
        now_ns = int(datetime(2023, 11, 16, 12, 0, tzinfo=UTC).timestamp() * 1e9)

        rollup_new_hours(eng, now_ns=now_ns)
        tsdb = TimeSeriesDB(store)

        for func in ("avg", "sum", "min", "max", "count"):
            # explicit end <= watermark so the rollup fast path is exercised

            rollup_val = tsdb.aggregate("m", end=now_ns, funcs=[func])[func]

            raw_rows = list(store.query_time_range(metric="m", end_ns=now_ns))

            if func == "avg":
                expected = sum(v for _, v in raw_rows) / len(raw_rows)
            elif func == "sum":
                expected = sum(v for _, v in raw_rows)
            elif func == "count":
                expected = len(raw_rows)
            elif func == "min":
                expected = min(v for _, v in raw_rows)
            else:
                expected = max(v for _, v in raw_rows)
            assert rollup_val == pytest.approx(expected, rel=1e-9)
    finally:
        eng.dispose()

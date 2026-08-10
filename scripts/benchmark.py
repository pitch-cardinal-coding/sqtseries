"""Performance benchmark: SQLite insert throughput + query latency.

Measures the raw engine and the rollup fast path. Data is written at evenly
spaced intervals over ``--span-hours`` hours across ``--series`` series, ending
on a completed hour boundary (two hours in the past) so that every measurement
is rollable.

Usage:
    python3 scripts/benchmark.py --db /tmp/bench.sqlite --rows 200000
"""

import argparse
import re
import statistics
import time

HOUR_NS = 3_600_000_000_000
_PARTITION_RE = re.compile(r"^measurements_\d{4}_\d{2}$")


def _completed_hour_end() -> int:
    """Start of a completed hour two hours in the past (all data rollable)."""
    now_ns = time.time_ns()
    return (now_ns // HOUR_NS) * HOUR_NS - 2 * HOUR_NS


def _data_bounds(engine) -> tuple[int | None, int | None]:
    """(min, max) timestamp across all measurement partitions."""
    lo = hi = None
    for name in engine.get_table_names():
        if not _PARTITION_RE.match(name):
            continue
        with engine.connect() as conn:
            r = conn.exec_driver_sql(
                f"SELECT MIN(timestamp_ns), MAX(timestamp_ns) FROM {name}"  # noqa: S608 - name validated by _PARTITION_RE
            ).first()
        if r[0] is not None:
            lo = r[0] if lo is None else min(lo, r[0])
            hi = r[1] if hi is None else max(hi, r[1])
    return lo, hi


def bench_inserts(
    db_path: str, rows: int, batch: int, series: int, span_hours: int
) -> dict:
    from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema

    engine = create_sqlite_engine(db_path)
    initialize_schema(engine)
    store = StorageEngine(engine)

    end = _completed_hour_end()
    per_series = rows // series
    span_ns = span_hours * HOUR_NS
    # fixed sample interval across the span
    spacing_ns = span_ns // per_series
    start = time.perf_counter()
    chunk = []
    for s in range(series):
        for k in range(per_series):
            ts = end - (per_series - 1 - k) * spacing_ns
            chunk.append(
                (
                    "bench.metric",
                    {"host": f"h{s % 20}", "id": str(s)},
                    float(k % 1000),
                    ts,
                )
            )
            if len(chunk) >= batch:
                store.insert_many(chunk)
                chunk = []
    if chunk:
        store.insert_many(chunk)
    elapsed = time.perf_counter() - start
    engine.dispose()
    return {
        "rows": rows,
        "seconds": round(elapsed, 3),
        "rows_per_sec": round(rows / elapsed, 1),
        "series": series,
        "points_per_series": per_series,
        "span_hours": span_hours,
    }


def run_queries(db_path: str, n: int) -> dict:
    from sqtseries.engine import StorageEngine, create_sqlite_engine
    from sqtseries.partition import rollup_new_hours
    from sqtseries.query import TimeSeriesDB

    engine = create_sqlite_engine(db_path)
    store = StorageEngine(engine)
    ts = TimeSeriesDB(store)

    sid = store.series_ids_for_metric("bench.metric")
    if not sid:
        return {"error": "no data"}
    sid = sid[0]

    lo, hi = _data_bounds(engine)
    if lo is None or hi is None:
        return {"error": "no data"}

    def measure(fn) -> tuple[float, float]:
        lat = []
        for _ in range(n):
            t0 = time.perf_counter()
            fn()
            lat.append((time.perf_counter() - t0) * 1e3)
        return (
            round(statistics.median(lat), 2),
            round(sorted(lat)[min(int(n * 0.95), len(lat) - 1)], 2),
        )

    # 1. Raw single-series 1h time range (clustered PK path).
    range_p50, range_p95 = measure(
        lambda: list(
            ts.query(series_ids=[sid], start=int(hi - 3600 * 1e9), end=int(hi))
        )
    )

    # 2. Wide-window aggregate BEFORE rollup (raw path, Python bucketing).
    agg_p50, agg_p95 = measure(
        lambda: list(
            ts.query(
                series_ids=[sid],
                start=int(lo),
                end=int(hi),
                aggregation="avg",
                interval="1h",
            )
        )
    )

    # 3. Build the hourly rollup, then re-measure the same wide-window query.
    rollup_rows = rollup_new_hours(engine)
    roll_p50, roll_p95 = measure(
        lambda: list(
            ts.query(
                series_ids=[sid],
                start=int(lo),
                end=int(hi),
                aggregation="avg",
                interval="1h",
            )
        )
    )

    engine.dispose()
    return {
        "samples": n,
        "raw_range_1h_p50_ms": range_p50,
        "raw_range_1h_p95_ms": range_p95,
        "agg_wide_raw_p50_ms": agg_p50,
        "agg_wide_raw_p95_ms": agg_p95,
        "rollup_rows": rollup_rows,
        "agg_wide_rollup_p50_ms": roll_p50,
        "agg_wide_rollup_p95_ms": roll_p95,
        "speedup": round(agg_p50 / roll_p50, 1) if roll_p50 else None,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/tmp/sqtest.sqlite")
    ap.add_argument("--rows", type=int, default=200000)
    ap.add_argument("--batch", type=int, default=5000)
    ap.add_argument("--series", type=int, default=1000)
    ap.add_argument("--span-hours", type=int, default=24)
    ap.add_argument("--queries", type=int, default=100)
    args = ap.parse_args()

    print(
        "inserts:",
        bench_inserts(args.db, args.rows, args.batch, args.series, args.span_hours),
    )
    print("queries:", run_queries(args.db, args.queries))


if __name__ == "__main__":
    main()

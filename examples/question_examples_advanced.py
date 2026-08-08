"""The next-level questions: per-series, rates, patterns, data quality.

Companion to ``question_examples.py`` (the first 13 questions). This script
answers the questions you ask once you are past the basics — the ones that
need per-series breakdowns, two windows at once, or a bit of arithmetic on
top of the query results. Seed data is deterministic and independent, but
it writes to the same database, so running both catalogs back to back is
fine:

    python3 examples/question_examples_advanced.py --db /path/to/db.sqlite

The ``--db PATH`` flag unlocks questions 1-3, which read series directly
from the database file (the embedded API) — the wire protocol queries a
whole metric, so per-series breakdowns need file access. All other
questions run against the service over ZMQ/HTTP like the base catalog.

Catalog:

  1.  Which host used the most CPU? (per-series breakdown)
  2.  What is this host's 95th percentile latency? (per-series percentile)
  3.  What are the per-page visitor totals? (tag breakdown)
  4.  Throughput: visitors per minute in the last hour?
  5.  The full percentile ladder (median, p95, p99) for latency.
  6.  Today vs yesterday vs the same day last week (avg comparison).
  7.  Which hour of the day is typically the busiest for CPU?
  8.  Did I receive all the readings I expected today? (data quality)
  9.  Is my data growing? (day-over-day point counts)
  10. What is the latest reading, and how old is it? (staleness)

Each "ANSWER:" line is machine-readable (key = value) so automated tests
can assert on the printed results. Connect overrides: --write-port /
--query-port / --http-port.
"""

import argparse
import json
import time

from sqtseries import Client

WRITE_PORT = 12501
QUERY_PORT = 12502
HTTP_PORT = 12505


def utc_day_start(days_ago: int = 0) -> int:
    """Epoch seconds of UTC midnight, ``days_ago`` before today."""
    now_s = int(time.time())
    return (now_s // 86400 - days_ago) * 86400


class ExampleEnv:
    """One live instance: seed data, then fire every question at it."""

    def __init__(
        self,
        host: str,
        write_port: int,
        query_port: int,
        http_port: int,
        admin_port: int,
        db_path: str | None,
    ):
        self.host = host
        self.http_port = http_port
        self.db_path = db_path
        self.client = Client(
            ports={
                "write": write_port,
                "query": query_port,
                "subscribe": -1,
                "admin": admin_port,
            }
        )
        self._sent = 0

    def seed(self) -> None:
        """Deterministic data: hosts, pages, latency, visitors, cpu."""
        now_s = time.time()

        # cpu per host: 3 hosts x 120 points every 30s over 1 hour.
        # Values scale by host (web1 low .. web3 high) so the per-series
        # rankings are deterministic.
        for hi, host in enumerate(["web1", "web2", "web3"]):
            for k in range(120):
                val = 30.0 + hi * 20 + (k * 7) % 40
                self.send("cpu.usage", val, now_s - (120 - k) * 30, {"host": host})

        # latency per page: 3 pages x 80 points every 90s over 2 hours.
        # page 3 is the slowest (deterministic).
        for pi in range(3):
            for k in range(80):
                val = 5.0 + pi * 15 + (k * 3) % 20
                self.send(
                    "latency.page",
                    val,
                    now_s - (80 - k) * 90,
                    {"page": f"/page/{pi + 1}"},
                )

        # visitors.page: DISTINCT metric per page, 80 points over 2h.
        # (A metric with a tag is a separate series; never mix tagged and
        # untagged points on the same metric name if you need clean splits.)
        for pi, page in enumerate(["/page/1", "/page/2", "/page/3"]):
            for k in range(80):
                val = 50.0 + pi * 80 + (k * 5) % 120
                self.send("visitors.page", val, now_s - (80 - k) * 90, {"page": page})

        # visitors.web: one reading per minute over 8 days, with a daily
        # rhythm (morning ramp) and +10% growth per day, so "today vs
        # yesterday vs a week ago" and the daily-volume trend all differ.
        for d in range(8):
            start = now_s - (8 - d) * 86400  # block ends at `now`
            growth = 1.0 + d * 0.10
            for m in range(1440):
                hour = m // 60
                val = (30.0 + hour * 9.0 + (m % 60) * 0.5) * growth
                self.send("visitors.web", val, start + m * 60)

        # cpu.load: 72 points every 5 min over 6 hours (busiest-hour Q)
        for k in range(72):
            val = 20.0 + (k * 7) % 81
            self.send("cpu.load", val, now_s - (72 - k) * 300)

    def send(
        self, metric: str, value: float, ts_s: float, tags: dict | None = None
    ) -> None:
        self._sent += 1
        self.client.write(metric, value, tags=tags, timestamp=ts_s)
        # Fire-and-forget PUSH: the client-side socket HWM (10,000) can fill
        # when seeding tens of thousands of points faster than the service
        # drains them; a short pause every 2,000 keeps the pipe open without
        # needing the service to be any particular speed.
        if self._sent % 2000 == 0:
            time.sleep(0.05)

    def wait_for_ingest(self, expected: int, timeout_s: float = 60.0) -> None:
        """Block until the service reports ``expected`` ingested messages.

        ZMQ ingestion is fire-and-forget: the client can push 10k+ points
        in well under a second, while the service takes a moment to drain
        the socket. Polling admin ``stats`` until the counter catches up
        makes the demo deterministic on any machine.
        """
        import time as _time

        deadline = _time.monotonic() + timeout_s
        ingested = 0
        while _time.monotonic() < deadline:
            try:
                st = self.client.admin("stats")
                ingested = st.get("ingested", 0)
                if ingested >= expected:
                    return
            except Exception:  # noqa: S110 - service may still be starting
                pass
            _time.sleep(0.25)
        print(
            f"  (warning: only {ingested}/{expected} "
            f"messages ingested within {timeout_s:.0f}s)"
        )

    # -- embedded reads (need --db; the wire cannot split by series) ------

    def open_store(self):
        from sqtseries.engine import StorageEngine, create_sqlite_engine

        engine = create_sqlite_engine(self.db_path)
        return StorageEngine(engine), engine

    def series_ids(self, metric: str) -> list[int]:
        store, engine = self.open_store()
        try:
            return store.series_ids_for_metric(metric)
        finally:
            engine.dispose()

    def series_tags(self, sid: int) -> dict:
        store, engine = self.open_store()
        try:
            _, tags_json = store.get_series_meta(sid)
            return json.loads(tags_json) if tags_json else {}
        finally:
            engine.dispose()

    def series_aggregate(self, sid: int, start_s: int, end_s: int, func: str) -> float:
        """One aggregation for one series over a window (embedded)."""
        from sqtseries.engine import StorageEngine, create_sqlite_engine
        from sqtseries.query import TimeSeriesDB

        engine = create_sqlite_engine(self.db_path)
        try:
            ts = TimeSeriesDB(StorageEngine(engine))
            rows = ts.query(
                series_ids=[sid],
                start=start_s * 10**9,
                end=end_s * 10**9,
                aggregation=func,
            )
            return rows[0][1] if rows else float("nan")
        finally:
            engine.dispose()


def q1_busiest_host(env: ExampleEnv) -> None:
    """Which host had the highest average CPU over the last hour?"""
    if not env.db_path:
        print("  ANSWER: busiest_host = skipped (needs --db)")
        return
    now_s = int(time.time())
    avgs: dict[str, float] = {}
    for sid in env.series_ids("cpu.usage"):
        host = env.series_tags(sid).get("host", "?")
        avgs[host] = env.series_aggregate(sid, now_s - 3600, now_s, "avg")
    if not avgs:
        print("  ANSWER: busiest_host = none")
        return
    hosts = sorted(avgs, key=lambda h: avgs[h])
    print("  per-host averages: " + " | ".join(f"{h}={avgs[h]:.1f}%" for h in hosts))
    print(f"  ANSWER: busiest_host = {hosts[-1]} ({avgs[hosts[-1]]:.1f}%)")


def q2_p95_per_page(env: ExampleEnv) -> None:
    """What is the 95th-percentile latency of each page over 2 hours?"""
    if not env.db_path:
        print("  ANSWER: p95_page = skipped (needs --db)")
        return
    now_s = int(time.time())
    for sid in env.series_ids("latency.page"):
        page = env.series_tags(sid).get("page", "?")
        p95 = env.series_aggregate(sid, now_s - 2 * 3600, now_s, "p95")
        key = "p95_" + page.strip("/").replace("/", "_")
        print(f"  ANSWER: {key} = {p95:.1f} ms")


def q3_visitors_per_page(env: ExampleEnv) -> None:
    """Which page drew the most visitors over the last 2 hours?"""
    if not env.db_path:
        print("  per-page totals: skipped (needs --db)")
        return
    now_s = int(time.time())
    totals: dict[str, float] = {}
    for sid in env.series_ids("visitors.page"):
        page = env.series_tags(sid).get("page", "?")
        totals[page] = env.series_aggregate(sid, now_s - 2 * 3600, now_s, "sum")
    if not totals:
        print("  (no tagged visitor series found)")
        return
    for page, total in sorted(totals.items()):
        key = "visitors_" + page.strip("/").replace("/", "_")
        print(f"  ANSWER: {key} = {total:.0f}")
    top = max(totals, key=totals.get)
    print(f"  ANSWER: top_page = {top} ({totals[top]:.0f})")


def q4_throughput(env: ExampleEnv) -> None:
    """Throughput: visitors per minute in the last hour."""
    now_ns = time.time_ns()
    count = env.client.aggregate(
        "visitors.web",
        start=now_ns - 3600 * 10**9,
        end=now_ns,
        funcs=["count"],
    )["count"]
    print(f"  ANSWER: visitors_per_minute = {count / 60:.1f}")
    print(f"  ({count:.0f} visitors over the last hour)")


def q5_percentile_ladder(env: ExampleEnv) -> None:
    """The full latency ladder for the last 2 hours: median/p95/p99."""
    now_ns = time.time_ns()
    stats = env.client.aggregate(
        "latency.page",
        start=now_ns - 2 * 3600 * 10**9,
        end=now_ns,
        funcs=["median", "p95", "p99"],
    )
    print(
        "  ANSWER: latency_ladder = "
        + " ".join(f"{k}={v:.1f}ms" for k, v in stats.items())
    )


def q6_compare_days(env: ExampleEnv) -> None:
    """Today vs yesterday vs the same weekday last week (visitors)."""
    result: list[str] = []
    for label, days_ago in (("today", 0), ("yesterday", 1), ("week_ago", 7)):
        start_s = utc_day_start(days_ago)
        avg = env.client.aggregate(
            "visitors.web",
            start=start_s * 10**9,
            end=(start_s + 86400) * 10**9,
            funcs=["avg"],
        )["avg"]
        result.append(f"{label}={avg:.0f}")
    print("  ANSWER: visitors_by_day = " + " | ".join(result))


def q7_busiest_hour(env: ExampleEnv) -> None:
    """Which hourly period did the most visitors arrive in today?"""
    start_s = utc_day_start(0)
    now_s = int(time.time())
    rows = env.client.query(
        "visitors.web",
        start=start_s * 10**9,
        end=now_s * 10**9,
        aggregation="sum",
        interval="1h",
    )
    if not rows:
        print("  ANSWER: busiest_visitors_hour = none")
        return
    hour_row = max(rows, key=lambda r: r["value"])
    hh = time.strftime("%H:%M", time.gmtime(hour_row["timestamp"]))
    print(
        f"  ANSWER: busiest_visitors_hour = {hh} UTC "
        f"({hour_row['value']:.0f} visitors)"
    )


def q8_data_quality(env: ExampleEnv) -> None:
    """Did we receive every visitor bucket we expected today?"""
    start_s = utc_day_start(0)
    now_s = int(time.time())
    count = env.client.aggregate(
        "visitors.web",
        start=start_s * 10**9,
        end=now_s * 10**9,
        funcs=["count"],
    )["count"]
    expected = max(now_s - start_s, 1) // 60  # seed: one reading per minute
    print(f"  ANSWER: readings_expected = {expected}")
    print(f"  ANSWER: readings_received = {count:.0f}")
    print(f"  ANSWER: readings_missing = {max(0, expected - int(count))}")


def q9_trend(env: ExampleEnv) -> None:
    """Is data volume growing? (visitor points per day over 2 days)"""
    now_s = int(time.time())
    rows = env.client.query(
        "visitors.web",
        start=utc_day_start(1) * 10**9,
        end=now_s * 10**9,
        aggregation="count",
        interval="1d",
    )
    if not rows:
        print("  ANSWER: daily_trend = flat (not enough data)")
        return
    counts = [r["value"] for r in rows]
    first, last = counts[0], counts[-1]
    slope = last - first
    trend = "growing" if slope > 2 else ("shrinking" if slope < -2 else "flat")
    print("  ANSWER: points_per_day = " + ", ".join(f"{c:.0f}" for c in counts))
    print(f"  ANSWER: daily_trend = {trend} ({first:.0f} -> {last:.0f})")


def q10_staleness(env: ExampleEnv) -> None:
    """How old is the newest reading; is a stream still alive?"""
    rows = env.client.query(
        "visitors.web",
        start=time.time_ns() - 7 * 86400 * 10**9,
        end=time.time_ns(),
        limit=1,
        order="desc",
    )
    if not rows:
        print("  ANSWER: latest_age_s = none")
        return
    age = int(time.time() - rows[0]["timestamp"])
    print(f"  ANSWER: latest_age_s = {age}")


def main():
    parser = argparse.ArgumentParser(
        description="Advanced questions: 10 more ways to ask your data"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--write-port", type=int, default=WRITE_PORT)
    parser.add_argument("--query-port", type=int, default=QUERY_PORT)
    parser.add_argument("--http-port", type=int, default=HTTP_PORT)
    parser.add_argument("--admin-port", type=int, default=12504)
    parser.add_argument(
        "--db",
        default=None,
        help="Database file path (enables the per-series questions)",
    )
    args = parser.parse_args()

    env = ExampleEnv(
        args.host,
        args.write_port,
        args.query_port,
        args.http_port,
        args.admin_port,
        args.db,
    )

    print("=== Seeding sample data (deterministic) ===")
    env.seed()
    total = 3 * 120 + 3 * 80 + 3 * 80 + 8 * 1440 + 72
    print(f"  seeded {total} points; waiting for ingestion...")
    env.wait_for_ingest(total)
    time.sleep(0.3)

    print("\n=== 1. Which host used the most CPU (last hour)? ===")
    q1_busiest_host(env)

    print("\n=== 2. 95th percentile latency per page (last 2h)? ===")
    q2_p95_per_page(env)

    print("\n=== 3. Visitors per page (last 2h)? ===")
    q3_visitors_per_page(env)

    print("\n=== 4. Throughput: visitors per minute (last hour)? ===")
    q4_throughput(env)

    print("\n=== 5. Latency percentile ladder (median / p95 / p99)? ===")
    q5_percentile_ladder(env)

    print("\n=== 6. Today vs yesterday vs a week ago (visitors)? ===")
    q6_compare_days(env)

    print("\n=== 7. Busiest hourly bucket of today? ===")
    q7_busiest_hour(env)

    print("\n=== 8. Data quality: expected vs received readings today? ===")
    q8_data_quality(env)

    print("\n=== 9. Trend: growth of daily data volume? ===")
    q9_trend(env)

    print("\n=== 10. Staleness: how old is the latest reading? ===")
    q10_staleness(env)

    env.client.close()


if __name__ == "__main__":
    main()

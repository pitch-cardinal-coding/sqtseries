"""Every question people actually ask a time-series database, answered.

This is the question catalog: each section asks one question — phrased the
way a human asks it — and answers it with working sqtseries code. All of
them run against a single running service; seed data is generated
deterministically so the printed answers are reproducible.

    python3 examples/question_examples.py

The catalog:

  1.  What was the average CPU load over the last hour?
  2.  What was the peak temperature yesterday?
  3.  Show me the 99th percentile response time for the past week.
  4.  How many measurements did I record today?
  5.  Is the load rising? (this hour vs the previous hour)
  6.  Give me a per-minute curve of the last hour, not just one number.
  7.  One min/avg/max per hour for the last 6 hours.
  8.  One min/avg/max per day for the whole week.
  9.  What was the busiest hour? Busiest day?
  10. How many visitors/units were counted in the last 24 hours (sum)?
  11. When did the last reading arrive — is the sensor still alive?
  12. What are the all-time numbers for this database?
  13. The same questions over HTTP, for dashboards and curl pipelines.

One thing to know before running: the service rejects client timestamps
older than ``ingestion.reject_client_timestamp_skew_s`` (default 300s) so
the hourly rollup stays in order. This demo intentionally sends "history"
(yesterday, last week), so start the service with the guard raised:

    # config.toml
    [ingestion]
    reject_client_timestamp_skew_s = 864000   # 10 days

Each "ANSWER:" line is machine-readable (key = value) so automated tests
can assert on the printed results. Connects to 127.0.0.1 by default;
override ports with --write-port / --query-port / --http-port.
"""

import argparse
import math
import time
import urllib.request

import orjson

from sqtseries import Client

WRITE_PORT = 12501
QUERY_PORT = 12502
HTTP_PORT = 12505


def _fmt(v, spec: str = ".2f") -> str:
    """Format an aggregate result, or 'no data' when it came back empty."""
    if v is None or (isinstance(v, float) and v != v):
        return "no data"
    return f"{v:{spec}}"


def hour_ago_ns() -> int:
    """Epoch ns one hour before now (Client/ZMQ queries take nanoseconds)."""
    return time.time_ns() - 3600 * 10**9


def day_start_ns() -> int:
    """Epoch ns of UTC midnight today."""
    now_s = time.time()
    return int((now_s // 86400) * 86400 * 10**9)


def yesterday_start_ns() -> int:
    """Epoch ns of UTC midnight yesterday."""
    return day_start_ns() - 86400 * 10**9


def week_ago_ns() -> int:
    """Epoch ns seven days before now."""
    return time.time_ns() - 7 * 86400 * 10**9


def main():
    parser = argparse.ArgumentParser(
        description="Answer the classic time-series questions with sqtseries"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--write-port", type=int, default=WRITE_PORT)
    parser.add_argument("--query-port", type=int, default=QUERY_PORT)
    parser.add_argument("--http-port", type=int, default=HTTP_PORT)
    args = parser.parse_args()

    env = ExampleEnv(
        host=args.host,
        write_port=args.write_port,
        query_port=args.query_port,
        http_port=args.http_port,
    )
    client = env.client

    print("=== Seeding sample data (deterministic) ===")
    env.seed()
    print("  cpu.load     : every 5 min for 6 hours (72 points)")
    print("  temp.celsius : every 60 min for 1 day (24 points)")
    print("  latency.web  : every 30 min for 8 days (384 points)")
    print("  visitors.web : every 30 min for 8 days (384 points)")
    time.sleep(0.5)

    print("\n=== 1. What was the average CPU load over the last hour? ===")
    answer_avg_cpu_last_hour(env)

    print("\n=== 2. What was the peak temperature yesterday? ===")
    answer_peak_temp_yesterday(env)

    print("\n=== 3. Show me the 99th percentile response time for the past week ===")
    answer_p99_week(env)

    print("\n=== 4. How many measurements did I record today? ===")
    answer_count_today(env)

    print("\n=== 5. Is the load rising? (last hour vs the hour before) ===")
    extra_trend(env)

    print("\n=== 6. Per-minute average of the last hour (a curve, not one number) ===")
    extra_per_minute_curve(env)

    print("\n=== 7. One min / avg / max per hour, the last 6 hours ===")
    extra_hourly_buckets(env)

    print("\n=== 8. One min / avg / max per day, the whole week ===")
    extra_daily_buckets(env)

    print("\n=== 9. The busiest hour and busiest day (by visitor count) ===")
    extra_busiest(env)

    print("\n=== 10. How many visitors arrived in the last 24 hours? (sum) ===")
    extra_visitors_sum(env)

    print("\n=== 11. When did the last latency reading arrive? ===")
    extra_staleness(env)

    print("\n=== 12. The all-time numbers for this database ===")
    extra_alltime(env)

    print("\n=== 13. The same questions over HTTP (curl, dashboards) ===")
    http_variants(env)

    client.close()


class ExampleEnv:
    """One live instance: seed data, then fire every question at it."""

    def __init__(self, host: str, write_port: int, query_port: int, http_port: int):
        self.host = host
        self.http_port = http_port
        self.client = Client(
            ports={
                "write": write_port,
                "query": query_port,
                "subscribe": -1,
                "admin": -1,
            }
        )

    def seed(self) -> None:
        """Insert deterministic points over the last 8 days (UTC-aligned)."""
        now_s = time.time()

        # cpu.load: 72 points, every 5 minutes, covering 6 hours.
        # Values cycle deterministically through 20.0..100.0.
        for k in range(72):
            val = 20.0 + (k * 7) % 81
            self.send("cpu.load", val, now_s - (72 - k) * 5 * 60)

        # temp.celsius: 24 points, every 60 minutes, covering 24 hours.
        # Sine wave between 5.0 and 25.0 degrees.
        for k in range(24):
            val = 15.0 + 10.0 * math.sin(k / 4.0)
            self.send("temp.celsius", val, now_s - (24 - k) * 60 * 60)

        # latency.web: 384 points, every 30 minutes, covering 8 days.
        # Uniform 10.0..109.0 ms so percentiles behave like real data.
        for k in range(8 * 48):
            val = 10.0 + (k % 100)
            self.send("latency.web", val, now_s - (8 * 48 - k) * 30 * 60)

        # visitors.web: 384 points, every 30 min over 8 days, 100..180
        # per bucket, so daily and hourly sums tell a story.
        for k in range(8 * 48):
            val = float(100 + (k * 7) % 81)
            self.send("visitors.web", val, now_s - (8 * 48 - k) * 30 * 60)

    def send(self, metric: str, value: float, ts_s: float) -> None:
        self.client.write(metric, value, timestamp=ts_s)


def answer_avg_cpu_last_hour(env: ExampleEnv) -> None:
    avg = env.client.aggregate(
        "cpu.load", start=hour_ago_ns(), end=time.time_ns(), funcs=["avg"]
    )["avg"]
    print(f"  ANSWER: avg_cpu_last_hour = {_fmt(avg)}")


def answer_peak_temp_yesterday(env: ExampleEnv) -> None:
    peak = env.client.aggregate(
        "temp.celsius",
        start=yesterday_start_ns(),
        end=day_start_ns(),
        funcs=["max"],
    )["max"]
    print(f"  ANSWER: peak_temp_yesterday = {_fmt(peak)}")


def answer_p99_week(env: ExampleEnv) -> None:
    p99 = env.client.aggregate(
        "latency.web", start=week_ago_ns(), end=time.time_ns(), funcs=["p99"]
    )["p99"]
    print(f"  ANSWER: p99_latency_week = {_fmt(p99)}")


def answer_count_today(env: ExampleEnv) -> None:
    count = env.client.aggregate(
        "visitors.web",
        start=day_start_ns(),
        end=time.time_ns(),
        funcs=["count"],
    )["count"]
    print(f"  ANSWER: count_today = {_fmt(count, '.0f')}")


def extra_trend(env: ExampleEnv) -> None:
    """This hour vs the previous: is the metric moving up or down?"""
    now = time.time_ns()
    this = env.client.aggregate(
        "cpu.load", start=now - 3600 * 10**9, end=now, funcs=["avg"]
    )["avg"]
    prev = env.client.aggregate(
        "cpu.load",
        start=now - 2 * 3600 * 10**9,
        end=now - 3600 * 10**9,
        funcs=["avg"],
    )["avg"]
    if this is None or prev is None:
        print("  ANSWER: trend = no data")
        return
    direction = "rising" if this > prev else "falling"
    print(f"  ANSWER: trend = {direction}")
    print(f"  this hour: {this:.2f} , previous hour: {prev:.2f}")


def extra_per_minute_curve(env: ExampleEnv) -> None:
    rows = env.client.query(
        "cpu.load",
        start=hour_ago_ns(),
        end=time.time_ns(),
        aggregation="avg",
        interval="1m",
    )
    for row in rows[:5]:
        print(f"    bucket t={row['timestamp']:.0f} avg={row['value']:.2f}")
    print(f"    ... {len(rows)} one-minute buckets total")


def extra_hourly_buckets(env: ExampleEnv) -> None:
    for func in ("min", "avg", "max"):
        rows = env.client.query(
            "cpu.load",
            start=time.time_ns() - 6 * 3600 * 10**9,
            end=time.time_ns(),
            aggregation=func,
            interval="1h",
        )
        print(f"    {func}: " + " | ".join(f"{r['value']:.1f}" for r in rows))


def extra_daily_buckets(env: ExampleEnv) -> None:
    for func in ("min", "avg", "max"):
        rows = env.client.query(
            "temp.celsius",
            start=time.time_ns() - 7 * 86400 * 10**9,
            end=time.time_ns(),
            aggregation=func,
            interval="1d",
        )
        print(f"    {func}: " + " | ".join(f"{r['value']:.1f}" for r in rows))


def extra_busiest(env: ExampleEnv) -> None:
    """Biggest hour and biggest day by visitor count (sum in buckets)."""
    now = time.time_ns()
    hours = env.client.query(
        "visitors.web",
        start=now - 2 * 86400 * 10**9,
        end=now,
        aggregation="sum",
        interval="1h",
    )
    if not hours:
        print("  ANSWER: busiest_hour = no data")
    else:
        hour_row = max(hours, key=lambda r: r["value"])
        print(
            "  ANSWER: busiest_hour = "
            f"{time.strftime('%H:%M UTC %Y-%m-%d', time.gmtime(hour_row['timestamp']))} "
            f"({hour_row['value']:.0f} visitors)"
        )

    days = env.client.query(
        "visitors.web",
        start=now - 7 * 86400 * 10**9,
        end=now,
        aggregation="sum",
        interval="1d",
    )
    if not days:
        print("  ANSWER: busiest_day = no data")
    else:
        day_row = max(days, key=lambda r: r["value"])
        print(
            "  ANSWER: busiest_day = "
            f"{time.strftime('%Y-%m-%d', time.gmtime(day_row['timestamp']))} "
            f"({day_row['value']:.0f} visitors)"
        )


def extra_visitors_sum(env: ExampleEnv) -> None:
    now = time.time_ns()
    total = env.client.aggregate(
        "visitors.web", start=now - 86400 * 10**9, end=now, funcs=["sum"]
    )["sum"]
    print(f"  ANSWER: visitors_last_24h = {_fmt(total, '.0f')}")


def extra_staleness(env: ExampleEnv) -> None:
    """When did the last latency reading arrive — and how old is it?"""
    rows = env.client.query(
        "latency.web",
        start=time.time_ns() - 7 * 86400 * 10**9,
        end=time.time_ns(),
        limit=1,
        order="desc",
    )
    if not rows:
        print("  ANSWER: last_reading_age_s = -1")
        return
    age_s = time.time() - rows[0]["timestamp"]
    print(f"  ANSWER: no_data_for_s = {age_s:.0f}")
    print(
        f"  last reading at t={rows[0]['timestamp']:.0f} value={rows[0]['value']:.1f}"
    )


def extra_alltime(env: ExampleEnv) -> None:
    """No window at all: the database-wide history."""
    stats = env.client.aggregate("cpu.load", funcs=["count", "min", "avg", "max"])
    print("  ANSWER: alltime = " + " ".join(f"{k}={_fmt(v)}" for k, v in stats.items()))

    # same multi-agg building block, over a rolling 3-day window
    stats = env.client.aggregate(
        "latency.web",
        start=time.time_ns() - 3 * 86400 * 10**9,
        end=time.time_ns(),
        funcs=["min", "avg", "median", "p95", "p99", "max"],
    )
    print(
        "  ANSWER: stats_3days = "
        + " ".join(f"{k}={_fmt(v)}" for k, v in stats.items())
    )


def http_variants(env: ExampleEnv) -> None:
    """Round the same four questions with curl-able HTTP URLs."""
    base = f"http://{env.host}:{env.http_port}/api/v1"
    now_s = int(time.time())

    urls = [
        (
            "avg CPU load, last hour",
            f"{base}/aggregate?metric=cpu.load&start={now_s - 3600}&end={now_s}"
            f"&funcs=avg",
        ),
        (
            "peak temp, yesterday",
            f"{base}/aggregate?metric=temp.celsius"
            f"&start={now_s // 86400 * 86400 - 86400}"
            f"&end={now_s // 86400 * 86400}&funcs=max",
        ),
        (
            "p99 latency, past week",
            f"{base}/aggregate?metric=latency.web"
            f"&start={now_s - 7 * 86400}&end={now_s}&funcs=p99",
        ),
        (
            "count today",
            f"{base}/aggregate?metric=visitors.web"
            f"&start={now_s // 86400 * 86400}&end={now_s}&funcs=count",
        ),
    ]
    for label, url in urls:
        try:
            with urllib.request.urlopen(url) as resp:
                body = orjson.loads(resp.read())
            aggs = body.get("aggregations", {})
            print(f"    {label}: {aggs}")
        except Exception as exc:
            print(f"    {label}: HTTP not available ({exc})")
    print('    (equivalent curl: curl "' + urls[0][1] + '")')


if __name__ == "__main__":
    main()

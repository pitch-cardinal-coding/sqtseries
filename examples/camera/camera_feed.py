"""Pump a camera-metrics WebSocket feed into sqtseries and answer every
question the accumulated data can answer — fully self-contained.

Two parts:

1. **Pump** (default): subscribe to a metrics WebSocket (e.g. the included
   ``overlay_server.py`` simulator) and write every reading into a running
   sqtseries instance over plain HTTP. It only needs Python's standard
   library plus ``websockets`` — it never imports sqtseries, so it works
   from any Python environment (different venv, different machine).

2. **Ask** (``--ask``): query sqtseries back and answer the full question
   catalog — traffic, vehicles, battery, CPU, correlations, health, and
   capacity planning. Run this after data has accumulated.

Self-contained demo (three terminals):

    # terminal 1 — the data source (this repo's simulator, random port)
    python3 examples/camera/overlay_server.py
    #   note the printed port (e.g. ws://localhost:58547/ws/metrics)

    # terminal 2 — sqtseries (any instance; default ports fine)
    sqtseries --config config.toml run

    # terminal 3 — pump readings into sqtseries
    python3 examples/camera/camera_feed.py --ws ws://localhost:58547/ws/metrics
    #   let it run for a few minutes

    # terminal 4 (anytime) — answer every question from the stored data
    python3 examples/camera/camera_feed.py --ask

Each incoming message is flattened into numeric measurements tagged with
``camera_id`` (sqtseries stores flat ``metric + value + tags``):

    traffic.people.current        21      camera_id=test_cam_001
    traffic.people.total          237     camera_id=test_cam_001
    traffic.vehicles.current      9       camera_id=test_cam_001
    traffic.vehicles.total        100     camera_id=test_cam_001
    traffic.people.last_minute    21      camera_id=test_cam_001
    traffic.cars.last_minute      9       camera_id=test_cam_001
    system.battery.percent        84.3    camera_id=test_cam_001
    system.battery.temperature    28.5    camera_id=test_cam_001
    system.cpu.usage              41.3    camera_id=test_cam_001
    system.cpu.temperature        62.4    camera_id=test_cam_001
    status.detection_overlay      1       camera_id=test_cam_001   (bool -> 1/0)

String fields (batteryHealth, stats_footer_text, overlay URLs) cannot be
stored as numeric measurements and are skipped.

Usage:
    pip install websockets          # only extra dependency (pump mode)
    python3 examples/camera/camera_feed.py --ws ws://localhost:58547/ws/metrics
    python3 examples/camera/camera_feed.py --ask
    python3 examples/camera/camera_feed.py --ask --hours 24
    python3 examples/camera/camera_feed.py --host 192.168.1.10 --http-port 12505
"""

import argparse
import datetime
import json
import statistics
import urllib.error
import urllib.request

try:
    import websockets
except ImportError:
    websockets = None

DEFAULT_WS = "ws://localhost:30080/ws/metrics"
DEFAULT_HTTP_HOST = "127.0.0.1"
DEFAULT_HTTP_PORT = 12505

# (metric name, dotted path into the JSON message)
FIELD_PATHS = [
    ("traffic.people.current", "current_visible_people"),
    ("traffic.people.total", "total_detected_people"),
    ("traffic.vehicles.current", "current_visible_cars"),
    ("traffic.vehicles.total", "total_detected_vehicles"),
    ("traffic.people.last_minute", "people_last_minute"),
    ("traffic.cars.last_minute", "cars_last_minute"),
    ("system.battery.percent", "system_metrics.batteryPercent"),
    ("system.battery.temperature", "system_metrics.batteryTemperature"),
    ("system.cpu.usage", "system_metrics.cpuUsagePercent"),
    ("system.cpu.temperature", "system_metrics.cpuTemperature"),
]

# (metric name, JSON boolean field -> stored as 1.0 / 0.0)
BOOL_PATHS = [
    ("status.detection_overlay", "detection_overlay_enabled"),
]

NOW_S = int(datetime.datetime.now(datetime.UTC).timestamp())


def _http_get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=15) as resp:
        return json.loads(resp.read())


def _ns(seconds: int) -> int:
    return seconds * 10**9


def agg(
    base: str, metric: str, funcs: list[str], start_s: int, end_s: int
) -> dict[str, float]:
    """Call /api/v1/aggregate; returns {func: value} (may be nan/None)."""
    funcs_str = ",".join(funcs)
    url = (
        f"{base}/api/v1/aggregate?metric={metric}&start={start_s}"
        f"&end={end_s}&funcs={funcs_str}"
    )
    return _http_get(url).get("aggregations", {})


def series(
    base: str, metric: str, start_s: int, end_s: int, interval: str, func: str = "avg"
) -> list[tuple[int, float]]:
    """Call /api/v1/read with interval; returns [(bucket_ts_s, value)]."""
    url = (
        f"{base}/api/v1/read?metric={metric}&start={start_s}&end={end_s}"
        f"&aggregation={func}&interval={interval}"
    )
    data = _http_get(url).get("data", [])
    return [(int(row["timestamp"]), float(row["value"])) for row in data]


def latest(base: str, metric: str, start_s: int) -> dict | None:
    """Newest reading of a metric in [start_s, now]; None if no data."""
    url = (
        f"{base}/api/v1/read?metric={metric}&start={start_s}"
        f"&end={NOW_S}&limit=1&order=desc"
    )
    data = _http_get(url).get("data", [])
    if not data:
        return None
    return {"timestamp": int(data[0]["timestamp"]), "value": float(data[0]["value"])}


def fmt_v(v: float | None, unit: str = "", nd: int = 1) -> str:
    if v is None or v != v:  # None or NaN
        return "no data"
    return f"{v:.{nd}f}{unit}"


def q_traffic_current(base: str, start_s: int, end_s: int) -> None:
    """Q1. Averages & extremes: how many people are visible, typically?"""
    print("\n[Q1] Traffic levels — visible people")
    st = agg(
        base,
        "traffic.people.current",
        ["avg", "min", "max", "median", "p95", "p99"],
        start_s,
        end_s,
    )
    print(
        f"  avg={fmt_v(st.get('avg'))}  min={fmt_v(st.get('min'))}  "
        f"max={fmt_v(st.get('max'))}  median={fmt_v(st.get('median'))}"
    )
    print(f"  p95={fmt_v(st.get('p95'))}  p99={fmt_v(st.get('p99'))}")
    st = agg(base, "traffic.vehicles.current", ["avg", "max"], start_s, end_s)
    print(f"  cars: avg={fmt_v(st.get('avg'))}  max={fmt_v(st.get('max'))}")


def q_busiest_hour_day(base: str, start_s: int, end_s: int) -> None:
    """Q2. When is the busiest hour and the busiest day?"""
    print(
        "\n[Q2] Busiest hour and busiest day — footfall rate "
        "(people_last_minute, summed)"
    )
    hours = series(base, "traffic.people.last_minute", start_s, end_s, "1h", "sum")
    if not hours:
        print("  no hourly data yet")
        return
    if len(hours) < 2:
        print(f"  only {len(hours)} hour(s) of data so far — need 2+ to compare hours")
    else:
        hh = max(hours, key=lambda r: r[1])
        print(
            f"  busiest hour: {datetime.datetime.fromtimestamp(hh[0], datetime.UTC):%Y-%m-%d %H:%M} UTC "
            f"({hh[1]:.0f} people that hour)"
        )
        quietest = min(hours, key=lambda r: r[1])
        print(
            f"  quietest hour: {datetime.datetime.fromtimestamp(quietest[0], datetime.UTC):%Y-%m-%d %H:%M} UTC "
            f"({quietest[1]:.0f} people)"
        )
    days = series(base, "traffic.people.last_minute", start_s, end_s, "1d", "sum")
    if days:
        dd = max(days, key=lambda r: r[1])
        print(
            f"  busiest day: {datetime.datetime.fromtimestamp(dd[0], datetime.UTC):%Y-%m-%d} "
            f"({dd[1]:.0f} people)"
        )
        if len(days) < 2:
            print("  (only 1 day of data so far — day ranking needs 2+)")


def q_footfall_total(base: str, start_s: int, end_s: int) -> None:
    """Q3. Total footfall over the window (sum of the per-minute rate)."""
    print("\n[Q3] Total footfall")
    st = agg(base, "traffic.people.last_minute", ["sum", "count"], start_s, end_s)
    total = st.get("sum")
    print(f"  people past the camera: {fmt_v(total, '', 0)}")
    st = agg(base, "traffic.cars.last_minute", ["sum"], start_s, end_s)
    print(f"  cars past the camera:   {fmt_v(st.get('sum'), '', 0)}")


def q_trend(base: str, start_s: int, end_s: int) -> None:
    """Q4. Day-over-day trend: is footfall growing?"""
    print("\n[Q4] Day-over-day trend — daily footfall sums")
    days = series(base, "traffic.people.last_minute", start_s, end_s, "1d", "sum")
    if len(days) < 2:
        print("  not enough days yet (need 2+)")
        return
    vals = [r[1] for r in days]
    print(
        "  daily: "
        + " | ".join(
            f"{datetime.datetime.fromtimestamp(r[0], datetime.UTC):%m-%d}={r[1]:.0f}"
            for r in days
        )
    )
    delta = vals[-1] - vals[0]
    pct = delta / vals[0] * 100 if vals[0] else 0
    direction = "growing" if delta > 0 else ("shrinking" if delta < 0 else "flat")
    print(
        f"  trend: {direction} (first day {vals[0]:.0f} -> last "
        f"{vals[-1]:.0f}, {pct:+.0f}%)"
    )


def q_turnover(base: str, start_s: int, end_s: int) -> None:
    """Q5. Turnover: how many people pass through vs how many are visible?"""
    print("\n[Q5] Turnover — detected total vs currently visible")
    st = agg(base, "traffic.people.total", ["max", "last"], start_s, end_s)
    cur = agg(base, "traffic.people.current", ["avg"], start_s, end_s)
    total = st.get("max")
    avg_cur = cur.get("avg")
    if total and avg_cur:
        print(
            f"  {fmt_v(total, '', 0)} people detected in total; on average "
            f"{fmt_v(avg_cur)} visible -> pass-through "
            f"{total / avg_cur:.0f}x the visible count"
        )
    st = agg(base, "traffic.vehicles.total", ["max"], start_s, end_s)
    cur = agg(base, "traffic.vehicles.current", ["avg"], start_s, end_s)
    if st.get("max") and cur.get("avg"):
        print(
            f"  vehicles: {fmt_v(st.get('max'), '', 0)} total vs "
            f"{fmt_v(cur.get('avg'))} avg visible"
        )


def q_percentiles(base: str, start_s: int, end_s: int) -> None:
    """Q6. Capacity planning: percentile ladder for peak load."""
    print("\n[Q6] Capacity planning — percentile ladder")
    for metric, label in (
        ("traffic.people.current", "visible people"),
        ("traffic.cars.last_minute", "cars / min"),
    ):
        st = agg(base, metric, ["median", "p95", "p99"], start_s, end_s)
        print(
            f"  {label}: median={fmt_v(st.get('median'))}  "
            f"p95={fmt_v(st.get('p95'))}  p99={fmt_v(st.get('p99'))}"
        )


def q_anomaly(base: str, start_s: int, end_s: int) -> None:
    """Q7. Anomaly vs the same hour over the previous days."""
    print("\n[Q7] Anomaly detection — current hour vs the same hour, previous days")
    now = datetime.datetime.now(datetime.UTC)
    cur_hour_start = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    cur = agg(base, "traffic.people.current", ["avg"], cur_hour_start, NOW_S).get("avg")
    prev_same: list[float] = []
    for day in range(1, 8):
        start = cur_hour_start - day * 86400
        v = agg(base, "traffic.people.current", ["avg"], start, start + 3600).get("avg")
        if v is not None and v == v:
            prev_same.append(v)
    if not prev_same:
        print("  not enough history yet (need data for previous days)")
        return
    baseline = statistics.mean(prev_same)
    hi = (
        statistics.mean(prev_same) + 2 * statistics.stdev(prev_same)
        if len(prev_same) > 1
        else baseline
    )
    if cur is not None and cur == cur:
        flag = "SUSPICIOUS" if cur > hi else "normal"
        print(
            f"  current hour avg {fmt_v(cur)} vs baseline "
            f"{fmt_v(baseline)} (prev days same-hour "
            f"{[round(v) for v in prev_same]}) -> {flag}"
        )


def q_battery(base: str, start_s: int, end_s: int) -> None:
    """Q8. Battery health: drain rate and projected time-to-empty."""
    print("\n[Q8] Battery — drain rate and life projection")
    url = (
        f"{base}/api/v1/read?metric=system.battery.percent&start={start_s}"
        f"&end={end_s}&limit=1&order=asc"
    )
    data = _http_get(url).get("data", [])
    if not data:
        print("  no battery data yet")
        return
    first_ts = int(data[0]["timestamp"])
    first_val = float(data[0]["value"])
    last = latest(base, "system.battery.percent", start_s)
    if last is None or last["timestamp"] == first_ts:
        print("  not enough battery history yet")
        return
    hours = (last["timestamp"] - first_ts) / 3600
    drain = (first_val - last["value"]) / hours if hours else 0
    print(
        f"  battery {fmt_v(first_val)}% -> {fmt_v(last['value'])}% "
        f"over {hours:.1f}h = {drain:.2f}%/h"
    )
    if drain > 0:
        hours_left = last["value"] / drain
        when = datetime.datetime.fromtimestamp(
            last["timestamp"] + hours_left * 3600, datetime.UTC
        )
        print(f"  projected empty in {hours_left:.0f}h (~{when:%Y-%m-%d %H:%M} UTC)")
    elif drain < 0:
        print("  battery gained charge (recharged or replaced) — no drain to project")
    else:
        print("  no net drain over this window")
    st = agg(base, "system.battery.temperature", ["min", "max", "avg"], start_s, end_s)
    print(
        f"  battery temp: min={fmt_v(st.get('min'), '°C')}  "
        f"max={fmt_v(st.get('max'), '°C')}  avg={fmt_v(st.get('avg'), '°C')}"
    )


def q_cpu(base: str, start_s: int, end_s: int) -> None:
    """Q9. CPU and thermal profile."""
    print("\n[Q9] CPU & thermal")
    st = agg(base, "system.cpu.usage", ["avg", "max"], start_s, end_s)
    print(
        f"  cpu usage: avg={fmt_v(st.get('avg'), '%')}  max={fmt_v(st.get('max'), '%')}"
    )
    st = agg(base, "system.cpu.temperature", ["min", "max", "avg"], start_s, end_s)
    print(
        f"  cpu temp: min={fmt_v(st.get('min'), '°C')}  "
        f"max={fmt_v(st.get('max'), '°C')}  avg={fmt_v(st.get('avg'), '°C')}"
    )
    hot = agg(base, "system.cpu.temperature", ["max"], start_s, end_s)
    if hot.get("max") and hot["max"] > 70:
        print("  WARNING: sustained hot CPU (max > 70°C)")


def q_correlation(base: str, start_s: int, end_s: int) -> None:
    """Q10. Does heavy footfall correlate with higher CPU usage?"""
    print("\n[Q10] Correlation — footfall vs CPU usage (per hour)")
    people = series(base, "traffic.people.last_minute", start_s, end_s, "1h")
    cpu = series(base, "system.cpu.usage", start_s, end_s, "1h")
    by_hour: dict[int, tuple[float, float]] = {}
    for ts, v in people:
        by_hour.setdefault(ts, [0.0, 0.0])[0] += v
    for ts, v in cpu:
        by_hour.setdefault(ts, [0.0, 0.0])[1] += v
    pairs = [(p, c) for ts, (p, c) in sorted(by_hour.items()) if p and c]
    if len(pairs) < 3:
        print("  not enough hourly data yet (need 3+ hours)")
        return
    xs = [p for p, _ in pairs]
    ys = [c for _, c in pairs]
    try:
        corr = statistics.correlation(xs, ys)
    except statistics.StatisticsError:
        corr = 0.0
    print(
        f"  pearson r={corr:+.2f} over {len(pairs)} hours "
        "(-1..+1; positive = busy hours also load the CPU)"
    )


def q_battery_vs_busy(base: str, start_s: int, end_s: int) -> None:
    """Q11. Does battery drain faster on busy days?"""
    print("\n[Q11] Battery drain on busy vs quiet days")
    days = series(base, "traffic.people.last_minute", start_s, end_s, "1d", "sum")
    bat = series(base, "system.battery.percent", start_s, end_s, "1d", "first")
    if len(days) < 2 or len(bat) < 2:
        print("  not enough daily data yet (need 2+ days)")
        return
    b0, b1 = bat[0][1], bat[-1][1]
    print(f"  battery {b0:.1f}% -> {b1:.1f}% over the window")


def q_staleness(base: str, start_s: int, end_s: int) -> None:
    """Q12. Is the feed alive? How old is the newest reading?"""
    print("\n[Q12] Feed health — staleness")
    for metric, label in (
        ("traffic.people.current", "traffic"),
        ("system.battery.percent", "battery"),
    ):
        row = latest(base, metric, start_s)
        if row is None:
            print(f"  {label}: no data")
            continue
        age = NOW_S - row["timestamp"]
        print(f"  {label}: last reading {age}s ago (value {fmt_v(row['value'])})")


def q_uptime(base: str, start_s: int, end_s: int) -> None:
    """Q13. Expected vs actual readings — feed uptime."""
    print("\n[Q13] Feed uptime — expected vs received readings")
    url = (
        f"{base}/api/v1/read?metric=traffic.people.current&start={start_s}"
        f"&end={end_s}&limit=1&order=asc"
    )
    data = _http_get(url).get("data", [])
    if not data:
        print("  no data")
        return
    first_ts = int(data[0]["timestamp"])
    st = agg(base, "traffic.people.current", ["count"], first_ts, end_s)
    got = st.get("count")
    if got is None:
        print("  no data")
        return
    # Measure from the first stored reading to now, not the whole window,
    # so a feed that started mid-window is judged fairly.
    span_h = max((end_s - first_ts) / 3600, 1 / 3600)
    expected = int(span_h * 3600 * 4)  # overlay pushes ~4 messages/sec
    pct = got / expected * 100 if expected else 0
    print(
        f"  received {got:.0f} of ~{expected} expected readings over "
        f"{span_h:.1f}h ({pct:.0f}%)"
    )


def q_multicam(base: str, start_s: int, end_s: int) -> None:
    """Q14. Per-camera breakdown — which camera is busiest / hottest?"""
    print("\n[Q14] Per-camera breakdown")
    print("  (the wire API queries a whole metric; per-camera splits need")
    print("   the embedded API with --db PATH — see question_examples_advanced.py)")
    # Without per-series access we can still report per-camera values the
    # feed wrote with different camera_id tags IF we query by series — the
    # HTTP API can't, so we list what we can: total series count via stats.
    try:
        stats = _http_get(f"{base}/api/v1/stats")
        print(
            f"  series stored: {stats.get('series')}  metrics: {stats.get('metrics')}"
        )
    except Exception:
        print("  (stats endpoint unreachable)")


def q_monthly_projection(base: str, start_s: int, end_s: int) -> None:
    """Q15. Capacity planning: monthly footfall projection."""
    print("\n[Q15] Monthly projection")
    st = agg(base, "traffic.people.last_minute", ["sum"], start_s, NOW_S)
    total = st.get("sum")
    if total is None or total != total:
        print("  no footfall data yet")
        return
    days = max((NOW_S - start_s) / 86400, 1)
    per_day = total / days
    print(
        f"  {fmt_v(total, '', 0)} people over {days:.1f} days = "
        f"{per_day:.0f}/day -> ~{per_day * 30:.0f}/month "
        f"({per_day * 365:.0f}/year)"
    )


def q_storage(base: str, start_s: int, end_s: int) -> None:
    """Q16. Storage growth estimate (points/day, ~100 bytes each)."""
    print("\n[Q16] Storage growth")
    st = agg(base, "traffic.people.current", ["count"], start_s, NOW_S)
    got = st.get("count")
    if got is None:
        print("  no data")
        return
    days = max((NOW_S - start_s) / 86400, 1)
    per_day = got / days * 11  # 11 metrics per message
    mb_day = per_day * 100 / 1024 / 1024
    print(
        f"  ~{per_day:.0f} points/day (~{mb_day:.1f} MB/day at ~100 "
        f"bytes/pt) -> ~{mb_day * 30:.0f} MB/month"
    )


def q_overlay(base: str, start_s: int, end_s: int) -> None:
    """Q17. Overlay stream availability — fraction of time enabled."""
    print("\n[Q17] Overlay availability")
    st = agg(base, "status.detection_overlay", ["avg", "count"], start_s, NOW_S)
    avg = st.get("avg")
    if avg is None or avg != avg:
        print("  no overlay data yet")
        return
    print(
        f"  detection overlay enabled {avg * 100:.1f}% of the time "
        f"({st.get('count'):.0f} readings)"
    )


ALL_QUESTIONS = [
    q_traffic_current,
    q_busiest_hour_day,
    q_footfall_total,
    q_trend,
    q_turnover,
    q_percentiles,
    q_anomaly,
    q_battery,
    q_cpu,
    q_correlation,
    q_battery_vs_busy,
    q_staleness,
    q_uptime,
    q_multicam,
    q_monthly_projection,
    q_storage,
    q_overlay,
]


def ask(base: str, hours: float) -> None:
    """Run every question against [now - hours, now]."""
    end_s = NOW_S
    start_s = end_s - int(hours * 3600)
    print(
        f"Answering all questions over the last {hours:.0f}h "
        f"({datetime.datetime.fromtimestamp(start_s, datetime.UTC):%Y-%m-%d %H:%M} "
        f"-> now UTC) against {base}"
    )
    for q in ALL_QUESTIONS:
        try:
            q(base, start_s, end_s)
        except Exception as exc:
            print(f"\n[Q{q_num(q)}] error: {exc}")
    print("\nDone.")


def q_num(fn) -> int:
    return ALL_QUESTIONS.index(fn) + 1


def _get_path(msg: dict, path: str):
    node = msg
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def _source_ts(msg: dict) -> float | None:
    """Epoch seconds from the message's generated_at (ISO-8601 with offset)."""
    raw = msg.get("generated_at")
    if not raw:
        return None
    try:
        return datetime.datetime.fromisoformat(raw).timestamp()
    except ValueError:
        return None


def flatten(msg: dict) -> list[dict]:
    """Turn one WebSocket message into sqtseries write points (with tags)."""
    camera_id = msg.get("camera_id", "unknown")
    ts = _source_ts(msg)
    points: list[dict] = []
    for metric, path in FIELD_PATHS:
        value = _get_path(msg, path)
        if value is None:
            continue
        point = {
            "metric": metric,
            "value": float(value),
            "tags": {"camera_id": camera_id},
        }
        if ts is not None:
            point["timestamp"] = ts  # epoch seconds; also fine to omit
        points.append(point)
    for metric, path in BOOL_PATHS:
        value = _get_path(msg, path)
        if value is None:
            continue
        point = {
            "metric": metric,
            "value": 1.0 if value else 0.0,
            "tags": {"camera_id": camera_id},
        }
        if ts is not None:
            point["timestamp"] = ts
        points.append(point)
    return points


def post_batch(base_url: str, points: list[dict]) -> int:
    """POST one array of points to /api/v1/write; returns HTTP status."""
    req = urllib.request.Request(
        f"{base_url}/api/v1/write",
        data=json.dumps(points).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
            return resp.status
    except urllib.error.HTTPError as exc:
        print(f"  HTTP {exc.code}: {exc.read()[:200]!r}")
        return exc.code
    except urllib.error.URLError as exc:
        print(f"  write failed (is sqtseries running?): {exc}")
        return 0


def main():
    parser = argparse.ArgumentParser(
        description="Stream camera metrics from a WebSocket into sqtseries "
        "and answer questions about them"
    )
    parser.add_argument(
        "--ws",
        default=DEFAULT_WS,
        help="metrics WebSocket URL (default "
        f"{DEFAULT_WS!r}, which assumes the overlay was started with "
        "--port 30080; the overlay defaults to a random free port, so pass "
        "the URL it prints, e.g. --ws ws://localhost:PORT/ws/metrics)",
    )
    parser.add_argument("--host", default=DEFAULT_HTTP_HOST, help="sqtseries HTTP host")
    parser.add_argument(
        "--http-port",
        type=int,
        default=DEFAULT_HTTP_PORT,
        help="sqtseries HTTP port (default 12505)",
    )
    parser.add_argument(
        "--ask",
        action="store_true",
        help="answer all questions from stored data instead of pumping",
    )
    parser.add_argument(
        "--hours", type=float, default=24, help="window for --ask (default 24h)"
    )
    parser.add_argument(
        "--once", action="store_true", help="pump one message and exit (for testing)"
    )
    args = parser.parse_args()

    base_url = f"http://{args.host}:{args.http_port}"

    if args.ask:
        ask(base_url, args.hours)
        return

    if websockets is None:
        raise SystemExit(
            "pip install websockets   (only needed for pump mode)"
        ) from None

    print(f"subscribing to {args.ws}")
    print(f"writing to {base_url}/api/v1/write")

    async def run():
        async with websockets.connect(args.ws, open_timeout=15) as ws:
            print("connected; pumping readings ...")
            while True:
                raw = await ws.recv()
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                points = flatten(msg)
                if points:
                    post_batch(base_url, points)
                if args.once:
                    print(f"one message -> {len(points)} points")
                    return

    import asyncio

    asyncio.run(run())


if __name__ == "__main__":
    main()

"""Tags: what they are, why to use them, and how to query per-tag.

A runnable walkthrough of sqtseries tags (the key=value dimensions on a
measurement). It demonstrates:

  1. writing the same metric for several hosts / rooms (tags = dimensions)
  2. series identity: (metric, tags) is one series — different tag sets are
     separate series, the same set accumulates
  3. querying the WHOLE metric over the wire (merged across all tag sets)
  4. per-tag breakdowns via the embedded API (--db PATH)
  5. tag validation (non-string tag values are rejected)
  6. a cardinality warning (what NOT to tag)

Usage:
    python3 examples/tags_examples.py                      # wire-only
    python3 examples/tags_examples.py --db /path/db.sqlite # + per-tag reads

Connects to default ports on 127.0.0.1 unless overridden.
"""

import argparse
import json
import time
import urllib.error
import urllib.request

from sqtseries import Client


def seed(c: Client) -> None:
    """Write a small, bounded tagged dataset (6 series)."""
    # cpu per host + environment
    c.write("demo.cpu.usage", 0.72, {"host": "web1", "env": "prod"})
    c.write("demo.cpu.usage", 0.65, {"host": "web2", "env": "prod"})
    c.write("demo.cpu.usage", 0.81, {"host": "web3", "env": "staging"})
    # temperature per sensor location
    for room, floor, temp in (
        ("lobby", "1", 21.0),
        ("kitchen", "1", 24.5),
        ("basement", "-1", 18.2),
    ):
        c.write("demo.temp.celsius", temp, {"room": room, "floor": floor})


def whole_metric(c: Client) -> None:
    """The wire protocol queries a whole metric — all tag sets merged."""
    stats = c.aggregate("demo.cpu.usage", funcs=["avg", "max", "count"])
    print(
        "  demo.cpu.usage (all hosts merged): "
        f"avg={stats.get('avg'):.2f} max={stats.get('max'):.2f} "
        f"count={stats.get('count'):.0f}"
    )
    stats = c.aggregate("demo.temp.celsius", funcs=["min", "max"])
    print(
        "  demo.temp.celsius (all rooms merged): "
        f"min={stats.get('min'):.1f} max={stats.get('max'):.1f}°C"
    )


def per_tag(db: str) -> None:
    """Embedded per-tag breakdown: series_ids -> tags -> query each group."""
    from sqtseries.engine import StorageEngine, create_sqlite_engine, initialize_schema

    engine = create_sqlite_engine(db)
    initialize_schema(engine)
    store = StorageEngine(engine)
    now_ns = time.time_ns()
    day_ns = 24 * 3600 * 10**9

    for metric, tag_key in (("demo.cpu.usage", "host"), ("demo.temp.celsius", "room")):
        by_tag: dict[str, list[int]] = {}
        for sid in store.series_ids_for_metric(metric):
            _, tags_json = store.get_series_meta(sid)
            tags = json.loads(tags_json) if tags_json else {}
            by_tag.setdefault(tags.get(tag_key, "untagged"), []).append(sid)
        print(f"  {metric} per-{tag_key}:")
        for tag, sids in sorted(by_tag.items()):
            rows = list(
                store.query_time_range(
                    series_ids=sids, start_ns=now_ns - day_ns, end_ns=now_ns
                )
            )
            avg = sum(v for _, v in rows) / len(rows) if rows else 0.0
            print(f"    {tag:<8} {len(rows):>2} points  avg {avg:.2f}")
    engine.dispose()


def show_validation(http_port: int) -> None:
    """Non-string tag values are rejected (HTTP gives a clear 400)."""
    url = f"http://127.0.0.1:{http_port}/api/v1/write"
    body = json.dumps(
        {"metric": "demo.bad", "value": 1.0, "tags": {"cpu_count": 8}}
    ).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"  unexpected 200: {resp.read()!r}")
    except urllib.error.HTTPError as exc:
        print(
            f"  tags {{'cpu_count': 8}} -> HTTP {exc.code} {exc.read().decode().strip()}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Walk through sqtseries tags: why, how, and per-tag queries"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--write-port", type=int, default=12501)
    parser.add_argument("--query-port", type=int, default=12502)
    parser.add_argument("--http-port", type=int, default=12505)
    parser.add_argument(
        "--db",
        help="database path for embedded per-tag reads (e.g. ~/.sqtseries/data/db.sqlite)",
    )
    args = parser.parse_args()

    c = Client(ports={"write": args.write_port, "query": args.query_port})
    try:
        print("=== 1. Writing tagged measurements (tags = dimensions) ===")
        seed(c)
        time.sleep(0.3)
        print("  demo.cpu.usage   {host, env} for web1/web2/web3")
        print("  demo.temp.celsius {room, floor} for lobby/kitchen/basement")

        print("\n=== 2. Series identity: (metric, tags) is one series ===")
        print("  web1+web2+web3 are THREE separate cpu.usage series;")
        print("  writing demo.cpu.usage {host:web1, env:prod} again later")
        print("  appends to the SAME series (same series_id).")

        print("\n=== 3. Whole-metric query over the wire (merged) ===")
        whole_metric(c)

        if args.db:
            print("\n=== 4. Per-tag breakdown (embedded API, --db) ===")
            per_tag(args.db)
        else:
            print("\n=== 4. Per-tag breakdown ===")
            print("  pass --db /path/to/db.sqlite to also query per host/room")

        print("\n=== 5. Tag validation ===")
        show_validation(args.http_port)
        print("  string values are fine: {'cpu_count': '8'} would be accepted")

        print("\n=== 6. Cardinality warning ===")
        print("  tag DIMENSIONS you slice by (host, room, env), never unique")
        print("  ids (request_id, session_id, timestamp): every distinct tag set")
        print("  is a new series, so high-cardinality tags multiply series and")
        print("  slow per-series queries. Put the reading in 'value', not tags.")
    finally:
        c.close()


if __name__ == "__main__":
    main()

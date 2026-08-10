"""Query sqtseries via ZMQ REQ or HTTP — every query type demonstrated.

This script runs through all query capabilities the database supports:
raw reads, every aggregation function (avg to p99), downsampling with
intervals, whole-window multi-aggregation, and a note on gap filling
(embedded API only, no wire parameter).

It seeds the database with sample data first so every example returns
real results. Run it against a running sqtseries service:

    python3 examples/query_examples.py

Connects to default ports on 127.0.0.1. Override with --host.
"""

import argparse
import time

import orjson
import zmq

QUERY_PORT = 12502
HTTP_PORT = 12505


def main():
    parser = argparse.ArgumentParser(
        description="Run every query type against sqtseries"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--write-port", type=int, default=12501)
    parser.add_argument("--query-port", type=int, default=QUERY_PORT)
    parser.add_argument("--http-port", type=int, default=HTTP_PORT)
    args = parser.parse_args()

    host = args.host
    query_ep = f"tcp://{host}:{args.query_port}"
    http_base = f"http://{host}:{args.http_port}"

    print("=== Seeding sample data ===")
    seed_data(host, args.write_port)
    time.sleep(0.5)

    print("\n=== 1. Raw query (last hour, no aggregation) ===")
    raw_query(query_ep, "demo.cpu")

    print("\n=== 2. Raw query with limit and descending order ===")
    raw_query_limit(query_ep, "demo.cpu", limit=5, order="desc")

    print("\n=== 3. Whole-window aggregation (one value over entire window) ===")
    for func in [
        "avg",
        "sum",
        "min",
        "max",
        "count",
        "first",
        "last",
        "median",
        "p95",
        "p99",
    ]:
        whole_window_agg(query_ep, "demo.cpu", func)

    print("\n=== 4. Downsampling (aggregation per interval bucket) ===")
    downsample_query(query_ep, "demo.cpu", "avg", "1m")

    print("\n=== 5. Multi-aggregation (several functions in one request) ===")
    multi_agg(query_ep, "demo.cpu", ["avg", "min", "max", "p95"])

    print("\n=== 6. HTTP query (same capabilities, different transport) ===")
    http_query(http_base, "demo.cpu")

    print("\n=== 7. HTTP aggregate endpoint ===")
    http_aggregate(http_base, "demo.cpu", "avg,min,max,p95")

    print("\n=== 8. Gap filling (linear interpolation of small gaps) ===")
    print("Gap filling is available via the embedded Python API (TimeSeriesDB.query)")
    print("with fill_gaps_ns parameter. See docs/queries.html for details.")


def zmq_req(endpoint: str, payload: dict) -> dict:
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 500)
    sock.connect(endpoint)
    sock.send(orjson.dumps(payload))
    reply = orjson.loads(sock.recv())
    sock.close(linger=0)
    ctx.term()
    return reply


def seed_data(host: str, port: int = 12501):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    sock.setsockopt(zmq.LINGER, 2000)
    sock.connect(f"tcp://{host}:{port}")
    now = time.time()
    for i in range(120):
        ts = now - (120 - i) * 30
        point = {"metric": "demo.cpu", "value": 50 + (i % 20) * 2.5, "timestamp": ts}
        sock.send(orjson.dumps(point))
    # LINGER=2000 (not 0): flush the queued messages on close instead of
    # dropping them — fire-and-forget PUSH loses everything with linger=0.
    sock.close(linger=2000)
    ctx.term()
    print("  Seeded 120 data points (demo.cpu, 30s intervals over 1 hour)")


def raw_query(endpoint: str, metric: str):
    now_ns = time.time_ns()
    reply = zmq_req(
        endpoint,
        {
            "type": "query",
            "metric": metric,
            "start": now_ns - 3600 * 10**9,
            "end": now_ns,
        },
    )
    data = reply.get("data", [])
    print(f"  {len(data)} data points returned")
    if data:
        print(f"  First: ts={data[0]['timestamp']:.0f} value={data[0]['value']}")
        print(f"  Last:  ts={data[-1]['timestamp']:.0f} value={data[-1]['value']}")


def raw_query_limit(endpoint: str, metric: str, limit: int, order: str):
    now_ns = time.time_ns()
    reply = zmq_req(
        endpoint,
        {
            "type": "query",
            "metric": metric,
            "start": now_ns - 3600 * 10**9,
            "end": now_ns,
            "limit": limit,
            "order": order,
        },
    )
    data = reply.get("data", [])
    print(f"  Latest {limit} points ({order}): {[p['value'] for p in data]}")


def whole_window_agg(endpoint: str, metric: str, func: str):
    now_ns = time.time_ns()
    reply = zmq_req(
        endpoint,
        {
            "type": "query",
            "metric": metric,
            "aggregation": func,
            "start": now_ns - 3600 * 10**9,
            "end": now_ns,
        },
    )
    data = reply.get("data", [])
    if data:
        print(f"  {func:>6s}: {data[0]['value']:.2f}")


def downsample_query(endpoint: str, metric: str, func: str, interval: str):
    now_ns = time.time_ns()
    reply = zmq_req(
        endpoint,
        {
            "type": "query",
            "metric": metric,
            "aggregation": func,
            "interval": interval,
            "start": now_ns - 3600 * 10**9,
            "end": now_ns,
        },
    )
    data = reply.get("data", [])
    print(f"  {len(data)} buckets ({interval} interval, {func}):")
    for row in data[:5]:
        print(f"    bucket={row['timestamp']:.0f}  {func}={row['value']:.2f}")
    if len(data) > 5:
        print(f"    ... and {len(data) - 5} more buckets")


def multi_agg(endpoint: str, metric: str, funcs: list[str]):
    now_ns = time.time_ns()
    reply = zmq_req(
        endpoint,
        {
            "type": "query",
            "metric": metric,
            "aggregations": ",".join(funcs),
            "start": now_ns - 3600 * 10**9,
            "end": now_ns,
        },
    )
    data = reply.get("data", {})
    print("  Multi-aggregation over 1 hour window:")
    for f in funcs:
        print(f"    {f:>6s}: {data.get(f, 'N/A'):.2f}")


def http_query(base: str, metric: str):
    import urllib.request

    now = int(time.time())
    url = f"{base}/api/v1/read?metric={metric}&start={now - 3600}&end={now}&limit=3"
    with urllib.request.urlopen(url) as resp:
        body = orjson.loads(resp.read())
    data = body.get("data", [])
    print(f"  HTTP read returned {len(data)} points (limit=3)")


def http_aggregate(base: str, metric: str, funcs: str):
    import urllib.request

    now = int(time.time())
    url = f"{base}/api/v1/aggregate?metric={metric}&start={now - 3600}&end={now}&funcs={funcs}"
    with urllib.request.urlopen(url) as resp:
        body = orjson.loads(resp.read())
    data = body.get("aggregations", {})
    print(f"  HTTP aggregate: {data}")


if __name__ == "__main__":
    main()

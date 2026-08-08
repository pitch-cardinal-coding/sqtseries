"""Query sqtseries via ZMQ REQ and via HTTP from a separate process.

Connects to a running service and runs queries against it.
Does not import sqtseries itself — these are raw ZMQ calls any
language can replicate.

Usage:
    python3 examples/consumer.py --port 12502
    python3 examples/consumer.py --http http://127.0.0.1:12505
"""

import argparse

import orjson
import zmq

QUERY_PORT = 12502
HTTP_PORT = 12505


def main():
    parser = argparse.ArgumentParser(description="Query sqtseries via ZMQ or HTTP")
    parser.add_argument("--port", type=int, default=QUERY_PORT)
    parser.add_argument(
        "--http",
        type=str,
        default=None,
        help="HTTP base URL (e.g. http://127.0.0.1:12505)",
    )
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    if args.http:
        http_query(args.http)
    else:
        zmq_query(args.host, args.port)


def zmq_query(host: str, port: int):
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 500)
    sock.connect(f"tcp://{host}:{port}")

    sock.send(
        orjson.dumps(
            {
                "type": "query",
                "metric": "demo.cpu",
                "aggregation": "avg",
                "interval": "1m",
            }
        )
    )
    reply = orjson.loads(sock.recv())
    print("ZMQ query reply:", orjson.dumps(reply, option=orjson.OPT_INDENT_2).decode())
    sock.close(linger=0)
    ctx.term()


def http_query(base: str):
    import urllib.request

    url = f"{base}/api/v1/read?metric=demo.cpu&aggregation=avg&interval=1m"
    with urllib.request.urlopen(url) as resp:
        body = orjson.loads(resp.read())
    print("HTTP query reply:", orjson.dumps(body, option=orjson.OPT_INDENT_2).decode())


if __name__ == "__main__":
    main()

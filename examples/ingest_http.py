"""Send measurements to sqtseries via HTTP (no ZMQ needed).

Works with any HTTP client — curl, wget, Python's urllib, JavaScript fetch,
or any language's HTTP library. Use this when you cannot or prefer not to
install a ZMQ binding.

Usage:
    python3 examples/ingest_http.py
    python3 examples/ingest_http.py --batch 100
"""

import argparse
import random
import time
import urllib.request

import orjson

HTTP_PORT = 12505


def main():
    parser = argparse.ArgumentParser(
        description="Send measurements to sqtseries via HTTP"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=HTTP_PORT)
    parser.add_argument(
        "--rate",
        type=float,
        default=2,
        help="messages per second (0 = as fast as possible)",
    )
    parser.add_argument(
        "--batch", type=int, default=20, help="send N points per HTTP request"
    )
    parser.add_argument(
        "--count", type=int, default=0, help="total points to send (0 = forever)"
    )
    args = parser.parse_args()

    base = f"http://{args.host}:{args.port}"
    write_url = f"{base}/api/v1/write"

    metrics = ["sensor.temp", "sensor.humidity", "sensor.pressure"]
    sent = 0

    print(f"HTTP ingestion to {write_url} ({args.batch} points/batch)")
    interval = 1.0 / args.rate if args.rate > 0 else 0.0

    while args.count == 0 or sent < args.count:
        batch = []
        batch_size = (
            min(args.batch, args.count - sent) if args.count > 0 else args.batch
        )
        for _ in range(batch_size):
            batch.append(
                {
                    "metric": random.choice(metrics),
                    "value": round(random.uniform(15, 35), 1),
                    "tags": {"room": random.choice(["lobby", "office", "lab"])},
                }
            )

        body = orjson.dumps(batch)
        req = urllib.request.Request(
            write_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            result = orjson.loads(resp.read())

        sent += len(batch)
        if result.get("status") == "ok":
            print(f"  wrote {result['written']} points (total: {sent})")
        else:
            print(f"  ERROR: {result}")

        time.sleep(interval)

    print(f"Done: {sent} points sent")


if __name__ == "__main__":
    main()

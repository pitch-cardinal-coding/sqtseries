"""Send measurements into sqtseries from any language via raw ZMQ (no Client needed).

This is the fastest write path. Each message is one JSON frame sent to the
PUSH socket on port 12501. The server writes each frame in its own
transaction (there is no batching queue on the service path — see
docs/ingestion.html).

Usage:
    python3 examples/producer.py --host 127.0.0.1 --port 12501 --rate 10
"""

import argparse
import random
import time

import orjson
import zmq


def main():
    parser = argparse.ArgumentParser(
        description="Send measurements to sqtseries via ZMQ PUSH"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=12501)
    parser.add_argument(
        "--rate",
        type=float,
        default=10,
        help="messages per second (0 = as fast as possible)",
    )
    parser.add_argument(
        "--count", type=int, default=0, help="stop after this many (0 = forever)"
    )
    parser.add_argument(
        "--batch", type=int, default=500, help="messages per checkpoint print"
    )
    args = parser.parse_args()

    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    sock.setsockopt(zmq.SNDHWM, 10000)
    sock.setsockopt(zmq.LINGER, 500)
    sock.connect(f"tcp://{args.host}:{args.port}")

    metrics = ["cpu.usage", "mem.usage", "disk.usage", "net.bytes"]
    hosts = ["web1", "web2", "web3"]
    count = 0

    print(f"Sending to tcp://{args.host}:{args.port} at ~{args.rate} msg/s")
    start = time.monotonic()
    interval = 1.0 / args.rate if args.rate > 0 else 0

    while args.count == 0 or count < args.count:
        point = {
            "metric": random.choice(metrics),
            "value": round(random.random() * 100, 2),
            "tags": {"host": random.choice(hosts)},
        }
        sock.send(orjson.dumps(point))
        count += 1

        if count % args.batch == 0:
            elapsed = time.monotonic() - start
            rate = count / elapsed if elapsed > 0 else 0
            print(f"sent {count} points in {elapsed:.1f}s ({rate:.0f}/s)")

        if interval > 0:
            time.sleep(interval)

    elapsed = time.monotonic() - start
    rate = count / elapsed if elapsed > 0 else 0
    print(f"Done: {count} points in {elapsed:.1f}s ({rate:.0f}/s)")
    sock.close(linger=2000)  # flush any queued points instead of dropping them
    ctx.term()


if __name__ == "__main__":
    main()

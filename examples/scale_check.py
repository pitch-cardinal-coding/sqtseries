"""Churn N subscriber connections and verify the registry tracks each
join and leave exactly (server counts return to baseline afterwards).

Usage:
    python3 examples/scale_check.py --port 12503 --admin-port 12504 --topics 200
"""

import argparse
import json
import time

import zmq


def admin(admin_port, cmd):
    payload = json.dumps({"cmd": cmd}).encode()
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 2000)
    sock.connect(f"tcp://127.0.0.1:{admin_port}")
    try:
        sock.send(payload)
        if sock.poll(10000, zmq.POLLIN):
            return json.loads(sock.recv())
        raise RuntimeError("admin timeout")
    finally:
        sock.close(0)
        ctx.term()


def wait_for(pred, timeout, what):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for {what}")


def main():
    parser = argparse.ArgumentParser(
        description="Verify subscriber join/leave accounting at scale"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=12503)
    parser.add_argument("--admin-port", type=int, default=12504)
    parser.add_argument("--topics", type=int, default=200)
    args = parser.parse_args()

    def total():
        return admin(args.admin_port, "subscribers")["zmq_subscribers"]

    base = total()
    ctx = zmq.Context()
    ctx.set(zmq.MAX_SOCKETS, max(2048, args.topics + 128))
    socks = []
    for i in range(args.topics):
        s = ctx.socket(zmq.SUB)
        s.setsockopt(zmq.LINGER, 0)
        # Subscribe BEFORE connect (no slow-joiner gap), like stream_examples.
        s.setsockopt(zmq.SUBSCRIBE, f"scale.{i}".encode())
        s.connect(f"tcp://{args.host}:{args.port}")
        socks.append(s)
    wait_for(lambda: total() - base == args.topics, 30, "joins counted")
    print(f"joins counted: {args.topics}/{args.topics}")
    for s in socks:
        s.close(0)
    wait_for(lambda: total() == base, 30, "baseline restored")
    print(f"leaves counted: baseline {base} restored")
    ctx.term()
    print("PASS: scale_check")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Validate the live connection/subscription feed of a running sqtseries.

Connects ``--clients`` WebSocket subscribers and ``--zmq-subs`` ZMQ SUB
subscribers, watches the push-only ``/ws/connections`` feed, and asserts every
join and leave arrives instantly with the exact figures (client count, ZMQ
subscriber counts per topic). Exits non-zero on any mismatch.

This backs the camera dashboard's "Connected clients" / "ZMQ subscribers"
tables (examples/camera/overlay.html), which render this feed in real time.

Usage:
    python3 examples/camera/conn_validate.py \
        --host 127.0.0.1 --http-port 12505 --stream-port 12503
    python3 examples/camera/conn_validate.py --clients 5 --zmq-subs 3
"""

import argparse
import asyncio
import json
import sys
import time

import httpx
import websockets
import zmq
import zmq.asyncio


async def next_msg(ws, timeout: float) -> dict:
    return json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))


async def wait_for(ws, pred, timeout: float, label: str) -> tuple[dict, list[dict]]:
    """Consume frames until ``pred(msg)`` is true; return (msg, all seen)."""
    deadline = time.monotonic() + timeout
    seen: list[dict] = []
    while time.monotonic() < deadline:
        msg = await next_msg(ws, timeout=max(0.1, deadline - time.monotonic()))
        seen.append(msg)
        if pred(msg):
            return msg, seen
    raise AssertionError(f"timeout waiting for {label}; saw {seen}")


async def validate(args) -> None:
    base = f"http://{args.host}:{args.http_port}"
    sub_url = f"ws://{args.host}:{args.http_port}/ws/subscribe"
    conn_url = f"ws://{args.host}:{args.http_port}/ws/connections"
    stream = f"tcp://{args.host}:{args.stream_port}"

    ctx = zmq.asyncio.Context()
    zsocks: list[zmq.asyncio.Socket] = []
    wss: list[websockets.WebSocketClientProtocol] = []

    try:
        async with websockets.connect(conn_url, open_timeout=15) as mon:
            snap = await next_msg(mon, timeout=10)
            assert snap["type"] == "snapshot", snap
            baseline_ids = {c["id"] for c in snap.get("connections", [])}
            baseline_zmq = {
                s["topic"]: s["subscribers"] for s in snap.get("subscriptions", [])
            }
            print(
                f"snapshot: ws_connections={snap['ws_connections']} "
                f"zmq_subscribers={snap['zmq_subscribers']} "
                f"(baseline {len(baseline_ids)} ws / {sum(baseline_zmq.values())} zmq)"
            )

            # --- join --clients WebSocket subscribers ----------------------
            for i in range(args.clients):
                topic = f"v{i}."
                ws = await websockets.connect(
                    f"{sub_url}?metric={topic}", open_timeout=15
                )
                wss.append(ws)
                t0 = time.monotonic()
                msg, _ = await wait_for(
                    mon,
                    lambda m, t=topic: (
                        m.get("type") == "conn"
                        and m.get("connected") is True
                        and m.get("topic") == t
                    ),
                    timeout=args.timeout,
                    label=f"conn join {topic}",
                )
                print(
                    f"  join ws {topic}: conn event in "
                    f"{(time.monotonic() - t0) * 1000:.0f}ms"
                )
                assert isinstance(msg.get("connected_at"), (int, float)), msg
                print(f"           arrival {msg['connected_at']:.3f}")

            # --- join --zmq-subs ZMQ subscribers (distinct topics) ---------
            for i in range(args.zmq_subs):
                topic = f"cpu{i}.".encode()
                zs = ctx.socket(zmq.SUB)
                zs.connect(stream)
                zs.setsockopt(zmq.SUBSCRIBE, topic)
                zsocks.append(zs)
            sub_topics = {f"cpu{i}.": 0 for i in range(args.zmq_subs)}
            sub_arrived = {}
            deadline = time.monotonic() + args.timeout
            while time.monotonic() < deadline and any(
                n < 1 for n in sub_topics.values()
            ):
                msg = await next_msg(mon, timeout=args.timeout)
                if msg.get("type") == "sub":
                    t = msg.get("topic", "")
                    if t in sub_topics:
                        sub_topics[t] = msg.get("subscribers", 0)
                        sub_arrived[t] = msg.get("arrived_at")
                        print(f"  join zmq {t!r}: sub event -> {sub_topics[t]}")
            for t, at in sub_arrived.items():
                assert isinstance(at, (int, float)), (t, at)
                print(f"           arrival {t!r} {at:.3f}")

            # --- current figures must be exact (beyond the baseline) --------
            with httpx.Client(timeout=10) as c:
                conns = c.get(f"{base}/api/v1/connections").json()["data"]
                subs = c.get(f"{base}/api/v1/subscribers").json()
            new_conns = [e for e in conns if e["id"] not in baseline_ids]
            names = sorted(e["topic"] for e in new_conns)
            print(
                f"after joins: +{len(new_conns)} new ws "
                f"(connections={names}) zmq={subs['zmq_subscribers']}"
            )
            assert len(new_conns) == args.clients, new_conns
            assert names == [f"v{i}." for i in range(args.clients)], names
            for s in subs["subscriptions"]:
                if s["topic"] in sub_topics and s["subscribers"]:
                    sub_topics[s["topic"]] = s["subscribers"]
            assert all(n == 1 for n in sub_topics.values()), sub_topics

            # --- leave: WebSocket clients one at a time --------------------
            for i in reversed(range(args.clients)):
                topic = f"v{i}."
                t0 = time.monotonic()
                await wss[i].close()
                msg, _ = await wait_for(
                    mon,
                    lambda m, t=topic: (
                        m.get("type") == "conn"
                        and m.get("connected") is False
                        and m.get("topic") == t
                    ),
                    timeout=args.timeout,
                    label=f"conn leave {topic}",
                )
                print(
                    f"  leave ws {topic}: conn event in "
                    f"{(time.monotonic() - t0) * 1000:.0f}ms"
                )
                assert isinstance(msg.get("connected_at"), (int, float)), msg
                assert isinstance(msg.get("left_at"), (int, float)), msg
                assert msg["left_at"] >= msg["connected_at"], msg
                print(
                    f"           left {msg['left_at']:.3f} "
                    f"(after {msg['left_at'] - msg['connected_at']:.1f}s)"
                )
            wss.clear()

            # --- leave: ZMQ subscribers ------------------------------------
            for zs in zsocks:
                zs.close(linger=0)
            zsocks.clear()
            left = {f"cpu{i}.": None for i in range(args.zmq_subs)}
            left_at = {}
            deadline = time.monotonic() + args.timeout
            while time.monotonic() < deadline and any(v is None for v in left.values()):
                msg = await next_msg(mon, timeout=args.timeout)
                if msg.get("type") == "sub":
                    t = msg.get("topic", "")
                    if t in left:
                        left[t] = msg.get("subscribers", 0)
                        left_at[t] = msg
            print(f"  zmq leaves -> subscribers {left}")
            assert left == {f"cpu{i}.": 0 for i in range(args.zmq_subs)}, left
            for t, msg in left_at.items():
                assert isinstance(msg.get("left_at"), (int, float)), msg
                fs = msg.get("first_seen")
                if isinstance(fs, (int, float)):
                    assert msg["left_at"] >= fs, msg
                print(
                    f"           left {t!r} at {msg['left_at']:.3f}"
                    f" (first_seen {fs:.3f})"
                    if isinstance(fs, (int, float))
                    else f"           left {t!r} at {msg['left_at']:.3f}"
                )

            # --- final figures: back to the baseline -----------------------
            with httpx.Client(timeout=10) as c:
                conns = c.get(f"{base}/api/v1/connections").json()["data"]
                subs = c.get(f"{base}/api/v1/subscribers").json()
            present_ids = {e["id"] for e in conns}
            zmq_topics = {s["topic"] for s in subs["subscriptions"]}
            print(
                f"final: ws_connections={len(conns)} "
                f"(baseline {len(baseline_ids)}) "
                f"zmq={subs['zmq_subscribers']} (baseline {sum(baseline_zmq.values())})"
            )
            assert present_ids == baseline_ids, (present_ids, baseline_ids)
            assert not zmq_topics & set(sub_topics), zmq_topics
            assert subs["zmq_subscribers"] == sum(baseline_zmq.values()), subs
    finally:
        for ws in wss:
            await ws.close()
        for zs in zsocks:
            zs.close(linger=0)
        ctx.term()

    print("\nOK: every join/leave figure correct and pushed live")


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Validate sqtseries' live connection/subscription feed"
    )
    p.add_argument("--host", default="127.0.0.1", help="sqtseries host")
    p.add_argument("--http-port", type=int, default=12505, help="HTTP/WS port")
    p.add_argument("--stream-port", type=int, default=12503, help="ZMQ streaming port")
    p.add_argument(
        "--clients", type=int, default=3, help="WebSocket clients to join/leave"
    )
    p.add_argument(
        "--zmq-subs", type=int, default=2, help="ZMQ subscribers to join/leave"
    )
    p.add_argument("--timeout", type=float, default=5.0, help="wait per event (s)")
    return p.parse_args(argv)


def main(argv: list[str]) -> None:
    asyncio.run(validate(parse_args(argv)))


if __name__ == "__main__":
    main(sys.argv[1:])

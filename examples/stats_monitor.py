"""Monitor connection and subscription activity from the stats PUB socket.

sqtseries emits real-time events on port 12506 (ZMQ PUB). This script
subscribes to that socket and prints every connect, disconnect, and
subscription change with a timestamp — useful as a live activity monitor
or as a template for building a dashboard or alerting pipeline.

Usage:
    python3 examples/stats_monitor.py          # all events
    python3 examples/stats_monitor.py conn     # only connection events
    python3 examples/stats_monitor.py sub      # only subscription events
    python3 examples/stats_monitor.py report   # only periodic report events
"""

import argparse
import time

import orjson
import zmq

STATS_PORT = 12506


def main():
    parser = argparse.ArgumentParser(description="Subscribe to sqtseries stats events")
    parser.add_argument(
        "filter",
        nargs="?",
        default="",
        help="Event filter prefix (conn, sub, report, or empty for all)",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=STATS_PORT)
    args = parser.parse_args()

    ctx = zmq.Context()
    sock = ctx.socket(zmq.SUB)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f"tcp://{args.host}:{args.port}")
    sock.setsockopt(zmq.SUBSCRIBE, args.filter.encode() if args.filter else b"")

    prefix = f"'{args.filter}'" if args.filter else "all"
    print(f"Subscribed to stats PUB on {args.host}:{args.port} (filter: {prefix})")
    print("Waiting for events... (Ctrl+C to stop)")
    print()

    try:
        while True:
            event_type, payload = sock.recv_multipart()
            data = orjson.loads(payload)
            ts = time.strftime("%H:%M:%S")
            etype = event_type.decode()

            if etype == "conn":
                if data.get("connected"):
                    print(
                        f"{ts}  CONNECTED    {data['kind']:>3s}  {data['id']}  {data.get('peer', '?')}"
                    )
                else:
                    print(f"{ts}  DISCONNECTED {data['kind']:>3s}  {data['id']}")
            elif etype == "sub":
                subs = data.get("subscribers", 0)
                print(f"{ts}  SUB          {data['topic']:20s}  listeners: {subs}")
            elif etype == "report":
                print(
                    f"{ts}  REPORT  ws={data.get('ws_connections', 0)}  "
                    f"zmq_subs={data.get('zmq_subscribers', 0)}  "
                    f"topics={data.get('active_topics', 0)}  "
                    f"uptime={data.get('uptime_s', 0)}s"
                )
            else:
                print(f"{ts}  {etype}: {orjson.dumps(data).decode()}")
    except KeyboardInterrupt:
        print("stopped")
    finally:
        sock.close(linger=0)
        ctx.term()


if __name__ == "__main__":
    main()

"""Run every admin command against a running sqtseries service.

The admin REP socket (port 12504) accepts 9 commands for operational
control and monitoring. This script demonstrates each one with its
output so you can see exactly what the service returns.

Usage:
    python3 examples/admin_examples.py
    python3 examples/admin_examples.py --host 192.168.1.10
"""

import argparse

import orjson
import zmq

ADMIN_PORT = 12504


def admin(endpoint: str, cmd: str, **kwargs) -> dict:
    """Send an admin command and return the reply."""
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 500)
    sock.connect(endpoint)
    payload = {"cmd": cmd}
    payload.update(kwargs)
    sock.send(orjson.dumps(payload))
    reply = orjson.loads(sock.recv())
    sock.close(linger=0)
    ctx.term()
    return reply


def main():
    parser = argparse.ArgumentParser(
        description="Run all admin commands against sqtseries"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=ADMIN_PORT)
    args = parser.parse_args()

    ep = f"tcp://{args.host}:{args.port}"
    print(f"Admin socket: {ep}\n")

    # 1. ping
    print("--- ping ---")
    r = admin(ep, "ping")
    print(r)
    assert r.get("pong") is True

    # 2. health
    print("\n--- health ---")
    r = admin(ep, "health")
    print(f"  status:   {r.get('status')}")
    print(f"  version:  {r.get('version')}")
    print(f"  uptime:   {r.get('uptime')}s")
    print(f"  database: {'healthy' if r.get('database') else 'degraded'}")

    # 3. stats
    print("\n--- stats ---")
    r = admin(ep, "stats")
    keys = [
        "uptime_s",
        "ingested",
        "invalid",
        "queries",
        "published",
        "subscribers",
        "series",
        "metrics",
        "wal_bytes",
        "checkpoints",
    ]
    for k in keys:
        if k in r:
            print(f"  {k:>22s}: {r[k]}")

    # 4. connections
    print("\n--- connections ---")
    r = admin(ep, "connections")
    conns = r.get("data", [])
    print(f"  {len(conns)} active WebSocket connection(s)")
    for c in conns:
        print(f"    {c['id'][:12]}  {c['kind']}  {c['peer']:>21s}  {c['topic']}")

    # 5. conncheck
    print("\n--- conncheck ---")
    all_ids = [c["id"] for c in conns]
    r = admin(ep, "conncheck", ids=all_ids + ["nonexistent-id"])
    present = r.get("present", [])
    print(f"  Checked {len(all_ids) + 1} IDs, {len(present)} present")

    # 6. subscribers
    print("\n--- subscribers ---")
    r = admin(ep, "subscribers")
    print(f"  Total ZMQ subscribers: {r.get('zmq_subscribers', 0)}")
    for sub in r.get("subscriptions", []):
        print(f"    {sub['topic']:>20s}: {sub['subscribers']} listener(s)")

    # 7. optimize
    print("\n--- optimize ---")
    r = admin(ep, "optimize")
    print(r)

    # 8. backup
    print("\n--- backup ---")
    r = admin(ep, "backup")
    if r.get("status") == "ok":
        print(f"  Backup created: {r['path']}")
    else:
        print(f"  Backup failed: {r.get('error', {}).get('message', 'unknown')}")

    # 9. vacuum
    print("\n--- vacuum ---")
    r = admin(ep, "vacuum")
    if r.get("status") == "ok":
        print("  VACUUM complete")
    else:
        print(f"  VACUUM failed: {r.get('error', {}).get('message', '')}")
        print(
            "  (This is expected while the service is running. Use 'sqtseries stop && sqtseries vacuum')"
        )


if __name__ == "__main__":
    main()

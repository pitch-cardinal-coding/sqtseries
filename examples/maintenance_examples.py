"""Run maintenance operations and health checks against sqtseries.

Covers backup, vacuum, optimize, and health checking — the operations
you run periodically to keep the database healthy and recoverable.

Usage:
    python3 examples/maintenance_examples.py
    python3 examples/maintenance_examples.py --backup-only
    python3 examples/maintenance_examples.py --health-loop 60
    python3 examples/maintenance_examples.py --admin-port 14004 --http-port 14005
"""

import argparse
import time

import orjson
import zmq

ADMIN_PORT = 12504
HTTP_PORT = 12505


def admin(host: str, cmd: str, port: int = ADMIN_PORT, **kwargs) -> dict:
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 500)
    sock.connect(f"tcp://{host}:{port}")
    payload = {"cmd": cmd}
    payload.update(kwargs)
    sock.send(orjson.dumps(payload))
    reply = orjson.loads(sock.recv())
    sock.close(linger=0)
    ctx.term()
    return reply


def health_check(
    host: str, admin_port: int = ADMIN_PORT, http_port: int = HTTP_PORT
) -> bool:
    """Run a health check via the admin socket and HTTP endpoint."""
    print("=== Health check ===")

    # Admin socket health
    h = admin(host, "health", port=admin_port)
    status = "HEALTHY" if h.get("status") == "ok" and h.get("database") else "DEGRADED"
    print(f"  ZMQ admin: {status}")
    if h.get("status") != "ok":
        print(f"    status={h.get('status')}, database={h.get('database')}")

    # HTTP health endpoint (simpler, for load balancers)
    import urllib.request

    try:
        with urllib.request.urlopen(f"http://{host}:{http_port}/api/v1/health") as resp:
            body = orjson.loads(resp.read())
        print(f"  HTTP:      {'HEALTHY' if body.get('status') == 'ok' else 'DEGRADED'}")
    except Exception as exc:
        print(f"  HTTP:      unreachable ({exc})")

    return h.get("status") == "ok" and h.get("database")


def backup_database(host: str, admin_port: int = ADMIN_PORT):
    """Create a point-in-time backup via VACUUM INTO."""
    print("\n=== Backup ===")
    r = admin(host, "backup", port=admin_port)
    if r.get("status") == "ok":
        path = r["path"]
        print(f"  Backup created: {path}")
        # Check file size
        import os

        try:
            size = os.path.getsize(path)
            print(f"  Size: {size:,} bytes ({size / 1024 / 1024:.1f} MB)")
        except OSError:
            print("  (file not directly accessible)")
    else:
        print(f"  Backup failed: {r.get('error', {}).get('message', 'unknown')}")


def optimize_planner(host: str, admin_port: int = ADMIN_PORT):
    """Refresh SQLite query-planner statistics."""
    print("\n=== Optimize ===")
    r = admin(host, "optimize", port=admin_port)
    print(f"  {'OK' if r.get('optimized') else 'FAILED'}")


def vacuum_database(host: str, admin_port: int = ADMIN_PORT):
    """Reclaim disk space. Full VACUUM requires the service to be stopped."""
    print("\n=== Vacuum ===")
    r = admin(host, "vacuum", port=admin_port)
    if r.get("status") == "ok":
        print("  VACUUM complete")
    else:
        msg = r.get("error", {}).get("message", "")
        print(f"  Cannot vacuum while running: {msg}")
        print("  To do a full VACUUM:")
        print("    sqtseries stop")
        print("    sqtseries vacuum")
        print("    sqtseries run")


def show_stats(host: str, admin_port: int = ADMIN_PORT):
    """Display current database statistics."""
    print("\n=== Statistics ===")
    r = admin(host, "stats", port=admin_port)
    for key in ["ingested", "queries", "published", "series", "metrics"]:
        if key in r:
            print(f"  {key:>12s}: {r[key]}")
    if "wal_bytes" in r:
        print(f"  {'wal_bytes':>12s}: {r['wal_bytes']:,}")


def main():
    parser = argparse.ArgumentParser(
        description="Run maintenance operations against sqtseries"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--admin-port",
        type=int,
        default=ADMIN_PORT,
        help="ZMQ admin REP port (default 12504)",
    )
    parser.add_argument(
        "--http-port",
        type=int,
        default=HTTP_PORT,
        help="HTTP gateway port (default 12505)",
    )
    parser.add_argument(
        "--backup-only", action="store_true", help="Only create a backup and exit"
    )
    parser.add_argument(
        "--health-loop",
        type=float,
        default=0,
        help="Run health checks in a loop every N seconds",
    )
    args = parser.parse_args()

    if args.health_loop > 0:
        print(f"Health check loop every {args.health_loop}s. Ctrl+C to stop.")
        try:
            while True:
                ok = health_check(args.host, args.admin_port, args.http_port)
                if not ok:
                    print(f"  [{time.strftime('%H:%M:%S')}] ALERT: service degraded!")
                time.sleep(args.health_loop)
        except KeyboardInterrupt:
            print("stopped")
        return

    health_check(args.host, args.admin_port, args.http_port)

    if args.backup_only:
        backup_database(args.host, args.admin_port)
        return

    show_stats(args.host, args.admin_port)
    backup_database(args.host, args.admin_port)
    optimize_planner(args.host, args.admin_port)
    vacuum_database(args.host, args.admin_port)
    health_check(args.host, args.admin_port, args.http_port)


if __name__ == "__main__":
    main()

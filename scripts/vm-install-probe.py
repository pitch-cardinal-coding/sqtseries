"""vm-install-probe.py — verify an installed sqtseries, from outside it.

Run it with the interpreter of the install under test, on the host that runs
the service:

    /opt/sqtseries/bin/python3 scripts/vm-install-probe.py

Exercises every surface a new operator depends on: ZMQ ingest/query/admin,
HTTP write/read/aggregate/health, WebSocket streaming, the dashboard, /docs,
and the CLI. One PASS/FAIL line per check; exits non-zero if any fail.

Use it after an install on a clean machine. A venv built from requirements.txt
once shipped without `websockets`, which left every HTTP route answering 200
and every WebSocket endpoint dead — the kind of break that only a probe
exercising the real endpoints will catch.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

HTTP = "http://127.0.0.1:12505"
INGEST, QUERY, STREAM, ADMIN, STATS = 12500, 12502, 12503, 12504, 12506

results: list[tuple[bool, str, str]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    results.append((ok, name, detail))
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
    return ok


def http(path: str, method: str = "GET", body: dict | None = None) -> tuple[int, str]:
    data = json.dumps(body).encode() if body is not None else None
    # HTTP is a fixed loopback constant; no caller supplies a scheme.
    req = urllib.request.Request(  # noqa: S310
        f"{HTTP}{path}", data=data, method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:  # noqa: S310
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def zmq_req(port: int, payload: dict, timeout: int = 5000) -> dict:
    import zmq

    ctx = zmq.Context.instance()
    s = ctx.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, timeout)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(f"tcp://127.0.0.1:{port}")
    try:
        s.send_json(payload)
        return s.recv_json()
    finally:
        s.close()


def main() -> int:
    metric = f"vmcheck.metric.{int(time.time())}"
    now = time.time()

    # ---- HTTP: write / read / aggregate -----------------------------
    st, body = http("/api/v1/write", "POST", {"metric": metric, "value": 21.0,
                                              "timestamp": now - 10, "tags": {"host": "vm"}})
    check("HTTP write", st == 200 and json.loads(body).get("written") == 1, f"status={st}")

    st, body = http("/api/v1/write", "POST", {"metric": metric, "value": 23.0,
                                              "timestamp": now, "tags": {"host": "vm"}})
    check("HTTP write (ISO-less epoch + tags)", st == 200, f"status={st}")

    iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 5))
    st, body = http("/api/v1/write", "POST", {"metric": metric, "value": 99.0,
                                              "timestamp": iso})
    check("HTTP write (ISO-8601 timestamp)", st == 200, f"status={st}")

    st, body = http(f"/api/v1/read?metric={metric}")
    data = json.loads(body).get("data", []) if st == 200 else []
    check("HTTP read returns rows", st == 200 and len(data) == 3, f"status={st} rows={len(data)}")

    st, body = http(f"/api/v1/read?metric={metric}&aggregation=avg")
    check("HTTP aggregate avg", st == 200, f"status={st}")

    st, body = http("/api/v1/health")
    check("HTTP health", st == 200 and json.loads(body).get("status") == "ok", f"status={st}")

    st, _ = http("/api/v1/write", "POST", {"metric": metric})
    check("HTTP write rejects a missing value (400)", st == 400, f"status={st}")

    # ---- WebSocket: silently dead when websockets is absent ------------
    try:
        import websockets
        ws_mod = websockets.__version__
    except ImportError:
        ws_mod = None
    check("`websockets` importable (runtime dep)", ws_mod is not None, f"version={ws_mod}")

    import asyncio

    async def ws_probe() -> tuple[bool, str]:
        try:
            import websockets
        except ImportError:
            return False, "websockets not installed"
        import zmq

        async def feed() -> None:
            # The endpoint is push-only, so a frame only arrives if the
            # ingest path produces one while the socket is open.
            await asyncio.sleep(1.5)
            ctx = zmq.Context.instance()
            push = ctx.socket(zmq.PUSH)
            push.setsockopt(zmq.LINGER, 0)
            push.connect("tcp://127.0.0.1:12500")
            for i in range(5):
                push.send_json({"metric": metric, "value": 40.0 + i,
                                "timestamp": time.time()})
                await asyncio.sleep(0.1)
            push.close()

        try:
            async with websockets.connect(
                "ws://127.0.0.1:12505/ws/subscribe", open_timeout=10
            ) as sock:
                feeder = asyncio.create_task(feed())
                got = await asyncio.wait_for(sock.recv(), timeout=20)
                feeder.cancel()
                return True, f"recv {got[:70]}"
        except Exception as e:
            return False, f"{type(e).__name__}: {e}"

    ok, detail = asyncio.run(ws_probe())
    check("WebSocket subscribe/stream", ok, detail)

    # ---- ZMQ: query + admin + streaming ------------------------------
    try:
        r = zmq_req(QUERY, {"cmd": "query", "metric": metric, "aggregation": "avg", "interval": "1h"})
        check("ZMQ query", bool(r.get("data")) or r.get("error") is None, str(r)[:80])
    except Exception as e:
        check("ZMQ query", False, f"{type(e).__name__}: {e}")

    for cmd in ("ping", "health", "stats", "connections", "subscribers"):
        try:
            r = zmq_req(ADMIN, {"cmd": cmd})
            check(f"ZMQ admin {cmd}", r.get("error") in (None, ""), str(r)[:80])
        except Exception as e:
            check(f"ZMQ admin {cmd}", False, f"{type(e).__name__}: {e}")

    try:
        import zmq

        ctx = zmq.Context.instance()
        sub = ctx.socket(zmq.SUB)
        sub.setsockopt(zmq.SUBSCRIBE, b"")
        sub.setsockopt(zmq.RCVTIMEO, 3000)
        sub.connect(f"tcp://127.0.0.1:{STREAM}")
        pub = ctx.socket(zmq.PUSH)
        pub.setsockopt(zmq.LINGER, 0)
        pub.connect(f"tcp://127.0.0.1:{INGEST}")
        time.sleep(0.6)  # let the subscription reach the XPUB
        for i in range(5):
            pub.send_json({"metric": metric, "value": 30.0 + i, "timestamp": time.time()})
        frames = 0
        deadline = time.time() + 5
        while time.time() < deadline and frames < 1:
            try:
                parts = sub.recv_multipart()
                json.loads(parts[-1])
                frames += 1
            except zmq.Again:
                break
        sub.close()
        pub.close()
        check("ZMQ ingest -> XPUB stream", frames >= 1, f"frames={frames}")
    except Exception as e:
        check("ZMQ ingest -> XPUB stream", False, f"{type(e).__name__}: {e}")

    # ---- HTTP static surfaces ---------------------------------------
    for path, label in (("/dashboard", "dashboard"), ("/docs/", "docs index"),
                        ("/docs/index.html", "docs page"), ("/docs/favicon.svg", "favicon")):
        st, _ = http(path)
        check(f"HTTP {label}", st == 200, f"status={st}")

    # ---- CLI --------------------------------------------------------
    for args, label in (
        (["sqtseries", "--version"], "cli --version"),
        (["sqtseries", "status"], "cli status"),
        (["sqtseries", "ports"], "cli ports"),
        (["sqtseries", "health"], "cli health"),
        (["sqtseries", "stats"], "cli stats"),
    ):
        p = subprocess.run(args, capture_output=True, text=True, timeout=90)  # noqa: S603
        check(label, p.returncode == 0, f"rc={p.returncode} {p.stderr.strip()[:60]}")

    # ---- summary ----------------------------------------------------
    failed = [n for ok, n, _ in results if not ok]
    print("\n" + "=" * 60)
    print(f"{len(results) - len(failed)}/{len(results)} checks passed")
    if failed:
        print("FAILED: " + ", ".join(failed))
    print("=" * 60)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

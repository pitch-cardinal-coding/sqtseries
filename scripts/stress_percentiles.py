#!/usr/bin/env python3
"""Stress every live sqtseries path and report latency percentiles (P50/P90/P99).

Spawns a real service on free ports, pumps data through ZMQ ingest and HTTP
write, then hammers every read path — ZMQ query, HTTP read, HTTP aggregate,
WebSocket fanout, admin commands — and reports P50/P90/P99 per path.

Usage:
    python3 scripts/stress_percentiles.py [--duration 30] [--rate 2000]
                                          [--clients 8] [--json out.json]
"""

from __future__ import annotations

import argparse
import contextlib
import json
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

import zmq

PROJECT = Path(__file__).resolve().parent.parent
PY = sys.executable


def free_ports() -> dict[str, int]:
    def one() -> int:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    ports: dict[str, int] = {}
    for name in ("ingest", "query", "streaming", "admin", "http", "stats"):
        p = one()
        while p in ports.values():
            p = one()
        ports[name] = p
    return ports


def wait_admin(port: int, timeout_s: float = 30.0) -> bool:
    """Ping the admin port until it answers.

    One fresh REQ socket per attempt: a reply that arrives after the poll
    timeout leaves a REQ socket in the 'awaiting reply' state, and the next
    send on that socket raises EFSM. Recreating per attempt sidesteps the
    whole class (same reason the workers reset on timeout).
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        ctx = zmq.Context()
        sock = ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.RCVTIMEO, 1000)
        sock.connect(f"tcp://127.0.0.1:{port}")
        try:
            sock.send_json({"cmd": "ping"})
            if sock.poll(1000) & zmq.POLLIN and sock.recv_json().get("pong"):
                return True
        except zmq.ZMQError:
            pass
        finally:
            sock.close(0)
            ctx.term()
        time.sleep(0.2)
    return False


class Latencies:
    """Thread-safe per-path latency collector (ms)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._samples: dict[str, list[float]] = {}
        self._errors: dict[str, int] = {}

    def record(self, path: str, ms: float) -> None:
        with self._lock:
            self._samples.setdefault(path, []).append(ms)

    def error(self, path: str) -> None:
        with self._lock:
            self._errors[path] = self._errors.get(path, 0) + 1

    def report(self) -> dict[str, dict]:
        out = {}
        for path, raw in self._samples.items():
            xs = sorted(raw)
            n = len(xs)

            def pct(p: float, *, _xs: list[float] = xs, _n: int = n) -> float:
                return round(_xs[min(int(_n * p), _n - 1)], 3)

            out[path] = {
                "n": n,
                "errors": self._errors.get(path, 0),
                "p50_ms": pct(0.50),
                "p90_ms": pct(0.90),
                "p95_ms": pct(0.95),
                "p99_ms": pct(0.99),
                "max_ms": round(xs[-1], 3),
                "mean_ms": round(statistics.fmean(xs), 3),
            }
        for path, n_err in self._errors.items():
            if path not in out:
                out[path] = {"n": 0, "errors": n_err}
        return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=30.0, help="query phase seconds")
    ap.add_argument("--rate", type=int, default=2000, help="pump points/sec total")
    ap.add_argument("--clients", type=int, default=8, help="concurrent query clients")
    ap.add_argument("--warmup-rows", type=int, default=20000)
    ap.add_argument("--json", type=str, default="", help="also write raw report JSON")
    ap.add_argument(
        "--http-port",
        type=int,
        default=0,
        help="fixed HTTP port (0 = auto); lets external clients (e.g. a browser probe) target the run",
    )
    args = ap.parse_args()

    import http.client

    import orjson

    ports = free_ports()
    if args.http_port:
        ports["http"] = args.http_port
    workdir = Path("/tmp/sqtseries-stress")  # noqa: S108 - throwaway stress dir
    shutil.rmtree(workdir, ignore_errors=True)  # stale config would fail TOML
    workdir.mkdir(exist_ok=True)
    cfg = workdir / "config.toml"
    cfg.write_text(f"""[database]
path = "{workdir / "db.sqlite"}"

[ingestion]
port = {ports["ingest"]}

[query]
port = {ports["query"]}

[streaming]
port = {ports["streaming"]}

[admin]
port = {ports["admin"]}

[http]
port = {ports["http"]}
# The stress harness hammers from one loopback address; the default 600/min
# fixed-window limiter would (correctly!) 429 the whole run.
rate_limit_per_minute = 1000000
max_websocket_connections = 1000000

[stats]
port = {ports["stats"]}

[ports]
auto_detect = false
""")

    proc = subprocess.Popen(  # noqa: S603 - fixed argv, no user input
        [PY, "-u", "-m", "sqtseries", "--config", str(cfg), "run"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    lat = Latencies()
    stop = threading.Event()
    results: dict[str, object] = {"ports": ports}

    try:
        if not wait_admin(ports["admin"]):
            print("FAIL: service did not start")
            return 2
        print(f"service up on {ports}")

        ctx = zmq.Context.instance()

        # --- warm-up data: ZMQ PUSH ingest -------------------------------
        push = ctx.socket(zmq.PUSH)
        push.setsockopt(zmq.SNDHWM, 50_000)
        push.setsockopt(zmq.LINGER, 1000)
        push.connect(f"tcp://127.0.0.1:{ports['ingest']}")
        t0 = time.monotonic()
        n = 0
        while n < args.warmup_rows:
            push.send(
                orjson.dumps(
                    {
                        "metric": f"stress.m{n % 25}",
                        "value": (n % 1000) / 10.0,
                        "tags": {"host": f"h{n % 10}"},
                    }
                )
            )
            n += 1
            if n % 500 == 0:
                time.sleep(0.002)  # let the server drain
        print(f"pumped {n} warm-up rows in {time.monotonic() - t0:.1f}s")

        # --- sustained pump (ZMQ) at --rate during the whole query phase --
        # The pump runs as a separate PROCESS, not a thread: under the GIL
        # with a dozen churning client threads, a pump thread's sleeps stretch
        # (measured: 2000/s advertised, ~54/s delivered). A process is
        # unaffected by the harness's own concurrency.
        pump_code = f"""
import json, time, zmq
ctx = zmq.Context()
sock = ctx.socket(zmq.PUSH)
sock.setsockopt(zmq.SNDHWM, 50000)
sock.setsockopt(zmq.LINGER, 1000)
sock.connect("tcp://127.0.0.1:{ports["ingest"]}")
rate = {args.rate}
metrics = [f"stress.m{{i}}" for i in range(25)]
n = 0
batch = 50
interval = batch / rate
deadline = time.monotonic() + {args.duration!r}
while time.monotonic() < deadline:
    t0 = time.monotonic()
    for i in range(batch):
        sock.send_json({{"metric": metrics[n % 25], "value": (n % 1000) / 10.0,
                         "tags": {{"host": f"h{{n % 10}}"}}}})
        n += 1
    elapsed = time.monotonic() - t0
    if elapsed < interval:
        time.sleep(interval - elapsed)
print(json.dumps({{"pumped": n}}))
"""
        pump_proc = subprocess.Popen(  # noqa: S603 - fixed argv, no user input
            [PY, "-c", pump_code],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )

        # --- HTTP write path (own thread, measures write latency) ---------
        def http_writer() -> None:
            body = b'{"metric":"stress.http","value":1.0,"tags":{"host":"h0"}}'
            hdr = {"Content-Type": "application/json"}
            conn = http.client.HTTPConnection("127.0.0.1", ports["http"], timeout=10)
            while not stop.is_set():
                t = time.perf_counter()
                try:
                    conn.request("POST", "/api/v1/write", body=body, headers=hdr)
                    r = conn.getresponse()
                    r.read()
                    if r.status == 200:
                        lat.record("http_write", (time.perf_counter() - t) * 1e3)
                    else:
                        lat.error("http_write")
                except Exception:
                    lat.error("http_write")
                    with contextlib.suppress(Exception):
                        conn.close()
                    time.sleep(0.05)
                    conn = http.client.HTTPConnection(
                        "127.0.0.1", ports["http"], timeout=10
                    )
                time.sleep(0.05)

        threading.Thread(target=http_writer, daemon=True).start()

        # --- ZMQ query clients (REQ, concurrent) --------------------------
        def zmq_query_worker(worker: int) -> None:
            sock = ctx.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 0)
            sock.setsockopt(zmq.RCVTIMEO, 5000)
            sock.connect(f"tcp://127.0.0.1:{ports['query']}")
            while not stop.is_set():
                t = time.perf_counter()
                try:
                    sock.send_json(
                        {
                            "type": "query",
                            "metric": f"stress.m{worker % 25}",
                            "start": 0,
                            "end": int(time.time() * 1e9),
                            "aggregation": "avg",
                            "interval": "1m",
                        }
                    )
                    if sock.poll(5000) & zmq.POLLIN:
                        sock.recv_json()
                        lat.record("zmq_query", (time.perf_counter() - t) * 1e3)
                    else:
                        # Timed out mid REQ: the state machine is stuck in
                        # 'sending' until the reply arrives — reset it with a
                        # fresh socket or every later send raises EFSM.
                        lat.error("zmq_query")
                        sock.close(0)
                        sock = ctx.socket(zmq.REQ)
                        sock.setsockopt(zmq.LINGER, 0)
                        sock.setsockopt(zmq.RCVTIMEO, 5000)
                        sock.connect(f"tcp://127.0.0.1:{ports['query']}")
                except zmq.ZMQError:
                    lat.error("zmq_query")
                    sock.close(0)
                    sock = ctx.socket(zmq.REQ)
                    sock.setsockopt(zmq.LINGER, 0)
                    sock.setsockopt(zmq.RCVTIMEO, 5000)
                    sock.connect(f"tcp://127.0.0.1:{ports['query']}")

        # --- HTTP read + aggregate (concurrent) ---------------------------
        def http_read_worker(worker: int, agg: bool) -> None:
            path_kind = "http_agg" if agg else "http_read"
            metric = f"stress.m{worker % 25}"
            conn = http.client.HTTPConnection("127.0.0.1", ports["http"], timeout=10)
            while not stop.is_set():
                url = f"/api/v1/read?metric={metric}"
                if agg:
                    url += "&aggregation=avg&interval=1m"
                t = time.perf_counter()
                try:
                    conn.request("GET", url)
                    r = conn.getresponse()
                    r.read()
                    if r.status == 200:
                        lat.record(path_kind, (time.perf_counter() - t) * 1e3)
                    else:
                        lat.error(path_kind)
                except Exception:
                    lat.error(path_kind)
                    with contextlib.suppress(Exception):
                        conn.close()
                    time.sleep(0.05)
                    conn = http.client.HTTPConnection(
                        "127.0.0.1", ports["http"], timeout=10
                    )
                time.sleep(0.02)

        # --- admin path ---------------------------------------------------
        def admin_worker() -> None:
            sock = ctx.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 0)
            sock.setsockopt(zmq.RCVTIMEO, 5000)
            sock.connect(f"tcp://127.0.0.1:{ports['admin']}")
            cmds = ["ping", "stats", "connections"]
            i = 0
            while not stop.is_set():
                t = time.perf_counter()
                try:
                    sock.send_json({"cmd": cmds[i % 3]})
                    if sock.poll(5000) & zmq.POLLIN:
                        sock.recv_json()
                        lat.record("admin", (time.perf_counter() - t) * 1e3)
                    else:
                        lat.error("admin")
                        sock.close(0)
                        sock = ctx.socket(zmq.REQ)
                        sock.setsockopt(zmq.LINGER, 0)
                        sock.setsockopt(zmq.RCVTIMEO, 5000)
                        sock.connect(f"tcp://127.0.0.1:{ports['admin']}")
                except zmq.ZMQError:
                    lat.error("admin")
                    sock.close(0)
                    sock = ctx.socket(zmq.REQ)
                    sock.setsockopt(zmq.LINGER, 0)
                    sock.setsockopt(zmq.RCVTIMEO, 5000)
                    sock.connect(f"tcp://127.0.0.1:{ports['admin']}")
                i += 1
                time.sleep(0.05)

        # --- WebSocket live fanout (measure per-message delivery) ---------
        ws_samples: list[float] = []
        ws_done = threading.Event()

        def ws_worker() -> None:
            try:
                from websockets.sync.client import connect

                with connect(
                    f"ws://127.0.0.1:{ports['http']}/ws/subscribe?metric=stress.m1",
                    max_size=None,
                ) as ws:
                    deadline = time.monotonic() + args.duration + 5
                    while time.monotonic() < deadline and not stop.is_set():
                        try:
                            t = time.perf_counter()
                            ws.recv(timeout=1.0)
                            ws_samples.append((time.perf_counter() - t) * 1e3)
                        except TimeoutError:
                            continue
            except Exception as e:
                print(f"ws_worker: {e}", file=sys.stderr)
            finally:
                ws_done.set()

        threads = [
            threading.Thread(target=zmq_query_worker, args=(w,))
            for w in range(args.clients)
        ]
        threads += [
            threading.Thread(target=http_read_worker, args=(w, w % 2 == 1))
            for w in range(max(2, args.clients // 2))
        ]
        threads += [threading.Thread(target=admin_worker)]
        for t in threads:
            t.start()
        ws_t = threading.Thread(target=ws_worker, daemon=True)
        ws_t.start()

        print(
            f"stress phase: {args.duration}s, {args.clients} zmq + http readers, pump {args.rate}/s"
        )
        time.sleep(args.duration)
        stop.set()
        for t in threads:
            t.join(timeout=10)
        ws_done.wait(timeout=10)

        # Params FIRST: the pump-collect path below writes into it, and a
        # crash before assignment would lose the run's configuration.
        results["params"] = {
            "duration_s": args.duration,
            "rate_per_s": args.rate,
            "zmq_clients": args.clients,
            "warmup_rows": args.warmup_rows,
        }

        # Collect the pump process's actual delivered count (it self-terminates
        # at its own duration deadline; terminate() is just belt-and-braces).
        if pump_proc.poll() is None:
            pump_proc.terminate()
        try:
            out, _ = pump_proc.communicate(timeout=10)
            results["params"]["pumped_actual"] = json.loads(out or b"{}").get(
                "pumped", 0
            )
        except Exception:
            pump_proc.kill()
            pump_proc.communicate()
            results["params"]["pumped_actual"] = -1

        if ws_samples:
            ws_samples.sort()
            k = len(ws_samples)

            def wpct(p: float) -> float:
                return round(ws_samples[min(int(k * p), k - 1)], 3)

            results["websocket_delivery"] = {
                "n": k,
                "p50_ms": wpct(0.50),
                "p90_ms": wpct(0.90),
                "p99_ms": wpct(0.99),
            }

        results["paths"] = lat.report()

        print(json.dumps(results, indent=2))
        if args.json:
            Path(args.json).write_text(json.dumps(results, indent=2))
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())

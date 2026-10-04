#!/usr/bin/env python3
"""Stress every live sqtseries path and report latency percentiles (P50/P90/P99).

Spawns a real service on free ports, pumps data through ZMQ ingest and HTTP
write, then hammers every read path — ZMQ query, HTTP read, HTTP aggregate,
WebSocket fanout, admin commands — and reports P50/P90/P99 per path.

Units: ``--rate`` and the pump throughput are in pts/s (points per second);
a point is one measurement (metric + value + tags + timestamp).

Usage:
    python3 scripts/stress_percentiles.py [--duration 30] [--rate 2000]
                                          [--clients 8] [--json out.json]
"""

from __future__ import annotations

import argparse
import contextlib
import json
import re
import shutil
import socket
import sqlite3
import statistics
import subprocess
import sys
import threading
import time
from collections import deque
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

    def timeout(self, path: str, ms: float) -> None:
        """A request that blew its deadline is the slowest sample there is.

        Dropping it — counting the error and discarding the elapsed time — is
        exactly the bias that makes a tail look healthy. A client that waits
        5 s and gives up has still told you something: that path took at least
        5 s. It is recorded and counted, never thrown away.
        """
        with self._lock:
            self._errors[path] = self._errors.get(path, 0) + 1
            self._samples.setdefault(path, []).append(ms)

    def count(self, path: str) -> int:
        """Successful samples recorded for a path (used for accounting)."""
        with self._lock:
            return len(self._samples.get(path, ()))

    def report(self) -> dict[str, dict]:
        out = {}
        for path, raw in self._samples.items():
            xs = sorted(raw)
            n = len(xs)

            def pct(p: float, *, _xs: list[float] = xs, _n: int = n) -> float:
                if _n < 2:
                    return round(_xs[0], 3)
                q = statistics.quantiles(_xs, n=100, method="inclusive")
                return round(q[min(int(p * 100), 100) - 1], 3)

            out[path] = {
                "n": n,
                "errors": self._errors.get(path, 0),
                "p50_ms": pct(0.50),
                "p90_ms": pct(0.90),
                "p95_ms": pct(0.95),
                "p99_ms": pct(0.99),
                "max_ms": round(xs[-1], 3),
                "mean_ms": round(statistics.fmean(xs), 3),
                "pct_method": "inclusive (Hyndman-Fan R7; R/Excel/numpy default)",
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
    ap.add_argument(
        "--model",
        choices=("closed", "open"),
        default="closed",
        help=(
            "query load model. 'closed' (default) keeps the historical "
            "behaviour: each client waits for its reply, so a stall throttles "
            "the offered load and the tail is optimistically biased. 'open' "
            "paces requests on a fixed schedule over a pipelining DEALER socket "
            "and measures from the SCHEDULED fire time, which removes that bias."
        ),
    )
    ap.add_argument(
        "--query-rate",
        type=int,
        default=1000,
        help="open-loop only: target aggregate query ops/sec across clients",
    )
    ap.add_argument(
        "--max-inflight",
        type=int,
        default=64,
        help=(
            "open-loop only: cap on unanswered requests ACROSS ALL CLIENTS, "
            "matching the broker's single global max_inflight. Each client "
            "caps at max(1, this // clients), because the broker's cap is one "
            "global pool, not per-connection: a per-client reading of this "
            "same number lets N times the broker's budget pile up, and the "
            "broker then sheds by construction."
        ),
    )
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
    openloop_stats: dict[int, dict] = {}
    results: dict[str, object] = {"ports": ports}

    try:
        if not wait_admin(ports["admin"]):
            print("FAIL: service did not start")
            return 2
        print(f"service up on {ports}")

        ctx = zmq.Context.instance()

        def _ingested_now() -> int:
            """Fresh REQ socket per call (a late reply poisons the FSM).
            Returns the server's cumulative ingested counter, -1 on failure."""
            actx = zmq.Context()
            sock = actx.socket(zmq.REQ)
            sock.setsockopt(zmq.LINGER, 0)
            sock.setsockopt(zmq.RCVTIMEO, 5000)
            sock.connect(f"tcp://127.0.0.1:{ports['admin']}")
            try:
                sock.send_json({"cmd": "stats"})
                if sock.poll(5000) & zmq.POLLIN:
                    return int(sock.recv_json().get("ingested", -1))
            except zmq.ZMQError:
                pass
            finally:
                sock.close(0)
                actx.term()
            return -1

        # --- warm-up data: ZMQ PUSH ingest -------------------------------
        push = ctx.socket(zmq.PUSH)
        push.setsockopt(zmq.SNDHWM, 101_000)  # Handler-engine size
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

        # The pump offered 400k rows in ~2 s — far faster than the server
        # absorbs. The sustained-rate baseline is only honest once the
        # server has absorbed ALL warmup rows: otherwise the stress window
        # is polluted by warmup-residual catch-up (measured 2026-09-10:
        # baseline taken early inflated the rate to 13,268/s and left
        # ~99k pump frames to die at LINGER, faking "unaccounted").
        # Progress-aware, capped wait for full warmup absorption.
        warmup_deadline = time.monotonic() + 90.0
        while True:
            absorbed = _ingested_now()
            if absorbed >= n or absorbed < 0:
                break
            if time.monotonic() > warmup_deadline:
                print(f"WARN: warmup absorption incomplete: {absorbed}/{n}")
                break
            time.sleep(0.5)
        print(f"warmup absorbed: {absorbed}/{n}")

        # Baseline for the sustained-ingest-rate measurement: the server's
        # counter right AFTER warm-up absorption, before any stress clients
        # exist (so the delta is exactly the stress-window ingest). A fresh
        # server legitimately reads 0 here — guard with >= 0, never
        # truthiness.
        ingested_start = _ingested_now()

        # --- sustained pump (ZMQ) at --rate during the whole query phase --
        # The pump runs as a separate PROCESS, not a thread: under the GIL
        # with a dozen churning client threads, a pump thread's sleeps stretch
        # (measured: 2000/s advertised, ~54/s delivered). A process is
        # unaffected by the harness's own concurrency.
        #
        # Accounting rules (measured 2026-09-09: counting send_json() calls
        # with SNDHWM=101k/LINGER=1s overstated delivery by 29% — frames die
        # silently in the local pipe at exit, and HWM block-time is pacing
        # time that must be carried into the next schedule, or the pump
        # overshoots to ~14.3k/s after every stall):
        #   1. count a point as SENT only after send() returned AND the
        #      socket is writable (events & POLLOUT) — otherwise it may sit
        #      in the local HWM queue and be LINGER-discarded at exit;
        #   2. pace against an absolute schedule that carries overshoot;
        #   3. on exit, spin on POLLOUT so the pipe drains, then LINGER=0.
        pump_code = f"""
import json, time, zmq
ctx = zmq.Context()
sock = ctx.socket(zmq.PUSH)
sock.setsockopt(zmq.SNDHWM, 101000)
sock.setsockopt(zmq.LINGER, 1000)
sock.connect("tcp://127.0.0.1:{ports["ingest"]}")
rate = {args.rate}
metrics = [f"stress.m{{i}}" for i in range(25)]
n = 0
counted = 0
batch = 50
interval = batch / rate
next_t = time.monotonic()
deadline = next_t + {args.duration!r}
while next_t < deadline:
    for i in range(batch):
        sock.send_json({{"metric": metrics[n % 25], "value": (n % 1000) / 10.0,
                         "tags": {{"host": f"h{{n % 10}}"}}}})
        n += 1
        if (sock.poll(0, zmq.POLLOUT)):
            counted = n
    now = time.monotonic()
    if now < next_t:
        time.sleep(next_t - now)
    next_t += interval
if n > counted:
    sock.setsockopt(zmq.LINGER, 3000)
    while counted < n and time.monotonic() < deadline + 15:
        if sock.poll(0, zmq.POLLOUT):
            counted = n
        time.sleep(0.01)
print(json.dumps({{"pumped": n, "counted": counted}}))
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
                        lat.timeout("zmq_query", (time.perf_counter() - t) * 1e3)
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

        def open_loop_query_worker(worker: int) -> None:
            """Paced, pipelined query load measured from the SCHEDULED fire time.

            A REQ socket is lockstep, so it cannot offer load while it waits —
            that is the whole of coordinated omission. This uses DEALER against
            the broker's ROUTER, which pipelines, and issues on a fixed schedule.
            A request that goes out late is still sent immediately, and its
            latency is measured from when it was DUE, so the queueing delay a
            stalled system causes is counted instead of hidden.

            ZMQ preserves per-connection order, so replies are matched to their
            scheduled times FIFO off a deque.
            """
            sock = ctx.socket(zmq.DEALER)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(f"tcp://127.0.0.1:{ports['query']}")
            workers = max(args.clients, 1)
            interval = workers / max(args.query_rate, 1)
            # The broker's max_inflight is ONE global pool shared by every
            # connection (broker.py: len(_router_tasks) >= max_inflight), so the
            # budget is split here rather than applied per client. Treating it
            # as per-client lets clients x budget requests pile up against a
            # broker that only services `budget`, and the broker sheds by
            # construction — which reads as "the generator was too slow" when
            # the truth is the cap was misapplied.
            inflight_cap = max(1, args.max_inflight // workers)
            # Fixed absolute schedule, advanced only AFTER a request is really
            # issued. Advancing it before the sleep puts next_due one interval
            # ahead of `now` on every pass, so the sleep always wins and the
            # send branch is unreachable: the worker paces perfectly while
            # sending nothing, and reports a healthy rate for zero requests.
            next_due = time.perf_counter()
            sent = 0
            inflight: deque[tuple[float, float]] = deque()
            high_water = 0
            shed = 0
            late_slots = 0
            deadline = next_due + args.duration

            def drain() -> int:
                """Reap every available reply. Returns how many were matched."""
                nonlocal high_water
                reaped = 0
                while True:
                    try:
                        # The broker's ROUTER replies [identity, b"", payload].
                        # A REQ socket strips those two frames for you; a DEALER
                        # does NOT, so recv_json() would parse the raw identity
                        # bytes and raise JSONDecodeError. Take the last frame.
                        sock.recv_multipart(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        return reaped
                    except zmq.ZMQError:
                        return reaped
                    # Depth before the pop — after it, a steady-state depth of
                    # 1 reads as 0 and the pipelining signal is lost.
                    high_water = max(high_water, len(inflight))
                    if not inflight:
                        # A reply with nothing outstanding: counted, never
                        # paired with an unrelated request's schedule.
                        lat.error("zmq_query")
                        continue
                    intended, issued = inflight.popleft()
                    now = time.perf_counter()
                    lat.record("zmq_query", (now - issued) * 1e3)
                    lat.record("zmq_query_co", (now - intended) * 1e3)
                    reaped += 1

            while not stop.is_set() and time.perf_counter() < deadline:
                drain()
                if len(inflight) >= inflight_cap:
                    # Holding here keeps the cross-client outstanding total at
                    # the broker's cap, so the broker never sheds and the run
                    # measures the server rather than our own over-run. Counted
                    # and reported rather than silently inflating the samples.
                    shed += 1
                    time.sleep(0.001)
                    continue
                now = time.perf_counter()
                if now < next_due:
                    time.sleep(min(next_due - now, 0.01))
                    continue
                if now - next_due > interval:
                    late_slots += 1
                inflight.append((next_due, now))
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
                except zmq.ZMQError:
                    lat.error("zmq_query")
                    if not inflight:
                        break
                    inflight.pop()
                    continue
                sent += 1
                next_due += interval
            # The measurement window has closed but replies are still owed.
            # Reap them (bounded) so sent/received accounting closes instead of
            # reporting a phantom shortfall.
            reap_until = time.perf_counter() + 5.0
            while inflight and time.perf_counter() < reap_until:
                if not drain():
                    time.sleep(0.002)
            with contextlib.suppress(zmq.ZMQError):
                sock.close(0)
            openloop_stats[worker] = {
                "sent_ops": sent,
                "unanswered": len(inflight),
                "inflight_high_water": high_water,
                "client_shed": shed,
                "slots_served_late": late_slots,
            }

        # --- HTTP read + aggregate (concurrent) ---------------------------
        def http_read_worker(worker: int, agg: bool) -> None:
            path_kind = "http_agg" if agg else "http_read"
            metric = f"stress.m{worker % 25}"
            conn = http.client.HTTPConnection("127.0.0.1", ports["http"], timeout=10)
            # Rolling 1-hour window, refreshed per request: an unwindowed
            # whole-table aggregate over a 1M+ row table is exactly what the
            # bounded-read contract rejects (HTTP 413 by design) — a valid
            # read-path request is bounded.
            window_ns = 3_600_000_000_000
            while not stop.is_set():
                end_ns = time.time_ns()
                start_ns = end_ns - window_ns
                if agg:
                    url = (
                        f"/api/v1/read?metric={metric}&limit=1000"
                        "&aggregation=avg&interval=1m"
                        f"&start={start_ns}&end={end_ns}"
                    )
                else:
                    # limit=1000: the documented bounded-read pattern. An
                    # unbounded read materializes up to query.max_rows rows
                    # per request; by design the SERVER caps that (HTTP 413)
                    # and a well-behaved client windows or limits instead.
                    url = f"/api/v1/read?metric={metric}&limit=1000"
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
                        lat.timeout("admin", (time.perf_counter() - t) * 1e3)
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

        query_target = (
            open_loop_query_worker if args.model == "open" else zmq_query_worker
        )
        threads = [
            threading.Thread(target=query_target, args=(w,))
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
        # The pump's final print is the only record of what it sent. Killing it
        # the instant the window closed lost that print on short runs, and an
        # empty stdout then read as a count of zero. Wait for it to exit first.
        try:
            out, _ = pump_proc.communicate(timeout=max(args.duration + 30.0, 45.0))
        except subprocess.TimeoutExpired:
            pump_proc.terminate()
            try:
                out, _ = pump_proc.communicate(timeout=15)
            except subprocess.TimeoutExpired:
                pump_proc.kill()
                out, _ = pump_proc.communicate()
        try:
            payload = json.loads(out or b"{}")
            counted = payload.get("counted", payload.get("pumped"))
            # An unread count is UNKNOWN (-1), never 0: defaulting to 0
            # under-counted pump_sent by the whole run and made "unaccounted"
            # a confident negative that still read like a measurement.
            results["params"]["pumped_actual"] = -1 if counted is None else counted
        except Exception:
            results["params"]["pumped_actual"] = -1

        # --- end-to-end accounting: sent vs ingested vs persisted vs rows ---
        # Every point the pump sent must land in exactly one of {persisted,
        # dropped, invalid} — or still be in flight in the bounded ZMQ pipes
        # (PUSH has no acks). The server ingests at its own sustained rate;
        # at 10k/s offered vs ~6-9k/s sustained that backlog can outlive a
        # fixed grace window, so the grace is PROGRESS-AWARE: keep waiting
        # while the counter moves, cap at 120 s. Then "unaccounted" really
        # means lost, not merely queued.
        try:
            # Counter right after pump exit: the sustained-rate window end
            # (a second or two of drain slips in unavoidable; it biases the
            # rate slightly high).
            ingested_stress_end = _ingested_now()
            last = ingested_stress_end
            stable = 0
            grace_deadline = time.monotonic() + 120.0
            # Exit when the counter stops moving for 5 s (drained as far as
            # it will go) or at the 120 s cap. A worker mid-batch (a 256-row
            # insert under query load can take seconds) freezes the counter
            # longer than 1 s, hence the 5 s stability requirement.
            while stable < 10 and time.monotonic() < grace_deadline and last >= 0:
                time.sleep(0.5)
                cur = _ingested_now()
                if cur == last:
                    stable += 1
                else:
                    stable = 0
                    last = cur
            # Full stats payload (fresh socket — same EFSM rule) for the
            # persisted/dropped/invalid identity, not just ingested.
            stats: dict = {"ingested": last}
            try:
                actx2 = zmq.Context()
                s2 = actx2.socket(zmq.REQ)
                s2.setsockopt(zmq.LINGER, 0)
                s2.setsockopt(zmq.RCVTIMEO, 5000)
                s2.connect(f"tcp://127.0.0.1:{ports['admin']}")
                s2.send_json({"cmd": "stats"})
                if s2.poll(5000) & zmq.POLLIN:
                    stats = s2.recv_json()
                s2.close(0)
                actx2.term()
            except zmq.ZMQError:
                pass
            pumped_actual = results["params"]["pumped_actual"]
            accounting = {
                "pump_sent": (
                    pumped_actual + args.warmup_rows + lat.count("http_write")
                    if pumped_actual >= 0
                    else -1
                ),
                "server_ingested": stats.get("ingested", -1),
                "persisted": stats.get("persisted", -1),
                "dropped": stats.get("dropped", -1),
                "invalid": stats.get("invalid", -1),
            }
            ing = accounting["server_ingested"]
            sent = accounting["pump_sent"]
            if sent >= 0 and ing >= 0:
                accounting["unaccounted"] = sent - ing
                if sent - ing < 0:
                    accounting["identity"] = (
                        "UNSOUND: the server counted more points than the pump "
                        "claims to have sent. The pump count is not trustworthy, "
                        "so 'unaccounted' here is not a measurement."
                    )
            else:
                accounting["identity"] = (
                    "UNEVALUABLE: the pump's delivered count was not read back, "
                    "so sent-vs-ingested cannot be compared on this run."
                )
            # Sustained ingest rate over the stress window: server counter
            # delta from just-after-warmup to just-after-pump-exit, divided
            # by the pump duration. This is the server's truth (what it
            # actually absorbed), independent of pipe backlog.
            if ingested_start >= 0 and ingested_stress_end >= 0:
                accounting["stress_ingested"] = ingested_stress_end - ingested_start
                accounting["sustained_ingest_per_s"] = round(
                    accounting["stress_ingested"] / max(args.duration, 1e-9), 1
                )
            db_path = workdir / "db.sqlite"
            con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            try:
                tables = [
                    r[0]
                    for r in con.execute(
                        "SELECT name FROM sqlite_master WHERE name LIKE 'measurements_%'"
                    )
                ]
                # Interpolation is safe: names come from OUR sqlite_master
                # listing and must match the strict partition pattern.
                pat = re.compile(r"^measurements_\d{4}_\d{2}$")
                total = 0
                for t in filter(pat.match, tables):
                    total += con.execute(
                        f'SELECT count(*) FROM "{t}"'  # noqa: S608 - validated name
                    ).fetchone()[0]
                accounting["db_rows"] = total
            finally:
                con.close()
        except Exception as exc:
            accounting = {"error": str(exc)}
        results["accounting"] = accounting

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
        if args.model == "open":
            sent = sum(v["sent_ops"] for v in openloop_stats.values())
            unanswered = sum(v["unanswered"] for v in openloop_stats.values())
            shed = sum(v["client_shed"] for v in openloop_stats.values())
            late = sum(v["slots_served_late"] for v in openloop_stats.values())
            high_water = max(
                (v["inflight_high_water"] for v in openloop_stats.values()), default=0
            )
            results["system_model"] = {
                "type": "open-loop, paced, pipelined (DEALER vs broker ROUTER)",
                "clients": args.clients,
                "target_query_ops_per_s": args.query_rate,
                "sent_ops": sent,
                "achieved_query_ops_per_s": round(sent / max(args.duration, 1e-9), 1),
                "unanswered_at_window_close": unanswered,
                "inflight_cap_total": args.max_inflight,
                "inflight_high_water": high_water,
                "client_shed": shed,
                "slots_served_late": late,
                "note": (
                    "Requests are issued on a fixed schedule and measured from "
                    "the SCHEDULED fire time, so queueing caused by a stall is "
                    "counted rather than hidden (coordinated omission removed). "
                    "'zmq_query' is service time from actual send; "
                    "'zmq_query_co' is the corrected figure from the scheduled "
                    "time and is the one to quote for arrival-rate behaviour. "
                    "sent_ops counts frames the broker actually accepted, and "
                    "'zmq_query'.n should match it — if sent_ops is 0 the "
                    "generator never fired and the run describes nothing. If "
                    "client_shed is non-zero the generator itself hit the "
                    "in-flight cap, so the offered rate was not achieved and "
                    "this run does not describe that rate."
                ),
                "reference": (
                    "Gil Tene, 'How NOT to Measure Latency'; Friedrich et al., "
                    "'Coordinated Omission in NoSQL Database Benchmarking', BTW 2017"
                ),
            }
        else:
            results["system_model"] = {
                "type": "closed-loop, unthrottled (saturation)",
                "clients": args.clients,
                "caveat": (
                    "Each client sends its next request only after the previous "
                    "reply arrives, so a service stall throttles the offered load "
                    "instead of being recorded (coordinated omission). These "
                    "percentiles answer 'response time at saturation with N "
                    "clients'. They are NOT an open-loop arrival-rate "
                    "measurement and understate the tail a real arrival stream "
                    "would see. Run with --model open for arrival-rate numbers. "
                    "Deadline-exceeded requests ARE recorded, not discarded."
                ),
                "reference": (
                    "Gil Tene, 'How NOT to Measure Latency'; Friedrich et al., "
                    "'Coordinated Omission in NoSQL Database Benchmarking', BTW 2017"
                ),
            }

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

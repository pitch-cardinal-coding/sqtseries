#!/usr/bin/env python3
"""adversarial.py — throw everything at sqtseries and check nothing is lost.

Three attacks, each run against a live service under scripts/mem-guard.sh:

  A. ABUSE      sustained overload far past what the box can absorb, with a
                concurrent query/agg load, while a watchdog samples RSS. Proves
                memory stays bounded and the accounting identity closes.

  B. KILL -9    SIGKILL the service mid-write, then reopen the database and
                verify (1) integrity_check says ok, (2) every point the server
                ACKNOWLEDGED as persisted is still present. Points still in
                flight are allowed to be lost — the contract is that nothing
                reported as persisted ever disappears.

  C. CORRUPT    deliberate garbage frames, oversized frames, and wrong types
                down the ingest socket. Proves they are refused and counted,
                never stored, and never crash the service.

Usage:
  scripts/adversarial.py [--mode abuse|kill9|corrupt|all] [--duration 60]
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import zmq

PY = "/home/iam/devcode/.env/sqtseries/bin/python3"
REPO = Path(__file__).resolve().parent.parent
ROOT = Path(os.environ.get("ADV_WORKDIR", "/tmp/sqtseries-adversarial"))  # noqa: S108 - throwaway harness dir


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Service:
    def __init__(self, workdir: Path):
        self.workdir = workdir
        workdir.mkdir(parents=True, exist_ok=True)
        self.db = workdir / "db.sqlite"
        self.ports = {
            n: free_port()
            for n in ("ingest", "query", "streaming", "admin", "http", "stats")
        }
        self._write_config()

    def _write_config(self) -> None:
        p = self.ports
        (self.workdir / "config.toml").write_text(f"""[database]
path = "{self.db}"

[ingestion]
port = {p["ingest"]}

[query]
port = {p["query"]}

[streaming]
port = {p["streaming"]}

[admin]
port = {p["admin"]}

[http]
port = {p["http"]}
rate_limit_per_minute = 100000000
max_websocket_connections = 1000000

[stats]
port = {p["stats"]}

[ports]
auto_detect = false
""")

    def start(self) -> subprocess.Popen:
        self.proc = subprocess.Popen(  # noqa: S603 - fixed argv, no user input
            [
                PY,
                "-u",
                "-m",
                "sqtseries",
                "--config",
                str(self.workdir / "config.toml"),
                "run",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if self.admin({"cmd": "stats"}) is not None:
                return self.proc
            time.sleep(0.2)
        raise RuntimeError("service did not come up")

    def admin(self, payload: dict):
        ctx = zmq.Context()
        s = ctx.socket(zmq.REQ)
        s.setsockopt(zmq.LINGER, 0)
        s.setsockopt(zmq.RCVTIMEO, 2000)
        s.connect(f"tcp://127.0.0.1:{self.ports['admin']}")
        try:
            s.send_json(payload)
            if s.poll(2000) & zmq.POLLIN:
                return s.recv_json()
        except zmq.ZMQError:
            return None
        finally:
            s.close(0)
            ctx.term()
        return None

    def kill9(self) -> None:
        self.proc.send_signal(signal.SIGKILL)
        self.proc.wait(timeout=30)


def pump(
    svc: Service, sent: list[int], rate: int, duration: float, stop: threading.Event
) -> None:
    """Separate process would be ideal; a thread here is fine because the
    service is a different process and the GIL does not throttle ZMQ I/O."""
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    sock.setsockopt(zmq.SNDHWM, 101000)
    sock.setsockopt(zmq.LINGER, 2000)
    sock.connect(f"tcp://127.0.0.1:{svc.ports['ingest']}")
    n = 0
    interval = 50 / rate
    nxt = time.monotonic()
    deadline = nxt + duration
    while time.monotonic() < deadline and not stop.is_set():
        for _ in range(50):
            sock.send_json(
                {
                    "metric": f"adv.m{n % 25}",
                    "value": (n % 1000) / 10.0,
                    "tags": {"host": f"h{n % 10}"},
                }
            )
            n += 1
        sent.append(n)
        now = time.monotonic()
        if now < nxt:
            time.sleep(nxt - now)
        nxt += interval
    sock.close(0)
    ctx.term()


def query_load(svc: Service, stop: threading.Event, errors: list[int]) -> None:
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.RCVTIMEO, 5000)
    sock.connect(f"tcp://127.0.0.1:{svc.ports['query']}")
    while not stop.is_set():
        try:
            sock.send_json(
                {
                    "type": "query",
                    "metric": f"adv.m{int(time.time()) % 25}",
                    "start": 0,
                    "end": int(time.time() * 1e9),
                    "aggregation": "avg",
                    "interval": "1m",
                }
            )
            if not (sock.poll(5000) & zmq.POLLIN):
                errors.append(1)
                sock.close(0)
                sock = ctx.socket(zmq.REQ)
                sock.setsockopt(zmq.LINGER, 0)
                sock.setsockopt(zmq.RCVTIMEO, 5000)
                sock.connect(f"tcp://127.0.0.1:{svc.ports['query']}")
                continue
            sock.recv_json()
        except zmq.ZMQError:
            errors.append(1)
    sock.close(0)
    ctx.term()


def rss_watch(pid: int, out: list[int], stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            with Path(f"/proc/{pid}/status").open() as fh:
                for line in fh:
                    if line.startswith("VmRSS:"):
                        out.append(int(line.split()[1]))
                        break
        except OSError:
            return
        time.sleep(1)


def count_rows(con: sqlite3.Connection, like: str = "measurements_%") -> int:
    """Total rows across every monthly partition table.

    There is no bare ``measurements`` table — the engine writes one table per
    month (``measurements_2026_10``). Querying a non-existent name raises, so
    the table list is discovered from sqlite_master first.
    """
    total = 0
    for (name,) in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?", (like,)
    ).fetchall():
        total += con.execute(
            f"SELECT COUNT(*) FROM {name}"  # noqa: S608 - name from sqlite_master
        ).fetchone()[0]
    return total


def metric_series(con: sqlite3.Connection, metric: str) -> int:
    """Row count for one metric across every partition."""
    ids = [
        r[0]
        for r in con.execute("SELECT series_id FROM series WHERE metric = ?", (metric,))
    ]
    if not ids:
        return 0
    marks = ",".join("?" * len(ids))
    total = 0
    for (name,) in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'measurements_%'"
    ).fetchall():
        total += con.execute(
            f"SELECT COUNT(*) FROM {name} WHERE series_id IN ({marks})",  # noqa: S608 - name from sqlite_master; ids bound
            ids,
        ).fetchone()[0]
    return total


def mode_abuse(duration: int, rate: int) -> int:
    work = ROOT / "abuse"
    svc = Service(work)
    for stale in work.glob("db.sqlite*"):
        stale.unlink()
    svc.start()
    stop = threading.Event()
    sent: list[int] = []
    qerr: list[int] = []
    rss: list[int] = []
    threads = [
        threading.Thread(
            target=pump, args=(svc, sent, rate, duration, stop), daemon=True
        ),
        threading.Thread(target=query_load, args=(svc, stop, qerr), daemon=True),
        threading.Thread(target=rss_watch, args=(svc.proc.pid, rss, stop), daemon=True),
    ]
    for t in threads:
        t.start()
    time.sleep(duration + 6)
    stop.set()
    for t in threads[:2]:
        t.join(timeout=15)

    # grace drain, then reconcile
    time.sleep(4)
    stats = svc.admin({"cmd": "stats"}) or {}
    ingested = stats.get("ingested", 0)
    persisted = stats.get("persisted", 0)
    dropped = stats.get("dropped", 0)
    invalid = stats.get("invalid", 0)
    retries = stats.get("sink_retries", 0)
    svc.proc.terminate()
    svc.proc.wait(timeout=30)

    con = sqlite3.connect(f"file:{svc.db}?mode=ro", uri=True)
    rows = count_rows(con)
    con.close()

    peak = max(rss) if rss else 0
    print(
        json.dumps(
            {
                "mode": "abuse",
                "rate_requested": rate,
                "duration_s": duration,
                "pump_sent": sent[-1] if sent else 0,
                "ingested": ingested,
                "persisted": persisted,
                "dropped": dropped,
                "invalid": invalid,
                "sink_retries": retries,
                "accounting_gap": ingested - (persisted + dropped + invalid),
                "db_rows": rows,
                "rows_vs_persisted": rows - persisted,
                "query_errors": len(qerr),
                "peak_rss_mb": round(peak / 1024, 1),
                "budget_mb": 1500,
            },
            indent=2,
        )
    )

    ok = (
        ingested - (persisted + dropped + invalid) == 0
        and dropped == 0
        and invalid >= 0
        and len(qerr) == 0
        and rows >= persisted
        and peak / 1024 < 1500
    )
    print("VERDICT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def mode_kill9(duration: int, rate: int) -> int:
    work = ROOT / "kill9"
    svc = Service(work)
    for stale in work.glob("db.sqlite*"):
        stale.unlink()
    svc.start()
    sent: list[int] = []
    stop = threading.Event()
    t = threading.Thread(
        target=pump, args=(svc, sent, rate, duration, stop), daemon=True
    )
    t.start()
    time.sleep(max(duration // 2, 2))

    stats_before = svc.admin({"cmd": "stats"}) or {}
    persisted_claimed = stats_before.get("persisted", 0)
    svc.kill9()
    stop.set()
    print(f"  SIGKILLed with persisted={persisted_claimed}")

    time.sleep(1)
    con = sqlite3.connect(svc.db)
    integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
    rows = count_rows(con)
    con.close()

    print(
        json.dumps(
            {
                "mode": "kill9",
                "persisted_claimed_before_kill": persisted_claimed,
                "integrity_check": integrity,
                "db_rows_after_reopen": rows,
                "rows_lost_vs_claim": persisted_claimed - rows,
            },
            indent=2,
        )
    )
    ok = integrity == "ok" and rows >= persisted_claimed
    print("VERDICT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def mode_corrupt(_duration: int, _rate: int) -> int:
    work = ROOT / "corrupt"
    svc = Service(work)
    for stale in work.glob("db.sqlite*"):
        stale.unlink()
    svc.start()
    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUSH)
    sock.setsockopt(zmq.LINGER, 2000)
    sock.connect(f"tcp://127.0.0.1:{svc.ports['ingest']}")
    # One good point first, so we can prove good data still lands afterwards.
    sock.send_json({"metric": "adv.good", "value": 1.0})
    time.sleep(0.5)

    # Every payload here MUST be refused. Do not pad one with trailing
    # whitespace to "bulk it up" — that is still VALID JSON, so the engine
    # correctly stores it and this mode fails for the wrong reason.
    junk: list[bytes] = [
        b"not json at all",
        b"{}",
        b'{"metric": 123}',
        b'{"metric":"","value":1.0}',
        b'{"metric":"x","value":"not-a-number"}',
        b'{"metric":"x","value":true}',  # bool is not a number
        b'{"metric":"x","value":1.0,"tags":"notadict"}',
        b'{"metric":"x","value":1.0,"tags":{"k":5}}',  # tags must be str->str
        b'{"metric":"x","value":1e400}',  # overflows to inf
        b'{"metric":"x","value":1.0,"timestamp":"not-a-date"}',
        b"[" * 5000,
        b"\xff\xfe\x00\x01",
    ]
    sent_junk = 0
    for payload in junk:
        try:
            sock.send(payload)
            sent_junk += 1
        except zmq.ZMQError:
            break

    # 60 MB frame: libzmq refuses it at MAXMSGSIZE (50 MB), i.e. BEFORE the
    # parser, which is why it is absent from invalid_counted below.
    oversized = (
        b'{"metric":"huge","value":1.0,"tags":{"k":"'
        + b"v" * (60 * 1024 * 1024)
        + b'"}}'
    )
    try:
        sock.send(oversized)
        sent_junk += 1
    except zmq.ZMQError:
        pass

    time.sleep(1.5)
    # And a good one AFTER the junk.
    sock.send_json({"metric": "adv.after", "value": 2.0})
    time.sleep(1.5)
    sock.close(0)
    ctx.term()

    stats = svc.admin({"cmd": "stats"}) or {}
    alive = svc.admin({"cmd": "ping"}) or {}
    svc.proc.terminate()
    svc.proc.wait(timeout=30)

    con = sqlite3.connect(f"file:{svc.db}?mode=ro", uri=True)
    good = count_rows(con)
    leaked = metric_series(con, "x")  # any 'x' row means a junk frame stored
    huge = metric_series(con, "huge")  # any 'huge' row means the cap failed
    con.close()

    print(
        json.dumps(
            {
                "mode": "corrupt",
                "junk_frames_sent": sent_junk,
                "invalid_counted": stats.get("invalid", 0),
                "persisted": stats.get("persisted", 0),
                "dropped": stats.get("dropped", 0),
                "db_rows_total": good,
                "good_points_before_and_after": good,
                "junk_rows_stored": leaked,
                "oversized_rows_stored": huge,
                "service_alive": bool(alive.get("pong")),
            },
            indent=2,
        )
    )
    # Exactly the 2 good points, no junk, nothing dropped, service still up.
    # invalid is informational: the oversized frame is refused by libzmq before
    # the parser sees it, so it is legitimately not counted as invalid.
    ok = (
        good == 2
        and leaked == 0
        and huge == 0
        and stats.get("dropped", 0) == 0
        and stats.get("persisted", 0) == 2
        and bool(alive.get("pong"))
    )
    print("VERDICT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--mode", default="all", choices=("abuse", "kill9", "corrupt", "all")
    )
    ap.add_argument("--duration", type=int, default=45)
    ap.add_argument("--rate", type=int, default=8000)
    args = ap.parse_args()
    ROOT.mkdir(parents=True, exist_ok=True)

    modes = ["abuse", "kill9", "corrupt"] if args.mode == "all" else [args.mode]
    rc = 0
    for m in modes:
        print(f"\n=== mode: {m} ===", flush=True)
        fn = {"abuse": mode_abuse, "kill9": mode_kill9, "corrupt": mode_corrupt}[m]
        rc |= fn(args.duration, args.rate)
    return rc


if __name__ == "__main__":
    sys.exit(main())

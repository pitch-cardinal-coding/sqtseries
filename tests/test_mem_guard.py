"""Tests for scripts/mem-guard.sh — the HARD memory governor.

Contract pinned here (2026-09-10 incident: an unbounded stress run froze the
PC via OOM):
  - preflight REFUSES to start when the host cannot afford the budget (exit 3)
  - a command that stays inside its budget runs to completion untouched (rc 0)
  - a command that blows through the budget is SIGKILLed with the whole tree
    and the guard exits 42 — BEFORE the host can be pushed into OOM.

All breach tests use tiny budgets and fixed-size hogs so they exercise the
kill path quickly and never stress the host machine itself.
"""

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
GUARD = REPO / "scripts" / "mem-guard.sh"
# The interpreter running pytest: portable across dev box and deployed hosts
# (a hardcoded dev-venv path made these tests fail on any other machine).
PY = sys.executable


def run_guard(
    *args: str, cmd: list[str], timeout: float = 120
) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(GUARD), *args, "--", *cmd],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=REPO,
    )


def test_preflight_refuses_impossible_budget() -> None:
    proc = run_guard("--budget-mb", "999999", "--name", "pytest-t3", cmd=["/bin/true"])
    assert proc.returncode == 3
    assert "REFUSING" in proc.stdout


def test_within_budget_runs_to_completion() -> None:
    proc = run_guard(
        "--budget-mb",
        "500",
        "--floor-mb",
        "500",
        "--name",
        "pytest-t1",
        cmd=[PY, "-c", "x = bytearray(300*1024*1024); print('held')"],
    )
    assert proc.returncode == 0
    assert "held" in proc.stdout
    assert "BREACH" not in proc.stdout


def test_breach_kills_tree_and_exits_42() -> None:
    # 1.5 GB hog vs a 500 MB budget: the guard must kill it within ~a poll
    # interval — the sleep(60) proves it was SIGKILLed, not timed out.
    proc = run_guard(
        "--budget-mb",
        "500",
        "--floor-mb",
        "500",
        "--name",
        "pytest-t2",
        cmd=[PY, "-c", "x = bytearray(1500*1024*1024)\nimport time\ntime.sleep(60)"],
        timeout=60,
    )
    assert proc.returncode == 42
    assert "BREACH" in proc.stdout
    assert "KILLING" in proc.stdout


def test_breach_detected_when_tree_contains_a_zombie() -> None:
    # A zombie has a readable /proc/<pid>/status with NO VmRSS line, so awk
    # exits 0 printing nothing and a `|| echo 0` guard never fires. The empty
    # operand then aborts the sum and tree_rss_kb UNDER-reports — enough to miss
    # the breach outright: measured 1715MB against a 400MB budget reported
    # "peak RSS stayed under 400MB", rc 0, nothing killed. Zombies are
    # unavoidable in real runs (any subprocess that outlives its parent for a
    # moment), and the other breach tests here have no zombie in the tree,
    # which is exactly why they passed against the broken guard too.
    code = (
        "import os, time\n"
        "hog = bytearray(700*1024*1024)\n"
        "z = os.fork()\n"
        "if z == 0:\n"
        "    os._exit(0)\n"  # exits immediately, never reaped -> zombie
        "k = os.fork()\n"
        "if k == 0:\n"
        "    b = bytearray(300*1024*1024)\n"
        "    time.sleep(60)\n"
        "    os._exit(0)\n"
        "print('ready', flush=True)\n"
        "time.sleep(60)\n"
    )
    proc = run_guard(
        "--budget-mb",
        "400",
        "--floor-mb",
        "400",
        "--name",
        "pytest-tzombie",
        cmd=[PY, "-c", code],
        timeout=60,
    )
    assert proc.returncode == 42, (
        f"guard missed the breach: rc={proc.returncode}\n{proc.stdout}"
    )
    assert "BREACH" in proc.stdout
    assert "arithmetic syntax error" not in proc.stderr


def test_warns_when_host_floor_becomes_unreadable(tmp_path) -> None:
    # The preflight already refuses loudly when MemAvailable is unreadable at
    # startup (exit 3). The in-loop read trusted it to stay readable, and an
    # empty read made `[ "" -lt N ]` fail with rc=2 — silently SKIPPING the
    # floor for that poll, i.e. the desktop protection quietly switching itself
    # off while still looking like protection. A healthy host cannot reach that
    # branch, so drive it with a patched copy whose read succeeds at preflight
    # and fails once the command trips a sentinel: it must warn out loud, and it
    # must NOT kill a tree that is inside its budget.
    sentinel = tmp_path / "break-meminfo"
    guard = tmp_path / "guard.sh"
    replacement = (
        "mem_available_mb() {\n"
        f'    [ -f "{sentinel}" ] && return 0\n'
        "    awk '/^MemAvailable:/ {print int($2/1024)}' /proc/meminfo 2>/dev/null\n"
        "}\n"
    )
    patched, n = re.subn(
        r"mem_available_mb\(\) \{.*?\n\}",
        replacement,
        GUARD.read_text(),
        count=1,
        flags=re.S,
    )
    assert n == 1, "mem_available_mb not found — this test needs updating"
    guard.write_text(patched)
    guard.chmod(0o755)

    code = (
        "import pathlib, time\n"
        "print('ready', flush=True)\n"
        "time.sleep(1)\n"
        f"pathlib.Path({str(sentinel)!r}).touch()\n"
        "time.sleep(6)\n"
    )
    proc = subprocess.run(
        [
            str(guard),
            "--budget-mb",
            "500",
            "--floor-mb",
            "500",
            "--name",
            "pytest-tfloor",
            "--",
            PY,
            "-c",
            code,
        ],
        capture_output=True,
        text=True,
        timeout=90,
        cwd=REPO,
    )
    assert "NOT being enforced" in proc.stdout, (
        f"guard skipped the host floor silently\nstdout:\n{proc.stdout}"
    )
    assert proc.returncode == 0, (
        f"guard killed a tree inside its budget: rc={proc.returncode}\n{proc.stdout}"
    )


def test_breach_kills_whole_tree_including_grandchildren() -> None:
    # Parent python spawns a CHILD that holds the memory — the guard must
    # kill the full tree (parent + child), not just the direct command.
    parent_code = (
        "import subprocess, sys, time\n"
        "p = subprocess.Popen([sys.executable, '-c', "
        "'x = bytearray(1200*1024*1024); import time; time.sleep(60)'])\n"
        "time.sleep(60)\n"
    )
    proc = run_guard(
        "--budget-mb",
        "500",
        "--floor-mb",
        "500",
        "--name",
        "pytest-t4",
        cmd=[PY, "-c", parent_code],
        timeout=60,
    )
    assert proc.returncode == 42
    assert "BREACH" in proc.stdout
    # Nothing from the tree may survive the guard.
    pgrep = subprocess.run(
        ["/usr/bin/pgrep", "-af", "python"], capture_output=True, text=True
    )
    assert "bytearray(1200" not in pgrep.stdout

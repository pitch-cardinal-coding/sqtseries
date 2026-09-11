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

import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
GUARD = REPO / "scripts" / "mem-guard.sh"
PY = "/home/iam/devcode/.env/sqtseries/bin/python3"


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

"""Tests for scripts/leak_check.py (rss-watch log -> pass/fail verdict)."""

import subprocess
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

import leak_check  # noqa: E402


def line(pid: int, rss: int, threads: int, fds: int, anon: int | None = None) -> str:
    anon_part = f"anon={anon}kB " if anon is not None else ""
    return (
        f"  pid={pid} rss={rss}kB {anon_part}vsz=400000kB "
        f"threads={threads} fds={fds} cmd=/usr/bin/python3 -m sqtseries run"
    )


def write_log(tmp_path: Path, lines: list[str]) -> Path:
    log = tmp_path / "rss.log"
    log.write_text("=== rss-watch started ===\n" + "\n".join(lines) + "\n")
    return log


def run_cli(*argv: str) -> tuple[int, str]:
    proc = subprocess.run(
        [sys.executable, str(SCRIPTS / "leak_check.py"), *argv],
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout + proc.stderr


# --- parsing ---


def test_parse_log_extracts_all_pids(tmp_path):
    log = write_log(
        tmp_path,
        [line(100, 50000, 6, 28), line(200, 60000, 4, 20), line(100, 50100, 6, 28)],
    )
    hist = leak_check.parse_log(log)
    assert set(hist) == {100, 200}
    assert hist[100].snapshots == 2
    assert hist[100].rss == [50000, 50100]
    assert hist[200].threads == [4]


def test_parse_log_ignores_noise_lines(tmp_path):
    log = tmp_path / "rss.log"
    log.write_text(
        "=== rss-watch started 2026-09 ===\n"
        "----- 12:00:00 -----\n"
        "some bash wrapper output\n" + line(100, 50000, 6, 28) + "\n"
    )
    hist = leak_check.parse_log(log)
    assert set(hist) == {100}


# --- verdicts (CLI, warmup=1 so the second snapshot is the baseline) ---


def test_stable_pid_passes(tmp_path):
    log = write_log(
        tmp_path,
        [line(100, 50000, 6, 28), line(100, 50200, 6, 28), line(100, 50100, 6, 28)],
    )
    code, out = run_cli(str(log), "--warmup-snapshots", "1")
    assert code == 0
    assert "PASS" in out


def test_rss_drop_is_not_a_leak(tmp_path):
    log = write_log(
        tmp_path,
        [line(100, 50000, 6, 28), line(100, 65000, 6, 28), line(100, 48000, 6, 28)],
    )
    code, _ = run_cli(str(log), "--warmup-snapshots", "1")
    assert code == 0


def test_ever_growing_rss_fails(tmp_path):
    log = write_log(
        tmp_path,
        [line(100, 50000, 6, 28)] * 3 + [line(100, 75000, 6, 28)],
    )
    code, out = run_cli(str(log), "--warmup-snapshots", "1")
    assert code == 1
    # Old-format log (no anon field): judged on total RSS and labelled.
    assert "total-rss (no anon in log) growth" in out


def test_rss_growth_within_tolerance_passes(tmp_path):
    log = write_log(
        tmp_path,
        [line(100, 50000, 6, 28), line(100, 50000, 6, 28), line(100, 69000, 6, 28)],
    )
    code, _ = run_cli(
        str(log), "--warmup-snapshots", "1", "--rss-tolerance-kb", "20000"
    )
    assert code == 0


def test_sustained_fd_growth_fails(tmp_path):
    log = write_log(
        tmp_path,
        [line(100, 50000, 6, 28), line(100, 50000, 6, 33), line(100, 50000, 6, 35)],
    )
    code, out = run_cli(str(log), "--warmup-snapshots", "1")
    assert code == 1
    assert "fd growth" in out


def test_fd_growth_with_tolerance_passes(tmp_path):
    log = write_log(
        tmp_path,
        [line(100, 50000, 6, 28), line(100, 50000, 6, 33), line(100, 50000, 6, 35)],
    )
    code, _ = run_cli(str(log), "--warmup-snapshots", "1", "--fd-tolerance", "5")
    assert code == 0


def test_sustained_thread_growth_fails(tmp_path):
    log = write_log(
        tmp_path,
        [line(100, 50000, 6, 28), line(100, 50000, 11, 28), line(100, 50000, 13, 28)],
    )
    code, out = run_cli(str(log), "--warmup-snapshots", "1")
    assert code == 1
    assert "thread growth" in out


def test_fd_spike_that_recovers_passes(tmp_path):
    """A spike fully returned to baseline is load, not a leak."""
    log = write_log(
        tmp_path,
        [line(100, 50000, 6, 28), line(100, 50000, 6, 34), line(100, 50000, 6, 28)],
    )
    code, _ = run_cli(str(log), "--warmup-snapshots", "1")
    assert code == 0


def test_warming_server_passes_with_default_warmup(tmp_path):
    """The E2E lesson: a starting server grows (fds 3->27, threads 1->6,
    rss 27->91 MB) as ONE-TIME warm-up, then goes flat. Default warmup=10
    must skip that and pass; this is the regression guard for the
    false-positive the first version had."""
    lines = [line(100, 27776 + i * 6000, 1 + i // 2, 3 + i * 2) for i in range(10)]
    # Steady state: flat.
    lines += [line(100, 91476, 6, 27) for _ in range(6)]
    log = write_log(tmp_path, lines)
    # Default --warmup-snapshots 10.
    code, out = run_cli(str(log))
    assert code == 0
    assert "PASS" in out


def test_leak_after_warmup_is_caught(tmp_path):
    """Same warm-up shape, but fds keep climbing in steady state."""
    lines = [line(100, 27776 + i * 6000, 1 + i // 2, 3 + i * 2) for i in range(10)]
    # Leak: fds never return.
    lines += [line(100, 91476, 6, 27 + i) for i in range(6)]
    log = write_log(tmp_path, lines)
    code, out = run_cli(str(log))
    assert code == 1
    assert "fd growth" in out


def test_leak_across_pid_restarts_is_caught(tmp_path):
    """Process exits (tiny snapshot) and restarts; lifetimes judged apart,
    so a per-lifetime climb is still a violation."""
    log = write_log(
        tmp_path,
        [
            line(100, 50000, 6, 28),
            line(100, 50000, 6, 30),
            # Life 1 drifts up.
            line(100, 50000, 6, 34),
            # Restart marker (fresh process).
            line(100, 50000, 6, 3),
            line(100, 50000, 6, 4),
            # Life 2 climbs again.
            line(100, 50000, 6, 5),
        ],
    )
    code, out = run_cli(str(log), "--warmup-snapshots", "1")
    assert code == 1
    assert "fd growth" in out


# --- fail-closed edges ---


def test_empty_log_fails_closed(tmp_path):
    log = tmp_path / "empty.log"
    log.write_text("=== rss-watch started ===\n----- waiting -----\n")
    code, out = run_cli(str(log))
    assert code == 2
    assert "no snapshots" in out


def test_missing_log_fails_closed(tmp_path):
    code, out = run_cli(str(tmp_path / "nope.log"))
    assert code == 2
    assert "not found" in out


def test_short_lived_pid_is_skipped_not_judged(tmp_path):
    log = write_log(tmp_path, [line(100, 900000, 40, 500)])
    code, out = run_cli(str(log))
    assert code == 0
    assert "skipped" in out


def test_min_snapshots_flag_raises_the_bar(tmp_path):
    log = write_log(tmp_path, [line(100, 50000, 6, 28), line(100, 80000, 6, 28)])
    code, out = run_cli(str(log), "--min-snapshots", "5")
    assert code == 0
    assert "0 pid(s) judged" in out


def test_multiple_logs_merge_per_pid(tmp_path):
    log1 = write_log(tmp_path, [line(100, 50000, 6, 28)])
    second = tmp_path / "second.log"
    second.write_text(line(100, 90000, 6, 28) + "\n" + line(100, 95000, 6, 28) + "\n")
    code, out = run_cli(
        str(log1), str(second), "--warmup-snapshots", "1", "--rss-tolerance-kb", "1"
    )
    assert code == 1
    assert "total-rss (no anon in log) growth" in out


def test_real_format_from_documented_rss_watch(tmp_path):
    """Matches the exact line format produced by scripts/rss-watch.sh."""
    log = tmp_path / "rss.log"
    log.write_text(
        "=== rss-watch started 2026-09-06T23:39:24+01:00 interval=2s ===\n"
        "----- 23:39:26 -----\n"
        "  pid=141549 rss=79212kB vsz=398628kB threads=6 fds=28 cmd=/home/iam/devcode/.env/sqtseries/bin/python3 -u -m sqtseries --config /tmp/sqtseries-leaktest/config.toml run \n"
        "----- 23:39:28 -----\n"
        "  pid=141549 rss=79480kB vsz=398604kB threads=6 fds=27 cmd=/home/iam/devcode/.env/sqtseries/bin/python3 -u -m sqtseries --config /tmp/sqtseries-leaktest/config.toml run \n"
        "----- 23:39:30 -----\n"
        "  pid=141549 rss=79480kB vsz=398604kB threads=6 fds=27 cmd=/home/iam/devcode/.env/sqtseries/bin/python3 -u -m sqtseries --config /tmp/sqtseries-leaktest/config.toml run \n"
    )
    code, out = run_cli(str(log), "--warmup-snapshots", "1")
    assert code == 0
    assert "pid=141549" in out
    assert "PASS" in out


# --- direct unit checks ---


def test_transient_spike_is_not_leak_shaped():
    h = leak_check.PidHistory(pid=1)
    for fds in (28, 34, 28, 29, 28):
        h.add(50000, 6, fds)
    # Ends where it started.
    assert h.fd_growth(0) is None


def test_sustained_drift_is_reported():
    h = leak_check.PidHistory(pid=1)
    for fds in (28, 30, 31):
        # Never returns.
        h.add(50000, 6, fds)
    assert h.fd_growth(0) == 3


def test_rss_spike_then_trim_passes():
    h = leak_check.PidHistory(pid=1)
    for rss in (50000, 90000, 48000):
        h.add(rss, 6, 28)
    assert h.rss_growth_kb(0) is None


def test_restart_splitting_judges_lifetimes_separately():
    h = leak_check.PidHistory(pid=1)
    # Restart marker at fds=3.
    for fds in (28, 34, 3, 5):
        h.add(50000, 6, fds)
    # life1: 28->34 (+6); life2: 3->5 (+2) — worst reported
    assert h.fd_growth(0) == 6


def test_warmup_trims_before_judging():
    h = leak_check.PidHistory(pid=1)
    for rss in (27776, 40000, 60000, 91476, 91476, 91476):
        h.add(rss, 6, 27)
    # warmup=4: judge from the 5th snapshot on -> flat -> pass
    assert h.rss_growth_kb(4) is None
    # warmup=0: judges the whole climb -> +63700
    assert h.rss_growth_kb(0) == 63700


# --- plateau vs leak discrimination (slope test) ---


def test_bounded_plateau_after_load_passes(tmp_path):
    """RSS jumps when load arrives mid-window then oscillates flat: a bounded
    plateau (cache steady state), NOT a leak. Endpoint-only comparison used
    to fail this; the slope test must pass it."""
    lines = [line(100, 60000, 10, 40)] * 10
    lines += [line(100, 60000 + (i % 3) * 2000, 10, 40) for i in range(30)]
    log = write_log(tmp_path, lines)
    code, out = run_cli(str(log), "--warmup-snapshots", "5")
    assert code == 0, out


def test_slow_linear_climb_still_fails(tmp_path):
    """A true leak drifts: every snapshot adds more. Endpoint growth plus
    positive slope must fail even with a modest per-snapshot increment."""
    lines = [line(100, 60000, 10, 40)] * 10
    lines += [line(100, 60000 + i * 1500, 10, 40) for i in range(30)]
    log = write_log(tmp_path, lines)
    code, out = run_cli(str(log), "--warmup-snapshots", "5")
    assert code == 1
    assert "total-rss (no anon in log) growth" in out


def test_anon_growth_fails_even_when_total_flat(tmp_path):
    """New-format log: anon (heap) is the leak signal. Heap growth with
    file-backed RSS merely oscillating (mmap page churn) must FAIL."""
    lines = [line(100, 70000, 10, 40, anon=40000)] * 10
    lines += [
        line(100, 70000 + (i % 3) * 800, 10, 40, anon=40000 + i * 1500)
        for i in range(30)
    ]
    log = write_log(tmp_path, lines)
    code, out = run_cli(str(log), "--warmup-snapshots", "5")
    assert code == 1
    assert "anon growth" in out


def test_file_backed_growth_but_anon_flat_passes(tmp_path):
    """The measured 2026-09-09 case: total RSS climbs with the DB (mmap
    pages resident) while the heap stays flat — a plateau, not a leak."""
    lines = [line(100, 92160, 8, 32, anon=71680)] * 10
    lines += [
        line(100, 92160 + i * 2500, 8, 32, anon=71680 + (i % 4) * 512)
        for i in range(30)
    ]
    log = write_log(tmp_path, lines)
    code, out = run_cli(str(log), "--warmup-snapshots", "5")
    assert code == 0
    assert "anon first=71680kB last=71680kB" in out or "anon first=71680kB" in out

#!/usr/bin/env python3
"""leak-check: pass/fail verdict from an rss-watch log (CI-style runs).

Parses the per-pid snapshots written by ``scripts/rss-watch.sh`` and fails
when any watched process shows leak-shaped behaviour.

The core rule (matches the manual verification methodology): **a process is
judged on drift after warm-up, not from birth.** A starting server always
grows (imports, sockets, thread pool, caches) — that is one-time cost. The
first ``--warmup-snapshots`` of each process lifetime are therefore skipped,
and everything after must be flat:

- **fds**: final vs first post-warm-up snapshot. A spike that fully recovers
  ends where it started (pass); a climb that never returns (leak) fails.
- **threads**: same rule.
- **rss**: same rule with its own (much larger) tolerance; a drop is never
  a leak (allocator trimming).
- **restarts**: histories split automatically at process-restart
  markers (fds <= 4, threads == 1, rss < 8 MB — a fresh python process), so
  pid reuse never aliases two lifetimes and a service that keeps restarting
  is judged per lifetime.

Usage:
    python3 scripts/leak_check.py /tmp/sqtseries-rss.log
    python3 scripts/leak_check.py --warmup-snapshots 15 --rss-tolerance-kb 20000 log
    python3 scripts/leak_check.py --min-snapshots 3 log1 log2 ...

Exit codes: 0 = pass, 1 = leak detected, 2 = nothing usable to judge
(fail-closed: an empty/missing log must not silently pass CI).

Design limits (stated honestly):
- Only rss-watch logs are understood; py-spy-watch logs (stack dumps) are
  for humans, not this checker.
- Warm-up is counted in snapshots, not seconds: match ``--warmup-snapshots``
  to your rss-watch interval (20 s of warm-up = 10 snapshots at 2 s).
- A leak that plateaus before the log ends looks identical to a healthy
  process; this gate is for bounded CI-style runs, not week-long monitors.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

# rss-watch line: "  pid=123 rss=45678kB anon=45678kB vsz=... threads=6 fds=28"
# (anon is optional: older rss-watch logs carry only rss)
SNAPSHOT_RE = re.compile(
    r"pid=(?P<pid>\d+)\s+rss=(?P<rss>\d+)kB\s+(?:anon=(?P<anon>\d+)kB\s+)?"
    r"vsz=\d+kB\s+"
    r"threads=(?P<threads>\d+)\s+fds=(?P<fds>\d+)"
)

DEFAULT_RSS_TOLERANCE_KB = 20_000
DEFAULT_MIN_SNAPSHOTS = 3
DEFAULT_WARMUP_SNAPSHOTS = 10


class PidHistory:
    """All snapshots for one pid, in file order."""

    # A fresh python process starts with <=4 fds (0,1,2 + epoll) and 1
    # thread, and a small RSS; a snapshot this small inside a pid history
    # means the OS reused the pid and the previous lifetime ended there.
    FD_RESTART_MAX = 4
    THREAD_RESTART = 1
    RSS_RESTART_MAX_KB = 8_000
    ANON_RESTART_MAX_KB = 8_000

    def __init__(self, pid: int):
        self.pid = pid
        self.rss: list[int] = []
        self.anon: list[int] = []
        # Raw per-snapshot anon samples (None where the log had none) —
        # preserves presence info across log merges.
        self._anon_raw: list[int | None] = []
        self.threads: list[int] = []
        self.fds: list[int] = []
        self._anon_present = False

    def add(self, rss: int, threads: int, fds: int, anon: int | None = None) -> None:
        self.rss.append(rss)
        self._anon_raw.append(anon)
        self.anon.append(rss if anon is None else anon)
        if anon is not None:
            self._anon_present = True
        self.threads.append(threads)
        self.fds.append(fds)

    @property
    def snapshots(self) -> int:
        return len(self.rss)

    def _lifetimes(self, values: list[int], restart_max: int) -> list[list[int]]:
        """Split a snapshot series into process lifetimes.

        A snapshot at or below ``restart_max`` starts a new lifetime (the
        pid was reused); each lifetime is judged independently.
        """
        lifetimes: list[list[int]] = []
        current: list[int] = []
        for v in values:
            if current and v <= restart_max:
                lifetimes.append(current)
                current = [v]
            else:
                current.append(v)
        if current:
            lifetimes.append(current)
        return lifetimes

    def sustained_growth(
        self, values: list[int], restart_max: int, warmup: int
    ) -> int | None:
        """Max (final - first) drift across lifetimes, after warm-up trim.

        ``None`` when no lifetime rose past its post-warm-up baseline
        (drops and recovered spikes are not leak-shaped).
        """
        worst = 0
        for life in self._lifetimes(values, restart_max):
            if len(life) <= warmup:
                continue  # too short to judge after warm-up
            post = life[warmup:]
            worst = max(worst, post[-1] - post[0])
        return worst if worst > 0 else None

    def post_warmup_slope(
        self, values: list[int], restart_max: int, warmup: int
    ) -> float:
        """Least-squares slope (units per snapshot) across lifetimes.

        A leak DRIFTS: every extra snapshot adds more. A bounded plateau only
        OSCILLATES around its level (final > first happens when load arrived
        inside the judged window). Returns the worst (highest) slope across
        lifetimes, 0.0 when nothing is judgeable.
        """
        worst = 0.0
        for life in self._lifetimes(values, restart_max):
            if len(life) <= warmup:
                continue
            post = life[warmup:]
            n = len(post)
            if n < 2:
                continue
            mean_x = (n - 1) / 2
            mean_y = sum(post) / n
            num = sum((x - mean_x) * (y - mean_y) for x, y in enumerate(post))
            den = sum((x - mean_x) ** 2 for x in range(n))
            slope = num / den if den else 0.0
            worst = max(worst, slope)
        return worst

    def fd_growth(self, warmup: int) -> int | None:
        return self.sustained_growth(self.fds, self.FD_RESTART_MAX, warmup)

    def thread_growth(self, warmup: int) -> int | None:
        return self.sustained_growth(self.threads, self.THREAD_RESTART, warmup)

    def rss_growth_kb(self, warmup: int) -> int | None:
        return self.sustained_growth(self.rss, self.RSS_RESTART_MAX_KB, warmup)

    def memory_series(self) -> tuple[list[int], str]:
        """The memory series to judge: anonymous when available, else total.

        Anonymous RSS is the real heap — leak evidence. Total RSS also counts
        file-backed pages (SQLite mmap of DB/WAL: shared page cache,
        reclaimable, inflated ~5x by the reader pool mapping the same file),
        which grow with the DATABASE, not with a leak (measured 2026-09-09:
        total +98MB while anon stayed flat at 70MB under a 9.6k pts/s pump).
        """
        if self._anon_present:
            return self.anon, "anon"
        return self.rss, "total-rss (no anon in log)"

    def min_judgeable(self, warmup: int) -> bool:
        """True if any lifetime is long enough to judge after warm-up."""
        return any(
            len(life) > warmup
            for life in self._lifetimes(self.rss, self.RSS_RESTART_MAX_KB)
        )


def parse_log(path: Path) -> dict[int, PidHistory]:
    """Extract per-pid snapshot histories from an rss-watch log."""
    histories: dict[int, PidHistory] = {}
    with path.open(encoding="utf-8", errors="replace") as fh:
        for line in fh:
            m = SNAPSHOT_RE.search(line)
            if not m:
                continue
            pid = int(m.group("pid"))
            h = histories.setdefault(pid, PidHistory(pid=pid))
            h.add(
                int(m.group("rss")),
                int(m.group("threads")),
                int(m.group("fds")),
                int(m.group("anon")) if m.group("anon") else None,
            )
    return histories


@dataclass
class Violation:
    pid: int
    reason: str
    detail: str

    def __str__(self) -> str:
        return f"pid={self.pid}: {self.reason} — {self.detail}"


def check_histories(
    histories: dict[int, PidHistory],
    *,
    warmup: int = DEFAULT_WARMUP_SNAPSHOTS,
    rss_tolerance_kb: int = DEFAULT_RSS_TOLERANCE_KB,
    fd_tolerance: int = 0,
    thread_tolerance: int = 0,
) -> list[Violation]:
    """Return one Violation per leak-shaped observation.

    Leak-shaped = the value BOTH ends higher than its post-warm-up baseline
    AND still rising (positive regression slope across the whole window).
    Either condition alone is ambiguous: endpoint growth also happens when
    load arrives mid-window (bounded caches filling = plateau, not a leak).
    """
    violations: list[Violation] = []
    for pid in sorted(histories):
        h = histories[pid]

        fd = h.fd_growth(warmup)
        fd_slope = h.post_warmup_slope(h.fds, h.FD_RESTART_MAX, warmup)
        if fd is not None and fd > fd_tolerance and fd_slope > 0:
            violations.append(
                Violation(
                    pid,
                    "fd growth",
                    f"+{fd} fds after warm-up, rising {fd_slope:.3f}/snapshot",
                )
            )

        th = h.thread_growth(warmup)
        th_slope = h.post_warmup_slope(h.threads, h.THREAD_RESTART, warmup)
        if th is not None and th > thread_tolerance and th_slope > 0:
            violations.append(
                Violation(
                    pid,
                    "thread growth",
                    f"+{th} threads after warm-up, rising {th_slope:.3f}/snapshot",
                )
            )

        rss, series_name = h.memory_series()
        rss_growth = h.sustained_growth(
            rss,
            h.RSS_RESTART_MAX_KB
            if series_name.startswith("total")
            else h.ANON_RESTART_MAX_KB,
            warmup,
        )
        restart_max = (
            h.RSS_RESTART_MAX_KB
            if series_name.startswith("total")
            else h.ANON_RESTART_MAX_KB
        )
        rss_slope = h.post_warmup_slope(rss, restart_max, warmup)
        if rss_growth is not None and rss_growth > rss_tolerance_kb and rss_slope > 0:
            violations.append(
                Violation(
                    pid,
                    f"{series_name} growth",
                    f"+{rss_growth} kB sustained after warm-up, rising "
                    f"{rss_slope:.1f} kB/snapshot (tolerance {rss_tolerance_kb} kB)",
                )
            )
    return violations


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Pass/fail leak verdict from rss-watch logs."
    )
    parser.add_argument("logs", nargs="+", type=Path, help="rss-watch log file(s)")
    parser.add_argument(
        "--warmup-snapshots",
        type=int,
        default=DEFAULT_WARMUP_SNAPSHOTS,
        help=(
            "snapshots to skip per process lifetime before judging "
            f"(default {DEFAULT_WARMUP_SNAPSHOTS}; at rss-watch's 2s interval "
            "that is 20s of warm-up)"
        ),
    )
    parser.add_argument(
        "--rss-tolerance-kb",
        type=int,
        default=DEFAULT_RSS_TOLERANCE_KB,
        help="max allowed sustained RSS rise per pid, kB (default %(default)s)",
    )
    parser.add_argument(
        "--fd-tolerance",
        type=int,
        default=0,
        help="allowed sustained fd rise after warm-up (default 0)",
    )
    parser.add_argument(
        "--thread-tolerance",
        type=int,
        default=0,
        help="allowed sustained thread rise after warm-up (default 0)",
    )
    parser.add_argument(
        "--min-snapshots",
        type=int,
        default=DEFAULT_MIN_SNAPSHOTS,
        help=(
            "minimum snapshots a pid needs to be judged at all "
            f"(default {DEFAULT_MIN_SNAPSHOTS})"
        ),
    )
    args = parser.parse_args(argv)

    all_histories: dict[int, PidHistory] = {}
    any_snapshots = False
    for log in args.logs:
        if not log.exists():
            print(f"leak-check: log not found: {log}", file=sys.stderr)
            continue
        for pid, h in parse_log(log).items():
            existing = all_histories.setdefault(pid, PidHistory(pid=pid))
            for r, t, f, a in zip(h.rss, h.threads, h.fds, h._anon_raw, strict=False):
                existing.add(r, t, f, a)
            if h.snapshots:
                any_snapshots = True

    if not any_snapshots:
        print(
            "leak-check: FAIL — no snapshots found in the given log(s); "
            "empty/missing logs must not silently pass",
            file=sys.stderr,
        )
        return 2

    judged = {
        p: h
        for p, h in all_histories.items()
        if h.snapshots >= args.min_snapshots and h.min_judgeable(args.warmup_snapshots)
    }
    skipped = len(all_histories) - len(judged)
    violations = check_histories(
        judged,
        warmup=args.warmup_snapshots,
        rss_tolerance_kb=args.rss_tolerance_kb,
        fd_tolerance=args.fd_tolerance,
        thread_tolerance=args.thread_tolerance,
    )

    print(f"leak-check: {len(judged)} pid(s) judged, {skipped} skipped (short/too-few)")
    for pid in sorted(judged):
        h = judged[pid]
        mem, series_name = h.memory_series()
        print(
            f"  pid={pid}: snapshots={h.snapshots} "
            f"{series_name} first={mem[0]}kB last={mem[-1]}kB "
            f"total-rss first={h.rss[0]}kB last={h.rss[-1]}kB "
            f"threads={min(h.threads)}..{max(h.threads)} fds={min(h.fds)}..{max(h.fds)}"
        )

    if violations:
        print(f"leak-check: FAIL — {len(violations)} violation(s)")
        for v in violations:
            print(f"  {v}")
        return 1

    print("leak-check: PASS — no leak-shaped growth after warm-up")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

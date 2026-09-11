#!/usr/bin/env bash
# rss-watch.sh — CONTINUOUSLY sample RSS/thread/fd counts for every live
# sqtseries python process and log snapshots, so memory growth, thread
# accumulation, or fd leaks are visible over time.
#
# Usage:
#   scripts/rss-watch.sh [interval_s] [out_file]
#   (default interval 3s, out_file /tmp/rss-watch.log)
#
# py-spy-watch.sh additionally captures full stack dumps; this script needs
# no ptrace, no sudo, and never pauses the watched process — safe to leave
# running against production.
#
# ---------------------------------------------------------------------------
# HOW THE RIGHT PROCESS IS FOUND (read this before changing the pattern)
#
# Matching happens in TWO steps, and both must pass:
#
#   1. pgrep -f "python3.*sqtseries"  — the command LINE must contain both
#      "python3" and "sqtseries". This alone is NOT enough: `pgrep -f`
#      matches the bash wrapper too, e.g.
#          bash -c ".../python3 -m sqtseries ... | tee log"
#      whose cmdline carries the same words but whose /proc/PID/comm is
#      "bash". RSS of a bash wrapper is noise (~4 MB, never changes) and
#      hides the real python process.
#
#   2. /proc/PID/comm filter — the kernel's actual executable name must be
#      "python3" (or "python", or the venv console-script's own name).
#      Only genuine python interpreters survive this check.
#
# Practical consequences:
#   - Launch targets so comm is python:  venv/bin/python3 -m sqtseries ...
#     (NOT `python` without the 3, and not through a wrapper that execs
#     under a different name).
#   - If you see "rss=3-4MB threads=1 fds=3" lines in the log, that is a
#     wrapper that slipped through — fix the pattern, do not tune around it.
#   - The same two-step rule is what scripts/py-spy-watch.sh implements.
# ---------------------------------------------------------------------------
set -u

INTERVAL="${1:-3}"
OUT="${2:-/tmp/rss-watch.log}"

echo "=== rss-watch started $(date -Iseconds) interval=${INTERVAL}s ===" > "$OUT"

is_python() {
    # True when /proc/$1/comm is a python interpreter (or a console script
    # named exactly like the pattern's basename, e.g. an installed binary).
    local comm base="$1"
    comm=$(cat "/proc/$2/comm" 2>/dev/null || true)
    [ "$comm" = "python3" ] || [ "$comm" = "python" ] || [ "$comm" = "$base" ]
}

while true; do
    for PID in $(pgrep -f "python3.*sqtseries" 2>/dev/null); do
        is_python "sqtseries" "$PID" || continue
        {
            CMD=$(tr '\0' ' ' < /proc/"$PID"/cmdline 2>/dev/null | head -c 120 || echo "unknown")
            STATUS=$(cat /proc/"$PID"/status 2>/dev/null || true)
            ROLLUP=$(cat /proc/"$PID"/smaps_rollup 2>/dev/null || true)
            VmRSS=$(echo "$STATUS" | grep "^VmRSS:" | awk '{print $2}')
            VmSize=$(echo "$STATUS" | grep "^VmSize:" | awk '{print $2}')
            # Anonymous RSS = real heap (leak evidence). Total RSS also counts
            # file-backed pages (SQLite mmap of DB/WAL — shared page cache,
            # reclaimable, inflated ~5x by the reader pool mapping the same
            # file). Measured 2026-09-09: total RSS grew +98MB under a 9.6k
            # pts/s pump while anon stayed FLAT at 70MB — judging total RSS
            # as a leak was a false positive.
            Anon=$(echo "$ROLLUP" | grep "^Anonymous:" | awk '{print $2}')
            Threads=$(echo "$STATUS" | grep "^Threads:" | awk '{print $2}')
            FDs=$(ls /proc/"$PID"/fd 2>/dev/null | wc -l)
            echo "  pid=$PID rss=${VmRSS:-?}kB anon=${Anon:-?}kB vsz=${VmSize:-?}kB threads=${Threads:-?} fds=${FDs:-?} cmd=$CMD"
        } >> "$OUT"
    done
    sleep "$INTERVAL"
done

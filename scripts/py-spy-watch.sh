#!/usr/bin/env bash
# py-spy-watch.sh — CONTINUOUSLY watch every sqtseries/pytest python process:
# fast RSS trending with periodic full stack dumps for as long as they run.
# Covers a whole test-suite or examples run: the pytest main process (tests
# run engines in-process, not only as servers) plus every spawned
# `python3 -m sqtseries` service.
#
# Usage:
#   scripts/py-spy-watch.sh [interval_s] [out_file] [dump_interval_s] [--clean]
#   (defaults: 1 s, /tmp/py-spy-watch.log, 30 s)
#
# Behaviour (design adapted from airbits/scripts/py-spy-watch.sh):
#   - Multiple processes at once; every matching pid gets its own trend line.
#   - RSS sampled every interval; each line carries a "+N kB" delta against
#     that pid's first sample, so growth is obvious while tailing.
#   - py-spy dumps pause the target briefly -> throttled to once per
#     dump_interval per pid instead of firing every tick.
#   - Match requires cmdline AND comm: shell wrappers that merely carry the
#     pattern in their bash -c string never match (their comm is "bash").
#   - py-spy needs ptrace (yama/ptrace_scope=1) -> sudo. Passwordless sudo is
#     probed once at startup; without it the log carries RSS only.
#   - Works on Python 3.14+ (py-spy 0.4.2 attaches fine; verified 2026-09).
#   - --clean removes the log on exit (default: keep it for post-mortem).
#
# What the logs tell you:
#   - rss delta climbing while idle = leak; flat after warm-up = healthy
#   - repeated dumps of the same deep stack = stuck loop or deadlock
#   - growing thread lists across dumps = un-joined tasks/threads
set -u

INTERVAL="${1:-1}"
OUT="${2:-/tmp/py-spy-watch.log}"
DUMP_EVERY="${3:-30}"

CLEAN_ON_EXIT=false
if [[ "${4:-}" == "--clean" ]]; then CLEAN_ON_EXIT=true; fi
cleanup() {
    if $CLEAN_ON_EXIT; then rm -f "$OUT" 2>/dev/null; echo "Cleaned $OUT" >&2; fi
    echo "py-spy-watch stopped $(date -Iseconds)" >&2
}
trap cleanup EXIT INT TERM

PYSPY=/home/iam/devcode/.env/sqtseries/bin/py-spy
[ -x "$PYSPY" ] || PYSPY=$(command -v py-spy || echo /home/iam/.cargo/bin/py-spy)

SUDO=()
if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
    SUDO=(sudo -n env "PATH=$PATH")
else
    echo "WARN: passwordless sudo unavailable — logging RSS only (dumps skipped)." >&2
fi

declare -A FIRST_RSS LAST_DUMP

echo "=== py-spy-watch started $(date -Iseconds) interval=${INTERVAL}s dump_every=${DUMP_EVERY}s ===" > "$OUT"

# Each pattern is one watched *kind* of process; matches are comm-filtered.
# "pytest" covers the test runner itself (in-process engines live inside it);
# the sqtseries pattern covers example scripts and spawned servers.
PATTERNS=("python3.*sqtseries" "pytest")

find_pids() {
    # Emit pids whose cmdline matches $1 AND whose comm looks like a python
    # binary (or the script name itself, for console-script entry points).
    local pattern="$1" pid comm base
    base=$(basename "$pattern" 2>/dev/null || echo "$pattern")
    for pid in $(pgrep -f "$pattern" 2>/dev/null); do
        comm=$(cat "/proc/$pid/comm" 2>/dev/null || true)
        if [ "$comm" = "python3" ] || [ "$comm" = "python" ] || [ "$comm" = "$pattern" ] || [ "$comm" = "$base" ]; then
            echo "$pid"
        fi
    done
}

rss_kb() {
    awk '/^VmRSS:/ {print $2}' "/proc/$1/status" 2>/dev/null
}

log_rss_line() {
    local label="$1" pid="$2" rss first delta sign=""
    rss=$(rss_kb "$pid")
    if [ -z "$rss" ]; then
        echo "$(date +%H:%M:%S) $label pid=$pid RSS unavailable"
        return
    fi
    first=${FIRST_RSS["$label",$pid]:-}
    if [ -z "$first" ]; then
        FIRST_RSS["$label",$pid]=$rss
        first=$rss
    fi
    delta=$((rss - first))
    [ "$delta" -ge 0 ] && sign="+"
    echo "$(date +%H:%M:%S) $label pid=$pid rss=${rss}kB (${sign}${delta} kB since first sample)"
}

maybe_dump() {
    local label="$1" pid="$2" now
    [ ${#SUDO[@]} -gt 0 ] || return 0
    now=$(date +%s)
    if [ -z "${LAST_DUMP["$label",$pid]:-}" ] || (( now - LAST_DUMP["$label",$pid] >= DUMP_EVERY )); then
        {
            echo "--- dump $label pid=$pid $(date +%H:%M:%S) ---"
            "${SUDO[@]}" "$PYSPY" dump --pid "$pid" 2>&1
        } >> "$OUT"
        LAST_DUMP["$label",$pid]=$now
    fi
}

# Forget state for pids that vanished (their slot number is freed for reuse).
prune_dead() {
    local label="$1" pid key alive
    shift
    for key in "${!FIRST_RSS[@]}"; do
        [[ "$key" == "$label",* ]] || continue
        pid="${key#*,}"
        alive=0
        for p in "$@"; do
            [ "$p" = "$pid" ] && alive=1 && break
        done
        if [ "$alive" = 0 ]; then
            echo "$(date +%H:%M:%S) $label pid=$pid exited (last rss=${FIRST_RSS[$key]}kB)" >> "$OUT"
            unset "FIRST_RSS[$key]" "LAST_DUMP[$label,$pid]"
        fi
    done
}

while true; do
    found_any=0
    for pattern in "${PATTERNS[@]}"; do
        mapfile -t PIDS < <(find_pids "$pattern")
        prune_dead "$pattern" "${PIDS[@]}"
        for pid in "${PIDS[@]}"; do
            found_any=1
            log_rss_line "$pattern" "$pid" >> "$OUT"
            maybe_dump "$pattern" "$pid"
        done
    done
    if [ "$found_any" -eq 0 ]; then
        echo "----- $(date +%H:%M:%S) no watched processes found, waiting... -----" >> "$OUT"
    fi
    sleep "$INTERVAL"
done

#!/usr/bin/env bash
# run-stress-rig.sh — one-command, fully-instrumented stress verification rig.
#
# What it does:
#   1. rss-watch            -> per-pid RSS/threads/fds trend log
#   2. stress_percentiles   -> separate-process pump at a fixed rate, ZMQ+HTTP
#                              query clients, WS sampling, end-to-end
#                              accounting (sent vs ingested vs persisted vs
#                              db_rows) with a grace-drain for PUSH in-flight
#   3. dashboard_probe      -> optional headless Chromium against /dashboard
#                              (console errors, stalls, JS heap trend)
#   4. leak_check           -> pass/fail verdict on the rss log after the run
#
# Usage:
#   scripts/run-stress-rig.sh [--duration 300] [--rate 2000] [--clients 8]
#                             [--warmup-rows 20000] [--http-port 12599]
#                             [--probe] [--probe-duration 620]
#                             [--tag myrun]
#                             [--no-guard] [--guard-budget-mb N] [--probe-budget-mb N]
#
# Outputs (all under /tmp/sqtseries-rig-<tag>/):
#   rss.log          per-pid RSS/threads/fds samples  (leak_check input)
#   stress.json      percentiles + end-to-end accounting
#   stress.log       harness stdout
#   probe.log        dashboard probe report (with --probe)
#   probe-shots/     start/mid/end screenshots    (with --probe)
#   verdict.txt      leak_check PASS/FAIL summary
#
# Exit code: 0 if leak_check passes (probe problems are reported but do not
# fail the run unless --strict-probe is given), 1 otherwise.
set -u

cd "$(dirname "$0")/.." || exit 2
PY="${SQT_PY:-/home/iam/devcode/.env/sqtseries/bin/python3}"

DURATION=300
RATE=2000
CLIENTS=8
WARMUP=20000
HTTP_PORT=0
PROBE=0
PROBE_DURATION=620
STRICT_PROBE=0
TAG=""
# HARD MEMORY GOVERNOR (2026-09-10 incident: an unbounded run froze the PC).
# The stress tree (service+pump+clients) and the probe tree (headless
# Chromium) each run under scripts/mem-guard.sh; a breach SIGKILLs that
# whole tree and exits 42 instead of taking the desktop down.
GUARD=1
GUARD_BUDGET=1500
PROBE_BUDGET=2500

while [ $# -gt 0 ]; do
    case "$1" in
        --duration) DURATION="$2"; shift 2 ;;
        --rate) RATE="$2"; shift 2 ;;
        --clients) CLIENTS="$2"; shift 2 ;;
        --warmup-rows) WARMUP="$2"; shift 2 ;;
        --http-port) HTTP_PORT="$2"; shift 2 ;;
        --probe) PROBE=1; shift ;;
        --probe-duration) PROBE_DURATION="$2"; shift 2 ;;
        --strict-probe) STRICT_PROBE=1; shift ;;
        --no-guard) GUARD=0; shift ;;
        --guard-budget-mb) GUARD_BUDGET="$2"; shift 2 ;;
        --probe-budget-mb) PROBE_BUDGET="$2"; shift 2 ;;
        --tag) TAG="$2"; shift 2 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
done

GUARD_SH="scripts/mem-guard.sh"

OUT="/tmp/sqtseries-rig-${TAG:-run}"
rm -rf "$OUT"
mkdir -p "$OUT/probe-shots"

# jemalloc by default (2026-09-10 A/B: glibc retained ~52 B/row of freed
# native memory in per-thread arenas — RSS +583 MB over a 10-min 10k pts/s
# run and ingest sagged to ~9.6k/s; jemalloc held +22 MB BOUNDED and kept
# the full 10,005 pts/s with 0 unaccounted). Opt out with RIG_NO_JEMALLOC=1.
if [ "${RIG_NO_JEMALLOC:-0}" != "1" ] && [ -z "${LD_PRELOAD:-}" ] \
    && ldconfig -p 2>/dev/null | grep -q libjemalloc.so.2; then
    JEMALLOC_SO=$(ldconfig -p | awk '/libjemalloc\.so\.2/{print $NF; exit}')
    export LD_PRELOAD="$JEMALLOC_SO"
    echo "== jemalloc enabled: $LD_PRELOAD (RIG_NO_JEMALLOC=1 to disable) =="
fi

if [ "$HTTP_PORT" = "0" ]; then
    # Free ephemeral-ish port in the documented service range.
    HTTP_PORT=$(python3 - <<'PYEOF'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PYEOF
)
fi

echo "== rig $OUT: ${DURATION}s @ ${RATE}/s, ${CLIENTS} clients, http=$HTTP_PORT =="

# 1) RSS trend (3s sampling) — starts first so even the warm-up is captured.
tmux kill-session -t rig-rss 2>/dev/null
tmux new -d -s rig-rss "scripts/rss-watch.sh 3 '$OUT/rss.log'"

# 2) Dashboard probe (optional) — starts before the server exists; it retries
#    connecting until the harness service binds the port.
if [ "$PROBE" = "1" ]; then
    tmux kill-session -t rig-probe 2>/dev/null
    if [ "${GUARD:-1}" = "1" ]; then
        # Chromium can quietly eat 1-2 GB; the probe tree gets its own budget.
        tmux new -d -s rig-probe "scripts/mem-guard.sh --budget-mb ${PROBE_BUDGET:-2500} --name rig-probe -- \
            $PY -u scripts/dashboard_probe.py \
            --url http://127.0.0.1:$HTTP_PORT --duration $PROBE_DURATION \
            --screenshot-dir '$OUT/probe-shots' > '$OUT/probe.log' 2>&1; \
            echo PROBE_EXIT=\$? >> '$OUT/probe.log'"
    else
        tmux new -d -s rig-probe "$PY -u scripts/dashboard_probe.py \
            --url http://127.0.0.1:$HTTP_PORT --duration $PROBE_DURATION \
            --screenshot-dir '$OUT/probe-shots' > '$OUT/probe.log' 2>&1; \
            echo PROBE_EXIT=\$? >> '$OUT/probe.log'"
    fi
fi

# 3) The stress run itself (blocking), under the memory governor by default.
if [ "${GUARD:-1}" = "1" ]; then
    scripts/mem-guard.sh --budget-mb "${GUARD_BUDGET:-1500}" --name rig-stress -- \
        $PY scripts/stress_percentiles.py \
        --duration "$DURATION" --rate "$RATE" --clients "$CLIENTS" \
        --warmup-rows "$WARMUP" --http-port "$HTTP_PORT" \
        --json "$OUT/stress.json" > "$OUT/stress.log" 2>&1
    STRESS_EXIT=$?
else
    $PY scripts/stress_percentiles.py \
        --duration "$DURATION" --rate "$RATE" --clients "$CLIENTS" \
        --warmup-rows "$WARMUP" --http-port "$HTTP_PORT" \
        --json "$OUT/stress.json" > "$OUT/stress.log" 2>&1
    STRESS_EXIT=$?
fi

# 4) Leak verdict from the RSS trend. The absolute RSS allowance scales
#    mildly with run length (30 kB per snapshot at the 3 s rss interval on
#    top of the 20 MB base): a bounded allocator residue over a 10-minute
#    run (~24 MB measured with jemalloc) must PASS while a true leak (583 MB
#    over the same window, ~20x over allowance) must FAIL.
RSS_TOL=$((20000 + (DURATION / 3) * 30))
$PY scripts/leak_check.py --rss-tolerance-kb "$RSS_TOL" "$OUT/rss.log" > "$OUT/verdict.txt" 2>&1
LEAK_EXIT=$?

tmux kill-session -t rig-rss 2>/dev/null
# Let a still-sampling probe finish its verdict instead of SIGKILLing it
# mid-write (that EPIPEs the Playwright driver and loses the report).
if [ "$PROBE" = "1" ]; then
    for _ in $(seq 1 30); do
        tmux ls 2>/dev/null | grep -q "^rig-probe" || break
        grep -q "^PROBE_EXIT" "$OUT/probe.log" 2>/dev/null && break
        sleep 2
    done
fi
tmux kill-session -t rig-probe 2>/dev/null

echo "== stress exit=$STRESS_EXIT  leak_check exit=$LEAK_EXIT =="
cat "$OUT/verdict.txt"
if [ "$PROBE" = "1" ]; then
    echo "== probe =="
    tail -5 "$OUT/probe.log"
fi

if [ "$STRICT_PROBE" = "1" ] && [ "$PROBE" = "1" ]; then
    PEXIT=$(sed -n 's/^PROBE_EXIT=//p' "$OUT/probe.log" | tail -1)
    [ "${PEXIT:-1}" = "0" ] || LEAK_EXIT=1
fi

exit $(( LEAK_EXIT == 0 && STRESS_EXIT == 0 ? 0 : 1 ))

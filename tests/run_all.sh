#!/bin/bash
# Test runner: clean start -> per-file pytest suites -> summary -> cleanup.
#
# Usage:  ./run_all.sh [suite ...]   (from the tests/ directory OR the
#         repo root; bare names allowed, e.g. ./run_all.sh test_query.py)
#         Runs from the REPO ROOT so root-relative imports (examples.*)
#         resolve exactly like `make test` (`python -m pytest` puts cwd
#         on sys.path; cwd=tests/ would break them).
# Env:    SQT_PY=<python>            (default: the sqtseries venv python)
#         SUITE_TIMEOUT=NNN          override per-suite hard timeout (default 1200s)
#         COVERAGE=1                 run every suite under pytest-cov (append mode),
#                                    then combine + report + html
#
# Liveliness: every suite runs under a hard timeout (SUITE_TIMEOUT) and
# prints a heartbeat every 20s while running, so a stuck suite can never
# hang the run silently or forever.
#
# Post-mortem: every suite's stdout+stderr is captured to tests/results/
# (which clean_artifacts does NOT wipe), so output is preserved for diagnosis.
#
# Cross-suite isolation: after EVERY suite, inter_suite_cleanup() kills
# leftover service processes so the next suite starts clean.
#
# Kill-pattern safety: cleanup patterns never match this runner's own
# cmdline (no bare substrings of suite filenames — a bare `pkill -f`
# once murdered an equivalent runner via its argv; patterns below are
# anchored to service invocations only).

set -uo pipefail

cd "$(dirname "$0")/.."
export SQT_PY="${SQT_PY:-/home/iam/devcode/.env/sqtseries/bin/python3}"
mkdir -p tests/logs tests/results
TESTS_DIR="tests"

SUITE_TIMEOUT="${SUITE_TIMEOUT:-1200}"   # seconds per suite (0 = no limit)
HEARTBEAT_S=20
RESULT_DIR="tests/results"
FAILED_SUITES=""

# Suites excluded by default (need a display/browser or live camera feed;
# pass them explicitly to force, e.g. ./run_all.sh test_camera_feed.py).
DEFAULT_IGNORES="test_camera_overlay_page.py test_camera_feed.py"

COVERAGE_MODE="${COVERAGE:-0}"
COV_ARGS=()
if [ "$COVERAGE_MODE" = "1" ]; then
    rm -f .coverage .coverage.*
    COV_ARGS=(--cov=sqtseries --cov-append --cov-report=)
fi

# ── Orphan cleanup: leftover service processes only ──
# Matches `python3 -m sqtseries ...` / `sqtseries run` service invocations.
# Cannot match this runner (bash, no python3 in cmdline) or pytest suite
# processes (`python -m pytest ...` has no `-m sqtseries`).
inter_suite_cleanup() {
    pkill -f "python3.*-m sqtseries" 2>/dev/null || true
    pkill -f "sqtseries run" 2>/dev/null || true
    sleep 0.5
    pkill -9 -f "python3.*-m sqtseries" 2>/dev/null || true
    pkill -9 -f "sqtseries run" 2>/dev/null || true
    sleep 0.5
    rm -f /tmp/sqtseries_*.sqlite /tmp/sqtseries_*.db 2>/dev/null || true
}

echo "=============================================================="
echo " sqtseries test run  ($(date '+%F %T'))"
echo " python: $SQT_PY"
echo " suite timeout: ${SUITE_TIMEOUT}s  heartbeat: ${HEARTBEAT_S}s"
echo "=============================================================="

echo
echo "=== [0] clean start ==="
inter_suite_cleanup
rm -rf tests/__pycache__ src/__pycache__ src/sqtseries/__pycache__ 2>/dev/null
find tests src -name "*.pyc" -delete 2>/dev/null
rm -f "$RESULT_DIR"/*.out
rm -f /tmp/pytest-*.log 2>/dev/null
echo "clean (processes, __pycache__, *.pyc, temp DBs)"

# Suite list: explicit filter or every test_*.py minus default ignores.
if [ "$#" -gt 0 ]; then
    _picked=""
    for _want in "$@"; do
        case "$_want" in *.py) ;; *) _want="$_want.py";; esac
        if [ -f "$TESTS_DIR/$_want" ]; then
            _picked="$_picked $_want"
        else
            echo "unknown suite: $_want"; exit 2
        fi
    done
    SUITE_LIST="$_picked"
else
    SUITE_LIST=""
    for _s in "$TESTS_DIR"/test_*.py; do
        _s=$(basename "$_s")
        _skip=0
        for _ig in $DEFAULT_IGNORES; do
            if [ "$_s" = "$_ig" ]; then _skip=1; break; fi
        done
        if [ "$_skip" -eq 0 ]; then SUITE_LIST="$SUITE_LIST $_s"; fi
    done
fi
TOTAL_SUITES=$(echo "$SUITE_LIST" | wc -w)
FAILED=0
START_ALL=$(date +%s)
IDX=0
for suite in $SUITE_LIST; do
    IDX=$((IDX + 1))
    echo
    echo "=== [$IDX/$TOTAL_SUITES $(date '+%T')] $suite ==="
    START=$(date +%s)
    if [ "$SUITE_TIMEOUT" -gt 0 ]; then
        timeout "$SUITE_TIMEOUT" "$SQT_PY" -m pytest "$TESTS_DIR/$suite" -q --tb=short "${COV_ARGS[@]}" &> "$RESULT_DIR/$suite.out" &
    else
        "$SQT_PY" -m pytest "$TESTS_DIR/$suite" -q --tb=short "${COV_ARGS[@]}" &> "$RESULT_DIR/$suite.out" &
    fi
    RUNPID=$!
    while kill -0 "$RUNPID" 2>/dev/null; do
        sleep "$HEARTBEAT_S"
        echo "  [$(date '+%T')] still running: $suite ($(( $(date +%s) - START ))s)"
    done
    wait "$RUNPID"
    rc=$?
    if [ "$rc" -eq 124 ]; then
        echo "  !! $suite TIMED OUT after ${SUITE_TIMEOUT}s (killed)"
        FAILED=1
        FAILED_SUITES="$FAILED_SUITES  $suite (TIMEOUT)\n"
    elif [ "$rc" -ne 0 ]; then
        echo "  !! $suite FAILED (exit code $rc)"
        FAILED=1
        FAILED_SUITES="$FAILED_SUITES  $suite (exit $rc)\n"
    fi
    ELAPSED=$(( $(date +%s) - START ))
    echo "  -- $suite finished in ${ELAPSED}s (exit $rc)"
    if [ "$rc" -ne 0 ] && [ -f "$RESULT_DIR/$suite.out" ]; then
        echo "  --- last 15 lines of $suite ---"
        tail -15 "$RESULT_DIR/$suite.out" | sed 's/^/  /'
        echo "  --- full output: $RESULT_DIR/$suite.out ---"
    fi
    inter_suite_cleanup
done

echo
echo "=== [cleanup] ==="
inter_suite_cleanup
rm -rf tests/__pycache__ 2>/dev/null
find tests src -name "*.pyc" -delete 2>/dev/null

echo
echo "=============================================================="
echo " total run time: $(( $(date +%s) - START_ALL ))s"
if [ "$COVERAGE_MODE" = "1" ]; then
    echo "--- combining coverage data ---"
    "$SQT_PY" -m coverage combine 2>/dev/null || true
    "$SQT_PY" -m coverage report
    "$SQT_PY" -m coverage html -d /tmp/sqtseries_htmlcov 2>/dev/null || true
    echo "--- html report: /tmp/sqtseries_htmlcov ---"
fi
if [ $FAILED -eq 0 ]; then
    echo "ALL SUITES PASSED"
else
    echo "SOME TESTS FAILED:"
    echo -e "$FAILED_SUITES"
    echo "Full output in $RESULT_DIR/*.out"
fi
echo "=============================================================="
exit $FAILED

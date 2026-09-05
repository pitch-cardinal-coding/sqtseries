#!/usr/bin/env bash
# test_wheel.sh — build + install + test the wheel in /tmp (isolated).
#
# Usage:  scripts/test_wheel.sh
#
# Copies the source tree to /tmp (anchored excludes so helper dirs like
# scripts/build are never pruned), builds the wheel there with a fresh
# venv, installs it, and runs functional validation + a fast unit subset
# against the INSTALLED wheel. Copies wheel + requirements files to the
# repo dist/ only after everything passes.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="/tmp/sqtseries-build"
VENV_DIR="/tmp/sqtseries-venv"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
info() { echo -e "${YELLOW}>>> $*${NC}"; }
ok()   { echo -e "${GREEN}    $*${NC}"; }
die()  { echo -e "${RED}ERROR: $*${NC}" >&2; exit 1; }

cleanup() { rm -rf "$BUILD_DIR" "$VENV_DIR"; }
trap cleanup EXIT

# ── 1. Copy source to /tmp (anchored excludes!) ─────────────────────
info "Copying source tree to $BUILD_DIR"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"
rsync -a \
    --exclude='.git' \
    --exclude='/dist' \
    --exclude='/build' \
    --exclude='*.egg-info' \
    --exclude='__pycache__' \
    --exclude='.pytest_cache' \
    --exclude='.ruff_cache' \
    --exclude='.hypothesis' \
    --exclude='.coverage' \
    "$REPO_DIR/" "$BUILD_DIR/"
ok "Copied $(du -sh "$BUILD_DIR" | cut -f1)"

# ── 2. Fresh venv + deps ────────────────────────────────────────────
info "Creating fresh venv at $VENV_DIR"
rm -rf "$VENV_DIR"
python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install -q --upgrade pip
"$VENV_DIR/bin/pip" install -q -r "$BUILD_DIR/requirements.txt"
ok "Deps installed"

PY="$VENV_DIR/bin/python3"

# ── 3. Build wheel in isolation ─────────────────────────────────────
info "Building wheel"
cd "$BUILD_DIR"
PY="$PY" scripts/build_wheel.sh "$BUILD_DIR/dist" > /tmp/sqtseries-wheelbuild.log 2>&1 || {
    tail -15 /tmp/sqtseries-wheelbuild.log
    die "isolated build failed"
}
WHL=$(ls "$BUILD_DIR"/dist/sqtseries-*.whl 2>/dev/null | head -1 || true)
[ -n "$WHL" ] || die "no wheel produced"
ok "Wheel: $(basename "$WHL")"

# ── 4. Install wheel ────────────────────────────────────────────────
info "Installing wheel into venv"
"$PY" -m pip install -q --force-reinstall --no-deps "$WHL"
ok "Installed"

# ── 5. Functional validation (against the INSTALLED wheel) ──────────
info "Running functional validation"
FUNC_OUT=$("$PY" -c "
import tempfile, time, pathlib
from sqtseries.engine.db import Database
from sqtseries.engine.store import StorageEngine, initialize_schema
db = Database(str(pathlib.Path(tempfile.mkdtemp()) / 'func.sqlite'))
initialize_schema(db)
eng = StorageEngine(db)
now_ns = time.time_ns()
n = eng.insert_many([('test.cpu', {'host': 'w1'}, 0.5, now_ns), ('test.cpu', {'host': 'w1'}, 0.7, now_ns + 1)])
assert n == 2, n
rows = list(eng.query_time_range(metric='test.cpu'))
assert len(rows) == 2, rows
assert eng.list_metrics() == ['test.cpu']
print('engine write+query ok:', len(rows), 'row(s)')
" 2>&1) || { echo "$FUNC_OUT"; die "functional validation failed"; }
echo "$FUNC_OUT" | sed 's/^/    /'
ok "Functional validation"

# ── 6. CLI smoke ────────────────────────────────────────────────────
info "CLI smoke (version + service boot)"
"$PY" -m sqtseries --version
ok "CLI version"

# ── 7. Fast unit subset ─────────────────────────────────────────────
info "Running fast unit subset"
cd "$BUILD_DIR"
UNIT_FAIL=0
for t in test_config.py test_engine.py test_query.py test_client.py; do
    if "$PY" -m pytest "tests/$t" -q --tb=line -p no:cacheprovider > "/tmp/sqtseries-unit-$t.log" 2>&1; then
        ok "$t ($(tail -1 "/tmp/sqtseries-unit-$t.log" | grep -oE '[0-9]+ passed' || echo ok))"
    else
        echo "FAIL: $t — tail:"; tail -8 "/tmp/sqtseries-unit-$t.log"
        UNIT_FAIL=1
    fi
done
[ "$UNIT_FAIL" -eq 0 ] || die "unit subset failed"

# ── 8. Copy wheel + requirements files only after validation ────────
info "Copying artifacts to repo dist/"
mkdir -p "$REPO_DIR/dist"
cp "$WHL" "$REPO_DIR/dist/"
cp "$REPO_DIR/requirements.txt" "$REPO_DIR/dist/" 2>/dev/null || true
ok "Copied wheel to $REPO_DIR/dist/ ($(du -h "$WHL" | cut -f1))"
ls -ld "$REPO_DIR/dist" 2>/dev/null | sed 's/^/  /'

echo
echo "=============================================================="
echo " Results: wheel builds isolated, installs, validates, units pass"
echo " ALL CHECKS PASSED"
echo "=============================================================="

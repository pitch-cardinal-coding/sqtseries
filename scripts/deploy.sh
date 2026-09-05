#!/usr/bin/env bash
# deploy.sh — full production deploy cycle: stop → uninstall → build → install.
#
# Usage:
#   scripts/deploy.sh              # build + install (default)
#   scripts/deploy.sh --no-build   # skip build, deploy the existing dist/*.whl
#   scripts/deploy.sh --restart    # also restart the systemd service after install
#
# Requires: sudo for systemctl and /opt/sqtseries operations.
# Kill-pattern safety: service patterns below are anchored to
# `python3 -m sqtseries` invocations and can never match this script's
# own cmdline (no bare substrings).
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PREFIX="/opt/sqtseries"
VENV_PY="$PREFIX/bin/python3"
PIP_ARGS=(-m pip)
WHL_DIR="$REPO_DIR/dist"
CONFIG="$PREFIX/config.toml"

DO_BUILD=1
DO_RESTART=0

for arg in "$@"; do
    case "$arg" in
        --no-build) DO_BUILD=0 ;;
        --restart)  DO_RESTART=1 ;;
        -h|--help)
            echo "Usage: scripts/deploy.sh [--no-build] [--restart]"
            echo "  --no-build   skip build, deploy the existing dist/*.whl"
            echo "  --restart    restart the systemd service after install"
            exit 0
            ;;
        *) echo "Unknown arg: $arg" >&2; exit 1 ;;
    esac
done

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
info() { echo -e "${YELLOW}>>> $*${NC}"; }
ok()   { echo -e "${GREEN}    $*${NC}"; }
fail() { echo -e "${RED}ERROR: $*${NC}" >&2; exit 1; }

# ====================================================================
# Phase 1: Stop running services
# ====================================================================
info "Phase 1: Stopping running sqtseries services..."

# systemd (system + user scope)
sudo systemctl stop sqtseries.service 2>/dev/null || true
sudo systemctl disable sqtseries.service 2>/dev/null || true
systemctl --user stop sqtseries.service 2>/dev/null || true
systemctl --user disable sqtseries.service 2>/dev/null || true

# stray service processes (anchored patterns only)
pkill -f "python3.*-m sqtseries" 2>/dev/null || true
pkill -f "sqtseries run" 2>/dev/null || true
sleep 1
ok "Services stopped"

# ====================================================================
# Phase 2: Uninstall old production venv (keep /opt/sqtseries/config.toml)
# ====================================================================
info "Phase 2: Uninstalling old production install..."
if [ -x "$VENV_PY" ]; then
    sudo "$VENV_PY" "${PIP_ARGS[@]}" uninstall -y sqtseries 2>/dev/null | tail -1 || true
    ok "Old sqtseries uninstalled"
else
    info "No existing $PREFIX venv (fresh install)"
fi

# ====================================================================
# Phase 3: Build (unless --no-build)
# ====================================================================
if [ "$DO_BUILD" -eq 1 ]; then
    info "Phase 3: Building new wheel..."
    _BPY="${PY:-/home/iam/devcode/.env/sqtseries/bin/python3}"
    [ -x "$_BPY" ] || _BPY="python3"
    PY="$_BPY" "$REPO_DIR/scripts/build_wheel.sh" "$WHL_DIR" || fail "build failed"
else
    info "Phase 3: Skipped (--no-build)"
fi
WHL=$(ls -t "$WHL_DIR"/sqtseries-*.whl 2>/dev/null | head -1 || true)
[ -n "$WHL" ] || fail "no wheel in $WHL_DIR (run without --no-build first)"

# ====================================================================
# Phase 4: Install into production venv
# ====================================================================
info "Phase 4: Installing into production venv ($PREFIX)..."
if [ ! -x "$VENV_PY" ]; then
    sudo mkdir -p "$PREFIX"
    sudo python3 -m venv "$PREFIX"
    ok "Created $PREFIX venv"
fi
sudo "$VENV_PY" "${PIP_ARGS[@]}" install --upgrade pip 2>/dev/null | tail -1 || true
if [ -f "$REPO_DIR/dist/requirements-prod.txt" ]; then
    sudo "$VENV_PY" "${PIP_ARGS[@]}" install -r "$REPO_DIR/dist/requirements-prod.txt" 2>&1 | tail -2
elif [ -f "$REPO_DIR/requirements-prod.txt" ]; then
    sudo "$VENV_PY" "${PIP_ARGS[@]}" install -r "$REPO_DIR/requirements-prod.txt" 2>&1 | tail -2
fi
sudo "$VENV_PY" "${PIP_ARGS[@]}" install --force-reinstall --no-deps "$WHL" 2>&1 | tail -2

# Console-script symlink (only sqtseries ships; drop stale variants)
sudo ln -sf "$PREFIX/bin/sqtseries" /usr/local/bin/sqtseries

# Default config (never overwrite an operator's config)
if sudo test ! -f "$CONFIG"; then
    if [ -f "$REPO_DIR/dist/config.toml" ]; then
        sudo cp "$REPO_DIR/dist/config.toml" "$CONFIG"
    elif [ -f "$REPO_DIR/config.toml" ]; then
        sudo cp "$REPO_DIR/config.toml" "$CONFIG"
    fi
    ok "Installed default $CONFIG"
fi
# Publish docs to the user's home (no sudo: must stay owned+readable by
# the logged-in user, like airbits ~/airbitsdocs)
mkdir -p ~/sqtseriesdocs && rm -rf ~/sqtseriesdocs/* 2>/dev/null || true
cp -r "$REPO_DIR/dist/docs/"* ~/sqtseriesdocs/ 2>/dev/null || true
chmod -R u+rwX,go+rX ~/sqtseriesdocs 2>/dev/null || true
ok "Installed to $PREFIX + symlink + docs"

# ====================================================================
# Phase 5: Verify (smoke on a temp database, never the prod DB)
# ====================================================================
info "Phase 5: Verifying installation..."
sudo "$VENV_PY" -m sqtseries --version || fail "version check failed"
SMOKE_DB=$(mktemp -d)/smoke.sqlite
sudo "$VENV_PY" -c "
from sqtseries.engine.db import Database
from sqtseries.engine.store import StorageEngine, initialize_schema
import time
db = Database('$SMOKE_DB')
initialize_schema(db)
eng = StorageEngine(db)
assert eng.insert_many([('smoke.cpu', None, 1.0, time.time_ns())]) == 1
assert len(list(eng.query_time_range(metric='smoke.cpu'))) == 1
print('prod wheel write+query ok')
" || fail "prod wheel smoke failed"
sudo "$VENV_PY" -m sqtseries --db "$SMOKE_DB" health || fail "health smoke failed"
rm -rf "$(dirname "$SMOKE_DB")"
ok "Smoke test passed"

if [ "$DO_RESTART" -eq 1 ]; then
    info "Restarting systemd service..."
    sudo systemctl start sqtseries.service 2>/dev/null && ok "Service started" || info "Service not installed (run: sqtseries install)"
fi

echo
ok "Deploy complete!"
echo
echo "    Status:  /opt/sqtseries/bin/python3 -m sqtseries status"

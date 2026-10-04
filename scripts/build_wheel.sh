#!/usr/bin/env bash
# build_wheel.sh — build the sqtseries release wheel (pure Python, setuptools).
#
# Usage:  scripts/build_wheel.sh [DIST_DIR]   (default DIST_DIR=dist)
#
# Cleans every temp artifact whether it succeeds or fails; only the
# finished .whl is left in DIST_DIR. DIST_DIR is created if absent.
# Kill-pattern safety: service patterns below never match this script's
# own cmdline (anchored to `python3 -m sqtseries` service invocations;
# no bare substrings).
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DIST_DIR="${1:-$REPO_DIR/dist}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
info() { echo -e "${YELLOW}>>> $*${NC}"; }
ok()   { echo -e "${GREEN}    $*${NC}"; }
fail() { echo -e "${RED}ERROR: $*${NC}" >&2; exit 1; }

# Interpreter: prefer the project venv, else whatever python3 is on PATH.
# Keep the fallback — without it the build only works on a machine that
# happens to have the venv at the hardcoded path.
PY="${PY:-/home/iam/devcode/.env/sqtseries/bin/python3}"
[ -x "$PY" ] || PY="$(command -v python3 || true)"
[ -n "$PY" ] || fail "no python3 interpreter found (set PY=/path/to/python3)"

mkdir -p "$DIST_DIR"
DIST_DIR="$(cd "$DIST_DIR" && pwd)"

# 1. stop anything serving (a stale service can hold the DB, never the wheel,
#    but a clean tree builds reproducibly)
info "Stopping leftover services..."
pkill -f "python3.*-m sqtseries" 2>/dev/null || true
sleep 1

# 2. build
info "Building wheel (pure Python, setuptools)..."
cd "$REPO_DIR"
rm -rf build src/*.egg-info src/sqtseries.egg-info .eggs 2>/dev/null || true

# Regenerate dist/docs instead of trusting the checked-in copy: it is both the
# release directory's doc site and what gets packaged as /docs.
info "Building docs (dist/docs)..."
"$PY" scripts/docs-build/sqtseries_build.py >/dev/null || fail "docs build failed"
[ -f "$REPO_DIR/dist/docs/index.html" ] || fail "docs build produced no dist/docs/index.html"

# The doc pages reach the wheel through the in-tree build backend
# (build_support/), so `pip install .` ships them too.
"$PY" -m pip wheel . --no-deps -w "$DIST_DIR" 2>&1 | tail -2
WHL=$(ls -t "$DIST_DIR"/sqtseries-*.whl 2>/dev/null | head -1 || true)
[ -n "$WHL" ] || fail "no wheel produced"

# 3. verify contents
info "Verifying wheel contents..."
"$PY" - <<EOF || fail "wheel verification failed"
import sys, zipfile
names = zipfile.ZipFile("$WHL").namelist()
assert any(n.endswith("METADATA") for n in names), "no METADATA"
assert any("entry_points" in n.lower() or n.endswith("entry_points.txt") for n in names), "no entry_points"
assert any(n == "sqtseries/cli.py" or n.endswith("/sqtseries/cli.py") for n in names), "no cli.py"
assert not any(n.startswith("tests/") for n in names), "tests leaked into wheel"
assert any(n.startswith("sqtseries/docs_data/") and n.endswith("index.html") for n in names), "docs not packaged"
rec = [n for n in names if n.endswith("RECORD")]
assert rec, "no RECORD"
print("OK: entry_points + cli.py present, no tests leakage, RECORD present")
EOF

# 4. assemble DIST_DIR (wheel + install inputs). The installer and its
#    HOW-TO come from release/, the one place they are edited; dist/docs/ is
#    the docs source the wheel already consumed.
info "Assembling $(basename "$DIST_DIR")/..."
cp "$REPO_DIR/requirements.txt" "$DIST_DIR/requirements.txt"
cp "$REPO_DIR/requirements-prod.txt" "$DIST_DIR/requirements-prod.txt"
cp "$REPO_DIR/config.toml" "$DIST_DIR/config.toml"
cp "$REPO_DIR/release/Makefile" "$DIST_DIR/Makefile"
cp "$REPO_DIR/release/HOW-TO-INSTALL.md" "$DIST_DIR/HOW-TO-INSTALL.md"
if [ "$DIST_DIR" != "$REPO_DIR/dist" ]; then
    rm -rf "$DIST_DIR/docs"
    cp -r "$REPO_DIR/dist/docs" "$DIST_DIR/docs"
fi
# The release copy of README sits beside docs/, so root-relative dist/docs/
# and release/HOW-TO-INSTALL links are rewritten to plain relative links.
sed -e 's#](dist/docs/#](docs/#g' -e 's#](release/HOW-TO-INSTALL\.md#](HOW-TO-INSTALL.md#g' \
    "$REPO_DIR/README.md" > "$DIST_DIR/README.md"
ok "assembled ($(ls "$DIST_DIR" | tr '\n' ' '))"

# 5. clean temp artifacts (dist stays)
rm -rf "$REPO_DIR/build" "$REPO_DIR/src"/*.egg-info "$REPO_DIR/src/sqtseries.egg-info" "$REPO_DIR/.eggs" src/sqtseries/docs_data 2>/dev/null || true
ok "Wheel: $(basename "$WHL") ($(du -h "$WHL" | cut -f1))"
ok "Build and verification complete!"
echo
echo "    Install:  make install-prod   (or: $PY -m pip install $WHL)"

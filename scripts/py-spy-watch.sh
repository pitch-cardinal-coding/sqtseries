#!/usr/bin/env bash
# py-spy-watch.sh — CONTINUOUSLY sample py-spy on the sqtseries server and log
# every dump. Run this alongside the server so py-spy is watching at ALL
# times (acceptance criterion). Stops when the server dies or stdin closes.
#
# Usage:
#   scripts/py-spy-watch.sh [interval_s] [out_file]
#   (default interval 3s, out_file /tmp/py-spy-watch.log)
#
# Needs sudo for ptrace under yama/ptrace_scope=1.
set -u

INTERVAL="${1:-3}"
OUT="${2:-/tmp/py-spy-watch.log}"
PY=/home/iam/devcode/.env/sqtseries/bin/py-spy

echo "=== py-spy-watch started $(date -Iseconds) interval=${INTERVAL}s ===" > "$OUT"

while true; do
    PID=$(pgrep -f "python3.*sqtseries" | grep -v grep | head -1 || true)
    if [ -n "$PID" ]; then
        {
            echo "----- $(date +%H:%M:%S) dump pid=$PID -----"
            sudo env "PATH=$PATH" "$PY" dump --pid "$PID" 2>&1
        } >> "$OUT"
    fi
    sleep "$INTERVAL"
done

#!/usr/bin/env bash
set -euo pipefail

SESSION="sqtcam"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKDIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUNDIR="/tmp/sqtcam"
CONFIG="$RUNDIR/conf.toml"
DB_PATH="$RUNDIR/sqtcam.sqlite"
OVERLAY_LOG="$RUNDIR/overlay.log"
HTTP_PORT="17105"

# Prefer the project venv if present; otherwise fall back to plain python3.
if [[ -x "/home/iam/devcode/.env/sqtseries/bin/python3" ]]; then
    PYTHON="/home/iam/devcode/.env/sqtseries/bin/python3"
else
    PYTHON="$(command -v python3 || true)"
fi
if [[ -z "$PYTHON" || ! -x "$PYTHON" ]]; then
    echo "ERROR: no usable python3 found." >&2
    exit 1
fi

mkdir -p "$RUNDIR"

# Generate a default config on first run. Edit $CONFIG to change ports/DB;
# the camera feed window assumes http.port == $HTTP_PORT.
if [[ ! -f "$CONFIG" ]]; then
    cat > "$CONFIG" <<EOF
[database]
path = "$DB_PATH"
batch_size = 500

[ingestion]
port = 17101

[query]
port = 17102

[streaming]
port = 17103

[stats]
port = 17106

[admin]
port = 17104

[http]
port = $HTTP_PORT

[ports]
auto_detect = false
EOF
    echo "Wrote default config: $CONFIG"
fi

# Don't create a duplicate session.
if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "tmux session '$SESSION' already exists."
    echo "Attach with:"
    echo "  tmux attach -t $SESSION"
    exit 0
fi

# Create detached session with sqtseries.
tmux new-session -d \
    -s "$SESSION" \
    -n sqtseries \
    "cd '$WORKDIR' && exec '$PYTHON' -m sqtseries --config '$CONFIG' run"

# Overlay server (logs its random port to $OVERLAY_LOG).
tmux new-window \
    -t "$SESSION:" \
    -n overlay \
    "cd '$WORKDIR' && exec '$PYTHON' -u examples/camera/overlay_server.py 2>&1 | tee $OVERLAY_LOG"

# Wait for the overlay to report its WebSocket port, then start the feed.
OVERLAY_PORT=""
for _ in $(seq 1 50); do
    OVERLAY_PORT="$(grep -oP 'ws://localhost:\K[0-9]+' "$OVERLAY_LOG" 2>/dev/null | head -1 || true)"
    if [[ -n "$OVERLAY_PORT" ]]; then
        break
    fi
    sleep 0.2
done

if [[ -z "$OVERLAY_PORT" ]]; then
    echo "ERROR: overlay server did not report a WebSocket port."
    exit 1
fi

# Camera feed.
tmux new-window \
    -t "$SESSION:" \
    -n camera \
    "cd '$WORKDIR' && exec '$PYTHON' -u examples/camera/camera_feed.py --ws ws://localhost:$OVERLAY_PORT/ws/metrics --host 127.0.0.1 --http-port $HTTP_PORT"

echo "Started tmux session: $SESSION"
echo
echo "Attach with:"
echo "  tmux attach -t $SESSION"
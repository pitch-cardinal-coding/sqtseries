#!/usr/bin/env bash
set -euo pipefail

SESSION="sqtcam"
PYTHON="/home/iam/devcode/.env/sqtseries/bin/python3"
WORKDIR="/home/iam/devcode/sqtseries"
CONFIG="/tmp/sqtcam/conf.toml"
OVERLAY_LOG="/tmp/sqtcam/overlay.log"
HTTP_PORT="17105"

# Create working directory if it doesn't exist.
mkdir -p "$WORKDIR"

# Validate required config.
if [[ ! -f "$CONFIG" ]]; then
    echo "ERROR: Config file not found:"
    echo "  $CONFIG"
    exit 1
fi

# Validate Python executable.
if [[ ! -x "$PYTHON" ]]; then
    echo "ERROR: Python executable not found or not executable:"
    echo "  $PYTHON"
    exit 1
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
    "cd '$WORKDIR' && exec '$PYTHON' -u examples/overlay_server.py 2>&1 | tee $OVERLAY_LOG"

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
    "cd '$WORKDIR' && exec '$PYTHON' -u examples/camera_feed.py --ws ws://localhost:$OVERLAY_PORT/ws/metrics --host 127.0.0.1 --http-port $HTTP_PORT"

echo "Started tmux session: $SESSION"
echo
echo "Attach with:"
echo "  tmux attach -t $SESSION"
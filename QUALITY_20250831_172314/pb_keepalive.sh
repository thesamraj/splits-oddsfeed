#!/usr/bin/env bash
# PointsBet collector keepalive script
set -euo pipefail

PB_DIR="/Users/sam/Desktop/splits-oddsfeed/collectors/pointsbet_sandbox"
LOG_FILE="$PB_DIR/pb_collector.log"

cd "$PB_DIR" || { echo "PointsBet directory not found"; exit 1; }

while true; do
  if ! pgrep -fa "pb_collector.py" >/dev/null; then
    echo "[$(date)] Restarting pb_collector.py" | tee -a "$LOG_FILE"
    python3 pb_collector.py >> "$LOG_FILE" 2>&1 &
    disown
  fi
  sleep 15
done

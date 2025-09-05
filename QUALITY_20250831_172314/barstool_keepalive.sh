#!/usr/bin/env bash
# Barstool/ESPN BET collector keepalive script
set -euo pipefail

BS_DIR="/Users/sam/Desktop/splits-oddsfeed/collectors/barstool_sandbox"
LOG_FILE="$BS_DIR/barstool_collector.log"

cd "$BS_DIR" || { echo "Barstool directory not found"; exit 1; }

while true; do
  if ! pgrep -fa "barstool_collector.py" >/dev/null; then
    echo "[$(date)] Restarting barstool_collector.py" | tee -a "$LOG_FILE"
    python3 barstool_collector.py >> "$LOG_FILE" 2>&1 &
    disown
  fi
  sleep 15
done

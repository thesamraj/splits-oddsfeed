#!/bin/bash
# PointsBet collector host runner
# Runs collector on host machine to bypass Docker detection

cd /Users/sam/Desktop/splits-oddsfeed/collectors/pointsbet_sandbox

echo "Starting PointsBet collector on host machine..."
echo "This bypasses Docker detection issues"

# Install dependencies if needed
pip3 install redis requests 2>/dev/null || echo "Dependencies already installed"

# Run collector with auto-restart
while true; do
    echo "[$(date)] Starting PointsBet collector..."
    python3 pb_collector.py
    echo "[$(date)] Collector crashed, restarting in 5 seconds..."
    sleep 5
done

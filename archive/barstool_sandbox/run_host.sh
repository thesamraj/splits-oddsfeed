#!/bin/bash
# Run Barstool collector on host machine
# (Runs on host to avoid potential Docker blocking)

set -e

echo "Starting Barstool/ESPN BET collector on host..."
echo "This collector fetches from ESPN's public API"
echo "Press Ctrl+C to stop"
echo ""

# Change to script directory
cd "$(dirname "$0")"

# Install dependencies if needed
echo "Checking Python dependencies..."
pip3 install --quiet redis requests

# Run collector with auto-restart
while true; do
    echo "[$(date)] Starting Barstool collector..."
    python3 barstool_collector.py

    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 0 ]; then
        echo "[$(date)] Collector exited normally"
    else
        echo "[$(date)] Collector crashed with exit code $EXIT_CODE"
    fi

    echo "[$(date)] Restarting in 5 seconds..."
    sleep 5
done

#!/bin/bash
set -e

echo "Installing dependencies..."
pip install redis playwright --quiet

echo "Starting FanDuel collector..."
exec python /app/fd_simple_collector.py

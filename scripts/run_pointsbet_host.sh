#!/bin/bash

# PointsBet Host Collector - Runs outside Docker to avoid detection
# This script runs the PointsBet collector on the host machine

echo "Starting PointsBet collector on host..."

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Python3 is not installed. Please install Python3 first."
    exit 1
fi

# Install required packages if not already installed
pip3 install --quiet requests redis

# Run the collector directly on host, connecting to Docker Redis
cd /Users/sam/Desktop/splits-oddsfeed/collectors/pointsbet_sandbox

# Export environment variables
export REDIS_URL="redis://localhost:6379/0"
export CHANNEL="odds.raw.pointsbet"
export RUN_ON_HOST="true"

echo "Running PointsBet collector on host (bypassing Docker detection)..."
python3 pb_collector.py

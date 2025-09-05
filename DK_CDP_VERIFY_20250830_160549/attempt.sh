#!/bin/bash
set -euo pipefail
ROOT="/Users/sam/Desktop/splits-oddsfeed/DK_CDP_VERIFY_20250830_160549"
compose="docker compose -f docker-compose.yml -f docker-compose.override.dk-cdp.yml"

min="$1"
allow="$2"
xkeys="$3"
url="$4"

echo "[Attempt] MIN_JSON_BYTES=$min ALLOW_ALL=$allow EXTRA_KEYS=$xkeys" | tee -a "$ROOT/attempts.log"

# Stop and remove existing collector
$compose stop collector-dk-cdp >/dev/null 2>&1 || true
$compose rm -f collector-dk-cdp >/dev/null 2>&1 || true

# Create temp override with new env vars
cat > "$ROOT/dk.override.yml" <<YML
services:
  collector-dk-cdp:
    image: mcr.microsoft.com/playwright/python:v1.45.0-jammy
    working_dir: /app
    volumes:
      - ./collectors/dk_cdp:/app
    command: ["bash", "-c", "pip install redis && python -m playwright install chromium && python /app/collector.py"]
    environment:
      - REDIS_URL=redis://broker:6379/0
      - CHANNEL=odds.raw.dk.cdp
      - URL=$url
      - MIN_JSON_BYTES=$min
      - ALLOW_ALL=$allow
      - EXTRA_KEYS=$xkeys
    depends_on: [broker]
    restart: unless-stopped
YML

docker compose -f docker-compose.yml -f "$ROOT/dk.override.yml" up -d --build collector-dk-cdp
echo "Collector started, waiting for initialization..."
sleep 30

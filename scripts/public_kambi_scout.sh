#!/usr/bin/env bash
set -euo pipefail
OUT="/Users/sam/Desktop/splits-oddsfeed/scouts/SCOUT_$(date -u +%Y%m%d).log"
mkdir -p /Users/sam/Desktop/splits-oddsfeed/scouts
TOKENS=("rsi2uspa") # seed with known-good; append new candidates over time
for t in "${TOKENS[@]}"; do
  URL="https://eu-offering-api.kambicdn.com/offering/v2018/${t}/event/live/open.json"
  code=$(curl -fsS -o /dev/null -w "%{http_code}" "$URL" 2>/dev/null || echo "000")
  echo "[$(date -u +%F %T)] $t -> $code" >> "$OUT"
  sleep 2
done

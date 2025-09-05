#!/usr/bin/env bash
set -euo pipefail
OUT="scouts/scan_${HOSTNAME}_$(date -u +%Y%m%d_%H%M%S).log"
TOKENS=("rsi2uspa" "rsi2usnj" "br2uspa" "br2usnj" "betano" "betano-br" "betano-co" "betano-on")
for t in "${TOKENS[@]}"; do
  URL="https://eu-offering-api.kambicdn.com/offering/v2018/${t}/event/live/open.json"
  code=$(curl -s -o /dev/null -w "%{http_code}" "$URL" || echo 000)
  echo "$(date -u +%FT%TZ) $t $code $URL" | tee -a "$OUT"
  sleep 2
done

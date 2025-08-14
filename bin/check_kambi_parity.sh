#!/bin/bash
set -euo pipefail

# Kambi Parity Checker - API vs DB count validation
# Guards against regressions in the odds-first API query

echo "=== Kambi Parity Check ==="

# Database query - count distinct events with odds in last 15 minutes
DB_COUNT=$(docker compose exec -T store psql -U odds -d oddsfeed -tAc "
SELECT COUNT(DISTINCT event_id)
FROM odds
WHERE book = 'kambi'
  AND ts > now() - interval '15 minutes'
")

# API query - get count from /odds endpoint
API_RESPONSE=$(curl -s "http://localhost:8080/odds?book=kambi&minutes=15")
API_COUNT=$(echo "$API_RESPONSE" | jq -r '.count // 0')

echo "Database distinct events (15m): $DB_COUNT"
echo "API response count (15m): $API_COUNT"

DIFF=$((API_COUNT - DB_COUNT))
ABS_DIFF=${DIFF#-}  # absolute value

# Allow small timing differences (±2 events) since data is live
if [ "$ABS_DIFF" -le 2 ]; then
    echo "✅ PARITY CHECK PASSED - API count ($API_COUNT) ~= DB count ($DB_COUNT), diff: $DIFF"
    exit 0
else
    echo "❌ PARITY CHECK FAILED - API ($API_COUNT) != DB ($DB_COUNT), diff: $DIFF"
    echo "Debug API response:"
    echo "$API_RESPONSE" | jq '.'
    exit 1
fi

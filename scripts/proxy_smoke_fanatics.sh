#!/bin/bash
# Smoke test for Fanatics/PointsBet endpoints via proxy

set -e

# Load proxy env vars
if [ -f .env.proxy ]; then
    export $(grep -v '^#' .env.proxy | xargs)
fi

# Check required vars
if [ -z "$PROXY_HOST" ] || [ -z "$PROXY_PORT" ] || [ -z "$PROXY_USER" ] || [ -z "$PROXY_PASS" ]; then
    echo "ERROR: Missing proxy configuration. Set PROXY_HOST, PROXY_PORT, PROXY_USER, PROXY_PASS"
    exit 1
fi

# Build proxy URL
PROXY_URL="${PROXY_PROTO:-http}://${PROXY_USER}:${PROXY_PASS}@${PROXY_HOST}:${PROXY_PORT}"

echo "========================================="
echo "Fanatics/PointsBet Proxy Smoke Test"
echo "Proxy: ${PROXY_HOST}:${PROXY_PORT}"
echo "Tag: ${PROXY_TAG:-untagged}"
echo "========================================="

# Test endpoints
declare -a ENDPOINTS=(
    "https://api.on.pointsbet.com/api/mes/v3/events?competitionIds=5883"
    "https://api.on.pointsbet.com/api/mes/v4/competitions/league/2389"
    "https://api.nj.pointsbet.com/api/mes/v3/events?competitionIds=5883"
    "https://sb-content-cache.fanatics.com/api/v1/event/american-football"
    "https://sportsbook.fanatics.com/api/content/v1/leagues?sport=american-football"
)

PASS_COUNT=0
FAIL_COUNT=0

for i in "${!ENDPOINTS[@]}"; do
    URL="${ENDPOINTS[$i]}"
    echo -e "\n[$(($i+1))/${#ENDPOINTS[@]}] Testing: ${URL:0:60}..."

    HTTP_CODE=$(curl -s -x "$PROXY_URL" -o /dev/null -w "%{http_code}" \
        --connect-timeout 10 --max-time 20 \
        -H "User-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15" \
        -H "Accept: application/json" \
        -H "Origin: https://sportsbook.fanatics.com" \
        -H "Referer: https://sportsbook.fanatics.com/" \
        "$URL" 2>/dev/null || echo "000")

    echo "HTTP Code: $HTTP_CODE"

    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
        echo "✓ PASS"
        ((PASS_COUNT++))
    elif [ "$HTTP_CODE" = "403" ] || [ "$HTTP_CODE" = "401" ]; then
        echo "⚠ Auth/Geo blocked"
        ((FAIL_COUNT++))
    elif [ "$HTTP_CODE" = "000" ]; then
        echo "✗ Connection failed"
        ((FAIL_COUNT++))
    else
        echo "⚠ Unexpected code"
        ((FAIL_COUNT++))
    fi
done

echo -e "\n========================================="
echo "Results: $PASS_COUNT passed, $FAIL_COUNT failed"
if [ $PASS_COUNT -gt 0 ]; then
    echo "✓ At least one endpoint accessible via proxy"
else
    echo "✗ All endpoints blocked or failing"
fi
echo "========================================="
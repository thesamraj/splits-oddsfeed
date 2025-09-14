#!/bin/bash
# Smoke test for Kambi API via proxy

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
echo "Kambi API Proxy Smoke Test"
echo "Proxy: ${PROXY_HOST}:${PROXY_PORT}"
echo "Tag: ${PROXY_TAG:-untagged}"
echo "========================================="

# Test 1: Check proxy IP
echo -e "\n[1/3] Testing proxy connectivity..."
PROXY_IP=$(curl -s -x "$PROXY_URL" --connect-timeout 10 --max-time 15 https://httpbin.org/ip | grep -oP '"origin":\s*"\K[^"]+' || echo "FAILED")
if [ "$PROXY_IP" != "FAILED" ]; then
    echo "✓ Proxy IP: $PROXY_IP"
else
    echo "✗ Failed to get proxy IP"
    exit 1
fi

# Test 2: Kambi NFL endpoint (BetRivers)
echo -e "\n[2/3] Testing Kambi NFL API (BetRivers)..."
KAMBI_URL="https://eu-offering.kambicdn.org/offering/v2018/betrivers/listView/american_football/nfl.json"
RESPONSE=$(curl -s -x "$PROXY_URL" -w "\nHTTP_CODE:%{http_code}" --connect-timeout 10 --max-time 20 \
    -H "User-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15" \
    -H "Accept: application/json" \
    "$KAMBI_URL" 2>/dev/null || echo "CURL_FAILED")

if [[ "$RESPONSE" == "CURL_FAILED" ]]; then
    echo "✗ Curl failed"
    exit 1
fi

HTTP_CODE=$(echo "$RESPONSE" | grep "HTTP_CODE:" | cut -d: -f2)
BODY=$(echo "$RESPONSE" | sed '/HTTP_CODE:/d')
PREVIEW=$(echo "$BODY" | head -c 200)

echo "HTTP Code: $HTTP_CODE"
echo "First 200 bytes: $PREVIEW"

if [ "$HTTP_CODE" = "200" ] && [[ "$BODY" == *"events"* || "$BODY" == *"competitions"* ]]; then
    echo "✓ PASS: Kambi API accessible, contains events/competitions"
else
    echo "✗ FAIL: HTTP $HTTP_CODE or missing expected content"
fi

# Test 3: Alternative Kambi endpoint (SugarHouse)
echo -e "\n[3/3] Testing alternative Kambi endpoint (SugarHouse)..."
ALT_URL="https://eu-offering.kambicdn.org/offering/v2018/shou/listView/american_football/nfl.json"
ALT_RESPONSE=$(curl -s -x "$PROXY_URL" -o /dev/null -w "%{http_code}" --connect-timeout 10 --max-time 20 \
    -H "User-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15" \
    "$ALT_URL" 2>/dev/null || echo "000")

echo "SugarHouse HTTP Code: $ALT_RESPONSE"
if [ "$ALT_RESPONSE" = "200" ]; then
    echo "✓ Alternative endpoint accessible"
fi

echo -e "\n========================================="
echo "Smoke test complete"
echo "========================================="
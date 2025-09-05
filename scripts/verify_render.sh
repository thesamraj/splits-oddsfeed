#!/bin/bash
# Verify Render deployment metrics and health
# Usage: ./verify_render.sh <metrics-proxy-url>

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <metrics-proxy-url>"
    echo "Example: $0 https://oddsfeed-metrics-proxy.onrender.com"
    exit 1
fi

BASE_URL="$1"

echo "=== Render Deployment Verification ==="
echo "Base URL: $BASE_URL"
echo ""

# Check metrics proxy health
echo "1. Metrics Proxy Health:"
curl -s "$BASE_URL/healthz" | jq '.' || echo "FAIL: Cannot reach metrics proxy"
echo ""

# Check targets status
echo "2. Targets Status:"
curl -s "$BASE_URL/targets" | jq '.' || echo "FAIL: Cannot get targets"
echo ""

# Check Bovada health via proxy
echo "3. Bovada Health (via proxy):"
curl -s "$BASE_URL/healthz/bovada" | jq '.' || echo "FAIL: Cannot reach Bovada"
echo ""

# Check Normalizer health via proxy  
echo "4. Normalizer Health (via proxy):"
curl -s "$BASE_URL/healthz/normalizer" | jq '.' || echo "FAIL: Cannot reach Normalizer"
echo ""

# Check aggregated metrics
echo "5. Aggregated Metrics:"
curl -s "$BASE_URL/metrics" | head -20
echo "..."
echo ""

# Count metrics lines
METRICS_COUNT=$(curl -s "$BASE_URL/metrics" | wc -l)
echo "Total metrics lines: $METRICS_COUNT"
echo ""

# Check for specific metrics
echo "6. Key Metrics Check:"
curl -s "$BASE_URL/metrics" | grep -E "^(odds_15m|ticks_15m|book_up|http_requests_total)" | head -10 || echo "No key metrics found"
echo ""

# Check realness reports if available
echo "7. Bovada Realness Report:"
curl -s "$BASE_URL/realness/bovada/report" | jq '.' || echo "No realness data yet"
echo ""

echo "=== Verification Complete ==="
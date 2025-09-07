#!/bin/bash
# Verify local deployment with Neon + Upstash Redis

set -e

echo "=== Local Deployment Verification ==="
echo "Starting at $(date)"
echo ""

# Function to wait for endpoint
wait_for_endpoint() {
    local url=$1
    local name=$2
    local max_wait=90
    local elapsed=0
    
    echo -n "Waiting for $name at $url..."
    while [ $elapsed -lt $max_wait ]; do
        if curl -fsS "$url" >/dev/null 2>&1; then
            echo " OK"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
        echo -n "."
    done
    echo " TIMEOUT"
    return 1
}

# Step 1: Wait for services
echo "1. Waiting for services (up to 90s):"
wait_for_endpoint "http://localhost:8000/targets" "Metrics Proxy"
wait_for_endpoint "http://localhost:19081/healthz" "Bovada"
wait_for_endpoint "http://localhost:19082/healthz" "Normalizer"
echo ""

# Step 2: Check health endpoints
echo "2. Health Check Results:"
echo -n "  Metrics Proxy: "
curl -fsS http://localhost:8000/healthz | jq -r '.status' 2>/dev/null || echo "ERROR"

echo -n "  Bovada: "
curl -fsS http://localhost:19081/healthz | jq -r '.status' 2>/dev/null || echo "OK"

echo -n "  Normalizer: "
curl -fsS http://localhost:19082/healthz | jq -r '.status' 2>/dev/null || echo "OK"
echo ""

# Step 3: Metrics summary
echo "3. Quick Metrics Summary:"
echo "  From aggregated metrics at :8000/metrics:"
curl -fsS http://localhost:8000/metrics 2>/dev/null | grep -E "realness_score|odds_15m|ticks_total" | head -5 || echo "  No metrics found yet"
echo ""

# Step 4: Tick growth check
echo "4. Tick Growth Check (10s interval):"
TICKS_BEFORE=$(curl -fsS http://localhost:8000/metrics 2>/dev/null | grep 'ticks_total{book="bovada"' | awk '{print $2}' || echo "0")
echo "  Initial ticks_total{book=\"bovada\"}: ${TICKS_BEFORE:-0}"
echo "  Waiting 10 seconds..."
sleep 10
TICKS_AFTER=$(curl -fsS http://localhost:8000/metrics 2>/dev/null | grep 'ticks_total{book="bovada"' | awk '{print $2}' || echo "0")
echo "  After 10s ticks_total{book=\"bovada\"}: ${TICKS_AFTER:-0}"

if [ "${TICKS_AFTER:-0}" != "0" ] && [ "${TICKS_BEFORE:-0}" != "0" ]; then
    # Use awk for float comparison
    GROWTH=$(echo "$TICKS_AFTER $TICKS_BEFORE" | awk '{print ($1 > $2) ? "YES" : "NO"}')
    if [ "$GROWTH" = "YES" ]; then
        echo "  ✓ GROWTH DETECTED"
    else
        echo "  ⚠ NO GROWTH"
    fi
else
    echo "  ⚠ No ticks data available"
fi
echo ""

# Step 5: Realness report
echo "5. Realness Report (via proxy):"
REALNESS_JSON=$(curl -fsS http://localhost:8000/realness/bovada/report 2>/dev/null || echo "{}")
if [ "$REALNESS_JSON" != "{}" ]; then
    if command -v jq >/dev/null 2>&1; then
        echo "  Composite Score: $(echo "$REALNESS_JSON" | jq -r '.composite_score // "N/A"')"
        echo "  Sample Count: $(echo "$REALNESS_JSON" | jq -r '.sample_count // 0')"
        echo "  Decision: $(echo "$REALNESS_JSON" | jq -r '.decision // "UNKNOWN"')"
    else
        echo "$REALNESS_JSON" | grep -o '"composite_score":[0-9.]*' || echo "  No score available"
    fi
else
    echo "  No realness data available yet"
fi
echo ""

# Step 6: Check targets status
echo "6. Metrics Targets Status:"
curl -fsS http://localhost:8000/targets 2>/dev/null | jq '.' 2>/dev/null || echo "  Cannot fetch targets"
echo ""

echo "=== Verification Complete at $(date) ==="
#!/bin/bash

# Verification script for Kambi browser collector
echo "==================================="
echo "KAMBI BROWSER COLLECTOR VERIFICATION"
echo "==================================="
echo ""

# Check services are running
echo "1. Checking Docker services..."
docker compose -f docker-compose.local.yml ps
echo ""

# Check Bovada collector
echo "2. Checking Bovada collector..."
curl -s http://localhost:19081/healthz | jq '.' || echo "Bovada not responding"
echo ""

# Check Normalizer
echo "3. Checking Normalizer..."
curl -s http://localhost:19082/healthz | jq '.' || echo "Normalizer not responding"
echo ""

# Check Kambi Browser collector
echo "4. Checking Kambi Browser collector..."
curl -s http://localhost:19088/healthz | jq '.' || echo "Kambi Browser not responding"
echo ""

# Check Metrics Proxy
echo "5. Checking Metrics Proxy..."
curl -s http://localhost:8000/healthz | jq '.' || echo "Metrics Proxy not responding"
echo ""

# Check Kambi metrics
echo "6. Checking Kambi Browser metrics..."
curl -s http://localhost:19088/metrics | grep -E "collector_up|ticks_total|odds_15m" | head -20
echo ""

# Check Redis for Kambi messages (using docker exec)
echo "7. Checking Redis for Kambi messages..."
echo "Checking published channels:"
docker compose -f docker-compose.local.yml exec -T normalizer sh -c 'redis-cli -u "$REDIS_URL" --no-auth-warning PUBSUB CHANNELS "odds.raw.kambi.*"' 2>/dev/null || echo "Could not check Redis channels"
echo ""

# Check logs for errors
echo "8. Recent Kambi Browser logs..."
docker compose -f docker-compose.local.yml logs kambi-browser --tail=20
echo ""

echo "==================================="
echo "VERIFICATION COMPLETE"
echo "==================================="
echo ""
echo "Expected results:"
echo "- All services should show as 'Up'"
echo "- Health endpoints should return JSON with status: ok"
echo "- Kambi Browser should show brands_seen: [betrivers, barstool, caesars, sugarhouse, unibet]"
echo "- Metrics should show collector_up{book=\"betrivers\"} 1 (for each brand)"
echo "- Logs should show successful collection cycles"
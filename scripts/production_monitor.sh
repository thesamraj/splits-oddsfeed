#!/bin/bash

# Production Monitoring Script
# Monitors all aspects of the odds collection pipeline

echo "==========================================="
echo "PRODUCTION MONITORING REPORT"
echo "Generated: $(date)"
echo "==========================================="

# Check container health
echo -e "\n[CONTAINER STATUS]"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "broker|store|api|normalizer|collector" | head -20

# Check Redis channels
echo -e "\n[ACTIVE REDIS CHANNELS]"
docker exec splits-oddsfeed-broker-1 redis-cli PUBSUB CHANNELS "odds.raw.*" 2>/dev/null | sort

# Check message flow rates
echo -e "\n[MESSAGE FLOW (Last 60s)]"
for book in bovada draftkings fanduel betmgm betrivers barstool sugarhouse unibet caesars pinnacle pointsbet; do
    count=$(docker exec splits-oddsfeed-broker-1 redis-cli --raw LLEN "stats:$book:60s" 2>/dev/null || echo "0")
    if [ "$count" != "0" ]; then
        echo "$book: $count messages"
    fi
done

# Check normalizer processing
echo -e "\n[NORMALIZER STATUS]"
docker logs splits-oddsfeed-normalizer-1 2>&1 | grep -E "INFO.*Stored|ERROR" | tail -5

# Check database storage
echo -e "\n[DATABASE STORAGE]"
docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -c "
SELECT
    COUNT(*) as total_odds,
    COUNT(DISTINCT book) as active_books,
    MAX(created_at) as latest_update
FROM odds
WHERE created_at > NOW() - INTERVAL '1 hour';" 2>/dev/null || echo "No recent data"

# Check collector errors
echo -e "\n[COLLECTOR ERRORS (Last 5 min)]"
for container in $(docker ps --format "{{.Names}}" | grep collector); do
    errors=$(docker logs $container 2>&1 | grep -c ERROR || echo "0")
    if [ "$errors" -gt "0" ]; then
        echo "$container: $errors errors"
        docker logs $container 2>&1 | grep ERROR | tail -2
    fi
done

# Browser collector specific status
echo -e "\n[BROWSER COLLECTORS]"
for browser in draftkings-browser fanduel-browser; do
    container="splits-oddsfeed-collector-${browser}-1"
    if docker ps | grep -q $container; then
        echo -e "\n$browser:"
        docker logs $container 2>&1 | grep -E "Published|No live odds|Error" | tail -3
    fi
done

# Check WebSocket collectors
echo -e "\n[WEBSOCKET COLLECTORS]"
for ws in draftkings-ws fanduel-ws; do
    container="splits-oddsfeed-collector-${ws}-1"
    if docker ps | grep -q $container; then
        echo -e "\n$ws:"
        docker logs $container 2>&1 | tail -3
    fi
done

echo -e "\n==========================================="
echo "END OF REPORT"
echo "==========================================="

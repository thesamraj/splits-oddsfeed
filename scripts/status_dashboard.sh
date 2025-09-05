#!/bin/bash

# Status Dashboard - Shows comprehensive system status

echo "=============================================="
echo "ODDS COLLECTION SYSTEM STATUS DASHBOARD"
echo "Generated: $(date)"
echo "=============================================="

# Book Status
echo -e "\n[BOOK COVERAGE STATUS]"
echo "----------------------"

# Get book data from database
docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -t -c "
SELECT
    book,
    COUNT(*) as odds,
    COUNT(DISTINCT event_id) as events,
    EXTRACT(EPOCH FROM (NOW() - MAX(ts)))::INTEGER as age_seconds
FROM odds
WHERE ts > NOW() - INTERVAL '15 minutes'
GROUP BY book
ORDER BY book;" | while read book odds events age; do
    if [ ! -z "$book" ]; then
        # Determine status
        if [ "$age" -lt "60" ]; then
            status="✓ LIVE"
        elif [ "$age" -lt "300" ]; then
            status="⚠ STALE"
        else
            status="✗ DOWN"
        fi

        printf "%-12s: %s (%.0f odds, %.0f events, %ds ago)\n" "$book" "$status" "$odds" "$events" "$age"
    fi
done

# Missing books
echo -e "\nMissing books:"
for book in sugarhouse unibet caesars; do
    found=$(docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -t -c "
        SELECT COUNT(*) FROM odds WHERE book='$book' AND ts > NOW() - INTERVAL '15 minutes';" | tr -d ' ')
    if [ "$found" -eq "0" ]; then
        echo "  - $book: No recent data"
    fi
done

# Collector Health
echo -e "\n[COLLECTOR HEALTH]"
echo "------------------"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep collector | head -15

# Redis Activity
echo -e "\n[REDIS CHANNEL ACTIVITY]"
echo "------------------------"
channels=$(docker exec splits-oddsfeed-broker-1 redis-cli PUBSUB CHANNELS "odds.raw.*" | wc -l)
echo "Active channels: $channels"
docker exec splits-oddsfeed-broker-1 redis-cli PUBSUB CHANNELS "odds.raw.*" | head -10

# Normalizer Status
echo -e "\n[NORMALIZER PROCESSING]"
echo "-----------------------"
docker logs splits-oddsfeed-normalizer-1 2>&1 | grep "Stored" | tail -5

# Database Summary
echo -e "\n[DATABASE SUMMARY]"
echo "------------------"
docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -c "
SELECT
    COUNT(*) as total_odds,
    COUNT(DISTINCT book) as active_books,
    COUNT(DISTINCT event_id) as total_events,
    MIN(ts) as oldest_record,
    MAX(ts) as newest_record
FROM odds
WHERE ts > NOW() - INTERVAL '1 hour';"

# Performance Metrics
echo -e "\n[PERFORMANCE METRICS]"
echo "---------------------"
echo "CPU Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}" | grep -E "normalizer|broker|store" | head -5

echo -e "\nMemory Usage:"
docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}" | grep -E "normalizer|broker|store" | head -5

# Alert Status
echo -e "\n[SYSTEM ALERTS]"
echo "---------------"

# Check for critical issues
issues=0

# Check database connectivity
if ! docker exec splits-oddsfeed-store-1 pg_isready -U odds &>/dev/null; then
    echo "⚠️  CRITICAL: Database not responding"
    ((issues++))
fi

# Check Redis connectivity
if ! docker exec splits-oddsfeed-broker-1 redis-cli ping &>/dev/null; then
    echo "⚠️  CRITICAL: Redis not responding"
    ((issues++))
fi

# Check normalizer
if ! docker ps | grep -q normalizer; then
    echo "⚠️  CRITICAL: Normalizer not running"
    ((issues++))
fi

# Check data flow
recent=$(docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -t -c "
    SELECT COUNT(*) FROM odds WHERE ts > NOW() - INTERVAL '2 minutes';" 2>/dev/null | tr -d ' ')
if [ "$recent" -lt "100" ]; then
    echo "⚠️  WARNING: Low data flow ($recent records in 2 minutes)"
    ((issues++))
fi

if [ "$issues" -eq "0" ]; then
    echo "✅ All systems operational"
fi

echo -e "\n=============================================="
echo "END OF STATUS DASHBOARD"
echo "=============================================="

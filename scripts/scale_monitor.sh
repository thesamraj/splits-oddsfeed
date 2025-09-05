#!/bin/bash

# Scaling Monitor - Track performance of optimized collectors

echo "========================================="
echo "SCALING PERFORMANCE MONITOR"
echo "Generated: $(date)"
echo "========================================="

# Check optimized collector performance
echo -e "\n[OPTIMIZED COLLECTORS]"
for book in bovada barstool; do
    container="splits-oddsfeed-collector-${book}-1"

    # Get interval from environment
    interval=$(docker inspect $container 2>/dev/null | grep "INTERVAL" -A1 | tail -1 | cut -d'"' -f4)

    # Count recent messages
    count=$(docker exec splits-oddsfeed-broker-1 redis-cli --raw LLEN "stats:$book:60s" 2>/dev/null || echo "0")

    echo "$book: Interval=${interval}s, Messages/min=${count}"
done

# Check storage rates
echo -e "\n[STORAGE RATES (Last 5 min)]"
docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -c "
SELECT
    book,
    COUNT(*) as records,
    COUNT(*)/5.0 as records_per_min,
    COUNT(DISTINCT event_id) as unique_events
FROM odds
WHERE ts > NOW() - INTERVAL '5 minutes'
GROUP BY book
ORDER BY records DESC;" 2>&1

# Check normalizer throughput
echo -e "\n[NORMALIZER THROUGHPUT]"
docker logs splits-oddsfeed-normalizer-1 2>&1 | grep "Stored" | tail -10 | awk '{print $2, $8, $9, $10, $11}'

# Check system resources
echo -e "\n[RESOURCE USAGE]"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" | grep -E "collector|normalizer|broker|store"

echo -e "\n========================================="

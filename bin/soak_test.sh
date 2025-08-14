#!/bin/bash
set -euo pipefail

# 5-minute soak test with freshness monitoring
echo "=== 5-Minute Kambi Soak Test ==="

START_TIME=$(date +%s)
END_TIME=$((START_TIME + 300))  # 5 minutes
SAMPLES=()

echo "Starting soak test at $(date)"
echo "Will run until $(date -d @$END_TIME)"

while [ $(date +%s) -lt $END_TIME ]; do
    CURRENT=$(date +%s)

    # Get freshness data
    FRESHNESS_RESPONSE=$(curl -s http://localhost:9200/metrics | grep kambi_last_insert_ts_seconds || echo "kambi_last_insert_ts_seconds 0")
    LAST_INSERT_TS=$(echo "$FRESHNESS_RESPONSE" | awk '{print $2}')

    if [ "$LAST_INSERT_TS" != "0" ]; then
        STALENESS=$((CURRENT - ${LAST_INSERT_TS%.*}))  # Remove decimal part
        SAMPLES+=($STALENESS)
        echo "[$(date +'%H:%M:%S')] Staleness: ${STALENESS}s, Events: $(curl -s 'http://localhost:8080/odds?book=kambi&minutes=15' | jq -r '.count // 0')"
    else
        echo "[$(date +'%H:%M:%S')] No freshness data available"
    fi

    sleep 30  # Sample every 30 seconds
done

echo ""
echo "=== Soak Test Results ==="

if [ ${#SAMPLES[@]} -gt 0 ]; then
    # Sort samples for percentile calculation
    IFS=$'\n' SORTED_SAMPLES=($(sort -n <<<"${SAMPLES[*]}"))
    unset IFS

    COUNT=${#SORTED_SAMPLES[@]}
    P50_INDEX=$((COUNT / 2))
    P95_INDEX=$((COUNT * 95 / 100))

    MIN=${SORTED_SAMPLES[0]}
    MAX=${SORTED_SAMPLES[$((COUNT-1))]}
    P50=${SORTED_SAMPLES[$P50_INDEX]}
    P95=${SORTED_SAMPLES[$P95_INDEX]}

    # Calculate average
    SUM=0
    for sample in "${SORTED_SAMPLES[@]}"; do
        SUM=$((SUM + sample))
    done
    AVG=$((SUM / COUNT))

    echo "Staleness Statistics (seconds):"
    echo "  Samples: $COUNT"
    echo "  Min: ${MIN}s"
    echo "  Avg: ${AVG}s"
    echo "  P50: ${P50}s"
    echo "  P95: ${P95}s"
    echo "  Max: ${MAX}s"

    # Final parity check
    echo ""
    echo "=== Final Parity Check ==="
    /Users/sam/Desktop/splits-oddsfeed/bin/check_kambi_parity.sh
else
    echo "No staleness samples collected"
fi

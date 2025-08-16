#!/bin/bash
set -euo pipefail

# Soak test for Kambi WebSocket latency - 10 minute validation
DURATION_MINUTES=${DURATION_MINUTES:-10}
INTERVAL_SECONDS=30
MAX_P50_MS=2000
MAX_P95_MS=5000
MAX_PARITY_DIFF=2

PROM_URL="http://localhost:9090"
API_URL="http://localhost:8080"

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

get_histogram_quantile() {
    local metric="$1"
    local quantile="$2"
    local timespan="${3:-5m}"

    # Query Prometheus for histogram quantile
    query="histogram_quantile(${quantile}, rate(${metric}_bucket[${timespan}]))"
    result=$(curl -s "${PROM_URL}/api/v1/query" --data-urlencode "query=${query}" | \
        jq -r '.data.result[0].value[1] // "0"')
    echo "${result}"
}

get_api_count() {
    local minutes="$1"
    result=$(curl -s "${API_URL}/odds?book=kambi&minutes=${minutes}" | \
        jq -r '.count // 0')
    echo "${result}"
}

get_db_count() {
    local minutes="$1"
    # Query database via docker exec since psql may not be installed locally
    result=$(docker compose exec -T store psql -U odds -d oddsfeed -t -c \
        "SELECT COUNT(*) FROM odds WHERE book='kambi' AND ts > NOW() - INTERVAL '${minutes} minutes';" | \
        tr -d ' ')
    echo "${result}"
}

get_backlog() {
    result=$(curl -s "http://localhost:9200/metrics" | \
        grep '^kambi_norm_backlog ' | awk '{print $2}' | head -1)
    echo "${result:-0}"
}

# Optional: restart containers to clear metrics
if [[ "${1:-}" == "--restart" ]]; then
    log "Restarting browser and normalizer to clear metrics"
    docker compose restart collector-kambi-browser normalizer
    log "Waiting 30s for startup..."
    sleep 30
fi

log "Starting 10-minute Kambi WebSocket latency soak test"
log "Acceptance criteria: p50 ≤ ${MAX_P50_MS}ms, p95 ≤ ${MAX_P95_MS}ms, parity diff ≤ ${MAX_PARITY_DIFF}"

iterations=$((DURATION_MINUTES * 60 / INTERVAL_SECONDS))
start_time=$(date +%s)

all_p50_values=()
all_p95_values=()
all_api_counts=()
all_db_counts=()
all_diffs=()

for i in $(seq 1 $iterations); do
    current_time=$(date +%s)
    elapsed_minutes=$(( (current_time - start_time) / 60 ))

    # Get metrics
    p50=$(get_histogram_quantile "kambi_e2e_latency_ms" "0.5")
    p95=$(get_histogram_quantile "kambi_e2e_latency_ms" "0.95")
    api_count=$(get_api_count 15)
    db_count=$(get_db_count 15)
    diff_count=$((api_count > db_count ? api_count - db_count : db_count - api_count))
    backlog=$(get_backlog)

    # Convert to integers for comparison
    p50_int=$(echo "$p50" | grep -E '^[0-9]+' | cut -d. -f1 || echo "0")
    p95_int=$(echo "$p95" | grep -E '^[0-9]+' | cut -d. -f1 || echo "0")

    # Store values for final analysis
    all_p50_values+=("$p50_int")
    all_p95_values+=("$p95_int")
    all_api_counts+=("$api_count")
    all_db_counts+=("$db_count")
    all_diffs+=("$diff_count")

    log "[$elapsed_minutes/${DURATION_MINUTES}min] p50=${p50_int}ms p95=${p95_int}ms API=${api_count} DB=${db_count} diff=${diff_count} backlog=${backlog}"

    # Early failure detection
    if [[ $p50_int -gt $MAX_P50_MS ]]; then
        log "❌ FAIL: p50 latency $p50_int ms exceeds limit $MAX_P50_MS ms"
    fi

    if [[ $p95_int -gt $MAX_P95_MS ]]; then
        log "❌ FAIL: p95 latency $p95_int ms exceeds limit $MAX_P95_MS ms"
    fi

    if [[ $diff_count -gt $MAX_PARITY_DIFF ]]; then
        log "⚠️  WARN: Parity diff $diff_count exceeds limit $MAX_PARITY_DIFF"
    fi

    if [[ $i -lt $iterations ]]; then
        sleep $INTERVAL_SECONDS
    fi
done

log "🏁 Soak test completed. Analyzing results..."

# Calculate final statistics
final_p50=$(printf '%s\n' "${all_p50_values[@]}" | sort -n | awk '{a[NR]=$0} END {print (NR%2==1) ? a[(NR+1)/2] : (a[NR/2]+a[NR/2+1])/2}')
final_p95=$(printf '%s\n' "${all_p95_values[@]}" | sort -n | awk '{a[NR]=$0} END {print a[int(NR*0.95)]}')
max_diff=$(printf '%s\n' "${all_diffs[@]}" | sort -n | tail -1)
avg_api=$(printf '%s\n' "${all_api_counts[@]}" | awk '{sum+=$1} END {print int(sum/NR)}')

# Get additional metrics
capture_to_publish=$(get_histogram_quantile "kambi_capture_to_publish_ms" "0.5")
publish_to_normalize=$(get_histogram_quantile "kambi_publish_to_normalize_ms" "0.5")

log ""
log "📊 FINAL RESULTS"
log "================"
log "End-to-end latency:"
log "  • Median p50: ${final_p50}ms (limit: ${MAX_P50_MS}ms)"
log "  • p95: ${final_p95}ms (limit: ${MAX_P95_MS}ms)"
log ""
log "Parity check (15min window):"
log "  • Max API vs DB diff: ${max_diff} (limit: ${MAX_PARITY_DIFF})"
log "  • Avg API events: ${avg_api} (target: ≥8)"
log ""
log "Component latencies:"
log "  • Median capture→publish: ${capture_to_publish}ms"
log "  • Median publish→normalize: ${publish_to_normalize}ms"
log ""

# Determine pass/fail
exit_code=0

if [[ $(echo "$final_p50 > $MAX_P50_MS" | bc -l) -eq 1 ]]; then
    log "❌ FAILED: p50 latency $final_p50 ms > $MAX_P50_MS ms"
    exit_code=1
fi

if [[ $(echo "$final_p95 > $MAX_P95_MS" | bc -l) -eq 1 ]]; then
    log "❌ FAILED: p95 latency $final_p95 ms > $MAX_P95_MS ms"
    exit_code=1
fi

if [[ $max_diff -gt $MAX_PARITY_DIFF ]]; then
    log "❌ FAILED: Max parity diff $max_diff > $MAX_PARITY_DIFF"
    exit_code=1
fi

if [[ $avg_api -lt 8 ]]; then
    log "❌ FAILED: Avg API events $avg_api < 8"
    exit_code=1
fi

if [[ $exit_code -eq 0 ]]; then
    log "✅ ALL TESTS PASSED"
else
    log "❌ SOME TESTS FAILED"
fi

log ""
log "Prometheus queries used:"
log "  • p50: histogram_quantile(0.5, rate(kambi_e2e_latency_ms_bucket[5m]))"
log "  • p95: histogram_quantile(0.95, rate(kambi_e2e_latency_ms_bucket[5m]))"
log ""

exit $exit_code

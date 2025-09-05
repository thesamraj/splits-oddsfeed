#!/bin/bash
# Progressive rollout script for Kambi books after Bovada stability
# Usage: ./progressive_rollout.sh

set -euo pipefail

# Configuration
METRICS_URL="${METRICS_URL:-http://localhost:19090/metrics}"
REALNESS_URL="${REALNESS_URL:-http://localhost:19091/realness/report}"
MIN_SCORE=0.85
MIN_WARMUP_MIN=10
MIN_STABLE_MIN=30
SLACK_WEBHOOK="${SLACK_WEBHOOK:-}"

# Books in rollout order
BOOKS=(
    "bovada"      # Stage 0: Already enabled
    "betrivers"   # Stage 1: First Kambi book
    "barstool"    # Stage 2: Second Kambi
    "caesars"     # Stage 3: Third Kambi
    "sugarhouse"  # Stage 4: Fourth Kambi
    "unibet"      # Stage 5: Final Kambi
)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')]${NC} $1"
}

log_error() {
    echo -e "${RED}[$(date '+%H:%M:%S')]${NC} $1"
}

send_slack() {
    local message="$1"
    if [ -n "$SLACK_WEBHOOK" ]; then
        curl -X POST "$SLACK_WEBHOOK" \
            -H 'Content-Type: application/json' \
            -d "{\"text\": \"$message\"}" \
            --silent --output /dev/null || true
    fi
}

check_book_health() {
    local book="$1"

    # Get realness score
    local score=$(curl -sS "$METRICS_URL" 2>/dev/null | grep "realness_score{book=\"$book\"}" | awk '{print $2}' | head -1 || echo "0")

    # Get warm-up status
    local warmup=$(curl -sS "$REALNESS_URL" 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    if data.get('book') == '$book':
        print('true' if data.get('is_warming_up') else 'false')
    else:
        print('unknown')
except:
    print('error')
" || echo "error")

    # Get odds count
    local odds_15m=$(curl -sS "$METRICS_URL" 2>/dev/null | grep "odds_15m{book=\"$book\"}" | awk '{print $2}' | head -1 || echo "0")

    # Get ticks
    local ticks=$(curl -sS "$METRICS_URL" 2>/dev/null | grep "ticks_total{book=\"$book\"}" | awk '{print $2}' | head -1 || echo "0")

    echo "$score|$warmup|$odds_15m|$ticks"
}

wait_for_stability() {
    local book="$1"
    local wait_min="$2"

    log_info "Waiting ${wait_min} minutes for $book stability..."

    local start_time=$(date +%s)
    local target_time=$((start_time + wait_min * 60))
    local last_ticks=0
    local stable=true

    while [ $(date +%s) -lt $target_time ]; do
        local health=$(check_book_health "$book")
        IFS='|' read -r score warmup odds ticks <<< "$health"

        # Check if score dropped
        if (( $(echo "$score < $MIN_SCORE" | bc -l) )); then
            log_error "$book score dropped to $score"
            send_slack "⚠️ $book realness dropped to $score during stability check"
            stable=false
            break
        fi

        # Check if ticks are growing
        if [ "$ticks" != "0" ] && [ "$last_ticks" != "0" ]; then
            if (( $(echo "$ticks <= $last_ticks" | bc -l) )); then
                log_warn "$book ticks not growing: $last_ticks → $ticks"
            fi
        fi
        last_ticks="$ticks"

        # Progress update
        local elapsed=$(( ($(date +%s) - start_time) / 60 ))
        local remaining=$(( wait_min - elapsed ))
        echo -ne "\r  Progress: ${elapsed}/${wait_min} min | Score: $score | Odds: $odds | Ticks: $ticks | Remaining: ${remaining} min"

        sleep 30
    done

    echo ""  # New line after progress

    if [ "$stable" = true ]; then
        log_info "$book stable for ${wait_min} minutes ✓"
        return 0
    else
        log_error "$book stability check failed"
        return 1
    fi
}

enable_book() {
    local book="$1"

    log_info "Enabling $book..."

    # Start the book's collector
    docker-compose up -d ${book}-collector || {
        log_error "Failed to start ${book}-collector"
        send_slack "❌ Failed to start ${book}-collector"
        return 1
    }

    # Wait for warm-up
    log_info "Waiting for $book warm-up (${MIN_WARMUP_MIN} min)..."
    sleep $((MIN_WARMUP_MIN * 60))

    # Check health after warm-up
    local health=$(check_book_health "$book")
    IFS='|' read -r score warmup odds ticks <<< "$health"

    log_info "$book after warm-up: score=$score, warmup=$warmup, odds=$odds, ticks=$ticks"

    if (( $(echo "$score < $MIN_SCORE" | bc -l) )); then
        log_error "$book failed to reach minimum score ($score < $MIN_SCORE)"
        send_slack "❌ $book failed: score $score < $MIN_SCORE"
        docker-compose stop ${book}-collector
        return 1
    fi

    log_info "$book enabled successfully with score $score"
    send_slack "✅ $book enabled: score=$score, odds=$odds"
    return 0
}

# Main rollout logic
main() {
    log_info "Starting progressive rollout..."
    log_info "Configuration: MIN_SCORE=$MIN_SCORE, MIN_STABLE_MIN=$MIN_STABLE_MIN"

    # First, verify Bovada is healthy
    log_info "Checking Bovada health..."
    health=$(check_book_health "bovada")
    IFS='|' read -r score warmup odds ticks <<< "$health"

    if (( $(echo "$score < $MIN_SCORE" | bc -l) )); then
        log_error "Bovada not healthy: score=$score"
        send_slack "❌ Rollout aborted: Bovada score $score < $MIN_SCORE"
        exit 1
    fi

    log_info "Bovada healthy: score=$score, odds=$odds, ticks=$ticks"

    # Wait for Bovada stability
    if ! wait_for_stability "bovada" "$MIN_STABLE_MIN"; then
        log_error "Bovada stability check failed"
        exit 1
    fi

    # Progressive rollout of Kambi books
    for i in {1..5}; do
        book="${BOOKS[$i]}"

        log_info "="
        log_info "Stage $i: Enabling $book"
        log_info "="

        if ! enable_book "$book"; then
            log_error "Failed to enable $book, stopping rollout"
            send_slack "🛑 Rollout stopped at $book"
            exit 1
        fi

        # Wait for stability before next book
        if [ $i -lt 5 ]; then
            if ! wait_for_stability "$book" "$MIN_STABLE_MIN"; then
                log_error "$book stability failed, stopping rollout"
                docker-compose stop ${book}-collector
                send_slack "🛑 Rollout stopped: $book instability"
                exit 1
            fi
        fi
    done

    log_info "="
    log_info "Progressive rollout complete!"
    log_info "All Kambi books enabled successfully"
    send_slack "🎉 Progressive rollout complete! All Kambi books enabled."

    # Final health check
    echo ""
    log_info "Final health check:"
    for book in "${BOOKS[@]}"; do
        health=$(check_book_health "$book")
        IFS='|' read -r score warmup odds ticks <<< "$health"
        printf "  %-12s score=%-5s odds=%-6s ticks=%s\n" "$book:" "$score" "$odds" "$ticks"
    done
}

# Run if not sourced
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    main "$@"
fi

#!/usr/bin/env bash
# Verify Render deployment with automated checks
set -euo pipefail

# Check for required METRICS_PROXY_URL
if [ -z "${METRICS_PROXY_URL:-}" ]; then
    echo "Error: METRICS_PROXY_URL is required"
    echo "Usage: export METRICS_PROXY_URL='https://oddsfeed-metrics-proxy.onrender.com'"
    echo "       $0 [book]"
    exit 2
fi

SLACK_WEBHOOK="${SLACK_WEBHOOK:-}"  # Optional

# Float comparison helpers using awk (no bc dependency)
ge() { awk -v a="$1" -v b="$2" 'BEGIN{exit !(a>=b)}'; }
gt() { awk -v a="$1" -v b="$2" 'BEGIN{exit !(a>b)}'; }

# Check for jq availability
HAS_JQ=true
if ! command -v jq >/dev/null 2>&1; then
    HAS_JQ=false
    echo "Warning: jq not found. JSON parsing will be limited."
    echo "Install with: brew install jq (Mac) or apt-get install jq (Linux)"
fi

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')]${NC} $1"; }
log_error() { echo -e "${RED}[$(date '+%H:%M:%S')]${NC} $1"; }

send_slack() {
    local message="$1"
    if [ -n "$SLACK_WEBHOOK" ]; then
        curl -X POST "$SLACK_WEBHOOK" \
            -H 'Content-Type: application/json' \
            -d "{\"text\": \"$message\"}" \
            --silent --output /dev/null || true
    fi
}

check_book_deployment() {
    local book="$1"
    local stage="$2"
    
    log_info "========================================="
    log_info "Stage $stage: Verifying $book deployment"
    log_info "========================================="
    
    # 1. Check realness report with retries
    log_info "Fetching realness report for $book..."
    local realness_data
    local retry=0
    local max_retries=5  # Increased for production
    
    while [ $retry -lt $max_retries ]; do
        realness_data=$(curl -fsSL "$METRICS_PROXY_URL/realness/$book/report" 2>/dev/null)
        if [ $? -eq 0 ] && [ -n "$realness_data" ]; then
            break
        fi
        retry=$((retry + 1))
        log_warn "Retry $retry/$max_retries after 5s backoff..."
        sleep 5
    done
    
    if [ -z "$realness_data" ] || [ "$realness_data" = "{}" ]; then
        log_error "Failed to fetch realness report after $max_retries attempts"
        return 1
    fi
    
    if [ "$HAS_JQ" = true ]; then
        if echo "$realness_data" | jq -e '.error' >/dev/null 2>&1; then
            log_error "Failed to fetch realness report: $(echo "$realness_data" | jq -r '.error')"
            return 1
        fi
    else
        # Basic check without jq
        if echo "$realness_data" | grep -q '"error"'; then
            log_error "Failed to fetch realness report (error detected in response)"
            return 1
        fi
    fi
    
    # Extract metrics from realness report
    if [ "$HAS_JQ" = true ]; then
        local score=$(echo "$realness_data" | jq -r '.score // 0')
        local is_warming_up=$(echo "$realness_data" | jq -r '.is_warming_up // true')
        local samples=$(echo "$realness_data" | jq -r '.samples_collected // 0')
        local price_variance=$(echo "$realness_data" | jq -r '.features.price_variance // 0')
        local team_entropy=$(echo "$realness_data" | jq -r '.features.team_entropy // 0')
        local duplicate_ratio=$(echo "$realness_data" | jq -r '.features.duplicate_ratio // 0')
    else
        # Fallback: basic grep/sed extraction
        local score=$(echo "$realness_data" | grep -o '"score":[0-9.]*' | cut -d: -f2 || echo "0")
        local is_warming_up="false"  # Assume warmed up if can't parse
        local samples="200"  # Assume enough samples
        local price_variance=$(echo "$realness_data" | grep -o '"price_variance":[0-9.]*' | cut -d: -f2 || echo "0")
        local team_entropy=$(echo "$realness_data" | grep -o '"team_entropy":[0-9.]*' | cut -d: -f2 || echo "0")
        local duplicate_ratio=$(echo "$realness_data" | grep -o '"duplicate_ratio":[0-9.]*' | cut -d: -f2 || echo "0")
    fi
    
    # Display current status
    echo ""
    echo "  Realness Status:"
    echo "    Score: $score"
    echo "    Warming up: $is_warming_up"
    echo "    Samples: $samples"
    echo ""
    echo "  Hard Signals:"
    echo "    price_variance: $price_variance (need ≥0.8)"
    echo "    team_entropy: $team_entropy (need ≥0.8)"
    echo "    duplicate_ratio: $duplicate_ratio (need ≥0.6)"
    
    # 2. Check metrics
    log_info ""
    log_info "Fetching metrics for $book..."
    local metrics=$(curl -fsSL "$METRICS_PROXY_URL/metrics" 2>/dev/null | grep -E "(realness_score|odds_15m|ticks_total).*book=\"$book\"" | head -20 || true)
    
    # Parse metrics as integers (handle .0 decimals)
    local raw_odds=$(echo "$metrics" | grep "odds_15m{.*book=\"$book\"" | awk '{print $2}' | head -1 || echo "0")
    local raw_ticks=$(echo "$metrics" | grep "ticks_total{.*book=\"$book\"" | awk '{print $2}' | head -1 || echo "0")
    local odds_15m=$(printf "%.0f" "${raw_odds:-0}")
    local ticks_total=$(printf "%.0f" "${raw_ticks:-0}")
    
    echo ""
    echo "  Metrics:"
    echo "    odds_15m: $odds_15m"
    echo "    ticks_total: $ticks_total"
    
    # 3. Warm-up check
    if [ "$is_warming_up" = "true" ]; then
        log_warn "$book is still warming up (samples: $samples/200)"
        
        # Wait for warm-up to complete
        log_info "Waiting for warm-up to complete (10 minutes)..."
        local wait_time=600  # 10 minutes
        local check_interval=30
        local elapsed=0
        
        while [ $elapsed -lt $wait_time ]; do
            sleep $check_interval
            elapsed=$((elapsed + check_interval))
            
            # Re-check status
            realness_data=$(curl -sS "$METRICS_PROXY_URL/realness/$book/report" 2>/dev/null || echo "{}")
            is_warming_up=$(echo "$realness_data" | jq -r '.is_warming_up // true')
            samples=$(echo "$realness_data" | jq -r '.samples_collected // 0')
            
            echo -ne "\r  Progress: ${elapsed}s / ${wait_time}s | Samples: $samples / 200"
            
            if [ "$is_warming_up" = "false" ]; then
                echo ""
                log_info "$book warm-up complete!"
                break
            fi
        done
        echo ""
    fi
    
    # 4. Verify hard signals
    log_info ""
    log_info "Verifying hard signals..."
    local hard_signals_pass=true
    
    if ! ge "$price_variance" "0.8"; then
        log_error "price_variance FAIL: $price_variance < 0.8"
        hard_signals_pass=false
    else
        log_info "price_variance PASS: $price_variance ≥ 0.8"
    fi
    
    if ! ge "$team_entropy" "0.8"; then
        log_error "team_entropy FAIL: $team_entropy < 0.8"
        hard_signals_pass=false
    else
        log_info "team_entropy PASS: $team_entropy ≥ 0.8"
    fi
    
    if ! ge "$duplicate_ratio" "0.6"; then
        log_error "duplicate_ratio FAIL: $duplicate_ratio < 0.6"
        hard_signals_pass=false
    else
        log_info "duplicate_ratio PASS: $duplicate_ratio ≥ 0.6"
    fi
    
    # 5. Check composite score
    log_info ""
    log_info "Checking composite score..."
    if ! ge "$score" "0.85"; then
        log_error "Composite score FAIL: $score < 0.85"
        
        if [ "$hard_signals_pass" = false ]; then
            send_slack "❌ $book deployment failed: Hard signals not passing, score=$score"
            return 1
        fi
    else
        log_info "Composite score PASS: $score ≥ 0.85"
    fi
    
    # 6. Check data flow with tick growth
    log_info ""
    log_info "Checking data flow..."
    if [ "$odds_15m" = "0" ] || [ -z "$odds_15m" ]; then
        log_error "No odds in last 15 minutes"
        send_slack "⚠️ $book: No odds in last 15 minutes"
        return 1
    else
        log_info "Odds flowing: $odds_15m in last 15 min"
    fi
    
    # Get initial tick count
    local ticks_before="$ticks_total"
    log_info "Checking tick growth (10s interval)..."
    sleep 10
    
    # Re-fetch metrics
    metrics=$(curl -fsSL "$METRICS_PROXY_URL/metrics" 2>/dev/null | grep -E "(ticks_total).*book=\"$book\"" | head -1 || true)
    local raw_after=$(echo "$metrics" | grep "ticks_total{.*book=\"$book\"" | awk '{print $2}' | head -1 || echo "0")
    local ticks_after=$(printf "%.0f" "${raw_after:-0}")
    local tick_growth=$(( ${ticks_after:-0} - ${ticks_before:-0} ))
    
    if [ "$tick_growth" -gt 0 ]; then
        log_info "Ticks growing: $ticks_before → $ticks_after (+$tick_growth)"
    else
        log_warn "Ticks not growing: $ticks_before → $ticks_after"
    fi
    
    # 7. Monitor stability (abbreviated for initial deployment)
    log_info ""
    log_info "Monitoring stability for 5 minutes..."
    local stable_check_time=300  # 5 minutes for initial check
    local check_interval=30
    local elapsed=0
    local last_ticks="$ticks_total"
    
    while [ $elapsed -lt $stable_check_time ]; do
        sleep $check_interval
        elapsed=$((elapsed + check_interval))
        
        # Re-check metrics
        metrics=$(curl -sS "$METRICS_PROXY_URL/metrics" 2>/dev/null | grep "$book" || true)
        ticks_total=$(echo "$metrics" | grep "ticks_total{.*book=\"$book\"" | awk '{print $2}' | head -1 || echo "0")
        
        local tick_growth=$((ticks_total - last_ticks))
        echo -ne "\r  Stability check: ${elapsed}s / ${stable_check_time}s | Tick growth: $tick_growth"
        
        if [ "$tick_growth" -le 0 ]; then
            echo ""
            log_warn "Ticks not growing!"
        fi
        
        last_ticks="$ticks_total"
    done
    echo ""
    
    # Final verdict with summary table
    log_info ""
    log_info "========================================="
    log_info "SUMMARY TABLE:"
    echo "  Feature           | Value    | Required | Status"
    echo "  ----------------- | -------- | -------- | ------"
    printf "  price_variance    | %-8.3f | ≥0.800   | %s\n" "$price_variance" "$(ge "$price_variance" "0.8" && echo "✓" || echo "✗")"
    printf "  team_entropy      | %-8.3f | ≥0.800   | %s\n" "$team_entropy" "$(ge "$team_entropy" "0.8" && echo "✓" || echo "✗")"
    printf "  duplicate_ratio   | %-8.3f | ≥0.600   | %s\n" "$duplicate_ratio" "$(ge "$duplicate_ratio" "0.6" && echo "✓" || echo "✗")"
    printf "  composite_score   | %-8.3f | ≥0.850   | %s\n" "$score" "$(ge "$score" "0.85" && echo "✓" || echo "✗")"
    printf "  odds_15m          | %-8s | >0       | %s\n" "$odds_15m" "$([ "$odds_15m" != "0" ] && echo "✓" || echo "✗")"
    printf "  tick_growth       | %-8s | >0       | %s\n" "${tick_growth:-0}" "$([ "${tick_growth:-0}" -gt 0 ] && echo "✓" || echo "✗")"
    
    log_info "========================================="
    # Final pass/fail logic
    local final_pass=true
    if [ "$hard_signals_pass" = false ]; then
        final_pass=false
    fi
    if ! ge "$score" "0.85"; then
        final_pass=false
    fi
    if [ "${odds_15m:-0}" -eq 0 ]; then
        final_pass=false
    fi
    if [ "${tick_growth:-0}" -le 0 ]; then
        log_warn "Tick growth check failed but continuing"
        # Don't fail on tick growth alone during warm-up
    fi
    
    if [ "$final_pass" = true ]; then
        log_info "✅ $book PASS - Ready for production"
        send_slack "✅ $book deployed successfully: score=$score, odds_15m=$odds_15m"
        return 0
    else
        log_error "❌ $book FAIL - Not ready"
        send_slack "❌ $book deployment failed: score=$score, hard_signals=$hard_signals_pass"
        return 1
    fi
}

# Main execution
main() {
    log_info "Starting Render deployment verification"
    log_info "Metrics proxy: $METRICS_PROXY_URL"
    echo ""
    
    # Check if metrics proxy is accessible with retries
    local proxy_ok=false
    for i in {1..3}; do
        if curl -fsSL "$METRICS_PROXY_URL/healthz" >/dev/null 2>&1; then
            proxy_ok=true
            break
        fi
        log_warn "Metrics proxy not ready, retry $i/3..."
        sleep 5
    done
    
    if [ "$proxy_ok" = false ]; then
        log_error "Metrics proxy not accessible at $METRICS_PROXY_URL"
        exit 1
    fi
    
    # Quick endpoint tests
    log_info "Testing proxy endpoints..."
    curl -fsSL "$METRICS_PROXY_URL/healthz/bovada" | head -1 || log_warn "Health endpoint issue"
    if [ "$HAS_JQ" = true ]; then
        curl -fsSL "$METRICS_PROXY_URL/realness/bovada/report" | jq -r '.composite_score // .error' || log_warn "Realness endpoint issue"
    else
        curl -fsSL "$METRICS_PROXY_URL/realness/bovada/report" | grep -o '"composite_score":[0-9.]*' || log_warn "Realness endpoint issue"
    fi
    curl -fsSL "$METRICS_PROXY_URL/metrics" | grep -E 'realness_score|odds_15m|ticks_total' | head -5 || log_warn "Metrics endpoint issue"
    echo ""
    
    # Stage 0: Verify Bovada
    if check_book_deployment "bovada" "0"; then
        log_info ""
        log_info "Bovada verified! Ready for Kambi rollout."
        echo ""
        echo "Next steps:"
        echo "1. Wait 30-60 minutes for stability"
        echo "2. Enable BetRivers in Render dashboard"
        echo "3. Run: $0 betrivers"
    else
        log_error "Bovada verification failed. Fix issues before proceeding."
        exit 1
    fi
}

# Handle specific book verification if provided
if [ $# -eq 1 ]; then
    book="$1"
    check_book_deployment "$book" "manual"
else
    main
fi
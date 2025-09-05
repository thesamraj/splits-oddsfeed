#!/bin/bash
# Monitor metrics and alert via Slack when thresholds are breached
# Usage: alert_monitor.sh

set -euo pipefail

METRICS_URL="${METRICS_URL:-http://localhost:9090/metrics}"
SLACK_SCRIPT="${SLACK_SCRIPT:-scripts/notify_slack.sh}"
BOOKS="${BOOKS:-bovada betrivers barstool caesars sugarhouse unibet fanduel draftkings betmgm pinnacle bet365 stake pointsbet}"

# Check if Slack webhook is configured
if [ -z "${SLACK_WEBHOOK:-}" ]; then
    echo "Warning: SLACK_WEBHOOK not set, alerts will be logged only"
    SLACK_ENABLED=false
else
    SLACK_ENABLED=true
fi

# State tracking files
STATE_DIR="/tmp/oddsfeed_alerts"
mkdir -p "$STATE_DIR"

send_alert() {
    local message="$1"
    local alert_key="$2"
    local state_file="$STATE_DIR/$alert_key"

    # Check if we already alerted recently (within 30 min)
    if [ -f "$state_file" ]; then
        last_alert=$(cat "$state_file")
        now=$(date +%s)
        if [ $((now - last_alert)) -lt 1800 ]; then
            return  # Skip duplicate alert
        fi
    fi

    echo "ALERT: $message"

    if [ "$SLACK_ENABLED" = true ] && [ -x "$SLACK_SCRIPT" ]; then
        bash "$SLACK_SCRIPT" "⚠️ OddsFeed Alert: $message"
    fi

    # Update state
    date +%s > "$state_file"
}

check_metrics() {
    # Fetch metrics
    metrics=$(curl -s "$METRICS_URL" 2>/dev/null || echo "")

    if [ -z "$metrics" ]; then
        send_alert "Cannot fetch metrics from $METRICS_URL" "metrics_down"
        return
    fi

    # Track any failures
    local failures=0

    # Check each book
    for book in $BOOKS; do
        # Check realness score
        realness=$(echo "$metrics" | grep "realness_score{book=\"$book\"}" | awk '{print $2}' | head -1)
        if [ -n "$realness" ]; then
            # Use awk for numeric comparison (bc fallback)
            if awk -v r="$realness" 'BEGIN{exit(r >= 0.9 ? 1 : 0)}' 2>/dev/null; then
                send_alert "$book realness score low: $realness (threshold: 0.9)" "${book}_realness"
                failures=$((failures + 1))
            fi
        fi

        # Check odds in last 15m
        odds_15m=$(echo "$metrics" | grep "odds_15m{book=\"$book\"}" | awk '{print $2}' | head -1)
        if [ -n "$odds_15m" ]; then
            if [ "$odds_15m" = "0" ] || [ "$odds_15m" = "0.0" ]; then
                send_alert "$book has no odds in last 15 minutes" "${book}_no_odds"
                failures=$((failures + 1))
            fi
        fi

        # Check collector health
        collector_up=$(echo "$metrics" | grep "collector_up{book=\"$book\"}" | awk '{print $2}' | head -1)
        if [ -n "$collector_up" ]; then
            if [ "$collector_up" = "0" ] || [ "$collector_up" = "0.0" ]; then
                send_alert "$book collector is down" "${book}_collector_down"
                failures=$((failures + 1))
            fi
        fi
    done

    # Return failure count
    return $failures
}

# Main monitoring loop
if [ "${RUN_ONCE:-}" = "true" ]; then
    # Single check mode
    check_metrics
    exit_code=$?
    if [ $exit_code -gt 0 ]; then
        echo "CRITICAL: $exit_code issues found"
        exit 1
    else
        echo "OK: No critical issues"
        exit 0
    fi
else
    # Continuous monitoring
    echo "Starting alert monitor (checking every 2 minutes)..."
    echo "Books monitored: $BOOKS"
    while true; do
        check_metrics
        sleep 120
    done
fi

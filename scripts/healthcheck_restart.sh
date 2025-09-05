#!/bin/bash

# Auto-restart script for failed odds collectors
# Run this via cron every 5 minutes: */5 * * * * /path/to/healthcheck_restart.sh

LOG_FILE="/tmp/odds_healthcheck.log"
POSTGRES_USER="odds"
POSTGRES_DB="oddsfeed"

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> $LOG_FILE
}

check_and_restart() {
    local book=$1
    local container_pattern=$2
    local min_odds_threshold=$3

    # Check odds in last 5 minutes
    odds_count=$(docker exec splits-oddsfeed-store-1 psql -U $POSTGRES_USER -d $POSTGRES_DB -t -c "
        SELECT COUNT(*) FROM odds
        WHERE book = '$book'
        AND ts > NOW() - INTERVAL '5 minutes';
    " 2>/dev/null | xargs)

    if [ -z "$odds_count" ] || [ "$odds_count" -eq "0" ]; then
        log_message "WARNING: $book has 0 odds in last 5 minutes"

        # Find and restart relevant containers
        containers=$(docker ps -a --filter "name=$container_pattern" --format "{{.Names}}")

        for container in $containers; do
            status=$(docker inspect -f '{{.State.Status}}' $container 2>/dev/null)

            if [ "$status" != "running" ]; then
                log_message "RESTARTING: $container (was $status)"
                docker start $container
                sleep 2
            else
                log_message "RESTARTING: $container (was running but not producing data)"
                docker restart $container
                sleep 2
            fi
        done
    elif [ "$odds_count" -lt "$min_odds_threshold" ]; then
        log_message "WARNING: $book has low odds count: $odds_count (threshold: $min_odds_threshold)"
    fi
}

# Main execution
log_message "Starting health check"

# Check each sportsbook
check_and_restart "draftkings" "draftkings" 1000
check_and_restart "betrivers" "betrivers\|kambi" 100
check_and_restart "barstool" "barstool" 50
check_and_restart "pointsbet" "pointsbet" 20
check_and_restart "fanduel" "fanduel" 10

# Check main normalizer
normalizer_status=$(docker inspect -f '{{.State.Status}}' splits-oddsfeed-normalizer-1 2>/dev/null)
if [ "$normalizer_status" != "running" ]; then
    log_message "CRITICAL: Main normalizer not running, restarting"
    docker start splits-oddsfeed-normalizer-1
fi

log_message "Health check completed"

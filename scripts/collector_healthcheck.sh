#!/bin/bash
# Collector Health Check and Auto-Recovery Script
# Monitors collectors and automatically restarts failed ones

# Configuration
CHECK_INTERVAL=60  # Check every 60 seconds
REDIS_HOST="localhost"
REDIS_PORT="6379"
LOG_FILE="/tmp/collector_healthcheck.log"

# Collectors to monitor (book name : expected channel)
declare -A COLLECTORS=(
    ["bovada"]="odds.raw.bovada"
    ["barstool"]="odds.raw.barstool"
    ["betrivers"]="odds.raw.betrivers"
    ["sugarhouse"]="odds.raw.sugarhouse"
    ["unibet"]="odds.raw.unibet"
    ["caesars"]="odds.raw.caesars"
    ["betmgm"]="odds.raw.betmgm"
    ["pointsbet"]="odds.raw.pointsbet"
    ["pinnacle"]="odds.raw.pinnacle"
)

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

check_container_health() {
    local collector=$1
    local container_name="splits-oddsfeed-collector-${collector}-1"

    # Check if container exists and is running
    if docker ps --format "{{.Names}}" | grep -q "^${container_name}$"; then
        # Container is running, check if it's actually publishing
        local channel="${COLLECTORS[$collector]}"

        # Check Redis for recent activity (messages in last 2 minutes)
        local last_message=$(docker exec splits-oddsfeed-broker-1 redis-cli --raw \
            EVAL "local msgs = redis.call('LRANGE', 'channel_history:${channel}', -1, -1); \
                  if #msgs > 0 then return msgs[1] else return '' end" 0 2>/dev/null)

        if [ -z "$last_message" ]; then
            log "WARNING: ${collector} container running but no recent messages"
            return 1
        else
            log "OK: ${collector} is healthy"
            return 0
        fi
    else
        log "ERROR: ${collector} container not running"
        return 2
    fi
}

restart_collector() {
    local collector=$1
    log "RESTARTING: ${collector} collector"

    # Use docker-compose to restart the specific service
    docker-compose -f docker-compose.unified.yml restart "collector-${collector}" 2>&1 | tee -a "$LOG_FILE"

    if [ $? -eq 0 ]; then
        log "SUCCESS: ${collector} restarted"
    else
        log "FAILED: Could not restart ${collector}"
    fi
}

check_database_connection() {
    # Check if normalizer can connect to database
    local db_status=$(docker exec splits-oddsfeed-store-1 pg_isready -U odds 2>&1)

    if [[ "$db_status" == *"accepting connections"* ]]; then
        log "OK: Database is accepting connections"
        return 0
    else
        log "ERROR: Database connection issue: $db_status"
        return 1
    fi
}

check_redis_connection() {
    # Check if Redis is responsive
    local redis_ping=$(docker exec splits-oddsfeed-broker-1 redis-cli ping 2>&1)

    if [ "$redis_ping" == "PONG" ]; then
        log "OK: Redis is responsive"
        return 0
    else
        log "ERROR: Redis not responding: $redis_ping"
        return 1
    fi
}

main_loop() {
    log "Starting collector health check monitor"

    while true; do
        log "=== Starting health check cycle ==="

        # Check infrastructure first
        if ! check_redis_connection; then
            log "CRITICAL: Redis down, skipping collector checks"
            sleep $CHECK_INTERVAL
            continue
        fi

        if ! check_database_connection; then
            log "WARNING: Database issues detected"
        fi

        # Check each collector
        for collector in "${!COLLECTORS[@]}"; do
            if ! check_container_health "$collector"; then
                restart_collector "$collector"
                sleep 5  # Give it time to start
            fi
        done

        # Check normalizer
        if ! docker ps --format "{{.Names}}" | grep -q "splits-oddsfeed-normalizer-1"; then
            log "CRITICAL: Normalizer not running, restarting..."
            docker-compose -f docker-compose.unified.yml restart normalizer
        fi

        log "=== Health check cycle complete ==="
        sleep $CHECK_INTERVAL
    done
}

# Run the main loop
main_loop

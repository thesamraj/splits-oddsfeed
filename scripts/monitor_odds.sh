#!/bin/bash

# Odds Collection Monitoring Script
# Monitors all sportsbook collectors and alerts on issues

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
POSTGRES_USER="odds"
POSTGRES_DB="oddsfeed"
WARNING_THRESHOLD=50  # Warn if odds/min drops below this
CRITICAL_THRESHOLD=10 # Critical if odds/min drops below this

echo "=== Odds Collection Monitor ==="
echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "================================"

# Function to check book status
check_book() {
    local book=$1
    local expected_min=$2

    # Get stats from database
    stats=$(docker exec splits-oddsfeed-store-1 psql -U $POSTGRES_USER -d $POSTGRES_DB -t -c "
        SELECT
            COUNT(*) as odds_count,
            COUNT(DISTINCT event_id) as events,
            ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(ts))), 0) as last_update_sec
        FROM odds
        WHERE book = '$book'
        AND ts > NOW() - INTERVAL '2 minutes';
    " 2>/dev/null)

    if [ -z "$stats" ]; then
        echo -e "${RED}✗ $book: DATABASE ERROR${NC}"
        return 1
    fi

    odds_count=$(echo $stats | awk '{print $1}')
    events=$(echo $stats | awk '{print $3}')
    last_update=$(echo $stats | awk '{print $5}')
    odds_per_min=$((odds_count / 2))

    # Check container status
    container_status=$(docker ps --filter "name=$book" --format "table {{.Status}}" | tail -n +2 | head -1)

    # Determine status
    if [ "$odds_per_min" -eq 0 ]; then
        echo -e "${RED}✗ $book: DOWN (0 odds/min, last update: ${last_update}s ago)${NC}"
        echo "  Container: $container_status"
        return 1
    elif [ "$odds_per_min" -lt "$CRITICAL_THRESHOLD" ]; then
        echo -e "${RED}✗ $book: CRITICAL (${odds_per_min} odds/min, ${events} events)${NC}"
        return 1
    elif [ "$odds_per_min" -lt "$WARNING_THRESHOLD" ]; then
        echo -e "${YELLOW}⚠ $book: WARNING (${odds_per_min} odds/min, ${events} events)${NC}"
        return 0
    elif [ "$odds_per_min" -lt "$expected_min" ]; then
        echo -e "${YELLOW}⚠ $book: BELOW EXPECTED (${odds_per_min}/${expected_min} odds/min, ${events} events)${NC}"
        return 0
    else
        echo -e "${GREEN}✓ $book: OK (${odds_per_min} odds/min, ${events} events, ${last_update}s ago)${NC}"
        return 0
    fi
}

# Check normalizer containers
echo ""
echo "Checking Normalizers:"
echo "---------------------"
for normalizer in "normalizer" "barstool-normalizer" "fanduel-normalizer"; do
    status=$(docker ps --filter "name=$normalizer" --format "table {{.Names}} {{.Status}}" | grep $normalizer | head -1)
    if [ -n "$status" ]; then
        echo -e "${GREEN}✓ $status${NC}"
    else
        echo -e "${RED}✗ $normalizer: NOT RUNNING${NC}"
    fi
done

# Check each sportsbook
echo ""
echo "Checking Sportsbooks:"
echo "--------------------"
check_book "draftkings" 3000
check_book "betrivers" 200
check_book "barstool" 100
check_book "pointsbet" 50
check_book "fanduel" 10

# Check Redis broker
echo ""
echo "Checking Redis Broker:"
echo "---------------------"
redis_ping=$(docker exec splits-oddsfeed-broker-1 redis-cli ping 2>/dev/null)
if [ "$redis_ping" = "PONG" ]; then
    echo -e "${GREEN}✓ Redis: OK${NC}"
else
    echo -e "${RED}✗ Redis: NOT RESPONDING${NC}"
fi

# Check PostgreSQL
echo ""
echo "Checking PostgreSQL:"
echo "-------------------"
pg_status=$(docker exec splits-oddsfeed-store-1 pg_isready -U $POSTGRES_USER 2>/dev/null)
if [[ $pg_status == *"accepting connections"* ]]; then
    echo -e "${GREEN}✓ PostgreSQL: OK${NC}"

    # Get total stats
    total_stats=$(docker exec splits-oddsfeed-store-1 psql -U $POSTGRES_USER -d $POSTGRES_DB -t -c "
        SELECT
            COUNT(*) as total_odds,
            COUNT(DISTINCT book) as active_books,
            COUNT(DISTINCT event_id) as total_events
        FROM odds
        WHERE ts > NOW() - INTERVAL '5 minutes';
    " 2>/dev/null)

    echo "  Total odds (5 min): $(echo $total_stats | awk '{print $1}')"
    echo "  Active books: $(echo $total_stats | awk '{print $3}')"
    echo "  Total events: $(echo $total_stats | awk '{print $5}')"
else
    echo -e "${RED}✗ PostgreSQL: NOT READY${NC}"
fi

# Summary
echo ""
echo "================================"
echo "Press Ctrl+C to exit"
echo "Refreshing in 30 seconds..."

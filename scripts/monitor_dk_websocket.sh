#!/bin/bash

# Monitor DraftKings WebSocket stability
# Updates every 30 seconds for 1 hour

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color
BLUE='\033[0;34m'

START_TIME=$(date +%s)
END_TIME=$((START_TIME + 3600))  # 1 hour from now

echo "====================================="
echo "DraftKings WebSocket Monitor"
echo "Started: $(date)"
echo "Will run until: $(date -d @$END_TIME 2>/dev/null || date -r $END_TIME)"
echo "====================================="

# Track statistics
TOTAL_CHECKS=0
SUCCESSFUL_CHECKS=0
FAILED_CHECKS=0
LAST_EVENT_COUNT=0

while [ $(date +%s) -lt $END_TIME ]; do
    clear
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    REMAINING=$((END_TIME - CURRENT_TIME))

    echo "========================================="
    echo "  DraftKings WebSocket Monitor"
    echo "========================================="
    echo "Elapsed: $(($ELAPSED / 60))m $(($ELAPSED % 60))s | Remaining: $(($REMAINING / 60))m $(($REMAINING % 60))s"
    echo ""

    # Check WebSocket container status
    WS_STATUS=$(docker ps --filter "name=dk-websocket" --format "table {{.Status}}" | tail -1)
    if [[ $WS_STATUS == *"Up"* ]]; then
        echo -e "${GREEN}✓ WebSocket Container: RUNNING${NC}"
        echo "  Status: $WS_STATUS"
        SUCCESSFUL_CHECKS=$((SUCCESSFUL_CHECKS + 1))
    else
        echo -e "${RED}✗ WebSocket Container: DOWN${NC}"
        FAILED_CHECKS=$((FAILED_CHECKS + 1))
    fi

    # Check recent logs for events
    RECENT_EVENTS=$(docker logs dk-websocket --tail 100 2>&1 | grep "Published" | tail -5)
    EVENT_COUNT=$(echo "$RECENT_EVENTS" | grep -c "Published")

    echo ""
    echo "Recent Activity:"
    if [ $EVENT_COUNT -gt 0 ]; then
        echo -e "${GREEN}✓ Publishing events (last 5):${NC}"
        echo "$RECENT_EVENTS" | sed 's/^/  /'
    else
        echo -e "${YELLOW}⚠ No recent events published${NC}"
    fi

    # Check for errors
    RECENT_ERRORS=$(docker logs dk-websocket --tail 100 2>&1 | grep -i "error\|failed\|exception" | tail -3)
    if [ -n "$RECENT_ERRORS" ]; then
        echo ""
        echo -e "${RED}Recent Errors:${NC}"
        echo "$RECENT_ERRORS" | sed 's/^/  /'
    fi

    # Check Redis channel activity
    echo ""
    echo "Redis Channel Status:"
    REDIS_SUBS=$(docker exec splits-oddsfeed-broker-1 redis-cli PUBSUB NUMSUB odds.raw.draftkings 2>/dev/null | tail -1)
    if [ -n "$REDIS_SUBS" ] && [ "$REDIS_SUBS" != "0" ]; then
        echo -e "${GREEN}✓ Subscribers: $REDIS_SUBS${NC}"
    else
        echo -e "${YELLOW}⚠ No subscribers on channel${NC}"
    fi

    # Database statistics (if available)
    echo ""
    echo "Database Activity:"
    DB_CHECK=$(docker exec splits-oddsfeed-store-1 psql -U odds -d oddsdb -t -c "
        SELECT COUNT(*) as count,
               MAX(ts) as latest
        FROM odds
        WHERE book = 'draftkings'
        AND ts > NOW() - INTERVAL '2 minutes'
    " 2>/dev/null || echo "Database unavailable")

    if [[ $DB_CHECK != *"unavailable"* ]]; then
        echo "  Recent records: $DB_CHECK"
    else
        echo "  $DB_CHECK"
    fi

    # Summary statistics
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    SUCCESS_RATE=$(( (SUCCESSFUL_CHECKS * 100) / TOTAL_CHECKS ))

    echo ""
    echo "========================================="
    echo "Statistics:"
    echo "  Total Checks: $TOTAL_CHECKS"
    echo "  Uptime: ${SUCCESS_RATE}%"
    echo "  Successful: $SUCCESSFUL_CHECKS | Failed: $FAILED_CHECKS"
    echo "========================================="

    # Alert if down
    if [[ $WS_STATUS != *"Up"* ]]; then
        echo -e "${RED}⚠⚠⚠ ALERT: WebSocket is DOWN! ⚠⚠⚠${NC}"
        echo "Attempting restart..."
        docker restart dk-websocket
    fi

    sleep 30
done

echo ""
echo "====================================="
echo "Monitoring Complete"
echo "Final Statistics:"
echo "  Total Runtime: 1 hour"
echo "  Total Checks: $TOTAL_CHECKS"
echo "  Uptime: ${SUCCESS_RATE}%"
echo "  Successful: $SUCCESSFUL_CHECKS"
echo "  Failed: $FAILED_CHECKS"
echo "====================================="

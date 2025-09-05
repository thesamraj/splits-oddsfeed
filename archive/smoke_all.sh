#!/bin/bash

# Smoke test for all production books
# Excludes disabled test publishers

echo "=== SMOKE TEST - $(date) ==="
echo ""

# Check for test data (must be 0)
echo "1. Checking for test data in last 60 minutes..."
TEST_ROWS=$(docker exec splits-oddsfeed-store-1 sh -c "PGPASSWORD=odds psql -h localhost -U odds oddsfeed" -t -c "
    SELECT COUNT(*)
    FROM odds
    WHERE ts > NOW() - INTERVAL '60 minutes'
    AND (
        price_home IS NULL
        OR price_away IS NULL
        OR event_id LIKE '%test%'
        OR event_id LIKE '%dummy%'
        OR home_team LIKE '%Test%'
        OR away_team LIKE '%Test%'
    )
")

if [ "$TEST_ROWS" = "0" ] || [ -z "$TEST_ROWS" ]; then
    echo "  ✅ PASS: No test data found"
else
    echo "  ❌ FAIL: Found $TEST_ROWS test rows"
fi

echo ""
echo "2. Production books status (15-min window):"

# Books to check
BOOKS=("betrivers" "draftkings" "pointsbet" "barstool" "bovada")

for book in "${BOOKS[@]}"; do
    printf "  %-12s: " "$book"

    ODDS=$(docker exec splits-oddsfeed-store-1 sh -c "PGPASSWORD=odds psql -h localhost -U odds oddsfeed -t -c \"
        SELECT COUNT(*) FROM odds
        WHERE book = '$book' AND ts > NOW() - INTERVAL '15 minutes'
    \"" | xargs)

    if [ -n "$ODDS" ] && [ "$ODDS" -gt 0 ]; then
        echo "✅ $ODDS odds"
    else
        echo "⚠️  No recent data"
    fi
done

echo ""
echo "3. Bovada Acceptance Criteria Check:"

# Check Bovada specifically
BOVADA_ODDS=$(docker exec splits-oddsfeed-store-1 sh -c "PGPASSWORD=odds psql -h localhost -U odds oddsfeed -t -c \"
    SELECT COUNT(*) FROM odds
    WHERE book = 'bovada' AND ts > NOW() - INTERVAL '15 minutes'
\"" | xargs)

BOVADA_EVENTS=$(docker exec splits-oddsfeed-store-1 sh -c "PGPASSWORD=odds psql -h localhost -U odds oddsfeed -t -c \"
    SELECT COUNT(DISTINCT event_id) FROM odds
    WHERE book = 'bovada' AND ts > NOW() - INTERVAL '15 minutes'
\"" | xargs)

echo "  Bovada 15-min window:"
echo "    Odds: $BOVADA_ODDS (requirement: ≥200)"
echo "    Events: $BOVADA_EVENTS (requirement: ≥10)"

if [ "$BOVADA_ODDS" -ge 200 ] && [ "$BOVADA_EVENTS" -ge 10 ]; then
    echo "  ✅ PASS: Bovada meets acceptance criteria"
else
    echo "  ❌ FAIL: Bovada does not meet acceptance criteria"
fi

echo ""
echo "=== SMOKE TEST COMPLETE ==="

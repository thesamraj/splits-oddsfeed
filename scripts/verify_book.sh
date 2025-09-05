#!/bin/bash
# Verify a sportsbook is running correctly
# Usage: verify_book.sh <book>

set -euo pipefail

BOOK="${1:-}"
if [ -z "$BOOK" ]; then
    echo "Usage: $0 <book>"
    echo "Books: bovada, fanduel, draftkings, betmgm, pinnacle, bet365, stake, pointsbet, betrivers, barstool, caesars, sugarhouse, unibet"
    exit 1
fi

echo "🔍 Verifying $BOOK..."
FAILURES=0
START_TIME=$(date +%s)

# Check database availability upfront
DB_AVAILABLE=false
if [ -n "${DATABASE_URL:-}" ] && command -v psql >/dev/null 2>&1; then
    # Test connection
    if PGCONNECT_TIMEOUT=2 psql "$DATABASE_URL" -c "SELECT 1" >/dev/null 2>&1; then
        DB_AVAILABLE=true
        echo "ℹ️  Database connection available"
    else
        echo "⚠️  Database configured but unreachable"
    fi
else
    echo "ℹ️  Using metrics only (no database)"
fi

# Wait for metrics to appear (bounded 90s)
echo "Waiting for metrics (max 90s)..."
METRICS_FOUND=false
for i in {1..18}; do
    # Try standard port 9090 first, alternate 19090, then proxy on 8000
    if curl -sS --max-time 2 localhost:9090/metrics 2>/dev/null | grep -q "realness_score{book=\"$BOOK\"}"; then
        METRICS_FOUND=true
        echo "  Metrics available after $((i*5))s (port 9090)"
        break
    elif curl -sS --max-time 2 localhost:19090/metrics 2>/dev/null | grep -q "realness_score{book=\"$BOOK\"}"; then
        METRICS_FOUND=true
        echo "  Metrics available after $((i*5))s (port 19090)"
        break
    elif curl -sS --max-time 2 localhost:8000/metrics 2>/dev/null | grep -q "realness_score{book=\"$BOOK\"}"; then
        METRICS_FOUND=true
        echo "  Metrics available after $((i*5))s (proxy port 8000)"
        break
    fi
    echo -n "."
    sleep 5
done

if [ "$METRICS_FOUND" = false ]; then
    echo " timeout!"
    echo "❌ No metrics found after 90s"
    FAILURES=$((FAILURES + 1))
fi

# 1. Check realness score
echo -n "Checking realness score... "
# Try standard port first (9090), alternate (19090), then proxy (8000), then container
REALNESS=$(curl --fail -sS --max-time 5 --connect-timeout 3 localhost:9090/metrics 2>/dev/null | grep "realness_score{book=\"$BOOK\"}" | awk '{print $2}' | head -1)

if [ -z "$REALNESS" ]; then
    REALNESS=$(curl --fail -sS --max-time 5 --connect-timeout 3 localhost:19090/metrics 2>/dev/null | grep "realness_score{book=\"$BOOK\"}" | awk '{print $2}' | head -1)
fi

if [ -z "$REALNESS" ]; then
    REALNESS=$(curl --fail -sS --max-time 5 --connect-timeout 3 localhost:8000/metrics 2>/dev/null | grep "realness_score{book=\"$BOOK\"}" | awk '{print $2}' | head -1)
fi

if [ -z "$REALNESS" ]; then
    # Fallback to container
    CONTAINER=$(docker compose ps -q ${BOOK}-collector 2>/dev/null || echo "splits-oddsfeed-${BOOK}-collector-1")
    if [ -n "$CONTAINER" ]; then
        REALNESS=$(docker exec "$CONTAINER" curl -sS --max-time 5 localhost:9090/metrics 2>/dev/null | grep "realness_score{book=\"$BOOK\"}" | awk '{print $2}' | head -1 || true)
        [ -n "$REALNESS" ] && echo -n "(from container) "
    fi
fi

if [ -z "$REALNESS" ]; then
    # Fallback to proxy
    METRICS_URL=${METRICS_URL:-http://localhost:8000/metrics}
    REALNESS=$(curl --fail -sS --max-time 5 --connect-timeout 3 "$METRICS_URL" 2>/dev/null | grep "realness_score{book=\"$BOOK\"}" | awk '{print $2}' | head -1)
    [ -n "$REALNESS" ] && echo -n "(from proxy) "
fi

if [ -z "$REALNESS" ]; then
    echo "❌ No realness score found"
    FAILURES=$((FAILURES + 1))
elif (( $(echo "$REALNESS < 0.9" | bc -l) )); then
    echo "❌ Realness score too low: $REALNESS (need >= 0.9)"
    FAILURES=$((FAILURES + 1))

    # Fetch realness report to show why score is low
    echo "  Fetching realness report..."
    # Try standard health port first (9091), alternate (19091), then container
    REPORT=$(curl -sS --max-time 3 localhost:9091/realness/report 2>/dev/null || echo "{}")
    if [ "$REPORT" = "{}" ] || [ -z "$REPORT" ]; then
        REPORT=$(curl -sS --max-time 3 localhost:19091/realness/report 2>/dev/null || echo "{}")
    fi
    if [ "$REPORT" = "{}" ] || [ -z "$REPORT" ]; then
        REPORT=$(docker exec "splits-oddsfeed-${BOOK}-collector-1" wget -qO- localhost:9091/realness/report 2>/dev/null || echo "{}")
    fi
    if [ "$REPORT" != "{}" ] && [ -n "$REPORT" ]; then
        echo "  📊 Realness Report (top reasons):"
        echo "$REPORT" | python3 -m json.tool 2>/dev/null | head -200 || echo "$REPORT"
    fi
else
    echo "✅ Realness: $REALNESS"
fi

# 2. Check odds in last 15 minutes
echo -n "Checking recent odds... "
if [ "$DB_AVAILABLE" = true ]; then
    ODDS_15M=$(PGCONNECT_TIMEOUT=5 psql "$DATABASE_URL" -t -c "
        SELECT COUNT(*) FROM odds
        WHERE book = '$BOOK'
        AND ts > now() - interval '15 minutes';
    " 2>/dev/null | xargs || echo "0")

    if [ "$ODDS_15M" -eq 0 ]; then
        echo "❌ No odds in last 15 minutes (DB)"
        FAILURES=$((FAILURES + 1))
    else
        echo "✅ Odds from DB (15m): $ODDS_15M"
    fi
else
    # Fallback to metrics if no DB
    ODDS_METRIC=$(curl --fail -sS --max-time 5 --connect-timeout 3 localhost:8000/metrics 2>/dev/null | grep "odds_15m{book=\"$BOOK\"}" | awk '{print $2}' | head -1 || echo "")
    if [ -z "$ODDS_METRIC" ] || [ "$ODDS_METRIC" = "0" ]; then
        # Try odds_upserts_total as alternative
        ODDS_UPSERTS=$(curl --fail -sS --max-time 5 --connect-timeout 3 localhost:8000/metrics 2>/dev/null | grep "odds_upserts_total{book=\"$BOOK\"}" | awk '{print $2}' | head -1 || echo "0")
        if [ -n "$ODDS_UPSERTS" ] && [ "$ODDS_UPSERTS" != "0" ]; then
            echo "✅ Odds upserts: $ODDS_UPSERTS (15m metric not available)"
        else
            echo "⚠️  No recent odds metrics available"
        fi
    else
        echo "✅ Odds metric: $ODDS_METRIC"
    fi
fi

# 3. Check ticks are increasing
echo -n "Checking tick growth... "
TICKS_BEFORE=$(curl --fail -sS --max-time 5 --connect-timeout 3 localhost:9090/metrics 2>/dev/null | grep "ticks_total{book=\"$BOOK\"}" | awk '{print $2}' | head -1 || echo "0")

if [ "$TICKS_BEFORE" = "0" ] || [ -z "$TICKS_BEFORE" ]; then
    TICKS_BEFORE=$(curl --fail -sS --max-time 5 --connect-timeout 3 localhost:8000/metrics 2>/dev/null | grep "ticks_total{book=\"$BOOK\"}" | awk '{print $2}' | head -1 || echo "0")
fi

if [ "$TICKS_BEFORE" = "0" ] || [ -z "$TICKS_BEFORE" ]; then
    # Try container
    CONTAINER=$(docker compose ps -q ${BOOK}-collector 2>/dev/null || echo "splits-oddsfeed-${BOOK}-collector-1")
    if [ -n "$CONTAINER" ]; then
        TICKS_BEFORE=$(docker exec "$CONTAINER" curl -sS --max-time 5 localhost:9090/metrics 2>/dev/null | grep "ticks_total{book=\"$BOOK\"}" | awk '{print $2}' | head -1 || echo "0")
    fi
fi

sleep 5

TICKS_AFTER=$(curl --fail -sS --max-time 5 --connect-timeout 3 localhost:9090/metrics 2>/dev/null | grep "ticks_total{book=\"$BOOK\"}" | awk '{print $2}' | head -1 || echo "0")

if [ "$TICKS_AFTER" = "0" ] || [ -z "$TICKS_AFTER" ]; then
    TICKS_AFTER=$(curl --fail -sS --max-time 5 --connect-timeout 3 localhost:8000/metrics 2>/dev/null | grep "ticks_total{book=\"$BOOK\"}" | awk '{print $2}' | head -1 || echo "0")
fi

if [ "$TICKS_AFTER" = "0" ] || [ -z "$TICKS_AFTER" ]; then
    # Try container
    if [ -n "$CONTAINER" ]; then
        TICKS_AFTER=$(docker exec "$CONTAINER" curl -sS --max-time 5 localhost:9090/metrics 2>/dev/null | grep "ticks_total{book=\"$BOOK\"}" | awk '{print $2}' | head -1 || echo "0")
    fi
fi

if [ -z "$TICKS_BEFORE" ] || [ "$TICKS_BEFORE" = "0" ]; then
    echo "❌ No ticks found"
    FAILURES=$((FAILURES + 1))
elif (( $(echo "$TICKS_AFTER <= $TICKS_BEFORE" | bc -l) )); then
    echo "❌ Ticks not increasing (was: $TICKS_BEFORE, now: $TICKS_AFTER)"
    FAILURES=$((FAILURES + 1))
else
    echo "✅ Ticks growing: $TICKS_BEFORE → $TICKS_AFTER"
fi

# 4. Check for key markets (h2h, spread, total)
echo -n "Checking market coverage... "
if [ "$DB_AVAILABLE" = true ]; then
    MARKETS=$(PGCONNECT_TIMEOUT=5 psql "$DATABASE_URL" -t -c "
        SELECT DISTINCT market FROM odds
        WHERE book = '$BOOK'
        AND ts > now() - interval '15 minutes'
        ORDER BY market;
    " 2>/dev/null | xargs | tr ' ' ',' || echo "")

    if [ -z "$MARKETS" ]; then
        echo "⚠️  No markets found (may be normal for some books)"
    else
        echo "✅ Markets: $MARKETS"
    fi
else
    echo "⏭️  Skipped (no DB access)"
fi

# 5. Check collector health
echo -n "Checking collector health... "
COLLECTOR_UP=$(curl --fail -sS --max-time 5 --connect-timeout 3 localhost:9090/metrics 2>/dev/null | grep "collector_up{book=\"$BOOK\"}" | awk '{print $2}' | head -1)

if [ -z "$COLLECTOR_UP" ]; then
    COLLECTOR_UP=$(curl --fail -sS --max-time 5 --connect-timeout 3 localhost:8000/metrics 2>/dev/null | grep "collector_up{book=\"$BOOK\"}" | awk '{print $2}' | head -1)
fi

if [ -z "$COLLECTOR_UP" ]; then
    # Try proxy
    METRICS_URL=${METRICS_URL:-http://localhost:8000/metrics}
    COLLECTOR_UP=$(curl --fail -sS --max-time 5 --connect-timeout 3 "$METRICS_URL" 2>/dev/null | grep "collector_up{book=\"$BOOK\"}" | awk '{print $2}' | head -1)
fi

if [ -z "$COLLECTOR_UP" ]; then
    echo "❌ Collector metric not found"
    FAILURES=$((FAILURES + 1))
elif [ "$COLLECTOR_UP" != "1.0" ] && [ "$COLLECTOR_UP" != "1" ]; then
    echo "❌ Collector not healthy (status: $COLLECTOR_UP)"
    FAILURES=$((FAILURES + 1))
else
    echo "✅ Collector UP"
fi

# 6. Check quarantine table (if DB available)
if [ "$DB_AVAILABLE" = true ]; then
    echo -n "Checking quarantine status... "
    QUARANTINE_COUNT=$(PGCONNECT_TIMEOUT=5 psql "$DATABASE_URL" -t -c "
        SELECT COUNT(*) FROM quarantine_odds
        WHERE book = '$BOOK'
        AND created_at > now() - interval '15 minutes';
    " 2>/dev/null | xargs || echo "0")

    ODDS_COUNT=$(PGCONNECT_TIMEOUT=5 psql "$DATABASE_URL" -t -c "
        SELECT COUNT(*) FROM odds
        WHERE book = '$BOOK'
        AND ts > now() - interval '15 minutes';
    " 2>/dev/null | xargs || echo "0")

    if [ "$QUARANTINE_COUNT" -gt 0 ]; then
        echo "⚠️  $QUARANTINE_COUNT events quarantined, $ODDS_COUNT in main table (last 15m)"
        # Show sample reasons
        REASONS=$(PGCONNECT_TIMEOUT=5 psql "$DATABASE_URL" -t -c "
            SELECT DISTINCT failure_reasons FROM quarantine_odds
            WHERE book = '$BOOK'
            AND created_at > now() - interval '15 minutes'
            LIMIT 3;
        " 2>/dev/null || echo "")
        if [ -n "$REASONS" ]; then
            echo "  Failure reasons: $REASONS"
        fi
    else
        echo "✅ No quarantined events, $ODDS_COUNT in main table"
    fi
fi

# Final result
echo ""
if [ $FAILURES -eq 0 ]; then
    echo "✅ $BOOK verification PASSED"
    exit 0
else
    echo "❌ $BOOK verification FAILED ($FAILURES issues)"
    exit 1
fi

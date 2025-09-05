#!/bin/bash
# Sportsbook Monitor - Real-time status tracking
# Updates every 30 seconds with alerts for low-volume books

while true; do
  clear
  echo "==============================================="
  echo "    SPORTSBOOK MONITOR - $(date '+%Y-%m-%d %H:%M:%S')"
  echo "==============================================="
  echo ""

  # Get status for all books
  docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -c "
    SELECT
      book,
      COUNT(*) as odds_15m,
      COUNT(DISTINCT event_id) as events,
      COUNT(DISTINCT market) as markets,
      ROUND(EXTRACT(EPOCH FROM (now()-MAX(ts)))) as sec_ago,
      CASE
        WHEN COUNT(*) = 0 THEN '🔴 DEAD'
        WHEN COUNT(*) < 100 THEN '🟡 LOW'
        WHEN COUNT(*) < 1000 THEN '🟠 SLOW'
        ELSE '🟢 LIVE'
      END as status
    FROM odds
    WHERE ts > now() - interval '15 min'
    GROUP BY book
    ORDER BY COUNT(*) DESC;" 2>/dev/null || echo "Database connection error"

  echo ""
  echo "-----------------------------------------------"
  echo "ALERTS:"

  # Check for dead or low-volume books
  LOW_BOOKS=$(docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -Atc "
    SELECT book || ' (' || COUNT(*) || ')' FROM (
      SELECT book, COUNT(*) as cnt
      FROM odds WHERE ts > now() - interval '15 min'
      GROUP BY book
    ) t WHERE cnt < 100
    ORDER BY cnt;" 2>/dev/null)

  if [ ! -z "$LOW_BOOKS" ]; then
    echo "⚠️  LOW VOLUME BOOKS:"
    echo "$LOW_BOOKS" | while read line; do
      echo "   - $line odds/15min"
    done
  else
    echo "✅ All books above minimum threshold (100 odds/15min)"
  fi

  # Check for crashed containers
  echo ""
  CRASHED=$(docker ps -a --format "{{.Names}}" --filter "status=exited" --filter "status=restarting" | grep -E "collector-|normalizer-" | head -5)
  if [ ! -z "$CRASHED" ]; then
    echo "🔴 CRASHED CONTAINERS:"
    echo "$CRASHED" | while read container; do
      echo "   - $container"
    done
  fi

  # Show total system metrics
  echo ""
  echo "-----------------------------------------------"
  echo "SYSTEM METRICS:"
  docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -Atc "
    SELECT
      'Total Odds (15m): ' || COUNT(*) ||
      ' | Books: ' || COUNT(DISTINCT book) ||
      ' | Events: ' || COUNT(DISTINCT event_id)
    FROM odds
    WHERE ts > now() - interval '15 min';" 2>/dev/null

  # API health check
  echo -n "API Response Time: "
  START=$(date +%s%N)
  curl -s "http://127.0.0.1:8080/health" > /dev/null 2>&1 && {
    END=$(date +%s%N)
    ELAPSED=$((($END - $START) / 1000000))
    if [ $ELAPSED -lt 1000 ]; then
      echo "✅ ${ELAPSED}ms"
    elif [ $ELAPSED -lt 5000 ]; then
      echo "🟡 ${ELAPSED}ms (slow)"
    else
      echo "🔴 ${ELAPSED}ms (critical)"
    fi
  } || echo "🔴 Not responding"

  sleep 30
done

#!/usr/bin/env bash
set -euo pipefail
while true; do
  TS=$(date -u +%FT%TZ)
  ODDS=$(docker compose exec -T store psql -U postgres odds -t -c "SELECT count(*) FROM odds WHERE ts > now() - '15 minutes'::interval" 2>/dev/null | xargs || echo 0)
  TICKS=$(docker compose exec -T store psql -U postgres odds -t -c "SELECT count(*) FROM odds_ticks WHERE ts > now() - '15 minutes'::interval" 2>/dev/null | xargs || echo 0)
  EVENTS=$(docker compose exec -T store psql -U postgres odds -t -c "SELECT count(DISTINCT event_id) FROM odds WHERE ts > now() - '15 minutes'::interval" 2>/dev/null | xargs || echo 0)
  API_OK=false
  if curl -fsS 'http://127.0.0.1:8080/odds?brand=betrivers&minutes=15&limit=1' >/dev/null 2>&1; then
    API_OK=true
  fi

  JSON="{\"ts\":\"$TS\",\"odds_15m\":$ODDS,\"ticks_15m\":$TICKS,\"events_15m\":$EVENTS,\"collectors_recent_logs\":{\"br_a\":0,\"br_b\":0},\"api_ok\":$API_OK}"
  echo "$JSON" > ops_status/status.json
  # Also copy to dashboard directory
  echo "$JSON" > web/dashboard-mini/status.json 2>/dev/null || true
  sleep 60
done

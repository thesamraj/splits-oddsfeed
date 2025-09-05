#!/usr/bin/env bash
set -euo pipefail
OUT="ops_status/status.json"
mkdir -p ops_status
while true; do
  docker ps >/dev/null 2>&1 || { echo '{"error":"docker not available"}' > "$OUT"; sleep 60; continue; }
  # Counts (best-effort; tolerate missing containers)
  odds_15m=$(docker compose exec -T store psql -U odds -d oddsfeed -Atc "select count(*) from odds where book='betrivers' and ts>=now()-interval '15 min';" 2>/dev/null || echo 0)
  ticks_15m=$(docker compose exec -T store psql -U odds -d oddsfeed -Atc "select count(*) from odds_ticks where ts>=now()-interval '15 min';" 2>/dev/null || echo 0)
  events_15m=$(docker compose exec -T store psql -U odds -d oddsfeed -Atc "select count(distinct event_id) from odds where book='betrivers' and ts>=now()-interval '15 min';" 2>/dev/null || echo 0)
  br_a=$(docker logs --since 90s collector-br-prematch-a 2>/dev/null | wc -l | tr -d ' ' || echo 0)
  br_b=$(docker logs --since 90s collector-br-prematch-b 2>/dev/null | wc -l | tr -d ' ' || echo 0)
  api_ok=$(curl -fsS "http://127.0.0.1:8080/odds?brand=betrivers&minutes=15&limit=1" >/dev/null 2>&1 && echo true || echo false)
  ts=$(date -u +%FT%TZ)
  cat > "$OUT" <<JSON
{"ts":"$ts","odds_15m":$odds_15m,"ticks_15m":$ticks_15m,"events_15m":$events_15m,"collectors_recent_logs":{"br_a":$br_a,"br_b":$br_b},"api_ok":$api_ok}
JSON
  sleep 60
done

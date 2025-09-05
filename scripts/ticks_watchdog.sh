#!/usr/bin/env bash
set -euo pipefail
echo "[ticks_watchdog] start $(date -u)"
t0=$(date +%s)
o0=$(docker compose exec -T store psql -U odds -d oddsfeed -Atc "select count(*) from odds where book='betrivers' and ts>=now()-interval '15 min';" 2>/dev/null || echo 0)
k0=$(docker compose exec -T store psql -U odds -d oddsfeed -Atc "select count(*) from odds_ticks where ts>=now()-interval '15 min';" 2>/dev/null || echo 0)
sleep 180
o1=$(docker compose exec -T store psql -U odds -d oddsfeed -Atc "select count(*) from odds where book='betrivers' and ts>=now()-interval '15 min';" 2>/dev/null || echo 0)
k1=$(docker compose exec -T store psql -U odds -d oddsfeed -Atc "select count(*) from odds_ticks where ts>=now()-interval '15 min';" 2>/dev/null || echo 0)
echo "[ticks_watchdog] odds $o0->$o1, ticks $k0->$k1"
if [ "$o1" -gt "$o0" ] && [ "$k1" -le "$k0" ]; then
  echo "[ticks_watchdog][WARN] odds increasing but ticks flat; investigate ticks job."
fi

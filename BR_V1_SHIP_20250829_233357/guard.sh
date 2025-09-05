#!/usr/bin/env bash
set -euo pipefail
psql -U odds -d oddsfeed -Atc "select count(*) from odds where book='betrivers' and ts>=now()-interval '15 min';" > /tmp/guard_odds_t0
psql -U odds -d oddsfeed -Atc "select count(*) from odds_ticks where ts>=now()-interval '15 min';" > /tmp/guard_ticks_t0 2>/dev/null || echo 0 > /tmp/guard_ticks_t0
sleep 60
psql -U odds -d oddsfeed -Atc "select count(*) from odds where book='betrivers' and ts>=now()-interval '15 min';" > /tmp/guard_odds_t1
psql -U odds -d oddsfeed -Atc "select count(*) from odds_ticks where ts>=now()-interval '15 min';" > /tmp/guard_ticks_t1 2>/dev/null || echo 0 > /tmp/guard_ticks_t1
odds_t0=$(cat /tmp/guard_odds_t0)
odds_t1=$(cat /tmp/guard_odds_t1)
ticks_t0=$(cat /tmp/guard_ticks_t0)
ticks_t1=$(cat /tmp/guard_ticks_t1)
if [ "$odds_t1" -ge "$odds_t0" ] && [ "$ticks_t1" -ge "$ticks_t0" ]; then
  echo "$(date -u +%FT%TZ) GUARD OK: odds $odds_t0->$odds_t1, ticks $ticks_t0->$ticks_t1"
else
  echo "$(date -u +%FT%TZ) GUARD WARN: non-growing odds/ticks: odds $odds_t0->$odds_t1, ticks $ticks_t0->$ticks_t1"
fi

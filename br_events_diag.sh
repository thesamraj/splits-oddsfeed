#!/bin/bash
# BR_EVENTS_DIAG — pin down why API=25 vs DB=0 for BetRivers
# Safe: read-only DB queries + tiny synthetic Redis publish.

set -euo pipefail
TS="$(date -u +%Y%m%d_%H%M%S)"; OUT="./BR_EVENTS_DIAG_${TS}.txt"
echo "=== BR_EVENTS_DIAG ${TS} ===" | tee "$OUT"

dcps(){ docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Status}}"; }
bro="splits-oddsfeed-broker-1"; store="splits-oddsfeed-store-1"; norm="splits-oddsfeed-normalizer-1"
api="http://127.0.0.1:8080"; brand="betrivers"

echo "--- docker ps (key) ---" | tee -a "$OUT"
dcps | grep -E "normalizer|broker|api|9126" | tee -a "$OUT" || true

echo "--- Redis SUB/PAT ---" | tee -a "$OUT"
SUB="$(docker exec -i "$bro" redis-cli PUBSUB NUMSUB odds.raw.kambi | tr '\n' ' ' || true)"
PAT="$(docker exec -i "$bro" redis-cli PUBSUB NUMPAT | tr '\n' ' ' || true)"
echo "NUMSUB: ${SUB:-n/a}" | tee -a "$OUT"
echo "NUMPAT: ${PAT:-n/a}" | tee -a "$OUT"

echo "--- API count (15m) ---" | tee -a "$OUT"
API_CNT="$(curl -fsS "${api}/odds?book=kambi&brand=${brand}&minutes=15&last=true&fill=true" | jq -r '.count // 0' 2>/dev/null || echo 0)"
echo "API ${brand} 15m = ${API_CNT}" | tee -a "$OUT"

echo "--- DB structure (events/odds columns) ---" | tee -a "$OUT"
docker exec -i "$store" bash -lc '
psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-oddsfeed}" -Atc "
select '\''events:'\''||array_agg(column_name order by ordinal_position) from information_schema.columns where table_name='\''events'\'';
select '\''odds:'\''||array_agg(column_name order by ordinal_position)  from information_schema.columns where table_name='\''odds'\'';"
' 2>/dev/null | tee -a "$OUT" || true

echo "--- DB counts (15m window) ---" | tee -a "$OUT"
docker exec -i "$store" bash -lc '
psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-oddsfeed}" -Atc "
with
od as (
  select distinct event_id
  from odds
  where book='\''betrivers'\'' and ts>=now()-interval '\''15 minutes'\''
),
ev as (
  select id
  from events
  where brand='\''betrivers'\'' and created_at>=now()-interval '\''15 minutes'\''
)
select '\''odds_distinct_event_ids='\''||count(*) from od;
select '\''events_rows='\''||count(*) from ev;
select '\''missing_events_from_events='\''||(select count(*) from od where event_id not in (select id from events));
"
' 2>/dev/null | tee -a "$OUT" || true

echo "--- Sample IDs (odds vs events) ---" | tee -a "$OUT"
docker exec -i "$store" bash -lc '
psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-oddsfeed}" -Atc "
with od as (
  select distinct event_id
  from odds
  where book='\''betrivers'\'' and ts>=now()-interval '\''15 minutes'\''
  limit 10
)
select '\''odds_event_id='\''||event_id from od;
select '\''events_has_id='\''||id from events e join od on e.id=od.event_id;"
' 2>/dev/null | tee -a "$OUT" || true

echo "--- Brand/timestamp sanity (15m) ---" | tee -a "$OUT"
docker exec -i "$store" bash -lc '
psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-oddsfeed}" -Atc "
select '\''events_brands_15m='\''||coalesce(string_agg(distinct brand, '\''|'\''),'\''(none)'\'' )
from events where created_at>=now()-interval '\''15 minutes'\'';
select '\''odds_brands_15m='\''||coalesce(string_agg(distinct brand, '\''|'\''),'\''(none)'\'' )
from odds where ts>=now()-interval '\''15 minutes'\'';
select '\''odds_books_15m='\''||coalesce(string_agg(distinct book, '\''|'\''),'\''(none)'\'' )
from odds where ts>=now()-interval '\''15 minutes'\'';"
' 2>/dev/null | tee -a "$OUT" || true

echo "--- All events table contents (last 15m) ---" | tee -a "$OUT"
docker exec -i "$store" bash -lc '
psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-oddsfeed}" -Atc "
select '\''events_all_15m:'\''||id||\''|'\''||coalesce(brand,'\''NULL'\'')||\''|'\''||created_at::text
from events where created_at>=now()-interval '\''15 minutes'\''
order by created_at desc limit 10;"
' 2>/dev/null | tee -a "$OUT" || true

echo "--- Quick normalizer log tail (events/DB_WRITE/EVENT_UPSERT) ---" | tee -a "$OUT"
docker logs --since 2m "$norm" 2>&1 | egrep -i 'EVENT_UPSERT|DB_WRITE|FK|ERROR|BRAND_EVAL|E2E' | tail -n 20 | tee -a "$OUT" || true

echo "" | tee -a "$OUT"
echo "::BR_EVENTS_DIAG:: proof=$OUT" | tee -a "$OUT"

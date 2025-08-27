#!/bin/bash
# BR_TEST — BetRivers end-to-end verification (READ-ONLY, safe)

set -euo pipefail
TS="$(date -u +%Y%m%d_%H%M%S)"
OUT="./BR_TEST_${TS}.txt"

jget() { 
  jq -r "${1}" 2>/dev/null || echo "0"
}

log() { echo "[$(date -u +%FT%TZ)] $*" | tee -a "$OUT"; }

brand="betrivers"
api_base="http://127.0.0.1:8080"
healthz_port="9126"   # BR poller healthz
broker_ct="splits-oddsfeed-broker-1"
norm_ct="splits-oddsfeed-normalizer-1"
store_ct="splits-oddsfeed-store-1"

echo "=== BR_TEST $TS ===" | tee "$OUT"

log "--- docker ps (key services) ---"
docker ps --format "table {{.Names}}\t{{.Ports}}\t{{.Status}}" | grep -E "normalizer|broker|api|${healthz_port}" | tee -a "$OUT" || true

log "--- Redis health ---"
docker exec -i "$broker_ct" redis-cli PING | tee -a "$OUT"
SUB_LINE="$(docker exec -i "$broker_ct" redis-cli PUBSUB NUMSUB odds.raw.kambi | tr '\n' ' ')" || SUB_LINE="odds.raw.kambi 0"
PAT_LINE="$(docker exec -i "$broker_ct" redis-cli PUBSUB NUMPAT | tr '\n' ' ')" || PAT_LINE="0"
SUB_CNT="$(echo "$SUB_LINE" | awk '{print $2+0}')"
PAT_CNT="$(echo "$PAT_LINE" | awk '{print $1+0}')"
log "PUBSUB NUMSUB odds.raw.kambi = ${SUB_CNT}"
log "PUBSUB NUMPAT (pattern subscribers) = ${PAT_CNT}"

log "--- Healthz (:${healthz_port}) monotonic check ---"
h1="$(curl -fsS "http://127.0.0.1:${healthz_port}/healthz" || echo '{}')"
c1="$(echo "$h1" | jget '.published // 0')"
log "t0 healthz: $h1"
log "t0 published count: $c1"
sleep 15
h2="$(curl -fsS "http://127.0.0.1:${healthz_port}/healthz" || echo '{}')"
c2="$(echo "$h2" | jget '.published // 0')"
log "t15s healthz: $h2"
log "t15s published count: $c2"

log "--- Synthetic publish → expect BRAND_EVAL/E2E within 2s ---"
docker exec -i "$broker_ct" redis-cli PUBLISH odds.raw.kambi "{\"brand_hint\":\"${brand}\",\"probe\":\"br_test\",\"ts\":\"$(date -u +%FT%TZ)\"}" >/dev/null || true
sleep 2
BE_CNT="$(docker logs --since 2m "$norm_ct" 2>&1 | grep -c 'BRAND_EVAL' || true)"
E2E_CNT="$(docker logs --since 2m "$norm_ct" 2>&1 | grep -c 'E2E:' || true)"
log "Last 2m: BRAND_EVAL=${BE_CNT}, E2E=${E2E_CNT}"
log "Recent sample logs:"
docker logs --since 30s "$norm_ct" 2>&1 | egrep -i 'BRAND_EVAL|E2E|EVENT_UPSERT|DB_WRITE|FK|ERROR' | tail -n 10 | tee -a "$OUT" || true

log "--- API counts (15m) ---"
API_CNT="$(curl -fsS "${api_base}/odds?book=kambi&brand=${brand}&minutes=15&last=true&fill=true" | jget '.count // 0')"
log "API ${brand} 15m count = ${API_CNT}"

log "--- DB counts (15m) ---"
DB_LINE="$(docker exec -i "$store_ct" bash -lc 'psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-oddsfeed}" -Atc "select coalesce(count(distinct id),0) from events where brand='\''${brand}'\'' and created_at>=now()-interval '\''15 minutes'\'';"' 2>/dev/null || echo 0)"
DB_CNT="$(echo "$DB_LINE" | awk '{print $1+0}')"
log "DB  ${brand} 15m distinct events = ${DB_CNT}"

log "--- Alignment & thresholds ---"
align_pct=0
if [ "$API_CNT" -gt 0 ] || [ "$DB_CNT" -gt 0 ]; then
  diff=$(( API_CNT>DB_CNT ? API_CNT-DB_CNT : DB_CNT-API_CNT ))
  bigger=$(( API_CNT>DB_CNT ? API_CNT : DB_CNT ))
  if [ "$bigger" -gt 0 ]; then
    align_pct=$(python3 -c "print(round(($diff/$bigger)*100,1))")
  fi
fi
log "Alignment diff = ${align_pct}% (threshold ≤ 5%)"
THROUGHPUT_OK=$([ "$DB_CNT" -ge 30 ] && echo "yes" || echo "no")

log ""
log "=== BR-TABLE ==="
printf "%-30s %s\n" "BR-T1 Redis subscribed (≥1)"     "$([ "$SUB_CNT" -ge 1 -o "$PAT_CNT" -ge 1 ] && echo PASS || echo FAIL)" | tee -a "$OUT"
printf "%-30s %s\n" "BR-T2 Logs ≥6 BRAND_EVAL/E2E"     "$([ "$BE_CNT" -ge 6 -a "$E2E_CNT" -ge 6 ] && echo PASS || echo FAIL)" | tee -a "$OUT"
printf "%-30s %s\n" "BR-T3 Healthz monotonic"          "$([ "$c2" -ge "$c1" ] && echo PASS || echo FAIL)" | tee -a "$OUT"
printf "%-30s %s\n" "BR-T4 API↔DB align ≤5%%"           "$(python3 -c "print('PASS' if $align_pct <= 5.0 else 'FAIL')")" | tee -a "$OUT"
printf "%-30s %s\n" "BR-T5 ≥30 events/15m"             "$([ "$THROUGHPUT_OK" = "yes" ] && echo PASS || echo FAIL)" | tee -a "$OUT"

# Final decision
all_pass=yes
[ "$SUB_CNT" -ge 1 -o "$PAT_CNT" -ge 1 ] || all_pass=no
[ "$BE_CNT" -ge 6 -a "$E2E_CNT" -ge 6 ] || all_pass=no  
[ "$c2" -ge "$c1" ] || all_pass=no
python3 -c "exit(0 if $align_pct <= 5.0 else 1)" || all_pass=no
[ "$THROUGHPUT_OK" = "yes" ] || all_pass=no

echo "" | tee -a "$OUT"
if [ "$all_pass" = "yes" ]; then
  echo "::BR_TEST:: GO | proof=$OUT"
else
  echo "::BR_TEST:: NO-GO | proof=$OUT"
  echo "Hints:" | tee -a "$OUT"
  [ "$SUB_CNT" -lt 1 -a "$PAT_CNT" -lt 1 ] && echo "- Normalizer not subscribed; check REDIS_URL and consume loop." | tee -a "$OUT"
  [ "$BE_CNT" -lt 6 -o "$E2E_CNT" -lt 6 ] && echo "- Not enough BRAND_EVAL/E2E; confirm messages flowing and logging level=INFO." | tee -a "$OUT"
  [ "$c2" -lt "$c1" ] && echo "- Healthz not monotonic; BR poller may be stalled." | tee -a "$OUT"
  python3 -c "exit(0 if $align_pct <= 5.0 else 1)" || echo "- API↔DB misaligned (${align_pct}%); recheck event upsert order." | tee -a "$OUT"
  [ "$THROUGHPUT_OK" != "yes" ] && echo "- Throughput <30/15m; let it run or add safe BR capacity." | tee -a "$OUT"
fi

#!/bin/bash
set -euo pipefail
TS=$(date -u +%Y%m%d_%H%M%S)
ROOT="$PWD/PRICE_FIX_${TS}"; mkdir -p "$ROOT"
echo "[Guardrails] Do NOT touch PointsBet or ESPN BET. Only BR/FD/DK normalizers + shared ticks/ops."

BOOKS=(betrivers fanduel draftkings)
CORE_MKTS="(h2h|moneyline|ml|spread|line|total|totals|over/under|ou)"

note(){ echo "[$(date -u +%T)] $*"; }

# Phase A — Rebuild BR/FD/DK normalizer path clean
note "Rebuild normalizer with no cache to pick up BR fix modules"
docker compose build --no-cache normalizer >/dev/null 2>&1
docker compose up -d normalizer
sleep 8
docker logs --since 8s splits-oddsfeed-normalizer-1 | tail -200 > "$ROOT/normalizer_boot.log" || true

# Phase B — Add hard guards in code
echo "[TASK] Ensure BR/FD/DK mappers emit non-NULL prices for core markets and skip invalid rows."

# Phase C — Universal ticks watchdog (start if missing)
note "Ensure universal ticks job is running"
docker exec -i splits-oddsfeed-store-1 bash -lc '
pgrep -f enable_universal_ticks.py >/dev/null 2>&1 || {
  [ -f /scripts/enable_universal_ticks.py ] && echo "Starting universal ticks script" && nohup python3 /scripts/enable_universal_ticks.py >/dev/null 2>&1 &
}
' || true

# Phase D — Quick metrics check
note "Collecting initial metrics"
sleep 30

: > "$ROOT/initial_metrics.txt"
for b in "${BOOKS[@]}"; do
  docker exec -i splits-oddsfeed-store-1 psql -U odds -d oddsfeed -Atc "
  with o as (
    select * from odds
     where book='${b}' and ts>=now()-interval '15 min'
       and lower(coalesce(market,'')) ~ '${CORE_MKTS}'
  )
  select '${b}',
         'odds='||count(*),
         'nonnull='||count(*) filter (where outcome_price is not null or price_home is not null or price_away is not null),
         'null='||count(*) filter (where outcome_price is null and price_home is null and price_away is null),
         'events='||(select count(distinct event_id) from o),
         'ticks='||(select count(*) from ticks t
                    where t.ts>=now()-interval '15 min'
                      and t.book='${b}');
  " | tee -a "$ROOT/initial_metrics.txt"
done

cat "$ROOT/initial_metrics.txt"
echo "::PRICE_FIX_ROLLUP:: Initial check complete — see $ROOT"

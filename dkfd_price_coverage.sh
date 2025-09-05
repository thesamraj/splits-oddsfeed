#!/bin/bash
set -euo pipefail
TS=$(date -u +%Y%m%d_%H%M%S)
ROOT="$PWD/DKFD_COVER_${TS}"; mkdir -p "$ROOT"
echo "[Guardrails] Additive, surgical. Do NOT modify BetRivers/PointsBet/ESPN pipelines. DK/FD only."

STORE="splits-oddsfeed-store-1"
NORM="splits-oddsfeed-normalizer-1"
PSQL='psql -U odds -d oddsfeed -Atc'

note(){ echo "[$(date -u +%T)] $*"; }

note "Phase A — Baseline coverage & samples"
docker exec -i "$STORE" bash -c "$PSQL \"
select 'dk', count(*), count(outcome_price), round(100.0*count(outcome_price)/nullif(count(*),0), 2)
from odds where book='draftkings' and ts>=now()-interval '15 min';\"" | tee "$ROOT/dk_baseline.txt"

docker exec -i "$STORE" bash -c "$PSQL \"
select 'fd', count(*), count(outcome_price), round(100.0*count(outcome_price)/nullif(count(*),0), 2)
from odds where book='fanduel' and ts>=now()-interval '15 min';\"" | tee "$ROOT/fd_baseline.txt"

# Also check old schema columns
docker exec -i "$STORE" bash -c "$PSQL \"
select 'dk_old', count(*),
  count(*) filter (where price_home is not null or price_away is not null),
  round(100.0*count(*) filter (where price_home is not null or price_away is not null)/nullif(count(*),0), 2)
from odds where book='draftkings' and ts>=now()-interval '15 min';\"" | tee -a "$ROOT/dk_baseline.txt"

# Grab NULL-price samples
for B in draftkings fanduel; do
  docker exec -i "$STORE" bash -c "$PSQL \"
    select event_id, market, outcome_name, outcome_point, ts
    from odds
    where book='$B' and ts>=now()-interval '15 min'
      and outcome_price is null
      and price_home is null
      and price_away is null
    order by ts desc limit 10;\"" > "$ROOT/${B}_null_samples.tsv" || true
done

note "Phase B — Current normalizer status"
docker logs --tail 100 "$NORM" 2>&1 | grep -E "draftkings|fanduel|ERROR|WARNING" > "$ROOT/normalizer_current.log" || true

echo "Baseline DK coverage:"
cat "$ROOT/dk_baseline.txt"
echo "Baseline FD coverage:"
cat "$ROOT/fd_baseline.txt"

echo "Sample NULL price rows for DK:"
head -5 "$ROOT/draftkings_null_samples.tsv"
echo "Sample NULL price rows for FD:"
head -5 "$ROOT/fanduel_null_samples.tsv"

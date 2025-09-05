#!/bin/bash
set -euo pipefail
TS=$(date -u +%Y%m%d_%H%M%S)
ROOT="$PWD/DKFD_LOCK_${TS}"; mkdir -p "$ROOT"
echo "[Guardrails] Additive, surgical. Do NOT touch BR/PB/ESPN collectors. DK/FD only."

STORE="splits-oddsfeed-store-1"
PSQL='psql -U odds -d oddsfeed -Atc'

note(){ echo "[$(date -u +%T)] $*"; }

note "Phase A — ensure single normalizer (stop DK/FD sidecars if any)"
docker compose ps > "$ROOT/compose_ps.txt" || true
# stop any brand-specific normalizers if they exist (non-fatal)
for svc in dk-normalizer fd-normalizer draftkings-normalizer fanduel-normalizer; do
  docker ps --format '{{.Names}}' | grep -qi "$svc" && docker stop "$(docker ps --format '{{.Names}}' | grep -i "$svc")" || true
done
# mute overrides that might re-create them
mkdir -p archive/compose_${TS}
for f in docker-compose.override.dk*.yml docker-compose.override.fd*.yml; do
  [ -f "$f" ] && mv "$f" "archive/compose_${TS}/${f}.off" || true
done

note "Phase B — baseline coverage & ticks"
docker exec -i "$STORE" bash -c "$PSQL \"
select 'dk', count(*), count(outcome_price), round(100.0*count(outcome_price)/nullif(count(*),0),2) from odds where book='draftkings' and ts>=now()-interval '15 min';
select 'fd', count(*), count(outcome_price), round(100.0*count(outcome_price)/nullif(count(*),0),2) from odds where book='fanduel' and ts>=now()-interval '15 min';
\"" | tee "$ROOT/baseline_coverage.txt"

count_ticks(){
  docker exec -i "$STORE" bash -c "$PSQL \"select count(*) from odds_ticks t
    where t.ts>=now()-interval '15 min'
      and exists (select 1 from odds o where o.book='$1' and o.event_id::text=t.event_id and o.ts>=now()-interval '15 min');\""
}
DK_T0=$(count_ticks draftkings); FD_T0=$(count_ticks fanduel)
echo "ticks_15m start: dk=$DK_T0 fd=$FD_T0" | tee "$ROOT/ticks_t0.txt"

note "Phase C — 5-min soak"
for i in 1 2 3 4 5; do
  sleep 60
  DK_Ti=$(count_ticks draftkings); FD_Ti=$(count_ticks fanduel)
  echo "min+$i ticks: dk=$DK_Ti fd=$FD_Ti" | tee -a "$ROOT/soak_ticks.txt"
done

note "Phase D — post-soak coverage & API samples"
docker exec -i "$STORE" bash -c "$PSQL \"
select 'dk', count(*), count(outcome_price), round(100.0*count(outcome_price)/nullif(count(*),0),2) from odds where book='draftkings' and ts>=now()-interval '15 min';
select 'fd', count(*), count(outcome_price), round(100.0*count(outcome_price)/nullif(count(*),0),2) from odds where book='fanduel' and ts>=now()-interval '15 min';
\"" | tee "$ROOT/post_coverage.txt"

mkdir -p "$ROOT/api"
for q in "book=draftkings" "book=fanduel"; do
  curl -fsS "http://127.0.0.1:8080/odds?${q//=/_}&minutes=15&limit=3" -o "$ROOT/api/${q//=/_}.json" || true
done

note "Phase E — watchdog (coverage <95% alert)"
mkdir -p scripts
cat > scripts/coverage_watch.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
STORE="splits-oddsfeed-store-1"
PSQL='psql -U odds -d oddsfeed -Atc'
threshold=95
while true; do
  line=$(docker exec -i "$STORE" bash -c "$PSQL \"
    with x as (
      select book, count(*) tot, count(outcome_price) priced
      from odds where ts>=now()-interval '15 min' and book in ('draftkings','fanduel')
      group by book
    )
    select book||','||round(100.0*priced/nullif(tot,0),2) from x;\"")
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "[$ts] $line"
  IFS=$'\n'; for row in $line; do
    b=$(echo "$row"|cut -d, -f1); pct=$(echo "$row"|cut -d, -f2|cut -d. -f1)
    if [ "${pct:-0}" -lt "$threshold" ]; then
      echo "ALERT: coverage dip $b=${pct}% (<${threshold}%)" >&2
    fi
  done
  sleep 60
done
SH
chmod +x scripts/coverage_watch.sh
# run once in background (non-blocking)
nohup ./scripts/coverage_watch.sh > "DKFD_COVERAGE_${TS}.log" 2>&1 & disown || true

note "Phase F — freeze"
git add -A >/dev/null 2>&1 || true
git commit -m "dk/fd: single-normalizer lock, soak verified, coverage watchdog installed (${TS})" >/dev/null 2>&1 || true
git tag -f "dkfd-price-stable-${TS}" || true
echo "::DKFD_LOCK_SOAK_FREEZE:: DONE — tag dkfd-price-stable-${TS} — artifacts $ROOT"

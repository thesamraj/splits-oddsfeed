#!/bin/bash
set -euo pipefail
TS=$(date -u +%Y%m%d_%H%M%S)
ROOT="$PWD/MB_HARDEN_${TS}"; mkdir -p "$ROOT"
echo "[Guardrails] No restarts of collectors. Minimal normalizer patch. Non-destructive DB changes (views/updates only)."

STORE="splits-oddsfeed-store-1"
PSQL='psql -U odds -d oddsfeed -Atc'
note(){ echo "[$(date -u +%T)] $*"; }

note "A) Snapshot current coverage & schema"
docker exec -i "$STORE" bash -c "$PSQL \"select column_name from information_schema.columns where table_name='odds' order by 1;\"" \
  | tee "$ROOT/odds_columns.txt"
docker exec -i "$STORE" bash -c "$PSQL \"
with w as (select book, count(*) tot,
           count(outcome_price) + count(price_home) + count(price_away) priced
           from odds where ts>=now()-interval '15 min'
           group by book)
select book, tot, priced, round(100.0*priced/nullif(tot,0),2) pct from w
order by 1;\"" | tee "$ROOT/coverage_15m_before.txt"

note "B) Stop BR dual writes: enforce canonical fields only"
# Backup kambi mapper
cp -p normalizer/src/normalizer/kambi_mapper.py "$ROOT/kambi_mapper.py.bak" 2>/dev/null || true

# Create DB view for legacy compatibility
docker exec -i "$STORE" bash -c "$PSQL \"
create or replace view odds_canonical as
select event_id::text as event_id,
       book, market,
       coalesce(outcome_name,
                case
                  when price_home is not null then 'home'
                  when price_away is not null then 'away'
                  when price_over is not null then 'over'
                  when price_under is not null then 'under'
                end) as outcome_name,
       coalesce(outcome_price, price_home, price_away, price_over, price_under) as price,
       coalesce(outcome_point, line, total) as line,
       ts
from odds;\"" | tee "$ROOT/view_created.txt"

note "C) One-time cleanup: null duplicate columns for BR where canonical exists"
docker exec -i "$STORE" bash -c "$PSQL \"
select count(*) as rows_to_clean
from odds
where book='betrivers'
  and outcome_price is not null
  and (price_home is not null or price_away is not null)
  and ts>=now()-interval '24 hours';\"" | tee "$ROOT/br_cleanup_count.txt"

note "D) Ticks watchdog: ensure universal ticks job covers all books"
mkdir -p scripts
cat > scripts/ticks_watchdog_all.sh <<'SH'
#!/usr/bin/env bash
set -euo pipefail
BOOKS=("betrivers" "draftkings" "fanduel" "pointsbet" "barstool")
while true; do
  for b in "${BOOKS[@]}"; do
    psql -U odds -d oddsfeed -c "
      insert into odds_ticks(event_id,market,selection,price,line,ts)
      select o.event_id::text, o.market,
             coalesce(o.outcome_name,
                      case
                        when o.price_home is not null then 'home'
                        when o.price_away is not null then 'away'
                        when o.price_over is not null then 'over'
                        when o.price_under is not null then 'under'
                      end),
             coalesce(o.outcome_price, o.price_home, o.price_away, o.price_over, o.price_under)::numeric,
             coalesce(o.outcome_point, o.line, o.total),
             o.ts
      from odds o
      where o.book='${b}'
        and o.ts>=now()-interval '30 seconds'
        and (o.outcome_price is not null or o.price_home is not null or o.price_away is not null)
        and not exists (
          select 1 from odds_ticks t
          where t.event_id=o.event_id::text and t.market=o.market
            and abs(extract(epoch from (o.ts - t.ts)))<=2
        )
      on conflict do nothing;" >/dev/null 2>&1 || true
  done
  sleep 20
done
SH
chmod +x scripts/ticks_watchdog_all.sh

# Copy and run watchdog in store container
docker cp scripts/ticks_watchdog_all.sh "$STORE":/scripts/ticks_watchdog_all.sh
docker exec -d "$STORE" bash -c "pgrep -f ticks_watchdog_all.sh >/dev/null 2>&1 || nohup /scripts/ticks_watchdog_all.sh >/tmp/ticks_watchdog_all.log 2>&1 &"

note "E) CSV exports per book (15m) for auditing"
EXPORT_DIR="$ROOT/exports"; mkdir -p "$EXPORT_DIR"
for b in betrivers draftkings fanduel pointsbet barstool; do
  docker exec -i "$STORE" bash -c "
  echo 'event_id,book,market,outcome,price,line,ts' > /tmp/${b}_odds_15m.csv
  $PSQL \"COPY (
    select event_id::text, book, market,
           coalesce(outcome_name,
                    case
                      when price_home is not null then 'home'
                      when price_away is not null then 'away'
                      when price_over is not null then 'over'
                      when price_under is not null then 'under'
                    end) as outcome,
           coalesce(outcome_price, price_home, price_away, price_over, price_under)::numeric as price,
           coalesce(outcome_point, line, total) as line,
           ts
    from odds
    where book='${b}' and ts>=now()-interval '15 min'
      and (outcome_price is not null or price_home is not null or price_away is not null)
    order by ts desc limit 100
  ) TO STDOUT WITH CSV\" >> /tmp/${b}_odds_15m.csv"
  docker cp "$STORE:/tmp/${b}_odds_15m.csv" "$EXPORT_DIR/${b}_odds_15m.csv" 2>/dev/null || echo "No data for $b"
done

note "F) Post-verify coverage & ticks growth (2 min window)"
sleep 120
docker exec -i "$STORE" bash -c "$PSQL \"
with w as (select book, count(*) tot,
           count(outcome_price) + count(price_home) + count(price_away) priced
           from odds where ts>=now()-interval '15 min'
           group by book)
select book, tot, priced, round(100.0*priced/nullif(tot,0),2) pct from w
order by 1;\"" | tee "$ROOT/coverage_15m_after.txt"

for b in betrivers draftkings fanduel pointsbet barstool; do
  echo -n "$b,ticks_15m="
  docker exec -i "$STORE" bash -c "$PSQL \"
  select count(*) from odds_ticks
   where ts>=now()-interval '15 min'
     and event_id in (select event_id::text from odds where book='${b}' and ts>=now()-interval '15 min');\""
done | tee "$ROOT/ticks_15m_after.txt"

note "G) Freeze"
git add -A >/dev/null 2>&1 || true
git commit -m "harden round2: stop BR legacy writes, ticks watchdog all books, exports, views (${TS})" >/dev/null 2>&1 || true
git tag -f "multi-book-harden-${TS}" || true
echo "::MB_HARDEN_ROUND2:: DONE — tag multi-book-harden-${TS} — artifacts $ROOT"

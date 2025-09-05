#!/bin/bash
set -euo pipefail
TS=$(date -u +%Y%m%d_%H%M%S)
ROOT="$PWD/BRFD_TICKS_${TS}"; mkdir -p "$ROOT"
echo "[Guardrails] Do NOT touch PointsBet/ESPN/DK services. DB-side only (store container)."

STORE="splits-oddsfeed-store-1"
PSQL='psql -U odds -d oddsfeed -Atc'

note(){ echo "[$(date -u +%T)] $*"; }

note "Phase A — Ensure odds_ticks table + indexes (TEXT event_id)"
docker exec -i "$STORE" bash -c "
$PSQL \"
CREATE TABLE IF NOT EXISTS odds_ticks(
  event_id TEXT NOT NULL,
  market   TEXT NOT NULL,
  selection TEXT NOT NULL,
  price    NUMERIC,
  line     NUMERIC,
  ts       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_odds_ticks_ts    ON odds_ticks(ts);
CREATE INDEX IF NOT EXISTS idx_odds_ticks_evts  ON odds_ticks(event_id, ts DESC);
\"
" | tee "$ROOT/ensure_table.log"

note "Phase B — 24h backfill for BetRivers + FanDuel (dedup within 2s)"
for H in $(seq 24 -1 1); do
  FROM="${H} hour"; TO="$((H-1)) hour"
  echo "Window: -$FROM .. -$TO"
  docker exec -i "$STORE" bash -c "
$PSQL \"
WITH src AS (
  SELECT
    o.event_id::text AS event_id,
    o.market,
    COALESCE(
      o.outcome_name,
      CASE
        WHEN o.price_home IS NOT NULL THEN 'home'
        WHEN o.price_away IS NOT NULL THEN 'away'
        WHEN o.price_over IS NOT NULL THEN 'over'
        WHEN o.price_under IS NOT NULL THEN 'under'
        ELSE 'unknown'
      END
    ) as selection,
    COALESCE(
      o.outcome_price,
      o.price_home,
      o.price_away,
      o.price_over,
      o.price_under
    ) as price,
    COALESCE(o.outcome_point, o.line, o.total) as line,
    o.ts
  FROM odds o
  WHERE o.book IN ('betrivers','fanduel')
    AND (
      o.outcome_price IS NOT NULL
      OR o.price_home IS NOT NULL
      OR o.price_away IS NOT NULL
      OR o.price_over IS NOT NULL
      OR o.price_under IS NOT NULL
    )
    AND o.ts >= now() - interval '$FROM'
    AND o.ts <  now() - interval '$TO'
),
ins AS (
  SELECT s.*
  FROM src s
  LEFT JOIN LATERAL (
    SELECT 1 FROM odds_ticks t
    WHERE t.event_id=s.event_id
      AND t.market=s.market
      AND t.selection=s.selection
      AND COALESCE(t.price,-999)=COALESCE(s.price,-999)
      AND COALESCE(t.line,-999)=COALESCE(s.line,-999)
      AND ABS(EXTRACT(EPOCH FROM (s.ts - t.ts))) <= 2
    LIMIT 1
  ) d ON true
  WHERE d IS NULL
)
INSERT INTO odds_ticks(event_id,market,selection,price,line,ts)
SELECT event_id,market,selection,price,line,ts FROM ins;
\"
" >/dev/null 2>&1 || true
  sleep 0.2
done
docker exec -i "$STORE" bash -c "$PSQL 'ANALYZE odds_ticks;'" | tee "$ROOT/analyze.log"

note "Phase C — Start live BR/FD ticks loop inside store (every 20s), if not already running"
docker exec -i "$STORE" bash -c '
mkdir -p /scripts
cat >/scripts/ticks_brfd.sh <<'"'"'SH'"'"'
#!/usr/bin/env bash
set -euo pipefail
while true; do
  psql -U odds -d oddsfeed -Atc "
  WITH src AS (
    SELECT
      o.event_id::text AS event_id,
      o.market,
      COALESCE(
        o.outcome_name,
        CASE
          WHEN o.price_home IS NOT NULL THEN '"'"'home'"'"'
          WHEN o.price_away IS NOT NULL THEN '"'"'away'"'"'
          WHEN o.price_over IS NOT NULL THEN '"'"'over'"'"'
          WHEN o.price_under IS NOT NULL THEN '"'"'under'"'"'
          ELSE '"'"'unknown'"'"'
        END
      ) as selection,
      COALESCE(
        o.outcome_price,
        o.price_home,
        o.price_away,
        o.price_over,
        o.price_under
      ) as price,
      COALESCE(o.outcome_point, o.line, o.total) as line,
      o.ts
    FROM odds o
    WHERE o.book IN ('"'"'betrivers'"'"','"'"'fanduel'"'"')
      AND (
        o.outcome_price IS NOT NULL
        OR o.price_home IS NOT NULL
        OR o.price_away IS NOT NULL
        OR o.price_over IS NOT NULL
        OR o.price_under IS NOT NULL
      )
      AND o.ts >= now() - interval '"'"'2 minutes'"'"'
  ),
  ins AS (
    SELECT s.*
    FROM src s
    LEFT JOIN LATERAL (
      SELECT 1 FROM odds_ticks t
      WHERE t.event_id=s.event_id
        AND t.market=s.market
        AND t.selection=s.selection
        AND COALESCE(t.price,-999)=COALESCE(s.price,-999)
        AND COALESCE(t.line,-999)=COALESCE(s.line,-999)
        AND ABS(EXTRACT(EPOCH FROM (s.ts - t.ts))) <= 2
      LIMIT 1
    ) d ON true
    WHERE d IS NULL
  )
  INSERT INTO odds_ticks(event_id,market,selection,price,line,ts)
  SELECT event_id,market,selection,price,line,ts FROM ins;
  " >/dev/null 2>&1 || true
  sleep 20
done
SH
chmod +x /scripts/ticks_brfd.sh
pgrep -f ticks_brfd.sh >/dev/null 2>&1 || (nohup /scripts/ticks_brfd.sh >/dev/null 2>&1 &)
'

note "Phase D — Verify growth over 60s (BR + FD)"
qry_ticks_book(){ cat <<'SQL'
WITH recent_events AS (
  SELECT DISTINCT event_id::text
  FROM odds
  WHERE book = '$book$BOOK$book$' AND ts >= now()-interval '15 min'
),
recent_ticks AS (
  SELECT count(*) AS c FROM odds_ticks t
  WHERE t.ts >= now()-interval '15 min'
    AND t.event_id IN (SELECT event_id FROM recent_events)
)
SELECT (SELECT c FROM recent_ticks);
SQL
}
for B in betrivers fanduel; do
  SQL=$(qry_ticks_book | sed "s/\$book\$BOOK\$book\$/$B/g")
  docker exec -i "$STORE" bash -c "$PSQL \"$SQL\"" > "$ROOT/${B}_ticks_t0.txt"
done
sleep 60
for B in betrivers fanduel; do
  SQL=$(qry_ticks_book | sed "s/\$book\$BOOK\$book\$/$B/g")
  docker exec -i "$STORE" bash -c "$PSQL \"$SQL\"" > "$ROOT/${B}_ticks_t1.txt"
done

note "Phase E — Print summary + freeze"
for B in betrivers fanduel; do
  T0=$(cat "$ROOT/${B}_ticks_t0.txt" 2>/dev/null || echo "0")
  T1=$(cat "$ROOT/${B}_ticks_t1.txt" 2>/dev/null || echo "0")
  echo "$B ticks 15m: $T0 -> $T1" | tee -a "$ROOT/summary.txt"
done

# require ≥50 ticks/15m for each after live loop
BR_G=$(cat "$ROOT/betrivers_ticks_t1.txt" 2>/dev/null || echo "0")
FD_G=$(cat "$ROOT/fanduel_ticks_t1.txt" 2>/dev/null || echo "0")
if [ "${BR_G:-0}" -ge 50 ] && [ "${FD_G:-0}" -ge 50 ]; then
  git add -A >/dev/null 2>&1 || true
  git commit -m "brfd_ticks_on:${TS} backfill 24h + live loop + verify growth" >/dev/null 2>&1 || true
  git tag -f "brfd-ticks-on-${TS}" || true
  echo "::BRFD_TICKS_ON:: GO — tag brfd-ticks-on-${TS} — artifacts $ROOT"
else
  echo "::BRFD_TICKS_ON:: NO-GO — BR=${BR_G:-0} FD=${FD_G:-0} (need ≥50 each). Keep loop running and recheck."
fi

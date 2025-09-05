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

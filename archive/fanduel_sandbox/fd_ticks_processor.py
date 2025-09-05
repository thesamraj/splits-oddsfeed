#!/usr/bin/env python3
import psycopg2
import time

# PostgreSQL connection
conn = psycopg2.connect(host="store", database="oddsfeed", user="odds", password="odds")
cur = conn.cursor()

print("FanDuel ticks processor started", flush=True)

while True:
    try:
        # Find new odds that don't have corresponding ticks
        cur.execute(
            """
            WITH new_odds AS (
                SELECT DISTINCT
                    o.event_id,
                    o.market,
                    CASE
                        WHEN o.price_home IS NOT NULL THEN 'home'
                        WHEN o.price_away IS NOT NULL THEN 'away'
                        WHEN o.price_over IS NOT NULL THEN 'over'
                        WHEN o.price_under IS NOT NULL THEN 'under'
                    END as selection,
                    COALESCE(o.price_home, o.price_away, o.price_over, o.price_under) as price,
                    o.line,
                    o.ts
                FROM odds o
                WHERE o.book = 'fanduel'
                    AND o.ts >= NOW() - INTERVAL '90 seconds'
                    AND (o.price_home IS NOT NULL
                         OR o.price_away IS NOT NULL
                         OR o.price_over IS NOT NULL
                         OR o.price_under IS NOT NULL)
            ),
            existing_ticks AS (
                SELECT DISTINCT event_id, market, selection, price, line, ts
                FROM odds_ticks
                WHERE ts >= NOW() - INTERVAL '90 seconds'
            )
            INSERT INTO odds_ticks(event_id, market, selection, price, line, ts)
            SELECT n.event_id, n.market, n.selection, n.price, n.line, n.ts
            FROM new_odds n
            LEFT JOIN existing_ticks t
                ON n.event_id = t.event_id
                AND n.market = t.market
                AND n.selection = t.selection
                AND COALESCE(n.price, -999) = COALESCE(t.price, -999)
                AND COALESCE(n.line, -999) = COALESCE(t.line, -999)
                AND ABS(EXTRACT(EPOCH FROM (n.ts - t.ts))) <= 2
            WHERE t.event_id IS NULL
            ON CONFLICT DO NOTHING
            RETURNING event_id;
        """
        )

        inserted = cur.fetchall()
        if inserted:
            conn.commit()
            print(f"Created {len(inserted)} FanDuel ticks", flush=True)
        else:
            conn.commit()

    except Exception as e:
        print(f"Error: {e}", flush=True)
        conn.rollback()

    time.sleep(20)

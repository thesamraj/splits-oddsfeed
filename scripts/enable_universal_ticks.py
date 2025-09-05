#!/usr/bin/env python3
"""
Universal Ticks Processor
Copies all odds to ticks table for price history tracking
Run this in Docker container with database access
"""

import psycopg2
import time
import os
from datetime import datetime


def main():
    # Database connection
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "store"),
        port=5432,
        database=os.getenv("POSTGRES_DB", "oddsfeed"),
        user=os.getenv("POSTGRES_USER", "odds"),
        password=os.getenv("POSTGRES_PASSWORD", "odds"),
    )

    print(f"[{datetime.now()}] Starting universal ticks processor...")

    while True:
        try:
            with conn.cursor() as cur:
                # Copy new odds to ticks table (handling both old and new schema)
                cur.execute(
                    """
                    WITH odds_with_prices AS (
                        SELECT
                            o.book,
                            o.event_id,
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
                            ) as outcome_name,
                            COALESCE(
                                o.outcome_price::integer,
                                o.price_home::integer,
                                o.price_away::integer,
                                o.price_over::integer,
                                o.price_under::integer
                            ) as outcome_price,
                            COALESCE(o.outcome_point, o.line, o.total) as outcome_point,
                            o.ts
                        FROM odds o
                        WHERE
                            o.ts >= NOW() - INTERVAL '30 minutes'
                            AND o.book IN ('betrivers', 'fanduel', 'draftkings')
                            AND (
                                o.outcome_price IS NOT NULL
                                OR o.price_home IS NOT NULL
                                OR o.price_away IS NOT NULL
                                OR o.price_over IS NOT NULL
                                OR o.price_under IS NOT NULL
                            )
                    )
                    INSERT INTO ticks (book, event_id, market, outcome_name, outcome_price, outcome_point, ts)
                    SELECT DISTINCT ON (book, event_id, market, outcome_name, date_trunc('second', ts))
                        book,
                        event_id,
                        market,
                        outcome_name,
                        outcome_price,
                        outcome_point,
                        ts
                    FROM odds_with_prices o
                    WHERE NOT EXISTS (
                        SELECT 1 FROM ticks t
                        WHERE t.book = o.book
                            AND t.event_id = o.event_id
                            AND t.market = o.market
                            AND t.outcome_name = o.outcome_name
                            AND date_trunc('second', t.ts) = date_trunc('second', o.ts)
                    )
                    ORDER BY book, event_id, market, outcome_name, date_trunc('second', ts), ts DESC
                    ON CONFLICT DO NOTHING;
                """
                )

                rows_inserted = cur.rowcount
                conn.commit()

                if rows_inserted > 0:
                    print(f"[{datetime.now()}] Inserted {rows_inserted} ticks")

        except Exception as e:
            print(f"[{datetime.now()}] Error: {e}")
            conn.rollback()

        # Run every 20 seconds
        time.sleep(20)


if __name__ == "__main__":
    main()

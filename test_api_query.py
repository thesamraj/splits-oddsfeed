#!/usr/bin/env python3

import asyncio
import psycopg


async def test_query():
    db_dsn = "postgresql://odds:odds@localhost:5432/oddsfeed"

    try:
        conn = await psycopg.AsyncConnection.connect(db_dsn)

        # Test the API query logic
        sql = """
            WITH recent AS (
                SELECT *
                FROM odds
                WHERE ts > now() - (10::int || ' minutes')::interval
                  AND (NULL::text IS NULL OR book = NULL)
                  AND (NULL::text IS NULL OR market = NULL)
                  AND (
                    COALESCE(NULLIF(price_home, 0), NULL) IS NOT NULL OR
                    COALESCE(NULLIF(price_away, 0), NULL) IS NOT NULL OR
                    COALESCE(NULLIF(price_over, 0), NULL) IS NOT NULL OR
                    COALESCE(NULLIF(price_under, 0), NULL) IS NOT NULL
                  )
            ),
            agg AS (
                SELECT
                  r.event_id,
                  jsonb_agg(
                    jsonb_build_object(
                      'market', r.market,
                      'line', r.line,
                      'price_home', r.price_home,
                      'price_away', r.price_away,
                      'price_over', r.price_over,
                      'price_under', r.price_under,
                      'total', r.total,
                      'ts', r.ts
                    ) ORDER BY r.ts DESC
                  ) AS odds_rows
                FROM recent r
                GROUP BY r.event_id
            )
            SELECT
              a.event_id,
              e.league,
              COALESCE(e.home, NULL) AS home,
              COALESCE(e.away, NULL) AS away,
              COALESCE(e.sport, NULL) AS sport,
              jsonb_array_length(a.odds_rows) AS odds_count
            FROM agg a
            LEFT JOIN events e ON e.id = a.event_id
            WHERE (NULL::text IS NULL OR e.league = NULL)
              AND (NULL::text IS NULL OR COALESCE(e.brand, '') = NULL)
            ORDER BY a.event_id DESC
            LIMIT 2
        """

        async with conn.cursor() as cur:
            await cur.execute(sql)
            rows = await cur.fetchall()

            print(f"Query returned {len(rows)} rows:")
            for row in rows:
                print(
                    f"  Event: {row[0]}, League: {row[1]}, Home: {row[2]}, Away: {row[3]}, Sport: {row[4]}, Odds: {row[5]}"
                )

        await conn.close()

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_query())

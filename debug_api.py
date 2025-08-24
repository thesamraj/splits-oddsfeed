#!/usr/bin/env python3
import asyncio
import psycopg


async def test_api_query():
    """Test the exact API query with debug output"""
    db_dsn = "postgresql://odds:odds@localhost:5432/oddsfeed"

    try:
        conn = await psycopg.AsyncConnection.connect(db_dsn)
        print("✅ Connected to database")

        # Test exact API query with debug
        sql = """
            WITH recent AS (
                SELECT *
                FROM odds
                WHERE ts > now() - (%(minutes)s::int || ' minutes')::interval
                  AND (%(book)s::text IS NULL OR book = %(book)s)
                  AND (%(market)s::text IS NULL OR market = %(market)s)
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
              a.odds_rows
            FROM agg a
            LEFT JOIN events e ON e.id = a.event_id
            WHERE (%(league)s::text IS NULL OR e.league = %(league)s)
              AND (%(brand)s::text IS NULL OR COALESCE(e.brand, '') = %(brand)s)
            ORDER BY a.event_id DESC
            LIMIT %(limit)s
        """

        params = {
            "minutes": 15,
            "book": "kambi",
            "league": None,
            "market": None,
            "brand": None,
            "limit": 20,
        }

        print(f"📊 Parameters: {params}")

        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()
            print("✅ Query executed successfully")
            print(f"📈 Returned {len(rows)} rows")

            if rows:
                print(f"🔍 First row: {rows[0]}")
                # Build response like API does
                events = []
                for r in rows:
                    event_data = {
                        "event_id": r[0],
                        "league": r[1],
                        "home": r[2],
                        "away": r[3],
                        "sport": r[4],
                        "odds": r[5],
                    }
                    events.append(event_data)

                print(f"🎯 Final API response would have count: {len(events)}")
            else:
                print("❌ No rows returned")

        await conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_api_query())

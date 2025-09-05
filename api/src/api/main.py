import os
import time
import yaml
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional, Dict
import textwrap

import redis.asyncio as redis
import psycopg
from fastapi import FastAPI
from fastapi.responses import Response, HTMLResponse
import uvicorn
from prometheus_client import Histogram

from .metrics import setup_metrics


# Simple brand guard function
def allowed(brand: str) -> bool:
    """Check if brand is allowed"""
    allowed_brands = {
        "betrivers",
        "kambi",
        "betparx",
        "sugarhouse",
        "fanduel",
        "draftkings",
        "betmgm",
        "caesars",
        "pointsbet",
        "barstool",
        "bovada",
    }
    return brand.lower() in allowed_brands


# Load brand aliases
BRAND_ALIASES: Dict[str, str] = {}
try:
    with open("config/brand_alias.yml", "r") as f:
        config = yaml.safe_load(f)
        if config and "aliases" in config:
            BRAND_ALIASES = config["aliases"]
            print(f"Loaded brand aliases: {BRAND_ALIASES}")
except Exception as e:
    print(f"Could not load brand aliases: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    app.state.redis = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))

    db_dsn = os.getenv("DATABASE_URL", "postgresql://odds:odds@store:5432/oddsfeed")
    try:
        # Use connection pool instead of single connection
        from psycopg_pool import AsyncConnectionPool

        app.state.db_pool = AsyncConnectionPool(
            db_dsn, min_size=2, max_size=10, timeout=5.0, max_idle=300.0
        )
        await app.state.db_pool.open()
        app.state.db_conn = None  # Backward compatibility
    except ImportError:
        # Fallback to single connection if pool not available
        try:
            app.state.db_conn = await psycopg.AsyncConnection.connect(db_dsn)
            app.state.db_pool = None
        except Exception as e:
            print(f"Database connection failed: {e}")
            app.state.db_conn = None
            app.state.db_pool = None
    except Exception as e:
        print(f"Database pool creation failed: {e}")
        app.state.db_pool = None
        app.state.db_conn = None

    yield

    if getattr(app.state, "db_pool", None):
        await app.state.db_pool.close()
    elif getattr(app.state, "db_conn", None):
        await app.state.db_conn.close()
    await app.state.redis.close()


app = FastAPI(title="OddsFeed API", version="0.1.0", lifespan=lifespan)

# Add specific /odds latency metric
odds_request_seconds = Histogram(
    "odds_request_seconds", "Time taken to process /odds requests"
)

setup_metrics(app)


@app.get("/debug/brand_counts")
async def debug_brand_counts(minutes: int = 60, brand: Optional[str] = None):
    """Simple brand counts endpoint to prove database connectivity"""
    conn = getattr(app.state, "db_conn", None)
    if not conn:
        return {"error": "Database not available"}

    # Store original brand and apply alias
    original_brand = brand
    if brand in BRAND_ALIASES:
        brand = BRAND_ALIASES[brand]

    try:
        sql = """
        SELECT COUNT(*) AS count
        FROM odds o
        JOIN events e ON e.id = o.event_id
        WHERE o.ts >= now() - (%s || ' minutes')::interval
          AND (%s::text IS NULL OR COALESCE(e.brand, '') = %s)
        """

        async with conn.cursor() as cur:
            await cur.execute(sql, (minutes, brand, brand))
            result = await cur.fetchone()
            count = result[0] if result else 0

        response = {
            "brand": original_brand or "ALL",
            "minutes": minutes,
            "count": count,
        }
        if original_brand and original_brand != brand:
            response["source_book"] = brand
        return response
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug/events_count")
async def debug_events_count(minutes: int = 15, brand: Optional[str] = None):
    """Simple events count endpoint using events.created_at"""
    conn = getattr(app.state, "db_conn", None)
    if not conn:
        return {"error": "Database not available"}

    # Store original brand and apply alias
    original_brand = brand
    if brand in BRAND_ALIASES:
        brand = BRAND_ALIASES[brand]

    try:
        sql = """
        WITH snap AS (
          SELECT now() - (%s || ' minutes')::interval AS cutoff
        )
        SELECT COUNT(DISTINCT e.id) AS count
        FROM events e, snap s
        WHERE e.created_at >= s.cutoff
          AND (%s::text IS NULL OR e.brand = %s)
        """

        async with conn.cursor() as cur:
            await cur.execute(sql, (minutes, brand, brand))
            result = await cur.fetchone()
            count = result[0] if result else 0

        response = {
            "brand": original_brand or "ALL",
            "minutes": minutes,
            "count": count,
        }
        if original_brand and original_brand != brand:
            response["source_book"] = brand
        return response
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug/events_ids")
async def debug_events_ids(
    minutes: int = 15, brand: Optional[str] = None, limit: int = 10
):
    """Simple events IDs endpoint using events.created_at"""
    conn = getattr(app.state, "db_conn", None)
    if not conn:
        return {"error": "Database not available"}

    try:
        sql = """
        WITH snap AS (
          SELECT now() - (%s || ' minutes')::interval AS cutoff
        )
        SELECT e.id, e.brand, e.league, e.home, e.away, e.created_at
        FROM events e, snap s
        WHERE e.created_at >= s.cutoff
          AND (%s::text IS NULL OR e.brand = %s)
        ORDER BY e.created_at DESC
        LIMIT %s
        """

        async with conn.cursor() as cur:
            await cur.execute(sql, (minutes, brand, brand, limit))
            results = await cur.fetchall()

        events = []
        for row in results:
            events.append(
                {
                    "id": row[0],
                    "brand": row[1],
                    "league": row[2],
                    "home": row[3],
                    "away": row[4],
                    "created_at": str(row[5]),
                }
            )

        return {
            "brand": brand or "ALL",
            "minutes": minutes,
            "count": len(events),
            "events": events,
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug/brand_table")
async def debug_brand_table(minutes: int = 60):
    """Simple brand breakdown table to prove database connectivity"""
    conn = getattr(app.state, "db_conn", None)
    if not conn:
        return {"error": "Database not available"}

    try:
        sql = """
        SELECT
            COALESCE(e.brand, 'unknown') AS brand,
            COUNT(*) AS count
        FROM odds o
        JOIN events e ON e.id = o.event_id
        WHERE o.ts >= now() - (%s || ' minutes')::interval
        GROUP BY COALESCE(e.brand, 'unknown')
        ORDER BY count DESC
        """

        async with conn.cursor() as cur:
            await cur.execute(sql, (minutes,))
            results = await cur.fetchall()

        brands = {}
        total = 0
        for row in results:
            brand_name, count = row
            brands[brand_name] = count
            total += count

        return {"minutes": minutes, "total_rows": total, "brands": brands}
    except Exception as e:
        return {"error": str(e)}


def format_odds_pretty(odds_rows):
    """Format odds rows for pretty output."""
    if not odds_rows:
        return []

    pretty_odds = []
    for odd in odds_rows:
        market = odd.get("market")
        if not market:
            continue

        pretty_odd = {}

        # Scale line/total by /1000.0 if not null (Kambi scaling)
        line = odd.get("line")
        if line is not None:
            pretty_odd["line"] = round(line / 1000.0, 1) if line != 0 else 0.0

        total = odd.get("total")
        if total is not None:
            pretty_odd["total"] = round(total / 1000.0, 1) if total != 0 else 0.0

        # Map prices based on market type
        if market == "h2h":
            if odd.get("price_home") is not None:
                pretty_odd["home"] = odd["price_home"]
            if odd.get("price_away") is not None:
                pretty_odd["away"] = odd["price_away"]

        elif market == "spreads":
            if "line" in pretty_odd:  # Only include if we have a line
                if odd.get("price_home") is not None:
                    pretty_odd["home"] = odd["price_home"]
                if odd.get("price_away") is not None:
                    pretty_odd["away"] = odd["price_away"]

        elif market == "totals":
            if "total" in pretty_odd:  # Only include if we have a total
                if odd.get("price_over") is not None:
                    pretty_odd["over"] = odd["price_over"]
                if odd.get("price_under") is not None:
                    pretty_odd["under"] = odd["price_under"]

        # Add market and timestamp
        pretty_odd["market"] = market
        if odd.get("ts"):
            pretty_odd["ts"] = odd["ts"]

        # Only add if we have at least one price
        price_fields = ["home", "away", "over", "under"]
        if any(field in pretty_odd for field in price_fields):
            pretty_odds.append(pretty_odd)

    return pretty_odds


@app.get("/health")
async def health_check():
    # CANARY LOG to prove new build deployment
    print("CANARY_BUILD api:1756064600")

    try:
        await app.state.redis.ping()
        redis_status = "ok"
    except Exception:
        redis_status = "error"

    db_status = "error"
    conn = getattr(app.state, "db_conn", None)

    # Try to (re)establish the connection if needed
    if conn is None or getattr(conn, "closed", False):
        try:
            app.state.db_conn = await psycopg.AsyncConnection.connect(
                os.getenv("DATABASE_URL", "postgresql://odds:odds@store:5432/oddsfeed")
            )
            conn = app.state.db_conn
        except Exception as e:
            print(f"DB reconnect failed: {e}")
            conn = None

    if conn is not None:
        try:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                await cur.fetchone()
            db_status = "ok"
        except Exception as e:
            print(f"DB health check query failed: {e}")
            db_status = "error"

    overall_status = "ok" if redis_status == "ok" and db_status == "ok" else "degraded"

    return {
        "status": overall_status,
        "components": {"redis": redis_status, "database": db_status},
    }


@app.get("/odds")
async def get_odds(
    minutes: int = 10,
    limit: int = 20,
    book: Optional[str] = None,
    league: Optional[str] = None,
    market: Optional[str] = None,
    brand: Optional[str] = None,
    sport: Optional[str] = None,
    include_empty: bool = False,
    format: str = "raw",
    last: bool = False,
    fill: bool = False,
):
    """Get recent odds data from the database"""
    # Store original brand for response
    original_brand = brand

    # Apply brand alias mapping
    if brand in BRAND_ALIASES:
        mapped_brand = BRAND_ALIASES[brand]
        print(f"Brand alias: {brand} -> {mapped_brand}")
        brand = mapped_brand

    if brand and not allowed(brand):
        return {"count": 0, "events": []}

    conn = getattr(app.state, "db_conn", None)
    if not conn:
        return {"error": "Database not available"}

    # Start timer for latency metric
    start_time = time.time()

    # Use simple path when API_USE_SIMPLE=1 env var is set
    use_simple = os.getenv("API_USE_SIMPLE", "0") == "1"

    if use_simple:
        try:
            # Single-cutoff atomic query (matches DB ground truth exactly)
            sql = """
            WITH snap AS (
              SELECT now() - (%s || ' minutes')::interval AS cutoff
            )
            SELECT COUNT(DISTINCT e.id) as count
            FROM events e, snap s
            WHERE e.created_at >= s.cutoff
              AND (%s::text IS NULL OR e.brand = %s)
              AND (%s::text IS NULL OR e.league = %s)
              AND (%s::text IS NULL OR e.sport = %s)
              AND (%s::text != 'kambi' OR COALESCE(e.brand, '') != 'betparx')
            """

            async with conn.cursor() as cur:
                await cur.execute(
                    sql, (minutes, brand, brand, league, league, sport, sport, book)
                )
                result = await cur.fetchone()
                count = result[0] if result else 0

            # Record latency metric
            duration = time.time() - start_time
            odds_request_seconds.observe(duration)

            return {
                "status": "ok",
                "count": count,
                "events": [],  # Simple path doesn't return full event data
                "debug": {
                    "simple_path": True,
                    "brand": brand,
                    "sport": sport,
                    "league": league,
                },
            }
        except Exception as e:
            duration = time.time() - start_time
            odds_request_seconds.observe(duration)
            return {"error": f"Simple path failed: {str(e)}"}

    try:
        # Exclude betparx from Kambi book queries (betparx is non-Kambi)
        if book == "kambi" and brand is None:
            # Auto-exclude betparx when querying Kambi book without specific brand filter
            extra_brand_filter = "AND COALESCE(e.brand, '') != 'betparx'"
        else:
            extra_brand_filter = ""

        # Build the filter condition for non-empty rows
        empty_filter = ""
        if not include_empty:
            empty_filter = """
                  AND (
                    COALESCE(NULLIF(price_home, 0), NULL) IS NOT NULL OR
                    COALESCE(NULLIF(price_away, 0), NULL) IS NOT NULL OR
                    COALESCE(NULLIF(price_over, 0), NULL) IS NOT NULL OR
                    COALESCE(NULLIF(price_under, 0), NULL) IS NOT NULL
                  )"""

        # Map brand to correct book for odds filtering
        # When brand=betrivers is requested, we need odds.book='betrivers'
        # When brand=kambi is requested, we need odds.book='kambi'
        odds_book = brand if brand else book
        if book == "kambi" and brand == "betrivers":
            odds_book = "betrivers"  # Override: query betrivers odds specifically

        # Build different query based on last and fill parameters
        if last and fill:
            # For last=true AND fill=true, use side-specific queries to carry forward latest non-null values
            brand_filter = (
                "AND (%(brand)s::text IS NULL OR COALESCE(e.brand, '') = %(brand)s)"
                if brand
                else ""
            )
            # Add betparx exclusion for Kambi queries
            if extra_brand_filter:
                brand_filter += f" {extra_brand_filter}"
            sport_filter = (
                "AND (%(sport)s::text IS NULL OR e.sport = %(sport)s)" if sport else ""
            )

            sql = textwrap.dedent(
                f"""
                WITH
                -- H2H and SPREADS (home & away) - key by (event_id, market, line)
                latest_home AS (
                    SELECT DISTINCT ON (o.event_id, o.market, o.line)
                      o.event_id, o.market, o.line, o.ts AS ts_home, o.price_home
                    FROM odds o
                    JOIN events e ON e.id = o.event_id
                    WHERE o.book = COALESCE(%(odds_book)s, o.book)
                      AND o.ts >= now() - (%(minutes)s::int || ' minutes')::interval
                      AND o.price_home IS NOT NULL
                      AND (%(market)s::text IS NULL OR o.market = %(market)s)
                      AND (%(league)s::text IS NULL OR e.league = %(league)s)
                      AND e.id IN (SELECT id FROM event_activity WHERE activity_at >= now() - (%(minutes)s::int || ' minutes')::interval)
                      {brand_filter}
                      {sport_filter}
                    ORDER BY o.event_id, o.market, o.line, o.ts DESC
                ),
                latest_away AS (
                    SELECT DISTINCT ON (o.event_id, o.market, o.line)
                      o.event_id, o.market, o.line, o.ts AS ts_away, o.price_away
                    FROM odds o
                    JOIN events e ON e.id = o.event_id
                    WHERE o.book = COALESCE(%(odds_book)s, o.book)
                      AND o.ts >= now() - (%(minutes)s::int || ' minutes')::interval
                      AND o.price_away IS NOT NULL
                      AND (%(market)s::text IS NULL OR o.market = %(market)s)
                      AND (%(league)s::text IS NULL OR e.league = %(league)s)
                      AND e.id IN (SELECT id FROM event_activity WHERE activity_at >= now() - (%(minutes)s::int || ' minutes')::interval)
                      {brand_filter}
                      {sport_filter}
                    ORDER BY o.event_id, o.market, o.line, o.ts DESC
                ),
                -- TOTALS (over & under) - key by (event_id, market, total)
                latest_over AS (
                    SELECT DISTINCT ON (o.event_id, o.market, o.total)
                      o.event_id, o.market, o.total, o.ts AS ts_over, o.price_over
                    FROM odds o
                    JOIN events e ON e.id = o.event_id
                    WHERE o.book = COALESCE(%(odds_book)s, o.book)
                      AND o.ts >= now() - (%(minutes)s::int || ' minutes')::interval
                      AND o.price_over IS NOT NULL
                      AND (%(market)s::text IS NULL OR o.market = %(market)s)
                      AND (%(league)s::text IS NULL OR e.league = %(league)s)
                      AND e.id IN (SELECT id FROM event_activity WHERE activity_at >= now() - (%(minutes)s::int || ' minutes')::interval)
                      {brand_filter}
                      {sport_filter}
                    ORDER BY o.event_id, o.market, o.total, o.ts DESC
                ),
                latest_under AS (
                    SELECT DISTINCT ON (o.event_id, o.market, o.total)
                      o.event_id, o.market, o.total, o.ts AS ts_under, o.price_under
                    FROM odds o
                    JOIN events e ON e.id = o.event_id
                    WHERE o.book = COALESCE(%(odds_book)s, o.book)
                      AND o.ts >= now() - (%(minutes)s::int || ' minutes')::interval
                      AND o.price_under IS NOT NULL
                      AND (%(market)s::text IS NULL OR o.market = %(market)s)
                      AND (%(league)s::text IS NULL OR e.league = %(league)s)
                      AND e.id IN (SELECT id FROM event_activity WHERE activity_at >= now() - (%(minutes)s::int || ' minutes')::interval)
                      {brand_filter}
                      {sport_filter}
                    ORDER BY o.event_id, o.market, o.total, o.ts DESC
                ),
                -- Combine h2h/spreads results
                home_away_combined AS (
                    SELECT
                      COALESCE(h.event_id, a.event_id) AS event_id,
                      COALESCE(h.market, a.market) AS market,
                      COALESCE(h.line, a.line) AS line,
                      NULL::integer AS total,
                      GREATEST(COALESCE(h.ts_home, '1970-01-01'::timestamp), COALESCE(a.ts_away, '1970-01-01'::timestamp)) AS ts,
                      h.price_home, a.price_away, NULL::integer AS price_over, NULL::integer AS price_under
                    FROM latest_home h
                    FULL JOIN latest_away a
                      ON h.event_id = a.event_id AND h.market = a.market AND h.line = a.line
                    WHERE COALESCE(h.market, a.market) IN ('h2h','spreads')
                ),
                -- Combine totals results
                over_under_combined AS (
                    SELECT
                      COALESCE(o.event_id, u.event_id) AS event_id,
                      COALESCE(o.market, u.market) AS market,
                      NULL::integer AS line,
                      COALESCE(o.total, u.total) AS total,
                      GREATEST(COALESCE(o.ts_over, '1970-01-01'::timestamp), COALESCE(u.ts_under, '1970-01-01'::timestamp)) AS ts,
                      NULL::integer AS price_home, NULL::integer AS price_away, o.price_over, u.price_under
                    FROM latest_over o
                    FULL JOIN latest_under u
                      ON o.event_id = u.event_id AND o.market = u.market AND o.total = u.total
                    WHERE COALESCE(o.market, u.market) = 'totals'
                ),
                -- Union all results
                all_odds AS (
                    SELECT * FROM home_away_combined
                    UNION ALL
                    SELECT * FROM over_under_combined
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
                    FROM all_odds r
                    GROUP BY r.event_id
                )
                SELECT
                  a.event_id,
                  ea.league,
                  COALESCE(ea.home, NULL) AS home,
                  COALESCE(ea.away, NULL) AS away,
                  COALESCE(ea.sport, NULL) AS sport,
                  a.odds_rows
                FROM agg a
                LEFT JOIN events e ON e.id = a.event_id
                WHERE (%(league)s::text IS NULL OR e.league = %(league)s)
                  {brand_filter}
                  {sport_filter}
                ORDER BY a.event_id DESC
                LIMIT %(limit)s
            """
            )
        elif last:
            # For last=true, use window functions to get only latest per market/line/side
            sql = textwrap.dedent(
                f"""
                WITH recent AS (
                    SELECT *
                    FROM odds
                    WHERE ts > now() - (%(minutes)s::int || ' minutes')::interval
                      AND (%(odds_book)s::text IS NULL OR book = %(odds_book)s)
                      AND (%(market)s::text IS NULL OR market = %(market)s)
                      {empty_filter}
                ),
                latest AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY event_id, market,
                                           COALESCE(line, 0), COALESCE(total, 0),
                                           CASE
                                               WHEN price_home IS NOT NULL THEN 'home'
                                               WHEN price_away IS NOT NULL THEN 'away'
                                               WHEN price_over IS NOT NULL THEN 'over'
                                               WHEN price_under IS NOT NULL THEN 'under'
                                           END
                               ORDER BY ts DESC
                           ) as rn
                    FROM recent
                ),
                filtered AS (
                    SELECT * FROM latest WHERE rn = 1
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
                    FROM filtered r
                    GROUP BY r.event_id
                )
                SELECT
                  a.event_id,
                  ea.league,
                  COALESCE(ea.home, NULL) AS home,
                  COALESCE(ea.away, NULL) AS away,
                  COALESCE(ea.sport, NULL) AS sport,
                  a.odds_rows
                FROM agg a
                LEFT JOIN event_activity ea ON ea.id = a.event_id
                WHERE (%(league)s::text IS NULL OR ea.league = %(league)s)
                  AND (%(brand)s::text IS NULL OR COALESCE(ea.brand, '') = %(brand)s)
                  AND ea.activity_at >= now() - (%(minutes)s::int || ' minutes')::interval
                  {extra_brand_filter.replace('e.brand', "COALESCE(ea.brand, '')") if extra_brand_filter else ''}
                ORDER BY a.event_id DESC
                LIMIT %(limit)s
            """
            )
        else:
            # Original query for all data
            sql = textwrap.dedent(
                f"""
                WITH recent AS (
                    SELECT *
                    FROM odds
                    WHERE ts > now() - (%(minutes)s::int || ' minutes')::interval
                      AND (%(odds_book)s::text IS NULL OR book = %(odds_book)s)
                      AND (%(market)s::text IS NULL OR market = %(market)s)
                      {empty_filter}
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
                  ea.league,
                  COALESCE(ea.home, NULL) AS home,
                  COALESCE(ea.away, NULL) AS away,
                  COALESCE(ea.sport, NULL) AS sport,
                  a.odds_rows
                FROM agg a
                LEFT JOIN event_activity ea ON ea.id = a.event_id
                WHERE (%(league)s::text IS NULL OR ea.league = %(league)s)
                  AND (%(brand)s::text IS NULL OR COALESCE(ea.brand, '') = %(brand)s)
                  AND ea.activity_at >= now() - (%(minutes)s::int || ' minutes')::interval
                  {extra_brand_filter.replace('e.brand', "COALESCE(ea.brand, '')") if extra_brand_filter else ''}
                ORDER BY a.event_id DESC
                LIMIT %(limit)s
            """
            )

        params = {
            "minutes": minutes,
            "book": book,
            "odds_book": odds_book,
            "league": league,
            "market": market,
            "brand": brand,
            "sport": sport,
            "limit": limit,
        }

        # Debug logging
        print(f"DEBUG: brand={brand}, sport={sport}, league={league}")
        print(f"DEBUG: params={params}")

        async with conn.cursor() as cur:
            print(f"DEBUG: Executing SQL with params: {params}")
            await cur.execute(sql, params)
            rows = await cur.fetchall()
            print(f"DEBUG: Query returned {len(rows)} rows")
            if rows:
                print(f"DEBUG: First row: {rows[0]}")

            # Shape response based on format parameter
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

                # Apply pretty formatting if requested
                if format == "pretty":
                    event_data["odds"] = format_odds_pretty(r[5])

                events.append(event_data)

            print(f"DEBUG: Shaped {len(events)} events")

            # Record latency metric
            duration = time.time() - start_time
            odds_request_seconds.observe(duration)

            # Apply brand alias to response if it was mapped
            response_data = {
                "status": "ok",
                "count": len(events),
                "events": events,
                "debug": {
                    "brand": original_brand if original_brand else brand,
                    "sport": sport,
                    "league": league,
                    "params": params,
                },
            }

            # Add source_book if brand was aliased
            if original_brand and original_brand != brand:
                response_data["source_book"] = brand
                response_data["brand"] = original_brand

            return response_data

    except Exception as e:
        # Record latency metric even on failure
        duration = time.time() - start_time
        odds_request_seconds.observe(duration)
        return {"error": f"Failed to fetch odds: {str(e)}"}


@app.get("/odds/timeline")
async def get_odds_timeline(
    event_id: str,
    book: str = "kambi",
    minutes: int = 60,
    every: int = 60,
    market: Optional[str] = None,
    line_or_total: Optional[int] = None,
):
    """Get timeline of odds data with carry-forward for missing values"""
    conn = getattr(app.state, "db_conn", None)
    if not conn:
        return {"error": "Database not available"}

    # Start timer for latency metric
    start_time = time.time()

    try:
        # Fixed query with proper decimal handling and Kambi scaling
        sql = textwrap.dedent(
            """
            select
              to_char(o.ts,'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') as ts_iso,
              o.event_id,
              o.market,
              case when o.line  is null then null else (o.line  / 1000.0)::double precision end as line,
              case when o.total is null then null else (o.total / 1000.0)::double precision end as total,
              (o.price_home )::double precision as price_home,
              (o.price_away )::double precision as price_away,
              (o.price_over )::double precision as price_over,
              (o.price_under)::double precision as price_under
            from odds o
            left join events e on e.id = o.event_id
            where o.book = %s
              and o.ts > now() - %s * INTERVAL '1 minute'
              and (
                coalesce(o.price_home,0) <> 0 or
                coalesce(o.price_away,0) <> 0 or
                coalesce(o.price_over,0) <> 0 or
                coalesce(o.price_under,0) <> 0
              )
              and o.event_id = %s
            order by o.ts asc
            """
        )

        async with conn.cursor() as cur:
            await cur.execute(sql, (book, minutes, event_id))
            rows = await cur.fetchall()

            # Format timeline data - columns are now pre-scaled and cast to double precision
            timeline = []
            for r in rows:
                entry = {
                    "ts": r[0],  # Already formatted as ISO string
                    "event_id": r[1],
                    "market": r[2],
                    "line": r[
                        3
                    ],  # Already scaled by /1000.0 and cast to double precision
                    "total": r[
                        4
                    ],  # Already scaled by /1000.0 and cast to double precision
                    "price_home": r[5],  # Already cast to double precision
                    "price_away": r[6],  # Already cast to double precision
                    "price_over": r[7],  # Already cast to double precision
                    "price_under": r[8],  # Already cast to double precision
                }
                timeline.append(entry)

            # Record latency metric
            duration = time.time() - start_time
            odds_request_seconds.observe(duration)

            return {
                "status": "ok",
                "event_id": event_id,
                "book": book,
                "market": market,
                "line_or_total": line_or_total,
                "timeline": timeline,
            }

    except Exception as e:
        # Record latency metric even on failure
        duration = time.time() - start_time
        odds_request_seconds.observe(duration)
        return {"error": f"Failed to fetch timeline: {str(e)}"}


@app.get("/odds/latest")
async def get_odds_latest(
    brand: str = "betrivers",
    sport: Optional[str] = None,
    league: Optional[str] = None,
    limit: int = 50,
):
    """Get latest odds for each event"""
    conn = getattr(app.state, "db_conn", None)
    if not conn:
        return {"error": "Database not available"}

    start_time = time.time()

    try:
        sql = """
        WITH latest_odds AS (
            SELECT
                o.event_id,
                o.market,
                o.line,
                o.price_home,
                o.price_away,
                o.price_over,
                o.price_under,
                o.total,
                o.ts,
                e.league,
                e.home,
                e.away,
                e.sport,
                e.brand,
                ROW_NUMBER() OVER (
                    PARTITION BY o.event_id, o.market
                    ORDER BY o.ts DESC
                ) as rn
            FROM odds o
            JOIN events e ON o.event_id = e.id
            WHERE e.brand = %s
              AND (%s::text IS NULL OR e.sport = %s)
              AND (%s::text IS NULL OR e.league = %s)
              AND o.ts >= NOW() - INTERVAL '1 hour'
        )
        SELECT
            event_id,
            market,
            line,
            price_home,
            price_away,
            price_over,
            price_under,
            total,
            ts,
            league,
            home,
            away,
            sport,
            brand
        FROM latest_odds
        WHERE rn = 1
        ORDER BY ts DESC
        LIMIT %s
        """

        async with conn.cursor() as cur:
            await cur.execute(sql, (brand, sport, sport, league, league, limit))
            rows = await cur.fetchall()

        # Convert to dict format
        results = []
        for row in rows:
            results.append(
                {
                    "event_id": row[0],
                    "market": row[1],
                    "line": row[2],
                    "price_home": float(row[3]) if row[3] else None,
                    "price_away": float(row[4]) if row[4] else None,
                    "price_over": float(row[5]) if row[5] else None,
                    "price_under": float(row[6]) if row[6] else None,
                    "total": float(row[7]) if row[7] else None,
                    "timestamp": row[8].isoformat() if row[8] else None,
                    "league": row[9],
                    "home": row[10],
                    "away": row[11],
                    "sport": row[12],
                    "brand": row[13],
                }
            )

        duration = time.time() - start_time
        odds_request_seconds.observe(duration)

        return {
            "count": len(results),
            "odds": results,
            "filters": {
                "brand": brand,
                "sport": sport,
                "league": league,
                "limit": limit,
            },
            "latency_ms": round(duration * 1000, 2),
        }

    except Exception as e:
        return {"error": f"Failed to fetch latest odds: {str(e)}"}


@app.get("/odds/history")
async def get_odds_history(
    event_id: int,
    market: str = "h2h",
    selection: Optional[str] = None,
    hours: int = 24,
    limit: int = 100,
):
    """Get odds history (tick data) for a specific event"""
    conn = getattr(app.state, "db_conn", None)
    if not conn:
        return {"error": "Database not available"}

    start_time = time.time()

    try:
        if selection:
            # Get history for specific selection
            sql = """
            SELECT
                event_id,
                market,
                selection,
                odds_decimal,
                brand,
                created_at
            FROM odds_ticks
            WHERE event_id = %s
              AND market = %s
              AND selection = %s
              AND created_at >= NOW() - (%s || ' hours')::INTERVAL
            ORDER BY created_at DESC
            LIMIT %s
            """
            params = (event_id, market, selection, hours, limit)
        else:
            # Get history for all selections in the market
            sql = """
            SELECT
                event_id,
                market,
                selection,
                odds_decimal,
                brand,
                created_at
            FROM odds_ticks
            WHERE event_id = %s
              AND market = %s
              AND created_at >= NOW() - (%s || ' hours')::INTERVAL
            ORDER BY created_at DESC, selection
            LIMIT %s
            """
            params = (event_id, market, hours, limit)

        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

        # Convert to dict format
        results = []
        for row in rows:
            results.append(
                {
                    "event_id": row[0],
                    "market": row[1],
                    "selection": row[2],
                    "odds_decimal": float(row[3]),
                    "brand": row[4],
                    "timestamp": row[5].isoformat(),
                }
            )

        duration = time.time() - start_time
        odds_request_seconds.observe(duration)

        return {
            "count": len(results),
            "ticks": results,
            "filters": {
                "event_id": event_id,
                "market": market,
                "selection": selection,
                "hours": hours,
                "limit": limit,
            },
            "latency_ms": round(duration * 1000, 2),
        }

    except Exception as e:
        return {"error": f"Failed to fetch odds history: {str(e)}"}


@app.get("/demo/kambi", response_class=HTMLResponse)
async def demo_kambi(
    minutes: int = 60,
    limit: int = 100,
    brand: Optional[str] = None,
    sport: Optional[str] = None,
):
    """Demo page showing latest Kambi odds in HTML table format."""
    try:
        # Get current timestamp for "Last updated" display
        from datetime import datetime

        last_updated = datetime.now().strftime("%H:%M:%S")

        # Get latest Kambi odds using same logic as /odds endpoint with query params
        odds_data = await get_odds(
            minutes=minutes,
            limit=limit,
            book="kambi",
            brand=brand,
            sport=sport,
            format="pretty",
            last=True,
            fill=True,
        )

        if not odds_data or odds_data.get("count", 0) == 0:
            return HTMLResponse(
                content=f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Kambi Odds Demo</title>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 20px; }}
                        .no-data {{ text-align: center; color: #666; margin-top: 50px; }}
                        .last-updated {{ color: #888; font-size: 14px; margin-bottom: 20px; }}
                    </style>
                    <script>
                        setTimeout(function() {{ location.reload(); }}, 2000);
                    </script>
                </head>
                <body>
                    <h1>Kambi Odds Demo</h1>
                    <div class="last-updated">Last updated: {last_updated}</div>
                    <div class="no-data">No odds yet.</div>
                </body>
                </html>
            """
            )

        # Build HTML table
        html_rows = []
        for event in odds_data.get("events", []):
            event_id = event.get("event_id", "")
            sport = event.get("sport", "")
            league = event.get("league", "")
            home = event.get("home", "")
            away = event.get("away", "")
            event_display = f"{home} vs {away}" if home and away else event_id

            for odd in event.get("odds", []):
                market = odd.get("market", "")
                ts = odd.get("ts", "")

                # Format market-specific columns
                if market == "h2h":
                    line_total = "-"
                    prices = (
                        f"Home: {odd.get('home', '-')}, Away: {odd.get('away', '-')}"
                    )
                elif market == "spreads":
                    line_total = str(odd.get("line", "-"))
                    prices = f"Line: {line_total}, Home: {odd.get('home', '-')}, Away: {odd.get('away', '-')}"
                elif market == "totals":
                    line_total = str(odd.get("total", "-"))
                    prices = f"Total: {line_total}, Over: {odd.get('over', '-')}, Under: {odd.get('under', '-')}"
                else:
                    line_total = "-"
                    prices = str(odd)

                html_rows.append(
                    f"""
                    <tr>
                        <td>{ts[:19] if ts else '-'}</td>
                        <td>{sport}</td>
                        <td>{league}</td>
                        <td>{event_display}</td>
                        <td>{market}</td>
                        <td>{line_total}</td>
                        <td>{prices}</td>
                    </tr>
                """
                )

        table_content = "".join(html_rows)

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Kambi Odds Demo</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                .header {{ margin-bottom: 20px; }}
                .count {{ color: #666; font-size: 14px; }}
                .last-updated {{ color: #888; font-size: 14px; margin-bottom: 10px; }}
                .filters {{ margin: 10px 0; }}
                .filters label {{ margin-right: 15px; }}
                .filters select, .filters input {{ margin-left: 5px; }}
            </style>
            <script>
                setTimeout(function() {{ location.reload(); }}, 2000);

                function updateFilters() {{
                    const brand = document.getElementById('brandFilter').value;
                    const sport = document.getElementById('sportFilter').value;
                    const minutes = document.getElementById('minutesInput').value;
                    const limit = document.getElementById('limitInput').value;

                    const params = new URLSearchParams();
                    if (brand) params.append('brand', brand);
                    if (sport) params.append('sport', sport);
                    params.append('minutes', minutes);
                    params.append('limit', limit);

                    window.location.href = '/demo/kambi?' + params.toString();
                }}
            </script>
        </head>
        <body>
            <div class="header">
                <h1>Kambi Odds Demo</h1>
                <div class="last-updated">Last updated: {last_updated}</div>
                <div class="filters">
                    <label>Brand:
                        <select onchange="updateFilters()" id="brandFilter">
                            <option value="">All</option>
                            <option value="betrivers" {"selected" if brand == "betrivers" else ""}>BetRivers</option>
                            <option value="sugarhouse" {"selected" if brand == "sugarhouse" else ""}>SugarHouse</option>
                            <option value="kambi" {"selected" if brand == "kambi" else ""}>Kambi</option>
                        </select>
                    </label>
                    <label>Sport:
                        <select onchange="updateFilters()" id="sportFilter">
                            <option value="">All</option>
                            <option value="american_football" {"selected" if sport == "american_football" else ""}>Football</option>
                            <option value="basketball" {"selected" if sport == "basketball" else ""}>Basketball</option>
                            <option value="baseball" {"selected" if sport == "baseball" else ""}>Baseball</option>
                            <option value="hockey" {"selected" if sport == "hockey" else ""}>Hockey</option>
                        </select>
                    </label>
                    <label>Minutes: <input type="number" id="minutesInput" value="{minutes}" onchange="updateFilters()" min="1" max="1440"></label>
                    <label>Limit: <input type="number" id="limitInput" value="{limit}" onchange="updateFilters()" min="1" max="1000"></label>
                </div>
                <p class="count">Showing {odds_data.get("count", 0)} events with latest odds (filters: brand={brand or "all"}, sport={sport or "all"}, minutes={minutes}, limit={limit})</p>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Time</th>
                        <th>Sport</th>
                        <th>League</th>
                        <th>Event</th>
                        <th>Market</th>
                        <th>Line/Total</th>
                        <th>Prices</th>
                    </tr>
                </thead>
                <tbody>
                    {table_content}
                </tbody>
            </table>
        </body>
        </html>
        """

        return HTMLResponse(content=html_content)

    except Exception as e:
        # Defensive error handling
        from datetime import datetime

        error_time = datetime.now().strftime("%H:%M:%S")
        return HTMLResponse(
            content=f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Kambi Odds Demo - Error</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    .error {{ color: red; text-align: center; margin-top: 50px; }}
                    .last-updated {{ color: #888; font-size: 14px; margin-bottom: 20px; }}
                </style>
                <script>
                    setTimeout(function() {{ location.reload(); }}, 2000);
                </script>
            </head>
            <body>
                <h1>Kambi Odds Demo</h1>
                <div class="last-updated">Last updated: {error_time}</div>
                <div class="error">Error loading odds: {str(e)}</div>
            </body>
            </html>
        """
        )


@app.get("/metrics")
async def metrics():
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

    data = generate_latest()  # bytes
    # Use media_type without charset so Starlette appends a single charset parameter
    media_type = CONTENT_TYPE_LATEST.split("; charset=")[0]
    return Response(content=data, media_type=media_type)


if __name__ == "__main__":
    uvicorn.run("api.main:app", host="0.0.0.0", port=8080, reload=False)

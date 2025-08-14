import os
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional
import textwrap

import redis.asyncio as redis
import psycopg
from fastapi import FastAPI
from fastapi.responses import Response
import uvicorn
from prometheus_client import Histogram

from .metrics import setup_metrics


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    app.state.redis = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))

    db_dsn = os.getenv("DATABASE_URL", "postgresql://odds:odds@store:5432/oddsfeed")
    try:
        app.state.db_conn = await psycopg.AsyncConnection.connect(db_dsn)
    except Exception as e:
        print(f"Database connection failed: {e}")
        app.state.db_conn = None

    yield

    if getattr(app.state, "db_conn", None):
        await app.state.db_conn.close()
    await app.state.redis.close()


app = FastAPI(title="OddsFeed API", version="0.1.0", lifespan=lifespan)

# Add specific /odds latency metric
odds_request_seconds = Histogram(
    "odds_request_seconds", "Time taken to process /odds requests"
)

setup_metrics(app)


@app.get("/health")
async def health_check():

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
):
    """Get recent odds data from the database"""
    conn = getattr(app.state, "db_conn", None)
    if not conn:
        return {"error": "Database not available"}

    # Start timer for latency metric
    start_time = time.time()

    try:
        sql = textwrap.dedent(
            """
            WITH recent AS (
                SELECT *
                FROM odds
                WHERE ts > now() - (%(minutes)s::int || ' minutes')::interval
                  AND (%(book)s::text IS NULL OR book = %(book)s)
                  AND (%(market)s::text IS NULL OR market = %(market)s)
            ),
            evs AS (
                SELECT DISTINCT event_id
                FROM recent
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
              a.odds_rows
            FROM agg a
            LEFT JOIN events e ON e.id = a.event_id
            WHERE (%(league)s::text IS NULL OR e.league = %(league)s)
            ORDER BY a.event_id DESC
            LIMIT %(limit)s
        """
        )

        params = {
            "minutes": minutes,
            "book": book,
            "league": league,
            "market": market,
            "limit": limit,
        }

        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            rows = await cur.fetchall()

            # Shape response similar to before
            events = []
            for r in rows:
                events.append(
                    {
                        "event_id": r[0],
                        "league": r[1],
                        "home": r[2],
                        "away": r[3],
                        "odds": r[4],
                    }
                )

            # Record latency metric
            duration = time.time() - start_time
            odds_request_seconds.observe(duration)

            return {"status": "ok", "count": len(events), "events": events}

    except Exception as e:
        # Record latency metric even on failure
        duration = time.time() - start_time
        odds_request_seconds.observe(duration)
        return {"error": f"Failed to fetch odds: {str(e)}"}


@app.get("/metrics")
async def metrics():
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

    data = generate_latest()  # bytes
    # Use media_type without charset so Starlette appends a single charset parameter
    media_type = CONTENT_TYPE_LATEST.split("; charset=")[0]
    return Response(content=data, media_type=media_type)


if __name__ == "__main__":
    uvicorn.run("api.main:app", host="0.0.0.0", port=8080, reload=False)

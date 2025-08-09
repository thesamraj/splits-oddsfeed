import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import redis.asyncio as redis
import psycopg
from fastapi import FastAPI, Query
from fastapi.responses import Response
import uvicorn

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
    league: Optional[str] = Query(None, description="Filter by league"),
    book: Optional[str] = Query(None, description="Filter by book"),
    market: Optional[str] = Query(None, description="Filter by market type"),
    limit: int = Query(100, description="Maximum number of records to return"),
):
    """Get recent odds data from the database"""
    conn = getattr(app.state, "db_conn", None)
    if not conn:
        return {"error": "Database not available"}

    try:
        # Build dynamic query with filters
        query = """
            SELECT
                e.id as event_id,
                e.league,
                e.start_time,
                e.home,
                e.away,
                e.sport,
                o.book,
                o.market,
                o.outcome_name,
                o.outcome_price,
                o.outcome_point,
                o.ts
            FROM events e
            JOIN odds o ON e.id = o.event_id
            WHERE o.ts >= NOW() - INTERVAL '10 minutes'
        """

        params = []
        if league:
            query += " AND e.league ILIKE %s"
            params.append(f"%{league}%")
        if book:
            query += " AND o.book = %s"
            params.append(book)
        if market:
            query += " AND o.market = %s"
            params.append(market)

        query += " ORDER BY o.ts DESC LIMIT %s"
        params.append(limit)

        async with conn.cursor() as cur:
            await cur.execute(query, params)
            rows = await cur.fetchall()

            # Group by event for cleaner response
            events_dict = {}
            for row in rows:
                event_id = row[0]
                if event_id not in events_dict:
                    events_dict[event_id] = {
                        "event_id": event_id,
                        "league": row[1],
                        "start_time": row[2].isoformat() if row[2] else None,
                        "home": row[3],
                        "away": row[4],
                        "sport": row[5],
                        "odds": [],
                    }

                events_dict[event_id]["odds"].append(
                    {
                        "book": row[6],
                        "market": row[7],
                        "outcome_name": row[8],
                        "outcome_price": float(row[9]) if row[9] else None,
                        "outcome_point": float(row[10]) if row[10] else None,
                        "timestamp": row[11].isoformat() if row[11] else None,
                    }
                )

            return {
                "events": list(events_dict.values()),
                "count": len(events_dict),
                "filters": {"league": league, "book": book, "market": market},
            }

    except Exception as e:
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

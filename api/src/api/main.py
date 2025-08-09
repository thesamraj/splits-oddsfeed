import os
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional
from datetime import datetime

import redis.asyncio as redis
import psycopg
from fastapi import FastAPI, Query
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
odds_request_seconds = Histogram("odds_request_seconds", "Time taken to process /odds requests")

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
    minutes: int = Query(10, description="Lookback period in minutes"),
    after: Optional[str] = Query(None, description="ISO timestamp cursor for pagination"),
):
    """Get recent odds data from the database"""
    conn = getattr(app.state, "db_conn", None)
    if not conn:
        return {"error": "Database not available"}

    # Start timer for latency metric
    start_time = time.time()

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
            WHERE o.ts >= NOW() - INTERVAL %(minutes)s MINUTES
        """

        params = {"minutes": minutes}

        # Add pagination cursor
        if after:
            try:
                after_dt = datetime.fromisoformat(after.replace("Z", "+00:00"))
                query += " AND o.ts > %(after)s"
                params["after"] = after_dt
            except ValueError:
                return {"error": "Invalid after timestamp format. Use ISO format."}

        if league:
            query += " AND e.league ILIKE %(league)s"
            params["league"] = f"%{league}%"
        if book:
            query += " AND o.book = %(book)s"
            params["book"] = book
        if market:
            query += " AND o.market = %(market)s"
            params["market"] = market

        query += " ORDER BY o.ts DESC LIMIT %(limit)s"
        params["limit"] = limit

        async with conn.cursor() as cur:
            await cur.execute(query, params)
            rows = await cur.fetchall()

            # Group by event for cleaner response
            events_dict = {}
            max_ts = None

            for row in rows:
                event_id = row[0]
                ts = row[11]  # timestamp column

                # Track max timestamp for next_cursor
                if max_ts is None or (ts and ts > max_ts):
                    max_ts = ts

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
                        "timestamp": ts.isoformat() if ts else None,
                    }
                )

            # Record latency metric
            duration = time.time() - start_time
            odds_request_seconds.observe(duration)

            result = {
                "events": list(events_dict.values()),
                "count": len(events_dict),
                "filters": {"league": league, "book": book, "market": market},
            }

            # Add next_cursor if we have data
            if max_ts and len(rows) == limit:
                result["next_cursor"] = max_ts.isoformat()

            return result

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

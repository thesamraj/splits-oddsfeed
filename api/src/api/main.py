import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import redis.asyncio as redis
import psycopg
from fastapi import FastAPI
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


@app.get("/metrics")
async def metrics():
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

    data = generate_latest()  # bytes
    # Use media_type without charset so Starlette appends a single charset parameter
    media_type = CONTENT_TYPE_LATEST.split("; charset=")[0]
    return Response(content=data, media_type=media_type)


if __name__ == "__main__":
    uvicorn.run("api.main:app", host="0.0.0.0", port=8080, reload=False)

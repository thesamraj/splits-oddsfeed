import os
import pytest
import redis
import psycopg
import httpx


def test_environment_variables():
    required_vars = ["DATABASE_URL", "REDIS_URL"]

    for var in required_vars:
        assert os.getenv(var) is not None, f"Environment variable {var} is not set"


@pytest.mark.asyncio
async def test_redis_connection():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    client = redis.from_url(redis_url)

    try:
        result = await client.ping()
        assert result is True
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_database_connection():
    db_url = os.getenv("DATABASE_URL", "postgresql://odds:odds@localhost:5432/oddsfeed")

    try:
        async with psycopg.AsyncConnection.connect(db_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                result = await cur.fetchone()
                assert result[0] == 1
    except Exception as e:
        pytest.fail(f"Database connection failed: {e}")


@pytest.mark.asyncio
async def test_api_health_endpoint():
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get("http://localhost:8080/health", timeout=5.0)
            assert response.status_code == 200

            data = response.json()
            assert "status" in data
        except httpx.RequestError:
            pytest.skip("API service not available for testing")

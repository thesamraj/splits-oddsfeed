#!/usr/bin/env python3
import os
import sys
from datetime import datetime, timedelta

# Database check
db_url = os.getenv("DATABASE_URL", "")
if db_url:
    try:
        import psycopg
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM odds WHERE created_at > NOW() - INTERVAL '10 minutes'")
                count = cur.fetchone()[0]
                print(f"DB: Connected, {count} odds in last 10min")
    except Exception as e:
        print(f"DB: Failed - {str(e)[:50]}")
else:
    print("DB: No DATABASE_URL")

# Redis check  
redis_url = os.getenv("REDIS_URL", "")
if redis_url:
    try:
        import redis
        r = redis.from_url(redis_url)
        r.ping()
        print(f"Redis: Connected")
    except Exception as e:
        print(f"Redis: Failed - {str(e)[:50]}")
else:
    print("Redis: No REDIS_URL")
import json
import time
import os
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL = os.getenv("REDIS_CHANNEL", "odds.raw")


def publish_envelope(source: str, url: str, status: int, data: dict):
    """Publish an envelope to Redis"""
    try:
        rconn = redis.Redis.from_url(REDIS_URL)

        payload = {
            "source": source,
            "url": url,
            "status": status,
            "ts": time.time(),
            "data": data,
        }

        msg = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        rconn.publish(REDIS_CHANNEL, msg)

    except Exception as e:
        print(f"[publisher] redis publish error: {e}", flush=True)

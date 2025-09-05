#!/usr/bin/env python3
"""
Redis Proxy Service - Ensures all messages reach normalizer
Monitors all book channels and republishes to unified channel
"""

import os
import redis
import logging
from datetime import datetime
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("redis_proxy")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# All books to monitor
BOOKS = [
    "bovada",
    "draftkings",
    "fanduel",
    "betmgm",
    "betrivers",
    "barstool",
    "sugarhouse",
    "unibet",
    "caesars",
    "pinnacle",
    "pointsbet",
    "mybookie",
    "stake",
]


class RedisProxy:
    def __init__(self):
        self.sub_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.pub_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.pubsub = self.sub_client.pubsub()
        self.stats = {}

    def subscribe_all(self):
        """Subscribe to all book channels"""
        channels = [f"odds.raw.{book}" for book in BOOKS]
        channels.append("odds.raw.kambi")

        logger.info(f"Subscribing to {len(channels)} channels")
        self.pubsub.subscribe(*channels)

    def run(self):
        """Main proxy loop with auto-restart"""
        while True:
            try:
                self.subscribe_all()
                logger.info(
                    "Redis Proxy started - ensuring all messages reach normalizer"
                )

                for message in self.pubsub.listen():
                    if message["type"] == "message":
                        channel = message["channel"]
                        data = message["data"]

                        # Extract book name
                        book = channel.replace("odds.raw.", "")

                        # Track statistics
                        if book not in self.stats:
                            self.stats[book] = {"count": 0, "last_seen": None}
                        self.stats[book]["count"] += 1
                        self.stats[book]["last_seen"] = datetime.utcnow()

                        # Republish to unified channel
                        self.pub_client.publish("odds.raw.unified", data)

                        # Log every 100th message
                        if self.stats[book]["count"] % 100 == 0:
                            logger.info(
                                f"{book}: {self.stats[book]['count']} messages proxied"
                            )

                        # Store stats in Redis
                        stats_key = f"proxy:stats:{book}"
                        self.pub_client.hset(
                            stats_key,
                            mapping={
                                "count": self.stats[book]["count"],
                                "last_seen": self.stats[book]["last_seen"].isoformat(),
                            },
                        )
                        self.pub_client.expire(stats_key, 86400)

            except Exception as e:
                logger.error(f"Proxy error: {e}")
                logger.info("Restarting in 5 seconds...")
                time.sleep(5)


if __name__ == "__main__":
    proxy = RedisProxy()
    proxy.run()

#!/usr/bin/env python3
"""
Redis Proxy - Ensures all book channels reach the normalizer
Subscribes to individual channels and republishes to unified channel
"""

import redis
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("redis_proxy")

# Redis connections
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# All books we need to monitor
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

    def subscribe_all(self):
        """Subscribe to all book channels"""
        channels = [f"odds.raw.{book}" for book in BOOKS]
        channels.append("odds.raw.kambi")  # Special Kambi channel

        logger.info(f"Subscribing to {len(channels)} channels")
        self.pubsub.subscribe(*channels)

    def run(self):
        """Main proxy loop"""
        self.subscribe_all()
        logger.info("Redis Proxy started - forwarding all messages to unified channel")

        for message in self.pubsub.listen():
            if message["type"] == "message":
                channel = message["channel"]
                data = message["data"]

                # Extract book name from channel
                book = channel.replace("odds.raw.", "")

                # Log activity
                logger.info(f"Received from {channel} - forwarding to unified channel")

                # Republish to unified channel that normalizer definitely listens to
                self.pub_client.publish("odds.raw.unified", data)

                # Also republish to original channel (keep existing flow)
                self.pub_client.publish(channel, data)

                # Track statistics
                stats_key = (
                    f"proxy:stats:{book}:{datetime.utcnow().strftime('%Y%m%d%H')}"
                )
                self.pub_client.hincrby(stats_key, "count", 1)
                self.pub_client.expire(stats_key, 86400)  # 24 hour expiry


if __name__ == "__main__":
    proxy = RedisProxy()
    proxy.run()

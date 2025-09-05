#!/usr/bin/env python3
"""
Rate Limiter Service - Controls publishing rates for high-volume books
Reduces database load by throttling excessive publishers
"""

import os
import redis
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rate_limiter")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Rate limits per book (messages per second)
RATE_LIMITS = {
    "bovada": 10,  # Limit to 10 msg/sec (was ~100+)
    "barstool": 5,
    "draftkings": 5,
    "fanduel": 5,
}


class RateLimiter:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.pubsub = self.redis_client.pubsub()
        self.last_publish = {}
        self.stats = {}

    def should_publish(self, book):
        """Check if we should publish based on rate limit"""
        if book not in RATE_LIMITS:
            return True

        min_interval = 1.0 / RATE_LIMITS[book]
        now = time.time()

        if book not in self.last_publish:
            self.last_publish[book] = now
            return True

        elapsed = now - self.last_publish[book]

        if elapsed >= min_interval:
            self.last_publish[book] = now
            return True

        return False

    def process_message(self, channel, message):
        """Process and rate-limit messages"""
        try:
            book = channel.replace("odds.raw.", "")

            # Track stats
            if book not in self.stats:
                self.stats[book] = {"received": 0, "published": 0, "throttled": 0}

            self.stats[book]["received"] += 1

            # Check rate limit
            if self.should_publish(book):
                # Republish to rate-limited channel
                limited_channel = f"odds.limited.{book}"
                self.redis_client.publish(limited_channel, message)
                self.stats[book]["published"] += 1
            else:
                self.stats[book]["throttled"] += 1

            # Log stats periodically
            total_received = sum(s["received"] for s in self.stats.values())
            if total_received % 1000 == 0:
                for book, stat in self.stats.items():
                    rate = (
                        stat["throttled"]
                        / (stat["published"] + stat["throttled"])
                        * 100
                        if stat["published"] + stat["throttled"] > 0
                        else 0
                    )
                    logger.info(
                        f"{book}: Received={stat['received']}, Published={stat['published']}, Throttled={stat['throttled']} ({rate:.1f}%)"
                    )

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def run(self):
        """Main rate limiting loop"""
        logger.info(f"Starting rate limiter - Limits: {RATE_LIMITS}")

        # Subscribe to high-volume books
        channels = [f"odds.raw.{book}" for book in RATE_LIMITS.keys()]

        self.pubsub.subscribe(*channels)
        logger.info(f"Rate limiting {len(channels)} channels")

        for message in self.pubsub.listen():
            if message["type"] == "message":
                self.process_message(message["channel"], message["data"])


if __name__ == "__main__":
    limiter = RateLimiter()
    limiter.run()

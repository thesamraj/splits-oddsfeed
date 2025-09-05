#!/usr/bin/env python3
"""
Deduplication Service - Prevents duplicate odds from being stored
Uses Redis cache to track recent odds and filter duplicates
"""

import os
import redis
import json
import hashlib
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("deduplicator")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
CACHE_TTL = int(os.getenv("CACHE_TTL", 300))  # 5 minutes


class Deduplicator:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.pubsub = self.redis_client.pubsub()
        self.stats = {"processed": 0, "duplicates": 0, "unique": 0}

    def create_hash(self, data):
        """Create hash of odds data for deduplication"""
        try:
            # Extract key fields for hashing
            key_data = {
                "book": data.get("book"),
                "event_id": data.get("event_id"),
                "market": data.get("market"),
                "selection": data.get("selection"),
                "price": data.get("price"),
                "line": data.get("line"),
            }

            # Create deterministic hash
            hash_str = json.dumps(key_data, sort_keys=True)
            return hashlib.md5(hash_str.encode()).hexdigest()

        except Exception as e:
            logger.debug(f"Error creating hash: {e}")
            return None

    def is_duplicate(self, book, data_hash):
        """Check if this odds data was recently seen"""
        cache_key = f"dedup:{book}:{data_hash}"

        # Check if exists
        if self.redis_client.exists(cache_key):
            return True

        # Mark as seen
        self.redis_client.setex(cache_key, CACHE_TTL, "1")
        return False

    def process_message(self, channel, message):
        """Process and deduplicate message"""
        try:
            data = json.loads(message)
            book = data.get("book", channel.replace("odds.raw.", ""))

            self.stats["processed"] += 1

            # Process events
            if "events" in data:
                unique_events = []

                for event in data["events"]:
                    event_hash = self.create_hash(event)

                    if event_hash and not self.is_duplicate(book, event_hash):
                        unique_events.append(event)
                        self.stats["unique"] += 1
                    else:
                        self.stats["duplicates"] += 1

                # Only republish if we have unique events
                if unique_events:
                    data["events"] = unique_events
                    data["deduplicated"] = True

                    # Publish to deduplicated channel
                    dedup_channel = f"{channel}.dedup"
                    self.redis_client.publish(dedup_channel, json.dumps(data))

                    logger.info(
                        f"{book}: {len(unique_events)} unique, {len(data['events']) - len(unique_events)} duplicates"
                    )

            # Log stats periodically
            if self.stats["processed"] % 1000 == 0:
                logger.info(
                    f"Stats - Processed: {self.stats['processed']}, Unique: {self.stats['unique']}, Duplicates: {self.stats['duplicates']}"
                )

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def run(self):
        """Main deduplication loop"""
        logger.info("Starting deduplication service")

        # Subscribe to high-volume books
        high_volume_books = ["bovada", "barstool", "draftkings", "fanduel"]
        channels = [f"odds.raw.{book}" for book in high_volume_books]

        self.pubsub.subscribe(*channels)
        logger.info(f"Monitoring {len(channels)} high-volume channels")

        for message in self.pubsub.listen():
            if message["type"] == "message":
                self.process_message(message["channel"], message["data"])


if __name__ == "__main__":
    dedup = Deduplicator()
    dedup.run()

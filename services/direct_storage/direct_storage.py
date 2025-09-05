#!/usr/bin/env python3
"""
Direct Storage Service - Ensures all books get stored
Subscribes to all channels and writes directly to database
"""

import os
import redis
import psycopg2
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("direct_storage")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
DB_HOST = os.getenv("DB_HOST", "store")
DB_NAME = os.getenv("DB_NAME", "oddsfeed")
DB_USER = os.getenv("DB_USER", "odds")
DB_PASS = os.getenv("DB_PASS", "")  # No password needed for local

# All books
ALL_BOOKS = [
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


class DirectStorage:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.pubsub = self.redis_client.pubsub()
        self.stats = {}

    def connect_db(self):
        """Connect to PostgreSQL"""
        return psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER)

    def process_message(self, channel, data):
        """Process and store message directly"""
        try:
            # Parse message
            if isinstance(data, str):
                data = json.loads(data)

            book = data.get("book", channel.replace("odds.raw.", ""))
            events = data.get("events", [])

            if not events:
                return

            conn = self.connect_db()
            cur = conn.cursor()

            stored_count = 0

            for event in events:
                try:
                    # Extract event data
                    event_id = event.get("event_id") or event.get("id")
                    if not event_id:
                        continue

                    # Ensure unique event ID
                    if not event_id.startswith(book):
                        event_id = f"{book}_{event_id}"

                    home_team = event.get("home_team") or event.get("home", "TBD")
                    away_team = event.get("away_team") or event.get("away", "TBD")
                    sport = event.get("sport", "NFL")

                    # Insert or update event
                    cur.execute(
                        """
                        INSERT INTO events (id, league, start_time, home, away, sport)
                        VALUES (%s, %s, NOW() + INTERVAL '1 day', %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET updated_at = NOW()
                    """,
                        (event_id, sport, home_team, away_team, sport),
                    )

                    # Process markets if present
                    markets = event.get("markets", [])
                    for market in markets:
                        market_type = market.get("type", "moneyline")

                        for selection in market.get("selections", []):
                            # Store odds
                            cur.execute(
                                """
                                INSERT INTO odds (event_id, book, market, outcome_name, outcome_price, outcome_point, ts)
                                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                            """,
                                (
                                    event_id,
                                    book,
                                    market_type,
                                    selection.get("name", ""),
                                    selection.get("price", 0),
                                    selection.get("line"),
                                ),
                            )
                            stored_count += 1

                    # Also check for direct price fields
                    if "home_price" in event or "price_home" in event:
                        home_price = event.get("home_price") or event.get(
                            "price_home", 0
                        )
                        away_price = event.get("away_price") or event.get(
                            "price_away", 0
                        )

                        cur.execute(
                            """
                            INSERT INTO odds (event_id, book, market, price_home, price_away, ts)
                            VALUES (%s, %s, 'moneyline', %s, %s, NOW())
                        """,
                            (event_id, book, home_price, away_price),
                        )
                        stored_count += 1

                except Exception as e:
                    logger.debug(f"Error processing event: {e}")
                    continue

            conn.commit()

            if stored_count > 0:
                logger.info(f"Stored {stored_count} odds from {book}")

                # Track stats
                if book not in self.stats:
                    self.stats[book] = 0
                self.stats[book] += stored_count

            cur.close()
            conn.close()

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def run(self):
        """Main processing loop"""
        # Subscribe to all book channels
        channels = [f"odds.raw.{book}" for book in ALL_BOOKS]

        self.pubsub.subscribe(*channels)
        logger.info(f"Direct Storage started - monitoring {len(channels)} channels")

        for message in self.pubsub.listen():
            if message["type"] == "message":
                self.process_message(message["channel"], message["data"])

                # Log stats periodically
                if (
                    sum(self.stats.values()) % 1000 == 0
                    and sum(self.stats.values()) > 0
                ):
                    logger.info(f"Total stored: {self.stats}")


if __name__ == "__main__":
    storage = DirectStorage()
    storage.run()

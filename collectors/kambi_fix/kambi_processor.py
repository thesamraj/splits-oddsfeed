#!/usr/bin/env python3
"""
Kambi Processor - Ensures Kambi books data gets stored
Subscribes to Kambi channels and writes directly to database
"""

import os
import redis
import psycopg2
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kambi_processor")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
DB_HOST = os.getenv("DB_HOST", "store")
DB_NAME = os.getenv("DB_NAME", "oddsfeed")
DB_USER = os.getenv("DB_USER", "<user>")  # Placeholder
DB_PASS = os.getenv("DB_PASS", "<password>")  # Placeholder

# Kambi books
KAMBI_BOOKS = ["betrivers", "sugarhouse", "unibet", "caesars"]


class KambiProcessor:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.pubsub = self.redis_client.pubsub()
        self.stats = {}

    def connect_db(self):
        """Connect to PostgreSQL"""
        return psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
        )

    def process_kambi_message(self, book, data):
        """Process Kambi format message and store in database"""
        try:
            # Parse message
            if isinstance(data, str):
                data = json.loads(data)

            events = data.get("events", [])
            if not events:
                logger.debug(f"No events in {book} message")
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

                    # Prefix with book name to avoid conflicts
                    event_id = f"{book}_{event_id}"

                    home_team = event.get("home_team") or event.get("home", "TBD")
                    away_team = event.get("away_team") or event.get("away", "TBD")
                    sport = event.get("sport", "UNKNOWN")

                    # Insert event
                    cur.execute(
                        """
                        INSERT INTO events (id, league, start_time, home, away, sport)
                        VALUES (%s, %s, NOW() + INTERVAL '1 day', %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET updated_at = NOW()
                    """,
                        (event_id, sport, home_team, away_team, sport),
                    )

                    # Process markets/odds
                    markets = event.get("markets", [])
                    for market in markets:
                        market_type = market.get("type", "moneyline")

                        # Process selections
                        for selection in market.get("selections", []):
                            cur.execute(
                                """
                                INSERT INTO odds (event_id, book, market, outcome_name, outcome_price, ts)
                                VALUES (%s, %s, %s, %s, %s, NOW())
                            """,
                                (
                                    event_id,
                                    book,
                                    market_type,
                                    selection.get("name", ""),
                                    selection.get("price", 0),
                                ),
                            )
                            stored_count += 1

                    # Also check for direct odds format
                    if "home_price" in event and "away_price" in event:
                        cur.execute(
                            """
                            INSERT INTO odds (event_id, book, market, price_home, price_away, ts)
                            VALUES (%s, %s, 'moneyline', %s, %s, NOW())
                        """,
                            (
                                event_id,
                                book,
                                event.get("home_price", 0),
                                event.get("away_price", 0),
                            ),
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
            logger.error(f"Error processing {book} message: {e}")

    def run(self):
        """Main processing loop"""
        # Subscribe to Kambi book channels
        channels = [f"odds.raw.{book}" for book in KAMBI_BOOKS]
        channels.append("odds.raw.kambi")  # Also subscribe to main Kambi channel

        self.pubsub.subscribe(*channels)
        logger.info(f"Kambi Processor started - monitoring {len(channels)} channels")

        for message in self.pubsub.listen():
            if message["type"] == "message":
                channel = message["channel"]
                book = channel.replace("odds.raw.", "")

                # Map kambi channel to individual books
                if book == "kambi":
                    # Process for all Kambi books
                    for kambi_book in KAMBI_BOOKS:
                        self.process_kambi_message(kambi_book, message["data"])
                else:
                    self.process_kambi_message(book, message["data"])

                # Log stats periodically
                if sum(self.stats.values()) % 100 == 0:
                    logger.info(f"Stats: {self.stats}")


if __name__ == "__main__":
    processor = KambiProcessor()
    processor.run()

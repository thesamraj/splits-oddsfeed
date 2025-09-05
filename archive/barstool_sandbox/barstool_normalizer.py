#!/usr/bin/env python3
"""
Barstool/ESPN BET Normalizer
Processes raw odds from Redis and stores in PostgreSQL
"""

import json
import time
import redis
import psycopg2
import logging
from datetime import datetime, timezone
from typing import Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("barstool_normalizer")


class BarstoolNormalizer:
    def __init__(self):
        # Redis connection
        self.redis_client = redis.from_url("redis://broker:6379/0")
        self.pubsub = self.redis_client.pubsub()

        # PostgreSQL connection
        self.db_conn = self.connect_db()

        # Stats
        self.stats = {
            "messages_processed": 0,
            "events_created": 0,
            "odds_inserted": 0,
            "ticks_inserted": 0,
            "errors": 0,
        }

    def connect_db(self):
        """Connect to PostgreSQL database"""
        max_retries = 5
        for i in range(max_retries):
            try:
                conn = psycopg2.connect(
                    host="store",
                    port=5432,
                    database="oddsfeed",
                    user="odds",
                    password="odds",
                )
                logger.info("Connected to PostgreSQL")
                return conn
            except Exception as e:
                logger.error(
                    f"Failed to connect to DB (attempt {i+1}/{max_retries}): {e}"
                )
                time.sleep(5)

        raise Exception("Could not connect to database")

    def ensure_event_exists(self, event_data: Dict) -> str:
        """Ensure event exists in database, create if not"""
        cursor = self.db_conn.cursor()

        try:
            # Check if event exists
            cursor.execute(
                "SELECT id FROM events WHERE id = %s", (event_data["event_id"],)
            )

            if cursor.fetchone():
                return event_data["event_id"]

            # Create event
            sport = event_data.get("sport", "unknown")
            league = event_data.get("league", sport)

            # Map sport names to standard format
            sport_map = {
                "nfl": "football",
                "nba": "basketball",
                "mlb": "baseball",
                "nhl": "hockey",
                "ncaaf": "football",
                "ncaab": "basketball",
            }

            sport_normalized = sport_map.get(sport.lower(), sport)

            cursor.execute(
                """
                INSERT INTO events (
                    id, sport, league, home, away,
                    start_time, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO NOTHING
                RETURNING id
            """,
                (
                    event_data["event_id"],
                    sport_normalized,
                    league,
                    event_data.get("home_team", "TBD"),
                    event_data.get("away_team", "TBD"),
                    event_data.get("start_time", datetime.now(timezone.utc)),
                ),
            )

            self.db_conn.commit()
            self.stats["events_created"] += 1
            logger.info(f"Created event: {event_data['event_id']}")

            return event_data["event_id"]

        except Exception as e:
            logger.error(f"Error ensuring event exists: {e}")
            self.db_conn.rollback()
            raise
        finally:
            cursor.close()

    def store_odds(self, message: Dict):
        """Store odds in database"""
        cursor = self.db_conn.cursor()

        try:
            # Ensure event exists
            event_id = self.ensure_event_exists(message)

            # Store each selection as an odds record
            for selection in message.get("selections", []):
                # Insert odds
                cursor.execute(
                    """
                    INSERT INTO odds (
                        book, event_id, market, outcome_name,
                        outcome_price, outcome_point, ts
                    ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """,
                    (
                        "barstool",
                        event_id,
                        message.get("market", "unknown"),
                        selection.get("name", ""),
                        selection.get("price", 0),
                        selection.get("handicap") or selection.get("total"),
                    ),
                )

                # Insert tick for price tracking
                cursor.execute(
                    """
                    INSERT INTO ticks (
                        book, event_id, market, outcome_name,
                        outcome_price, outcome_point, ts
                    ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """,
                    (
                        "barstool",
                        event_id,
                        message.get("market", "unknown"),
                        selection.get("name", ""),
                        selection.get("price", 0),
                        selection.get("handicap") or selection.get("total"),
                    ),
                )

                self.stats["odds_inserted"] += 1
                self.stats["ticks_inserted"] += 1

            self.db_conn.commit()

        except Exception as e:
            logger.error(f"Error storing odds: {e}")
            self.db_conn.rollback()
            self.stats["errors"] += 1
        finally:
            cursor.close()

    def process_message(self, message: Dict):
        """Process a single odds message"""
        try:
            self.stats["messages_processed"] += 1

            # Store odds in database
            self.store_odds(message)

            # Log progress
            if self.stats["messages_processed"] % 10 == 0:
                logger.info(f"Stats: {self.stats}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.stats["errors"] += 1

    def run(self):
        """Main processing loop"""
        logger.info("Starting Barstool normalizer...")

        # Subscribe to Redis channel
        channel = "odds.raw.barstool"
        self.pubsub.subscribe(channel)
        logger.info(f"Subscribed to Redis channel: {channel}")

        # Process messages
        for redis_message in self.pubsub.listen():
            try:
                if redis_message["type"] != "message":
                    continue

                # Parse message
                message = json.loads(redis_message["data"])

                # Process it
                self.process_message(message)

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON: {e}")
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(1)

        # Cleanup
        self.pubsub.unsubscribe()
        self.db_conn.close()
        logger.info(f"Final stats: {self.stats}")


if __name__ == "__main__":
    normalizer = BarstoolNormalizer()
    normalizer.run()

#!/usr/bin/env python3
"""
Universal Normalizer - Production Version
Handles all sportsbooks with proper odds conversion
"""
import os
import sys
import json
import redis
import psycopg2
from datetime import datetime

# Add utils to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
try:
    from utils.odds_converter import normalize_odds, validate_price
except ImportError:
    # Fallback if utils not available
    def normalize_odds(price, book=None):
        """Convert American odds to decimal"""
        try:
            price = float(price)
            if price < -100 or price > 100:
                # Likely American odds
                if price > 0:
                    return round((price / 100) + 1, 3)
                else:
                    return round((100 / abs(price)) + 1, 3)
            elif 1.01 <= price <= 100:
                return round(price, 3)
        except:
            pass
        return None

    def validate_price(price, format="decimal"):
        try:
            return 1.01 <= float(price) <= 100
        except:
            return False


class UniversalNormalizer:
    def __init__(self):
        self.redis_client = redis.from_url(
            os.getenv("REDIS_URL", "redis://broker:6379/0")
        )
        self.db_conn = psycopg2.connect(
            os.getenv("DATABASE_URL", "postgresql://odds:odds@store:5432/oddsfeed")
        )
        self.pubsub = self.redis_client.pubsub()

        # Subscribe to all raw channels
        self.pubsub.psubscribe("odds.raw.*")

        self.stats = {"processed": 0, "stored": 0, "converted": 0, "invalid": 0}

        print(f"[UNIVERSAL] Normalizer started at {datetime.utcnow()}", flush=True)

    def process_message(self, message):
        """Process incoming message"""
        if message["type"] not in ["pmessage", "message"]:
            return

        try:
            # Get book from channel
            channel = (
                message.get("channel", b"").decode()
                if isinstance(message.get("channel"), bytes)
                else str(message.get("channel", ""))
            )
            book = channel.split(".")[-1] if channel else None

            # Parse data
            data = json.loads(message["data"])

            # Override book if in data
            if "book" in data:
                book = data["book"]

            if not book:
                return

            # Process events array
            events = data.get("events", [])

            for event in events:
                self.process_event(book, event)

            self.stats["processed"] += 1
            if self.stats["processed"] % 100 == 0:
                self.log_stats()

        except Exception as e:
            print(f"[UNIVERSAL] Error: {e}", flush=True)

    def process_event(self, book, event):
        """Process a single event from various formats"""
        try:
            # Extract event ID
            event_id = event.get("event_id", event.get("id", ""))
            if not event_id:
                return

            # Ensure event exists
            self.ensure_event(event_id, event)

            # Handle different data formats

            # Format 1: Direct price fields (Bovada format)
            if "price_home" in event or "price_away" in event:
                if "price_home" in event:
                    self.store_odds(book, event_id, "h2h", "home", event["price_home"])
                if "price_away" in event:
                    self.store_odds(book, event_id, "h2h", "away", event["price_away"])

            # Format 2: outcome_price field (spreads/totals)
            if "outcome_price" in event:
                market = event.get("market", "unknown")
                name = event.get("outcome_name", "unknown")
                if "line" in event:
                    name = f"line_{event['line']}"
                elif "total" in event:
                    name = f"total_{event['total']}"
                self.store_odds(book, event_id, market, name, event["outcome_price"])

            # Format 3: markets array with outcomes
            if "markets" in event:
                for market in event["markets"]:
                    market_type = market.get("type", "h2h")
                    for outcome in market.get("outcomes", []):
                        name = outcome.get("name", "unknown")
                        price = outcome.get("price", 0)
                        self.store_odds(book, event_id, market_type, name, price)

        except Exception as e:
            print(f"[UNIVERSAL] Event error: {e}", flush=True)

    def ensure_event(self, event_id, event_data):
        """Ensure event exists in database"""
        try:
            cur = self.db_conn.cursor()

            home = event_data.get("home_team", event_data.get("home", ""))
            away = event_data.get("away_team", event_data.get("away", ""))
            sport = event_data.get("sport", "NFL")

            cur.execute(
                """
                INSERT INTO events (id, home, away, sport, league, start_time)
                VALUES (%s, %s, %s, %s, %s, NOW() + interval '1 day')
                ON CONFLICT (id) DO NOTHING
            """,
                (str(event_id), home, away, sport, sport),
            )

            self.db_conn.commit()
            cur.close()
        except:
            self.db_conn.rollback()

    def store_odds(self, book, event_id, market, outcome_name, raw_price):
        """Store odds with conversion"""
        try:
            # Convert price
            decimal_price = normalize_odds(raw_price, book)

            if not decimal_price or not validate_price(decimal_price, "decimal"):
                self.stats["invalid"] += 1
                return

            # Track conversions
            if raw_price < -100 or raw_price > 100:
                self.stats["converted"] += 1

            # Store in database
            cur = self.db_conn.cursor()
            cur.execute(
                """
                INSERT INTO odds (book, event_id, market, outcome_name, outcome_price, ts)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT DO NOTHING
            """,
                (book, str(event_id), market, outcome_name, decimal_price),
            )

            self.db_conn.commit()
            cur.close()

            self.stats["stored"] += 1

        except Exception:
            self.db_conn.rollback()

    def log_stats(self):
        """Log statistics"""
        print(
            f"[UNIVERSAL STATS] Processed: {self.stats['processed']} | "
            f"Stored: {self.stats['stored']} | "
            f"Converted: {self.stats['converted']} | "
            f"Invalid: {self.stats['invalid']}",
            flush=True,
        )

    def run(self):
        """Main loop"""
        print("[UNIVERSAL] Starting main loop...", flush=True)

        try:
            for message in self.pubsub.listen():
                self.process_message(message)
        except KeyboardInterrupt:
            print("[UNIVERSAL] Shutting down...", flush=True)
        finally:
            self.log_stats()
            self.pubsub.close()
            self.db_conn.close()


if __name__ == "__main__":
    normalizer = UniversalNormalizer()
    normalizer.run()

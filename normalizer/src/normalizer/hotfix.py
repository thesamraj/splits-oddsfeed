#!/usr/bin/env python3
"""
Universal Normalizer Hotfix
Handles all books with proper odds conversion and validation
"""
import os
import sys
import json
import redis
import psycopg2
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from utils.odds_converter import normalize_odds, validate_price, detect_odds_format


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

        # Market standardization mappings
        self.market_mappings = {
            "moneyline": "h2h",
            "ml": "h2h",
            "money_line": "h2h",
            "head_to_head": "h2h",
            "spread": "spread",
            "spreads": "spread",
            "handicap": "spread",
            "point_spread": "spread",
            "total": "total",
            "totals": "total",
            "over_under": "total",
            "ou": "total",
            "o/u": "total",
        }

        # Stats tracking
        self.stats = {
            "processed": 0,
            "stored": 0,
            "invalid": 0,
            "converted": 0,
            "errors": 0,
        }

        print(
            f"[HOTFIX] Universal Normalizer started at {datetime.utcnow()}", flush=True
        )
        print("[HOTFIX] Subscribed to odds.raw.* channels", flush=True)

    def standardize_market(self, market_name):
        """Convert various market names to standard format"""
        if not market_name:
            return "h2h"
        clean = str(market_name).lower().strip().replace("-", "_")
        return self.market_mappings.get(clean, market_name)

    def process_message(self, message):
        """Process a single message from Redis"""
        if message["type"] not in ["pmessage", "message"]:
            return

        try:
            # Parse message
            data = json.loads(message["data"])
            book = data.get("book", "")

            # Skip if no book
            if not book:
                channel = (
                    str(message.get("channel", "")).decode()
                    if isinstance(message.get("channel"), bytes)
                    else message.get("channel", "")
                )
                if "odds.raw." in channel:
                    book = channel.split(".")[-1]

            if not book:
                return

            # Process events
            events = data.get("events", [])

            # Also check for liveEvents (Kambi format)
            if not events and "liveEvents" in data:
                events = data["liveEvents"]

            # Process each event
            for event in events:
                self.process_event(book, event)

            # Log stats every 100 messages
            self.stats["processed"] += 1
            if self.stats["processed"] % 100 == 0:
                self.log_stats()

        except Exception as e:
            self.stats["errors"] += 1
            print(f"[HOTFIX] Error processing message: {e}", flush=True)

    def process_event(self, book, event):
        """Process a single event"""
        try:
            event_id = event.get("id", "")
            if not event_id:
                return

            # Ensure event exists in events table
            self.ensure_event(event_id, event)

            # Process markets
            markets = event.get("markets", [])

            # Check for Kambi format betOffers
            if not markets and "betOffers" in event:
                markets = self.convert_kambi_betoffers(event["betOffers"])

            # Process each market
            for market in markets:
                self.process_market(book, event_id, market)

        except Exception as e:
            print(f"[HOTFIX] Error processing event {event_id}: {e}", flush=True)

    def convert_kambi_betoffers(self, bet_offers):
        """Convert Kambi betOffers to standard market format"""
        markets = []
        for offer in bet_offers:
            market = {
                "type": self.map_kambi_criterion(offer.get("criterion", {})),
                "outcomes": [],
            }

            for outcome in offer.get("outcomes", []):
                market["outcomes"].append(
                    {
                        "name": outcome.get("label", outcome.get("englishLabel", "")),
                        "price": outcome.get("odds", 0)
                        / 1000.0,  # Kambi odds are in thousandths
                    }
                )

            markets.append(market)
        return markets

    def map_kambi_criterion(self, criterion):
        """Map Kambi criterion to standard market type"""
        label = criterion.get("label", "").lower()
        if "spread" in label or "handicap" in label:
            return "spread"
        elif "total" in label or "over/under" in label:
            return "total"
        else:
            return "h2h"

    def ensure_event(self, event_id, event_data):
        """Ensure event exists in database"""
        try:
            cur = self.db_conn.cursor()

            # Extract event details
            home = event_data.get("home", event_data.get("homeName", ""))
            away = event_data.get("away", event_data.get("awayName", ""))
            sport = event_data.get("sport", "NFL")
            league = event_data.get("league", "NFL")

            # Insert event if not exists
            cur.execute(
                """
                INSERT INTO events (id, home, away, sport, league, start_time)
                VALUES (%s, %s, %s, %s, %s, NOW() + interval '1 day')
                ON CONFLICT (id) DO NOTHING
            """,
                (str(event_id), home, away, sport, league),
            )

            self.db_conn.commit()
            cur.close()

        except Exception:
            self.db_conn.rollback()
            # Event might already exist, that's OK
            pass

    def process_market(self, book, event_id, market):
        """Process a single market"""
        try:
            market_type = self.standardize_market(market.get("type", "h2h"))

            for outcome in market.get("outcomes", []):
                self.process_outcome(book, event_id, market_type, outcome)

        except Exception as e:
            print(f"[HOTFIX] Error processing market: {e}", flush=True)

    def process_outcome(self, book, event_id, market_type, outcome):
        """Process a single outcome with price conversion"""
        try:
            # Get raw price
            raw_price = outcome.get("price", 0)
            if not raw_price:
                return

            # Normalize the price (convert American to decimal if needed)
            normalized_price = normalize_odds(raw_price, book)

            if not normalized_price:
                self.stats["invalid"] += 1
                return

            # Track if we converted
            if detect_odds_format(raw_price) == "american":
                self.stats["converted"] += 1

            # Validate the normalized price
            if not validate_price(normalized_price, "decimal"):
                self.stats["invalid"] += 1
                return

            # Store in database
            cur = self.db_conn.cursor()
            cur.execute(
                """
                INSERT INTO odds (book, event_id, market, outcome_name, outcome_price, ts)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT DO NOTHING
            """,
                (
                    book,
                    str(event_id),
                    market_type,
                    outcome.get("name", ""),
                    normalized_price,
                ),
            )

            self.db_conn.commit()
            cur.close()

            self.stats["stored"] += 1

        except Exception as e:
            self.db_conn.rollback()
            self.stats["errors"] += 1
            print(f"[HOTFIX] Error storing outcome: {e}", flush=True)

    def log_stats(self):
        """Log current statistics"""
        print(
            f"[HOTFIX STATS] Processed: {self.stats['processed']} | "
            f"Stored: {self.stats['stored']} | "
            f"Converted: {self.stats['converted']} | "
            f"Invalid: {self.stats['invalid']} | "
            f"Errors: {self.stats['errors']}",
            flush=True,
        )

    def run(self):
        """Main run loop"""
        print("[HOTFIX] Starting main loop...", flush=True)

        try:
            for message in self.pubsub.listen():
                self.process_message(message)

        except KeyboardInterrupt:
            print("[HOTFIX] Shutting down...", flush=True)
        except Exception as e:
            print(f"[HOTFIX] Fatal error: {e}", flush=True)
        finally:
            self.log_stats()
            self.pubsub.close()
            self.db_conn.close()


if __name__ == "__main__":
    normalizer = UniversalNormalizer()
    normalizer.run()

#!/usr/bin/env python3
"""Canonical message handler for non-Kambi books"""

import os
import sys
import json
import redis
import psycopg2
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, List
from prometheus_client import Counter, Gauge, start_http_server

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Metrics
messages_consumed_total = Counter('messages_consumed_total', 'Total messages consumed', ['book'])
rows_written_total = Counter('rows_written_total', 'Total rows written to DB', ['book'])
errors_total = Counter('errors_total', 'Total errors', ['book', 'error_type'])
last_write_ts = Gauge('last_write_ts', 'Last successful write timestamp', ['book'])

class CanonicalNormalizer:
    """Handle messages from non-Kambi collectors"""
    
    def __init__(self):
        # Redis connection
        self.redis_client = redis.from_url(
            os.getenv("REDIS_URL", "redis://broker:6379/0")
        )
        
        # DB connection
        self.db_conn = psycopg2.connect(
            os.getenv("DATABASE_URL", "postgresql://odds:odds@store:5432/oddsfeed")
        )
        
        # Subscribe to specific channels
        self.pubsub = self.redis_client.pubsub()
        self.channels = [
            "odds.raw.circa",
            "odds.raw.superbook", 
            "odds.raw.betonline",
            "odds.raw.bookmaker",
            "odds.raw.betway",
            "odds.raw.wynnbet",
            "odds.raw.pinnacle_site",
            "odds.raw.pointsbet",
            "odds.raw.fanatics",
            "odds.raw.hardrock",
            "odds.raw.betus",
            "odds.raw.mybookie",
            "odds.raw.betfred",
            "odds.raw.betnow",
            "odds.raw.everygame",
            "odds.raw.heritage"
        ]
        
        for channel in self.channels:
            self.pubsub.subscribe(channel)
            logger.info(f"Subscribed to {channel}")
        
        # Start metrics server
        start_http_server(5000)
        logger.info("Metrics server started on :5000")
        
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process incoming Redis message"""
        if message["type"] not in ["message"]:
            return
            
        try:
            # Get book from channel
            channel = message.get("channel", b"").decode() if isinstance(message.get("channel"), bytes) else str(message.get("channel", ""))
            book = channel.split(".")[-1] if channel else None
            
            if not book:
                return
                
            messages_consumed_total.labels(book=book).inc()
            
            # Parse data
            data = json.loads(message["data"])
            
            # Handle both single event and events array
            events = data.get("events", [])
            if not events and "event_id" in data:
                # Single event format
                events = [data]
            
            for event in events:
                self.process_event(book, event)
                
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from {book}: {e}")
            errors_total.labels(book=book or "unknown", error_type="json").inc()
        except Exception as e:
            logger.error(f"Process error for {book}: {e}")
            errors_total.labels(book=book or "unknown", error_type="process").inc()
    
    def process_event(self, book: str, event: Dict[str, Any]) -> None:
        """Process a single event"""
        try:
            # Extract required fields with defaults
            event_id = event.get("event_id", f"{book}_{int(time.time()*1000)}")
            
            # Ensure event record exists
            self.ensure_event(book, event_id, event)
            
            # Process markets
            markets = event.get("markets", [])
            
            # If no markets array, try to extract from price fields
            if not markets:
                if "price_home" in event or "price_away" in event:
                    market = {"key": "moneyline", "outcomes": []}
                    if "price_home" in event:
                        market["outcomes"].append({"name": "home", "price": event["price_home"]})
                    if "price_away" in event:
                        market["outcomes"].append({"name": "away", "price": event["price_away"]})
                    markets = [market]
            
            # Insert odds for each market
            for market in markets:
                market_key = market.get("key", market.get("type", "unknown"))
                
                for outcome in market.get("outcomes", []):
                    self.insert_odds(
                        book=book,
                        event_id=event_id,
                        market=market_key,
                        outcome_name=outcome.get("name", "unknown"),
                        price=outcome.get("price", -110),
                        point=outcome.get("point"),
                        ts=event.get("ts")
                    )
            
            rows_written_total.labels(book=book).inc()
            last_write_ts.labels(book=book).set(time.time())
            
        except Exception as e:
            logger.warning(f"Failed to process event from {book}: {e}")
            errors_total.labels(book=book, error_type="event").inc()
    
    def ensure_event(self, book: str, event_id: str, event: Dict[str, Any]) -> None:
        """Ensure event exists in events table"""
        try:
            cur = self.db_conn.cursor()
            
            # Extract team names with various fallbacks
            home = event.get("home", event.get("home_team", f"{book} Home"))
            away = event.get("away", event.get("away_team", f"{book} Away"))
            sport = event.get("sport", "football")
            league = event.get("league", "NFL")
            
            # Handle commence time
            commence = event.get("commence_time", event.get("start_time"))
            if not commence:
                commence = datetime.utcnow()
            elif isinstance(commence, str):
                try:
                    commence = datetime.fromisoformat(commence.replace('Z', '+00:00'))
                except:
                    commence = datetime.utcnow()
            
            cur.execute("""
                INSERT INTO events (id, home, away, sport, league, start_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    home = EXCLUDED.home,
                    away = EXCLUDED.away
            """, (event_id, home, away, sport, league, commence))
            
            self.db_conn.commit()
            cur.close()
            
        except Exception as e:
            self.db_conn.rollback()
            logger.debug(f"Event ensure failed: {e}")
    
    def insert_odds(self, book: str, event_id: str, market: str, 
                   outcome_name: str, price: Any, point: Any = None, ts: Any = None) -> None:
        """Insert odds record"""
        try:
            cur = self.db_conn.cursor()
            
            # Convert price to float
            try:
                price = float(price) if price is not None else -110.0
            except:
                price = -110.0
            
            # Handle timestamp
            if not ts:
                ts = datetime.utcnow()
            elif isinstance(ts, (int, float)):
                if ts > 10**10:  # milliseconds
                    ts = datetime.utcfromtimestamp(ts / 1000)
                else:
                    ts = datetime.utcfromtimestamp(ts)
            elif isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                except:
                    ts = datetime.utcnow()
            
            # Insert with the actual columns that exist
            cur.execute("""
                INSERT INTO odds (book, event_id, market, outcome_name, outcome_price, outcome_point, ts)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (book, event_id, market, outcome_name, price, float(point) if point else None, ts))
            
            self.db_conn.commit()
            cur.close()
            
        except Exception as e:
            self.db_conn.rollback()
            logger.debug(f"Odds insert failed: {e}")
    
    def run(self):
        """Main loop"""
        logger.info("Starting canonical normalizer...")
        
        for message in self.pubsub.listen():
            self.process_message(message)


if __name__ == "__main__":
    normalizer = CanonicalNormalizer()
    normalizer.run()
#!/usr/bin/env python3
"""Ingest module with adapter routing and metrics"""

import os
import sys
import json
import redis
import psycopg2
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
from prometheus_client import Counter, Gauge, CollectorRegistry

# Import adapters
from .adapters.circa_adapter import CircaAdapter
from .adapters.superbook_adapter import SuperbookAdapter
from .adapters.betonline_adapter import BetonlineAdapter
from .adapters.bookmaker_adapter import BookmakerAdapter
from .adapters.betway_adapter import BetwayAdapter
from .adapters.wynnbet_adapter import WynnbetAdapter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Metrics
registry = CollectorRegistry()
ingest_rejected_total = Counter('ingest_rejected_total', 'Total rejected messages', ['book', 'reason'], registry=registry)
ingest_written_total = Counter('ingest_written_total', 'Total written to DB', ['book'], registry=registry)
last_ingest_ts = Gauge('last_ingest_ts', 'Last successful ingest timestamp', ['book'], registry=registry)


class IngestRouter:
    """Routes messages to appropriate adapters and writes to DB"""
    
    def __init__(self):
        # Initialize adapters
        self.adapters = {
            'circa': CircaAdapter(),
            'superbook': SuperbookAdapter(),
            'betonline': BetonlineAdapter(),
            'bookmaker': BookmakerAdapter(),
            'betway': BetwayAdapter(),
            'wynnbet': WynnbetAdapter()
        }
        
        # Redis connection
        self.redis_client = redis.from_url(
            os.getenv("REDIS_URL", "redis://broker:6379/0")
        )
        
        # DB connection
        self.db_conn = psycopg2.connect(
            os.getenv("DATABASE_URL", "postgresql://odds:odds@store:5432/oddsfeed")
        )
        
        # Subscribe to canonical channels
        self.pubsub = self.redis_client.pubsub()
        self.pubsub.psubscribe("odds.canon.*")
        # Also subscribe to raw for backward compatibility
        self.pubsub.psubscribe("odds.raw.*")
        
        logger.info(f"Ingest router started with {len(self.adapters)} adapters")
    
    def process_message(self, message: Dict[str, Any]) -> None:
        """Process incoming Redis message"""
        if message["type"] not in ["pmessage", "message"]:
            return
        
        try:
            # Get book from channel
            channel = message.get("channel", b"").decode() if isinstance(message.get("channel"), bytes) else str(message.get("channel", ""))
            book = channel.split(".")[-1] if channel else None
            
            # Parse data
            data = json.loads(message["data"])
            
            # Override book if in data
            if "book" in data:
                book = data["book"]
            
            if not book:
                ingest_rejected_total.labels(book="unknown", reason="no_book").inc()
                logger.warning("Message has no book identifier")
                return
            
            # Route to adapter if exists
            if book in self.adapters:
                canonical = self.adapters[book].to_canonical(data)
                if canonical:
                    self.write_canonical(canonical, book)
                    last_ingest_ts.labels(book=book).set(time.time())
                else:
                    ingest_rejected_total.labels(book=book, reason="adapter_failed").inc()
                    logger.warning(f"Adapter failed for {book}")
            else:
                # Try to process directly if already canonical
                if self.is_canonical(data):
                    self.write_canonical(data, book)
                else:
                    ingest_rejected_total.labels(book=book, reason="no_adapter").inc()
                    logger.warning(f"No adapter for book: {book}")
                    
        except json.JSONDecodeError as e:
            ingest_rejected_total.labels(book=book or "unknown", reason="invalid_json").inc()
            logger.error(f"Invalid JSON: {e}")
        except Exception as e:
            ingest_rejected_total.labels(book=book or "unknown", reason="error").inc()
            logger.error(f"Process error: {e}")
    
    def is_canonical(self, data: Dict[str, Any]) -> bool:
        """Check if message is already in canonical format"""
        required = ['book', 'event_id', 'markets']
        return all(field in data for field in required)
    
    def write_canonical(self, canonical: Dict[str, Any], book: str) -> None:
        """Write canonical message to database"""
        try:
            # Handle both single event and events array
            events = canonical.get('events', [canonical])
            
            for event in events:
                if not isinstance(event, dict):
                    continue
                    
                event_id = event.get('event_id')
                if not event_id:
                    ingest_rejected_total.labels(book=book, reason="no_event_id").inc()
                    continue
                
                # Insert/update event
                self.ensure_event(event)
                
                # Insert odds
                for market in event.get('markets', []):
                    market_key = market.get('key', 'unknown')
                    
                    for outcome in market.get('outcomes', []):
                        self.insert_odds(
                            book=book,
                            event_id=event_id,
                            market=market_key,
                            outcome_name=outcome.get('name'),
                            price=outcome.get('price'),
                            point=outcome.get('point'),
                            ts=event.get('ts')
                        )
                
                ingest_written_total.labels(book=book).inc()
                
        except Exception as e:
            ingest_rejected_total.labels(book=book, reason="write_error").inc()
            logger.error(f"Write error for {book}: {e}")
    
    def ensure_event(self, event: Dict[str, Any]) -> None:
        """Ensure event exists in events table"""
        try:
            cur = self.db_conn.cursor()
            
            event_id = event.get('event_id')
            home = event.get('home', '')
            away = event.get('away', '')
            sport = event.get('sport', 'unknown')
            league = event.get('league', sport)
            commence = event.get('commence_time')
            
            # Convert commence time if needed
            if commence and isinstance(commence, str):
                try:
                    commence = datetime.fromisoformat(commence.replace('Z', '+00:00'))
                except:
                    commence = None
            
            if not commence:
                commence = datetime.utcnow()
            
            cur.execute("""
                INSERT INTO events (id, home, away, sport, league, start_time)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    home = EXCLUDED.home,
                    away = EXCLUDED.away,
                    sport = EXCLUDED.sport,
                    league = EXCLUDED.league
            """, (event_id, home, away, sport, league, commence))
            
            self.db_conn.commit()
            cur.close()
            
        except Exception as e:
            self.db_conn.rollback()
            logger.warning(f"Failed to ensure event: {e}")
    
    def insert_odds(self, book: str, event_id: str, market: str, 
                   outcome_name: str, price: Any, point: Any = None, ts: Any = None) -> None:
        """Insert odds record"""
        try:
            if price is None:
                return
                
            cur = self.db_conn.cursor()
            
            # Handle timestamp
            if ts:
                if isinstance(ts, (int, float)):
                    if ts > 10**10:  # milliseconds
                        ts = datetime.utcfromtimestamp(ts / 1000)
                    else:
                        ts = datetime.utcfromtimestamp(ts)
                elif isinstance(ts, str):
                    ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            else:
                ts = datetime.utcnow()
            
            # Insert odds
            cur.execute("""
                INSERT INTO odds (book, event_id, market, outcome_name, outcome_price, outcome_point, ts)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (book, event_id, market, outcome_name, float(price), float(point) if point else None, ts))
            
            self.db_conn.commit()
            cur.close()
            
        except Exception as e:
            self.db_conn.rollback()
            logger.warning(f"Failed to insert odds: {e}")
    
    def run(self):
        """Main loop"""
        logger.info("Starting ingest loop...")
        
        for message in self.pubsub.listen():
            self.process_message(message)


if __name__ == "__main__":
    router = IngestRouter()
    router.run()
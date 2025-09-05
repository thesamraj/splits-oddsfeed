#!/usr/bin/env python3
"""
Base normalizer with fail-soft error handling
Never crashes on data issues - logs and continues
"""

import os
import json
import time
import redis
import psycopg2
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from prometheus_client import Counter, Gauge, start_http_server
from flask import Flask, jsonify
import threading

logger = logging.getLogger(__name__)

# Standard Prometheus metrics
NORMALIZER_PROCESSED = Counter(
    "normalizer_processed_total", "Total messages processed", ["book"]
)
NORMALIZER_ERRORS = Counter(
    "normalizer_errors_total", "Total normalization errors", ["book", "market"]
)
NORMALIZER_SKIPPED = Counter(
    "normalizer_skipped_total", "Total messages skipped", ["book", "reason"]
)
ODDS_INSERTED = Counter("odds_inserted_total", "Total odds inserted", ["book"])
LAST_SUCCESS_TS = Gauge("last_success_ts", "Last successful normalization", ["book"])
NORMALIZER_UP = Gauge("normalizer_up", "Normalizer status", ["book"])


class FailSoftNormalizer:
    """Base normalizer that never crashes on data issues"""

    def __init__(self, book_name: str):
        self.book = book_name
        self.redis_url = os.getenv("REDIS_URL", "redis://broker:6379/0")
        self.db_url = os.getenv("DATABASE_URL", "")

        # Connect to Redis
        self.redis_client = redis.from_url(self.redis_url)
        self.input_channel = f"odds.raw.{book_name}"

        # Database connection (will retry if fails)
        self.db_conn = None
        self.connect_db()

        # Metrics tracking
        self.consecutive_failures = 0
        self.last_success = time.time()

        # Start metrics server
        metrics_port = int(os.getenv("METRICS_PORT", "9090"))
        health_port = int(os.getenv("HEALTH_PORT", "9091"))
        threading.Thread(
            target=lambda: start_http_server(metrics_port), daemon=True
        ).start()

        # Health endpoint
        self.app = Flask(__name__)
        self.app.route("/healthz")(self._healthz)
        threading.Thread(
            target=lambda: self.app.run(host="0.0.0.0", port=health_port), daemon=True
        ).start()

        # Mark as up
        NORMALIZER_UP.labels(book=self.book).set(1)
        logger.info(f"Initialized {book_name} normalizer with fail-soft handling")

    def connect_db(self):
        """Connect to database with retry logic"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                if self.db_url:
                    self.db_conn = psycopg2.connect(self.db_url)
                    self.db_conn.autocommit = True
                    logger.info(f"Connected to database for {self.book}")
                    return True
                else:
                    logger.error("DATABASE_URL not set")
                    return False
            except Exception as e:
                logger.error(f"DB connection attempt {attempt+1} failed: {e}")
                time.sleep(2**attempt)

        logger.error(f"Failed to connect to database after {max_retries} attempts")
        return False

    def _healthz(self):
        """Health check endpoint"""
        healthy = (
            time.time() - self.last_success
        ) < 300  # Unhealthy if no success in 5 min

        return jsonify(
            {
                "status": "healthy" if healthy else "degraded",
                "book": self.book,
                "timestamp": time.time(),
                "last_success": self.last_success,
                "consecutive_failures": self.consecutive_failures,
                "normalizer_up": NORMALIZER_UP._value.get((self.book,), 0),
            }
        ), (200 if healthy else 503)

    def normalize_message(
        self, message: Dict[str, Any]
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Override this method in subclasses to implement book-specific normalization
        Should return list of normalized odds or None if skip
        """
        raise NotImplementedError("Subclasses must implement normalize_message")

    def run(self):
        """Main processing loop - never crashes"""
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(self.input_channel)

        logger.info(f"Listening on {self.input_channel}")

        for message in pubsub.listen():
            if message["type"] != "message":
                continue

            try:
                # Parse message
                data = json.loads(message["data"])
                NORMALIZER_PROCESSED.labels(book=self.book).inc()

                # Normalize with error handling
                try:
                    normalized = self.normalize_message(data)

                    if not normalized:
                        NORMALIZER_SKIPPED.labels(
                            book=self.book, reason="no_odds"
                        ).inc()
                        continue

                    # Insert to database
                    if self.db_conn:
                        self.insert_odds(normalized)
                        self.last_success = time.time()
                        LAST_SUCCESS_TS.labels(book=self.book).set(self.last_success)
                        self.consecutive_failures = 0
                    else:
                        # Try to reconnect
                        if self.connect_db():
                            self.insert_odds(normalized)
                        else:
                            NORMALIZER_ERRORS.labels(
                                book=self.book, market="db_connection"
                            ).inc()

                except Exception as e:
                    # Log error but continue processing
                    market = data.get("data", {}).get("market", "unknown")
                    NORMALIZER_ERRORS.labels(book=self.book, market=market).inc()
                    self.consecutive_failures += 1

                    logger.error(
                        f"Normalization error for {self.book}: {e}", exc_info=True
                    )
                    logger.error(f"Failed message: {json.dumps(data)[:500]}")

                    # Don't exit, just continue
                    continue

            except json.JSONDecodeError as e:
                NORMALIZER_ERRORS.labels(book=self.book, market="json_decode").inc()
                logger.error(f"Invalid JSON from {self.input_channel}: {e}")
                continue
            except Exception as e:
                NORMALIZER_ERRORS.labels(book=self.book, market="unknown").inc()
                logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
                continue

            # Check if we're in a bad state (no success for 5 minutes)
            if time.time() - self.last_success > 300:
                NORMALIZER_UP.labels(book=self.book).set(0)
                logger.warning(
                    f"No successful normalization for {self.book} in 5 minutes"
                )

    def insert_odds(self, odds_list: List[Dict[str, Any]]):
        """Insert normalized odds to database"""
        if not self.db_conn or not odds_list:
            return

        inserted = 0
        with self.db_conn.cursor() as cur:
            for odds in odds_list:
                try:
                    cur.execute(
                        """
                        INSERT INTO odds (
                            book, event_id, sport, home_team, away_team,
                            market, selection, price, ts
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (book, event_id, market, selection)
                        DO UPDATE SET price = EXCLUDED.price, ts = EXCLUDED.ts
                    """,
                        (
                            self.book,
                            odds["event_id"],
                            odds.get("sport", "unknown"),
                            odds.get("home_team", ""),
                            odds.get("away_team", ""),
                            self.canonicalize_market(odds.get("market", "h2h")),
                            odds.get("selection", ""),
                            odds.get("price", 0),
                            datetime.now(timezone.utc),
                        ),
                    )
                    inserted += 1
                    ODDS_INSERTED.labels(book=self.book).inc()
                except Exception as e:
                    logger.error(f"Failed to insert odds: {e}")
                    continue

        if inserted > 0:
            logger.info(f"Inserted {inserted} odds for {self.book}")

    def canonicalize_market(self, market: str) -> str:
        """Standardize market names"""
        market_map = {
            "moneyline": "h2h",
            "ml": "h2h",
            "money line": "h2h",
            "spread": "spread",
            "spreads": "spread",
            "handicap": "spread",
            "total": "total",
            "totals": "total",
            "over/under": "total",
            "over_under": "total",
            "ou": "total",
        }
        clean = str(market).lower().strip()
        return market_map.get(clean, clean)

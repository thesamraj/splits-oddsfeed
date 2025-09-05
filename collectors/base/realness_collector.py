#!/usr/bin/env python3
"""
Base collector with realness gate enforcement
All production collectors should inherit from this
"""

import os
import sys
import json
import time
import redis
import logging
from typing import List, Dict, Any
from prometheus_client import Counter, Gauge, start_http_server
from flask import Flask, jsonify
import threading

# Add infra to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from infra.realness_gate import RealnessGate

logger = logging.getLogger(__name__)

# Prometheus metrics (standard across all collectors)
TICKS_TOTAL = Counter("ticks_total", "Total ticks processed", ["book"])
ODDS_UPSERTS_TOTAL = Counter("odds_upserts_total", "Total odds upserted", ["book"])
ERRORS_TOTAL = Counter("errors_total", "Total errors", ["book", "type"])
HTTP_429_TOTAL = Counter("http_429_total", "Total HTTP 429 errors", ["book"])
REALNESS_SCORE = Gauge("realness_score", "Current realness score", ["book"])
REALNESS_OK = Gauge("realness_ok", "Realness validation status", ["book"])
LAST_SUCCESS_TS = Gauge(
    "last_success_ts", "Last successful collection timestamp", ["book"]
)
COLLECTOR_UP = Gauge("collector_up", "Collector status", ["book"])


class RealnessCollector:
    """Base collector with realness gate enforcement"""

    def __init__(self, book_name: str, redis_url: str = None, channel: str = None):
        self.book = book_name
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://broker:6379/0")
        self.channel = channel or f"odds.raw.{book_name}"
        self.redis_client = redis.from_url(self.redis_url)

        # Initialize realness gate
        self.gate = RealnessGate(strict_mode=True)
        self.buffer = []
        self.buffer_size = int(os.getenv("BUFFER_SIZE", "200"))

        # Environment flags
        self.real_only = os.getenv("REAL_ONLY", "true").lower() == "true"
        self.min_threshold = float(os.getenv("REALNESS_THRESHOLD", "0.9"))

        # Metrics and health ports
        metrics_port = int(os.getenv("METRICS_PORT", "9090"))
        health_port = int(os.getenv("HEALTH_PORT", "9091"))

        # Start metrics server in background thread
        threading.Thread(
            target=lambda: start_http_server(metrics_port), daemon=True
        ).start()

        # Flask app for health endpoint
        self.app = Flask(__name__)
        self.app.route("/healthz")(self._healthz)
        threading.Thread(
            target=lambda: self.app.run(host="0.0.0.0", port=health_port), daemon=True
        ).start()

        # Initialize collector as up
        COLLECTOR_UP.labels(book=self.book).set(1)
        logger.info(
            f"Initialized {book_name} collector with realness gate (threshold={self.min_threshold})"
        )

    def _healthz(self):
        """Health check endpoint"""
        return jsonify(
            {
                "status": "healthy",
                "book": self.book,
                "timestamp": time.time(),
                "last_success": LAST_SUCCESS_TS._value.get((self.book,), 0),
                "realness_ok": REALNESS_OK._value.get((self.book,), 0),
            }
        )

    def validate_and_publish(self, events: List[Dict[str, Any]]) -> bool:
        """
        Validate realness and publish if allowed

        Args:
            events: List of event dictionaries to validate

        Returns:
            True if published, False if blocked
        """
        if not events:
            return False

        # Add to buffer
        self.buffer.extend(events)

        # Check if we have enough events to validate
        if len(self.buffer) < self.buffer_size:
            logger.debug(f"Buffering events: {len(self.buffer)}/{self.buffer_size}")
            return False

        # Compute realness score on buffer
        sample_data = {
            "events": self.buffer[: self.buffer_size],
            "source": self.book,
            "timestamp": time.time(),
        }

        score = self.gate.compute_realness(sample_data)

        # Update metrics
        REALNESS_SCORE.labels(book=self.book).set(score)

        # Check if we should allow write
        if self.real_only and not self.gate.allow_write(score, self.min_threshold):
            REALNESS_OK.labels(book=self.book).set(0)
            ERRORS_TOTAL.labels(book=self.book, type="realness").inc()

            logger.error(
                f"Realness check FAILED for {self.book}: score={score:.3f} < {self.min_threshold}"
            )
            logger.error(
                f"Sample event IDs: {[e.get('event_id') for e in self.buffer[:5]]}"
            )

            # In production with REAL_ONLY=true, exit to prevent bad data
            if self.real_only:
                COLLECTOR_UP.labels(book=self.book).set(0)
                sys.exit(2)  # Exit code 2 for realness failure

            # Clear buffer and continue
            self.buffer = []
            return False

        # Realness check passed
        REALNESS_OK.labels(book=self.book).set(1)
        logger.info(f"Realness check PASSED for {self.book}: score={score:.3f}")

        # Publish all buffered events
        published = 0
        for event in self.buffer:
            message = {
                "book": self.book,
                "data": event,
                "timestamp": time.time(),
                "realness_score": score,
            }
            self.redis_client.publish(self.channel, json.dumps(message))
            published += 1
            ODDS_UPSERTS_TOTAL.labels(book=self.book).inc()

        TICKS_TOTAL.labels(book=self.book).inc(published)
        LAST_SUCCESS_TS.labels(book=self.book).set(time.time())

        logger.info(f"Published {published} events for {self.book}")

        # Clear buffer
        self.buffer = []
        return True

    def handle_rate_limit(self, attempt: int) -> float:
        """
        Handle rate limiting with exponential backoff and jitter

        Args:
            attempt: Current retry attempt number

        Returns:
            Sleep time in seconds
        """
        HTTP_429_TOTAL.labels(book=self.book).inc()
        base_wait = 2**attempt
        jitter = time.time() % 1  # Add up to 1 second of jitter
        sleep_time = base_wait + jitter
        logger.warning(f"Rate limited for {self.book}, sleeping {sleep_time:.1f}s")
        return sleep_time

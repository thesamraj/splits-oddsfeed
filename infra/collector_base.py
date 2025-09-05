#!/usr/bin/env python3
"""
Base collector class with realness validation and metrics
All production collectors should inherit from this
"""

import os
import sys
import json
import redis
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from prometheus_client import Counter, Gauge, generate_latest, start_http_server
import time

from realness_gate import RealnessGate

logger = logging.getLogger("collector_base")

# Prometheus metrics
TICKS_TOTAL = Counter("ticks_total", "Total ticks processed", ["book"])
ODDS_UPSERTS_TOTAL = Counter("odds_upserts_total", "Total odds upserted", ["book"])
ERRORS_TOTAL = Counter("errors_total", "Total errors", ["book", "type"])
HTTP_429_TOTAL = Counter("http_429_total", "Total HTTP 429 errors", ["book"])
REALNESS_SCORE = Gauge("realness_score", "Current realness score", ["book"])
LAST_SUCCESS_TS = Gauge(
    "last_success_ts", "Last successful collection timestamp", ["book"]
)
COLLECTOR_UP = Gauge("collector_up", "Collector status", ["book"])
GATE_BLOCKS = Counter(
    "collector_gate_blocks_total", "Total blocks by realness gate", ["book", "reason"]
)


class RealCollectorBase:
    """
    Base class for real data collectors with built-in validation
    """

    def __init__(self, book_name: str, strict_validation: bool = True):
        """
        Initialize collector

        Args:
            book_name: Name of the sportsbook
            strict_validation: Use strict realness validation
        """
        self.book = book_name
        self.gate = RealnessGate(strict_mode=strict_validation)

        # Redis connection
        self.redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "broker"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True,
        )

        # Stats
        self.stats = {"collections": 0, "blocked": 0, "errors": 0, "last_success": None}

        # Start metrics server
        metrics_port = int(os.getenv("METRICS_PORT", "9090"))
        start_http_server(metrics_port)

        # Initialize collector as up
        COLLECTOR_UP.labels(book=book_name).set(1)

        logger.info(
            f"Initialized {book_name} collector with {'strict' if strict_validation else 'permissive'} validation"
        )

    def collect_data(self) -> Optional[Dict[str, Any]]:
        """
        Collect data from source. Must be implemented by subclass.

        Returns:
            Dict containing events data or None if failed
        """
        raise NotImplementedError("Subclass must implement collect_data()")

    def publish_if_real(self, data: Dict[str, Any]) -> bool:
        """
        Validate and publish data if it passes realness check

        Args:
            data: Data to validate and publish

        Returns:
            bool: True if published, False if blocked
        """
        try:
            # Compute realness score
            score = self.gate.compute_realness(data)
            REALNESS_SCORE.labels(book=self.book).set(score)

            # Check if REAL_ONLY mode is enabled
            real_only = os.getenv("REAL_ONLY", "true").lower() == "true"
            min_threshold = float(os.getenv("REALNESS_THRESHOLD", "0.9"))

            # Use allow_write to check
            if real_only and not self.gate.allow_write(score, min_threshold):
                reason = "fake" if score < 0.5 else "suspicious"
                GATE_BLOCKS.labels(book=self.book, reason=reason).inc()

                self.stats["blocked"] += 1
                print(f"REALNESS_OK=0 score={score:.2f}")
                logger.warning(f"{self.book}: REALNESS_OK=0 score={score:.2f}")

                if os.getenv("EXIT_ON_FAKE", "false").lower() == "true":
                    logger.error(f"{self.book}: Exiting due to fake data")
                    sys.exit(2)

                return False

            # Publish to Redis
            message = {
                "book": self.book,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "realness_score": score,
                **data,
            }

            channel = f"odds.raw.{self.book}"
            self.redis_client.publish(channel, json.dumps(message))

            # Update metrics
            event_count = len(data.get("events", []))
            ODDS_UPSERTS_TOTAL.labels(book=self.book).inc(event_count)
            LAST_SUCCESS_TS.labels(book=self.book).set(time.time())
            TICKS_TOTAL.labels(book=self.book).inc()

            self.stats["collections"] += 1
            self.stats["last_success"] = datetime.now(timezone.utc)

            print(f"REALNESS_OK=1 score={score:.2f}")
            logger.info(
                f"{self.book}: REALNESS_OK=1 score={score:.2f} events={event_count}"
            )
            return True

        except Exception as e:
            ERRORS_TOTAL.labels(book=self.book, type="publish").inc()
            self.stats["errors"] += 1
            logger.error(f"{self.book}: Error in publish_if_real: {e}")
            return False

    def run_collection_cycle(self) -> bool:
        """
        Run one collection cycle with timing and error handling

        Returns:
            bool: True if successful, False otherwise
        """
        start_time = time.time()

        try:
            # Collect data
            data = self.collect_data()

            if data:
                # Record collection time
                duration = time.time() - start_time
                COLLECTION_TIME.labels(book=self.book).observe(duration)

                # Validate and publish
                return self.publish_if_real(data)
            else:
                logger.warning(f"{self.book}: No data collected")
                return False

        except Exception as e:
            EVENTS_COLLECTED.labels(book=self.book, status="error").inc()
            self.stats["errors"] += 1
            logger.error(f"{self.book}: Collection error: {e}")
            return False

        finally:
            duration = time.time() - start_time
            if duration > 30:
                logger.warning(f"{self.book}: Slow collection: {duration:.1f}s")

    def get_stats(self) -> Dict[str, Any]:
        """Get collector statistics"""
        gate_metrics = self.gate.get_metrics()

        return {"book": self.book, "stats": self.stats, "gate_metrics": gate_metrics}

    def get_prometheus_metrics(self) -> bytes:
        """Get Prometheus metrics in text format"""
        return generate_latest()

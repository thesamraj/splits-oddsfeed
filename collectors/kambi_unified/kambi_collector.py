#!/usr/bin/env python3
"""
Kambi Unified Collector - Supports 5 brands via KAMBI_BRAND env var
Brands: betrivers, barstool, caesars, sugarhouse, unibet
"""

import os
import sys
import time
import json
import random
import hashlib
import requests
import redis
from datetime import datetime
from typing import Dict, List, Any, Optional
from prometheus_client import Counter, Gauge, start_http_server
from flask import Flask, jsonify
import threading
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
KAMBI_BRAND = os.getenv("KAMBI_BRAND", "betrivers").lower()
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379")
DATABASE_URL = os.getenv("DATABASE_URL")
REAL_ONLY = os.getenv("REAL_ONLY", "true").lower() == "true"
REALNESS_THRESHOLD = float(os.getenv("REALNESS_THRESHOLD", "0.9"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9090"))
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "9091"))
PROXY_URL = os.getenv("PROXY_URL")
COLLECTION_INTERVAL = int(os.getenv("COLLECTION_INTERVAL", "60"))

# Brand configurations
BRAND_CONFIG = {
    "betrivers": {
        "base_url": "https://eu-offering.kambicdn.com/offering/v2018/rsi2us",
        "endpoints": ["listView/american_football/nfl", "event/upcoming.json"],
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    },
    "barstool": {
        "base_url": "https://eu-offering.kambicdn.com/offering/v2018/barstoolsports",
        "endpoints": ["listView/american_football/nfl", "event/upcoming.json"],
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    },
    "caesars": {
        "base_url": "https://eu-offering.kambicdn.com/offering/v2018/caesarspa",
        "endpoints": ["listView/american_football/nfl", "event/upcoming.json"],
        "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    },
    "sugarhouse": {
        "base_url": "https://eu-offering.kambicdn.com/offering/v2018/shpa",
        "endpoints": ["listView/american_football/nfl", "event/upcoming.json"],
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    },
    "unibet": {
        "base_url": "https://eu-offering.kambicdn.com/offering/v2018/ubuspa",
        "endpoints": ["listView/american_football/nfl", "event/upcoming.json"],
        "user_agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
    },
}

# Prometheus metrics
collector_up = Gauge("collector_up", "Collector health status", ["book"])
ticks_total = Counter("ticks_total", "Total ticks processed", ["book"])
odds_15m = Gauge("odds_15m", "Odds in last 15 minutes", ["book"])
realness_score_metric = Gauge("realness_score", "Realness validation score", ["book"])
http_429_total = Counter("http_429_total", "Total 429 rate limit responses", ["book"])
errors_total = Counter("errors_total", "Total errors", ["book", "type"])

# Flask app for health checks
app = Flask(__name__)


class TokenBucket:
    """Rate limiter with configurable rate and burst"""

    def __init__(self, rate=6, burst=12):
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last_update = time.time()

    def consume(self, tokens=1):
        now = time.time()
        elapsed = now - self.last_update
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.last_update = now

        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    def wait_time(self):
        if self.tokens < 1:
            return (1 - self.tokens) / self.rate
        return 0


class RealnessGate:
    """Validate data is real before publishing"""

    def __init__(self, threshold=0.9):
        self.threshold = threshold
        self.buffer = []
        self.buffer_size = 200

    def add_event(self, event: Dict[str, Any]):
        self.buffer.append(event)
        if len(self.buffer) > self.buffer_size:
            self.buffer.pop(0)

    def compute_realness(self, data: List[Dict]) -> float:
        """Score 0-1 based on data patterns"""
        if not data:
            return 0.0

        score = 1.0

        # Check for test patterns
        test_patterns = ["test", "demo", "sample", "fake", "mock"]
        for event in data:
            event_str = json.dumps(event).lower()
            if any(pattern in event_str for pattern in test_patterns):
                score *= 0.5

        # Check for realistic odds ranges
        for event in data:
            if "odds" in event:
                odds_val = event.get("odds", 0)
                if isinstance(odds_val, (int, float)):
                    if odds_val < -10000 or odds_val > 10000:
                        score *= 0.8

        # Check for timestamps
        for event in data:
            if "timestamp" in event or "ts" in event or "time" in event:
                score *= 1.1

        # Check variety
        if len(set(json.dumps(e) for e in data)) < len(data) * 0.5:
            score *= 0.7

        return min(1.0, max(0.0, score))

    def allow_write(self, score: float) -> bool:
        return score >= self.threshold


class KambiCollector:
    def __init__(self):
        self.brand = KAMBI_BRAND
        self.config = BRAND_CONFIG.get(self.brand)
        if not self.config:
            logger.error(f"Unknown brand: {self.brand}")
            sys.exit(1)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": self.config["user_agent"],
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )

        if PROXY_URL:
            self.session.proxies = {"http": PROXY_URL, "https": PROXY_URL}

        self.redis_client = redis.from_url(REDIS_URL)
        self.rate_limiter = TokenBucket(rate=6, burst=12)
        self.realness_gate = RealnessGate(threshold=REALNESS_THRESHOLD)
        self.dedup_cache = {}  # event_id -> timestamp
        self.backoff_time = 1
        self.max_backoff = 60
        self.odds_15m_window = []  # Sliding window for odds_15m metric

        # Initialize metrics
        collector_up.labels(book=self.brand).set(0)
        realness_score_metric.labels(book=self.brand).set(0)

    def fetch_odds(self, endpoint: str, retry_count: int = 3) -> Optional[Dict]:
        """Fetch odds with rate limiting and retry logic"""
        for attempt in range(retry_count):
            # Wait for rate limit token
            wait_time = self.rate_limiter.wait_time()
            if wait_time > 0:
                jitter = random.uniform(0, min(1, wait_time * 0.1))
                time.sleep(wait_time + jitter)

            if not self.rate_limiter.consume():
                time.sleep(1)
                continue

            url = f"{self.config['base_url']}/{endpoint}"

            try:
                # Rotate user agent occasionally
                if random.random() < 0.1:
                    ua_list = [
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
                        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Firefox/120.0",
                    ]
                    self.session.headers["User-Agent"] = random.choice(ua_list)

                response = self.session.get(
                    url, timeout=(5, 10)
                )  # (connect, read) timeouts

                if response.status_code == 429:
                    http_429_total.labels(book=self.brand).inc()
                    self.backoff_time = min(self.backoff_time * 2, self.max_backoff)
                    logger.warning(f"Rate limited, backing off {self.backoff_time}s")
                    time.sleep(self.backoff_time + random.uniform(0, 1))
                    continue

                if response.status_code == 403:
                    logger.error(f"Access forbidden for {self.brand}, may need proxy")
                    time.sleep(60)  # Wait longer for 403
                    continue

                response.raise_for_status()
                self.backoff_time = 1  # Reset on success
                return response.json()

            except requests.exceptions.Timeout:
                logger.warning(
                    f"Timeout on attempt {attempt + 1}/{retry_count} for {url}"
                )
                time.sleep(2**attempt)  # Exponential backoff
            except Exception as e:
                errors_total.labels(book=self.brand, type="fetch").inc()
                logger.error(f"Error fetching {url}: {e}")
                if attempt < retry_count - 1:
                    time.sleep(2**attempt)

        return None

    def normalize_event(self, raw_event: Dict) -> Optional[Dict]:
        """Normalize Kambi event to standard format"""
        try:
            # Generate idempotency key with more granularity
            event_id = raw_event.get("event", {}).get("id", "")
            if not event_id:
                return None

            # Include more fields in dedup key for better uniqueness
            market_hash = hashlib.md5(
                json.dumps(raw_event.get("betOffers", []), sort_keys=True).encode()
            ).hexdigest()[:8]
            idempotency_key = hashlib.md5(
                f"{event_id}_{self.brand}_{market_hash}".encode()
            ).hexdigest()

            # Check dedup cache
            if idempotency_key in self.dedup_cache:
                if time.time() - self.dedup_cache[idempotency_key] < 60:
                    return None  # Skip duplicate

            self.dedup_cache[idempotency_key] = time.time()

            # Clean old cache entries
            if len(self.dedup_cache) > 10000:
                cutoff = time.time() - 300
                self.dedup_cache = {
                    k: v for k, v in self.dedup_cache.items() if v > cutoff
                }

            normalized = {
                "book": self.brand,
                "event_id": event_id,
                "idempotency_key": idempotency_key,
                "sport": raw_event.get("event", {}).get("sport", "unknown"),
                "home_team": raw_event.get("event", {}).get("homeName", ""),
                "away_team": raw_event.get("event", {}).get("awayName", ""),
                "timestamp": datetime.utcnow().isoformat(),
                "markets": [],
            }

            # Extract markets
            for offer in raw_event.get("betOffers", []):
                market = {
                    "type": self.map_market_type(
                        offer.get("criterion", {}).get("label", "")
                    ),
                    "odds": [],
                }

                for outcome in offer.get("outcomes", []):
                    market["odds"].append(
                        {
                            "label": outcome.get("label", ""),
                            "odds": (
                                outcome.get("odds", 0) / 1000.0
                                if outcome.get("odds")
                                else 0
                            ),
                            "american_odds": outcome.get("oddsAmerican", ""),
                        }
                    )

                if market["odds"]:
                    normalized["markets"].append(market)

            return normalized

        except Exception as e:
            errors_total.labels(book=self.brand, type="normalize").inc()
            logger.error(f"Error normalizing event: {e}")
            return None

    def map_market_type(self, label: str) -> str:
        """Map Kambi market labels to standard types"""
        label_lower = label.lower()
        if "spread" in label_lower or "handicap" in label_lower:
            return "spread"
        elif "total" in label_lower or "over/under" in label_lower:
            return "total"
        elif "moneyline" in label_lower or "match winner" in label_lower:
            return "h2h"
        return "other"

    def publish_events(self, events: List[Dict]):
        """Publish events to Redis after realness check"""
        if not events:
            return

        # Add to realness buffer
        for event in events:
            self.realness_gate.add_event(event)

        # Compute realness score
        score = self.realness_gate.compute_realness(events)
        realness_score_metric.labels(book=self.brand).set(score)

        # Check gate
        if REAL_ONLY and not self.realness_gate.allow_write(score):
            logger.error(f"Realness check failed for {self.brand}: score={score:.3f}")
            collector_up.labels(book=self.brand).set(0)
            if REAL_ONLY:
                sys.exit(2)  # Exit with code 2 for realness failure
            return

        # Publish to Redis
        channel = f"odds.{self.brand}"
        for event in events:
            try:
                self.redis_client.publish(channel, json.dumps(event))
                ticks_total.labels(book=self.brand).inc()
            except Exception as e:
                errors_total.labels(book=self.brand, type="publish").inc()
                logger.error(f"Error publishing to Redis: {e}")

    def collect_cycle(self):
        """Single collection cycle"""
        all_events = []

        for endpoint in self.config["endpoints"]:
            data = self.fetch_odds(endpoint)
            if not data:
                continue

            # Extract events from response
            events = []
            if isinstance(data, dict):
                if "events" in data:
                    events = data["events"]
                elif "betOffers" in data:
                    events = [data]  # Single event
            elif isinstance(data, list):
                events = data

            # Normalize events
            for raw_event in events:
                normalized = self.normalize_event(raw_event)
                if normalized:
                    all_events.append(normalized)

        # Update sliding window for odds_15m metric
        now = time.time()
        self.odds_15m_window.append((now, len(all_events)))
        # Keep only last 15 minutes
        self.odds_15m_window = [
            (t, c) for t, c in self.odds_15m_window if now - t < 900
        ]
        # Sum events in window
        total_15m = sum(c for _, c in self.odds_15m_window)
        odds_15m.labels(book=self.brand).set(total_15m)

        # Publish if we have events
        if all_events:
            self.publish_events(all_events)
            collector_up.labels(book=self.brand).set(1)
            logger.info(f"Collected {len(all_events)} events for {self.brand}")
        else:
            logger.warning(f"No events collected for {self.brand}")

    def run(self):
        """Main collection loop"""
        logger.info(f"Starting Kambi collector for brand: {self.brand}")

        while True:
            try:
                self.collect_cycle()
            except Exception as e:
                errors_total.labels(book=self.brand, type="cycle").inc()
                logger.error(f"Collection cycle error: {e}")
                collector_up.labels(book=self.brand).set(0)

            time.sleep(COLLECTION_INTERVAL)


@app.route("/healthz")
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "brand": KAMBI_BRAND})


@app.route("/metrics")
def metrics():
    """Metrics endpoint (redirect to Prometheus)"""
    return "", 200


def start_health_server():
    """Start Flask health server in background"""
    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=HEALTH_PORT), daemon=True
    ).start()


def main():
    # Start Prometheus metrics server
    start_http_server(METRICS_PORT)
    logger.info(f"Metrics server started on port {METRICS_PORT}")

    # Start health server
    start_health_server()
    logger.info(f"Health server started on port {HEALTH_PORT}")

    # Start collector
    collector = KambiCollector()
    collector.run()


if __name__ == "__main__":
    main()

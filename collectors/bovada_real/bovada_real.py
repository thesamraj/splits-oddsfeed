#!/usr/bin/env python3
"""
Bovada Real Odds Collector - Fixed parser with realness validation
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import json
import time
import redis
import logging
import requests
from datetime import datetime, timezone
from prometheus_client import Counter, Gauge, start_http_server
from flask import Flask, jsonify

# Import realness gate
from infra.realness_gate import RealnessGate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bovada_real")

# Prometheus metrics
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

# Flask app for /healthz
app = Flask(__name__)


@app.route("/healthz")
def healthz():
    return jsonify(
        {
            "status": "healthy",
            "timestamp": time.time(),
            "last_success": LAST_SUCCESS_TS._value.get(("bovada",), 0),
        }
    )


class BovadaRealCollector:
    def __init__(self):
        self.redis_client = redis.from_url("redis://broker:6379/0")

        # Initialize RealnessGate with startup guard
        real_only = os.getenv("REAL_ONLY", "true").lower() == "true"
        if real_only:
            self.gate = RealnessGate(strict_mode=True)
            if not self.gate:
                logger.error(
                    "FATAL: REAL_ONLY=true but RealnessGate failed to initialize"
                )
                sys.exit(1)
            logger.info("RealnessGate initialized with strict_mode=True")
        else:
            self.gate = RealnessGate(strict_mode=False)
            logger.warning(
                "RealnessGate initialized with strict_mode=False (REAL_ONLY not set)"
            )
        self.book = "bovada"

        # Initialize metrics
        COLLECTOR_UP.labels(book=self.book).set(1)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.bovada.lv/",
            }
        )

        self.endpoints = {
            "nfl": "https://www.bovada.lv/services/sports/event/v2/events/A/description/football/nfl",
            "nba": "https://www.bovada.lv/services/sports/event/v2/events/A/description/basketball/nba",
            "mlb": "https://www.bovada.lv/services/sports/event/v2/events/A/description/baseball/mlb",
            "nhl": "https://www.bovada.lv/services/sports/event/v2/events/A/description/hockey/nhl",
            "ncaaf": "https://www.bovada.lv/services/sports/event/v2/events/A/description/football/college-football",
            "ncaab": "https://www.bovada.lv/services/sports/event/v2/events/A/description/basketball/college-basketball",
        }

        self.stats = {
            "events_processed": 0,
            "odds_published": 0,
            "errors": 0,
            "last_success": None,
        }

    def fetch_sport(self, sport, url):
        """Fetch real odds from Bovada with rate limiting"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=15)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    HTTP_429_TOTAL.labels(book=self.book).inc()
                    # Exponential backoff with jitter
                    sleep_time = (2**attempt) + (time.time() % 1)  # Add jitter
                    logger.warning(f"Rate limited, sleeping {sleep_time:.1f}s")
                    time.sleep(sleep_time)
                else:
                    logger.warning(f"Got status {response.status_code} for {sport}")
            except Exception as e:
                ERRORS_TOTAL.labels(book=self.book, type="network").inc()
                logger.error(f"Error fetching {sport}: {e}")
            self.stats["errors"] += 1
        return None

    def parse_bovada_event(self, event, sport):
        """Parse Bovada event - fixed for nested structure"""
        odds_rows = []

        try:
            event_id = f"bovada_{event.get('id', '')}"

            # Extract teams from competitors
            competitors = event.get("competitors", [])
            if len(competitors) != 2:
                return odds_rows

            away_team = competitors[0].get("name", "Unknown")
            home_team = competitors[1].get("name", "Unknown")

            # Process display groups
            for group in event.get("displayGroups", []):
                for market in group.get("markets", []):
                    market_desc = market.get("description", "").lower()
                    outcomes = market.get("outcomes", [])

                    # Moneyline
                    if "moneyline" in market_desc or "money line" in market_desc:
                        for outcome in outcomes:
                            desc = outcome.get("description", "")
                            price = outcome.get("price", {})
                            american = price.get("american")

                            if american:
                                if home_team in desc:
                                    home_price = american
                                elif away_team in desc:
                                    away_price = american

                        if "home_price" in locals() and "away_price" in locals():
                            odds_rows.append(
                                {
                                    "event_id": event_id,
                                    "sport": sport,
                                    "home_team": home_team,
                                    "away_team": away_team,
                                    "market": "h2h",
                                    "price_home": (
                                        int(home_price) if home_price else None
                                    ),
                                    "price_away": (
                                        int(away_price) if away_price else None
                                    ),
                                }
                            )

                    # Spreads
                    elif "spread" in market_desc:
                        for outcome in outcomes:
                            price = outcome.get("price", {})
                            american = price.get("american")
                            handicap = price.get("handicap")

                            if american and handicap is not None:
                                odds_rows.append(
                                    {
                                        "event_id": event_id,
                                        "sport": sport,
                                        "home_team": home_team,
                                        "away_team": away_team,
                                        "market": "spreads",
                                        "line": float(handicap),
                                        "outcome_price": int(american),
                                    }
                                )

                    # Totals
                    elif "total" in market_desc:
                        for outcome in outcomes:
                            desc = outcome.get("description", "").lower()
                            price = outcome.get("price", {})
                            american = price.get("american")
                            total = price.get("handicap")

                            if american and total is not None:
                                odds_rows.append(
                                    {
                                        "event_id": event_id,
                                        "sport": sport,
                                        "home_team": home_team,
                                        "away_team": away_team,
                                        "market": "totals",
                                        "total": float(total),
                                        "outcome_price": int(american),
                                    }
                                )

        except Exception as e:
            logger.debug(f"Parse error: {e}")

        return odds_rows

    def run(self):
        logger.info("Starting Bovada REAL collector")

        # Initialize collector status metric
        COLLECTOR_UP.labels(book=self.book).set(1)

        cycle = 0

        while True:
            try:
                cycle += 1
                logger.info(f"\n=== Cycle {cycle} ===")

                all_odds = []

                for sport, url in self.endpoints.items():
                    data = self.fetch_sport(sport, url)

                    if data:
                        # Handle nested structure
                        events = []
                        for item in data:
                            if "events" in item:
                                events.extend(item["events"])

                        if events:
                            logger.info(f"{sport.upper()}: {len(events)} events")
                            for event in events:
                                odds = self.parse_bovada_event(event, sport)
                                all_odds.extend(odds)
                                if odds:
                                    self.stats["events_processed"] += 1

                        time.sleep(2)

                # Validate with realness gate before publishing
                if all_odds:
                    # Prepare data for validation
                    validation_data = {
                        "events": all_odds[:200]
                    }  # Buffer first 200 events

                    # Compute realness score
                    score = self.gate.compute_realness(validation_data)
                    REALNESS_SCORE.labels(book=self.book).set(score)

                    # Check if write allowed
                    real_only = os.getenv("REAL_ONLY", "true").lower() == "true"
                    min_threshold = float(os.getenv("REALNESS_THRESHOLD", "0.9"))

                    if real_only and not self.gate.allow_write(score, min_threshold):
                        REALNESS_OK.labels(book=self.book).set(0)
                        logger.error(
                            f"REALNESS_OK=0 score={score:.2f} (threshold={min_threshold})"
                        )
                        ERRORS_TOTAL.labels(book=self.book, type="realness").inc()

                        if os.getenv("EXIT_ON_FAKE", "false").lower() == "true":
                            logger.error(
                                f"Exiting due to fake data detection (score={score:.2f})"
                            )
                            sys.exit(2)
                        continue

                    # Realness check passed
                    REALNESS_OK.labels(book=self.book).set(1)
                    logger.info(f"REALNESS_OK=1 score={score:.2f}")

                    # Publish to Redis
                    message = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "source": "bovada_real",
                        "realness_score": score,
                        "events": all_odds,
                    }

                    self.redis_client.publish("odds.raw.bovada", json.dumps(message))

                    # Update metrics
                    ODDS_UPSERTS_TOTAL.labels(book=self.book).inc(len(all_odds))
                    TICKS_TOTAL.labels(book=self.book).inc()
                    LAST_SUCCESS_TS.labels(book=self.book).set(time.time())

                    self.stats["odds_published"] += len(all_odds)
                    logger.info(f"Published {len(all_odds)} odds")
                    logger.info(f"Stats: {self.stats}")

                time.sleep(15)

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Error: {e}")
                time.sleep(30)


if __name__ == "__main__":
    # Start metrics server
    metrics_port = int(os.getenv("METRICS_PORT", "9090"))
    start_http_server(metrics_port)
    logger.info(f"Metrics server started on port {metrics_port}")

    # Start Flask health server in thread
    import threading

    health_thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=9091, debug=False)
    )
    health_thread.daemon = True
    health_thread.start()

    BovadaRealCollector().run()

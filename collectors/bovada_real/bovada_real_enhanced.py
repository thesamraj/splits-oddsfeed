#!/usr/bin/env python3
"""
Bovada Real Odds Collector - Enhanced with explainable realness
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import json
import time
import redis
import logging
import requests
import random
import threading
import hashlib
import re
from collections import OrderedDict
from datetime import datetime, timezone
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from flask import Flask, jsonify

# Import explainable realness
from collectors.base.explainable_realness import ExplainableRealnessGate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bovada_real")

# Standard metrics
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

# Dedup metrics
DEDUP_DROPPED_TOTAL = Counter(
    "dedup_dropped_total", "Total duplicate events dropped", ["book", "reason"]
)
HTTP_STATUS_COUNT = Counter(
    "http_status_count", "HTTP status codes", ["book", "status"]
)
PAYLOAD_BYTES = Histogram(
    "payload_bytes", "Response payload size", ["book", "endpoint"]
)
# Parser/schema metrics
PARSER_SCHEMA_CHANGES = Counter(
    "parser_schema_changes", "Schema changes detected", ["book"]
)
# REQUEST_DURATION is already defined in explainable_realness.py


class BovadaRealCollector:
    def __init__(self):
        self.redis_client = redis.from_url(
            os.getenv("REDIS_URL", "redis://broker:6379/0")
        )
        self.book = "bovada"

        # Initialize explainable realness gate
        self.gate = ExplainableRealnessGate(book=self.book)

        # Flask app for realness endpoints
        self.flask_app = Flask(__name__)
        self._setup_routes()

        # Start Flask in background
        health_port = int(os.getenv("HEALTH_PORT", "9091"))
        threading.Thread(
            target=lambda: self.flask_app.run(
                host="0.0.0.0", port=health_port, debug=False
            ),
            daemon=True,
        ).start()

        # Initialize metrics
        COLLECTOR_UP.labels(book=self.book).set(1)

        # Dedup ring buffer - LRU with 30m TTL
        self.dedup_buffer = OrderedDict()  # key -> timestamp
        self.dedup_ttl = 1800  # 30 minutes
        self.snapshot_seen = {}  # Track snapshot hashes
        self.max_dedup_size = 10000

        # Schema tracking
        self.last_schema_keys = None
        self.sanitized_sample = None

        # Stats tracking
        self.stats = {"events_processed": 0, "odds_published": 0}

        # Team name normalization map
        self.team_aliases = {
            "ny jets": "New York Jets",
            "ny giants": "New York Giants",
            "la rams": "Los Angeles Rams",
            "la chargers": "Los Angeles Chargers",
            "tb": "Tampa Bay",
            "sf": "San Francisco",
            "kc": "Kansas City",
            "ne": "New England",
            "no": "New Orleans",
        }

        # Real Bovada endpoints - log at startup
        self.endpoints = {
            "nfl": "https://www.bovada.lv/services/sports/event/v2/events/A/description/football/nfl",
            "nba": "https://www.bovada.lv/services/sports/event/v2/events/A/description/basketball/nba",
            "mlb": "https://www.bovada.lv/services/sports/event/v2/events/A/description/baseball/mlb",
            "nhl": "https://www.bovada.lv/services/sports/event/v2/events/A/description/hockey/nhl",
            "ncaaf": "https://www.bovada.lv/services/sports/event/v2/events/A/description/football/college-football",
            "ncaab": "https://www.bovada.lv/services/sports/event/v2/events/A/description/basketball/college-basketball",
            "tennis": "https://www.bovada.lv/services/sports/event/v2/events/A/description/tennis",
        }

        # LIGHT_SOCCER mode: fetch soccer but filter to top competitions
        if os.getenv("LIGHT_SOCCER", "false").lower() == "true":
            self.endpoints["soccer"] = (
                "https://www.bovada.lv/services/sports/event/v2/events/A/description/soccer"
            )
            logger.info("LIGHT_SOCCER mode enabled - will filter to top competitions")
            self.soccer_filter = True
            # Top competitions to keep
            self.top_competitions = {
                "mls",
                "epl",
                "premier league",
                "champions league",
                "europa",
                "world cup",
                "euro",
                "copa america",
                "bundesliga",
                "la liga",
                "serie a",
                "ligue 1",
            }
            self.max_soccer_payload = 25 * 1024 * 1024  # 25MB cap
        else:
            self.soccer_filter = False

        logger.info(
            f"Bovada collector initialized with {len(self.endpoints)} endpoints:"
        )
        for sport, url in self.endpoints.items():
            logger.info(f"  {sport}: {url}")

        # User agents for rotation
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Version/17.0 Mobile Safari/604.1",
        ]

        self.stats = {
            "events_processed": 0,
            "odds_published": 0,
            "errors": 0,
            "last_success": None,
        }

    def _setup_routes(self):
        """Setup Flask routes for observability"""

        @self.flask_app.route("/healthz")
        def healthz():
            return jsonify(
                {
                    "status": "healthy",
                    "timestamp": time.time(),
                    "book": self.book,
                    "last_success": LAST_SUCCESS_TS._value.get((self.book,), 0),
                    "realness_ok": REALNESS_OK._value.get((self.book,), 0),
                }
            )

        @self.flask_app.route("/realness/report")
        def realness_report():
            return jsonify(self.gate.get_report())
        
        @self.flask_app.route("/metrics")
        def metrics():
            """Expose Prometheus metrics"""
            return Response(generate_latest(REGISTRY), mimetype=CONTENT_TYPE_LATEST)

        @self.flask_app.route("/realness/sample")
        def realness_sample():
            # Return our sanitized sample if available
            if self.sanitized_sample:
                return jsonify(self.sanitized_sample)
            return jsonify(self.gate.get_sample())

    def clean_dedup_buffer(self):
        """Clean expired entries from dedup buffer"""
        now = time.time()
        expired = [k for k, v in self.dedup_buffer.items() if now - v > self.dedup_ttl]
        for k in expired:
            del self.dedup_buffer[k]

        # Trim to max size if needed
        while len(self.dedup_buffer) > self.max_dedup_size:
            self.dedup_buffer.popitem(last=False)

    def build_market_key(self, event_id, market_type, selection, line, price):
        """Build a stable market key for deduplication.
        Uses line rounding (0.5 for spreads/totals) and price banding (5-cent intervals).
        """
        # Round lines to 0.5 for spread/total
        if market_type in ["spread", "total"] and line is not None:
            line_rounded = round(float(line) * 2) / 2  # Round to nearest 0.5
        else:
            line_rounded = 0.0

        # Band prices to 5-cent intervals to reduce jitter
        price_banded = round(float(price) / 5) * 5 if price else 0

        return f"{event_id}|{market_type}|{selection}|{line_rounded}|{price_banded}"

    def compute_snapshot_hash(self, data):
        """Compute hash of sorted outcomes for snapshot detection.
        Returns a short hash of the entire data structure.
        """
        outcomes = []
        for item in data:
            if "events" in item:
                for event in item["events"]:
                    event_id = event.get("id", "")
                    for group in event.get("displayGroups", []):
                        for market in group.get("markets", []):
                            market_id = market.get("id", "")
                            for outcome in market.get("outcomes", []):
                                outcome_id = outcome.get("id", "")
                                price = outcome.get("price", {}).get("american", 0)
                                outcomes.append(
                                    f"{event_id}:{market_id}:{outcome_id}:{price}"
                                )

        # Sort outcomes for stable hash
        sorted_json = json.dumps(sorted(outcomes), separators=(",", ":"))
        return hashlib.sha1(sorted_json.encode()).hexdigest()[:12]

    def is_duplicate(self, event_id, market_type, selection, line, price):
        """Check if this is a duplicate event using market key"""
        # Build market key with banding
        key = self.build_market_key(event_id, market_type, selection, line, price)
        now = time.time()

        if key in self.dedup_buffer:
            # Update timestamp
            self.dedup_buffer.move_to_end(key)
            self.dedup_buffer[key] = now
            DEDUP_DROPPED_TOTAL.labels(book=self.book, reason="market_key").inc()
            return True

        # Add to buffer
        self.dedup_buffer[key] = now

        # Clean periodically
        if random.random() < 0.01:  # 1% chance
            self.clean_dedup_buffer()

        return False

    def is_snapshot_duplicate(self, sport, snapshot_hash):
        """Check if this snapshot hash was seen recently (within 30s)"""
        key = f"{sport}:{snapshot_hash}"
        now = time.time()

        if key in self.snapshot_seen:
            last_seen = self.snapshot_seen[key]
            if now - last_seen < 30:  # Within 30 seconds
                return True
            # Update timestamp
            self.snapshot_seen[key] = now
        else:
            # New snapshot
            self.snapshot_seen[key] = now
            # Clean old entries if buffer is too large
            if len(self.snapshot_seen) > 100:
                # Remove entries older than 60s
                self.snapshot_seen = {
                    k: v for k, v in self.snapshot_seen.items() if now - v < 60
                }

        return False

    def parse_event_id(self, event):
        """Extract stable event ID"""
        if event.get("id"):
            return f"bovada_{event['id']}"

        # Fallback to SHA1 hash
        path = event.get("path", [])
        league_id = path[0].get("id", "") if path else ""
        competitors = event.get("competitors", [])
        start_time = event.get("startTime", 0)

        if len(competitors) >= 2 and start_time:
            # Use canonical names for stable hash
            home, away = self.parse_teams(event)
            if home and away:
                key = f"{league_id}:{home}:{away}:{start_time}"
                return f"bovada_{hashlib.sha1(key.encode()).hexdigest()[:12]}"

        return None

    def parse_teams(self, event):
        """Extract and normalize team names"""
        competitors = event.get("competitors", [])
        if len(competitors) < 2:
            return None, None

        home_team = None
        away_team = None

        for comp in competitors:
            name = comp.get("name", "")
            # Clean up name
            name = re.sub(r"^\d+\.?\s*", "", name)  # Remove rankings
            name = re.sub(r"\s*\([^)]+\)", "", name)  # Remove parentheticals
            name = name.strip()

            # Apply aliases
            lower_name = name.lower()
            if lower_name in self.team_aliases:
                name = self.team_aliases[lower_name]
            elif not name[0].isupper() and len(name) > 1:
                name = name.title()

            if comp.get("home"):
                home_team = name
            else:
                away_team = name

        return home_team, away_team

    def parse_start_time(self, event):
        """Parse and validate start time from Bovada event"""
        start_time = event.get("startTime")

        # Reject missing or zero timestamps
        if not start_time or start_time == 0:
            return None

        try:
            # Bovada uses epoch milliseconds
            if isinstance(start_time, (int, float)):
                dt = datetime.fromtimestamp(start_time / 1000, tz=timezone.utc)
            else:
                # Fallback for ISO strings
                dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))

            # Reject if more than 1 year out or in the past by more than 1 day
            now = datetime.now(timezone.utc)
            days_diff = (dt - now).days
            if days_diff > 365 or days_diff < -1:
                return None

            return dt.isoformat()
        except Exception as e:
            logger.debug(f"Failed to parse start_time {start_time}: {e}")
            return None

    def fetch_sport(self, sport, url):
        """Fetch odds for a sport with observability"""
        start_time = time.time()
        try:
            # Rotate user agents
            headers = {
                "User-Agent": random.choice(self.user_agents),
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": "https://www.bovada.lv/sports",
                "DNT": "1",
            }

            response = requests.get(url, headers=headers, timeout=15)
            duration = time.time() - start_time

            # Check payload size for soccer if LIGHT_SOCCER enabled
            payload_size = len(response.content)
            if sport == "soccer" and self.soccer_filter:
                if payload_size > self.max_soccer_payload:
                    logger.warning(
                        f"Soccer payload {payload_size/1024/1024:.1f}MB > {self.max_soccer_payload/1024/1024}MB cap, skipping"
                    )
                    HTTP_STATUS_COUNT.labels(book=self.book, status="payload_cap").inc()
                    return None

            # Record metrics
            # Use REQUEST_DURATION from explainable_realness (different label structure)
            HTTP_STATUS_COUNT.labels(
                book=self.book, status=str(response.status_code)
            ).inc()
            PAYLOAD_BYTES.labels(book=self.book, endpoint=sport).observe(payload_size)

            # Record upstream metrics
            self.gate.record_upstream(
                url=url,
                status=response.status_code,
                content_type=response.headers.get("content-type", ""),
                payload_bytes=len(response.content),
                duration=duration,
            )

            if response.status_code == 200:
                logger.debug(
                    f"Fetched {sport}: {len(response.content)} bytes in {duration:.2f}s"
                )
                return response.json()
            elif response.status_code == 429:
                HTTP_429_TOTAL.labels(book=self.book).inc()
                logger.warning(f"Rate limited for {sport}")
                return None
            else:
                logger.warning(f"Got {response.status_code} for {sport}")
                return None
        except Exception as e:
            ERRORS_TOTAL.labels(book=self.book, type="fetch").inc()
            logger.error(f"Error fetching {sport}: {e}")
            return None

    def parse_markets(self, event, home_team, away_team):
        """Parse and normalize markets"""
        markets = []

        display_groups = event.get("displayGroups", [])
        for group in display_groups:
            for market in group.get("markets", []):
                market_desc = market.get("description", "").lower()
                market_id = market.get("id", "")

                # Skip futures and non-game markets
                if any(
                    x in market_desc
                    for x in ["futures", "outright", "winner", "champion"]
                ):
                    continue

                for outcome in market.get("outcomes", []):
                    if outcome.get("status") in ["SUSPENDED", "DEACTIVATED"]:
                        continue

                    price_obj = outcome.get("price", {})
                    american = price_obj.get("american")

                    if not american:
                        continue

                    outcome_desc = outcome.get("description", "")
                    handicap = price_obj.get("handicap")

                    # Determine market type and selection
                    if "moneyline" in market_desc or "money line" in market_desc:
                        mkt_type = "h2h"
                        if home_team and home_team in outcome_desc:
                            selection = "home"
                        elif away_team and away_team in outcome_desc:
                            selection = "away"
                        else:
                            continue
                        line = None

                    elif "spread" in market_desc or "point spread" in market_desc:
                        mkt_type = "spread"
                        if home_team and home_team in outcome_desc:
                            selection = "home"
                        elif away_team and away_team in outcome_desc:
                            selection = "away"
                        else:
                            continue
                        # Normalize pk to 0.0
                        line = 0.0 if str(handicap).lower() == "pk" else handicap

                    elif "total" in market_desc or "over/under" in market_desc:
                        mkt_type = "total"
                        if "over" in outcome_desc.lower():
                            selection = "over"
                        elif "under" in outcome_desc.lower():
                            selection = "under"
                        else:
                            continue
                        line = handicap

                    else:
                        continue

                    markets.append(
                        {
                            "market_type": mkt_type,
                            "selection": selection,
                            "line": float(line) if line else None,
                            "price": int(american),
                            "market_id": market_id,
                            "outcome_id": outcome.get("id", ""),
                        }
                    )

        return markets

    def parse_bovada_event(self, event, sport):
        """Parse Bovada event to our format with dedup"""
        odds_rows = []

        # Filter soccer to top competitions if enabled
        if sport == "soccer" and self.soccer_filter:
            path = event.get("path", [])
            competition = " ".join([p.get("description", "") for p in path]).lower()
            # Check if any top competition keyword matches
            if not any(comp in competition for comp in self.top_competitions):
                return odds_rows  # Skip non-top competitions

        try:
            # Extract components
            event_id = self.parse_event_id(event)
            if not event_id:
                return odds_rows

            home_team, away_team = self.parse_teams(event)
            if not home_team or not away_team:
                return odds_rows

            start_time = self.parse_start_time(event)
            if not start_time:
                return odds_rows

            # Get league from path
            path = event.get("path", [])
            league = (
                path[0].get("description", sport.upper()) if path else sport.upper()
            )

            # Parse markets
            markets = self.parse_markets(event, home_team, away_team)

            # Filter out futures and noise
            if any(
                x in event.get("description", "").lower()
                for x in ["futures", "outright", "winner"]
            ):
                return odds_rows

            for mkt in markets:
                # Check dedup
                if self.is_duplicate(
                    event_id,
                    mkt["market_type"],
                    mkt["selection"],
                    mkt["line"],
                    mkt["price"],
                ):
                    continue

                # Build odds row
                row = {
                    "event_id": event_id,
                    "sport": sport.upper(),
                    "league": league,
                    "home_team": home_team,
                    "away_team": away_team,
                    "market_type": mkt["market_type"],
                    "selection": mkt["selection"],
                    "line": mkt["line"],
                    "price": mkt["price"],
                    "start_time": start_time,
                }

                # Add h2h prices for compatibility
                if mkt["market_type"] == "h2h":
                    if mkt["selection"] == "home":
                        row["home_price"] = mkt["price"]
                    elif mkt["selection"] == "away":
                        row["away_price"] = mkt["price"]

                odds_rows.append(row)

        except Exception as e:
            logger.debug(f"Parse error for event: {e}")

        return odds_rows

    def run(self):
        logger.info("Starting Bovada REAL collector (enhanced)")

        # Initialize collector status
        COLLECTOR_UP.labels(book=self.book).set(1)

        cycle = 0

        while True:
            try:
                cycle += 1
                logger.info(f"\n=== Cycle {cycle} ===")

                all_odds = []

                # Fetch each sport with small delay
                for sport, url in self.endpoints.items():
                    data = self.fetch_sport(sport, url)

                    if data:
                        # Check for snapshot duplicate
                        snapshot_hash = self.compute_snapshot_hash(data)
                        if self.is_snapshot_duplicate(sport, snapshot_hash):
                            DEDUP_DROPPED_TOTAL.labels(
                                book=self.book, reason="snapshot"
                            ).inc()
                            logger.debug(
                                f"Skipping {sport} - snapshot unchanged (hash: {snapshot_hash})"
                            )
                            continue

                        # Check for schema changes
                        current_keys = set()
                        for item in data:
                            if isinstance(item, dict):
                                current_keys.update(item.keys())

                        if (
                            self.last_schema_keys
                            and current_keys != self.last_schema_keys
                        ):
                            PARSER_SCHEMA_CHANGES.labels(book=self.book).inc()
                            logger.info(
                                f"Schema change detected: {current_keys ^ self.last_schema_keys}"
                            )
                            self.last_schema_keys = current_keys
                        elif not self.last_schema_keys:
                            self.last_schema_keys = current_keys

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

                            # Store sanitized sample on first run
                            if not self.sanitized_sample and events:
                                self.sanitized_sample = {
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                    "sport": sport,
                                    "event_count": len(events),
                                    "sample_event": {
                                        "id": events[0].get("id"),
                                        "competitors": len(
                                            events[0].get("competitors", [])
                                        ),
                                        "markets": len(
                                            events[0]
                                            .get("displayGroups", [{}])[0]
                                            .get("markets", [])
                                        ),
                                    },
                                }

                        # Small delay between sports
                        time.sleep(random.uniform(1, 3))

                # Validate with realness gate
                if all_odds:
                    # Prepare validation data
                    validation_data = {
                        "events": all_odds[:500]
                    }  # Sample for validation

                    # Compute realness with explanations
                    score, failures = self.gate.compute_realness(validation_data)
                    REALNESS_SCORE.labels(book=self.book).set(score)

                    # Check if we should enforce
                    if self.gate.should_enforce(score):
                        REALNESS_OK.labels(book=self.book).set(0)
                        logger.error(
                            f"REALNESS_OK=0 score={score:.2f} reasons={failures[:3]}"
                        )

                        # Quarantine if enabled
                        if self.gate.quarantine:
                            self.gate.quarantine_events(all_odds, score, failures)
                            logger.info(
                                f"Quarantined {len(all_odds)} events with score {score:.2f}"
                            )

                        # Don't publish to main stream
                        continue

                    # Passed realness check (or in warm-up)
                    REALNESS_OK.labels(book=self.book).set(1)
                    warmup_tag = "(warm-up)" if self.gate.is_warming_up else ""
                    logger.info(f"REALNESS_OK=1 score={score:.2f} {warmup_tag}")

                    # Publish to Redis
                    message = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "source": "bovada_real_enhanced",
                        "realness_score": score,
                        "is_warmup": self.gate.is_warming_up,
                        "events": all_odds,
                    }

                    self.redis_client.publish("odds.raw.bovada", json.dumps(message))

                    # Update metrics
                    ODDS_UPSERTS_TOTAL.labels(book=self.book).inc(len(all_odds))
                    TICKS_TOTAL.labels(book=self.book).inc()
                    LAST_SUCCESS_TS.labels(book=self.book).set(time.time())

                    self.stats["odds_published"] += len(all_odds)
                    logger.info(
                        f"Published {len(all_odds)} odds, total: {self.stats['odds_published']}"
                    )

                # Add jitter to interval
                base_interval = int(os.getenv("COLLECTION_INTERVAL", "60"))
                jitter = random.randint(-10, 10)
                sleep_time = max(20, base_interval + jitter)
                logger.info(f"Sleeping {sleep_time}s until next cycle")
                time.sleep(sleep_time)

            except KeyboardInterrupt:
                break
            except Exception as e:
                ERRORS_TOTAL.labels(book=self.book, type="runtime").inc()
                logger.error(f"Runtime error: {e}")
                time.sleep(30)


if __name__ == "__main__":
    # Start metrics server
    metrics_port = int(os.getenv("METRICS_PORT", "9090"))
    start_http_server(metrics_port)
    logger.info(f"Prometheus metrics on :{metrics_port}")

    # Run collector
    BovadaRealCollector().run()

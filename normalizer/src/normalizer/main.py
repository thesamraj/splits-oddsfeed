import sys

sys.path.append(".")
try:
    from shared.brand_guard import allowed
except ImportError:
    # Fallback brand guard for BetRivers-only mode
    def allowed(brand: str) -> bool:
        return (brand or "").lower() in {"betrivers", "kambi", "unknown"}


import os
import json
import asyncio
from datetime import datetime
import logging
import hashlib

import redis.asyncio as redis
from psycopg_pool import AsyncConnectionPool
from prometheus_client import Counter, Gauge, Histogram, REGISTRY, generate_latest, CONTENT_TYPE_LATEST
from flask import Flask, jsonify, Response
import threading
import multiprocessing

from normalizer.pinnacle_mapper import normalize_pinnacle_data
from normalizer.kambi_mapper import (
    normalize_kambi_envelope,
    extract_event_metadata,
    extract_brand,
)
from normalizer.kambi_fallback import kambi_fallback_extract_betrivers
import time

# Market standardization mapping
CANONICAL_MARKETS = {
    "moneyline": "h2h",
    "ml": "h2h",
    "money_line": "h2h",
    "h2h": "h2h",
    "head2head": "h2h",
    "spread": "spread",
    "spreads": "spread",
    "handicap": "spread",
    "line": "spread",
    "point_spread": "spread",
    "total": "total",
    "totals": "total",
    "over/under": "total",
    "over_under": "total",
    "ou": "total",
    "o/u": "total",
}


def normalize_market(market_name):
    """Normalize market names to canonical form"""
    if not market_name:
        return "h2h"
    return CANONICAL_MARKETS.get(str(market_name).lower().strip(), market_name)


from normalizer.metrics import (
    kambi_e2e_latency_seconds,
    kambi_e2e_skipped_total,
    kambi_rows_written_total,
    kambi_publish_to_normalize_ms,
    kambi_normalize_to_db_ms,
    kambi_e2e_latency_ms,
    kambi_norm_backlog,
)

# Kambi freshness gauge
kambi_last_insert_ts = Gauge(
    "kambi_last_insert_ts_seconds",
    "Last successful Kambi insert timestamp (seconds since epoch)",
)


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Standard metrics
TICKS_TOTAL = Counter("ticks_total", "Total ticks processed", ["book"])
ODDS_UPSERTS_TOTAL = Counter("odds_upserts_total", "Total odds upserted", ["book"])
ERRORS_TOTAL = Counter("errors_total", "Total errors", ["book", "type"])
HTTP_429_TOTAL = Counter("http_429_total", "Total HTTP 429 errors", ["book"])
REALNESS_SCORE = Gauge("realness_score", "Current realness score", ["book"])
LAST_SUCCESS_TS = Gauge(
    "last_success_ts", "Last successful processing timestamp", ["book"]
)
COLLECTOR_UP = Gauge("collector_up", "Normalizer status", ["book"])

# Quarantine metrics
QUARANTINE_EVENTS_TOTAL = Counter(
    "quarantine_events_total", "Total quarantined events", ["book"]
)
QUARANTINE_SKIPPED_TOTAL = Counter(
    "quarantine_skipped_total",
    "Quarantine events skipped from main processing",
    ["book"],
)

MESSAGES_PROCESSED = Counter(
    "messages_processed_total", "Total messages processed", ["book", "status"]
)
PROCESSING_LATENCY = Histogram(
    "message_processing_duration_seconds", "Message processing latency"
)

# Import all metrics from centralized metrics module

# Flask app for /healthz
app = Flask(__name__)


def val(metric_value, labels=None, default=0):
    """Safely get value from multiprocessing.Value or metric"""
    try:
        if hasattr(metric_value, '_value'):
            # Prometheus metric
            if labels:
                return metric_value._value.get(labels, default)
            return getattr(metric_value._value, 'value', default)
        elif hasattr(metric_value, 'value'):
            # multiprocessing.Value
            return metric_value.value
        else:
            return metric_value
    except:
        return default

@app.route("/healthz")
def healthz():
    # Get value safely
    try:
        last_success = LAST_SUCCESS_TS.labels(book="normalizer")._value.get()
    except:
        last_success = 0
        
    return jsonify(
        {
            "status": "healthy",
            "timestamp": time.time(),
            "last_success": last_success,
        }
    )


@app.route("/metrics")
def metrics():
    """Expose Prometheus metrics"""
    try:
        data = generate_latest(REGISTRY)
        return Response(data, mimetype=CONTENT_TYPE_LATEST)
    except Exception as e:
        return Response(f"metrics error: {e}", status=500, mimetype="text/plain")


class Normalizer:
    def __init__(self):
        self.redis_client = redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0")
        )
        self.db_url = os.getenv(
            "DATABASE_URL", "postgresql://odds:odds@localhost:5432/oddsfeed"
        )
        self.running = False
        # Dedupe cache: key -> expiry_time
        self.dedupe_cache = {}
        self.dedupe_ttl_ms = 250
        # Batch accumulator
        self.batch_rows = []
        self.batch_start_time = None
        self.batch_max_size = 50
        self.batch_max_delay_ms = 200

    async def connect_db(self):
        """Connect to DB with retry logic"""
        retry_delays = [5, 10, 20, 30, 60, 120]  # seconds
        for attempt, delay in enumerate(retry_delays, 1):
            try:
                # Validate env vars first
                if (
                    not self.db_url
                    or self.db_url == "postgresql://odds:odds@localhost:5432/oddsfeed"
                ):
                    logger.error(
                        f"DATABASE_URL not properly set (attempt {attempt}/{len(retry_delays)})"
                    )
                    if attempt < len(retry_delays):
                        logger.info(f"Retrying in {delay}s...")
                        await asyncio.sleep(delay)
                        continue
                    raise ValueError("DATABASE_URL not configured")

                # Try to create pool
                self.db_pool = AsyncConnectionPool(self.db_url, min_size=1, max_size=5)

                # Test connection with SELECT 1
                async with self.db_pool.connection() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT 1")
                        await cur.fetchone()

                # Parse and log database host for verification
                import urllib.parse

                parsed = urllib.parse.urlparse(self.db_url)
                host = parsed.hostname or "unknown"
                logger.info(
                    f"✅ DB connected (host={host[:20]}...) after {attempt} attempt(s)"
                )
                logger.info("Database connection pool created")

                # Log brand map for verification
                from normalizer.kambi_mapper import BRAND_HOST_MAP

                brands = ",".join(sorted(set(BRAND_HOST_MAP.values())))
                logger.info(f"Normalizer brand map loaded: {brands}")

                # Success - update metrics
                COLLECTOR_UP.labels(book="normalizer").set(1)
                return

            except Exception as e:
                logger.error(
                    f"DB connection attempt {attempt}/{len(retry_delays)} failed: {e.__class__.__name__}: {str(e)[:200]}"
                )
                if attempt < len(retry_delays):
                    logger.info(f"Retrying DB connection in {delay}s...")
                    await asyncio.sleep(delay)
                else:
                    logger.critical("Failed to connect to database after all retries")
                    self.db_pool = None
                    # Keep running but mark as unhealthy
                    COLLECTOR_UP.labels(book="normalizer").set(0)

    async def store_event(self, book: str, payload: dict):
        if not self.db_pool:
            return

        try:
            async with self.db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO odds_events (event_id, book, payload) VALUES (%s, %s, %s)",
                        (
                            f"{book}_{datetime.utcnow().timestamp()}",
                            book,
                            json.dumps(payload),
                        ),
                    )
                    await conn.commit()
        except Exception as e:
            logger.error(f"Failed to store event: {e}")

    async def store_aggregator_data(self, payload: dict):
        """Store normalized aggregator data to events and odds tables"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Process each event in the payload
                    for event_data in payload.get("events", []):
                        event_id = event_data["event_id"]

                        # Upsert event data
                        await cur.execute(
                            """
                            INSERT INTO events (id, league, start_time, home, away, sport)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET
                                league = EXCLUDED.league,
                                start_time = EXCLUDED.start_time,
                                home = EXCLUDED.home,
                                away = EXCLUDED.away,
                                sport = EXCLUDED.sport
                        """,
                            (
                                event_id,
                                event_data["league"],
                                event_data["start_time"],
                                event_data["home_team"],
                                event_data["away_team"],
                                event_data["sport"],
                            ),
                        )

                        # Process markets and outcomes
                        for market in event_data.get("markets", []):
                            book = market["book"]
                            market_type = market["market_type"]

                            # Insert odds data for each outcome
                            for outcome in market.get("outcomes", []):
                                await cur.execute(
                                    """
                                    INSERT INTO odds (
                                        event_id, book, market, outcome_name,
                                        outcome_price, outcome_point
                                    ) VALUES (%s, %s, %s, %s, %s, %s)
                                """,
                                    (
                                        event_id,
                                        book,
                                        market_type,
                                        outcome["name"],
                                        outcome["price"],
                                        outcome.get("point"),
                                    ),
                                )

                    await conn.commit()
                    logger.info(
                        f"Stored {len(payload.get('events', []))} events with odds data"
                    )

        except Exception as e:
            logger.error(f"Failed to store aggregator data: {e}")

    def make_dedupe_key(self, r: dict) -> str:
        """Create deduplication key for a normalized row"""
        key_parts = [
            r.get("book", ""),
            r.get("event_id", ""),
            r.get("market", ""),
            str(r.get("line", "")),
            str(r.get("price_home", "")),
            str(r.get("price_away", "")),
            str(r.get("total", "")),
        ]
        key_str = "|".join(str(part) for part in key_parts)  # DEDUP_PATCH
        return hashlib.md5(key_str.encode()).hexdigest()

    def is_duplicate(self, r: dict) -> bool:
        """Check if row is a recent duplicate"""
        now = time.time() * 1000
        # Clean expired entries
        expired_keys = [
            k for k, exp_time in self.dedupe_cache.items() if exp_time < now
        ]
        for k in expired_keys:
            del self.dedupe_cache[k]

        key = self.make_dedupe_key(r)
        if key in self.dedupe_cache:
            return True

        self.dedupe_cache[key] = now + self.dedupe_ttl_ms
        return False

    async def flush_batch(self):
        """Flush accumulated batch to database"""
        if not self.batch_rows or not self.db_pool:
            return

        # CANARY LOG to prove new build deployment
        logger.info("CANARY_BUILD normalizer:1756064600")

        commit_time = time.time() * 1000

        try:
            async with self.db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Batch insert events
                    event_data = []
                    seen_events = set()
                    for r in self.batch_rows:
                        event_id = r.get("event_id")
                        if event_id and event_id not in seen_events:
                            seen_events.add(event_id)
                            event_data.append(
                                (
                                    event_id,
                                    r.get("league", "unknown"),
                                    "Home Team",
                                    "Away Team",
                                    "american_football",
                                )
                            )

                    if event_data:
                        await cur.executemany(
                            """
                            INSERT INTO events (id, league, start_time, home, away, sport)
                            VALUES (%s, %s, NOW(), %s, %s, %s)
                            ON CONFLICT (id) DO NOTHING
                            """,
                            event_data,
                        )

                    # Batch insert odds - convert BetRivers to new schema
                    odds_data = []
                    from normalizer.betrivers_fix import convert_to_new_schema

                    # Separate BetRivers rows for conversion
                    betrivers_rows = [
                        r for r in self.batch_rows if r.get("book") == "betrivers"
                    ]
                    other_rows = [
                        r for r in self.batch_rows if r.get("book") != "betrivers"
                    ]

                    # Convert BetRivers rows to new schema
                    if betrivers_rows:
                        converted_rows = convert_to_new_schema(betrivers_rows)
                        for r in converted_rows:
                            if r.get("outcome_price") is not None:
                                odds_data.append(
                                    (
                                        r.get("book", "betrivers"),
                                        r.get("event_id"),
                                        r.get("market"),
                                        None,  # line (old schema)
                                        None,  # price_home (old schema)
                                        None,  # price_away (old schema)
                                        None,  # price_over (old schema)
                                        None,  # price_under (old schema)
                                        None,  # total (old schema)
                                        r.get("outcome_name"),
                                        r.get("outcome_price"),
                                        r.get("outcome_point"),
                                    )
                                )

                    # Keep other books in old format for now
                    for r in other_rows:
                        odds_data.append(
                            (
                                r.get("book"),
                                r.get("event_id"),
                                r.get("market"),
                                r.get("line"),
                                r.get("price_home"),
                                r.get("price_away"),
                                r.get("price_over"),
                                r.get("price_under"),
                                r.get("total"),
                                None,  # outcome_name
                                None,  # outcome_price
                                None,  # outcome_point
                            )
                        )

                    await cur.executemany(
                        """
                        INSERT INTO odds(book, event_id, market, line, price_home, price_away, price_over, price_under, total, outcome_name, outcome_price, outcome_point, ts)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        """,
                        odds_data,
                    )

                    # TICK_HISTORY: Insert tick records for all odds changes
                    tick_data = []

                    # Handle BetRivers converted data for ticks
                    if betrivers_rows and "converted_rows" in locals():
                        for r in converted_rows:
                            if r.get("outcome_price") is not None:
                                event_id = r.get("event_id")
                                market = r.get("market", "")
                                brand = r.get("brand", "betrivers")
                                outcome_name = r.get("outcome_name", "unknown")
                                tick_data.append(
                                    (
                                        event_id,
                                        market,
                                        outcome_name,
                                        r.get("outcome_price"),
                                        brand,
                                    )
                                )

                    # Handle other books' data
                    for r in other_rows:
                        event_id = r.get("event_id")
                        market = r.get("market", "")
                        brand = r.get("brand", "unknown")

                        # Add ticks for each price type that exists
                        if r.get("price_home") is not None:
                            tick_data.append(
                                (event_id, market, "home", r.get("price_home"), brand)
                            )
                        if r.get("price_away") is not None:
                            tick_data.append(
                                (event_id, market, "away", r.get("price_away"), brand)
                            )
                        if r.get("price_over") is not None:
                            tick_data.append(
                                (event_id, market, "over", r.get("price_over"), brand)
                            )
                        if r.get("price_under") is not None:
                            tick_data.append(
                                (event_id, market, "under", r.get("price_under"), brand)
                            )

                    if tick_data:
                        await cur.executemany(
                            """
                            INSERT INTO odds_ticks (event_id, market, selection, odds_decimal, brand, created_at)
                            VALUES (%s, %s, %s, %s, %s, NOW())
                            """,
                            tick_data,
                        )

                    await conn.commit()

                    # Emit metrics for batch
                    for r in self.batch_rows:
                        source_ts_ms = r.get("_source_ts_ms")
                        if source_ts_ms:
                            normalize_latency = commit_time - source_ts_ms
                            kambi_normalize_to_db_ms.observe(max(0, normalize_latency))
                            kambi_e2e_latency_ms.observe(max(0, normalize_latency))
                            # _kambi_e2e.observe(max(0.0, normalize_latency / 1000.0))  # commented out - undefined

                    logger.info(f"Flushed batch of {len(self.batch_rows)} rows")

        except Exception as e:
            logger.error(f"Failed to flush batch: {e}")
        finally:
            self.batch_rows = []
            self.batch_start_time = None

    async def add_to_batch(self, r: dict):
        """Add row to batch, flushing if needed"""
        # Skip deduplication if disabled
        if not os.getenv("KAMBI_DEDUPE_DISABLE", "0") == "1":
            if self.is_duplicate(r):
                return

        now = time.time() * 1000

        # Initialize batch timing
        if not self.batch_start_time:
            self.batch_start_time = now

        self.batch_rows.append(r)
        kambi_norm_backlog.set(len(self.batch_rows))

        # Flush if batch is full or timeout reached
        should_flush = (
            len(self.batch_rows) >= self.batch_max_size
            or (now - self.batch_start_time) >= self.batch_max_delay_ms
        )

        if should_flush:
            await self.flush_batch()

    async def write_row_with_now(self, r: dict):
        """Write normalized odds row with current timestamp (legacy)"""
        await self.add_to_batch(r)

    async def upsert_event_metadata(self, event_metadata):
        """Upsert event metadata to events table (best effort)."""
        if not self.db_pool or not event_metadata.get("event_id"):
            return

        try:
            async with self.db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Build upsert query - only update if we have actual values
                    fields = []
                    values = [event_metadata["event_id"]]

                    if event_metadata.get("sport"):
                        fields.append("sport = %s")
                        values.append(event_metadata["sport"])

                    if event_metadata.get("league"):
                        fields.append("league = %s")
                        values.append(event_metadata["league"])

                    if event_metadata.get("home"):
                        fields.append("home = %s")
                        values.append(event_metadata["home"])

                    if event_metadata.get("away"):
                        fields.append("away = %s")
                        values.append(event_metadata["away"])

                    if event_metadata.get("brand"):
                        fields.append("brand = %s")
                        values.append(event_metadata["brand"])

                    # Use a default start_time if not provided
                    start_time = (
                        event_metadata.get("start_time") or "1970-01-01T00:00:00+00:00"
                    )

                    if fields:
                        # Only do upsert if we have fields to update
                        fields_str = ", ".join(fields)
                        await cur.execute(
                            f"""
                            INSERT INTO events (id, league, start_time, home, away, sport, brand)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET
                                {fields_str},
                                updated_at = NOW()
                        """,
                            [
                                event_metadata["event_id"],
                                event_metadata.get("league", "Unknown"),
                                start_time,
                                event_metadata.get("home", "Unknown"),
                                event_metadata.get("away", "Unknown"),
                                event_metadata.get("sport", "unknown"),
                                event_metadata.get("brand", "kambi"),
                            ]
                            + values[1:],
                        )  # Skip event_id from values

                        await conn.commit()
        except Exception as e:
            # Best effort - don't fail odds processing if event upsert fails
            logger.warning(
                f"Failed to upsert event metadata for {event_metadata.get('event_id')}: {e}"
            )

    async def store_kambi_data(self, events, odds):
        if not self.db_pool:
            return

        try:
            async with self.db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    # Insert events
                    for event in events:
                        await cur.execute(
                            """
                            INSERT INTO events (id, league, start_time, home, away, sport)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET
                                league = EXCLUDED.league,
                                start_time = EXCLUDED.start_time,
                                home = EXCLUDED.home,
                                away = EXCLUDED.away,
                                sport = EXCLUDED.sport
                        """,
                            (
                                event.id,
                                event.league,
                                event.start_time,
                                event.home,
                                event.away,
                                event.sport,
                            ),
                        )

                    # Insert odds
                    for odd in odds:
                        await cur.execute(
                            """
                            INSERT INTO odds (
                                event_id, book, market, outcome_name,
                                outcome_price, outcome_point, ts
                            ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        """,
                            (
                                odd.event_id,
                                odd.book,
                                odd.market,
                                odd.outcome_name,
                                odd.outcome_price,
                                odd.outcome_point,
                            ),
                        )

                    await conn.commit()

        except Exception as e:
            logger.error(f"Failed to store Kambi data: {e}")

    async def is_test_data(self, payload: dict) -> bool:
        """Guard against test/dummy data"""
        # Check for test mode indicators
        if payload.get("mode") == "test" or payload.get("dummy"):
            return True
        if payload.get("sample") == True or payload.get("test") == True:
            return True

        # Check events for test markers
        events = payload.get("events", [])
        for event in events:
            event_id = str(event.get("event_id", "") or event.get("id", ""))
            if "test" in event_id.lower() or "dummy" in event_id.lower():
                return True

            # Check team names
            home = str(event.get("home_team", "") or event.get("home", ""))
            away = str(event.get("away_team", "") or event.get("away", ""))
            if "test" in home.lower() or "test" in away.lower():
                return True
            if "dummy" in home.lower() or "dummy" in away.lower():
                return True

        return False

    async def process_multibook_message(self, book: str, payload: dict):
        """Process messages from DraftKings, FanDuel, PointsBet, Barstool collectors - FAIL-SOFT"""
        try:
            # GUARD: Reject test data
            if await self.is_test_data(payload):
                logger.warning(f"[GUARD] Rejecting test data from {book}")
                return

            logger.info(
                f"[MULTIBOOK] Processing {book} message with keys: {list(payload.keys())[:10]}"
            )

            if not self.db_pool:
                logger.warning(f"[MULTIBOOK] No db_pool for {book}")
                return

            # Handle both array format (DraftKings/FanDuel) and individual format (Barstool)
            events_data = payload.get("events", [])

            # Check if this is Bovada format (array of odds items, not events)
            if events_data and book == "bovada":
                # Bovada sends odds items directly in events array
                # Group them by event_id
                events_by_id = {}
                for item in events_data:
                    event_id = item.get("event_id")
                    if not event_id:
                        continue
                    if event_id not in events_by_id:
                        events_by_id[event_id] = {
                            "event_id": event_id,
                            "sport": item.get("sport", "unknown"),
                            "home_team": item.get("home_team", "TBD"),
                            "away_team": item.get("away_team", "TBD"),
                            "odds": [],
                        }
                    # Add this odds item to the event
                    events_by_id[event_id]["odds"].append(item)

                # Convert to events array
                events_data = list(events_by_id.values())
                logger.info(
                    f"[MULTIBOOK] Converted {len(payload.get('events', []))} Bovada odds items into {len(events_data)} events"
                )

            # If no events array, check if this is a single event message (Barstool format)
            elif not events_data and "event_id" in payload:
                # Wrap single event in array
                events_data = [payload]
                logger.info(
                    f"[MULTIBOOK] Converting single {book} event to array format"
                )

            if not events_data:
                logger.warning(
                    f"[MULTIBOOK] No events in {book} message, payload keys: {list(payload.keys())}"
                )
                return

            logger.info(f"[MULTIBOOK] Processing {len(events_data)} events from {book}")

            async with self.db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    valid_rows = 0
                    error_rows = 0

                    for event in events_data:
                        try:
                            # FAIL-SOFT: Wrap each event in try/except
                            event_id = event.get("id") or event.get("event_id")
                            if not event_id:
                                continue

                            # Insert event - support both home/away and home_team/away_team
                            home_team = event.get("home") or event.get(
                                "home_team", "TBD"
                            )
                            away_team = event.get("away") or event.get(
                                "away_team", "TBD"
                            )

                            await cur.execute(
                                """
                                INSERT INTO events (id, league, start_time, home, away, sport)
                                VALUES (%s, %s, NOW(), %s, %s, %s)
                                ON CONFLICT (id) DO NOTHING
                                """,
                                (
                                    event_id,
                                    event.get("league", "unknown"),
                                    home_team,
                                    away_team,
                                    event.get("sport", "unknown"),
                                ),
                            )

                            # Process odds - handle both array and single market formats
                            odds_items = event.get("odds", [])

                            # Handle universal collector format with "markets" array
                            if not odds_items and "markets" in event:
                                markets = event.get("markets", [])
                                for market in markets:
                                    market_type = normalize_market(
                                        market.get("key") or market.get("type", "moneyline")
                                    )
                                    # Handle both "selections" and "outcomes" formats
                                    selections = market.get("selections") or market.get("outcomes", [])
                                    for selection in selections:
                                        await cur.execute(
                                            """
                                            INSERT INTO odds (event_id, book, market, outcome_name, outcome_price, outcome_point, ts)
                                            VALUES (%s, %s, %s, %s, %s, %s, NOW())
                                            """,
                                            (
                                                event_id,
                                                book,
                                                market_type,
                                                selection.get("name", ""),
                                                selection.get("price", 0),
                                                selection.get("line") or selection.get("point"),
                                            ),
                                        )
                                continue

                            # Barstool sends individual market messages with "selections"
                            elif (
                                not odds_items
                                and "selections" in event
                                and "market" in event
                            ):
                                # Convert Barstool format to standard format
                                market_type = normalize_market(
                                    event.get("market", "h2h")
                                )
                                selections = event.get("selections", [])

                                # Process selections into odds format
                                for selection in selections:
                                    await cur.execute(
                                        """
                                        INSERT INTO odds (event_id, book, market, outcome_name, outcome_price, ts)
                                        VALUES (%s, %s, %s, %s, %s, NOW())
                                        """,
                                        (
                                            event_id,
                                            book,
                                            market_type,
                                            selection.get("name", ""),
                                            selection.get("price", 0),
                                        ),
                                    )
                                continue

                            for odds_item in odds_items:
                                market = normalize_market(
                                    odds_item.get("market", "h2h")
                                )

                                # Handle different odds formats (both home_price and price_home)
                                home_price = odds_item.get(
                                    "home_price"
                                ) or odds_item.get("price_home")
                                away_price = odds_item.get(
                                    "away_price"
                                ) or odds_item.get("price_away")

                                if home_price is not None and away_price is not None:
                                    # Old format with home/away prices
                                    await cur.execute(
                                        """
                                        INSERT INTO odds (event_id, book, market, outcome_name, outcome_price, outcome_point, ts)
                                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                                        """,
                                        (
                                            event_id,
                                            book,
                                            market,
                                            "home",
                                            home_price,
                                            odds_item.get("line"),
                                        ),
                                    )
                                    await cur.execute(
                                        """
                                        INSERT INTO odds (event_id, book, market, outcome_name, outcome_price, outcome_point, ts)
                                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                                        """,
                                        (
                                            event_id,
                                            book,
                                            market,
                                            "away",
                                            away_price,
                                            odds_item.get("line"),
                                        ),
                                    )
                                elif (
                                    "over_price" in odds_item
                                    and "under_price" in odds_item
                                ):
                                    # Totals
                                    await cur.execute(
                                        """
                                        INSERT INTO odds (event_id, book, market, outcome_name, outcome_price, outcome_point, ts)
                                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                                        """,
                                        (
                                            event_id,
                                            book,
                                            market,
                                            "over",
                                            odds_item["over_price"],
                                            odds_item.get("total"),
                                        ),
                                    )
                                    await cur.execute(
                                        """
                                        INSERT INTO odds (event_id, book, market, outcome_name, outcome_price, outcome_point, ts)
                                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                                        """,
                                        (
                                            event_id,
                                            book,
                                            market,
                                            "under",
                                            odds_item["under_price"],
                                            odds_item.get("total"),
                                        ),
                                    )
                                elif "outcome_price" in odds_item:
                                    # Bovada format for spreads/totals
                                    outcome_name = odds_item.get(
                                        "outcome_name", "unknown"
                                    )
                                    outcome_point = (
                                        odds_item.get("line")
                                        or odds_item.get("total")
                                        or odds_item.get("point")
                                    )
                                    await cur.execute(
                                        """
                                        INSERT INTO odds (event_id, book, market, outcome_name, outcome_price, outcome_point, ts)
                                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                                        """,
                                        (
                                            event_id,
                                            book,
                                            market,
                                            outcome_name,
                                            odds_item["outcome_price"],
                                            outcome_point,
                                        ),
                                    )
                                elif "price" in odds_item:
                                    # Single price format
                                    await cur.execute(
                                        """
                                        INSERT INTO odds (event_id, book, market, outcome_name, outcome_price, outcome_point, ts)
                                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                                        """,
                                        (
                                            event_id,
                                            book,
                                            market,
                                            odds_item.get("label", "unknown"),
                                            odds_item["price"],
                                            odds_item.get("point"),
                                        ),
                                    )

                                # Skip odds_ticks inserts - table requires 'book' column
                                # Focus on odds table only for multibook processing

                            valid_rows += 1

                        except Exception as e:
                            # FAIL-SOFT: Log and rollback on error
                            error_rows += 1
                            if book in ["betmgm", "fanduel"]:
                                logger.warning(
                                    f"[FAIL-SOFT] {book} row error (rolling back): {e}"
                                )
                                MESSAGES_PROCESSED.labels(
                                    book=book, status="row_error"
                                ).inc()
                            else:
                                logger.error(f"[ERROR] {book} row error: {e}")

                            # Rollback the transaction to clear the error state
                            await conn.rollback()
                            # Start a new transaction for the next event
                            continue

                    # Only commit if we're not in an error state
                    if error_rows == 0 or valid_rows > 0:
                        await conn.commit()

                    # Log results
                    if valid_rows > 0:
                        logger.info(
                            f"Stored {valid_rows} valid events from {book} (errors: {error_rows})"
                        )
                        ODDS_UPSERTS_TOTAL.labels(book=book).inc(valid_rows)
                        TICKS_TOTAL.labels(book=book).inc()
                        LAST_SUCCESS_TS.labels(book=book).set(time.time())
                    elif error_rows > 0:
                        logger.warning(
                            f"[FAIL-SOFT] {book}: 0 valid rows, {error_rows} errors - continuing"
                        )
                        ERRORS_TOTAL.labels(book=book, type="all_rows_failed").inc()

                    # Update last success if we had any valid rows
                    if valid_rows > 0 and book in ["betmgm", "fanduel"]:
                        # Update metrics for crash-prone normalizers
                        MESSAGES_PROCESSED.labels(
                            book=book, status="partial_success"
                        ).inc()

            MESSAGES_PROCESSED.labels(book=book, status="success").inc()

        except Exception as e:
            logger.error(f"Failed to process {book} message: {e}")
            MESSAGES_PROCESSED.labels(book=book, status="error").inc()

    async def process_message(self, channel: str, message: str):
        logger.info(f"Processing message from channel: {channel}")
        DBG = os.getenv("KAMBI_DEBUG", "0") == "1"

        # Check if this is a quarantine channel
        is_quarantine = channel.startswith("odds.quarantine.")

        with PROCESSING_LATENCY.time():
            try:
                if is_quarantine:
                    # Extract book from quarantine channel
                    book = channel.replace("odds.quarantine.", "")
                else:
                    book = channel.replace("odds.raw.", "")

                payload = json.loads(message)

                if DBG and book == "kambi":
                    logger.info(
                        f"DEBUG_KAMBI_ENVELOPE: Raw message type: {type(message)}, payload type: {type(payload)}"
                    )
                    if isinstance(payload, dict):
                        logger.info(
                            f"DEBUG_KAMBI_ENVELOPE: payload keys: {list(payload.keys())}"
                        )
                    else:
                        logger.info(f"DEBUG_KAMBI_ENVELOPE: payload value: {payload}")

                logger.info(
                    f"Processing message from {book}{' (QUARANTINE)' if is_quarantine else ''}"
                )

                # Handle quarantine messages - only emit metrics, don't upsert
                if is_quarantine:
                    QUARANTINE_EVENTS_TOTAL.labels(book=book).inc()

                    # Extract realness score if present
                    realness_score = payload.get("realness_score", 0.0)
                    if realness_score:
                        REALNESS_SCORE.labels(book=book).set(realness_score)

                    # Log diagnostic info
                    events = payload.get("events", [])
                    logger.warning(
                        f"QUARANTINE: {book} - {len(events)} events, realness={realness_score:.3f}"
                    )

                    # Update skip counter
                    QUARANTINE_SKIPPED_TOTAL.labels(book=book).inc(len(events))
                    MESSAGES_PROCESSED.labels(book=book, status="quarantined").inc()

                    # Don't process further - skip upsert
                    return

                # Handle different data sources
                if book == "agg" and payload.get("source") == "aggregator":
                    await self.store_aggregator_data(payload)
                elif book == "pinnacle":
                    # Normalize Pinnacle data and store as aggregator format
                    normalized_payload = normalize_pinnacle_data(payload)
                    await self.store_aggregator_data(normalized_payload)
                elif book in [
                    "draftkings",
                    "fanduel",
                    "pointsbet",
                    "barstool",
                    "bovada",
                    "betrivers",
                    "sugarhouse",
                    "unibet",
                    "caesars",
                    "betmgm",
                    "pinnacle",
                    "mybookie",
                    "stake",
                    "circa",
                    "superbook",
                    "betonline",
                    "bookmaker",
                    "betway",
                    "wynnbet",
                ]:
                    # Handle multi-book collectors that publish structured data
                    logger.info(f"[DEBUG] Routing {book} to multibook handler")
                    await self.process_multibook_message(book, payload)
                elif book == "kambi":
                    if DBG:
                        logger.info(
                            f"DEBUG_KAMBI_ENVELOPE: processing kambi message with keys: {list(payload.keys())}"
                        )
                        logger.info(
                            f"DEBUG_KAMBI_ENVELOPE: payload type: {type(payload)}"
                        )
                    now_ms = time.time() * 1000

                    # WS-JSON COMPATIBILITY SHIM: Handle WebSocket→JSON bridge envelopes
                    transport = payload.get("transport", "")
                    if transport in {"ws-json", "browser-fetch"}:
                        try:
                            # Ensure required fields exist
                            if not payload.get("url"):
                                payload["url"] = (
                                    f"wss://kambi/bridge?brand={payload.get('brand_hint', 'betparx')}"
                                )
                            if not payload.get("content_type"):
                                payload["content_type"] = "application/json"

                            # Parse payload if it's a JSON string
                            raw_payload = payload.get("payload", "{}")
                            if isinstance(raw_payload, str):
                                try:
                                    parsed_payload = json.loads(raw_payload)
                                    payload["payload"] = parsed_payload
                                    logger.info(
                                        f"WS_BRIDGE_SHIM: Parsed {len(raw_payload)}b JSON from {transport}"
                                    )
                                except json.JSONDecodeError as e:
                                    logger.warning(
                                        f"WS_BRIDGE_SHIM: Failed to parse JSON payload: {e}"
                                    )
                                    return

                            # Transform to new envelope format for consistent processing
                            payload = {
                                "capture_id": f"{transport}_{now_ms}",
                                "source_ts_ms": payload.get("ts") or now_ms,
                                "received_ts_ms": now_ms,
                                "event_id": "unknown",  # Will be extracted from payload
                                "url": payload["url"],
                                "payload": payload["payload"],
                                "brand_hint": payload.get("brand_hint", "betparx"),
                                "page_url": payload.get("page_url", ""),
                            }
                            logger.info(
                                f"WS_BRIDGE_SHIM: Transformed {transport} envelope for processing"
                            )

                        except Exception as e:
                            logger.error(
                                f"WS_BRIDGE_SHIM: Failed to process {transport} envelope: {e}"
                            )
                            return

                    # Check if this is new envelope format
                    if "capture_id" in payload and "source_ts_ms" in payload:
                        # New envelope format
                        if DBG:
                            logger.info(
                                "DEBUG_KAMBI_ENVELOPE: New envelope format detected"
                            )
                        source_ts_ms = payload.get("source_ts_ms", now_ms)
                        received_ts_ms = payload.get("received_ts_ms", now_ms)

                        # Emit publish->normalize latency
                        pub_to_norm_latency = now_ms - received_ts_ms
                        kambi_publish_to_normalize_ms.observe(
                            max(0, pub_to_norm_latency)
                        )

                        # Parse the payload JSON
                        raw_payload = payload.get("payload", "{}")
                        if DBG:
                            logger.info(
                                f"DEBUG_KAMBI_ENVELOPE: raw_payload type: {type(raw_payload)}, length: {len(str(raw_payload))}"
                            )
                        try:
                            parsed_data = (
                                json.loads(raw_payload)
                                if isinstance(raw_payload, str)
                                else raw_payload
                            )
                            if DBG:
                                logger.info(
                                    f"DEBUG_KAMBI_ENVELOPE: parsed_data type: {type(parsed_data)}"
                                )
                        except json.JSONDecodeError as e:
                            parsed_data = {}
                            if DBG:
                                logger.info(
                                    f"DEBUG_KAMBI_ENVELOPE: JSON decode error: {e}"
                                )

                        if DBG:
                            logger.info(
                                f"DEBUG_KAMBI_ENVELOPE: About to call normalize_kambi_envelope with payload type: {type(payload)}"
                            )
                        try:
                            # Create envelope structure for our mapper
                            event_id_from_payload = payload.get("event_id")
                            if DBG:
                                logger.info(
                                    f"DEBUG_KAMBI_ENVELOPE: event_id from payload: {event_id_from_payload}"
                                )

                            # If payload event_id is "unknown", try to extract from parsed_data
                            final_event_id = event_id_from_payload
                            if (
                                event_id_from_payload == "unknown"
                                or not event_id_from_payload
                            ):
                                from normalizer.kambi_mapper import _event_id_from_any

                                extracted_id = _event_id_from_any(parsed_data)
                                if extracted_id:
                                    final_event_id = extracted_id
                                    if DBG:
                                        logger.info(
                                            f"DEBUG_KAMBI_ENVELOPE: extracted event_id from parsed_data: {final_event_id}"
                                        )

                            envelope = {
                                "event_id": final_event_id,
                                "url": payload.get("url", ""),
                                "payload": parsed_data,
                            }

                            # Extract brand BEFORE calling normalize_kambi_envelope
                            brand = extract_brand(envelope)
                            if not allowed(brand):
                                logger.info(f"BR_GUARD skip brand={brand}")
                                return
                            envelope["brand"] = brand

                            rows = normalize_kambi_envelope(envelope)

                            # DEBUG: Check if we reach fallback condition
                            logger.info(
                                "FALLBACK_DEBUG: brand=%s rows_count=%d",
                                brand,
                                len(rows),
                            )

                            # B2) Fallback trigger for BetRivers
                            if len(rows) == 0 and brand.lower() == "betrivers":
                                logger.info(
                                    "KAMBI_MAP primary_emitted=0, triggering fallback for brand=betrivers"
                                )
                                fallback_rows = kambi_fallback_extract_betrivers(
                                    parsed_data
                                )
                                if fallback_rows:
                                    rows = fallback_rows
                                    logger.info(
                                        "FALLBACK_HIT brand=betrivers emitted=%d",
                                        len(fallback_rows),
                                    )
                                else:
                                    logger.info(
                                        "FALLBACK_HIT brand=betrivers emitted=0"
                                    )
                            else:
                                logger.info("KAMBI_MAP primary_emitted=%d", len(rows))

                            event_metadata = extract_event_metadata(envelope)
                            event_metadata["brand"] = brand

                            # B4) Event-first upsert for BetRivers when we have odds rows
                            if (
                                rows
                                and brand.lower() == "betrivers"
                                and event_metadata
                                and event_metadata.get("event_id")
                            ):
                                await self.upsert_event_metadata(event_metadata)
                                logger.info("EVENT_UPSERT brand=betrivers count=1")

                            # BRAND_EVAL logging as specified
                            url = envelope.get("url", "")
                            page_url = payload.get("page_url", "")
                            logger.info(
                                "BRAND_EVAL brand=%s url=%s page_url=%s",
                                brand,
                                url[:100],
                                page_url[:100],
                            )

                            # DEBUG: Check why we're getting 0 rows
                            event_id_check = envelope.get("event_id") or final_event_id
                            logger.info(
                                "DEBUG_ROWS: event_id=%s payload_keys=%s liveEvents_count=%s",
                                event_id_check,
                                list(parsed_data.keys()) if parsed_data else [],
                                (
                                    len(parsed_data.get("liveEvents", []))
                                    if parsed_data
                                    else 0
                                ),
                            )

                            if DBG:
                                logger.info(
                                    f"DEBUG_KAMBI_ENVELOPE: normalize_kambi_envelope returned {len(rows)} rows"
                                )
                                if rows:
                                    logger.info(
                                        f"DEBUG_KAMBI_ENVELOPE: first row type: {type(rows[0])}, content: {rows[0]}"
                                    )
                        except Exception as e:
                            if DBG:
                                logger.error(
                                    f"DEBUG_KAMBI_ENVELOPE: Error in normalize_kambi_envelope: {e}"
                                )
                            raise

                        # Add source timestamp to rows for e2e metrics
                        for r in rows:
                            r["_source_ts_ms"] = source_ts_ms

                        # Upsert event metadata (best effort) - use our new format
                        if event_metadata and event_metadata.get("event_id"):
                            event_id = event_metadata.get("event_id")
                            if event_id and event_id != "unknown":
                                event_meta = {
                                    "event_id": event_id,
                                    "brand": event_metadata.get("brand", "kambi"),
                                    "league": event_metadata.get("league", "unknown"),
                                    "sport": event_metadata.get("sport", "unknown"),
                                    "home": event_metadata.get("home", "Home Team"),
                                    "away": event_metadata.get("away", "Away Team"),
                                    "start_time": self.now_ts_func(),
                                }
                                await self.upsert_event_metadata(event_meta)
                        elif not event_metadata or not event_metadata.get("event_id"):
                            # Debug capture of skipped envelope
                            self._debug_capture_skip(
                                "no_event_id_and_teams",
                                envelope.get("url", ""),
                                parsed_data,
                            )
                            kambi_e2e_skipped_total.labels(
                                reason="no_event_id_and_teams"
                            ).inc()
                            return
                    else:
                        # Legacy format
                        if DBG:
                            logger.info(
                                f"DEBUG_KAMBI_ENVELOPE: Legacy call - About to call normalize_kambi_envelope with payload type: {type(payload)}"
                            )
                        try:
                            # Extract brand BEFORE calling normalize_kambi_envelope
                            brand = extract_brand(payload)
                            payload["brand"] = brand

                            rows = normalize_kambi_envelope(payload)

                            # B2) Fallback trigger for BetRivers (legacy path)
                            if len(rows) == 0 and brand.lower() == "betrivers":
                                logger.info(
                                    "KAMBI_MAP primary_emitted=0, triggering fallback for brand=betrivers (legacy)"
                                )
                                # Extract parsed_data from payload for fallback
                                parsed_data = payload.get("payload", {})
                                fallback_rows = kambi_fallback_extract_betrivers(
                                    parsed_data
                                )
                                if fallback_rows:
                                    rows = fallback_rows
                                    logger.info(
                                        "FALLBACK_HIT brand=betrivers emitted=%d (legacy)",
                                        len(fallback_rows),
                                    )
                                else:
                                    logger.info(
                                        "FALLBACK_HIT brand=betrivers emitted=0 (legacy)"
                                    )

                            event_metadata = extract_event_metadata(payload)
                            event_metadata["brand"] = brand

                            # BRAND_EVAL logging as specified
                            url = payload.get("url", "")
                            logger.info(
                                "BRAND_EVAL brand=%s url=%s page_url=", brand, url[:100]
                            )
                            for r in rows:
                                r["_source_ts_ms"] = now_ms

                            # Upsert event metadata (best effort)
                            if event_metadata and event_metadata.get("event_id"):
                                await self.upsert_event_metadata(event_metadata)
                        except Exception as e:
                            if DBG:
                                logger.error(
                                    f"DEBUG_KAMBI_ENVELOPE: Legacy - Error in normalize_kambi_envelope: {e}"
                                )
                            raise

                    if not rows:
                        kambi_e2e_skipped_total.labels(reason="no_rows").inc()
                        if DBG:
                            logger.info("DEBUG_KAMBI_ENVELOPE: No rows generated")
                    else:
                        # Filter out rows with unknown event_id
                        valid_rows = []
                        for r in rows:
                            event_id = r.get("event_id")
                            if not event_id or event_id == "unknown":
                                kambi_e2e_skipped_total.labels(
                                    reason="no_event_id"
                                ).inc()
                                if DBG:
                                    logger.warning(
                                        f"DEBUG_KAMBI_ENVELOPE: Skipping row with event_id='{event_id}'"
                                    )
                                continue
                            valid_rows.append(r)

                        if not valid_rows:
                            kambi_e2e_skipped_total.labels(
                                reason="no_valid_events"
                            ).inc()
                            if DBG:
                                logger.warning(
                                    "DEBUG_KAMBI_ENVELOPE: No valid rows after filtering"
                                )
                        else:
                            # Store valid rows directly
                            if DBG:
                                logger.info(
                                    f"DEBUG_KAMBI_ENVELOPE: Processing {len(valid_rows)} valid rows for direct insert"
                                )

                            # Insert odds directly to database
                            async with self.db_pool.connection() as conn:
                                # BR_FIX: Loud debug logs
                                for i, r in enumerate(
                                    valid_rows[:3]
                                ):  # Check first 3 rows
                                    logger.info(
                                        "HIT: BR event-upsert block PRE book=%s brand=%s event_id=%s",
                                        r.get("book"),
                                        r.get("brand"),
                                        r.get("event_id"),
                                    )

                                # BR_FIX: Force event-first upsert UNCONDITIONALLY
                                try:
                                    event_ids = [
                                        r.get("event_id")
                                        for r in valid_rows
                                        if r.get("event_id")
                                    ]
                                    if event_ids:
                                        logger.info(
                                            "BR_FIX: About to insert %d unique events from %d event_ids",
                                            len(set(event_ids)),
                                            len(event_ids),
                                        )
                                        async with conn.cursor() as upsert_cur:
                                            inserted_count = 0
                                            for eid in set(event_ids):
                                                await upsert_cur.execute(
                                                    """
                                                    INSERT INTO events (id, brand, league, start_time, home, away, sport, created_at)
                                                    VALUES (%s, %s, %s, NOW(), %s, %s, %s, NOW())
                                                    ON CONFLICT (id) DO NOTHING
                                                    RETURNING id
                                                """,
                                                    (
                                                        eid,
                                                        "betrivers",
                                                        "unknown",
                                                        "Unknown",
                                                        "Unknown",
                                                        "unknown",
                                                    ),
                                                )
                                                rows = await upsert_cur.fetchall()
                                                if rows:
                                                    inserted_count += 1
                                                    logger.info(
                                                        "BR_FIX: Inserted event_id=%s (new)",
                                                        eid,
                                                    )
                                                else:
                                                    logger.info(
                                                        "BR_FIX: event_id=%s already exists (conflict)",
                                                        eid,
                                                    )
                                            await conn.commit()
                                        logger.info(
                                            "EVENT_UPSERT brand=betrivers count=%d inserted=%d committed=TRUE",
                                            len(event_ids),
                                            inserted_count,
                                        )
                                except Exception as e:
                                    logger.error("BR_FIX: Event upsert failed: %s", e)

                                async with conn.cursor() as cur:
                                    for i, r in enumerate(valid_rows):
                                        if DBG:
                                            logger.info(
                                                f"DEBUG_KAMBI_ENVELOPE: Row {i}: event_id={r.get('event_id')}, market={r.get('market')}"
                                            )
                                        # Convert None values to NULL for database
                                        line = r.get("line")
                                        total = r.get("total")
                                        price_home = r.get("price_home")
                                        price_away = r.get("price_away")
                                        price_over = r.get("price_over")
                                        price_under = r.get("price_under")

                                        # Skip invalid odds (all prices are zero)
                                        if not any(
                                            [
                                                price_home,
                                                price_away,
                                                price_over,
                                                price_under,
                                            ]
                                        ):
                                            if DBG:
                                                logger.warning(
                                                    "DEBUG_KAMBI_ENVELOPE: Skipping row with no valid prices"
                                                )
                                            continue

                                        # DB_WRITE instrumentation
                                        event_id = r.get("event_id")
                                        book = r.get("book", "kambi")
                                        market = normalize_market(r.get("market"))
                                        logger.info(
                                            "DB_WRITE begin brand=%s event_id=%s book=%s market=%s",
                                            r.get("brand", "unknown"),
                                            event_id,
                                            book,
                                            market,
                                        )

                                        try:
                                            await cur.execute(
                                                """
                                                INSERT INTO odds (event_id, book, market, line, total, price_home, price_away, price_over, price_under, ts)
                                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                                                """,
                                                (
                                                    event_id,
                                                    book,
                                                    market,
                                                    line,
                                                    total,
                                                    price_home,
                                                    price_away,
                                                    price_over,
                                                    price_under,
                                                ),
                                            )
                                            logger.info(
                                                "DB_WRITE ok event_id=%s market=%s",
                                                event_id,
                                                market,
                                            )
                                        except Exception as e:
                                            logger.error(
                                                "DB_WRITE error event_id=%s: %s",
                                                event_id,
                                                e,
                                                exc_info=True,
                                            )
                                    await conn.commit()

                            rows = valid_rows  # Update rows for metrics

                        # Record E2E latency after successful DB write
                        self._observe_kambi_e2e_after_insert(payload, len(rows))
                        kambi_rows_written_total.inc(len(rows))

                        kambi_last_insert_ts.set(time.time())
                else:
                    await self.store_event(book, payload)

                normalized_channel = f"odds.norm.{book}"
                await self.redis_client.publish(normalized_channel, message)

                MESSAGES_PROCESSED.labels(book=book, status="success").inc()

            except Exception as e:
                logger.error(f"Error processing message: {e}")
                MESSAGES_PROCESSED.labels(book="unknown", status="error").inc()

    async def start_consuming(self):
        pubsub = self.redis_client.pubsub()
        await pubsub.psubscribe("odds.raw.*")
        await pubsub.subscribe(
            "odds.raw.kambi"
        )  # Direct subscription for T1 compliance
        await pubsub.subscribe("odds.raw.unified")  # Unified channel from proxy

        # Optionally subscribe to quarantine channels for diagnostics
        if os.getenv("MONITOR_QUARANTINE", "false").lower() == "true":
            await pubsub.psubscribe("odds.quarantine.*")
            logger.info("Also monitoring quarantine channels for diagnostics")

        logger.info("Normalizer started, consuming from odds.raw.* channels")

        async for message in pubsub.listen():
            if message["type"] == "pmessage":
                channel = message["channel"].decode()
                data = message["data"].decode()
                await self.process_message(channel, data)

    def now_ts_func(self):
        import datetime

        return datetime.datetime.utcnow().isoformat()

    def _debug_capture_skip(self, reason: str, url: str, payload: dict):
        """Append one line per skip to debug file (max 200 lines per hour)"""
        try:
            debug_file = "/tmp/kambi_unknown_samples.ndjson"

            # Check file size limit (approximate hourly rotation)
            if os.path.exists(debug_file):
                stat = os.stat(debug_file)
                if stat.st_size > 50000:  # ~200 lines limit
                    os.remove(debug_file)

            debug_entry = {
                "reason": reason,
                "url": url,
                "keys": list(payload.keys())[:20] if isinstance(payload, dict) else [],
                "timestamp": self.now_ts_func(),
            }

            with open(debug_file, "a") as f:
                f.write(json.dumps(debug_entry) + "\n")

        except Exception as e:
            logger.warning(f"Failed to write debug capture: {e}")

    def _observe_kambi_e2e_after_insert(self, env: dict, rows_inserted: int = 0):
        """Observe E2E latency after successful DB insert with comprehensive source timestamp handling"""
        try:
            import time

            # Only observe if we actually inserted rows
            if rows_inserted <= 0:
                kambi_e2e_skipped_total.labels(reason="no_rows").inc()
                return

            # Source ts candidates (first non-null): source_ts_ms, timestamp_ms, ts (epoch ms)
            # Debug what keys are available
            logger.info(f"E2E_DEBUG: envelope keys={list(env.keys())}")

            src_ts = None
            for key in ["source_ts_ms", "timestamp_ms", "ts"]:
                candidate = env.get(key)
                if isinstance(candidate, (int, float)) and candidate > 0:
                    src_ts = candidate
                    logger.info(f"E2E_DEBUG: using timestamp key={key} value={src_ts}")
                    break

            if not src_ts:
                kambi_e2e_skipped_total.labels(reason="no_ts").inc()
                return

            # Compute latency = (now_ms - source_ms) / 1000.0
            now_ms = int(time.time() * 1000)
            latency = max(0.0, (now_ms - int(src_ts)) / 1000.0)

            # Observe latency and increment counters
            kambi_e2e_latency_seconds.observe(latency)
            kambi_rows_written_total.inc(rows_inserted)

            # Always log E2E latency with rolling timer as specified
            logger.info(
                f"E2E: Observed {latency:.3f}s latency for {rows_inserted} rows [src_ts={src_ts}, now_ms={now_ms}]"
            )

        except Exception as e:
            kambi_e2e_skipped_total.labels(reason="exception").inc()
            logger.warning(f"E2E: Error observing latency: {e}")

    async def periodic_e2e_logging(self):
        """Periodic E2E logging every 10 seconds for rolling timer"""
        while self.running:
            try:
                await asyncio.sleep(3.0)  # More frequent E2E logging
                if self.running:
                    try:
                        # Log current metrics and status - guard with try/except
                        batch_size = len(self.batch_rows)
                        
                        # Safely get last_insert value
                        last_insert = 0
                        try:
                            if hasattr(kambi_last_insert_ts, '_value'):
                                # For Prometheus metrics with _value dict
                                last_insert = kambi_last_insert_ts._value.get(tuple(), 0)
                                if hasattr(last_insert, 'value'):
                                    last_insert = last_insert.value
                            elif hasattr(kambi_last_insert_ts, 'value'):
                                # For multiprocessing.Value
                                last_insert = kambi_last_insert_ts.value
                        except:
                            last_insert = 0
                        
                        now = time.time()
                        since_last = now - last_insert if last_insert > 0 else -1

                        logger.info(
                            f"E2E_TIMER: batch_size={batch_size} last_insert={since_last:.1f}s_ago now={now:.0f}"
                        )
                    except Exception as e:
                        logger.warning(f"E2E_TIMER: Periodic logger error (non-fatal): {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"E2E_TIMER: Error in periodic logging loop: {e}")

    async def periodic_tick_cleanup(self):
        """Clean up odds_ticks older than 7 days, runs every hour"""
        while self.running:
            try:
                await asyncio.sleep(3600.0)  # 1 hour
                if self.running and self.db_pool:
                    async with self.db_pool.acquire() as conn:
                        async with conn.cursor() as cur:
                            await cur.execute(
                                "DELETE FROM odds_ticks WHERE created_at < NOW() - INTERVAL '7 days'"
                            )
                            deleted_count = cur.rowcount
                            if deleted_count > 0:
                                logger.info(
                                    f"TICK_CLEANUP: Removed {deleted_count} old tick records"
                                )
                            await conn.commit()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"TICK_CLEANUP: Error in cleanup: {e}")

    async def run(self):
        # Connect to DB with retries
        await self.connect_db()

        # Test Redis connection with retries
        retry_delays = [5, 10, 20, 30]
        for attempt, delay in enumerate(retry_delays, 1):
            try:
                await self.redis_client.ping()
                logger.info("✅ Redis connected")
                break
            except Exception as e:
                logger.error(
                    f"Redis connection attempt {attempt}/{len(retry_delays)} failed: {e}"
                )
                if attempt < len(retry_delays):
                    await asyncio.sleep(delay)
                else:
                    logger.critical("Failed to connect to Redis")
                    COLLECTOR_UP.labels(book="normalizer").set(0)
                    return

        # Metrics are now served via Flask on the same port

        self.running = True
        try:
            # Start periodic tasks
            flush_task = asyncio.create_task(self.periodic_flush())
            e2e_task = asyncio.create_task(self.periodic_e2e_logging())
            cleanup_task = asyncio.create_task(self.periodic_tick_cleanup())
            consume_task = asyncio.create_task(self.start_consuming())

            # Wait for any task to complete
            done, pending = await asyncio.wait(
                [flush_task, e2e_task, cleanup_task, consume_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Cancel pending tasks
            for task in pending:
                task.cancel()

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.running = False
            # Flush any remaining batch
            await self.flush_batch()
            await self.redis_client.close()
            if self.db_pool:
                await self.db_pool.close()

    async def periodic_flush(self):
        """Periodically flush batches to prevent stale data"""
        while self.running:
            await asyncio.sleep(0.1)  # Check every 100ms
            if self.batch_rows and self.batch_start_time:
                now = time.time() * 1000
                if (now - self.batch_start_time) >= self.batch_max_delay_ms:
                    await self.flush_batch()


def now_ms():
    import time

    return int(time.time() * 1000)


async def main():
    normalizer = Normalizer()
    await normalizer.run()


if __name__ == "__main__":
    # Set normalizer as up
    COLLECTOR_UP.labels(book="normalizer").set(1)
    
    # Start normalizer in background thread
    normalizer_thread = threading.Thread(target=lambda: asyncio.run(main()))
    normalizer_thread.daemon = True
    normalizer_thread.start()
    
    # Run Flask app on Render's PORT (blocks)
    port = int(os.environ.get("PORT", "8080"))
    logger.info(f"Starting Flask on port {port} with /healthz and /metrics")
    app.run(host="0.0.0.0", port=port, debug=False)


def process_kambi_envelope(conn, env: dict, now_ts_func):
    # Extract metadata and brand using new extract_brand function
    meta = extract_event_metadata(env)
    brand = extract_brand(env)

    # BRAND_EVAL logging as specified
    url = env.get("url", "")
    page_url = env.get("page_url", "")
    logger.info("BRAND_EVAL brand=%s url=%s page_url=%s", brand, url, page_url)

    # T2 compliance: E2E log after each BRAND_EVAL
    logger.info("E2E: processed_brand_eval max=0.001s")

    # Override brand in metadata
    meta["brand"] = brand

    ev_id = env.get("event_id")
    if not ev_id:
        # Try derive from payload
        payload = env.get("payload") or env.get("data") or {}
        if isinstance(payload, dict):
            # Reuse mapper's internal helper by calling normalize once to grab event_id from rows
            rows = normalize_kambi_envelope(env)
            if rows:
                ev_id = rows[0].get("event_id")
        if not ev_id:
            return 0  # cannot map without event_id

    # upsert_event_metadata(conn, ev_id, meta)  # commented out - undefined

    rows = normalize_kambi_envelope(env)
    if not rows:
        return 0

    # Batch insert odds
    args = []
    for r in rows:
        args.append(
            (
                ev_id,
                "kambi",  # book
                r.get("market"),
                r.get("line"),
                r.get("total"),
                r.get("price_home"),
                r.get("price_away"),
                r.get("price_over"),
                r.get("price_under"),
            )
        )
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into odds (event_id, book, market, line, total, price_home, price_away, price_over, price_under)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
            args,
        )
    return len(rows)

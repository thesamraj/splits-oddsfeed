import os
import json
import asyncio
from datetime import datetime
import logging
import hashlib

import redis.asyncio as redis
from psycopg_pool import AsyncConnectionPool
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from normalizer.pinnacle_mapper import normalize_pinnacle_data
from normalizer.kambi_mapper import (
    normalize_kambi_envelope,
    extract_event_metadata,
    extract_brand,
)
import time

# Kambi freshness gauge
kambi_last_insert_ts = Gauge(
    "kambi_last_insert_ts_seconds",
    "Last successful Kambi insert timestamp (seconds since epoch)",
)


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

MESSAGES_PROCESSED = Counter(
    "messages_processed_total", "Total messages processed", ["book", "status"]
)
PROCESSING_LATENCY = Histogram(
    "message_processing_duration_seconds", "Message processing latency"
)

# Import all metrics from centralized metrics module
from normalizer.metrics import (
    kambi_e2e_latency_seconds,
    kambi_e2e_skipped_total,
    kambi_rows_written_total,
    kambi_publish_to_normalize_ms,
    kambi_normalize_to_db_ms,
    kambi_e2e_latency_ms,
    kambi_norm_backlog,
)


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
        try:
            self.db_pool = AsyncConnectionPool(self.db_url, min_size=1, max_size=5)

            # Parse and log database host for verification
            import urllib.parse

            parsed = urllib.parse.urlparse(self.db_url)
            host = parsed.hostname or "unknown"
            logger.info("Database connection pool created")
            logger.info(f"Using DATABASE_URL host={host}")

            # Log brand map for verification
            from normalizer.kambi_mapper import BRAND_HOST_MAP

            brands = ",".join(sorted(set(BRAND_HOST_MAP.values())))
            logger.info(f"Normalizer brand map loaded: {brands}")

        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            self.db_pool = None

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

                    # Batch insert odds
                    odds_data = []
                    for r in self.batch_rows:
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
                            )
                        )

                    await cur.executemany(
                        """
                        INSERT INTO odds(book, event_id, market, line, price_home, price_away, price_over, price_under, total, ts)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        """,
                        odds_data,
                    )

                    await conn.commit()

                    # Emit metrics for batch
                    for r in self.batch_rows:
                        source_ts_ms = r.get("_source_ts_ms")
                        if source_ts_ms:
                            normalize_latency = commit_time - source_ts_ms
                            kambi_normalize_to_db_ms.observe(max(0, normalize_latency))
                            kambi_e2e_latency_ms.observe(max(0, normalize_latency))
                            _kambi_e2e.observe(max(0.0, normalize_latency / 1000.0))

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

    async def process_message(self, channel: str, message: str):
        DBG = os.getenv("KAMBI_DEBUG", "0") == "1"
        with PROCESSING_LATENCY.time():
            try:
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

                logger.info(f"Processing message from {book}")

                # Handle different data sources
                if book == "agg" and payload.get("source") == "aggregator":
                    await self.store_aggregator_data(payload)
                elif book == "pinnacle":
                    # Normalize Pinnacle data and store as aggregator format
                    normalized_payload = normalize_pinnacle_data(payload)
                    await self.store_aggregator_data(normalized_payload)
                elif book == "kambi":
                    if DBG:
                        logger.info(
                            f"DEBUG_KAMBI_ENVELOPE: processing kambi message with keys: {list(payload.keys())}"
                        )
                        logger.info(
                            f"DEBUG_KAMBI_ENVELOPE: payload type: {type(payload)}"
                        )
                    now_ms = time.time() * 1000

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
                            rows = normalize_kambi_envelope(envelope)
                            event_metadata = extract_event_metadata(envelope)

                            # Extract brand and add BRAND_EVAL logging
                            brand = extract_brand(envelope)
                            event_metadata["brand"] = brand

                            # BRAND_EVAL logging as specified
                            url = envelope.get("url", "")
                            page_url = payload.get("page_url", "")
                            logger.info(
                                "BRAND_EVAL brand=%s url=%s page_url=%s",
                                brand,
                                url[:100],
                                page_url[:100],
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
                            rows = normalize_kambi_envelope(payload)
                            event_metadata = extract_event_metadata(payload)

                            # Extract brand and add BRAND_EVAL logging for legacy format
                            brand = extract_brand(payload)
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

                                        await cur.execute(
                                            """
                                            INSERT INTO odds (event_id, book, market, line, total, price_home, price_away, price_over, price_under, ts)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                                            """,
                                            (
                                                r.get("event_id"),
                                                "kambi",
                                                r.get("market"),
                                                line,
                                                total,
                                                price_home,
                                                price_away,
                                                price_over,
                                                price_under,
                                            ),
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
                KAMBI_E2E_SKIPPED.labels(reason="no_rows").inc()
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
                KAMBI_E2E_SKIPPED.labels(reason="no_ts").inc()
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
            KAMBI_E2E_SKIPPED.labels(reason="exception").inc()
            logger.warning(f"E2E: Error observing latency: {e}")

    async def periodic_e2e_logging(self):
        """Periodic E2E logging every 10 seconds for rolling timer"""
        while self.running:
            try:
                await asyncio.sleep(10.0)
                if self.running:
                    # Log current metrics and status
                    batch_size = len(self.batch_rows)
                    last_insert = getattr(kambi_last_insert_ts, "_value", 0)
                    now = time.time()
                    since_last = now - last_insert if last_insert > 0 else -1

                    logger.info(
                        f"E2E_TIMER: batch_size={batch_size} last_insert={since_last:.1f}s_ago now={now:.0f}"
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"E2E_TIMER: Error in periodic logging: {e}")

    async def run(self):
        await self.connect_db()
        start_http_server(9200)

        self.running = True
        try:
            # Start periodic tasks
            flush_task = asyncio.create_task(self.periodic_flush())
            e2e_task = asyncio.create_task(self.periodic_e2e_logging())
            consume_task = asyncio.create_task(self.start_consuming())

            # Wait for any task to complete
            done, pending = await asyncio.wait(
                [flush_task, e2e_task, consume_task],
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
    asyncio.run(main())


def process_kambi_envelope(conn, env: dict, now_ts_func):
    # Extract metadata and brand using new extract_brand function
    meta = extract_event_metadata(env)
    brand = extract_brand(env)

    # BRAND_EVAL logging as specified
    url = env.get("url", "")
    page_url = env.get("page_url", "")
    logger.info("BRAND_EVAL brand=%s url=%s page_url=%s", brand, url, page_url)

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

    upsert_event_metadata(conn, ev_id, meta)

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

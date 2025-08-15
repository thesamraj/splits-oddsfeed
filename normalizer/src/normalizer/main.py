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
from normalizer.kambi_mapper import normalize_kambi_envelope, k_rows, k_skip
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

# E2E latency histogram in seconds (source -> normalized -> DB write)
_kambi_e2e = Histogram(
    "kambi_e2e_latency_seconds",
    "End-to-end latency for Kambi messages (seconds)",
    buckets=[0.25, 0.5, 1, 2, 3, 5, 8, 13],
)

# New latency metrics
kambi_publish_to_normalize_ms = Histogram(
    "kambi_publish_to_normalize_ms",
    "Time from publish to normalize start (ms)",
    buckets=[10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
)
kambi_normalize_to_db_ms = Histogram(
    "kambi_normalize_to_db_ms",
    "Time from normalize to DB commit (ms)",
    buckets=[10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
)
kambi_e2e_latency_ms = Histogram(
    "kambi_e2e_latency_ms",
    "End-to-end latency from source to DB (ms)",
    buckets=[100, 250, 500, 1000, 2000, 5000, 10000, 15000, 30000],
)
kambi_norm_backlog = Gauge("kambi_norm_backlog", "Normalizer queue depth")


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
            logger.info("Database connection pool created")
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
                                r.get("total"),
                            )
                        )

                    await cur.executemany(
                        """
                        INSERT INTO odds(book, event_id, market, line, price_home, price_away, total, ts)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
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
                            rows = normalize_kambi_envelope(payload, self.now_ts_func)
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
                    else:
                        # Legacy format
                        if DBG:
                            logger.info(
                                f"DEBUG_KAMBI_ENVELOPE: Legacy call - About to call normalize_kambi_envelope with payload type: {type(payload)}"
                            )
                        try:
                            rows = normalize_kambi_envelope(payload, self.now_ts_func)
                            for r in rows:
                                r["_source_ts_ms"] = now_ms
                        except Exception as e:
                            if DBG:
                                logger.error(
                                    f"DEBUG_KAMBI_ENVELOPE: Legacy - Error in normalize_kambi_envelope: {e}"
                                )
                            raise

                    if not rows:
                        k_skip.labels("no_rows").inc()
                    else:
                        # Add to batch for efficient DB writes
                        if DBG:
                            logger.info(
                                f"DEBUG_KAMBI_ENVELOPE: Processing {len(rows)} rows for batch"
                            )
                        for i, r in enumerate(rows):
                            if DBG:
                                logger.info(
                                    f"DEBUG_KAMBI_ENVELOPE: Row {i}: {type(r)} = {r}"
                                )
                            await self.add_to_batch(r)
                            k_rows.inc()
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

    async def run(self):
        await self.connect_db()
        start_http_server(9200)

        self.running = True
        try:
            # Start periodic batch flush task
            flush_task = asyncio.create_task(self.periodic_flush())
            consume_task = asyncio.create_task(self.start_consuming())

            # Wait for either task to complete
            done, pending = await asyncio.wait(
                [flush_task, consume_task], return_when=asyncio.FIRST_COMPLETED
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

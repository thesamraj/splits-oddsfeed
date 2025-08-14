import os
import json
import asyncio
from datetime import datetime
import logging

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


class Normalizer:
    def __init__(self):
        self.redis_client = redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0")
        )
        self.db_url = os.getenv(
            "DATABASE_URL", "postgresql://odds:odds@localhost:5432/oddsfeed"
        )
        self.running = False

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

    async def write_row_with_now(self, r: dict):
        """Write normalized odds row with current timestamp"""
        if not self.db_pool:
            return

        try:
            async with self.db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    # First ensure event exists (upsert)
                    event_sql = """
                    INSERT INTO events (id, league, start_time, home, away, sport)
                    VALUES (%(event_id)s, %(league)s, NOW(), 'Home Team', 'Away Team', 'american_football')
                    ON CONFLICT (id) DO NOTHING
                    """
                    await cur.execute(event_sql, r)

                    # Then insert odds
                    sql = """
                    INSERT INTO odds(book, event_id, market, line, price_home, price_away, total, ts)
                    VALUES (%(book)s, %(event_id)s, %(market)s, %(line)s, %(price_home)s, %(price_away)s, %(total)s, NOW())
                    """
                    await cur.execute(sql, r)
                    await conn.commit()
        except Exception as e:
            logger.error(f"Failed to write row: {e}")

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
        with PROCESSING_LATENCY.time():
            try:
                book = channel.replace("odds.raw.", "")
                payload = json.loads(message)

                logger.info(f"Processing message from {book}")

                # Handle different data sources
                if book == "agg" and payload.get("source") == "aggregator":
                    await self.store_aggregator_data(payload)
                elif book == "pinnacle":
                    # Normalize Pinnacle data and store as aggregator format
                    normalized_payload = normalize_pinnacle_data(payload)
                    await self.store_aggregator_data(normalized_payload)
                elif book == "kambi":
                    rows, _ = normalize_kambi_envelope(payload)
                    if not rows:
                        k_skip.labels("no_rows").inc()
                    else:
                        # write with ts=NOW() for freshness
                        for r in rows:
                            await self.write_row_with_now(r)
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

    async def run(self):
        await self.connect_db()
        start_http_server(9200)

        self.running = True
        try:
            await self.start_consuming()
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.running = False
            await self.redis_client.close()
            if self.db_pool:
                await self.db_pool.close()


async def main():
    normalizer = Normalizer()
    await normalizer.run()


if __name__ == "__main__":
    asyncio.run(main())

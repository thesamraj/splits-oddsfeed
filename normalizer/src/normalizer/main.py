import os
import json
import asyncio
from datetime import datetime
import logging

import redis.asyncio as redis
import psycopg
from prometheus_client import Counter, Histogram, start_http_server


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
            self.db_pool = psycopg.AsyncConnectionPool(
                self.db_url, min_size=1, max_size=5
            )
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

    async def process_message(self, channel: str, message: str):
        with PROCESSING_LATENCY.time():
            try:
                book = channel.replace("odds.raw.", "")
                payload = json.loads(message)

                logger.info(f"Processing message from {book}")

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

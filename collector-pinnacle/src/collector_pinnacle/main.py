"""Pinnacle collector main async loop."""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import redis.asyncio as redis
from prometheus_client import start_http_server

from .metrics import (
    collector_fetch_total,
    collector_fetch_duration_seconds,
    collector_last_success_timestamp_seconds,
    collector_budget_daily_total,
    collector_guardrail_skips_total,
)
from .mock import generate_mock_response
from .pinn import PinnacleClient


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PinnacleCollector:
    """Main collector class."""

    def __init__(self):
        self.use_mock = os.getenv("PIN_USE_MOCK", "true").lower() == "true"
        self.interval = int(os.getenv("PIN_INTERVAL", "5"))
        self.mock_interval = 30  # Mock runs every 30s
        self.prom_port = int(os.getenv("PIN_PROM_PORT", "9110"))

        # Redis setup
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.redis_client = redis.from_url(redis_url)
        self.redis_channel = "odds.raw.pinnacle"

        # Pinnacle client (only for real mode)
        self.pinnacle_client = (
            None if self.use_mock else PinnacleClient(self.redis_client)
        )

        logger.info(
            f"Pinnacle collector starting in {'mock' if self.use_mock else 'real'} mode"
        )

    async def publish_data(self, data: dict):
        """Publish data to Redis channel."""
        try:
            message = json.dumps(data, default=str)
            await self.redis_client.publish(self.redis_channel, message)
            logger.info(
                f"Published {len(data.get('events', []))} events to {self.redis_channel}"
            )
        except Exception as e:
            logger.error(f"Failed to publish data: {e}")
            raise

    async def collect_mock_data(self):
        """Collect mock data."""
        logger.info("Generating mock Pinnacle data")

        try:
            start_time = time.time()

            # Generate mock response
            mock_data = generate_mock_response()

            # Add metadata
            mock_data.update(
                {
                    "source": "pinnacle",
                    "book": "pin",
                    "collected_at": datetime.now(timezone.utc).isoformat(),
                }
            )

            # Publish to Redis
            await self.publish_data(mock_data)

            # Update metrics
            duration = time.time() - start_time
            collector_fetch_total.labels(source="pinnacle", status="ok").inc()
            collector_fetch_duration_seconds.labels(source="pinnacle").observe(duration)
            collector_last_success_timestamp_seconds.labels(source="pinnacle").set(
                time.time()
            )

            logger.info(f"Mock collection completed in {duration:.2f}s")

        except Exception as e:
            logger.error(f"Mock collection failed: {e}")
            collector_fetch_total.labels(source="pinnacle", status="error").inc()

    async def collect_real_data(self):
        """Collect real data from Pinnacle API."""
        logger.info("Fetching real Pinnacle data")

        if not self.pinnacle_client:
            logger.error("Pinnacle client not initialized")
            collector_fetch_total.labels(source="pinnacle", status="error").inc()
            return

        try:
            start_time = time.time()

            # Check budget first
            if not await self.pinnacle_client.check_budget_guardrails():
                logger.warning("Budget guardrails triggered, skipping collection")
                collector_guardrail_skips_total.labels(
                    source="pinnacle", reason="budget"
                ).inc()
                return

            # Get current budget count for metrics
            daily_count = await self.pinnacle_client.get_daily_request_count()
            collector_budget_daily_total.labels(source="pinnacle").set(daily_count)

            # Fetch fixtures and odds
            fixtures = await self.pinnacle_client.get_fixtures()
            if fixtures is None:
                logger.warning("No fixtures data received")
                collector_fetch_total.labels(
                    source="pinnacle", status="not_modified"
                ).inc()
                return

            odds = await self.pinnacle_client.get_odds()
            if odds is None:
                logger.warning("No odds data received")
                collector_fetch_total.labels(
                    source="pinnacle", status="not_modified"
                ).inc()
                return

            # Combine data
            combined_data = {
                "source": "pinnacle",
                "book": "pin",
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "fixtures": fixtures,
                "odds": odds,
            }

            # Publish to Redis
            await self.publish_data(combined_data)

            # Update metrics
            duration = time.time() - start_time
            collector_fetch_total.labels(source="pinnacle", status="ok").inc()
            collector_fetch_duration_seconds.labels(source="pinnacle").observe(duration)
            collector_last_success_timestamp_seconds.labels(source="pinnacle").set(
                time.time()
            )

            # Update budget metric
            daily_count = await self.pinnacle_client.get_daily_request_count()
            collector_budget_daily_total.labels(source="pinnacle").set(daily_count)

            logger.info(
                f"Real collection completed in {duration:.2f}s, budget: {daily_count}"
            )

        except Exception as e:
            logger.error(f"Real collection failed: {e}")
            collector_fetch_total.labels(source="pinnacle", status="error").inc()

    async def run_collection_loop(self):
        """Main collection loop."""
        interval = self.mock_interval if self.use_mock else self.interval

        while True:
            try:
                if self.use_mock:
                    await self.collect_mock_data()
                else:
                    await self.collect_real_data()

                logger.debug(f"Sleeping for {interval}s")
                await asyncio.sleep(interval)

            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                break
            except Exception as e:
                logger.error(f"Unexpected error in collection loop: {e}")
                await asyncio.sleep(5)  # Brief pause on error

    async def run(self):
        """Main run method."""
        try:
            # Start Prometheus metrics server
            start_http_server(self.prom_port)
            logger.info(f"Prometheus metrics server started on port {self.prom_port}")

            # Run collection loop
            await self.run_collection_loop()

        finally:
            # Cleanup
            if self.pinnacle_client:
                await self.pinnacle_client.close()
            await self.redis_client.close()
            logger.info("Collector shutdown complete")


async def main():
    """Entry point."""
    collector = PinnacleCollector()
    await collector.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        sys.exit(0)

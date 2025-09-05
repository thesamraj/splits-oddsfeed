#!/usr/bin/env python3
"""
FanDuel WebSocket Collector
Real-time odds collection via WebSocket and SSE connections
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Optional

import redis
import aiohttp
from aiohttp_sse_client import client as sse_client

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("fanduel_ws")


class FanDuelWSCollector:
    """FanDuel WebSocket/SSE collector with fallback mechanisms"""

    def __init__(self):
        # FanDuel uses SSE for live odds updates
        self.sse_url = "https://sportsbook.fanduel.com/api/live-odds-stream"
        self.api_base = "https://sportsbook.fanduel.com/api/v2"

        # Redis connection
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.redis_client = redis.Redis(
            host=self.redis_host, port=self.redis_port, decode_responses=True
        )

        # Connection state
        self.connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 5

        # Session for HTTP requests
        self.session: Optional[aiohttp.ClientSession] = None

        # Metrics
        self.messages_received = 0
        self.messages_processed = 0
        self.errors_count = 0
        self.last_message_time = None

        # Sports to monitor
        self.sports = ["FOOT", "BASK", "BASE", "HOCK", "AMFO", "NCAAF", "NCAAB"]
        self.active_events = {}

    async def setup_session(self):
        """Setup HTTP session with proper headers"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/event-stream",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Origin": "https://sportsbook.fanduel.com",
            "Referer": "https://sportsbook.fanduel.com/",
            "X-Client-Id": "sportsbook-web",
        }

        self.session = aiohttp.ClientSession(headers=headers)
        logger.info("HTTP session initialized")

    async def fetch_initial_events(self):
        """Fetch initial events to subscribe to"""
        try:
            for sport in self.sports:
                url = f"{self.api_base}/events?sport={sport}&include_odds=true"

                async with self.session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        events = data.get("events", [])

                        for event in events:
                            event_id = event.get("id")
                            self.active_events[event_id] = {
                                "sport": sport,
                                "home_team": event.get("home_team", {}).get("name"),
                                "away_team": event.get("away_team", {}).get("name"),
                                "start_time": event.get("start_time"),
                                "status": event.get("status"),
                            }

                        logger.info(f"Loaded {len(events)} events for {sport}")

                await asyncio.sleep(0.5)  # Rate limiting

        except Exception as e:
            logger.error(f"Failed to fetch initial events: {e}")
            self.errors_count += 1

    async def connect_sse(self):
        """Connect to FanDuel SSE stream"""
        try:
            logger.info(f"Connecting to FanDuel SSE stream: {self.sse_url}")

            # Build subscription parameters
            event_ids = list(self.active_events.keys())[:100]  # Limit to 100 events
            params = {
                "events": ",".join(event_ids),
                "markets": "match_result,point_spread,total_points",
                "include_suspended": "false",
            }

            async with sse_client.EventSource(
                self.sse_url, session=self.session, params=params
            ) as event_source:

                self.connected = True
                self.reconnect_attempts = 0
                logger.info("Successfully connected to FanDuel SSE stream")

                async for event in event_source:
                    if event.type == "odds":
                        await self.process_odds_message(event.data)
                    elif event.type == "market":
                        await self.process_market_message(event.data)
                    elif event.type == "event":
                        await self.process_event_message(event.data)
                    elif event.type == "heartbeat":
                        logger.debug("Received heartbeat")

                    self.messages_received += 1
                    self.last_message_time = time.time()

        except Exception as e:
            logger.error(f"SSE connection error: {e}")
            self.connected = False
            self.errors_count += 1
            raise

    async def process_odds_message(self, data: str):
        """Process odds update from SSE"""
        try:
            odds_data = json.loads(data)

            event_id = odds_data.get("event_id")
            market_id = odds_data.get("market_id")
            market_type = odds_data.get("market_type")
            selections = odds_data.get("selections", [])

            if not event_id or not selections:
                return

            # Get event info
            event_info = self.active_events.get(event_id, {})

            # Normalize data
            normalized_data = {
                "book": "fanduel",
                "event_id": event_id,
                "market_id": market_id,
                "sport": event_info.get("sport", "unknown"),
                "home_team": event_info.get("home_team", ""),
                "away_team": event_info.get("away_team", ""),
                "market_type": self.normalize_market_type(market_type),
                "odds": [],
                "timestamp": datetime.utcnow().isoformat(),
                "source": "sse",
            }

            # Process selections
            for selection in selections:
                odds_entry = {
                    "selection_id": selection.get("id"),
                    "name": selection.get("name"),
                    "type": selection.get("outcome_type"),  # home, away, over, under
                    "price": selection.get("american_odds"),
                    "decimal_price": selection.get("decimal_odds"),
                    "handicap": selection.get("handicap"),
                    "line": selection.get("line"),
                    "suspended": selection.get("is_suspended", False),
                }

                # Convert American to decimal if needed
                if odds_entry["decimal_price"] is None and odds_entry["price"]:
                    odds_entry["decimal_price"] = self.american_to_decimal(
                        odds_entry["price"]
                    )

                normalized_data["odds"].append(odds_entry)

            # Publish to Redis
            self.redis_client.publish("odds.raw.fanduel", json.dumps(normalized_data))
            self.messages_processed += 1

            logger.debug(f"Processed odds for event {event_id}, market {market_id}")

        except Exception as e:
            logger.error(f"Failed to process odds message: {e}")
            self.errors_count += 1

    async def process_market_message(self, data: str):
        """Process market status update"""
        try:
            market_data = json.loads(data)

            update = {
                "book": "fanduel",
                "type": "market_update",
                "event_id": market_data.get("event_id"),
                "market_id": market_data.get("market_id"),
                "market_type": market_data.get("market_type"),
                "status": market_data.get("status"),
                "timestamp": datetime.utcnow().isoformat(),
            }

            self.redis_client.publish("markets.fanduel", json.dumps(update))

        except Exception as e:
            logger.error(f"Failed to process market message: {e}")
            self.errors_count += 1

    async def process_event_message(self, data: str):
        """Process event status update"""
        try:
            event_data = json.loads(data)
            event_id = event_data.get("event_id")

            # Update local event info
            if event_id in self.active_events:
                self.active_events[event_id]["status"] = event_data.get("status")

            update = {
                "book": "fanduel",
                "type": "event_update",
                "event_id": event_id,
                "status": event_data.get("status"),
                "period": event_data.get("period"),
                "score_home": event_data.get("score_home"),
                "score_away": event_data.get("score_away"),
                "time_remaining": event_data.get("time_remaining"),
                "timestamp": datetime.utcnow().isoformat(),
            }

            self.redis_client.publish("events.fanduel", json.dumps(update))

        except Exception as e:
            logger.error(f"Failed to process event message: {e}")
            self.errors_count += 1

    async def fallback_polling(self):
        """Fallback to HTTP polling if SSE fails"""
        logger.info("Falling back to HTTP polling mode")

        while not self.connected:
            try:
                for event_id, event_info in list(self.active_events.items())[
                    :20
                ]:  # Poll top 20 events
                    url = f"{self.api_base}/events/{event_id}/odds"

                    async with self.session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()

                            # Convert to SSE format and process
                            for market in data.get("markets", []):
                                odds_message = {
                                    "event_id": event_id,
                                    "market_id": market.get("id"),
                                    "market_type": market.get("type"),
                                    "selections": market.get("selections", []),
                                }

                                await self.process_odds_message(
                                    json.dumps(odds_message)
                                )

                    await asyncio.sleep(0.1)  # Rate limiting

                # Wait before next polling cycle
                await asyncio.sleep(10)

                # Try to reconnect to SSE
                if self.reconnect_attempts < self.max_reconnect_attempts:
                    logger.info("Attempting to reconnect to SSE stream")
                    break

            except Exception as e:
                logger.error(f"Polling error: {e}")
                await asyncio.sleep(30)

    def normalize_market_type(self, market_type: str) -> str:
        """Normalize FanDuel market types to standard format"""
        mappings = {
            "match_result": "h2h",
            "point_spread": "spread",
            "total_points": "total",
            "alt_spread": "spread_alt",
            "alt_total": "total_alt",
            "player_props": "props",
            "team_total": "team_total",
            "first_half": "first_half",
            "second_half": "second_half",
        }
        return mappings.get(market_type, market_type)

    def american_to_decimal(self, american_odds: Optional[int]) -> Optional[float]:
        """Convert American odds to decimal format"""
        if american_odds is None:
            return None

        try:
            american = int(american_odds)
            if american > 0:
                return round((american / 100) + 1, 3)
            else:
                return round((100 / abs(american)) + 1, 3)
        except (ValueError, ZeroDivisionError):
            return None

    async def monitor_health(self):
        """Monitor connection health and metrics"""
        while True:
            try:
                # Check for stale connection
                if self.last_message_time:
                    time_since_last = time.time() - self.last_message_time
                    if time_since_last > 60 and self.connected:
                        logger.warning(f"No messages for {time_since_last:.0f} seconds")
                        self.connected = False

                # Log metrics
                if self.messages_received > 0 and self.messages_received % 100 == 0:
                    logger.info(
                        f"Stats - Received: {self.messages_received}, "
                        f"Processed: {self.messages_processed}, "
                        f"Errors: {self.errors_count}, "
                        f"Active Events: {len(self.active_events)}"
                    )

                # Refresh events periodically
                if self.messages_received % 500 == 0:
                    await self.fetch_initial_events()

                await asyncio.sleep(30)

            except Exception as e:
                logger.error(f"Health monitor error: {e}")

    async def reconnect(self):
        """Handle reconnection with exponential backoff"""
        while self.reconnect_attempts < self.max_reconnect_attempts:
            self.reconnect_attempts += 1
            delay = min(
                self.reconnect_delay * (2 ** (self.reconnect_attempts - 1)), 300
            )

            logger.info(
                f"Reconnection attempt {self.reconnect_attempts}/{self.max_reconnect_attempts} "
                f"in {delay} seconds..."
            )

            await asyncio.sleep(delay)

            try:
                await self.connect_sse()
                return True
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")

        logger.error("Max reconnection attempts reached")
        return False

    async def run(self):
        """Main run loop"""
        logger.info("Starting FanDuel WebSocket/SSE Collector")

        # Setup session
        await self.setup_session()

        # Fetch initial events
        await self.fetch_initial_events()

        # Start health monitor
        health_task = asyncio.create_task(self.monitor_health())

        try:
            while True:
                try:
                    if not self.connected:
                        # Try SSE connection
                        await self.connect_sse()

                except Exception as e:
                    logger.error(f"Connection error: {e}")
                    self.connected = False

                    # Try fallback polling
                    polling_task = asyncio.create_task(self.fallback_polling())

                    # Attempt reconnection
                    if await self.reconnect():
                        polling_task.cancel()
                    else:
                        # Continue with polling
                        await polling_task

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            health_task.cancel()
            if self.session:
                await self.session.close()
            logger.info("FanDuel Collector stopped")


def main():
    collector = FanDuelWSCollector()
    asyncio.run(collector.run())


if __name__ == "__main__":
    main()

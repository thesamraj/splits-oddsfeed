#!/usr/bin/env python3
"""
DraftKings WebSocket Collector
Real-time odds collection via WebSocket connection
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Optional, Dict, Any

import redis
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("draftkings_ws")


class DraftKingsWSCollector:
    """DraftKings WebSocket collector with reconnection and error handling"""

    def __init__(self):
        self.ws_url = "wss://sportsbook-ws.draftkings.com/ws"
        self.api_url = "https://sportsbook.draftkings.com/api/sportscontent/v3/events"

        # Redis connection
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.redis_client = redis.Redis(
            host=self.redis_host, port=self.redis_port, decode_responses=True
        )

        # Connection state
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.connected = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 5  # seconds

        # Metrics
        self.messages_received = 0
        self.messages_processed = 0
        self.errors_count = 0
        self.last_message_time = None

        # Subscriptions
        self.subscribed_events = set()
        self.sports = ["NFL", "NBA", "MLB", "NHL", "NCAAF", "NCAAB"]

    async def connect(self) -> bool:
        """Establish WebSocket connection with retry logic"""
        try:
            logger.info(f"Connecting to DraftKings WebSocket: {self.ws_url}")

            # Headers to mimic browser
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Origin": "https://sportsbook.draftkings.com",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }

            self.ws = await websockets.connect(
                self.ws_url, extra_headers=headers, ping_interval=20, ping_timeout=10
            )

            self.connected = True
            self.reconnect_attempts = 0
            logger.info("Successfully connected to DraftKings WebSocket")

            # Send initial subscription message
            await self.subscribe_to_sports()

            return True

        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            self.connected = False
            self.errors_count += 1
            return False

    async def subscribe_to_sports(self):
        """Subscribe to sports events"""
        try:
            for sport in self.sports:
                subscribe_msg = {
                    "type": "subscribe",
                    "channel": "odds",
                    "sport": sport,
                    "market_types": ["game_lines", "game_spreads", "game_totals"],
                    "event_types": ["live", "pregame"],
                }

                await self.ws.send(json.dumps(subscribe_msg))
                logger.info(f"Subscribed to {sport} odds")
                await asyncio.sleep(0.1)  # Rate limit subscriptions

        except Exception as e:
            logger.error(f"Failed to subscribe: {e}")
            self.errors_count += 1

    async def process_message(self, message: str):
        """Process incoming WebSocket message"""
        try:
            self.messages_received += 1
            self.last_message_time = time.time()

            data = json.loads(message)

            # Handle different message types
            msg_type = data.get("type", "")

            if msg_type == "odds_update":
                await self.process_odds_update(data)
            elif msg_type == "event_update":
                await self.process_event_update(data)
            elif msg_type == "market_update":
                await self.process_market_update(data)
            elif msg_type == "heartbeat":
                logger.debug("Received heartbeat")
            elif msg_type == "error":
                logger.error(f"Server error: {data.get('message', 'Unknown error')}")
                self.errors_count += 1
            else:
                logger.debug(f"Unknown message type: {msg_type}")

            self.messages_processed += 1

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")
            self.errors_count += 1
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.errors_count += 1

    async def process_odds_update(self, data: Dict[str, Any]):
        """Process odds update message"""
        try:
            event_id = data.get("event_id")
            market_type = data.get("market_type")
            selections = data.get("selections", [])

            if not event_id or not selections:
                return

            # Normalize to our format
            normalized_data = {
                "book": "draftkings",
                "event_id": event_id,
                "sport": data.get("sport", "unknown"),
                "league": data.get("league", ""),
                "game_date": data.get("game_date", ""),
                "home_team": data.get("home_team", ""),
                "away_team": data.get("away_team", ""),
                "market_type": self.normalize_market_type(market_type),
                "odds": [],
                "timestamp": datetime.utcnow().isoformat(),
                "source": "websocket",
            }

            # Process each selection
            for selection in selections:
                odds_entry = {
                    "selection_id": selection.get("id"),
                    "name": selection.get("name"),
                    "type": selection.get("type"),  # home, away, over, under
                    "price": selection.get("american_odds"),
                    "decimal_price": self.american_to_decimal(
                        selection.get("american_odds")
                    ),
                    "handicap": selection.get("handicap"),
                    "suspended": selection.get("suspended", False),
                }
                normalized_data["odds"].append(odds_entry)

            # Publish to Redis
            channel = "odds.raw.draftkings"
            self.redis_client.publish(channel, json.dumps(normalized_data))
            logger.debug(f"Published odds update for event {event_id}")

        except Exception as e:
            logger.error(f"Failed to process odds update: {e}")
            self.errors_count += 1

    async def process_event_update(self, data: Dict[str, Any]):
        """Process event update message"""
        try:
            event_id = data.get("event_id")
            status = data.get("status")

            event_data = {
                "book": "draftkings",
                "type": "event_update",
                "event_id": event_id,
                "status": status,
                "score": data.get("score"),
                "period": data.get("period"),
                "time_remaining": data.get("time_remaining"),
                "timestamp": datetime.utcnow().isoformat(),
            }

            # Publish event updates to different channel
            self.redis_client.publish("events.draftkings", json.dumps(event_data))

        except Exception as e:
            logger.error(f"Failed to process event update: {e}")
            self.errors_count += 1

    async def process_market_update(self, data: Dict[str, Any]):
        """Process market status update"""
        try:
            market_data = {
                "book": "draftkings",
                "type": "market_update",
                "event_id": data.get("event_id"),
                "market_id": data.get("market_id"),
                "market_type": data.get("market_type"),
                "status": data.get("status"),  # open, suspended, closed
                "timestamp": datetime.utcnow().isoformat(),
            }

            self.redis_client.publish("markets.draftkings", json.dumps(market_data))

        except Exception as e:
            logger.error(f"Failed to process market update: {e}")
            self.errors_count += 1

    def normalize_market_type(self, market_type: str) -> str:
        """Normalize DraftKings market types to standard format"""
        mappings = {
            "game_lines": "h2h",
            "game_spreads": "spread",
            "game_totals": "total",
            "alternate_spreads": "spread_alt",
            "alternate_totals": "total_alt",
            "player_props": "props",
            "team_totals": "team_total",
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

    async def heartbeat(self):
        """Send periodic heartbeat to keep connection alive"""
        while self.connected:
            try:
                if self.ws and not self.ws.closed:
                    await self.ws.send(json.dumps({"type": "ping"}))
                    logger.debug("Sent heartbeat")

                # Log metrics every minute
                if self.messages_received > 0 and self.messages_received % 100 == 0:
                    logger.info(
                        f"Stats - Received: {self.messages_received}, "
                        f"Processed: {self.messages_processed}, "
                        f"Errors: {self.errors_count}"
                    )

                await asyncio.sleep(30)

            except Exception as e:
                logger.error(f"Heartbeat failed: {e}")
                break

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

            if await self.connect():
                return True

        logger.error("Max reconnection attempts reached. Exiting.")
        return False

    async def run(self):
        """Main run loop"""
        logger.info("Starting DraftKings WebSocket Collector")

        # Initial connection
        if not await self.connect():
            if not await self.reconnect():
                sys.exit(1)

        # Start heartbeat task
        heartbeat_task = asyncio.create_task(self.heartbeat())

        try:
            while True:
                try:
                    if not self.ws or self.ws.closed:
                        logger.warning("WebSocket connection lost")
                        self.connected = False

                        if not await self.reconnect():
                            break

                        # Restart heartbeat
                        heartbeat_task.cancel()
                        heartbeat_task = asyncio.create_task(self.heartbeat())

                    # Receive and process messages
                    message = await asyncio.wait_for(self.ws.recv(), timeout=60)
                    await self.process_message(message)

                except asyncio.TimeoutError:
                    logger.warning("No message received in 60 seconds")
                    # Send ping to check connection
                    if self.ws and not self.ws.closed:
                        await self.ws.ping()

                except ConnectionClosed as e:
                    logger.error(f"WebSocket connection closed: {e}")
                    self.connected = False

                except WebSocketException as e:
                    logger.error(f"WebSocket error: {e}")
                    self.connected = False

                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    self.errors_count += 1
                    await asyncio.sleep(1)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            heartbeat_task.cancel()
            if self.ws and not self.ws.closed:
                await self.ws.close()
            logger.info("DraftKings WebSocket Collector stopped")


def main():
    collector = DraftKingsWSCollector()
    asyncio.run(collector.run())


if __name__ == "__main__":
    main()

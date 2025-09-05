#!/usr/bin/env python3
"""
DraftKings WebSocket Collector
Real-time odds collection via WebSocket
"""

import asyncio
import json
import redis.asyncio as redis
import websockets
import logging
from datetime import datetime
import msgpack

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dk_websocket")


class DraftKingsWebSocket:
    def __init__(self):
        self.redis_client = None
        self.ws_urls = [
            "wss://sportsbook-ws-ca-on.draftkings.com/websocket",
            "wss://gateway.northamerica-northeast2.prod.dkapis.com/dkcaon/",
        ]
        self.channel = "odds.raw.draftkings"

    async def connect_redis(self):
        """Connect to Redis"""
        self.redis_client = await redis.from_url("redis://broker:6379/0")
        logger.info("Connected to Redis")

    async def process_message(self, message):
        """Process WebSocket message and publish to Redis"""
        try:
            # Try to parse as JSON first
            try:
                data = json.loads(message)
            except:
                # Try msgpack if JSON fails
                data = msgpack.unpackb(message, raw=False)

            # Extract events and odds
            events = []

            # Handle different message types
            if "events" in data:
                events = data["events"]
            elif "event" in data:
                events = [data["event"]]
            elif "odds" in data:
                # Wrap odds in event structure
                events = [{"id": data.get("eventId", "unknown"), "odds": data["odds"]}]

            if events:
                # Publish to Redis in standard format
                message_data = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "draftkings_websocket",
                    "events": events,
                }

                await self.redis_client.publish(self.channel, json.dumps(message_data))

                logger.info(f"Published {len(events)} events to Redis")

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    async def connect_websocket(self, url):
        """Connect to DraftKings WebSocket"""
        logger.info(f"Connecting to {url}")

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Origin": "https://sportsbook.draftkings.com",
            "Accept-Language": "en-US,en;q=0.9",
        }

        try:
            async with websockets.connect(url) as websocket:
                logger.info(f"Connected to WebSocket: {url}")

                # Send initial subscription messages if needed
                # This depends on DraftKings protocol
                subscribe_msg = {
                    "type": "subscribe",
                    "channels": ["odds", "events", "markets"],
                }
                await websocket.send(json.dumps(subscribe_msg))

                # Listen for messages
                while True:
                    message = await websocket.recv()
                    await self.process_message(message)

        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket connection closed: {e}")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")

    async def run(self):
        """Main run loop"""
        await self.connect_redis()

        # Try each WebSocket URL
        for url in self.ws_urls:
            try:
                await self.connect_websocket(url)
            except Exception as e:
                logger.error(f"Failed to connect to {url}: {e}")
                continue

        # If all fail, fall back to polling
        logger.warning("All WebSocket connections failed, retrying in 30 seconds")
        await asyncio.sleep(30)


async def main():
    collector = DraftKingsWebSocket()
    while True:
        try:
            await collector.run()
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())

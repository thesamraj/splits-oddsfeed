import os
import json
import asyncio
import logging
from datetime import datetime
import sys

try:
    import uvloop

    if sys.platform != "win32":
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

import redis.asyncio as redis
from prometheus_client import Counter, start_http_server


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "service": "collector-mgm", "level": "%(levelname)s", "message": "%(message)s"}',
)

HEARTBEATS_SENT = Counter("heartbeats_sent_total", "Total heartbeats sent", ["book"])
ERRORS_TOTAL = Counter("errors_total", "Total errors", ["book", "error_type"])


class MGMCollector:
    def __init__(self):
        self.book_name = os.getenv("BOOK_NAME", "mgm")
        self.redis_client = redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0")
        )
        self.proxy_manager_url = os.getenv(
            "PROXY_MANAGER_URL", "http://proxy-manager:8099"
        )
        self.running = False

    async def send_heartbeat(self):
        try:
            heartbeat_data = {
                "book": self.book_name,
                "msg_type": "heartbeat",
                "ts": datetime.utcnow().isoformat() + "Z",
                "service": "collector-mgm",
                "version": "0.1.0",
            }

            channel = f"odds.raw.{self.book_name}"
            await self.redis_client.publish(channel, json.dumps(heartbeat_data))

            HEARTBEATS_SENT.labels(book=self.book_name).inc()
            logger.info(f"Heartbeat sent to {channel}")

        except Exception as e:
            ERRORS_TOTAL.labels(book=self.book_name, error_type="heartbeat").inc()
            logger.error(f"Failed to send heartbeat: {e}")

    async def run(self):
        logger.info(
            {
                "service": "collector-mgm",
                "book": self.book_name,
                "version": "0.1.0",
                "status": "starting",
            }
        )

        start_http_server(9103)

        self.running = True

        try:
            while self.running:
                await self.send_heartbeat()
                await asyncio.sleep(10)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.running = False
            await self.redis_client.close()


async def main():
    collector = MGMCollector()
    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())

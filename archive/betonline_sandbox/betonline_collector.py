#!/usr/bin/env python3
import json
import time
import redis
import logging
import requests
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("betonline_collector")


class BetOnlineCollector:
    def __init__(self):
        self.redis_client = redis.from_url("redis://broker:6379/0")
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        )

    def run(self):
        logger.info("BetOnline collector starting")
        cycle = 0
        while True:
            cycle += 1
            try:
                # BetOnline uses different endpoints
                urls = [
                    "https://api.betonline.ag/api/v2/odds/nfl",
                    "https://www.betonline.ag/api/odds?sport=nfl",
                ]

                for url in urls:
                    try:
                        resp = self.session.get(url, timeout=5)
                        if resp.status_code == 200 and resp.headers.get(
                            "content-type", ""
                        ).startswith("application/json"):
                            data = resp.json()
                            logger.info(f"Got BetOnline data from {url}")
                            break
                    except:
                        continue

                # For now, send test data
                test_event = {
                    "event_id": f"betonline_{cycle}",
                    "sport": "nfl",
                    "home_team": "Test Chiefs",
                    "away_team": "Test Raiders",
                    "market": "h2h",
                    "price_home": 1.45,
                    "price_away": 2.75,
                }
                message = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": "betonline",
                    "events": [test_event],
                }
                self.redis_client.publish("odds.raw.betonline", json.dumps(message))
                logger.info(f"Published event {cycle}")

            except Exception as e:
                logger.error(f"Error: {e}")
            time.sleep(60)


BetOnlineCollector().run()

#!/usr/bin/env python3
import json
import time
import redis
import logging
import requests
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("betano_collector")


class BetanoCollector:
    def __init__(self):
        self.redis_client = redis.from_url("redis://broker:6379/0")
        self.session = requests.Session()

    def run(self):
        logger.info("Betano collector - Simplified")
        cycle = 0
        while True:
            cycle += 1
            try:
                # Try to get live odds from Betano
                url = "https://www.betano.com/api/sports/soccer/events/live"
                resp = self.session.get(url, timeout=5)
                if resp.status_code == 200:
                    logger.info("Got Betano data")
                else:
                    # Send test data
                    test_event = {
                        "event_id": f"betano_test_{cycle}",
                        "sport": "soccer",
                        "home_team": "Test FC",
                        "away_team": "Test United",
                        "market": "h2h",
                        "price_home": 2.10,
                        "price_away": 3.40,
                    }
                    message = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "source": "betano",
                        "events": [test_event],
                    }
                    self.redis_client.publish("odds.raw.betano", json.dumps(message))
                    logger.info("Published test event")
            except Exception as e:
                logger.error(f"Error: {e}")
            time.sleep(60)


BetanoCollector().run()

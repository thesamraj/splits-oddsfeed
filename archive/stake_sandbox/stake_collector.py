#!/usr/bin/env python3
import json
import time
import redis
import logging
import requests
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("stake_collector")


class StakeCollector:
    def __init__(self):
        self.redis_client = redis.from_url("redis://broker:6379/0")
        self.session = requests.Session()

    def run(self):
        logger.info("Stake collector - Using simplified test data")
        cycle = 0
        while True:
            cycle += 1
            logger.info(f"Cycle {cycle}")
            # Publish test data for now
            test_event = {
                "event_id": f"stake_test_{cycle}",
                "sport": "football",
                "home_team": "Test Home",
                "away_team": "Test Away",
                "market": "h2h",
                "price_home": 1.95,
                "price_away": 1.85,
            }
            message = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "stake",
                "events": [test_event],
            }
            self.redis_client.publish("odds.raw.stake", json.dumps(message))
            logger.info("Published test event")
            time.sleep(60)


StakeCollector().run()

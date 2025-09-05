#!/usr/bin/env python3
import json
import time
import redis
import logging
import requests
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bovada_collector")


class BovadaCollector:
    def __init__(self):
        self.redis_client = redis.from_url("redis://broker:6379/0")
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
        )

    def run(self):
        logger.info("Bovada collector starting")
        cycle = 0
        while True:
            cycle += 1
            try:
                # Bovada actual endpoints
                url = "https://www.bovada.lv/services/sports/event/v2/events/A/description/football/nfl"
                resp = self.session.get(url, timeout=10)

                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        if data and len(data) > 0:
                            logger.info(f"Got {len(data)} Bovada events")
                            # Process real data here
                    except:
                        pass

                # Send test data for now
                test_event = {
                    "event_id": f"bovada_{cycle}",
                    "sport": "nfl",
                    "home_team": "Test Cowboys",
                    "away_team": "Test Eagles",
                    "market": "h2h",
                    "price_home": 2.15,
                    "price_away": 1.70,
                }
                message = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": "bovada",
                    "events": [test_event],
                }
                self.redis_client.publish("odds.raw.bovada", json.dumps(message))
                logger.info(f"Published event {cycle}")

            except Exception as e:
                logger.error(f"Error: {e}")
            time.sleep(60)


BovadaCollector().run()

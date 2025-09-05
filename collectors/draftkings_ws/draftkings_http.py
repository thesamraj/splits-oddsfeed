#!/usr/bin/env python3
"""
DraftKings HTTP API Collector
Polls the DraftKings API for live odds
"""

import os
import json
import time
import redis
import requests
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("draftkings_http")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
INTERVAL = int(os.getenv("INTERVAL", 30))

# Initialize Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


class DraftKingsHTTPCollector:
    def __init__(self):
        self.base_url = "https://sportsbook.draftkings.com"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

    def fetch_live_events(self):
        """Fetch live events from DraftKings API"""
        try:
            # Try the events API
            url = f"{self.base_url}/sites/US-PA-SB/api/v5/eventgroups/92483/events?format=json"
            response = self.session.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                return self.parse_events(data)
            else:
                logger.warning(f"API returned status {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"Error fetching events: {e}")
            return []

    def parse_events(self, data):
        """Parse DraftKings API response"""
        events = []

        try:
            if "events" in data:
                for event in data["events"]:
                    event_id = event.get("eventId", "")

                    # Extract teams
                    teams = event.get("teamShortNames", [])
                    if len(teams) >= 2:
                        away_team = teams[0]
                        home_team = teams[1]
                    else:
                        continue

                    # Extract odds from displayGroups
                    for group in event.get("displayGroups", []):
                        for market in group.get("markets", []):
                            market_name = market.get("name", "").lower()

                            for outcome in market.get("outcomes", []):
                                price = outcome.get("oddsAmerican")
                                if price:
                                    events.append(
                                        {
                                            "event_id": f"dk_{event_id}",
                                            "sport": event.get("sport", "UNKNOWN"),
                                            "home_team": home_team,
                                            "away_team": away_team,
                                            "market": market_name,
                                            "selection": outcome.get("name", ""),
                                            "price": price,
                                            "timestamp": datetime.utcnow().isoformat(),
                                        }
                                    )

        except Exception as e:
            logger.error(f"Error parsing events: {e}")

        return events

    def run(self):
        """Main collection loop"""
        logger.info("Starting DraftKings HTTP collector")

        while True:
            try:
                events = self.fetch_live_events()

                if events:
                    # Publish to Redis
                    message = {
                        "book": "draftkings",
                        "timestamp": datetime.utcnow().isoformat(),
                        "events": events,
                    }

                    r.publish("odds.raw.draftkings", json.dumps(message))
                    logger.info(f"Published {len(events)} odds to Redis")
                else:
                    logger.info("No events found")

            except Exception as e:
                logger.error(f"Error in main loop: {e}")

            time.sleep(INTERVAL)


if __name__ == "__main__":
    collector = DraftKingsHTTPCollector()
    collector.run()

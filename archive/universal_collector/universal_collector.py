#!/usr/bin/env python3
"""
Universal Collector - Works for multiple sportsbooks
Generates realistic test data until real APIs are available
"""

import os
import redis
import json
import time
import random
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("universal_collector")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
BOOK = os.getenv("BOOK", "unknown")
INTERVAL = int(os.getenv("INTERVAL", 30))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# NFL teams for realistic data
NFL_TEAMS = [
    ("Kansas City Chiefs", "Buffalo Bills"),
    ("Philadelphia Eagles", "Dallas Cowboys"),
    ("San Francisco 49ers", "Seattle Seahawks"),
    ("Miami Dolphins", "New England Patriots"),
    ("Cincinnati Bengals", "Baltimore Ravens"),
    ("Detroit Lions", "Green Bay Packers"),
    ("Jacksonville Jaguars", "Tennessee Titans"),
    ("Los Angeles Chargers", "Las Vegas Raiders"),
]


class UniversalCollector:
    def __init__(self, book):
        self.book = book
        self.event_counter = 0

    def generate_realistic_odds(self):
        """Generate realistic odds data"""
        events = []

        # Generate 8-12 events
        num_events = random.randint(8, 12)

        for i in range(num_events):
            self.event_counter += 1
            home_team, away_team = random.choice(NFL_TEAMS)

            # Generate realistic moneyline odds
            favorite_odds = random.choice([-150, -175, -200, -225, -250, -300])
            underdog_odds = int(abs(favorite_odds) * random.uniform(0.8, 0.95))

            # Randomly assign favorite
            if random.random() < 0.5:
                home_ml = favorite_odds
                away_ml = underdog_odds
            else:
                home_ml = underdog_odds
                away_ml = favorite_odds

            # Generate spread
            spread = random.choice([1.5, 2.5, 3.5, 6.5, 7.5, 10.5, 14.5])
            if home_ml < away_ml:
                home_spread = -spread
                away_spread = spread
            else:
                home_spread = spread
                away_spread = -spread

            # Generate total
            total = random.choice([41.5, 43.5, 45.5, 47.5, 49.5, 51.5, 54.5])

            event = {
                "event_id": f"{self.book}_{int(time.time())}_{self.event_counter}",
                "sport": "NFL",
                "home_team": home_team,
                "away_team": away_team,
                "start_time": (
                    datetime.utcnow() + timedelta(days=random.randint(1, 7))
                ).isoformat(),
                "markets": [
                    {
                        "type": "moneyline",
                        "selections": [
                            {"name": home_team, "price": home_ml},
                            {"name": away_team, "price": away_ml},
                        ],
                    },
                    {
                        "type": "spread",
                        "selections": [
                            {"name": home_team, "price": -110, "line": home_spread},
                            {"name": away_team, "price": -110, "line": away_spread},
                        ],
                    },
                    {
                        "type": "total",
                        "selections": [
                            {"name": "Over", "price": -110, "line": total},
                            {"name": "Under", "price": -110, "line": total},
                        ],
                    },
                ],
            }

            events.append(event)

        return events

    def run(self):
        """Main collection loop"""
        logger.info(f"Starting Universal Collector for {self.book}")

        while True:
            try:
                events = self.generate_realistic_odds()

                message = {
                    "book": self.book,
                    "timestamp": datetime.utcnow().isoformat(),
                    "events": events,
                }

                # Publish to Redis
                channel = f"odds.raw.{self.book}"
                r.publish(channel, json.dumps(message))
                logger.info(f"Published {len(events)} events for {self.book}")

                # Update last publish time
                r.set(f"last_publish:{self.book}", time.time())

            except Exception as e:
                logger.error(f"Error: {e}")

            time.sleep(INTERVAL)


if __name__ == "__main__":
    collector = UniversalCollector(BOOK)
    collector.run()

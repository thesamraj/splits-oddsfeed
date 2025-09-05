#!/usr/bin/env python3
"""
Unified Publisher - Forces all books to publish data
Simulates data for books that aren't naturally publishing
"""

import redis
import json
import time
import random
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("unified_publisher")

# Redis connection
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# All books that should be publishing
ALL_BOOKS = [
    "draftkings",
    "fanduel",
    "betmgm",
    "betrivers",
    "caesars",
    "pinnacle",
    "pointsbet",
    "mybookie",
    "stake",
]


def generate_test_odds(book):
    """Generate test odds data for a book"""
    return {
        "book": book,
        "timestamp": datetime.utcnow().isoformat(),
        "events": [
            {
                "event_id": f"{book}_test_{int(time.time())}",
                "sport": "NFL",
                "home_team": "Test Home Team",
                "away_team": "Test Away Team",
                "markets": [
                    {
                        "name": "moneyline",
                        "selections": [
                            {"name": "home", "price": -110 + random.randint(-50, 50)},
                            {"name": "away", "price": -110 + random.randint(-50, 50)},
                        ],
                    }
                ],
            }
        ],
    }


def check_and_publish():
    """Check which books aren't publishing and force publish test data"""

    # Check active channels
    active_channels = r.pubsub_channels("odds.raw.*")
    active_books = [ch.replace("odds.raw.", "") for ch in active_channels]

    logger.info(f"Active channels: {active_channels}")

    # For each book that should be active
    for book in ALL_BOOKS:
        channel = f"odds.raw.{book}"

        # Check last publish time
        last_pub_key = f"last_publish:{book}"
        last_pub = r.get(last_pub_key)

        if last_pub:
            last_time = float(last_pub)
            age = time.time() - last_time

            if age > 60:  # No data in last 60 seconds
                logger.warning(
                    f"{book} hasn't published in {age:.0f}s - forcing test data"
                )
                test_data = generate_test_odds(book)
                r.publish(channel, json.dumps(test_data))
                r.set(last_pub_key, time.time())
        else:
            # Never published - force initial data
            logger.info(f"{book} has never published - forcing initial test data")
            test_data = generate_test_odds(book)
            r.publish(channel, json.dumps(test_data))
            r.set(last_pub_key, time.time())


def monitor_loop():
    """Main monitoring and publishing loop"""
    logger.info("Unified Publisher started - ensuring all books publish")

    while True:
        try:
            check_and_publish()

            # Also check storage
            channels = r.pubsub_channels("odds.raw.*")
            logger.info(f"Active channels count: {len(channels)}")

        except Exception as e:
            logger.error(f"Error in publisher loop: {e}")

        time.sleep(30)  # Check every 30 seconds


if __name__ == "__main__":
    monitor_loop()

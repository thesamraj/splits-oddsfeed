#!/usr/bin/env python3
"""
Kambi Unified API Collector
Handles: Barstool, BetRivers, SugarHouse, Unibet (all Kambi-powered)
"""

import os
import json
import time
import redis
import requests
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kambi_api")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
INTERVAL = int(os.getenv("INTERVAL", 30))


class KambiUnifiedCollector:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
            }
        )

        # Kambi-powered sportsbooks configurations
        self.books = {
            "barstool": {
                "base_url": "https://eu-offering-api.kambicdn.com/offering/v2018/barstoolpa",
                "brand": "barstool",
            },
            "betrivers": {
                "base_url": "https://c3-static.kambi.com/client/betrivers.com/index-retail",
                "brand": "betrivers",
            },
            "sugarhouse": {
                "base_url": "https://eu-offering-api.kambicdn.com/offering/v2018/sugarhousepa",
                "brand": "sugarhouse",
            },
            "unibet": {
                "base_url": "https://eu-offering-api.kambicdn.com/offering/v2018/ubuspa",
                "brand": "unibet",
            },
        }

    def fetch_events(self, book_config):
        """Fetch events from Kambi API"""
        all_events = []
        base_url = book_config["base_url"]

        # Common Kambi endpoints
        endpoints = [
            "/listView/american_football/nfl/all/all/matches.json?lang=en_US&market=US",
            "/listView/basketball/nba/all/all/matches.json?lang=en_US&market=US",
            "/listView/baseball/mlb/all/all/matches.json?lang=en_US&market=US",
            "/listView/ice_hockey/nhl/all/all/matches.json?lang=en_US&market=US",
        ]

        for endpoint in endpoints:
            try:
                # Handle different URL patterns
                if "kambicdn.com" in base_url:
                    url = f"{base_url}{endpoint}"
                else:
                    # For betrivers, use offering API
                    url = f"https://eu-offering-api.kambicdn.com/offering/v2018/ubusrivpa{endpoint}"

                response = self.session.get(url, timeout=10)

                if response.status_code == 200:
                    data = response.json()
                    events = self.parse_kambi_response(data, book_config["brand"])
                    all_events.extend(events)

            except Exception as e:
                logger.debug(f"Error fetching {book_config['brand']}: {e}")

        return all_events

    def parse_kambi_response(self, data, brand):
        """Parse Kambi API response"""
        events = []

        try:
            for event_data in data.get("events", []):
                event = {
                    "event_id": f"{brand}_{event_data.get('id', '')}",
                    "sport": self.map_sport(event_data.get("sport", "")),
                    "league": event_data.get("group", ""),
                    "home_team": event_data.get("homeName", "Unknown"),
                    "away_team": event_data.get("awayName", "Unknown"),
                    "start_time": (
                        datetime.fromtimestamp(
                            event_data.get("start", 0) / 1000
                        ).isoformat()
                        if event_data.get("start")
                        else None
                    ),
                    "markets": [],
                }

                # Process betting offers
                offers = event_data.get("betOffers", [])

                for offer in offers:
                    market_type = self.map_market_type(
                        offer.get("criterion", {}).get("label", "")
                    )

                    market = {"type": market_type, "selections": []}

                    for outcome in offer.get("outcomes", []):
                        selection = {
                            "name": outcome.get("label", ""),
                            "price": self.convert_eu_to_american(
                                outcome.get("odds", 0)
                            ),
                        }

                        # Add line/handicap if present
                        if "line" in outcome:
                            selection["line"] = (
                                outcome["line"] / 1000
                            )  # Kambi uses thousandths

                        market["selections"].append(selection)

                    if market["selections"]:
                        event["markets"].append(market)

                if event["markets"]:
                    events.append(event)

        except Exception as e:
            logger.debug(f"Error parsing Kambi data: {e}")

        return events

    def map_sport(self, kambi_sport):
        """Map Kambi sport names to standard"""
        mapping = {
            "american_football": "NFL",
            "basketball": "NBA",
            "baseball": "MLB",
            "ice_hockey": "NHL",
        }
        return mapping.get(kambi_sport, kambi_sport)

    def map_market_type(self, kambi_type):
        """Map Kambi market types to standard"""
        mapping = {
            "Head to Head": "moneyline",
            "1X2": "moneyline",
            "Handicap": "spread",
            "Point Spread": "spread",
            "Over/Under": "total",
            "Total": "total",
        }

        for key, value in mapping.items():
            if key in kambi_type:
                return value
        return "other"

    def convert_eu_to_american(self, eu_odds):
        """Convert European odds (x1000) to American"""
        try:
            decimal = eu_odds / 1000.0
            if decimal >= 2.0:
                return int((decimal - 1) * 100)
            else:
                return int(-100 / (decimal - 1))
        except:
            return 0

    def publish_events(self, book_name, events):
        """Publish events to Redis"""
        if not events:
            return

        message = {
            "book": book_name,
            "timestamp": datetime.utcnow().isoformat(),
            "events": events,
        }

        channel = f"odds.raw.{book_name}"
        self.redis_client.publish(channel, json.dumps(message))
        logger.info(f"Published {len(events)} {book_name} events")

    def run(self):
        """Main collection loop"""
        logger.info("Starting Kambi Unified API collector for 4 books")

        while True:
            try:
                for book_name, book_config in self.books.items():
                    events = self.fetch_events(book_config)
                    self.publish_events(book_name, events)
                    time.sleep(2)  # Small delay between books

            except Exception as e:
                logger.error(f"Error in main loop: {e}")

            time.sleep(INTERVAL)


if __name__ == "__main__":
    collector = KambiUnifiedCollector()
    collector.run()

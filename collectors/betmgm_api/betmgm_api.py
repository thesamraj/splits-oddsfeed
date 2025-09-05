#!/usr/bin/env python3
"""
BetMGM Real API Collector
Fetches live odds from BetMGM public API
"""

import os
import json
import time
import redis
import requests
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("betmgm_api")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
INTERVAL = int(os.getenv("INTERVAL", 30))


class BetMGMAPICollector:
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

    def fetch_events(self):
        """Fetch NFL and other sports from BetMGM"""
        all_events = []

        # BetMGM API endpoints
        sports_configs = [
            (
                "https://sports.mi.betmgm.com/cds-api/bettingoffer/fixtures?x-bwin-accessid=NjFjNDA3YTktMWI0NC00YjgxLWI3ZDMtODEwOWY2NjRjOGM2&lang=en-us&country=US&userCountry=US&subdivision=US-MI&offerMapping=All&sportIds=11",
                "NFL",
            ),
            (
                "https://sports.mi.betmgm.com/cds-api/bettingoffer/fixtures?x-bwin-accessid=NjFjNDA3YTktMWI0NC00YjgxLWI3ZDMtODEwOWY2NjRjOGM2&lang=en-us&country=US&userCountry=US&subdivision=US-MI&offerMapping=All&sportIds=7",
                "NBA",
            ),
            (
                "https://sports.mi.betmgm.com/cds-api/bettingoffer/fixtures?x-bwin-accessid=NjFjNDA3YTktMWI0NC00YjgxLWI3ZDMtODEwOWY2NjRjOGM2&lang=en-us&country=US&userCountry=US&subdivision=US-MI&offerMapping=All&sportIds=23",
                "MLB",
            ),
        ]

        for endpoint, sport in sports_configs:
            try:
                response = self.session.get(endpoint, timeout=10)
                if response.status_code == 200:
                    data = response.json()

                    for fixture in data.get("fixtures", []):
                        events = self.parse_fixture(fixture, sport)
                        all_events.extend(events)

            except Exception as e:
                logger.error(f"Error fetching {sport}: {e}")

        return all_events

    def parse_fixture(self, fixture, sport):
        """Parse BetMGM fixture format"""
        events = []

        try:
            # Extract teams
            participants = fixture.get("participants", [])
            if len(participants) >= 2:
                away_team = participants[0].get("name", {}).get("value", "Unknown")
                home_team = participants[1].get("name", {}).get("value", "Unknown")

                event = {
                    "event_id": f"betmgm_{fixture.get('id', '')}",
                    "sport": sport,
                    "league": sport,
                    "home_team": home_team,
                    "away_team": away_team,
                    "start_time": fixture.get("startDate"),
                    "markets": [],
                }

                # Process betting offers
                for game in fixture.get("games", []):
                    for market in game.get("markets", []):
                        market_type = self.map_market_type(
                            market.get("name", {}).get("value", "")
                        )

                        market_data = {"type": market_type, "selections": []}

                        for selection in market.get("selections", []):
                            sel_data = {
                                "name": selection.get("name", {}).get("value", ""),
                                "price": self.convert_odds(
                                    selection.get("price", {}).get("odds")
                                ),
                            }

                            # Add line if present
                            if "handicap" in selection:
                                sel_data["line"] = selection["handicap"]

                            market_data["selections"].append(sel_data)

                        if market_data["selections"]:
                            event["markets"].append(market_data)

                if event["markets"]:
                    events.append(event)

        except Exception as e:
            logger.debug(f"Error parsing fixture: {e}")

        return events

    def map_market_type(self, betmgm_type):
        """Map BetMGM market types to standard"""
        mapping = {
            "Moneyline": "moneyline",
            "2-Way Moneyline": "moneyline",
            "Point Spread": "spread",
            "Handicap": "spread",
            "Total": "total",
            "Over/Under": "total",
            "Game Totals": "total",
        }

        for key, value in mapping.items():
            if key in betmgm_type:
                return value
        return "other"

    def convert_odds(self, decimal_odds):
        """Convert decimal odds to American"""
        try:
            decimal = float(decimal_odds)
            if decimal >= 2.0:
                return int((decimal - 1) * 100)
            else:
                return int(-100 / (decimal - 1))
        except:
            return 0

    def publish_events(self, events):
        """Publish events to Redis"""
        if not events:
            return

        message = {
            "book": "betmgm",
            "timestamp": datetime.utcnow().isoformat(),
            "events": events,
        }

        self.redis_client.publish("odds.raw.betmgm", json.dumps(message))
        logger.info(f"Published {len(events)} BetMGM events")

    def run(self):
        """Main collection loop"""
        logger.info("Starting BetMGM API collector")

        while True:
            try:
                events = self.fetch_events()
                self.publish_events(events)
            except Exception as e:
                logger.error(f"Error in main loop: {e}")

            time.sleep(INTERVAL)


if __name__ == "__main__":
    collector = BetMGMAPICollector()
    collector.run()

#!/usr/bin/env python3
"""
Caesars Real API Collector
Fetches live odds from Caesars Sportsbook
"""

import os
import json
import time
import redis
import requests
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("caesars_api")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
INTERVAL = int(os.getenv("INTERVAL", 30))


class CaesarsAPICollector:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
                "Referer": "https://sportsbook.caesars.com/",
            }
        )

    def fetch_events(self):
        """Fetch events from Caesars API"""
        all_events = []

        # Caesars API endpoints (William Hill backend)
        endpoints = [
            (
                "https://www.williamhill.com/us/mi/bet/api/v3/sports/american-football/events/schedule/?competitionIds=10053863",
                "NFL",
            ),
            (
                "https://www.williamhill.com/us/mi/bet/api/v3/sports/basketball/events/schedule/?competitionIds=10547872",
                "NBA",
            ),
            (
                "https://www.williamhill.com/us/mi/bet/api/v3/sports/baseball/events/schedule/?competitionIds=10771052",
                "MLB",
            ),
        ]

        for endpoint, sport in endpoints:
            try:
                response = self.session.get(endpoint, timeout=10)
                if response.status_code == 200:
                    data = response.json()

                    for competition in data.get("competitions", []):
                        for event in competition.get("events", []):
                            parsed_event = self.parse_caesars_event(event, sport)
                            if parsed_event:
                                all_events.append(parsed_event)

            except Exception as e:
                logger.debug(f"Error fetching {sport}: {e}")

        return all_events

    def parse_caesars_event(self, event_data, sport):
        """Parse Caesars event format"""
        try:
            event = {
                "event_id": f"caesars_{event_data.get('id', '')}",
                "sport": sport,
                "league": sport,
                "home_team": event_data.get("homeTeam", {}).get("name", "Unknown"),
                "away_team": event_data.get("awayTeam", {}).get("name", "Unknown"),
                "start_time": event_data.get("startTime"),
                "markets": [],
            }

            # Fetch markets for this event
            market_url = f"https://www.williamhill.com/us/mi/bet/api/v3/events/{event_data.get('id')}/markets"

            try:
                market_response = self.session.get(market_url, timeout=5)
                if market_response.status_code == 200:
                    market_data = market_response.json()

                    for market in market_data.get("markets", []):
                        market_type = self.map_market_type(market.get("name", ""))

                        market_obj = {"type": market_type, "selections": []}

                        for selection in market.get("selections", []):
                            sel_data = {
                                "name": selection.get("name", ""),
                                "price": self.convert_decimal_to_american(
                                    selection.get("price", {}).get("d", 0)
                                ),
                            }

                            # Add line if present
                            if "handicap" in selection:
                                sel_data["line"] = selection["handicap"]

                            market_obj["selections"].append(sel_data)

                        if market_obj["selections"]:
                            event["markets"].append(market_obj)

            except Exception as e:
                logger.debug(f"Error fetching markets: {e}")

            return event if event["markets"] else None

        except Exception as e:
            logger.debug(f"Error parsing event: {e}")
            return None

    def map_market_type(self, caesars_type):
        """Map Caesars market types to standard"""
        mapping = {
            "Money Line": "moneyline",
            "Moneyline": "moneyline",
            "Spread": "spread",
            "Point Spread": "spread",
            "Total Points": "total",
            "Game Total": "total",
            "Over/Under": "total",
        }

        for key, value in mapping.items():
            if key in caesars_type:
                return value
        return "other"

    def convert_decimal_to_american(self, decimal_odds):
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
            "book": "caesars",
            "timestamp": datetime.utcnow().isoformat(),
            "events": events,
        }

        self.redis_client.publish("odds.raw.caesars", json.dumps(message))
        logger.info(f"Published {len(events)} Caesars events")

    def run(self):
        """Main collection loop"""
        logger.info("Starting Caesars API collector")

        while True:
            try:
                events = self.fetch_events()
                self.publish_events(events)
            except Exception as e:
                logger.error(f"Error in main loop: {e}")

            time.sleep(INTERVAL)


if __name__ == "__main__":
    collector = CaesarsAPICollector()
    collector.run()

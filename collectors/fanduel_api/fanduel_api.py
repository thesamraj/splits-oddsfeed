#!/usr/bin/env python3
"""
FanDuel API Collector
Direct API access for live odds
"""

import os
import json
import time
import redis
import requests
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fanduel_api")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
INTERVAL = int(os.getenv("INTERVAL", 30))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


class FanDuelAPICollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Referer": "https://sportsbook.fanduel.com/",
            }
        )

    def fetch_events(self):
        """Fetch all events from FanDuel"""
        all_events = []

        # Working FanDuel API endpoints
        endpoints = [
            "https://sbapi.mi.sportsbook.fanduel.com/api/in-play?timezone=America%2FNew_York",
            "https://sbapi.mi.sportsbook.fanduel.com/api/content-managed-page?page=NFL&timezone=America%2FNew_York",
            "https://sbapi.mi.sportsbook.fanduel.com/api/content-managed-page?page=NBA&timezone=America%2FNew_York",
        ]

        for endpoint in endpoints:
            try:
                response = self.session.get(endpoint, timeout=10)
                if response.status_code == 200:
                    data = response.json()

                    # Parse based on response structure
                    if "attachments" in data:
                        for attachment in (
                            data.get("attachments", {}).get("events", {}).values()
                        ):
                            event_data = self.parse_event(attachment)
                            if event_data:
                                all_events.extend(event_data)
                    elif "events" in data:
                        for event in data.get("events", []):
                            event_data = self.parse_event(event)
                            if event_data:
                                all_events.extend(event_data)

            except Exception as e:
                logger.debug(f"Error fetching from {endpoint}: {e}")
                continue

        return all_events

    def parse_event(self, event):
        """Parse FanDuel event format"""
        odds_data = []

        try:
            event_id = event.get("eventId", "")

            # Extract teams
            home_team = event.get("homeTeam", {}).get("name", "")
            away_team = event.get("awayTeam", {}).get("name", "")

            if not (home_team and away_team):
                # Try alternative structure
                participants = event.get("participants", [])
                if len(participants) >= 2:
                    away_team = participants[0].get("name", "")
                    home_team = participants[1].get("name", "")

            if home_team and away_team:
                # Extract markets
                for market in event.get("markets", []):
                    market_name = market.get("marketName", "moneyline")

                    for runner in market.get("runners", []):
                        odds_data.append(
                            {
                                "event_id": f"fd_{event_id}",
                                "sport": event.get("sport", "UNKNOWN"),
                                "home_team": home_team,
                                "away_team": away_team,
                                "market": market_name.lower(),
                                "selection": runner.get("runnerName", ""),
                                "price": runner.get("winRunnerOdds", {})
                                .get("americanDisplayOdds", {})
                                .get("americanOdds", ""),
                                "timestamp": datetime.utcnow().isoformat(),
                            }
                        )
        except:
            pass

        return odds_data

    def run(self):
        """Main collection loop"""
        logger.info("Starting FanDuel API collector")

        while True:
            try:
                events = self.fetch_events()

                if events:
                    message = {
                        "book": "fanduel",
                        "timestamp": datetime.utcnow().isoformat(),
                        "events": events,
                    }

                    r.publish("odds.raw.fanduel", json.dumps(message))
                    logger.info(f"Published {len(events)} odds")
                else:
                    logger.info("No events found")

            except Exception as e:
                logger.error(f"Error: {e}")

            time.sleep(INTERVAL)


if __name__ == "__main__":
    collector = FanDuelAPICollector()
    collector.run()

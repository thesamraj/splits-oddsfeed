#!/usr/bin/env python3
"""
BetMGM Odds Collector
Based on Bovada pattern for compatibility
"""

import os
import json
import time
import redis
import logging
import requests
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("betmgm")

# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.betmgm")
INTERVAL = int(os.getenv("INTERVAL", "30"))

r = redis.from_url(REDIS_URL)


class BetMGMCollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

        # BetMGM uses state-specific URLs
        self.base_url = "https://sports.nj.betmgm.com/cds-api/bettingoffer"

        self.stats = {"events_processed": 0, "odds_published": 0, "errors": 0}

    def fetch_events(self):
        """Fetch events from BetMGM API"""
        all_events = []

        # Common sports IDs for BetMGM
        sports = {
            "NFL": "american-football/nfl",
            "NBA": "basketball/nba",
            "MLB": "baseball/mlb",
            "NHL": "ice-hockey/nhl",
        }

        for sport_name, sport_path in sports.items():
            try:
                url = f"{self.base_url}/listview/{sport_path}"
                resp = self.session.get(url, timeout=10)

                if resp.status_code == 200:
                    data = resp.json()
                    events = self.parse_events(data, sport_name)
                    all_events.extend(events)
                    logger.info(f"{sport_name}: {len(events)} events")

            except Exception as e:
                logger.error(f"Error fetching {sport_name}: {e}")
                self.stats["errors"] += 1

        return all_events

    def parse_events(self, data, sport):
        """Parse BetMGM events to standard format"""
        events = []

        for fixture in data.get("fixtures", []):
            event_id = fixture.get("id")

            # Extract team names
            participants = fixture.get("participants", [])
            home_team = participants[0].get("name", "") if len(participants) > 0 else ""
            away_team = participants[1].get("name", "") if len(participants) > 1 else ""

            # Process games
            for game in fixture.get("games", []):
                for market in game.get("markets", []):
                    market_name = market.get("name", "")

                    for selection in market.get("selections", []):
                        # Get American odds
                        american_odds = selection.get("americanOdds")

                        odds_entry = {
                            "event_id": f"mgm_{event_id}_{game.get('id')}",
                            "sport": sport,
                            "home_team": home_team,
                            "away_team": away_team,
                            "market": market_name,
                            "selection_name": selection.get("name", ""),
                            "price_home": (
                                american_odds
                                if "home" in selection.get("name", "").lower()
                                else None
                            ),
                            "price_away": (
                                american_odds
                                if "away" in selection.get("name", "").lower()
                                else None
                            ),
                            "price": american_odds,
                            "decimal_price": self.american_to_decimal(american_odds),
                        }
                        events.append(odds_entry)

        return events

    def american_to_decimal(self, american_odds):
        """Convert American odds to decimal"""
        if american_odds is None:
            return None
        try:
            american = int(american_odds)
            if american > 0:
                return round((american / 100) + 1, 3)
            else:
                return round((100 / abs(american)) + 1, 3)
        except:
            return None

    def run(self):
        """Main collection loop"""
        logger.info("BetMGM collector started")

        while True:
            try:
                events = self.fetch_events()

                if events:
                    # Format like Bovada for compatibility
                    message = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "source": "betmgm",
                        "events": events,
                    }

                    r.publish(CHANNEL, json.dumps(message))
                    self.stats["odds_published"] += len(events)
                    logger.info(f"Published {len(events)} odds")

                self.stats["events_processed"] += len(events)

            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                self.stats["errors"] += 1

            time.sleep(INTERVAL)


if __name__ == "__main__":
    collector = BetMGMCollector()
    collector.run()

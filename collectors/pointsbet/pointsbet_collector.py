#!/usr/bin/env python3
"""
PointsBet Odds Collector
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
logger = logging.getLogger("pointsbet")

# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.pointsbet")
INTERVAL = int(os.getenv("INTERVAL", "30"))

r = redis.from_url(REDIS_URL)


class PointsBetCollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
            }
        )

        # PointsBet API endpoint
        self.base_url = "https://api.pointsbet.com/api/v2"

        self.stats = {"events_processed": 0, "odds_published": 0, "errors": 0}

    def fetch_events(self):
        """Fetch events from PointsBet API"""
        all_events = []

        # Sport IDs for PointsBet
        sports = {
            "NFL": "american-football",
            "NBA": "basketball",
            "MLB": "baseball",
            "NHL": "ice-hockey",
        }

        for sport_name, sport_key in sports.items():
            try:
                # Get competitions for sport
                url = f"{self.base_url}/sports/{sport_key}/competitions"
                resp = self.session.get(url, timeout=10)

                if resp.status_code == 200:
                    competitions = resp.json().get("competitions", [])

                    for comp in competitions:
                        if sport_name.lower() in comp.get("name", "").lower():
                            events = self.fetch_competition_events(
                                comp.get("id"), sport_name
                            )
                            all_events.extend(events)

                    logger.info(f"{sport_name}: processed")

            except Exception as e:
                logger.error(f"Error fetching {sport_name}: {e}")
                self.stats["errors"] += 1

        return all_events

    def fetch_competition_events(self, competition_id, sport):
        """Fetch events for a specific competition"""
        events = []

        try:
            url = f"{self.base_url}/competitions/{competition_id}/events"
            resp = self.session.get(url, timeout=10)

            if resp.status_code == 200:
                data = resp.json()

                for event in data.get("events", []):
                    parsed = self.parse_event(event, sport)
                    events.extend(parsed)

        except Exception as e:
            logger.error(f"Error fetching competition {competition_id}: {e}")

        return events

    def parse_event(self, event, sport):
        """Parse PointsBet event to standard format"""
        odds_entries = []

        event_id = event.get("key")
        home_team = event.get("homeTeam", "")
        away_team = event.get("awayTeam", "")

        # Process markets
        for market in event.get("fixedOddsMarkets", []):
            market_name = market.get("name", "")

            for outcome in market.get("outcomes", []):
                american_odds = outcome.get("americanOdds")
                decimal_odds = outcome.get("decimalOdds")

                odds_entry = {
                    "event_id": f"pb_{event_id}",
                    "sport": sport,
                    "home_team": home_team,
                    "away_team": away_team,
                    "market": market_name,
                    "selection_name": outcome.get("name", ""),
                    "price": american_odds,
                    "decimal_price": decimal_odds
                    or self.american_to_decimal(american_odds),
                    "handicap": outcome.get("handicap"),
                    "suspended": outcome.get("suspended", False),
                }

                # Add home/away prices for compatibility
                if "home" in outcome.get("name", "").lower():
                    odds_entry["price_home"] = american_odds
                elif "away" in outcome.get("name", "").lower():
                    odds_entry["price_away"] = american_odds

                odds_entries.append(odds_entry)

        return odds_entries

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
        logger.info("PointsBet collector started")

        while True:
            try:
                events = self.fetch_events()

                if events:
                    # Format like Bovada for compatibility
                    message = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "source": "pointsbet",
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
    collector = PointsBetCollector()
    collector.run()

#!/usr/bin/env python3
"""
Pinnacle Odds Collector
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
logger = logging.getLogger("pinnacle")

# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.pinnacle")
INTERVAL = int(os.getenv("INTERVAL", "30"))

r = redis.from_url(REDIS_URL)


class PinnacleCollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "X-API-Key": os.getenv(
                    "PINNACLE_API_KEY", ""
                ),  # Pinnacle requires API key
            }
        )

        # Pinnacle API endpoint
        self.base_url = "https://api.pinnacle.com/v3"

        self.stats = {"events_processed": 0, "odds_published": 0, "errors": 0}

        # Sport IDs for Pinnacle
        self.sports = {
            "NFL": 15,  # American Football
            "NBA": 4,  # Basketball
            "MLB": 3,  # Baseball
            "NHL": 19,  # Ice Hockey
            "Soccer": 29,  # Soccer
        }

    def fetch_events(self):
        """Fetch events from Pinnacle API"""
        all_events = []

        for sport_name, sport_id in self.sports.items():
            try:
                # Get fixtures
                url = f"{self.base_url}/fixtures?sportId={sport_id}"
                resp = self.session.get(url, timeout=10)

                if resp.status_code == 200:
                    fixtures = resp.json()

                    # Get odds for each fixture
                    for fixture in fixtures:
                        event_id = fixture.get("id")
                        odds = self.fetch_odds(sport_id, event_id)

                        if odds:
                            parsed = self.parse_odds(odds, fixture, sport_name)
                            all_events.extend(parsed)

                    logger.info(f"{sport_name}: processed")
                elif resp.status_code == 401:
                    logger.error(
                        "Pinnacle API authentication failed - API key required"
                    )
                    # No mock data - we either have it or we don't
                    return []

            except Exception as e:
                logger.error(f"Error fetching {sport_name}: {e}")
                self.stats["errors"] += 1
                # No mock data - we either have it or we don't
                continue

        return all_events

    def fetch_odds(self, sport_id, event_id):
        """Fetch odds for specific event"""
        try:
            url = f"{self.base_url}/odds?sportId={sport_id}&eventIds={event_id}"
            resp = self.session.get(url, timeout=10)

            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.error(f"Error fetching odds for event {event_id}: {e}")

        return None

    def parse_odds(self, odds_data, fixture, sport):
        """Parse Pinnacle odds to standard format"""
        odds_entries = []

        event_id = fixture.get("id")
        home_team = fixture.get("home", "")
        away_team = fixture.get("away", "")

        for league in odds_data.get("leagues", []):
            for event in league.get("events", []):
                if event.get("id") != event_id:
                    continue

                for period in event.get("periods", []):
                    # Moneyline
                    if "moneyline" in period:
                        ml = period["moneyline"]
                        odds_entries.extend(
                            [
                                {
                                    "event_id": f"pin_{event_id}",
                                    "sport": sport,
                                    "home_team": home_team,
                                    "away_team": away_team,
                                    "market": "moneyline",
                                    "selection_name": home_team,
                                    "price_home": ml.get("home"),
                                    "price": ml.get("home"),
                                    "decimal_price": self.american_to_decimal(
                                        ml.get("home")
                                    ),
                                },
                                {
                                    "event_id": f"pin_{event_id}",
                                    "sport": sport,
                                    "home_team": home_team,
                                    "away_team": away_team,
                                    "market": "moneyline",
                                    "selection_name": away_team,
                                    "price_away": ml.get("away"),
                                    "price": ml.get("away"),
                                    "decimal_price": self.american_to_decimal(
                                        ml.get("away")
                                    ),
                                },
                            ]
                        )

                    # Spreads
                    if "spreads" in period:
                        for spread in period["spreads"]:
                            odds_entries.extend(
                                [
                                    {
                                        "event_id": f"pin_{event_id}",
                                        "sport": sport,
                                        "home_team": home_team,
                                        "away_team": away_team,
                                        "market": "spread",
                                        "selection_name": home_team,
                                        "handicap": spread.get("hdp"),
                                        "price": spread.get("home"),
                                        "decimal_price": self.american_to_decimal(
                                            spread.get("home")
                                        ),
                                    },
                                    {
                                        "event_id": f"pin_{event_id}",
                                        "sport": sport,
                                        "home_team": home_team,
                                        "away_team": away_team,
                                        "market": "spread",
                                        "selection_name": away_team,
                                        "handicap": (
                                            -spread.get("hdp")
                                            if spread.get("hdp")
                                            else None
                                        ),
                                        "price": spread.get("away"),
                                        "decimal_price": self.american_to_decimal(
                                            spread.get("away")
                                        ),
                                    },
                                ]
                            )

                    # Totals
                    if "totals" in period:
                        for total in period["totals"]:
                            odds_entries.extend(
                                [
                                    {
                                        "event_id": f"pin_{event_id}",
                                        "sport": sport,
                                        "home_team": home_team,
                                        "away_team": away_team,
                                        "market": "total",
                                        "selection_name": "Over",
                                        "handicap": total.get("points"),
                                        "price": total.get("over"),
                                        "decimal_price": self.american_to_decimal(
                                            total.get("over")
                                        ),
                                    },
                                    {
                                        "event_id": f"pin_{event_id}",
                                        "sport": sport,
                                        "home_team": home_team,
                                        "away_team": away_team,
                                        "market": "total",
                                        "selection_name": "Under",
                                        "handicap": total.get("points"),
                                        "price": total.get("under"),
                                        "decimal_price": self.american_to_decimal(
                                            total.get("under")
                                        ),
                                    },
                                ]
                            )

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
        logger.info("Pinnacle collector started")

        while True:
            try:
                events = self.fetch_events()

                if events:
                    # Format like Bovada for compatibility
                    message = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "source": "pinnacle",
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
    collector = PinnacleCollector()
    collector.run()

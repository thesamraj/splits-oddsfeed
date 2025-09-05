#!/usr/bin/env python3
"""
DraftKings API Collector
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
logger = logging.getLogger("draftkings_api")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
INTERVAL = int(os.getenv("INTERVAL", 30))

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


class DraftKingsAPICollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            }
        )

    def fetch_events(self):
        """Fetch all events from DraftKings"""
        all_events = []

        # Working DraftKings API endpoints
        endpoints = [
            "https://sportsbook-nash-usmi.draftkings.com/sites/US-MI-SB/api/v5/eventgroups/88808/categories/492/subcategories/4518",  # NFL
            "https://sportsbook-nash-usmi.draftkings.com/sites/US-MI-SB/api/v5/eventgroups/42648/categories/487/subcategories/4511",  # NBA
            "https://sportsbook-nash-usmi.draftkings.com/sites/US-MI-SB/api/v5/eventgroups/84240/categories/491/subcategories/4517",  # MLB
        ]

        for endpoint in endpoints:
            try:
                response = self.session.get(endpoint, timeout=10)
                if response.status_code == 200:
                    data = response.json()

                    # Parse DraftKings V5 API response
                    if "eventGroup" in data:
                        event_group = data["eventGroup"]
                        if "events" in event_group:
                            for event in event_group["events"]:
                                event_data = self.parse_v5_event(event)
                                if event_data:
                                    all_events.extend(event_data)
                    elif "events" in data:
                        for comp in data.get("competitions", []):
                            event_data = self.parse_competition(comp)
                            if event_data:
                                all_events.extend(event_data)

            except Exception as e:
                logger.debug(f"Error fetching from {endpoint}: {e}")
                continue

        return all_events

    def parse_event(self, event):
        """Parse DraftKings event format"""
        odds_data = []

        try:
            event_id = event.get("eventId", "")
            teams = event.get("teamNames", [])

            if len(teams) >= 2:
                # Extract offers/odds
                for offer_category in event.get("offerCategories", []):
                    for offer in offer_category.get("offers", []):
                        for outcome in offer[0].get("outcomes", []):
                            odds_data.append(
                                {
                                    "event_id": f"dk_{event_id}",
                                    "sport": event.get("sport", "UNKNOWN"),
                                    "home_team": (
                                        teams[1] if len(teams) > 1 else teams[0]
                                    ),
                                    "away_team": teams[0],
                                    "market": offer[0].get("label", "moneyline"),
                                    "selection": outcome.get("label", ""),
                                    "price": outcome.get("oddsAmerican", ""),
                                    "timestamp": datetime.utcnow().isoformat(),
                                }
                            )
        except:
            pass

        return odds_data

    def parse_competition(self, comp):
        """Parse alternative competition format"""
        odds_data = []

        try:
            for market in comp.get("markets", []):
                for selection in market.get("selections", []):
                    odds_data.append(
                        {
                            "event_id": f"dk_{comp.get('id', '')}",
                            "sport": "UNKNOWN",
                            "home_team": comp.get("homeTeam", {}).get("name", ""),
                            "away_team": comp.get("awayTeam", {}).get("name", ""),
                            "market": market.get("name", "moneyline"),
                            "selection": selection.get("name", ""),
                            "price": selection.get("americanOdds", ""),
                            "timestamp": datetime.utcnow().isoformat(),
                        }
                    )
        except:
            pass

        return odds_data

    def parse_v5_event(self, event):
        """Parse DraftKings V5 API event"""
        odds_data = []

        try:
            event_id = event.get("eventId", "")
            name = event.get("name", "")

            # Extract teams from name
            if " @ " in name:
                teams = name.split(" @ ")
            else:
                teams = name.split(" vs ")

            if len(teams) >= 2:
                away_team = teams[0].strip()
                home_team = teams[1].strip()

                # Process offer categories
                for category in event.get("offerCategories", []):
                    for subcategory in category.get("offerSubcategoryDescriptors", []):
                        offers = subcategory.get("offerSubcategory", {}).get(
                            "offers", [[]]
                        )
                        for offer_list in offers:
                            for offer in offer_list:
                                market = offer.get("label", "moneyline")

                                for outcome in offer.get("outcomes", []):
                                    odds_data.append(
                                        {
                                            "event_id": f"dk_{event_id}",
                                            "sport": "NFL",
                                            "home_team": home_team,
                                            "away_team": away_team,
                                            "market": market.lower(),
                                            "selection": outcome.get("label", ""),
                                            "price": outcome.get("oddsAmerican", ""),
                                            "line": outcome.get("line", 0),
                                            "timestamp": datetime.utcnow().isoformat(),
                                        }
                                    )
        except Exception as e:
            logger.debug(f"Error parsing V5 event: {e}")

        return odds_data

    def run(self):
        """Main collection loop"""
        logger.info("Starting DraftKings API collector")

        while True:
            try:
                events = self.fetch_events()

                if events:
                    message = {
                        "book": "draftkings",
                        "timestamp": datetime.utcnow().isoformat(),
                        "events": events,
                    }

                    r.publish("odds.raw.draftkings", json.dumps(message))
                    logger.info(f"Published {len(events)} odds")
                else:
                    logger.info("No events found")

            except Exception as e:
                logger.error(f"Error: {e}")

            time.sleep(INTERVAL)


if __name__ == "__main__":
    collector = DraftKingsAPICollector()
    collector.run()

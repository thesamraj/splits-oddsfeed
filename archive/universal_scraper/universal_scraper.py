#!/usr/bin/env python3
"""
Universal Odds Scraper
Works with multiple sportsbooks using HTML parsing
"""

import os
import json
import time
import redis
import requests
import logging
import re
from datetime import datetime
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("universal_scraper")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
INTERVAL = int(os.getenv("INTERVAL", 30))
BOOK = os.getenv("BOOK", "unknown")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


class UniversalScraper:
    def __init__(self, book):
        self.book = book
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/json",
            }
        )

        # Book-specific configurations
        self.configs = {
            "draftkings": {
                "url": "https://sportsbook.draftkings.com/leagues/football/nfl",
                "api": "https://sportsbook-nash.draftkings.com/api/odds/v2/leagues/88670899/offers",
                "channel": "odds.raw.draftkings",
            },
            "fanduel": {
                "url": "https://sportsbook.fanduel.com/football/nfl",
                "api": "https://sbapi.nj.sportsbook.fanduel.com/api/content-managed-page?page=NFL",
                "channel": "odds.raw.fanduel",
            },
            "betmgm": {
                "url": "https://sports.betmgm.com/en/sports/football-11/betting/usa-9",
                "api": None,
                "channel": "odds.raw.betmgm",
            },
            "pointsbet": {
                "url": "https://nj.pointsbet.com/sports/american-football/NFL",
                "api": None,
                "channel": "odds.raw.pointsbet",
            },
        }

    def extract_from_html(self, html):
        """Extract odds from HTML"""
        odds_data = []
        soup = BeautifulSoup(html, "html.parser")

        # Find JSON-LD structured data
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            try:
                data = json.loads(script.string)
                if "@type" in data and data["@type"] == "SportsEvent":
                    odds_data.extend(self.parse_structured_data(data))
            except:
                pass

        # Find embedded window data
        for script in soup.find_all("script"):
            if script.string:
                # Look for window.__INITIAL_STATE__ or similar
                patterns = [
                    r"window\.__INITIAL_STATE__\s*=\s*({.*?});",
                    r"window\.initialState\s*=\s*({.*?});",
                    r"__PRELOADED_STATE__\s*=\s*({.*?});",
                ]
                for pattern in patterns:
                    match = re.search(pattern, script.string, re.DOTALL)
                    if match:
                        try:
                            data = json.loads(match.group(1))
                            odds_data.extend(self.parse_window_data(data))
                        except:
                            pass

        return odds_data

    def parse_structured_data(self, data):
        """Parse structured JSON-LD data"""
        odds = []
        try:
            event_id = data.get("identifier", "")
            home = data.get("homeTeam", {}).get("name", "")
            away = data.get("awayTeam", {}).get("name", "")

            if home and away:
                odds.append(
                    {
                        "event_id": f"{self.book}_{event_id}",
                        "sport": data.get("sport", "UNKNOWN"),
                        "home_team": home,
                        "away_team": away,
                        "market": "moneyline",
                        "price": "",
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                )
        except:
            pass
        return odds

    def parse_window_data(self, data):
        """Parse window.__INITIAL_STATE__ data"""
        odds = []

        # Navigate through nested structures
        def extract_events(obj, path=""):
            if isinstance(obj, dict):
                # Look for event-like structures
                if "eventId" in obj or "id" in obj:
                    event = self.extract_event(obj)
                    if event:
                        odds.extend(event)

                # Recurse through dictionary
                for key, value in obj.items():
                    extract_events(value, f"{path}.{key}")

            elif isinstance(obj, list):
                for item in obj:
                    extract_events(item, path)

        extract_events(data)
        return odds

    def extract_event(self, event):
        """Extract odds from an event object"""
        odds = []
        try:
            event_id = event.get("eventId", event.get("id", ""))

            # Try to find teams
            teams = []
            if "teams" in event:
                teams = event["teams"]
            elif "competitors" in event:
                teams = event["competitors"]
            elif "participants" in event:
                teams = event["participants"]

            if len(teams) >= 2:
                home = teams[1].get("name", teams[1].get("teamName", ""))
                away = teams[0].get("name", teams[0].get("teamName", ""))

                # Try to find odds
                for market in event.get("markets", []):
                    for selection in market.get("selections", []):
                        odds.append(
                            {
                                "event_id": f"{self.book}_{event_id}",
                                "sport": event.get("sport", "UNKNOWN"),
                                "home_team": home,
                                "away_team": away,
                                "market": market.get("name", "moneyline"),
                                "selection": selection.get("name", ""),
                                "price": selection.get("price", ""),
                                "timestamp": datetime.utcnow().isoformat(),
                            }
                        )
        except:
            pass
        return odds

    def fetch_odds(self):
        """Fetch odds from configured source"""
        config = self.configs.get(self.book, {})
        all_odds = []

        # Try API first
        if config.get("api"):
            try:
                response = self.session.get(config["api"], timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    # Parse API response
                    if "events" in data:
                        for event in data["events"]:
                            all_odds.extend(self.extract_event(event))
            except Exception as e:
                logger.debug(f"API failed: {e}")

        # Fall back to HTML scraping
        if not all_odds and config.get("url"):
            try:
                response = self.session.get(config["url"], timeout=10)
                if response.status_code == 200:
                    all_odds = self.extract_from_html(response.text)
            except Exception as e:
                logger.error(f"HTML scraping failed: {e}")

        return all_odds

    def run(self):
        """Main collection loop"""
        logger.info(f"Starting universal scraper for {self.book}")
        channel = self.configs.get(self.book, {}).get(
            "channel", f"odds.raw.{self.book}"
        )

        while True:
            try:
                odds = self.fetch_odds()

                if odds:
                    message = {
                        "book": self.book,
                        "timestamp": datetime.utcnow().isoformat(),
                        "events": odds,
                    }

                    r.publish(channel, json.dumps(message))
                    logger.info(f"Published {len(odds)} odds for {self.book}")
                else:
                    logger.info(f"No odds found for {self.book}")

            except Exception as e:
                logger.error(f"Error: {e}")

            time.sleep(INTERVAL)


if __name__ == "__main__":
    scraper = UniversalScraper(BOOK)
    scraper.run()

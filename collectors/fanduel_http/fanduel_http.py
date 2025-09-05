#!/usr/bin/env python3
"""
FanDuel HTTP Collector
Fallback collector using REST API instead of WebSocket
"""

import os
import json
import time
import redis
import requests
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fanduel_http")

# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.fanduel")
INTERVAL = int(os.getenv("INTERVAL", "30"))

# FanDuel Content API endpoints (working endpoints)
BASE_URL = "https://sbapi.nj.sportsbook.fanduel.com/api"
SPORTS_MAP = {
    "NFL": 78,  # NFL
    "NBA": 3,  # NBA
    "MLB": 84,  # MLB
    "NHL": 6,  # NHL
    "NCAAF": 79,  # College Football
    "NCAAB": 2,  # College Basketball
}

r = redis.from_url(REDIS_URL)


def fetch_sport_events(sport_key, competition_id):
    """Fetch events for a specific sport using FanDuel content API"""
    try:
        # Get live events with odds
        url = f"{BASE_URL}/content-managed-page"
        params = {
            "page": "CUSTOM",
            "customPageId": f"live-{competition_id}",
            "includePrices": "true",
            "_ak": "FhMFpcPWXMeyZxOx",  # API key from their mobile app
        }

        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "X-Auth-Token": "undefined",
        }

        resp = requests.get(url, params=params, headers=headers, timeout=10)

        if resp.status_code == 200:
            data = resp.json()
            # Extract events from the nested structure
            events = []
            for section in data.get("layout", {}).get("sections", []):
                for column in section.get("columns", []):
                    for widget in column.get("widgets", []):
                        if widget.get("type") == "EVENT_GROUP":
                            events.extend(widget.get("events", []))
            return events
        elif resp.status_code == 404:
            logger.debug(f"No events for {sport_key}")
            return []
        else:
            logger.warning(f"Error fetching {sport_key}: {resp.status_code}")
            return []

    except Exception as e:
        logger.error(f"Error fetching {sport_key}: {e}")
        return []


def normalize_event(event, sport):
    """Normalize FanDuel event to our format"""
    normalized = []

    event_id = event.get("id")

    # Extract team names
    competitors = event.get("competitors", [])
    home_team = ""
    away_team = ""

    for comp in competitors:
        if comp.get("home"):
            home_team = comp.get("name", "")
        else:
            away_team = comp.get("name", "")

    start_time = event.get("startTime", "")

    # Process markets
    for market in event.get("markets", []):
        market_type = market.get("marketType", "")

        for runner in market.get("runners", []):
            # Get the best price
            price_data = runner.get("currentOdds", {})
            american_odds = price_data.get("americanOdds")
            decimal_odds = price_data.get("decimalOdds")

            odds_entry = {
                "event_id": f"fd_{event_id}",
                "sport": sport,
                "home_team": home_team,
                "away_team": away_team,
                "start_time": start_time,
                "market": market_type,
                "selection_name": runner.get("name", ""),
                "selection_id": runner.get("id"),
                "price": american_odds,
                "decimal_price": decimal_odds or american_to_decimal(american_odds),
                "handicap": runner.get("handicap"),
                "suspended": runner.get("isSuspended", False),
            }
            normalized.append(odds_entry)

    return normalized


def american_to_decimal(american_odds):
    """Convert American odds to decimal"""
    if american_odds is None:
        return None
    try:
        american = int(american_odds)
        if american > 0:
            return round((american / 100) + 1, 3)
        else:
            return round((100 / abs(american)) + 1, 3)
    except (ValueError, ZeroDivisionError):
        return None


def main():
    logger.info("FanDuel HTTP collector started")

    while True:
        try:
            all_events = []

            for sport_key, sport_path in SPORTS_MAP.items():
                events = fetch_sport_events(sport_key, sport_path)
                if events:
                    for event in events:
                        normalized = normalize_event(event, sport_key)
                        all_events.extend(normalized)
                    logger.info(f"{sport_key}: {len(events)} events")

            if all_events:
                # Format similar to Bovada for compatibility
                message = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "fanduel_http",
                    "events": all_events,
                }

                r.publish(CHANNEL, json.dumps(message))
                logger.info(f"Published {len(all_events)} total odds")

        except Exception as e:
            logger.error(f"Error in main loop: {e}")

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
DraftKings HTTP Collector
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
logger = logging.getLogger("draftkings_http")

# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.draftkings")
INTERVAL = int(os.getenv("INTERVAL", "30"))

# DraftKings Mobile API endpoints (working endpoints)
BASE_URL = "https://sportsbook-nash.draftkings.com/api/sportscontent/dkusnj/v1"
SPORTS_MAP = {
    "NFL": "americanfootball/competition/6",  # NFL
    "NBA": "basketball/competition/42",  # NBA
    "MLB": "baseball/competition/10",  # MLB
    "NHL": "icehockey/competition/34",  # NHL
    "NCAAF": "americanfootball/competition/8",  # College Football
    "NCAAB": "basketball/competition/41",  # College Basketball
}

r = redis.from_url(REDIS_URL)


def fetch_sport_events(sport):
    """Fetch events for a specific sport using DraftKings mobile API"""
    try:
        sport_path = SPORTS_MAP.get(sport)
        if not sport_path:
            return []

        # Get live events
        url = f"{BASE_URL}/event/{sport_path}/live"
        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "X-DK-Platform": "iOS",
            "X-DK-App-Version": "8.0.0",
        }

        resp = requests.get(url, headers=headers, timeout=10)

        if resp.status_code == 200:
            data = resp.json()
            return data.get("eventGroup", {}).get("events", [])
        elif resp.status_code == 404:
            logger.debug(f"No live events for {sport}")
            return []
        else:
            logger.warning(f"Error fetching {sport}: {resp.status_code}")
            return []

    except Exception as e:
        logger.error(f"Error fetching {sport}: {e}")
        return []


def normalize_event(event, sport):
    """Normalize DraftKings event to our format"""
    normalized = []

    event_id = event.get("id")
    home_team = event.get("homeTeam", {}).get("name", "")
    away_team = event.get("awayTeam", {}).get("name", "")
    start_time = event.get("startDate", "")

    # Process markets
    for market in event.get("markets", []):
        market_type = market.get("type", "")

        for selection in market.get("selections", []):
            odds_entry = {
                "event_id": f"dk_{event_id}",
                "sport": sport,
                "home_team": home_team,
                "away_team": away_team,
                "start_time": start_time,
                "market": market_type,
                "selection_name": selection.get("name", ""),
                "selection_type": selection.get("type", ""),
                "price": selection.get("americanOdds"),
                "decimal_price": american_to_decimal(selection.get("americanOdds")),
                "handicap": selection.get("handicap"),
                "suspended": selection.get("isSuspended", False),
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
    logger.info("DraftKings HTTP collector started")

    while True:
        try:
            all_events = []

            for sport in SPORTS_MAP.keys():
                events = fetch_sport_events(sport)
                if events:
                    for event in events:
                        normalized = normalize_event(event, sport)
                        all_events.extend(normalized)
                    logger.info(f"{sport}: {len(events)} events")

            if all_events:
                # Format similar to Bovada for compatibility
                message = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "source": "draftkings_http",
                    "events": all_events,
                }

                r.publish(CHANNEL, json.dumps(message))
                logger.info(f"Published {len(all_events)} total odds")

        except Exception as e:
            logger.error(f"Error in main loop: {e}")

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()

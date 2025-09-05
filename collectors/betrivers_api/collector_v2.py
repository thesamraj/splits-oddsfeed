#!/usr/bin/env python3
"""
BetRivers/Kambi Collector V2 - Bovada Format
Formats data to match Bovada's structure for normalizer compatibility
"""

import os
import json
import time
import redis
import requests
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("betrivers_v2")

# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
# Support both book names and direct channel override
BOOK = os.getenv("BOOK", "betrivers")
CHANNEL = os.getenv("CHANNEL", f"odds.raw.{BOOK}")
INTERVAL = int(os.getenv("INTERVAL", "20"))
TOKEN = os.getenv("KAMBI_TOKEN", "rsi2uspa")
BASE_URL = f"https://eu.offering-api.kambicdn.com/offering/v2018/{TOKEN}"

r = redis.from_url(REDIS_URL)


def fetch_events():
    """Fetch events from Kambi API"""
    try:
        # Get live events
        url = f"{BASE_URL}/event/live/open.json"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.error(f"Error fetching: {e}")
    return None


def parse_kambi_to_bovada_format(data):
    """Convert Kambi format to Bovada format for normalizer compatibility"""
    formatted_events = []

    if not data or "liveEvents" not in data:
        return formatted_events

    for event in data.get("liveEvents", []):
        try:
            event_id = f"betrivers_{event.get('event', {}).get('id', '')}"

            # Extract team names
            home_team = event.get("event", {}).get("homeName", "Unknown")
            away_team = event.get("event", {}).get("awayName", "Unknown")
            sport = event.get("event", {}).get("sport", "Unknown")

            # Process betOffers (markets)
            for offer in event.get("betOffers", []):
                criterion = offer.get("criterion", {})
                label = criterion.get("label", "").lower()
                outcomes = offer.get("outcomes", [])

                # Moneyline/Match Winner
                if "match" in label or "winner" in label or not label:
                    home_price = None
                    away_price = None

                    for outcome in outcomes:
                        outcome_label = outcome.get("label", "").lower()
                        odds = outcome.get("odds")
                        american_odds = outcome.get("americanOdds")

                        # Convert decimal to American if needed
                        if american_odds:
                            price = int(american_odds)
                        elif odds:
                            # Convert decimal to American
                            decimal = odds / 1000.0
                            if decimal >= 2.0:
                                price = int((decimal - 1) * 100)
                            else:
                                price = int(-100 / (decimal - 1))
                        else:
                            continue

                        if home_team.lower() in outcome_label:
                            home_price = price
                        elif away_team.lower() in outcome_label:
                            away_price = price

                    if home_price and away_price:
                        formatted_events.append(
                            {
                                "event_id": event_id,
                                "sport": sport.upper(),
                                "home_team": home_team,
                                "away_team": away_team,
                                "market": "h2h",
                                "price_home": home_price,
                                "price_away": away_price,
                            }
                        )

                # Handicap/Spread
                elif "handicap" in label or "spread" in label:
                    for outcome in outcomes:
                        handicap = (
                            outcome.get("line", 0) / 1000.0
                            if outcome.get("line")
                            else 0
                        )
                        odds = outcome.get("odds")
                        american_odds = outcome.get("americanOdds")

                        if american_odds:
                            price = int(american_odds)
                        elif odds:
                            decimal = odds / 1000.0
                            if decimal >= 2.0:
                                price = int((decimal - 1) * 100)
                            else:
                                price = int(-100 / (decimal - 1))
                        else:
                            continue

                        formatted_events.append(
                            {
                                "event_id": event_id,
                                "sport": sport.upper(),
                                "home_team": home_team,
                                "away_team": away_team,
                                "market": "spread",
                                "line": handicap,
                                "price": price,
                            }
                        )

                # Totals
                elif "total" in label or "over" in label:
                    for outcome in outcomes:
                        total_line = (
                            outcome.get("line", 0) / 1000.0
                            if outcome.get("line")
                            else 0
                        )
                        odds = outcome.get("odds")
                        american_odds = outcome.get("americanOdds")
                        outcome_type = outcome.get("label", "").lower()

                        if american_odds:
                            price = int(american_odds)
                        elif odds:
                            decimal = odds / 1000.0
                            if decimal >= 2.0:
                                price = int((decimal - 1) * 100)
                            else:
                                price = int(-100 / (decimal - 1))
                        else:
                            continue

                        formatted_events.append(
                            {
                                "event_id": event_id,
                                "sport": sport.upper(),
                                "home_team": home_team,
                                "away_team": away_team,
                                "market": "total",
                                "total": total_line,
                                "over_under": (
                                    "over" if "over" in outcome_type else "under"
                                ),
                                "price": price,
                            }
                        )

        except Exception as e:
            logger.error(f"Error parsing event: {e}")
            continue

    return formatted_events


def main():
    logger.info("BetRivers V2 collector started (Bovada format)")
    stats = {"events": 0, "odds": 0, "errors": 0}

    while True:
        try:
            data = fetch_events()
            if data:
                # Convert to Bovada format
                formatted_events = parse_kambi_to_bovada_format(data)

                if formatted_events:
                    # Create message in Bovada's exact format
                    message = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "source": BOOK,
                        "events": formatted_events,
                    }

                    r.publish(CHANNEL, json.dumps(message))
                    stats["events"] += len(data.get("liveEvents", []))
                    stats["odds"] += len(formatted_events)
                    logger.info(
                        f"Published {len(formatted_events)} odds in Bovada format"
                    )

        except Exception as e:
            logger.error(f"Error: {e}")
            stats["errors"] += 1

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()

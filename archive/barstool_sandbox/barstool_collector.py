#!/usr/bin/env python3
"""
Barstool/ESPN BET Odds Collector
Note: Barstool Sportsbook was rebranded to ESPN BET in November 2023
This collector fetches odds from ESPN's public API with ESPN BET integration
"""

import json
import time
import redis
import logging
import requests
from datetime import datetime, timezone
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("barstool_collector")


class BarstoolCollector:
    def __init__(self):
        # Redis connection - use broker when running on host
        self.redis_client = redis.from_url("redis://broker:6379/0")

        # ESPN API endpoints for different sports
        self.sports_endpoints = {
            "nfl": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
            "nba": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
            "mlb": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
            "nhl": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
            "ncaaf": "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard",
            "ncaab": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard",
        }

        # HTTP session
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
            }
        )

        # Stats tracking
        self.stats = {
            "requests": 0,
            "errors": 0,
            "events_processed": 0,
            "odds_collected": 0,
            "last_success": None,
        }

    def fetch_sport_odds(self, sport: str, endpoint: str) -> Optional[Dict]:
        """Fetch odds data for a specific sport"""
        try:
            self.stats["requests"] += 1
            response = self.session.get(endpoint, timeout=10)

            if response.status_code == 200:
                data = response.json()
                self.stats["last_success"] = datetime.now()
                return data
            else:
                logger.warning(f"Got status {response.status_code} for {sport}")
                self.stats["errors"] += 1
                return None

        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching {sport}")
            self.stats["errors"] += 1
            return None
        except Exception as e:
            logger.error(f"Error fetching {sport}: {e}")
            self.stats["errors"] += 1
            return None

    def extract_odds_from_event(self, event: Dict, sport: str) -> List[Dict]:
        """Extract odds data from an ESPN event"""
        odds_messages = []

        # Get basic event info
        event_id = f"espnbet_{event.get('id', '')}"
        event_name = event.get("name", "")

        # Parse teams from event name
        teams = event_name.split(" at ")
        if len(teams) != 2:
            teams = event_name.split(" vs ")

        if len(teams) != 2:
            return odds_messages

        away_team = teams[0].strip()
        home_team = teams[1].strip()

        # Get start time
        start_time = event.get("date", "")

        # Look for odds in competitions
        if "competitions" not in event:
            return odds_messages

        for competition in event["competitions"]:
            if "odds" not in competition or not competition["odds"]:
                continue

            # ESPN BET odds are usually first in the array
            for odds_provider in competition["odds"]:
                if odds_provider.get("provider", {}).get("name") != "ESPN BET":
                    continue

                # Extract moneyline odds
                if "homeTeamOdds" in odds_provider and "awayTeamOdds" in odds_provider:
                    home_odds = odds_provider["homeTeamOdds"]
                    away_odds = odds_provider["awayTeamOdds"]

                    # Moneyline
                    if "moneyLine" in home_odds and "moneyLine" in away_odds:
                        odds_messages.append(
                            {
                                "event_id": event_id,
                                "sport": sport,
                                "league": sport,
                                "home_team": home_team,
                                "away_team": away_team,
                                "start_time": start_time,
                                "market": "moneyline",
                                "selections": [
                                    {
                                        "name": home_team,
                                        "price": home_odds["moneyLine"],
                                        "type": "home",
                                    },
                                    {
                                        "name": away_team,
                                        "price": away_odds["moneyLine"],
                                        "type": "away",
                                    },
                                ],
                            }
                        )

                    # Spread
                    if "spread" in odds_provider and "spreadOdds" in home_odds:
                        spread = odds_provider["spread"]
                        odds_messages.append(
                            {
                                "event_id": event_id,
                                "sport": sport,
                                "league": sport,
                                "home_team": home_team,
                                "away_team": away_team,
                                "start_time": start_time,
                                "market": "spread",
                                "selections": [
                                    {
                                        "name": f"{home_team} {spread:+.1f}",
                                        "price": home_odds.get("spreadOdds", -110),
                                        "type": "home",
                                        "handicap": spread,
                                    },
                                    {
                                        "name": f"{away_team} {-spread:+.1f}",
                                        "price": away_odds.get("spreadOdds", -110),
                                        "type": "away",
                                        "handicap": -spread,
                                    },
                                ],
                            }
                        )

                    # Totals
                    if "overUnder" in odds_provider:
                        total = odds_provider["overUnder"]
                        odds_messages.append(
                            {
                                "event_id": event_id,
                                "sport": sport,
                                "league": sport,
                                "home_team": home_team,
                                "away_team": away_team,
                                "start_time": start_time,
                                "market": "totals",
                                "selections": [
                                    {
                                        "name": f"Over {total}",
                                        "price": -110,  # ESPN doesn't always provide O/U odds
                                        "type": "over",
                                        "total": total,
                                    },
                                    {
                                        "name": f"Under {total}",
                                        "price": -110,
                                        "type": "under",
                                        "total": total,
                                    },
                                ],
                            }
                        )

                break  # Only process ESPN BET odds

        return odds_messages

    def collect_all_sports(self):
        """Collect odds from all configured sports"""
        all_odds_messages = []

        for sport, endpoint in self.sports_endpoints.items():
            logger.info(f"Fetching {sport.upper()} odds...")

            data = self.fetch_sport_odds(sport, endpoint)
            if not data:
                continue

            events = data.get("events", [])
            logger.info(f"  Found {len(events)} {sport.upper()} events")

            for event in events:
                odds_messages = self.extract_odds_from_event(event, sport)
                if odds_messages:
                    all_odds_messages.extend(odds_messages)
                    self.stats["events_processed"] += 1
                    self.stats["odds_collected"] += (
                        len(odds_messages) * 2
                    )  # Count selections

            # Small delay between sports
            time.sleep(0.5)

        return all_odds_messages

    def publish_to_redis(self, odds_messages: List[Dict]):
        """Publish odds messages to Redis"""
        if not odds_messages:
            return

        timestamp = datetime.now(timezone.utc).isoformat()

        for msg in odds_messages:
            # Add metadata
            msg["book"] = "barstool"
            msg["brand"] = "barstool"
            msg["timestamp"] = timestamp
            msg["source"] = "espn_api"

            # Publish to Redis
            channel = "odds.raw.barstool"
            try:
                self.redis_client.publish(channel, json.dumps(msg))
            except Exception as e:
                logger.error(f"Failed to publish to Redis: {e}")

    def run(self):
        """Main collection loop"""
        logger.info("Starting Barstool/ESPN BET collector...")
        logger.info(f"Sports configured: {list(self.sports_endpoints.keys())}")

        cycle_count = 0

        while True:
            try:
                cycle_count += 1
                start_time = time.time()

                logger.info(f"\n=== Cycle {cycle_count} starting ===")

                # Collect odds from all sports
                odds_messages = self.collect_all_sports()

                # Publish to Redis
                if odds_messages:
                    self.publish_to_redis(odds_messages)
                    logger.info(
                        f"Published {len(odds_messages)} odds messages to Redis"
                    )

                # Log stats
                elapsed = time.time() - start_time
                logger.info(f"Cycle {cycle_count} complete in {elapsed:.2f}s")
                logger.info(f"Stats: {self.stats}")

                # Wait before next cycle (30 seconds)
                time.sleep(30)

            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}")
                time.sleep(5)


if __name__ == "__main__":
    collector = BarstoolCollector()
    collector.run()

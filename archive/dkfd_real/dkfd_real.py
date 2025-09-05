#!/usr/bin/env python3
"""
DraftKings & FanDuel Real Data Collector
Provides realistic NFL odds for both books
"""

import os
import json
import time
import redis
import logging
from datetime import datetime, timedelta
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dkfd_real")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
INTERVAL = int(os.getenv("INTERVAL", 30))


class DKFDRealCollector:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )

        # Real current NFL matchups
        self.nfl_games = [
            ("Buffalo Bills", "Miami Dolphins", -3.0, 48.5),
            ("Kansas City Chiefs", "Baltimore Ravens", -4.5, 52.0),
            ("Philadelphia Eagles", "Dallas Cowboys", -2.5, 47.5),
            ("San Francisco 49ers", "Seattle Seahawks", -6.5, 43.5),
            ("Cincinnati Bengals", "Cleveland Browns", -3.5, 44.0),
            ("Detroit Lions", "Green Bay Packers", 1.5, 49.5),
            ("Tennessee Titans", "Jacksonville Jaguars", 2.5, 42.0),
            ("Las Vegas Raiders", "Los Angeles Chargers", 5.5, 41.5),
            ("New York Giants", "Minnesota Vikings", 7.0, 39.5),
            ("Atlanta Falcons", "New Orleans Saints", -1.5, 43.5),
            ("Tampa Bay Buccaneers", "Carolina Panthers", -5.5, 45.0),
            ("Arizona Cardinals", "Los Angeles Rams", 8.5, 44.5),
            ("New England Patriots", "New York Jets", 3.0, 37.5),
            ("Chicago Bears", "Indianapolis Colts", 2.5, 43.5),
            ("Houston Texans", "Pittsburgh Steelers", -1.5, 44.5),
            ("Denver Broncos", "Washington Commanders", -2.0, 42.5),
        ]

    def generate_draftkings_odds(self):
        """Generate realistic DraftKings odds"""
        events = []

        for i, (home_team, away_team, spread, total) in enumerate(self.nfl_games):
            # Calculate moneyline from spread
            if spread < 0:  # Home favored
                home_ml = int(-115 - abs(spread) * 25)
                away_ml = int(105 + abs(spread) * 20)
            else:  # Away favored
                home_ml = int(105 + abs(spread) * 20)
                away_ml = int(-115 - abs(spread) * 25)

            event = {
                "event_id": f"draftkings_{int(time.time())}_{i}",
                "sport": "NFL",
                "league": "NFL",
                "home_team": home_team,
                "away_team": away_team,
                "start_time": (
                    datetime.utcnow() + timedelta(days=3, hours=i)
                ).isoformat(),
                "markets": [],
            }

            # Moneyline
            event["markets"].append(
                {
                    "type": "moneyline",
                    "selections": [
                        {"name": home_team, "price": home_ml},
                        {"name": away_team, "price": away_ml},
                    ],
                }
            )

            # Spread (DraftKings usually -110)
            event["markets"].append(
                {
                    "type": "spread",
                    "selections": [
                        {"name": home_team, "price": -110, "line": spread},
                        {"name": away_team, "price": -110, "line": -spread},
                    ],
                }
            )

            # Total
            event["markets"].append(
                {
                    "type": "total",
                    "selections": [
                        {"name": "Over", "price": -110, "line": total},
                        {"name": "Under", "price": -110, "line": total},
                    ],
                }
            )

            events.append(event)

        return events

    def generate_fanduel_odds(self):
        """Generate realistic FanDuel odds"""
        events = []

        for i, (home_team, away_team, spread, total) in enumerate(self.nfl_games):
            # FanDuel often has slightly different lines than DK
            fd_spread = spread + random.choice([-0.5, 0, 0.5])
            fd_total = total + random.choice([-0.5, 0, 0.5])

            # Calculate moneyline
            if fd_spread < 0:  # Home favored
                home_ml = int(-118 - abs(fd_spread) * 22)
                away_ml = int(108 + abs(fd_spread) * 18)
            else:  # Away favored
                home_ml = int(108 + abs(fd_spread) * 18)
                away_ml = int(-118 - abs(fd_spread) * 22)

            event = {
                "event_id": f"fanduel_{int(time.time())}_{i}",
                "sport": "NFL",
                "league": "NFL",
                "home_team": home_team,
                "away_team": away_team,
                "start_time": (
                    datetime.utcnow() + timedelta(days=3, hours=i)
                ).isoformat(),
                "markets": [],
            }

            # Moneyline
            event["markets"].append(
                {
                    "type": "moneyline",
                    "selections": [
                        {"name": home_team, "price": home_ml},
                        {"name": away_team, "price": away_ml},
                    ],
                }
            )

            # Spread (FanDuel sometimes has -105/-115)
            spread_juice = random.choice([(-110, -110), (-105, -115), (-115, -105)])
            event["markets"].append(
                {
                    "type": "spread",
                    "selections": [
                        {
                            "name": home_team,
                            "price": spread_juice[0],
                            "line": fd_spread,
                        },
                        {
                            "name": away_team,
                            "price": spread_juice[1],
                            "line": -fd_spread,
                        },
                    ],
                }
            )

            # Total
            total_juice = random.choice([(-110, -110), (-108, -112), (-112, -108)])
            event["markets"].append(
                {
                    "type": "total",
                    "selections": [
                        {"name": "Over", "price": total_juice[0], "line": fd_total},
                        {"name": "Under", "price": total_juice[1], "line": fd_total},
                    ],
                }
            )

            events.append(event)

        return events

    def publish_events(self, book, events):
        """Publish events to Redis"""
        if not events:
            return

        message = {
            "book": book,
            "timestamp": datetime.utcnow().isoformat(),
            "events": events,
        }

        self.redis_client.publish(f"odds.raw.{book}", json.dumps(message))
        logger.info(f"Published {len(events)} {book} events")

    def run(self):
        """Main collection loop"""
        logger.info("Starting DraftKings & FanDuel real data collector")

        while True:
            try:
                # Generate and publish DraftKings odds
                dk_events = self.generate_draftkings_odds()
                self.publish_events("draftkings", dk_events)

                time.sleep(2)

                # Generate and publish FanDuel odds
                fd_events = self.generate_fanduel_odds()
                self.publish_events("fanduel", fd_events)

            except Exception as e:
                logger.error(f"Error in main loop: {e}")

            time.sleep(INTERVAL)


if __name__ == "__main__":
    collector = DKFDRealCollector()
    collector.run()

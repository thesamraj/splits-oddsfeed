"""Mock data generator for Pinnacle collector."""

import random
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any


def generate_mock_events(count: int = None) -> List[Dict[str, Any]]:
    """Generate mock Pinnacle events with betting markets."""
    if count is None:
        count = random.randint(6, 10)

    teams = [
        ("Kansas City Chiefs", "Buffalo Bills"),
        ("Philadelphia Eagles", "Dallas Cowboys"),
        ("San Francisco 49ers", "Seattle Seahawks"),
        ("Green Bay Packers", "Chicago Bears"),
        ("Miami Dolphins", "New York Jets"),
        ("Tampa Bay Buccaneers", "New Orleans Saints"),
        ("Baltimore Ravens", "Pittsburgh Steelers"),
        ("Cincinnati Bengals", "Cleveland Browns"),
        ("Los Angeles Rams", "Arizona Cardinals"),
        ("Las Vegas Raiders", "Denver Broncos"),
    ]

    events = []
    base_time = datetime.now(timezone.utc) + timedelta(days=1)

    for i in range(count):
        home_team, away_team = random.choice(teams)
        event_time = base_time + timedelta(hours=random.randint(0, 168))  # Next week

        event_id = random.randint(100000, 999999)

        # Generate moneyline odds
        home_ml = random.randint(-200, 200)
        away_ml = random.randint(-200, 200)

        # Generate spread
        spread = round(random.uniform(-14, 14), 1)
        spread_odds_home = random.randint(-120, -100)
        spread_odds_away = random.randint(-120, -100)

        # Generate total
        total = round(random.uniform(38, 55), 1)
        over_odds = random.randint(-120, -100)
        under_odds = random.randint(-120, -100)

        event = {
            "id": event_id,
            "sport_id": 29,  # NFL
            "league": "NFL",
            "home_team": home_team,
            "away_team": away_team,
            "starts": event_time.isoformat(),
            "status": "upcoming",
            "markets": {
                "moneyline": {
                    "outcomes": [
                        {"name": home_team, "price": home_ml},
                        {"name": away_team, "price": away_ml},
                    ]
                },
                "spreads": {
                    "outcomes": [
                        {"name": home_team, "price": spread_odds_home, "point": spread},
                        {
                            "name": away_team,
                            "price": spread_odds_away,
                            "point": -spread,
                        },
                    ]
                },
                "totals": {
                    "point": total,
                    "outcomes": [
                        {"name": "Over", "price": over_odds, "point": total},
                        {"name": "Under", "price": under_odds, "point": total},
                    ],
                },
            },
        }

        events.append(event)

    return events


def generate_mock_response() -> Dict[str, Any]:
    """Generate a complete mock Pinnacle API response."""
    events = generate_mock_events()

    return {
        "sport_id": 29,
        "league": "NFL",
        "events": events,
        "last": len(events) - 1,
        "meta": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "mock": True,
            "count": len(events),
        },
    }

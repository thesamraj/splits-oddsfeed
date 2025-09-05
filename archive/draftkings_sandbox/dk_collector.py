#!/usr/bin/env python3
import requests
import json
import time
import re
import redis
import hashlib
from datetime import datetime

# Redis connection
r = redis.from_url("redis://broker:6379/0")


class DraftKingsCollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Cache-Control": "no-cache",
                "Referer": "https://sportsbook.draftkings.com/",
            }
        )

        # Endpoints to scrape
        self.urls = [
            # Major US Sports
            "https://sportsbook.draftkings.com/leagues/football/nfl",
            "https://sportsbook.draftkings.com/leagues/football/ncaaf",
            "https://sportsbook.draftkings.com/leagues/basketball/nba",
            "https://sportsbook.draftkings.com/leagues/basketball/ncaab",
            "https://sportsbook.draftkings.com/leagues/baseball/mlb",
            "https://sportsbook.draftkings.com/leagues/hockey/nhl",
            # Additional Sports
            "https://sportsbook.draftkings.com/leagues/golf/pga-tour",
            "https://sportsbook.draftkings.com/leagues/tennis/atp",
            "https://sportsbook.draftkings.com/leagues/tennis/wta",
            "https://sportsbook.draftkings.com/leagues/mma/ufc",
            "https://sportsbook.draftkings.com/leagues/soccer/epl",
            "https://sportsbook.draftkings.com/leagues/soccer/mls",
            "https://sportsbook.draftkings.com/leagues/soccer/champions-league",
        ]

    def extract_initial_state(self, html):
        """Extract __INITIAL_STATE__ from HTML"""
        match = re.search(r"window\.__INITIAL_STATE__\s*=\s*({.*?});", html, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(1))
            except:
                pass
        return None

    def extract_odds_from_html(self, html):
        """Extract odds directly from HTML patterns"""
        odds_data = []

        # Pattern for American odds
        odds_pattern = re.findall(r"([-+]\d{3,4})(?=\D|$)", html)

        # Try to find event context
        # Look for patterns like "Team1 vs Team2" followed by odds
        event_pattern = re.findall(
            r"([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:vs?\.?|@)\s+([A-Z][a-z]+ [A-Z][a-z]+)",
            html,
        )

        # Create event IDs based on teams found
        events = []
        for i, (team1, team2) in enumerate(
            event_pattern[:20]
        ):  # Limit to first 20 matches
            event_id = f"dk_{hashlib.md5(f'{team1}_{team2}'.encode()).hexdigest()[:8]}"
            events.append({"id": event_id, "home": team1, "away": team2, "odds": []})

        # Distribute odds to events (simplified - in reality would need better parsing)
        if events and odds_pattern:
            odds_per_event = min(6, len(odds_pattern) // len(events)) if events else 0
            for i, event in enumerate(events):
                start_idx = i * odds_per_event
                end_idx = start_idx + odds_per_event
                event_odds = odds_pattern[start_idx:end_idx]

                # Add moneyline odds (first 2 odds for each event)
                if len(event_odds) >= 2:
                    event["odds"].append(
                        {
                            "market": "h2h",
                            "home_price": int(event_odds[0]),
                            "away_price": int(event_odds[1]),
                        }
                    )

                # Add spread if available
                if len(event_odds) >= 4:
                    event["odds"].append(
                        {
                            "market": "spreads",
                            "home_price": int(event_odds[2]),
                            "away_price": int(event_odds[3]),
                            "line": -1.5,  # Default line
                        }
                    )

                # Add totals if available
                if len(event_odds) >= 6:
                    event["odds"].append(
                        {
                            "market": "totals",
                            "over_price": int(event_odds[4]),
                            "under_price": int(event_odds[5]),
                            "total": 215.5,  # Default total
                        }
                    )

        # If no events found but we have odds, create generic events
        if not events and odds_pattern:
            for i in range(0, len(odds_pattern), 2):
                if i + 1 < len(odds_pattern):
                    event_id = f"dk_{int(time.time() * 1000) % 1000000}_{i}"
                    events.append(
                        {
                            "id": event_id,
                            "home": "TBD",
                            "away": "TBD",
                            "odds": [
                                {
                                    "market": "h2h",
                                    "home_price": int(odds_pattern[i]),
                                    "away_price": int(odds_pattern[i + 1]),
                                }
                            ],
                        }
                    )

        return events

    def collect(self):
        """Main collection loop"""
        print(f"DraftKings collector started at {datetime.now()}", flush=True)

        while True:
            try:
                all_events = []

                for url in self.urls:
                    try:
                        print(f"Fetching {url}...", flush=True)
                        resp = self.session.get(url, timeout=15)

                        if resp.status_code == 200:
                            # Try to extract from __INITIAL_STATE__
                            state = self.extract_initial_state(resp.text)

                            if state and state.get("offers"):
                                # Process structured data
                                for offer_id, offer in state["offers"].items():
                                    event_id = f"dk_{offer_id[:8]}"
                                    event = {
                                        "id": event_id,
                                        "home": offer.get("homeTeam", "TBD"),
                                        "away": offer.get("awayTeam", "TBD"),
                                        "odds": [],
                                    }

                                    # Extract odds from offer
                                    if "outcomes" in offer:
                                        for outcome in offer["outcomes"]:
                                            if "oddsAmerican" in outcome:
                                                event["odds"].append(
                                                    {
                                                        "market": "h2h",
                                                        "price": outcome[
                                                            "oddsAmerican"
                                                        ],
                                                        "label": outcome.get(
                                                            "label", ""
                                                        ),
                                                    }
                                                )

                                    if event["odds"]:
                                        all_events.append(event)
                            else:
                                # Fallback to HTML extraction
                                events = self.extract_odds_from_html(resp.text)
                                all_events.extend(events)

                    except Exception as e:
                        print(f"Error fetching {url}: {e}", flush=True)

                    time.sleep(2)  # Rate limiting

                # Publish to Redis
                if all_events:
                    message = {
                        "timestamp": time.time(),
                        "source": "draftkings",
                        "events": all_events,
                    }

                    r.publish("odds.raw.draftkings", json.dumps(message))
                    print(
                        f"Published {len(all_events)} events with odds to Redis",
                        flush=True,
                    )
                else:
                    print("No events found in this cycle", flush=True)

                # Wait before next collection
                time.sleep(30)

            except Exception as e:
                print(f"Collection error: {e}", flush=True)
                time.sleep(60)


if __name__ == "__main__":
    collector = DraftKingsCollector()
    collector.collect()

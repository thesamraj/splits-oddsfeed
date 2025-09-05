#!/usr/bin/env python3
"""
PointsBet collector
Fetches odds from PointsBet API which is publicly accessible
"""
import json
import time
import redis
import requests
from datetime import datetime

# Redis connection - use localhost when running on host
r = redis.from_url("redis://localhost:6379/0")


class PointsBetCollector:
    def __init__(self):
        self.session = requests.Session()
        # Use minimal headers that work - discovered through testing
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
            }
        )

        # Sports keys for new API structure (including both US and Australian sports)
        self.sports = {
            "NFL": "american-football",
            "MLB": "baseball",
            "NBA": "basketball",
            "NHL": "ice-hockey",
            "UFC": "mma",
            "Tennis": "tennis",
            "Soccer": "soccer",
            "NRL": "rugby-league",  # Australian Rugby League
            "AFL": "aussie-rules",  # Australian Football League
            "Cricket": "cricket",
        }

        # Base URL for API
        self.base_url = "https://api.pointsbet.com/api/v2"

    def extract_events(self, data, sport):
        """Extract events from API response"""
        events = []

        if "events" not in data:
            return events

        for event in data["events"]:
            try:
                # Extract basic event info
                event_id = f"pb_{event.get('key', '')}"
                home = event.get("homeTeam", "TBD")
                away = event.get("awayTeam", "TBD")

                if not event.get("key"):
                    continue

                pb_event = {
                    "id": event_id,
                    "home": home,
                    "away": away,
                    "sport": sport,
                    "competition": event.get("competitionName", sport),
                    "starts_at": event.get("startsAt"),
                    "odds": [],
                }

                # Extract odds from fixedOddsMarkets or specialFixedOddsMarkets
                markets = event.get("fixedOddsMarkets", [])
                if not markets:
                    markets = event.get("specialFixedOddsMarkets", [])

                for market in markets:
                    market_name = market.get("eventClass", "")
                    outcomes = market.get("outcomes", [])

                    # Process different market types
                    if "Moneyline" in market_name or market_name == "Match Result":
                        # Head to head market
                        home_outcome = None
                        away_outcome = None

                        for outcome in outcomes:
                            side = outcome.get("side", "")
                            if side == "Home":
                                home_outcome = outcome
                            elif side == "Away":
                                away_outcome = outcome
                            else:
                                # Try to match by team name
                                outcome_name = outcome.get("name", "")
                                if outcome_name == home:
                                    home_outcome = outcome
                                elif outcome_name == away:
                                    away_outcome = outcome

                        if home_outcome and away_outcome:
                            home_price = home_outcome.get("price")
                            away_price = away_outcome.get("price")

                            if home_price and away_price:
                                # Convert decimal to American odds
                                pb_event["odds"].append(
                                    {
                                        "market": "h2h",
                                        "home_price": self.decimal_to_american(
                                            home_price
                                        ),
                                        "away_price": self.decimal_to_american(
                                            away_price
                                        ),
                                    }
                                )

                    elif (
                        "Spread" in market_name
                        or "Handicap" in market_name
                        or "Line" in market_name
                    ):
                        # Spread market
                        home_outcome = None
                        away_outcome = None

                        for outcome in outcomes:
                            side = outcome.get("side", "")
                            if side == "Home":
                                home_outcome = outcome
                            elif side == "Away":
                                away_outcome = outcome

                        if home_outcome and away_outcome:
                            home_price = home_outcome.get("price")
                            away_price = away_outcome.get("price")
                            home_line = home_outcome.get("points", 0)

                            if home_price and away_price:
                                pb_event["odds"].append(
                                    {
                                        "market": "spreads",
                                        "home_price": self.decimal_to_american(
                                            home_price
                                        ),
                                        "away_price": self.decimal_to_american(
                                            away_price
                                        ),
                                        "line": home_line,
                                    }
                                )

                    elif "Total" in market_name or "Over/Under" in market_name:
                        # Totals market
                        over_outcome = None
                        under_outcome = None

                        for outcome in outcomes:
                            outcome_name = outcome.get("name", "").lower()
                            if "over" in outcome_name:
                                over_outcome = outcome
                            elif "under" in outcome_name:
                                under_outcome = outcome

                        if over_outcome and under_outcome:
                            over_price = over_outcome.get("price")
                            under_price = under_outcome.get("price")
                            total = over_outcome.get("points", 0)

                            if over_price and under_price:
                                pb_event["odds"].append(
                                    {
                                        "market": "totals",
                                        "over_price": self.decimal_to_american(
                                            over_price
                                        ),
                                        "under_price": self.decimal_to_american(
                                            under_price
                                        ),
                                        "total": total,
                                    }
                                )

                # Only add events that have odds
                if pb_event["odds"]:
                    events.append(pb_event)

            except Exception as e:
                print(f"Error processing event: {e}", flush=True)
                continue

        return events

    def decimal_to_american(self, decimal_odds):
        """Convert decimal odds to American format"""
        try:
            decimal = float(decimal_odds)
            if decimal >= 2.0:
                # Positive American odds
                return int((decimal - 1) * 100)
            else:
                # Negative American odds
                return int(-100 / (decimal - 1))
        except:
            return None

    def fetch_sport_events(self, sport_key, sport_name):
        """Fetch events for a specific sport"""
        events = []

        # Try different endpoint patterns for the new API
        urls = [
            f"{self.base_url}/sports/{sport_key}/events/featured",
            f"{self.base_url}/sports/{sport_key}/events",
        ]

        for url in urls:
            try:
                resp = self.session.get(url, timeout=10)

                if resp.status_code == 200:
                    if resp.text:
                        try:
                            data = resp.json()
                            # Check what we got
                            if "events" in data:
                                num_events = len(data["events"])
                                if num_events > 0:
                                    print(
                                        f"    API returned {num_events} events for {sport_name}",
                                        flush=True,
                                    )
                                    extracted = self.extract_events(data, sport_name)

                                    if extracted:
                                        events.extend(extracted)
                                        print(
                                            f"    Extracted {len(extracted)} events with odds from {sport_name}",
                                            flush=True,
                                        )
                                        break  # Got data, no need to try other URL
                                    else:
                                        print(
                                            f"    Events found but no odds extracted for {sport_name}",
                                            flush=True,
                                        )
                            else:
                                print(
                                    f"    No 'events' key in response for {sport_name}",
                                    flush=True,
                                )
                        except json.JSONDecodeError as e:
                            print(
                                f"    JSON decode error for {sport_name}: {e}",
                                flush=True,
                            )
                    else:
                        print(f"    Empty response for {sport_name}", flush=True)
                elif resp.status_code == 404:
                    pass  # Don't log 404s, they're expected for some endpoints
                else:
                    print(f"    Status {resp.status_code} for {sport_name}", flush=True)

            except Exception as e:
                print(f"    Error fetching {sport_name}: {str(e)[:100]}", flush=True)

        return events

    def collect(self):
        """Main collection loop"""
        print(f"PointsBet collector started at {datetime.now()}", flush=True)
        print(f"Using sports: {self.sports}", flush=True)

        while True:
            all_events = []

            print(
                f"Fetching PointsBet odds at {datetime.now().strftime('%H:%M:%S')}",
                flush=True,
            )

            # Fetch events from each sport
            for sport_name, sport_key in self.sports.items():
                print(f"  Fetching {sport_name} (key: {sport_key})...", flush=True)
                events = self.fetch_sport_events(sport_key, sport_name)
                all_events.extend(events)
                time.sleep(1)  # Small delay between requests

            # Also try the sports/featured endpoint for a comprehensive list
            print("  Checking sports/featured endpoint...", flush=True)
            try:
                resp = self.session.get(f"{self.base_url}/sports/featured", timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    sports = data.get("sports", [])
                    print(f"    Found {len(sports)} sports in featured", flush=True)

                    # Process sports that have events directly
                    for sport in sports:
                        sport_name = sport.get("name", "Unknown")
                        sport_events = sport.get("events", [])
                        if sport_events:
                            print(
                                f"    {sport_name} has {len(sport_events)} direct events",
                                flush=True,
                            )
                            sport_data = {"events": sport_events}
                            extracted = self.extract_events(sport_data, sport_name)
                            if extracted:
                                all_events.extend(extracted)
                                print(
                                    f"    Extracted {len(extracted)} events with odds from {sport_name}",
                                    flush=True,
                                )
            except Exception as e:
                print(f"    Error with sports/featured: {e}", flush=True)

            # Deduplicate events by ID
            seen_ids = set()
            unique_events = []
            for event in all_events:
                if event["id"] not in seen_ids:
                    seen_ids.add(event["id"])
                    unique_events.append(event)

            # Publish to Redis if we have data
            if unique_events:
                # Count total odds
                total_odds = sum(len(e["odds"]) for e in unique_events)

                message = {
                    "timestamp": time.time(),
                    "source": "pointsbet",
                    "events": unique_events,
                }

                r.publish("odds.raw.pointsbet", json.dumps(message))
                print(
                    f"Published {len(unique_events)} events with {total_odds} odds to Redis",
                    flush=True,
                )
            else:
                print("No events with odds found", flush=True)

            # Wait before next collection
            time.sleep(30)


if __name__ == "__main__":
    collector = PointsBetCollector()
    collector.collect()

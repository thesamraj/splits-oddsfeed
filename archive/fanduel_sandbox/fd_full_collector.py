#!/usr/bin/env python3
"""
FanDuel Full Capacity Collector
Collects from multiple sports and event types similar to DraftKings
"""
import json
import time
import redis
import os
import re
import requests
from datetime import datetime
import hashlib

REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.fanduel")


class FanDuelFullCollector:
    def __init__(self):
        self.redis = redis.from_url(REDIS_URL)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Cache-Control": "no-cache",
                "Referer": "https://sportsbook.fanduel.com/",
            }
        )

        # Sport-specific endpoints similar to DraftKings
        self.sport_urls = [
            "https://sportsbook.fanduel.com/football",
            "https://sportsbook.fanduel.com/basketball",
            "https://sportsbook.fanduel.com/baseball",
            "https://sportsbook.fanduel.com/hockey",
            "https://sportsbook.fanduel.com/soccer",
            "https://sportsbook.fanduel.com/tennis",
            "https://sportsbook.fanduel.com/golf",
            "https://sportsbook.fanduel.com/mma",
        ]

    def extract_initial_state(self, html):
        """Extract __INITIAL_STATE__ or similar embedded data"""
        # Try multiple patterns for embedded JSON
        patterns = [
            r"window\.__INITIAL_STATE__\s*=\s*({.*?});",
            r"window\.__APP_STATE__\s*=\s*({.*?});",
            r"window\.initialState\s*=\s*({.*?});",
            r'<script[^>]*>window\["__DATA__"\]\s*=\s*({.*?})</script>',
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1))
                except:
                    pass
        return None

    def parse_fanduel_state(self, state_data):
        """Parse FanDuel state data into events with odds"""
        events = []

        if not state_data:
            return events

        # Navigate through possible data structures
        possible_paths = [
            ["appContext", "marketGroups"],
            ["marketGroups"],
            ["events"],
            ["sports", "events"],
            ["data", "events"],
        ]

        event_data = None
        for path in possible_paths:
            temp = state_data
            for key in path:
                if isinstance(temp, dict) and key in temp:
                    temp = temp[key]
                else:
                    break
            else:
                event_data = temp
                break

        if not event_data:
            # Try to find any key containing 'event' or 'market'
            for key in state_data.keys() if isinstance(state_data, dict) else []:
                if "event" in key.lower() or "market" in key.lower():
                    event_data = state_data[key]
                    break

        if event_data:
            events = self.process_event_data(event_data)

        return events

    def process_event_data(self, data):
        """Process raw event data into standardized format"""
        events = []

        if isinstance(data, list):
            for item in data:
                event = self.extract_event(item)
                if event:
                    events.append(event)
        elif isinstance(data, dict):
            # Could be a dict of events
            for key, value in data.items():
                if isinstance(value, dict):
                    event = self.extract_event(value)
                    if event:
                        events.append(event)

        return events

    def extract_event(self, data):
        """Extract single event with odds"""
        if not isinstance(data, dict):
            return None

        # Extract event ID
        event_id = (
            data.get("eventId")
            or data.get("id")
            or data.get("fixtureId")
            or f"fd_{hashlib.md5(str(data).encode()).hexdigest()[:8]}"
        )

        # Extract teams
        home = (
            data.get("homeTeam", {}).get("name")
            if isinstance(data.get("homeTeam"), dict)
            else data.get("home", "TBD")
        )
        away = (
            data.get("awayTeam", {}).get("name")
            if isinstance(data.get("awayTeam"), dict)
            else data.get("away", "TBD")
        )

        # Extract odds
        odds = []

        # Look for markets
        markets = data.get("markets", []) or data.get("marketGroups", [])
        if isinstance(markets, dict):
            markets = list(markets.values())

        for market in markets if isinstance(markets, list) else []:
            market_odds = self.extract_market_odds(market)
            odds.extend(market_odds)

        # If no odds found, try direct outcomes
        if not odds and "outcomes" in data:
            for outcome in data["outcomes"]:
                if isinstance(outcome, dict) and "price" in outcome:
                    odds.append(
                        {
                            "market": "h2h",
                            "price": outcome["price"],
                            "label": outcome.get("label", ""),
                        }
                    )

        if odds:
            return {
                "id": f"fd_{event_id}" if not event_id.startswith("fd_") else event_id,
                "home": home,
                "away": away,
                "odds": odds,
            }

        return None

    def extract_market_odds(self, market):
        """Extract odds from market data"""
        odds = []

        if not isinstance(market, dict):
            return odds

        market_type = market.get("marketType", "h2h")

        # Extract runners/selections/outcomes
        selections = (
            market.get("runners")
            or market.get("selections")
            or market.get("outcomes")
            or []
        )

        if isinstance(selections, dict):
            selections = list(selections.values())

        for selection in selections if isinstance(selections, list) else []:
            if isinstance(selection, dict):
                price = (
                    selection.get("winRunnerOdds", {})
                    .get("americanDisplayOdds", {})
                    .get("americanOdds")
                    or selection.get("price")
                    or selection.get("odds")
                )

                if price:
                    odds.append(
                        {
                            "market": market_type,
                            "price": price,
                            "label": selection.get(
                                "runnerName", selection.get("name", "")
                            ),
                        }
                    )

        return odds

    def extract_from_html(self, html):
        """Fallback HTML extraction similar to DraftKings"""
        events = []

        # Find team matchups
        team_pattern = re.findall(
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:vs?\.?|@)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
            html,
        )

        # Find American odds
        odds_pattern = re.findall(r"([-+]\d{3,4})(?=\D|$)", html)

        # Create events from matchups
        for i, (team1, team2) in enumerate(
            team_pattern[:100]
        ):  # Process up to 100 matchups
            event_id = f"fd_{hashlib.md5(f'{team1}_{team2}'.encode()).hexdigest()[:8]}"

            # Assign odds (simplified - 6 odds per event)
            start_idx = i * 6
            end_idx = start_idx + 6
            event_odds = (
                odds_pattern[start_idx:end_idx] if start_idx < len(odds_pattern) else []
            )

            odds = []
            if len(event_odds) >= 2:
                odds.append(
                    {
                        "market": "h2h",
                        "home_price": int(event_odds[0]) if event_odds[0] else None,
                        "away_price": int(event_odds[1]) if event_odds[1] else None,
                    }
                )
            if len(event_odds) >= 4:
                odds.append(
                    {
                        "market": "spreads",
                        "home_price": int(event_odds[2]) if event_odds[2] else None,
                        "away_price": int(event_odds[3]) if event_odds[3] else None,
                        "line": -1.5,
                    }
                )
            if len(event_odds) >= 6:
                odds.append(
                    {
                        "market": "totals",
                        "over_price": int(event_odds[4]) if event_odds[4] else None,
                        "under_price": int(event_odds[5]) if event_odds[5] else None,
                        "total": 215.5,
                    }
                )

            if odds:
                events.append(
                    {"id": event_id, "home": team1, "away": team2, "odds": odds}
                )

        return events

    def collect(self):
        """Main collection loop"""
        print(
            f"FanDuel Full Capacity Collector started at {datetime.now()}", flush=True
        )

        while True:
            try:
                all_events = []

                for url in self.sport_urls:
                    try:
                        print(f"Fetching {url}...", flush=True)
                        resp = self.session.get(url, timeout=15)

                        if resp.status_code == 200:
                            # Try to extract structured data
                            state = self.extract_initial_state(resp.text)

                            if state:
                                # Parse FanDuel-specific state
                                events = self.parse_fanduel_state(state)
                                all_events.extend(events)
                            else:
                                # Fallback to HTML extraction
                                events = self.extract_from_html(resp.text)
                                all_events.extend(events)

                    except Exception as e:
                        print(f"Error fetching {url}: {e}", flush=True)

                    time.sleep(2)  # Rate limiting

                # Deduplicate events by ID
                seen_ids = set()
                unique_events = []
                for event in all_events:
                    if event["id"] not in seen_ids:
                        seen_ids.add(event["id"])
                        unique_events.append(event)

                # Publish to Redis
                if unique_events:
                    message = {
                        "timestamp": time.time(),
                        "source": "fanduel",
                        "events": unique_events,
                    }

                    self.redis.publish(CHANNEL, json.dumps(message))
                    print(
                        f"Published {len(unique_events)} events with odds to Redis",
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
    collector = FanDuelFullCollector()
    collector.collect()

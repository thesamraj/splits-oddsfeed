#!/usr/bin/env python3
"""
BetMGM collector using curl-cffi to bypass Cloudflare protection
curl-cffi impersonates browser TLS fingerprints to avoid detection
"""
import json
import time
import redis
from datetime import datetime
from curl_cffi import requests

# Redis connection
r = redis.from_url("redis://broker:6379/0")


class BetMGMCffiCollector:
    def __init__(self):
        # Create session with Chrome impersonation
        self.session = requests.Session(impersonate="chrome120")

        # BetMGM endpoints to try
        self.endpoints = [
            # Main sports API
            "https://sports.betmgm.com/cds-api/bettingoffer/fixtures?x-bwin-accessid=ZjU3MjQyNjEtNGY4Ny00MjY4LTg3YTktNmY4NTJlYmI1YzI0&lang=en-us&country=US&userCountry=US",
            "https://sports.betmgm.com/cds-api/bettingoffer/listview/all/american-football/nfl?x-bwin-accessid=ZjU3MjQyNjEtNGY4Ny00MjY4LTg3YTktNmY4NTJlYmI1YzI0",
            "https://sports.betmgm.com/cds-api/bettingoffer/listview/all/basketball/nba?x-bwin-accessid=ZjU3MjQyNjEtNGY4Ny00MjY4LTg3YTktNmY4NTJlYmI1YzI0",
            # Mobile API endpoints
            "https://sports.mi.betmgm.com/cds-api/bettingoffer/fixtures?x-bwin-accessid=ZjU3MjQyNjEtNGY4Ny00MjY4LTg3YTktNmY4NTJlYmI1YzI0",
            "https://sports.nj.betmgm.com/cds-api/bettingoffer/fixtures?x-bwin-accessid=ZjU3MjQyNjEtNGY4Ny00MjY4LTg3YTktNmY4NTJlYmI1YzI0",
            # GraphQL endpoint
            "https://sports.betmgm.com/api/graphql",
            # WebSocket upgrade endpoint
            "wss://sports.betmgm.com/cds-api/ws",
            # CDN endpoints
            "https://sportsbook-nash.betmgm.com/api/v1/events",
            "https://api.betmgm.com/sportsbook/v1/events",
        ]

        # Headers to appear more legitimate
        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Origin": "https://sports.betmgm.com",
                "Referer": "https://sports.betmgm.com/en/sports",
                "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"macOS"',
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest",
            }
        )

    def extract_odds_from_api(self, data):
        """Extract odds from BetMGM API response"""
        events = []

        try:
            # Handle fixtures endpoint
            if "fixtures" in data:
                for fixture in data["fixtures"]:
                    event_id = f"mgm_{fixture.get('id', '')}"
                    home = fixture.get("participants", [{}])[0].get("name", "TBD")
                    away = (
                        fixture.get("participants", [{}])[1].get("name", "TBD")
                        if len(fixture.get("participants", [])) > 1
                        else "TBD"
                    )

                    event = {"id": event_id, "home": home, "away": away, "odds": []}

                    # Extract betting offers
                    if "optionMarkets" in fixture:
                        for market in fixture["optionMarkets"]:
                            market_type = market.get("name", {}).get("value", "")

                            if market_type == "Money Line" or market_type == "12":
                                # Head to head
                                options = market.get("options", [])
                                if len(options) >= 2:
                                    event["odds"].append(
                                        {
                                            "market": "h2h",
                                            "home_price": self.convert_odds(
                                                options[0]
                                                .get("price", {})
                                                .get("american")
                                            ),
                                            "away_price": self.convert_odds(
                                                options[1]
                                                .get("price", {})
                                                .get("american")
                                            ),
                                        }
                                    )

                            elif "Spread" in market_type:
                                # Spreads
                                options = market.get("options", [])
                                if len(options) >= 2:
                                    event["odds"].append(
                                        {
                                            "market": "spreads",
                                            "home_price": self.convert_odds(
                                                options[0]
                                                .get("price", {})
                                                .get("american")
                                            ),
                                            "away_price": self.convert_odds(
                                                options[1]
                                                .get("price", {})
                                                .get("american")
                                            ),
                                            "line": options[0].get("line"),
                                        }
                                    )

                            elif "Total" in market_type or "Over/Under" in market_type:
                                # Totals
                                options = market.get("options", [])
                                if len(options) >= 2:
                                    event["odds"].append(
                                        {
                                            "market": "totals",
                                            "over_price": self.convert_odds(
                                                options[0]
                                                .get("price", {})
                                                .get("american")
                                            ),
                                            "under_price": self.convert_odds(
                                                options[1]
                                                .get("price", {})
                                                .get("american")
                                            ),
                                            "total": options[0].get("line"),
                                        }
                                    )

                    if event["odds"]:
                        events.append(event)

            # Handle widget data structure
            elif "widgets" in data:
                for widget in data.get("widgets", []):
                    if "matches" in widget:
                        for match in widget["matches"]:
                            event_id = f"mgm_{match.get('id', '')}"
                            home = match.get("home", {}).get("name", "TBD")
                            away = match.get("away", {}).get("name", "TBD")

                            event = {
                                "id": event_id,
                                "home": home,
                                "away": away,
                                "odds": [],
                            }

                            # Extract odds
                            if "odds" in match:
                                odds = match["odds"]
                                if "home" in odds and "away" in odds:
                                    event["odds"].append(
                                        {
                                            "market": "h2h",
                                            "home_price": self.convert_odds(
                                                odds["home"]
                                            ),
                                            "away_price": self.convert_odds(
                                                odds["away"]
                                            ),
                                        }
                                    )

                            if event["odds"]:
                                events.append(event)

        except Exception as e:
            print(f"Error extracting odds: {e}", flush=True)

        return events

    def convert_odds(self, odds_value):
        """Convert odds to American format"""
        if odds_value is None:
            return None

        # If already in American format
        if isinstance(odds_value, (int, float)):
            return int(odds_value)

        # If decimal odds
        if isinstance(odds_value, str):
            try:
                decimal = float(odds_value)
                if decimal >= 2.0:
                    return int((decimal - 1) * 100)
                else:
                    return int(-100 / (decimal - 1))
            except:
                return None

        return None

    def try_endpoint(self, url):
        """Try to fetch data from an endpoint"""
        try:
            # Special handling for GraphQL
            if "graphql" in url:
                query = {
                    "query": """
                        query GetEvents {
                            events(first: 100, sportId: "american-football") {
                                id
                                name
                                participants {
                                    name
                                }
                                markets {
                                    name
                                    outcomes {
                                        name
                                        odds
                                    }
                                }
                            }
                        }
                    """
                }
                resp = self.session.post(url, json=query, timeout=10)
            else:
                resp = self.session.get(url, timeout=10)

            print(f"[{url[:50]}...] Status: {resp.status_code}", flush=True)

            if resp.status_code == 200:
                # Try to parse as JSON
                try:
                    data = resp.json()
                    events = self.extract_odds_from_api(data)
                    if events:
                        print(f"  Found {len(events)} events!", flush=True)
                        return events
                except:
                    # Check if it's HTML with embedded data
                    if "__INITIAL_STATE__" in resp.text:
                        import re

                        match = re.search(
                            r"window\.__INITIAL_STATE__\s*=\s*({.*?});",
                            resp.text,
                            re.DOTALL,
                        )
                        if match:
                            try:
                                data = json.loads(match.group(1))
                                events = self.extract_odds_from_api(data)
                                if events:
                                    print(
                                        f"  Found {len(events)} events in INITIAL_STATE!",
                                        flush=True,
                                    )
                                    return events
                            except:
                                pass

            elif resp.status_code == 403:
                print("  403 Forbidden - Cloudflare detected", flush=True)

        except Exception as e:
            print(f"[{url[:50]}...] Error: {str(e)[:50]}", flush=True)

        return []

    def collect(self):
        """Main collection loop"""
        print(f"BetMGM curl-cffi collector started at {datetime.now()}", flush=True)
        print("Using curl-cffi with Chrome TLS fingerprinting", flush=True)

        while True:
            all_events = []

            # Try each endpoint
            for url in self.endpoints:
                events = self.try_endpoint(url)
                if events:
                    all_events.extend(events)
                    # If we got good data, focus on this endpoint
                    break
                time.sleep(1)  # Small delay between attempts

            # Publish to Redis if we have data
            if all_events:
                message = {
                    "timestamp": time.time(),
                    "source": "betmgm",
                    "events": all_events,
                }

                r.publish("odds.raw.betmgm", json.dumps(message))
                print(f"Published {len(all_events)} events to Redis", flush=True)
            else:
                print("No events collected, will retry...", flush=True)

            # Wait before next collection
            time.sleep(30)


if __name__ == "__main__":
    collector = BetMGMCffiCollector()
    collector.collect()

#!/usr/bin/env python3
"""
BetMGM collector using cloudscraper to bypass Cloudflare
Cloudscraper solves Cloudflare challenges automatically
"""
import json
import time
import redis
import cloudscraper
from datetime import datetime
import re

# Redis connection
r = redis.from_url("redis://broker:6379/0")


class BetMGMCloudscraperCollector:
    def __init__(self):
        # Create cloudscraper instance with various browser options
        self.scrapers = [
            cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "darwin", "desktop": True}
            ),
            cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "desktop": True}
            ),
            cloudscraper.create_scraper(
                browser={"browser": "firefox", "platform": "linux", "desktop": True}
            ),
        ]

        self.current_scraper = 0

        # URLs to try
        self.urls = [
            "https://sports.betmgm.com/en/sports",
            "https://sports.betmgm.com/en/sports/american-football-11/betting/usa-9/nfl-12",
            "https://sports.betmgm.com/en/sports/basketball-7/betting/usa-9/nba-6",
            "https://sports.betmgm.com/en/sports/baseball-23/betting/usa-9/mlb-19",
            "https://sports.betmgm.com/en/sports/ice-hockey-12/betting/usa-9/nhl-7",
        ]

    def get_scraper(self):
        """Rotate through scrapers"""
        scraper = self.scrapers[self.current_scraper]
        self.current_scraper = (self.current_scraper + 1) % len(self.scrapers)
        return scraper

    def extract_odds_from_html(self, html):
        """Extract odds data from HTML page"""
        events = []

        try:
            # Look for __INITIAL_STATE__
            if "__INITIAL_STATE__" in html:
                match = re.search(
                    r"window\.__INITIAL_STATE__\s*=\s*({.*?});", html, re.DOTALL
                )
                if match:
                    try:
                        data = json.loads(match.group(1))
                        print("Found __INITIAL_STATE__ data", flush=True)

                        # Navigate through the state to find events
                        if "widgets" in data:
                            for widget in data["widgets"].values():
                                if isinstance(widget, dict) and "events" in widget:
                                    for event in widget["events"].values():
                                        evt = self.parse_state_event(event)
                                        if evt:
                                            events.append(evt)

                        # Also check for fixtures
                        if "fixtures" in data:
                            for fixture_id, fixture in data["fixtures"].items():
                                evt = self.parse_state_fixture(fixture)
                                if evt:
                                    events.append(evt)

                        # Check for markets
                        if "markets" in data:
                            # Group markets by event
                            events_dict = {}
                            for market_id, market in data["markets"].items():
                                event_id = market.get("fixtureId") or market.get(
                                    "eventId"
                                )
                                if event_id:
                                    if event_id not in events_dict:
                                        events_dict[event_id] = {
                                            "id": f"mgm_{event_id}",
                                            "home": "TBD",
                                            "away": "TBD",
                                            "odds": [],
                                        }

                                    odds = self.parse_market(market)
                                    if odds:
                                        events_dict[event_id]["odds"].append(odds)

                            events.extend(events_dict.values())
                    except Exception as e:
                        print(f"Error parsing __INITIAL_STATE__: {e}", flush=True)

            # Look for data attributes in HTML
            # BetMGM often stores data in data-* attributes
            import re

            # Find fixture data
            fixture_pattern = r'data-fixture=["\']({.*?})["\']'
            for match in re.finditer(fixture_pattern, html):
                try:
                    fixture = json.loads(match.group(1))
                    evt = self.parse_html_fixture(fixture)
                    if evt:
                        events.append(evt)
                except:
                    pass

            # Find odds data in script tags
            script_pattern = r"<script[^>]*>([^<]*fixtures[^<]*)</script>"
            for match in re.finditer(script_pattern, html, re.DOTALL):
                script_content = match.group(1)
                try:
                    # Look for JSON objects in script
                    json_pattern = r'\{[^{}]*"fixtures"[^{}]*\}'
                    for json_match in re.finditer(json_pattern, script_content):
                        try:
                            data = json.loads(json_match.group(0))
                            if "fixtures" in data:
                                for fixture in data["fixtures"]:
                                    evt = self.parse_html_fixture(fixture)
                                    if evt:
                                        events.append(evt)
                        except:
                            pass
                except:
                    pass

        except Exception as e:
            print(f"Error extracting from HTML: {e}", flush=True)

        return events

    def parse_state_event(self, event):
        """Parse event from state data"""
        try:
            event_id = f"mgm_{event.get('id', '')}"
            home = event.get("home", {}).get("name", "TBD")
            away = event.get("away", {}).get("name", "TBD")

            evt = {"id": event_id, "home": home, "away": away, "odds": []}

            # Extract markets
            if "markets" in event:
                for market in event["markets"].values():
                    odds = self.parse_market(market)
                    if odds:
                        evt["odds"].append(odds)

            if evt["odds"]:
                return evt
        except:
            pass
        return None

    def parse_state_fixture(self, fixture):
        """Parse fixture from state data"""
        try:
            event_id = f"mgm_{fixture.get('id', '')}"
            participants = fixture.get("participants", [])

            if len(participants) >= 2:
                home = participants[0].get("name", "TBD")
                away = participants[1].get("name", "TBD")

                evt = {"id": event_id, "home": home, "away": away, "odds": []}

                return evt
        except:
            pass
        return None

    def parse_html_fixture(self, fixture):
        """Parse fixture from HTML data"""
        try:
            event_id = f"mgm_{fixture.get('id', '')}"
            home = fixture.get("home", "TBD")
            away = fixture.get("away", "TBD")

            evt = {"id": event_id, "home": home, "away": away, "odds": []}

            # Extract odds if present
            if "odds" in fixture:
                if isinstance(fixture["odds"], dict):
                    if "home" in fixture["odds"] and "away" in fixture["odds"]:
                        evt["odds"].append(
                            {
                                "market": "h2h",
                                "home_price": fixture["odds"]["home"],
                                "away_price": fixture["odds"]["away"],
                            }
                        )

            if evt["odds"]:
                return evt
        except:
            pass
        return None

    def parse_market(self, market):
        """Parse market data"""
        try:
            market_type = market.get("type", "") or market.get("name", "")

            if "moneyline" in market_type.lower() or "h2h" in market_type.lower():
                outcomes = market.get("outcomes", []) or market.get("selections", [])
                if len(outcomes) >= 2:
                    return {
                        "market": "h2h",
                        "home_price": outcomes[0].get("odds")
                        or outcomes[0].get("price"),
                        "away_price": outcomes[1].get("odds")
                        or outcomes[1].get("price"),
                    }

            elif "spread" in market_type.lower():
                outcomes = market.get("outcomes", []) or market.get("selections", [])
                if len(outcomes) >= 2:
                    return {
                        "market": "spreads",
                        "home_price": outcomes[0].get("odds")
                        or outcomes[0].get("price"),
                        "away_price": outcomes[1].get("odds")
                        or outcomes[1].get("price"),
                        "line": outcomes[0].get("line") or outcomes[0].get("handicap"),
                    }

            elif "total" in market_type.lower() or "over" in market_type.lower():
                outcomes = market.get("outcomes", []) or market.get("selections", [])
                if len(outcomes) >= 2:
                    return {
                        "market": "totals",
                        "over_price": outcomes[0].get("odds")
                        or outcomes[0].get("price"),
                        "under_price": outcomes[1].get("odds")
                        or outcomes[1].get("price"),
                        "total": outcomes[0].get("line") or outcomes[0].get("total"),
                    }
        except:
            pass
        return None

    def collect(self):
        """Main collection loop"""
        print(f"BetMGM Cloudscraper collector started at {datetime.now()}", flush=True)

        while True:
            all_events = []

            for url in self.urls:
                try:
                    scraper = self.get_scraper()
                    print(f"Fetching {url[:50]}...", flush=True)

                    resp = scraper.get(url, timeout=30)

                    print(f"  Status: {resp.status_code}", flush=True)

                    if resp.status_code == 200:
                        events = self.extract_odds_from_html(resp.text)
                        if events:
                            all_events.extend(events)
                            print(f"  Found {len(events)} events", flush=True)

                        # If we got good data, save it for analysis
                        if events and len(resp.text) < 1000000:  # Don't save huge files
                            with open("betmgm_success.html", "w") as f:
                                f.write(resp.text)
                            print(
                                "  Saved successful response to betmgm_success.html",
                                flush=True,
                            )

                    elif resp.status_code == 403:
                        print("  Still blocked by Cloudflare", flush=True)

                except Exception as e:
                    print(f"  Error: {str(e)[:100]}", flush=True)

                time.sleep(2)  # Delay between requests

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
    collector = BetMGMCloudscraperCollector()
    collector.collect()

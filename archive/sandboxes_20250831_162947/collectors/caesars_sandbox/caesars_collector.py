#!/usr/bin/env python3
"""
Caesars Sportsbook collector
Using web scraping approach for Caesars/William Hill
"""
import json
import time
import redis
import requests
from bs4 import BeautifulSoup
import re
import hashlib
from datetime import datetime

# Redis connection
r = redis.from_url("redis://broker:6379/0")


class CaesarsCollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )

        # URLs to try - various Caesars and William Hill endpoints
        self.urls = [
            # State-specific Caesars URLs
            "https://sportsbook.caesars.com/us/ny/bet",
            "https://sportsbook.caesars.com/us/nj/bet",
            "https://sportsbook.caesars.com/us/pa/bet",
            "https://sportsbook.caesars.com/us/mi/bet",
            "https://sportsbook.caesars.com/us/az/bet",
            "https://sportsbook.caesars.com/us/co/bet",
            "https://sportsbook.caesars.com/us/in/bet",
            "https://sportsbook.caesars.com/us/ia/bet",
            "https://sportsbook.caesars.com/us/la/bet",
            "https://sportsbook.caesars.com/us/tn/bet",
            "https://sportsbook.caesars.com/us/va/bet",
            "https://sportsbook.caesars.com/us/wv/bet",
            # Sport-specific pages
            "https://sportsbook.caesars.com/us/ny/bet/american-football",
            "https://sportsbook.caesars.com/us/ny/bet/american-football/nfl",
            "https://sportsbook.caesars.com/us/ny/bet/basketball",
            "https://sportsbook.caesars.com/us/ny/bet/basketball/nba",
            "https://sportsbook.caesars.com/us/ny/bet/baseball",
            "https://sportsbook.caesars.com/us/ny/bet/baseball/mlb",
            "https://sportsbook.caesars.com/us/ny/bet/ice-hockey",
            "https://sportsbook.caesars.com/us/ny/bet/ice-hockey/nhl",
            # William Hill legacy URLs
            "https://www.williamhill.com/us/ny/bet",
            "https://www.williamhill.com/us/nj/bet",
            "https://sports.williamhill.com/betting/en-us",
        ]

        self.current_url_index = 0

    def extract_events_from_html(self, html, url):
        """Extract events and odds from HTML"""
        events = []
        soup = BeautifulSoup(html, "html.parser")

        try:
            # Method 1: Look for __INITIAL_STATE__
            if "__INITIAL_STATE__" in html:
                match = re.search(
                    r"window\.__INITIAL_STATE__\s*=\s*({.*?});", html, re.DOTALL
                )
                if match:
                    try:
                        state_data = json.loads(match.group(1))
                        events.extend(self.parse_state_data(state_data))
                    except:
                        pass

            # Method 2: Look for data-event attributes
            event_elements = soup.find_all(attrs={"data-event-id": True})
            for elem in event_elements:
                event_id = elem.get("data-event-id")
                if event_id:
                    event = self.parse_event_element(elem, event_id)
                    if event:
                        events.append(event)

            # Method 3: Look for betting containers with specific classes
            # Common patterns: .event-container, .match-container, .betting-event
            for selector in [
                ".event-container",
                ".match-container",
                ".betting-event",
                ".game-container",
            ]:
                containers = soup.select(selector)
                for container in containers:
                    event = self.parse_container(container)
                    if event:
                        events.append(event)

            # Method 4: Look for odds patterns in text
            if not events:
                events.extend(self.extract_odds_from_text(html))

            # Method 5: Look for JSON-LD structured data
            json_ld = soup.find_all("script", type="application/ld+json")
            for script in json_ld:
                try:
                    data = json.loads(script.string)
                    if "@type" in data and "SportsEvent" in data.get("@type", ""):
                        event = self.parse_json_ld_event(data)
                        if event:
                            events.append(event)
                except:
                    pass

        except Exception as e:
            print(f"Error extracting events: {e}", flush=True)

        return events

    def parse_state_data(self, data):
        """Parse __INITIAL_STATE__ data"""
        events = []

        def search_for_events(obj, path=""):
            if isinstance(obj, dict):
                # Look for event-like structures
                if all(k in obj for k in ["id", "name"]) or all(
                    k in obj for k in ["eventId", "eventName"]
                ):
                    event = self.convert_to_event(obj)
                    if event:
                        events.append(event)

                # Recurse
                for k, v in obj.items():
                    if k in ["events", "matches", "games", "fixtures"]:
                        if isinstance(v, list):
                            for item in v:
                                event = self.convert_to_event(item)
                                if event:
                                    events.append(event)
                    else:
                        search_for_events(v, f"{path}.{k}")

            elif isinstance(obj, list):
                for item in obj:
                    search_for_events(item, path)

        search_for_events(data)
        return events

    def convert_to_event(self, obj):
        """Convert various event formats to our standard format"""
        try:
            # Try to extract basic info
            event_id = obj.get("id") or obj.get("eventId") or obj.get("matchId")
            if not event_id:
                return None

            event = {
                "id": f"caesars_{event_id}",
                "home": "TBD",
                "away": "TBD",
                "odds": [],
            }

            # Extract teams
            if "competitors" in obj:
                competitors = obj["competitors"]
                if len(competitors) >= 2:
                    event["home"] = competitors[0].get("name", "TBD")
                    event["away"] = competitors[1].get("name", "TBD")
            elif "home" in obj and "away" in obj:
                event["home"] = (
                    obj["home"].get("name")
                    if isinstance(obj["home"], dict)
                    else obj["home"]
                )
                event["away"] = (
                    obj["away"].get("name")
                    if isinstance(obj["away"], dict)
                    else obj["away"]
                )
            elif "teams" in obj:
                teams = obj["teams"]
                if len(teams) >= 2:
                    event["home"] = teams[0].get("name", "TBD")
                    event["away"] = teams[1].get("name", "TBD")

            # Extract odds
            if "markets" in obj:
                for market in obj["markets"]:
                    odds = self.parse_market(market)
                    if odds:
                        event["odds"].append(odds)
            elif "odds" in obj:
                if isinstance(obj["odds"], dict):
                    # Direct odds object
                    if "home" in obj["odds"] and "away" in obj["odds"]:
                        event["odds"].append(
                            {
                                "market": "h2h",
                                "home_price": obj["odds"]["home"],
                                "away_price": obj["odds"]["away"],
                            }
                        )

            if event["odds"] or (event["home"] != "TBD" and event["away"] != "TBD"):
                return event

        except:
            pass

        return None

    def parse_market(self, market):
        """Parse market data"""
        try:
            market_type = market.get("type", "") or market.get("name", "")

            if "moneyline" in market_type.lower() or "winner" in market_type.lower():
                selections = market.get("selections", []) or market.get("outcomes", [])
                if len(selections) >= 2:
                    return {
                        "market": "h2h",
                        "home_price": selections[0].get("odds")
                        or selections[0].get("price"),
                        "away_price": selections[1].get("odds")
                        or selections[1].get("price"),
                    }

            elif "spread" in market_type.lower() or "handicap" in market_type.lower():
                selections = market.get("selections", []) or market.get("outcomes", [])
                if len(selections) >= 2:
                    return {
                        "market": "spreads",
                        "home_price": selections[0].get("odds")
                        or selections[0].get("price"),
                        "away_price": selections[1].get("odds")
                        or selections[1].get("price"),
                        "line": selections[0].get("line")
                        or selections[0].get("handicap"),
                    }

            elif "total" in market_type.lower() or "over" in market_type.lower():
                selections = market.get("selections", []) or market.get("outcomes", [])
                if len(selections) >= 2:
                    return {
                        "market": "totals",
                        "over_price": selections[0].get("odds")
                        or selections[0].get("price"),
                        "under_price": selections[1].get("odds")
                        or selections[1].get("price"),
                        "total": selections[0].get("line")
                        or selections[0].get("total"),
                    }
        except:
            pass

        return None

    def parse_event_element(self, elem, event_id):
        """Parse HTML element with event data"""
        try:
            event = {
                "id": f"caesars_{event_id}",
                "home": elem.get("data-home-team", "TBD"),
                "away": elem.get("data-away-team", "TBD"),
                "odds": [],
            }

            # Look for odds in data attributes
            home_odds = elem.get("data-home-odds")
            away_odds = elem.get("data-away-odds")

            if home_odds and away_odds:
                event["odds"].append(
                    {
                        "market": "h2h",
                        "home_price": self.convert_odds(home_odds),
                        "away_price": self.convert_odds(away_odds),
                    }
                )

            if event["odds"]:
                return event
        except:
            pass

        return None

    def parse_container(self, container):
        """Parse a betting container element"""
        try:
            # Extract team names
            teams = container.select(".team-name, .competitor-name, .team")
            if len(teams) >= 2:
                home = teams[0].get_text(strip=True)
                away = teams[1].get_text(strip=True)

                # Generate event ID
                event_id = hashlib.md5(
                    f"{home}_{away}_{time.time()}".encode()
                ).hexdigest()[:8]

                event = {
                    "id": f"caesars_{event_id}",
                    "home": home,
                    "away": away,
                    "odds": [],
                }

                # Look for odds
                odds_elements = container.select(".odds, .price, .bet-price")
                if len(odds_elements) >= 2:
                    event["odds"].append(
                        {
                            "market": "h2h",
                            "home_price": self.convert_odds(
                                odds_elements[0].get_text(strip=True)
                            ),
                            "away_price": self.convert_odds(
                                odds_elements[1].get_text(strip=True)
                            ),
                        }
                    )

                if event["odds"]:
                    return event
        except:
            pass

        return None

    def parse_json_ld_event(self, data):
        """Parse JSON-LD structured data"""
        try:
            event_id = data.get(
                "@id", hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
            )

            event = {
                "id": f"caesars_{event_id}",
                "home": data.get("homeTeam", {}).get("name", "TBD"),
                "away": data.get("awayTeam", {}).get("name", "TBD"),
                "odds": [],
            }

            return event if event["home"] != "TBD" else None
        except:
            pass

        return None

    def extract_odds_from_text(self, html):
        """Extract odds from raw text patterns"""
        events = []

        # Find team vs team patterns
        team_pattern = re.findall(
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:vs?\.?|@|versus)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
            html,
        )

        # Find American odds patterns
        odds_pattern = re.findall(r"[+-]\d{3,4}", html)

        if team_pattern and odds_pattern:
            # Try to match teams with odds
            for i, (home, away) in enumerate(team_pattern[:10]):  # Limit to first 10
                if i * 2 < len(odds_pattern):
                    event_id = hashlib.md5(
                        f"{home}_{away}_{time.time()}".encode()
                    ).hexdigest()[:8]

                    event = {
                        "id": f"caesars_{event_id}",
                        "home": home,
                        "away": away,
                        "odds": [],
                    }

                    # Try to get corresponding odds
                    if i * 2 + 1 < len(odds_pattern):
                        event["odds"].append(
                            {
                                "market": "h2h",
                                "home_price": self.convert_odds(odds_pattern[i * 2]),
                                "away_price": self.convert_odds(
                                    odds_pattern[i * 2 + 1]
                                ),
                            }
                        )

                    if event["odds"]:
                        events.append(event)

        return events

    def convert_odds(self, odds_str):
        """Convert odds string to integer"""
        try:
            if isinstance(odds_str, (int, float)):
                return int(odds_str)

            # Clean the string
            odds_str = str(odds_str).strip()

            # Handle American odds
            if odds_str.startswith("+") or odds_str.startswith("-"):
                return int(odds_str)

            # Try to extract number
            match = re.search(r"[+-]?\d+", odds_str)
            if match:
                return int(match.group())
        except:
            pass

        return None

    def fetch_page(self, url):
        """Fetch a page with proper session management"""
        try:
            # Add delay to avoid rate limiting
            time.sleep(2)

            resp = self.session.get(url, timeout=15)

            if resp.status_code == 200:
                return resp.text
            else:
                print(f"Got status {resp.status_code} for {url}", flush=True)

        except Exception as e:
            print(f"Error fetching {url}: {e}", flush=True)

        return None

    def collect(self):
        """Main collection loop"""
        print(f"Caesars collector started at {datetime.now()}", flush=True)

        while True:
            all_events = []

            # Try different URLs in rotation
            urls_to_try = self.urls[self.current_url_index : self.current_url_index + 3]
            self.current_url_index = (self.current_url_index + 3) % len(self.urls)

            for url in urls_to_try:
                print(f"Trying {url}...", flush=True)
                html = self.fetch_page(url)

                if html:
                    events = self.extract_events_from_html(html, url)
                    if events:
                        all_events.extend(events)
                        print(f"  Found {len(events)} events", flush=True)

                    # If we got good data from one URL, use it
                    if len(all_events) > 20:
                        break

            # Publish to Redis if we have data
            if all_events:
                message = {
                    "timestamp": time.time(),
                    "source": "caesars",
                    "events": all_events,
                }

                r.publish("odds.raw.caesars", json.dumps(message))
                print(f"Published {len(all_events)} events to Redis", flush=True)
            else:
                print("No events found, will retry...", flush=True)

            # Wait before next collection
            time.sleep(30)


if __name__ == "__main__":
    collector = CaesarsCollector()
    collector.collect()

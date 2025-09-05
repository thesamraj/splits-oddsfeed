#!/usr/bin/env python3
"""
FanDuel Mobile API Collector
Uses mobile app API endpoints that typically don't have Cloudflare protection
"""
import json
import time
import redis
import os
import requests
from datetime import datetime
import uuid

REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.fanduel")


class FanDuelMobileAPICollector:
    def __init__(self):
        self.redis = redis.from_url(REDIS_URL)
        self.session = requests.Session()

        # Mobile app headers
        self.session.headers.update(
            {
                "User-Agent": "FanDuel Sportsbook/5.43.0 (iPhone; iOS 17.2; Scale/3.00)",
                "Accept": "application/json",
                "Accept-Language": "en-US;q=1",
                "Accept-Encoding": "gzip, deflate, br",
                "X-Application": "FD-SB-iOS",
                "X-App-Version": "5.43.0",
                "X-Device-Id": str(uuid.uuid4()),
                "X-Platform": "iOS",
                "X-Api-Key": "FhMFpcPWXMeyZxOx",  # Common API key from web version
                "Connection": "keep-alive",
            }
        )

        # Mobile API base URLs (multiple regions for redundancy)
        self.api_bases = [
            "https://sbapi.nj.sportsbook.fanduel.com",
            "https://sbapi.pa.sportsbook.fanduel.com",
            "https://sbapi.mi.sportsbook.fanduel.com",
            "https://sbapi.in.sportsbook.fanduel.com",
            "https://sbapi.co.sportsbook.fanduel.com",
            "https://sbapi.az.sportsbook.fanduel.com",
            "https://sbapi.tn.sportsbook.fanduel.com",
            "https://sbapi.il.sportsbook.fanduel.com",
            "https://sbapi.wv.sportsbook.fanduel.com",
            "https://sbapi.ny.sportsbook.fanduel.com",
        ]

        # Sport IDs used by FanDuel
        self.sport_configs = [
            {"id": 7522, "name": "NFL", "tab": "american_football"},
            {"id": 7523, "name": "NCAAF", "tab": "american_football"},
            {"id": 7524, "name": "NBA", "tab": "basketball"},
            {"id": 7525, "name": "NCAAB", "tab": "basketball"},
            {"id": 84240, "name": "MLB", "tab": "baseball"},
            {"id": 7526, "name": "NHL", "tab": "ice_hockey"},
            {"id": 7511, "name": "EPL", "tab": "soccer"},
            {"id": 7527, "name": "MLS", "tab": "soccer"},
            {"id": 2, "name": "Tennis", "tab": "tennis"},
            {"id": 12, "name": "Golf", "tab": "golf"},
            {"id": 7528, "name": "UFC", "tab": "mma"},
        ]

        self.working_base = None

    def find_working_endpoint(self):
        """Find a working API endpoint"""
        for base in self.api_bases:
            try:
                # Test with a simple endpoint
                test_url = f"{base}/api/content"
                resp = self.session.get(test_url, timeout=5)
                if resp.status_code in [
                    200,
                    404,
                ]:  # 404 is ok, means endpoint exists but needs params
                    self.working_base = base
                    print(f"Found working endpoint: {base}", flush=True)
                    return True
            except:
                continue
        return False

    def get_events_for_sport(self, sport_config):
        """Get events for a specific sport"""
        if not self.working_base:
            return []

        events = []

        # Multiple endpoint patterns used by mobile app
        endpoints = [
            f"/api/content-managed-page?page={sport_config['tab']}&_ak=FhMFpcPWXMeyZxOx",
            f"/api/events?sport_id={sport_config['id']}&include_prices=true",
            f"/api/sports/{sport_config['id']}/events",
            f"/api/competitions?sport={sport_config['id']}&tab={sport_config['tab']}",
            f"/api/event-hub?sport={sport_config['id']}",
            f"/api/content/markets?sport_id={sport_config['id']}",
            f"/api/meetings?sport={sport_config['id']}",
        ]

        for endpoint in endpoints:
            try:
                url = self.working_base + endpoint
                resp = self.session.get(url, timeout=10)

                if resp.status_code == 200:
                    try:
                        data = resp.json()
                        parsed_events = self.parse_mobile_response(
                            data, sport_config["name"]
                        )
                        if parsed_events:
                            events.extend(parsed_events)
                            print(
                                f"  Found {len(parsed_events)} events from {endpoint[:30]}...",
                                flush=True,
                            )
                    except json.JSONDecodeError:
                        pass

            except Exception:
                continue

        return events

    def parse_mobile_response(self, data, sport_name):
        """Parse mobile API response"""
        events = []

        if not data:
            return events

        # Handle different response structures
        if isinstance(data, dict):
            # Look for events in various keys
            for key in [
                "events",
                "eventMarkets",
                "competitions",
                "meetings",
                "fixtures",
                "attachments",
            ]:
                if key in data:
                    items = data[key]
                    if isinstance(items, list):
                        for item in items:
                            event = self.extract_mobile_event(item, sport_name)
                            if event:
                                events.append(event)
                    elif isinstance(items, dict):
                        for sub_item in items.values():
                            event = self.extract_mobile_event(sub_item, sport_name)
                            if event:
                                events.append(event)

            # Check for nested structure
            if "page" in data and isinstance(data["page"], dict):
                if "attachments" in data["page"]:
                    for attachment in data["page"]["attachments"]:
                        if "events" in attachment:
                            for evt in attachment["events"]:
                                event = self.extract_mobile_event(evt, sport_name)
                                if event:
                                    events.append(event)

        elif isinstance(data, list):
            for item in data:
                event = self.extract_mobile_event(item, sport_name)
                if event:
                    events.append(event)

        return events

    def extract_mobile_event(self, data, sport_name):
        """Extract event from mobile API data"""
        if not isinstance(data, dict):
            return None

        # Extract event ID
        event_id = (
            data.get("eventId")
            or data.get("id")
            or data.get("externalId")
            or data.get("fixtureId")
        )

        if not event_id:
            return None

        # Extract teams/participants
        home = None
        away = None

        # Try different structures
        if (
            "teams" in data
            and isinstance(data["teams"], list)
            and len(data["teams"]) >= 2
        ):
            home = data["teams"][0].get("name", "TBD")
            away = data["teams"][1].get("name", "TBD")
        elif "competitors" in data and isinstance(data["competitors"], list):
            for comp in data["competitors"]:
                if comp.get("home"):
                    home = comp.get("name", "TBD")
                else:
                    away = comp.get("name", "TBD")
        elif "home" in data and "away" in data:
            home = (
                data["home"].get("name")
                if isinstance(data["home"], dict)
                else data.get("home", "TBD")
            )
            away = (
                data["away"].get("name")
                if isinstance(data["away"], dict)
                else data.get("away", "TBD")
            )
        elif "name" in data:
            # Parse from event name
            import re

            match = re.search(r"(.+?)\s+(?:@|vs?\.?)\s+(.+)", data["name"])
            if match:
                away = match.group(1).strip()
                home = match.group(2).strip()

        if not home or not away:
            home = (
                data.get("homeTeam", {}).get("name", "TBD")
                if isinstance(data.get("homeTeam"), dict)
                else "TBD"
            )
            away = (
                data.get("awayTeam", {}).get("name", "TBD")
                if isinstance(data.get("awayTeam"), dict)
                else "TBD"
            )

        # Extract odds
        odds = []

        # Look for markets
        markets = data.get("markets", []) or data.get("marketTypes", [])
        if isinstance(markets, dict):
            markets = list(markets.values())

        for market in markets if isinstance(markets, list) else []:
            market_odds = self.extract_market_odds(market)
            odds.extend(market_odds)

        # Also check for direct odds in the event
        if "odds" in data and isinstance(data["odds"], dict):
            for market_type, market_data in data["odds"].items():
                if isinstance(market_data, list):
                    for selection in market_data:
                        if "americanOdds" in selection or "price" in selection:
                            odds.append(
                                {
                                    "market": market_type,
                                    "price": selection.get(
                                        "americanOdds", selection.get("price")
                                    ),
                                    "label": selection.get("label", ""),
                                }
                            )

        # If no odds but has price data
        if not odds and "prices" in data:
            for price_item in data["prices"]:
                if isinstance(price_item, dict):
                    odds.append(
                        {
                            "market": price_item.get("marketType", "h2h"),
                            "price": price_item.get(
                                "americanOdds", price_item.get("price")
                            ),
                            "label": price_item.get("selectionName", ""),
                        }
                    )

        if odds or (home != "TBD" and away != "TBD"):
            return {
                "id": f"fd_{event_id}",
                "home": home,
                "away": away,
                "sport": sport_name,
                "odds": odds,
            }

        return None

    def extract_market_odds(self, market):
        """Extract odds from market object"""
        odds = []

        if not isinstance(market, dict):
            return odds

        market_type = market.get("marketType", market.get("type", "h2h"))

        # Look for selections/runners
        selections = (
            market.get("runners")
            or market.get("selections")
            or market.get("outcomes", [])
        )

        if isinstance(selections, dict):
            selections = list(selections.values())

        for selection in selections if isinstance(selections, list) else []:
            if isinstance(selection, dict):
                # Extract price
                price = None

                # Try different price fields
                if "americanOdds" in selection:
                    price = selection["americanOdds"]
                elif "price" in selection:
                    price = selection["price"]
                elif "odds" in selection and isinstance(selection["odds"], dict):
                    price = selection["odds"].get(
                        "americanOdds", selection["odds"].get("american")
                    )
                elif "winRunnerOdds" in selection and isinstance(
                    selection["winRunnerOdds"], dict
                ):
                    price = (
                        selection["winRunnerOdds"]
                        .get("americanDisplayOdds", {})
                        .get("americanOdds")
                    )

                if price:
                    # Handle different market types
                    if market_type in ["MATCH", "MONEY_LINE", "h2h", "moneyline"]:
                        if (
                            "home" in selection.get("name", "").lower()
                            or selection.get("type") == "HOME"
                        ):
                            odds.append({"market": "h2h", "home_price": price})
                        elif (
                            "away" in selection.get("name", "").lower()
                            or selection.get("type") == "AWAY"
                        ):
                            odds.append({"market": "h2h", "away_price": price})
                    elif market_type in ["SPREAD", "HANDICAP", "spreads"]:
                        line = selection.get("handicap", selection.get("line", -1.5))
                        odds.append(
                            {
                                "market": "spreads",
                                "price": price,
                                "line": line,
                                "label": selection.get("name", ""),
                            }
                        )
                    elif market_type in ["TOTAL", "OVER_UNDER", "totals"]:
                        total = selection.get("line", market.get("total", 215.5))
                        if "over" in selection.get("name", "").lower():
                            odds.append(
                                {
                                    "market": "totals",
                                    "over_price": price,
                                    "total": total,
                                }
                            )
                        elif "under" in selection.get("name", "").lower():
                            odds.append(
                                {
                                    "market": "totals",
                                    "under_price": price,
                                    "total": total,
                                }
                            )
                    else:
                        odds.append(
                            {
                                "market": market_type,
                                "price": price,
                                "label": selection.get(
                                    "name", selection.get("runnerName", "")
                                ),
                            }
                        )

        return odds

    def collect(self):
        """Main collection loop"""
        print(f"FanDuel Mobile API Collector started at {datetime.now()}", flush=True)

        # Find working endpoint
        if not self.find_working_endpoint():
            print("No working endpoints found, will retry...", flush=True)

        while True:
            try:
                # Find working endpoint if needed
                if not self.working_base:
                    self.find_working_endpoint()
                    if not self.working_base:
                        print("Still no working endpoints, waiting...", flush=True)
                        time.sleep(60)
                        continue

                all_events = []

                # Collect from each sport
                for sport_config in self.sport_configs:
                    print(f"Fetching {sport_config['name']}...", flush=True)
                    events = self.get_events_for_sport(sport_config)
                    all_events.extend(events)
                    time.sleep(1)  # Rate limiting

                # Deduplicate events
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
                self.working_base = None  # Reset endpoint on error
                time.sleep(60)


if __name__ == "__main__":
    collector = FanDuelMobileAPICollector()
    collector.collect()

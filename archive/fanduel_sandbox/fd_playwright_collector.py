#!/usr/bin/env python3
"""
FanDuel Playwright Collector with Stealth Mode
Uses browser automation to bypass Cloudflare protection
"""
import json
import time
import redis
import os
import asyncio
import hashlib
from datetime import datetime
from playwright.async_api import async_playwright

REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.fanduel")


class FanDuelPlaywrightCollector:
    def __init__(self):
        self.redis = redis.from_url(REDIS_URL)
        self.browser = None
        self.context = None
        self.page = None

        # URLs to scrape
        self.sport_urls = [
            "https://sportsbook.fanduel.com/navigation/nfl",
            "https://sportsbook.fanduel.com/navigation/nba",
            "https://sportsbook.fanduel.com/navigation/mlb",
            "https://sportsbook.fanduel.com/navigation/nhl",
            "https://sportsbook.fanduel.com/navigation/ncaaf",
            "https://sportsbook.fanduel.com/navigation/ncaab",
            "https://sportsbook.fanduel.com/navigation/soccer",
            "https://sportsbook.fanduel.com/navigation/tennis",
        ]

    async def setup_browser(self):
        """Setup Playwright browser with stealth settings"""
        playwright = await async_playwright().start()

        # Use Chromium with stealth settings
        self.browser = await playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            ],
        )

        # Create context with viewport and permissions
        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            },
        )

        # Add stealth scripts to bypass detection
        await self.context.add_init_script(
            """
            // Overwrite the `navigator.webdriver` property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });

            // Mock languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });

            // Mock plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    {
                        0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
                        description: "Portable Document Format",
                        filename: "internal-pdf-viewer",
                        length: 1,
                        name: "Chrome PDF Plugin"
                    }
                ]
            });

            // Mock permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
        """
        )

        self.page = await self.context.new_page()

        # Set extra headers
        await self.page.set_extra_http_headers(
            {
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )

    async def extract_odds_from_page(self):
        """Extract odds data from the current page"""
        events = []

        try:
            # Wait for content to load
            await self.page.wait_for_load_state("networkidle", timeout=10000)

            # Try multiple extraction methods

            # Method 1: Extract from window.__INITIAL_STATE__
            try:
                initial_state = await self.page.evaluate(
                    """
                    () => {
                        if (window.__INITIAL_STATE__) {
                            return JSON.stringify(window.__INITIAL_STATE__);
                        }
                        return null;
                    }
                """
                )

                if initial_state:
                    state_data = json.loads(initial_state)
                    events.extend(self.parse_initial_state(state_data))
            except:
                pass

            # Method 2: Extract from DOM elements
            try:
                # Look for event cards/containers
                event_elements = await self.page.query_selector_all(
                    '[data-test-id*="event"], [class*="event-card"], [class*="EventCard"], [class*="match-card"]'
                )

                for element in event_elements[:50]:  # Limit to 50 events
                    event_data = await self.extract_event_from_element(element)
                    if event_data:
                        events.append(event_data)
            except:
                pass

            # Method 3: Intercept API calls
            # This is handled in the request interception setup

        except Exception as e:
            print(f"Error extracting odds: {e}", flush=True)

        return events

    async def extract_event_from_element(self, element):
        """Extract event data from a DOM element"""
        try:
            event_text = await element.text_content()

            # Extract teams (look for vs, @, or v patterns)
            import re

            teams_match = re.search(
                r"([A-Za-z\s]+?)\s+(?:vs?\.?|@|v)\s+([A-Za-z\s]+)", event_text
            )

            if not teams_match:
                return None

            home_team = teams_match.group(1).strip()
            away_team = teams_match.group(2).strip()

            # Extract odds (American format)
            odds_matches = re.findall(r"([-+]\d{3,4})", event_text)

            if not odds_matches:
                return None

            event_id = (
                f"fd_{hashlib.md5(f'{home_team}_{away_team}'.encode()).hexdigest()[:8]}"
            )

            odds = []
            if len(odds_matches) >= 2:
                odds.append(
                    {
                        "market": "h2h",
                        "home_price": int(odds_matches[0]),
                        "away_price": int(odds_matches[1]),
                    }
                )
            if len(odds_matches) >= 4:
                odds.append(
                    {
                        "market": "spreads",
                        "home_price": int(odds_matches[2]),
                        "away_price": int(odds_matches[3]),
                        "line": -1.5,
                    }
                )
            if len(odds_matches) >= 6:
                odds.append(
                    {
                        "market": "totals",
                        "over_price": int(odds_matches[4]),
                        "under_price": int(odds_matches[5]),
                        "total": 215.5,
                    }
                )

            return {"id": event_id, "home": home_team, "away": away_team, "odds": odds}
        except:
            return None

    def parse_initial_state(self, state_data):
        """Parse __INITIAL_STATE__ data"""
        events = []

        if not state_data:
            return events

        # Navigate through possible data structures
        def find_events_recursive(obj, depth=0):
            if depth > 5:  # Prevent infinite recursion
                return []

            found_events = []

            if isinstance(obj, dict):
                # Check if this looks like an event
                if any(key in obj for key in ["eventId", "fixtureId", "marketId"]):
                    event = self.extract_event_from_object(obj)
                    if event:
                        found_events.append(event)

                # Recurse through dict values
                for key, value in obj.items():
                    if key.lower() in ["events", "fixtures", "markets", "competitions"]:
                        found_events.extend(find_events_recursive(value, depth + 1))

            elif isinstance(obj, list):
                for item in obj[:100]:  # Limit recursion
                    found_events.extend(find_events_recursive(item, depth + 1))

            return found_events

        events = find_events_recursive(state_data)

        # Deduplicate by ID
        seen_ids = set()
        unique_events = []
        for event in events:
            if event["id"] not in seen_ids:
                seen_ids.add(event["id"])
                unique_events.append(event)

        return unique_events

    def extract_event_from_object(self, obj):
        """Extract event from a data object"""
        try:
            # Extract ID
            event_id = (
                obj.get("eventId")
                or obj.get("fixtureId")
                or obj.get("id")
                or f"fd_{hashlib.md5(str(obj).encode()).hexdigest()[:8]}"
            )

            # Extract teams
            home = (
                obj.get("home", {}).get("name")
                if isinstance(obj.get("home"), dict)
                else obj.get("homeName", "TBD")
            )
            away = (
                obj.get("away", {}).get("name")
                if isinstance(obj.get("away"), dict)
                else obj.get("awayName", "TBD")
            )

            # Extract odds
            odds = []

            # Look for markets
            markets = obj.get("markets", [])
            if isinstance(markets, dict):
                markets = list(markets.values())

            for market in markets[:10]:  # Limit markets
                if isinstance(market, dict):
                    for selection in market.get("selections", []):
                        if isinstance(selection, dict) and "price" in selection:
                            odds.append(
                                {
                                    "market": market.get("marketType", "h2h"),
                                    "price": selection["price"],
                                    "label": selection.get("name", ""),
                                }
                            )

            if odds:
                return {
                    "id": (
                        f"fd_{event_id}"
                        if not str(event_id).startswith("fd_")
                        else event_id
                    ),
                    "home": home,
                    "away": away,
                    "odds": odds,
                }
        except:
            pass

        return None

    async def collect_with_api_interception(self):
        """Collect data by intercepting API responses"""
        collected_events = []

        # Setup request interception
        async def handle_response(response):
            try:
                url = response.url

                # Check if this is an API endpoint
                if any(
                    pattern in url
                    for pattern in ["/api/", "/sbapi/", "/cache/", ".json"]
                ):
                    if response.status == 200:
                        try:
                            data = await response.json()
                            events = self.parse_api_response(data)
                            collected_events.extend(events)
                        except:
                            pass
            except:
                pass

        self.page.on("response", handle_response)

        # Visit main page first
        try:
            print("Navigating to FanDuel main page...", flush=True)
            await self.page.goto(
                "https://sportsbook.fanduel.com/",
                wait_until="networkidle",
                timeout=30000,
            )
            await asyncio.sleep(3)
        except Exception as e:
            print(f"Error loading main page: {e}", flush=True)

        # Visit each sport page
        for url in self.sport_urls:
            try:
                print(f"Fetching {url}...", flush=True)
                await self.page.goto(url, wait_until="networkidle", timeout=20000)

                # Extract odds from the page
                page_events = await self.extract_odds_from_page()
                collected_events.extend(page_events)

                # Wait a bit for any async requests
                await asyncio.sleep(2)

            except Exception as e:
                print(f"Error fetching {url}: {e}", flush=True)

        return collected_events

    def parse_api_response(self, data):
        """Parse API response data"""
        events = []

        if isinstance(data, dict):
            # Check for events in common locations
            for key in ["events", "fixtures", "attachments", "competitions"]:
                if key in data:
                    events.extend(self.parse_initial_state({key: data[key]}))

        elif isinstance(data, list):
            for item in data:
                event = self.extract_event_from_object(item)
                if event:
                    events.append(event)

        return events

    async def run(self):
        """Main collection loop"""
        print(f"FanDuel Playwright Collector started at {datetime.now()}", flush=True)

        await self.setup_browser()

        while True:
            try:
                # Collect events
                all_events = await self.collect_with_api_interception()

                # Deduplicate
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
                await asyncio.sleep(30)

            except Exception as e:
                print(f"Collection error: {e}", flush=True)
                await asyncio.sleep(60)


if __name__ == "__main__":
    collector = FanDuelPlaywrightCollector()
    asyncio.run(collector.run())

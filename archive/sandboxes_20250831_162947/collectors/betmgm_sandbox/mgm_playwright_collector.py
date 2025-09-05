#!/usr/bin/env python3
"""
BetMGM collector using Playwright with stealth mode
Uses browser automation to bypass Cloudflare
"""
import json
import time
import redis
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright

# Redis connection
r = redis.from_url("redis://broker:6379/0")


class BetMGMPlaywrightCollector:
    def __init__(self):
        self.events_data = []

    async def intercept_network(self, response):
        """Intercept network responses to capture API data"""
        try:
            # Look for API responses
            if "cds-api" in response.url or "api" in response.url:
                if response.status == 200:
                    content_type = response.headers.get("content-type", "")
                    if "json" in content_type:
                        try:
                            data = await response.json()
                            print(
                                f"Intercepted API: {response.url[:80]}...", flush=True
                            )

                            # Extract events from the response
                            events = self.extract_events_from_response(data)
                            if events:
                                self.events_data.extend(events)
                                print(f"  Extracted {len(events)} events", flush=True)
                        except:
                            pass
        except Exception:
            pass

    def extract_events_from_response(self, data):
        """Extract events from intercepted API response"""
        events = []

        try:
            # Check various response structures
            if isinstance(data, dict):
                # Look for fixtures
                if "fixtures" in data:
                    for fixture in data["fixtures"]:
                        event = self.parse_fixture(fixture)
                        if event:
                            events.append(event)

                # Look for events
                elif "events" in data:
                    for evt in data["events"]:
                        event = self.parse_event(evt)
                        if event:
                            events.append(event)

                # Look for widgets (mobile structure)
                elif "widgets" in data:
                    for widget in data["widgets"]:
                        if "events" in widget:
                            for evt in widget["events"]:
                                event = self.parse_event(evt)
                                if event:
                                    events.append(event)
        except:
            pass

        return events

    def parse_fixture(self, fixture):
        """Parse a fixture object"""
        try:
            event_id = f"mgm_{fixture.get('id', '')}"
            participants = fixture.get("participants", [])

            if len(participants) >= 2:
                home = participants[0].get("name", {}).get("value", "TBD")
                away = participants[1].get("name", {}).get("value", "TBD")

                event = {"id": event_id, "home": home, "away": away, "odds": []}

                # Extract odds from option markets
                for market in fixture.get("optionMarkets", []):
                    market_name = market.get("name", {}).get("value", "")
                    options = market.get("options", [])

                    if "Money Line" in market_name and len(options) >= 2:
                        event["odds"].append(
                            {
                                "market": "h2h",
                                "home_price": options[0]
                                .get("price", {})
                                .get("american"),
                                "away_price": options[1]
                                .get("price", {})
                                .get("american"),
                            }
                        )
                    elif "Spread" in market_name and len(options) >= 2:
                        event["odds"].append(
                            {
                                "market": "spreads",
                                "home_price": options[0]
                                .get("price", {})
                                .get("american"),
                                "away_price": options[1]
                                .get("price", {})
                                .get("american"),
                                "line": options[0].get("line", {}).get("american"),
                            }
                        )

                if event["odds"]:
                    return event
        except:
            pass

        return None

    def parse_event(self, evt):
        """Parse an event object"""
        try:
            event_id = f"mgm_{evt.get('id', '')}"
            home = evt.get("home", {}).get("name", "TBD")
            away = evt.get("away", {}).get("name", "TBD")

            event = {"id": event_id, "home": home, "away": away, "odds": []}

            # Extract markets
            for market in evt.get("markets", []):
                market_type = market.get("type", "")
                outcomes = market.get("outcomes", [])

                if market_type == "moneyline" and len(outcomes) >= 2:
                    event["odds"].append(
                        {
                            "market": "h2h",
                            "home_price": outcomes[0].get("odds"),
                            "away_price": outcomes[1].get("odds"),
                        }
                    )

            if event["odds"]:
                return event
        except:
            pass

        return None

    async def collect_with_browser(self):
        """Collect odds using browser automation"""
        async with async_playwright() as p:
            # Launch browser with stealth settings
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-web-security",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                ],
            )

            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )

            # Add stealth scripts
            await context.add_init_script(
                """
                // Override navigator properties
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => false,
                });

                // Override chrome property
                window.chrome = {
                    runtime: {},
                };

                // Override permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
            """
            )

            page = await context.new_page()

            # Set up network interception
            page.on("response", self.intercept_network)

            # Clear events data
            self.events_data = []

            print("Navigating to BetMGM...", flush=True)

            try:
                # Navigate to BetMGM
                await page.goto(
                    "https://sports.betmgm.com/en/sports",
                    wait_until="networkidle",
                    timeout=30000,
                )
                print("Page loaded", flush=True)

                # Wait for content
                await page.wait_for_timeout(5000)

                # Try to click on NFL if available
                try:
                    await page.click("text=NFL", timeout=5000)
                    await page.wait_for_timeout(3000)
                    print("Clicked on NFL", flush=True)
                except:
                    pass

                # Try to extract data from the page
                page_data = await page.evaluate(
                    """
                    () => {
                        // Look for window variables with data
                        const data = {};

                        if (window.__INITIAL_STATE__) {
                            data.initial_state = window.__INITIAL_STATE__;
                        }

                        if (window.__PRELOADED_STATE__) {
                            data.preloaded_state = window.__PRELOADED_STATE__;
                        }

                        if (window.APP_STATE) {
                            data.app_state = window.APP_STATE;
                        }

                        // Look for React props
                        const reactRoot = document.querySelector('#root') || document.querySelector('[data-reactroot]');
                        if (reactRoot && reactRoot._reactRootContainer) {
                            try {
                                const fiber = reactRoot._reactRootContainer._internalRoot.current;
                                if (fiber && fiber.memoizedProps) {
                                    data.react_props = fiber.memoizedProps;
                                }
                            } catch (e) {}
                        }

                        return data;
                    }
                """
                )

                if page_data:
                    # Extract events from page data
                    for key, value in page_data.items():
                        if value:
                            events = self.extract_events_from_response(value)
                            if events:
                                self.events_data.extend(events)
                                print(
                                    f"Extracted {len(events)} events from {key}",
                                    flush=True,
                                )

            except Exception as e:
                print(f"Browser error: {e}", flush=True)

            await browser.close()

            return self.events_data

    async def run(self):
        """Main collection loop"""
        print(f"BetMGM Playwright collector started at {datetime.now()}", flush=True)

        while True:
            try:
                events = await self.collect_with_browser()

                if events:
                    message = {
                        "timestamp": time.time(),
                        "source": "betmgm",
                        "events": events,
                    }

                    r.publish("odds.raw.betmgm", json.dumps(message))
                    print(f"Published {len(events)} events to Redis", flush=True)
                else:
                    print("No events collected", flush=True)

            except Exception as e:
                print(f"Collection error: {e}", flush=True)

            # Wait before next collection
            await asyncio.sleep(30)


async def main():
    collector = BetMGMPlaywrightCollector()
    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())

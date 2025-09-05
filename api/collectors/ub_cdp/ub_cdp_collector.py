#!/usr/bin/env python3
"""
Unibet CDP Stealth Collector
Activates when HTTP collector signals fallback needed
"""
import json
import os
import asyncio
from datetime import datetime
import redis
from playwright.async_api import async_playwright
from typing import Dict, Any, Optional

# Configuration
UB_URL = os.getenv("UB_URL", "https://pa.unibet.com/?page=sportsbook#home")
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
PUBLISH_CHANNEL = os.getenv("PUBLISH_CHANNEL", "odds.raw.unibet")
FALLBACK_SIGNAL_KEY = "ub:collector:fallback_required"
HEALTHZ_PORT = int(os.getenv("HEALTHZ_PORT", "9135"))
SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL_SEC", "120"))


class UBCDPCollector:
    def __init__(self):
        self.redis_client = redis.from_url(REDIS_URL)
        self.is_active = False
        self.last_scrape_success = None
        self.events_captured = 0
        self.browser = None
        self.context = None
        self.page = None

    async def setup_browser(self):
        """Setup stealth browser with CDP"""
        playwright = await async_playwright().start()

        # Stealth settings
        self.browser = await playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
                "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            ],
        )

        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

        # Add stealth scripts
        await self.context.add_init_script(
            """
            // Override navigator.webdriver
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });

            // Override chrome runtime
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

        self.page = await self.context.new_page()

        # Setup CDP session for network interception
        client = await self.page.context.new_cdp_session(self.page)
        await client.send("Network.enable")
        await client.send("Runtime.enable")

        # Intercept API responses
        self.api_responses = []

        async def handle_response(response):
            if "kambi" in response.url or "offering" in response.url:
                try:
                    data = await response.json()
                    self.api_responses.append(
                        {
                            "url": response.url,
                            "data": data,
                            "timestamp": datetime.utcnow().isoformat(),
                        }
                    )
                except:
                    pass

        self.page.on("response", handle_response)

    async def scrape_unibet(self) -> Optional[Dict[str, Any]]:
        """Scrape Unibet using CDP"""
        try:
            # Clear previous responses
            self.api_responses = []

            # Navigate to Unibet
            await self.page.goto(UB_URL, wait_until="networkidle", timeout=30000)

            # Wait for content to load
            await self.page.wait_for_timeout(5000)

            # Try to navigate to live section
            try:
                await self.page.click('text="Live"', timeout=5000)
                await self.page.wait_for_timeout(3000)
            except:
                pass

            # Extract data from page state
            events = []

            # Try to get Kambi data from window object
            try:
                kambi_data = await self.page.evaluate(
                    """() => {
                    // Try multiple possible locations
                    if (window.__KAMBI_DATA__) return window.__KAMBI_DATA__;
                    if (window.kambi && window.kambi.data) return window.kambi.data;
                    if (window.KambiBC && window.KambiBC.data) return window.KambiBC.data;

                    // Check localStorage
                    const stored = localStorage.getItem('kambi_events');
                    if (stored) return JSON.parse(stored);

                    return null;
                }"""
                )

                if kambi_data:
                    if isinstance(kambi_data, dict) and "events" in kambi_data:
                        events.extend(kambi_data["events"])
            except:
                pass

            # Process intercepted API responses
            for response in self.api_responses:
                if "data" in response and isinstance(response["data"], dict):
                    if "events" in response["data"]:
                        events.extend(response["data"]["events"])
                    elif "liveEvents" in response["data"]:
                        events.extend(response["data"]["liveEvents"])

            if events:
                self.events_captured = len(events)
                self.last_scrape_success = datetime.utcnow()

                return {
                    "source": "unibet_cdp",
                    "timestamp": datetime.utcnow().isoformat(),
                    "events": events,
                    "market": "US-PA",
                    "status": "success",
                    "fallback_mode": True,
                }

            return None

        except Exception as e:
            print(f"[UB_CDP] Scraping error: {e}")
            return None

    def check_fallback_signal(self) -> bool:
        """Check if HTTP collector has signaled for fallback"""
        signal = self.redis_client.get(FALLBACK_SIGNAL_KEY)
        return signal is not None

    async def run(self):
        """Main collection loop"""
        print("[UB_CDP] Starting Unibet CDP collector in standby mode")
        print(f"[UB_CDP] URL: {UB_URL}")
        print(f"[UB_CDP] Publishing to: {PUBLISH_CHANNEL}")

        # Health check endpoint
        from aiohttp import web

        async def health_handler(request):
            status = {
                "status": "active" if self.is_active else "standby",
                "events_captured": self.events_captured,
                "last_success": (
                    self.last_scrape_success.isoformat()
                    if self.last_scrape_success
                    else None
                ),
            }
            return web.json_response(status)

        app = web.Application()
        app.router.add_get("/healthz", health_handler)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", HEALTHZ_PORT)
        asyncio.create_task(site.start())

        while True:
            try:
                # Check if we should activate
                if not self.is_active and self.check_fallback_signal():
                    print("[UB_CDP] Fallback signal detected, activating CDP collector")
                    self.is_active = True
                    await self.setup_browser()

                # Scrape if active
                if self.is_active:
                    data = await self.scrape_unibet()
                    if data:
                        # Publish to Redis
                        self.redis_client.publish(PUBLISH_CHANNEL, json.dumps(data))
                        print(
                            f"[UB_CDP] Published {len(data['events'])} events via CDP"
                        )

                    # Check if HTTP collector is back online
                    signal = self.redis_client.get(FALLBACK_SIGNAL_KEY)
                    if not signal:
                        print("[UB_CDP] HTTP collector back online, going to standby")
                        self.is_active = False
                        if self.browser:
                            await self.browser.close()
                            self.browser = None

                await asyncio.sleep(SCRAPE_INTERVAL if self.is_active else 30)

            except KeyboardInterrupt:
                print("[UB_CDP] Shutting down")
                break
            except Exception as e:
                print(f"[UB_CDP] Unexpected error: {e}")
                await asyncio.sleep(SCRAPE_INTERVAL)

        if self.browser:
            await self.browser.close()


if __name__ == "__main__":
    collector = UBCDPCollector()
    asyncio.run(collector.run())

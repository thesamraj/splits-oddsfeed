#!/usr/bin/env python3
import asyncio
import json
import time
import redis
import os
from playwright.async_api import async_playwright

REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.fanduel")
URL = os.getenv("URL", "https://sportsbook.fanduel.com/")


class FanDuelCollector:
    def __init__(self):
        self.redis = redis.from_url(REDIS_URL)

    async def collect(self):
        """Main collection method"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
            )

            page = await context.new_page()

            # Set up network interception
            api_responses = []

            async def capture_response(response):
                url = response.url
                if response.status == 200 and any(
                    p in url for p in ["api", "cache", "sbk", "event", "odds", "market"]
                ):
                    try:
                        body = await response.body()
                        text = body.decode("utf-8", errors="ignore")
                        if len(text) > 100:  # Skip small responses
                            api_responses.append(
                                {"url": url, "size": len(text), "sample": text[:200]}
                            )
                            print(f"Captured API: {url[-50:]} ({len(text)} bytes)")
                    except Exception:
                        pass

            page.on("response", capture_response)

            print(f"Loading {URL}")
            try:
                await page.goto(URL, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(5000)
            except Exception as e:
                print(f"Navigation error: {e}")

            # Extract from DOM
            print("Extracting from DOM...")
            dom_data = await page.evaluate(
                """() => {
                const result = {
                    odds: [],
                    events: [],
                    markets: []
                };

                // Find American odds format
                const oddsPattern = /[+-]\\d{3,4}/g;
                const allText = document.body.innerText;
                const matches = allText.match(oddsPattern);
                if (matches) {
                    result.odds = [...new Set(matches)];  // Unique odds
                }

                // Look for event titles (usually h3, h4, or specific classes)
                document.querySelectorAll('h3, h4, [data-test-id*="event"], [class*="event-name"]').forEach(el => {
                    const text = el.textContent.trim();
                    if (text && text.length > 5 && text.length < 100) {
                        result.events.push(text);
                    }
                });

                // Look for market types
                document.querySelectorAll('[data-test-id*="market"], [class*="market-type"]').forEach(el => {
                    const text = el.textContent.trim();
                    if (text && text.length > 2 && text.length < 50) {
                        result.markets.push(text);
                    }
                });

                return result;
            }"""
            )

            await browser.close()

            # Prepare data
            all_odds = []
            if dom_data.get("odds"):
                all_odds = [
                    {"type": "american", "value": odd} for odd in dom_data["odds"][:100]
                ]

            # Publish results
            payload = {
                "source": "fanduel",
                "timestamp": time.time(),
                "raw_odds": all_odds,
                "events": dom_data.get("events", [])[:20],
                "markets": dom_data.get("markets", [])[:20],
                "api_endpoints": len(api_responses),
                "api_samples": [
                    {"url": r["url"][-80:], "size": r["size"]}
                    for r in api_responses[:5]
                ],
            }

            self.redis.publish(CHANNEL, json.dumps(payload))
            print(
                f"Published: {len(all_odds)} odds, {len(dom_data.get('events', []))} events, {len(api_responses)} APIs captured"
            )

            return len(all_odds) > 0 or len(api_responses) > 0

    async def run(self):
        """Run continuous collection"""
        consecutive_failures = 0

        while True:
            try:
                print(f"\n{'='*50}")
                print(f"Collection cycle: {time.strftime('%H:%M:%S')}")
                success = await self.collect()

                if success:
                    consecutive_failures = 0
                    await asyncio.sleep(10)
                else:
                    consecutive_failures += 1
                    wait_time = min(60, 10 * consecutive_failures)
                    print(f"No data collected, waiting {wait_time}s")
                    await asyncio.sleep(wait_time)

            except Exception as e:
                print(f"Error: {e}")
                import traceback

                traceback.print_exc()
                await asyncio.sleep(30)


if __name__ == "__main__":
    print("Starting FanDuel Simple Collector...")
    collector = FanDuelCollector()
    asyncio.run(collector.run())

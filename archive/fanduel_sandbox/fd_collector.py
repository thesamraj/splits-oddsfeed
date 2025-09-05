#!/usr/bin/env python3
import asyncio
import json
import time
import redis
import os
from playwright.async_api import async_playwright
from fd_extractor import extract_odds_from_html, parse_fanduel_json

REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.fanduel")
URL = os.getenv("URL", "https://sportsbook.fanduel.com/")


class FanDuelCollector:
    def __init__(self):
        self.redis = redis.from_url(REDIS_URL)
        self.collected_odds = []
        self.api_endpoints = set()

    async def collect(self):
        """Main collection method"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-accelerated-2d-canvas",
                    "--no-first-run",
                    "--no-zygote",
                    "--single-process",
                    "--disable-gpu",
                ],
            )

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
            )

            page = await context.new_page()

            # Set up network interception
            responses_captured = []

            async def capture_response(response):
                if response.status == 200:
                    url = response.url
                    if any(
                        pattern in url
                        for pattern in ["api", "cache", "data", "events", "odds"]
                    ):
                        try:
                            body = await response.body()
                            responses_captured.append(
                                {
                                    "url": url,
                                    "data": body.decode("utf-8", errors="ignore")[
                                        :10000
                                    ],
                                }
                            )
                        except:
                            pass

            page.on("response", capture_response)

            print(f"Loading {URL}")
            await page.goto(URL, wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)

            # Get page content
            html = await page.content()

            # Extract from HTML
            html_data = extract_odds_from_html(html)

            # Extract from DOM using JavaScript
            dom_data = await page.evaluate(
                """() => {
                const result = {
                    odds: [],
                    events: []
                };

                // Find all text nodes containing odds patterns
                const walker = document.createTreeWalker(
                    document.body,
                    NodeFilter.SHOW_TEXT,
                    null,
                    false
                );

                let node;
                const oddsPattern = /[+-]\\d{3,4}/g;
                while (node = walker.nextNode()) {
                    const text = node.textContent;
                    const matches = text.match(oddsPattern);
                    if (matches) {
                        result.odds.push(...matches);
                    }
                }

                // Look for structured data
                const scripts = document.querySelectorAll('script[type="application/json"], script[type="application/ld+json"]');
                scripts.forEach(script => {
                    try {
                        const data = JSON.parse(script.textContent);
                        result.events.push(data);
                    } catch {}
                });

                return result;
            }"""
            )

            await browser.close()

            # Process captured responses
            for resp in responses_captured:
                try:
                    data = json.loads(resp["data"])
                    events = parse_fanduel_json(data)
                    if events:
                        self.collected_odds.extend(events)
                except:
                    # Try to extract odds from raw text
                    if (
                        "odds" in resp["data"].lower()
                        or "price" in resp["data"].lower()
                    ):
                        self.api_endpoints.add(resp["url"])

            # Combine all extracted data
            all_odds = []

            # From HTML extraction
            if html_data["american_odds"]:
                all_odds.extend(
                    [
                        {"type": "american", "value": odd}
                        for odd in html_data["american_odds"]
                    ]
                )

            # From DOM extraction
            if dom_data.get("odds"):
                all_odds.extend(
                    [{"type": "dom", "value": odd} for odd in dom_data["odds"]]
                )

            # Publish to Redis
            if all_odds or self.collected_odds:
                payload = {
                    "source": "fanduel",
                    "timestamp": time.time(),
                    "raw_odds": all_odds[:200],  # Limit to 200 odds
                    "events": self.collected_odds[:50],  # Limit to 50 events
                    "api_count": len(self.api_endpoints),
                }

                self.redis.publish(CHANNEL, json.dumps(payload))
                print(
                    f"Published: {len(all_odds)} odds, {len(self.collected_odds)} events"
                )
                return True

            return False

    async def run(self):
        """Run continuous collection"""
        consecutive_failures = 0

        while True:
            try:
                print(f"\nCollection cycle: {time.strftime('%H:%M:%S')}")
                success = await self.collect()

                if success:
                    consecutive_failures = 0
                    await asyncio.sleep(10)  # Fast refresh for sub-1s latency
                else:
                    consecutive_failures += 1
                    wait_time = min(60, 10 * consecutive_failures)
                    print(f"No data collected, waiting {wait_time}s")
                    await asyncio.sleep(wait_time)

            except Exception as e:
                print(f"Error: {e}")
                await asyncio.sleep(30)


if __name__ == "__main__":
    collector = FanDuelCollector()
    asyncio.run(collector.run())

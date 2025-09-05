#!/usr/bin/env python3
import asyncio
import json
import time
import redis
import re
from playwright.async_api import async_playwright

REDIS_URL = "redis://broker:6379/0"
CHANNEL = "odds.raw.fanduel"

async def extract_fanduel_odds():
    """Use real browser to extract FanDuel odds"""

    async with async_playwright() as p:
        # Launch browser with stealth settings
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process'
            ]
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080},
            locale='en-US',
            timezone_id='America/New_York'
        )

        # Add stealth scripts
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            window.chrome = {runtime: {}};
            Object.defineProperty(navigator, 'permissions', {
                get: () => ({
                    query: () => Promise.resolve({state: 'granted'})
                })
            });
        """)

        page = await context.new_page()

        # Intercept API responses
        intercepted_data = []

        async def handle_response(response):
            url = response.url
            if response.status == 200 and any(x in url for x in ['api', 'cache', 'psevent', 'psmg']):
                try:
                    body = await response.body()
                    text = body.decode('utf-8', errors='ignore')

                    # Check if it contains odds data
                    if len(text) > 100 and any(term in text.lower() for term in ['odds', 'price', 'spread', 'total', 'moneyline']):
                        print(f"✓ Intercepted: {url[:100]}")
                        intercepted_data.append({
                            'url': url,
                            'data': text[:5000]  # First 5KB
                        })
                except:
                    pass

        page.on('response', handle_response)

        print("Loading FanDuel main page...")
        await page.goto('https://sportsbook.fanduel.com/', wait_until='domcontentloaded')
        await page.wait_for_timeout(5000)

        # Try to navigate to NFL section
        print("Looking for NFL section...")

        # Multiple strategies to get to NFL odds
        strategies = [
            # Click NFL link
            async def(): await page.click('text=/NFL/i', timeout=3000),
            # Navigate directly
            async def(): await page.goto('https://sportsbook.fanduel.com/football', wait_until='networkidle'),
            # Click on Football then NFL
            async def(): (
                await page.click('text=/Football/i', timeout=3000),
                await page.wait_for_timeout(2000),
                await page.click('text=/NFL/i', timeout=3000)
            ),
        ]

        for strategy in strategies:
            try:
                await strategy()
                print("✓ Navigation successful")
                break
            except:
                continue

        await page.wait_for_timeout(5000)

        # Extract odds from the DOM
        print("Extracting odds from DOM...")

        odds_data = await page.evaluate('''() => {
            const events = [];

            // Strategy 1: Look for data attributes
            document.querySelectorAll('[data-event], [data-test*="event"], [data-id*="event"]').forEach(el => {
                const text = el.innerText || '';
                // Look for odds patterns
                const odds = text.match(/[+-]\\d{3,4}/g);
                if (odds && odds.length > 0) {
                    events.push({
                        html: el.outerHTML.substring(0, 500),
                        odds: odds
                    });
                }
            });

            // Strategy 2: Look for common class patterns
            const selectors = [
                '[class*="event"]',
                '[class*="Event"]',
                '[class*="match"]',
                '[class*="Match"]',
                '[class*="odds"]',
                '[class*="Odds"]'
            ];

            selectors.forEach(selector => {
                document.querySelectorAll(selector).forEach(el => {
                    const text = el.innerText || '';
                    const odds = text.match(/[+-]\\d{3,4}/g);
                    if (odds && odds.length >= 2) {
                        const teams = text.match(/([A-Z][a-z]+ [A-Z][a-z]+)/g);
                        if (teams && teams.length >= 2) {
                            events.push({
                                teams: teams.slice(0, 2),
                                odds: odds,
                                text: text.substring(0, 200)
                            });
                        }
                    }
                });
            });

            // Strategy 3: Extract all visible odds
            const allOdds = [];
            document.querySelectorAll('*').forEach(el => {
                const text = el.innerText || '';
                const matches = text.match(/[+-]\\d{3,4}/g);
                if (matches) {
                    allOdds.push(...matches);
                }
            });

            return {
                events: events.slice(0, 20),
                totalOdds: allOdds.slice(0, 100),
                intercepted: window.__intercepted || []
            };
        }''')

        await browser.close()

        # Process results
        print(f"\nResults:")
        print(f"- Events found: {len(odds_data.get('events', []))}")
        print(f"- Total odds found: {len(odds_data.get('totalOdds', []))}")
        print(f"- Intercepted APIs: {len(intercepted_data)}")

        if odds_data.get('events'):
            print("\nSample events:")
            for event in odds_data['events'][:3]:
                print(f"  {event}")

        if odds_data.get('totalOdds'):
            print(f"\nSample odds: {odds_data['totalOdds'][:20]}")

        # Publish to Redis
        if odds_data.get('totalOdds') or odds_data.get('events'):
            try:
                r = redis.from_url(REDIS_URL)
                payload = {
                    'source': 'fanduel',
                    'timestamp': time.time(),
                    'events': odds_data.get('events', []),
                    'odds': odds_data.get('totalOdds', [])
                }
                r.publish(CHANNEL, json.dumps(payload))
                print(f"\n✓ Published to Redis channel: {CHANNEL}")
            except Exception as e:
                print(f"Redis error: {e}")

        return odds_data, intercepted_data

async def continuous_collection():
    """Run continuous collection"""
    while True:
        try:
            print(f"\n{'='*50}")
            print(f"Collection cycle started at {time.strftime('%Y-%m-%d %H:%M:%S')}")

            odds, apis = await extract_fanduel_odds()

            if odds.get('totalOdds'):
                print(f"✓ Successfully collected {len(odds['totalOdds'])} odds")
            else:
                print("⚠ No odds collected, retrying...")

            # Wait before next collection
            await asyncio.sleep(30)

        except Exception as e:
            print(f"Error in collection: {e}")
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(continuous_collection())

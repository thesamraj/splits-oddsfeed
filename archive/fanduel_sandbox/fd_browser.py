import asyncio
import json
from playwright.async_api import async_playwright

# FanDuel NFL page - public, no login required
URL = "https://sportsbook.fanduel.com/football/nfl"
REDIS_URL = "redis://broker:6379/0"
CHANNEL = "odds.raw.fanduel"


async def extract_odds_from_page(page):
    """Extract odds data from the FanDuel page"""
    odds_data = []

    # Wait for odds to load
    await page.wait_for_selector('[data-test-id*="Event"]', timeout=10000)

    # Extract using JavaScript in the browser context
    odds_data = await page.evaluate(
        """() => {
        const events = [];

        // Try multiple selectors that FanDuel might use
        const eventElements = document.querySelectorAll('[data-test-id*="Event"], [class*="event-card"], [class*="EventCard"]');

        eventElements.forEach(el => {
            const event = {};

            // Extract team names
            const teamElements = el.querySelectorAll('[class*="team"], [class*="competitor"], [data-test*="team"]');
            if (teamElements.length >= 2) {
                event.home = teamElements[0].textContent.trim();
                event.away = teamElements[1].textContent.trim();
            }

            // Extract odds
            const oddsElements = el.querySelectorAll('[class*="odds"], [class*="price"], [data-test*="odds"]');
            event.odds = [];
            oddsElements.forEach(odd => {
                const text = odd.textContent.trim();
                if (text.match(/[+-]\\d+/)) {
                    event.odds.push(text);
                }
            });

            if (event.home && event.away && event.odds.length > 0) {
                events.push(event);
            }
        });

        return events;
    }"""
    )

    return odds_data


async def intercept_network(page):
    """Intercept network requests to find API calls"""
    api_calls = []

    async def handle_response(response):
        url = response.url
        if "api" in url or "cache" in url or "feed" in url:
            if response.status == 200:
                try:
                    content_type = response.headers.get("content-type", "")
                    if "json" in content_type:
                        body = await response.text()
                        if len(body) > 100 and (
                            "odds" in body.lower() or "price" in body.lower()
                        ):
                            print(f"Found API: {url[:100]}")
                            api_calls.append({"url": url, "data": json.loads(body)})
                except:
                    pass

    page.on("response", handle_response)
    return api_calls


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )

        page = await context.new_page()

        # Set up network interception
        api_calls = await intercept_network(page)

        print(f"Loading {URL}...")
        await page.goto(URL, wait_until="networkidle")

        # Wait for content to load
        await page.wait_for_timeout(5000)

        # Extract odds from page
        odds_data = await extract_odds_from_page(page)

        print(f"Extracted {len(odds_data)} events from page")
        for event in odds_data[:3]:
            print(f"  {event}")

        # Check intercepted API calls
        print(f"\nIntercepted {len(api_calls)} API calls")

        await browser.close()

        return odds_data, api_calls


if __name__ == "__main__":
    data, apis = asyncio.run(main())
    print(f"\nTotal: {len(data)} events, {len(apis)} API calls")

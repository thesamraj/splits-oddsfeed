#!/usr/bin/env python3
"""
DraftKings Browser Automation Collector V2
Simplified version that focuses on the main live page
"""

import os
import json
import redis
import asyncio
import logging
from datetime import datetime
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("draftkings_browser")

# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.draftkings")
INTERVAL = int(os.getenv("INTERVAL", "60"))
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

r = redis.from_url(REDIS_URL)


class DraftKingsBrowserCollector:
    def __init__(self):
        self.browser = None
        self.page = None
        self.stats = {"events_processed": 0, "odds_published": 0, "errors": 0}

    async def setup_browser(self):
        """Initialize browser with anti-detection measures"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-gpu",
                "--disable-web-security",
            ],
        )

        context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            bypass_csp=True,
        )

        self.page = await context.new_page()

        # Remove automation indicators
        await self.page.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        """
        )

    async def extract_live_odds(self):
        """Extract odds from the main DraftKings page"""
        odds_data = []

        try:
            logger.info("Navigating to DraftKings...")
            # Go to main page first
            await self.page.goto(
                "https://sportsbook.draftkings.com/",
                wait_until="domcontentloaded",
                timeout=15000,
            )
            await asyncio.sleep(3)

            # Try to find and click "LIVE NOW" or similar
            try:
                live_selectors = [
                    'text="LIVE NOW"',
                    'text="LIVE"',
                    'text="In-Play"',
                    '[aria-label*="live"]',
                    'a[href*="/live"]',
                ]

                for selector in live_selectors:
                    try:
                        live_element = await self.page.query_selector(selector)
                        if live_element:
                            await live_element.click()
                            await asyncio.sleep(3)
                            break
                    except:
                        continue
            except Exception as e:
                logger.debug(f"Could not find live section: {e}")

            # Extract any visible events (whether live or upcoming)
            event_selectors = [
                ".sportsbook-event-accordion__wrapper",
                '[data-test*="event"]',
                ".event-cell",
                'div[class*="event-card"]',
                'article[class*="event"]',
            ]

            events_found = False
            for selector in event_selectors:
                try:
                    await self.page.wait_for_selector(selector, timeout=3000)
                    events = await self.page.query_selector_all(selector)

                    if events:
                        logger.info(
                            f"Found {len(events)} events using selector: {selector}"
                        )
                        events_found = True

                        for event in events[
                            :10
                        ]:  # Process max 10 events to avoid timeout
                            try:
                                event_data = await self.page.evaluate(
                                    """(element) => {
                                    const getText = (el, selector) => {
                                        const found = el.querySelector(selector);
                                        return found ? found.textContent.trim() : '';
                                    };

                                    // Multiple strategies to find team names
                                    let teams = [];

                                    // Try common team selectors
                                    const teamSelectors = [
                                        '.event-cell__name-text',
                                        '.sportsbook-event-accordion__title-text',
                                        'span[class*="team"]',
                                        'div[class*="competitor"]'
                                    ];

                                    for (const selector of teamSelectors) {
                                        const found = element.querySelectorAll(selector);
                                        if (found.length >= 2) {
                                            teams = Array.from(found).slice(0, 2).map(t => t.textContent.trim());
                                            break;
                                        }
                                    }

                                    // Extract odds
                                    const oddsElements = element.querySelectorAll('.sportsbook-odds-cell__line, [class*="odds"], [class*="price"]');
                                    const odds = [];

                                    oddsElements.forEach(el => {
                                        const text = el.textContent.trim();
                                        if (text && text !== '—' && (text.includes('+') || text.includes('-'))) {
                                            odds.push(text);
                                        }
                                    });

                                    return {
                                        teams: teams,
                                        odds: odds,
                                        html: element.innerHTML.substring(0, 200)  // For debugging
                                    };
                                }""",
                                    event,
                                )

                                if (
                                    event_data["teams"].length >= 2
                                    and event_data["odds"].length > 0
                                ):
                                    event_id = f"dk_{event_data['teams'][0]}_{event_data['teams'][1]}".replace(
                                        " ", "_"
                                    )

                                    for i, odd in enumerate(
                                        event_data["odds"][:6]
                                    ):  # Max 6 odds per event
                                        odds_data.append(
                                            {
                                                "event_id": event_id,
                                                "sport": "UNKNOWN",
                                                "home_team": event_data["teams"][1],
                                                "away_team": event_data["teams"][0],
                                                "market": (
                                                    "moneyline"
                                                    if i < 2
                                                    else "spread" if i < 4 else "total"
                                                ),
                                                "price": odd,
                                                "timestamp": datetime.utcnow().isoformat(),
                                            }
                                        )

                            except Exception as e:
                                logger.debug(f"Error processing event: {e}")
                                continue

                        if odds_data:
                            break  # Found data, stop trying other selectors

                except Exception as e:
                    logger.debug(f"Selector {selector} failed: {e}")
                    continue

            if not events_found:
                logger.warning("No events found on page")
                # Log page title for debugging
                title = await self.page.title()
                logger.info(f"Page title: {title}")

        except Exception as e:
            logger.error(f"Error extracting odds: {e}")
            self.stats["errors"] += 1

        return odds_data

    async def run(self):
        """Main collection loop"""
        logger.info("Starting DraftKings browser collector V2")

        await self.setup_browser()

        while True:
            try:
                # Extract odds
                odds = await self.extract_live_odds()

                if odds:
                    # Publish to Redis
                    message = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "source": "draftkings_browser",
                        "events": odds,
                    }

                    r.publish(CHANNEL, json.dumps(message))
                    self.stats["odds_published"] += len(odds)
                    logger.info(f"Published {len(odds)} odds")
                else:
                    logger.info("No odds found")

                self.stats["events_processed"] += 1

            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                self.stats["errors"] += 1

                # Restart browser on error
                if self.browser:
                    await self.browser.close()
                await self.setup_browser()

            # Wait before next collection
            await asyncio.sleep(INTERVAL)


async def main():
    collector = DraftKingsBrowserCollector()
    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())

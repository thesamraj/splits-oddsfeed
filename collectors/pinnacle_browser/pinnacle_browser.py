#!/usr/bin/env python3
"""
BetMGM Browser Automation Collector
Scrapes odds from BetMGM website without API credentials
"""

import os
import json
import redis
import asyncio
import logging
from datetime import datetime
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("betmgm_browser")

# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.betmgm")
INTERVAL = int(os.getenv("INTERVAL", "60"))
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

r = redis.from_url(REDIS_URL)


class BetMGMBrowserCollector:
    def __init__(self):
        self.browser = None
        self.page = None

    async def setup_browser(self):
        """Initialize browser with anti-detection"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ],
        )

        context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )

        self.page = await context.new_page()
        await self.page.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        """
        )

    async def extract_odds(self):
        """Extract odds from BetMGM"""
        odds_data = []

        try:
            logger.info("Navigating to BetMGM...")
            await self.page.goto(
                "https://sports.betmgm.com/en/sports",
                wait_until="domcontentloaded",
                timeout=15000,
            )
            await asyncio.sleep(3)

            # Try to find live betting section
            try:
                live_link = await self.page.query_selector('a[href*="live"]')
                if live_link:
                    await live_link.click()
                    await asyncio.sleep(2)
            except:
                pass

            # Extract events
            events = await self.page.query_selector_all(".event-fixture")

            for event in events[:10]:
                try:
                    event_data = await self.page.evaluate(
                        """(element) => {
                        const teams = element.querySelectorAll('.participant');
                        const odds = element.querySelectorAll('.option-value');

                        return {
                            teams: Array.from(teams).map(t => t.textContent.trim()),
                            odds: Array.from(odds).map(o => o.textContent.trim())
                        };
                    }""",
                        event,
                    )

                    if len(event_data["teams"]) >= 2:
                        for i, odd in enumerate(event_data["odds"][:3]):
                            odds_data.append(
                                {
                                    "event_id": f"mgm_{event_data['teams'][0]}_{event_data['teams'][1]}".replace(
                                        " ", "_"
                                    ),
                                    "sport": "UNKNOWN",
                                    "home_team": event_data["teams"][1],
                                    "away_team": event_data["teams"][0],
                                    "market": "moneyline",
                                    "price": odd,
                                    "timestamp": datetime.utcnow().isoformat(),
                                }
                            )
                except:
                    continue

        except Exception as e:
            logger.error(f"Error extracting odds: {e}")

        return odds_data

    async def run(self):
        """Main loop"""
        logger.info("Starting BetMGM browser collector")
        await self.setup_browser()

        while True:
            try:
                odds = await self.extract_odds()

                if odds:
                    message = {
                        "book": "betmgm",
                        "timestamp": datetime.utcnow().isoformat(),
                        "events": odds,
                    }
                    r.publish(CHANNEL, json.dumps(message))
                    logger.info(f"Published {len(odds)} odds")
                else:
                    logger.info("No odds found")

            except Exception as e:
                logger.error(f"Error: {e}")
                if self.browser:
                    await self.browser.close()
                await self.setup_browser()

            await asyncio.sleep(INTERVAL)


if __name__ == "__main__":
    collector = BetMGMBrowserCollector()
    asyncio.run(collector.run())

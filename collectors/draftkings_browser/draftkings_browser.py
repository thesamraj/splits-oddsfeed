#!/usr/bin/env python3
"""
DraftKings Browser Automation Collector
Uses Playwright to scrape real-time odds from DraftKings website
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
            ],
        )

        context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

        self.page = await context.new_page()

        # Remove automation indicators
        await self.page.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        """
        )

    async def navigate_to_live(self):
        """Navigate to DraftKings live betting page"""
        try:
            logger.info("Navigating to DraftKings live betting...")
            # Use domcontentloaded instead of networkidle for faster loading
            await self.page.goto(
                "https://sportsbook.draftkings.com/live",
                wait_until="domcontentloaded",
                timeout=15000,
            )
            await asyncio.sleep(5)  # Wait for dynamic content to load

            # Check if we need to handle any popups or cookie banners
            try:
                close_button = await self.page.query_selector(
                    'button[aria-label*="close"]'
                )
                if close_button:
                    await close_button.click()
            except:
                pass

            return True
        except Exception as e:
            logger.error(f"Navigation error: {e}")
            # Try alternative URL
            try:
                await self.page.goto(
                    "https://sportsbook.draftkings.com/",
                    wait_until="domcontentloaded",
                    timeout=15000,
                )
                await asyncio.sleep(3)
                return True
            except:
                return False

    async def extract_odds(self):
        """Extract odds from the page"""
        odds_data = []

        try:
            # Wait for odds to load
            await self.page.wait_for_selector(
                ".sportsbook-event-accordion__wrapper", timeout=10000
            )

            # Extract all event cards
            events = await self.page.query_selector_all(
                ".sportsbook-event-accordion__wrapper"
            )

            for event in events:
                try:
                    # Extract event details
                    event_data = await self.page.evaluate(
                        """(element) => {
                        const getTextContent = (selector) => {
                            const el = element.querySelector(selector);
                            return el ? el.textContent.trim() : '';
                        };

                        // Extract teams
                        const teams = element.querySelectorAll('.event-cell__name-text');
                        const awayTeam = teams[0] ? teams[0].textContent.trim() : '';
                        const homeTeam = teams[1] ? teams[1].textContent.trim() : '';

                        // Extract odds
                        const oddsElements = element.querySelectorAll('.sportsbook-odds-cell');
                        const odds = [];

                        oddsElements.forEach(el => {
                            const american = el.querySelector('.sportsbook-odds-cell__line');
                            const label = el.querySelector('.sportsbook-odds-cell__label');
                            if (american) {
                                odds.push({
                                    price: american.textContent.trim(),
                                    label: label ? label.textContent.trim() : ''
                                });
                            }
                        });

                        return {
                            homeTeam,
                            awayTeam,
                            odds
                        };
                    }""",
                        event,
                    )

                    if event_data["homeTeam"] and event_data["awayTeam"]:
                        # Format for our system
                        event_id = f"dk_{event_data['homeTeam']}_{event_data['awayTeam']}".replace(
                            " ", "_"
                        )

                        for odd in event_data["odds"]:
                            if odd["price"] and odd["price"] != "—":
                                odds_data.append(
                                    {
                                        "event_id": event_id,
                                        "sport": "UNKNOWN",  # Will be determined from page context
                                        "home_team": event_data["homeTeam"],
                                        "away_team": event_data["awayTeam"],
                                        "market": (
                                            "moneyline"
                                            if not odd["label"]
                                            else odd["label"].lower()
                                        ),
                                        "price": odd["price"],
                                        "timestamp": datetime.utcnow().isoformat(),
                                    }
                                )

                except Exception as e:
                    logger.debug(f"Error processing event: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error extracting odds: {e}")
            self.stats["errors"] += 1

        return odds_data

    async def collect_sports(self):
        """Collect odds from all major sports"""
        all_odds = []

        sports = ["NFL", "NBA", "MLB", "NHL", "NCAAF", "NCAAB"]

        for sport in sports:
            try:
                # Navigate to sport-specific live page
                sport_url = f"https://sportsbook.draftkings.com/leagues/{sport.lower()}"
                await self.page.goto(sport_url, wait_until="networkidle", timeout=20000)
                await asyncio.sleep(2)

                # Click on "LIVE" tab if available
                try:
                    live_tab = await self.page.query_selector('text="LIVE"')
                    if live_tab:
                        await live_tab.click()
                        await asyncio.sleep(2)
                except:
                    pass

                # Extract odds
                sport_odds = await self.extract_odds()

                # Add sport to each odd
                for odd in sport_odds:
                    odd["sport"] = sport

                all_odds.extend(sport_odds)
                logger.info(f"{sport}: {len(sport_odds)} odds extracted")

            except Exception as e:
                logger.error(f"Error collecting {sport}: {e}")
                continue

        return all_odds

    async def run(self):
        """Main collection loop"""
        logger.info("Starting DraftKings browser collector")

        await self.setup_browser()

        while True:
            try:
                # Navigate to live betting
                if await self.navigate_to_live():
                    # Collect odds from all sports
                    odds = await self.collect_sports()

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
                        logger.info("No live odds found")

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

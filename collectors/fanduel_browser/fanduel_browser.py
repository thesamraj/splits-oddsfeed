#!/usr/bin/env python3
"""
FanDuel Browser Automation Collector
Uses Playwright to scrape real-time odds from FanDuel website
"""

import os
import json
import redis
import asyncio
import logging
from datetime import datetime
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fanduel_browser")

# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.fanduel")
INTERVAL = int(os.getenv("INTERVAL", "60"))
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"

r = redis.from_url(REDIS_URL)


class FanDuelBrowserCollector:
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
        """Navigate to FanDuel live betting page"""
        try:
            logger.info("Navigating to FanDuel live betting...")
            await self.page.goto(
                "https://sportsbook.fanduel.com/live",
                wait_until="networkidle",
                timeout=30000,
            )
            await asyncio.sleep(3)  # Wait for dynamic content
            return True
        except Exception as e:
            logger.error(f"Navigation error: {e}")
            return False

    async def extract_odds(self):
        """Extract odds from the page"""
        odds_data = []

        try:
            # More flexible selector - try multiple options
            selectors = [
                '[role="group"]',
                'div[data-test-id*="event"]',
                'div[class*="event-card"]',
                'div[class*="EventCard"]',
                "article",
            ]

            element_found = False
            for selector in selectors:
                try:
                    await self.page.wait_for_selector(selector, timeout=3000)
                    element_found = True
                    break
                except:
                    continue

            if not element_found:
                logger.debug("No event elements found on page")
                return odds_data

            # Extract all event groups
            events = await self.page.query_selector_all('[role="group"]')

            for event in events:
                try:
                    # Extract event details using JavaScript
                    event_data = await self.page.evaluate(
                        """(element) => {
                        // Helper function
                        const getTextContent = (selector) => {
                            const el = element.querySelector(selector);
                            return el ? el.textContent.trim() : '';
                        };

                        // Extract team names - FanDuel specific selectors
                        const teamElements = element.querySelectorAll('span[role="text"]');
                        let homeTeam = '';
                        let awayTeam = '';

                        // Parse team names from various possible locations
                        teamElements.forEach((el, idx) => {
                            const text = el.textContent.trim();
                            if (text && !text.includes('+') && !text.includes('-') && !text.includes('.')) {
                                if (!awayTeam) awayTeam = text;
                                else if (!homeTeam) homeTeam = text;
                            }
                        });

                        // Extract odds buttons
                        const oddsButtons = element.querySelectorAll('button[aria-label*="odds"]');
                        const odds = [];

                        oddsButtons.forEach(button => {
                            const ariaLabel = button.getAttribute('aria-label') || '';
                            const priceText = button.textContent.trim();

                            if (priceText && priceText !== '—') {
                                odds.push({
                                    price: priceText,
                                    label: ariaLabel
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
                        event_id = f"fd_{event_data['homeTeam']}_{event_data['awayTeam']}".replace(
                            " ", "_"
                        )

                        for odd in event_data["odds"]:
                            if odd["price"]:
                                # Determine market type from label
                                market = "moneyline"
                                if "spread" in odd["label"].lower():
                                    market = "spread"
                                elif (
                                    "total" in odd["label"].lower()
                                    or "over" in odd["label"].lower()
                                    or "under" in odd["label"].lower()
                                ):
                                    market = "total"

                                odds_data.append(
                                    {
                                        "event_id": event_id,
                                        "sport": "UNKNOWN",  # Will be determined from page context
                                        "home_team": event_data["homeTeam"],
                                        "away_team": event_data["awayTeam"],
                                        "market": market,
                                        "price": odd["price"],
                                        "label": odd["label"],
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

        # FanDuel sports navigation
        sports_urls = {
            "NFL": "https://sportsbook.fanduel.com/football/nfl",
            "NBA": "https://sportsbook.fanduel.com/basketball/nba",
            "MLB": "https://sportsbook.fanduel.com/baseball/mlb",
            "NHL": "https://sportsbook.fanduel.com/ice-hockey/nhl",
            "NCAAF": "https://sportsbook.fanduel.com/football/ncaaf",
            "NCAAB": "https://sportsbook.fanduel.com/basketball/ncaab",
        }

        for sport, url in sports_urls.items():
            try:
                # Navigate to sport page
                await self.page.goto(url, wait_until="networkidle", timeout=20000)
                await asyncio.sleep(2)

                # Try to click on "LIVE" filter
                try:
                    live_button = await self.page.query_selector(
                        'button:has-text("LIVE")'
                    )
                    if not live_button:
                        live_button = await self.page.query_selector(
                            'a:has-text("LIVE")'
                        )
                    if live_button:
                        await live_button.click()
                        await asyncio.sleep(2)
                except:
                    logger.debug(f"No LIVE button found for {sport}")

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
        logger.info("Starting FanDuel browser collector")

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
                            "source": "fanduel_browser",
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
    collector = FanDuelBrowserCollector()
    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())

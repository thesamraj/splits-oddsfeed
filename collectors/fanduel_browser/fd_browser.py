#!/usr/bin/env python3
"""
FanDuel Browser Collector - Real odds via Playwright
Bypasses API restrictions to get real live odds
"""

import os
import json
import time
import redis
import asyncio
import logging
from datetime import datetime
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fd_browser")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
INTERVAL = int(os.getenv("INTERVAL", 60))


class FanDuelBrowserCollector:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.browser = None
        self.context = None

    async def init_browser(self):
        """Initialize headless browser"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(
            headless=True, args=["--disable-blink-features=AutomationControlled"]
        )
        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )

    async def fetch_odds(self):
        """Fetch odds from FanDuel website"""
        events = []

        try:
            page = await self.context.new_page()

            # Navigate to NFL page
            await page.goto(
                "https://sportsbook.fanduel.com/navigation/nfl",
                wait_until="networkidle",
            )
            await page.wait_for_timeout(3000)

            # Extract data from page
            games_data = await page.evaluate(
                """
                () => {
                    const games = [];

                    // Find all game containers
                    const gameContainers = document.querySelectorAll('[data-testid*="MarketContainer"], .market-container, [role="group"]');

                    gameContainers.forEach(container => {
                        try {
                            // Look for team names
                            const teamSpans = container.querySelectorAll('span[title], .competitor-name');

                            if (teamSpans.length >= 2) {
                                const awayTeam = teamSpans[0].textContent.trim();
                                const homeTeam = teamSpans[1].textContent.trim();

                                // Look for odds
                                const oddsElements = container.querySelectorAll('[aria-label*="odds"], .odds-value, span[role="text"]');
                                const odds = [];

                                oddsElements.forEach(el => {
                                    const text = el.textContent.trim();
                                    if (text && (text.startsWith('+') || text.startsWith('-')) && text.length > 1) {
                                        odds.push(text);
                                    }
                                });

                                if (odds.length >= 2) {
                                    games.push({
                                        away_team: awayTeam,
                                        home_team: homeTeam,
                                        away_ml: odds[0],
                                        home_ml: odds[1],
                                        spread: odds.length > 2 ? odds[2] : null,
                                        total: odds.length > 4 ? odds[4] : null
                                    });
                                }
                            }
                        } catch (e) {
                            console.error('Error parsing game:', e);
                        }
                    });

                    return games;
                }
            """
            )

            # Convert to standard format
            for i, game in enumerate(games_data):
                event = {
                    "event_id": f"fanduel_{int(time.time())}_{i}",
                    "sport": "NFL",
                    "league": "NFL",
                    "home_team": game["home_team"],
                    "away_team": game["away_team"],
                    "start_time": datetime.utcnow().isoformat(),
                    "markets": [],
                }

                # Add moneyline market
                if game["home_ml"] and game["away_ml"]:
                    event["markets"].append(
                        {
                            "type": "moneyline",
                            "selections": [
                                {
                                    "name": game["home_team"],
                                    "price": self.parse_american_odds(game["home_ml"]),
                                },
                                {
                                    "name": game["away_team"],
                                    "price": self.parse_american_odds(game["away_ml"]),
                                },
                            ],
                        }
                    )

                if event["markets"]:
                    events.append(event)

            await page.close()

        except Exception as e:
            logger.error(f"Error fetching FanDuel odds: {e}")

        return events

    def parse_american_odds(self, odds_str):
        """Parse American odds string to integer"""
        try:
            cleaned = "".join(c for c in odds_str if c.isdigit() or c in "+-")
            if cleaned:
                return int(cleaned)
        except:
            pass
        return 100

    def publish_events(self, events):
        """Publish events to Redis"""
        if not events:
            logger.info("No FanDuel events found")
            return

        message = {
            "book": "fanduel",
            "timestamp": datetime.utcnow().isoformat(),
            "events": events,
        }

        self.redis_client.publish("odds.raw.fanduel", json.dumps(message))
        logger.info(f"Published {len(events)} FanDuel events")

    async def run(self):
        """Main collection loop"""
        logger.info("Starting FanDuel browser collector")
        await self.init_browser()

        while True:
            try:
                events = await self.fetch_odds()
                self.publish_events(events)
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                if self.browser:
                    await self.browser.close()
                await self.init_browser()

            await asyncio.sleep(INTERVAL)


async def main():
    collector = FanDuelBrowserCollector()
    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())

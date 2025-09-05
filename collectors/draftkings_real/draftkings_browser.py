#!/usr/bin/env python3
"""
DraftKings Real Browser Collector
Production-grade collector using Playwright for real odds
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import json
import time
import logging
from typing import Dict, Any, Optional, List
from playwright.sync_api import sync_playwright
import re

from infra.collector_base import RealCollectorBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("draftkings_browser")


class DraftKingsBrowserCollector(RealCollectorBase):
    """
    DraftKings browser-based collector with anti-detection measures
    """

    def __init__(self):
        super().__init__("draftkings", strict_validation=True)

        self.base_url = "https://sportsbook.draftkings.com"
        self.browser = None
        self.context = None
        self.page = None

        # Sports to collect
        self.sports_paths = {
            "nfl": "/leagues/football/nfl",
            "nba": "/leagues/basketball/nba",
            "mlb": "/leagues/baseball/mlb",
            "nhl": "/leagues/hockey/nhl",
            "ncaaf": "/leagues/football/ncaaf",
            "ncaab": "/leagues/basketball/ncaab",
        }

    def _setup_browser(self):
        """Setup browser with anti-detection measures"""
        try:
            if self.browser:
                self.browser.close()

            playwright = sync_playwright().start()

            # Use real Chrome/Chromium with anti-detection
            self.browser = playwright.chromium.launch(
                headless=True,  # Set to False for debugging
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-web-security",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--disable-setuid-sandbox",
                ],
            )

            # Create context with realistic viewport and user agent
            self.context = self.browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                locale="en-US",
                timezone_id="America/New_York",
            )

            # Add anti-detection scripts
            self.context.add_init_script(
                """
                // Override automation detection
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });

                // Override plugins to look normal
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });

                // Override permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
            """
            )

            self.page = self.context.new_page()

            # Set extra headers
            self.page.set_extra_http_headers(
                {
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                }
            )

            logger.info("DraftKings: Browser setup complete")

        except Exception as e:
            logger.error(f"DraftKings: Browser setup failed: {e}")
            raise

    def _navigate_with_retry(self, url: str, max_retries: int = 3) -> bool:
        """Navigate to URL with retries and error handling"""
        for attempt in range(max_retries):
            try:
                response = self.page.goto(
                    url, wait_until="domcontentloaded", timeout=30000
                )

                if response and response.status == 200:
                    # Wait for content to load
                    self.page.wait_for_timeout(2000)
                    return True
                elif response and response.status == 403:
                    logger.warning(
                        "DraftKings: Access denied (403) - may need different region"
                    )
                    return False
                else:
                    logger.warning(
                        f"DraftKings: Got status {response.status if response else 'None'}"
                    )

            except Exception as e:
                logger.warning(
                    f"DraftKings: Navigation attempt {attempt + 1} failed: {e}"
                )

            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))  # Exponential backoff

        return False

    def _extract_odds_from_page(self) -> List[Dict]:
        """Extract odds data from current page"""
        events = []

        try:
            # Wait for odds to load
            self.page.wait_for_selector('[data-testid*="event-cell"]', timeout=10000)

            # Extract data using JavaScript
            odds_data = self.page.evaluate(
                """
                () => {
                    const events = [];
                    const eventCells = document.querySelectorAll('[data-testid*="event-cell"]');

                    eventCells.forEach(cell => {
                        try {
                            // Extract teams
                            const teamElements = cell.querySelectorAll('[data-testid*="team-name"]');
                            if (teamElements.length < 2) return;

                            const awayTeam = teamElements[0].innerText.trim();
                            const homeTeam = teamElements[1].innerText.trim();

                            // Extract odds
                            const oddsElements = cell.querySelectorAll('[data-testid*="odds-button"]');
                            const odds = [];

                            oddsElements.forEach(button => {
                                const label = button.getAttribute('aria-label') || '';
                                const value = button.innerText.trim();
                                if (value && value !== '—') {
                                    odds.push({ label, value });
                                }
                            });

                            if (odds.length > 0) {
                                events.push({
                                    homeTeam,
                                    awayTeam,
                                    odds
                                });
                            }
                        } catch (e) {
                            console.error('Error extracting event:', e);
                        }
                    });

                    return events;
                }
            """
            )

            # Parse and normalize the extracted data
            for raw_event in odds_data:
                event = self._parse_browser_event(raw_event)
                if event:
                    events.append(event)

        except Exception as e:
            logger.debug(f"DraftKings: Error extracting odds: {e}")

        return events

    def _parse_browser_event(self, raw_event: Dict) -> Optional[Dict]:
        """Parse browser-extracted event into normalized format"""
        try:
            home_team = raw_event.get("homeTeam", "")
            away_team = raw_event.get("awayTeam", "")

            if not home_team or not away_team:
                return None

            # Generate event ID
            event_id = f"dk_{home_team.lower().replace(' ', '_')}_{away_team.lower().replace(' ', '_')}_{int(time.time())}"

            # Parse odds into markets
            markets = []
            raw_odds = raw_event.get("odds", [])

            # Group odds by market type
            moneyline_odds = []
            spread_odds = []
            total_odds = []

            for odd in raw_odds:
                label = odd.get("label", "").lower()
                value = odd.get("value", "")

                # Parse American odds
                if value.startswith("+") or value.startswith("-"):
                    try:
                        price = int(value)
                    except:
                        continue

                    # Categorize by label content
                    if "spread" in label or "point" in label:
                        spread_odds.append({"label": label, "price": price})
                    elif "total" in label or "over" in label or "under" in label:
                        total_odds.append({"label": label, "price": price})
                    elif "moneyline" in label or "win" in label:
                        moneyline_odds.append({"label": label, "price": price})

            # Build moneyline market
            if len(moneyline_odds) >= 2:
                markets.append(
                    {
                        "type": "moneyline",
                        "selections": [
                            {"name": home_team, "price": moneyline_odds[0]["price"]},
                            {"name": away_team, "price": moneyline_odds[1]["price"]},
                        ],
                    }
                )

            # Build spread market
            if len(spread_odds) >= 2:
                # Extract line from label if possible
                line = self._extract_line_from_label(spread_odds[0]["label"])
                if line is not None:
                    markets.append(
                        {
                            "type": "spread",
                            "selections": [
                                {
                                    "name": home_team,
                                    "price": spread_odds[0]["price"],
                                    "line": line,
                                },
                                {
                                    "name": away_team,
                                    "price": spread_odds[1]["price"],
                                    "line": -line,
                                },
                            ],
                        }
                    )

            # Build total market
            if len(total_odds) >= 2:
                line = self._extract_line_from_label(total_odds[0]["label"])
                if line is not None:
                    markets.append(
                        {
                            "type": "total",
                            "selections": [
                                {
                                    "name": "Over",
                                    "price": total_odds[0]["price"],
                                    "line": line,
                                },
                                {
                                    "name": "Under",
                                    "price": total_odds[1]["price"],
                                    "line": line,
                                },
                            ],
                        }
                    )

            if not markets:
                return None

            return {
                "event_id": event_id,
                "sport": "NFL",  # Will be set by calling context
                "league": "NFL",
                "home_team": home_team,
                "away_team": away_team,
                "start_time": None,  # Would need additional extraction
                "markets": markets,
            }

        except Exception as e:
            logger.debug(f"DraftKings: Error parsing event: {e}")
            return None

    def _extract_line_from_label(self, label: str) -> Optional[float]:
        """Extract line/spread value from label text"""
        try:
            # Look for patterns like "+3.5", "-7", "42.5"
            match = re.search(r"([+-]?\d+\.?\d*)", label)
            if match:
                return float(match.group(1))
        except:
            pass
        return None

    def collect_data(self) -> Optional[Dict[str, Any]]:
        """
        Collect real data from DraftKings using browser automation

        Returns:
            Dict containing events data or None if failed
        """
        all_events = []

        try:
            # Setup or refresh browser if needed
            if not self.page:
                self._setup_browser()

            for sport, path in self.sports_paths.items():
                try:
                    url = f"{self.base_url}{path}"
                    logger.info(f"DraftKings: Fetching {sport} from {url}")

                    # Navigate to sport page
                    if not self._navigate_with_retry(url):
                        logger.warning(f"DraftKings: Failed to load {sport}")
                        continue

                    # Extract odds from page
                    events = self._extract_odds_from_page()

                    # Add sport/league info
                    for event in events:
                        event["sport"] = sport.upper()
                        event["league"] = sport.upper()

                    all_events.extend(events)
                    logger.info(f"DraftKings: Collected {len(events)} {sport} events")

                    # Rate limiting between sports
                    time.sleep(3)

                except Exception as e:
                    logger.error(f"DraftKings: Error collecting {sport}: {e}")

            if all_events:
                return {"events": all_events}
            else:
                logger.warning("DraftKings: No events collected")
                return None

        except Exception as e:
            logger.error(f"DraftKings: Collection error: {e}")
            # Reset browser on major errors
            self._cleanup_browser()
            return None

    def _cleanup_browser(self):
        """Clean up browser resources"""
        try:
            if self.page:
                self.page.close()
                self.page = None
            if self.context:
                self.context.close()
                self.context = None
            if self.browser:
                self.browser.close()
                self.browser = None
        except:
            pass

    def run(self):
        """
        Main collection loop
        """
        logger.info("Starting DraftKings browser collector with realness validation")

        interval = int(os.getenv("COLLECTION_INTERVAL", 120))

        try:
            while True:
                try:
                    # Run collection cycle with validation
                    success = self.run_collection_cycle()

                    if success:
                        logger.info(
                            "DraftKings: Collection cycle completed successfully"
                        )
                    else:
                        logger.warning("DraftKings: Collection cycle failed or blocked")

                    # Log stats periodically
                    if self.stats["collections"] % 10 == 0:
                        logger.info(
                            f"DraftKings stats: {json.dumps(self.get_stats(), default=str)}"
                        )

                except KeyboardInterrupt:
                    logger.info("DraftKings: Shutting down")
                    break
                except Exception as e:
                    logger.error(f"DraftKings: Unexpected error in main loop: {e}")

                time.sleep(interval)

        finally:
            self._cleanup_browser()


if __name__ == "__main__":
    collector = DraftKingsBrowserCollector()
    collector.run()

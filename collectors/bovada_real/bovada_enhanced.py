#!/usr/bin/env python3
"""
Bovada Enhanced Collector with Realness Validation
Production-grade collector using real API with validation
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import json
import time
import logging
import requests
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from infra.collector_base import RealCollectorBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bovada_enhanced")


class BovadaEnhancedCollector(RealCollectorBase):
    """
    Enhanced Bovada collector with realness validation and metrics
    """

    def __init__(self):
        super().__init__(
            "bovada", strict_validation=False
        )  # Bovada is known real, use permissive

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.bovada.lv/",
                "Origin": "https://www.bovada.lv",
            }
        )

        self.endpoints = {
            "nfl": "https://www.bovada.lv/services/sports/event/v2/events/A/description/football/nfl",
            "nba": "https://www.bovada.lv/services/sports/event/v2/events/A/description/basketball/nba",
            "mlb": "https://www.bovada.lv/services/sports/event/v2/events/A/description/baseball/mlb",
            "nhl": "https://www.bovada.lv/services/sports/event/v2/events/A/description/hockey/nhl",
            "ncaaf": "https://www.bovada.lv/services/sports/event/v2/events/A/description/football/college-football",
            "ncaab": "https://www.bovada.lv/services/sports/event/v2/events/A/description/basketball/college-basketball",
        }

    def collect_data(self) -> Optional[Dict[str, Any]]:
        """
        Collect real data from Bovada API

        Returns:
            Dict with events or None if failed
        """
        all_events = []

        for sport, url in self.endpoints.items():
            try:
                logger.debug(f"Fetching {sport} from Bovada")
                response = self.session.get(url, timeout=15)

                if response.status_code == 200:
                    data = response.json()
                    events = self._parse_bovada_response(data, sport)
                    all_events.extend(events)
                    logger.info(f"Bovada: Collected {len(events)} {sport} events")
                elif response.status_code == 404:
                    logger.debug(f"Bovada: No {sport} events available")
                else:
                    logger.warning(f"Bovada: {sport} returned {response.status_code}")

                # Rate limiting
                time.sleep(0.5)

            except requests.exceptions.Timeout:
                logger.warning(f"Bovada: Timeout fetching {sport}")
            except Exception as e:
                logger.error(f"Bovada: Error fetching {sport}: {e}")

        if all_events:
            return {"events": all_events}
        else:
            logger.warning("Bovada: No events collected")
            return None

    def _parse_bovada_response(self, data: List[Dict], sport: str) -> List[Dict]:
        """
        Parse Bovada API response into normalized format

        Args:
            data: Raw Bovada API response
            sport: Sport name

        Returns:
            List of normalized events
        """
        events = []

        if not isinstance(data, list):
            return events

        for item in data:
            try:
                # Skip if no events
                if "events" not in item or not item["events"]:
                    continue

                for event in item["events"]:
                    parsed = self._parse_event(event, sport)
                    if parsed:
                        events.append(parsed)

            except Exception as e:
                logger.debug(f"Error parsing Bovada item: {e}")

        return events

    def _parse_event(self, event: Dict, sport: str) -> Optional[Dict]:
        """
        Parse single Bovada event

        Args:
            event: Raw event from Bovada
            sport: Sport name

        Returns:
            Normalized event dict or None
        """
        try:
            # Extract basic info
            event_id = f"bovada_{event.get('id', '')}"
            description = event.get("description", "")

            # Parse teams from description
            teams = (
                description.split(" @ ")
                if " @ " in description
                else description.split(" vs ")
            )
            if len(teams) != 2:
                return None

            away_team = teams[0].strip()
            home_team = teams[1].strip()

            # Parse start time
            start_millis = event.get("startTime")
            if start_millis:
                start_time = datetime.fromtimestamp(
                    start_millis / 1000, tz=timezone.utc
                ).isoformat()
            else:
                start_time = None

            # Parse markets
            markets = []
            display_groups = event.get("displayGroups", [])

            for group in display_groups:
                for market in group.get("markets", []):
                    parsed_market = self._parse_market(market, home_team, away_team)
                    if parsed_market:
                        markets.append(parsed_market)

            if not markets:
                return None

            return {
                "event_id": event_id,
                "sport": sport.upper(),
                "league": sport.upper(),
                "home_team": home_team,
                "away_team": away_team,
                "start_time": start_time,
                "markets": markets,
            }

        except Exception as e:
            logger.debug(f"Error parsing Bovada event: {e}")
            return None

    def _parse_market(
        self, market: Dict, home_team: str, away_team: str
    ) -> Optional[Dict]:
        """
        Parse Bovada market into normalized format

        Args:
            market: Raw market from Bovada
            home_team: Home team name
            away_team: Away team name

        Returns:
            Normalized market dict or None
        """
        try:
            market_type = market.get("period", {}).get("description", "").lower()
            outcomes = market.get("outcomes", [])

            if not outcomes:
                return None

            selections = []

            for outcome in outcomes:
                price_data = outcome.get("price", {})
                american_odds = price_data.get("american")

                if american_odds:
                    # Convert string odds to int
                    if isinstance(american_odds, str):
                        american_odds = int(
                            american_odds.replace("EVEN", "100").replace("+", "")
                        )

                    selection = {
                        "name": outcome.get("description", ""),
                        "price": american_odds,
                    }

                    # Add handicap/line if present
                    if "handicap" in price_data:
                        selection["line"] = float(price_data["handicap"])

                    selections.append(selection)

            if not selections:
                return None

            # Map market types
            if "moneyline" in market_type or "win" in market_type:
                market_type = "moneyline"
            elif "spread" in market_type or "handicap" in market_type:
                market_type = "spread"
            elif "total" in market_type or "over/under" in market_type:
                market_type = "total"
            else:
                market_type = "other"

            return {"type": market_type, "selections": selections}

        except Exception as e:
            logger.debug(f"Error parsing Bovada market: {e}")
            return None

    def run(self):
        """
        Main collection loop
        """
        logger.info("Starting Bovada enhanced collector with realness validation")

        interval = int(os.getenv("COLLECTION_INTERVAL", 60))

        while True:
            try:
                # Run collection cycle with validation
                success = self.run_collection_cycle()

                if success:
                    logger.info("Bovada: Collection cycle completed successfully")
                else:
                    logger.warning("Bovada: Collection cycle failed or blocked")

                # Log stats periodically
                if self.stats["collections"] % 10 == 0:
                    logger.info(
                        f"Bovada stats: {json.dumps(self.get_stats(), default=str)}"
                    )

            except KeyboardInterrupt:
                logger.info("Bovada: Shutting down")
                break
            except Exception as e:
                logger.error(f"Bovada: Unexpected error in main loop: {e}")

            time.sleep(interval)


if __name__ == "__main__":
    collector = BovadaEnhancedCollector()
    collector.run()

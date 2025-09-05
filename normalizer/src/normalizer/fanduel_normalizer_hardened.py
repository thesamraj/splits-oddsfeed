#!/usr/bin/env python3
"""
FanDuel Normalizer - Hardened version that never crashes
"""

import sys
import os

sys.path.append(os.path.dirname(__file__))

from base_normalizer import FailSoftNormalizer
import logging

logger = logging.getLogger("fanduel_normalizer")


class FanDuelNormalizer(FailSoftNormalizer):
    """FanDuel specific normalizer with fail-soft handling"""

    def __init__(self):
        super().__init__("fanduel")

    def normalize_message(self, message):
        """Normalize FanDuel message to standard format"""
        try:
            data = message.get("data", {})

            if not data:
                return None

            normalized = []

            # FanDuel has multiple data formats
            # Format 1: attachments.events structure
            if "attachments" in data:
                events = data.get("attachments", {}).get("events", {})
                markets = data.get("attachments", {}).get("markets", {})

                for event_id, event in events.items():
                    try:
                        home_team = event.get("home", {}).get("name", "")
                        away_team = event.get("away", {}).get("name", "")
                        sport = event.get("sport", "unknown")

                        # Find markets for this event
                        for market_id, market in markets.items():
                            if market.get("eventId") != event_id:
                                continue

                            market_type = market.get("marketType", "h2h")

                            for runner in market.get("runners", []):
                                try:
                                    odds_entry = {
                                        "event_id": f"fanduel_{event_id}",
                                        "sport": sport,
                                        "home_team": home_team,
                                        "away_team": away_team,
                                        "market": market_type,
                                        "selection": runner.get("runnerName", ""),
                                        "price": self.parse_price(
                                            runner.get("winRunnerOdds")
                                        ),
                                    }
                                    normalized.append(odds_entry)
                                except Exception as e:
                                    logger.debug(f"Failed to parse runner: {e}")
                                    continue
                    except Exception as e:
                        logger.debug(f"Failed to process event {event_id}: {e}")
                        continue

            # Format 2: Direct event structure
            elif "event_id" in data or "id" in data:
                event_id = data.get("event_id") or data.get("id")
                home_team = data.get("home_team", "")
                away_team = data.get("away_team", "")
                sport = data.get("sport", "unknown")

                # Process markets
                markets = data.get("markets", [])
                if not markets and "market" in data:
                    markets = [data]

                for market_data in markets:
                    try:
                        market_type = market_data.get("market", "h2h")

                        # Handle odds format
                        if "odds" in market_data:
                            odds = market_data["odds"]

                            # Home odds
                            if "home" in odds:
                                normalized.append(
                                    {
                                        "event_id": f"fanduel_{event_id}",
                                        "sport": sport,
                                        "home_team": home_team,
                                        "away_team": away_team,
                                        "market": market_type,
                                        "selection": home_team,
                                        "price": self.parse_price(odds["home"]),
                                    }
                                )

                            # Away odds
                            if "away" in odds:
                                normalized.append(
                                    {
                                        "event_id": f"fanduel_{event_id}",
                                        "sport": sport,
                                        "home_team": home_team,
                                        "away_team": away_team,
                                        "market": market_type,
                                        "selection": away_team,
                                        "price": self.parse_price(odds["away"]),
                                    }
                                )

                        # Handle selections format
                        elif "selections" in market_data:
                            for selection in market_data["selections"]:
                                try:
                                    normalized.append(
                                        {
                                            "event_id": f"fanduel_{event_id}",
                                            "sport": sport,
                                            "home_team": home_team,
                                            "away_team": away_team,
                                            "market": market_type,
                                            "selection": selection.get("name", ""),
                                            "price": self.parse_price(
                                                selection.get("price")
                                            ),
                                        }
                                    )
                                except Exception as e:
                                    logger.debug(f"Failed to parse selection: {e}")
                                    continue

                    except Exception as e:
                        logger.debug(f"Failed to process market: {e}")
                        continue

            return normalized if normalized else None

        except Exception as e:
            logger.error(f"FanDuel normalization failed: {e}")
            return None

    def parse_price(self, price):
        """Parse price from various FanDuel formats"""
        if price is None:
            return 0

        try:
            # Handle direct numeric
            if isinstance(price, (int, float)):
                return float(price)

            # Handle string prices
            if isinstance(price, str):
                price = price.replace("+", "")
                return float(price)

            # Handle dict format (americanOdds, decimalOdds, etc)
            if isinstance(price, dict):
                if "americanOdds" in price:
                    return float(price["americanOdds"].get("americanOddsInt", 0))
                elif "american" in price:
                    return float(price["american"])
                elif "decimal" in price:
                    # Convert decimal to American
                    dec = float(price["decimal"])
                    if dec >= 2.0:
                        return (dec - 1) * 100
                    else:
                        return -100 / (dec - 1)

        except Exception as e:
            logger.debug(f"Failed to parse price {price}: {e}")

        return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    normalizer = FanDuelNormalizer()
    logger.info("Starting FanDuel normalizer (hardened)")
    normalizer.run()

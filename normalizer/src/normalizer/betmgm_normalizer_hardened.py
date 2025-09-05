#!/usr/bin/env python3
"""
BetMGM Normalizer - Hardened version that never crashes
"""

import sys
import os

sys.path.append(os.path.dirname(__file__))

from base_normalizer import FailSoftNormalizer
import logging

logger = logging.getLogger("betmgm_normalizer")


class BetMGMNormalizer(FailSoftNormalizer):
    """BetMGM specific normalizer with fail-soft handling"""

    def __init__(self):
        super().__init__("betmgm")

    def normalize_message(self, message):
        """Normalize BetMGM message to standard format"""
        try:
            data = message.get("data", {})

            if not data:
                return None

            normalized = []

            # Extract event details
            event_id = data.get("event_id") or data.get("id")
            if not event_id:
                logger.warning("No event_id found in BetMGM message")
                return None

            sport = data.get("sport", "unknown")
            home_team = data.get("home_team") or data.get("home", {}).get("name", "")
            away_team = data.get("away_team") or data.get("away", {}).get("name", "")

            # Process markets
            markets = data.get("markets", [])
            if not markets and "market" in data:
                # Single market format
                markets = [data]

            for market_data in markets:
                try:
                    market_type = market_data.get("market") or market_data.get(
                        "type", "h2h"
                    )

                    # Handle different market structures
                    if "selections" in market_data:
                        # Multiple selections format
                        for selection in market_data["selections"]:
                            try:
                                odds_entry = {
                                    "event_id": f"betmgm_{event_id}",
                                    "sport": sport,
                                    "home_team": home_team,
                                    "away_team": away_team,
                                    "market": market_type,
                                    "selection": selection.get("name", ""),
                                    "price": self.parse_price(selection.get("price")),
                                }
                                normalized.append(odds_entry)
                            except Exception as e:
                                logger.debug(f"Failed to parse selection: {e}")
                                continue

                    elif "price_home" in market_data and "price_away" in market_data:
                        # Home/away price format
                        if market_data.get("price_home"):
                            normalized.append(
                                {
                                    "event_id": f"betmgm_{event_id}",
                                    "sport": sport,
                                    "home_team": home_team,
                                    "away_team": away_team,
                                    "market": market_type,
                                    "selection": home_team,
                                    "price": self.parse_price(
                                        market_data["price_home"]
                                    ),
                                }
                            )

                        if market_data.get("price_away"):
                            normalized.append(
                                {
                                    "event_id": f"betmgm_{event_id}",
                                    "sport": sport,
                                    "home_team": home_team,
                                    "away_team": away_team,
                                    "market": market_type,
                                    "selection": away_team,
                                    "price": self.parse_price(
                                        market_data["price_away"]
                                    ),
                                }
                            )

                except Exception as e:
                    logger.debug(f"Failed to process market: {e}")
                    continue

            return normalized if normalized else None

        except Exception as e:
            logger.error(f"BetMGM normalization failed: {e}")
            return None

    def parse_price(self, price):
        """Parse price from various formats"""
        if price is None:
            return 0

        try:
            # Handle American odds
            if isinstance(price, (int, float)):
                return float(price)

            # Handle string prices
            if isinstance(price, str):
                # Remove + sign if present
                price = price.replace("+", "")
                return float(price)

            # Handle dict format
            if isinstance(price, dict):
                return float(price.get("american", 0) or price.get("decimal", 0) or 0)

        except:
            pass

        return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    normalizer = BetMGMNormalizer()
    logger.info("Starting BetMGM normalizer (hardened)")
    normalizer.run()

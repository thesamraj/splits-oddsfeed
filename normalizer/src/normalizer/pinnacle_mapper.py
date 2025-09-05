"""Pinnacle data mapper for normalizer."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


def normalize_pinnacle_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize Pinnacle API response to unified schema.

    Expected input format:
    - Mock: {events: [...], source: "pinnacle", book: "pin"}
    - Real: {fixtures: {...}, odds: {...}, source: "pinnacle", book: "pin"}
    """

    try:
        normalized_events = []

        # Handle mock format
        if "events" in payload:
            for event in payload["events"]:
                normalized_event = _normalize_mock_event(event)
                if normalized_event:
                    normalized_events.append(normalized_event)

        # Handle real API format
        elif "fixtures" in payload and "odds" in payload:
            fixtures = payload["fixtures"]
            odds_data = payload["odds"]
            normalized_events = _normalize_real_data(fixtures, odds_data)

        else:
            logger.warning("Unrecognized Pinnacle payload format")
            return payload  # Return as-is

        return {
            "source": "pinnacle",
            "book": "pin",
            "timestamp": datetime.utcnow().isoformat(),
            "events": normalized_events,
        }

    except Exception as e:
        logger.error(f"Failed to normalize Pinnacle data: {e}")
        return payload  # Return original on error


def _normalize_mock_event(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a single mock event."""
    try:
        # Handle both 'event_id' and 'id' fields
        raw_id = event.get("event_id") or event.get("id")
        if not raw_id:
            logger.error(f"No event ID found in event: {event.keys()}")
            return None

        # Don't double-prefix if already prefixed
        if raw_id.startswith("pinnacle_"):
            event_id = raw_id
        else:
            event_id = f"pin_{raw_id}"

        # Extract markets
        markets = []

        # Handle different market formats
        market_data = event.get("markets", [])

        # If markets is a list (universal collector format)
        if isinstance(market_data, list):
            for market in market_data:
                normalized_market = {
                    "book": "pin",
                    "market_type": _map_market_type(market.get("type", "moneyline")),
                    "outcomes": [],
                }

                # Process selections/outcomes
                for selection in market.get("selections", []):
                    normalized_outcome = {
                        "name": selection.get("name"),
                        "price": _normalize_price(selection.get("price")),
                    }

                    # Add line/point if present
                    if "line" in selection:
                        normalized_outcome["point"] = _normalize_point(
                            selection["line"]
                        )

                    normalized_market["outcomes"].append(normalized_outcome)

                if normalized_market["outcomes"]:
                    markets.append(normalized_market)

        # If markets is a dict (older format)
        elif isinstance(market_data, dict):
            for market_type, market_info in market_data.items():
                normalized_market = {
                    "book": "pin",
                    "market_type": _map_market_type(market_type),
                    "outcomes": [],
                }

                # Process outcomes
                for outcome in market_info.get("outcomes", []):
                    normalized_outcome = {
                        "name": outcome["name"],
                        "price": _normalize_price(outcome["price"]),
                    }

                    # Add point if present
                    if "point" in outcome:
                        normalized_outcome["point"] = _normalize_point(outcome["point"])

                    normalized_market["outcomes"].append(normalized_outcome)

                if normalized_market["outcomes"]:
                    markets.append(normalized_market)

        return {
            "event_id": event_id,
            "league": event.get("league", "NFL"),
            "sport": event.get("sport", "american_football"),
            "start_time": event.get("start_time")
            or event.get("starts")
            or datetime.utcnow().isoformat(),
            "home_team": event.get("home_team") or event.get("home", "TBD"),
            "away_team": event.get("away_team") or event.get("away", "TBD"),
            "markets": markets,
        }

    except Exception as e:
        logger.error(f"Failed to normalize mock event: {e}")
        return None


def _normalize_real_data(
    fixtures: Dict[str, Any], odds_data: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Normalize real Pinnacle fixtures and odds data."""
    # This would implement the actual Pinnacle API format mapping
    # For now, return empty list as we're primarily testing with mock
    logger.info("Real Pinnacle data normalization not yet implemented")
    return []


def _map_market_type(pinnacle_market: str) -> str:
    """Map Pinnacle market types to unified types."""
    mapping = {
        "moneyline": "h2h",
        "spreads": "spreads",
        "totals": "totals",
        "point_spread": "spreads",
        "total": "totals",
    }
    return mapping.get(pinnacle_market, pinnacle_market)


def _normalize_price(price: Any) -> Optional[Decimal]:
    """Normalize price to Decimal."""
    try:
        if price is None:
            return None
        return Decimal(str(price))
    except (ValueError, TypeError):
        logger.warning(f"Invalid price format: {price}")
        return None


def _normalize_point(point: Any) -> Optional[Decimal]:
    """Normalize point/handicap to Decimal."""
    try:
        if point is None:
            return None
        return Decimal(str(point))
    except (ValueError, TypeError):
        logger.warning(f"Invalid point format: {point}")
        return None

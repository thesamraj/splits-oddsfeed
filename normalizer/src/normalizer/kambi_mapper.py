"""Kambi data mapper for normalizer."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any, Optional
from prometheus_client import Counter

logger = logging.getLogger(__name__)

# Metrics
kambi_odds_skipped_total = Counter(
    "kambi_odds_skipped_total", "Skipped Kambi odds", ["reason"]
)


def normalize_kambi_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize Kambi API response to unified schema.

    Expected input format from browser:
    {
        "source": "kambi",
        "url": "https://eu.offering-api.kambicdn.com/offering/v2018/...",
        "status": 200,
        "ts": 1234567890.0,
        "data": {...}  # The actual Kambi response
    }
    """

    try:
        normalized_events = []

        # Debug logging
        logger.info(f"Kambi payload keys: {list(payload.keys())}")

        # Extract the actual Kambi data
        kambi_data = payload.get("data", {})
        logger.info(
            f"Kambi data keys: {list(kambi_data.keys()) if isinstance(kambi_data, dict) else type(kambi_data)}"
        )
        logger.info(f"Kambi data sample: {str(kambi_data)[:500]}...")

        # Kambi group.json typically contains a single group or multiple groups
        if "groups" in kambi_data:
            for group in kambi_data["groups"]:
                events = _normalize_group_events(group)
                normalized_events.extend(events)
        elif "group" in kambi_data:
            # Handle single group structure
            events = _normalize_group_events(kambi_data["group"])
            normalized_events.extend(events)

        # Handle direct events in response
        elif "events" in kambi_data:
            for event in kambi_data["events"]:
                normalized_event = _normalize_kambi_event(event)
                if normalized_event:
                    normalized_events.append(normalized_event)

        # Handle betOffers (betting markets) structure
        elif "betOffers" in kambi_data:
            # Extract events from betOffers structure
            events = _extract_events_from_bet_offers(kambi_data)
            normalized_events.extend(events)

        else:
            logger.debug(
                f"Unrecognized Kambi payload structure: {list(kambi_data.keys())}"
            )
            # Try to extract any events we can find
            if isinstance(kambi_data, dict):
                events = _fallback_extract_events(kambi_data)
                normalized_events.extend(events)

        return {
            "source": "kambi",
            "book": "kambi",
            "timestamp": datetime.utcnow().isoformat(),
            "events": normalized_events,
        }

    except Exception as e:
        logger.error(f"Failed to normalize Kambi data: {e}")
        return payload  # Return original on error


def _normalize_group_events(group: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalize events from a Kambi group."""
    events = []
    for event in group.get("events", []):
        normalized_event = _normalize_kambi_event(event)
        if normalized_event:
            events.append(normalized_event)
    return events


def _normalize_kambi_event(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a single Kambi event."""
    try:
        event_id = f"kambi_{event.get('id', 'unknown')}"

        # Extract event details
        event_data = {
            "event_id": event_id,
            "league": "NFL",  # Default for now
            "sport": "american_football",
            "start_time": event.get("start") or event.get("date"),
            "home_team": _extract_team_name(event, "home"),
            "away_team": _extract_team_name(event, "away"),
            "markets": [],
        }

        # Process betting offers (markets)
        bet_offers = event.get("betOffers", [])
        for bet_offer in bet_offers:
            market = _normalize_bet_offer(bet_offer)
            if market:
                event_data["markets"].append(market)

        return event_data if event_data["markets"] else None

    except Exception as e:
        logger.error(f"Failed to normalize Kambi event: {e}")
        return None


def _extract_events_from_bet_offers(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract events from betOffers structure."""
    events = []

    # Group betOffers by event
    events_by_id = {}

    for bet_offer in data.get("betOffers", []):
        event_info = bet_offer.get("event", {})
        event_id = event_info.get("id")

        if not event_id:
            continue

        if event_id not in events_by_id:
            events_by_id[event_id] = {
                "id": event_id,
                "start": event_info.get("start"),
                "participants": event_info.get("participants", []),
                "betOffers": [],
            }

        events_by_id[event_id]["betOffers"].append(bet_offer)

    # Convert to normalized format
    for event_data in events_by_id.values():
        normalized_event = _normalize_kambi_event(event_data)
        if normalized_event:
            events.append(normalized_event)

    return events


def _fallback_extract_events(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Fallback method to extract any events from unknown Kambi structure."""
    events = []

    # Recursively search for event-like structures
    def find_events(obj, path=""):
        if isinstance(obj, dict):
            if "betOffers" in obj and "participants" in obj:
                # Looks like an event
                normalized_event = _normalize_kambi_event(obj)
                if normalized_event:
                    events.append(normalized_event)
            else:
                for key, value in obj.items():
                    find_events(value, f"{path}.{key}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                find_events(item, f"{path}[{i}]")

    find_events(data)
    return events


def _extract_team_name(event: Dict[str, Any], side: str) -> Optional[str]:
    """Extract team name from Kambi event participants."""
    participants = event.get("participants", [])

    for participant in participants:
        if participant.get("type") == side:
            return participant.get("name")

    # Fallback: use participant order (home=0, away=1)
    if side == "home" and len(participants) > 0:
        return participants[0].get("name")
    elif side == "away" and len(participants) > 1:
        return participants[1].get("name")

    return None


def _normalize_bet_offer(bet_offer: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a Kambi bet offer to a market."""
    try:
        criterion = bet_offer.get("criterion", {})
        if not criterion:
            kambi_odds_skipped_total.labels("missing_criterion").inc()
            return None

        market_type = _map_kambi_market_type(criterion.get("label", ""))

        if not market_type:
            kambi_odds_skipped_total.labels("unmapped_market").inc()
            return None

        market = {
            "book": "kambi",
            "market_type": market_type,
            "outcomes": [],
        }

        # Process outcomes
        outcomes = bet_offer.get("outcomes", [])
        if not outcomes:
            kambi_odds_skipped_total.labels("no_outcomes").inc()
            return None

        for outcome in outcomes:
            if outcome.get("status") != "OPEN":
                continue

            # Check for required fields
            if not outcome.get("label"):
                kambi_odds_skipped_total.labels("missing_outcome_label").inc()
                continue

            odds_value = outcome.get("odds")
            if odds_value is None:
                kambi_odds_skipped_total.labels("missing_odds").inc()
                continue

            normalized_outcome = {
                "name": outcome.get("label", ""),
                "price": _normalize_kambi_odds(odds_value),
            }

            # Skip if odds normalization failed
            if normalized_outcome["price"] is None:
                kambi_odds_skipped_total.labels("invalid_odds").inc()
                continue

            # Add point/line if present
            line = outcome.get("line")
            if line is not None:
                normalized_outcome["point"] = _normalize_point(line)

            market["outcomes"].append(normalized_outcome)

        return market if market["outcomes"] else None

    except Exception as e:
        logger.error(f"Failed to normalize bet offer: {e}")
        kambi_odds_skipped_total.labels("processing_error").inc()
        return None


def _map_kambi_market_type(label: str) -> Optional[str]:
    """Map Kambi market labels to unified types."""
    label_lower = label.lower()

    if "moneyline" in label_lower or "winner" in label_lower:
        return "h2h"
    elif "spread" in label_lower or "handicap" in label_lower:
        return "spreads"
    elif "total" in label_lower or "over/under" in label_lower:
        return "totals"

    return None  # Skip unmapped markets


def _normalize_kambi_odds(odds_value: Any) -> Optional[Decimal]:
    """Normalize Kambi odds to American format decimal."""
    try:
        if odds_value is None:
            return None

        # Kambi odds are typically in decimal format (European)
        decimal_odds = Decimal(str(odds_value))

        # Convert to American odds
        if decimal_odds >= 2:
            american_odds = (decimal_odds - 1) * 100
        else:
            american_odds = -100 / (decimal_odds - 1)

        return american_odds.quantize(Decimal("0.1"))

    except (ValueError, TypeError, ZeroDivisionError):
        logger.warning(f"Invalid Kambi odds format: {odds_value}")
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

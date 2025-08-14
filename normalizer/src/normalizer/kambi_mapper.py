"""Kambi data mapper for normalizer."""

import logging
import re
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any, Optional, Tuple, NamedTuple
from urllib.parse import urlparse, parse_qs
from prometheus_client import Counter

logger = logging.getLogger(__name__)

# Metrics
kambi_norm_rows_total = Counter("kambi_norm_rows_total", "Kambi normalized rows inserted")
kambi_norm_skipped_total = Counter("kambi_norm_skipped_total", "Skipped Kambi rows", ["reason"])


class EventRow(NamedTuple):
    id: str
    league: str
    start_time: Optional[str]
    home: str
    away: str
    sport: str = "american_football"


class OddsRow(NamedTuple):
    event_id: str
    book: str
    market: str
    outcome_name: str
    outcome_price: Decimal
    outcome_point: Optional[Decimal]
    ts: str


def decimal_to_american(decimal_odds: float) -> int:
    """Convert decimal odds to American format"""
    if decimal_odds >= 2.0:
        return int((decimal_odds - 1) * 100)
    else:
        return int(-100 / (decimal_odds - 1))


def extract_line_from_label(label: str) -> Optional[float]:
    """Extract point/line from outcome label using regex"""
    # Match patterns like "+7.5", "-3", "Over 47.5", "Under 42"
    match = re.search(r"[+-]?\d+\.?\d*", label)
    if match:
        try:
            return float(match.group())
        except ValueError:
            pass
    return None


def extract_kambi(payload: dict, src_url: str) -> Tuple[List[EventRow], List[OddsRow]]:
    """Extract events and odds from Kambi payload"""
    events = []
    odds = []

    try:
        # 1. Find event IDs from URL query params
        event_ids_from_url = set()
        if src_url:
            parsed = urlparse(src_url)
            params = parse_qs(parsed.query)
            for key in ["eventId", "eventIds"]:
                if key in params:
                    for val in params[key]:
                        event_ids_from_url.update(val.split(","))

        # 2. Find events from payload
        events_data = []

        # Try different payload structures
        if "events" in payload:
            events_data.extend(payload["events"])
        if "data" in payload and isinstance(payload["data"], dict):
            if "events" in payload["data"]:
                events_data.extend(payload["data"]["events"])
            if "group" in payload["data"] and "events" in payload["data"]["group"]:
                events_data.extend(payload["data"]["group"]["events"])

        # Process events from betOffers if no direct events found
        if not events_data and "betOffers" in payload:
            events_by_id = {}
            for bet_offer in payload["betOffers"]:
                event_id = bet_offer.get("eventId")
                if event_id:
                    if event_id not in events_by_id:
                        events_by_id[event_id] = {"id": event_id, "betOffers": []}
                    events_by_id[event_id]["betOffers"].append(bet_offer)
            events_data = list(events_by_id.values())

        # 3. Process each event
        for event_data in events_data:
            event_id = str(event_data.get("id", ""))
            if not event_id:
                kambi_norm_skipped_total.labels("missing_event_id").inc()
                continue

            # Extract teams
            home_team = None
            away_team = None

            # Try different team extraction methods
            if "homeName" in event_data and "awayName" in event_data:
                home_team = event_data["homeName"]
                away_team = event_data["awayName"]
            elif "home" in event_data and "away" in event_data:
                home_team = event_data["home"]
                away_team = event_data["away"]
            elif "participants" in event_data:
                for p in event_data["participants"]:
                    if p.get("qualifier") == "home" or p.get("type") == "home":
                        home_team = p.get("name")
                    elif p.get("qualifier") == "away" or p.get("type") == "away":
                        away_team = p.get("name")
            elif "name" in event_data:
                # Parse "Team A @ Team B" or "Team A vs Team B"
                name = event_data["name"]
                for delimiter in [" @ ", " vs ", " - "]:
                    if delimiter in name:
                        parts = name.split(delimiter, 1)
                        if len(parts) == 2:
                            away_team = parts[0].strip()
                            home_team = parts[1].strip()
                            break

            if not home_team or not away_team:
                kambi_norm_skipped_total.labels("missing_teams").inc()
                continue

            # Extract start time
            start_time = None
            for time_field in ["start", "startTime", "openDate"]:
                if time_field in event_data:
                    start_time = event_data[time_field]
                    break

            # Create event row
            event_row = EventRow(
                id=f"kambi_{event_id}", league="NFL", start_time=start_time, home=home_team, away=away_team
            )
            events.append(event_row)

            # 4. Extract odds from betOffers
            bet_offers = event_data.get("betOffers", [])
            if "betOffers" in payload:
                # Add global betOffers for this event
                for bo in payload["betOffers"]:
                    if str(bo.get("eventId", "")) == event_id:
                        bet_offers.append(bo)

            for bet_offer in bet_offers:
                market_type = None
                criterion = bet_offer.get("criterion", {})
                label = criterion.get("label", "").lower()

                # Market mapping
                if any(x in label for x in ["moneyline", "match winner", "3-way moneyline"]):
                    market_type = "h2h"
                elif "point spread" in label:
                    market_type = "spreads"
                elif any(x in label for x in ["total", "total points"]):
                    market_type = "totals"
                else:
                    kambi_norm_skipped_total.labels("unrecognized_market").inc()
                    continue

                outcomes = bet_offer.get("outcomes", [])
                for outcome in outcomes:
                    if outcome.get("status") != "OPEN":
                        continue

                    # Extract price
                    price = None
                    odds_data = outcome.get("odds")
                    if isinstance(odds_data, (int, float)):
                        price = decimal_to_american(odds_data / 1000.0)  # Kambi uses milliodds
                    elif isinstance(odds_data, dict):
                        decimal_odds = odds_data.get("decimal")
                        if decimal_odds:
                            price = decimal_to_american(decimal_odds)

                    if price is None:
                        kambi_norm_skipped_total.labels("missing_price").inc()
                        continue

                    # Extract line/point
                    point = None
                    if "line" in outcome:
                        point = outcome["line"]
                        if isinstance(point, int):
                            point = point / 1000.0  # Convert millipoints
                    elif "handicap" in outcome:
                        point = outcome["handicap"]
                    else:
                        # Try to extract from label
                        line_from_label = extract_line_from_label(outcome.get("label", ""))
                        if line_from_label is not None:
                            point = line_from_label

                    odds_row = OddsRow(
                        event_id=f"kambi_{event_id}",
                        book="kambi",
                        market=market_type,
                        outcome_name=outcome.get("label", ""),
                        outcome_price=Decimal(str(price)),
                        outcome_point=Decimal(str(point)) if point is not None else None,
                        ts=datetime.utcnow().isoformat(),
                    )
                    odds.append(odds_row)

        return events, odds

    except Exception as e:
        logger.error(f"Failed to extract Kambi data: {e}")
        kambi_norm_skipped_total.labels("extraction_error").inc()
        return [], []


def normalize_kambi_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Legacy wrapper for compatibility"""
    src_url = payload.get("url", "")
    data = payload.get("data", {})

    events, odds = extract_kambi(data, src_url)

    # Convert to legacy format for existing normalizer
    normalized_events = []
    for event in events:
        event_odds = [o for o in odds if o.event_id == event.id]
        if event_odds:
            markets = {}
            for odd in event_odds:
                if odd.market not in markets:
                    markets[odd.market] = {"book": "kambi", "market_type": odd.market, "outcomes": []}
                markets[odd.market]["outcomes"].append(
                    {
                        "name": odd.outcome_name,
                        "price": float(odd.outcome_price),
                        "point": float(odd.outcome_point) if odd.outcome_point else None,
                    }
                )

            normalized_events.append(
                {
                    "event_id": event.id,
                    "league": event.league,
                    "sport": event.sport,
                    "start_time": event.start_time,
                    "home_team": event.home,
                    "away_team": event.away,
                    "markets": list(markets.values()),
                }
            )

    return {
        "source": "kambi",
        "book": "kambi",
        "timestamp": datetime.utcnow().isoformat(),
        "events": normalized_events,
    }

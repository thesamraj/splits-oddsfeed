"""
Enhanced Kambi mapper that processes ALL bet offers, not just mainBetOffer
This will capture spreads, totals, and all other market types
"""

import logging
import time
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


def process_all_bet_offers(
    event_node: Dict[str, Any],
    event_id: str,
    home: str,
    away: str,
    sport: str,
    league: str,
) -> List[Dict[str, Any]]:
    """
    Process ALL betOffers from a Kambi event, not just mainBetOffer
    Returns list of odds rows for all market types
    """
    all_rows = []

    # Get all betOffers - search comprehensively
    bet_offers = []
    processed_ids = set()  # Avoid duplicates

    # Helper to add unique bet offers
    def add_bet_offer(bo):
        if isinstance(bo, dict):
            bo_id = bo.get("id") or id(bo)
            if bo_id not in processed_ids:
                processed_ids.add(bo_id)
                bet_offers.append(bo)

    # Priority 1: Check for betOffers array at top level
    if "betOffers" in event_node and isinstance(event_node["betOffers"], list):
        for bo in event_node["betOffers"]:
            add_bet_offer(bo)

    # Priority 2: Check in 'raw' data
    raw_data = event_node.get("raw", {})
    if isinstance(raw_data, dict):
        if "betOffers" in raw_data and isinstance(raw_data["betOffers"], list):
            for bo in raw_data["betOffers"]:
                add_bet_offer(bo)
        if "mainBetOffer" in raw_data and isinstance(raw_data["mainBetOffer"], dict):
            add_bet_offer(raw_data["mainBetOffer"])

    # Priority 3: Check mainBetOffer at top level
    if "mainBetOffer" in event_node and isinstance(event_node["mainBetOffer"], dict):
        add_bet_offer(event_node["mainBetOffer"])

    # Priority 4: Deep search in event structure
    event_obj = event_node.get("event", {})
    if isinstance(event_obj, dict):
        if "betOffers" in event_obj and isinstance(event_obj["betOffers"], list):
            for bo in event_obj["betOffers"]:
                add_bet_offer(bo)
        if "mainBetOffer" in event_obj and isinstance(event_obj["mainBetOffer"], dict):
            add_bet_offer(event_obj["mainBetOffer"])

    logger.info(f"Found {len(bet_offers)} bet offers for event {event_id}")

    for bet_offer in bet_offers:
        if not isinstance(bet_offer, dict):
            continue

        # Identify market type from betOfferType or criterion
        market_type = identify_market_type(bet_offer)

        # Extract outcomes
        outcomes = bet_offer.get("outcomes", [])
        if not outcomes:
            continue

        # Process based on market type
        if market_type == "h2h" or market_type == "moneyline":
            row = process_h2h_market(bet_offer, event_id, home, away, sport, league)
            if row:
                all_rows.append(row)

        elif market_type == "spread" or "handicap" in market_type.lower():
            rows = process_spread_market(bet_offer, event_id, home, away, sport, league)
            all_rows.extend(rows)

        elif market_type == "total" or "total" in market_type.lower():
            rows = process_totals_market(bet_offer, event_id, home, away, sport, league)
            all_rows.extend(rows)

        else:
            # Generic processing for other markets
            rows = process_generic_market(
                bet_offer, event_id, home, away, sport, league, market_type
            )
            all_rows.extend(rows)

    # If no rows were created, create a minimal event row
    if not all_rows:
        all_rows.append(
            {
                "event_id": event_id,
                "market": "h2h",
                "line": None,
                "total": None,
                "price_home": None,
                "price_away": None,
                "price_over": None,
                "price_under": None,
                "ts": int(time.time()),
                "book": "betrivers",
                "brand": "betrivers",
                "sport": sport.lower() if sport else "unknown",
                "league": league,
                "home_team": home,
                "away_team": away,
            }
        )

    return all_rows


def identify_market_type(bet_offer: Dict[str, Any]) -> str:
    """Identify the market type from bet offer structure"""

    # Check betOfferType
    bet_offer_type = bet_offer.get("betOfferType", {})
    if isinstance(bet_offer_type, dict):
        type_name = bet_offer_type.get("englishName", "").lower()
        if "match" in type_name or "money" in type_name or "win" in type_name:
            return "h2h"
        elif "handicap" in type_name or "spread" in type_name:
            return "spread"
        elif "total" in type_name or "over" in type_name:
            return "total"

    # Check criterion
    criterion = bet_offer.get("criterion", {})
    if isinstance(criterion, dict):
        label = criterion.get("englishLabel", "").lower()
        if "handicap" in label or "spread" in label:
            return "spread"
        elif "total" in label or "over/under" in label:
            return "total"
        elif "full time" in label or "match" in label:
            return "h2h"

    # Check outcomes for clues
    outcomes = bet_offer.get("outcomes", [])
    if len(outcomes) == 3:  # Likely 1X2 market
        return "h2h"
    elif len(outcomes) == 2:
        # Check if it's over/under
        for outcome in outcomes:
            label = outcome.get("englishLabel", "").lower()
            if "over" in label or "under" in label:
                return "total"
            elif "+" in label or "-" in label:
                return "spread"

    return "other"


def process_h2h_market(
    bet_offer: Dict, event_id: str, home: str, away: str, sport: str, league: str
) -> Optional[Dict]:
    """Process head-to-head/moneyline market"""
    outcomes = bet_offer.get("outcomes", [])

    price_home = None
    price_away = None

    for outcome in outcomes:
        if not isinstance(outcome, dict):
            continue

        odds_val = outcome.get("odds")
        if not odds_val:
            continue

        # Convert odds (Kambi uses 1000-based format)
        decimal_odds = float(odds_val) / 1000 if odds_val > 100 else float(odds_val)

        # Identify if home or away
        participant = outcome.get("participant", "").lower()
        label = outcome.get("englishLabel", "").lower()
        outcome_type = outcome.get("type", "")

        if (
            home.lower() in participant
            or "home" in participant
            or outcome_type == "OT_ONE"
        ):
            price_home = decimal_odds
        elif (
            away.lower() in participant
            or "away" in participant
            or outcome_type == "OT_TWO"
        ):
            price_away = decimal_odds

    if price_home or price_away:
        return {
            "event_id": event_id,
            "market": "h2h",
            "line": None,
            "total": None,
            "price_home": price_home,
            "price_away": price_away,
            "price_over": None,
            "price_under": None,
            "ts": int(time.time()),
            "book": "betrivers",
            "brand": "betrivers",
            "sport": sport.lower() if sport else "unknown",
            "league": league,
            "home_team": home,
            "away_team": away,
        }

    return None


def process_spread_market(
    bet_offer: Dict, event_id: str, home: str, away: str, sport: str, league: str
) -> List[Dict]:
    """Process spread/handicap markets"""
    rows = []
    outcomes = bet_offer.get("outcomes", [])

    for outcome in outcomes:
        if not isinstance(outcome, dict):
            continue

        odds_val = outcome.get("odds")
        if not odds_val:
            continue

        # Get the line/handicap value
        line = outcome.get("line") or outcome.get("handicap")
        if line is None:
            # Try to extract from label
            label = outcome.get("englishLabel", "")
            import re

            line_match = re.search(r"([+-]?\d+\.?\d*)", label)
            if line_match:
                line = float(line_match.group(1))

        decimal_odds = float(odds_val) / 1000 if odds_val > 100 else float(odds_val)

        # Determine if home or away
        participant = outcome.get("participant", "").lower()
        is_home = home.lower() in participant or "home" in participant

        rows.append(
            {
                "event_id": event_id,
                "market": "spread",
                "line": float(line) if line else None,
                "total": None,
                "price_home": decimal_odds if is_home else None,
                "price_away": decimal_odds if not is_home else None,
                "price_over": None,
                "price_under": None,
                "ts": int(time.time()),
                "book": "betrivers",
                "brand": "betrivers",
                "sport": sport.lower() if sport else "unknown",
                "league": league,
                "home_team": home,
                "away_team": away,
            }
        )

    return rows


def process_totals_market(
    bet_offer: Dict, event_id: str, home: str, away: str, sport: str, league: str
) -> List[Dict]:
    """Process totals/over-under markets"""
    rows = []
    outcomes = bet_offer.get("outcomes", [])

    # Try to get the total line
    total_line = None
    for outcome in outcomes:
        if isinstance(outcome, dict):
            line = outcome.get("line") or outcome.get("total")
            if line:
                total_line = float(line)
                break

    price_over = None
    price_under = None

    for outcome in outcomes:
        if not isinstance(outcome, dict):
            continue

        odds_val = outcome.get("odds")
        if not odds_val:
            continue

        decimal_odds = float(odds_val) / 1000 if odds_val > 100 else float(odds_val)

        label = outcome.get("englishLabel", "").lower()
        if "over" in label:
            price_over = decimal_odds
        elif "under" in label:
            price_under = decimal_odds

    if price_over or price_under:
        rows.append(
            {
                "event_id": event_id,
                "market": "total",
                "line": None,
                "total": total_line,
                "price_home": None,
                "price_away": None,
                "price_over": price_over,
                "price_under": price_under,
                "ts": int(time.time()),
                "book": "betrivers",
                "brand": "betrivers",
                "sport": sport.lower() if sport else "unknown",
                "league": league,
                "home_team": home,
                "away_team": away,
            }
        )

    return rows


def process_generic_market(
    bet_offer: Dict,
    event_id: str,
    home: str,
    away: str,
    sport: str,
    league: str,
    market_type: str,
) -> List[Dict]:
    """Process any other market types using new schema"""
    rows = []
    outcomes = bet_offer.get("outcomes", [])

    for outcome in outcomes:
        if not isinstance(outcome, dict):
            continue

        odds_val = outcome.get("odds")
        if not odds_val:
            continue

        decimal_odds = float(odds_val) / 1000 if odds_val > 100 else float(odds_val)
        american_odds = outcome.get("oddsAmerican")

        # Use new schema for generic markets
        rows.append(
            {
                "event_id": event_id,
                "market": market_type,
                "outcome_name": outcome.get("englishLabel", ""),
                "outcome_price": decimal_odds,
                "outcome_point": outcome.get("line"),
                "american_odds": american_odds,
                "ts": int(time.time()),
                "book": "betrivers",
                "brand": "betrivers",
                "sport": sport.lower() if sport else "unknown",
                "league": league,
                "home_team": home,
                "away_team": away,
            }
        )

    return rows

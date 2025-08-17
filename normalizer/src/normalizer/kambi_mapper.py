"""Kambi data mapper for normalizer."""

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Any, Optional, Set
from prometheus_client import Counter

k_rows = Counter("kambi_norm_rows_total", "normalized odds rows")
k_events = Counter("kambi_norm_events_total", "normalized events seen")
k_skip = Counter("kambi_norm_skipped_total", "skips by reason", ["reason"])
k_miss = Counter(
    "kambi_norm_market_map_miss_total", "unmapped market labels", ["label"]
)

_EVENT_ID_PAT = re.compile(r"/event[s]?/(\d+)")


def _decimal_to_american(d: Decimal) -> Optional[int]:
    """Convert Kambi decimal odds (scaled by 1000) to American odds."""
    try:
        # Kambi uses scaled integers (multiply by 1000), so convert back to decimal
        if isinstance(d, (int, float)) and d > 100:
            d = d / 1000.0
        d = float(d)
        if d <= 1:
            return None
        if d >= 2:
            result = int(round((d - 1) * 100))
        else:
            # 1 < d < 2
            result = int(round(-100 / (d - 1)))

        return result
    except Exception:
        return None


def _extract_event_ids(envelope_url: str, data: Dict[str, Any]) -> Set[str]:
    ids: Set[str] = set()
    for key in ("eventId", "event_id"):
        v = data.get(key)
        if isinstance(v, (int, str)):
            ids.add(str(v))
    ev = data.get("event") or {}
    if isinstance(ev, dict):
        v = ev.get("id")
        if isinstance(v, (int, str)):
            ids.add(str(v))
    for bo in data.get("betOffers") or []:
        v = bo.get("eventId") or (bo.get("event") or {}).get("id")
        if isinstance(v, (int, str)):
            ids.add(str(v))
        for oc in bo.get("outcomes") or []:
            v = oc.get("eventId")
            if isinstance(v, (int, str)):
                ids.add(str(v))
    m = _EVENT_ID_PAT.search(envelope_url or "")
    if m:
        ids.add(m.group(1))
    return ids


def _label_to_market(label: str) -> Optional[str]:
    lower_label = (label or "").lower()
    if any(x in lower_label for x in ["moneyline", "h2h", "1x2"]):
        return "h2h"
    if (
        "spread" in lower_label
        or "point spread" in lower_label
        or "handicap" in lower_label
    ):
        return "spreads"
    if "total" in lower_label or "over/under" in lower_label or "totals" in lower_label:
        return "totals"
    return None


def _emit_rows_from_betoffers(
    book: str, league: str, event_id: str, betOffers: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for bo in betOffers or []:
        label = (bo.get("criterion") or {}).get("label") or ""
        market = _label_to_market(label)
        if not market:
            k_miss.labels(label or "unknown").inc()
            continue
        for oc in bo.get("outcomes") or []:
            price_dec = oc.get("odds") or oc.get("oddsDecimal") or oc.get("decimalOdds")
            line = oc.get("line") or (bo.get("criterion") or {}).get("label2")
            try:
                price_amer = (
                    _decimal_to_american(Decimal(str(price_dec)))
                    if price_dec is not None
                    else None
                )
            except (InvalidOperation, TypeError):
                price_amer = None
            side = (oc.get("label") or oc.get("participant") or "").lower()
            # map side → home/away if obvious
            is_home = "home" in side
            is_away = "away" in side
            rows.append(
                {
                    "book": book,
                    "league": league,
                    "event_id": event_id,
                    "market": market,
                    "line": (
                        float(line)
                        if isinstance(line, (int, float, str))
                        and str(line).replace(".", "", 1).lstrip("-").isdigit()
                        else None
                    ),
                    "price_home": price_amer if is_home else None,
                    "price_away": price_amer if is_away else None,
                    "total": (
                        float(line)
                        if market == "totals" and line not in (None, "")
                        else None
                    ),
                }
            )
    return rows


def extract_event_metadata(envelope, payload, event_id):
    """Extract event metadata from Kambi payload."""
    metadata = {
        "event_id": event_id,
        "sport": None,
        "league": None,
        "home": None,
        "away": None,
        "start_time": None,
    }

    if not event_id or event_id == "unknown":
        return metadata

    try:
        # Look for event metadata in various places in Kambi payload

        # Shape A: Direct event object
        event_obj = payload.get("event") or {}

        # Shape B: liveEvents[].event
        if not event_obj and payload.get("liveEvents"):
            for le in payload.get("liveEvents") or []:
                ev = (le or {}).get("event") or {}
                if str(ev.get("id", "")) == event_id:
                    event_obj = ev
                    break

        # Shape C: events[] array
        if not event_obj and payload.get("events"):
            for ev in payload.get("events") or []:
                if (
                    str(
                        ev.get("id", "")
                        or ev.get("eventId", "")
                        or ev.get("event_id", "")
                    )
                    == event_id
                ):
                    event_obj = ev
                    break

        # Extract metadata from event object
        if event_obj:
            # Sport - try multiple fields
            sport = event_obj.get("sport") or event_obj.get("sportName")
            if sport:
                metadata["sport"] = str(sport).lower()

            # League/competition
            league = (
                (event_obj.get("group") or {}).get("name")
                or event_obj.get("league")
                or event_obj.get("competition")
            )
            if league:
                metadata["league"] = str(league)

            # Teams
            home_name = (
                event_obj.get("homeName")
                or event_obj.get("home")
                or event_obj.get("homeTeam")
            )
            away_name = (
                event_obj.get("awayName")
                or event_obj.get("away")
                or event_obj.get("awayTeam")
            )

            if home_name:
                metadata["home"] = str(home_name)
            if away_name:
                metadata["away"] = str(away_name)

            # Start time
            start_time = (
                event_obj.get("start")
                or event_obj.get("startTime")
                or event_obj.get("start_time")
            )
            if start_time:
                metadata["start_time"] = str(start_time)

        # Try to extract from group context if available
        if not metadata["league"] and payload.get("group"):
            group = payload.get("group") or {}
            group_name = group.get("name") or group.get("label")
            if group_name:
                metadata["league"] = str(group_name)

        # Infer sport from context if missing
        if not metadata["sport"]:
            # Try to infer from URL or envelope context
            envelope_url = envelope.get("url", "")
            if "american_football" in envelope_url or "nfl" in envelope_url.lower():
                metadata["sport"] = "american_football"
            elif "basketball" in envelope_url or "nba" in envelope_url.lower():
                metadata["sport"] = "basketball"
            elif "baseball" in envelope_url or "mlb" in envelope_url.lower():
                metadata["sport"] = "baseball"

        # Default league based on sport if missing
        if metadata["sport"] and not metadata["league"]:
            sport_to_league = {
                "american_football": "NFL",
                "basketball": "NBA",
                "baseball": "MLB",
                "hockey": "NHL",
            }
            metadata["league"] = sport_to_league.get(metadata["sport"])

    except Exception:
        # Best effort - don't fail if metadata extraction fails
        pass

    return metadata


def normalize_kambi_envelope(envelope, now_ts_func):
    import json as _json
    from collections import defaultdict

    rows = []
    raw = envelope.get("payload") or envelope.get("data")
    if isinstance(raw, str):
        try:
            payload = _json.loads(raw)
        except Exception:
            return rows
    else:
        payload = raw or {}

    ts_now = now_ts_func()  # CALL function (bug fix)
    event_id = extract_event_id(envelope, payload) or ""

    # Extract event metadata for upsert
    event_metadata = extract_event_metadata(envelope, payload, event_id)

    def safe_price_conversion(odds_obj):
        """Convert odds to American, return None if invalid or 0."""
        price = price_from_odds_obj(odds_obj or {})
        return price if price not in (None, 0) else None

    def process_bet_offers(bet_offers):
        """Process bet offers and group outcomes appropriately by market type."""
        # Group outcomes by market and line for proper pairing
        market_groups = defaultdict(lambda: defaultdict(list))

        for bo in bet_offers or []:
            crit = (bo.get("criterion") or {}).get("label")
            market = map_market_label(crit)
            if not market:
                continue

            for oc in bo.get("outcomes") or []:
                price = safe_price_conversion(oc.get("odds"))
                if price is None:
                    continue

                line = oc.get("line")
                label = (
                    oc.get("label") or oc.get("participant") or oc.get("name") or ""
                ).lower()

                # Store outcome info for grouping
                outcome_info = {
                    "price": price,
                    "line": line,
                    "label": label,
                    "original_line": line,  # preserve original for totals
                }

                if market == "spreads":
                    # Group by absolute line value for spreads
                    abs_line = abs(float(line)) if line not in (None, "") else 0
                    market_groups[market][abs_line].append(outcome_info)
                elif market == "totals":
                    # Group by line value for totals
                    total_line = float(line) if line not in (None, "") else 0
                    market_groups[market][total_line].append(outcome_info)
                else:  # h2h
                    # Group all h2h outcomes together
                    market_groups[market][0].append(outcome_info)

        # Convert grouped outcomes to rows
        for market, line_groups in market_groups.items():
            for line_key, outcomes in line_groups.items():
                if market == "totals":
                    # Create one row per total line with over/under prices
                    row = {
                        "book": "kambi",
                        "event_id": str(event_id),
                        "market": market,
                        "line": None,
                        "price_home": None,
                        "price_away": None,
                        "price_over": None,
                        "price_under": None,
                        "total": float(line_key) if line_key else None,
                        "ts": ts_now,
                    }

                    for oc in outcomes:
                        label = oc["label"]
                        if "over" in label:
                            row["price_over"] = oc["price"]
                        elif "under" in label:
                            row["price_under"] = oc["price"]

                    # Only add row if we have at least one price
                    if row["price_over"] is not None or row["price_under"] is not None:
                        rows.append(row)

                elif market == "spreads":
                    # Create one row per absolute line with home/away prices
                    row = {
                        "book": "kambi",
                        "event_id": str(event_id),
                        "market": market,
                        "line": None,
                        "price_home": None,
                        "price_away": None,
                        "price_over": None,
                        "price_under": None,
                        "total": None,
                        "ts": ts_now,
                    }

                    # Determine home/away from original line signs and labels
                    for oc in outcomes:
                        label = oc["label"]
                        original_line = oc["original_line"]

                        # Set the line from the first outcome (should be consistent)
                        if row["line"] is None and original_line not in (None, ""):
                            row["line"] = float(original_line)

                        # Determine if this is home or away based on label and line sign
                        if "home" in label or (
                            "away" not in label
                            and original_line
                            and float(original_line) >= 0
                        ):
                            row["price_home"] = oc["price"]
                        elif (
                            "away" in label
                            or original_line
                            and float(original_line) < 0
                        ):
                            row["price_away"] = oc["price"]

                    # Only add row if we have at least one price
                    if row["price_home"] is not None or row["price_away"] is not None:
                        rows.append(row)

                else:  # h2h
                    # Create one row with home/away prices
                    row = {
                        "book": "kambi",
                        "event_id": str(event_id),
                        "market": market,
                        "line": None,
                        "price_home": None,
                        "price_away": None,
                        "price_over": None,
                        "price_under": None,
                        "total": None,
                        "ts": ts_now,
                    }

                    for oc in outcomes:
                        label = oc["label"]
                        if "home" in label:
                            row["price_home"] = oc["price"]
                        elif "away" in label:
                            row["price_away"] = oc["price"]

                    # Only add row if we have at least one price
                    if row["price_home"] is not None or row["price_away"] is not None:
                        rows.append(row)

    # Shape A: betOffers[]
    process_bet_offers(payload.get("betOffers"))

    # Shape B: liveEvents[] with mainBetOffer + betOffers
    for le in payload.get("liveEvents") or []:
        ev = (le or {}).get("event") or {}
        if ev.get("id") and not event_id:
            event_id = str(ev.get("id"))

        bos = []
        mbo = le.get("mainBetOffer")
        if mbo and isinstance(mbo, dict):
            bos.append(mbo)
        bos.extend(le.get("betOffers") or [])

        process_bet_offers(bos)

    # Shape C: events[] with markets/outcomes - convert to betOffers format
    for ev in payload.get("events") or []:
        if (not event_id) and isinstance(ev, dict):
            eid = ev.get("id") or ev.get("eventId") or ev.get("event_id")
            if eid:
                event_id = str(eid)

        # Convert markets to betOffers format
        bet_offers = []
        for mk in ev.get("markets") or []:
            bo = {
                "criterion": {"label": mk.get("name") or mk.get("label")},
                "outcomes": mk.get("outcomes") or [],
            }
            bet_offers.append(bo)

        process_bet_offers(bet_offers)

    # FLAT result only - filter out any rows without prices
    filtered_rows = [
        r
        for r in rows
        if r.get("book") == "kambi"
        and r.get("event_id")
        and any(
            [
                r.get("price_home"),
                r.get("price_away"),
                r.get("price_over"),
                r.get("price_under"),
            ]
        )
    ]

    return filtered_rows, event_metadata


def normalize_kambi_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Legacy wrapper for compatibility - calls new envelope function"""

    def legacy_now_ts():
        import datetime

        return datetime.datetime.utcnow().isoformat()

    rows, event_metadata = normalize_kambi_envelope(payload, legacy_now_ts)
    k_events.inc(len(set(r.get("event_id", "") for r in rows)))
    k_rows.inc(len(rows))

    # Legacy format expected by existing normalizer
    return {
        "source": "kambi",
        "book": "kambi",
        "timestamp": datetime.utcnow().isoformat(),
        "rows": rows,  # Return rows directly for new normalizer flow
        "event_metadata": event_metadata,  # Include event metadata
    }


def extract_event_id(envelope, payload):
    # envelope event_id first
    ev = envelope.get("event_id")
    if ev:
        return str(ev)
    # Kambi betOffers/event structures
    try:
        if isinstance(payload, dict):
            # common places:
            # payload['event']['id'], or payload['betOffers'][0]['event']['id']
            if (
                "event" in payload
                and isinstance(payload["event"], dict)
                and "id" in payload["event"]
            ):
                return str(payload["event"]["id"])
            if "betOffers" in payload and isinstance(payload["betOffers"], list):
                for bo in payload["betOffers"]:
                    try:
                        e = bo.get("event") or {}
                        if "id" in e:
                            return str(e["id"])
                    except Exception:
                        pass
    except Exception:
        pass
    return None


def decimal_to_american(dec):
    """Convert decimal odds to American odds, never return 0."""
    try:
        # Handle Kambi scaled integers
        if isinstance(dec, (int, float)) and dec > 100:
            dec = dec / 1000.0
        d = float(dec)
        if d <= 1.0:
            return None
        if d >= 2.0:
            result = int(round((d - 1) * 100))
        else:
            result = int(round(-100 / (d - 1)))

        return result
    except Exception:
        return None


def parse_american(v):
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            return int(v)
        if isinstance(v, str):
            v = v.strip()
            if v.startswith("+"):
                v = v[1:]
            return int(v)
    except Exception:
        return None


def price_from_odds_obj(odds):
    """Extract price from odds object, never return 0."""
    if not isinstance(odds, dict):
        return None

    # Try American odds first
    am = parse_american(odds.get("american"))
    if am is not None and am != 0:
        return am

    # Support decimal as float or scaled int (Kambi uses scaled by 1000)
    dec = odds.get("decimal") or odds.get("trueOdds")
    if dec is not None:
        result = decimal_to_american(dec)
        return result if result not in (None, 0) else None

    return None


def map_market_label(label: str):
    lab = (label or "").lower()
    if "spread" in lab or "handicap" in lab:
        return "spreads"
    if "total" in lab or "over/under" in lab or "over under" in lab:
        return "totals"
    if (
        "moneyline" in lab
        or "money line" in lab
        or "moneyline 3-way" in lab
        or "3way" in lab
        or "match winner" in lab
        or "h2h" in lab
    ):
        return "h2h"
    return None

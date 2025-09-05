# -*- coding: utf-8 -*-
from __future__ import annotations
import re
from typing import Any, Dict, Iterable, List, Optional

# Hostname -> brand map (extend as needed)
BRAND_HOST_MAP = {
    "pa.betrivers.com": "betrivers",
    "on.betrivers.com": "betrivers",
    "pa.sugarhouse.com": "sugarhouse",
    "pa.betparx.com": "betparx",
    "nj.betparx.com": "betparx",
    "betrivers.com": "betrivers",
    "sugarhouse.com": "sugarhouse",
    "kambi.com": "kambi",
}


def _infer_brand_from_host(host: str) -> str:
    if not host:
        return "unknown"
    # Exact map first
    if host in BRAND_HOST_MAP:
        return BRAND_HOST_MAP[host]
    # Wildcard-ish suffix handling
    if host.endswith(".betparx.com"):
        return "betparx"
    if host.endswith(".betrivers.com") or host.endswith(".on.betrivers.com"):
        return "betrivers"
    if host.endswith(".sugarhouse.com"):
        return "sugarhouse"
    return "unknown"


# ---------- Helpers ----------


def _to_american_from_decimal(decimal_odds: Optional[float]) -> Optional[int]:
    if not decimal_odds or decimal_odds <= 1.0:
        return None
    # American from decimal
    if decimal_odds >= 2.0:
        return int(round((decimal_odds - 1.0) * 100))
    else:
        return int(round(-100 / (decimal_odds - 1.0)))


def _to_american_from_kambi_scaled(value: Optional[int]) -> Optional[int]:
    # Kambi sometimes provides integer odds scaled by 1000 for decimal
    if value is None:
        return None
    dec = value / 1000.0
    return _to_american_from_decimal(dec)


def _clean_str(x: Optional[str], default: str = "unknown") -> str:
    x = (x or "").strip()
    return x if x else default


def _market_name(criterion_label: str) -> Optional[str]:
    s = (criterion_label or "").lower()
    if "point spread" in s or "spread" in s or "handicap" in s:
        return "spreads"
    if "total" in s or "over/under" in s or "o/u" in s:
        return "totals"
    if "moneyline" in s or "1x2" in s or "match winner" in s or "winner" in s:
        return "h2h"
    return None


def _side_from_label(lbl: str, index: int = 0) -> str:
    s = (lbl or "").lower()
    if "over" in s:
        return "over"
    if "under" in s:
        return "under"
    if "home" in s:
        return "home"
    if "away" in s:
        return "away"
    # For spreads/moneyline, often first outcome is home, second is away
    # This is a common Kambi pattern
    if index == 0:
        return "home"
    if index == 1:
        return "away"
    return "unknown"


def _iter_betoffers_anywhere(obj: Any) -> Iterable[Dict[str, Any]]:
    """
    Walk dict/list and yield any bet-offer like objects:
    - arrays under 'betOffers'
    - single 'mainBetOffer'
    - listView/listview/selections style
    - any dict with 'criterion' and 'outcomes'
    """
    stack = [obj]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            # direct bet offer
            if "criterion" in cur and "outcomes" in cur:
                yield cur
            # containers
            for k, v in cur.items():
                if k in ("betOffers", "betoffers") and isinstance(v, list):
                    for it in v:
                        if isinstance(it, dict):
                            yield it
                elif k in ("mainBetOffer", "mainBetoffer") and isinstance(v, dict):
                    yield v
                else:
                    stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)


def _event_meta_from_payload(payload: Dict[str, Any]) -> Dict[str, str]:
    """
    Try multiple shapes:
    - payload.get('event')
    - any dict with keys 'homeName'/'awayName'
    - competition/league names in various nests
    - liveEvents[] with 'event'
    """
    home, away, league, sport = None, None, None, None

    # Common shapes
    ev = payload.get("event") if isinstance(payload, dict) else None
    if isinstance(ev, dict):
        home = ev.get("homeName") or ev.get("home") or ev.get("homeTeam")
        away = ev.get("awayName") or ev.get("away") or ev.get("awayTeam")
        comp = ev.get("competition") or ev.get("tournament") or {}
        league = comp.get("name") if isinstance(comp, dict) else league
        # Sometimes sport nested under event.sport or competition.sport
        sport_obj = (
            ev.get("sport") or comp.get("sport") if isinstance(comp, dict) else None
        )
        if isinstance(sport_obj, dict):
            sport = sport_obj.get("name") or sport

    # liveEvents shape
    if not home or not away:
        live = payload.get("liveEvents")
        if isinstance(live, list) and live:
            e = live[0].get("event") if isinstance(live[0], dict) else None
            if isinstance(e, dict):
                home = home or e.get("homeName") or e.get("home")
                away = away or e.get("awayName") or e.get("away")
                comp = e.get("competition") or {}
                league = league or (
                    comp.get("name") if isinstance(comp, dict) else None
                )
                s2 = e.get("sport") or {}
                if isinstance(s2, dict):
                    sport = sport or s2.get("name")

    # listview / events arrays
    if not league or not (home and away):
        arr = (
            payload.get("events") or payload.get("listview") or payload.get("listView")
        )
        if isinstance(arr, list) and arr:
            e = arr[0]
            if isinstance(e, dict):
                home = home or e.get("homeName") or e.get("home")
                away = away or e.get("awayName") or e.get("away")
                comp = e.get("competition") or {}
                if isinstance(comp, dict):
                    league = league or comp.get("name")
                    s2 = comp.get("sport")
                    if isinstance(s2, dict):
                        sport = sport or s2.get("name")

    # Fall back safely for NOT NULL
    return {
        "home": _clean_str(home, "Home Team"),
        "away": _clean_str(away, "Away Team"),
        "league": _clean_str(league, "unknown"),
        "sport": _clean_str(sport, "unknown"),
    }


def extract_event_id(payload: dict, url: str = "") -> str | None:
    """Bulletproof event ID extraction with comprehensive fallback strategies"""
    # Try many common Kambi shapes
    key_paths = [
        ("event", "id"),
        ("mainBetOffer", "event", "id"),
        ("liveEvent", "id"),
        ("liveEvents", 0, "id"),
        ("events", 0, "id"),
        ("listView", "events", 0, "id"),
        ("id",),
        ("eventId",),
        ("openEventId",),
    ]
    for path in key_paths:
        try:
            v = payload
            for k in path:
                v = v[k]
            if v:
                return str(v)
        except Exception:
            pass

    # Offers/outcomes shapes
    try:
        for bo in payload.get("betOffers") or []:
            ev = bo.get("event") or {}
            if "id" in ev:
                return str(ev["id"])
            if "eventId" in bo:
                return str(bo["eventId"])
            for oc in bo.get("outcomes") or []:
                if "eventId" in oc:
                    return str(oc["eventId"])
    except Exception:
        pass

    # Outcomes at root
    try:
        for oc in payload.get("outcomes") or []:
            if "eventId" in oc:
                return str(oc["eventId"])
    except Exception:
        pass

    # URL fallback: last 6+ digits anywhere in path/query
    try:
        import re

        m = re.search(r"/events?/(\d+)", url) or re.search(r"(?<!\d)(\d{6,})", url)
        if m:
            return m.group(1)
    except Exception:
        pass

    # Deterministic synthetic ID as LAST resort (to avoid 'unknown')
    try:
        brand = infer_brand_from_url(url) or ""
        league = (
            (payload.get("league") or {}).get("name") or payload.get("leagueName") or ""
        )
        home = (
            (payload.get("home") or {}).get("name") or payload.get("homeName") or ""
        ).strip()
        away = (
            (payload.get("away") or {}).get("name") or payload.get("awayName") or ""
        ).strip()
        start = (
            payload.get("start")
            or payload.get("startTime")
            or payload.get("startDate")
            or ""
        )
        s = f"{brand}|{league}|{home}|{away}|{start}"
        import hashlib

        if home or away:
            return "kambi_" + hashlib.sha1(s.encode()).hexdigest()[:16]
    except Exception:
        pass

    return None


def _event_id_from_any(obj: Any) -> Optional[str]:
    """Legacy wrapper for backward compatibility"""
    if isinstance(obj, dict):
        return extract_event_id(obj, "")
    return None


def infer_brand_from_url(url: str) -> str:
    try:
        # First try hostname mapping
        host = re.sub(r"^https?://", "", url).split("/")[0]
        brand = _infer_brand_from_host(host)
        if brand != "unknown":
            return brand

        # For Kambi CDN URLs, check path tokens
        if "kambicdn.com" in host or "kambi.com" in host:
            # Brand token mapping for Kambi paths
            brand_tokens = {
                "rsi2uspa": "betrivers",  # BetRivers PA
                "rsi2usnj": "betrivers",  # BetRivers NJ
                "rsi2uson": "betrivers",  # BetRivers ON
                "sg2uspa": "sugarhouse",  # SugarHouse PA
                "sg2usnj": "sugarhouse",  # SugarHouse NJ
                "bp2uspa": "betparx",  # BetParx PA
                "bp2usnj": "betparx",  # BetParx NJ
            }

            # Check for any brand token in the URL
            for token, brand_name in brand_tokens.items():
                if token in url:
                    return brand_name

        return "kambi"
    except Exception:
        return "kambi"


# ---------- Public API ----------


def extract_event_metadata(envelope: Dict[str, Any]) -> Dict[str, str]:
    url = str(envelope.get("url", ""))
    brand = infer_brand_from_url(url)
    payload = envelope.get("payload") or envelope.get("data") or {}
    meta = _event_meta_from_payload(payload if isinstance(payload, dict) else {})
    meta["brand"] = _clean_str(brand, "kambi")

    # Require a valid event_id - skip if None and no teams
    event_id = extract_event_id(payload, url)
    if event_id:
        meta["event_id"] = event_id
    else:
        # Check if we have team data to justify synthetic ID
        home = meta.get("home", "")
        away = meta.get("away", "")
        if not home or not away or home == "Home Team" or away == "Away Team":
            # No meaningful data - mark for skipping
            meta["event_id"] = None
        else:
            # Last resort synthetic ID
            import hashlib

            s = f"{brand}|{home}|{away}|{url}"
            meta["event_id"] = "kambi_" + hashlib.sha1(s.encode()).hexdigest()[:16]

    return meta


def normalize_kambi_envelope(envelope: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Return list of row dicts with fields:
      event_id, market, line, total, price_home, price_away, price_over, price_under, ts
    """
    # now_iso = None  # ts added at DB layer if desired
    payload = envelope.get("payload") or envelope.get("data") or {}
    if not isinstance(payload, dict):
        return []

    url = str(envelope.get("url", ""))
    event_id = envelope.get("event_id") or extract_event_id(payload, url)
    if not event_id:
        # Skip processing if we can't extract a valid event_id
        return []

    # Extract brand from envelope - use provided brand or extract from URL
    brand = envelope.get("brand") or extract_brand(envelope)

    rows: List[Dict[str, Any]] = []
    for bo in _iter_betoffers_anywhere(payload):
        try:
            crit = bo.get("criterion") or {}
            mkt = _market_name(_clean_str(crit.get("label", "")))
            if not mkt:
                continue

            # line / total from betOffer or outcome (Kambi often uses "line"/"criterion" or "variant")
            line = None
            total = None
            # common places - try bet offer level first
            if "line" in bo and isinstance(bo["line"], (int, float)):
                # If looks scaled (thousands), preserve raw here; pretty API will scale
                line = (
                    int(bo["line"])
                    if isinstance(bo["line"], int)
                    else int(round(float(bo["line"])))
                )
            # totals frequently put number as "line" too; we set total for totals market
            if mkt == "totals":
                total = line
                line = None

            outs = bo.get("outcomes") or []
            for idx, oc in enumerate(outs):
                # Check for line/total at outcome level if not found at bet offer level
                outcome_line = line
                outcome_total = total
                if (
                    outcome_line is None
                    and "line" in oc
                    and isinstance(oc["line"], (int, float))
                ):
                    outcome_line = (
                        int(oc["line"])
                        if isinstance(oc["line"], int)
                        else int(round(float(oc["line"])))
                    )
                    if mkt == "totals":
                        outcome_total = outcome_line
                        outcome_line = None

                # figure side and odds
                side = _side_from_label(_clean_str(oc.get("label", "")), idx)
                american = None

                # Many shapes: oc.get('odds', {'american': int, 'decimal': float or scaled int})
                odds_obj = oc.get("odds")
                if isinstance(odds_obj, dict):
                    if "american" in odds_obj and odds_obj["american"] not in (None, 0):
                        american = int(odds_obj["american"])
                    elif "decimal" in odds_obj and odds_obj["decimal"] not in (None, 0):
                        if isinstance(odds_obj["decimal"], int):
                            american = _to_american_from_kambi_scaled(
                                odds_obj["decimal"]
                            )
                        else:
                            american = _to_american_from_decimal(
                                float(odds_obj["decimal"])
                            )
                elif isinstance(odds_obj, (int, float)) and odds_obj > 0:
                    # Direct Kambi scaled odds (common case)
                    american = _to_american_from_kambi_scaled(int(odds_obj))

                # Also check outcome oddsAmerican field directly
                if american is None:
                    odds_american = oc.get("oddsAmerican")
                    if odds_american and odds_american not in (None, 0):
                        try:
                            american = int(odds_american)
                        except (ValueError, TypeError):
                            pass

                # Some feeds provide 'fraction' or scaled int elsewhere — try generic scaled value
                if american is None:
                    scaled = oc.get("oddsAmericanScaled") or oc.get("oddsDecimalScaled")
                    if isinstance(scaled, int):
                        american = _to_american_from_kambi_scaled(scaled)

                # Skip if still invalid
                if american is None:
                    continue

                row = {
                    "book": brand,
                    "event_id": str(event_id),
                    "market": mkt,
                    "line": outcome_line,
                    "total": outcome_total,
                    "price_home": None,
                    "price_away": None,
                    "price_over": None,
                    "price_under": None,
                }

                if mkt == "h2h":
                    if side == "home":
                        row["price_home"] = american
                    elif side == "away":
                        row["price_away"] = american
                    else:
                        # If we can't tell, skip ambiguous h2h outcome
                        continue
                elif mkt == "spreads":
                    # spreads: two sides by absolute line; assume home/away
                    if side == "home":
                        row["price_home"] = american
                    elif side == "away":
                        row["price_away"] = american
                    else:
                        continue
                elif mkt == "totals":
                    if side == "over":
                        row["price_over"] = american
                    elif side == "under":
                        row["price_under"] = american
                    else:
                        continue
                else:
                    continue

                rows.append(row)
        except Exception:
            # skip malformed bet offer safely
            continue

    # If we generated rows where only one side appeared (e.g., only price_home),
    # we still keep them — API can filter empties, but DB wants actual prices.
    return rows


def extract_brand(envelope: Dict[str, Any]) -> str:
    """Extract brand from envelope using token and host fallback"""
    # Token-based mapping with precedence
    token_map = {
        "rsi2uspa": "betrivers",
        "rsi2usnj": "betrivers",
        "rsiusnj": "betrivers",
        "rsi2uson": "betrivers",
        "sg2uspa": "sugarhouse",
        "sg2usnj": "sugarhouse",
        "bp2uspa": "betparx",
        "bp2usnj": "betparx",
        "ub2uspa": "unibet",
        "ub2usnj": "unibet",
    }

    # Try to extract token from URL
    url = envelope.get("offering_url") or envelope.get("url") or ""
    for token, brand in token_map.items():
        if token in url:
            return brand

    # Fallback to host-based mapping
    page_host = envelope.get("page_host", "")
    if page_host:
        return _infer_brand_from_host(page_host)

    # Last resort - extract from any URL in envelope
    for url_field in ["page_url", "ws_url", "offering_url"]:
        url_val = envelope.get(url_field, "")
        if url_val and "//" in url_val:
            try:
                host = url_val.split("//")[1].split("/")[0].split("?")[0].lower()
                brand = _infer_brand_from_host(host)
                if brand != "unknown":
                    return brand
            except:
                pass

    return envelope.get("brand_hint", "kambi")

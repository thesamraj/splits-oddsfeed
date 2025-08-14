"""Kambi data mapper for normalizer."""

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Any, Optional, Tuple, Set
from prometheus_client import Counter

k_rows = Counter("kambi_norm_rows_total", "normalized odds rows")
k_events = Counter("kambi_norm_events_total", "normalized events seen")
k_skip = Counter("kambi_norm_skipped_total", "skips by reason", ["reason"])
k_miss = Counter(
    "kambi_norm_market_map_miss_total", "unmapped market labels", ["label"]
)

_EVENT_ID_PAT = re.compile(r"/event[s]?/(\d+)")


def _decimal_to_american(d: Decimal) -> Optional[int]:
    try:
        if d <= 1:
            return None
        if d >= 2:
            return int(round((d - 1) * 100))
        # 1 < d < 2
        return int(round(-100 / (d - 1)))
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


def normalize_kambi_envelope(
    envelope: Dict[str, Any], default_league: str = "NFL"
) -> Tuple[List[Dict[str, Any]], Set[str]]:
    """
    Returns (rows, event_ids_seen)
    Envelope:
      { source: "kambi", url: "...", status: 200, ts: "...", data: {...} }
    """
    book = "kambi"
    url = envelope.get("url", "")
    data = envelope.get("data") or {}
    ev_ids = _extract_event_ids(url, data)
    rows: List[Dict[str, Any]] = []

    # Case A: betOffers root
    if isinstance(data.get("betOffers"), list) and data.get("betOffers"):
        league = default_league
        for eid in ev_ids or {"unknown"}:
            rows.extend(_emit_rows_from_betoffers(book, league, eid, data["betOffers"]))

    # Case B: events with markets/outcomes
    for evlist_key in ("events",):
        if isinstance(data.get(evlist_key), list):
            for ev in data[evlist_key]:
                eid = str(ev.get("id") or ev.get("eventId") or "")
                if eid:
                    ev_ids.add(eid)
                # markets flavor
                if isinstance(ev.get("markets"), list):
                    betOffers = []
                    for mk in ev["markets"]:
                        bo = {
                            "criterion": {"label": mk.get("label")},
                            "outcomes": mk.get("outcomes"),
                        }
                        betOffers.append(bo)
                    rows.extend(
                        _emit_rows_from_betoffers(
                            book, default_league, eid or "unknown", betOffers
                        )
                    )

    # Case C: group/list responses (no odds) → nothing to emit, rely on fan-out to fetch /event/{id}.json
    return rows, ev_ids


def normalize_kambi_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Legacy wrapper for compatibility - calls new envelope function"""
    rows, event_ids = normalize_kambi_envelope(payload)
    k_events.inc(len(event_ids))
    k_rows.inc(len(rows))

    # Legacy format expected by existing normalizer
    return {
        "source": "kambi",
        "book": "kambi",
        "timestamp": datetime.utcnow().isoformat(),
        "rows": rows,  # Return rows directly for new normalizer flow
    }

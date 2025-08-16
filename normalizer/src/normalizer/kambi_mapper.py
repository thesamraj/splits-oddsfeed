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


def normalize_kambi_envelope(envelope, now_ts_func):
    import json as _json

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

    def add_row(market, line, price, side_hint=None):
        row = {
            "book": "kambi",
            "event_id": str(event_id),
            "market": market,
            "line": float(line) if line not in (None, "") else None,
            "price_home": None,
            "price_away": None,
            "total": (
                float(line) if market == "totals" and line not in (None, "") else None
            ),
            "ts": ts_now,
        }
        # distribute price to home/away if possible
        if side_hint == "home":
            row["price_home"] = price
        elif side_hint == "away":
            row["price_away"] = price
        else:
            if row["price_home"] is None:
                row["price_home"] = price
            else:
                row["price_away"] = price
        rows.append(row)

    # Shape A: betOffers[]
    for bo in payload.get("betOffers") or []:
        crit = (bo.get("criterion") or {}).get("label")
        market = map_market_label(crit)
        if not market:
            continue
        for oc in bo.get("outcomes") or []:
            price = price_from_odds_obj(oc.get("odds") or {})
            if price is None:
                continue
            line = oc.get("line")
            label = (
                oc.get("label") or oc.get("participant") or oc.get("name") or ""
            ).lower()
            side = "home" if "home" in label else ("away" if "away" in label else None)
            add_row(market, line, price, side)

    # Shape B: liveEvents[] with mainBetOffer + betOffers
    for le in payload.get("liveEvents") or []:
        ev = (le or {}).get("event") or {}
        if ev.get("id") and not event_id:
            event_id = str(ev.get("id"))
        mbo = le.get("mainBetOffer")
        bos = []
        if mbo and isinstance(mbo, dict):
            bos.append(mbo)
        bos.extend(le.get("betOffers") or [])
        for bo in bos:
            market = map_market_label((bo.get("criterion") or {}).get("label"))
            if not market:
                continue
            for oc in bo.get("outcomes") or []:
                price = price_from_odds_obj(oc.get("odds") or {})
                if price is None:
                    continue
                line = oc.get("line")
                label = (
                    oc.get("label") or oc.get("participant") or oc.get("name") or ""
                ).lower()
                side = (
                    "home" if "home" in label else ("away" if "away" in label else None)
                )
                add_row(market, line, price, side)

    # Shape C: events[] with markets/outcomes
    for ev in payload.get("events") or []:
        if (not event_id) and isinstance(ev, dict):
            eid = ev.get("id") or ev.get("eventId") or ev.get("event_id")
            if eid:
                event_id = str(eid)
        for mk in ev.get("markets") or []:
            market = map_market_label(mk.get("name") or mk.get("label"))
            if not market:
                continue
            for oc in mk.get("outcomes") or []:
                price = price_from_odds_obj(oc.get("odds") or {})
                if price is None:
                    continue
                line = oc.get("line")
                label = (
                    oc.get("label") or oc.get("participant") or oc.get("name") or ""
                ).lower()
                side = (
                    "home" if "home" in label else ("away" if "away" in label else None)
                )
                add_row(market, line, price, side)

    # FLAT result only
    return [r for r in rows if r.get("book") == "kambi" and r.get("event_id")]


def normalize_kambi_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Legacy wrapper for compatibility - calls new envelope function"""

    def legacy_now_ts():
        import datetime

        return datetime.datetime.utcnow().isoformat()

    rows = normalize_kambi_envelope(payload, legacy_now_ts)
    k_events.inc(len(set(r.get("event_id", "") for r in rows)))
    k_rows.inc(len(rows))

    # Legacy format expected by existing normalizer
    return {
        "source": "kambi",
        "book": "kambi",
        "timestamp": datetime.utcnow().isoformat(),
        "rows": rows,  # Return rows directly for new normalizer flow
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
    try:
        d = float(dec)
        if d <= 1.0:
            return 0
        if d >= 2.0:
            return int(round((d - 1) * 100))
        return int(round(-100 / (d - 1)))
    except Exception:
        return 0


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
    if not isinstance(odds, dict):
        return None
    am = parse_american(odds.get("american"))
    if am is not None:
        return am
    # Support decimal as float or scaled int
    dec = odds.get("decimal")
    return decimal_to_american(dec)


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

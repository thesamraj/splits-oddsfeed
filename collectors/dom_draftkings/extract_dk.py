import re
import json
import time
from html import unescape

NEXT_RE = re.compile(
    r'<script[^>]+id="__NEXT_DATA__"[^>]*>\s*(\{.*?\})\s*</script>', re.S
)
APOLLO_RE = re.compile(r"__APOLLO_STATE__\s*=\s*(\{.*?\});", re.S)
BLOB_RE = re.compile(
    r'\{[^{}]*"(?:offerCategories|outcomes|americanOdds|oddsAmerican|homeTeam|awayTeam|eventId|startTime)"[^{}]*\}+',
    re.S,
)


def parse_struct(html: str):
    html = unescape(html)
    for finder, label in ((NEXT_RE, "next_data"), (APOLLO_RE, "apollo_state")):
        m = finder.search(html)
        if m:
            try:
                j = json.loads(m.group(1))
                payload = harvest_from_dk(j)
                if payload["events"]:
                    return payload, label
            except:
                pass
    # JSON blob sweep
    events = []
    for raw in BLOB_RE.findall(html)[:150]:
        try:
            j = json.loads(raw)
            events += harvest_from_dk(j)["events"]
        except:
            pass
    if events:
        return {
            "events": dedupe_events(events),
            "ts": time.time(),
            "source": "json_blobs",
        }, "json_blobs"
    # last resort: no structured data
    return {"events": [], "ts": time.time(), "source": "none"}, "none"


def harvest_from_dk(j):
    events = []

    def walk(x):
        if isinstance(x, dict):
            # DraftKings common shapes
            # 1) offerCategories -> … -> offers -> outcomes
            if "offerCategories" in x:
                events.extend(extract_offers_tree(x))
            # 2) explicit event node
            if ("homeTeam" in x or "awayTeam" in x) and (
                "eventId" in x or "startTime" in x
            ):
                ev = normalize_event(x)
                if ev and ev["markets"]:
                    events.append(ev)
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(j)
    return {"events": dedupe_events(events)}


def extract_offers_tree(node):
    out = []
    # expected path: offerCategories[*].offerSubcategoryDescriptors[*].offerSubcategory.offers[*].outcomes[*]
    try:
        cats = node.get("offerCategories", [])
        for c in cats:
            descs = c.get("offerSubcategoryDescriptors", [])
            for d in descs:
                sub = d.get("offerSubcategory") or {}
                for offer in sub.get("offers", []):
                    ev_meta = {
                        "event_id": str(offer.get("eventId") or ""),
                        "start": offer.get("startTime"),
                    }
                    # outcomes array has team/label and odds
                    outcomes = offer.get("outcomes") or []
                    mkts = []
                    for o in outcomes:
                        price = (
                            o.get("oddsAmerican")
                            or o.get("americanOdds")
                            or (o.get("displayOdds") or {}).get("american")
                        )
                        sel = (o.get("label") or o.get("participant") or "").lower()
                        line = o.get("line") or (o.get("handicap") or {}).get("value")
                        mkt = guess_market(o, offer)
                        if price is not None and mkt:
                            mkts.append(
                                {
                                    "market": mkt,
                                    "selection": normalize_selection(sel),
                                    "price": to_price(price),
                                    "line": to_num(line),
                                }
                            )
                    if mkts:
                        out.append(
                            {
                                "event_id": ev_meta["event_id"],
                                "home": offer.get("homeTeam") or offer.get("home"),
                                "away": offer.get("awayTeam") or offer.get("away"),
                                "start": ev_meta["start"],
                                "markets": mkts,
                            }
                        )
    except Exception:
        pass
    return out


def normalize_event(x):
    ev_id = str(x.get("eventId") or x.get("id") or "")
    home, away = x.get("homeTeam") or x.get("home"), x.get("awayTeam") or x.get("away")
    mkts = []
    # moneyline variants
    for k in ("moneyline", "ml", "odds"):
        if isinstance(x.get(k), dict):
            obj = x[k]
            for side in ("home", "away", "draw"):
                v = obj.get(side) or obj.get(f"{side}Odds") or obj.get(f"{side}Price")
                if v is not None:
                    mkts.append(
                        {
                            "market": "h2h",
                            "selection": side,
                            "price": to_price(v),
                            "line": None,
                        }
                    )
    if mkts:
        return {
            "event_id": ev_id,
            "home": home,
            "away": away,
            "start": x.get("startTime"),
            "markets": mkts,
        }
    return None


def guess_market(o, offer):
    lbl = (o.get("label") or "").lower()
    if "moneyline" in (offer.get("label", "").lower() + lbl):
        return "h2h"
    if any(k in lbl for k in ("spread", "handicap", "point spread")):
        return "spread"
    if any(k in lbl for k in ("total", "over/under", "o/u", "over", "under")):
        return "total"
    # DK often omits labels; infer by participant keywords
    p = (o.get("participant") or "").lower()
    if p in ("home", "away"):
        return "spread"  # best-effort
    if p in ("over", "under"):
        return "total"
    return "h2h"


def normalize_selection(sel):
    if sel in ("team1", "home team", "home"):
        return "home"
    if sel in ("team2", "away team", "away"):
        return "away"
    return sel or "unknown"


def to_price(v):
    try:
        return (
            int(str(v).replace("\u2212", "-").replace("+", "").rjust(3, "0"))
            if str(v).lstrip("+-").isdigit()
            else int(v)
        )
    except:
        try:
            return int(v)
        except:
            return None


def to_num(v):
    try:
        return float(v)
    except:
        return None


def dedupe_events(evs):
    # collapse by (event_id, market, selection, line, price)
    seen = set()
    out = []
    for ev in evs:
        uniq = []
        for m in ev.get("markets", []):
            key = (m["market"], m.get("selection"), m.get("line"), m.get("price"))
            if key in seen:
                continue
            seen.add(key)
            uniq.append(m)
        if uniq:
            ev2 = ev.copy()
            ev2["markets"] = uniq
            out.append(ev2)
    return out

def walk(o):
    if isinstance(o, dict):
        for v in o.values():
            yield from walk(v)
    elif isinstance(o, list):
        for v in o:
            yield from walk(v)


def to_int(v):
    try:
        return int(str(v).replace("\u2212", "-"))
    except:
        return None


def guess_market(lbl):
    s = (lbl or "").lower()
    if "moneyline" in s or "ml" == s:
        return "h2h"
    if any(k in s for k in ("spread", "handicap")):
        return "spread"
    if any(k in s for k in ("total", "over", "under", "o/u")):
        return "total"
    return None


def normalize_sel(x):
    x = (x or "").lower()
    if x in ("home team", "home"):
        return "home"
    if x in ("away team", "away"):
        return "away"
    if x in ("over", "under"):
        return x
    return x or "unknown"


def harvest(j):
    events = []
    # Look for DraftKings shapes: offers -> outcomes with american odds
    for node in walk(j):
        if not isinstance(node, dict):
            continue
        oc = node.get("outcomes") or node.get("selections") or []
        if isinstance(oc, list) and oc:
            ev_id = str(
                node.get("eventId")
                or node.get("event_id")
                or node.get("eventIdNumeric")
                or ""
            )
            home = node.get("homeTeam") or node.get("home")
            away = node.get("awayTeam") or node.get("away")
            start = node.get("startTime") or node.get("start")
            mkts = []
            for o in oc:
                price = (
                    o.get("oddsAmerican")
                    or o.get("americanOdds")
                    or (o.get("displayOdds") or {}).get("american")
                )
                sel = o.get("label") or o.get("participant") or o.get("selection")
                line = o.get("line") or (o.get("handicap") or {}).get("value")
                mkt = guess_market(
                    (node.get("label") or "") + " " + (o.get("label") or "")
                )
                if price is None:
                    continue
                if not mkt:
                    p = (o.get("participant") or "").lower()
                    if p in ("home", "away"):
                        mkt = "spread"
                    elif p in ("over", "under"):
                        mkt = "total"
                    else:
                        mkt = "h2h"
                mkts.append(
                    {
                        "market": mkt,
                        "selection": normalize_sel(sel),
                        "price": to_int(price),
                        "line": (
                            float(line)
                            if isinstance(line, (int, float, str))
                            and str(line).replace(".", "", 1).lstrip("-").isdigit()
                            else None
                        ),
                    }
                )
            if mkts:
                events.append(
                    {
                        "event_id": ev_id,
                        "home": home,
                        "away": away,
                        "start": start,
                        "markets": mkts,
                    }
                )
    # Dedupe markets within events
    norm = []
    seen = set()
    for ev in events:
        uniq = []
        for m in ev["markets"]:
            key = (ev["event_id"], m["market"], m["selection"], m["line"], m["price"])
            if key in seen:
                continue
            seen.add(key)
            uniq.append(m)
        if uniq:
            e = ev.copy()
            e["markets"] = uniq
            norm.append(e)
    return norm

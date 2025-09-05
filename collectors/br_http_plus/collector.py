import asyncio
import random
import aiohttp
import os
import json
import time
import sys
import redis

# --- polite rate limiting ---
TOKENS = float(os.getenv("TOKENS", "6"))  # calls per second budget
BURST = float(os.getenv("BURST", "12"))
_refill_ts = time.time()
TOKEN_BUCKET = BURST


def _refill():
    global TOKEN_BUCKET, _refill_ts
    now = time.time()
    TOKEN_BUCKET = min(BURST, TOKEN_BUCKET + (now - _refill_ts) * TOKENS)
    _refill_ts = now


def _take(n=1):
    _refill()
    global TOKEN_BUCKET
    if TOKEN_BUCKET >= n:
        TOKEN_BUCKET -= n
        return
    need = n - TOKEN_BUCKET
    sleep_time = need / max(TOKENS, 0.1)
    time.sleep(sleep_time)
    _refill()
    TOKEN_BUCKET -= n


TOKEN = os.getenv("KAMBI_TOKEN", "rsi2uspa")
BASE = "https://eu.offering-api.kambicdn.com/offering/v2018"
LIST_URL = f"{BASE}/{TOKEN}/event/live/open.json"
BET_URL = f"{BASE}/{TOKEN}/betoffer/event/{{event_id}}.json"
INTERVAL = int(os.getenv("INTERVAL", "20"))
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "120"))  # cap per cycle
CONC = int(os.getenv("CONC", "6"))  # polite concurrency
TIMEOUT = float(os.getenv("TIMEOUT", "8"))
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.kambi")
EVENT_TTL = float(os.getenv("EVENT_TTL", "25"))
SEEN_CACHE = {}  # publish same channel normalizer already uses

r = redis.from_url(REDIS_URL)


def now_ms():
    return int(time.time() * 1000)


async def fetch_json(sess, url):
    try:
        timeout = aiohttp.ClientTimeout(total=TIMEOUT)
        async with sess.get(
            url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}
        ) as resp:
            if resp.status == 429:
                print("Rate limited (429), backing off", flush=True)
                await asyncio.sleep(5 + random.random() * 5)
                return None
            if resp.status != 200:
                print(f"Error {resp.status} for {url}", flush=True)
                return None
            return await resp.json()
    except Exception as e:
        print(f"Fetch error for {url}: {e}", flush=True)
        return None


def attach_offers(event_obj, betoffers_payload):
    # betoffer endpoint returns {"betOffers":[...]} or {"betOffers":[...], ...}
    if not betoffers_payload:
        return event_obj
    offers = (
        betoffers_payload.get("betOffers") or betoffers_payload.get("betoffers") or []
    )
    # embed into the same structure the mapper expects: an item with 'event' + 'betOffers'
    return {
        "event": event_obj.get("event") or event_obj,
        "betOffers": offers,
        "mainBetOffer": event_obj.get("mainBetOffer"),
    }


async def run_once():
    t0 = time.time()
    async with aiohttp.ClientSession() as sess:
        listing = await fetch_json(sess, LIST_URL)
        if not listing:
            print(f"No listing from {LIST_URL}", flush=True)
            return {"published": 0, "events": 0, "ms": int((time.time() - t0) * 1000)}
        live = listing.get("liveEvents") or listing.get("events") or []
        print(f"Found {len(live)} live events", flush=True)
        # trim & rotate: take first N per cycle
        # Filter by TTL to avoid hammering same events
        now = time.time()
        filtered = []
        for e in live[: MAX_EVENTS * 2]:  # Check more to find enough fresh ones
            eid = (e.get("event") or {}).get("id") or e.get("id")
            if not eid:
                continue
            last = SEEN_CACHE.get(eid, 0)
            if now - last >= EVENT_TTL:
                filtered.append(e)
                SEEN_CACHE[eid] = now
                if len(filtered) >= MAX_EVENTS:
                    break
        events = filtered

        async def one(ev):
            ev_id = (ev.get("event") or {}).get("id") or ev.get("id")
            if not ev_id:
                return None
            j = await fetch_json(sess, BET_URL.format(event_id=ev_id))
            return attach_offers(ev, j)

        sem = asyncio.Semaphore(CONC)

        async def guarded(ev):
            async with sem:
                return await one(ev)

        stitched = [x for x in await asyncio.gather(*[guarded(e) for e in events]) if x]
        # publish small batches to keep messages manageable
        batch_size = int(os.getenv("BATCH_SIZE", "25"))
        published = 0
        for i in range(0, len(stitched), batch_size):
            payload = {
                "brand": "betrivers",
                "book": "betrivers",
                "ts": now_ms(),
                "liveEvents": stitched[i : i + batch_size],
            }
            r.publish(CHANNEL, json.dumps(payload))
            published += len(stitched[i : i + batch_size])
        return {
            "published": published,
            "events": len(events),
            "ms": int((time.time() - t0) * 1000),
        }


async def main():
    while True:
        stats = await run_once()
        print(
            f"BR_HTTP_PLUS cycle published={stats['published']} from events={stats['events']} in {stats['ms']}ms",
            flush=True,
        )
        await asyncio.sleep(INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)

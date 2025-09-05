import asyncio
import aiohttp
import json
import time
import redis

TOKEN = "rsi2uspa"
BASE = "https://eu.offering-api.kambicdn.com/offering/v2018"
LIST_URL = f"{BASE}/{TOKEN}/event/live/open.json"
BET_URL = f"{BASE}/{TOKEN}/betoffer/event/{{event_id}}.json"
r = redis.from_url("redis://broker:6379")


async def fetch_json(sess, url):
    try:
        async with sess.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except:
        return None


def attach_offers(event_obj, betoffers_payload):
    if not betoffers_payload:
        return event_obj
    offers = betoffers_payload.get("betOffers") or []
    return {
        "event": event_obj.get("event") or event_obj,
        "betOffers": offers,
        "mainBetOffer": event_obj.get("mainBetOffer"),
    }


async def run_once():
    async with aiohttp.ClientSession() as sess:
        listing = await fetch_json(sess, LIST_URL)
        if not listing:
            return 0
        live = listing.get("liveEvents") or []
        events = live[:10]  # Process first 10

        stitched = []
        for ev in events:
            ev_id = (ev.get("event") or {}).get("id")
            if not ev_id:
                continue
            j = await fetch_json(sess, BET_URL.format(event_id=ev_id))
            stitched.append(attach_offers(ev, j))

        if stitched:
            payload = {
                "brand": "betrivers",
                "book": "betrivers",
                "ts": int(time.time() * 1000),
                "liveEvents": stitched,
            }
            r.publish("odds.raw.kambi", json.dumps(payload))
        return len(stitched)


async def main():
    for _ in range(10):  # Run 10 cycles then exit
        n = await run_once()
        print(f"Published {n} events with betOffers", flush=True)
        await asyncio.sleep(20)


asyncio.run(main())

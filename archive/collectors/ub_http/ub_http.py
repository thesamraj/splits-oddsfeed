#!/usr/bin/env python3
import os
import time
import json
import requests
import redis

TOKENS = [
    t.strip()
    for t in os.getenv("UB_TOKENS", "ub2uspa,ubuspa,ub2usnj,ubusnj").split(",")
    if t.strip()
]
INTERVAL = int(os.getenv("INTERVAL", "20"))
r = redis.from_url(os.getenv("REDIS_URL", "redis://broker:6379/0"))
REF = os.getenv("UB_REFERER", "https://pa.unibet.com/?page=sportsbook#live")
ORG = os.getenv("UB_ORIGIN", "https://pa.unibet.com")
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
HDR = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": REF,
    "Origin": ORG,
    "Connection": "keep-alive",
}


def urls(tokens):
    for t in tokens:
        yield f"https://eu.offering-api.kambicdn.com/offering/v2018/{t}/event/live/open.json"
        yield f"https://eu.offering-api.kambicdn.com/offering/v2018/{t}/event/upcoming.json"


print(f"[UB_HTTP] Starting with tokens: {TOKENS}", flush=True)
while True:
    had_ok = False
    for u in urls(TOKENS):
        try:
            resp = requests.get(u, headers=HDR, timeout=10)
            if resp.status_code == 200 and resp.headers.get(
                "content-type", ""
            ).startswith("application/json"):
                r.publish(
                    "odds.raw.kambi",
                    json.dumps(
                        {
                            "brand_hint": "unibet",
                            "transport": "http",
                            "page_url": u,
                            "frame": resp.text,
                        }
                    ),
                )
                print(f"[UB_HTTP] OK from {u.split('/')[-3]}", flush=True)
                had_ok = True
            else:
                print(
                    f"[UB_HTTP] {resp.status_code} from {u.split('/')[-3]}", flush=True
                )
        except Exception as e:
            print(f"[UB_HTTP] Error: {e}", flush=True)
    time.sleep(INTERVAL if had_ok else INTERVAL + 7)

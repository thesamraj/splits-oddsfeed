#!/usr/bin/env python3
import os
import time
import json
import requests
import redis

r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
TOKENS = os.getenv("UB_TOKENS", "ub2uspa").split(",")
INTERVAL = int(os.getenv("INTERVAL", "20"))
URLS = [
    f"https://eu.offering-api.kambicdn.com/offering/v2018/{t}/event/live/open.json"
    for t in TOKENS
]

print(f"[UB_HTTP] Starting with tokens: {TOKENS}")
while True:
    ok = False
    for u in URLS:
        try:
            resp = requests.get(u, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200 and resp.headers.get(
                "content-type", ""
            ).startswith("application/json"):
                payload = resp.text
                # publish as raw for existing Kambi mapper; hint unibet
                msg = json.dumps(
                    {
                        "brand_hint": "unibet",
                        "transport": "http",
                        "page_url": u,
                        "frame": payload,
                    }
                )
                r.publish("odds.raw.kambi", msg)
                print(f"[UB_HTTP] Published from {u[:50]}")
                ok = True
        except Exception as e:
            print(f"[UB_HTTP] Failed {u[:50]}: {e}")
    time.sleep(INTERVAL if ok else INTERVAL + 7)

import os
import time
import json
import requests
import redis

R = redis.from_url(os.getenv("REDIS_URL", "redis://broker:6379/0"))
CH = "odds.raw.kambi"
TOKENS = [t.strip() for t in os.getenv("TOKENS", "").split(",") if t.strip()]
ENDPTS = [
    "https://eu.offering-api.kambicdn.com/offering/v2018/{tok}/event/live/open.json",
    "https://eu.offering-api.kambicdn.com/offering/v2018/{tok}/events/upcoming.json",
]


def ok_json(u):
    try:
        r = requests.get(
            u,
            headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
            timeout=12,
        )
        return r.status_code == 200 and r.text.strip().startswith("{"), r.text[:200]
    except Exception as e:
        return False, str(e)


hit = False
for tok in TOKENS:
    for e in ENDPTS:
        u = e.format(tok=tok)
        good, _ = ok_json(u)
        print(json.dumps({"try": u, "ok": good}), flush=True)
        if good:
            hit = True
            # Publish once to prove flow, then stop
            R.publish(CH, requests.get(u).text)
            print(json.dumps({"GO_TOKEN": tok}), flush=True)
            raise SystemExit(0)
time.sleep(0.1)
raise SystemExit(1 if not hit else 0)

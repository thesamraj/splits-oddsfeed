#!/usr/bin/env python3
import os
import json
import time
import redis
import requests
from datetime import datetime

# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.stake")
INTERVAL = int(os.getenv("INTERVAL", "20"))

r = redis.from_url(REDIS_URL)


def fetch_events():
    try:
        # Stake API endpoint
        url = "https://api.stake.com/sports/events?sport=american-football&league=nfl"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"Error fetching: {e}", flush=True)
    return None


print("Stake API collector started", flush=True)

while True:
    try:
        data = fetch_events()
        if data:
            events = data.get("data", {}).get("events", [])
            msg = {
                "book": "stake",
                "brand": "stake",
                "timestamp": datetime.utcnow().isoformat(),
                "events": events,
                "raw": data,
            }
            r.publish(CHANNEL, json.dumps(msg))
            print(f"Published {len(events)} events", flush=True)
    except Exception as e:
        print(f"Error: {e}", flush=True)

    time.sleep(INTERVAL)

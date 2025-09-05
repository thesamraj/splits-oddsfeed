#!/usr/bin/env python3
import os
import json
import time
import redis
import requests
from datetime import datetime

# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.betrivers")
INTERVAL = int(os.getenv("INTERVAL", "20"))
TOKEN = os.getenv("KAMBI_TOKEN", "rsi2uspa")
BASE_URL = f"https://eu.offering-api.kambicdn.com/offering/v2018/{TOKEN}"

r = redis.from_url(REDIS_URL)


def fetch_events():
    try:
        # Get live events
        url = f"{BASE_URL}/event/live/open.json"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"Error fetching: {e}", flush=True)
    return None


print("BetRivers API collector started", flush=True)

while True:
    try:
        data = fetch_events()
        if data:
            events = data.get("liveEvents", [])
            msg = {
                "book": "betrivers",
                "brand": "betrivers",
                "timestamp": datetime.utcnow().isoformat(),
                "events": events,
                "liveEvents": events,
                "raw": data,
            }
            r.publish(CHANNEL, json.dumps(msg))
            print(f"Published {len(events)} events", flush=True)
    except Exception as e:
        print(f"Error: {e}", flush=True)

    time.sleep(INTERVAL)

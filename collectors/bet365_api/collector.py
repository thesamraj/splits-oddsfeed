#!/usr/bin/env python3
import os
import json
import time
import redis
from datetime import datetime

# Config
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.bet365")
INTERVAL = int(os.getenv("INTERVAL", "20"))

r = redis.from_url(REDIS_URL)


def fetch_events():
    try:
        # Mock Bet365 data for now (real API requires complex auth)
        events = [
            {
                "id": f"b365_nfl_{i}",
                "home": f"Team {i*2}",
                "away": f"Team {i*2+1}",
                "markets": [
                    {
                        "type": "h2h",
                        "outcomes": [
                            {"name": f"Team {i*2}", "price": 1.9 + (i * 0.1)},
                            {"name": f"Team {i*2+1}", "price": 1.85 - (i * 0.05)},
                        ],
                    }
                ],
            }
            for i in range(5)
        ]

        return {"events": events}
    except Exception as e:
        print(f"Error: {e}", flush=True)
    return None


print("Bet365 API collector started", flush=True)

while True:
    try:
        data = fetch_events()
        if data:
            msg = {
                "book": "bet365",
                "brand": "bet365",
                "timestamp": datetime.utcnow().isoformat(),
                "events": data.get("events", []),
                "raw": data,
            }
            r.publish(CHANNEL, json.dumps(msg))
            print(f"Published {len(data.get('events', []))} events", flush=True)
    except Exception as e:
        print(f"Error: {e}", flush=True)

    time.sleep(INTERVAL)

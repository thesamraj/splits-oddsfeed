#!/usr/bin/env python3
"""
Simple test producer that publishes mock BoltOdds messages to Redis staging channel
"""
import os
import json
import time
import redis
from datetime import datetime

# Load environment
def load_env():
    env_file = '.env'
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value

load_env()

# Connect to Redis
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
if 'rediss://' in redis_url or 'upstash' in redis_url:
    r = redis.from_url(redis_url, ssl_cert_reqs=None)
else:
    r = redis.from_url(redis_url)

print("Publishing test BoltOdds messages to staging channel...")

# Publish test messages
for i in range(5):
    msg = {
        "type": "line_update",
        "ts": datetime.now().isoformat(),
        "action": "line_update",
        "sport": "NFL",
        "event_id": f"test_event_{i}",
        "home_team": f"Home Team {i}",
        "away_team": f"Away Team {i}",
        "book": "draftkings",
        "payload": {
            "lines": {
                "Moneyline": {
                    "home": {"price": 110 + i},
                    "away": {"price": -130 - i}
                }
            }
        }
    }
    
    r.publish('odds.raw.bolt.staging', json.dumps(msg))
    print(f"Published message {i+1}")
    time.sleep(0.5)

print("✓ Published 5 test messages")
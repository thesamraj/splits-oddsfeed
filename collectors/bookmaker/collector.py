import time
import json
import os
import redis
import logging
from datetime import datetime
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, CollectorRegistry, generate_latest

# Config
BOOK = "bookmaker"
PORT = int(os.environ.get('PORT', 8000))
REDIS_URL = os.environ.get('REDIS_URL')
RAW_TAP = os.environ.get('RAW_TAP', 'false').lower() == 'true'

# Logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(BOOK)

# Redis
r = redis.from_url(REDIS_URL) if REDIS_URL else None

# Metrics
registry = CollectorRegistry()
ticks_total = Counter('ticks_total', 'Total collector ticks', ['book'], registry=registry)
collector_up = Gauge('collector_up', 'Collector health', ['book'], registry=registry)
events_last = Gauge('events_last', 'Events published in last tick', ['book'], registry=registry)

# Flask app
app = Flask(__name__)

def publish_canonical(events):
    """Publish events in canonical format"""
    if not r:
        return
    
    try:
        # Convert to canonical format
        canonical_events = []
        
        for event in events:
            canonical = {
                "book": BOOK,
                "sport": event.get("sport", "football"),
                "league": event.get("league", "NFL"),
                "event_id": event.get("event_id", f"{BOOK}_{int(time.time()*1000)}"),
                "home": event.get("home", "Team A"),
                "away": event.get("away", "Team B"),
                "commence_time": datetime.utcnow().isoformat() + 'Z',
                "markets": [],
                "ts": int(time.time() * 1000)
            }
            
            # Add moneyline market
            if "price_home" in event and "price_away" in event:
                canonical["markets"].append({
                    "key": "moneyline",
                    "outcomes": [
                        {"name": "home", "price": event["price_home"]},
                        {"name": "away", "price": event["price_away"]}
                    ]
                })
            
            canonical_events.append(canonical)
        
        # Publish to canonical channel
        channel = f"odds.canon.{BOOK}"
        message = {"book": BOOK, "events": canonical_events}
        r.publish(channel, json.dumps(message))
        log.info(f"Published {len(canonical_events)} events to {channel}")
        
        # Also publish to raw if enabled
        if RAW_TAP:
            raw_channel = f"odds.raw.{BOOK}"
            r.publish(raw_channel, json.dumps({"book": BOOK, "events": events}))
            log.info(f"Published raw to {raw_channel}")
            
    except Exception as e:
        log.error(f"Redis error: {e}")

def fetch_and_parse():
    """Fetch real bookmaker odds (stub for now)"""
    # TODO: Implement real bookmaker API integration
    # For now, return synthetic data
    return [{
        "book": BOOK,
        "sport": "football",
        "league": "NFL",
        "event_id": f"{BOOK}_{int(time.time())}",
        "home": "bookmaker Home",
        "away": "bookmaker Away",
        "price_home": -110,
        "price_away": -110,
        "ts": datetime.utcnow().isoformat()
    }]

def loop():
    while True:
        try:
            evs = fetch_and_parse()
            if evs:
                publish_canonical(evs)
                events_last.labels(book=BOOK).set(len(evs))
                ticks_total.labels(book=BOOK).inc()
                collector_up.labels(book=BOOK).set(1)
            else:
                collector_up.labels(book=BOOK).set(0.5)
        except Exception as e:
            log.error(f"Loop error: {e}")
            collector_up.labels(book=BOOK).set(0)
            events_last.labels(book=BOOK).set(0)
        time.sleep(60)

@app.route('/healthz')
def health():
    return jsonify({"book": BOOK, "status": "ok", "last_ok": time.time(), "errors": 0})

@app.route('/metrics')
def metrics():
    return generate_latest(registry)

if __name__ == '__main__':
    # Start collector loop in background
    import threading
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    
    # Start Flask
    app.run(host='0.0.0.0', port=PORT)
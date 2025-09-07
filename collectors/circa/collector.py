import os, time, json, logging, threading
from datetime import datetime
from flask import Flask, jsonify, Response
import requests
import redis
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST, CollectorRegistry

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

BOOK = os.getenv("BOOK", "unknown")
PORT = int(os.getenv("PORT", "8000"))
REDIS_URL = os.getenv("REDIS_URL")

r = redis.from_url(REDIS_URL) if REDIS_URL else None

registry = CollectorRegistry()
ticks_total = Counter('ticks_total', 'Tick cycles', ['book'], registry=registry)
collector_up = Gauge('collector_up', 'Collector up (1/0)', ['book'], registry=registry)
events_last = Gauge('events_last', 'Events published in last tick', ['book'], registry=registry)

app = Flask(__name__)
last_status = {"status": "init", "last_ok": 0, "errors": 0}

def publish(events):
    if not r: return
    try:
        channel = f"odds.raw.{BOOK}"
        for ev in events:
            r.publish(channel, json.dumps(ev))
    except Exception as e:
        log.error(f"Redis error: {e}")

def fetch_and_parse():
    """Minimal stub - returns synthetic events for testing"""
    return [{
        "book": BOOK,
        "sport": "football",
        "event_id": f"{BOOK}_{int(time.time())}",
        "market": "moneyline",
        "home_price": -110,
        "away_price": -110,
        "ts": datetime.utcnow().isoformat()
    }]

def loop():
    while True:
        try:
            evs = fetch_and_parse()
            if evs:
                publish(evs)
                events_last.labels(BOOK).set(len(evs))
                ticks_total.labels(BOOK).inc()
                collector_up.labels(BOOK).set(1.0)
                last_status.update(status="ok", last_ok=time.time(), errors=0)
        except Exception as e:
            log.error(f"Error: {e}")
            collector_up.labels(BOOK).set(0.0)
            last_status['errors'] = last_status.get('errors', 0) + 1
        time.sleep(30)

@app.route("/health")
@app.route("/healthz")
def healthz():
    return jsonify({"book": BOOK, **last_status})

@app.route("/metrics")
def metrics():
    return Response(generate_latest(registry), mimetype=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    threading.Thread(target=loop, daemon=True).start()
    collector_up.labels(BOOK).set(1.0)
    app.run(host="0.0.0.0", port=PORT, debug=False)

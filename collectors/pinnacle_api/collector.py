import os, time, json, logging, threading
from datetime import datetime
from flask import Flask, jsonify, Response
import requests
import redis
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST, CollectorRegistry

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

BOOK = "pinnacle"
PORT = int(os.getenv("PORT", "8000"))
REAL_ONLY = os.getenv("REAL_ONLY", "true").lower() == "true"
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")

r = redis.from_url(REDIS_URL) if REDIS_URL else None

registry = CollectorRegistry()
ticks_total = Counter('ticks_total', 'Tick cycles', ['book'], registry=registry)
collector_up = Gauge('collector_up', 'Collector up (1/0)', ['book'], registry=registry)
events_last = Gauge('events_last', 'Events published in last tick', ['book'], registry=registry)

app = Flask(__name__)
last_status = {"status": "init", "last_ok": 0, "errors": 0}

def publish(events):
    if not r: 
        log.warning("No Redis connection")
        return
    try:
        channel = f"odds.raw.{BOOK}"
        for ev in events:
            r.publish(channel, json.dumps(ev))
        log.info(f"Published {len(events)} events to {channel}")
    except Exception as e:
        log.error(f"Redis publish error: {e}")

def fetch_and_parse():
    """Fetch Pinnacle odds - uses their public JSON feed"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9"
    }
    out = []
    
    try:
        # Pinnacle provides public odds feed for major sports
        sports_mapping = {
            "29": "football",  # NFL
            "4": "basketball",  # NBA
            "3": "baseball",    # MLB
            "19": "hockey"      # NHL
        }
        
        for sport_id, sport_name in sports_mapping.items():
            try:
                # Public Pinnacle odds endpoint
                url = f"https://guest.api.arcadia.pinnacle.com/0.1/leagues/{sport_id}/markets/straight"
                resp = requests.get(url, headers=headers, timeout=10)
                
                if resp.status_code == 200:
                    data = resp.json()
                    
                    for match in data:
                        event_id = match.get("matchupId")
                        
                        # Extract moneyline prices
                        if "prices" in match:
                            for price in match["prices"]:
                                if price.get("designation") == "Home":
                                    home_ml = price.get("price")
                                elif price.get("designation") == "Away":
                                    away_ml = price.get("price")
                                
                                if home_ml and away_ml:
                                    out.append({
                                        "book": BOOK,
                                        "sport": sport_name,
                                        "event_id": f"pinnacle_{event_id}",
                                        "market": "moneyline",
                                        "home_price": home_ml,
                                        "away_price": away_ml,
                                        "ts": datetime.utcnow().isoformat()
                                    })
                
                time.sleep(0.5)  # Rate limit between sports
                
            except Exception as e:
                log.warning(f"Pinnacle {sport_name} fetch error: {e}")
                
    except Exception as e:
        log.error(f"Pinnacle fetch error: {e}")
    
    return out

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
                log.info(f"Tick complete: {len(evs)} events")
            else:
                log.warning("No events fetched")
                collector_up.labels(BOOK).set(0.5)
        except Exception as e:
            log.error(f"Tick error: {e}")
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
import os
import time
import json
import threading
from flask import Flask, jsonify
import requests
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
INTERVAL = int(os.getenv("INTERVAL", "20"))
PORT = int(os.getenv("PORT", "9129"))

TOKENS = ["rsi2uspa", "rsi2us"]  # BetRivers Kambi tokens (fallback list)
PATHS = [
    "event/upcoming.json",
    "event/upcoming/sportsbook.json",
    "events/upcoming.json",
    "event/live/open.json",
]
BASES = ["https://eu.offering-api.kambicdn.com/offering/v2018"]

app = Flask(__name__)
state = {
    "status": "initializing",
    "published": 0,
    "last_status": None,
    "last_error": None,
    "last_url": None,
    "interval": INTERVAL,
}


def publish(r, url, text, status):
    envelope = {
        "book": "betrivers",
        "brand_hint": "betrivers",
        "transport": "http",
        "url": url,
        "status": status,
        "ts": int(time.time()),
        "payload": text,
    }
    r.publish("odds.raw.kambi", json.dumps(envelope))
    state["published"] += 1


def loop():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    session = requests.Session()
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        "Referer": "https://pa.betrivers.com/",
        "Origin": "https://pa.betrivers.com",
    }
    state["status"] = "active"
    while True:
        ok = False
        for base in BASES:
            for token in TOKENS:
                for path in PATHS:
                    url = f"{base}/{token}/{path}"
                    try:
                        resp = session.get(url, headers=headers, timeout=10)
                        state["last_url"] = url
                        state["last_status"] = resp.status_code
                        if resp.status_code == 200 and resp.text and len(resp.text) > 2:
                            publish(r, url, resp.text, resp.status_code)
                            ok = True
                            break
                    except Exception as e:
                        state["last_error"] = str(e)
                if ok:
                    break
            if ok:
                break
        time.sleep(INTERVAL)


@app.route("/healthz")
def healthz():
    return jsonify(state)


def main():
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()

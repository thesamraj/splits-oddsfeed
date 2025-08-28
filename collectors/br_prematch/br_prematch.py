import os
import json
import time
import threading
from datetime import datetime, timezone
from urllib.parse import urljoin
import requests
import redis
from flask import Flask, jsonify

TOKEN = os.getenv("KAMBI_TOKEN", "rsi2uspa")  # BetRivers
HOST = os.getenv("KAMBI_HOST", "eu.offering-api.kambicdn.com")
BASE = f"https://{HOST}/offering/v2018/{TOKEN}/"
INTERVAL = int(os.getenv("POLL_SECONDS", "20"))
CHANNEL = os.getenv("REDIS_CHANNEL", "odds.raw.kambi")
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
BRAND = "betrivers"
PORT = int(os.getenv("HEALTHZ_PORT", "9129"))

# Candidate endpoints for prematch "upcoming"
CANDIDATES = [
    "event/upcoming.json",
    "event/upcoming/sportsbook.json",
    "events/upcoming.json",
    # Fallbacks (still useful for throughput)
    "event/live/open.json",
]

rcli = redis.from_url(REDIS_URL)
app = Flask(__name__)
state = {
    "brand": BRAND,
    "polls": 0,
    "published": 0,
    "last_status": None,
    "last_ok_ts": None,
    "last_url": None,
    "last_err": None,
}


def fetch_json():
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    }
    sess = requests.Session()
    for path in CANDIDATES:
        url = urljoin(BASE, path)
        try:
            resp = sess.get(url, headers=headers, timeout=10)
            state["last_status"] = resp.status_code
            state["last_url"] = url
            if resp.status_code == 200 and resp.headers.get(
                "content-type", ""
            ).startswith("application/json"):
                return url, resp.json()
        except Exception as e:
            state["last_err"] = str(e)
    return None, None


def extract_events(payload):
    # Accept common Kambi shapes: {liveEvents:[{event:{id,...}}]} OR {events:[{id,...}]}
    out = []
    if not isinstance(payload, dict):
        return out
    # liveEvents path
    le = payload.get("liveEvents") or []
    for node in le if isinstance(le, list) else []:
        ev = node.get("event") or {}
        eid = ev.get("id")
        if eid:
            out.append({"event": ev, "raw": node})
    # events path
    evs = payload.get("events") or []
    for ev in evs if isinstance(evs, list) else []:
        eid = ev.get("id")
        if eid:
            out.append({"event": ev, "raw": ev})
    return out


def publisher_loop():
    while True:
        state["polls"] += 1
        url, js = fetch_json()
        if js:
            events = extract_events(js)
            now_iso = datetime.now(timezone.utc).isoformat()
            for item in events:
                envelope = {
                    "book": "kambi",
                    "brand": BRAND,
                    "token": TOKEN,
                    "source_url": url,
                    "kind": "prematch",
                    "ts": now_iso,
                    "payload": item,  # carry event + raw node
                }
                # Publish as JSON text to odds.raw.kambi
                try:
                    rcli.publish(CHANNEL, json.dumps(envelope))
                    state["published"] += 1
                    state["last_ok_ts"] = now_iso
                except Exception as e:
                    state["last_err"] = f"redis_publish:{e}"
        time.sleep(INTERVAL)


@app.route("/healthz")
def healthz():
    return jsonify(
        {
            "brand": state["brand"],
            "status": "active",
            "polls": state["polls"],
            "published": state["published"],
            "last_status": state["last_status"],
            "last_ok_ts": state["last_ok_ts"],
            "last_url": state["last_url"],
            "last_err": state["last_err"],
            "channel": CHANNEL,
            "host": HOST,
        }
    )


if __name__ == "__main__":
    t = threading.Thread(target=publisher_loop, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=PORT)

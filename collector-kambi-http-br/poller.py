#!/usr/bin/env python3
"""
Minimal Kambi HTTP Poller for BetRivers
Polls Kambi offering API and publishes to Redis
"""
import os
import time
import json
import requests
import redis
from flask import Flask, jsonify
import threading
from datetime import datetime

# Configuration
BRAND = "betrivers"
KAMBI_URL = (
    "https://eu.offering-api.kambicdn.com/offering/v2018/rsi2uspa/event/live/open.json"
)
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))  # seconds
PORT = int(os.getenv("PORT", "9126"))

# Initialize Redis
redis_client = redis.from_url(REDIS_URL)

# Metrics
metrics = {
    "status": "initializing",
    "polls": 0,
    "published": 0,
    "last_success_ts": None,
    "last_error_ts": None,
    "last_error": None,
}

# Flask app for healthz
app = Flask(__name__)


@app.route("/healthz")
def healthz():
    return jsonify(
        {
            "status": metrics["status"],
            "brand": BRAND,
            "polls": metrics["polls"],
            "published": metrics["published"],
            "last_success_ts": metrics["last_success_ts"],
            "last_error_ts": metrics["last_error_ts"],
            "last_error": metrics["last_error"],
        }
    )


def poll_and_publish():
    """Main polling loop"""
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        }
    )

    print(f"[{BRAND}-poller] Starting poller for {KAMBI_URL}")
    print(f"[{BRAND}-poller] Publishing to Redis channel: odds.raw.kambi")
    print(f"[{BRAND}-poller] Poll interval: {POLL_INTERVAL}s")

    metrics["status"] = "active"

    while True:
        try:
            metrics["polls"] += 1
            print(
                f"[{BRAND}-poller] Poll #{metrics['polls']} - {datetime.now().isoformat()}"
            )

            response = session.get(KAMBI_URL, timeout=15)
            response.raise_for_status()

            # Parse JSON to validate
            data = response.json()

            # Create envelope for normalizer
            envelope = {
                "capture_id": f"{BRAND}_{int(time.time() * 1000)}",
                "transport": "http",
                "url": KAMBI_URL,
                "page_url": f"https://pa.{BRAND}.com",
                "page_host": f"pa.{BRAND}.com",
                "ws_url": "",
                "offering_url": KAMBI_URL,
                "brand_hint": BRAND,
                "source_ts_ms": int(time.time() * 1000),
                "received_ts_ms": int(time.time() * 1000),
                "content_type": "application/json",
                "payload": data,
            }

            # Publish to Redis
            message = json.dumps(envelope)
            redis_client.publish("odds.raw.kambi", message)

            metrics["published"] += 1
            metrics["last_success_ts"] = datetime.now().isoformat()
            metrics["last_error"] = None

            print(
                f"[{BRAND}-poller] Published envelope with {len(data.get('events', []))} events"
            )

        except Exception as e:
            error_msg = str(e)
            metrics["last_error"] = error_msg
            metrics["last_error_ts"] = datetime.now().isoformat()
            print(f"[{BRAND}-poller] Error: {error_msg}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    # Start polling thread
    polling_thread = threading.Thread(target=poll_and_publish, daemon=True)
    polling_thread.start()

    # Start Flask healthz server
    print(f"[{BRAND}-poller] Healthz server starting on :{PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False)

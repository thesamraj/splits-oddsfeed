import os
import time
import json
import threading
from flask import Flask, jsonify
import requests
import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
INTERVAL = int(os.getenv("KAMBI_INTERVAL_SEC", "20"))
HEALTHZ_PORT = int(os.getenv("HEALTHZ_PORT", "9134"))
PUBLISH_CHANNEL = os.getenv("KAMBI_PUBLISH_CHANNEL", "odds.envelope.kambi.ub")

# Configurable Kambi parameters
BASE_URL = os.getenv(
    "KAMBI_BASE_URL", "https://eu.offering-api.kambicdn.com/offering/v2018"
)
BRAND = os.getenv("KAMBI_BRAND", "ub2uspa")
ENDPOINTS = os.getenv(
    "KAMBI_ENDPOINTS", "event/upcoming.json,event/live/open.json"
).split(",")

# Fallback endpoints
FALLBACK_ENDPOINTS = ["events/upcoming.json", "event/upcoming/sportsbook.json"]

app = Flask(__name__)
state = {
    "brand": BRAND,
    "channel": PUBLISH_CHANNEL,
    "host": BASE_URL.split("//")[1].split("/")[0],
    "last_err": None,
    "last_ok_ts": None,
    "last_status": None,
    "last_url": None,
    "polls": 0,
    "published": 0,
    "status": "initializing",
}


def publish_envelope(r, url, payload, status_code):
    """Publish HTTP envelope for shim processing"""
    envelope = {
        "book": "unibet",
        "brand_hint": "unibet",
        "transport": "http",
        "url": url,
        "status": status_code,
        "ts": int(time.time()),
        "payload": payload,
    }
    r.publish(PUBLISH_CHANNEL, json.dumps(envelope))
    state["published"] += 1
    print(
        f"UB_COLLECT: Published to {PUBLISH_CHANNEL}, status={status_code}, len={len(payload)}",
        flush=True,
    )


def collector_loop():
    """Main collector loop"""
    r = redis.from_url(REDIS_URL, decode_responses=True)
    session = requests.Session()

    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://unibet.com/",
        "Origin": "https://unibet.com",
    }

    state["status"] = "active"
    print(
        f"UB_COLLECT: Starting loop, brand={BRAND}, interval={INTERVAL}s, port={HEALTHZ_PORT}",
        flush=True,
    )

    while True:
        state["polls"] += 1
        success = False

        # Try primary endpoints first
        all_endpoints = ENDPOINTS + FALLBACK_ENDPOINTS

        for endpoint in all_endpoints:
            url = f"{BASE_URL}/{BRAND}/{endpoint}"
            try:
                start_time = time.time()
                resp = session.get(url, headers=headers, timeout=10)
                elapsed = time.time() - start_time

                state["last_url"] = url
                state["last_status"] = resp.status_code

                print(
                    f"UB_COLLECT: {url} -> {resp.status_code} ({elapsed:.2f}s) len={len(resp.text)}",
                    flush=True,
                )

                if resp.status_code == 200 and resp.text and len(resp.text) > 10:
                    # Log first 2k of response for debugging
                    sample = resp.text[:2000].replace("\n", " ")
                    print(f"UB_COLLECT: SUCCESS sample: {sample}...", flush=True)

                    publish_envelope(r, url, resp.text, resp.status_code)
                    state["last_ok_ts"] = time.strftime("%Y-%m-%dT%H:%M:%S+00:00")
                    state["last_err"] = None
                    success = True
                    break
                elif resp.status_code in [418, 403]:
                    print(
                        f"UB_COLLECT: BLOCKED {resp.status_code} from {url}", flush=True
                    )
                    state["last_err"] = f"HTTP {resp.status_code} blocked"
                else:
                    print(
                        f"UB_COLLECT: BAD_RESPONSE {resp.status_code} from {url}",
                        flush=True,
                    )

            except Exception as e:
                print(f"UB_COLLECT: ERROR {url} -> {e}", flush=True)
                state["last_err"] = str(e)

        if not success:
            print(
                f"UB_COLLECT: All endpoints failed for poll #{state['polls']}",
                flush=True,
            )

        time.sleep(INTERVAL)


@app.route("/healthz")
def healthz():
    """Health check endpoint"""
    return jsonify(state)


def main():
    """Start collector and health server"""
    collector_thread = threading.Thread(target=collector_loop, daemon=True)
    collector_thread.start()

    print(f"UB_COLLECT: Health server starting on port {HEALTHZ_PORT}", flush=True)
    app.run(host="0.0.0.0", port=HEALTHZ_PORT, debug=False)


if __name__ == "__main__":
    main()

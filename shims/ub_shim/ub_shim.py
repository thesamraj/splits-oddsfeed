import os
import json
import time
import sys
import threading
from flask import Flask, jsonify
import redis

# Configuration
SUBSCRIBE_CHANNEL = os.getenv("SUBSCRIBE_CHANNEL", "odds.envelope.kambi.ub")
PUBLISH_CHANNEL = os.getenv("PUBLISH_CHANNEL", "odds.raw.kambi")
BRAND_HINT = os.getenv("BRAND_HINT", "unibet")
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
HEALTHZ_PORT = int(os.getenv("HEALTHZ_PORT", "9136"))

app = Flask(__name__)
state = {
    "brand_hint": BRAND_HINT,
    "subscribe_channel": SUBSCRIBE_CHANNEL,
    "publish_channel": PUBLISH_CHANNEL,
    "processed": 0,
    "republished": 0,
    "errors": 0,
    "status": "initializing",
}


def is_envelope(obj):
    """Check if object is an HTTP envelope with payload"""
    return (
        isinstance(obj, dict) and "payload" in obj and isinstance(obj["payload"], str)
    )


def shim_loop():
    """Main shim processing loop"""
    r = redis.from_url(REDIS_URL, decode_responses=True)

    print(
        f"UB_SHIM: Starting, subscribe={SUBSCRIBE_CHANNEL}, publish={PUBLISH_CHANNEL}",
        flush=True,
    )
    ps = r.pubsub()
    ps.subscribe(SUBSCRIBE_CHANNEL)

    state["status"] = "active"

    while True:
        try:
            msg = ps.get_message(timeout=1.0)
            if not msg or msg.get("type") != "message":
                time.sleep(0.05)
                continue

            data = msg.get("data")
            state["processed"] += 1

            try:
                envelope = json.loads(data)
            except Exception as e:
                state["errors"] += 1
                print(f"UB_SHIM: JSON parse error: {e}", flush=True)
                continue

            if not is_envelope(envelope):
                print("UB_SHIM: Not an envelope, skipping", flush=True)
                continue

            payload = envelope["payload"]

            # Ensure payload looks like JSON
            if not (payload and payload.strip().startswith("{")):
                print("UB_SHIM: Payload not JSON-like, skipping", flush=True)
                continue

            # Republish raw payload to odds.raw.kambi for normalizer
            r.publish(PUBLISH_CHANNEL, payload)
            state["republished"] += 1

            # Enhanced logging
            url = envelope.get("url", "unknown")
            status = envelope.get("status", "unknown")
            brand = envelope.get("brand_hint", "unknown")
            payload_len = len(payload)

            print(
                f"UB_SHIM: Republished {brand} from {url} status={status} len={payload_len}",
                flush=True,
            )

        except Exception as e:
            state["errors"] += 1
            print(f"UB_SHIM: Loop error: {e}", file=sys.stderr, flush=True)
            time.sleep(0.25)


@app.route("/healthz")
def healthz():
    """Health check endpoint"""
    return jsonify(state)


def main():
    """Start shim and health server"""
    shim_thread = threading.Thread(target=shim_loop, daemon=True)
    shim_thread.start()

    print(f"UB_SHIM: Health server starting on port {HEALTHZ_PORT}", flush=True)
    app.run(host="0.0.0.0", port=HEALTHZ_PORT, debug=False)


if __name__ == "__main__":
    main()

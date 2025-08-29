import os
import json
import time
import sys
import redis

CHANNEL = os.getenv("CHANNEL", "odds.raw.kambi")
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
r = redis.from_url(REDIS_URL, decode_responses=True)

print(f"[SHIM] starting; channel={CHANNEL}", flush=True)
ps = r.pubsub()
ps.subscribe(CHANNEL)


def is_wrapper(obj):
    return (
        isinstance(obj, dict) and "payload" in obj and isinstance(obj["payload"], str)
    )


while True:
    try:
        msg = ps.get_message(timeout=1.0)
        if not msg or msg.get("type") != "message":
            time.sleep(0.05)
            continue
        data = msg.get("data")
        # Only unwrap messages that are JSON envelopes with 'payload'
        try:
            obj = json.loads(data)
        except Exception:
            continue
        if not is_wrapper(obj):
            continue
        payload = obj["payload"]
        # Ignore if payload is not JSON-ish
        if not (payload and payload.strip().startswith("{")):
            continue
        # Republish RAW payload for the normalizer
        r.publish(CHANNEL, payload)
        print(
            f"[SHIM] republished RAW from {obj.get('brand_hint','?')} url={obj.get('url','')} len={len(payload)}",
            flush=True,
        )
    except Exception as e:
        print(f"[SHIM] error: {e}", file=sys.stderr, flush=True)
        time.sleep(0.25)

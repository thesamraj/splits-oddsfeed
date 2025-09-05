#!/usr/bin/env python3
import os
import json
import time
import redis

r = redis.from_url(os.getenv("REDIS_URL", "redis://broker:6379/0"))
# Subscribe to UB DOM channel; republish any JSON-looking payloads into odds.raw.kambi for the normalizer path.
SUB = os.getenv("SUB", "odds.raw.unibet_dom")
PUB = "odds.raw.kambi"
p = r.pubsub(ignore_subscribe_messages=True)
p.subscribe(SUB)

print(f"[UB_SHIM] Starting: {SUB} -> {PUB}", flush=True)


def looks_json(s):
    try:
        json.loads(s)
        return True
    except:
        return False


msg_count = 0
while True:
    msg = p.get_message(timeout=1.0)
    if not msg:
        time.sleep(0.2)
        continue
    try:
        data = msg["data"]
        if isinstance(data, bytes):
            data = data.decode("utf-8", "ignore")
        # If frame exists and looks like JSON, pass through. Otherwise wrap as note.
        obj = json.loads(data)
        frame = obj.get("frame")
        if isinstance(frame, str) and looks_json(frame):
            msg_count += 1
            print(f"[UB_SHIM] Forwarding JSON frame #{msg_count}", flush=True)
            r.publish(
                PUB, frame
            )  # feed normalizer with JSON as-is (if it's genuine offering shape)
        else:
            # lightweight heuristic: nothing to do; leave as telemetry only
            transport = obj.get("transport", "unknown")
            print(f"[UB_SHIM] Received {transport} message", flush=True)
    except Exception as e:
        print(f"[UB_SHIM] Error: {e}", flush=True)

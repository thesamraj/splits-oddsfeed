#!/usr/bin/env python3
import os
import json
import time
import redis

r = redis.from_url(os.getenv("REDIS_URL", "redis://broker:6379/0"))
subch = os.getenv("SUB", "odds.raw.unibet_dom")
pubch = "odds.raw.kambi"
p = r.pubsub(ignore_subscribe_messages=True)
p.subscribe(subch)

print(f"[UB_SHIM_FINAL] Starting: {subch} -> {pubch}", flush=True)


def maybe_json(s):
    try:
        json.loads(s)
        return True
    except:
        return False


msg_count = 0
while True:
    m = p.get_message(timeout=1.0)
    if not m:
        time.sleep(0.2)
        continue
    try:
        data = m["data"]
        if isinstance(data, bytes):
            data = data.decode("utf-8", "ignore")
        obj = json.loads(data)
        frame = obj.get("frame")
        if isinstance(frame, str) and maybe_json(frame):
            msg_count += 1
            print(f"[UB_SHIM_FINAL] Forwarding JSON frame #{msg_count}", flush=True)
            r.publish(pubch, frame)
    except Exception:
        pass

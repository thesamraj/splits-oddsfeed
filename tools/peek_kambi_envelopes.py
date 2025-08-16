import os
import json
import time
import redis

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True,
)
p = r.pubsub()
p.subscribe("odds.raw.kambi")
n, limit = 0, 10
out = []
start = time.time()
for msg in p.listen():
    if msg.get("type") != "message":
        continue
    try:
        env = json.loads(msg["data"])
        if isinstance(env, dict) and env.get("source") == "kambi":
            out.append(env)
            n += 1
            if n >= limit:
                break
    except Exception:
        pass
ts = int(time.time())
path = f"artifacts/kambi/envelopes/sample_{ts}.json"
with open(path, "w") as f:
    json.dump(out, f, indent=2)
print(path)

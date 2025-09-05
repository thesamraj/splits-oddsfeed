import time
import json
import redis

r = redis.from_url("redis://broker:6379/0")
p = r.pubsub()
p.psubscribe("odds.raw.dk.structured")
t = time.time()
msg = 0
evs = 0
prices = 0

print("DK struct listener started", flush=True)

for m in p.listen():
    if m["type"] != "pmessage":
        continue
    msg += 1
    try:
        j = json.loads(m["data"])
        evs += len(j.get("events", []))
        for e in j.get("events", []):
            prices += len(e.get("markets", []))
    except:
        pass
    if time.time() - t > 60:
        print(f"DK struct msgs: {msg} events: {evs} markets: {prices}", flush=True)
        msg = evs = prices = 0
        t = time.time()

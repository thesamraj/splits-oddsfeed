import redis

r = redis.from_url("redis://broker:6379/0")
p = r.pubsub()
p.psubscribe("odds.raw.dk.sandbox")
print("DK cache sidecar started", flush=True)
while True:
    m = p.get_message(ignore_subscribe_messages=True, timeout=1.0)
    if not m:
        continue
    try:
        r.lpush("dk:sbx:latest", m["data"])
        r.ltrim("dk:sbx:latest", 0, 9)
        print("Cached message to dk:sbx:latest", flush=True)
    except Exception as e:
        print("cache_err", e, flush=True)

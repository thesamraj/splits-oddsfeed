import redis

r = redis.from_url("redis://broker:6379/0")
p = r.pubsub()
p.psubscribe("odds.raw.dk.structured")
print("DK struct cache started", flush=True)
while True:
    m = p.get_message(ignore_subscribe_messages=True, timeout=1.0)
    if not m:
        continue
    try:
        r.lpush("dk:sbx:structured", m["data"])
        r.ltrim("dk:sbx:structured", 0, 19)
        print("Cached structured message", flush=True)
    except Exception as e:
        print("cache_err", e, flush=True)

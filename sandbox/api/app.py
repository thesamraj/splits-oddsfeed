from flask import Flask, jsonify
import os
import json
import redis
import time

app = Flask(__name__)
r = redis.from_url(os.getenv("REDIS_URL", "redis://broker:6379/0"))


@app.get("/sandbox/dk/latest")
def latest():
    # Keep last N messages in a Redis list as a simple cache
    msgs = r.lrange("dk:sbx:latest", 0, 9)
    out = []
    for m in msgs:
        try:
            out.append(json.loads(m))
        except:
            pass
    return jsonify({"count": len(out), "items": out})


@app.get("/sandbox/dk/ping")
def ping():
    return jsonify({"ok": True, "ts": time.time()})


@app.get("/sandbox/dk/structured")
def dk_struct():
    # Keep last N messages from structured channel
    items = []
    for raw in r.lrange("dk:sbx:structured", 0, 19):
        try:
            items.append(json.loads(raw))
        except:
            pass
    return jsonify({"count": len(items), "items": items})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

import os
import time
import json
import random
import threading
import requests
import redis
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

BRAND = os.getenv("BRAND", "betparx")
STATE = Path(os.getenv("STATE", "/state/var_betparx/state.json"))
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.kambi")
PORT = int(os.getenv("HEALTHZ_PORT", "9124"))
SPLAY_MIN = int(os.getenv("SPLAY_MIN", "90"))
SPLAY_MAX = int(os.getenv("SPLAY_MAX", "150"))

R = redis.from_url(REDIS_URL, decode_responses=True)
state = {
    "messages_received": 0,
    "last_code": None,
    "last_ok_ts": None,
    "last_err_ts": None,
    "cookie_age_sec": None,
}
S = requests.Session()


def load_bootstrap():
    if not STATE.exists():
        return None
    try:
        obj = json.loads(STATE.read_text())
        ua = obj.get("ua") or "Mozilla/5.0"
        S.headers.update(
            {
                "User-Agent": ua,
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.8",
                "Referer": f"https://pa.{BRAND}.com/",
                "Origin": f"https://pa.{BRAND}.com",
                "Connection": "keep-alive",
            }
        )
        for c in obj.get("cookies", []):
            S.cookies.set(c.get("name"), c.get("value"), domain=c.get("domain"))
        state["cookie_age_sec"] = time.time() - float(obj.get("ts", time.time()))
        return obj
    except:
        return None


def publish(url, payload):
    env = {
        "brand_hint": BRAND,
        "source_url": url,
        "payload": payload,
        "ts": time.time(),
        "ua": S.headers.get("User-Agent"),
    }
    R.publish(CHANNEL, json.dumps(env))
    state["messages_received"] += 1
    state["last_ok_ts"] = time.time()


def poll_once(obj):
    url = obj.get("offering_url")
    if not url:
        return False
    try:
        resp = S.get(url, timeout=20)
        state["last_code"] = resp.status_code
        if resp.status_code == 200:
            data = resp.json()
            publish(
                url,
                {"keys": list(data.keys())[:8] if isinstance(data, dict) else ["list"]},
            )
            return True
        elif resp.status_code in (418, 429):
            time.sleep(90)
    except Exception:
        state["last_err_ts"] = time.time()
    return False


def loop():
    while True:
        obj = load_bootstrap()
        if obj:
            ok = poll_once(obj)
        else:
            time.sleep(30)  # Wait for bootstrap state to be available
        time.sleep(random.randint(SPLAY_MIN, SPLAY_MAX))


class H(BaseHTTPRequestHandler):
    def log_message(self, *args, **kw):
        pass

    def do_GET(self):
        if self.path != "/healthz":
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(
            json.dumps({"status": "active", "brand": BRAND, **state}).encode()
        )


if __name__ == "__main__":
    threading.Thread(target=loop, daemon=True).start()
    HTTPServer(("0.0.0.0", PORT), H).serve_forever()

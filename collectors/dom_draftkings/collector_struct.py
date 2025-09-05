import os
import time
import json
import requests
import extract_dk

BOOK = os.getenv("BOOK", "draftkings")
BRAND = os.getenv("BRAND", "draftkings")
URL = os.getenv(
    "URL",
    "https://sportsbook.draftkings.com/leagues/football/88670846?category=game-lines",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.dk.structured")
INTERVAL = int(os.getenv("INTERVAL", "20"))
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"


def rpublish(url, channel, payload):
    import redis

    redis.from_url(url).publish(channel, payload)


def main():
    s = requests.Session()
    while True:
        try:
            r = s.get(
                URL,
                timeout=12,
                headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"},
            )
            payload, mode = extract_dk.parse_struct(r.text)
            env = {
                "brand_hint": BRAND,
                "book": BOOK,
                "transport": "dom_struct",
                "mode": mode,
                "ts": time.time(),
                "events": payload.get("events", []),
            }
            rpublish(REDIS_URL, CHANNEL, json.dumps(env))
            print(f"PUB {len(env['events'])} events (mode={mode})", flush=True)
        except Exception as e:
            print("ERR", e, flush=True)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()

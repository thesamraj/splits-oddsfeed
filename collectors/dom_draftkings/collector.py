import os
import re
import time
import json
import sys
import requests

BOOK = os.getenv("BOOK", "draftkings")
BRAND = os.getenv("BRAND", "draftkings")
URL = os.getenv(
    "URL",
    "https://sportsbook.draftkings.com/leagues/football/88670846?category=game-lines",
)
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.dom")
INTERVAL = int(os.getenv("INTERVAL", "20"))
UA = os.getenv(
    "UA",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
)


def rpublish(url, channel, payload):
    import redis

    redis.from_url(url).publish(channel, payload)


PRICE_RE = re.compile(r"([+\u2212-]\d{2,4})")
JSON_PRICE_RE = re.compile(r'"(price|americanOdds|displayOddsAmerican)"\s*:\s*-?\d+')


def extract(html):
    prices = PRICE_RE.findall(html)
    jprices = JSON_PRICE_RE.findall(html)
    return {
        "brand": BRAND,
        "book": BOOK,
        "kind": "dom_snapshot",
        "url": URL,
        "ts": time.time(),
        "price_tokens": prices[:300],
        "json_hits": len(jprices),
    }


def main():
    s = requests.Session()
    while True:
        try:
            r = s.get(
                URL,
                timeout=12,
                headers={"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"},
            )
            data = extract(r.text)
            rpublish(REDIS_URL, CHANNEL, json.dumps(data))
            print(
                f"PUB {len(data.get('price_tokens',[]))} tokens / json_hits={data['json_hits']} from {URL}",
                flush=True,
            )
        except Exception as e:
            print("ERR", e, file=sys.stderr, flush=True)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()

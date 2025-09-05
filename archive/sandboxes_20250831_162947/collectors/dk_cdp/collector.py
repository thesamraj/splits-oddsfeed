import os
import json
import asyncio
import time
import redis
from playwright.async_api import async_playwright
from extract_json import harvest

URL = os.getenv(
    "URL",
    "https://sportsbook.draftkings.com/leagues/football/88670846?category=game-lines",
)
REDIS = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.dk.cdp")
UA = os.getenv(
    "UA",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
)
MIN_JSON_BYTES = int(os.getenv("MIN_JSON_BYTES", "1500"))
ALLOW_ALL = os.getenv("ALLOW_ALL", "0") == "1"
EXTRA_KEYS = os.getenv("EXTRA_KEYS", "")

BASE_KEYS = (
    "offerCategories",
    "outcomes",
    "americanOdds",
    "oddsAmerican",
    "homeTeam",
    "awayTeam",
    "eventId",
    "startTime",
)
KEYS = tuple(
    list(BASE_KEYS) + ([k.strip() for k in EXTRA_KEYS.split(",") if k.strip()])
)
r = redis.from_url(REDIS)


async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        ctx = await browser.new_context(
            user_agent=UA, locale="en-US", viewport={"width": 1280, "height": 860}
        )
        page = await ctx.new_page()

        async def handle_response(resp):
            try:
                ctype = resp.headers.get("content-type", "")
                if "application/json" not in ctype:
                    return
                body = await resp.body()
                if len(body) < MIN_JSON_BYTES:
                    return
                txt = body.decode("utf-8", "ignore")
                if not ALLOW_ALL and not any(k in txt for k in KEYS):
                    return
                j = json.loads(txt)
                events = harvest(j)
                if not events:
                    return
                env = {
                    "brand_hint": "draftkings",
                    "book": "draftkings",
                    "transport": "dk_cdp",
                    "ts": time.time(),
                    "url": resp.url,
                    "events": events,
                }
                r.publish(CHANNEL, json.dumps(env))
                print(f"PUB {len(events)} events from {resp.url}", flush=True)
            except Exception as e:
                print("resp_err", e, flush=True)

        page.on("response", handle_response)
        await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        # let XHR/GraphQL chatter flow
        for _ in range(45):  # ~90s @2s
            await page.wait_for_timeout(2000)
        await browser.close()


if __name__ == "__main__":
    asyncio.run(run())

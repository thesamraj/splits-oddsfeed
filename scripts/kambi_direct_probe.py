import asyncio, json, time, os, sys
from playwright.async_api import async_playwright

URL = os.environ.get("KAMBI_URL") or "https://eu-offering.kambicdn.org/offering/v2018/betrivers/listView/american_football/nfl"
TIMEOUT = int(os.environ.get("TIMEOUT_MS","30000"))

async def run():
    t0 = time.time()
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36")
        page = await ctx.new_page()
        try:
            resp = await page.goto(URL, timeout=TIMEOUT, wait_until="domcontentloaded")
            status = resp.status if resp else None
            body = await page.content()
            ok = bool(status and 200 <= status < 400 and len(body) > 200)
            print(json.dumps({
                "ok": ok,
                "status": status,
                "ms": int((time.time()-t0)*1000),
                "len": len(body),
                "preview": body[:400]
            }))
        except Exception as e:
            print(json.dumps({"ok": False, "error": str(e)}))
        finally:
            await browser.close()

asyncio.run(run())
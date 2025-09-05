import json
import time
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

CONFIG = [
    {
        "brand": "betparx",
        "home": "https://pa.betparx.com/?page=sportsbook#live",
        "token": "bp2uspa",
        "state": "/Users/sam/Desktop/splits-oddsfeed/var_betparx/state.json",
        "healthz": 9124,
    },
    {
        "brand": "unibet",
        "home": "https://pa.unibet.com/?page=sportsbook#live",
        "token": "ub2uspa",
        "state": "/Users/sam/Desktop/splits-oddsfeed/var_unibet/state.json",
        "healthz": 9125,
    },
]
OFFERING = "https://e0-api.kambi.com/offering/v2018/{token}/listView/all/all/matches.json?lang=en_US&market=US&client_id=2&channel_id=1&ncid=1000"


async def run_brand(pw, cfg):
    brand = cfg["brand"]
    home = cfg["home"]
    token = cfg["token"]
    state_path = Path(cfg["state"])
    browser = await pw.chromium.launch(
        headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"]
    )
    ctx = await browser.new_context()
    page = await ctx.new_page()
    ua = await page.evaluate("() => navigator.userAgent")
    try:
        await page.goto(home, wait_until="networkidle", timeout=45000)
        # best-effort cookie banner click
        for sel in [
            'button:has-text("Accept")',
            "text=Accept all",
            "text=I agree",
            "[data-testid*=accept]",
        ]:
            try:
                await page.click(sel, timeout=2000)
                break
            except:
                pass
        # small dwell to allow scripts to set cookies
        await page.wait_for_timeout(2500)
        # fetch one offering URL from the page context (keeps brand cookies attached)
        url = OFFERING.format(token=token)
        resp = await page.request.get(url)
        ok = resp.status == 200
        cookies = await ctx.cookies()
        state = {
            "brand": brand,
            "ua": ua,
            "cookies": cookies,
            "offering_url": url,
            "ok": ok,
            "ts": time.time(),
        }
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps(state, indent=2))
        print(
            f"BOOTSTRAP {brand}: status={resp.status} cookies={len(cookies)} saved={state_path}"
        )
    finally:
        await ctx.close()
        await browser.close()


async def main():
    async with async_playwright() as pw:
        for cfg in CONFIG:
            try:
                await run_brand(pw, cfg)
            except Exception as e:
                print(f"BOOTSTRAP {cfg['brand']} ERROR: {e}")


if __name__ == "__main__":
    asyncio.run(main())

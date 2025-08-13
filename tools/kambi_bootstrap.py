import asyncio
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import urlparse
from playwright.async_api import async_playwright

STATE_HOST = os.getenv("KAMBI_STATE_HOST", "pa.betrivers.com")
START_URL = f"https://{STATE_HOST}/sports"  # generic sports entry
OUT_DIR = Path(os.getenv("KAMBI_BOOTSTRAP_OUTDIR", "artifacts"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "kambi_bootstrap.json"

PROXY_URL = os.getenv("PROXY_URL") or None
HEADLESS = os.getenv("BOOT_HEADFUL", "false").lower() not in ("1", "true", "yes")


def is_kambi(url: str) -> bool:
    u = urlparse(url)
    h = u.netloc.lower()
    return "kambi" in h and ("api" in h or "offering" in u.path)


async def main():
    rec = {"state_host": STATE_HOST, "start_url": START_URL, "captured": []}
    proxy_arg = None
    if PROXY_URL:
        # Playwright wants host/port fields; allow http://user:pass@host:port
        m = re.match(r"(\w+?)://(?:(.+?):(.+?)@)?(.+?):(\d+)", PROXY_URL)
        if m:
            scheme, user, pwd, host, port = m.groups()
            proxy_arg = {"server": f"{scheme}://{host}:{port}"}
            if user and pwd:
                proxy_arg["username"] = user
                proxy_arg["password"] = pwd

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            locale="en-US",
            proxy=proxy_arg,
        )
        page = await ctx.new_page()

        # Collect Kambi XHR/Fetch responses
        async def on_response(resp):
            try:
                url = resp.url
                if is_kambi(url):
                    headers = {k.lower(): v for k, v in resp.request.headers.items()}
                    meth = resp.request.method
                    status = resp.status
                    item = {
                        "url": url,
                        "method": meth,
                        "status": status,
                        "req_headers": headers,
                        "timestamp": time.time(),
                    }
                    rec["captured"].append(item)
            except Exception:
                pass

        page.on("response", on_response)

        await page.goto(START_URL, wait_until="domcontentloaded")
        # Light interaction to let the app load network calls
        try:
            await page.wait_for_timeout(3000)
        except:
            pass

        # Get cookies for both site and any Kambi domains
        cookies = await ctx.cookies()
        rec["cookies"] = cookies

        # Also grab localStorage/sessionStorage dumps (frontends sometimes stash IDs here)
        try:
            ls = await page.evaluate(
                "JSON.stringify(Object.assign({}, window.localStorage))"
            )
            ss = await page.evaluate(
                "JSON.stringify(Object.assign({}, window.sessionStorage))"
            )
            rec["localStorage"] = json.loads(ls)
            rec["sessionStorage"] = json.loads(ss)
        except:
            pass

        await browser.close()

    # Heuristic: pick a "best" Kambi request to seed headers (highest-status recent 200/204)
    best = None
    for item in rec["captured"]:
        if item.get("status", 0) in (200, 204):
            best = item
            break
    rec["seed_headers"] = best["req_headers"] if best else {}

    OUT_PATH.write_text(json.dumps(rec, indent=2))
    print(
        f"Wrote bootstrap to {OUT_PATH.resolve()} with {len(rec['captured'])} Kambi hits"
    )
    if not best:
        print(
            "WARNING: no 200/204 Kambi responses captured; will still save cookies/headers we have."
        )


if __name__ == "__main__":
    asyncio.run(main())

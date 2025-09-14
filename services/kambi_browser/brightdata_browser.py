import os, sys, asyncio, json, time
from urllib.parse import urlparse
from playwright.async_api import async_playwright

WSS = os.environ.get("BRD_WSS", "")
URL = os.environ.get("TEST_URL", "")
TIMEOUT_MS = int(os.environ.get("BROWSER_TIMEOUT_MS", "45000"))

if not WSS or not URL:
    print(json.dumps({"ok": False, "reason": "missing_env", "hint": "Set BRD_WSS and TEST_URL"}))
    sys.exit(2)

async def main():
    t0 = time.time()
    async with async_playwright() as pw:
        # Bright Data Browser API speaks CDP; use connect_over_cdp
        try:
            browser = await pw.chromium.connect_over_cdp(WSS)
        except Exception as e:
            msg = str(e)
            reason = "connect_failed"
            if "407" in msg or "wrong_password" in msg:
                reason = "auth_failed"
            print(json.dumps({"ok": False, "stage": "connect", "reason": reason, "error": msg}))
            return

        # Use the first context if present, otherwise create one
        contexts = browser.contexts
        context = contexts[0] if contexts else await browser.new_context()
        page = await context.new_page()

        status = None
        html_snippet = ""
        try:
            # Hard nav timeout bound by TIMEOUT_MS
            resp = await page.goto(URL, timeout=TIMEOUT_MS, wait_until="domcontentloaded")
            status = resp.status if resp else None
            # Grab a small snippet to prove payload
            html_snippet = (await page.content())[:5000]
        except Exception as e:
            await browser.close()
            print(json.dumps({"ok": False, "stage": "navigate", "reason": "nav_error", "error": str(e)}))
            return

        await browser.close()
        elapsed = round((time.time() - t0)*1000)
        print(json.dumps({
            "ok": True if status and 200 <= status < 400 else False,
            "stage": "done",
            "status": status,
            "ms": elapsed,
            "url": URL,
            "has_html": True if html_snippet else False,
            "html_preview": html_snippet[:400]  # keep output small
        }))

if __name__ == "__main__":
    asyncio.run(main())
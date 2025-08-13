import asyncio
import json
import os
import re
import time
import traceback
from typing import Optional, Dict, Any
from urllib.parse import urlparse
from prometheus_client import Counter, Gauge, Histogram, start_http_server
import redis
from playwright.async_api import async_playwright

# ---- ENV ----
KAMBI_STATE_URL = os.getenv(
    "KAMBI_STATE_URL", "https://pa.betrivers.com/?page=sportsbook#american_football/nfl"
)
KAMBI_CAPTURE_PATTERNS = os.getenv(
    "KAMBI_CAPTURE_PATTERNS",
    r"/offering/.*(v2018|events|selections)|e0-api\.kambi\.com/.*",
)
KAMBI_STORAGE = os.getenv("KAMBI_STORAGE", "state/kambi-browser/state.json")
KAMBI_HEADFUL = os.getenv("KAMBI_BROWSER_HEADFUL", "false").lower() == "true"
PROXY_URL = os.getenv("PROXY_URL", "")
PROXY_MANAGER_URL = os.getenv("PROXY_MANAGER_URL", "")
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
REDIS_CHANNEL = os.getenv("REDIS_CHANNEL", "odds.raw.kambi")
METRICS_PORT = int(os.getenv("KAMBI_BROWSER_METRICS_PORT", "9118"))
LAST_RESORT_AFTER_SEC = int(os.getenv("KAMBI_LAST_RESORT_AFTER_SEC", "15"))
NAV_TIMEOUT = int(os.getenv("KAMBI_NAV_TIMEOUT_SEC", "25"))

# ---- METRICS ----
m_captured = Counter("kambi_browser_captured_total", "Captured offering JSON responses")
m_published = Counter("kambi_browser_published_total", "Published messages to Redis")
m_errors = Counter("kambi_browser_errors_total", "Errors", ["type"])
m_last_success_ts = Gauge(
    "kambi_browser_last_success_timestamp_seconds", "Last success ts"
)
m_latency = Histogram(
    "kambi_browser_nav_latency_seconds", "Nav-to-first-capture latency"
)

pattern = re.compile(KAMBI_CAPTURE_PATTERNS)


def log(msg: str):
    print(f"[kambi-browser] {msg}", flush=True)


def _parse_proxy(url: str) -> Optional[Dict[str, Any]]:
    if not url:
        return None
    u = urlparse(url)
    if not u.scheme or not u.hostname or not u.port:
        return None
    proxy = {"server": f"{u.scheme}://{u.hostname}:{u.port}"}
    if u.username:
        proxy["username"] = u.username
    if u.password:
        proxy["password"] = u.password
    return proxy


async def pick_proxy() -> Optional[Dict[str, Any]]:
    p = _parse_proxy(PROXY_URL)
    if p:
        log(f"using static proxy {p.get('server')}")
        return p
    if PROXY_MANAGER_URL:
        try:
            import urllib.request
            import json as _json

            with urllib.request.urlopen(
                f"{PROXY_MANAGER_URL.rstrip('/')}/lease", timeout=5
            ) as r:
                if r.status == 200:
                    data = _json.loads(r.read().decode())
                    q = _parse_proxy(data.get("proxy", ""))
                    if q:
                        log(f"leased proxy {q.get('server')}")
                        return q
        except Exception as e:
            log(f"proxy-manager lease error: {e}")
            m_errors.labels("proxy_manager").inc()
    return None


async def ensure_storage_path(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


async def publish(rconn, payload):
    try:
        msg = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        rconn.publish(REDIS_CHANNEL, msg)
        m_published.inc()
    except Exception as e:
        log(f"redis publish error: {e}")
        m_errors.labels("redis").inc()


async def run_once():
    await ensure_storage_path(KAMBI_STORAGE)
    rconn = redis.Redis.from_url(REDIS_URL)
    proxy = await pick_proxy()

    t0 = time.time()

    try:
        log("initializing playwright")
        async with async_playwright() as pw:
            launch_args = dict(headless=not KAMBI_HEADFUL)
            if proxy:
                launch_args["proxy"] = proxy
            browser = await pw.chromium.launch(**launch_args)
            log("chromium launched")

            storage_state = KAMBI_STORAGE if os.path.exists(KAMBI_STORAGE) else None
            context = await browser.new_context(storage_state=storage_state)
            await context.set_extra_http_headers(
                {
                    "Referer": "https://pa.betrivers.com/",
                    "Origin": "https://pa.betrivers.com",
                }
            )
            page = await context.new_page()
            log("context + page ready")

            first_capture = asyncio.get_event_loop().create_future()

            async def handle_response(resp):
                try:
                    url = resp.url
                    if not pattern.search(url):
                        return
                    ctype = (resp.headers or {}).get("content-type", "")
                    if "json" not in ctype:
                        return
                    data = await resp.json()
                    payload = {
                        "source": "kambi",
                        "url": url,
                        "status": resp.status,
                        "ts": time.time(),
                        "data": data,
                    }
                    await publish(rconn, payload)
                    m_captured.inc()
                    if not first_capture.done():
                        first_capture.set_result(True)
                        m_last_success_ts.set(time.time())
                    log(f"captured {resp.status} {url}")
                except Exception as e:
                    log(f"response handler error: {e}")
                    m_errors.labels("resp_parse").inc()

            page.on("response", handle_response)

            try:
                log(f"goto {KAMBI_STATE_URL}")
                await page.goto(
                    KAMBI_STATE_URL,
                    wait_until="domcontentloaded",
                    timeout=NAV_TIMEOUT * 1000,
                )
            except Exception as e:
                log(f"nav error: {e}")
                m_errors.labels("nav").inc()

            try:
                await asyncio.wait_for(first_capture, timeout=LAST_RESORT_AFTER_SEC)
            except asyncio.TimeoutError:
                # Last resort: in-page fetch with session cookies
                try:
                    candidate = "https://e0-api.kambi.com/offering/v2018/pa/listView/american_football/nfl/all/all"
                    js = f"""
                    async () => {{
                      const r = await fetch("{candidate}", {{
                        credentials: "include",
                        headers: {{
                          "Accept": "application/json",
                          "Referer": "https://pa.betrivers.com/",
                          "Origin": "https://pa.betrivers.com"
                        }}
                      }});
                      const text = await r.text();
                      return {{status: r.status, url: r.url, body: text}};
                    }}
                    """
                    res = await page.evaluate(js)
                    try:
                        data = json.loads(res["body"])
                        payload = {
                            "source": "kambi",
                            "url": res.get("url", ""),
                            "status": res.get("status", 0),
                            "ts": time.time(),
                            "data": data,
                        }
                        await publish(rconn, payload)
                        m_captured.inc()
                        m_last_success_ts.set(time.time())
                        log(
                            f"last-resort captured {res.get('status')} {res.get('url')}"
                        )
                    except Exception as e:
                        log(f"last-resort parse error: {e}")
                        m_errors.labels("last_resort_parse").inc()
                except Exception as e:
                    log(f"last-resort fetch error: {e}")
                    m_errors.labels("last_resort_fetch").inc()

            try:
                await context.storage_state(path=KAMBI_STORAGE)
                log("storage saved")
            except Exception as e:
                log(f"storage save error: {e}")
                m_errors.labels("storage_save").inc()

            await context.close()
            await browser.close()
            m_latency.observe(max(0.0, time.time() - t0))
            log("shutdown ok")

    except Exception as e:
        log(f"browser launch failed: {e}")
        m_errors.labels("browser_launch").inc()
        # Keep metrics alive and wait instead of exiting
        log("keeping metrics alive despite browser failure")
        await asyncio.sleep(60)  # Keep running for metrics
        return 0

    # keep metrics alive briefly (supervisor expects process to exit or long-run; compose restarts on exit)
    await asyncio.sleep(2)
    return 0


def main():
    # Start metrics server first, before any async operations
    try:
        start_http_server(METRICS_PORT)
        log(f"metrics on :{METRICS_PORT}")
    except Exception as e:
        log(f"metrics server failed: {e}")

    try:
        asyncio.run(run_once())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        log(f"fatal error in main: {e}")
        traceback.print_exc()
        # Don't exit - keep the process alive for metrics
        log("keeping process alive for metrics")
        import time

        time.sleep(3600)  # Keep alive for 1 hour


if __name__ == "__main__":
    main()

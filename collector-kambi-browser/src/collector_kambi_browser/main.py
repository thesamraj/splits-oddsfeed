import asyncio
import json
import os
import re
import time
import traceback
import hashlib
import random
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
    r".*kambicdn\.com/offering/.*|.*kambi\.com/.*",
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
m_clicks = Counter("kambi_browser_clicks_total", "Total clicks performed")
m_payloads_seen = Counter("kambi_payloads_seen_total", "Total payloads intercepted")
m_payloads_odds = Counter("kambi_payloads_odds_total", "Payloads containing odds data")
m_publish_status = Counter("kambi_publish_total", "Publish attempts", ["status"])
kambi_last_success_timestamp_seconds = Gauge(
    "kambi_last_success_timestamp_seconds", "Last successful odds capture"
)

pattern = re.compile(KAMBI_CAPTURE_PATTERNS)


def log(msg: str):
    print(f"[kambi-browser] {msg}", flush=True)


def has_odds_data(data: Dict[str, Any]) -> bool:
    """Heuristic to detect odds-bearing payloads"""

    def check_recursive(obj, depth=0):
        if depth > 10:  # Prevent infinite recursion
            return False
        if isinstance(obj, dict):
            # Check for key patterns (case-insensitive)
            for key in obj.keys():
                key_lower = key.lower()
                if any(
                    pattern in key_lower
                    for pattern in [
                        "betoffers",
                        "betoffer",
                        "outcomes",
                        "selections",
                        "markets",
                    ]
                ):
                    # Check if it contains actual odds data
                    value = obj[key]
                    if isinstance(value, list) and value:
                        # Check first item for price/odds data
                        first_item = value[0] if value else {}
                        if isinstance(first_item, dict):
                            if any(
                                k.lower() in ["price", "odds", "outcome"]
                                for k in first_item.keys()
                            ):
                                return True
                    elif isinstance(value, dict) and any(
                        k.lower() in ["price", "odds", "outcome"] for k in value.keys()
                    ):
                        return True
            # Recursively check nested objects
            for value in obj.values():
                if check_recursive(value, depth + 1):
                    return True
        elif isinstance(obj, list):
            for item in obj:
                if check_recursive(item, depth + 1):
                    return True
        return False

    return check_recursive(data)


async def save_odds_artifact(data: Dict[str, Any], url: str) -> str:
    """Save odds payload to artifacts directory"""
    epoch = int(time.time())
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]

    # Ensure artifacts directory exists
    artifacts_dir = "artifacts/kambi"
    os.makedirs(artifacts_dir, exist_ok=True)

    filename = f"odds-{epoch}-{url_hash}.json"
    filepath = os.path.join(artifacts_dir, filename)

    # Save the payload
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    # Append to index
    index_path = os.path.join(artifacts_dir, "odds_index.txt")
    with open(index_path, "a") as f:
        f.write(f"{filepath}\n")

    return filepath


async def jittered_sleep():
    """Add jittered think time between interactions"""
    await asyncio.sleep(random.uniform(0.7, 1.5))


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
        m_publish_status.labels("ok").inc()
    except Exception as e:
        log(f"redis publish error: {e}")
        m_errors.labels("redis").inc()
        m_publish_status.labels("error").inc()


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
                    m_payloads_seen.inc()

                    # Check if this payload contains odds data
                    contains_odds = has_odds_data(data)

                    if contains_odds:
                        m_payloads_odds.inc()
                        kambi_last_success_timestamp_seconds.set(time.time())

                        # Save artifact
                        try:
                            artifact_path = await save_odds_artifact(data, url)
                            log(f"saved odds artifact: {artifact_path}")
                        except Exception as e:
                            log(f"failed to save artifact: {e}")

                    payload = {
                        "source": "kambi",
                        "url": url,
                        "status": resp.status,
                        "ts": time.time(),
                        "data": data,
                    }

                    # Publish all matching payloads (not just odds-bearing ones)
                    await publish(rconn, payload)
                    m_captured.inc()

                    if not first_capture.done():
                        first_capture.set_result(True)
                        m_last_success_ts.set(time.time())

                    log(
                        f"captured {resp.status} {url} {'(odds)' if contains_odds else ''}"
                    )
                except Exception as e:
                    log(f"response handler error: {e}")
                    m_errors.labels("resp_parse").inc()

            page.on("response", handle_response)

            # Enhanced navigation to capture event odds
            try:
                log(f"goto {KAMBI_STATE_URL}")
                await page.goto(
                    KAMBI_STATE_URL,
                    wait_until="domcontentloaded",
                    timeout=NAV_TIMEOUT * 1000,
                )

                # Wait for initial page load and capture any immediate responses
                await asyncio.sleep(3)

                # Navigate to NFL if not already there
                try:
                    # Look for NFL links (case-insensitive)
                    nfl_selectors = [
                        "text=/NFL/i",
                        "text=/National Football League/i",
                        "[data-sport*='nfl' i]",
                        "[href*='nfl' i]",
                    ]

                    for selector in nfl_selectors:
                        try:
                            nfl_link = page.locator(selector).first
                            if await nfl_link.is_visible(timeout=2000):
                                log("clicking NFL section")
                                await nfl_link.click()
                                m_clicks.inc()
                                await jittered_sleep()
                                break
                        except Exception:
                            continue

                except Exception as e:
                    log(f"nfl navigation attempt failed: {e}")

                # Scroll to load more events
                log("scrolling to load events")
                for i in range(3):
                    await page.mouse.wheel(0, 800)
                    await asyncio.sleep(1)

                # Find and click on NFL event links
                event_clicks = 0
                max_events = 6

                try:
                    # Look for event links/cards
                    event_selectors = [
                        "[data-testid*='event']",
                        "[class*='event']",
                        "[class*='match']",
                        "a[href*='event']",
                        "[role='button']:has-text('vs')",
                        ".event-card",
                        ".match-card",
                    ]

                    for selector in event_selectors:
                        if event_clicks >= max_events:
                            break

                        try:
                            events = page.locator(selector)
                            count = await events.count()
                            log(f"found {count} elements for selector {selector}")

                            for i in range(min(count, max_events - event_clicks)):
                                try:
                                    event = events.nth(i)
                                    if await event.is_visible(timeout=1000):
                                        # Skip if it looks like a live video element
                                        text = await event.text_content() or ""
                                        if any(
                                            skip in text.lower()
                                            for skip in ["live", "video", "stream"]
                                        ):
                                            continue

                                        log(f"clicking event {i+1}")
                                        await event.click()
                                        m_clicks.inc()
                                        event_clicks += 1

                                        # Wait for network idle to capture event-specific requests
                                        await page.wait_for_load_state(
                                            "networkidle", timeout=5000
                                        )
                                        await asyncio.sleep(
                                            3
                                        )  # Additional wait for XHR

                                        # Go back to events list
                                        try:
                                            await page.go_back()
                                            await asyncio.sleep(2)
                                        except Exception:
                                            # If back doesn't work, reload NFL page
                                            await page.goto(
                                                KAMBI_STATE_URL,
                                                wait_until="domcontentloaded",
                                            )
                                            await asyncio.sleep(2)

                                        await jittered_sleep()

                                except Exception as e:
                                    log(f"event click {i} failed: {e}")
                                    continue
                        except Exception as e:
                            log(f"selector {selector} failed: {e}")
                            continue

                    log(f"completed {event_clicks} event interactions")

                except Exception as e:
                    log(f"event clicking failed: {e}")
                    m_errors.labels("event_clicks").inc()

                # Wait a bit more to capture any final requests
                await asyncio.sleep(5)

            except Exception as e:
                log(f"nav error: {e}")
                m_errors.labels("nav").inc()

                # Fallback: try direct API call
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
                        log(f"fallback captured {res.get('status')} {res.get('url')}")
                    except Exception as e:
                        log(f"fallback parse error: {e}")
                        m_errors.labels("last_resort_parse").inc()
                except Exception as e:
                    log(f"fallback fetch error: {e}")
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

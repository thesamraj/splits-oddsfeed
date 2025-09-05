import os
import time
import json
import threading
from flask import Flask, jsonify
import redis
from playwright.sync_api import sync_playwright

SH_URL = os.getenv("SH_URL", "https://ct.playsugarhouse.com/?page=sportsbook")
INTERVAL = int(os.getenv("SCRAPE_INTERVAL_SEC", "60"))
PUBLISH_CHANNEL = os.getenv("PUBLISH_CHANNEL", "odds.raw.kambi")
HEALTHZ_PORT = int(os.getenv("HEALTHZ_PORT", "9135"))
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")

app = Flask(__name__)
state = {
    "brand": "sugarhouse",
    "url": SH_URL,
    "polls": 0,
    "published": 0,
    "events_detected": 0,
    "ws_frames": 0,
    "http_captures": 0,
    "status": "initializing",
}

captured_data = []


def extract_odds_from_payload(payload_str):
    """Extract odds from various payload formats"""
    events = []
    try:
        data = json.loads(payload_str)

        # Check for Kambi-style structure
        if "events" in data or "liveEvents" in data:
            raw_events = data.get("liveEvents", []) + data.get("events", [])
            for evt in raw_events[:20]:  # Limit processing
                event = evt.get("event", evt)
                event_id = str(event.get("id", ""))
                home = event.get("homeName", "")
                away = event.get("awayName", "")

                # Extract markets
                markets = []
                bet_offers = event.get("betOffers", [])

                for offer in bet_offers[:5]:  # Limit markets
                    market_type = offer.get("criterion", {}).get("label", "unknown")
                    outcomes = offer.get("outcomes", [])

                    for outcome in outcomes:
                        price = None
                        if "americanOdds" in outcome:
                            price = outcome["americanOdds"]
                        elif "odds" in outcome:
                            price = outcome["odds"]
                        elif "oddsFractional" in outcome:
                            price = outcome["oddsFractional"]

                        if price:
                            markets.append(
                                {
                                    "market": market_type,
                                    "selection": outcome.get("label", ""),
                                    "price": price,
                                }
                            )

                if event_id and (home or away) and markets:
                    events.append(
                        {
                            "event_id": event_id,
                            "home": home,
                            "away": away,
                            "markets": markets,
                        }
                    )

        # Check for other structures
        elif "data" in data and isinstance(data["data"], list):
            # Alternative structure
            for item in data["data"][:20]:
                if "eventId" in item or "id" in item:
                    events.append(
                        {
                            "event_id": str(item.get("eventId", item.get("id", ""))),
                            "raw": item,
                        }
                    )
    except:
        pass

    return events


def scrape_with_cdp():
    """Scrape using CDP to capture network traffic"""
    global captured_data

    print("SH_CDP: scrape_with_cdp() called", flush=True)

    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        print("SH_CDP: Redis connected", flush=True)
    except Exception as e:
        print(f"SH_CDP: Redis connection error: {e}", flush=True)
        return

    print("SH_CDP: Starting Playwright", flush=True)
    with sync_playwright() as p:
        print("SH_CDP: Launching browser", flush=True)
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--ignore-certificate-errors",
            ],
        )

        print("SH_CDP: Creating context", flush=True)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
            locale="en-US",
            timezone_id="America/New_York",
            geolocation={"latitude": 41.3, "longitude": -72.9},
            permissions=["geolocation"],
            ignore_https_errors=True,
        )

        print("SH_CDP: Creating page", flush=True)
        page = context.new_page()

        # Capture network traffic
        def handle_response(response):
            try:
                url = response.url
                if response.status == 200:
                    # Look for JSON responses
                    if "json" in response.headers.get("content-type", ""):
                        state["http_captures"] += 1
                        body = response.text()

                        # Check for odds data
                        if any(
                            x in url
                            for x in [
                                "kambi",
                                "offering",
                                "event",
                                "bet",
                                "odds",
                                "market",
                            ]
                        ):
                            # Log first few payloads for debugging
                            if state["http_captures"] <= 3:
                                print(
                                    f"SH_CDP: Sample payload from {url[:80]}",
                                    flush=True,
                                )
                                print(
                                    f"SH_CDP: Payload preview: {body[:500]}", flush=True
                                )

                            events = extract_odds_from_payload(body)
                            if events:
                                state["events_detected"] += len(events)
                                captured_data.append(
                                    {"type": "http", "url": url, "events": events}
                                )

                                # Publish to Redis
                                envelope = {
                                    "book": "sugarhouse",
                                    "brand": "sugarhouse",
                                    "transport": "cdp",
                                    "url": url,
                                    "events": events,
                                    "ts": int(time.time()),
                                }
                                r.publish(PUBLISH_CHANNEL, json.dumps(envelope))
                                state["published"] += 1
                                print(
                                    f"SH_CDP: Published {len(events)} events from {url[:50]}",
                                    flush=True,
                                )
            except:
                pass

        page.on("response", handle_response)

        # Enable CDP for WebSocket capture
        print("SH_CDP: Enabling CDP", flush=True)
        try:
            cdp = page.context.new_cdp_session(page)
            cdp.send("Network.enable")
            cdp.send(
                "Target.setAutoAttach",
                {"autoAttach": True, "waitForDebuggerOnStart": False, "flatten": True},
            )
        except Exception as e:
            print(f"SH_CDP: CDP setup error: {e}", flush=True)
            cdp = None

        def handle_ws(params):
            try:
                if params.get("response", {}).get("payloadData"):
                    payload = params["response"]["payloadData"]
                    state["ws_frames"] += 1

                    # Log first few WS frames for debugging
                    if state["ws_frames"] <= 3:
                        print("SH_CDP: Sample WS frame:", flush=True)
                        print(f"SH_CDP: WS preview: {payload[:500]}", flush=True)

                    events = extract_odds_from_payload(payload)
                    if events:
                        state["events_detected"] += len(events)
                        captured_data.append({"type": "ws", "events": events})

                        # Publish
                        envelope = {
                            "book": "sugarhouse",
                            "brand": "sugarhouse",
                            "transport": "ws",
                            "events": events,
                            "ts": int(time.time()),
                        }
                        r.publish(PUBLISH_CHANNEL, json.dumps(envelope))
                        state["published"] += 1
                        print(f"SH_CDP: Published {len(events)} WS events", flush=True)
            except:
                pass

        if cdp:
            cdp.on("Network.webSocketFrameReceived", handle_ws)

        print(f"SH_CDP: Loading {SH_URL}", flush=True)
        try:
            response = page.goto(SH_URL, wait_until="networkidle", timeout=30000)
            print(
                f"SH_CDP: Page loaded with status {response.status if response else 'None'}",
                flush=True,
            )
        except Exception as e:
            print(f"SH_CDP: Page load error: {e}", flush=True)

        # Try to interact with the page
        print("SH_CDP: Waiting for page to settle", flush=True)
        page.wait_for_timeout(5000)

        # Take a screenshot for debugging
        try:
            page.screenshot(path="/tmp/sh_page.png")
            print("SH_CDP: Screenshot saved to /tmp/sh_page.png", flush=True)
        except:
            pass

        # Get page title and URL
        try:
            title = page.title()
            url = page.url
            print(f"SH_CDP: Page title: {title}, URL: {url}", flush=True)
        except:
            pass

        # Click on Sportsbook if on landing page
        print("SH_CDP: Looking for Sportsbook button", flush=True)
        try:
            page.click('text="SPORTSBOOK"', timeout=5000)
            print("SH_CDP: Clicked SPORTSBOOK", flush=True)
            page.wait_for_timeout(5000)
        except:
            print("SH_CDP: SPORTSBOOK button not found", flush=True)

        # Click on sports sections if visible
        print("SH_CDP: Looking for sports sections", flush=True)
        try:
            page.click('text="Football"', timeout=5000)
            print("SH_CDP: Clicked Football", flush=True)
            page.wait_for_timeout(3000)
        except:
            print("SH_CDP: Football section not found", flush=True)

        try:
            page.click('text="Basketball"', timeout=5000)
            print("SH_CDP: Clicked Basketball", flush=True)
            page.wait_for_timeout(3000)
        except:
            print("SH_CDP: Basketball section not found", flush=True)

        # Extract from page state
        print("SH_CDP: Extracting page state", flush=True)
        try:
            page_data = page.evaluate(
                """() => {
                return {
                    redux: window.__REDUX_STATE__ || null,
                    preloaded: window.__PRELOADED_STATE__ || null,
                    data: window.__data || null,
                    kambi: window.kambi || null,
                    hasKambi: typeof window.kambi !== 'undefined',
                    location: window.location.href
                }
            }"""
            )
            print(
                f"SH_CDP: Page data keys: {list(page_data.keys()) if page_data else 'None'}",
                flush=True,
            )

            if page_data:
                for key, val in page_data.items():
                    if val:
                        events = extract_odds_from_payload(json.dumps(val))
                        if events:
                            state["events_detected"] += len(events)
                            print(
                                f"SH_CDP: Found {len(events)} events in window.{key}",
                                flush=True,
                            )

                            envelope = {
                                "book": "sugarhouse",
                                "brand": "sugarhouse",
                                "transport": "page_state",
                                "source": key,
                                "events": events,
                                "ts": int(time.time()),
                            }
                            r.publish(PUBLISH_CHANNEL, json.dumps(envelope))
                            state["published"] += 1
        except:
            pass

        # Keep capturing for a while
        print("SH_CDP: Monitoring for 20 seconds", flush=True)
        page.wait_for_timeout(20000)

        print("SH_CDP: Closing browser", flush=True)
        browser.close()
        print(
            f"SH_CDP: Poll complete - events: {state['events_detected']}, published: {state['published']}",
            flush=True,
        )


def scrape_loop():
    """Main scraping loop"""
    state["status"] = "active"

    while True:
        state["polls"] += 1
        print(f"SH_CDP: Starting poll #{state['polls']}", flush=True)

        try:
            scrape_with_cdp()
        except Exception as e:
            import traceback

            print(f"SH_CDP: Error: {e}", flush=True)
            print(f"SH_CDP: Traceback: {traceback.format_exc()}", flush=True)

        print(f"SH_CDP: Waiting {INTERVAL} seconds before next poll", flush=True)
        time.sleep(INTERVAL)


@app.route("/healthz")
def healthz():
    return jsonify(state)


def main():
    scraper_thread = threading.Thread(target=scrape_loop, daemon=True)
    scraper_thread.start()

    print(f"SH_CDP: Starting on port {HEALTHZ_PORT}", flush=True)
    app.run(host="0.0.0.0", port=HEALTHZ_PORT, debug=False)


if __name__ == "__main__":
    main()

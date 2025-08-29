import os
import time
import json
import threading
import re
from flask import Flask, jsonify
import redis
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# Configuration
SH_BASE_URL = os.getenv("SH_BASE_URL", "https://pa.sugarhouse.com/sports")
SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL_SEC", "30"))
PUBLISH_CHANNEL = os.getenv("PUBLISH_CHANNEL", "odds.raw.kambi")
BRAND_HINT = os.getenv("BRAND_HINT", "sugarhouse")
HEALTHZ_PORT = int(os.getenv("HEALTHZ_PORT", "9141"))
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")

app = Flask(__name__)
state = {
    "brand": BRAND_HINT,
    "channel": PUBLISH_CHANNEL,
    "base_url": SH_BASE_URL,
    "interval": SCRAPE_INTERVAL,
    "last_err": None,
    "last_ok_ts": None,
    "last_status": None,
    "last_url": None,
    "polls": 0,
    "published": 0,
    "events_detected": 0,
    "prices_extracted": 0,
    "status": "initializing",
}


def extract_odds_from_dom(html_content, url):
    """Extract odds from SugarHouse DOM structure"""
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        events_data = []

        # Look for common sportsbook DOM patterns
        # Pattern 1: Look for game/event containers
        game_containers = soup.find_all(
            ["div", "section"], class_=re.compile(r"(game|event|match|contest)", re.I)
        )

        print(
            f"SH_DOM: Found {len(game_containers)} potential game containers",
            flush=True,
        )

        for container in game_containers[:10]:  # Limit to first 10 for testing
            try:
                # Extract team names
                teams = container.find_all(
                    ["span", "div", "p"],
                    class_=re.compile(r"(team|name|participant)", re.I),
                )

                # Extract odds values
                odds_elements = container.find_all(
                    ["span", "div", "button"],
                    class_=re.compile(r"(odd|price|bet|wager)", re.I),
                )

                if len(teams) >= 2 and len(odds_elements) > 0:
                    team_names = [t.get_text(strip=True) for t in teams[:2]]
                    odds_values = []

                    for odd_elem in odds_elements[:6]:  # Limit odds per game
                        odd_text = odd_elem.get_text(strip=True)
                        # Look for American odds pattern (+/-XXX) or decimal (X.XX)
                        if re.match(r"^[+-]?\d{2,4}$|^\d{1,2}\.\d{2}$", odd_text):
                            odds_values.append(odd_text)

                    if team_names[0] and team_names[1] and odds_values:
                        event_data = {
                            "event_id": f"sh_{hash(f'{team_names[0]}_{team_names[1]}') % 1000000}",
                            "home": team_names[1],
                            "away": team_names[0],
                            "sport": "unknown",
                            "league": "unknown",
                            "odds_raw": odds_values,
                            "extracted_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                        }
                        events_data.append(event_data)

            except Exception as e:
                print(f"SH_DOM: Error parsing container: {e}", flush=True)
                continue

        # Pattern 2: Look for embedded JSON data
        script_tags = soup.find_all("script", type="application/json")
        for script in script_tags[:5]:  # Check first 5 JSON scripts
            try:
                json_data = json.loads(script.string or "{}")
                if (
                    "events" in json_data
                    or "games" in json_data
                    or "matches" in json_data
                ):
                    print(
                        f"SH_DOM: Found embedded JSON with {len(json_data)} keys",
                        flush=True,
                    )
                    # Could extract from JSON structure here
            except:
                continue

        print(f"SH_DOM: Extracted {len(events_data)} events from DOM", flush=True)
        return events_data

    except Exception as e:
        print(f"SH_DOM: Parse error: {e}", flush=True)
        return []


def publish_odds(r, events_data, url):
    """Publish odds data to Redis"""
    if not events_data:
        return

    envelope = {
        "book": "sugarhouse",
        "brand_hint": "sugarhouse",
        "transport": "dom_scrape",
        "url": url,
        "status": 200,
        "ts": int(time.time()),
        "events": events_data,
        "payload": json.dumps({"events": events_data}),
    }

    r.publish(PUBLISH_CHANNEL, json.dumps(envelope))
    state["published"] += 1
    state["events_detected"] += len(events_data)

    # Count total prices
    total_prices = sum(len(event.get("odds_raw", [])) for event in events_data)
    state["prices_extracted"] += total_prices

    print(
        f"SH_SCRAPE: Published {len(events_data)} events, {total_prices} prices to {PUBLISH_CHANNEL}",
        flush=True,
    )


def scrape_loop():
    """Main scraping loop using Playwright"""
    r = redis.from_url(REDIS_URL, decode_responses=True)

    print(
        f"SH_SCRAPE: Starting DOM scraper, interval={SCRAPE_INTERVAL}s, port={HEALTHZ_PORT}",
        flush=True,
    )

    state["status"] = "active"

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--ignore-certificate-errors",
                "--ignore-ssl-errors",
            ],
        )
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            ignore_https_errors=True,
        )

        while True:
            state["polls"] += 1

            try:
                # Test different SugarHouse URLs
                urls_to_try = [
                    "https://www.sugarhouse.com",
                    "https://sugarhouse.com",
                    "https://pa.playsugarhouse.com",
                    "https://www.playsugarhouse.com",
                    "https://playsugarhouse.com",
                    "https://nj.sugarhouse.com",
                    "https://pa.sugarhouse.com",
                ]

                success = False

                for url in urls_to_try[:2]:  # Try first 2 URLs per cycle
                    try:
                        print(f"SH_SCRAPE: Loading {url}", flush=True)

                        page = context.new_page()
                        page.set_default_timeout(10000)  # 10s timeout

                        start_time = time.time()
                        response = page.goto(url, wait_until="domcontentloaded")
                        load_time = time.time() - start_time

                        state["last_url"] = url
                        state["last_status"] = response.status if response else 0

                        print(
                            f"SH_SCRAPE: {url} -> {response.status} ({load_time:.2f}s)",
                            flush=True,
                        )

                        if response and response.status == 200:
                            # Wait a bit for dynamic content
                            page.wait_for_timeout(3000)  # 3s wait

                            html_content = page.content()
                            page.close()

                            # Log page sample for debugging
                            title = page.title() if not page.is_closed() else "Unknown"
                            print(
                                f"SH_SCRAPE: Page loaded, title='{title}', size={len(html_content)}",
                                flush=True,
                            )

                            # Extract odds from DOM
                            events_data = extract_odds_from_dom(html_content, url)

                            if events_data:
                                publish_odds(r, events_data, url)
                                state["last_ok_ts"] = time.strftime(
                                    "%Y-%m-%dT%H:%M:%S+00:00"
                                )
                                state["last_err"] = None
                                success = True
                                break
                            else:
                                print(
                                    f"SH_SCRAPE: No odds extracted from {url}",
                                    flush=True,
                                )
                                # Save HTML sample for debugging
                                sample = html_content[:2000].replace("\n", " ")
                                print(
                                    f"SH_SCRAPE: HTML sample: {sample}...", flush=True
                                )
                        else:
                            print(
                                f"SH_SCRAPE: Bad response {response.status} from {url}",
                                flush=True,
                            )
                            if page and not page.is_closed():
                                page.close()

                    except Exception as e:
                        print(f"SH_SCRAPE: Error loading {url}: {e}", flush=True)
                        state["last_err"] = str(e)
                        if "page" in locals() and not page.is_closed():
                            page.close()
                        continue

                if not success:
                    print(
                        f"SH_SCRAPE: All URLs failed for poll #{state['polls']}",
                        flush=True,
                    )

            except Exception as e:
                print(f"SH_SCRAPE: Loop error: {e}", flush=True)
                state["last_err"] = str(e)

            time.sleep(SCRAPE_INTERVAL)


@app.route("/healthz")
def healthz():
    """Health check endpoint"""
    return jsonify(state)


def main():
    """Start scraper and health server"""
    scraper_thread = threading.Thread(target=scrape_loop, daemon=True)
    scraper_thread.start()

    print(f"SH_SCRAPE: Health server starting on port {HEALTHZ_PORT}", flush=True)
    app.run(host="0.0.0.0", port=HEALTHZ_PORT, debug=False)


if __name__ == "__main__":
    main()

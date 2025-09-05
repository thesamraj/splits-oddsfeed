#!/usr/bin/env python3
import requests
import json
import re

session = requests.Session()
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://sportsbook.draftkings.com/",
    "Origin": "https://sportsbook.draftkings.com",
}
session.headers.update(headers)

# First, get the main page to establish session
print("Getting main page for session...")
resp = session.get("https://sportsbook.draftkings.com/")
print(f"Main page status: {resp.status_code}")

# Try the offers endpoint that returned 200
print("\nTrying offers endpoint...")
url = "https://sportsbook.draftkings.com/api/odds/v2/leagues/88808/offers"
resp = session.get(url)
print(f"Offers endpoint status: {resp.status_code}")
print(f"Content type: {resp.headers.get('content-type', 'unknown')}")

# Check if it's HTML with embedded data
if "text/html" in resp.headers.get("content-type", ""):
    print("Got HTML response, looking for embedded data...")

    # Look for JSON data in script tags
    matches = re.findall(
        r"<script[^>]*>.*?window\.__INITIAL_STATE__\s*=\s*({.*?});.*?</script>",
        resp.text,
        re.DOTALL,
    )
    if matches:
        print(f"Found {len(matches)} embedded state objects")

    # Look for Next.js data
    matches = re.findall(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', resp.text)
    if matches:
        print("Found __NEXT_DATA__")
        try:
            data = json.loads(matches[0])
            print(f"Next.js data keys: {list(data.keys())}")
            if "props" in data:
                print(f"Props keys: {list(data['props'].keys())[:10]}")
        except:
            pass

    # Look for any odds patterns in HTML
    odds_patterns = re.findall(r"[-+]\d{3,4}(?=\D|$)", resp.text)
    if odds_patterns:
        print(f"Found {len(odds_patterns)} potential odds values")
        print(f"Sample odds: {odds_patterns[:10]}")

# Try specific sport endpoints
print("\nTrying specific sport pages...")
sport_urls = [
    "https://sportsbook.draftkings.com/leagues/football/nfl",
    "https://sportsbook.draftkings.com/leagues/basketball/nba",
    "https://sportsbook.draftkings.com/leagues/baseball/mlb",
]

for url in sport_urls:
    try:
        resp = session.get(url, timeout=10)
        print(f"\n{url}: {resp.status_code}")

        # Look for embedded data
        if "__INITIAL_STATE__" in resp.text:
            print("  Found __INITIAL_STATE__")
            # Extract it
            start = resp.text.find("window.__INITIAL_STATE__ = ") + len(
                "window.__INITIAL_STATE__ = "
            )
            end = resp.text.find("};", start) + 1
            if start > 0 and end > start:
                try:
                    data = json.loads(resp.text[start:end])
                    # Check for actual data
                    has_events = any(
                        [
                            len(data.get("eventGroups", {})) > 0,
                            len(data.get("offers", {})) > 0,
                            len(data.get("outcomes", {})) > 0,
                        ]
                    )
                    if has_events:
                        print(
                            f"  ✓ Found data! EventGroups: {len(data.get('eventGroups', {}))}, Offers: {len(data.get('offers', {}))}"
                        )
                        # Save this one
                        league = url.split("/")[-1]
                        with open(f"dk_{league}_data.json", "w") as f:
                            json.dump(data, f, indent=2)
                        print(f"  Saved to dk_{league}_data.json")

                        # Show sample offer
                        if data.get("offers"):
                            offer_id = list(data["offers"].keys())[0]
                            offer = data["offers"][offer_id]
                            print(f"  Sample offer: {offer}")
                            break  # Found working endpoint
                except Exception as e:
                    print(f"  Error parsing: {e}")

        # Look for odds in HTML
        odds = re.findall(r"[-+]\d{3,4}", resp.text)
        if odds:
            print(f"  Found {len(odds)} odds values")
    except Exception as e:
        print(f"{url}: Error - {str(e)[:50]}")

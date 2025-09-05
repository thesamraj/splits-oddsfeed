#!/usr/bin/env python3
import requests
import json
import time

# Try different approaches for DraftKings
session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
    }
)

print("Attempting DraftKings main page to get session...")
try:
    # First get the main page to establish session
    resp = session.get("https://sportsbook.draftkings.com/", timeout=10)
    print(f"Main page: {resp.status_code}")

    # Look for data in the HTML
    if "window.__INITIAL_STATE__" in resp.text:
        print("Found __INITIAL_STATE__ in page")
        # Extract the JSON data
        start = resp.text.find("window.__INITIAL_STATE__ = ") + len(
            "window.__INITIAL_STATE__ = "
        )
        end = resp.text.find("};", start) + 1
        if start > 0 and end > start:
            try:
                data = json.loads(resp.text[start:end])
                print(f"Extracted initial state with keys: {list(data.keys())[:10]}")

                # Look for odds data
                if "odds" in str(data):
                    print("Found odds data in initial state!")
                    # Save sample
                    with open("dk_initial_state.json", "w") as f:
                        json.dump(data, f, indent=2)
                    print("Saved to dk_initial_state.json")
            except:
                pass

    # Try API endpoints with session
    print("\nTrying API endpoints with session...")

    # These are commonly exposed endpoints
    test_urls = [
        "https://sportsbook.draftkings.com/api/odds/v2/leagues/88808/offers",
        "https://sportsbook-gql.draftkings.com/graphql",
        "https://sportsbook.draftkings.com/__data.json",
        "https://sportsbook.draftkings.com/leagues/football/nfl",
    ]

    for url in test_urls:
        try:
            if "graphql" in url:
                # Try GraphQL introspection
                resp = session.post(
                    url, json={"query": "{ __schema { types { name } } }"}, timeout=5
                )
            else:
                resp = session.get(url, timeout=5)

            print(f"{url}: {resp.status_code}")
            if resp.status_code == 200:
                content = resp.text[:200]
                if "{" in content or "[" in content:
                    print(f"  Found JSON/data: {content[:100]}...")
        except Exception as e:
            print(f"{url}: Error - {str(e)[:50]}")
        time.sleep(1)

except Exception as e:
    print(f"Error: {e}")

# Also try to find their CDN/static endpoints
print("\nChecking for CDN/static data...")
cdn_urls = [
    "https://dk-static.sportsbook.draftkings.com/api/odds/live",
    "https://sportsbook-static.draftkings.com/data/odds",
    "https://www.draftkings.com/sportsbook-api/odds",
]

for url in cdn_urls:
    try:
        resp = requests.get(url, headers=session.headers, timeout=5)
        print(f"{url}: {resp.status_code}")
    except Exception as e:
        print(f"{url}: Error - {str(e)[:30]}")

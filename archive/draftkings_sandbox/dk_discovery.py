#!/usr/bin/env python3
import requests
import time

# DraftKings endpoint discovery
endpoints = [
    # Public/featured endpoints
    "https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/88808/categories/492/subcategories/4511",  # NFL
    "https://sportsbook.draftkings.com/sites/US-PA-SB/api/v5/eventgroups/88808",  # PA variant
    "https://sportsbook-nash.draftkings.com/api/sportscontent/v1/leagues",
    "https://api.draftkings.com/sportsbook/v1/odds",
    "https://sportsbook.draftkings.com/api/odds/v2/leagues/88808/offers",
    "https://sb-content.draftkings.com/api/v1/events",
    # WebSocket discovery endpoints
    "https://sportsbook.draftkings.com/sportsbook-odds-feed/",
    "https://web-ops.draftkings.com/odds/feed",
    # Mobile API endpoints
    "https://api.draftkings.com/sportsbook/v3/featured",
    "https://sportsbook-feed.draftkings.com/events",
    # State-specific
    "https://sportsbook.draftkings.com/sites/US-NJ-SB/api/v5/eventgroups/88808",
    "https://sportsbook.draftkings.com/sites/US-NY-SB/api/v5/eventgroups/88808",
    "https://sportsbook.draftkings.com/sites/US-MI-SB/api/v5/eventgroups/88808",
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

print("Testing DraftKings endpoints...")
for url in endpoints:
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code == 200:
            content = resp.text[:100]
            if "{" in content or "[" in content:
                print(f"✓ {resp.status_code} JSON: {url}")
                # Try to parse and show structure
                try:
                    data = resp.json()
                    if isinstance(data, dict):
                        print(f"  Keys: {list(data.keys())[:5]}")
                    elif isinstance(data, list) and len(data) > 0:
                        print(f"  Array[{len(data)}] items")
                except:
                    pass
            else:
                print(f"✓ {resp.status_code} HTML: {url}")
        else:
            print(f"✗ {resp.status_code}: {url}")
    except Exception as e:
        print(f"✗ Error: {url} - {str(e)[:50]}")
    time.sleep(0.5)

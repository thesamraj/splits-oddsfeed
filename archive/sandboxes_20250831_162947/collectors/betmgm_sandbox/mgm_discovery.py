#!/usr/bin/env python3
import requests
import time
import re

# BetMGM endpoint discovery
endpoints = [
    # Main BetMGM endpoints
    "https://sports.betmgm.com/cds-api/bettingoffer/fixtures",
    "https://sports.betmgm.com/cds-api/bettingoffer/listview/all/basketball",
    "https://sports.betmgm.com/cds-api/bettingoffer/listview/all/american-football",
    "https://sports.betmgm.com/en/sports",
    "https://sports.betmgm.com/api/v1/sports",
    # Legacy Entain/XML endpoints
    "https://sports.betmgm.com/feeds/events/",
    "https://sports.betmgm.com/feeds/odds/",
    "https://api.betmgm.com/sports/events",
    # Regional variants
    "https://sports.nj.betmgm.com/cds-api/bettingoffer/fixtures",
    "https://sports.pa.betmgm.com/cds-api/bettingoffer/fixtures",
    "https://sports.mi.betmgm.com/cds-api/bettingoffer/fixtures",
    # Mobile API endpoints
    "https://api.betmgm.com/v2/sports/events",
    "https://mobile.betmgm.com/api/sports",
    # Widget/embed endpoints
    "https://widgets.betmgm.com/api/events",
    "https://sportsbook.betmgm.com/api/events",
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

print("Testing BetMGM endpoints...")
for url in endpoints:
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        print(f"{url}: {resp.status_code}")

        if resp.status_code == 200:
            content = resp.text[:200]
            if "{" in content or "[" in content:
                print("  Found JSON/data")
                try:
                    data = resp.json()
                    if isinstance(data, dict):
                        print(f"  Keys: {list(data.keys())[:5]}")
                    elif isinstance(data, list) and len(data) > 0:
                        print(f"  Array[{len(data)}] items")
                except:
                    pass
            elif "<" in content:
                print("  HTML response")
                # Look for embedded data
                if "__INITIAL_STATE__" in resp.text:
                    print("  Found __INITIAL_STATE__")
                if "window.APP_STATE" in resp.text:
                    print("  Found APP_STATE")
                # Check for odds patterns
                odds = re.findall(r"[-+]\d{3,4}", resp.text[:5000])
                if odds:
                    print(f"  Found {len(odds)} potential odds values")

    except Exception as e:
        print(f"{url}: Error - {str(e)[:50]}")
    time.sleep(0.5)

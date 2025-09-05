#!/usr/bin/env python3
"""Test PointsBet API directly to diagnose issues"""
import requests
import json
import time

print("Testing PointsBet API access from host machine...")
print("=" * 60)

# Test different header combinations
test_configs = [
    {
        "name": "Basic headers",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
        },
    },
    {
        "name": "Full browser headers",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Referer": "https://pointsbet.com/",
            "Origin": "https://pointsbet.com",
        },
    },
    {"name": "Minimal headers", "headers": {}},
]

urls = [
    "https://api.pointsbet.com/api/v2/sports/featured",
    "https://api.pointsbet.com/api/v2/sports/american-football/events/featured",
    "https://api.au.pointsbet.com/api/v2/sports/rugby-league/events/featured",
]

for config in test_configs:
    print(f"\nTesting with {config['name']}:")
    print("-" * 40)

    session = requests.Session()
    session.headers.update(config["headers"])

    for url in urls:
        try:
            resp = session.get(url, timeout=10, allow_redirects=False)
            print(f"\n  URL: {url}")
            print(f"  Status: {resp.status_code}")

            if resp.status_code == 200:
                if resp.text:
                    print(f"  Response length: {len(resp.text)} bytes")
                    try:
                        data = json.loads(resp.text)
                        if "events" in data:
                            print(f"  ✅ SUCCESS: {len(data['events'])} events found!")
                            if data["events"]:
                                event = data["events"][0]
                                print(
                                    f"     Sample: {event.get('homeTeam', 'N/A')} vs {event.get('awayTeam', 'N/A')}"
                                )
                        elif "sports" in data:
                            print(f"  ✅ SUCCESS: {len(data['sports'])} sports found!")
                    except json.JSONDecodeError:
                        print(f"  ❌ Not JSON: {resp.text[:100]}")
                else:
                    print("  ❌ Empty response (0 bytes)")
            elif resp.status_code == 403:
                print("  ❌ Forbidden - likely blocked")
            elif resp.status_code == 302 or resp.status_code == 301:
                print(f"  ➡️ Redirect to: {resp.headers.get('Location', 'unknown')}")

        except Exception as e:
            print(f"  ❌ Error: {e}")

        time.sleep(1)  # Small delay between requests

print("\n" + "=" * 60)
print("DIAGNOSIS:")
print("If all requests return empty or 403, PointsBet may be:")
print("1. Blocking your IP/location")
print("2. Requiring authentication")
print("3. Using advanced bot detection")
print("4. API may have moved or changed")

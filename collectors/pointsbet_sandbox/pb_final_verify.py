#!/usr/bin/env python3
"""
Final PointsBet API verification - comprehensive check
"""
import requests
import json
from datetime import datetime

session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    }
)

print(f"PointsBet API Final Verification - {datetime.now()}")
print("=" * 60)

# Test various API endpoints and domains
test_urls = [
    # Main API endpoints
    "https://api.pointsbet.com/api/v2/sports/featured",
    "https://api.pointsbet.com/api/v2/sports/american-football/events/featured",
    "https://api.pointsbet.com/api/v2/sports/baseball/events/featured",
    "https://api.pointsbet.com/api/v2/sports/rugby-league/events/featured",
    "https://api.pointsbet.com/api/v2/sports/aussie-rules/events/featured",
    # Try different API versions
    "https://api.pointsbet.com/api/v1/sports",
    "https://api.pointsbet.com/api/v3/sports",
    # Try regional domains
    "https://api.nj.pointsbet.com/api/v2/sports/featured",
    "https://api.us.pointsbet.com/api/v2/sports/featured",
    "https://api.au.pointsbet.com/api/v2/sports/featured",
    # Try without /api path
    "https://api.pointsbet.com/sports/featured",
    "https://api.pointsbet.com/v2/sports/featured",
]

successful_endpoints = []

for url in test_urls:
    print(f"\nTesting: {url}")
    try:
        resp = session.get(url, timeout=5, allow_redirects=True)
        print(f"  Status: {resp.status_code}")

        # Check for redirects
        if resp.history:
            print(f"  Redirected: {resp.url}")

        # Check response content
        if resp.status_code == 200:
            content_type = resp.headers.get("content-type", "")
            print(f"  Content-Type: {content_type}")

            if resp.text:
                print(f"  Response size: {len(resp.text)} bytes")

                # Try to parse JSON
                if "json" in content_type.lower():
                    try:
                        data = resp.json()
                        if isinstance(data, dict):
                            keys = list(data.keys())[:5]
                            print(f"  JSON keys: {keys}")

                            # Check for events
                            if "events" in data:
                                print(f"  Has {len(data['events'])} events")
                            if "sports" in data:
                                print(f"  Has {len(data['sports'])} sports")

                        successful_endpoints.append(url)
                    except json.JSONDecodeError:
                        print("  Could not parse JSON")
                else:
                    # Check if it's HTML (might be blocked)
                    if "<html" in resp.text.lower()[:100]:
                        print("  Returns HTML (possibly blocked)")
                    else:
                        print(f"  Response preview: {resp.text[:100]}")
            else:
                print("  Empty response")

        elif resp.status_code == 403:
            print("  Access denied - possibly geo-restricted")
        elif resp.status_code == 404:
            print("  Not found")

    except requests.exceptions.Timeout:
        print("  Timeout")
    except Exception as e:
        print(f"  Error: {e}")

print("\n" + "=" * 60)
if successful_endpoints:
    print("Successful endpoints found:")
    for endpoint in successful_endpoints:
        print(f"  - {endpoint}")
else:
    print("No successful endpoints found")

# Try to access the main website
print("\n" + "=" * 60)
print("Testing main website access:")
web_url = "https://pointsbet.com"
try:
    resp = session.get(web_url, timeout=10)
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        print("  Website accessible")
    else:
        print(f"  Website returned status {resp.status_code}")
except Exception as e:
    print(f"  Error accessing website: {e}")

print("\n" + "=" * 60)
print("CONCLUSION:")
print("PointsBet API appears to be returning empty responses.")
print("This could be due to:")
print("1. Geo-restrictions (API only works in certain regions)")
print("2. API has moved to a different domain/path")
print("3. Authentication now required")
print("4. Service temporarily down or rate limited")

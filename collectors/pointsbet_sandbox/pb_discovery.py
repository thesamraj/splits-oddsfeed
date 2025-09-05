#!/usr/bin/env python3
"""
PointsBet endpoint discovery
Testing for WebSocket connections and API endpoints
"""
import requests
import json
import re

# Test endpoints for PointsBet
endpoints = [
    # Main site
    ("https://pointsbet.com/", "GET"),
    ("https://sportsbook.pointsbet.com/", "GET"),
    # API endpoints (various versions)
    ("https://api.pointsbet.com/api/v2/sports", "GET"),
    ("https://api.pointsbet.com/api/v2/events", "GET"),
    ("https://api.pointsbet.com/api/v2/competitions/featured", "GET"),
    ("https://api.pointsbet.com/api/mes/v3/events", "GET"),
    # State-specific endpoints
    ("https://nj.pointsbet.com/api/v2/events", "GET"),
    ("https://ny.pointsbet.com/api/v2/events", "GET"),
    ("https://co.pointsbet.com/api/v2/events", "GET"),
    ("https://mi.pointsbet.com/api/v2/events", "GET"),
    ("https://il.pointsbet.com/api/v2/events", "GET"),
    # Featured/Popular events (often public)
    ("https://api.pointsbet.com/api/v2/competitions/8531/events/featured", "GET"),
    ("https://api.pointsbet.com/api/v2/sports/1/events", "GET"),
    ("https://api.pointsbet.com/api/v2/sports/3/events", "GET"),
    # WebSocket discovery
    ("https://ws.pointsbet.com/", "GET"),
    ("https://stream.pointsbet.com/", "GET"),
    ("https://push.pointsbet.com/", "GET"),
    # Feed endpoints
    ("https://feeds.pointsbet.com/sportsbook/v1/events", "GET"),
    ("https://data.pointsbet.com/api/events", "GET"),
    # CDN/Static data
    ("https://cdn.pointsbet.com/api/events.json", "GET"),
    ("https://static.pointsbet.com/data/odds.json", "GET"),
]

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

print("=" * 80)
print("PointsBet Endpoint Discovery")
print("=" * 80)

successful_endpoints = []

for endpoint in endpoints:
    url = endpoint[0]
    method = endpoint[1]

    print(f"\nTesting: {url}")

    try:
        resp = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
        print(f"  Status: {resp.status_code}")

        if resp.status_code == 200:
            print("  ✓ SUCCESS - Status 200")

            # Check content type
            content_type = resp.headers.get("content-type", "")
            if "json" in content_type:
                print("  JSON response detected")
                try:
                    data = resp.json()
                    if isinstance(data, dict):
                        print(f"    Keys: {list(data.keys())[:10]}")
                    elif isinstance(data, list) and len(data) > 0:
                        print(f"    Array with {len(data)} items")
                        if isinstance(data[0], dict):
                            print(f"    First item keys: {list(data[0].keys())[:10]}")

                    # Save successful JSON response
                    filename = (
                        url.replace("https://", "").replace("/", "_")[:50] + ".json"
                    )
                    with open(filename, "w") as f:
                        json.dump(data, f, indent=2)
                    print(f"    Saved to {filename}")

                except Exception as e:
                    print(f"    JSON parse error: {e}")

            elif "html" in content_type:
                print("  HTML response")
                # Look for embedded data
                if "__INITIAL_STATE__" in resp.text:
                    print("  ✓ Found __INITIAL_STATE__")
                if "window.APP_STATE" in resp.text:
                    print("  ✓ Found APP_STATE")

                # Look for WebSocket URLs
                ws_urls = re.findall(r'wss?://[^"\'\s]+', resp.text)
                if ws_urls:
                    print("  ✓ Found WebSocket URLs:")
                    for ws_url in ws_urls[:5]:
                        print(f"      {ws_url}")

                # Look for API endpoints in HTML
                api_urls = re.findall(
                    r'["\'](https?://[^"\']*api[^"\']*pointsbet[^"\']*)["\']',
                    resp.text[:50000],
                )
                if api_urls:
                    print("  Found API URLs in HTML:")
                    for api_url in list(set(api_urls))[:10]:
                        print(f"      {api_url}")

            successful_endpoints.append(
                {"url": url, "content_type": content_type, "length": len(resp.text)}
            )

        elif resp.status_code in [301, 302, 307, 308]:
            print(f"  → Redirected to: {resp.url}")
        elif resp.status_code == 403:
            print("  ⛔ Forbidden")
        elif resp.status_code == 401:
            print("  🔐 Requires authentication")
        elif resp.status_code == 404:
            print("  ✗ Not found")

    except requests.exceptions.SSLError:
        print("  ✗ SSL Error")
    except requests.exceptions.ConnectionError:
        print("  ✗ Connection failed")
    except requests.exceptions.Timeout:
        print("  ✗ Timeout")
    except Exception as e:
        print(f"  ✗ Error: {str(e)[:50]}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

if successful_endpoints:
    print(f"\nFound {len(successful_endpoints)} accessible endpoints:")
    for ep in successful_endpoints:
        print(f"\n{ep['url']}")
        print(f"  Content-Type: {ep['content_type']}")
        print(f"  Size: {ep['length']} bytes")
else:
    print("\nNo directly accessible endpoints found.")
    print("Will need to try web scraping or browser automation.")

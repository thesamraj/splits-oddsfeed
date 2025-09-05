#!/usr/bin/env python3
"""
Caesars Sportsbook endpoint discovery
Caesars merged with William Hill, so checking both legacy and new endpoints
"""
import requests
import re

# Test endpoints - Caesars and William Hill legacy
endpoints = [
    # Caesars main endpoints
    ("https://sportsbook.caesars.com/", "GET", None),
    ("https://api.caesars.com/sportsbook/v1/events", "GET", None),
    ("https://api.caesars.com/api/v1/sports", "GET", None),
    ("https://www.caesars.com/sportsbook-api/events", "GET", None),
    # Legacy William Hill endpoints
    ("https://www.williamhill.com/us/nj/bet/api/v2/events", "GET", None),
    ("https://sports.williamhill.com/betting/en-us", "GET", None),
    ("https://api.williamhill.com/v2/events", "GET", None),
    # State-specific endpoints
    ("https://nj.caesars.com/api/events", "GET", None),
    ("https://ny.caesars.com/api/events", "GET", None),
    ("https://pa.caesars.com/api/events", "GET", None),
    ("https://mi.caesars.com/api/events", "GET", None),
    # Mobile/App endpoints
    ("https://mobile.caesars.com/api/sportsbook", "GET", None),
    ("https://app-api.caesars.com/v1/events", "GET", None),
    # CDN/Static endpoints
    ("https://cdn.caesars.com/sportsbook/odds.json", "GET", None),
    ("https://static.caesars.com/api/events.json", "GET", None),
    # RSS/Feed endpoints
    ("https://www.caesars.com/feeds/sportsbook/rss", "GET", None),
    ("https://feeds.caesars.com/sportsbook/v1/events", "GET", None),
    # GraphQL endpoints
    (
        "https://api.caesars.com/graphql",
        "POST",
        {"query": "{ events { id name odds } }"},
    ),
    # WebSocket discovery endpoints
    ("https://ws.caesars.com/socket.io/", "GET", None),
    ("https://push.caesars.com/events", "GET", None),
]

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

print("=" * 80)
print("Caesars Sportsbook Endpoint Discovery")
print("=" * 80)

successful_endpoints = []

for endpoint in endpoints:
    url = endpoint[0]
    method = endpoint[1]
    data = endpoint[2] if len(endpoint) > 2 else None

    print(f"\nTesting: {url}")
    print(f"Method: {method}")

    try:
        if method == "POST":
            resp = requests.post(url, json=data, headers=headers, timeout=10)
        else:
            resp = requests.get(url, headers=headers, timeout=10, allow_redirects=True)

        print(f"Status: {resp.status_code}")

        if resp.status_code == 200:
            print("✓ SUCCESS - Status 200")

            # Check content type
            content_type = resp.headers.get("content-type", "")
            if "json" in content_type:
                print("  JSON response detected")
                try:
                    data = resp.json()
                    if isinstance(data, dict):
                        print(f"  Keys: {list(data.keys())[:5]}")
                    elif isinstance(data, list) and len(data) > 0:
                        print(f"  Array with {len(data)} items")
                except:
                    pass
            elif "html" in content_type:
                print("  HTML response")
                # Check for embedded data
                if "__INITIAL_STATE__" in resp.text:
                    print("  ✓ Found __INITIAL_STATE__")
                if "window.APP_STATE" in resp.text:
                    print("  ✓ Found APP_STATE")
                if "caesarsOdds" in resp.text or "williamhill" in resp.text:
                    print("  ✓ Found odds references")

                # Look for API endpoints in HTML
                api_pattern = re.findall(
                    r'["\'](https?://[^"\']*api[^"\']*)["\']', resp.text[:10000]
                )
                if api_pattern:
                    print(f"  Found API URLs: {api_pattern[:3]}")

            successful_endpoints.append(
                {
                    "url": url,
                    "method": method,
                    "content_type": content_type,
                    "sample": resp.text[:500],
                }
            )

        elif resp.status_code in [301, 302, 307, 308]:
            print(f"→ Redirected to: {resp.url}")
            if resp.url != url:
                successful_endpoints.append(
                    {"url": resp.url, "method": "GET", "note": f"Redirected from {url}"}
                )
        elif resp.status_code == 403:
            print("⛔ Forbidden (likely Cloudflare)")
        elif resp.status_code == 401:
            print("🔐 Requires authentication")
        elif resp.status_code == 404:
            print("✗ Not found")

    except requests.exceptions.SSLError:
        print("✗ SSL Error")
    except requests.exceptions.ConnectionError:
        print("✗ Connection failed")
    except requests.exceptions.Timeout:
        print("✗ Timeout")
    except Exception as e:
        print(f"✗ Error: {str(e)[:50]}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

if successful_endpoints:
    print(f"\nFound {len(successful_endpoints)} accessible endpoints:")
    for ep in successful_endpoints:
        print(f"\n{ep['url']}")
        if "note" in ep:
            print(f"  Note: {ep['note']}")
        if "content_type" in ep:
            print(f"  Type: {ep['content_type']}")
        if "sample" in ep:
            sample = ep["sample"][:100].replace("\n", " ")
            print(f"  Sample: {sample}...")
else:
    print("\nNo directly accessible endpoints found.")
    print("Will need to try web scraping or browser automation.")

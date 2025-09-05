#!/usr/bin/env python3
"""
PointsBet API explorer - find working endpoints with actual data
"""
import requests
import json
import time

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

# Try different competition IDs and sport IDs
base_url = "https://api.pointsbet.com/api/v2"

print("PointsBet API Explorer")
print("=" * 80)

# Try different endpoints with various IDs
endpoints_to_try = [
    # Sports endpoints
    f"{base_url}/sports",
    f"{base_url}/sports/featured",
    # Competition endpoints with different IDs (NFL, NBA, MLB, NHL, etc.)
    f"{base_url}/competitions/featured",
    f"{base_url}/competitions/1/events/featured",  # NFL
    f"{base_url}/competitions/2/events/featured",  # NBA
    f"{base_url}/competitions/3/events/featured",  # MLB
    f"{base_url}/competitions/4/events/featured",  # NHL
    f"{base_url}/competitions/5/events/featured",
    f"{base_url}/competitions/10/events/featured",
    f"{base_url}/competitions/100/events/featured",
    f"{base_url}/competitions/1000/events/featured",
    # Events endpoints
    f"{base_url}/events/featured",
    f"{base_url}/events/upcoming",
    f"{base_url}/events/live",
    f"{base_url}/events/popular",
    # Market endpoints
    f"{base_url}/markets/featured",
    # Try v3 API
    "https://api.pointsbet.com/api/v3/events/featured",
    "https://api.pointsbet.com/api/v3/sports",
    # Try MES (Market Event Service) API
    "https://api.pointsbet.com/api/mes/v2/events",
    "https://api.pointsbet.com/api/mes/v3/competitions",
    # Try different jurisdictions
    "https://api.nj.pointsbet.com/api/v2/events/featured",
    "https://api.ny.pointsbet.com/api/v2/events/featured",
    "https://api.co.pointsbet.com/api/v2/events/featured",
    # Try with query parameters
    f"{base_url}/events?jurisdiction=NJ",
    f"{base_url}/events?sport=american-football",
    f"{base_url}/events?competition=nfl",
]

successful_endpoints = []

for url in endpoints_to_try:
    print(f"\nTrying: {url}")
    try:
        resp = requests.get(url, headers=headers, timeout=5)
        print(f"  Status: {resp.status_code}")

        if resp.status_code == 200:
            try:
                data = resp.json()

                # Check if we got actual data
                has_data = False
                if isinstance(data, dict):
                    if "events" in data and data["events"]:
                        has_data = True
                        print(f"  ✓ Found {len(data['events'])} events!")
                    elif "data" in data and data["data"]:
                        has_data = True
                        print("  ✓ Found data!")
                    elif any(
                        k for k in data.keys() if k not in ["key", "name", "nextPage"]
                    ):
                        has_data = True
                        print(f"  ✓ Has content: {list(data.keys())[:10]}")
                elif isinstance(data, list) and len(data) > 0:
                    has_data = True
                    print(f"  ✓ Found {len(data)} items!")

                if has_data:
                    successful_endpoints.append({"url": url, "data": data})

                    # Save the response
                    filename = f"pb_success_{len(successful_endpoints)}.json"
                    with open(filename, "w") as f:
                        json.dump(data, f, indent=2)
                    print(f"  Saved to {filename}")
                else:
                    print(f"  Empty response: {data}")

            except json.JSONDecodeError:
                print("  Not JSON")
        elif resp.status_code == 403:
            print("  Blocked/Forbidden")
        elif resp.status_code == 404:
            print("  Not found")

    except Exception as e:
        print(f"  Error: {str(e)[:50]}")

    time.sleep(0.5)

print("\n" + "=" * 80)
print("RESULTS")
print("=" * 80)

if successful_endpoints:
    print(f"\nFound {len(successful_endpoints)} endpoints with data:")
    for ep in successful_endpoints:
        print(f"\n{ep['url']}")
        data = ep["data"]
        if isinstance(data, dict) and "events" in data:
            events = data["events"]
            if events and len(events) > 0:
                print(f"  Sample event: {events[0]}")
else:
    print("\nNo endpoints with actual data found.")
    print("Will need to try web scraping the main site.")

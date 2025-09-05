#!/usr/bin/env python3
import requests
import json
from datetime import datetime

# Find active competitions
session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
    }
)

base_url = "https://api.pointsbet.com/api/v2"

print(f"Finding active PointsBet competitions at {datetime.now()}")
print("=" * 50)

# Try to find competitions endpoint
urls_to_try = [
    f"{base_url}/competitions",
    f"{base_url}/sports",
    f"{base_url}/categories",
    f"{base_url}/sports/featured",
    f"{base_url}/competitions/featured",
    f"{base_url}/events",
    "https://api.au.pointsbet.com/api/v2/competitions/featured",
    "https://api.nj.pointsbet.com/api/v2/competitions/featured",
]

for url in urls_to_try:
    print(f"\nTrying: {url}")
    try:
        resp = session.get(url, timeout=10)
        print(f"  Status: {resp.status_code}")

        if resp.status_code == 200:
            if resp.text and resp.text.strip():
                print(f"  Response length: {len(resp.text)} bytes")

                # Try to parse JSON
                try:
                    data = resp.json()

                    # Check what keys are in the response
                    if isinstance(data, dict):
                        print(f"  Keys: {list(data.keys())[:10]}")

                        # Look for events
                        if "events" in data:
                            events = data["events"]
                            print(f"  Found {len(events)} events")
                            if events:
                                event = events[0]
                                print(
                                    f"    Sample: {event.get('homeTeam', 'N/A')} vs {event.get('awayTeam', 'N/A')}"
                                )

                        # Look for competitions
                        if "competitions" in data:
                            comps = data["competitions"]
                            print(f"  Found {len(comps)} competitions")
                            if comps:
                                comp = comps[0]
                                print(f"    Sample competition: {comp}")

                    elif isinstance(data, list):
                        print(f"  Array with {len(data)} items")
                        if data:
                            print(f"    First item: {str(data[0])[:200]}")

                except json.JSONDecodeError as e:
                    print(f"  Could not parse JSON: {e}")
                    print(f"  Response preview: {resp.text[:200]}")
            else:
                print("  Empty response")
        elif resp.status_code == 404:
            print("  Not found")
        else:
            print(f"  Response: {resp.text[:100]}")

    except Exception as e:
        print(f"  Error: {e}")

# Also try a known working sports ID range
print("\n\nTrying competition ID range 1-20000 with step 1000:")
found_competitions = []

for comp_id in range(1000, 20000, 1000):
    url = f"{base_url}/competitions/{comp_id}/events/featured"
    try:
        resp = session.get(url, timeout=2)
        if resp.status_code == 200 and resp.text and len(resp.text) > 10:
            try:
                data = resp.json()
                if "events" in data and data["events"]:
                    print(f"  Competition {comp_id}: {len(data['events'])} events")
                    found_competitions.append(comp_id)
            except:
                pass
    except:
        pass

if found_competitions:
    print(f"\nFound active competitions: {found_competitions}")
else:
    print("\nNo active competitions found in ID range")

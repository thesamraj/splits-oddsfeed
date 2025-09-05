#!/usr/bin/env python3
import requests
import json
from datetime import datetime

# Test PointsBet API endpoints
session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Origin": "https://pointsbet.com",
        "Referer": "https://pointsbet.com/",
    }
)

base_url = "https://api.pointsbet.com/api/v2"

# Test different competition IDs
competitions = {
    "NFL": "11444",
    "MLB": "112658",
    "WNBA": "126714",
    "MLS": "135827",
}

print(f"Testing PointsBet API at {datetime.now()}")
print("=" * 50)

for sport, comp_id in competitions.items():
    print(f"\nTesting {sport} (competition {comp_id}):")

    # Try featured endpoint first
    url = f"{base_url}/competitions/{comp_id}/events/featured"
    try:
        resp = session.get(url, timeout=10)
        print(f"  Featured endpoint status: {resp.status_code}")

        if resp.status_code == 200:
            if resp.text:
                try:
                    data = resp.json()
                    events = data.get("events", [])
                    print(f"  Found {len(events)} events")

                    # Show first event if available
                    if events:
                        event = events[0]
                        print(
                            f"    Sample event: {event.get('homeTeam', 'N/A')} vs {event.get('awayTeam', 'N/A')}"
                        )
                        markets = event.get("fixedOddsMarkets", [])
                        print(f"    Markets available: {len(markets)}")
                except json.JSONDecodeError as e:
                    print(f"  JSON decode error: {e}")
            else:
                print("  Empty response body")
        else:
            print(f"  Response: {resp.text[:200]}")

    except Exception as e:
        print(f"  Error: {e}")

    # Also try the regular events endpoint
    url = f"{base_url}/competitions/{comp_id}/events"
    try:
        resp = session.get(url, timeout=10)
        print(f"  Events endpoint status: {resp.status_code}")

        if resp.status_code == 200 and resp.text:
            try:
                data = resp.json()
                events = data.get("events", [])
                print(f"  Found {len(events)} events")
            except:
                pass
    except Exception as e:
        print(f"  Error: {e}")

# Test the general featured events endpoint
print("\nTesting general featured events:")
url = f"{base_url}/events/featured"
try:
    resp = session.get(url, timeout=10)
    print(f"  Status: {resp.status_code}")

    if resp.status_code == 200 and resp.text:
        try:
            data = resp.json()
            events = data.get("events", [])
            print(f"  Found {len(events)} featured events")
        except:
            print("  Could not parse JSON")
except Exception as e:
    print(f"  Error: {e}")

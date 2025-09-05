#!/usr/bin/env python3
import requests
import json
from datetime import datetime

# Explore the sports/featured endpoint
session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
    }
)

base_url = "https://api.pointsbet.com/api/v2"

print(f"Exploring PointsBet sports/featured endpoint at {datetime.now()}")
print("=" * 50)

# Get sports data
url = f"{base_url}/sports/featured"
resp = session.get(url, timeout=10)

if resp.status_code == 200:
    data = resp.json()

    # Save full response for analysis
    with open("pb_sports_response.json", "w") as f:
        json.dump(data, f, indent=2)
    print("Saved full response to pb_sports_response.json")

    sports = data.get("sports", [])
    print(f"\nFound {len(sports)} sports")

    # Explore each sport
    for sport in sports[:10]:  # Limit to first 10
        sport_key = sport.get("key")
        sport_name = sport.get("name")

        print(f"\nSport: {sport_name} (key: {sport_key})")

        # Check for competitions
        competitions = sport.get("competitions", [])
        if competitions:
            print(f"  Found {len(competitions)} competitions:")
            for comp in competitions[:5]:  # Show first 5
                comp_key = comp.get("key")
                comp_name = comp.get("name")
                comp_id = comp.get("id")
                print(f"    - {comp_name} (id: {comp_id}, key: {comp_key})")

                # Check for events in competition
                events = comp.get("events", [])
                if events:
                    print(f"      Has {len(events)} events")

        # Check for direct events
        events = sport.get("events", [])
        if events:
            print(f"  Has {len(events)} direct events")

            # Show sample event
            event = events[0]
            print(f"    Sample event: {event.get('name')}")
            print(f"      Teams: {event.get('homeTeam')} vs {event.get('awayTeam')}")

            # Check for markets
            markets = event.get("fixedOddsMarkets", [])
            if markets:
                print(f"      Has {len(markets)} markets")

    # Try to fetch events for specific sports
    print("\n\nTrying to fetch events for specific sports:")

    for sport in sports[:5]:
        sport_key = sport.get("key")
        sport_name = sport.get("name")

        # Try different URL patterns
        urls = [
            f"{base_url}/sports/{sport_key}/events",
            f"{base_url}/sports/{sport_key}/events/featured",
            f"{base_url}/sports/{sport_key}/competitions",
        ]

        for url in urls:
            try:
                resp = session.get(url, timeout=5)
                if resp.status_code == 200 and resp.text:
                    try:
                        data = resp.json()
                        if "events" in data and data["events"]:
                            print(
                                f"  {sport_name}: Found {len(data['events'])} events at {url}"
                            )
                            break
                    except:
                        pass
            except:
                pass

else:
    print(f"Failed to get sports data: {resp.status_code}")

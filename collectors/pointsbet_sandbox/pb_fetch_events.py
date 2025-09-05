#!/usr/bin/env python3
"""
Fetch actual events from PointsBet using discovered competition IDs
"""
import requests
import json
import time

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

# US Sports competition IDs from discovery
competitions = {
    "NFL": "11444",
    "NFL Preseason": "15206",
    "NBA": "7176",
    "MLB": "112658",
    "NHL": "7208",
    "WNBA": "126714",
    "MLS": "135827",
    "UFC": "7874",
}

print("Fetching PointsBet Events")
print("=" * 80)

all_events = []

for sport, comp_id in competitions.items():
    print(f"\nFetching {sport} (competition {comp_id})...")

    # Try both /featured and direct endpoint
    urls = [
        f"https://api.pointsbet.com/api/v2/competitions/{comp_id}/events/featured",
        f"https://api.pointsbet.com/api/v2/competitions/{comp_id}/events",
    ]

    for url in urls:
        try:
            resp = requests.get(url, headers=headers, timeout=10)

            if resp.status_code == 200:
                data = resp.json()

                if "events" in data and data["events"]:
                    events = data["events"]
                    print(f"  ✓ Found {len(events)} events!")

                    # Save the events
                    filename = f"pb_{sport.lower().replace(' ', '_')}_events.json"
                    with open(filename, "w") as f:
                        json.dump(data, f, indent=2)
                    print(f"  Saved to {filename}")

                    all_events.extend(events)

                    # Show sample event structure
                    if events:
                        print(f"  Sample event keys: {list(events[0].keys())[:15]}")
                        if "markets" in events[0]:
                            print(f"    Has markets: {len(events[0]['markets'])}")
                        if "outcomes" in events[0]:
                            print(f"    Has outcomes: {len(events[0]['outcomes'])}")

                    break  # Found events, no need to try other URL

                elif "events" in data:
                    print("  Empty events array")
                else:
                    print("  No events field in response")

        except Exception as e:
            print(f"  Error: {str(e)[:100]}")

        time.sleep(0.5)

print(f"\n{'=' * 80}")
print(f"TOTAL: Found {len(all_events)} events across all sports")

if all_events:
    # Save all events
    with open("pb_all_events.json", "w") as f:
        json.dump({"events": all_events}, f, indent=2)
    print("All events saved to pb_all_events.json")

    # Analyze event structure
    print("\nEvent Structure Analysis:")
    sample = all_events[0]

    def analyze_structure(obj, prefix=""):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, (dict, list)):
                    print(f"{prefix}{key}: {type(value).__name__}")
                    if isinstance(value, list) and value:
                        analyze_structure(value[0], prefix + "  ")
                    elif isinstance(value, dict):
                        analyze_structure(value, prefix + "  ")
                else:
                    print(f"{prefix}{key}: {type(value).__name__} = {str(value)[:50]}")

    analyze_structure(sample)

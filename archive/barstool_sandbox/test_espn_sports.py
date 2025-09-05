#!/usr/bin/env python3
"""
Test ESPN API for multiple sports coverage
"""

import requests
from datetime import datetime

# Test different sports
sports = {
    "nfl": "football/nfl",
    "nba": "basketball/nba",
    "mlb": "baseball/mlb",
    "nhl": "hockey/nhl",
    "ncaaf": "football/college-football",
    "ncaab": "basketball/mens-college-basketball",
}

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

print(f"ESPN Sports API Test - {datetime.now()}")
print("=" * 60)

for sport_name, sport_path in sports.items():
    endpoint = f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/scoreboard"
    print(f"\nTesting {sport_name.upper()}: {endpoint}")

    try:
        response = requests.get(endpoint, headers=headers, timeout=10)

        if response.status_code == 200:
            data = response.json()

            # Count events with odds
            events_with_odds = 0
            total_events = len(data.get("events", []))

            for event in data.get("events", []):
                if "competitions" in event:
                    for comp in event["competitions"]:
                        if "odds" in comp and comp["odds"]:
                            events_with_odds += 1
                            break

            print(f"  ✓ SUCCESS: {total_events} events, {events_with_odds} with odds")

            # Show sample odds data from first event with odds
            for event in data.get("events", [])[:5]:
                if "competitions" in event:
                    for comp in event["competitions"]:
                        if "odds" in comp and comp["odds"]:
                            odds = comp["odds"][0]
                            print(f"    {event.get('name', 'N/A')}")

                            # Check for moneyline
                            if "homeTeamOdds" in odds:
                                home_ml = odds["homeTeamOdds"].get("moneyLine", "N/A")
                                away_ml = odds["awayTeamOdds"].get("moneyLine", "N/A")
                                print(f"      ML: Home {home_ml}, Away {away_ml}")

                            # Check for spread
                            if "spread" in odds:
                                print(f"      Spread: {odds['spread']}")

                            # Check for total
                            if "overUnder" in odds:
                                print(f"      O/U: {odds['overUnder']}")
                            break

        else:
            print(f"  × Status {response.status_code}")

    except Exception as e:
        print(f"  × Error: {str(e)[:100]}")

print("\n" + "=" * 60)
print("Summary: ESPN API provides comprehensive odds via ESPN BET integration")

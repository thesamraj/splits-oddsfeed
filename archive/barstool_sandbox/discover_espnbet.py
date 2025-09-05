#!/usr/bin/env python3
"""
ESPN Bet (formerly Barstool) API Discovery
Note: Barstool Sportsbook was rebranded to ESPN Bet in November 2023
"""

import requests
import json
from datetime import datetime
import re

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

print(f"ESPN Bet API Discovery - {datetime.now()}")
print("=" * 60)

# First, let's check the main ESPN Bet site
print("\n1. Checking ESPN Bet main site...")
try:
    response = requests.get("https://espnbet.com", headers=headers, timeout=10)
    print(f"   Main site status: {response.status_code}")

    # Look for API endpoints in the HTML
    if response.status_code == 200:
        # Search for API URLs in the response
        api_patterns = [
            r'https?://[^"\s]*api[^"\s]*',
            r'https?://[^"\s]*sportsbook[^"\s]*',
            r'"(\/api\/[^"]*)"',
            r'wss?://[^"\s]*',
        ]

        found_urls = set()
        for pattern in api_patterns:
            matches = re.findall(pattern, response.text)
            for match in matches:
                if isinstance(match, str) and (
                    "espn" in match.lower() or "api" in match.lower()
                ):
                    found_urls.add(match)

        if found_urls:
            print("\n   Found potential API URLs in HTML:")
            for url in list(found_urls)[:10]:
                print(f"   - {url}")

except Exception as e:
    print(f"   Error: {e}")

# Test ESPN's known API patterns
print("\n2. Testing ESPN API patterns...")
test_endpoints = [
    # ESPN's standard API
    "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events",
    # ESPN Bet specific
    "https://espnbet.com/api/sportsbook/v1/events",
    "https://espnbet.com/api/v1/sports",
    "https://espnbet.com/sportsbook/api/events",
    # Common sportsbook patterns
    "https://sportsbook.espn.com/api/events",
    "https://api.espn.com/sportsbook/v1/events",
    # GraphQL endpoint
    "https://espnbet.com/graphql",
    "https://api.espnbet.com/graphql",
]

for endpoint in test_endpoints:
    print(f"\n   Testing: {endpoint}")
    try:
        response = requests.get(
            endpoint, headers=headers, timeout=10, allow_redirects=True
        )
        print(f"   Status: {response.status_code}")

        if response.status_code == 200:
            content_type = response.headers.get("Content-Type", "")
            print(f"   Content-Type: {content_type}")

            if "json" in content_type:
                try:
                    data = response.json()
                    print("   ✓ Valid JSON response!")

                    # Save the response
                    filename = f"espnbet_{endpoint.split('/')[-1]}.json"
                    with open(filename, "w") as f:
                        json.dump(data, f, indent=2)
                    print(f"   Saved to: {filename}")

                    # Analyze structure
                    if isinstance(data, dict):
                        print(f"   Keys: {list(data.keys())[:5]}")
                    elif isinstance(data, list):
                        print(f"   Array with {len(data)} items")

                except json.JSONDecodeError:
                    print("   Invalid JSON")
            else:
                print("   Non-JSON response")

    except requests.exceptions.Timeout:
        print("   Timeout")
    except requests.exceptions.ConnectionError:
        print("   Connection error")
    except Exception as e:
        print(f"   Error: {str(e)[:100]}")

print("\n" + "=" * 60)
print("Discovery complete!")

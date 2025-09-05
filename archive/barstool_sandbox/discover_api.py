#!/usr/bin/env python3
"""
Barstool Sportsbook API Discovery Script
"""

import requests
import json
from datetime import datetime

# Test various potential Barstool API endpoints
test_endpoints = [
    # Main sportsbook domain variations
    "https://api.barstoolsportsbook.com/api/v2/events",
    "https://api.barstoolsportsbook.com/api/v1/sports",
    "https://api.barstoolsportsbook.com/api/sports/featured",
    "https://sportsbook.barstoolsports.com/api/events",
    # Penn/ESPN Bet variations (Barstool migrated to ESPN Bet)
    "https://api.espnbet.com/api/v2/events",
    "https://api.espnbet.com/api/sports",
    "https://espnbet.com/api/v1/events/featured",
    # Legacy Barstool endpoints
    "https://barstoolbets.com/api/events",
    "https://api.barstoolbets.com/v1/sports",
    # Common patterns
    "https://sportsbook-api.barstoolsports.com/events",
    "https://odds.barstoolsports.com/api/events",
]

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

print(f"Starting Barstool API discovery at {datetime.now()}")
print("=" * 60)

for endpoint in test_endpoints:
    print(f"\nTesting: {endpoint}")
    try:
        response = requests.get(endpoint, headers=headers, timeout=10)
        print(f"  Status: {response.status_code}")

        if response.status_code == 200:
            print(
                f"  ✓ SUCCESS! Content-Type: {response.headers.get('Content-Type', 'unknown')}"
            )
            print(f"  Response size: {len(response.content)} bytes")

            # Try to parse as JSON
            try:
                data = response.json()
                print("  JSON structure detected")

                # Save successful response
                filename = f"barstool_response_{endpoint.replace('https://', '').replace('/', '_')}.json"
                with open(filename, "w") as f:
                    json.dump(data, f, indent=2)
                print(f"  Saved to: {filename}")

                # Analyze structure
                if isinstance(data, dict):
                    print(f"  Top-level keys: {list(data.keys())[:5]}")
                elif isinstance(data, list):
                    print(f"  Array with {len(data)} items")

            except json.JSONDecodeError:
                print("  Non-JSON response")

        elif response.status_code == 403:
            print("  × Forbidden (may need auth or different headers)")
        elif response.status_code == 404:
            print("  × Not found")
        else:
            print(f"  × Status {response.status_code}")

    except requests.exceptions.Timeout:
        print("  × Timeout")
    except requests.exceptions.ConnectionError as e:
        print(f"  × Connection error: {str(e)[:100]}")
    except Exception as e:
        print(f"  × Error: {str(e)[:100]}")

print("\n" + "=" * 60)
print("Discovery complete!")

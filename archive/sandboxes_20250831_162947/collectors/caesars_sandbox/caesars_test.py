#!/usr/bin/env python3
"""Quick test of Caesars endpoints"""
import requests
import re
import json

session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
)

# Try different Caesars URLs
urls = [
    "https://caesars.com/sportsbook",
    "https://www.caesars.com/sportsbook-and-casino",
    "https://sportsbook-nj.caesars.com/us/nj/bet",
    "https://sportsbook.caesars.com/us/ny/bet",
]

for url in urls:
    print(f"\nTrying: {url}")
    try:
        resp = session.get(url, timeout=10, allow_redirects=True)
        print(f"  Final URL: {resp.url}")
        print(f"  Status: {resp.status_code}")

        if resp.status_code == 200:
            # Look for JavaScript data
            if "__INITIAL_STATE__" in resp.text:
                print("  ✓ Found __INITIAL_STATE__")
                match = re.search(
                    r"window\.__INITIAL_STATE__\s*=\s*({.*?});", resp.text, re.DOTALL
                )
                if match:
                    try:
                        data = json.loads(match.group(1))
                        print(f"    Keys: {list(data.keys())[:10]}")

                        # Look for events/odds data
                        for key in data:
                            if (
                                "event" in key.lower()
                                or "odds" in key.lower()
                                or "market" in key.lower()
                            ):
                                print(f"    Found key: {key}")

                        # Save for analysis
                        with open("caesars_state.json", "w") as f:
                            json.dump(data, f, indent=2)
                        print("    Saved to caesars_state.json")
                        break
                    except:
                        pass

            # Look for API calls in scripts
            api_urls = re.findall(r'["\'](https?://[^"\']*api[^"\']*)["\']', resp.text)
            if api_urls:
                print("  Found API URLs:")
                for api_url in api_urls[:5]:
                    print(f"    {api_url}")

            # Look for WebSocket connections
            ws_urls = re.findall(r'wss?://[^"\'\s]+', resp.text)
            if ws_urls:
                print("  Found WebSocket URLs:")
                for ws_url in ws_urls[:3]:
                    print(f"    {ws_url}")

            # Check for specific patterns
            if "caesars" in resp.text.lower() and (
                "odds" in resp.text.lower() or "spread" in resp.text.lower()
            ):
                print("  ✓ Contains odds-related content")

    except Exception as e:
        print(f"  Error: {e}")

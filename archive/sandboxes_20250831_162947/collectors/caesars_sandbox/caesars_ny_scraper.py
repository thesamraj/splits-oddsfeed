#!/usr/bin/env python3
"""Test scraping Caesars NY sportsbook"""
import requests
from bs4 import BeautifulSoup
import re
import json

session = requests.Session()
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.caesars.com/",
    }
)

url = "https://sportsbook.caesars.com/us/ny/bet"
print(f"Fetching: {url}")

try:
    resp = session.get(url, timeout=15)
    print(f"Status: {resp.status_code}")
    print(f"Content length: {len(resp.text)} characters")

    if resp.status_code == 200:
        # Look for __INITIAL_STATE__ or similar
        if "__INITIAL_STATE__" in resp.text:
            print("✓ Found __INITIAL_STATE__")
            match = re.search(
                r"window\.__INITIAL_STATE__\s*=\s*({.*?});", resp.text, re.DOTALL
            )
            if match:
                try:
                    data = json.loads(match.group(1))
                    print(f"  Keys: {list(data.keys())}")

                    # Look for sports/events data
                    events_found = []

                    def extract_events(obj, path=""):
                        if isinstance(obj, dict):
                            for k, v in obj.items():
                                new_path = f"{path}.{k}" if path else k
                                if (
                                    "event" in k.lower()
                                    or "game" in k.lower()
                                    or "match" in k.lower()
                                ):
                                    print(f"  Found: {new_path}")
                                    if isinstance(v, (dict, list)):
                                        events_found.append((new_path, v))
                                extract_events(v, new_path)
                        elif isinstance(obj, list):
                            for i, item in enumerate(obj[:5]):  # Check first 5 items
                                extract_events(item, f"{path}[{i}]")

                    extract_events(data)

                    # Save the state
                    with open("caesars_ny_state.json", "w") as f:
                        json.dump(data, f, indent=2)
                    print("  Saved to caesars_ny_state.json")

                except Exception as e:
                    print(f"  Error parsing state: {e}")

        # Look for William Hill patterns (legacy)
        if "williamhill" in resp.text.lower():
            print("✓ Found William Hill references")

        # Look for API endpoints
        api_pattern = re.findall(
            r'["\'](https?://[^"\']*(?:api|feed|data|event|odds)[^"\']*)["\']',
            resp.text,
        )
        if api_pattern:
            print("\nFound potential API endpoints:")
            seen = set()
            for api in api_pattern:
                if api not in seen and "caesars" in api:
                    print(f"  {api}")
                    seen.add(api)

        # Look for WebSocket URLs
        ws_pattern = re.findall(r'wss?://[^"\'\s]+', resp.text)
        if ws_pattern:
            print("\nFound WebSocket URLs:")
            for ws in ws_pattern[:5]:
                print(f"  {ws}")

        # Try to find any JSON data embedded
        json_blocks = re.findall(
            r'<script[^>]*type="application/json"[^>]*>(.*?)</script>',
            resp.text,
            re.DOTALL,
        )
        if json_blocks:
            print(f"\nFound {len(json_blocks)} JSON script blocks")
            for i, block in enumerate(json_blocks[:3]):
                try:
                    json_data = json.loads(block)
                    print(f"  Block {i+1} keys: {list(json_data.keys())[:5]}")
                except:
                    pass

        # Check for specific sports pages
        soup = BeautifulSoup(resp.text, "html.parser")

        # Look for sports navigation links
        sports_links = soup.find_all(
            "a",
            href=re.compile(
                r"/(american-football|football|basketball|baseball|hockey)"
            ),
        )
        if sports_links:
            print("\nFound sports links:")
            for link in sports_links[:5]:
                print(f"  {link.get('href')}")

        # Look for data attributes that might contain odds
        data_elements = soup.find_all(attrs={"data-event-id": True})
        if data_elements:
            print(f"\nFound {len(data_elements)} elements with data-event-id")
            for elem in data_elements[:3]:
                print(f"  Event ID: {elem.get('data-event-id')}")

        # Look for odds values (American format)
        odds_pattern = re.findall(r"[+-]\d{3,4}(?![0-9])", resp.text)
        if odds_pattern:
            print(f"\nFound {len(odds_pattern)} potential odds values")
            print(f"  Sample: {odds_pattern[:10]}")

except Exception as e:
    print(f"Error: {e}")

#!/usr/bin/env python3
"""Test curl-cffi against BetMGM"""
from curl_cffi import requests

# Test different browser impersonations
impersonations = ["chrome120", "chrome124", "safari17_0", "edge120", "firefox120"]

urls = [
    "https://sports.betmgm.com/en/sports",
    "https://sports.betmgm.com/cds-api/bettingoffer/fixtures",
    "https://sports.betmgm.com/en/sports/american-football-11/betting/usa-9/nfl-12",
]

print("Testing curl-cffi with different browser impersonations...\n")

for imp in impersonations:
    print(f"Testing with {imp}:")
    session = requests.Session(impersonate=imp)

    for url in urls:
        try:
            resp = session.get(url, timeout=10)
            print(f"  {url[:50]}... -> Status: {resp.status_code}")

            if resp.status_code == 200:
                # Check for odds data
                if (
                    "fixture" in resp.text
                    or "odds" in resp.text
                    or "__INITIAL_STATE__" in resp.text
                ):
                    print("    ✓ SUCCESS! Contains data")

                    # Try to extract JSON
                    try:
                        data = resp.json()
                        print(f"    JSON data found: {list(data.keys())[:5]}")
                    except:
                        # Check for embedded state
                        if "__INITIAL_STATE__" in resp.text:
                            print("    Found __INITIAL_STATE__ embedded data")

                    # Save successful response
                    with open(f"success_{imp}.html", "w") as f:
                        f.write(resp.text[:10000])
                    print(f"    Saved to success_{imp}.html")
                    break
            elif resp.status_code == 403:
                print("    ✗ Cloudflare detected")
        except Exception as e:
            print(f"    Error: {str(e)[:50]}")
    print()

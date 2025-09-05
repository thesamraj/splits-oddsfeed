#!/usr/bin/env python3
"""Test cloudscraper against BetMGM"""
import cloudscraper

scraper = cloudscraper.create_scraper()

urls = [
    "https://sports.betmgm.com/en/sports",
    "https://sports.betmgm.com/en/sports/american-football-11",
]

print("Testing cloudscraper against BetMGM...\n")

for url in urls:
    try:
        print(f"Testing: {url}")
        resp = scraper.get(url, timeout=30)
        print(f"  Status: {resp.status_code}")

        if resp.status_code == 200:
            print("  ✓ SUCCESS! Got through Cloudflare")
            print(f"  Response length: {len(resp.text)} characters")

            # Check for data
            if "__INITIAL_STATE__" in resp.text:
                print("  ✓ Found __INITIAL_STATE__ data")
            if "fixture" in resp.text.lower():
                print("  ✓ Found fixture data")
            if "odds" in resp.text.lower():
                print("  ✓ Found odds data")

            # Save response
            with open("cloudscraper_success.html", "w") as f:
                f.write(resp.text[:50000])
            print("  Saved to cloudscraper_success.html")
            break
        elif resp.status_code == 403:
            print("  ✗ Still blocked by Cloudflare")
            # Check if we got a challenge page
            if "challenge" in resp.text.lower() or "cloudflare" in resp.text.lower():
                print("  Cloudflare challenge detected")
    except Exception as e:
        print(f"  Error: {e}")
    print()

#!/usr/bin/env python3
"""Test without SSL verification"""
import cloudscraper
import warnings

warnings.filterwarnings("ignore")

# Disable SSL verification
scraper = cloudscraper.create_scraper()
scraper.verify = False

url = "https://sports.betmgm.com/en/sports"

print(f"Testing {url} without SSL verification...")
try:
    resp = scraper.get(url, timeout=30)
    print(f"Status: {resp.status_code}")

    if resp.status_code == 200:
        print("✓ SUCCESS! Got through")
        print(f"Response length: {len(resp.text)}")

        if "__INITIAL_STATE__" in resp.text:
            print("✓ Found __INITIAL_STATE__")
        if "odds" in resp.text.lower():
            print("✓ Found odds references")
    elif resp.status_code == 403:
        print("✗ Still blocked (403)")
        if "cloudflare" in resp.text.lower():
            print("Cloudflare challenge page")
except Exception as e:
    print(f"Error: {e}")

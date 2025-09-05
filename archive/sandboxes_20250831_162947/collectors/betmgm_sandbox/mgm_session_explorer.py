#!/usr/bin/env python3
import requests
import json
import re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Create session with retry strategy
session = requests.Session()
retry = Retry(total=3, backoff_factor=0.3)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)

# More comprehensive headers
session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
    }
)

print("Attempting BetMGM with session management...")

# First, try to get the main page
try:
    print("\n1. Getting main page...")
    resp = session.get("https://sports.betmgm.com/en/sports", timeout=10)
    print(f"Main page status: {resp.status_code}")

    if resp.status_code == 200:
        # Look for embedded data
        if "window.__INITIAL_STATE__" in resp.text:
            print("Found __INITIAL_STATE__")
            # Extract it
            match = re.search(
                r"window\.__INITIAL_STATE__\s*=\s*({.*?});", resp.text, re.DOTALL
            )
            if match:
                try:
                    data = json.loads(match.group(1))
                    print(f"Initial state keys: {list(data.keys())[:10]}")
                    with open("mgm_initial_state.json", "w") as f:
                        json.dump(data, f, indent=2)
                    print("Saved to mgm_initial_state.json")
                except:
                    pass

        # Look for API configuration
        if "apiUrl" in resp.text or "API_URL" in resp.text:
            print("Found API configuration")
            api_urls = re.findall(r'"(https?://[^"]*api[^"]*)"', resp.text)
            if api_urls:
                print(f"Found API URLs: {api_urls[:5]}")

        # Look for odds data
        odds_patterns = re.findall(r"[-+]\d{3,4}", resp.text)
        if odds_patterns:
            print(f"Found {len(odds_patterns)} potential odds values")
            print(f"Sample: {odds_patterns[:10]}")

    elif resp.status_code == 403:
        print("Still getting 403 - likely Cloudflare protection")

except Exception as e:
    print(f"Error: {e}")

# Try alternate approaches
print("\n2. Trying alternate domains...")
alternate_urls = [
    "https://promo.betmgm.com/en/promo/sports",
    "https://casino.betmgm.com/en/games",
    "https://www.betmgm.com/",
]

for url in alternate_urls:
    try:
        resp = session.get(url, timeout=5)
        print(f"{url}: {resp.status_code}")
        if resp.status_code == 200:
            # Check for redirects to sports
            if "sports" in resp.url:
                print(f"  Redirected to: {resp.url}")
    except Exception as e:
        print(f"{url}: Error - {str(e)[:30]}")

# Try to find embedded/widget endpoints
print("\n3. Looking for widget/embedded data...")
widget_urls = [
    "https://widgets.betmgm.com/widgets/scoreboard",
    "https://sbapi.betmgm.com/api/v1/events",
    "https://api.entaingroup.com/sports/events",
]

for url in widget_urls:
    try:
        resp = requests.get(url, headers=session.headers, timeout=5)
        print(f"{url}: {resp.status_code}")
    except Exception as e:
        print(f"{url}: Error - {str(e)[:30]}")

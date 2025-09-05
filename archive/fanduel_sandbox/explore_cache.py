import requests
import json

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
headers = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

# First check what's in the working endpoint
url = "https://sportsbook.fanduel.com/cache/psmg/UK/67388.json"
r = requests.get(url, headers=headers)
data = r.json()
print(f"Sample structure from {url}:")
print(json.dumps(data, indent=2)[:1000])

# Now let's try US market IDs and different patterns
print("\n\nTrying US market patterns...")

# Common US market IDs and sports
patterns = [
    # Try US instead of UK
    "https://sportsbook.fanduel.com/cache/psmg/US/{id}.json",
    "https://sportsbook.fanduel.com/cache/psevent/US/{id}/false",
    "https://sportsbook.fanduel.com/cache/ps/US/{id}.json",
    # Try different IDs (NFL, NBA, MLB common ranges)
    "https://sportsbook.fanduel.com/cache/psmg/UK/{id}.json",
]

# Sport IDs to try
sport_ids = [
    7522,
    7524,
    7525,  # NFL
    67388,
    67387,
    67386,  # Various sports
    6,
    7,
    8,
    11,  # Common sport IDs
    88808,
    88809,  # NBA ranges
    84240,
    84241,  # MLB ranges
]

working_endpoints = []
for pattern in patterns:
    for sport_id in sport_ids:
        url = pattern.format(id=sport_id)
        try:
            r = requests.get(url, headers=headers, timeout=3)
            if r.status_code == 200 and len(r.content) > 100:
                # Check if it has odds data
                text = r.text.lower()
                if any(
                    term in text
                    for term in ["price", "odds", "american", "spread", "moneyline"]
                ):
                    print(f"✓ FOUND: {url} ({len(r.content)} bytes)")
                    working_endpoints.append(url)
                    # Save a sample
                    with open(f"sample_{sport_id}.json", "w") as f:
                        json.dump(r.json(), f, indent=2)
        except:
            pass

print(f"\n\nFound {len(working_endpoints)} endpoints with odds data")
for ep in working_endpoints:
    print(f"  - {ep}")

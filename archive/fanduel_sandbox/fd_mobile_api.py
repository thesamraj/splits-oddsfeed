import requests

# Mobile app endpoints often have different protection levels
# These are common patterns for FanDuel mobile APIs

endpoints = [
    # Android app endpoints
    {
        "url": "https://api.fanduel.com/v2/content/markets",
        "headers": {
            "X-Platform": "android",
            "X-App-Version": "1.0",
        },
    },
    {
        "url": "https://sbapi.fanduel.com/api/v1/events",
        "headers": {
            "X-Client-Type": "mobile",
        },
    },
    # Try with different auth states
    {
        "url": "https://api.fanduel.com/sportsbook/v1/featured",
        "headers": {
            "X-Guest-Token": "guest",
        },
    },
    # State-specific endpoints
    {"url": "https://nj.sportsbook.fanduel.com/api/events/featured", "headers": {}},
    {"url": "https://pa.sportsbook.fanduel.com/api/events/featured", "headers": {}},
    # RSS/Feed endpoints (often less protected)
    {"url": "https://www.fanduel.com/odds/feed", "headers": {}},
    {"url": "https://sportsbook.fanduel.com/feed/odds.json", "headers": {}},
]

base_headers = {
    "User-Agent": "FanDuel/1.0 (Android 12; SDK 31)",
    "Accept": "application/json",
    "Accept-Language": "en-US",
}

print("Testing FanDuel mobile/app endpoints...\n")

working = []
for endpoint in endpoints:
    headers = {**base_headers, **endpoint["headers"]}
    try:
        r = requests.get(endpoint["url"], headers=headers, timeout=5)
        size = len(r.content)
        print(f"{r.status_code} {size:8d}b {endpoint['url']}")

        if r.status_code == 200 and size > 100:
            working.append(
                {
                    "url": endpoint["url"],
                    "headers": endpoint["headers"],
                    "content": r.text[:1000],
                }
            )
    except Exception as e:
        print(f"ERR {endpoint['url']}: {str(e)[:50]}")

print(f"\n{len(working)} working endpoints found")

if working:
    print("\nAnalyzing working endpoints...")
    for w in working:
        print(f"\n{w['url']}:")
        print(f"Headers needed: {w['headers']}")
        print(f"Sample: {w['content'][:300]}")

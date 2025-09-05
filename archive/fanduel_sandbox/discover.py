import requests
import json

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# Known FanDuel endpoints to test
endpoints = [
    # Public/Guest endpoints
    "https://sportsbook.fanduel.com/api/odds-surge",
    "https://sportsbook.fanduel.com/api/featured-bets",
    "https://sportsbook.fanduel.com/api/popular-bets",
    "https://sportsbook.fanduel.com/api/content/featured-content",
    "https://sportsbook.fanduel.com/api/content/sports-with-events",
    "https://sbapi.ny.sportsbook.fanduel.com/api/content/sports-with-events",
    # GraphQL endpoints
    "https://sportsbook.fanduel.com/api/content-managed-page",
    "https://sportsbook-nash.fanduel.com/graphql",
    # Mobile/App endpoints
    "https://mobile.fanduel.com/api/getUpcomingGames",
    "https://api.fanduel.com/contests/NFL",
    # State-specific
    "https://sbapi.nj.sportsbook.fanduel.com/api/in-play",
    "https://sbapi.pa.sportsbook.fanduel.com/api/in-play",
    # Events endpoints
    "https://sportsbook.fanduel.com/cache/psevent/UK/1/false",
    "https://sportsbook.fanduel.com/cache/psmg/UK/67388.json",
]

headers = {
    "User-Agent": UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://sportsbook.fanduel.com/",
    "Origin": "https://sportsbook.fanduel.com",
}

results = []
for url in endpoints:
    try:
        r = requests.get(url, headers=headers, timeout=5)
        size = len(r.content)
        has_odds = any(
            word in r.text.lower()
            for word in [
                "odds",
                "price",
                "american",
                "decimal",
                "spread",
                "moneyline",
                "total",
            ]
        )
        print(f"{r.status_code} {size:8d}b {has_odds} {url}")
        if r.status_code == 200 and size > 500:
            results.append(
                {"url": url, "size": size, "has_odds": has_odds, "sample": r.text[:500]}
            )
    except Exception as e:
        print(f"ERR {url}: {e}")

# Save promising endpoints
with open("promising.json", "w") as f:
    json.dump(results, f, indent=2)

print(f"\nFound {len(results)} working endpoints")

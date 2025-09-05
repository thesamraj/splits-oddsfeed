#!/usr/bin/env python3
"""
BetMGM Mobile API Finder
Searches for and tests mobile app API endpoints that may be less protected
"""
import json
import time
import requests
import hashlib


class BetMGMMobileAPIFinder:
    def __init__(self):
        self.session = requests.Session()

        # Mobile app user agents
        self.user_agents = [
            # iOS BetMGM app
            "BetMGM/5.0.0 (iPhone; iOS 17.0; Scale/3.0)",
            "BetMGM Sportsbook & Casino/4.72.0 CFNetwork/1474 Darwin/23.0.0",
            # Android BetMGM app
            "BetMGM/5.0.0 (Android 14; Mobile; rv:120.0)",
            "Dalvik/2.1.0 (Linux; U; Android 14; Pixel 7 Pro Build/UP1A.231005.007) BetMGM/5.0.0",
            # Generic mobile
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
        ]

        # Potential mobile/app API endpoints
        self.endpoints = [
            # Direct mobile app APIs
            ("https://api.betmgm.com/v3/sports/events", "GET", None),
            ("https://api.betmgm.com/sportsbook/v2/events", "GET", None),
            ("https://mobile-api.betmgm.com/v1/events", "GET", None),
            ("https://app.betmgm.com/api/events", "GET", None),
            # State-specific mobile endpoints
            ("https://nj-api.betmgm.com/sportsbook/events", "GET", None),
            ("https://mi-api.betmgm.com/sportsbook/events", "GET", None),
            ("https://pa-api.betmgm.com/sportsbook/events", "GET", None),
            # OAuth/Auth endpoints that might reveal API structure
            (
                "https://api.betmgm.com/oauth/token",
                "POST",
                {"grant_type": "client_credentials"},
            ),
            ("https://auth.betmgm.com/api/v1/anonymous", "POST", {}),
            # GraphQL endpoints
            (
                "https://api.betmgm.com/graphql",
                "POST",
                {"query": "query { events { id name odds { american } } }"},
            ),
            # Entain Group (parent company) endpoints
            ("https://api.entaingroup.com/sports/v1/events", "GET", None),
            ("https://services.entainpartners.com/api/events", "GET", None),
            # CDN/Static data endpoints
            ("https://cdn.betmgm.com/sportsbook/events.json", "GET", None),
            ("https://static.betmgm.com/api/odds-feed.json", "GET", None),
            # WebView endpoints (used in mobile apps)
            ("https://mweb.betmgm.com/api/events", "GET", None),
            ("https://m.betmgm.com/sportsbook/api/events", "GET", None),
            # Push notification endpoints (might have event data)
            ("https://push.betmgm.com/api/v1/events", "GET", None),
            ("https://notifications.betmgm.com/sports/events", "GET", None),
            # Analytics endpoints (sometimes contain odds data)
            ("https://analytics.betmgm.com/api/sportsbook/events", "GET", None),
            ("https://tracking.betmgm.com/v1/sports/data", "GET", None),
            # Partner/Affiliate APIs
            ("https://partners.betmgm.com/api/v1/odds", "GET", None),
            ("https://affiliates.betmgm.com/feeds/events", "GET", None),
            # Feed endpoints
            ("https://feeds.betmgm.com/sportsbook/v1/events", "GET", None),
            ("https://data.betmgm.com/api/sports/events", "GET", None),
            # Websocket endpoints (HTTP check first)
            ("https://ws.betmgm.com/socket.io/", "GET", None),
            ("https://stream.betmgm.com/events", "GET", None),
        ]

        # API keys found in mobile app decompilation (examples)
        self.api_keys = [
            "x-api-key: d7f3a8b9c2e1f4a5b6c7d8e9f0a1b2c3",
            "x-client-id: mobile-app-v5",
            "x-app-version: 5.0.0",
            "x-platform: ios",
            "x-device-id: " + hashlib.md5(str(time.time()).encode()).hexdigest(),
        ]

    def test_endpoint(self, url, method="GET", data=None, user_agent=None):
        """Test a single endpoint"""
        headers = {
            "User-Agent": user_agent or self.user_agents[0],
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        }

        # Add potential API keys
        for key in self.api_keys:
            if ":" in key:
                k, v = key.split(":", 1)
                headers[k.strip()] = v.strip()

        try:
            if method == "POST":
                resp = self.session.post(url, json=data, headers=headers, timeout=5)
            else:
                resp = self.session.get(url, headers=headers, timeout=5)

            return resp.status_code, resp.text[:500] if resp.text else ""
        except Exception as e:
            return None, str(e)[:100]

    def analyze_response(self, text):
        """Analyze response to determine if it contains odds data"""
        indicators = [
            "odds",
            "price",
            "american",
            "decimal",
            "fractional",
            "spread",
            "total",
            "moneyline",
            "event",
            "fixture",
            "home",
            "away",
            "team",
            "participant",
            "market",
            "bet",
            "wager",
            "line",
            "over",
            "under",
        ]

        if not text:
            return 0

        text_lower = text.lower()
        score = sum(1 for indicator in indicators if indicator in text_lower)

        # Bonus points for JSON structure
        try:
            json.loads(text)
            score += 5
        except:
            pass

        return score

    def search(self):
        """Search for working mobile API endpoints"""
        print("=" * 80)
        print("BetMGM Mobile API Endpoint Discovery")
        print("=" * 80)

        results = []

        for endpoint in self.endpoints:
            url = endpoint[0]
            method = endpoint[1]
            data = endpoint[2] if len(endpoint) > 2 else None

            print(f"\nTesting: {url}")
            print(f"Method: {method}")

            # Test with different user agents
            for ua in self.user_agents[:2]:  # Test with first 2 user agents
                status, response = self.test_endpoint(url, method, data, ua)

                if status:
                    score = self.analyze_response(response)
                    print(f"  UA: {ua[:50]}...")
                    print(f"  Status: {status}, Score: {score}")

                    if status == 200:
                        print("  ✓ SUCCESS - Status 200")
                        if score > 3:
                            print("  ✓ HIGH SCORE - Likely contains odds data!")
                            results.append(
                                {
                                    "url": url,
                                    "method": method,
                                    "user_agent": ua,
                                    "score": score,
                                    "sample": response[:200],
                                }
                            )
                    elif status in [301, 302, 307, 308]:
                        print("  → Redirect")
                    elif status == 401:
                        print("  🔐 Requires authentication")
                    elif status == 403:
                        print("  ⛔ Forbidden (Cloudflare or auth required)")
                else:
                    print(f"  ✗ Failed: {response}")

                time.sleep(0.5)  # Small delay between requests

        print("\n" + "=" * 80)
        print("RESULTS SUMMARY")
        print("=" * 80)

        if results:
            print(f"\nFound {len(results)} promising endpoints:\n")
            for r in results:
                print(f"URL: {r['url']}")
                print(f"Score: {r['score']}")
                print(f"Sample: {r['sample']}")
                print("-" * 40)
        else:
            print("\nNo working endpoints found with odds data.")
            print("\nRecommendations:")
            print("1. Use Playwright with stealth mode (mgm_playwright_collector.py)")
            print("2. Try curl-cffi with TLS fingerprinting (mgm_cffi_collector.py)")
            print("3. Reverse engineer the mobile app for actual API endpoints")
            print("4. Use a residential proxy service to avoid Cloudflare detection")
            print(
                "5. Consider using a headless browser service like Browserless or Puppeteer"
            )

        return results


if __name__ == "__main__":
    finder = BetMGMMobileAPIFinder()
    results = finder.search()

    if results:
        print("\n" + "=" * 80)
        print("RECOMMENDED APPROACH")
        print("=" * 80)
        best = max(results, key=lambda x: x["score"])
        print("\nBest endpoint found:")
        print(f"URL: {best['url']}")
        print(f"Method: {best['method']}")
        print(f"User-Agent: {best['user_agent']}")
        print("\nUse this endpoint in the collector for real odds data.")

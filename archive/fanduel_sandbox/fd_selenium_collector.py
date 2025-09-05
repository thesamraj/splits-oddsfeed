#!/usr/bin/env python3
import json
import time
import redis
import os
import re
import requests

REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
CHANNEL = os.getenv("CHANNEL", "odds.raw.fanduel")
URL = os.getenv("URL", "https://sportsbook.fanduel.com/")


class FanDuelSeleniumCollector:
    def __init__(self):
        self.redis = redis.from_url(REDIS_URL)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": "https://sportsbook.fanduel.com/",
            }
        )

    def collect(self):
        """Main collection method using direct API discovery"""
        print(f"Collection cycle: {time.strftime('%H:%M:%S')}")

        # Known FanDuel API endpoints patterns
        api_endpoints = [
            # Event feeds
            f"{URL}cache/psmg/UK/67388.json",
            f"{URL}cache/psmg/UK/67387.json",
            f"{URL}cache/psmg/UK/67389.json",
            # Sports specific
            f"{URL}sportsbook/v1/api/content-managed-page?page=AMERICAN_FOOTBALL&_ak=FhMFpcPWXMeyZxOx",
            f"{URL}sportsbook/v1/api/content-managed-page?page=BASEBALL&_ak=FhMFpcPWXMeyZxOx",
            f"{URL}sportsbook/v1/api/content-managed-page?page=BASKETBALL&_ak=FhMFpcPWXMeyZxOx",
            # Direct event APIs
            "https://sbapi.il.sportsbook.fanduel.com/api/event-page",
            "https://sbapi.il.sportsbook.fanduel.com/api/in-play",
            "https://sbapi.il.sportsbook.fanduel.com/api/content-managed-page",
            # Alternative domains
            "https://sportsbook-nash.fanduel.com/cache/psmg/UK/67388.json",
            "https://sportsbook-pa.fanduel.com/cache/psmg/UK/67388.json",
        ]

        collected_data = []
        events_map = {}  # Map event_id to event data

        for endpoint in api_endpoints:
            try:
                print(f"Trying: {endpoint[-60:]}")
                resp = self.session.get(endpoint, timeout=5)

                if resp.status_code == 200:
                    content_type = resp.headers.get("content-type", "")

                    if "json" in content_type:
                        try:
                            data = resp.json()
                            # Extract odds from JSON structure
                            odds = self.extract_odds_from_json(data)
                            if odds:
                                collected_data.extend(odds)
                                print(f"  Found {len(odds)} odds")
                        except:
                            pass
                    else:
                        # Try to extract from HTML/text
                        text = resp.text
                        odds = self.extract_odds_from_text(text)
                        if odds:
                            collected_data.extend(odds)
                            print(f"  Found {len(odds)} odds in HTML")

            except Exception:
                continue

        # Try main page for embedded data
        try:
            main_resp = self.session.get(URL, timeout=10)
            if main_resp.status_code == 200:
                # Look for JSON-LD data
                json_ld_pattern = (
                    r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
                )
                matches = re.findall(json_ld_pattern, main_resp.text, re.DOTALL)
                for match in matches:
                    try:
                        data = json.loads(match)
                        odds = self.extract_odds_from_json(data)
                        if odds:
                            collected_data.extend(odds)
                    except:
                        pass

                # Look for embedded odds
                text_odds = self.extract_odds_from_text(main_resp.text)
                if text_odds:
                    collected_data.extend(text_odds[:50])  # Limit to 50

        except Exception as e:
            print(f"Main page error: {e}")

        # Generate events with proper IDs
        if not events_map and collected_data:
            # Create synthetic events from odds
            for i, odd in enumerate(collected_data[:30]):  # Create up to 30 events
                event_id = odd.get(
                    "event_id", f"fd_{int(time.time() * 1000) % 1000000}_{i}"
                )
                events_map[event_id] = {"id": event_id, "odds": [odd]}

        # Publish results
        if collected_data or events_map:
            payload = {
                "source": "fanduel",
                "timestamp": time.time(),
                "raw_odds": collected_data[:200],  # Limit to 200 odds
                "events": list(events_map.values())[:50],  # Include event data
                "endpoints_tried": len(api_endpoints),
                "collection_method": "api_discovery",
            }

            self.redis.publish(CHANNEL, json.dumps(payload))
            print(f"Published: {len(collected_data)} odds, {len(events_map)} events")
            return True

        return False

    def extract_event_id(self, data):
        """Extract a stable event ID from various data sources"""
        import hashlib

        # Try direct keys first
        if isinstance(data, dict):
            for key in [
                "eventId",
                "event_id",
                "id",
                "altId",
                "marketGroupId",
                "fixtureId",
            ]:
                if key in data:
                    return f"fd_{data[key]}"

            # Try nested structures
            if "event" in data and isinstance(data["event"], dict):
                if "id" in data["event"]:
                    return f"fd_{data['event']['id']}"

            # Try to build from teams/time
            home = data.get("home", data.get("homeTeam", ""))
            away = data.get("away", data.get("awayTeam", ""))
            start = data.get("startTime", data.get("start", ""))

            if home and away:
                # Create deterministic hash
                event_str = f"{home}_{away}_{start}".lower()
                event_hash = hashlib.md5(event_str.encode()).hexdigest()[:8]
                return f"fd_{event_hash}"

        # Fallback to timestamp-based ID
        return f"fd_{int(time.time() * 1000) % 1000000}"

    def extract_odds_from_json(self, data):
        """Extract odds from JSON data structures"""
        odds = []
        event_id = self.extract_event_id(data)

        def recursive_extract(obj, path=""):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    # Look for odds-related keys
                    if any(
                        term in key.lower()
                        for term in [
                            "odds",
                            "price",
                            "line",
                            "spread",
                            "total",
                            "moneyline",
                        ]
                    ):
                        if isinstance(value, (int, float, str)):
                            try:
                                # Check if it looks like odds
                                if isinstance(value, str) and re.match(
                                    r"[+-]?\d+", value
                                ):
                                    odds.append(
                                        {
                                            "type": "american",
                                            "value": value,
                                            "path": f"{path}/{key}",
                                            "event_id": event_id,
                                        }
                                    )
                                elif (
                                    isinstance(value, (int, float))
                                    and -10000 < value < 10000
                                ):
                                    odds.append(
                                        {
                                            "type": "decimal",
                                            "value": str(value),
                                            "path": f"{path}/{key}",
                                            "event_id": event_id,
                                        }
                                    )
                            except:
                                pass
                    recursive_extract(value, f"{path}/{key}")
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    recursive_extract(item, f"{path}[{i}]")

        recursive_extract(data)
        return odds

    def extract_odds_from_text(self, text):
        """Extract odds from text/HTML"""
        odds = []

        # American odds pattern
        american_pattern = r"[+-]\d{3,4}"
        matches = re.findall(american_pattern, text)
        for match in set(matches):  # Unique only
            odds.append(
                {"type": "american", "value": match, "source": "text_extraction"}
            )

        # Decimal odds pattern (1.50 - 99.99)
        decimal_pattern = r"\b[1-9]\d?\.\d{2}\b"
        matches = re.findall(decimal_pattern, text)
        for match in set(matches):
            val = float(match)
            if 1.01 <= val <= 99.99:
                odds.append(
                    {"type": "decimal", "value": match, "source": "text_extraction"}
                )

        return odds[:100]  # Limit to 100 odds

    def run(self):
        """Run continuous collection"""
        consecutive_failures = 0

        while True:
            try:
                print(f"\n{'='*50}")
                success = self.collect()

                if success:
                    consecutive_failures = 0
                    time.sleep(10)  # Fast refresh
                else:
                    consecutive_failures += 1
                    wait_time = min(60, 10 * consecutive_failures)
                    print(f"No data collected, waiting {wait_time}s")
                    time.sleep(wait_time)

            except Exception as e:
                print(f"Error: {e}")
                time.sleep(30)


if __name__ == "__main__":
    print("Starting FanDuel Selenium Collector...")
    collector = FanDuelSeleniumCollector()
    collector.run()

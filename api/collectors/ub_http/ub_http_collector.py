#!/usr/bin/env python3
"""
Unibet HTTP Collector with Auto-Fallback Signal
Tries HTTP first, signals for CDP fallback on repeated failures
"""
import json
import time
import os
from datetime import datetime
import requests
import redis
from typing import Dict, Any, Optional

# Configuration
UB_BASE_URL = os.getenv(
    "UB_BASE_URL", "https://eu.offering-api.kambicdn.com/offering/v2018/ub2uspa"
)
UB_MARKETS = os.getenv("UB_MARKETS", "US-PA").split(",")
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
PUBLISH_CHANNEL = os.getenv("PUBLISH_CHANNEL", "odds.raw.unibet")
FALLBACK_SIGNAL_KEY = "ub:collector:fallback_required"
HEALTHZ_PORT = int(os.getenv("HEALTHZ_PORT", "9134"))
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL_SEC", "60"))

# Failure tracking
MAX_CONSECUTIVE_FAILURES = 6
ERROR_RATE_THRESHOLD = 0.5
WINDOW_SIZE = 10


class UBHttpCollector:
    def __init__(self):
        self.redis_client = redis.from_url(REDIS_URL)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://pa.unibet.com/",
                "Origin": "https://pa.unibet.com",
            }
        )

        self.consecutive_failures = 0
        self.recent_requests = []  # Sliding window for error rate
        self.last_success = datetime.utcnow()
        self.fallback_triggered = False

    def fetch_events(self) -> Optional[Dict[str, Any]]:
        """Fetch live events from Unibet API"""
        endpoints = [
            f"{UB_BASE_URL}/event/live/open.json",
            f"{UB_BASE_URL}/listView/american_football/nfl/all/matches.json?lang=en_US&market=US",
            f"{UB_BASE_URL}/listView/basketball/nba/all/matches.json?lang=en_US&market=US",
        ]

        all_events = []
        request_success = False

        for endpoint in endpoints:
            try:
                resp = self.session.get(endpoint, timeout=10)
                self.recent_requests.append(resp.status_code < 400)

                if len(self.recent_requests) > WINDOW_SIZE:
                    self.recent_requests.pop(0)

                if resp.status_code == 200:
                    data = resp.json()
                    if "events" in data:
                        all_events.extend(data["events"])
                    elif "liveEvents" in data:
                        all_events.extend(data["liveEvents"])
                    request_success = True
                    self.consecutive_failures = 0
                    self.last_success = datetime.utcnow()

                elif resp.status_code in [403, 429]:
                    print(f"[UB_HTTP] Access denied: {resp.status_code} on {endpoint}")
                    self.consecutive_failures += 1

            except Exception as e:
                print(f"[UB_HTTP] Error fetching {endpoint}: {e}")
                self.recent_requests.append(False)
                self.consecutive_failures += 1

        if all_events:
            return {
                "source": "unibet_http",
                "timestamp": datetime.utcnow().isoformat(),
                "events": all_events,
                "market": "US-PA",
                "status": "success",
            }
        return None

    def check_fallback_needed(self) -> bool:
        """Check if we should signal for CDP fallback"""
        # Check consecutive failures
        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
            return True

        # Check error rate
        if len(self.recent_requests) >= WINDOW_SIZE:
            error_rate = 1 - (sum(self.recent_requests) / len(self.recent_requests))
            if error_rate > ERROR_RATE_THRESHOLD:
                return True

        # Check if we haven't succeeded in 15 minutes
        if (datetime.utcnow() - self.last_success).seconds > 900:
            return True

        return False

    def signal_fallback(self):
        """Signal that CDP fallback should take over"""
        if not self.fallback_triggered:
            print("[UB_HTTP] Triggering CDP fallback due to repeated failures")
            self.redis_client.setex(
                FALLBACK_SIGNAL_KEY,
                3600,  # TTL 1 hour
                json.dumps(
                    {
                        "triggered_at": datetime.utcnow().isoformat(),
                        "consecutive_failures": self.consecutive_failures,
                        "error_rate": 1
                        - (
                            sum(self.recent_requests)
                            / max(1, len(self.recent_requests))
                        ),
                        "last_success": self.last_success.isoformat(),
                    }
                ),
            )
            self.fallback_triggered = True
            # Reduce polling frequency when in fallback
            global FETCH_INTERVAL
            FETCH_INTERVAL = 300  # 5 minutes

    def run(self):
        """Main collection loop"""
        print("[UB_HTTP] Starting Unibet HTTP collector")
        print(f"[UB_HTTP] Base URL: {UB_BASE_URL}")
        print(f"[UB_HTTP] Publishing to: {PUBLISH_CHANNEL}")

        # Health check endpoint
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import threading

        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/healthz":
                    self.send_response(200)
                    self.end_headers()
                    status = {
                        "status": (
                            "fallback" if collector.fallback_triggered else "active"
                        ),
                        "consecutive_failures": collector.consecutive_failures,
                        "last_success": collector.last_success.isoformat(),
                    }
                    self.wfile.write(json.dumps(status).encode())

            def log_message(self, format, *args):
                pass

        collector = self
        health_server = HTTPServer(("0.0.0.0", HEALTHZ_PORT), HealthHandler)
        health_thread = threading.Thread(target=health_server.serve_forever)
        health_thread.daemon = True
        health_thread.start()

        while True:
            try:
                # Check if we should fallback
                if self.check_fallback_needed():
                    self.signal_fallback()

                # Try to fetch events
                if not self.fallback_triggered:
                    data = self.fetch_events()
                    if data:
                        # Publish to Redis
                        self.redis_client.publish(PUBLISH_CHANNEL, json.dumps(data))
                        print(f"[UB_HTTP] Published {len(data['events'])} events")

                time.sleep(FETCH_INTERVAL)

            except KeyboardInterrupt:
                print("[UB_HTTP] Shutting down")
                break
            except Exception as e:
                print(f"[UB_HTTP] Unexpected error: {e}")
                time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    collector = UBHttpCollector()
    collector.run()

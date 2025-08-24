#!/usr/bin/env python3
"""
Polite SugarHouse collector with proper rate limiting and backoff
"""

import asyncio
import json
import os
import random
import time
import traceback
from typing import Optional
import requests
import redis
from prometheus_client import start_http_server, Counter, Gauge
from .healthz import start_health_server, health_state

# Environment configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
REDIS_CHANNEL = os.getenv("REDIS_CHANNEL", "odds.raw.kambi")
SUGARHOUSE_BASE_URL = "https://e0-api.kambi.com/offering/v2018/sg2uspa"
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
MIN_INTERVAL_SEC = int(os.getenv("MIN_INTERVAL_SEC", "30"))
MAX_BACKOFF_SEC = int(os.getenv("MAX_BACKOFF_SEC", "300"))
HEALTHZ_PORT = int(os.getenv("HEALTHZ_PORT", "9122"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9118"))

# Metrics
m_requests_total = Counter("sugarhouse_requests_total", "Total requests", ["status"])
m_backoff_events = Counter("sugarhouse_backoff_total", "Backoff events triggered")
m_last_success = Gauge("sugarhouse_last_success_timestamp", "Last successful request")
m_publish_total = Counter("sugarhouse_publish_total", "Publish attempts", ["status"])


def log(msg: str):
    """Consistent logging"""
    print(f"[sugarhouse] {msg}", flush=True)


def calculate_backoff(attempt: int) -> int:
    """Calculate exponential backoff with jitter"""
    base_backoff = min(MIN_INTERVAL_SEC * (2**attempt), MAX_BACKOFF_SEC)
    jitter = random.uniform(0.8, 1.2)
    return int(base_backoff * jitter)


async def fetch_odds_data(session: requests.Session) -> Optional[dict]:
    """Fetch odds data from SugarHouse API"""
    endpoints = [
        f"{SUGARHOUSE_BASE_URL}/listView/american_football/nfl/all/matches.json?lang=en_US&market=US&client_id=2&channel_id=1&ncid=1000&useCombined=true",
        f"{SUGARHOUSE_BASE_URL}/listView/basketball/nba/all/matches.json?lang=en_US&market=US&client_id=2&channel_id=1&ncid=1000&useCombined=true",
        f"{SUGARHOUSE_BASE_URL}/listView/baseball/mlb/all/matches.json?lang=en_US&market=US&client_id=2&channel_id=1&ncid=1000&useCombined=true",
    ]

    for endpoint in endpoints:
        try:
            log(f"requesting {endpoint}")
            response = session.get(endpoint, timeout=REQUEST_TIMEOUT)

            if response.status_code == 200:
                health_state.update_success()
                m_requests_total.labels("success").inc()
                m_last_success.set(time.time())

                data = response.json()
                if data and len(data) > 0:
                    log(f"received {len(str(data))} bytes from SugarHouse")
                    return data

            elif response.status_code == 429:
                # Rate limited - implement backoff
                retry_after = response.headers.get("Retry-After", "60")
                backoff_sec = int(retry_after) if retry_after.isdigit() else 60

                log(f"rate limited (429), backing off for {backoff_sec}s")
                health_state.update_rate_limited(backoff_sec)
                m_requests_total.labels("rate_limited").inc()
                m_backoff_events.inc()

                # Return None to trigger backoff
                return None

            else:
                log(f"unexpected status {response.status_code}: {response.text[:200]}")
                m_requests_total.labels("error").inc()
                health_state.update_idle()

        except requests.exceptions.Timeout:
            log(f"timeout requesting {endpoint}")
            m_requests_total.labels("timeout").inc()
        except Exception as e:
            log(f"error requesting {endpoint}: {e}")
            m_requests_total.labels("error").inc()

    health_state.update_idle()
    return None


def create_envelope(data: dict, url: str) -> dict:
    """Create standardized envelope for publishing"""
    now_ms = int(time.time() * 1000)

    return {
        "capture_id": f"sugarhouse_{now_ms}",
        "transport": "http",
        "url": url,
        "page_url": "https://pa.sugarhouse.com",
        "page_host": "pa.sugarhouse.com",
        "ws_url": "",
        "offering_url": url,
        "brand_hint": "sugarhouse",
        "source_ts_ms": now_ms,
        "received_ts_ms": now_ms,
        "event_id": "unknown",  # Will be extracted by normalizer
        "content_type": "application/json",
        "payload": json.dumps(data, separators=(",", ":"), ensure_ascii=False),
    }


async def publish_to_redis(redis_client, envelope: dict):
    """Publish envelope to Redis"""
    try:
        message = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False)
        redis_client.publish(REDIS_CHANNEL, message)
        m_publish_total.labels("success").inc()
        log(f"published envelope to {REDIS_CHANNEL}")
    except Exception as e:
        log(f"redis publish failed: {e}")
        m_publish_total.labels("error").inc()


async def run_collector():
    """Main collector loop with polite rate limiting"""
    log("starting polite SugarHouse collector")

    # Initialize Redis connection
    redis_client = redis.from_url(REDIS_URL)

    # Create HTTP session with appropriate headers
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://pa.sugarhouse.com/",
            "Origin": "https://pa.sugarhouse.com",
        }
    )

    backoff_attempt = 0

    while True:
        try:
            # Check if we're in backoff period
            if health_state.status == "rate_limited" and health_state.next_request_ts:
                wait_time = health_state.next_request_ts - time.time()
                if wait_time > 0:
                    log(f"in backoff period, waiting {wait_time:.1f}s")
                    await asyncio.sleep(wait_time)
                    continue

            # Fetch odds data
            data = await fetch_odds_data(session)

            if data:
                # Success - reset backoff and publish
                backoff_attempt = 0

                # Create and publish envelope
                envelope = create_envelope(data, SUGARHOUSE_BASE_URL)
                await publish_to_redis(redis_client, envelope)

                # Wait minimum interval before next request
                log(f"waiting {MIN_INTERVAL_SEC}s before next request")
                await asyncio.sleep(MIN_INTERVAL_SEC)

            else:
                # Failed or rate limited - increase backoff
                backoff_attempt += 1
                backoff_sec = calculate_backoff(backoff_attempt)

                log(f"backing off for {backoff_sec}s (attempt {backoff_attempt})")
                health_state.update_rate_limited(backoff_sec)
                await asyncio.sleep(backoff_sec)

        except KeyboardInterrupt:
            log("shutting down")
            break
        except Exception as e:
            log(f"unexpected error in collector loop: {e}")
            traceback.print_exc()
            await asyncio.sleep(30)  # Wait before retry


def main():
    """Main entry point"""
    try:
        # Start metrics server
        start_http_server(METRICS_PORT)
        log(f"metrics server started on port {METRICS_PORT}")

        # Start health server
        start_health_server(HEALTHZ_PORT)
        log(f"health server started on port {HEALTHZ_PORT}")

        # Run collector
        asyncio.run(run_collector())

    except KeyboardInterrupt:
        log("interrupted")
    except Exception as e:
        log(f"fatal error: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()

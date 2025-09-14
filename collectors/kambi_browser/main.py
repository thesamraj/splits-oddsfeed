#!/usr/bin/env python3
"""
Kambi Browser Collector - Uses Playwright to intercept XHR requests from Kambi CDN
Browser intercept mode only - no direct HTTP fetching
"""

import os
import sys
import time
import json
import random
import asyncio
import logging
import threading
import socket
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse
from collections import deque

import redis
import requests
from flask import Flask, jsonify, Response
from prometheus_client import Counter, Gauge, Histogram, REGISTRY, generate_latest, CONTENT_TYPE_LATEST
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Route

# Force IPv4
original_getaddrinfo = socket.getaddrinfo
def force_ipv4_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
socket.getaddrinfo = force_ipv4_getaddrinfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
KAMBI_BRANDS = os.getenv("KAMBI_BRANDS", "betrivers,barstool,caesars,sugarhouse,unibet").split(",")
PORT = int(os.getenv("PORT", "8000"))
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "30"))
BROWSER_INTERCEPT_ONLY = True  # Always use browser intercept
REQUEST_TIMEOUT = 30000  # 30 seconds in milliseconds

# Brand configurations
BRAND_CONFIG = {
    "betrivers": {
        "base_url": "https://eu-offering.kambicdn.org/offering/v2018/rsi2us",
        "site_url": "https://nj.betrivers.com/?page=sportsbook&group=1000093190&type=prematch"
    },
    "barstool": {
        "base_url": "https://eu-offering.kambicdn.org/offering/v2018/pivuspa",
        "site_url": "https://sportsbook.barstoolsportsbook.com/sports/american_football"
    },
    "caesars": {
        "base_url": "https://eu-offering.kambicdn.org/offering/v2018/pivusnjcz",
        "site_url": "https://sportsbook.caesars.com/us/nj/bet/sports"
    },
    "sugarhouse": {
        "base_url": "https://eu-offering.kambicdn.org/offering/v2018/pivusnjsh",
        "site_url": "https://sportsbook.sugarhouse.com/?page=sportsbook"
    },
    "unibet": {
        "base_url": "https://eu-offering.kambicdn.org/offering/v2018/ubuspa",
        "site_url": "https://pa.unibet.com/sportsbook-feeds/views/american_football"
    },
}

# User agents pool
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
]

# Prometheus metrics
collector_up = Gauge("collector_up", "Collector health status", ["book"])
ticks_total = Counter("ticks_total", "Total ticks processed", ["book"])
odds_15m = Gauge("odds_15m", "Odds in last 15 minutes", ["book"])
http_status_count = Counter("http_status_count", "HTTP status codes", ["book", "status"])
request_duration_seconds = Histogram("request_duration_seconds", "Request duration", ["book", "endpoint"])
errors_total = Counter("errors_total", "Total errors", ["book", "type"])
intercepted_urls_total = Counter("intercepted_urls_total", "Total offering URLs intercepted", ["book"])

# Flask app
app = Flask(__name__)

# Global state
last_success_by_brand = {}
brands_seen = set()
collector_status = {"running": False, "last_cycle": None}
odds_15m_window = {}  # brand -> [(timestamp, count)]
intercepted_urls = deque(maxlen=100)  # Last 100 intercepted URLs


class KambiBrowserCollector:
    def __init__(self):
        self.redis_client = redis.from_url(REDIS_URL)
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.intercepted_data = {}  # brand -> list of intercepted responses
        
        logger.info("Browser intercept mode enabled - will capture XHR responses")
        
        # Initialize metrics
        for brand in KAMBI_BRANDS:
            collector_up.labels(book=brand).set(0)
            odds_15m_window[brand] = []
            self.intercepted_data[brand] = []
    
    async def setup_browser(self):
        """Initialize Playwright browser"""
        playwright = await async_playwright().start()
        
        launch_args = {
            "headless": True,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
            ]
        }
        
        logger.info("Starting browser without proxy for direct connection")
        
        self.browser = await playwright.chromium.launch(**launch_args)
        
        # Create context with random user agent
        ua = random.choice(USER_AGENTS)
        self.context = await self.browser.new_context(
            user_agent=ua,
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York"
        )
        
        logger.info(f"Browser initialized with UA: {ua[:50]}...")
    
    async def intercept_offering_requests(self, route: Route, request):
        """Intercept and capture Kambi offering API responses"""
        url = request.url
        
        # Check if this is a Kambi offering URL
        if "kambicdn.org/offering" in url:
            # Log the intercepted URL
            intercepted_urls.append({
                "timestamp": datetime.utcnow().isoformat(),
                "url": url
            })
            logger.info(f"Intercepting offering URL: {url}")
            
            # Continue the request and capture response
            try:
                response = await route.fetch()
                if response.status == 200:
                    body = await response.body()
                    
                    # Try to parse as JSON
                    try:
                        data = json.loads(body)
                        
                        # Determine which brand this is for
                        for brand, config in BRAND_CONFIG.items():
                            if config["base_url"] in url:
                                self.intercepted_data[brand].append(data)
                                intercepted_urls_total.labels(book=brand).inc()
                                logger.info(f"Captured data for {brand}: {len(data.get('events', []))} events")
                                break
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse JSON from {url}")
                
                await route.fulfill(response=response)
            except Exception as e:
                logger.error(f"Error intercepting {url}: {e}")
                await route.continue_()
        else:
            await route.continue_()
    
    async def fetch_brand_data(self, brand: str) -> List[Dict]:
        """Navigate to brand site and intercept XHR requests"""
        if brand not in BRAND_CONFIG:
            logger.error(f"Unknown brand: {brand}")
            return []
        
        config = BRAND_CONFIG[brand]
        self.intercepted_data[brand] = []  # Clear previous data
        
        try:
            # Create a new page for this brand
            page = await self.context.new_page()
            
            # Set up request interception
            await page.route("**/*", self.intercept_offering_requests)
            
            # Navigate to the brand's site
            logger.info(f"{brand}: Navigating to {config['site_url']}")
            start_time = time.time()
            
            try:
                # Navigate with timeout
                await page.goto(
                    config["site_url"],
                    timeout=REQUEST_TIMEOUT,
                    wait_until="domcontentloaded"
                )
                
                # Wait a bit for XHR requests to complete
                await page.wait_for_timeout(5000)
                
                # Try to trigger more data loading by scrolling
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(2000)
                
                duration = time.time() - start_time
                request_duration_seconds.labels(book=brand, endpoint="site_navigation").observe(duration)
                
                # Process intercepted data
                all_events = []
                for data in self.intercepted_data[brand]:
                    if isinstance(data, dict) and "events" in data:
                        all_events.extend(data["events"])
                
                if all_events:
                    logger.info(f"{brand}: Collected {len(all_events)} events via interception")
                    collector_up.labels(book=brand).set(1)
                    http_status_count.labels(book=brand, status="200").inc()
                else:
                    logger.warning(f"{brand}: No events intercepted")
                    collector_up.labels(book=brand).set(0)
                
                await page.close()
                return all_events
                
            except Exception as e:
                if "timeout" in str(e).lower():
                    logger.warning(f"{brand}: Timeout on navigation - may be blocked")
                    errors_total.labels(book=brand, type="timeout").inc()
                else:
                    logger.error(f"{brand}: Navigation error: {e}")
                    errors_total.labels(book=brand, type="error").inc()
                
                collector_up.labels(book=brand).set(0)
                await page.close()
                return []
                
        except Exception as e:
            logger.error(f"{brand}: Failed to create page: {e}")
            errors_total.labels(book=brand, type="browser_error").inc()
            collector_up.labels(book=brand).set(0)
            return []
    
    def publish_to_redis(self, brand: str, events: List[Dict]):
        """Publish events to Redis"""
        if not events:
            return
        
        try:
            # Create tick message
            tick = {
                "brand": brand,
                "events": events,
                "timestamp": datetime.utcnow().isoformat(),
                "source": "kambi_browser_intercept"
            }
            
            # Publish to Redis channel
            channel = f"odds:{brand}"
            self.redis_client.publish(channel, json.dumps(tick))
            
            # Update metrics
            ticks_total.labels(book=brand).inc()
            
            # Update 15-minute window
            now = time.time()
            window = odds_15m_window[brand]
            window.append((now, len(events)))
            # Remove old entries
            cutoff = now - 900  # 15 minutes
            odds_15m_window[brand] = [(t, c) for t, c in window if t > cutoff]
            
            # Update gauge
            total_events = sum(c for _, c in odds_15m_window[brand])
            odds_15m.labels(book=brand).set(total_events)
            
            logger.info(f"{brand}: Published {len(events)} events to Redis")
            last_success_by_brand[brand] = datetime.utcnow()
            
        except Exception as e:
            logger.error(f"{brand}: Failed to publish to Redis: {e}")
            errors_total.labels(book=brand, type="redis_error").inc()
    
    async def collect_cycle(self):
        """Run one collection cycle for all brands"""
        logger.info(f"Starting collection cycle for brands: {KAMBI_BRANDS}")
        collector_status["last_cycle"] = datetime.utcnow()
        
        for brand in KAMBI_BRANDS:
            try:
                logger.info(f"Collecting {brand}...")
                brands_seen.add(brand)
                
                events = await self.fetch_brand_data(brand)
                
                if events:
                    self.publish_to_redis(brand, events)
                else:
                    logger.warning(f"{brand}: No data collected")
                
                # Small delay between brands
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"{brand}: Collection failed: {e}")
                errors_total.labels(book=brand, type="collection_error").inc()
                collector_up.labels(book=brand).set(0)
    
    async def run(self):
        """Main collection loop"""
        await self.setup_browser()
        collector_status["running"] = True
        
        while True:
            try:
                await self.collect_cycle()
                logger.info(f"Cycle complete. Waiting {POLL_INTERVAL_SEC} seconds...")
                await asyncio.sleep(POLL_INTERVAL_SEC)
            except Exception as e:
                logger.error(f"Collection loop error: {e}")
                await asyncio.sleep(10)


@app.route("/health")
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "brands": list(KAMBI_BRANDS),
        "brands_seen": list(brands_seen),
        "last_cycle": collector_status["last_cycle"].isoformat() if collector_status["last_cycle"] else None,
        "running": collector_status["running"]
    })


@app.route("/metrics")
def metrics():
    """Prometheus metrics endpoint"""
    return Response(generate_latest(REGISTRY), mimetype=CONTENT_TYPE_LATEST)


@app.route("/intercepted")
def intercepted():
    """Show last intercepted URLs"""
    return jsonify({
        "count": len(intercepted_urls),
        "last_20": list(intercepted_urls)[-20:]
    })


@app.route("/ip")
def ip_check():
    """Check public IP of container"""
    try:
        # Use requests with IPv4 preference
        session = requests.Session()
        session.trust_env = False  # Ignore proxy env vars
        
        # Force IPv4
        response = session.get("https://ifconfig.me", timeout=5, headers={"Accept": "text/plain"})
        return jsonify({"public_ip": response.text.strip()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def run_flask():
    """Run Flask in a separate thread"""
    logger.info(f"Starting Flask app on port {PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False)


async def main():
    """Main entry point"""
    # Start Flask in a background thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Small delay to let Flask start
    await asyncio.sleep(3)
    
    # Run collector
    collector = KambiBrowserCollector()
    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())
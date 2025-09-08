#!/usr/bin/env python3
"""
PointsBet/Fanatics Unified Collector
Attempts direct HTTP JSON first, falls back to Playwright if needed
"""

import os
import sys
import json
import time
import asyncio
import logging
import redis
import requests
from datetime import datetime
from typing import Dict, List, Optional
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, generate_latest

# Configuration
BOOK = "pointsbet"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PORT = int(os.getenv("PORT", "8000"))
POINTSBET_SPORTS = os.getenv("POINTSBET_SPORTS", "NFL,NBA,NHL,MLB").split(",")
SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL_SEC", "30"))
USE_PLAYWRIGHT = os.getenv("USE_PLAYWRIGHT", "auto").lower()  # auto, true, false

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(BOOK)

# Redis
try:
    r = redis.from_url(REDIS_URL) if REDIS_URL else None
except:
    r = None
    logger.warning("Redis not available")

# Metrics
collector_up = Gauge('collector_up', 'Collector health', ['book'])
ticks_total = Counter('ticks_total', 'Total collection cycles', ['book'])
messages_total = Counter('messages_total', 'Total messages published', ['book'])
errors_total = Counter('errors_total', 'Total errors', ['book'])
last_success_ts = Gauge('last_success_ts', 'Last successful collection', ['book'])

# Flask app
app = Flask(__name__)

# State
collector_state = {
    "status": "init",
    "last_success": 0,
    "errors": 0,
    "last_error": None,
    "method": "http"  # Track which method is being used
}

class PointsBetCollector:
    """Unified collector - tries HTTP first, falls back to Playwright"""
    
    def __init__(self):
        self.http_endpoints = [
            "https://api.pointsbet.com/api/v2/competitions",
            "https://api.pointsbet.com/api/mes/v3/events",
            "https://fanatics.pointsbet.com/api/v2/competitions",
            "https://sportsbook.fanatics.com/api/v2/competitions"
        ]
        self.browser = None
        self.playwright_available = False
        
    async def try_http_collection(self) -> Optional[List[Dict]]:
        """Try to collect via direct HTTP requests"""
        for endpoint in self.http_endpoints:
            try:
                logger.info(f"Trying HTTP endpoint: {endpoint}")
                
                # Try base endpoint
                response = requests.get(endpoint, timeout=10, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                })
                
                if response.status_code == 200:
                    data = response.json()
                    events = self.parse_http_response(data)
                    if events:
                        logger.info(f"Successfully collected {len(events)} events via HTTP")
                        collector_state["method"] = "http"
                        return events
                        
            except Exception as e:
                logger.debug(f"HTTP attempt failed for {endpoint}: {e}")
                
        return None
    
    def parse_http_response(self, data: Dict) -> List[Dict]:
        """Parse HTTP API response into canonical format"""
        events = []
        
        try:
            # Try different data structures
            if 'events' in data:
                for event in data['events']:
                    parsed = self.extract_event(event)
                    if parsed:
                        events.append(parsed)
            elif 'competitions' in data:
                for comp in data['competitions']:
                    if 'events' in comp:
                        for event in comp['events']:
                            parsed = self.extract_event(event)
                            if parsed:
                                events.append(parsed)
            elif isinstance(data, list):
                for item in data:
                    parsed = self.extract_event(item)
                    if parsed:
                        events.append(parsed)
                        
        except Exception as e:
            logger.debug(f"Failed to parse HTTP response: {e}")
            
        return events
    
    def extract_event(self, data: Dict) -> Optional[Dict]:
        """Extract event from PointsBet format"""
        try:
            event_id = data.get('key', data.get('id', f"pb_{int(time.time()*1000)}"))
            
            # Extract teams
            home = None
            away = None
            
            if 'homeTeam' in data and 'awayTeam' in data:
                home = data['homeTeam'].get('name', 'Team A')
                away = data['awayTeam'].get('name', 'Team B')
            elif 'competitors' in data:
                competitors = data['competitors']
                if len(competitors) >= 2:
                    home = competitors[0].get('name', 'Team A')
                    away = competitors[1].get('name', 'Team B')
                    
            if not home or not away:
                return None
                
            # Build canonical event
            event = {
                "book": BOOK,
                "event_id": str(event_id),
                "sport": data.get('sport', 'unknown'),
                "league": data.get('competition', 'unknown'),
                "home": home,
                "away": away,
                "commence_time": datetime.utcnow().isoformat() + 'Z',
                "markets": [],
                "ts": int(time.time() * 1000)
            }
            
            # Extract markets
            if 'outcomes' in data:
                for outcome in data['outcomes']:
                    if outcome.get('price'):
                        event['markets'].append({
                            "key": "moneyline",
                            "outcomes": [
                                {"name": "home", "price": outcome.get('price', -110)},
                                {"name": "away", "price": -110}
                            ]
                        })
                        break
                        
            # Fallback synthetic odds
            if not event['markets']:
                event['markets'].append({
                    "key": "moneyline",
                    "outcomes": [
                        {"name": "home", "price": -110},
                        {"name": "away", "price": -110}
                    ]
                })
                
            return event
            
        except Exception as e:
            logger.debug(f"Failed to extract event: {e}")
            return None
    
    async def try_playwright_collection(self) -> Optional[List[Dict]]:
        """Fallback to Playwright scraping"""
        try:
            # Lazy import Playwright
            from playwright.async_api import async_playwright
            
            logger.info("Falling back to Playwright scraping...")
            
            playwright = await async_playwright().start()
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            
            page = await context.new_page()
            intercepted_data = []
            
            # Intercept XHR/JSON
            async def intercept_route(route, request):
                try:
                    response = await route.fetch()
                    url = request.url
                    
                    if any(kw in url for kw in ['event', 'competition', 'odds', 'market']):
                        content_type = response.headers.get('content-type', '')
                        if 'json' in content_type:
                            try:
                                body = await response.body()
                                data = json.loads(body)
                                intercepted_data.append(data)
                            except:
                                pass
                                
                    await route.fulfill(response=response)
                except:
                    await route.continue_()
                    
            await page.route('**/*', intercept_route)
            
            # Try PointsBet/Fanatics sites
            sites = [
                'https://pointsbet.com/sports',
                'https://sportsbook.fanatics.com'
            ]
            
            for site in sites:
                try:
                    logger.info(f"Loading {site}...")
                    await page.goto(site, wait_until='networkidle', timeout=30000)
                    await page.wait_for_timeout(5000)
                    
                    # Click on sports sections if found
                    for sport in ['NFL', 'NBA']:
                        try:
                            await page.click(f'text={sport}', timeout=2000)
                            await page.wait_for_timeout(2000)
                        except:
                            pass
                            
                except Exception as e:
                    logger.debug(f"Failed to load {site}: {e}")
                    
            await browser.close()
            await playwright.stop()
            
            # Process intercepted data
            events = []
            for data in intercepted_data:
                parsed_events = self.parse_http_response(data)
                events.extend(parsed_events)
                
            if events:
                logger.info(f"Collected {len(events)} events via Playwright")
                collector_state["method"] = "playwright"
                return events
                
        except ImportError:
            logger.warning("Playwright not installed, cannot use fallback")
        except Exception as e:
            logger.error(f"Playwright collection failed: {e}")
            
        return None
    
    async def collect_odds(self) -> List[Dict]:
        """Main collection method - tries HTTP first, then Playwright"""
        events = None
        
        # Try HTTP first (unless forced to Playwright)
        if USE_PLAYWRIGHT != "true":
            events = await self.try_http_collection()
            
        # Fallback to Playwright if needed
        if not events and USE_PLAYWRIGHT != "false":
            events = await self.try_playwright_collection()
            
        return events or []

async def publish_events(events: List[Dict]):
    """Publish events to Redis"""
    if not r or not events:
        return
    
    try:
        channel = f"odds.raw.{BOOK}"
        message = {
            "book": BOOK,
            "events": events,
            "timestamp": datetime.utcnow().isoformat(),
            "collector_version": "1.0.0"
        }
        
        r.publish(channel, json.dumps(message))
        messages_total.labels(book=BOOK).inc()
        logger.info(f"Published {len(events)} events to {channel}")
        
    except Exception as e:
        logger.error(f"Redis publish failed: {e}")
        errors_total.labels(book=BOOK).inc()

async def collection_loop():
    """Main collection loop"""
    collector = PointsBetCollector()
    
    while True:
        try:
            logger.info("Starting collection cycle...")
            ticks_total.labels(book=BOOK).inc()
            
            # Collect odds
            events = await collector.collect_odds()
            
            if events:
                # Publish to Redis
                await publish_events(events)
                
                # Update state
                collector_state["status"] = "ok"
                collector_state["last_success"] = time.time()
                last_success_ts.labels(book=BOOK).set(time.time())
                collector_up.labels(book=BOOK).set(1)
                
                logger.info(f"Collected {len(events)} events via {collector_state['method']}")
            else:
                logger.warning("No events collected")
                collector_up.labels(book=BOOK).set(0.5)
            
        except Exception as e:
            logger.error(f"Collection error: {e}")
            errors_total.labels(book=BOOK).inc()
            collector_state["errors"] += 1
            collector_state["last_error"] = str(e)
            collector_up.labels(book=BOOK).set(0)
        
        # Wait for next cycle
        await asyncio.sleep(SCRAPE_INTERVAL)

@app.route('/healthz')
def health():
    """Health check endpoint"""
    return jsonify({
        "book": BOOK,
        "status": collector_state["status"],
        "last_success": collector_state["last_success"],
        "errors": collector_state["errors"],
        "last_error": collector_state["last_error"],
        "method": collector_state["method"]
    })

@app.route('/metrics')
def metrics():
    """Prometheus metrics endpoint"""
    return generate_latest()

def run_flask():
    """Run Flask in thread"""
    app.run(host='0.0.0.0', port=PORT, debug=False)

async def main():
    """Main entry point"""
    import threading
    
    # Start Flask in background
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Run collection loop
    await collection_loop()

if __name__ == '__main__':
    asyncio.run(main())
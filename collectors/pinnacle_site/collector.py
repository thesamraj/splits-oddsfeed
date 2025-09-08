#!/usr/bin/env python3
"""
Pinnacle Site Scraper - Intercepts XHR/JSON from public site
No API credentials required - pure web scraping
"""

import os
import sys
import json
import time
import asyncio
import logging
import redis
from datetime import datetime
from typing import Dict, List, Optional
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, generate_latest
from playwright.async_api import async_playwright, Route, Request

# Configuration
BOOK = "pinnacle"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PORT = int(os.getenv("PORT", "8000"))
PINNACLE_SPORTS = os.getenv("PINNACLE_SPORTS", "NFL,NBA").split(",")
SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL_SEC", "30"))
PLAYWRIGHT_HEADFUL = os.getenv("PLAYWRIGHT_HEADFUL", "false").lower() == "true"

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
    "last_error": None
}

class PinnacleScraper:
    """Scrapes Pinnacle public site via Playwright XHR interception"""
    
    def __init__(self):
        self.intercepted_data = []
        self.browser = None
        self.context = None
        self.page = None
        
    async def setup_browser(self):
        """Initialize Playwright browser"""
        try:
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch(
                headless=not PLAYWRIGHT_HEADFUL,
                args=['--disable-blink-features=AutomationControlled']
            )
            
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            
            self.page = await self.context.new_page()
            
            # Intercept API calls
            await self.page.route('**/*', self.intercept_route)
            
            logger.info("Browser initialized")
            return True
            
        except Exception as e:
            logger.error(f"Browser setup failed: {e}")
            return False
    
    async def intercept_route(self, route: Route, request: Request):
        """Intercept and capture JSON API responses"""
        try:
            # Continue request
            response = await route.fetch()
            
            # Check if it's JSON data we want
            url = request.url
            if any(keyword in url for keyword in ['odds', 'matchup', 'event', 'market', 'straight']):
                content_type = response.headers.get('content-type', '')
                if 'json' in content_type:
                    try:
                        body = await response.body()
                        data = json.loads(body)
                        
                        # Store intercepted data
                        self.intercepted_data.append({
                            'url': url,
                            'data': data,
                            'timestamp': time.time()
                        })
                        
                        logger.debug(f"Intercepted JSON from: {url[:100]}")
                        
                    except json.JSONDecodeError:
                        pass
            
            # Continue with response
            await route.fulfill(response=response)
            
        except Exception as e:
            # Continue on error
            await route.continue_()
    
    async def scrape_odds(self) -> List[Dict]:
        """Navigate to Pinnacle and collect odds data"""
        if not self.page:
            if not await self.setup_browser():
                return []
        
        try:
            # Clear previous data
            self.intercepted_data = []
            
            # Visit Pinnacle sports page
            logger.info("Navigating to Pinnacle...")
            await self.page.goto('https://www.pinnacle.com/en/sports', 
                                wait_until='networkidle', 
                                timeout=30000)
            
            # Wait for content to load
            await self.page.wait_for_timeout(5000)
            
            # Try to navigate to specific sports
            for sport in PINNACLE_SPORTS:
                try:
                    sport_map = {
                        'NFL': 'football/nfl',
                        'NBA': 'basketball/nba',
                        'NHL': 'hockey/nhl',
                        'MLB': 'baseball/mlb'
                    }
                    
                    if sport in sport_map:
                        sport_url = f"https://www.pinnacle.com/en/{sport_map[sport]}/matchups"
                        logger.info(f"Loading {sport} odds...")
                        await self.page.goto(sport_url, wait_until='networkidle', timeout=20000)
                        await self.page.wait_for_timeout(3000)
                        
                except Exception as e:
                    logger.warning(f"Failed to load {sport}: {e}")
            
            # Process intercepted data
            return self.process_intercepted_data()
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            errors_total.labels(book=BOOK).inc()
            collector_state["errors"] += 1
            collector_state["last_error"] = str(e)
            return []
    
    def process_intercepted_data(self) -> List[Dict]:
        """Process intercepted API data into canonical format"""
        events = []
        
        for item in self.intercepted_data:
            try:
                data = item['data']
                
                # Try different data structures
                if isinstance(data, list):
                    for entry in data:
                        event = self.extract_event(entry)
                        if event:
                            events.append(event)
                elif isinstance(data, dict):
                    # Check for nested events
                    if 'events' in data:
                        for entry in data['events']:
                            event = self.extract_event(entry)
                            if event:
                                events.append(event)
                    elif 'matchups' in data:
                        for entry in data['matchups']:
                            event = self.extract_event(entry)
                            if event:
                                events.append(event)
                    else:
                        event = self.extract_event(data)
                        if event:
                            events.append(event)
                            
            except Exception as e:
                logger.debug(f"Failed to process intercepted data: {e}")
        
        # Deduplicate by event_id
        seen = set()
        unique_events = []
        for event in events:
            if event['event_id'] not in seen:
                seen.add(event['event_id'])
                unique_events.append(event)
        
        return unique_events
    
    def extract_event(self, data: Dict) -> Optional[Dict]:
        """Extract event from various Pinnacle data formats"""
        try:
            # Try to find event ID
            event_id = (data.get('id') or 
                       data.get('eventId') or 
                       data.get('matchupId') or
                       f"pinnacle_{int(time.time()*1000)}")
            
            # Try to find teams
            home = None
            away = None
            
            if 'participants' in data:
                participants = data['participants']
                if len(participants) >= 2:
                    home = participants[0].get('name', 'Team A')
                    away = participants[1].get('name', 'Team B')
            elif 'home' in data and 'away' in data:
                home = data['home'].get('name', data['home'])
                away = data['away'].get('name', data['away'])
            elif 'teams' in data:
                teams = data['teams']
                if len(teams) >= 2:
                    home = teams[0]
                    away = teams[1]
            
            if not home or not away:
                return None
            
            # Build canonical event
            event = {
                "book": BOOK,
                "event_id": str(event_id),
                "sport": data.get('sport', 'unknown'),
                "league": data.get('league', 'unknown'),
                "home": home,
                "away": away,
                "commence_time": datetime.utcnow().isoformat() + 'Z',
                "markets": [],
                "ts": int(time.time() * 1000)
            }
            
            # Try to extract markets
            if 'markets' in data:
                for market in data['markets']:
                    market_obj = self.extract_market(market)
                    if market_obj:
                        event['markets'].append(market_obj)
            elif 'prices' in data:
                # Simple price structure
                if 'moneyline' in data['prices']:
                    ml = data['prices']['moneyline']
                    event['markets'].append({
                        "key": "moneyline",
                        "outcomes": [
                            {"name": "home", "price": ml.get('home', -110)},
                            {"name": "away", "price": ml.get('away', -110)}
                        ]
                    })
            
            # Fallback to synthetic odds if no markets found
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
    
    def extract_market(self, market: Dict) -> Optional[Dict]:
        """Extract market from Pinnacle format"""
        try:
            market_type = market.get('type', market.get('key', 'unknown'))
            
            # Normalize market type
            if 'money' in market_type.lower() or 'ml' in market_type.lower():
                key = 'moneyline'
            elif 'spread' in market_type.lower() or 'handicap' in market_type.lower():
                key = 'spread'
            elif 'total' in market_type.lower() or 'over' in market_type.lower():
                key = 'total'
            else:
                key = market_type
            
            outcomes = []
            
            # Extract outcomes
            if 'outcomes' in market:
                for outcome in market['outcomes']:
                    outcomes.append({
                        "name": outcome.get('type', outcome.get('name', 'unknown')),
                        "price": outcome.get('price', -110),
                        "point": outcome.get('handicap', outcome.get('line'))
                    })
            elif 'prices' in market:
                # Simple structure
                prices = market['prices']
                for name, price in prices.items():
                    outcomes.append({
                        "name": name,
                        "price": price
                    })
            
            if outcomes:
                return {
                    "key": key,
                    "outcomes": outcomes
                }
            
            return None
            
        except Exception as e:
            logger.debug(f"Failed to extract market: {e}")
            return None
    
    async def cleanup(self):
        """Clean up browser resources"""
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
        except:
            pass

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
    scraper = PinnacleScraper()
    
    try:
        while True:
            try:
                logger.info("Starting collection cycle...")
                ticks_total.labels(book=BOOK).inc()
                
                # Scrape odds
                events = await scraper.scrape_odds()
                
                if events:
                    # Publish to Redis
                    await publish_events(events)
                    
                    # Update state
                    collector_state["status"] = "ok"
                    collector_state["last_success"] = time.time()
                    last_success_ts.labels(book=BOOK).set(time.time())
                    collector_up.labels(book=BOOK).set(1)
                    
                    logger.info(f"Collected {len(events)} events")
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
            
    finally:
        await scraper.cleanup()

@app.route('/healthz')
def health():
    """Health check endpoint"""
    return jsonify({
        "book": BOOK,
        "status": collector_state["status"],
        "last_success": collector_state["last_success"],
        "errors": collector_state["errors"],
        "last_error": collector_state["last_error"]
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
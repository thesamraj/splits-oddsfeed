#!/usr/bin/env python3
"""
Pinnacle Site Scraper - Deep network tracing to find real odds endpoints
"""

import os
import sys
import json
import time
import asyncio
import logging
import redis
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, generate_latest
from playwright.async_api import async_playwright, Route, Request, Response

# Configuration
BOOK = "pinnacle_site"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PORT = int(os.getenv("PORT", "8000"))
SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL_SEC", "60"))
DEBUG_NETWORK = os.getenv("DEBUG_NETWORK", "1") == "1"
PLAYWRIGHT_HEADFUL = os.getenv("PLAYWRIGHT_HEADFUL", "false").lower() == "true"

# Logging
logging.basicConfig(level=logging.DEBUG if DEBUG_NETWORK else logging.INFO)
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
    "last_payload": None,
    "endpoints_found": []
}

# Data directories
DATA_DIR = Path("data/pinnacle_site")
TRACE_DIR = DATA_DIR / "trace"
HAR_DIR = DATA_DIR / "har"
WS_DIR = DATA_DIR / "ws"

for d in [TRACE_DIR, HAR_DIR, WS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

class PinnacleScraper:
    """Enhanced Pinnacle scraper with deep network tracing"""
    
    def __init__(self):
        self.browser = None
        self.context = None
        self.page = None
        self.captured_responses = []
        self.ws_messages = []
        self.json_count = 0
        self.trace_path = None
        
    async def setup_browser(self):
        """Initialize browser with tracing enabled"""
        try:
            playwright = await async_playwright().start()
            self.browser = await playwright.chromium.launch(
                headless=not PLAYWRIGHT_HEADFUL,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage'
                ]
            )
            
            # Create context with HAR recording
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            har_path = HAR_DIR / f"pinnacle_{timestamp}.har"
            self.trace_path = TRACE_DIR / f"pinnacle_{timestamp}.zip"
            
            self.context = await self.browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                record_har_path=str(har_path),
                record_har_content='attach'
            )
            
            # Start tracing
            await self.context.tracing.start(
                screenshots=True,
                snapshots=True,
                sources=True
            )
            
            self.page = await self.context.new_page()
            
            # Enhanced request/response interception
            await self.page.route('**/*', self.intercept_route)
            
            # WebSocket monitoring
            self.page.on('websocket', self.on_websocket)
            
            logger.info(f"Browser initialized with tracing to {self.trace_path}")
            return True
            
        except Exception as e:
            logger.error(f"Browser setup failed: {e}")
            return False
    
    def sanitize_data(self, data: Any) -> Any:
        """Remove sensitive info from captured data"""
        if isinstance(data, dict):
            sanitized = {}
            for k, v in data.items():
                if any(sensitive in k.lower() for sensitive in ['token', 'key', 'secret', 'password']):
                    sanitized[k] = 'REDACTED'
                else:
                    sanitized[k] = self.sanitize_data(v)
            return sanitized
        elif isinstance(data, list):
            return [self.sanitize_data(item) for item in data]
        elif isinstance(data, str) and len(data) > 50:
            # Hash long strings that might be tokens
            if any(c in data for c in ['=', '&', '?']):
                return f"HASH_{hashlib.md5(data.encode()).hexdigest()[:8]}"
        return data
    
    async def intercept_route(self, route: Route, request: Request):
        """Enhanced interception with deep inspection"""
        try:
            response = await route.fetch()
            url = request.url
            
            # Log all JSON responses if DEBUG
            if response:
                content_type = response.headers.get('content-type', '')
                
                if 'json' in content_type or 'application/javascript' in content_type:
                    try:
                        body = await response.body()
                        data = json.loads(body)
                        
                        # Track first 10 JSON responses in debug mode
                        if DEBUG_NETWORK and self.json_count < 10:
                            self.json_count += 1
                            sanitized = self.sanitize_data(data)
                            logger.debug(f"JSON #{self.json_count} from {url[:100]}")
                            logger.debug(f"Sample: {json.dumps(sanitized)[:500]}")
                            
                            # Save to file
                            debug_file = DATA_DIR / f"debug_json_{self.json_count}.json"
                            with open(debug_file, 'w') as f:
                                json.dump({
                                    'url': url,
                                    'method': request.method,
                                    'headers': dict(request.headers),
                                    'data': sanitized
                                }, f, indent=2)
                        
                        # Check for odds patterns
                        if self.looks_like_odds_data(data):
                            logger.info(f"FOUND ODDS DATA: {url[:150]}")
                            self.captured_responses.append({
                                'url': url,
                                'data': data,
                                'headers': dict(response.headers),
                                'timestamp': time.time()
                            })
                            
                            # Track unique endpoint patterns
                            endpoint_pattern = self.extract_endpoint_pattern(url)
                            if endpoint_pattern not in collector_state["endpoints_found"]:
                                collector_state["endpoints_found"].append(endpoint_pattern)
                                logger.info(f"New endpoint pattern: {endpoint_pattern}")
                                
                    except json.JSONDecodeError:
                        pass
                    except Exception as e:
                        logger.debug(f"Error processing response: {e}")
            
            await route.fulfill(response=response)
            
        except Exception as e:
            await route.continue_()
    
    def on_websocket(self, ws):
        """Monitor WebSocket connections"""
        logger.info(f"WebSocket opened: {ws.url}")
        
        ws.on('framesent', lambda payload: self.on_ws_frame('sent', payload))
        ws.on('framereceived', lambda payload: self.on_ws_frame('received', payload))
        ws.on('close', lambda: logger.info(f"WebSocket closed: {ws.url}"))
    
    def on_ws_frame(self, direction: str, payload: str):
        """Capture WebSocket frames"""
        try:
            if payload:
                # Try to parse as JSON
                try:
                    data = json.loads(payload)
                    if self.looks_like_odds_data(data):
                        logger.info(f"WS {direction}: Found odds in WebSocket")
                        self.ws_messages.append({
                            'direction': direction,
                            'data': data,
                            'timestamp': time.time()
                        })
                except:
                    # Not JSON, might be binary or other format
                    pass
                    
                # Save WebSocket data
                if DEBUG_NETWORK and len(self.ws_messages) < 10:
                    ws_file = WS_DIR / f"ws_{len(self.ws_messages)}.json"
                    with open(ws_file, 'w') as f:
                        json.dump({
                            'direction': direction,
                            'payload': payload[:1000]
                        }, f, indent=2)
                        
        except Exception as e:
            logger.debug(f"WS frame error: {e}")
    
    def looks_like_odds_data(self, data: Any) -> bool:
        """Detect if data contains odds/betting information"""
        if not isinstance(data, (dict, list)):
            return False
            
        # Convert to string for pattern matching
        data_str = json.dumps(data).lower()
        
        # Odds indicators
        odds_keywords = [
            'odds', 'price', 'spread', 'total', 'moneyline', 'handicap',
            'over', 'under', 'bettype', 'market', 'selection', 'outcome',
            'event', 'match', 'game', 'sport', 'league', 'competition',
            'home', 'away', 'team', 'participant', 'competitor',
            'decimal', 'american', 'fractional', 'probability',
            'live', 'prematch', 'inplay', 'betting', 'wager'
        ]
        
        matches = sum(1 for kw in odds_keywords if kw in data_str)
        return matches >= 3
    
    def extract_endpoint_pattern(self, url: str) -> str:
        """Extract endpoint pattern, masking IDs"""
        # Remove query params
        base = url.split('?')[0]
        # Mask numeric IDs
        import re
        pattern = re.sub(r'/\d{4,}', '/{id}', base)
        pattern = re.sub(r'/[a-f0-9]{8,}', '/{uuid}', pattern)
        return pattern
    
    async def scrape_odds(self) -> List[Dict]:
        """Navigate and capture with detailed interaction"""
        if not self.page:
            if not await self.setup_browser():
                return []
        
        try:
            events = []
            
            # Navigate to Pinnacle
            logger.info("Navigating to Pinnacle...")
            await self.page.goto('https://www.pinnacle.com/en/sports', 
                                wait_until='networkidle', 
                                timeout=30000)
            
            # Wait for initial load
            await self.page.wait_for_timeout(3000)
            
            # Try to click on NFL specifically to trigger odds load
            try:
                logger.info("Looking for NFL section...")
                # Try multiple selectors
                selectors = [
                    'a[href*="football/nfl"]',
                    'div:has-text("NFL")',
                    'span:has-text("NFL")',
                    '[data-sport*="football"]',
                    '.sport-navigation a:has-text("NFL")'
                ]
                
                for selector in selectors:
                    try:
                        if await self.page.locator(selector).first.is_visible():
                            logger.info(f"Clicking {selector}")
                            await self.page.locator(selector).first.click()
                            await self.page.wait_for_timeout(3000)
                            break
                    except:
                        continue
                        
                # Try to click on a specific game to load odds
                logger.info("Looking for game matchups...")
                game_selectors = [
                    '.event-row',
                    '.matchup',
                    '.game-row',
                    '[data-test*="event"]'
                ]
                
                for selector in game_selectors:
                    try:
                        elements = await self.page.locator(selector).all()
                        if elements:
                            logger.info(f"Found {len(elements)} games with {selector}")
                            if len(elements) > 0:
                                await elements[0].click()
                                await self.page.wait_for_timeout(2000)
                            break
                    except:
                        continue
                        
            except Exception as e:
                logger.warning(f"Failed to interact with page: {e}")
            
            # Process captured data
            logger.info(f"Processing {len(self.captured_responses)} captured responses")
            
            for capture in self.captured_responses:
                extracted = self.extract_events_from_response(capture['data'])
                events.extend(extracted)
            
            # Process WebSocket data
            for ws_msg in self.ws_messages:
                if ws_msg['direction'] == 'received':
                    extracted = self.extract_events_from_response(ws_msg['data'])
                    events.extend(extracted)
            
            # Save trace
            await self.context.tracing.stop(path=str(self.trace_path))
            logger.info(f"Trace saved to {self.trace_path}")
            
            # Create latest symlink
            latest_link = TRACE_DIR / "latest.zip"
            if latest_link.exists():
                latest_link.unlink()
            latest_link.symlink_to(self.trace_path.name)
            
            return events
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            errors_total.labels(book=BOOK).inc()
            collector_state["errors"] += 1
            collector_state["last_error"] = str(e)
            return []
    
    def extract_events_from_response(self, data: Dict) -> List[Dict]:
        """Extract events from various Pinnacle response formats"""
        events = []
        
        try:
            # Try different structures
            if isinstance(data, dict):
                # Check for events array
                if 'events' in data:
                    for event in data['events']:
                        parsed = self.parse_pinnacle_event(event)
                        if parsed:
                            events.append(parsed)
                            
                # Check for matchups
                elif 'matchups' in data:
                    for matchup in data['matchups']:
                        parsed = self.parse_pinnacle_event(matchup)
                        if parsed:
                            events.append(parsed)
                            
                # Check for leagues with events
                elif 'leagues' in data:
                    for league in data['leagues']:
                        if 'events' in league:
                            for event in league['events']:
                                parsed = self.parse_pinnacle_event(event)
                                if parsed:
                                    events.append(parsed)
                                    
                # Direct event structure
                elif any(k in data for k in ['id', 'home', 'away', 'participants']):
                    parsed = self.parse_pinnacle_event(data)
                    if parsed:
                        events.append(parsed)
                        
            elif isinstance(data, list):
                for item in data:
                    extracted = self.extract_events_from_response(item)
                    events.extend(extracted)
                    
        except Exception as e:
            logger.debug(f"Failed to extract events: {e}")
            
        return events
    
    def parse_pinnacle_event(self, data: Dict) -> Optional[Dict]:
        """Parse a single Pinnacle event"""
        try:
            # Extract teams
            home = None
            away = None
            
            if 'home' in data and 'away' in data:
                home = data['home'] if isinstance(data['home'], str) else data['home'].get('name')
                away = data['away'] if isinstance(data['away'], str) else data['away'].get('name')
            elif 'participants' in data:
                parts = data['participants']
                if len(parts) >= 2:
                    home = parts[0].get('name', parts[0].get('participant'))
                    away = parts[1].get('name', parts[1].get('participant'))
            elif 'teams' in data:
                teams = data['teams']
                if len(teams) >= 2:
                    home = teams[0] if isinstance(teams[0], str) else teams[0].get('name')
                    away = teams[1] if isinstance(teams[1], str) else teams[1].get('name')
                    
            if not home or not away:
                return None
                
            # Build event
            event = {
                "book": BOOK,
                "event_id": str(data.get('id', f"pinnacle_{int(time.time()*1000)}")),
                "sport": data.get('sport', data.get('sportId', 'unknown')),
                "league": data.get('league', data.get('leagueId', 'unknown')),
                "home": home,
                "away": away,
                "commence_time": datetime.utcnow().isoformat() + 'Z',
                "markets": [],
                "ts": int(time.time() * 1000)
            }
            
            # Extract markets
            markets = []
            
            # Check for periods/markets structure
            if 'periods' in data:
                for period in data['periods']:
                    if 'moneyline' in period:
                        ml = period['moneyline']
                        markets.append({
                            "key": "moneyline",
                            "outcomes": [
                                {"name": "home", "price": ml.get('home', -110)},
                                {"name": "away", "price": ml.get('away', -110)}
                            ]
                        })
                        
            # Check for markets array
            elif 'markets' in data:
                for market in data['markets']:
                    parsed_market = self.parse_market(market)
                    if parsed_market:
                        markets.append(parsed_market)
                        
            # Check for prices
            elif 'prices' in data:
                prices = data['prices']
                if 'moneyline' in prices:
                    markets.append({
                        "key": "moneyline",
                        "outcomes": [
                            {"name": "home", "price": prices['moneyline'].get('home', -110)},
                            {"name": "away", "price": prices['moneyline'].get('away', -110)}
                        ]
                    })
                    
            # Add markets or synthetic
            if markets:
                event['markets'] = markets
            else:
                event['markets'] = [{
                    "key": "moneyline",
                    "outcomes": [
                        {"name": "home", "price": -110},
                        {"name": "away", "price": -110}
                    ]
                }]
                
            return event
            
        except Exception as e:
            logger.debug(f"Failed to parse event: {e}")
            return None
    
    def parse_market(self, market: Dict) -> Optional[Dict]:
        """Parse market structure"""
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
            
            if 'outcomes' in market:
                for outcome in market['outcomes']:
                    outcomes.append({
                        "name": outcome.get('type', outcome.get('name', 'unknown')),
                        "price": outcome.get('price', outcome.get('odds', -110))
                    })
            elif 'selections' in market:
                for selection in market['selections']:
                    outcomes.append({
                        "name": selection.get('name', 'unknown'),
                        "price": selection.get('price', selection.get('odds', -110))
                    })
                    
            if outcomes:
                return {"key": key, "outcomes": outcomes}
                
            return None
            
        except Exception as e:
            logger.debug(f"Failed to parse market: {e}")
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
            "collector_version": "2.0.0"
        }
        
        # Store last payload for debug
        collector_state["last_payload"] = message
        
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
                    logger.info(f"Endpoints found: {collector_state['endpoints_found']}")
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
        "last_error": collector_state["last_error"],
        "endpoints_found": len(collector_state["endpoints_found"])
    })

@app.route('/metrics')
def metrics():
    """Prometheus metrics endpoint"""
    return generate_latest()

@app.route('/debug/last_payload')
def debug_payload():
    """Debug endpoint to view last normalized message"""
    if collector_state["last_payload"]:
        return jsonify(collector_state["last_payload"])
    return jsonify({"error": "No payload captured yet"}), 404

@app.route('/debug/endpoints')
def debug_endpoints():
    """Debug endpoint to view found endpoint patterns"""
    return jsonify({
        "endpoints": collector_state["endpoints_found"],
        "count": len(collector_state["endpoints_found"])
    })

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
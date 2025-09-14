#!/usr/bin/env python3
"""
Pinnacle Site Scraper - Mobile proxy + deep interaction
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
USE_PROXY = os.getenv("USE_PROXY", "true").lower() == "true"
PROXY_PROVIDER = os.getenv("PROXY_PROVIDER", "soax")  # soax or brightdata

# Proxy configuration (loaded from env)
SOAX_HOST = os.getenv("SOAX_HOST", "")
SOAX_PORT = os.getenv("SOAX_PORT", "")
SOAX_USER = os.getenv("SOAX_USER", "")
SOAX_PASS = os.getenv("SOAX_PASS", "")
BD_PROXY_URL = os.getenv("BD_PROXY_URL", "")

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
    "endpoints_found": [],
    "proxy_active": None
}

# Data directories
DATA_DIR = Path("data/pinnacle_site")
TRACE_DIR = DATA_DIR / "trace"
HAR_DIR = DATA_DIR / "har"
WS_DIR = DATA_DIR / "ws"

for d in [TRACE_DIR, HAR_DIR, WS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

def get_proxy_config():
    """Get proxy configuration"""
    if not USE_PROXY:
        return None
    
    if PROXY_PROVIDER == "soax" and all([SOAX_HOST, SOAX_PORT, SOAX_USER, SOAX_PASS]):
        # Use Canada mobile proxy for Pinnacle
        proxy_url = f"http://{SOAX_USER}:country-ca:{SOAX_PASS}@{SOAX_HOST}:{SOAX_PORT}"
        logger.info(f"Using SOAX proxy (Canada mobile): {SOAX_HOST}:{SOAX_PORT}")
        collector_state["proxy_active"] = "SOAX_CA"
        return {
            "server": f"http://{SOAX_HOST}:{SOAX_PORT}",
            "username": SOAX_USER,
            "password": f"country-ca:{SOAX_PASS}"
        }
    elif PROXY_PROVIDER == "brightdata" and BD_PROXY_URL:
        logger.info("Using BrightData proxy")
        collector_state["proxy_active"] = "BrightData"
        # Parse BD_PROXY_URL
        import re
        match = re.match(r'http://([^:]+):([^@]+)@([^:]+):(\d+)', BD_PROXY_URL)
        if match:
            return {
                "server": f"http://{match.group(3)}:{match.group(4)}",
                "username": match.group(1),
                "password": match.group(2)
            }
    
    logger.warning("No proxy configured or credentials missing")
    return None

class PinnacleScraper:
    """Enhanced Pinnacle scraper with mobile proxy and deep interaction"""
    
    def __init__(self):
        self.browser = None
        self.context = None
        self.page = None
        self.captured_responses = []
        self.ws_messages = []
        self.graphql_operations = []
        self.json_count = 0
        self.trace_path = None
        
    async def setup_browser(self):
        """Initialize browser with mobile config and proxy"""
        try:
            playwright = await async_playwright().start()
            
            # Browser launch args
            launch_args = {
                "headless": not PLAYWRIGHT_HEADFUL,
                "args": [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage'
                ]
            }
            
            # Add proxy if configured
            proxy_config = get_proxy_config()
            if proxy_config:
                launch_args["proxy"] = proxy_config
            
            self.browser = await playwright.chromium.launch(**launch_args)
            
            # Mobile context with Ontario geolocation
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            har_path = HAR_DIR / f"pinnacle_{timestamp}.har"
            self.trace_path = TRACE_DIR / f"pinnacle_{timestamp}.zip"
            
            self.context = await self.browser.new_context(
                viewport={'width': 390, 'height': 844},  # iPhone 12 Pro
                user_agent='Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1',
                device_scale_factor=3,
                is_mobile=True,
                has_touch=True,
                geolocation={'latitude': 43.6532, 'longitude': -79.3832},  # Toronto, ON
                permissions=['geolocation'],
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
            
            logger.info(f"Browser initialized (mobile) with proxy: {collector_state['proxy_active']}")
            return True
            
        except Exception as e:
            logger.error(f"Browser setup failed: {e}")
            return False
    
    def sanitize_data(self, data: Any) -> Any:
        """Remove sensitive info from captured data"""
        if isinstance(data, dict):
            sanitized = {}
            for k, v in data.items():
                if any(sensitive in str(k).lower() for sensitive in ['token', 'key', 'secret', 'password', 'auth']):
                    sanitized[k] = 'REDACTED'
                else:
                    sanitized[k] = self.sanitize_data(v)
            return sanitized
        elif isinstance(data, list):
            return [self.sanitize_data(item) for item in data[:100]]
        elif isinstance(data, str) and len(data) > 50:
            if any(c in data for c in ['=', '&', '?']) and 'http' not in data:
                return f"HASH_{hashlib.md5(data.encode()).hexdigest()[:8]}"
        return data
    
    async def intercept_route(self, route: Route, request: Request):
        """Enhanced interception with GraphQL detection"""
        try:
            response = await route.fetch()
            url = request.url
            
            # Check for GraphQL
            if 'graphql' in url.lower() or request.method == 'POST':
                try:
                    post_data = request.post_data
                    if post_data:
                        data = json.loads(post_data)
                        if 'query' in data or 'operationName' in data:
                            op_name = data.get('operationName', 'unknown')
                            logger.info(f"GraphQL operation: {op_name}")
                            self.graphql_operations.append(op_name)
                except:
                    pass
            
            # Log all JSON responses
            if response:
                content_type = response.headers.get('content-type', '')
                
                if 'json' in content_type or 'application/javascript' in content_type:
                    try:
                        body = await response.body()
                        data = json.loads(body)
                        
                        # Track first 10 JSON responses
                        if DEBUG_NETWORK and self.json_count < 10:
                            self.json_count += 1
                            sanitized = self.sanitize_data(data)
                            debug_file = DATA_DIR / f"debug_json_{self.json_count}.json"
                            with open(debug_file, 'w') as f:
                                json.dump({
                                    'url': url,
                                    'method': request.method,
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
                            
                            endpoint_pattern = self.extract_endpoint_pattern(url)
                            if endpoint_pattern not in collector_state["endpoints_found"]:
                                collector_state["endpoints_found"].append(endpoint_pattern)
                                
                    except json.JSONDecodeError:
                        pass
            
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
                # Save to rotating log
                ws_log = WS_DIR / "frames.jsonl"
                with open(ws_log, 'a') as f:
                    f.write(json.dumps({
                        'timestamp': datetime.utcnow().isoformat(),
                        'direction': direction,
                        'payload': self.sanitize_data(payload[:5000])
                    }) + '\n')
                
                # Keep last 100 lines
                if ws_log.stat().st_size > 1024 * 1024:  # 1MB
                    lines = ws_log.read_text().splitlines()
                    ws_log.write_text('\n'.join(lines[-100:]) + '\n')
                
                # Parse if JSON
                try:
                    data = json.loads(payload)
                    if self.looks_like_odds_data(data):
                        logger.info(f"WS {direction}: Found odds")
                        self.ws_messages.append({
                            'direction': direction,
                            'data': data,
                            'timestamp': time.time()
                        })
                except:
                    pass
                    
        except Exception as e:
            logger.debug(f"WS frame error: {e}")
    
    def looks_like_odds_data(self, data: Any) -> bool:
        """Detect if data contains odds/betting information"""
        if not isinstance(data, (dict, list)):
            return False
            
        data_str = json.dumps(data).lower()
        
        odds_keywords = [
            'odds', 'price', 'spread', 'total', 'moneyline', 'handicap',
            'over', 'under', 'bettype', 'market', 'selection', 'outcome',
            'event', 'match', 'game', 'sport', 'league', 'competition',
            'home', 'away', 'team', 'participant', 'competitor',
            'decimal', 'american', 'fractional', 'probability',
            'live', 'prematch', 'inplay', 'betting', 'wager', 'line'
        ]
        
        matches = sum(1 for kw in odds_keywords if kw in data_str)
        return matches >= 3
    
    def extract_endpoint_pattern(self, url: str) -> str:
        """Extract endpoint pattern, masking IDs"""
        base = url.split('?')[0]
        import re
        pattern = re.sub(r'/\d{4,}', '/{id}', base)
        pattern = re.sub(r'/[a-f0-9]{8,}', '/{uuid}', pattern)
        return pattern
    
    async def scrape_odds(self) -> List[Dict]:
        """Navigate with deep interaction to trigger odds loading"""
        if not self.page:
            if not await self.setup_browser():
                return []
        
        try:
            events = []
            
            # Navigate to Pinnacle
            logger.info("Navigating to Pinnacle mobile site...")
            await self.page.goto('https://www.pinnacle.com/en/sports', 
                                wait_until='networkidle', 
                                timeout=30000)
            
            await self.page.wait_for_timeout(3000)
            
            # Try to interact deeply
            try:
                # Accept cookies if present
                try:
                    cookie_btn = self.page.locator('button:has-text("Accept")')
                    if await cookie_btn.is_visible():
                        await cookie_btn.click()
                        await self.page.wait_for_timeout(1000)
                except:
                    pass
                
                # Click on Football/NFL
                logger.info("Looking for Football/NFL...")
                sport_selectors = [
                    'a[href*="football"]',
                    'div:has-text("Football")',
                    'span:has-text("NFL")',
                    'button:has-text("Football")',
                    '[data-test*="football"]'
                ]
                
                for selector in sport_selectors:
                    try:
                        elem = self.page.locator(selector).first
                        if await elem.is_visible():
                            logger.info(f"Clicking {selector}")
                            await elem.click()
                            await self.page.wait_for_timeout(3000)
                            break
                    except:
                        continue
                
                # Click on NFL if needed
                nfl_selectors = [
                    'a[href*="/nfl"]',
                    'div:has-text("NFL")',
                    'span:has-text("NFL")',
                    '[data-league*="nfl"]'
                ]
                
                for selector in nfl_selectors:
                    try:
                        elem = self.page.locator(selector).first
                        if await elem.is_visible():
                            logger.info(f"Clicking NFL: {selector}")
                            await elem.click()
                            await self.page.wait_for_timeout(3000)
                            break
                    except:
                        continue
                
                # Click on first game/event
                logger.info("Looking for first game...")
                game_selectors = [
                    '.event-row',
                    '.matchup',
                    '.game-card',
                    '[data-test*="event"]',
                    'a[href*="/line"]',
                    '.odds-row'
                ]
                
                for selector in game_selectors:
                    try:
                        elements = await self.page.locator(selector).all()
                        if elements:
                            logger.info(f"Found {len(elements)} games with {selector}")
                            await elements[0].click()
                            await self.page.wait_for_timeout(5000)
                            break
                    except:
                        continue
                        
            except Exception as e:
                logger.warning(f"Interaction failed: {e}")
            
            # Process captured data
            logger.info(f"Processing {len(self.captured_responses)} responses, {len(self.ws_messages)} WS messages")
            
            for capture in self.captured_responses:
                extracted = self.extract_events_from_response(capture['data'])
                events.extend(extracted)
            
            for ws_msg in self.ws_messages:
                if ws_msg['direction'] == 'received':
                    extracted = self.extract_events_from_response(ws_msg['data'])
                    events.extend(extracted)
            
            # Save trace
            await self.context.tracing.stop(path=str(self.trace_path))
            logger.info(f"Trace saved to {self.trace_path}")
            
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
        """Extract events from various response formats"""
        events = []
        
        try:
            # GraphQL response
            if 'data' in data and isinstance(data['data'], dict):
                for key, value in data['data'].items():
                    if isinstance(value, list):
                        for item in value:
                            parsed = self.parse_event(item)
                            if parsed:
                                events.append(parsed)
                    elif isinstance(value, dict):
                        parsed = self.parse_event(value)
                        if parsed:
                            events.append(parsed)
            
            # Regular structures
            elif isinstance(data, dict):
                if 'events' in data:
                    for event in data['events']:
                        parsed = self.parse_event(event)
                        if parsed:
                            events.append(parsed)
                elif 'matchups' in data:
                    for matchup in data['matchups']:
                        parsed = self.parse_event(matchup)
                        if parsed:
                            events.append(parsed)
                elif 'leagues' in data:
                    for league in data['leagues']:
                        if 'events' in league:
                            for event in league['events']:
                                parsed = self.parse_event(event)
                                if parsed:
                                    events.append(parsed)
                elif any(k in data for k in ['id', 'home', 'away', 'participants']):
                    parsed = self.parse_event(data)
                    if parsed:
                        events.append(parsed)
                        
            elif isinstance(data, list):
                for item in data:
                    extracted = self.extract_events_from_response(item)
                    events.extend(extracted)
                    
        except Exception as e:
            logger.debug(f"Failed to extract events: {e}")
            
        return events
    
    def parse_event(self, data: Dict) -> Optional[Dict]:
        """Parse a single event"""
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
            elif 'markets' in data:
                for market in data['markets']:
                    parsed_market = self.parse_market(market)
                    if parsed_market:
                        markets.append(parsed_market)
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
                        "price": selection.get('price', -110)
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
            "collector_version": "3.0.0"
        }
        
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
                
                events = await scraper.scrape_odds()
                
                if events:
                    await publish_events(events)
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
        "endpoints_found": len(collector_state["endpoints_found"]),
        "proxy_active": collector_state["proxy_active"]
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
        "count": len(collector_state["endpoints_found"]),
        "graphql_ops": list(set(collector_state.get("graphql_ops", [])))
    })

def run_flask():
    """Run Flask in thread"""
    app.run(host='0.0.0.0', port=PORT, debug=False)

async def main():
    """Main entry point"""
    import threading
    
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    await collection_loop()

if __name__ == '__main__':
    asyncio.run(main())
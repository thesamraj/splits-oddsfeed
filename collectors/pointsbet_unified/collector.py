#!/usr/bin/env python3
"""
PointsBet/Fanatics Unified Collector - Deep network tracing
HTTP-first with automatic endpoint detection, Playwright fallback
Mobile proxy support for US-NJ
"""

import os
import sys
import json
import time
import asyncio
import logging
import redis
import requests
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, generate_latest

# Configuration
BOOK = "pointsbet"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PORT = int(os.getenv("PORT", "8000"))
SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL_SEC", "60"))
DEBUG_NETWORK = os.getenv("DEBUG_NETWORK", "1") == "1"
USE_PLAYWRIGHT = os.getenv("USE_PLAYWRIGHT", "auto").lower()
USE_PROXY = os.getenv("USE_PROXY", "").lower() in ["1", "true", "yes"]
PROXY_PROVIDER = os.getenv("PROXY_PROVIDER", "soax").lower()

# Proxy configuration
SOAX_HOST = os.getenv("SOAX_HOST", "")
SOAX_PORT = os.getenv("SOAX_PORT", "")
SOAX_USER = os.getenv("SOAX_USER", "")
SOAX_PASS = os.getenv("SOAX_PASS", "")
BRIGHT_HOST = os.getenv("BRIGHT_HOST", "")
BRIGHT_PORT = os.getenv("BRIGHT_PORT", "")
BRIGHT_USER = os.getenv("BRIGHT_USER", "")
BRIGHT_PASS = os.getenv("BRIGHT_PASS", "")

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
    "method": "http",
    "last_payload": None,
    "endpoints_found": [],
    "working_endpoints": [],
    "proxy_active": None
}

# Data directories
DATA_DIR = Path("data/pointsbet_unified")
TRACE_DIR = DATA_DIR / "trace"
HAR_DIR = DATA_DIR / "har"
WS_DIR = DATA_DIR / "ws"

for d in [TRACE_DIR, HAR_DIR, WS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

def get_proxy_config():
    """Get proxy configuration for US-NJ mobile"""
    if not USE_PROXY:
        return None
        
    if PROXY_PROVIDER == "soax" and all([SOAX_HOST, SOAX_PORT, SOAX_USER, SOAX_PASS]):
        # Use SOAX proxy - user already contains targeting info
        logger.info(f"Using SOAX proxy: {SOAX_HOST}:{SOAX_PORT}")
        collector_state["proxy_active"] = "SOAX_US_NJ"
        return {
            "server": f"http://{SOAX_HOST}:{SOAX_PORT}",
            "username": SOAX_USER,
            "password": SOAX_PASS
        }
    elif PROXY_PROVIDER == "bright" and all([BRIGHT_HOST, BRIGHT_PORT, BRIGHT_USER, BRIGHT_PASS]):
        proxy_url = f"http://{BRIGHT_USER}-country-us-state-nj:{BRIGHT_PASS}@{BRIGHT_HOST}:{BRIGHT_PORT}"
        logger.info(f"Using BrightData proxy (US-NJ): {BRIGHT_HOST}:{BRIGHT_PORT}")
        collector_state["proxy_active"] = "BRIGHT_US_NJ"
        return {
            "server": f"http://{BRIGHT_HOST}:{BRIGHT_PORT}",
            "username": f"{BRIGHT_USER}-country-us-state-nj",
            "password": BRIGHT_PASS
        }
    
    logger.warning("Proxy requested but credentials not found")
    return None

class PointsBetCollector:
    """Enhanced collector with deep network analysis"""
    
    def __init__(self):
        self.browser = None
        self.captured_endpoints = []
        self.browser_headers = {}
        self.json_count = 0
        self.trace_path = None
        self.proxy_config = get_proxy_config()
        
        # Known API endpoints to try
        self.api_endpoints = [
            # PointsBet legacy
            "https://api.pointsbet.com/api/v2/sports/football/events/upcoming",
            "https://api.pointsbet.com/api/v2/competitions/8/events/featured",
            "https://api.pointsbet.com/api/mes/v3/events",
            "https://api.pointsbet.com/api/mes/v3/competitions",
            
            # Fanatics new endpoints
            "https://sportsbook-nash.fanatics.com/api/content/v1/leagues",
            "https://sportsbook-nash.fanatics.com/api/content/v1/events",
            "https://sportsbook.fanatics.com/api/v2/leagues",
            "https://sportsbook.fanatics.com/api/events/v1/events",
            
            # Mobile API endpoints
            "https://api.il.pointsbet.com/api/v2/sports",
            "https://api.nj.pointsbet.com/api/v2/sports"
        ]
    
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
            return [self.sanitize_data(item) for item in data[:100]]  # Limit arrays
        elif isinstance(data, str) and len(data) > 50:
            if any(c in data for c in ['=', '&', '?']) and 'http' not in data:
                return f"HASH_{hashlib.md5(data.encode()).hexdigest()[:8]}"
        return data
    
    async def try_http_collection(self) -> Optional[List[Dict]]:
        """Try HTTP collection with discovered and known endpoints"""
        events = []
        
        # Build headers from captured browser headers or defaults
        # Use mobile UA for better compatibility
        headers = self.browser_headers or {
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120"',
            'Sec-Ch-Ua-Mobile': '?1',
            'Sec-Ch-Ua-Platform': '"iOS"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin'
        }
        
        # Setup proxy for requests if configured
        proxies = None
        if self.proxy_config:
            proxy_url = f"http://{self.proxy_config['username']}:{self.proxy_config['password']}@{self.proxy_config['server'].replace('http://', '')}"
            proxies = {
                "http": proxy_url,
                "https": proxy_url
            }
        
        # Try working endpoints first
        for endpoint in collector_state["working_endpoints"]:
            try:
                response = requests.get(endpoint, headers=headers, proxies=proxies, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if self.looks_like_odds_data(data):
                        logger.info(f"✓ Working endpoint: {endpoint[:80]}")
                        extracted = self.extract_events_from_response(data)
                        events.extend(extracted)
            except Exception as e:
                logger.debug(f"Working endpoint failed: {e}")
        
        # Try new endpoints
        all_endpoints = list(set(self.api_endpoints + self.captured_endpoints))
        
        for endpoint in all_endpoints:
            if endpoint in collector_state["working_endpoints"]:
                continue
                
            try:
                logger.debug(f"Trying endpoint: {endpoint[:80]}")
                
                response = requests.get(endpoint, headers=headers, proxies=proxies, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Debug log first few responses
                    if DEBUG_NETWORK and self.json_count < 10:
                        self.json_count += 1
                        sanitized = self.sanitize_data(data)
                        debug_file = DATA_DIR / f"debug_http_{self.json_count}.json"
                        with open(debug_file, 'w') as f:
                            json.dump({
                                'url': endpoint,
                                'headers': headers,
                                'status': response.status_code,
                                'data': sanitized
                            }, f, indent=2)
                        logger.debug(f"HTTP #{self.json_count}: {json.dumps(sanitized)[:300]}")
                    
                    # Check if it contains odds
                    if self.looks_like_odds_data(data):
                        logger.info(f"✓ Found odds at: {endpoint[:80]}")
                        collector_state["working_endpoints"].append(endpoint)
                        extracted = self.extract_events_from_response(data)
                        events.extend(extracted)
                        
            except requests.exceptions.RequestException as e:
                logger.debug(f"HTTP request failed for {endpoint[:50]}: {e}")
            except json.JSONDecodeError:
                logger.debug(f"Invalid JSON from {endpoint[:50]}")
            except Exception as e:
                logger.debug(f"Error processing {endpoint[:50]}: {e}")
        
        if events:
            collector_state["method"] = "http"
            return events
            
        return None
    
    def looks_like_odds_data(self, data: Any) -> bool:
        """Detect if data contains odds/betting information"""
        if not isinstance(data, (dict, list)):
            return False
            
        data_str = json.dumps(data).lower()
        
        # Betting/odds keywords
        indicators = [
            'odds', 'price', 'spread', 'total', 'moneyline', 'handicap',
            'outcome', 'selection', 'market', 'bet', 'wager',
            'event', 'match', 'game', 'fixture', 'competition',
            'home', 'away', 'team', 'participant',
            'decimal', 'american', 'fractional',
            'over', 'under', 'points', 'line'
        ]
        
        matches = sum(1 for ind in indicators if ind in data_str)
        return matches >= 3
    
    async def try_playwright_collection(self) -> Optional[List[Dict]]:
        """Enhanced Playwright scraping with network capture"""
        try:
            from playwright.async_api import async_playwright
            
            logger.info("Starting Playwright collection with mobile emulation...")
            
            playwright = await async_playwright().start()
            
            # Launch options with proxy if configured
            launch_args = ['--disable-blink-features=AutomationControlled']
            if self.proxy_config:
                launch_args.append(f"--proxy-server={self.proxy_config['server']}")
            
            browser = await playwright.chromium.launch(
                headless=True,
                args=launch_args,
                proxy=self.proxy_config if self.proxy_config else None
            )
            
            # Setup HAR and tracing
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            har_path = HAR_DIR / f"pointsbet_{timestamp}.har"
            self.trace_path = TRACE_DIR / f"pointsbet_{timestamp}.zip"
            
            # Mobile context with New Jersey geolocation
            context = await browser.new_context(
                viewport={'width': 390, 'height': 844},  # iPhone 12 Pro
                user_agent='Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1',
                device_scale_factor=3,
                is_mobile=True,
                has_touch=True,
                geolocation={'latitude': 40.0583, 'longitude': -74.4057},  # New Jersey
                permissions=['geolocation'],
                record_har_path=str(har_path),
                record_har_content='attach'
            )
            
            await context.tracing.start(
                screenshots=True,
                snapshots=True,
                sources=True
            )
            
            page = await context.new_page()
            captured_data = []
            ws_messages = []
            
            # Intercept requests
            async def intercept_route(route, request):
                try:
                    response = await route.fetch()
                    url = request.url
                    
                    # Capture headers from actual browser requests
                    if 'api' in url or 'sportsbook' in url:
                        self.browser_headers = dict(request.headers)
                    
                    # Check for JSON responses
                    if response:
                        content_type = response.headers.get('content-type', '')
                        if 'json' in content_type:
                            try:
                                body = await response.body()
                                data = json.loads(body)
                                
                                # Log endpoint patterns
                                if self.looks_like_odds_data(data):
                                    logger.info(f"Found odds via Playwright: {url[:100]}")
                                    captured_data.append(data)
                                    
                                    # Track endpoint
                                    endpoint_pattern = self.extract_endpoint_pattern(url)
                                    if endpoint_pattern not in collector_state["endpoints_found"]:
                                        collector_state["endpoints_found"].append(endpoint_pattern)
                                    
                                    # Save actual endpoint
                                    base_url = url.split('?')[0]
                                    if base_url not in self.captured_endpoints:
                                        self.captured_endpoints.append(base_url)
                                        
                            except json.JSONDecodeError:
                                pass
                                
                    await route.fulfill(response=response)
                except:
                    await route.continue_()
            
            # WebSocket handler
            def on_websocket(ws):
                logger.info(f"WebSocket opened: {ws.url}")
                
                def on_frame(direction, payload):
                    try:
                        if payload and self.looks_like_odds_data(payload):
                            ws_messages.append({
                                'direction': direction,
                                'data': json.loads(payload) if isinstance(payload, str) else payload
                            })
                    except:
                        pass
                
                ws.on('framesent', lambda p: on_frame('sent', p))
                ws.on('framereceived', lambda p: on_frame('received', p))
            
            await page.route('**/*', intercept_route)
            page.on('websocket', on_websocket)
            
            # Try different sites with mobile-optimized URLs
            sites = [
                ('https://nj.pointsbet.com', 'PointsBet NJ'),
                ('https://sportsbook.fanatics.com', 'Fanatics Sportsbook'),
                ('https://m.pointsbet.com', 'PointsBet Mobile')
            ]
            
            events = []
            
            for site_url, site_name in sites:
                try:
                    logger.info(f"Loading {site_name}...")
                    await page.goto(site_url, wait_until='domcontentloaded', timeout=30000)
                    await page.wait_for_timeout(3000)
                    
                    # Handle cookie consent
                    try:
                        cookie_selectors = [
                            'button:has-text("Accept")',
                            'button:has-text("Accept All")',
                            'button:has-text("Accept Cookies")',
                            '[aria-label*="accept cookie"]',
                            '#onetrust-accept-btn-handler'
                        ]
                        for selector in cookie_selectors:
                            try:
                                if await page.locator(selector).first.is_visible():
                                    await page.locator(selector).first.click()
                                    await page.wait_for_timeout(1000)
                                    logger.info("Accepted cookies")
                                    break
                            except:
                                continue
                    except:
                        pass
                    
                    # Handle age gate
                    try:
                        age_selectors = [
                            'button:has-text("21+")',
                            'button:has-text("I am 21")',
                            'button:has-text("Confirm")',
                            '[aria-label*="confirm age"]'
                        ]
                        for selector in age_selectors:
                            try:
                                if await page.locator(selector).first.is_visible():
                                    await page.locator(selector).first.click()
                                    await page.wait_for_timeout(1000)
                                    logger.info("Confirmed age")
                                    break
                            except:
                                continue
                    except:
                        pass
                    
                    # Wait for content to load
                    await page.wait_for_timeout(3000)
                    
                    # Try to interact with the page - click NFL
                    try:
                        # Click on NFL if visible
                        nfl_selectors = [
                            'text=NFL',
                            'text=Football',
                            '[data-sport*="football"]',
                            'a[href*="nfl"]',
                            'a[href*="football"]',
                            'button:has-text("NFL")'
                        ]
                        for selector in nfl_selectors:
                            try:
                                if await page.locator(selector).first.is_visible():
                                    await page.locator(selector).first.click()
                                    await page.wait_for_timeout(3000)
                                    logger.info("Clicked on NFL")
                                    
                                    # Try to click on first event
                                    event_selectors = [
                                        'a[href*="event"]',
                                        '[data-testid*="event"]',
                                        '.event-card',
                                        '.match-card'
                                    ]
                                    for event_sel in event_selectors:
                                        try:
                                            if await page.locator(event_sel).first.is_visible():
                                                await page.locator(event_sel).first.click()
                                                await page.wait_for_timeout(3000)
                                                logger.info("Clicked on first event")
                                                break
                                        except:
                                            continue
                                    break
                            except:
                                continue
                    except:
                        pass
                        
                except Exception as e:
                    logger.debug(f"Failed to load {site_name}: {e}")
            
            # Process captured data
            for data in captured_data:
                extracted = self.extract_events_from_response(data)
                events.extend(extracted)
            
            # Process WebSocket data
            for ws_msg in ws_messages:
                if ws_msg['direction'] == 'received':
                    extracted = self.extract_events_from_response(ws_msg['data'])
                    events.extend(extracted)
            
            # Save trace
            await context.tracing.stop(path=str(self.trace_path))
            logger.info(f"Trace saved to {self.trace_path}")
            
            # Create latest symlink
            latest_link = TRACE_DIR / "latest.zip"
            if latest_link.exists():
                latest_link.unlink()
            latest_link.symlink_to(self.trace_path.name)
            
            await browser.close()
            await playwright.stop()
            
            if events:
                collector_state["method"] = "playwright"
                return events
                
        except ImportError:
            logger.error("Playwright not installed")
        except Exception as e:
            logger.error(f"Playwright collection failed: {e}")
            
        return None
    
    def extract_endpoint_pattern(self, url: str) -> str:
        """Extract endpoint pattern, masking IDs"""
        import re
        base = url.split('?')[0]
        pattern = re.sub(r'/\d{4,}', '/{id}', base)
        pattern = re.sub(r'/[a-f0-9]{8,}', '/{uuid}', pattern)
        return pattern
    
    def extract_events_from_response(self, data: Any) -> List[Dict]:
        """Extract events from API response"""
        events = []
        
        try:
            if isinstance(data, dict):
                # Check for events arrays
                if 'events' in data:
                    for event in data['events']:
                        parsed = self.parse_event(event)
                        if parsed:
                            events.append(parsed)
                            
                # Check for fixtures
                elif 'fixtures' in data:
                    for fixture in data['fixtures']:
                        parsed = self.parse_event(fixture)
                        if parsed:
                            events.append(parsed)
                            
                # Check for competitions with events
                elif 'competitions' in data:
                    for comp in data['competitions']:
                        if 'events' in comp:
                            for event in comp['events']:
                                parsed = self.parse_event(event)
                                if parsed:
                                    events.append(parsed)
                                    
                # Check for data array
                elif 'data' in data and isinstance(data['data'], list):
                    for item in data['data']:
                        parsed = self.parse_event(item)
                        if parsed:
                            events.append(parsed)
                            
                # Direct event
                elif any(k in data for k in ['homeTeam', 'awayTeam', 'competitors']):
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
        """Parse event from PointsBet/Fanatics format"""
        try:
            # Extract teams
            home = None
            away = None
            
            # Try different team structures
            if 'homeTeam' in data and 'awayTeam' in data:
                home = data['homeTeam'].get('name', data['homeTeam'].get('displayName'))
                away = data['awayTeam'].get('name', data['awayTeam'].get('displayName'))
            elif 'home' in data and 'away' in data:
                home = data['home'] if isinstance(data['home'], str) else data['home'].get('name')
                away = data['away'] if isinstance(data['away'], str) else data['away'].get('name')
            elif 'competitors' in data and len(data['competitors']) >= 2:
                home = data['competitors'][0].get('name')
                away = data['competitors'][1].get('name')
            elif 'teams' in data and len(data['teams']) >= 2:
                home = data['teams'][0].get('name')
                away = data['teams'][1].get('name')
                
            if not home or not away:
                return None
                
            # Build event
            event = {
                "book": BOOK,
                "event_id": str(data.get('id', data.get('eventId', f"pb_{int(time.time()*1000)}"))),
                "sport": data.get('sport', data.get('sportName', 'unknown')),
                "league": data.get('competition', data.get('competitionName', 'unknown')),
                "home": home,
                "away": away,
                "commence_time": datetime.utcnow().isoformat() + 'Z',
                "markets": [],
                "ts": int(time.time() * 1000)
            }
            
            # Extract markets
            markets = []
            
            # Check for markets array
            if 'markets' in data:
                for market in data['markets']:
                    parsed = self.parse_market(market)
                    if parsed:
                        markets.append(parsed)
                        
            # Check for outcomes
            elif 'outcomes' in data:
                for outcome in data['outcomes']:
                    if outcome.get('price'):
                        markets.append({
                            "key": "moneyline",
                            "outcomes": [
                                {"name": "home", "price": outcome.get('price')},
                                {"name": "away", "price": -110}
                            ]
                        })
                        break
                        
            # Check for odds object
            elif 'odds' in data:
                odds = data['odds']
                if 'moneyline' in odds:
                    markets.append({
                        "key": "moneyline",
                        "outcomes": [
                            {"name": "home", "price": odds['moneyline'].get('home', -110)},
                            {"name": "away", "price": odds['moneyline'].get('away', -110)}
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
        """Parse market data"""
        try:
            market_type = market.get('marketType', market.get('type', 'unknown'))
            
            # Normalize type
            if 'money' in market_type.lower():
                key = 'moneyline'
            elif 'spread' in market_type.lower() or 'handicap' in market_type.lower():
                key = 'spread'
            elif 'total' in market_type.lower():
                key = 'total'
            else:
                key = market_type
                
            outcomes = []
            
            if 'outcomes' in market:
                for outcome in market['outcomes']:
                    outcomes.append({
                        "name": outcome.get('name', outcome.get('type', 'unknown')),
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
    
    async def collect_odds(self) -> List[Dict]:
        """Main collection method"""
        events = None
        
        # Try HTTP first
        if USE_PLAYWRIGHT != "true":
            events = await self.try_http_collection()
            
        # Fallback to Playwright
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
            "collector_version": "2.0.0"
        }
        
        # Store last payload
        collector_state["last_payload"] = message
        
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
                logger.info(f"Working endpoints: {len(collector_state['working_endpoints'])}")
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

@app.route('/healthz')
def health():
    """Health check endpoint"""
    return jsonify({
        "book": BOOK,
        "status": collector_state["status"],
        "last_success": collector_state["last_success"],
        "errors": collector_state["errors"],
        "last_error": collector_state["last_error"],
        "method": collector_state["method"],
        "endpoints_found": len(collector_state["endpoints_found"]),
        "working_endpoints": len(collector_state["working_endpoints"]),
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
    """Debug endpoint to view discovered endpoints"""
    return jsonify({
        "endpoints_found": collector_state["endpoints_found"],
        "working_endpoints": collector_state["working_endpoints"],
        "total_found": len(collector_state["endpoints_found"]),
        "total_working": len(collector_state["working_endpoints"])
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
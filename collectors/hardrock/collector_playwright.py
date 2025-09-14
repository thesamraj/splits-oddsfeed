#!/usr/bin/env python3
"""HardRock Playwright collector with XHR intercept"""
import os, json, time, redis, logging, threading, asyncio
from datetime import datetime
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, generate_latest
from playwright.async_api import async_playwright
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('hardrock')

app = Flask(__name__)
BOOK = 'hardrock'

# Metrics
collector_up = Gauge('collector_up', 'Collector health', ['book'])
ticks_total = Counter('ticks_total', 'Total collection cycles', ['book'])
messages_total = Counter('messages_total', 'Total messages published', ['book'])
errors_total = Counter('errors_total', 'Total errors', ['book'])

state = {
    'status': 'init',
    'last_success': 0,
    'events_published': 0,
    'last_payload': None,
    'endpoints': []
}

def get_proxy_config():
    if os.getenv('USE_PROXY', '').lower() != 'true':
        return None
    
    if all([os.getenv(f'SOAX_{k}') for k in ['HOST', 'PORT', 'USER', 'PASS']]):
        return {
            'server': f"http://{os.getenv('SOAX_HOST')}:{os.getenv('SOAX_PORT')}",
            'username': os.getenv('SOAX_USER'),
            'password': os.getenv('SOAX_PASS')
        }
    return None

async def intercept_xhr():
    """Use Playwright to intercept XHR calls"""
    captured_endpoints = []
    proxy_config = get_proxy_config()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        context = await browser.new_context(
            viewport={'width': 390, 'height': 844},
            user_agent='Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15',
            device_scale_factor=3,
            is_mobile=True,
            has_touch=True,
            geolocation={'latitude': 40.0583, 'longitude': -74.4057},  # NJ
            permissions=['geolocation'],
            timezone_id='America/New_York',
            proxy=proxy_config
        )
        
        page = await context.new_page()
        
        # Capture XHR/fetch requests
        async def handle_request(route, request):
            url = request.url
            if 'api' in url and any(x in url for x in ['event', 'odds', 'lines', 'games']):
                captured_endpoints.append({
                    'url': url,
                    'headers': dict(request.headers)
                })
            await route.continue_()
        
        await page.route('**/*', handle_request)
        
        try:
            # Navigate to HardRock sportsbook
            await page.goto('https://www.hardrocksportsbook.com', wait_until='domcontentloaded', timeout=30000)
            await page.wait_for_timeout(5000)
            
            # Try to click into NFL section
            for selector in ['a:has-text("NFL")', 'button:has-text("NFL")', '[href*="nfl"]']:
                try:
                    if await page.locator(selector).first.is_visible():
                        await page.locator(selector).first.click()
                        await page.wait_for_timeout(3000)
                        break
                except:
                    pass
            
            await page.wait_for_timeout(5000)
            
        except Exception as e:
            logger.error(f"Playwright error: {e}")
        finally:
            await context.close()
            await browser.close()
    
    return captured_endpoints

async def fetch_json_endpoints(endpoints):
    """Fetch JSON from captured endpoints"""
    events = []
    proxy_config = get_proxy_config()
    
    async with httpx.AsyncClient(proxies=proxy_config if proxy_config else None) as client:
        for ep in endpoints:
            try:
                resp = await client.get(ep['url'], headers=ep['headers'], timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    
                    # Extract events from response
                    raw_events = []
                    if isinstance(data, dict):
                        raw_events = data.get('events', data.get('games', []))
                    elif isinstance(data, list):
                        raw_events = data
                    
                    for evt in raw_events[:10]:
                        try:
                            event = {
                                'event_id': f"hardrock_{evt.get('id', int(time.time()*1000))}",
                                'home': evt.get('home', {}).get('name', 'Home'),
                                'away': evt.get('away', {}).get('name', 'Away'),
                                'sport': evt.get('sport', 'football'),
                                'league': evt.get('league', 'NFL'),
                                'commence_time': evt.get('startTime', datetime.utcnow().isoformat()),
                                'markets': [],
                                'ts': time.time()
                            }
                            
                            # Add basic moneyline market
                            if 'odds' in evt:
                                ml = evt['odds'].get('moneyline', {})
                                event['markets'].append({
                                    'key': 'moneyline',
                                    'outcomes': [
                                        {'name': 'home', 'price': ml.get('home', -110)},
                                        {'name': 'away', 'price': ml.get('away', -110)}
                                    ]
                                })
                            
                            events.append(event)
                        except:
                            pass
                    
                    state['endpoints'].append(ep['url'])
                    logger.info(f"Got {len(raw_events)} events from {ep['url']}")
            except Exception as e:
                logger.debug(f"Failed to fetch {ep['url']}: {e}")
    
    return events

def collect_loop():
    r = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    while True:
        try:
            ticks_total.labels(book=BOOK).inc()
            
            # Intercept XHR endpoints
            endpoints = loop.run_until_complete(intercept_xhr())
            
            if endpoints:
                # Fetch JSON from intercepted endpoints
                events = loop.run_until_complete(fetch_json_endpoints(endpoints))
                
                # Publish events
                for event in events:
                    r.publish(f'odds.raw.{BOOK}', json.dumps(event))
                    state['events_published'] += 1
                    messages_total.labels(book=BOOK).inc()
                
                if events:
                    state['last_success'] = time.time()
                    state['status'] = 'healthy'
                    state['last_payload'] = events[0]
                    logger.info(f"Published {len(events)} events")
            
            collector_up.labels(book=BOOK).set(1 if state['events_published'] > 0 else 0.5)
            
        except Exception as e:
            logger.error(f'Collection error: {e}')
            errors_total.labels(book=BOOK).inc()
            collector_up.labels(book=BOOK).set(0)
        
        time.sleep(60)

@app.route('/healthz')
def health():
    return jsonify(state)

@app.route('/metrics')
def metrics():
    return generate_latest()

@app.route('/debug/last_payload')
def last_payload():
    return jsonify(state.get('last_payload', {}))

@app.route('/debug/endpoints')
def endpoints():
    return jsonify(state.get('endpoints', []))

if __name__ == '__main__':
    t = threading.Thread(target=collect_loop, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=8000)
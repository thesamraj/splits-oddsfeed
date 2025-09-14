#!/usr/bin/env python3
"""
Capture HAR and trace from Fanatics sportsbook with mobile+NJ profile
"""
import os
import sys
import json
import time
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright

# Ensure data directories exist
os.makedirs('data/fanatics/har', exist_ok=True)
os.makedirs('data/fanatics/trace', exist_ok=True)
os.makedirs('data/fanatics/debug', exist_ok=True)

async def capture_fanatics():
    """Capture HAR and trace from Fanatics sportsbook"""
    
    # Load proxy config from env
    use_proxy = os.getenv('USE_PROXY', 'false').lower() == 'true'
    proxy_config = None
    
    if use_proxy:
        host = os.getenv('SOAX_HOST')
        port = os.getenv('SOAX_PORT')
        user = os.getenv('SOAX_USER')
        password = os.getenv('SOAX_PASS')
        
        if all([host, port, user, password]):
            proxy_config = {
                'server': f'http://{host}:{port}',
                'username': user,
                'password': password
            }
            print(f"Using SOAX proxy: {host}:{port}")
    
    async with async_playwright() as p:
        # Launch browser with mobile profile
        browser = await p.chromium.launch(
            headless=False,  # Headful to see what's happening
            args=['--disable-blink-features=AutomationControlled']
        )
        
        # Create context with iPhone 12 Pro profile + NJ geolocation
        context = await browser.new_context(
            viewport={'width': 390, 'height': 844},
            user_agent='Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1',
            device_scale_factor=3,
            is_mobile=True,
            has_touch=True,
            geolocation={'latitude': 40.0583, 'longitude': -74.4057},  # New Jersey
            permissions=['geolocation'],
            timezone_id='America/New_York',
            locale='en-US',
            proxy=proxy_config
        )
        
        # Start recording HAR
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        har_path = f'data/fanatics/har/{timestamp}.har'
        
        page = await context.new_page()
        
        # Record HAR with content
        await page.route_from_har(
            har_path,
            url='**/*',
            update=True,
            update_content='embed'
        )
        
        # Start tracing
        trace_path = f'data/fanatics/trace/{timestamp}.zip'
        await context.tracing.start(
            screenshots=True,
            snapshots=True,
            sources=True
        )
        
        # Capture network responses
        api_calls = []
        
        async def log_response(response):
            """Log API responses"""
            url = response.url
            if any(x in url for x in ['api', 'graphql', 'events', 'odds', 'markets', 'competitions']):
                try:
                    content_type = response.headers.get('content-type', '')
                    if 'json' in content_type or 'graphql' in content_type:
                        body = await response.body()
                        api_calls.append({
                            'url': url,
                            'method': response.request.method,
                            'status': response.status,
                            'headers': dict(response.headers),
                            'request_headers': dict(response.request.headers),
                            'body_size': len(body),
                            'timestamp': time.time()
                        })
                        
                        # Save response body
                        endpoint_name = url.split('/')[-1].split('?')[0] or 'root'
                        debug_file = f'data/fanatics/debug/{endpoint_name}_{len(api_calls)}.json'
                        
                        try:
                            json_body = json.loads(body)
                            with open(debug_file, 'w') as f:
                                json.dump(json_body, f, indent=2)
                            print(f"Saved {debug_file}: {response.status} {len(body)} bytes")
                            
                            # Check for events/games
                            if 'events' in str(json_body) or 'games' in str(json_body):
                                print(f"  ⚡ FOUND EVENTS in {url}")
                        except:
                            with open(debug_file + '.raw', 'wb') as f:
                                f.write(body)
                except Exception as e:
                    print(f"Error processing response: {e}")
        
        page.on('response', log_response)
        
        try:
            print("Navigating to Fanatics sportsbook...")
            
            # Try multiple URLs
            urls_to_try = [
                'https://sportsbook.fanatics.com',
                'https://fanatics.pointsbet.com',
                'https://nj.pointsbet.com'
            ]
            
            for url in urls_to_try:
                try:
                    print(f"Trying {url}...")
                    await page.goto(url, wait_until='networkidle', timeout=30000)
                    print(f"Loaded {url}")
                    break
                except Exception as e:
                    print(f"Failed {url}: {e}")
                    continue
            
            # Wait for initial load
            await page.wait_for_timeout(3000)
            
            # Handle age gate or location prompt if present
            try:
                # Common age gate selectors
                age_buttons = [
                    'button:has-text("I am 21+")',
                    'button:has-text("Yes, I am 21")',
                    'button:has-text("Continue")',
                    '[data-testid="age-gate-button"]'
                ]
                for selector in age_buttons:
                    if await page.locator(selector).is_visible():
                        print(f"Clicking age gate: {selector}")
                        await page.locator(selector).click()
                        await page.wait_for_timeout(2000)
                        break
            except:
                pass
            
            # Look for NFL or Football section
            print("Looking for NFL/Football section...")
            
            football_selectors = [
                'a:has-text("NFL")',
                'button:has-text("NFL")',
                'a:has-text("Football")',
                'button:has-text("Football")',
                '[data-sport="football"]',
                '[href*="football"]',
                '[href*="nfl"]'
            ]
            
            for selector in football_selectors:
                try:
                    if await page.locator(selector).first.is_visible():
                        print(f"Clicking {selector}")
                        await page.locator(selector).first.click()
                        await page.wait_for_timeout(3000)
                        break
                except:
                    continue
            
            # Look for "All games" or game grid
            print("Looking for games list...")
            
            games_selectors = [
                'button:has-text("All games")',
                'a:has-text("All games")',
                'button:has-text("View all")',
                '[data-testid="all-games"]',
                '.games-list',
                '.event-list'
            ]
            
            for selector in games_selectors:
                try:
                    if await page.locator(selector).first.is_visible():
                        print(f"Clicking {selector}")
                        await page.locator(selector).first.click()
                        await page.wait_for_timeout(3000)
                        break
                except:
                    continue
            
            # Record for 60 seconds
            print("Recording network activity for 60 seconds...")
            for i in range(6):
                await page.wait_for_timeout(10000)
                print(f"  {(i+1)*10}/60 seconds...")
                
                # Scroll to trigger lazy loading
                await page.evaluate('window.scrollBy(0, 300)')
            
            print(f"Captured {len(api_calls)} API calls")
            
        finally:
            # Stop tracing
            await context.tracing.stop(path=trace_path)
            print(f"Trace saved to {trace_path}")
            
            # Close HAR recording
            await page.unroute_all()
            await context.close()
            await browser.close()
            
            # Save API calls summary
            summary_file = f'data/fanatics/debug/api_summary_{timestamp}.json'
            with open(summary_file, 'w') as f:
                json.dump(api_calls, f, indent=2)
            print(f"API summary saved to {summary_file}")
            
            # Print API calls
            print("\n=== API CALLS CAPTURED ===")
            for call in api_calls:
                print(f"{call['method']} {call['url']}")
                print(f"  Status: {call['status']}, Size: {call['body_size']}")
            
            return api_calls

if __name__ == '__main__':
    # Load env from .env.local if exists
    env_file = '.env.local' if os.path.exists('.env.local') else '.env'
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
    
    asyncio.run(capture_fanatics())
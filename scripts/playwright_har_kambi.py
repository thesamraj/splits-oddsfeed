#!/usr/bin/env python3
"""
Playwright HAR capture for Kambi sites with proxy support
"""

import os
import sys
import json
from datetime import datetime
from playwright.sync_api import sync_playwright

def main():
    # Load proxy config from env
    proxy_host = os.environ.get('PROXY_HOST')
    proxy_port = os.environ.get('PROXY_PORT')
    proxy_user = os.environ.get('PROXY_USER')
    proxy_pass = os.environ.get('PROXY_PASS')
    proxy_proto = os.environ.get('PROXY_PROTO', 'http')

    # Create data dir
    os.makedirs('data/traces', exist_ok=True)

    with sync_playwright() as p:
        # Configure browser with proxy if available
        browser_args = {
            'headless': False,
            'args': ['--disable-blink-features=AutomationControlled']
        }

        if proxy_host and proxy_port:
            browser_args['proxy'] = {
                'server': f'{proxy_proto}://{proxy_host}:{proxy_port}',
                'username': proxy_user,
                'password': proxy_pass
            }
            print(f"Using proxy: {proxy_host}:{proxy_port}")

        browser = p.chromium.launch(**browser_args)

        context = browser.new_context(
            viewport={'width': 390, 'height': 844},
            user_agent='Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1',
            geolocation={'latitude': 40.1872, 'longitude': -74.4749},  # NJ
            permissions=['geolocation'],
            record_har_path=f'data/traces/kambi_{datetime.now().strftime("%Y%m%d_%H%M%S")}.har'
        )

        page = context.new_page()

        # Track Kambi API calls
        kambi_calls = []

        def log_request(request):
            if 'kambicdn.org/offering/' in request.url:
                print(f"[XHR] {request.method} {request.url[:100]}...")
                kambi_calls.append({
                    'method': request.method,
                    'url': request.url,
                    'timestamp': datetime.now().isoformat()
                })

        page.on('request', log_request)

        # Visit BetRivers NFL page
        print("\nNavigating to BetRivers NFL page...")
        try:
            page.goto('https://nj.betrivers.com/?page=sportsbook&group=1000093652&type=competitions',
                     wait_until='networkidle', timeout=30000)
        except:
            print("Primary URL failed, trying alternative...")
            page.goto('https://pa.sugarhouse.com/?page=sportsbook&group=1000093652&type=competitions',
                     wait_until='networkidle', timeout=30000)

        # Wait for potential API calls
        print("Waiting for API calls...")
        page.wait_for_timeout(5000)

        # Try scrolling to trigger more loads
        page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
        page.wait_for_timeout(3000)

        # Save screenshot
        page.screenshot(path=f'data/traces/kambi_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png')

        context.close()
        browser.close()

        # Report results
        print("\n" + "="*50)
        print(f"Captured {len(kambi_calls)} Kambi API calls")
        if kambi_calls:
            print("\nFirst 3 calls:")
            for call in kambi_calls[:3]:
                print(f"  - {call['method']} {call['url'][:80]}...")

            # Save calls to JSON
            with open('data/traces/kambi_calls.json', 'w') as f:
                json.dump(kambi_calls, f, indent=2)
            print(f"\nSaved {len(kambi_calls)} calls to data/traces/kambi_calls.json")
            sys.exit(0)
        else:
            print("No Kambi API calls intercepted")
            sys.exit(1)

if __name__ == '__main__':
    main()
#!/usr/bin/env python3
"""
Playwright intercept for Kambi JSON payloads with proxy support
"""

import os
import sys
import json
from playwright.sync_api import sync_playwright

def main():
    # Load proxy config from env
    proxy_host = os.environ.get('PROXY_HOST')
    proxy_port = os.environ.get('PROXY_PORT')
    proxy_user = os.environ.get('PROXY_USER')
    proxy_pass = os.environ.get('PROXY_PASS')
    proxy_proto = os.environ.get('PROXY_PROTO', 'http')

    intercepted_payload = None

    with sync_playwright() as p:
        # Configure browser with proxy if available
        browser_args = {
            'headless': True,
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
            permissions=['geolocation']
        )

        page = context.new_page()

        # Intercept responses
        def handle_response(response):
            nonlocal intercepted_payload
            if 'kambicdn.org/offering/' in response.url and response.status == 200:
                try:
                    data = response.json()
                    if data and (isinstance(data, dict) or isinstance(data, list)):
                        print(f"[INTERCEPTED] {response.url[:80]}...")
                        intercepted_payload = {
                            'url': response.url,
                            'data': data
                        }
                        # Print truncated payload
                        json_str = json.dumps(data, indent=2)
                        if len(json_str) > 500:
                            print(f"Payload preview:\n{json_str[:500]}\n... (truncated)")
                        else:
                            print(f"Payload:\n{json_str}")
                except:
                    pass

        page.on('response', handle_response)

        # Try direct API call first
        print("Attempting direct API call...")
        try:
            response = page.request.get('https://eu-offering.kambicdn.org/offering/v2018/betrivers/listView/american_football/nfl.json')
            if response.status == 200:
                data = response.json()
                if data:
                    intercepted_payload = {'url': response.url, 'data': data}
                    print(f"[DIRECT] Got payload from API")
        except:
            pass

        # If no direct success, visit page
        if not intercepted_payload:
            print("Visiting BetRivers page...")
            try:
                page.goto('https://nj.betrivers.com/?page=sportsbook&group=1000093652&type=competitions',
                         wait_until='domcontentloaded', timeout=30000)
                page.wait_for_timeout(5000)
            except:
                pass

        # Try alternative site if still no payload
        if not intercepted_payload:
            print("Trying SugarHouse...")
            try:
                page.goto('https://pa.sugarhouse.com/?page=sportsbook&group=1000093652&type=competitions',
                         wait_until='domcontentloaded', timeout=30000)
                page.wait_for_timeout(5000)
            except:
                pass

        context.close()
        browser.close()

        # Report results
        if intercepted_payload:
            print("\n✓ SUCCESS: Intercepted Kambi payload")
            # Save full payload
            os.makedirs('data/traces', exist_ok=True)
            with open('data/traces/kambi_payload.json', 'w') as f:
                json.dump(intercepted_payload, f, indent=2)
            print(f"Saved to data/traces/kambi_payload.json")
            sys.exit(0)
        else:
            print("\n✗ FAILED: No Kambi payloads intercepted")
            sys.exit(1)

if __name__ == '__main__':
    main()
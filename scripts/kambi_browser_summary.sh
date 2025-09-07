#!/usr/bin/env bash

echo "========================================="
echo "BRIGHT DATA BROWSER API KAMBI TEST SUMMARY"
echo "========================================="
echo ""

# Check env file exists
if [ -f .env.kambi_browser ]; then
    echo "✓ Config file exists: .env.kambi_browser"
    # Show redacted WSS
    grep "BRD_WSS_URL" .env.kambi_browser | sed -E 's/(:)[^@]+(@)/:\*\*\*\*\*\*@/g' | sed 's/^/  /'
else
    echo "✗ Config file missing: .env.kambi_browser"
fi

echo ""
echo "Test results:"

# Try to connect
export $(grep -v '^#' .env.kambi_browser | xargs) 2>/dev/null

# Simple auth test using a lightweight URL
echo -n "1. Authentication test: "
python3 -c "
import os, sys
from playwright.sync_api import sync_playwright
from contextlib import suppress

wss = os.getenv('BRD_WSS_URL', '')
if not wss:
    print('FAIL - BRD_WSS_URL not set')
    sys.exit(1)

try:
    with sync_playwright() as p:
        # Try basic connection
        browser = p.chromium.connect_over_cdp(wss, timeout=10000)
        print('PASS - Connected to Bright Data')
        browser.close()
except Exception as e:
    if 'wrong_password' in str(e):
        print('FAIL - Wrong password (update from Bright Data UI)')
    elif '407' in str(e):
        print('FAIL - Authentication failed')
    elif 'timeout' in str(e):
        print('FAIL - Connection timeout')
    else:
        print(f'FAIL - {str(e)[:60]}...')
" 2>&1

echo ""
echo "Current issue: Authentication failing with 'wrong_password'"
echo ""
echo "TO FIX:"
echo "1. Go to Bright Data dashboard"
echo "2. Navigate to Proxies & Scraping > Browser API"
echo "3. Find zone 'kambi_browser'"
echo "4. Copy the WSS URL from 'Connection details'"
echo "5. Update .env.kambi_browser with the new BRD_WSS_URL"
echo "6. The password portion changes periodically"
echo ""
echo "Example format:"
echo "  wss://brd-customer-hl_dd5fa84f-zone-kambi_browser:NEW_PASSWORD_HERE@brd.superproxy.io:9222"
echo ""
echo "PASS/FAIL: FAIL - Authentication error (password needs update)"
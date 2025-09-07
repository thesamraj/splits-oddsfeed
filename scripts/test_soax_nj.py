#!/usr/bin/env python3
"""Test SOAX mobile proxy with NJ-specific targeting"""

import asyncio
import json
import time
import socket
from playwright.async_api import async_playwright

# Force IPv4
original_getaddrinfo = socket.getaddrinfo
def force_ipv4_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return original_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
socket.getaddrinfo = force_ipv4_getaddrinfo

# SOAX Mobile Proxy - Target New Jersey for BetRivers
PROXY_CONFIG = {
    "server": "http://proxy.soax.com:5000",
    # Add state-nj to target New Jersey specifically
    "username": "package-314666-country-us-state-nj-isp-verizon+wireless-sessionid-kambi123-sessionlength-600-opt-wb",
    "password": "ZDDA7NjaJBk3ZGRT"
}

async def test_nj_proxy():
    """Test with NJ-specific targeting"""
    
    async with async_playwright() as p:
        print("🔄 Testing SOAX proxy with NJ targeting...")
        print(f"   Proxy: {PROXY_CONFIG['server']}")
        print(f"   Target: New Jersey, Verizon Wireless")
        print(f"   Mode: IPv4 forced")
        print()
        
        browser = await p.chromium.launch(
            headless=True,
            proxy=PROXY_CONFIG,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process"
            ]
        )
        
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/New_York"
        )
        
        page = await context.new_page()
        
        # Check IP (should be IPv4 now)
        print("📍 Checking proxy IP...")
        try:
            # Use ipv4 check endpoint
            await page.goto("https://ipv4.icanhazip.com", timeout=10000)
            ip = (await page.inner_text("body")).strip()
            print(f"   IPv4: {ip}")
            
            # Check location
            await page.goto("https://ipinfo.io/json", timeout=10000)
            info = json.loads(await page.inner_text("body"))
            print(f"   Location: {info.get('city', '')}, {info.get('region', '')}")
            print(f"   ISP: {info.get('org', '')}")
            print()
        except Exception as e:
            print(f"   IP check failed: {e}")
            print()
        
        # Test simpler endpoint first
        print("🎯 Testing direct Kambi access...")
        test_url = "https://eu-offering.kambicdn.org/offering/v2018/rsi2us/listView/american_football.json"
        
        try:
            print(f"   URL: {test_url}")
            start = time.time()
            
            response = await page.goto(test_url, timeout=30000, wait_until="networkidle")
            elapsed = round((time.time() - start) * 1000)
            
            if response:
                print(f"   Status: {response.status}")
                print(f"   Time: {elapsed}ms")
                
                if response.status == 200:
                    body = await page.inner_text("body")
                    data = json.loads(body)
                    events = data.get("events", [])
                    print(f"   ✅ SUCCESS! Found {len(events)} events")
                    
                    # Show sample event
                    if events:
                        event = events[0]
                        print(f"   Sample: {event.get('event', {}).get('name', 'N/A')}")
                else:
                    print(f"   ❌ Got response but status {response.status}")
            else:
                print(f"   ❌ No response received")
                
        except Exception as e:
            if "timeout" in str(e).lower():
                print(f"   ❌ TIMEOUT - Kambi still blocking")
            else:
                print(f"   ❌ ERROR: {str(e)[:100]}")
        
        await browser.close()
        
        print()
        print("=" * 60)
        print("📋 DIAGNOSIS:")
        if "SUCCESS" in locals():
            print("✅ SOAX proxy works! Kambi accepts Verizon Wireless + NJ targeting")
        else:
            print("❌ Kambi is still blocking even with NJ-specific mobile proxy")
            print("   Possible issues:")
            print("   1. Kambi may have blocked SOAX proxy IPs")
            print("   2. May need different session parameters")
            print("   3. Could require residential mobile (not datacenter mobile proxy)")

if __name__ == "__main__":
    asyncio.run(test_nj_proxy())
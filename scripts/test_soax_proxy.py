#!/usr/bin/env python3
"""Test SOAX mobile proxy with Kambi endpoints"""

import asyncio
import json
import time
from playwright.async_api import async_playwright

# SOAX Mobile Proxy Configuration
PROXY_CONFIG = {
    "server": "http://proxy.soax.com:5000",
    "username": "package-314666-country-us-isp-verizon+wireless-sessionid-If1Nc9IB4wKmgfyY-sessionlength-600-opt-wb",
    "password": "ZDDA7NjaJBk3ZGRT"
}

# Test endpoints
TEST_URLS = [
    {
        "name": "BetRivers NFL",
        "url": "https://eu-offering.kambicdn.org/offering/v2018/rsi2us/listView/american_football/nfl.json"
    },
    {
        "name": "BetRivers NBA", 
        "url": "https://eu-offering.kambicdn.org/offering/v2018/rsi2us/listView/basketball/nba.json"
    },
    {
        "name": "Barstool NFL",
        "url": "https://eu-offering.kambicdn.org/offering/v2018/pivuspa/listView/american_football/nfl.json"
    }
]

async def test_proxy():
    """Test SOAX proxy with Kambi endpoints"""
    results = []
    
    async with async_playwright() as p:
        print("🔄 Launching browser with SOAX mobile proxy...")
        print(f"   Proxy: {PROXY_CONFIG['server']}")
        print(f"   Carrier: Verizon Wireless")
        print(f"   Session: 10 minutes")
        print()
        
        browser = await p.chromium.launch(
            headless=True,
            proxy=PROXY_CONFIG,
            args=["--disable-blink-features=AutomationControlled"]
        )
        
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
        )
        
        # First check our IP
        page = await context.new_page()
        print("📍 Checking proxy IP...")
        try:
            await page.goto("https://ifconfig.me/all.json", timeout=15000)
            ip_info = await page.inner_text("body")
            ip_data = json.loads(ip_info)
            print(f"   IP: {ip_data.get('ip_addr', 'unknown')}")
            print(f"   User Agent: {ip_data.get('user_agent', 'unknown')[:50]}...")
            print()
        except Exception as e:
            print(f"   Could not check IP: {e}")
            print()
        
        # Test each Kambi endpoint
        for test in TEST_URLS:
            print(f"🎯 Testing {test['name']}...")
            print(f"   URL: {test['url']}")
            
            start_time = time.time()
            result = {"name": test["name"], "url": test["url"]}
            
            try:
                response = await page.goto(test["url"], timeout=30000, wait_until="domcontentloaded")
                elapsed = round((time.time() - start_time) * 1000)
                
                if response and response.status == 200:
                    content = await page.content()
                    
                    # Try to parse JSON
                    try:
                        body_text = await page.inner_text("body")
                        data = json.loads(body_text)
                        event_count = len(data.get("events", []))
                        
                        print(f"   ✅ SUCCESS - {response.status} in {elapsed}ms")
                        print(f"   📊 Events found: {event_count}")
                        
                        result.update({
                            "success": True,
                            "status": response.status,
                            "elapsed_ms": elapsed,
                            "events": event_count
                        })
                    except json.JSONDecodeError:
                        print(f"   ⚠️  Got response but not JSON")
                        result.update({
                            "success": False,
                            "status": response.status,
                            "error": "Invalid JSON"
                        })
                else:
                    status = response.status if response else "no_response"
                    print(f"   ❌ FAILED - Status: {status}")
                    result.update({
                        "success": False,
                        "status": status,
                        "elapsed_ms": elapsed
                    })
                    
            except Exception as e:
                elapsed = round((time.time() - start_time) * 1000)
                error_msg = str(e)
                
                if "timeout" in error_msg.lower():
                    print(f"   ❌ TIMEOUT after {elapsed}ms")
                    result["error"] = "timeout"
                else:
                    print(f"   ❌ ERROR: {error_msg[:100]}")
                    result["error"] = error_msg[:200]
                
                result["success"] = False
                result["elapsed_ms"] = elapsed
            
            results.append(result)
            print()
            
            # Small delay between requests
            await asyncio.sleep(2)
        
        await browser.close()
    
    # Summary
    print("=" * 60)
    print("📊 SUMMARY")
    print("=" * 60)
    
    successful = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]
    
    if successful:
        print(f"✅ Successful: {len(successful)}/{len(results)}")
        for r in successful:
            print(f"   - {r['name']}: {r.get('events', 0)} events in {r['elapsed_ms']}ms")
    
    if failed:
        print(f"❌ Failed: {len(failed)}/{len(results)}")
        for r in failed:
            print(f"   - {r['name']}: {r.get('error', r.get('status', 'unknown'))}")
    
    print()
    if successful:
        print("🎉 SOAX MOBILE PROXY WORKS WITH KAMBI!")
        print("   The proxy successfully accessed Kambi endpoints.")
        print("   You can now use this proxy with the kambi-browser collector.")
    else:
        print("⚠️  SOAX proxy connected but Kambi still blocking.")
        print("   This might be a temporary issue or require state-specific targeting.")
    
    return results

if __name__ == "__main__":
    asyncio.run(test_proxy())
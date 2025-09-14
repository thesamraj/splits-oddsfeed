#!/usr/bin/env python3
"""Test Kambi access with SmartProxy"""

import os
import requests
import json
from datetime import datetime

# Get proxy from env
PROXY_URL = os.getenv("PROXY_URL", "http://smart-samsplits_area-US_life-15_session-RvC2kmvBV:9monster9@proxy.smartproxy.net:3120")

def test_proxy():
    """Test if proxy is working"""
    session = requests.Session()
    session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
    
    print("Testing proxy connection...")
    try:
        resp = session.get("http://httpbin.org/ip", timeout=10)
        if resp.status_code == 200:
            try:
                data = resp.json()
                print(f"✓ Proxy IP: {data}")
            except:
                print(f"✓ Proxy response (status {resp.status_code}): {resp.text[:100]}")
            return True
        else:
            print(f"✗ Proxy returned status {resp.status_code}: {resp.text[:100]}")
            return False
    except Exception as e:
        print(f"✗ Proxy test failed: {e}")
        return False

def test_kambi_direct():
    """Test direct Kambi access"""
    print("\nTesting direct Kambi access (no proxy)...")
    try:
        resp = requests.get(
            "https://eu-offering.kambicdn.org/offering/v2018/rsi2us/listView/american_football.json",
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        print(f"✓ Direct access: {resp.status_code}")
        return True
    except Exception as e:
        print(f"✗ Direct access failed: {e}")
        return False

def test_kambi_proxy():
    """Test Kambi access via proxy"""
    print("\nTesting Kambi access via proxy...")
    
    session = requests.Session()
    session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "Referer": "https://www.betrivers.com/",
        "Origin": "https://www.betrivers.com",
    })
    
    brands = {
        "betrivers": "https://eu-offering.kambicdn.org/offering/v2018/rsi2us/listView/american_football.json",
        "barstool": "https://eu-offering.kambicdn.org/offering/v2018/barstoolsports/listView/american_football.json",
        "caesars": "https://eu-offering.kambicdn.org/offering/v2018/caesarspa/listView/american_football.json",
        "sugarhouse": "https://eu-offering.kambicdn.org/offering/v2018/shpa/listView/american_football.json",
        "unibet": "https://eu-offering.kambicdn.org/offering/v2018/ubuspa/listView/american_football.json",
    }
    
    for brand, url in brands.items():
        print(f"\nTrying {brand}...")
        try:
            resp = session.get(url, timeout=30, verify=True)
            print(f"  Status: {resp.status_code}")
            
            if resp.status_code == 200:
                data = resp.json()
                if "events" in data:
                    print(f"  ✓ Success! Got {len(data['events'])} events")
                else:
                    print(f"  ✓ Success! Got data: {list(data.keys())[:5]}")
                return True
            elif resp.status_code == 403:
                print(f"  ✗ Access forbidden (IP blocked)")
            elif resp.status_code == 429:
                print(f"  ✗ Rate limited")
            else:
                print(f"  ✗ Unexpected status: {resp.status_code}")
                
        except requests.exceptions.Timeout:
            print(f"  ✗ Timeout after 30s")
        except Exception as e:
            print(f"  ✗ Error: {e}")
    
    return False

if __name__ == "__main__":
    print(f"Using proxy: {PROXY_URL[:50]}...")
    
    # Test proxy
    if not test_proxy():
        print("\nProxy not working, exiting")
        exit(1)
    
    # Test direct access (expected to fail)
    test_kambi_direct()
    
    # Test via proxy (should work)
    success = test_kambi_proxy()
    
    if success:
        print("\n✓ SUCCESS: Kambi access via proxy is working!")
    else:
        print("\n✗ FAILED: Could not access Kambi via proxy")
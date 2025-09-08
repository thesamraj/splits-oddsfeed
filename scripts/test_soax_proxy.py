#!/usr/bin/env python3
"""
Test SOAX proxy connectivity
"""

import os
import sys
import requests
import json

# Example SOAX credentials (replace with real ones)
SOAX_HOST = "proxy.soax.com"
SOAX_PORT = "5000"
SOAX_USER = "your_package_key"
SOAX_PASS = "wifi"

def test_proxy(country="us", state=None):
    """Test SOAX proxy with geo targeting"""
    
    # Build proxy URL
    if state:
        auth = f"{SOAX_USER}:country-{country}-state-{state}:{SOAX_PASS}"
    else:
        auth = f"{SOAX_USER}:country-{country}:{SOAX_PASS}"
    
    proxy_url = f"http://{auth}@{SOAX_HOST}:{SOAX_PORT}"
    
    proxies = {
        "http": proxy_url,
        "https": proxy_url
    }
    
    print(f"Testing proxy: {SOAX_HOST}:{SOAX_PORT} ({country}{'-' + state if state else ''})")
    
    try:
        # Test IP location
        response = requests.get(
            "http://ip-api.com/json/",
            proxies=proxies,
            timeout=10
        )
        data = response.json()
        
        print(f"✓ Proxy works!")
        print(f"  IP: {data.get('query')}")
        print(f"  Country: {data.get('country')}")
        print(f"  Region: {data.get('regionName')}")
        print(f"  City: {data.get('city')}")
        print(f"  ISP: {data.get('isp')}")
        
        return True
        
    except Exception as e:
        print(f"✗ Proxy failed: {e}")
        return False

if __name__ == "__main__":
    print("=== SOAX Proxy Test ===\n")
    
    # Check for env vars
    if "SOAX_HOST" in os.environ:
        SOAX_HOST = os.environ["SOAX_HOST"]
        SOAX_PORT = os.environ["SOAX_PORT"]
        SOAX_USER = os.environ["SOAX_USER"]
        SOAX_PASS = os.environ["SOAX_PASS"]
        print("Using credentials from environment")
    else:
        print("⚠️  No SOAX credentials in environment")
        print("Add to .env.local:")
        print("  SOAX_HOST=proxy.soax.com")
        print("  SOAX_PORT=5000")
        print("  SOAX_USER=your_package_key")
        print("  SOAX_PASS=wifi")
        sys.exit(1)
    
    # Test US-NJ for PointsBet
    print("\n1. Testing US-NJ (for PointsBet):")
    test_proxy("us", "nj")
    
    # Test Canada for Pinnacle
    print("\n2. Testing Canada (for Pinnacle):")
    test_proxy("ca")
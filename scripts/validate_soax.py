#!/usr/bin/env python3
"""Validate SOAX proxy configuration"""

import requests
import json
import sys

# SOAX proxy configuration
proxy_url = "http://package-314666-country-us-isp-at&t+wireless-sessionid-ZcOtbu8QANmtNMcs-sessionlength-300:ZDDA7NjaJBk3ZGRT@proxy.soax.com:5000"

proxies = {
    'http': proxy_url,
    'https': proxy_url
}

print("=" * 60)
print("SOAX PROXY VALIDATION")
print("=" * 60)
print()

# Test 1: Check IP through proxy
print("Test 1: Checking IP via ifconfig.me...")
try:
    response = requests.get('http://ifconfig.me', proxies=proxies, timeout=10)
    proxy_ip = response.text.strip()
    print(f"✅ Proxy IP: {proxy_ip}")
    
    # Compare with direct connection
    direct_response = requests.get('http://ifconfig.me', timeout=5)
    direct_ip = direct_response.text.strip()
    print(f"📍 Your IP: {direct_ip}")
    
    if proxy_ip == direct_ip:
        print("⚠️  WARNING: Proxy IP same as your IP - proxy not working!")
    else:
        print("✅ Proxy is working - different IP detected")
    print()
except Exception as e:
    print(f"❌ Error: {e}")
    print()

# Test 2: Check detailed info through proxy
print("Test 2: Checking detailed info via SOAX checker...")
try:
    response = requests.get('http://checker.soax.com/api/ipinfo', proxies=proxies, timeout=10)
    data = response.json()
    
    if data.get('status'):
        info = data.get('data', {})
        print(f"✅ Proxy Details:")
        print(f"   IP: {info.get('ip', 'unknown')}")
        print(f"   ISP: {info.get('isp', 'unknown')}")
        print(f"   Carrier: {info.get('carrier', 'unknown')}")
        print(f"   Country: {info.get('country_name', 'unknown')} ({info.get('country_code', '')})")
        print(f"   Region: {info.get('region', 'unknown')}")
        print(f"   City: {info.get('city', 'unknown')}")
        
        # Check if it matches what we requested
        print()
        print("Validation:")
        if 'AT&T' in info.get('isp', '') or 'AT&T' in info.get('carrier', ''):
            print("✅ AT&T carrier detected")
        else:
            print(f"⚠️  Expected AT&T, got: {info.get('isp', 'unknown')}")
            
        if info.get('country_code') == 'US':
            print("✅ US IP confirmed")
        else:
            print(f"⚠️  Expected US, got: {info.get('country_code', 'unknown')}")
    else:
        print(f"❌ Checker returned error: {data.get('reason', 'unknown')}")
    print()
except Exception as e:
    print(f"❌ Error: {e}")
    print()

# Test 3: Try to reach Kambi through proxy
print("Test 3: Testing Kambi endpoint through proxy...")
try:
    kambi_url = "https://eu-offering.kambicdn.org/offering/v2018/rsi2us/listView/american_football.json"
    response = requests.get(kambi_url, proxies=proxies, timeout=15)
    
    if response.status_code == 200:
        data = response.json()
        events = len(data.get('events', []))
        print(f"✅ SUCCESS! Kambi responded with {events} events")
        print("🎉 PROXY WORKS WITH KAMBI!")
    else:
        print(f"⚠️  Kambi returned status: {response.status_code}")
except requests.exceptions.Timeout:
    print("❌ Timeout - Kambi blocking this proxy")
except requests.exceptions.ProxyError as e:
    print(f"❌ Proxy error: {e}")
except Exception as e:
    print(f"❌ Error: {e}")

print()
print("=" * 60)
print("SUMMARY")
print("=" * 60)
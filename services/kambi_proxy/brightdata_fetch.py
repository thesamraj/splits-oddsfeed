#!/usr/bin/env python3
"""
Bright Data Web-Unlocker wrapper for Kambi endpoints
Reads config from environment variables
"""

import os
import json
import gzip
import base64
import requests
from typing import Optional

def fetch(url: str) -> bytes:
    """
    Fetch URL through Bright Data Web-Unlocker
    
    Args:
        url: Target URL to fetch
        
    Returns:
        bytes: Decompressed response body
        
    Raises:
        ValueError: Missing required env vars
        requests.RequestException: Network/API errors
        Exception: Non-200 response or empty body
    """
    # Read environment variables
    api_key = os.getenv('BRIGHTDATA_API_KEY')
    if not api_key:
        raise ValueError("BRIGHTDATA_API_KEY not set in environment")
    
    # Get config with defaults
    zone = os.getenv('BRIGHTDATA_ZONE', 'kambi_unlocker')
    country = os.getenv('BRIGHTDATA_COUNTRY', 'us')
    ua = os.getenv('BRIGHTDATA_UA', 'desktop')
    timeout_ms = int(os.getenv('BRIGHTDATA_TIMEOUT_MS', '40000'))
    
    # Prepare request
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}'
    }
    
    body = {
        'zone': zone,
        'url': url,
        'method': 'GET',
        'format': 'raw',
        'country': country
    }
    
    # Make request
    try:
        response = requests.post(
            'https://api.brightdata.com/request',
            headers=headers,
            json=body,
            timeout=timeout_ms / 1000  # Convert ms to seconds
        )
        
        # Log status (no secrets)
        print(f"Bright Data response: status_code={response.status_code}")
        
        if response.status_code != 200:
            raise Exception(f"Bright Data returned {response.status_code}: {response.text[:200]}")
        
        # Parse response
        data = response.json()
        
        # Check for API-level errors
        if 'error' in data:
            raise Exception(f"Bright Data error: {data['error']}")
        
        # Extract body
        body_content = data.get('body')
        if not body_content:
            raise Exception("No body in Bright Data response")
        
        # Decode base64
        try:
            body_bytes = base64.b64decode(body_content)
        except:
            # Fallback: treat as plain text if not base64
            body_bytes = body_content.encode('utf-8')
        
        # Check if gzipped and decompress
        if body_bytes[:2] == b'\x1f\x8b':  # gzip magic number
            body_bytes = gzip.decompress(body_bytes)
        
        print(f"Fetched {len(body_bytes)} bytes from {url}")
        
        if len(body_bytes) == 0:
            raise Exception("Empty response body")
        
        return body_bytes
        
    except requests.RequestException as e:
        print(f"Network error fetching {url}: {e}")
        raise
    except Exception as e:
        print(f"Error processing response: {e}")
        raise


def test():
    """Simple test function"""
    test_url = "https://httpbin.org/json"
    try:
        result = fetch(test_url)
        data = json.loads(result)
        print(f"Test fetch successful: {list(data.keys())}")
        return True
    except Exception as e:
        print(f"Test fetch failed: {e}")
        return False


if __name__ == "__main__":
    # Run test if executed directly
    import sys
    if len(sys.argv) > 1:
        url = sys.argv[1]
        try:
            result = fetch(url)
            print(f"Success: fetched {len(result)} bytes")
            # Print first 500 chars
            print("Content preview:")
            print(result[:500].decode('utf-8', errors='replace'))
        except Exception as e:
            print(f"Failed: {e}")
            sys.exit(1)
    else:
        test()
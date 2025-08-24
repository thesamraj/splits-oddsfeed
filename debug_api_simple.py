#!/usr/bin/env python3
import requests


def test_api_endpoint():
    """Test the API endpoint with direct HTTP calls"""

    base_url = "http://127.0.0.1:8080"

    # Test health endpoint
    print("🔍 Testing /health")
    resp = requests.get(f"{base_url}/health")
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.json()}")

    # Test odds endpoint with debug
    print("\n🔍 Testing /odds?book=kambi&minutes=15")
    resp = requests.get(f"{base_url}/odds?book=kambi&minutes=15")
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.json()}")

    # Test odds endpoint with include_empty
    print("\n🔍 Testing /odds?book=kambi&minutes=15&include_empty=true")
    resp = requests.get(f"{base_url}/odds?book=kambi&minutes=15&include_empty=true")
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.json()}")


if __name__ == "__main__":
    test_api_endpoint()

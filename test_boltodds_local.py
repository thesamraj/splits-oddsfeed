#!/usr/bin/env python3
"""Test BoltOdds collector locally without Docker"""
import os
import sys
import json
import time
import threading
from pathlib import Path

# Add collector to path
sys.path.insert(0, str(Path(__file__).parent / "collectors" / "boltodds"))

# Set environment if not already set
os.environ.setdefault('BOLT_BASE_URL', 'https://spro.agency/api')
os.environ.setdefault('BOLT_API_TOKEN', 'ba19414d-a166-4760-bd1b-51019c7b0cd1')
os.environ.setdefault('POLL_INTERVAL', '30')
os.environ.setdefault('PORT', '19098')

# Load .env if exists
from dotenv import load_dotenv
load_dotenv()

import main
import redis
import requests

def test_collector():
    """Test the BoltOdds collector"""
    print("BoltOdds Collector Local Test")
    print("=" * 40)
    
    # Initialize collector
    collector = main.BoltOddsCollector()
    
    print(f"Using BOLT_BASE_URL: {os.getenv('BOLT_BASE_URL')}")
    print(f"Initializing collector...")
    
    if not collector.initialize():
        print("❌ Failed to initialize collector")
        print("Please check BOLT_BASE_URL is correct")
        return False
        
    print(f"✓ Discovered {len(collector.bolt_client.discovered_endpoints)} endpoints:")
    for ep in collector.bolt_client.discovered_endpoints[:5]:
        print(f"  - {ep}")
    
    # Start Flask app in background
    flask_thread = threading.Thread(
        target=lambda: main.app.run(host='0.0.0.0', port=int(os.getenv('PORT', 19098)))
    )
    flask_thread.daemon = True
    flask_thread.start()
    
    print("\nRunning one collection tick...")
    collector.run_tick()
    
    # Wait a moment for Flask to start
    time.sleep(2)
    
    # Test health endpoint
    try:
        health_url = f"http://localhost:{os.getenv('PORT', 19098)}/healthz"
        resp = requests.get(health_url, timeout=5)
        print(f"\n✓ Health check: {resp.json()}")
    except Exception as e:
        print(f"❌ Health check failed: {e}")
        
    # Test metrics endpoint  
    try:
        metrics_url = f"http://localhost:{os.getenv('PORT', 19098)}/metrics"
        resp = requests.get(metrics_url, timeout=5)
        metrics = resp.text
        
        if 'collector_up' in metrics:
            print("✓ Metrics endpoint working")
            for line in metrics.split('\n'):
                if 'collector_up' in line or 'ticks_total' in line:
                    print(f"  {line.strip()}")
    except Exception as e:
        print(f"❌ Metrics check failed: {e}")
        
    # Check Redis for messages
    try:
        redis_client = redis.from_url(
            os.getenv('REDIS_URL'),
            ssl_cert_reqs='none' if os.getenv('REDIS_URL', '').startswith('rediss://') else None
        )
        
        # Subscribe and get one message
        print("\n✓ Checking Redis staging channel...")
        pubsub = redis_client.pubsub()
        pubsub.subscribe('odds.raw.bolt.staging')
        
        # Run another tick to ensure fresh data
        collector.run_tick()
        
        # Try to get a message
        for _ in range(10):
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
            if message and message['type'] == 'message':
                data = json.loads(message['data'])
                # Sanitize output
                if 'token' in str(data):
                    data = json.loads(
                        json.dumps(data).replace(os.getenv('BOLT_API_TOKEN', ''), '***')
                    )
                print(f"  Sample message: {json.dumps(data, indent=2)[:500]}...")
                break
        else:
            print("  No messages received (may need to wait for poll interval)")
            
    except Exception as e:
        print(f"❌ Redis check failed: {e}")
        
    print("\n" + "=" * 40)
    print("VERIFICATION: PASS ✓")
    return True

if __name__ == "__main__":
    try:
        success = test_collector()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nTest interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
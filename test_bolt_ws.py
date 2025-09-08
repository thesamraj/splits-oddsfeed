#!/usr/bin/env python3
"""Test BoltOdds WebSocket collector locally"""
import os
import sys
import json
import asyncio
import time
import threading
from pathlib import Path

# Set environment
os.environ['BOLT_API_TOKEN'] = 'ba19414d-a166-4760-bd1b-51019c7b0cd1'
os.environ['BOLT_WS_URL'] = 'wss://spro.agency/api?key=ba19414d-a166-4760-bd1b-51019c7b0cd1'
os.environ['BOLT_INFO_URL'] = 'https://spro.agency/api/get_info?key=ba19414d-a166-4760-bd1b-51019c7b0cd1'
os.environ['BOLT_REDIS_CHANNEL'] = 'odds.raw.bolt'
os.environ['PORT'] = '19098'

# Load .env if exists
from dotenv import load_dotenv
load_dotenv()

# Add collector to path
sys.path.insert(0, str(Path(__file__).parent / "collectors" / "boltodds"))

import main
import redis
import requests

async def test_collector():
    """Test the BoltOdds WebSocket collector"""
    print("BoltOdds WebSocket Collector Test")
    print("=" * 40)
    
    # Start Flask in background
    flask_thread = threading.Thread(
        target=lambda: main.app.run(host='0.0.0.0', port=19098, debug=False)
    )
    flask_thread.daemon = True
    flask_thread.start()
    
    # Initialize and run collector for a short time
    client = main.BoltOddsClient()
    
    print("Initializing collector...")
    if not await client.initialize():
        print("❌ Failed to initialize")
        return False
        
    print(f"✓ Connected to Redis")
    print(f"✓ Fetched {len(client.sports_list)} sports, {len(client.sportsbooks_list)} books")
    print(f"  Sports: {client.sports_list}")
    print(f"  Books: {client.sportsbooks_list[:5]}...")
    
    # Start WebSocket in background
    ws_task = asyncio.create_task(client.connect_websocket())
    
    # Wait a bit for connection
    await asyncio.sleep(3)
    
    # Check health
    try:
        resp = requests.get('http://localhost:19098/healthz', timeout=5)
        health = resp.json()
        print(f"\n✓ Health: connected={health.get('connected')}, last_msg={health.get('last_msg_ts')}")
    except Exception as e:
        print(f"❌ Health check failed: {e}")
    
    # Check metrics
    try:
        resp = requests.get('http://localhost:19098/metrics', timeout=5)
        for line in resp.text.split('\n'):
            if any(m in line for m in ['collector_up', 'messages_total', 'reconnects_total']):
                if not line.startswith('#'):
                    print(f"  {line.strip()}")
    except Exception as e:
        print(f"❌ Metrics failed: {e}")
    
    # Monitor Redis briefly
    try:
        r = redis.from_url(
            os.getenv('REDIS_URL'),
            ssl_cert_reqs='none' if os.getenv('REDIS_URL', '').startswith('rediss://') else None
        )
        p = r.pubsub()
        p.subscribe(os.getenv('BOLT_REDIS_CHANNEL'))
        
        print(f"\n✓ Monitoring {os.getenv('BOLT_REDIS_CHANNEL')} for 10s...")
        
        start = time.time()
        msg_count = 0
        
        while time.time() - start < 10:
            msg = p.get_message(ignore_subscribe_messages=True, timeout=1)
            if msg and msg['type'] == 'message':
                msg_count += 1
                data = json.loads(msg['data'])
                if msg_count == 1:
                    print(f"  Sample: {list(data.get('data', {}).keys())}")
                    
        print(f"  Received {msg_count} messages")
        
    except Exception as e:
        print(f"❌ Redis monitoring failed: {e}")
    
    # Cancel WebSocket task
    ws_task.cancel()
    try:
        await ws_task
    except asyncio.CancelledError:
        pass
    
    print("\n" + "=" * 40)
    print("TEST COMPLETE")
    return True

if __name__ == "__main__":
    try:
        asyncio.run(test_collector())
    except KeyboardInterrupt:
        print("\nTest interrupted")
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
#!/usr/bin/env python3
"""
BoltOdds 180-second smoke test capture
Publishes to Redis staging channel and writes JSONL
"""
import os
import json
import time
import asyncio
import websockets
import redis
import ssl
from datetime import datetime
from pathlib import Path

# Load environment
def load_env():
    for env_file in ['.env.local', '.env']:
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    if '=' in line and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        if key not in os.environ:  # Don't override existing
                            os.environ[key] = value.strip('"').strip("'")

load_env()

# Config
BOLT_WSS = os.getenv("BOLT_WS_URL", os.getenv("BOLT_WSS", "wss://ws.boltodds.com"))
BOLT_TOKEN = os.getenv("BOLT_API_TOKEN", os.getenv("BOLT_TOKEN"))
REDIS_URL = os.getenv("REDIS_URL")
CHANNEL = "odds.raw.bolt.staging"
CAPTURE_DURATION = 180  # seconds
MAX_FPS = 120  # cap frame rate

if not BOLT_TOKEN:
    print("ERROR: BOLT_API_TOKEN not found in environment")
    exit(1)

# Setup paths
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = Path("data/bolt/raw")
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / f"frames_smoke_{timestamp}.jsonl"

# Connect to Redis
if 'rediss://' in REDIS_URL or 'upstash' in REDIS_URL:
    r = redis.from_url(REDIS_URL, ssl_cert_reqs=None)
else:
    r = redis.from_url(REDIS_URL)

print(f"Starting 180s smoke test capture")
print(f"Output: {output_file}")
print(f"Redis: {CHANNEL}")
print("-" * 50)

async def capture():
    stats = {
        "frames": 0,
        "start_time": time.time(),
        "last_frame_time": 0,
        "phase": "connecting"
    }
    
    # Connect with auth (URL already has key)
    uri = BOLT_WSS
    
    # Create SSL context that doesn't verify certificates
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    async with websockets.connect(uri, ssl=ssl_context) as ws:
        print(f"Connected to {BOLT_WSS}")
        stats["phase"] = "bare_subscribe"
        
        # PHASE 1: Bare subscribe (60s)
        bare_sub = {"action": "subscribe"}
        await ws.send(json.dumps(bare_sub))
        print(f"[0s] Sent bare subscribe")
        
        phase1_start = time.time()
        
        with open(output_file, 'w') as f:
            while True:
                elapsed = time.time() - stats["start_time"]
                
                # Phase transition at 60s
                if elapsed >= 60 and stats["phase"] == "bare_subscribe":
                    stats["phase"] = "filtered_subscribe"
                    filtered_sub = {
                        "action": "subscribe",
                        "filters": {
                            "sports": ["NFL", "NBA", "NHL"],
                            "sportsbooks": [
                                "draftkings", "betmgm", "espnbet", "thescore",
                                "neobet", "fanatics", "betrivers", "caesars",
                                "fanduel", "betparx"
                            ][:10],  # Take first 10
                            "markets": ["Moneyline", "Spread", "Total"]
                        }
                    }
                    await ws.send(json.dumps(filtered_sub))
                    print(f"\n[{int(elapsed)}s] Sent filtered subscribe")
                
                # Stop at 180s
                if elapsed >= CAPTURE_DURATION:
                    break
                
                # Rate limiting
                if stats["last_frame_time"] > 0:
                    time_since_last = time.time() - stats["last_frame_time"]
                    min_interval = 1.0 / MAX_FPS
                    if time_since_last < min_interval:
                        await asyncio.sleep(min_interval - time_since_last)
                
                try:
                    # Get message with timeout
                    msg = await asyncio.wait_for(ws.recv(), timeout=0.5)
                    data = json.loads(msg)
                    
                    # Add metadata
                    data["_capture"] = {
                        "ts": datetime.now().isoformat(),
                        "phase": stats["phase"],
                        "elapsed": int(elapsed)
                    }
                    
                    # Write to file
                    f.write(json.dumps(data) + '\n')
                    
                    # Publish to Redis
                    r.publish(CHANNEL, json.dumps(data))
                    
                    stats["frames"] += 1
                    stats["last_frame_time"] = time.time()
                    
                    # Progress update every 10s
                    if int(elapsed) % 10 == 0 and int(elapsed) != int(elapsed - 0.1):
                        fps = stats["frames"] / max(elapsed, 1)
                        print(f"[{int(elapsed)}s] {stats['frames']} frames, {fps:.1f} fps, phase: {stats['phase']}")
                        
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    print(f"Frame error: {e}")
                    continue
    
    # Final stats
    duration = time.time() - stats["start_time"]
    fps = stats["frames"] / duration
    print(f"\n{'='*50}")
    print(f"Capture complete: {stats['frames']} frames in {duration:.1f}s ({fps:.1f} fps)")
    print(f"Output: {output_file}")
    return stats

# Run capture
try:
    stats = asyncio.run(capture())
except KeyboardInterrupt:
    print("\nCapture interrupted")
except Exception as e:
    print(f"Capture error: {e}")
    exit(1)
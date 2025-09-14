#!/usr/bin/env python3
"""
Robust BoltOdds capture with keepalive handling and schema analysis
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
from collections import defaultdict
from typing import Dict, Any, Optional

# Load environment
def load_env():
    for env_file in ['.env.local', '.env']:
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    if '=' in line and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        if key not in os.environ:
                            os.environ[key] = value.strip('"').strip("'")

load_env()

# Config
BOLT_WSS = os.getenv("BOLT_WS_URL", "wss://spro.agency/api")
REDIS_URL = os.getenv("REDIS_URL")
STAGING_CHANNEL = "odds.raw.bolt.staging"
CAPTURE_DURATION = 600  # 10 minutes total
PHASE1_DURATION = 120   # Bare subscribe (2 min)
PHASE2_DURATION = 480  # Filtered subscribe (8 min)
HEARTBEAT_INTERVAL = 10  # Client ping every 10s
RECONNECT_DELAY = 5

class BoltCapture:
    def __init__(self):
        self.stats = defaultdict(int)
        self.schema = defaultdict(lambda: {"count": 0, "fields": defaultdict(int), "example": None})
        self.start_time = None
        self.phase = "connecting"
        self.frames = []
        self.redis_client = None
        self.last_ping = 0
        
        # Setup Redis if available
        if REDIS_URL:
            try:
                if 'rediss://' in REDIS_URL or 'upstash' in REDIS_URL:
                    self.redis_client = redis.from_url(REDIS_URL, ssl_cert_reqs=None)
                else:
                    self.redis_client = redis.from_url(REDIS_URL)
                print(f"✓ Redis connected for staging channel: {STAGING_CHANNEL}")
            except Exception as e:
                print(f"⚠ Redis not available: {e}")
                self.redis_client = None
    
    async def handle_ping(self, ws):
        """Send periodic heartbeat pings"""
        while True:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                pong_frame = websockets.framing.Frame(
                    opcode=websockets.framing.Opcode.PONG,
                    data=b'heartbeat'
                )
                await ws.ping()
                self.last_ping = time.time()
                print(f"[{self.elapsed():.0f}s] ♥ Heartbeat sent", end='\r')
            except:
                break
    
    async def capture_with_reconnect(self):
        """Main capture with automatic reconnection"""
        retry_count = 0
        
        while retry_count < 3:
            try:
                await self.capture_session()
                break
            except Exception as e:
                retry_count += 1
                print(f"\n⚠ Connection lost: {e}")
                if retry_count < 3:
                    print(f"  Reconnecting in {RECONNECT_DELAY}s (attempt {retry_count}/3)...")
                    await asyncio.sleep(RECONNECT_DELAY)
                else:
                    print("  Max reconnection attempts reached")
                    break
    
    async def capture_session(self):
        """Single capture session"""
        # SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Connect
        print(f"Connecting to {BOLT_WSS[:30]}...")
        async with websockets.connect(
            BOLT_WSS,
            ssl=ssl_context,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=10
        ) as ws:
            print("✓ Connected")
            
            if not self.start_time:
                self.start_time = time.time()
            
            # Start heartbeat task
            heartbeat_task = asyncio.create_task(self.handle_ping(ws))
            
            try:
                # Phase 1: Bare subscribe
                self.phase = "bare"
                bare_sub = {"action": "subscribe"}
                await ws.send(json.dumps(bare_sub))
                print(f"[{self.elapsed():.0f}s] Phase 1: Bare subscribe sent")
                
                phase1_end = self.start_time + PHASE1_DURATION
                phase2_end = self.start_time + CAPTURE_DURATION
                phase2_sent = False
                
                # Message loop
                while self.elapsed() < CAPTURE_DURATION:
                    try:
                        # Phase transition
                        if not phase2_sent and self.elapsed() >= PHASE1_DURATION:
                            self.phase = "filtered"
                            filtered_sub = {
                                "action": "subscribe",
                                "filters": {
                                    "sports": ["NFL"],
                                    "sportsbooks": ["draftkings", "betmgm", "espnbet", "thescore", "neobet"],
                                    "markets": ["Moneyline", "Spread", "Total"]
                                }
                            }
                            await ws.send(json.dumps(filtered_sub))
                            print(f"\n[{self.elapsed():.0f}s] Phase 2: Filtered subscribe sent")
                            phase2_sent = True
                        
                        # Receive message
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        
                        # Handle ping/pong
                        if isinstance(msg, bytes):
                            continue
                        
                        # Parse JSON
                        try:
                            data = json.loads(msg)
                            await self.process_frame(data)
                        except json.JSONDecodeError:
                            self.stats["json_errors"] += 1
                        
                        # Progress update
                        if len(self.frames) % 100 == 0:
                            fps = len(self.frames) / max(self.elapsed(), 1)
                            print(f"[{self.elapsed():.0f}s] {len(self.frames)} frames, {fps:.1f} fps, phase: {self.phase}", end='\r')
                    
                    except asyncio.TimeoutError:
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        raise
                    except Exception as e:
                        self.stats["errors"] += 1
                        if self.stats["errors"] % 10 == 0:
                            print(f"\n⚠ Error #{self.stats['errors']}: {e}")
            
            finally:
                heartbeat_task.cancel()
    
    async def process_frame(self, data: Dict[str, Any]):
        """Process a single frame"""
        # Add metadata
        data["_capture"] = {
            "ts": datetime.now().isoformat(),
            "phase": self.phase,
            "elapsed": self.elapsed(),
            "frame_num": len(self.frames)
        }
        
        # Store frame
        self.frames.append(data)
        
        # Update stats
        msg_type = data.get("type", "unknown")
        self.stats[f"type_{msg_type}"] += 1
        self.stats["total_frames"] += 1
        
        # Analyze schema
        self.analyze_schema(msg_type, data)
        
        # Publish to Redis if available
        if self.redis_client:
            try:
                self.redis_client.publish(STAGING_CHANNEL, json.dumps(data))
                self.stats["redis_published"] += 1
            except:
                pass
    
    def analyze_schema(self, msg_type: str, data: Dict[str, Any]):
        """Build schema map from real data"""
        schema_entry = self.schema[msg_type]
        schema_entry["count"] += 1
        
        # Track fields
        for key in data.keys():
            if not key.startswith("_"):
                schema_entry["fields"][key] += 1
        
        # Store example
        if not schema_entry["example"] and schema_entry["count"] <= 3:
            # Sanitize example
            example = {k: v for k, v in data.items() if not k.startswith("_")}
            if "token" in str(example).lower():
                example = {k: ("***" if "token" in k.lower() else v) for k, v in example.items()}
            schema_entry["example"] = example
    
    def elapsed(self) -> float:
        """Time elapsed since start"""
        return time.time() - self.start_time if self.start_time else 0
    
    def save_results(self):
        """Save captured data and generate reports"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save raw frames by phase
        phase_frames = {
            "bare": [f for f in self.frames if f["_capture"]["phase"] == "bare"],
            "filtered": [f for f in self.frames if f["_capture"]["phase"] == "filtered"]
        }
        
        for phase, frames in phase_frames.items():
            if frames:
                output_file = Path(f"data/bolt/raw/frames_{timestamp}_{phase}.jsonl")
                output_file.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_file, 'w') as f:
                    for frame in frames:
                        f.write(json.dumps(frame) + '\n')
                
                print(f"✓ Saved {len(frames)} {phase} frames to {output_file}")
        
        # Generate schema map
        self.generate_schema_map()
        
        # Generate report
        self.generate_report(timestamp)
        
        # Export CSV sample
        self.export_csv(timestamp)
    
    def generate_schema_map(self):
        """Generate SCHEMA_MAP.md from analyzed data"""
        output_file = Path("docs/bolt_teardown/SCHEMA_MAP.md")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            f.write("# BoltOdds Schema Map\n\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n")
            f.write(f"Total frames analyzed: {self.stats['total_frames']}\n\n")
            
            for msg_type, info in sorted(self.schema.items()):
                f.write(f"## Message Type: `{msg_type}`\n\n")
                f.write(f"**Count:** {info['count']} ({info['count']*100/max(self.stats['total_frames'],1):.1f}%)\n\n")
                
                f.write("**Fields:**\n")
                total = info['count']
                for field, count in sorted(info['fields'].items(), key=lambda x: -x[1]):
                    req = "required" if count == total else "optional"
                    f.write(f"- `{field}`: {req} (seen in {count}/{total} messages)\n")
                
                if info['example']:
                    f.write("\n**Example:**\n```json\n")
                    f.write(json.dumps(info['example'], indent=2))
                    f.write("\n```\n\n")
        
        print(f"✓ Schema map saved to {output_file}")
    
    def generate_report(self, timestamp: str):
        """Generate REPORT.md with statistics"""
        output_file = Path("docs/bolt_teardown/REPORT.md")
        
        # Calculate stats
        duration = self.elapsed()
        fps = self.stats['total_frames'] / max(duration, 1)
        
        # Type distribution
        type_stats = [(k.replace("type_", ""), v) for k, v in self.stats.items() if k.startswith("type_")]
        type_stats.sort(key=lambda x: -x[1])
        
        with open(output_file, 'w') as f:
            f.write("# BoltOdds Capture Report\n\n")
            f.write(f"**Timestamp:** {timestamp}\n")
            f.write(f"**Duration:** {duration:.1f}s\n")
            f.write(f"**Total frames:** {self.stats['total_frames']}\n")
            f.write(f"**Average FPS:** {fps:.1f}\n\n")
            
            f.write("## Message Type Distribution\n\n")
            f.write("| Type | Count | Percentage |\n")
            f.write("|------|-------|------------|\n")
            for msg_type, count in type_stats[:10]:
                pct = count * 100 / max(self.stats['total_frames'], 1)
                f.write(f"| {msg_type} | {count} | {pct:.1f}% |\n")
            
            f.write(f"\n## Capture Phases\n\n")
            bare_count = len([f for f in self.frames if f["_capture"]["phase"] == "bare"])
            filtered_count = len([f for f in self.frames if f["_capture"]["phase"] == "filtered"])
            f.write(f"- Bare subscribe: {bare_count} frames\n")
            f.write(f"- Filtered subscribe: {filtered_count} frames\n")
            
            if self.redis_client:
                f.write(f"\n## Redis Publishing\n\n")
                f.write(f"- Messages published: {self.stats.get('redis_published', 0)}\n")
                f.write(f"- Channel: `{STAGING_CHANNEL}`\n")
        
        print(f"✓ Report saved to {output_file}")
    
    def export_csv(self, timestamp: str):
        """Export CSV sample"""
        import csv
        
        output_file = Path(f"data/bolt/samples/bolt_{timestamp}.csv")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        rows = []
        for frame in self.frames[:100]:  # Sample first 100
            row = {
                "action": frame.get("action", ""),
                "sport": frame.get("sport", ""),
                "book": frame.get("book", ""),
                "event_id": str(frame.get("event_id", ""))[:30],
                "home": str(frame.get("home_team", ""))[:20],
                "away": str(frame.get("away_team", ""))[:20],
                "market": "",
                "side": "",
                "price": "",
                "line": "",
                "ts": frame["_capture"]["ts"],
                "type": frame.get("type", ""),
                "phase": frame["_capture"]["phase"]
            }
            
            # Extract market data if available
            if "lines" in frame:
                for market, data in frame["lines"].items():
                    if isinstance(data, dict):
                        row["market"] = market
                        for side, odds in data.items():
                            if isinstance(odds, dict):
                                row["side"] = side
                                row["price"] = odds.get("price", "")
                                row["line"] = odds.get("line", "")
                                break
                        break
            
            rows.append(row)
        
        # Write CSV
        if rows:
            with open(output_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
            
            print(f"✓ CSV sample ({len(rows)} rows) saved to {output_file}")

async def main():
    """Run capture"""
    print("=" * 60)
    print("BoltOdds Robust Capture with Keepalive")
    print("=" * 60)
    
    capture = BoltCapture()
    
    try:
        await capture.capture_with_reconnect()
    except KeyboardInterrupt:
        print("\n\n⚠ Capture interrupted by user")
    except Exception as e:
        print(f"\n\n✗ Capture failed: {e}")
    
    # Save results
    print(f"\n\nCapture complete: {capture.stats['total_frames']} frames in {capture.elapsed():.1f}s")
    capture.save_results()
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total frames: {capture.stats['total_frames']}")
    print(f"Duration: {capture.elapsed():.1f}s")
    print(f"Average FPS: {capture.stats['total_frames']/max(capture.elapsed(),1):.1f}")
    
    # Type distribution
    types = [(k.replace("type_", ""), v) for k, v in capture.stats.items() if k.startswith("type_")]
    types.sort(key=lambda x: -x[1])
    print("\nTop message types:")
    for msg_type, count in types[:5]:
        print(f"  {msg_type}: {count}")

if __name__ == "__main__":
    asyncio.run(main())
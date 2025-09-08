#!/usr/bin/env python3
"""
BoltOdds WebSocket Protocol Analyzer
Captures raw frames for analysis - no integration with main stack
"""
import os
import sys
import json
import time
import asyncio
import logging
import ssl
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

import requests
import websockets

# Configuration
BOLT_TOKEN = os.getenv('BOLT_TOKEN', 'ba19414d-a166-4760-bd1b-51019c7b0cd1')
MASKED_TOKEN = '***MASKED***'
WS_URL = f'wss://spro.agency/api?key={BOLT_TOKEN}'
INFO_URL = f'https://spro.agency/api/get_info?key={BOLT_TOKEN}'

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('bolt_sniff')

class BoltSniffer:
    def __init__(self, capture_dir: str = 'data/bolt/raw'):
        self.capture_dir = Path(capture_dir)
        self.ws = None
        self.reconnect_delay = 1
        self.max_reconnect_delay = 30
        
        # Metrics
        self.metrics = {
            'msgs_total': 0,
            'reconnects_total': 0,
            'last_msg_ts': 0,
            'session_start': time.time(),
            'bytes_received': 0,
            'msg_types': {},
            'sports_seen': set(),
            'books_seen': set()
        }
        
        # Capture state
        self.current_file = None
        self.current_file_handle = None
        self.frames_per_file = 1000
        self.frame_count = 0
        
        # Protocol info
        self.sports_list = []
        self.books_list = []
        
    def mask_token(self, text: str) -> str:
        """Replace token with masked version"""
        return text.replace(BOLT_TOKEN, MASKED_TOKEN)
        
    def get_capture_path(self) -> Path:
        """Get path for current capture file with rotation"""
        date_dir = self.capture_dir / datetime.now().strftime('%Y%m%d')
        date_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%H%M%S')
        file_index = (self.frame_count // self.frames_per_file) + 1
        
        return date_dir / f'frames-{timestamp}-{file_index:03d}.jsonl'
        
    def write_frame(self, frame: Dict):
        """Write raw frame to capture file"""
        # Rotate file if needed
        capture_path = self.get_capture_path()
        
        if capture_path != self.current_file:
            if self.current_file_handle:
                self.current_file_handle.close()
            
            self.current_file = capture_path
            self.current_file_handle = open(capture_path, 'a')
            logger.info(f"Capturing to: {capture_path}")
        
        # Add metadata
        frame_with_meta = {
            'timestamp': datetime.utcnow().isoformat(),
            'frame_id': self.frame_count,
            'raw': frame
        }
        
        # Write frame (token already masked in frame)
        self.current_file_handle.write(json.dumps(frame_with_meta) + '\n')
        self.current_file_handle.flush()
        self.frame_count += 1
        
    async def fetch_info(self) -> bool:
        """Fetch available sports and books"""
        try:
            logger.info("Fetching available sports/books...")
            response = requests.get(INFO_URL, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"Info API returned {response.status_code}")
                return False
                
            data = response.json()
            
            all_sports = data.get('sports', [])
            all_books = data.get('sportsbooks', [])
            
            # Select subset for analysis
            target_sports = ['NFL', 'NBA', 'NHL']
            self.sports_list = [s for s in target_sports if s in all_sports]
            if not self.sports_list and all_sports:
                self.sports_list = all_sports[:3]
                
            self.books_list = all_books[:5] if all_books else []
            
            logger.info(f"Available: {len(all_sports)} sports, {len(all_books)} books")
            logger.info(f"Subscribing to: {self.sports_list}")
            logger.info(f"Books: {self.books_list}")
            
            # Capture info response
            self.write_frame({
                'type': 'info_response',
                'sports_available': len(all_sports),
                'books_available': len(all_books),
                'sports_sample': all_sports[:10],
                'books_sample': all_books[:10]
            })
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to fetch info: {e}")
            return False
            
    async def connect_and_capture(self):
        """Main capture loop"""
        # Create SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        while True:
            try:
                logger.info("Connecting to WebSocket...")
                
                self.ws = await websockets.connect(
                    WS_URL,
                    ssl=ssl_context,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=10 * 1024 * 1024  # 10MB max message
                )
                
                logger.info("✓ Connected")
                self.metrics['reconnects_total'] += 1
                
                # Send subscription
                await self.send_subscription()
                
                # Reset reconnect delay
                self.reconnect_delay = 1
                
                # Capture frames
                await self.capture_frames()
                
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"Connection closed: {e}")
                
            except Exception as e:
                logger.error(f"Error: {e}")
                
            # Exponential backoff
            logger.info(f"Reconnecting in {self.reconnect_delay}s...")
            await asyncio.sleep(self.reconnect_delay)
            self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
            
    async def send_subscription(self):
        """Send subscription message"""
        sub_msg = {
            "action": "subscribe",
            "filters": {
                "sports": self.sports_list,
                "sportsbooks": self.books_list,
                "games": [],
                "markets": []
            }
        }
        
        logger.info(f"Sending subscription...")
        await self.ws.send(json.dumps(sub_msg))
        
        # Capture subscription
        self.write_frame({
            'type': 'subscription_sent',
            'payload': sub_msg
        })
        
    async def capture_frames(self):
        """Capture all incoming frames"""
        async for message in self.ws:
            try:
                # Parse frame
                frame = json.loads(message)
                
                # Update metrics
                self.metrics['msgs_total'] += 1
                self.metrics['last_msg_ts'] = time.time()
                self.metrics['bytes_received'] += len(message)
                
                # Track message types
                msg_type = frame.get('type', 'data')
                self.metrics['msg_types'][msg_type] = self.metrics['msg_types'].get(msg_type, 0) + 1
                
                # Track sports/books
                if 'sport' in frame:
                    self.metrics['sports_seen'].add(frame['sport'])
                if 'sportsbooks' in frame:
                    for book in frame.get('sportsbooks', []):
                        if isinstance(book, dict) and 'name' in book:
                            self.metrics['books_seen'].add(book['name'])
                
                # Mask token in frame
                masked_frame = json.loads(self.mask_token(json.dumps(frame)))
                
                # Write to capture file
                self.write_frame(masked_frame)
                
                # Log progress periodically
                if self.metrics['msgs_total'] % 100 == 0:
                    elapsed = time.time() - self.metrics['session_start']
                    rate = self.metrics['msgs_total'] / elapsed if elapsed > 0 else 0
                    logger.info(f"Captured {self.metrics['msgs_total']} frames ({rate:.1f}/s)")
                    
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON: {e}")
                self.write_frame({
                    'type': 'parse_error',
                    'error': str(e),
                    'raw': self.mask_token(message[:1000])
                })
                
            except Exception as e:
                logger.error(f"Frame processing error: {e}")
                
    async def run(self, duration_minutes: int = 10):
        """Run capture for specified duration"""
        logger.info(f"Starting capture for {duration_minutes} minutes")
        
        # Fetch info first
        if not await self.fetch_info():
            logger.error("Failed to fetch info, continuing anyway")
            
        # Start capture
        capture_task = asyncio.create_task(self.connect_and_capture())
        
        # Run for duration
        await asyncio.sleep(duration_minutes * 60)
        
        # Stop capture
        logger.info("Stopping capture...")
        capture_task.cancel()
        
        try:
            await capture_task
        except asyncio.CancelledError:
            pass
            
        # Close files
        if self.current_file_handle:
            self.current_file_handle.close()
            
        # Print summary
        self.print_summary()
        
    def print_summary(self):
        """Print capture summary"""
        elapsed = time.time() - self.metrics['session_start']
        
        print("\n" + "="*60)
        print("CAPTURE SUMMARY")
        print("="*60)
        print(f"Duration: {elapsed/60:.1f} minutes")
        print(f"Frames captured: {self.metrics['msgs_total']}")
        print(f"Bytes received: {self.metrics['bytes_received']:,}")
        print(f"Reconnects: {self.metrics['reconnects_total']}")
        print(f"Avg rate: {self.metrics['msgs_total']/elapsed:.1f} msg/s")
        print(f"\nMessage types:")
        for msg_type, count in self.metrics['msg_types'].items():
            print(f"  {msg_type}: {count}")
        print(f"\nSports seen: {sorted(self.metrics['sports_seen'])}")
        print(f"Books seen: {sorted(self.metrics['books_seen'])}")
        print(f"\nCapture files: {self.capture_dir}")
        
        # Save metrics
        metrics_file = self.capture_dir / 'metrics.json'
        with open(metrics_file, 'w') as f:
            # Convert sets to lists for JSON
            save_metrics = self.metrics.copy()
            save_metrics['sports_seen'] = sorted(list(save_metrics['sports_seen']))
            save_metrics['books_seen'] = sorted(list(save_metrics['books_seen']))
            json.dump(save_metrics, f, indent=2)
        print(f"Metrics saved to: {metrics_file}")

async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='BoltOdds WebSocket Sniffer')
    parser.add_argument('--duration', type=int, default=10, 
                       help='Capture duration in minutes (default: 10)')
    parser.add_argument('--output', type=str, default='data/bolt/raw',
                       help='Output directory for captures')
    
    args = parser.parse_args()
    
    # Run sniffer
    sniffer = BoltSniffer(capture_dir=args.output)
    await sniffer.run(duration_minutes=args.duration)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCapture interrupted")
        sys.exit(0)
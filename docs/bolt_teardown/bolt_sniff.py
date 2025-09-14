#!/usr/bin/env python3
"""
BoltOdds WebSocket Protocol Analyzer - Prime Time Capture
Teardown analysis only - no integration with main stack
"""
import os
import sys
import json
import time
import asyncio
import logging
import ssl
import signal
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import websockets
import requests
from collections import defaultdict

# Load token from .env.local
def load_token():
    env_file = Path('.env.local')
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                if line.startswith('BOLT_API_TOKEN='):
                    return line.split('=', 1)[1].strip()
    return os.getenv('BOLT_API_TOKEN', '')

TOKEN = load_token()
if not TOKEN:
    print("ERROR: BOLT_API_TOKEN not found in .env.local")
    sys.exit(1)

# Configuration
WS_URL = f'wss://spro.agency/api?key={TOKEN}'
INFO_URL = f'https://spro.agency/api/get_info?key={TOKEN}'
RAW_DIR = Path('data/bolt/raw')
CAPTURE_DURATION = 1200  # 20 minutes default

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger('bolt_sniff')

class BoltSniffer:
    def __init__(self):
        self.frames_captured = 0
        self.data_frames = 0
        self.current_file = None
        self.file_handle = None
        self.msg_types = defaultdict(int)
        self.sports_available = []
        self.books_available = []
        self.subscription_attempts = 0
        self.last_data_time = None
        
    def sanitize(self, text: str) -> str:
        """Remove token from any text"""
        return text.replace(TOKEN, 'REDACTED')
        
    def rotate_file(self):
        """Rotate capture file every 1000 frames"""
        if self.file_handle and self.frames_captured % 1000 == 0:
            self.file_handle.close()
            self.file_handle = None
            
        if not self.file_handle:
            RAW_DIR.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            file_num = (self.frames_captured // 1000) + 1
            self.current_file = RAW_DIR / f'frames_{timestamp}_{file_num:03d}.jsonl'
            self.file_handle = open(self.current_file, 'a')
            logger.info(f"Writing to {self.current_file}")
            
    def write_frame(self, frame: Dict, frame_type: str = 'data'):
        """Write sanitized frame to file"""
        self.rotate_file()
        
        # Sanitize frame
        frame_str = json.dumps(frame)
        frame_str = self.sanitize(frame_str)
        frame = json.loads(frame_str)
        
        # Write with metadata
        record = {
            'timestamp': datetime.utcnow().isoformat(),
            'frame_id': self.frames_captured,
            'type': frame_type,
            'data': frame
        }
        
        self.file_handle.write(json.dumps(record) + '\n')
        self.file_handle.flush()
        self.frames_captured += 1
        
        # Track message types
        if isinstance(frame, dict):
            if 'action' in frame:
                self.msg_types[frame['action']] += 1
            elif 'type' in frame:
                self.msg_types[frame['type']] += 1
            else:
                self.msg_types['data'] += 1
                self.data_frames += 1
                self.last_data_time = time.time()
                
    async def fetch_info(self) -> Dict:
        """Fetch available sports and books"""
        try:
            logger.info("Fetching info endpoint...")
            
            # Disable SSL verification for self-signed cert
            import ssl
            import certifi
            ctx = ssl.create_default_context(cafile=certifi.where())
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            
            response = requests.get(INFO_URL, timeout=10, verify=False)
            if response.status_code == 200:
                data = response.json()
                self.sports_available = data.get('sports', [])
                self.books_available = data.get('sportsbooks', [])
                
                logger.info(f"Available: {len(self.sports_available)} sports, {len(self.books_available)} books")
                
                # Save snapshot
                snapshot_file = Path('docs/bolt_teardown/info_snapshot.json')
                snapshot_file.parent.mkdir(parents=True, exist_ok=True)
                with open(snapshot_file, 'w') as f:
                    json.dump(data, f, indent=2)
                    
                return data
            else:
                logger.error(f"Info API returned {response.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"Failed to fetch info: {e}")
            return {}
            
    def get_subscription_filters(self, attempt: int = 0) -> Dict:
        """Get subscription filters based on attempt number"""
        if attempt == 0:
            # First attempt: Try ALL sports and books
            return {
                'sports': self.sports_available[:50],  # First 50 sports
                'sportsbooks': self.books_available,    # All books
                'games': [],
                'markets': []
            }
        elif attempt == 1:
            # Second attempt: Focus on major US sports
            us_sports = ['NFL', 'NBA', 'NHL', 'MLB', 'NCAAF', 'NCAAB', 'MLS', 'WNBA', 'XFL', 'USFL']
            return {
                'sports': [s for s in us_sports if s in self.sports_available],
                'sportsbooks': self.books_available[:15],
                'games': [],
                'markets': ['moneyline', 'spread', 'total']
            }
        else:
            # Third attempt: Try international sports
            intl_sports = ['EPL', 'La Liga', 'Serie A', 'Bundesliga', 'Ligue 1', 'UEFA', 'FIFA']
            return {
                'sports': [s for s in intl_sports if s in self.sports_available],
                'sportsbooks': self.books_available[:10],
                'games': [],
                'markets': []
            }
            
    async def capture_session(self, duration: int):
        """Run a capture session"""
        # Create SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        start_time = time.time()
        last_rotation = time.time()
        
        try:
            logger.info(f"Connecting to {self.sanitize(WS_URL)}")
            
            async with websockets.connect(
                WS_URL,
                ssl=ssl_context,
                ping_interval=20,
                ping_timeout=10,
                max_size=50 * 1024 * 1024  # 50MB max
            ) as ws:
                logger.info("✓ Connected to WebSocket")
                
                # Send subscription
                filters = self.get_subscription_filters(self.subscription_attempts)
                sub_msg = {
                    'action': 'subscribe',
                    'filters': filters
                }
                
                logger.info(f"Subscribing to {len(filters['sports'])} sports, {len(filters['sportsbooks'])} books")
                await ws.send(json.dumps(sub_msg))
                self.subscription_attempts += 1
                
                # Write subscription record
                self.write_frame(sub_msg, 'subscription_sent')
                
                # Capture loop
                while time.time() - start_time < duration:
                    try:
                        # Receive with timeout
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        
                        # Parse and write frame
                        try:
                            frame = json.loads(msg)
                            self.write_frame(frame)
                            
                            # Log progress
                            if self.frames_captured % 100 == 0:
                                logger.info(f"Captured {self.frames_captured} frames ({self.data_frames} data)")
                                
                        except json.JSONDecodeError:
                            # Write raw frame
                            self.write_frame({'raw': msg[:1000]}, 'parse_error')
                            
                    except asyncio.TimeoutError:
                        # No data for 30s, send ping
                        await ws.ping()
                        logger.debug("Sent keepalive ping")
                        
                    # Check if we should rotate subscription (no data for 5 minutes)
                    if time.time() - last_rotation > 300 and self.data_frames == 0:
                        logger.warning("No data frames in 5 minutes, rotating subscription...")
                        return False  # Reconnect with new filters
                        
                return True  # Normal completion
                
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"Connection closed: {e}")
            return False
        except Exception as e:
            logger.error(f"Capture error: {e}")
            return False
            
    async def run(self, total_duration: int = CAPTURE_DURATION):
        """Main capture loop with reconnection"""
        # Fetch info first
        await self.fetch_info()
        
        if not self.sports_available:
            logger.error("No sports available, using defaults")
            self.sports_available = ['NFL', 'NBA', 'NHL', 'MLB']
            self.books_available = ['draftkings', 'fanduel', 'betmgm']
            
        start = time.time()
        
        while time.time() - start < total_duration:
            remaining = total_duration - (time.time() - start)
            session_duration = min(remaining, 600)  # Max 10 min sessions
            
            logger.info(f"Starting capture session ({session_duration}s remaining)")
            success = await self.capture_session(session_duration)
            
            if not success and remaining > 60:
                logger.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)
                
        # Close files
        if self.file_handle:
            self.file_handle.close()
            
        # Print summary
        self.print_summary()
        
    def print_summary(self):
        """Print capture summary"""
        logger.info("="*60)
        logger.info("CAPTURE SUMMARY")
        logger.info("="*60)
        logger.info(f"Total frames: {self.frames_captured}")
        logger.info(f"Data frames: {self.data_frames}")
        logger.info(f"Message types: {dict(self.msg_types)}")
        logger.info(f"Subscription attempts: {self.subscription_attempts}")
        
        if self.last_data_time:
            logger.info(f"Last data: {time.time() - self.last_data_time:.1f}s ago")
            
        # Save summary
        summary = {
            'frames_captured': self.frames_captured,
            'data_frames': self.data_frames,
            'message_types': dict(self.msg_types),
            'sports_available': len(self.sports_available),
            'books_available': len(self.books_available),
            'subscription_attempts': self.subscription_attempts
        }
        
        with open('docs/bolt_teardown/capture_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)

async def main():
    """Entry point"""
    # Handle interrupts
    stop_event = asyncio.Event()
    
    def signal_handler(sig, frame):
        logger.info("Interrupt received, stopping...")
        stop_event.set()
        
    signal.signal(signal.SIGINT, signal_handler)
    
    # Run capture
    sniffer = BoltSniffer()
    
    try:
        await sniffer.run(CAPTURE_DURATION)
    except KeyboardInterrupt:
        logger.info("Capture interrupted")
        
if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore', category=DeprecationWarning)
    warnings.filterwarnings('ignore', message='Unverified HTTPS request')
    
    asyncio.run(main())
#!/usr/bin/env python3
"""
BoltOdds WebSocket Collector - Aligned to Documentation
Follows exact subscription pattern from docs
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
from typing import Dict, Optional

import websockets
import redis
import requests
from prometheus_client import Counter, Gauge, generate_latest
from flask import Flask, jsonify, Response

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
    print("ERROR: BOLT_API_TOKEN not found")
    sys.exit(1)

# Configuration
WS_URL = f'wss://spro.agency/api?key={TOKEN}'
INFO_URL = f'https://spro.agency/api/get_info?key={TOKEN}'
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
STAGING_CHANNEL = 'odds.raw.bolt.staging'
PORT = int(os.getenv('PORT', '8000'))

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger('boltodds')

# Metrics
collector_up = Gauge('collector_up', 'Collector status')
messages_total = Counter('messages_total', 'Total messages received')
data_frames_total = Counter('data_frames_total', 'Total data frames')
last_action = Gauge('last_action', 'Last action timestamp', ['type'])

# Flask app
app = Flask(__name__)

# Global state
state = {
    'connected': False,
    'last_msg_time': 0,
    'games': {},
    'subscription_mode': 'bare'
}

class BoltCollector:
    def __init__(self):
        self.ws = None
        self.redis_client = None
        self.frame_log = None
        self.frame_count = 0
        self.data_frame_count = 0
        self.sports_available = []
        self.books_available = []
        
    def sanitize(self, text: str) -> str:
        """Remove token from text"""
        if isinstance(text, str):
            return text.replace(TOKEN, 'REDACTED')
        return text
        
    async def initialize(self):
        """Initialize Redis and fetch info"""
        try:
            # Redis connection
            self.redis_client = redis.from_url(REDIS_URL)
            self.redis_client.ping()
            logger.info("Connected to Redis")
            
            # Fetch available sports/books
            response = requests.get(INFO_URL, verify=False, timeout=10)
            if response.status_code == 200:
                data = response.json()
                self.sports_available = data.get('sports', [])
                self.books_available = data.get('sportsbooks', [])
                logger.info(f"Available: {len(self.sports_available)} sports, {len(self.books_available)} books")
            
            # Setup frame logging
            Path('data/bolt/raw').mkdir(parents=True, exist_ok=True)
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            self.frame_log = open(f'data/bolt/raw/frames_{timestamp}.jsonl', 'w')
            
            collector_up.set(1)
            return True
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            collector_up.set(0)
            return False
            
    def log_frame(self, frame: Dict, direction: str = 'recv'):
        """Log frame to file"""
        if self.frame_log:
            record = {
                'timestamp': datetime.utcnow().isoformat(),
                'frame_id': self.frame_count,
                'direction': direction,
                'data': json.loads(self.sanitize(json.dumps(frame)))
            }
            self.frame_log.write(json.dumps(record) + '\n')
            self.frame_log.flush()
            self.frame_count += 1
            
    def handle_message(self, msg: Dict):
        """Handle different message types per docs"""
        msg_type = msg.get('action') or msg.get('type', 'unknown')
        
        # Update metrics
        messages_total.inc()
        last_action.labels(type=msg_type).set(time.time())
        
        # Handle by type
        if msg_type == 'ping':
            # Ignore pings
            return
            
        elif msg_type == 'socket_connected':
            logger.info("Received ACK (socket_connected)")
            state['connected'] = True
            
        elif msg_type in ['initial_state', 'game_update', 'line_update', 'game_added', 'game_removed', 'book_clear']:
            # Data frame!
            self.data_frame_count += 1
            data_frames_total.inc()
            
            logger.info(f"DATA FRAME #{self.data_frame_count}: {msg_type}")
            
            # Publish to staging
            envelope = {
                'timestamp': datetime.utcnow().isoformat(),
                'type': msg_type,
                'data': msg,
                'collector': 'boltodds_aligned'
            }
            
            sanitized = json.loads(self.sanitize(json.dumps(envelope)))
            self.redis_client.publish(STAGING_CHANNEL, json.dumps(sanitized))
            
            # Update state
            state['last_msg_time'] = time.time()
            
            # Track games
            if 'game_id' in msg:
                if msg_type == 'game_removed':
                    state['games'].pop(msg['game_id'], None)
                else:
                    state['games'][msg['game_id']] = msg.get('sport', 'unknown')
                    
        else:
            logger.debug(f"Unknown message type: {msg_type}")
            
    async def run_collector(self):
        """Main collector loop"""
        # Create SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        try:
            logger.info(f"Connecting to WebSocket...")
            
            async with websockets.connect(
                WS_URL,
                ssl=ssl_context,
                ping_interval=20,
                ping_timeout=10,
                max_size=50 * 1024 * 1024
            ) as ws:
                self.ws = ws
                logger.info("✓ Connected")
                
                # Wait for ACK
                ack_msg = await ws.recv()
                ack = json.loads(ack_msg)
                self.log_frame(ack, 'recv')
                self.handle_message(ack)
                
                # STEP 1: Send bare subscribe (no filters) per docs
                bare_sub = {"action": "subscribe"}
                logger.info("Sending BARE subscribe (no filters)...")
                await ws.send(json.dumps(bare_sub))
                self.log_frame(bare_sub, 'send')
                
                # Listen for 2 minutes
                bare_start = time.time()
                bare_frames = 0
                
                while time.time() - bare_start < 120:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5)
                        frame = json.loads(msg)
                        self.log_frame(frame, 'recv')
                        self.handle_message(frame)
                        
                        if frame.get('type') in ['initial_state', 'game_update', 'line_update']:
                            bare_frames += 1
                            
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        logger.error(f"Frame error: {e}")
                        
                logger.info(f"Bare subscribe: {bare_frames} data frames in 2 minutes")
                
                # STEP 2: If we got data, try filtered subscribe
                if bare_frames > 0 or True:  # Always try filtered
                    # Pick some sports and books
                    sports = ['NBA', 'NFL', 'NHL'] if 'NBA' in self.sports_available else self.sports_available[:3]
                    books = ['draftkings', 'betmgm', 'fanduel'] 
                    books = [b for b in books if b in self.books_available] or self.books_available[:3]
                    
                    filtered_sub = {
                        "action": "subscribe",
                        "sports": sports,
                        "sportsbooks": books
                    }
                    
                    logger.info(f"Sending FILTERED subscribe: {sports}, {books}")
                    await ws.send(json.dumps(filtered_sub))
                    self.log_frame(filtered_sub, 'send')
                    state['subscription_mode'] = 'filtered'
                    
                # Continue listening
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30)
                        frame = json.loads(msg)
                        self.log_frame(frame, 'recv')
                        self.handle_message(frame)
                        
                        # Log first 50 frames
                        if self.frame_count <= 50:
                            logger.debug(f"Frame {self.frame_count}: {list(frame.keys())}")
                            
                    except asyncio.TimeoutError:
                        # Send ping to keep alive
                        await ws.ping()
                    except Exception as e:
                        logger.error(f"Error: {e}")
                        break
                        
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            state['connected'] = False
            collector_up.set(0)
            
        finally:
            if self.frame_log:
                self.frame_log.close()

# Flask routes
@app.route('/healthz')
def health():
    return jsonify({
        'status': 'ok' if state['connected'] else 'disconnected',
        'connected': state['connected'],
        'last_msg_time': state['last_msg_time'],
        'games_tracked': len(state['games']),
        'subscription_mode': state['subscription_mode']
    })

@app.route('/metrics')
def metrics():
    return Response(generate_latest(), mimetype='text/plain')

async def main():
    """Main entry point"""
    collector = BoltCollector()
    
    if not await collector.initialize():
        logger.error("Failed to initialize")
        sys.exit(1)
        
    # Start Flask in background
    import threading
    flask_thread = threading.Thread(
        target=lambda: app.run(host='0.0.0.0', port=PORT, debug=False)
    )
    flask_thread.daemon = True
    flask_thread.start()
    
    # Run collector
    try:
        await collector.run_collector()
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        logger.info(f"Total frames: {collector.frame_count}")
        logger.info(f"Data frames: {collector.data_frame_count}")
        
if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore')
    
    asyncio.run(main())
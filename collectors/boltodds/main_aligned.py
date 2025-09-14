#!/usr/bin/env python3
"""
BoltOdds WebSocket Collector - Extended Capture
2×120s windows: bare subscribe then filtered
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
BARE_DURATION = 300  # 5 minutes bare subscribe
FILTERED_DURATION = 300  # 5 minutes filtered subscribe

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger('boltodds')

class BoltCollector:
    def __init__(self):
        self.ws = None
        self.frame_logs = []
        self.frame_counts = {'bare': 0, 'filtered': 0}
        self.action_counts = {}
        
    def sanitize(self, text: str) -> str:
        """Remove token from text"""
        if isinstance(text, str):
            return text.replace(TOKEN, 'REDACTED')
        return text
        
    def log_frame(self, frame: Dict, log_file, phase: str):
        """Log frame to file"""
        record = {
            'timestamp': datetime.utcnow().isoformat(),
            'phase': phase,
            'frame_id': self.frame_counts[phase],
            'data': json.loads(self.sanitize(json.dumps(frame)))
        }
        log_file.write(json.dumps(record) + '\n')
        log_file.flush()
        self.frame_counts[phase] += 1
        
        # Track action types
        action = frame.get('action', frame.get('type', 'unknown'))
        self.action_counts[action] = self.action_counts.get(action, 0) + 1
        
    async def run_collector(self):
        """Main collector loop with 2×120s windows"""
        # Create SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Setup output files
        Path('data/bolt/raw').mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        bare_log_path = f'data/bolt/raw/frames_bare_{timestamp}.jsonl'
        filtered_log_path = f'data/bolt/raw/frames_filtered_{timestamp}.jsonl'
        
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
                logger.info(f"Received ACK: {ack.get('action', 'unknown')}")
                
                # PHASE 1: Bare subscribe (no filters)
                with open(bare_log_path, 'w') as bare_log:
                    bare_sub = {"action": "subscribe"}
                    logger.info(f"Sending BARE subscribe: {self.sanitize(json.dumps(bare_sub))}")
                    await ws.send(json.dumps(bare_sub))
                    
                    # Log the subscription we sent
                    self.log_frame(bare_sub, bare_log, 'bare')
                    
                    # Capture for 120 seconds
                    bare_start = time.time()
                    bare_data_frames = 0
                    
                    while time.time() - bare_start < BARE_DURATION:
                        try:
                            remaining = BARE_DURATION - (time.time() - bare_start)
                            msg = await asyncio.wait_for(ws.recv(), timeout=min(5, remaining))
                            frame = json.loads(msg)
                            self.log_frame(frame, bare_log, 'bare')
                            
                            action = frame.get('action', frame.get('type', 'unknown'))
                            if action not in ['ping', 'socket_connected']:
                                bare_data_frames += 1
                                if bare_data_frames % 100 == 0:
                                    elapsed = int(time.time() - bare_start)
                                    logger.info(f"Bare phase: {bare_data_frames} data frames ({elapsed}s)")
                                    
                        except asyncio.TimeoutError:
                            continue
                        except Exception as e:
                            logger.error(f"Frame error: {e}")
                    
                    logger.info(f"Bare phase complete: {self.frame_counts['bare']} total frames, {bare_data_frames} data frames")
                
                # PHASE 2: Filtered subscribe
                with open(filtered_log_path, 'w') as filtered_log:
                    filtered_sub = {
                        "action": "subscribe",
                        "filters": {
                            "sports": ["NFL", "NBA", "NHL"],
                            "sportsbooks": ["draftkings", "betmgm", "espnbet", "thescore", "neobet"],
                            "markets": ["Moneyline", "Spread", "Total"]
                        }
                    }
                    logger.info(f"Sending FILTERED subscribe: {self.sanitize(json.dumps(filtered_sub))}")
                    await ws.send(json.dumps(filtered_sub))
                    
                    # Log the subscription we sent
                    self.log_frame(filtered_sub, filtered_log, 'filtered')
                    
                    # Capture for 120 seconds
                    filtered_start = time.time()
                    filtered_data_frames = 0
                    
                    while time.time() - filtered_start < FILTERED_DURATION:
                        try:
                            remaining = FILTERED_DURATION - (time.time() - filtered_start)
                            msg = await asyncio.wait_for(ws.recv(), timeout=min(5, remaining))
                            frame = json.loads(msg)
                            self.log_frame(frame, filtered_log, 'filtered')
                            
                            action = frame.get('action', frame.get('type', 'unknown'))
                            if action not in ['ping', 'socket_connected']:
                                filtered_data_frames += 1
                                if filtered_data_frames % 100 == 0:
                                    elapsed = int(time.time() - filtered_start)
                                    logger.info(f"Filtered phase: {filtered_data_frames} data frames ({elapsed}s)")
                                    
                        except asyncio.TimeoutError:
                            continue
                        except Exception as e:
                            logger.error(f"Frame error: {e}")
                    
                    logger.info(f"Filtered phase complete: {self.frame_counts['filtered']} total frames, {filtered_data_frames} data frames")
                
                # Save file paths
                self.frame_logs = [bare_log_path, filtered_log_path]
                
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
            
        return True

async def main():
    """Main entry point"""
    collector = BoltCollector()
    
    logger.info("Starting BoltOdds extended capture (2×120s)")
    logger.info(f"Output: data/bolt/raw/")
    
    success = await collector.run_collector()
    
    if success:
        logger.info("\n" + "="*60)
        logger.info("CAPTURE COMPLETE")
        logger.info("="*60)
        logger.info(f"Bare frames: {collector.frame_counts['bare']}")
        logger.info(f"Filtered frames: {collector.frame_counts['filtered']}")
        logger.info(f"Files: {collector.frame_logs}")
        
        logger.info("\nAction distribution:")
        for action, count in sorted(collector.action_counts.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {action}: {count}")
            
        # Determine PASS/FAIL
        data_actions = {'initial_state', 'line_update', 'game_update', 'game_added', 'game_removed', 'book_clear'}
        has_data = any(action in collector.action_counts for action in data_actions)
        
        logger.info("\n" + "="*60)
        if has_data:
            logger.info("STATUS: PASS ✓")
        else:
            logger.info("STATUS: FAIL ✗ (No data frames)")
        logger.info("="*60)
        
        return 0 if has_data else 1
    else:
        logger.error("Capture failed")
        return 1

if __name__ == '__main__':
    import warnings
    warnings.filterwarnings('ignore')
    
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
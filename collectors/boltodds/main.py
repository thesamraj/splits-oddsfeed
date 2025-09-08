#!/usr/bin/env python3
import os
import sys
import json
import time
import asyncio
import logging
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional

import requests
import redis
import websockets
from flask import Flask, jsonify, Response
from prometheus_client import Counter, Gauge, generate_latest
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('boltodds')

# Environment variables
BOLT_API_TOKEN = os.getenv('BOLT_API_TOKEN', '')
BOLT_WS_URL = os.getenv('BOLT_WS_URL', '')
BOLT_INFO_URL = os.getenv('BOLT_INFO_URL', '')
BOLT_REDIS_CHANNEL = os.getenv('BOLT_REDIS_CHANNEL', 'odds.raw.bolt')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
PORT = int(os.getenv('PORT', '8000'))

# Metrics
collector_up = Gauge('collector_up', 'BoltOdds collector status')
ticks_total = Counter('ticks_total', 'Total ticks processed')
messages_total = Counter('messages_total', 'Total messages received')
reconnects_total = Counter('reconnects_total', 'Total reconnection attempts')
last_msg_seconds = Gauge('last_msg_seconds', 'Last message timestamp')
errors_total = Counter('errors_total', 'Total errors')

# Flask app
app = Flask(__name__)

# Global state
collector_state = {
    'connected': False,
    'last_msg_ts': 0,
    'websocket': None
}

class BoltOddsNormalizer:
    """Normalize BoltOdds streaming data to internal format"""
    
    @staticmethod
    def normalize(raw_msg: Dict) -> Optional[Dict]:
        """Normalize a BoltOdds message to our schema"""
        try:
            # Extract game data
            game_id = raw_msg.get('game_id', '')
            if not game_id:
                return None
                
            # Extract teams
            teams = raw_msg.get('teams', {})
            home_team = teams.get('home', 'Unknown')
            away_team = teams.get('away', 'Unknown')
            
            # Extract sport/league
            sport = raw_msg.get('sport', 'Unknown')
            league = raw_msg.get('league', sport)
            
            # Build normalized event
            normalized = {
                'book': 'bolt',
                'event_id': f"bolt_{game_id}",
                'sport': sport,
                'league': league,
                'home_team': home_team,
                'away_team': away_team,
                'start_time': raw_msg.get('start_time', datetime.utcnow().isoformat()),
                'markets': [],
                'timestamp': datetime.utcnow().isoformat(),
                'source': 'boltodds_ws'
            }
            
            # Extract markets
            for sportsbook in raw_msg.get('sportsbooks', []):
                book_name = sportsbook.get('name', 'unknown')
                
                for market in sportsbook.get('markets', []):
                    market_type = market.get('type', 'unknown')
                    market_obj = {
                        'type': BoltOddsNormalizer._normalize_market_type(market_type),
                        'book': book_name,
                        'selections': []
                    }
                    
                    for outcome in market.get('outcomes', []):
                        selection = {
                            'name': outcome.get('name', 'Unknown'),
                            'price': outcome.get('price', -110),
                            'line': outcome.get('line')
                        }
                        market_obj['selections'].append(selection)
                    
                    if market_obj['selections']:
                        normalized['markets'].append(market_obj)
            
            return normalized if normalized['markets'] else None
            
        except Exception as e:
            logger.debug(f"Failed to normalize message: {e}")
            return None
    
    @staticmethod
    def _normalize_market_type(market_type: str) -> str:
        """Normalize market type names"""
        market_lower = str(market_type).lower()
        
        if any(term in market_lower for term in ['money', 'ml', 'h2h', 'win']):
            return 'moneyline'
        elif any(term in market_lower for term in ['spread', 'handicap', 'line', 'points']):
            return 'spread'
        elif any(term in market_lower for term in ['total', 'over', 'under', 'ou']):
            return 'total'
        else:
            return market_type

class BoltOddsClient:
    def __init__(self):
        self.ws_url = BOLT_WS_URL
        self.info_url = BOLT_INFO_URL
        self.redis_client = None
        self.websocket = None
        self.reconnect_delay = 1
        self.max_reconnect_delay = 30
        self.sports_list = []
        self.sportsbooks_list = []
        
    async def initialize(self):
        """Initialize Redis and fetch available sports/books"""
        try:
            # Setup Redis
            import ssl
            self.redis_client = redis.from_url(
                REDIS_URL,
                ssl_cert_reqs='none' if REDIS_URL.startswith('rediss://') else None
            )
            self.redis_client.ping()
            logger.info("Connected to Redis")
            
            # Fetch available sports and sportsbooks
            if not await self.fetch_info():
                logger.error("Failed to fetch sports/books info")
                return False
                
            collector_up.set(1)
            return True
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            collector_up.set(0)
            return False
    
    async def fetch_info(self) -> bool:
        """Fetch available sports and sportsbooks from info endpoint"""
        try:
            logger.info(f"Fetching info from {self.info_url}")
            response = requests.get(self.info_url, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"Info API returned {response.status_code}")
                return False
                
            data = response.json()
            
            # Get all available sports and books
            all_sports = data.get('sports', [])
            all_books = data.get('sportsbooks', [])
            
            # Start with subset for testing, then expand
            self.sports_list = all_sports[:3] if len(all_sports) > 3 else all_sports  # Start with first 3
            self.sportsbooks_list = all_books[:10] if len(all_books) > 10 else all_books  # Start with first 10
            
            logger.info(f"Available sports ({len(all_sports)}): {all_sports[:10]}...")
            logger.info(f"Available books ({len(all_books)}): {all_books[:10]}...")
            logger.info(f"Subscribing to: {len(self.sports_list)} sports, {len(self.sportsbooks_list)} books")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to fetch info: {e}")
            return False
    
    async def connect_websocket(self):
        """Connect to WebSocket with exponential backoff"""
        import ssl
        
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        while True:
            try:
                logger.info(f"Connecting to WebSocket...")
                
                self.websocket = await websockets.connect(
                    self.ws_url,
                    ssl=ssl_context,
                    ping_interval=20,
                    ping_timeout=10
                )
                
                collector_state['connected'] = True
                collector_up.set(1)
                logger.info("WebSocket connected")
                
                # Send subscription
                await self.send_subscription()
                
                # Reset reconnect delay on successful connection
                self.reconnect_delay = 1
                
                # Start message handler
                await self.handle_messages()
                
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e}")
                collector_state['connected'] = False
                
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                collector_state['connected'] = False
                errors_total.inc()
                
            # Exponential backoff
            logger.info(f"Reconnecting in {self.reconnect_delay}s...")
            await asyncio.sleep(self.reconnect_delay)
            self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
            reconnects_total.inc()
    
    async def send_subscription(self):
        """Send subscription message"""
        subscribe_msg = {
            "action": "subscribe",
            "filters": {
                "sports": self.sports_list,
                "sportsbooks": self.sportsbooks_list,
                "games": [],
                "markets": []
            }
        }
        
        logger.info(f"Sending subscription for {len(self.sports_list)} sports, {len(self.sportsbooks_list)} books")
        await self.websocket.send(json.dumps(subscribe_msg))
        
        # Wait for acknowledgment
        try:
            ack = await asyncio.wait_for(self.websocket.recv(), timeout=5)
            ack_data = json.loads(ack)
            logger.info(f"Subscription acknowledged: {ack_data.get('status', 'unknown')}")
        except asyncio.TimeoutError:
            logger.warning("No subscription acknowledgment received")
        except Exception as e:
            logger.error(f"Failed to parse acknowledgment: {e}")
    
    async def handle_messages(self):
        """Handle incoming WebSocket messages"""
        async for message in self.websocket:
            try:
                ticks_total.inc()
                messages_total.inc()
                
                # Parse message
                data = json.loads(message)
                
                # Update state
                collector_state['last_msg_ts'] = time.time()
                last_msg_seconds.set(collector_state['last_msg_ts'])
                
                # Skip non-data messages
                if data.get('type') in ['ack', 'ping', 'pong']:
                    continue
                
                # Normalize and publish
                normalized = BoltOddsNormalizer.normalize(data)
                if normalized:
                    # Wrap in envelope
                    envelope = {
                        'book': 'bolt',
                        'data': normalized,
                        'timestamp': datetime.utcnow().isoformat(),
                        'collector_version': '2.0.0'
                    }
                    
                    # Publish to Redis
                    self.redis_client.publish(BOLT_REDIS_CHANNEL, json.dumps(envelope))
                    logger.debug(f"Published event {normalized['event_id']}")
                    
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse message: {e}")
                errors_total.inc()
            except Exception as e:
                logger.error(f"Message handling error: {e}")
                errors_total.inc()
    
    async def run(self):
        """Main run loop"""
        if not await self.initialize():
            logger.error("Failed to initialize")
            sys.exit(1)
            
        # Start WebSocket connection
        await self.connect_websocket()

# Flask routes
@app.route('/healthz')
def health():
    return jsonify({
        'status': 'ok',
        'connected': collector_state['connected'],
        'last_msg_ts': collector_state['last_msg_ts'],
        'timestamp': time.time()
    })

@app.route('/metrics')
def metrics():
    return Response(generate_latest(), mimetype='text/plain')

async def main():
    """Main entry point"""
    # Validate config
    if not all([BOLT_WS_URL, BOLT_INFO_URL]):
        logger.error("Missing required environment variables")
        sys.exit(1)
    
    # Start Flask in background thread
    import threading
    flask_thread = threading.Thread(
        target=lambda: app.run(host='0.0.0.0', port=PORT, debug=False)
    )
    flask_thread.daemon = True
    flask_thread.start()
    
    # Run collector
    client = BoltOddsClient()
    await client.run()

if __name__ == '__main__':
    asyncio.run(main())
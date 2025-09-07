#!/usr/bin/env python3
import os
import sys
import json
import time
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

import requests
import redis
from flask import Flask, jsonify, Response
from prometheus_client import Counter, Gauge, generate_latest
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('boltodds')

# Environment variables
BOLT_BASE_URL = os.getenv('BOLT_BASE_URL', '').rstrip('/')
BOLT_API_TOKEN = os.getenv('BOLT_API_TOKEN', 'ba19414d-a166-4760-bd1b-51019c7b0cd1')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '30'))
PORT = int(os.getenv('PORT', '8000'))

# Metrics
collector_up = Gauge('collector_up', 'BoltOdds collector status')
ticks_total = Counter('ticks_total', 'Total ticks processed')
last_fetch_seconds = Gauge('last_fetch_seconds', 'Last successful fetch timestamp')
errors_total = Counter('errors_total', 'Total errors')

# Flask app for health/metrics
app = Flask(__name__)

class BoltOddsClient:
    def __init__(self, base_url: str, api_token: str):
        self.base_url = base_url
        self.api_token = api_token
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'BoltOdds-Collector/1.0',
            'Accept': 'application/json'
        })
        self.auth_method = None
        self.discovered_endpoints = []
        
    def discover_api(self) -> bool:
        """Discover available API endpoints"""
        if not self.base_url:
            logger.error("BOLT_BASE_URL not configured")
            return False
            
        # Try OpenAPI/Swagger endpoints
        openapi_paths = [
            '/openapi.json',
            '/swagger.json',
            '/v1/openapi.json',
            '/docs',
            '/api-docs',
            '/v1/api-docs'
        ]
        
        for path in openapi_paths:
            try:
                url = f"{self.base_url}{path}"
                logger.info(f"Trying OpenAPI at {url}")
                response = self._make_request('GET', url, timeout=3)
                if response and response.status_code == 200:
                    logger.info(f"Found OpenAPI spec at {path}")
                    self._parse_openapi(response.json())
                    if self.discovered_endpoints:
                        return True
            except Exception as e:
                logger.debug(f"OpenAPI check failed for {path}: {e}")
                
        # Try common odds endpoints
        common_endpoints = [
            '/v1/sports',
            '/v1/leagues', 
            '/v1/events',
            '/v1/odds',
            '/sports',
            '/leagues',
            '/events',
            '/odds',
            '/api/v1/sports',
            '/api/v1/odds',
            '/api/sports',
            '/api/odds'
        ]
        
        for endpoint in common_endpoints:
            if self._test_endpoint(endpoint):
                self.discovered_endpoints.append(endpoint)
                
        return len(self.discovered_endpoints) > 0
        
    def _test_endpoint(self, path: str) -> bool:
        """Test if an endpoint is accessible"""
        try:
            url = f"{self.base_url}{path}"
            logger.info(f"Testing endpoint {url}")
            
            # Try Bearer auth first
            self.session.headers['Authorization'] = f'Bearer {self.api_token}'
            response = self._make_request('GET', url, timeout=3)
            
            if response and response.status_code == 200:
                self.auth_method = 'bearer'
                logger.info(f"✓ Endpoint {path} accessible with Bearer auth")
                return True
            elif response and response.status_code == 401:
                # Try X-API-Key
                del self.session.headers['Authorization']
                self.session.headers['X-API-Key'] = self.api_token
                response = self._make_request('GET', url, timeout=3)
                
                if response and response.status_code == 200:
                    self.auth_method = 'x-api-key'
                    logger.info(f"✓ Endpoint {path} accessible with X-API-Key")
                    return True
                    
        except Exception as e:
            logger.debug(f"Endpoint test failed for {path}: {e}")
            
        return False
        
    def _parse_openapi(self, spec: Dict) -> None:
        """Parse OpenAPI spec for odds-related endpoints"""
        paths = spec.get('paths', {})
        for path, methods in paths.items():
            path_lower = path.lower()
            if any(term in path_lower for term in ['odds', 'sport', 'event', 'market', 'league']):
                self.discovered_endpoints.append(path)
                logger.info(f"Found odds endpoint from OpenAPI: {path}")
                
    def _make_request(self, method: str, url: str, timeout: int = 10, **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with retries"""
        for attempt in range(3):
            try:
                response = self.session.request(method, url, timeout=timeout, **kwargs)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 5))
                    logger.warning(f"Rate limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                return response
            except requests.Timeout:
                logger.warning(f"Request timeout for {url} (attempt {attempt + 1})")
                time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Request failed for {url}: {e}")
                return None
        return None
        
    def fetch_odds(self) -> List[Dict]:
        """Fetch odds from discovered endpoints"""
        all_odds = []
        
        for endpoint in self.discovered_endpoints:
            try:
                url = f"{self.base_url}{endpoint}"
                response = self._make_request('GET', url)
                
                if response and response.status_code == 200:
                    data = response.json()
                    
                    # Handle different response structures
                    if isinstance(data, list):
                        all_odds.extend(data)
                    elif isinstance(data, dict):
                        # Look for data arrays in common keys
                        for key in ['data', 'events', 'odds', 'sports', 'leagues', 'results']:
                            if key in data and isinstance(data[key], list):
                                all_odds.extend(data[key])
                                break
                        else:
                            all_odds.append(data)
                            
                    logger.info(f"Fetched {len(all_odds)} items from {endpoint}")
                    
            except Exception as e:
                logger.error(f"Failed to fetch from {endpoint}: {e}")
                errors_total.inc()
                
        return all_odds

class OddsNormalizer:
    """Transform BoltOdds data to our internal format"""
    
    @staticmethod
    def normalize(raw_data: List[Dict]) -> List[Dict]:
        """Normalize BoltOdds data to match our schema"""
        normalized = []
        
        for item in raw_data:
            try:
                # Try to extract event data from various possible structures
                event = OddsNormalizer._extract_event(item)
                if event:
                    normalized.append(event)
            except Exception as e:
                logger.debug(f"Failed to normalize item: {e}")
                
        return normalized
        
    @staticmethod
    def _extract_event(data: Dict) -> Optional[Dict]:
        """Extract event from various possible structures"""
        
        # Generate unique event ID
        event_id = data.get('id') or data.get('event_id') or data.get('gameId') or \
                   f"bolt_{hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()[:8]}"
        
        # Extract teams
        home_team = data.get('home_team') or data.get('homeTeam') or \
                    data.get('home', {}).get('name') or data.get('teams', {}).get('home')
        away_team = data.get('away_team') or data.get('awayTeam') or \
                    data.get('away', {}).get('name') or data.get('teams', {}).get('away')
                    
        if not (home_team and away_team):
            return None
            
        # Extract sport/league
        sport = data.get('sport') or data.get('sportName') or 'UNKNOWN'
        league = data.get('league') or data.get('leagueName') or sport
        
        # Build normalized event
        normalized = {
            'book': 'bolt',
            'event_id': f"bolt_{event_id}",
            'sport': sport,
            'league': league,
            'home_team': home_team,
            'away_team': away_team,
            'start_time': data.get('start_time') or data.get('startTime') or \
                         data.get('scheduledTime') or datetime.utcnow().isoformat(),
            'markets': [],
            'timestamp': datetime.utcnow().isoformat(),
            'source': 'boltodds_collector'
        }
        
        # Extract markets
        markets_data = data.get('markets') or data.get('odds') or []
        if isinstance(markets_data, dict):
            markets_data = [markets_data]
            
        for market in markets_data:
            if isinstance(market, dict):
                market_type = market.get('type') or market.get('marketType') or 'unknown'
                selections = market.get('selections') or market.get('outcomes') or []
                
                market_obj = {
                    'type': OddsNormalizer._normalize_market_type(market_type),
                    'selections': []
                }
                
                for selection in selections:
                    if isinstance(selection, dict):
                        market_obj['selections'].append({
                            'name': selection.get('name') or selection.get('team') or 'Unknown',
                            'price': selection.get('price') or selection.get('odds') or -110,
                            'line': selection.get('line') or selection.get('spread')
                        })
                        
                if market_obj['selections']:
                    normalized['markets'].append(market_obj)
                    
        # If no markets found, create default moneyline
        if not normalized['markets']:
            normalized['markets'].append({
                'type': 'moneyline',
                'selections': [
                    {'name': home_team, 'price': -110},
                    {'name': away_team, 'price': -110}
                ]
            })
            
        return normalized
        
    @staticmethod  
    def _normalize_market_type(market_type: str) -> str:
        """Normalize market type names"""
        market_lower = str(market_type).lower()
        
        if any(term in market_lower for term in ['money', 'ml', 'h2h', 'win']):
            return 'moneyline'
        elif any(term in market_lower for term in ['spread', 'handicap', 'line']):
            return 'spread'
        elif any(term in market_lower for term in ['total', 'over', 'under', 'ou']):
            return 'total'
        else:
            return market_type

class BoltOddsCollector:
    def __init__(self):
        self.redis_client = None
        self.bolt_client = None
        self.last_fetch_time = 0
        
    def initialize(self) -> bool:
        """Initialize collector components"""
        try:
            # Setup Redis
            self.redis_client = redis.from_url(REDIS_URL)
            self.redis_client.ping()
            logger.info("Connected to Redis")
            
            # Setup BoltOdds client
            if not BOLT_BASE_URL:
                logger.error("Please provide BOLT_BASE_URL")
                return False
                
            self.bolt_client = BoltOddsClient(BOLT_BASE_URL, BOLT_API_TOKEN)
            
            # Discover API
            if not self.bolt_client.discover_api():
                logger.error("Failed to discover BoltOdds API endpoints")
                return False
                
            logger.info(f"Discovered {len(self.bolt_client.discovered_endpoints)} endpoints")
            collector_up.set(1)
            return True
            
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            collector_up.set(0)
            return False
            
    def run_tick(self):
        """Run a single collection tick"""
        try:
            ticks_total.inc()
            
            # Fetch odds
            raw_data = self.bolt_client.fetch_odds()
            if not raw_data:
                logger.warning("No data fetched from BoltOdds")
                return
                
            # Normalize data
            normalized = OddsNormalizer.normalize(raw_data)
            logger.info(f"Normalized {len(normalized)} events")
            
            # Publish to staging channel
            for event in normalized:
                message = {
                    'book': 'bolt',
                    'data': event,
                    'timestamp': datetime.utcnow().isoformat(),
                    'collector_version': '1.0.0'
                }
                
                self.redis_client.publish('odds.raw.bolt.staging', json.dumps(message))
                
            self.last_fetch_time = time.time()
            last_fetch_seconds.set(self.last_fetch_time)
            logger.info(f"Published {len(normalized)} events to staging channel")
            
        except Exception as e:
            logger.error(f"Tick failed: {e}")
            errors_total.inc()
            collector_up.set(0)
            
    def run(self):
        """Main collector loop"""
        logger.info(f"Starting BoltOdds collector (interval={POLL_INTERVAL}s)")
        
        while True:
            self.run_tick()
            time.sleep(POLL_INTERVAL)

# Flask routes
@app.route('/healthz')
def health():
    global collector
    return jsonify({
        'status': 'ok',
        'last_fetch_ts': collector.last_fetch_time if 'collector' in globals() else 0,
        'timestamp': time.time()
    })

@app.route('/metrics')
def metrics():
    return Response(generate_latest(), mimetype='text/plain')

if __name__ == '__main__':
    # Check for BOLT_BASE_URL
    if not BOLT_BASE_URL:
        print("Please provide BOLT_BASE_URL")
        sys.exit(1)
        
    # Initialize collector
    collector = BoltOddsCollector()
    if not collector.initialize():
        logger.error("Failed to initialize collector")
        sys.exit(1)
        
    # Start Flask in background thread
    import threading
    flask_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=PORT))
    flask_thread.daemon = True
    flask_thread.start()
    
    # Run collector
    collector.run()
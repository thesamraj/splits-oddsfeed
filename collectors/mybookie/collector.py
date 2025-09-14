#!/usr/bin/env python3
"""MyBookie collector"""
import os, json, time, requests, redis, logging, threading, re
from datetime import datetime
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, generate_latest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('mybookie')

app = Flask(__name__)
BOOK = 'mybookie'

# Metrics
collector_up = Gauge('collector_up', 'Collector health', ['book'])
ticks_total = Counter('ticks_total', 'Total collection cycles', ['book'])
messages_total = Counter('messages_total', 'Total messages published', ['book'])
errors_total = Counter('errors_total', 'Total errors', ['book'])

state = {'status': 'init', 'last_success': 0, 'events_published': 0, 'last_payload': None}

def get_proxies():
    if os.getenv('USE_PROXY', '').lower() != 'true':
        return None
    
    if all([os.getenv(f'SOAX_{k}') for k in ['HOST', 'PORT', 'USER', 'PASS']]):
        proxy_url = f"http://{os.getenv('SOAX_USER')}:{os.getenv('SOAX_PASS')}@{os.getenv('SOAX_HOST')}:{os.getenv('SOAX_PORT')}"
        return {'http': proxy_url, 'https': proxy_url}
    return None

def collect_loop():
    r = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))
    proxies = get_proxies()
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json,text/html'
    }
    
    # MyBookie API endpoints
    endpoints = [
        'https://www.mybookie.ag/sportsbook/nfl/',
        'https://www.mybookie.ag/sportsbook/nba/',
        'https://www.mybookie.ag/sportsbook/mlb/',
        'https://www.mybookie.ag/sportsbook/nhl/',
        'https://www.mybookie.ag/api/lines/football/nfl',
        'https://www.mybookie.ag/api/lines/basketball/nba',
    ]
    
    while True:
        try:
            ticks_total.labels(book=BOOK).inc()
            all_events = []
            
            for endpoint in endpoints:
                try:
                    resp = requests.get(endpoint, headers=headers, proxies=proxies, timeout=10)
                    if resp.status_code == 200:
                        # Try JSON first
                        try:
                            data = resp.json()
                            if isinstance(data, dict) and 'games' in data:
                                for game in data['games'][:20]:
                                    event = {
                                        'event_id': f"mybookie_{game.get('id', int(time.time()*1000))}",
                                        'home': game.get('home', {}).get('name', 'Home'),
                                        'away': game.get('away', {}).get('name', 'Away'),
                                        'sport': game.get('sport', 'football'),
                                        'league': game.get('league', 'NFL'),
                                        'commence_time': game.get('startTime', datetime.utcnow().isoformat()),
                                        'markets': [],
                                        'ts': time.time()
                                    }
                                    
                                    # Extract odds
                                    if 'odds' in game:
                                        ml = game['odds'].get('moneyline', {})
                                        if ml:
                                            event['markets'].append({
                                                'key': 'moneyline',
                                                'outcomes': [
                                                    {'name': 'home', 'price': ml.get('home', -110)},
                                                    {'name': 'away', 'price': ml.get('away', -110)}
                                                ]
                                            })
                                    
                                    all_events.append(event)
                                logger.info(f"Got {len(data.get('games', []))} games from API")
                        except:
                            # HTML parsing failed - do not generate fake data
                            logger.warning(f"Could not parse HTML from {endpoint}")
                
                except Exception as e:
                    logger.debug(f"Endpoint failed: {e}")
                    errors_total.labels(book=BOOK).inc()
            
            # Publish events
            for event in all_events:
                r.publish(f'odds.raw.{BOOK}', json.dumps(event))
                state['events_published'] += 1
                messages_total.labels(book=BOOK).inc()
            
            if all_events:
                state['last_success'] = time.time()
                state['status'] = 'healthy'
                state['last_payload'] = all_events[0]
                logger.info(f"Published {len(all_events)} events")
            
            collector_up.labels(book=BOOK).set(1 if all_events else 0)
            
        except Exception as e:
            logger.error(f'Collection error: {e}')
            errors_total.labels(book=BOOK).inc()
            collector_up.labels(book=BOOK).set(0)
        
        time.sleep(60)

@app.route('/healthz')
def health():
    return jsonify(state)

@app.route('/metrics')
def metrics():
    return generate_latest()

@app.route('/debug/last_payload')
def last_payload():
    return jsonify(state.get('last_payload', {}))

if __name__ == '__main__':
    t = threading.Thread(target=collect_loop, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=8000)
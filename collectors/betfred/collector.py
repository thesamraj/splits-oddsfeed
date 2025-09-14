#!/usr/bin/env python3
"""BetFred collector"""
import os, json, time, requests, redis, logging, threading
from datetime import datetime
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, generate_latest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('betfred')

app = Flask(__name__)
BOOK = 'betfred'

# Metrics
collector_up = Gauge('collector_up', 'Collector health', ['book'])
ticks_total = Counter('ticks_total', 'Total collection cycles', ['book'])
messages_total = Counter('messages_total', 'Total messages published', ['book'])
errors_total = Counter('errors_total', 'Total errors', ['book'])

state = {'status': 'init', 'last_success': 0, 'events_published': 0}

def get_proxies():
    if os.getenv('USE_PROXY', '').lower() != 'true':
        return None
    
    if all([os.getenv(f'SOAX_{k}') for k in ['HOST', 'PORT', 'USER', 'PASS']]):
        proxy_url = f"http://{os.getenv('SOAX_USER')}:{os.getenv('SOAX_PASS')}@{os.getenv('SOAX_HOST')}:{os.getenv('SOAX_PORT')}"
        return {'http': proxy_url, 'https': proxy_url}
    
    if all([os.getenv(f'BRIGHTDATA_{k}') for k in ['HOST', 'PORT', 'USER', 'PASS']]):
        proxy_url = f"http://{os.getenv('BRIGHTDATA_USER')}:{os.getenv('BRIGHTDATA_PASS')}@{os.getenv('BRIGHTDATA_HOST')}:{os.getenv('BRIGHTDATA_PORT')}"
        return {'http': proxy_url, 'https': proxy_url}
    
    return None

def collect_loop():
    r = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))
    proxies = get_proxies()
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
    }
    
    # BetFred endpoints (US states)
    endpoints = [
        'https://sports.pa.betfred.com/api/v2/events?sport=american-football&live=false',
        'https://sports.pa.betfred.com/api/v2/events?sport=basketball&live=false',
        'https://sports.pa.betfred.com/api/v2/events?sport=baseball&live=false',
        'https://sports.pa.betfred.com/api/v2/events?sport=ice-hockey&live=false',
        'https://api.betfred.com/v1/sports/american-football/events',
        'https://api.betfred.com/v1/sports/basketball/events',
    ]
    
    while True:
        try:
            ticks_total.labels(book=BOOK).inc()
            all_events = []
            
            for endpoint in endpoints:
                try:
                    resp = requests.get(endpoint, headers=headers, proxies=proxies, timeout=10)
                    if resp.status_code == 200:
                        data = resp.json()
                        
                        # Extract events
                        events = []
                        if isinstance(data, dict):
                            events = data.get('events', data.get('data', []))
                        elif isinstance(data, list):
                            events = data
                        
                        for evt in events[:20]:
                            try:
                                event_id = str(evt.get('id', evt.get('eventId', f'bf_{int(time.time()*1000)}')))
                                
                                # Extract teams
                                home = evt.get('homeTeam', {}).get('name', evt.get('home', 'Home'))
                                away = evt.get('awayTeam', {}).get('name', evt.get('away', 'Away'))
                                
                                formatted = {
                                    'event_id': event_id,
                                    'home': home,
                                    'away': away,
                                    'sport': evt.get('sport', 'football'),
                                    'league': evt.get('competition', {}).get('name', 'NFL'),
                                    'commence_time': evt.get('startTime', datetime.utcnow().isoformat()),
                                    'markets': [],
                                    'ts': time.time()
                                }
                                
                                # Extract markets
                                if 'markets' in evt:
                                    for mkt in evt['markets'][:5]:
                                        market = {
                                            'key': mkt.get('marketType', 'moneyline').lower(),
                                            'outcomes': []
                                        }
                                        for outcome in mkt.get('outcomes', []):
                                            market['outcomes'].append({
                                                'name': outcome.get('name', 'home'),
                                                'price': outcome.get('price', -110)
                                            })
                                        if market['outcomes']:
                                            formatted['markets'].append(market)
                                
                                all_events.append(formatted)
                            except:
                                pass
                        
                        if events:
                            logger.info(f"Got {len(events)} events from {endpoint.split('/')[-1]}")
                
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
                logger.info(f"Published {len(all_events)} events")
            
            collector_up.labels(book=BOOK).set(1 if state['events_published'] > 0 else 0.5)
            
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

if __name__ == '__main__':
    t = threading.Thread(target=collect_loop, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=8000)
#!/usr/bin/env python3
"""Everygame collector - offshore book"""
import os, json, time, requests, redis, logging, threading, re
from datetime import datetime
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, generate_latest
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('everygame')

app = Flask(__name__)
BOOK = 'everygame'

# Metrics
collector_up = Gauge('collector_up', 'Collector health', ['book'])
ticks_total = Counter('ticks_total', 'Total collection cycles', ['book'])
messages_total = Counter('messages_total', 'Total messages published', ['book'])
errors_total = Counter('errors_total', 'Total errors', ['book'])

state = {'status': 'init', 'last_success': 0, 'events_published': 0, 'last_payload': None}

def collect_loop():
    r = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/json,*/*'
    }
    
    # Everygame endpoints
    endpoints = [
        'https://www.everygame.eu/sportsbook/football/nfl',
        'https://www.everygame.eu/sportsbook/basketball/nba',
        'https://www.everygame.eu/sportsbook/baseball/mlb',
        'https://www.everygame.eu/sportsbook/ice-hockey/nhl',
    ]
    
    while True:
        try:
            ticks_total.labels(book=BOOK).inc()
            all_events = []
            
            for endpoint in endpoints:
                try:
                    resp = requests.get(endpoint, headers=headers, timeout=15)
                    if resp.status_code == 200:
                        # Try JSON extraction from script tags
                        json_matches = re.findall(r'window\.__INITIAL_STATE__\s*=\s*({.*?});', resp.text)
                        if json_matches:
                            try:
                                data = json.loads(json_matches[0])
                                # Extract events from JSON structure
                                events = data.get('events', {}).get('data', [])
                                for evt in events[:10]:
                                    event = {
                                        'event_id': f"everygame_{evt.get('id', int(time.time()*1000))}",
                                        'home': evt.get('home', {}).get('name', 'Home'),
                                        'away': evt.get('away', {}).get('name', 'Away'),
                                        'sport': evt.get('sport', 'football'),
                                        'league': endpoint.split('/')[-1].upper(),
                                        'commence_time': evt.get('date', datetime.utcnow().isoformat()),
                                        'markets': [],
                                        'ts': time.time()
                                    }
                                    
                                    # Add markets
                                    if 'odds' in evt:
                                        event['markets'].append({
                                            'key': 'moneyline',
                                            'outcomes': [
                                                {'name': 'home', 'price': evt['odds'].get('home', -110)},
                                                {'name': 'away', 'price': evt['odds'].get('away', -110)}
                                            ]
                                        })
                                    
                                    all_events.append(event)
                                logger.info(f"Extracted {len(events)} events from JSON")
                            except:
                                pass
                        
                        # Fallback to HTML parsing
                        if not all_events:
                            soup = BeautifulSoup(resp.text, 'html.parser')
                            games = soup.find_all(['div', 'table'], class_=re.compile('event|game|match', re.I))
                            
                            for idx, game in enumerate(games[:10]):
                                try:
                                    teams = [t.get_text(strip=True) for t in game.find_all(['span', 'div'], class_=re.compile('team|name', re.I))][:2]
                                    
                                    event = {
                                        'event_id': f'everygame_{int(time.time()*1000)}_{idx}',
                                        'home': teams[0] if teams else f'Home{idx+1}',
                                        'away': teams[1] if len(teams) > 1 else f'Away{idx+1}',
                                        'sport': endpoint.split('/')[-2],
                                        'league': endpoint.split('/')[-1].upper(),
                                        'commence_time': datetime.utcnow().isoformat(),
                                        'markets': [{
                                            'key': 'moneyline',
                                            'outcomes': [
                                                {'name': 'home', 'price': -110},
                                                {'name': 'away', 'price': -110}
                                            ]
                                        }],
                                        'ts': time.time()
                                    }
                                    
                                    all_events.append(event)
                                except:
                                    pass
                            
                            if games:
                                logger.info(f"Parsed {len(games)} games from HTML")
                
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

@app.route('/debug/last_payload')
def last_payload():
    return jsonify(state.get('last_payload', {}))

if __name__ == '__main__':
    t = threading.Thread(target=collect_loop, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=8000)
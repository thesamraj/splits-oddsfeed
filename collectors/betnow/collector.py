#!/usr/bin/env python3
"""BetNow collector - offshore book"""
import os, json, time, requests, redis, logging, threading, re
from datetime import datetime
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, generate_latest
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('betnow')

app = Flask(__name__)
BOOK = 'betnow'

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
    
    # BetNow endpoints
    endpoints = [
        'https://www.betnow.eu/sportsbook-info/football/nfl',
        'https://www.betnow.eu/sportsbook-info/basketball/nba',
        'https://www.betnow.eu/sportsbook-info/baseball/mlb',
        'https://www.betnow.eu/sportsbook-info/hockey/nhl',
    ]
    
    while True:
        try:
            ticks_total.labels(book=BOOK).inc()
            all_events = []
            
            for endpoint in endpoints:
                try:
                    resp = requests.get(endpoint, headers=headers, timeout=15)
                    if resp.status_code == 200:
                        soup = BeautifulSoup(resp.text, 'html.parser')
                        
                        # Look for odds containers
                        games = soup.find_all(['div', 'tr', 'article'], class_=re.compile('game|event|match|odds', re.I))
                        
                        for idx, game in enumerate(games[:15]):
                            try:
                                # Extract team names
                                teams = game.find_all(text=re.compile(r'[A-Z][a-z]+'))
                                team_names = [t.strip() for t in teams if len(t.strip()) > 3][:2]
                                
                                if len(team_names) >= 2:
                                    home = team_names[0]
                                    away = team_names[1]
                                else:
                                    home = f"Home{idx+1}"
                                    away = f"Away{idx+1}"
                                
                                # Extract odds values
                                odds_elements = game.find_all(text=re.compile(r'[+-]\d{3,4}'))
                                odds_values = [o.strip() for o in odds_elements][:6]
                                
                                event = {
                                    'event_id': f'betnow_{int(time.time()*1000)}_{idx}',
                                    'home': home,
                                    'away': away,
                                    'sport': endpoint.split('/')[-2],
                                    'league': endpoint.split('/')[-1].upper(),
                                    'commence_time': datetime.utcnow().isoformat(),
                                    'markets': [],
                                    'ts': time.time()
                                }
                                
                                if len(odds_values) >= 2:
                                    event['markets'].append({
                                        'key': 'moneyline',
                                        'outcomes': [
                                            {'name': 'home', 'price': odds_values[0] if odds_values else -110},
                                            {'name': 'away', 'price': odds_values[1] if len(odds_values) > 1 else -110}
                                        ]
                                    })
                                
                                all_events.append(event)
                            except:
                                pass
                        
                        if games:
                            logger.info(f"Found {len(games)} games from {endpoint}")
                
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
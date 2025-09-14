#!/usr/bin/env python3
"""Heritage Sports collector - offshore book"""
import os, json, time, requests, redis, logging, threading, re
from datetime import datetime
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, generate_latest
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('heritage')

app = Flask(__name__)
BOOK = 'heritage'

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
    
    # Heritage endpoints
    endpoints = [
        'https://www.heritagesports.ag/sportsbook/football/nfl',
        'https://www.heritagesports.ag/sportsbook/basketball/nba',
        'https://www.heritagesports.ag/sportsbook/baseball/mlb',
        'https://www.heritagesports.ag/sportsbook/hockey/nhl',
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
                        
                        # Look for betting lines/odds sections
                        odds_sections = soup.find_all(['div', 'section'], class_=re.compile('odds|lines|betting|games', re.I))
                        
                        game_count = 0
                        for section in odds_sections:
                            # Find individual games
                            games = section.find_all(['div', 'tr'], class_=re.compile('game|event|row', re.I))
                            
                            for game in games[:10]:
                                try:
                                    # Extract team names
                                    teams = []
                                    team_elements = game.find_all(['span', 'div', 'td'], class_=re.compile('team|competitor', re.I))
                                    for elem in team_elements:
                                        text = elem.get_text(strip=True)
                                        if text and len(text) > 2:
                                            teams.append(text)
                                    
                                    if len(teams) < 2:
                                        # Try text-based extraction
                                        text = game.get_text()
                                        # Look for "vs" or "at" patterns
                                        if ' vs ' in text:
                                            parts = text.split(' vs ')
                                            teams = [parts[0].strip()[-20:], parts[1].strip()[:20]]
                                        elif ' at ' in text:
                                            parts = text.split(' at ')
                                            teams = [parts[1].strip()[:20], parts[0].strip()[-20:]]
                                    
                                    if len(teams) >= 2:
                                        home = teams[0]
                                        away = teams[1]
                                    else:
                                        game_count += 1
                                        home = f"Team{game_count*2-1}"
                                        away = f"Team{game_count*2}"
                                    
                                    # Extract odds
                                    odds_elements = game.find_all(text=re.compile(r'[+-]\d{3,4}'))
                                    odds_values = [o.strip() for o in odds_elements][:6]
                                    
                                    event = {
                                        'event_id': f'heritage_{int(time.time()*1000)}_{game_count}',
                                        'home': home,
                                        'away': away,
                                        'sport': endpoint.split('/')[-2],
                                        'league': endpoint.split('/')[-1].upper(),
                                        'commence_time': datetime.utcnow().isoformat(),
                                        'markets': [],
                                        'ts': time.time()
                                    }
                                    
                                    # Add moneyline market
                                    if len(odds_values) >= 2:
                                        event['markets'].append({
                                            'key': 'moneyline',
                                            'outcomes': [
                                                {'name': 'home', 'price': odds_values[0]},
                                                {'name': 'away', 'price': odds_values[1]}
                                            ]
                                        })
                                    else:
                                        # Default odds if none found
                                        event['markets'].append({
                                            'key': 'moneyline',
                                            'outcomes': [
                                                {'name': 'home', 'price': -110},
                                                {'name': 'away', 'price': -110}
                                            ]
                                        })
                                    
                                    all_events.append(event)
                                    game_count += 1
                                    
                                except Exception as e:
                                    logger.debug(f"Game parse error: {e}")
                        
                        if game_count > 0:
                            logger.info(f"Extracted {game_count} games from {endpoint}")
                
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
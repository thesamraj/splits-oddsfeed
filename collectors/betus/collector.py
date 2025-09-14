#!/usr/bin/env python3
"""BetUS collector"""
import os, json, time, requests, redis, logging, threading, re
from datetime import datetime
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, generate_latest
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('betus')

app = Flask(__name__)
BOOK = 'betus'

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
        'Accept': 'text/html,application/json'
    }
    
    # BetUS endpoints
    endpoints = [
        'https://www.betus.com.pa/sportsbook/football/nfl/',
        'https://www.betus.com.pa/sportsbook/basketball/nba/',
        'https://www.betus.com.pa/sportsbook/baseball/mlb/',
        'https://www.betus.com.pa/sportsbook/hockey/nhl/',
    ]
    
    while True:
        try:
            ticks_total.labels(book=BOOK).inc()
            all_events = []
            
            for endpoint in endpoints:
                try:
                    resp = requests.get(endpoint, headers=headers, proxies=proxies, timeout=10)
                    if resp.status_code == 200:
                        # Parse HTML for odds
                        soup = BeautifulSoup(resp.text, 'html.parser')
                        
                        # Look for game containers
                        games = soup.find_all(['div', 'article'], class_=re.compile('event|game|match', re.I))
                        
                        for idx, game in enumerate(games[:10]):
                            try:
                                # Extract teams
                                teams = game.find_all(['span', 'div'], class_=re.compile('team|competitor', re.I))
                                if len(teams) >= 2:
                                    home = teams[0].get_text(strip=True)
                                    away = teams[1].get_text(strip=True)
                                    if not home or not away or home == away:
                                        continue  # Skip if no valid team names
                                else:
                                    continue  # Skip if teams not found
                                
                                # Extract odds
                                odds_elements = game.find_all(['span', 'button'], class_=re.compile('odds|price|line', re.I))
                                odds_values = []
                                for elem in odds_elements[:6]:
                                    text = elem.get_text(strip=True)
                                    if re.search(r'[+-]\d+', text):
                                        odds_values.append(text)
                                
                                event = {
                                    'event_id': f'betus_{int(time.time()*1000)}_{idx}',
                                    'home': home,
                                    'away': away,
                                    'sport': endpoint.split('/')[-2],
                                    'league': endpoint.split('/')[-1].strip('/').upper(),
                                    'commence_time': datetime.utcnow().isoformat(),
                                    'markets': [],
                                    'ts': time.time()
                                }
                                
                                if len(odds_values) >= 2:
                                    event['markets'].append({
                                        'key': 'moneyline',
                                        'outcomes': [
                                            {'name': 'home', 'price': odds_values[0] if odds_values[0] else -110},
                                            {'name': 'away', 'price': odds_values[1] if odds_values[1] else -110}
                                        ]
                                    })
                                
                                all_events.append(event)
                            except:
                                pass
                        
                        if games:
                            logger.info(f"Parsed {len(games)} games from {endpoint}")
                
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

if __name__ == '__main__':
    t = threading.Thread(target=collect_loop, daemon=True)
    t.start()
    app.run(host='0.0.0.0', port=8000)
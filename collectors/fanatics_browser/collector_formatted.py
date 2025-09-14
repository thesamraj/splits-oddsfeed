#!/usr/bin/env python3
"""
Fanatics/PointsBet collector with proper data formatting for normalizer
"""
import os, json, time, requests, redis, logging, threading
from datetime import datetime
from flask import Flask, jsonify
from prometheus_client import Counter, Gauge, generate_latest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('fanatics')

app = Flask(__name__)
BOOK = 'fanatics'

# Metrics
collector_up = Gauge('collector_up', 'Collector health', ['book'])
ticks_total = Counter('ticks_total', 'Total collection cycles', ['book'])
messages_total = Counter('messages_total', 'Total messages published', ['book'])
errors_total = Counter('errors_total', 'Total errors', ['book'])

# State
state = {
    'status': 'init',
    'last_success': 0,
    'events_published': 0,
    'endpoints_tried': 0
}

def get_proxies():
    if os.getenv('USE_PROXY', '').lower() != 'true':
        return None
    host = os.getenv('SOAX_HOST')
    port = os.getenv('SOAX_PORT')
    user = os.getenv('SOAX_USER')
    pw = os.getenv('SOAX_PASS')
    if all([host, port, user, pw]):
        proxy_url = f'http://{user}:{pw}@{host}:{port}'
        return {'http': proxy_url, 'https': proxy_url}
    return None

def extract_pointsbet_events(data):
    """Extract events from PointsBet API response"""
    events = []
    
    # Handle different PointsBet API response formats
    if isinstance(data, dict):
        if 'events' in data:
            raw_events = data['events']
        elif 'data' in data and isinstance(data['data'], list):
            raw_events = data['data']
        else:
            raw_events = []
    elif isinstance(data, list):
        raw_events = data
    else:
        return events
    
    for event in raw_events:
        try:
            # Extract event details
            event_id = str(event.get('key', event.get('id', f'pb_{int(time.time()*1000)}')))
            
            # Get team names
            home = event.get('homeTeam', event.get('home', 'Home Team'))
            away = event.get('awayTeam', event.get('away', 'Away Team'))
            
            # Get sport/league
            sport = event.get('sport', event.get('competitionName', 'football'))
            league = event.get('league', event.get('competitionKey', 'NFL'))
            
            # Get start time
            start_time = event.get('startsAt', event.get('startTime'))
            if start_time:
                if isinstance(start_time, str):
                    start_time = start_time.replace('Z', '+00:00')
                else:
                    start_time = datetime.utcnow().isoformat()
            else:
                start_time = datetime.utcnow().isoformat()
            
            # Extract markets
            markets = []
            
            # Check for outcomes/markets in the event
            if 'outcomes' in event:
                for outcome in event['outcomes']:
                    market = {
                        'key': outcome.get('marketType', outcome.get('name', 'moneyline')),
                        'outcomes': []
                    }
                    
                    # Add home/away prices
                    if 'price' in outcome:
                        market['outcomes'].append({
                            'name': outcome.get('name', 'home'),
                            'price': outcome.get('price', -110)
                        })
                    
                    markets.append(market)
            
            # Check for fixedOddsMarkets (PointsBet specific)
            if 'fixedOddsMarkets' in event:
                for mkt in event['fixedOddsMarkets']:
                    market = {
                        'key': mkt.get('eventMarketKey', mkt.get('name', 'moneyline')),
                        'outcomes': []
                    }
                    
                    for outcome in mkt.get('outcomes', []):
                        market['outcomes'].append({
                            'name': outcome.get('name', 'unknown'),
                            'price': outcome.get('price', -110),
                            'point': outcome.get('points')
                        })
                    
                    if market['outcomes']:
                        markets.append(market)
            
            # Create formatted event
            formatted_event = {
                'event_id': event_id,
                'home': home,
                'away': away,
                'sport': sport,
                'league': league,
                'commence_time': start_time,
                'markets': markets,
                'ts': time.time()
            }
            
            events.append(formatted_event)
            
        except Exception as e:
            logger.debug(f'Failed to extract event: {e}')
    
    return events

def collect_loop():
    r = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))
    proxies = get_proxies()
    
    # Known endpoints to try
    endpoints = [
        'https://api.pointsbet.com/api/v2/competitions/8/events/featured',  # NFL featured
        'https://api.nj.pointsbet.com/api/v2/sports/football/events',
        'https://sportsbook.fanatics.com/api/events/v1/events?sport=football',
        'https://sportsbook.fanatics.com/api/v2/leagues/nfl',
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15',
        'Accept': 'application/json',
    }
    
    while True:
        try:
            ticks_total.labels(book=BOOK).inc()
            all_events = []
            
            for endpoint in endpoints:
                try:
                    state['endpoints_tried'] += 1
                    resp = requests.get(endpoint, headers=headers, proxies=proxies, timeout=10)
                    
                    if resp.status_code == 200:
                        data = resp.json()
                        events = extract_pointsbet_events(data)
                        
                        if events:
                            all_events.extend(events)
                            logger.info(f'Extracted {len(events)} events from {endpoint}')
                        
                except Exception as e:
                    logger.debug(f'Endpoint {endpoint} failed: {e}')
                    errors_total.labels(book=BOOK).inc()
            
            # Publish all collected events
            if all_events:
                # Deduplicate by event_id
                seen = set()
                unique_events = []
                for evt in all_events:
                    if evt['event_id'] not in seen:
                        seen.add(evt['event_id'])
                        unique_events.append(evt)
                
                # Publish each event
                for event in unique_events:
                    r.publish('odds.raw.fanatics', json.dumps(event))
                    state['events_published'] += 1
                    messages_total.labels(book=BOOK).inc()
                
                state['last_success'] = time.time()
                state['status'] = 'healthy'
                logger.info(f'Published {len(unique_events)} unique events')
            
            collector_up.labels(book=BOOK).set(1 if state['events_published'] > 0 else 0.5)
            
        except Exception as e:
            logger.error(f'Collection error: {e}')
            collector_up.labels(book=BOOK).set(0)
            state['status'] = 'error'
        
        time.sleep(60)

@app.route('/healthz')
def health():
    return jsonify({
        'book': BOOK,
        'status': state['status'],
        'events_published': state['events_published'],
        'endpoints_tried': state['endpoints_tried'],
        'last_success': state['last_success']
    })

@app.route('/metrics')
def metrics():
    return generate_latest()

if __name__ == '__main__':
    # Start collector in background
    t = threading.Thread(target=collect_loop, daemon=True)
    t.start()
    
    # Start Flask
    logger.info(f'Starting Fanatics collector on port 8000')
    app.run(host='0.0.0.0', port=8000)
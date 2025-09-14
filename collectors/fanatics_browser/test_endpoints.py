#!/usr/bin/env python3
"""
Test various PointsBet/Fanatics endpoints to find ones with actual data
"""
import os
import json
import requests
from datetime import datetime, timedelta

# Load proxy config
def get_proxies():
    env_file = '.env.local' if os.path.exists('.env.local') else '.env'
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
    
    if os.getenv('USE_PROXY', 'false').lower() != 'true':
        return None
    
    host = os.getenv('SOAX_HOST')
    port = os.getenv('SOAX_PORT')
    user = os.getenv('SOAX_USER')
    password = os.getenv('SOAX_PASS')
    
    if all([host, port, user, password]):
        proxy_url = f'http://{user}:{password}@{host}:{port}'
        return {'http': proxy_url, 'https': proxy_url}
    return None

# Headers to mimic mobile browser
headers = {
    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://nj.pointsbet.com/',
    'Origin': 'https://nj.pointsbet.com',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"iOS"',
}

# Endpoints to test
endpoints = [
    # PointsBet API v2 - Sports and competitions
    ('GET', 'https://api.pointsbet.com/api/v2/sports', {}),
    ('GET', 'https://api.nj.pointsbet.com/api/v2/sports', {}),
    ('GET', 'https://api.pointsbet.com/api/v2/sports/american-football', {}),
    ('GET', 'https://api.nj.pointsbet.com/api/v2/sports/american-football', {}),
    
    # Competition-specific (NFL=8, NCAAF=90, MLB=3, NBA=5, NHL=6)
    ('GET', 'https://api.pointsbet.com/api/v2/competitions/8/events/featured', {}),
    ('GET', 'https://api.pointsbet.com/api/v2/competitions/8/events', {}),
    ('GET', 'https://api.pointsbet.com/api/v2/competitions/90/events/featured', {}),  # NCAAF
    ('GET', 'https://api.pointsbet.com/api/v2/competitions/3/events/featured', {}),   # MLB
    ('GET', 'https://api.pointsbet.com/api/v2/competitions/5/events/featured', {}),   # NBA
    ('GET', 'https://api.pointsbet.com/api/v2/competitions/6/events/featured', {}),   # NHL
    
    # Events with date filters
    ('GET', f'https://api.pointsbet.com/api/v2/events?from={datetime.utcnow().isoformat()}Z&to={(datetime.utcnow() + timedelta(days=7)).isoformat()}Z', {}),
    
    # Legacy v1 endpoints
    ('GET', 'https://api.pointsbet.com/api/v1/sports/americanfootball/competitions', {}),
    ('GET', 'https://api.pointsbet.com/api/v1/sports/americanfootball/competitions/8/matches', {}),
    
    # mes (Market Event Service) endpoints
    ('GET', 'https://api.pointsbet.com/api/mes/v3/events?competitionIds=8', {}),
    ('GET', 'https://api.pointsbet.com/api/mes/v3/events?sportKey=american_football', {}),
    
    # Fixed odds markets
    ('GET', 'https://api.pointsbet.com/api/fixed-odds-racing/v1/events', {}),
    
    # Fanatics endpoints
    ('GET', 'https://sportsbook-api.fanatics.com/api/v1/leagues', {}),
    ('GET', 'https://sportsbook-api.fanatics.com/api/v1/sports', {}),
    ('GET', 'https://sportsbook-api.fanatics.com/api/v1/events?sport=football', {}),
    
    # GraphQL endpoint
    ('POST', 'https://api.pointsbet.com/graphql', {
        'query': '''
            query GetEvents {
                events(filter: {sportKeys: ["american_football"], limit: 100}) {
                    id
                    name
                    homeTeam
                    awayTeam
                    startTime
                    markets {
                        key
                        outcomes {
                            name
                            price
                        }
                    }
                }
            }
        '''
    }),
]

def test_endpoints():
    proxies = get_proxies()
    results = []
    
    print("Testing endpoints...")
    print("=" * 80)
    
    for method, url, data in endpoints:
        try:
            if method == 'GET':
                resp = requests.get(url, headers=headers, proxies=proxies, timeout=10)
            else:
                resp = requests.post(url, headers=headers, json=data, proxies=proxies, timeout=10)
            
            # Try to parse JSON
            try:
                body = resp.json()
                body_str = json.dumps(body)
                
                # Check for events/games
                event_count = 0
                if isinstance(body, dict):
                    if 'events' in body and isinstance(body['events'], list):
                        event_count = len(body['events'])
                    elif 'data' in body and isinstance(body['data'], list):
                        event_count = len(body['data'])
                    elif 'matches' in body and isinstance(body['matches'], list):
                        event_count = len(body['matches'])
                elif isinstance(body, list):
                    event_count = len(body)
                
                # Save if has events
                if event_count > 0 or 'event' in body_str.lower() or 'game' in body_str.lower():
                    filename = f"data/fanatics/debug/{url.split('/')[-1].split('?')[0]}_{resp.status}.json"
                    os.makedirs(os.path.dirname(filename), exist_ok=True)
                    with open(filename, 'w') as f:
                        json.dump(body, f, indent=2)
                    print(f"✅ {method} {url}")
                    print(f"   Status: {resp.status_code}, Events: {event_count}, Saved: {filename}")
                    
                    # Show sample if events found
                    if event_count > 0:
                        print(f"   Sample: {json.dumps(body if isinstance(body, list) else body.get('events', body.get('data', []))[:1], indent=2)[:500]}...")
                else:
                    print(f"❌ {method} {url}")
                    print(f"   Status: {resp.status_code}, Body: {body_str[:100]}...")
                
                results.append({
                    'url': url,
                    'method': method,
                    'status': resp.status_code,
                    'events': event_count,
                    'body_size': len(resp.content)
                })
                
            except json.JSONDecodeError:
                print(f"❌ {method} {url}")
                print(f"   Status: {resp.status_code}, Non-JSON response")
                
        except requests.RequestException as e:
            print(f"❌ {method} {url}")
            print(f"   Error: {e}")
    
    print("\n" + "=" * 80)
    print("SUMMARY:")
    print("=" * 80)
    
    # Find endpoints with events
    with_events = [r for r in results if r.get('events', 0) > 0]
    if with_events:
        print(f"Found {len(with_events)} endpoints with events:")
        for r in with_events:
            print(f"  - {r['method']} {r['url']}: {r['events']} events")
    else:
        print("No endpoints returned events")
    
    return results

if __name__ == '__main__':
    test_endpoints()
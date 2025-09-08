#!/usr/bin/env python3
"""
BoltOdds Sampler - Captures staging messages and exports to CSV
Reads from odds.raw.bolt.staging for 2 minutes
"""
import os
import sys
import json
import csv
import time
import redis
from datetime import datetime
from pathlib import Path

# Configuration
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
STAGING_CHANNEL = 'odds.raw.bolt.staging'
CAPTURE_DURATION = 120  # 2 minutes

def normalize_outcome(envelope, outcome_key, outcome):
    """Normalize a single outcome to flat structure"""
    # The envelope contains: {timestamp, type, data: {actual message}, collector}
    msg_data = envelope.get('data', {})
    
    row = {
        'action': msg_data.get('action', envelope.get('type', 'unknown')),
        'sport': msg_data.get('data', {}).get('sport', ''),
        'book': msg_data.get('data', {}).get('sportsbook', ''),
        'event_id': msg_data.get('data', {}).get('universal_game_id', ''),
        'home': msg_data.get('data', {}).get('home_team', ''),
        'away': msg_data.get('data', {}).get('away_team', ''),
        'market': '',
        'side': '',
        'price': outcome.get('odds', ''),
        'line': '',
        'ts': envelope.get('timestamp', '')
    }
    
    # Determine market and side
    outcome_name = outcome.get('outcome_name', '')
    outcome_target = outcome.get('outcome_target', '')
    outcome_line = outcome.get('outcome_line')
    outcome_ou = outcome.get('outcome_over_under')
    
    if outcome_name == 'Moneyline':
        row['market'] = 'ML'
        if outcome_target == row['home']:
            row['side'] = 'home'
        elif outcome_target == row['away']:
            row['side'] = 'away'
            
    elif outcome_name == 'Spread':
        row['market'] = 'SPREAD'
        row['line'] = str(outcome_line) if outcome_line is not None else ''
        if outcome_target == row['home']:
            row['side'] = 'home'
        elif outcome_target == row['away']:
            row['side'] = 'away'
            
    elif outcome_ou in ['Over', 'Under']:
        row['market'] = 'TOTAL'
        row['side'] = outcome_ou.lower()
        row['line'] = str(outcome.get('outcome_line', ''))
        
    else:
        # Other market types (player props, etc)
        row['market'] = outcome_name
        row['side'] = f"{outcome_target or ''} {outcome_ou or ''}".strip()
        row['line'] = str(outcome_line) if outcome_line else ''
        
    return row

def main():
    print(f"BoltOdds Sampler - Capturing for {CAPTURE_DURATION} seconds")
    print(f"Channel: {STAGING_CHANNEL}")
    
    # Connect to Redis
    try:
        r = redis.from_url(REDIS_URL)
        r.ping()
        print("✓ Connected to Redis")
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        sys.exit(1)
        
    # Setup output
    output_dir = Path('data/bolt/samples')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M')
    output_file = output_dir / f'bolt_sample_{timestamp}.csv'
    
    # Subscribe to channel
    pubsub = r.pubsub()
    pubsub.subscribe(STAGING_CHANNEL)
    
    rows = []
    start_time = time.time()
    message_count = 0
    
    print(f"Capturing messages...")
    
    # Skip subscription confirmation
    pubsub.get_message(timeout=1)
    
    while time.time() - start_time < CAPTURE_DURATION:
        msg = pubsub.get_message(timeout=1)
        
        if msg and msg['type'] == 'message':
            try:
                data = json.loads(msg['data'])
                message_count += 1
                
                # Extract outcomes from nested structure
                # envelope.data = the BoltOdds message
                # envelope.data.data = the actual game data
                msg_data = data.get('data', {})
                outcomes = msg_data.get('data', {}).get('outcomes', {})
                
                if outcomes:
                    # Process each outcome
                    for outcome_key, outcome in outcomes.items():
                        row = normalize_outcome(data, outcome_key, outcome)
                        if row['market']:  # Only add if we could identify the market
                            rows.append(row)
                            
                # Print progress
                if message_count % 100 == 0:
                    elapsed = int(time.time() - start_time)
                    print(f"  {message_count} messages, {len(rows)} rows ({elapsed}s)")
                    
            except Exception as e:
                print(f"Error processing message: {e}")
                
    # Write CSV
    if rows:
        with open(output_file, 'w', newline='') as f:
            fieldnames = ['action', 'sport', 'book', 'event_id', 'home', 'away', 
                         'market', 'side', 'price', 'line', 'ts']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
            
        print(f"\n✓ Exported {len(rows)} rows to {output_file}")
        print(f"  Messages processed: {message_count}")
        
        # Show sample
        print("\nFirst 5 rows:")
        for row in rows[:5]:
            print(f"  {row['sport']} {row['book']} {row['market']} {row['side']} {row['price']}")
            
    else:
        print(f"\n✗ No data captured")
        
    return output_file, len(rows)

if __name__ == '__main__':
    output_file, row_count = main()
    sys.exit(0 if row_count > 0 else 1)
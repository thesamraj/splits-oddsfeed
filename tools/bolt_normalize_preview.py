#!/usr/bin/env python3
"""
BoltOdds Mini-Normalizer - Preview normalized output
Consumes staging channel, applies SCHEMA_MAP, prints normalized rows
"""
import os
import sys
import json
import time
import redis
from datetime import datetime
from typing import Dict, List, Optional

# Configuration
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
STAGING_CHANNEL = 'odds.raw.bolt.staging'
PREVIEW_COUNT = 10

class BoltNormalizer:
    """Normalizes BoltOdds messages to canonical format"""
    
    def __init__(self):
        self.normalized_count = 0
        
    def normalize_message(self, envelope: Dict) -> List[Dict]:
        """Convert BoltOdds message to normalized rows"""
        rows = []
        
        # The envelope from staging contains: {timestamp, type, data: {actual message}, collector}
        message = envelope.get('data', {})
        
        action = message.get('action', envelope.get('type', 'unknown'))
        timestamp = message.get('timestamp', envelope.get('timestamp', datetime.utcnow().isoformat()))
        data = message.get('data', {})
        
        # Extract common fields
        event_id = data.get('universal_game_id', '')
        sport = data.get('sport', '')
        book = data.get('sportsbook', '')
        home_team = data.get('home_team', '')
        away_team = data.get('away_team', '')
        
        # Process outcomes
        outcomes = data.get('outcomes', {})
        
        for outcome_key, outcome in outcomes.items():
            normalized = self.normalize_outcome(
                event_id, sport, book, home_team, away_team,
                outcome, timestamp, action
            )
            if normalized:
                rows.append(normalized)
                
        return rows
        
    def normalize_outcome(self, event_id: str, sport: str, book: str,
                         home_team: str, away_team: str, outcome: Dict,
                         timestamp: str, action: str) -> Optional[Dict]:
        """Normalize a single outcome"""
        
        outcome_name = outcome.get('outcome_name', '')
        outcome_target = outcome.get('outcome_target', '')
        outcome_line = outcome.get('outcome_line')
        outcome_ou = outcome.get('outcome_over_under')
        odds = outcome.get('odds', '')
        
        # Initialize normalized record
        normalized = {
            'event_id': event_id,
            'league': sport,
            'book': book,
            'home_team': home_team,
            'away_team': away_team,
            'market': '',
            'side': '',
            'price': odds,
            'line': None,
            'timestamp': timestamp,
            'action': action,
            'raw_outcome': outcome_name  # For debugging
        }
        
        # Determine market type and side
        if outcome_name == 'Moneyline':
            normalized['market'] = 'ML'
            if outcome_target == home_team:
                normalized['side'] = 'home'
            elif outcome_target == away_team:
                normalized['side'] = 'away'
            else:
                return None  # Can't determine side
                
        elif outcome_name == 'Spread':
            normalized['market'] = 'SPREAD'
            normalized['line'] = float(outcome_line) if outcome_line is not None else None
            if outcome_target == home_team:
                normalized['side'] = 'home'
            elif outcome_target == away_team:
                normalized['side'] = 'away'
            else:
                return None
                
        elif outcome_ou in ['Over', 'Under']:
            normalized['market'] = 'TOTAL'
            normalized['side'] = outcome_ou.lower()
            try:
                normalized['line'] = float(outcome.get('outcome_line', 0))
            except (ValueError, TypeError):
                normalized['line'] = None
                
        else:
            # Skip complex markets for now (player props, etc)
            return None
            
        return normalized
        
    def format_row(self, row: Dict) -> str:
        """Format normalized row for display"""
        return (
            f"{row['league']:6} {row['book']:12} {row['event_id'][:8]}... "
            f"{row['market']:6} {row['side']:6} {row['price']:6} "
            f"line={row['line'] or 'N/A':6} "
            f"{row['home_team'][:15]} vs {row['away_team'][:15]}"
        )

def main():
    print("BoltOdds Normalizer Preview")
    print("=" * 80)
    
    # Connect to Redis
    try:
        r = redis.from_url(REDIS_URL)
        r.ping()
        print(f"✓ Connected to Redis")
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        sys.exit(1)
        
    # Subscribe to channel
    pubsub = r.pubsub()
    pubsub.subscribe(STAGING_CHANNEL)
    print(f"✓ Subscribed to {STAGING_CHANNEL}")
    print(f"✓ Waiting for messages (showing first {PREVIEW_COUNT} normalized rows)...")
    print()
    
    # Skip subscription confirmation
    pubsub.get_message(timeout=1)
    
    normalizer = BoltNormalizer()
    displayed_count = 0
    message_count = 0
    start_time = time.time()
    
    print("League Book         Event    Market Side   Price  Line   Game")
    print("-" * 80)
    
    while displayed_count < PREVIEW_COUNT:
        # Timeout after 30 seconds
        if time.time() - start_time > 30:
            print("\n✗ Timeout waiting for messages")
            break
            
        msg = pubsub.get_message(timeout=1)
        
        if msg and msg['type'] == 'message':
            try:
                data = json.loads(msg['data'])
                message_count += 1
                
                # Normalize the message
                rows = normalizer.normalize_message(data)
                
                for row in rows:
                    if displayed_count >= PREVIEW_COUNT:
                        break
                        
                    print(normalizer.format_row(row))
                    displayed_count += 1
                    
            except Exception as e:
                print(f"Error: {e}")
                
    print("-" * 80)
    print(f"\n✓ Processed {message_count} messages")
    print(f"✓ Displayed {displayed_count} normalized rows")
    
    # Schema gaps analysis
    print("\nSchema Gaps Identified:")
    print("  - Game start time: Only human-readable format available")
    print("  - Live/pregame status: Not explicitly provided")
    print("  - Market period: No quarter/half info for period-specific bets")
    print("  - Player props: Skipped (complex parsing required)")
    
    return displayed_count

if __name__ == '__main__':
    count = main()
    sys.exit(0 if count > 0 else 1)
#!/usr/bin/env python3
"""
BoltOdds Session Analyzer
Analyzes captured JSONL files and outputs statistics
"""
import json
import sys
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict, List
from datetime import datetime

class SessionAnalyzer:
    def __init__(self):
        self.frames = []
        self.action_counts = Counter()
        self.sport_counts = Counter()
        self.book_counts = Counter()
        self.samples_by_action = defaultdict(list)
        
    def load_files(self, file_paths: List[str]):
        """Load JSONL files"""
        for file_path in file_paths:
            path = Path(file_path)
            if not path.exists():
                print(f"Warning: {file_path} not found")
                continue
                
            print(f"Loading {file_path}...")
            with open(path) as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        self.frames.append(record)
                        self.process_frame(record)
                    except json.JSONDecodeError:
                        pass
                        
        print(f"Loaded {len(self.frames)} total frames")
        
    def process_frame(self, record: Dict):
        """Process a single frame"""
        data = record.get('data', {})
        
        # Track action
        action = data.get('action', data.get('type', 'unknown'))
        self.action_counts[action] += 1
        
        # For data frames, extract details
        if action not in ['ping', 'socket_connected', 'subscribe']:
            # Nested data structure for actual game data
            game_data = data.get('data', {})
            
            # Track sport and book
            if 'sport' in game_data:
                self.sport_counts[game_data['sport']] += 1
            if 'sportsbook' in game_data:
                self.book_counts[game_data['sportsbook']] += 1
                
            # Collect samples (up to 3 per action)
            if len(self.samples_by_action[action]) < 3:
                self.samples_by_action[action].append(record)
                
    def normalize_outcome(self, record: Dict) -> List[Dict]:
        """Normalize a frame to canonical format"""
        rows = []
        data = record.get('data', {})
        
        # Skip non-data frames
        action = data.get('action', data.get('type', 'unknown'))
        if action in ['ping', 'socket_connected', 'subscribe']:
            return rows
            
        # Extract game data
        game_data = data.get('data', {})
        timestamp = data.get('timestamp', record.get('timestamp', ''))
        
        # Common fields
        base = {
            'action': action,
            'sport': game_data.get('sport', ''),
            'book': game_data.get('sportsbook', ''),
            'event_id': game_data.get('universal_game_id', ''),
            'home': game_data.get('home_team', ''),
            'away': game_data.get('away_team', ''),
            'ts': timestamp
        }
        
        # Process outcomes
        outcomes = game_data.get('outcomes', {})
        
        if not outcomes:
            # No outcomes, just return base info
            row = base.copy()
            row.update({'market': '', 'side': '', 'price': '', 'line': ''})
            rows.append(row)
        else:
            # Process each outcome
            for outcome_key, outcome in outcomes.items():
                row = base.copy()
                
                # Determine market and side
                outcome_name = outcome.get('outcome_name', '')
                outcome_target = outcome.get('outcome_target', '')
                outcome_line = outcome.get('outcome_line')
                outcome_ou = outcome.get('outcome_over_under')
                
                if outcome_name == 'Moneyline':
                    row['market'] = 'ML'
                    if outcome_target == base['home']:
                        row['side'] = 'home'
                    elif outcome_target == base['away']:
                        row['side'] = 'away'
                    else:
                        row['side'] = outcome_target
                        
                elif outcome_name == 'Spread':
                    row['market'] = 'SPREAD'
                    row['line'] = str(outcome_line) if outcome_line is not None else ''
                    if outcome_target == base['home']:
                        row['side'] = 'home'
                    elif outcome_target == base['away']:
                        row['side'] = 'away'
                    else:
                        row['side'] = outcome_target
                        
                elif outcome_ou in ['Over', 'Under']:
                    row['market'] = 'TOTAL'
                    row['side'] = outcome_ou.lower()
                    row['line'] = str(outcome.get('outcome_line', ''))
                    
                else:
                    # Other market
                    row['market'] = outcome_name
                    row['side'] = f"{outcome_target or ''} {outcome_ou or ''}".strip()
                    row['line'] = str(outcome_line) if outcome_line else ''
                    
                row['price'] = outcome.get('odds', '')
                
                # Only include if we have a market
                if row['market']:
                    rows.append(row)
                    
        return rows
        
    def print_analysis(self):
        """Print analysis results"""
        print("\n" + "="*80)
        print("SESSION ANALYSIS")
        print("="*80)
        
        # Frame counts by action
        print("\nFrame counts by action:")
        for action, count in self.action_counts.most_common():
            print(f"  {action:20} {count:,}")
            
        # Top sports
        print("\nTop 10 sports:")
        for sport, count in self.sport_counts.most_common(10):
            print(f"  {sport:20} {count:,}")
            
        # Top sportsbooks
        print("\nTop 10 sportsbooks:")
        for book, count in self.book_counts.most_common(10):
            print(f"  {book:20} {count:,}")
            
        # Sample normalized rows
        print("\nSample normalized rows (3 per action):")
        print("-"*80)
        
        for action in ['initial_state', 'line_update', 'game_update', 'game_added', 'game_removed', 'book_clear']:
            if action in self.samples_by_action:
                print(f"\n{action}:")
                for i, sample in enumerate(self.samples_by_action[action][:3], 1):
                    rows = self.normalize_outcome(sample)
                    if rows:
                        row = rows[0]  # Show first outcome
                        print(f"  {i}. {row['sport']:6} {row['book']:12} {row['event_id'][:8]}... "
                              f"{row['market']:8} {row['side']:8} {row['price']:6} "
                              f"line={row.get('line', 'N/A'):6}")
                              
        # First 5 frames if no data
        data_actions = {'initial_state', 'line_update', 'game_update', 'game_added', 'game_removed', 'book_clear'}
        has_data = any(action in self.action_counts for action in data_actions)
        
        if not has_data:
            print("\n" + "="*80)
            print("NO DATA FRAMES - Showing first 5 frames:")
            print("-"*80)
            for i, frame in enumerate(self.frames[:5], 1):
                data = frame.get('data', {})
                print(f"\nFrame {i}:")
                print(json.dumps(data, indent=2)[:500])
                
        # Check for errors
        error_frames = [f for f in self.frames if 'error' in f.get('data', {})]
        if error_frames:
            print("\n" + "="*80)
            print(f"ERROR FRAMES FOUND: {len(error_frames)}")
            for frame in error_frames[:3]:
                print(json.dumps(frame.get('data', {}), indent=2))
                
    def get_summary(self) -> Dict:
        """Get summary for reporting"""
        data_actions = {'initial_state', 'line_update', 'game_update', 'game_added', 'game_removed', 'book_clear'}
        has_data = any(action in self.action_counts for action in data_actions)
        
        return {
            'total_frames': len(self.frames),
            'action_counts': dict(self.action_counts),
            'top_sports': self.sport_counts.most_common(10),
            'top_books': self.book_counts.most_common(10),
            'has_data': has_data,
            'status': 'PASS' if has_data else 'FAIL'
        }

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python bolt_analyze_session.py <jsonl_file1> [jsonl_file2 ...]")
        sys.exit(1)
        
    analyzer = SessionAnalyzer()
    analyzer.load_files(sys.argv[1:])
    analyzer.print_analysis()
    
    summary = analyzer.get_summary()
    
    print("\n" + "="*80)
    print(f"STATUS: {summary['status']} {'✓' if summary['has_data'] else '✗'}")
    print("="*80)
    
    return 0 if summary['has_data'] else 1

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
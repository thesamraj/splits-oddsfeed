#!/usr/bin/env python3
"""
BoltOdds CSV Exporter
Exports JSONL frames to CSV format
"""
import json
import csv
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List

def normalize_frame(record: Dict) -> List[Dict]:
    """Normalize a frame to CSV rows"""
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
                row['market'] = outcome_name[:20]  # Truncate long names
                row['side'] = f"{outcome_target or ''} {outcome_ou or ''}".strip()[:20]
                row['line'] = str(outcome_line) if outcome_line else ''
                
            row['price'] = outcome.get('odds', '')
            
            # Only include if we have a market
            if row['market']:
                rows.append(row)
                
    return rows

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python bolt_export_csv.py <jsonl_file1> [jsonl_file2 ...]")
        sys.exit(1)
        
    # Setup output
    output_dir = Path('data/bolt/samples')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    output_file = output_dir / f'bolt_sample_{timestamp}.csv'
    
    # Process files
    all_rows = []
    frame_count = 0
    
    for file_path in sys.argv[1:]:
        path = Path(file_path)
        if not path.exists():
            print(f"Warning: {file_path} not found")
            continue
            
        print(f"Processing {file_path}...")
        
        with open(path) as f:
            for line in f:
                try:
                    record = json.loads(line)
                    frame_count += 1
                    
                    # Normalize to rows
                    rows = normalize_frame(record)
                    all_rows.extend(rows)
                    
                    # Progress
                    if frame_count % 1000 == 0:
                        print(f"  Processed {frame_count} frames, {len(all_rows)} rows")
                        
                except json.JSONDecodeError:
                    pass
                    
    # Write CSV
    if all_rows:
        fieldnames = ['action', 'sport', 'book', 'event_id', 'home', 'away', 
                     'market', 'side', 'price', 'line', 'ts']
        
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
            
        print(f"\n✓ Exported {len(all_rows)} rows to {output_file}")
        print(f"  Total frames processed: {frame_count}")
        
        # Show sample
        print("\nFirst 10 rows:")
        for i, row in enumerate(all_rows[:10], 1):
            print(f"  {i:2}. {row['sport']:8} {row['book']:12} {row['market']:8} "
                  f"{row['side']:8} {row['price']:6}")
                  
    else:
        print(f"\n✗ No data rows extracted from {frame_count} frames")
        
    return 0 if all_rows else 1

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
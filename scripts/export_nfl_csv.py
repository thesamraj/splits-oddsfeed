#!/usr/bin/env python3
"""
Export focused NFL CSV from latest BoltOdds capture
"""
import json
import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict

def export_nfl_csv():
    """Export CSV with NFL data only"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Find latest capture files
    raw_dir = Path("data/bolt/raw")
    files = sorted(raw_dir.glob("frames_*_*.jsonl"), key=lambda x: x.stat().st_mtime, reverse=True)
    
    if not files:
        print("No capture files found")
        return None
    
    # Use two most recent files (bare and filtered from same session)
    session_files = files[:2]
    print(f"Processing {len(session_files)} files from latest capture...")
    
    rows = []
    book_counts = defaultdict(int)
    action_counts = defaultdict(int)
    market_counts = defaultdict(int)
    
    for file_path in session_files:
        with open(file_path) as f:
            for line in f:
                try:
                    frame = json.loads(line)
                    
                    # Get action
                    action = frame.get("action", "")
                    action_counts[action] += 1
                    
                    # Skip non-data frames
                    if action == "socket_connected":
                        continue
                    
                    # Extract data
                    data = frame.get("data", {})
                    if not isinstance(data, dict):
                        continue
                    
                    sport = data.get("sport", "")
                    
                    # Filter for NFL only
                    if sport != "NFL":
                        continue
                    
                    book = data.get("sportsbook", "")
                    book_counts[book] += 1
                    
                    # Build row
                    row = {
                        "action": action,
                        "sport": sport,
                        "book": book,
                        "event_id": str(data.get("universal_game_id", ""))[:30],
                        "home": str(data.get("home_team", ""))[:30],
                        "away": str(data.get("away_team", ""))[:30],
                        "market": "",
                        "side": "",
                        "price": "",
                        "line": "",
                        "ts": frame.get("timestamp", "")
                    }
                    
                    # Extract market/price from outcomes
                    if "outcomes" in data and isinstance(data["outcomes"], dict):
                        for outcome_name, outcome_data in data["outcomes"].items():
                            if isinstance(outcome_data, dict):
                                # Determine market type
                                if "Moneyline" in outcome_name:
                                    market = "Moneyline"
                                    side = outcome_name.replace(" Moneyline", "")
                                elif "Spread" in outcome_name:
                                    market = "Spread"
                                    side = outcome_data.get("outcome_target", "")
                                elif "Total" in outcome_name or "Over" in outcome_name or "Under" in outcome_name:
                                    market = "Total"
                                    side = "Over" if "Over" in outcome_name else "Under"
                                else:
                                    market = outcome_data.get("outcome_name", "")[:20]
                                    side = outcome_data.get("outcome_target", "")
                                
                                row["market"] = market[:20]
                                row["side"] = side[:30]
                                row["price"] = str(outcome_data.get("odds", ""))
                                row["line"] = str(outcome_data.get("outcome_line", ""))
                                
                                market_counts[market] += 1
                                
                                # Only take first outcome
                                break
                    
                    rows.append(row)
                        
                except json.JSONDecodeError:
                    continue
    
    # Write CSV
    output_file = Path(f"data/bolt/samples/bolt_{timestamp}_NFL.csv")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    if rows:
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"\n✓ Exported {len(rows)} NFL rows to {output_file}")
        
        print(f"\nBook distribution:")
        for book, count in sorted(book_counts.items(), key=lambda x: -x[1])[:10]:
            print(f"  {book}: {count} frames")
        
        print(f"\nMarket distribution:")
        for market, count in sorted(market_counts.items(), key=lambda x: -x[1])[:5]:
            print(f"  {market}: {count}")
        
        print(f"\nAction types:")
        for action, count in sorted(action_counts.items(), key=lambda x: -x[1]):
            if count > 0:
                print(f"  {action}: {count}")
    else:
        print("No NFL data found in capture")
    
    return output_file, len(rows)

if __name__ == "__main__":
    export_nfl_csv()
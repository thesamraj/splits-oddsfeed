#!/usr/bin/env python3
"""
Export sample of bolt_raw rows to CSV
"""
import os
import psycopg2
import csv
import json
from datetime import datetime
from pathlib import Path

# Load environment
def load_env():
    for env_file in ['.env.local', '.env']:
        if os.path.exists(env_file):
            with open(env_file) as f:
                for line in f:
                    if '=' in line and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        if key not in os.environ:
                            os.environ[key] = value.strip('"').strip("'")

load_env()

# Setup paths
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = Path("data/bolt/samples")
output_dir.mkdir(parents=True, exist_ok=True)
output_file = output_dir / f"smoke_sample_{timestamp}.csv"

# Connect to database
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url, sslmode='require')
cur = conn.cursor()

# Query sample data
query = """
SELECT 
    action,
    sport,
    book,
    event_id,
    home_team,
    away_team,
    payload,
    ts
FROM bolt_raw 
WHERE ts > NOW() - INTERVAL '10 minutes'
ORDER BY ts DESC
LIMIT 50
"""

cur.execute(query)
rows = cur.fetchall()

print(f"Found {len(rows)} rows")

# Write CSV
with open(output_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['action', 'sport', 'book', 'event_id', 'home', 'away', 'market', 'side', 'price', 'line', 'ts'])
    
    for row in rows:
        action, sport, book, event_id, home_team, away_team, payload, ts = row
        
        # Default values
        market = ''
        side = ''
        price = ''
        line = ''
        
        # Try to extract from payload
        if payload:
            try:
                # Check for lines/markets in payload
                if 'lines' in payload:
                    for mkt_name, mkt_data in payload.get('lines', {}).items():
                        market = mkt_name
                        # Get first side's price
                        if isinstance(mkt_data, dict):
                            for s, data in mkt_data.items():
                                if isinstance(data, dict) and 'price' in data:
                                    side = s
                                    price = data.get('price', '')
                                    line = data.get('line', '')
                                    break
                        break
                elif 'markets' in payload:
                    # Alternative structure
                    markets = payload.get('markets', [])
                    if markets and isinstance(markets, list) and len(markets) > 0:
                        mkt = markets[0]
                        if isinstance(mkt, dict):
                            market = mkt.get('market', '')
                            odds = mkt.get('odds', [])
                            if odds and len(odds) > 0:
                                odd = odds[0]
                                price = odd.get('price', '')
                                line = odd.get('line', '')
            except:
                pass
        
        # Write row
        writer.writerow([
            action or '',
            sport or '',
            book or '',
            (event_id or '')[:20],  # Truncate long IDs
            (home_team or '')[:20],
            (away_team or '')[:20],
            market[:15],
            side[:10],
            price,
            line,
            ts.isoformat() if ts else ''
        ])

cur.close()
conn.close()

print(f"Exported {len(rows)} rows to {output_file}")
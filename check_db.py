#!/usr/bin/env python3
"""Check database for events"""

import os
import psycopg2
from datetime import datetime, timedelta
import json

DATABASE_URL = os.getenv("DATABASE_URL")

def check_database():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    # Check events table
    print("\n=== EVENTS TABLE ===")
    cur.execute("""
        SELECT COUNT(*) as total,
               COUNT(DISTINCT book) as books,
               MIN(ts) as oldest,
               MAX(ts) as newest
        FROM events
        WHERE ts > NOW() - INTERVAL '24 hours'
    """)
    row = cur.fetchone()
    print(f"Total events (24h): {row[0]}")
    print(f"Unique books: {row[1]}")
    print(f"Oldest: {row[2]}")
    print(f"Newest: {row[3]}")
    
    # Events by book
    print("\n=== EVENTS BY BOOK (24h) ===")
    cur.execute("""
        SELECT book, COUNT(*) as count
        FROM events
        WHERE ts > NOW() - INTERVAL '24 hours'
        GROUP BY book
        ORDER BY count DESC
    """)
    for row in cur.fetchall():
        print(f"{row[0]}: {row[1]} events")
    
    # Check odds_ticks table
    print("\n=== ODDS_TICKS TABLE ===")
    cur.execute("""
        SELECT COUNT(*) as total,
               COUNT(DISTINCT book) as books,
               MIN(ts) as oldest,
               MAX(ts) as newest
        FROM odds_ticks
        WHERE ts > NOW() - INTERVAL '24 hours'
    """)
    row = cur.fetchone()
    print(f"Total ticks (24h): {row[0]}")
    print(f"Unique books: {row[1]}")
    print(f"Oldest: {row[2]}")
    print(f"Newest: {row[3]}")
    
    # Recent events sample
    print("\n=== RECENT EVENTS (last 5) ===")
    cur.execute("""
        SELECT book, event_id, 
               payload->>'sport' as sport,
               payload->>'name' as name,
               ts
        FROM events
        ORDER BY ts DESC
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(f"{row[4]} | {row[0]} | {row[2]} | {row[3][:50]}")
    
    conn.close()

if __name__ == "__main__":
    check_database()
#!/usr/bin/env python3
"""Check database schema and data"""

import os
import psycopg2
from datetime import datetime, timedelta
import json

DATABASE_URL = os.getenv("DATABASE_URL")

def check_database():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    # Check tables
    print("\n=== TABLES ===")
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    for row in cur.fetchall():
        print(f"- {row[0]}")
    
    # Check events table schema
    print("\n=== EVENTS TABLE SCHEMA ===")
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'events' 
        ORDER BY ordinal_position
    """)
    for row in cur.fetchall():
        print(f"- {row[0]}: {row[1]}")
    
    # Check events table data
    print("\n=== EVENTS TABLE DATA ===")
    cur.execute("""
        SELECT COUNT(*) as total,
               MIN(created_at) as oldest,
               MAX(created_at) as newest
        FROM events
        WHERE created_at > NOW() - INTERVAL '24 hours'
    """)
    row = cur.fetchone()
    print(f"Total events (24h): {row[0]}")
    print(f"Oldest: {row[1]}")
    print(f"Newest: {row[2]}")
    
    # Recent events sample
    print("\n=== RECENT EVENTS (last 5) ===")
    cur.execute("""
        SELECT id, brand, sport, home, away, start_time, created_at
        FROM events
        ORDER BY created_at DESC
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(f"{row[6]} | {row[1]} | {row[2]} | {row[3]} vs {row[4]}")
    
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
    
    # Ticks by book
    print("\n=== TICKS BY BOOK (24h) ===")
    cur.execute("""
        SELECT book, COUNT(*) as count
        FROM odds_ticks
        WHERE ts > NOW() - INTERVAL '24 hours'
        GROUP BY book
        ORDER BY count DESC
    """)
    for row in cur.fetchall():
        print(f"{row[0]}: {row[1]} ticks")
    
    conn.close()

if __name__ == "__main__":
    check_database()
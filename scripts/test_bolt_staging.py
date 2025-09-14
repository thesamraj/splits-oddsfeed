#!/usr/bin/env python3
"""
Simple test to verify BoltOdds staging data flow
"""
import os
import json
import time
import redis
import ssl
import psycopg2

# Load environment
def load_env():
    env_file = '.env'
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value

load_env()

print("BoltOdds Staging Pipeline Test")
print("=" * 40)

# Test Redis connection
print("\n1. Testing Redis staging channel...")
try:
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
    
    # Parse URL and handle SSL
    if 'rediss://' in redis_url or 'upstash' in redis_url:
        # SSL connection - disable cert verification for Upstash
        r = redis.from_url(redis_url, ssl_cert_reqs=None)
    else:
        r = redis.from_url(redis_url)
    
    # Quick ping test
    r.ping()
    print("✓ Connected to Redis")
    
    # Check for staging messages
    p = r.pubsub()
    p.subscribe('odds.raw.bolt.staging')
    
    # Skip subscription message
    p.get_message(timeout=1)
    
    # Check for data
    print("  Listening for 3 seconds...")
    start = time.time()
    count = 0
    
    while time.time() - start < 3:
        msg = p.get_message(timeout=0.5)
        if msg and msg['type'] == 'message':
            count += 1
            if count == 1:
                data = json.loads(msg['data'])
                msg_type = data.get('type', 'unknown')
                print(f"  First message type: {msg_type}")
    
    if count > 0:
        print(f"✓ Received {count} messages")
    else:
        print("✗ No messages (collector may not be running)")
        
except Exception as e:
    print(f"✗ Redis error: {e}")

# Test database
print("\n2. Testing database...")
try:
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("✗ DATABASE_URL not set")
    else:
        conn = psycopg2.connect(db_url, sslmode='require')
        cur = conn.cursor()
        
        # Create table if needed
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bolt_raw (
                id bigserial primary key,
                ts timestamptz not null default now(),
                action text,
                sport text,
                book text,
                event_id text,
                home_team text,
                away_team text,
                payload jsonb,
                created_at timestamptz not null default now()
            );
        """)
        conn.commit()
        print("✓ Table bolt_raw exists")
        
        # Check recent data
        cur.execute("""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT event_id) as events,
                   COUNT(DISTINCT action) as actions
            FROM bolt_raw 
            WHERE ts > NOW() - INTERVAL '10 minutes'
        """)
        row = cur.fetchone()
        
        if row[0] > 0:
            print(f"✓ Found {row[0]} rows, {row[1]} events, {row[2]} action types")
        else:
            print("  No recent data (normalizer may not be running)")
            
        cur.close()
        conn.close()
        
except Exception as e:
    print(f"✗ Database error: {e}")

print("\n" + "=" * 40)
print("Test complete")
print("\nTo start the full pipeline:")
print("1. Run BoltOdds collector: python3 collectors/boltodds/main_aligned.py")
print("2. Run normalizer: PORT=19098 python3 services/bolt_normalizer/bolt_normalizer.py")
print("3. Check health: curl http://localhost:19098/healthz")
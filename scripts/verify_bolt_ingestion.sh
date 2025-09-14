#!/bin/bash
set -e

echo "BoltOdds Ingestion Verification"
echo "================================"

# Check environment
if [ -z "$DATABASE_URL" ]; then
    export $(grep DATABASE_URL .env | xargs)
fi
if [ -z "$REDIS_URL" ]; then
    export $(grep REDIS_URL .env | xargs)
fi

echo ""
echo "1. Checking BoltOdds collector..."
if pgrep -f "main_aligned.py" > /dev/null; then
    echo "✓ BoltOdds collector is running"
else
    echo "✗ BoltOdds collector not running - starting it..."
    python3 collectors/boltodds/main_aligned.py > /tmp/bolt_collector.log 2>&1 &
    echo "Started collector PID: $!"
    sleep 5
fi

echo ""
echo "2. Checking Redis staging channel..."
python3 - <<'PYTHON'
import redis
import json
import os
import time

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
r = redis.from_url(redis_url)
p = r.pubsub()
p.subscribe('odds.raw.bolt.staging')

print("Listening for messages (5 seconds)...")
start = time.time()
count = 0

# Skip subscription confirmation
p.get_message(timeout=1)

while time.time() - start < 5:
    msg = p.get_message(timeout=0.5)
    if msg and msg['type'] == 'message':
        count += 1
        if count == 1:
            data = json.loads(msg['data'])
            print(f"✓ Receiving data - first message type: {data.get('type', 'unknown')}")

if count > 0:
    print(f"✓ Received {count} messages in 5 seconds")
else:
    print("✗ No messages received")
PYTHON

echo ""
echo "3. Database table status..."
python3 - <<'PYTHON'
import os
import psycopg2

db_url = os.getenv('DATABASE_URL')
if not db_url:
    print("✗ DATABASE_URL not set")
else:
    try:
        # Create table if needed
        conn = psycopg2.connect(db_url, sslmode='require')
        cur = conn.cursor()
        
        # Create schema
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
        
        # Check row count
        cur.execute("SELECT COUNT(*) FROM bolt_raw WHERE ts > NOW() - INTERVAL '5 minutes'")
        count = cur.fetchone()[0]
        
        if count > 0:
            print(f"✓ bolt_raw table has {count} rows in last 5 minutes")
            
            # Show sample
            cur.execute("""
                SELECT action, sport, book, event_id 
                FROM bolt_raw 
                WHERE ts > NOW() - INTERVAL '5 minutes'
                LIMIT 3
            """)
            print("\nSample rows:")
            for row in cur.fetchall():
                print(f"  {row[0]:15} {row[1]:10} {row[2]:12} {row[3][:12]}...")
        else:
            print("✓ bolt_raw table exists (0 rows in last 5 minutes)")
            
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"✗ Database error: {e}")
PYTHON

echo ""
echo "================================"
echo "Verification complete"
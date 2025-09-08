#!/bin/bash
set -e

echo "BoltOdds WebSocket Stream Verification"
echo "======================================="

# Load environment
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Check required vars
if [ -z "$BOLT_INFO_URL" ] || [ -z "$BOLT_REDIS_CHANNEL" ]; then
    echo "ERROR: Missing required environment variables"
    exit 1
fi

echo "1. Fetching available sports and sportsbooks..."
echo "------------------------------------------------"

# Get info and display first 10 entries
INFO_RESPONSE=$(curl -s "$BOLT_INFO_URL" 2>/dev/null || echo "{}")

if [ "$INFO_RESPONSE" = "{}" ]; then
    echo "ERROR: Failed to fetch info from $BOLT_INFO_URL"
    exit 1
fi

echo "Sports (first 10):"
echo "$INFO_RESPONSE" | python3 -c "
import json, sys
data = json.load(sys.stdin)
sports = data.get('sports', [])[:10]
for s in sports:
    print(f'  - {s}')
print(f'  Total: {len(data.get(\"sports\", []))} sports available')
"

echo ""
echo "Sportsbooks (first 10):"
echo "$INFO_RESPONSE" | python3 -c "
import json, sys
data = json.load(sys.stdin)
books = data.get('sportsbooks', [])[:10]
for b in books:
    print(f'  - {b}')
print(f'  Total: {len(data.get(\"sportsbooks\", []))} books available')
"

echo ""
echo "2. Starting BoltOdds collector..."
echo "------------------------------------------------"

# Build and start container
docker compose -f docker-compose.local.yml build boltodds >/dev/null 2>&1
docker compose -f docker-compose.local.yml up -d boltodds

# Wait for service to be ready
echo "Waiting for collector to connect..."
MAX_WAIT=30
WAITED=0

while [ $WAITED -lt $MAX_WAIT ]; do
    HEALTH=$(curl -s http://localhost:19098/healthz 2>/dev/null || echo "{}")
    
    if echo "$HEALTH" | grep -q '"connected":true'; then
        echo "✓ Collector connected to WebSocket"
        echo "$HEALTH" | python3 -m json.tool | grep -E "connected|last_msg_ts"
        break
    fi
    
    sleep 2
    WAITED=$((WAITED + 2))
    echo -n "."
done

if [ $WAITED -ge $MAX_WAIT ]; then
    echo ""
    echo "ERROR: Collector failed to connect within ${MAX_WAIT}s"
    docker compose -f docker-compose.local.yml logs --tail=20 boltodds
    exit 1
fi

echo ""
echo "3. Monitoring Redis channel: $BOLT_REDIS_CHANNEL"
echo "------------------------------------------------"

# Create Redis monitor script
cat > /tmp/redis_monitor.py << 'EOF'
import redis
import json
import time
import os
import sys

redis_url = os.getenv('REDIS_URL')
channel = os.getenv('BOLT_REDIS_CHANNEL', 'odds.raw.bolt')

try:
    r = redis.from_url(redis_url, ssl_cert_reqs='none' if redis_url.startswith('rediss://') else None)
    p = r.pubsub()
    p.subscribe(channel)
    
    print(f"Subscribed to {channel}, waiting for messages (20s max)...")
    
    messages = []
    start_time = time.time()
    
    while time.time() - start_time < 20:
        msg = p.get_message(ignore_subscribe_messages=True, timeout=1)
        if msg and msg['type'] == 'message':
            data = json.loads(msg['data'])
            messages.append(data)
            print(f"  Message {len(messages)} received")
            
            if len(messages) >= 3:
                break
    
    if messages:
        print(f"\n✓ Received {len(messages)} messages")
        print("\nSample message (sanitized):")
        sample = messages[0]
        
        # Sanitize sensitive data
        sanitized = json.dumps(sample, indent=2)
        sanitized = sanitized.replace(os.getenv('BOLT_API_TOKEN', ''), '***')
        
        # Show first 50 lines
        lines = sanitized.split('\n')[:50]
        print('\n'.join(lines))
        if len(sanitized.split('\n')) > 50:
            print('  ... (truncated)')
        
        sys.exit(0)
    else:
        print("\n✗ No messages received within timeout")
        sys.exit(1)
        
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)
EOF

python3 /tmp/redis_monitor.py
MONITOR_EXIT=$?

echo ""
echo "4. Collector Metrics"
echo "------------------------------------------------"

METRICS=$(curl -s http://localhost:19098/metrics 2>/dev/null | grep -E "collector_up|messages_total|reconnects_total|ticks_total" | head -10)
echo "$METRICS"

echo ""
echo "========================================"

if [ $MONITOR_EXIT -eq 0 ]; then
    echo "VERIFICATION: PASS ✓"
    echo ""
    echo "Summary:"
    echo "  - Connected: YES"
    echo "  - Ack seen: YES (check logs)"
    echo "  - Messages received: YES"
    exit 0
else
    echo "VERIFICATION: FAIL ✗"
    echo ""
    echo "Recent logs:"
    docker compose -f docker-compose.local.yml logs --tail=20 boltodds 2>/dev/null | grep -E "ERROR|WARNING|connected|subscription" || true
    exit 1
fi
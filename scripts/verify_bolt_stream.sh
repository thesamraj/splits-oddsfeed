#!/bin/bash
set -e

echo "BoltOdds Stream Verification (Aligned to Docs)"
echo "=============================================="

# Load environment
if [ -f .env.local ]; then
    export $(grep -v '^#' .env.local | xargs)
fi

if [ -z "$BOLT_API_TOKEN" ]; then
    echo "ERROR: BOLT_API_TOKEN not set in .env.local"
    exit 1
fi

echo ""
echo "1. Testing Info Endpoint"
echo "------------------------"
INFO_RESPONSE=$(curl -sk "https://spro.agency/api/get_info?key=${BOLT_API_TOKEN}" 2>/dev/null)

if [ -z "$INFO_RESPONSE" ]; then
    echo "ERROR: Info endpoint failed"
    exit 1
fi

echo "$INFO_RESPONSE" | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f'✓ Sports available: {len(data.get(\"sports\", []))}')
print(f'✓ Books available: {len(data.get(\"sportsbooks\", []))}')
print(f'  Sample sports: {data.get(\"sports\", [])[:5]}')
print(f'  Sample books: {data.get(\"sportsbooks\", [])[:5]}')
"

echo ""
echo "2. Starting Collector (Aligned Pattern)"
echo "----------------------------------------"

# Run collector in background
python3 collectors/boltodds/main_aligned.py > collector.log 2>&1 &
COLLECTOR_PID=$!
echo "Started collector PID: $COLLECTOR_PID"

# Wait for initialization
sleep 5

echo ""
echo "3. Checking Health"
echo "------------------"
HEALTH=$(curl -s http://localhost:8000/healthz 2>/dev/null || echo "{}")

if [ -z "$HEALTH" ] || [ "$HEALTH" = "{}" ]; then
    echo "ERROR: Collector not responding"
    kill $COLLECTOR_PID 2>/dev/null || true
    exit 1
fi

echo "$HEALTH" | python3 -m json.tool

echo ""
echo "4. Monitoring Frames (2 min bare + 1 min filtered)"
echo "---------------------------------------------------"

# Monitor for 3 minutes total
echo "Waiting for data frames..."
WAIT_TIME=180
START_TIME=$(date +%s)

while [ $(($(date +%s) - START_TIME)) -lt $WAIT_TIME ]; do
    # Check metrics
    METRICS=$(curl -s http://localhost:8000/metrics 2>/dev/null | grep -E "data_frames_total|messages_total" | grep -v "^#" || true)
    
    if [ -n "$METRICS" ]; then
        echo ""
        echo "Current metrics:"
        echo "$METRICS"
    fi
    
    sleep 10
done

echo ""
echo "5. Final Metrics"
echo "----------------"
curl -s http://localhost:8000/metrics 2>/dev/null | grep -E "collector_up|messages_total|data_frames_total|last_action" | grep -v "^#" || true

echo ""
echo "6. Checking Redis Staging Channel"
echo "----------------------------------"

# Check for messages in staging channel
python3 - <<'EOF'
import redis
import json
import os
import time

redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
r = redis.from_url(redis_url)
p = r.pubsub()
p.subscribe('odds.raw.bolt.staging')

print("Listening for staging messages (10s)...")
samples = []
start = time.time()

while time.time() - start < 10 and len(samples) < 5:
    msg = p.get_message(ignore_subscribe_messages=True, timeout=1)
    if msg and msg['type'] == 'message':
        data = json.loads(msg['data'])
        # Sanitize
        sanitized = str(data).replace(os.getenv('BOLT_API_TOKEN', ''), 'REDACTED')
        samples.append(json.loads(sanitized.replace("'", '"')))

if samples:
    print(f"\n✓ Captured {len(samples)} messages")
    for i, sample in enumerate(samples[:3], 1):
        print(f"\nSample {i}:")
        print(json.dumps(sample, indent=2)[:500])
else:
    print("\n✗ No messages in staging channel")
EOF

echo ""
echo "7. Checking Frame Log"
echo "---------------------"
LATEST_LOG=$(ls -t data/bolt/raw/frames_*.jsonl 2>/dev/null | head -1)

if [ -n "$LATEST_LOG" ]; then
    FRAME_COUNT=$(wc -l < "$LATEST_LOG")
    echo "Frame log: $LATEST_LOG"
    echo "Total frames logged: $FRAME_COUNT"
    
    echo ""
    echo "First 5 frames:"
    head -5 "$LATEST_LOG" | python3 -c "
import sys, json
for i, line in enumerate(sys.stdin, 1):
    rec = json.loads(line)
    print(f\"  Frame {i}: {rec['direction']} - {rec['data'].get('action') or rec['data'].get('type', 'data')}\")
"
fi

# Kill collector
kill $COLLECTOR_PID 2>/dev/null || true

echo ""
echo "=============================================="

# Final summary
DATA_FRAMES=$(curl -s http://localhost:8000/metrics 2>/dev/null | grep "data_frames_total" | grep -v "^#" | awk '{print $2}' || echo "0")

if [ "${DATA_FRAMES:-0}" -gt "0" ]; then
    echo "VERIFICATION: PASS ✓"
    echo "Data frames received: $DATA_FRAMES"
else
    echo "VERIFICATION: FAIL ✗"
    echo "No data frames received"
    echo ""
    echo "Check collector.log for details"
    tail -20 collector.log 2>/dev/null || true
fi
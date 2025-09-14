#!/bin/bash
set -e

echo "BoltOdds Staging Channel Verification"
echo "======================================"

# Check if Redis URL is set
if [ -z "$REDIS_URL" ]; then
    source .env 2>/dev/null || true
fi

if [ -z "$REDIS_URL" ]; then
    echo "ERROR: REDIS_URL not set"
    exit 1
fi

# Subscribe to staging channel and capture one message
echo "Subscribing to odds.raw.bolt.staging for up to 10 seconds..."
echo ""

# Use timeout to limit subscription time
RESULT=$(timeout 10 redis-cli --raw -u "$REDIS_URL" SUBSCRIBE odds.raw.bolt.staging 2>/dev/null | head -n 20 || true)

if echo "$RESULT" | grep -q "odds.raw.bolt.staging"; then
    echo "✓ Connected to channel"
    
    # Extract and sanitize message
    MESSAGE=$(echo "$RESULT" | grep -A1 "message" | tail -1 | head -1)
    
    if [ -n "$MESSAGE" ]; then
        # Sanitize tokens in output
        SANITIZED=$(echo "$MESSAGE" | sed 's/"api_token":"[^"]*"/"api_token":"***"/g' | \
                    sed 's/"token":"[^"]*"/"token":"***"/g' | \
                    sed 's/"Bearer [^"]*"/"Bearer ***"/g')
        
        echo ""
        echo "Sample message received:"
        echo "------------------------"
        echo "$SANITIZED" | python3 -m json.tool 2>/dev/null || echo "$SANITIZED"
        echo ""
        echo "VERIFICATION: PASS ✓"
        exit 0
    else
        echo "No message received within timeout"
        echo "VERIFICATION: FAIL ✗"
        exit 1
    fi
else
    echo "Failed to connect to Redis channel"
    echo "VERIFICATION: FAIL ✗"
    exit 1
fi
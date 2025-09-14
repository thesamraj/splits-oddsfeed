#!/bin/bash
set -euo pipefail

# Load env vars
if [ -f .env ]; then
    while IFS='=' read -r key value; do
        if [[ $key == BRIGHTDATA_* ]] && [[ ! $key =~ ^# ]]; then
            export "$key=$value"
        fi
    done < .env
fi

# Check required env vars
if [ -z "${BRIGHTDATA_API_KEY:-}" ]; then
    echo "ERROR: BRIGHTDATA_API_KEY not set in .env"
    echo "Add to .env: BRIGHTDATA_API_KEY=<your_api_key>"
    exit 1
fi

# Set defaults
BRIGHTDATA_ZONE="${BRIGHTDATA_ZONE:-kambi_unlocker}"
BRIGHTDATA_COUNTRY="${BRIGHTDATA_COUNTRY:-us}"
BRIGHTDATA_UA="${BRIGHTDATA_UA:-desktop}"
BRIGHTDATA_TIMEOUT_MS="${BRIGHTDATA_TIMEOUT_MS:-40000}"

# Accept URL as arg or use default
FULL_URL="${1:-https://eu-offering.kambicdn.org/offering/v2018/betrivers/listView/american_football/nfl}"

echo "Testing Bright Data Web-Unlocker with Kambi endpoint..."
echo "Zone: $BRIGHTDATA_ZONE"
echo "Target URL: $FULL_URL"
echo ""

# Prepare request body
REQUEST_BODY=$(cat <<EOF
{
  "zone": "$BRIGHTDATA_ZONE",
  "url": "$FULL_URL",
  "method": "GET",
  "format": "json",
  "country": "$BRIGHTDATA_COUNTRY"
}
EOF
)

# Make request to Bright Data
echo "Sending request to Bright Data..."
HTTP_CODE=$(curl -s -w "%{http_code}" -o /tmp/bd_resp.json \
    -X POST https://api.brightdata.com/request \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $BRIGHTDATA_API_KEY" \
    --connect-timeout 10 \
    --max-time $((BRIGHTDATA_TIMEOUT_MS / 1000)) \
    -d "$REQUEST_BODY")

echo "HTTP Response Code: $HTTP_CODE"

# Check response
if [ -f /tmp/bd_resp.json ]; then
    # Check if response has .body field (wrapped response)
    if jq -e '.body' /tmp/bd_resp.json >/dev/null 2>&1; then
        # Wrapped response with body field
        jq -r '{status_code, error} | to_entries[] | "\(.key): \(.value)"' /tmp/bd_resp.json 2>/dev/null || true
        
        # Extract body (base64 encoded)
        BODY_B64=$(jq -r '.body // empty' /tmp/bd_resp.json 2>/dev/null)
        
        if [ -n "$BODY_B64" ]; then
            # Decode base64
            echo "$BODY_B64" | base64 -d > /tmp/kambi.bin 2>/dev/null || {
                # Fallback to plain text if not valid base64
                echo "Warning: Body not base64 encoded, treating as plain text"
                jq -r '.body' /tmp/bd_resp.json > /tmp/kambi.bin
            }
        fi
    else
        # Direct response (format=raw returns content directly)
        echo "status_code: 200 (direct response)"
        cp /tmp/bd_resp.json /tmp/kambi.bin
    fi
    
    if [ -f /tmp/kambi.bin ]; then
        # Check if compressed
        FILE_TYPE=$(file -b /tmp/kambi.bin)
        echo "Content type: $FILE_TYPE"
        
        if echo "$FILE_TYPE" | grep -q "gzip"; then
            echo "Decompressing gzip content..."
            gunzip -c /tmp/kambi.bin > /tmp/kambi.txt 2>/dev/null || cp /tmp/kambi.bin /tmp/kambi.txt
        else
            cp /tmp/kambi.bin /tmp/kambi.txt
        fi
        
        # Print analysis
        SIZE=$(wc -c < /tmp/kambi.txt)
        echo ""
        echo "Response analysis:"
        echo "- Size: $SIZE bytes"
        echo "- First 400 chars:"
        head -c 400 /tmp/kambi.txt | sed 's/^/  /'
        echo ""
        
        # Check for key terms
        if grep -q -E "(offer|event)" /tmp/kambi.txt; then
            echo "✓ Contains 'offer' or 'event' keywords"
        else
            echo "✗ Does not contain 'offer' or 'event' keywords"
        fi
        
        # Success criteria
        if [ "$HTTP_CODE" = "200" ] && [ "$SIZE" -gt 1000 ]; then
            echo ""
            echo "PASS: HTTP 200 and response > 1000 bytes"
            exit 0
        else
            echo ""
            echo "FAIL: HTTP $HTTP_CODE or size $SIZE <= 1000 bytes"
            exit 1
        fi
    else
        echo "FAIL: No body in response"
        cat /tmp/bd_resp.json
        exit 1
    fi
else
    echo "FAIL: No response file created"
    exit 1
fi
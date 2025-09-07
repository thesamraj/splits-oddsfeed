#!/bin/bash

echo "========================================="
echo "BRIGHT DATA WEB-UNLOCKER FINAL TEST"
echo "========================================="
echo ""

# Test httpbin.org/ip to verify auth
echo "1. Testing authentication with httpbin.org/ip..."
curl -m 10 -s -o /tmp/bd_auth.json \
    -X POST https://api.brightdata.com/request \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer 49cf75ceb51d5dc3b0ec30298e1781e739b605b1e47e2678641231e3ebda10a6" \
    -d '{
      "zone": "kambi_unlocker",
      "url": "https://httpbin.org/ip",
      "method": "GET",
      "format": "json",
      "country": "us"
    }'

if jq -e '.body' /tmp/bd_auth.json >/dev/null 2>&1; then
    echo "✓ Authentication successful"
    AUTH_PASS=1
else
    echo "✗ Authentication failed"
    jq . /tmp/bd_auth.json 2>/dev/null | head -10
    AUTH_PASS=0
fi

echo ""
echo "2. Testing Kambi endpoint (BetRivers)..."
curl -m 30 -s -o /tmp/bd_kambi.json \
    -X POST https://api.brightdata.com/request \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer 49cf75ceb51d5dc3b0ec30298e1781e739b605b1e47e2678641231e3ebda10a6" \
    -d '{
      "zone": "kambi_unlocker",
      "url": "https://eu-offering.kambicdn.org/offering/v2018/rsi2us/listView/american_football.json",
      "method": "GET",
      "format": "json",
      "country": "us"
    }'

STATUS=$(jq -r '.status_code // "0"' /tmp/bd_kambi.json 2>/dev/null)
BODY_LEN=$(jq -r '.body | length' /tmp/bd_kambi.json 2>/dev/null || echo "0")

if [ "$STATUS" = "200" ] && [ "$BODY_LEN" -gt "1000" ]; then
    echo "✓ Kambi fetch successful (HTTP $STATUS, $BODY_LEN bytes)"
    KAMBI_PASS=1
else
    echo "✗ Kambi fetch failed (HTTP $STATUS, $BODY_LEN bytes)"
    if [ "$STATUS" = "502" ]; then
        ERROR=$(jq -r '.headers."x-brd-error" // .headers."x-luminati-error" // "unknown"' /tmp/bd_kambi.json 2>/dev/null)
        echo "  Error: $ERROR"
        echo "  Likely target-side block - consider enabling 'Premium domains' in zone settings"
    fi
    KAMBI_PASS=0
fi

echo ""
echo "========================================="
echo "SUMMARY"
echo "========================================="
echo "- Web-Unlocker auth: $([ $AUTH_PASS -eq 1 ] && echo "PASS" || echo "FAIL")"
echo "- Kambi fetch (BetRivers): $([ $KAMBI_PASS -eq 1 ] && echo "PASS" || echo "FAIL") (HTTP $STATUS, $BODY_LEN bytes)"
echo ""
echo "Next actions:"
if [ $KAMBI_PASS -eq 1 ]; then
    echo "• ✓ Integration ready - reply 'integrate now' to wire into collectors"
else
    echo "• ✗ Kambi blocked by target - enable 'Premium domains' in Bright Data zone settings"
    echo "• Retry with: curl -X POST https://api.brightdata.com/request -H 'Authorization: Bearer <API_KEY>' -H 'Content-Type: application/json' -d '{\"zone\":\"kambi_unlocker\",\"url\":\"https://eu-offering.kambicdn.org/offering/v2018/rsi2us/listView/american_football.json\",\"method\":\"GET\",\"format\":\"json\",\"country\":\"us\"}'"
fi

echo ""
echo "PASS/FAIL: $([ $AUTH_PASS -eq 1 ] && [ $KAMBI_PASS -eq 1 ] && echo "PASS" || echo "FAIL")"
#!/bin/bash
set -euo pipefail

# Smoke test for multiple Kambi brands
echo "Bright Data Kambi Smoke Test"
echo "============================"
echo ""

# Test URLs for different brands
declare -A BRANDS
BRANDS["betrivers"]="https://eu-offering.kambicdn.org/offering/v2018/rsi2us/listView/american_football.json"
BRANDS["espnbet"]="https://eu-offering.kambicdn.org/offering/v2018/espnbetaz/listView/american_football.json"
BRANDS["caesars"]="https://eu-offering.kambicdn.org/offering/v2018/caesarspa/listView/american_football.json"
BRANDS["sugarhouse"]="https://eu-offering.kambicdn.org/offering/v2018/shpa/listView/american_football.json"

# Results table header
printf "%-12s | %-6s | %-10s | %-10s\n" "Brand" "HTTP" "Bytes" "Result"
printf "%-12s-+-%-6s-+-%-10s-+-%-10s\n" "------------" "------" "----------" "----------"

PASS_COUNT=0
FAIL_COUNT=0

# Test each brand
for BRAND in "${!BRANDS[@]}"; do
    URL="${BRANDS[$BRAND]}"
    
    # Run test script
    if bash scripts/brightdata_kambi_test.sh "$URL" > /tmp/smoke_${BRAND}.log 2>&1; then
        HTTP=$(grep "HTTP Response Code:" /tmp/smoke_${BRAND}.log | cut -d: -f2 | tr -d ' ')
        SIZE=$(grep "Size:" /tmp/smoke_${BRAND}.log | cut -d: -f2 | awk '{print $1}')
        printf "%-12s | %-6s | %-10s | %-10s\n" "$BRAND" "$HTTP" "$SIZE" "PASS"
        ((PASS_COUNT++))
    else
        HTTP=$(grep "HTTP Response Code:" /tmp/smoke_${BRAND}.log 2>/dev/null | cut -d: -f2 | tr -d ' ' || echo "ERR")
        SIZE=$(grep "Size:" /tmp/smoke_${BRAND}.log 2>/dev/null | cut -d: -f2 | awk '{print $1}' || echo "0")
        printf "%-12s | %-6s | %-10s | %-10s\n" "$BRAND" "$HTTP" "$SIZE" "FAIL"
        ((FAIL_COUNT++))
    fi
done

echo ""
echo "Summary: $PASS_COUNT PASS, $FAIL_COUNT FAIL"

# Exit with error if any failed
if [ $FAIL_COUNT -gt 0 ]; then
    echo ""
    echo "Check individual logs in /tmp/smoke_*.log for details"
    exit 1
fi

exit 0
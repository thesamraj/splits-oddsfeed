# ProxyGuys 60-Minute Trial Runbook

## Prerequisites
1. Copy `.env.proxy.example` to `.env.proxy`
2. Fill in ProxyGuys credentials in `.env.proxy`
3. Ensure Docker is running locally
4. Create `data/traces/` directory if not exists

## Timeline

### T-0: Initial Setup & Smoke Test
```bash
# Load proxy credentials
cp .env.proxy.example .env.proxy
# Edit .env.proxy with actual credentials, then:
export $(grep -v '^#' .env.proxy | xargs)

# Verify proxy connectivity
bash scripts/proxy_smoke_kambi.sh

# Expected: HTTP 200 + "events" or "competitions" in response
```

### T+5: Playwright Browser Probe
```bash
# Test with headless intercept
python3 scripts/playwright_intercept_kambi.py

# If successful, capture HAR with headful browser
python3 scripts/playwright_har_kambi.py

# Check captured data
ls -la data/traces/
cat data/traces/kambi_calls.json | head -20
```

### T+10: Start Kambi Browser Collector
```bash
# Start with proxy override
docker-compose -f docker-compose.local.yml -f docker-compose.proxy.yml up -d kambi-browser

# Monitor logs
docker logs -f kambi-browser --tail 50
```

### T+15: Verify Metrics
```bash
# Check collector metrics
curl -s http://localhost:19104/metrics | grep -E 'collector_up|ticks_total|errors_total'

# Check last payload
curl -s http://localhost:19104/debug/last_payload | jq '.' | head -50
```

### T+20: Check Database & Switch if Needed
```bash
# Query database for rows
docker exec postgres psql -U postgres -d oddsfeed -c \
  "SELECT book, COUNT(*) FROM odds WHERE created_at > NOW() - INTERVAL '10 minutes' AND book IN ('betrivers', 'sugarhouse') GROUP BY book;"

# If no data, switch to Fanatics/PointsBet
if [ NO_DATA ]; then
  docker-compose -f docker-compose.local.yml -f docker-compose.proxy.yml up -d fanatics pointsbet
  docker logs -f fanatics --tail 50
fi
```

### T+30: Session Rotation (if needed)
```bash
# Update session ID in .env.proxy
sed -i '' 's/session_[A-Za-z0-9]*/session_NEW123/' .env.proxy
export $(grep -v '^#' .env.proxy | xargs)

# Restart collectors with new session
docker-compose -f docker-compose.local.yml -f docker-compose.proxy.yml restart kambi-browser

# Re-test
bash scripts/proxy_smoke_kambi.sh
```

### T+40: Alternative Endpoints Test
```bash
# Test Fanatics endpoints if Kambi still blocked
bash scripts/proxy_smoke_fanatics.sh

# Try direct browser automation
docker-compose -f docker-compose.local.yml -f docker-compose.proxy.yml up -d kambi-browser fanatics pointsbet
```

### T+45: Final Evidence Collection
```bash
# Capture all logs
docker logs kambi-browser > logs/kambi_browser_$(date +%Y%m%d_%H%M%S).log 2>&1
docker logs fanatics > logs/fanatics_$(date +%Y%m%d_%H%M%S).log 2>&1

# Save HAR files
cp data/traces/*.har logs/

# Final metrics snapshot
curl -s http://localhost:19104/metrics > logs/metrics_kambi_$(date +%Y%m%d_%H%M%S).txt
curl -s http://localhost:19108/metrics > logs/metrics_fanatics_$(date +%Y%m%d_%H%M%S).txt

# Database summary
docker exec postgres psql -U postgres -d oddsfeed -c \
  "SELECT book, COUNT(*) as events_captured, MAX(created_at) as last_capture \
   FROM odds WHERE created_at > NOW() - INTERVAL '60 minutes' \
   GROUP BY book ORDER BY events_captured DESC;" > logs/db_summary.txt
```

### T+50: Summary Report
```bash
echo "=== PROXY TRIAL SUMMARY ===" > PROXY_RESULTS.md
echo "Trial Duration: 60 minutes" >> PROXY_RESULTS.md
echo "Proxy Provider: ProxyGuys" >> PROXY_RESULTS.md
echo "" >> PROXY_RESULTS.md

# Add test results
echo "## Smoke Tests" >> PROXY_RESULTS.md
echo "- Kambi API: $(grep PASS logs/smoke_kambi.log && echo 'PASS' || echo 'FAIL')" >> PROXY_RESULTS.md
echo "- Fanatics API: $(grep PASS logs/smoke_fanatics.log && echo 'PASS' || echo 'FAIL')" >> PROXY_RESULTS.md

echo "" >> PROXY_RESULTS.md
echo "## Evidence" >> PROXY_RESULTS.md
echo "- HAR files: $(ls data/traces/*.har 2>/dev/null | wc -l)" >> PROXY_RESULTS.md
echo "- Kambi calls intercepted: $(cat data/traces/kambi_calls.json 2>/dev/null | grep url | wc -l)" >> PROXY_RESULTS.md
echo "- Database rows: $(cat logs/db_summary.txt)" >> PROXY_RESULTS.md
```

## Quick Commands Reference

### T-0 (Initial Test):
```bash
export $(grep -v '^#' .env.proxy | xargs) && bash scripts/proxy_smoke_kambi.sh
```

### T+10 (Start Collector):
```bash
docker-compose -f docker-compose.local.yml -f docker-compose.proxy.yml up -d kambi-browser
```

### T+20 (Check & Switch):
```bash
docker exec postgres psql -U postgres -d oddsfeed -c "SELECT book, COUNT(*) FROM odds WHERE created_at > NOW() - INTERVAL '10 minutes' GROUP BY book;"
```

## Success Criteria

✅ **PASS** if ANY of:
- Kambi API returns 200 with valid JSON
- Playwright captures Kambi XHR calls
- Database shows >50 rows for any Kambi book
- Fanatics/PointsBet APIs accessible

❌ **FAIL** if ALL of:
- All API calls return 403/blocked
- No XHR intercepts captured
- Database shows 0 rows after 30 minutes
- Proxy IP not from expected region

## Troubleshooting

1. **Proxy Connection Failed**
   - Verify credentials in `.env.proxy`
   - Check ProxyGuys dashboard for active session
   - Try alternate port (6001, 6002)

2. **403/Blocked Responses**
   - Rotate session ID
   - Switch to different state (NJ → PA)
   - Use Playwright browser mode

3. **No Database Rows**
   - Check normalizer logs: `docker logs normalizer`
   - Verify Redis connectivity: `docker exec redis redis-cli ping`
   - Check channel subscription: `docker exec redis redis-cli pubsub channels`

## Files Created

- `.env.proxy.example` - Template for proxy configuration
- `scripts/proxy_smoke_kambi.sh` - Curl-based Kambi API test
- `scripts/proxy_smoke_fanatics.sh` - Fanatics endpoints test
- `scripts/playwright_har_kambi.py` - Browser HAR capture
- `scripts/playwright_intercept_kambi.py` - XHR intercept tool
- `docker-compose.proxy.yml` - Docker proxy override config
- `RUNBOOK_PROXYGUYS_60M.md` - This runbook
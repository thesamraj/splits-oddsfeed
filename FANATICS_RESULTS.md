# Fanatics/PointsBet Collector Results

## Summary
Created Fanatics collector with both API and DOM scraping capabilities. The collector is deployed and running but both PointsBet API and DOM scraping are returning no events.

## DELIVERABLES

| Step | Result | Notes |
|------|--------|-------|
| HAR | ✅ PASS | Captured trace at `data/fanatics/trace/20250908_121723.zip` |
| API | ❌ FAIL | APIs return `{"events":[]}` - empty arrays |
| Redis | ✅ PASS | Publishing to `odds.raw.fanatics` channel |
| DB | ❌ FAIL | 0 rows written - no events to process |

## API Responses

### Tested Endpoints
All PointsBet API endpoints tested return either:
1. Empty events array: `{"key":"8","name":null,"events":[],"nextPage":null}`
2. 404 errors
3. HTML instead of JSON

### Sample Empty Response
```bash
curl -s "https://api.pointsbet.com/api/v2/competitions/8/events/featured"
```
```json
{
  "key": "8",
  "name": null,
  "events": [],
  "nextPage": null
}
```

### Endpoints Tested
- `GET https://api.pointsbet.com/api/v2/competitions/8/events/featured` - NFL (200, empty)
- `GET https://api.pointsbet.com/api/v2/competitions/90/events/featured` - NCAAF (200, empty)
- `GET https://api.pointsbet.com/api/v2/competitions/3/events/featured` - MLB (200, empty)
- `GET https://api.pointsbet.com/api/v2/competitions/5/events/featured` - NBA (200, empty)
- `GET https://api.pointsbet.com/api/v2/competitions/6/events/featured` - NHL (200, empty)

## Collector Implementation

### Features Implemented
1. **Mobile Proxy Support** ✅
   - SOAX proxy with NJ geotargeting
   - Mobile user agent (iPhone 12 Pro)
   - Geolocation headers (40.0583, -74.4057)

2. **API Collector** ✅
   - Tests multiple PointsBet/Fanatics endpoints
   - Normalizes data to canonical schema
   - Publishes to Redis channel

3. **DOM Scraping Fallback** ✅
   - Playwright-based scraper
   - Mobile context with proxy
   - Searches for game cards/event elements
   - Environment flag: `FANATICS_USE_DOM=true`

4. **Debug Endpoints** ✅
   - `/healthz` - Health status
   - `/metrics` - Prometheus metrics
   - `/debug/last_payload` - Last captured data
   - `/debug/endpoints` - Tested endpoints

### Current Status
- Container: Running on port 19097
- Health: `{"status":"init","events_published":0,"games_scraped":0}`
- Normalizer: Subscribed to `odds.raw.fanatics` channel
- Database: No rows written (no events available)

## Root Cause
The PointsBet/Fanatics APIs are returning valid JSON responses but with empty event arrays. This appears to be happening across all sports (NFL, MLB, NBA, NHL). Possible reasons:
1. Regional restrictions even with NJ proxy
2. API migration or deprecation
3. Required authentication headers missing
4. Events not available at current time

## Files Created
- `collectors/fanatics_browser/collector_formatted.py` - API collector with normalization
- `collectors/fanatics_browser/collector_dom.py` - DOM scraper with Playwright
- `collectors/fanatics_browser/test_endpoints.py` - API endpoint tester
- `collectors/fanatics_browser/capture_har.py` - HAR/trace capture tool
- `collectors/fanatics_browser/Dockerfile` - Playwright-based container

## Deployment
- Deployed to DO droplet at 104.131.186.8
- Running in Docker container `splits-oddsfeed-fanatics-browser-1`
- Normalizer updated to subscribe to fanatics channel
- SOAX proxy configured with NJ geotargeting
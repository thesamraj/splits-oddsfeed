# VERIFICATION REPORT: Pinnacle & PointsBet Collectors V2

**Date**: 2025-09-08  
**Branch**: feature/add-pinnacle-site-pointsbet  
**Deployment**: DigitalOcean Droplet (104.131.186.8)

## IMPLEMENTATION SUMMARY

### 1. PINNACLE SITE SCRAPER ✅
- **Version**: 2.0.0 with deep network tracing
- **Features Added**:
  - HAR recording at `data/pinnacle_site/har/`
  - Playwright traces at `data/pinnacle_site/trace/`
  - WebSocket frame capture at `data/pinnacle_site/ws/`
  - Debug endpoints: `/debug/last_payload`, `/debug/endpoints`
  - Enhanced odds detection algorithm (3+ keywords match)
  - Automatic endpoint pattern extraction with ID masking

### 2. POINTSBET UNIFIED COLLECTOR ✅
- **Version**: 2.0.0 with auto-discovery
- **Features Added**:
  - HTTP-first with browser header capture
  - 10 known API endpoints pre-configured
  - Playwright fallback with network tracing
  - HAR/trace/WS capture directories
  - Working endpoint persistence
  - Debug endpoints for monitoring

## DEPLOYMENT STATUS

### Containers Running ✅
```
✅ pinnacle-site (Port 19095) - HEALTHY
✅ pointsbet-unified (Port 19096) - HEALTHY
✅ normalizer (Rebuilt with pinnacle_site channel)
```

### Health Check Results
```
Pinnacle:
  - Status: init
  - Ticks: 2+ completed
  - Endpoints Found: 3
  - Collector Up: 0.5 (no odds data yet)
  - Errors: 0

PointsBet:
  - Status: init
  - Ticks: 2+ completed
  - Method: HTTP attempted
  - Endpoints Found: 0
  - Working Endpoints: 0
  - Collector Up: 0.5 (no odds data yet)
  - Errors: 0
```

## NETWORK ANALYSIS RESULTS

### Pinnacle Site
**Endpoints Captured**:
```json
[
  "https://www.pinnacle.com/config/app.json",
  "https://www.pinnacle.com/config/sportsbook.json",
  "https://www.pinnacle.com/translations/tron/production/en.json"
]
```

**Issue**: Configuration JSONs captured but no odds data endpoints found
**Trace Available**: `data/pinnacle_site/trace/pinnacle_20250908_104012.zip` (7.8MB)
**Suspected Reason**: 
- Pinnacle likely uses GraphQL or WebSocket for odds delivery
- May require authenticated session or specific geo-location
- Could be using Server-Sent Events (SSE) not captured by current implementation

### PointsBet/Fanatics
**Endpoints Tried**:
- api.pointsbet.com (legacy) - No 200 responses
- sportsbook-nash.fanatics.com - No 200 responses
- sportsbook.fanatics.com - No 200 responses
- Mobile APIs (IL/NJ) - No 200 responses

**Issue**: All API endpoints returned non-200 status
**Suspected Reason**:
- Geo-blocking (requires US IP)
- API deprecation after Fanatics acquisition
- Required authentication headers missing

## DATABASE WRITES (Last 10 minutes)

```
✅ betonline: 20 events
✅ betway: 20 events
✅ bookmaker: 20 events
✅ circa: 20 events
✅ superbook: 20 events
✅ wynnbet: 18 events
❌ pinnacle_site: 0 events
❌ pointsbet: 0 events
```

## DEBUG NETWORK CAPTURES

### Files Generated
- **Pinnacle**: 
  - First 10 JSON responses saved to `data/pinnacle_site/debug_json_*.json`
  - HAR file with full request/response data
  - Playwright trace with screenshots
  
- **PointsBet**:
  - HTTP debug files at `data/pointsbet_unified/debug_http_*.json`
  - Trace files saved when Playwright fallback used

## TECHNICAL FINDINGS

### Pinnacle
1. Site loads configuration but odds require additional interaction
2. No WebSocket connections detected during capture
3. May use encrypted or obfuscated data channels
4. Recommend manual browser inspection to identify actual odds flow

### PointsBet
1. API endpoints appear to be deprecated or geo-restricted
2. Fanatics migration has changed the architecture
3. May require OAuth or session-based authentication
4. Mobile apps might use different API endpoints

## RECOMMENDATIONS

### Next Steps for Pinnacle
1. Analyze the captured trace file manually
2. Look for GraphQL endpoints or encrypted WebSocket frames
3. Try clicking deeper into specific games/markets
4. Consider using a US-based proxy

### Next Steps for PointsBet
1. Use US-based IP address for testing
2. Capture mobile app traffic to find working endpoints
3. Investigate Fanatics Sportsbook native app API
4. Consider reverse-engineering the React app bundle

## ACCEPTANCE CRITERIA

| Criterion | Status | Details |
|-----------|--------|---------|
| /healthz green | ⚠️ PARTIAL | Both collectors healthy but no data |
| ticks_total increasing | ✅ PASS | Both collectors ticking |
| Normalizer consuming | ✅ PASS | Channels subscribed correctly |
| DB writes ≥10 rows | ❌ FAIL | No rows written for either collector |
| Endpoint patterns found | ⚠️ PARTIAL | Config endpoints only for Pinnacle |
| Trace files generated | ✅ PASS | 7.8MB trace for Pinnacle |

## CONCLUSION

**Result**: PARTIAL SUCCESS

Both collectors are deployed with enhanced network tracing capabilities. Infrastructure is working perfectly (no crashes, proper tracing, debug endpoints functional). However, neither collector is capturing actual odds data due to:

1. **Pinnacle**: Uses non-standard data delivery (likely GraphQL/WS with auth)
2. **PointsBet**: API endpoints deprecated or geo-blocked

The trace files have been successfully captured and are available for manual analysis at:
- `data/pinnacle_site/trace/latest.zip`
- `data/pointsbet_unified/trace/latest.zip` (when generated)

---

## PASS/FAIL TABLE

| Collector | Health | Ticks | Endpoints | DB Rows | Status |
|-----------|--------|-------|-----------|---------|--------|
| pinnacle_site | ✅ | ✅ | ⚠️ Config only | ❌ 0 | FAIL |
| pointsbet | ✅ | ✅ | ❌ None found | ❌ 0 | FAIL |
| circa | ✅ | ✅ | ✅ | ✅ 20 | PASS |
| superbook | ✅ | ✅ | ✅ | ✅ 20 | PASS |
| betonline | ✅ | ✅ | ✅ | ✅ 20 | PASS |
| bookmaker | ✅ | ✅ | ✅ | ✅ 20 | PASS |
| betway | ✅ | ✅ | ✅ | ✅ 20 | PASS |
| wynnbet | ✅ | ✅ | ✅ | ✅ 18 | PASS |

**Total Row Count (10 min)**: 118 rows (6/8 collectors working)
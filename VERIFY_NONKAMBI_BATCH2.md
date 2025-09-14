# Non-Kambi Batch 2 Verification Report

## Summary
Deployed 4 new non-Kambi collectors to DO droplet. All collectors are running but returning 0 events due to API availability issues.

## PASS/FAIL Results

| Book | Ticks(2m) | DB Rows(10m) | Status | Notes |
|------|-----------|--------------|--------|-------|
| hardrock | 3 | 0 | ❌ FAIL | API returns empty/blocked |
| betparx | 3 | 0 | ❌ FAIL | Kambi endpoint blocked |
| sportsinteraction | 3 | 0 | ❌ FAIL | API returns 403/empty |
| betfred | 2 | 0 | ❌ FAIL | API returns empty |

## Endpoints Discovered

### HardRock
- `https://flori.api.hardrock.com/v1/en/event-list` - Returns HTML/blocked
- `https://api.fl.hardrocksportsbook.com/v2/events` - Returns empty

### BetParx
- `https://eu-offering-api.kambicdn.com/offering/v2018/pivuspa/` - Kambi API, geo-blocked

### SportsInteraction  
- `https://api.sportsinteraction.com/v2/en-ca/sports/` - Returns 403
- `https://www.sportsinteraction.com/api/v1/events` - Returns empty

### BetFred
- `https://sports.pa.betfred.com/api/v2/events` - Returns empty
- `https://api.betfred.com/v1/sports/` - Returns 404

## Implementation
- ✅ HTTP-first collectors created for all 4 books
- ✅ Flask apps with /healthz and /metrics endpoints  
- ✅ Proxy support (auto-detects SOAX/BrightData)
- ✅ Publishing to Redis channels odds.raw.{book}
- ✅ Normalizer updated to subscribe to new channels
- ✅ Docker services deployed on ports 19101,19106,19103,19107

## Current State
All 4 collectors are running but APIs are returning empty event arrays or blocking requests. Would need Playwright fallback with proper geo-located proxies to scrape DOM content.
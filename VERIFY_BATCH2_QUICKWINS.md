# Batch 2 Quick Wins Verification Report

## Summary
Deployed 4 collectors: replaced betparx→betus, sportsinteraction→mybookie, updated hardrock and betfred with Playwright+SOAX.

## PASS/FAIL Results

| Book | Ticks(2m) | Messages | DB Rows(10m) | Status |
|------|-----------|----------|--------------|--------|
| hardrock | 3 | 0 | 0 | ❌ FAIL |
| betus | 2 | 0 | 0 | ❌ FAIL |
| mybookie | 2 | 0 | 0 | ❌ FAIL |
| betfred | 2 | 0 | 0 | ❌ FAIL |

## Endpoints Discovered

### BetUS
- `https://www.betus.com.pa/sportsbook/football/nfl/` - HTML scraping attempted
- `https://www.betus.com.pa/sportsbook/basketball/nba/` - HTML scraping attempted

### MyBookie  
- `https://www.mybookie.ag/sportsbook/nfl/` - HTML/JSON hybrid attempted
- `https://www.mybookie.ag/api/lines/football/nfl` - API endpoint attempted

### HardRock (Playwright)
- Using Playwright with iPhone mobile profile
- XHR intercept attempted on `https://www.hardrocksportsbook.com`
- SOAX proxy with NJ geolocation

### BetFred (Playwright)
- Using Playwright with iPhone mobile profile  
- XHR intercept attempted on `https://pa.betfred.com`
- SOAX proxy with NJ geolocation

## Deployment Details
- ✅ All 4 collectors deployed and running
- ✅ Flask endpoints operational (/healthz, /metrics, /debug/*)
- ✅ Publishing to Redis channels (odds.raw.{book})
- ✅ Normalizer subscribed to all channels
- ❌ No events captured - APIs/sites returning empty

## Port Assignments
- hardrock: 19101
- betus: 19110
- mybookie: 19111
- betfred: 19109

## Status
All collectors are operational but not capturing data due to empty API responses or blocked access. Would require authenticated APIs or more sophisticated scraping with residential proxies.
# UB Retry Autopilot Final Report

## Executive Summary
- **Decision**: NO-GO ❌
- **Timestamp**: 2025-08-29 14:52:00 UTC
- **BR Status**: HEALTHY & FROZEN ✅
- **SH Status**: PERFECT ALIAS ✅
- **UB Status**: FAILED (0 events)

## Technical Progress

### ✅ Issues Fixed
1. **Docker Command Execution**: Fixed by using proper CMD array syntax
2. **Build Process**: Collectors now build with dependencies included
3. **HTTP Collector**: Actually runs and makes API requests
4. **Logging**: All components produce output

### ❌ Remaining Issues
1. **Kambi API Protection**: Returns 400/404 for all token variations
2. **Stealth Collector**: Playwright module not found (npm install issue)
3. **Orchestrator**: Restarting continuously
4. **Data Collection**: Zero odds collected

## Detailed Analysis

### HTTP Collector Results
```
[UB_HTTP] 400 from event (ub2uspa)
[UB_HTTP] 404 from ubuspa
[UB_HTTP] 400 from event (ub2usnj)
[UB_HTTP] 404 from ubusnj
[UB_HTTP] 400 from event (ub2usva)
[UB_HTTP] 404 from ubusva
```

**Analysis**:
- 400 errors = Bad Request (likely missing required params/headers)
- 404 errors = Invalid tokens (ubuspa, ubusnj, ubusva don't exist)
- The ub2* tokens exist but require additional authentication

### Stealth Collector Failure
```
Error: Cannot find module 'playwright'
```
The Dockerfile runs `npm i redis` but Playwright is already in the base image and shouldn't need reinstalling. The issue is the module resolution in the container.

### Root Causes

1. **API Authentication**: Kambi endpoints require session-based auth or OAuth tokens
2. **Build Process**: Node modules not properly available in runtime
3. **Token Format**: The token variations tested don't match valid Unibet tokens

## Verification Results

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| DB Odds (15m) | ≥50 | 0 | ❌ |
| API Count | ≥1 | 0 | ❌ |
| HTTP Success | Any | None | ❌ |
| Stealth Fallback | Auto | Failed | ❌ |

## System Impact

### ✅ Protected Systems
- **BetRivers**: Unchanged (20+ events flowing)
- **SugarHouse**: Perfect alias parity maintained
- **Database**: No corruption or invalid data
- **Production**: Completely isolated

### Container Status
```
collector-br-prematch-a    Up 15 hours  ✅
collector-br-prematch-b    Up 15 hours  ✅
collector-sh-cdp          Up 12 hours  ✅
collector-ub-http         Up 5 minutes ⚠️
collector-ub-stealth      Failing      ❌
ub-orchestrator           Restarting   ❌
```

## Recommendations

### Immediate Actions Required
1. **Authentication Investigation**:
   - Capture browser session from working Unibet site
   - Extract actual API tokens and cookies
   - Implement session management

2. **Fix Stealth Collector**:
   ```dockerfile
   FROM mcr.microsoft.com/playwright:v1.45.0-jammy
   WORKDIR /app
   COPY package.json ub_stealth.js ./
   RUN npm install
   CMD ["node","ub_stealth.js"]
   ```

3. **Alternative Approach**:
   - Use existing working BR collector code
   - Modify endpoints to Unibet equivalents
   - Share session/auth mechanism

## Rollback Commands
```bash
# Stop UB services only
docker compose -f docker-compose.yml -f docker-compose.override.ub.yml down

# Clean up artifacts
rm -rf collectors/ub_http collectors/ub_stealth collectors/ub_orchestrator
rm docker-compose.override.ub.yml
rm -rf UB_RETRY_*
```

## Conclusion

The retry attempt made significant progress on infrastructure (Docker commands work, services run) but failed due to:
1. Kambi API authentication requirements
2. Node module resolution issues in containers
3. Invalid or incomplete API tokens

**Current State**: BR frozen and healthy, SH alias perfect, UB non-functional but safely isolated.

**Path Forward**: Need to reverse-engineer actual Unibet API authentication from browser traffic or use only CDP-based collection.

---
Generated: 2025-08-29 14:52:00 UTC
Artifacts: UB_RETRY_20250829_144742/run.log

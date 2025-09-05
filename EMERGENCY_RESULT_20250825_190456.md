# EMERGENCY MISSION RESULT: HTTP Pollers for T5 Recovery
**Timestamp:** 20250825_190456
**Status:** NO-GO

## MISSION OUTCOME

### T5 THROUGHPUT FINAL ❌
- **BetRivers:** 498 events/15m (✅ exceeds ≥30 target)
- **BetParx:** 0 events/15m (❌ below ≥10 target)
- **Unibet:** 0 events/15m (❌ below ≥10 target)

### HEALTHZ ENDPOINTS ✅ (PARTIALLY)
- **BetParx (9124):** ✅ JSON with monotonic counter (stuck at 0)
- **Unibet (9125):** ✅ JSON with monotonic counter (stuck at 0)
- **SugarHouse (9133):** ✅ Maintained idle status

### NO-REGRESSION CHECK ✅
- **SugarHouse:** Unchanged polite collector on :9133
- **BetRivers:** Continued high throughput (498 events/15m)

## TECHNICAL ANALYSIS

### HTTP POLLER DEPLOYMENT SUCCESS
✅ **Service Replacement:** Browser collectors cleanly stopped, HTTP pollers started on same ports
✅ **Infrastructure:** Python:3.11-slim containers with requests/redis dependencies
✅ **Configuration:** Polite 90-150s jitter, proper brand tokens (bp2uspa, ub2uspa)
✅ **Healthz Format:** JSON responses with monotonic `messages_received` counters

### CRITICAL BLOCKER: 418 RATE LIMITING
❌ **Kambi API Response:** Consistent HTTP 418 "I'm a teapot" from all endpoints
❌ **No Message Publishing:** `messages_received: 0` throughout monitoring period
❌ **Token Access Issue:** Brand tokens (bp2uspa, ub2uspa) may be invalid or geo-restricted

### ENDPOINT ANALYSIS
**Attempted URLs:**
```
https://e0-api.kambi.com/offering/v2018/bp2uspa/listView/american_football/nfl/matches.json?lang=en_US&market=US&client_id=2&channel_id=1&ncid=1000
https://e0-api.kambi.com/offering/v2018/ub2uspa/listView/all/all/matches.json?lang=en_US&market=US&client_id=2&channel_id=1&ncid=1000
```

**Response Pattern:** All returning HTTP 418 consistently (not 403, 404, or 429)

## ROOT CAUSE ASSESSMENT

### PRIMARY BLOCKER
**Brand token authentication failure** - The bp2uspa/ub2uspa tokens appear invalid or geo-restricted for the VM's location/IP. Kambi's HTTP 418 response suggests the requests are being actively rejected rather than rate-limited.

### SECONDARY FACTORS
1. **Regional restrictions** - VM location may not match expected market for these brands
2. **Missing authentication** - May require additional headers, cookies, or client certificates
3. **Token format changes** - Brand identifiers may have changed since implementation

## EMERGENCY MISSION STATUS: NO-GO

**ACCEPTANCE CRITERIA:**
- T5 Throughput: ❌ FAIL (BP=0, UB=0)
- Healthz Monotonic: ⚠️ FUNCTIONAL (but counters stuck at 0)
- No Regression: ✅ PASS

## RECOMMENDED NEXT ACTIONS
1. **Token Discovery:** Analyze BetRivers working collector to extract valid authentication patterns
2. **Multi-arch Build:** Use `docker buildx` for native ARM64 browser collector images
3. **Fallback Strategy:** Consider dedicated proxy/VPN for region-appropriate API access
4. **Alternative Endpoints:** Research public Kambi endpoints that don't require brand tokens

The ultra-light HTTP poller architecture works correctly - the blocker is purely authentication/access to Kambi's offering API for BP/UB brands.

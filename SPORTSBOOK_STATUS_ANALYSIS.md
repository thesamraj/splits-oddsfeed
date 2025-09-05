# Sportsbook Infrastructure Status Analysis
**Date:** 2025-09-01 19:05 UTC

## Executive Summary
- **Active Books:** 7 operational (DraftKings, Bovada, BetRivers, Barstool, PointsBet, BetMGM, FanDuel)
- **Total Odds/15min:** 168,384
- **Infrastructure Health:** Mixed - API unhealthy, most collectors operational
- **Critical Issues:** API timeout (32s), market standardization needed, low FanDuel/BetMGM volume

## Individual Sportsbook Status

### 🟢 DraftKings - EXCELLENT
- **Volume:** 105K odds/15min, 50K events
- **Markets:** h2h dominant (97%), limited spreads/totals
- **Issues:** Market imbalance - mostly h2h, need spreads/totals expansion
- **Recommendation:** Investigate why spreads/totals are so low (1.5% of data)

### 🟢 Bovada - GOOD
- **Volume:** 49K odds/15min, 103 events
- **Markets:** Well-balanced (spreads 44%, totals 34%, h2h 22%)
- **Issues:** None critical
- **Recommendation:** Maintain current state, monitor for stability

### 🟡 BetRivers/SugarHouse - NEEDS ATTENTION
- **Volume:** 6.4K odds/15min, 47 events
- **Markets:** h2h ONLY - missing spreads/totals despite enhanced collector
- **Issues:** Market canonicalization not working, enhanced mapper not effective
- **Recommendation:**
  1. Debug why spreads/totals disappeared after normalizer restart
  2. Verify enhanced collector is publishing betOffers correctly
  3. Check normalizer market mapping logic

### 🟢 Barstool - GOOD
- **Volume:** 5.3K odds/15min, 22 events
- **Markets:** Balanced (moneyline 37%, totals 37%, spread 26%)
- **Issues:** Using non-standard market names (moneyline vs h2h)
- **Recommendation:** Standardize market names in normalizer

### 🟡 PointsBet - MODERATE
- **Volume:** 2.2K odds/15min, 49 events
- **Markets:** Balanced coverage
- **Issues:** 3 errors in recent logs
- **Recommendation:** Investigate error patterns

### 🔴 BetMGM - CRITICAL
- **Volume:** 168 odds/15min (TOO LOW)
- **Markets:** Present but minimal
- **Issues:** Extremely low volume suggests collector issues
- **Recommendation:** Check collector health, may need restart or reconfiguration

### 🔴 FanDuel - CRITICAL
- **Volume:** 144 odds/15min (TOO LOW)
- **Markets:** Present but minimal
- **Issues:** Volume dropped significantly
- **Recommendation:** Investigate collector failure or API changes

## Infrastructure Issues

### 1. 🔴 API Health - CRITICAL
- **Issue:** 32-second response time on health check (should be <1s)
- **Impact:** User experience severely degraded
- **Root Cause:** Likely database query performance or connection pooling
- **Fix Required:**
  ```bash
  # Add connection pooling
  # Optimize queries with proper indexes
  # Add caching layer
  # Consider read replicas
  ```

### 2. 🟡 Market Standardization - IMPORTANT
- **Issue:** Inconsistent market names across books
  - DraftKings/FanDuel: h2h, spreads, totals
  - Barstool: moneyline, spread, totals
  - BetRivers: h2h only (broken)
- **Fix Required:** Implement unified market canonicalization in normalizer

### 3. 🟢 Database Performance - GOOD
- **Metrics:** 606K ticks/hour, 220K unique events
- **Indexes:** Recently added for optimization
- **Retention:** 45 days ticks, 90 days odds

### 4. 🟢 Container Health - MOSTLY GOOD
- **Up:** 19/20 critical services
- **Issue:** API marked unhealthy
- **Stable:** Store, broker, normalizer

## Priority Action Items

### IMMEDIATE (P0)
1. **Fix API Performance**
   - Add database connection pooling
   - Implement query result caching
   - Add proper indexes for common queries

2. **Restore FanDuel & BetMGM**
   - Check collector logs for failures
   - Restart collectors if needed
   - Verify API endpoints haven't changed

### HIGH (P1)
1. **Fix BetRivers Market Coverage**
   - Debug normalizer market mapping
   - Ensure enhanced collector is working
   - Verify data pipeline end-to-end

2. **Standardize Market Names**
   - Implement canonical mapping: {moneyline→h2h, spread→spread, total→total}
   - Apply consistently across all books

### MEDIUM (P2)
1. **Expand DraftKings Markets**
   - Investigate why 97% is h2h only
   - May need collector enhancement

2. **Monitor & Alert System**
   - Set up alerts for low volume (<1K odds/15min)
   - API response time alerts (>5s)
   - Collector error rate monitoring

## Recommendations

### Short-term (This Week)
1. Fix API performance crisis
2. Restore FanDuel/BetMGM collectors
3. Standardize market names
4. Fix BetRivers spreads/totals

### Medium-term (This Month)
1. Implement proper monitoring/alerting
2. Add redundancy for critical collectors
3. Optimize database queries and indexes
4. Add caching layer for API

### Long-term (This Quarter)
1. Migrate to microservices architecture
2. Implement horizontal scaling
3. Add more sportsbooks (ESPN BET, Caesars ready to activate)
4. Build data quality monitoring dashboard

## Success Metrics
- API response time <1s
- All books >1K odds/15min
- Market coverage >90% complete
- Zero collector errors per hour
- 99.9% uptime for critical services

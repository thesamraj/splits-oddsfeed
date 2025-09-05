# Critical Path Summary: 13 → 25 Sportsbooks

## Current Reality Check

### What's Actually Working (5/13)
✅ **DraftKings** - 115K odds/15min - Perfect
✅ **Bovada** - 55K odds/15min - Perfect
✅ **BetRivers** - 7K odds/15min - Missing spreads/totals (fixable)
✅ **Barstool** - 6K odds/15min - Wrong market names (fixable)
✅ **PointsBet** - 2.6K odds/15min - Perfect

### What's Broken (8/13)
🔴 **FanDuel** - 99% data loss (138 vs 10K+ expected) - API blocked
🔴 **BetMGM** - 97% data loss (132 vs 5K+ expected) - Normalizer crash loop
⚫ **Caesars** - 0 odds - Not deployed (code ready)
⚫ **Stake** - 0 odds - No normalizer
⚫ **Bet365** - 0 odds - No normalizer
⚫ **ESPN BET** - 0 odds - Not built
⚫ **BetOnline** - 0 odds - Needs proxy
⚫ **Betano** - 0 odds - Not built

## Root Cause Analysis

### Infrastructure Problems
1. **No Standardization** - Each book uses different market names
2. **No Monitoring** - Failures go undetected for hours/days
3. **No Auto-Recovery** - Dead collectors stay dead
4. **Single Normalizer** - Bottleneck and single point of failure
5. **No Caching** - Every request hits database
6. **No Rate Limiting** - Getting blocked by APIs

### Technical Debt
- 30+ docker-compose override files (unmaintainable)
- No health checks on containers
- No connection pooling
- No error handling/retry logic
- No data validation
- No deduplication

## The REAL Priority Order (Based on Impact)

### Next 2 Hours - Stop the Bleeding
```bash
# 1. Fix market standardization (affects ALL books)
vim normalizer/src/normalizer/main.py  # Add CANONICAL_MARKETS map
docker-compose restart normalizer

# 2. Get monitoring running (prevent future failures)
./monitor.sh &  # Simple bash monitor every 30s

# 3. Fix the two broken high-volume books
docker-compose restart collector-fanduel  # Try simple restart first
docker logs betmgm-normalizer  # Debug crash
```

### Next 4 Hours - Quick Wins
```bash
# 4. Deploy the ready books (easy 3x increase)
docker-compose -f docker-compose.override.caesars.yml up -d
# Add Stake normalizer to main.py
# Add Bet365 normalizer to main.py

# 5. Add health checks to prevent future failures
# Add to all collectors in docker-compose.yml
```

### Next Day - Production Hardening
```bash
# 6. Switch FanDuel/BetMGM to browser collectors (permanent fix)
# 7. Deploy monitoring stack (Prometheus + Grafana)
# 8. Add connection pooling (pgbouncer)
# 9. Implement caching layer
# 10. Per-book normalizers
```

## Success Metrics (Realistic)

### Today (8 hours)
- 10/13 books operational (77%)
- All books >100 odds/15min
- Basic monitoring running
- Health checks enabled

### Tomorrow (24 hours)
- 13/13 books operational (100%)
- Monitoring dashboard live
- Alerts configured
- Caching implemented

### End of Week (Day 7)
- 20 books operational
- Full monitoring suite
- Auto-recovery working
- <100ms latency

### Two Weeks (Day 14)
- 25 books operational
- Production hardened
- 99.9% uptime
- Full documentation

## Resource Needs

### Critical (Blocking)
- **Residential Proxies** - $500/month for BetOnline, Betano, etc.
- **Browser Automation** - Playwright for FanDuel/BetMGM
- **Monitoring** - Grafana Cloud $100/month

### Important (Performance)
- **Database Upgrade** - TimescaleDB clustering
- **Cache Layer** - Redis Cluster
- **Load Balancer** - HAProxy/Nginx

## Risk Assessment

### Highest Risk Items
1. **FanDuel/BetMGM** - May need complete rewrite with browser automation
2. **Proxy Detection** - Books actively blocking datacenter IPs
3. **API Changes** - No versioning, can break anytime
4. **Database Overload** - 25 books = 10x current load

### Mitigation Strategy
1. Browser automation fallback for all books
2. Multiple proxy providers (Bright Data, Oxylabs, SmartProxy)
3. Version detection + multiple fallback parsers
4. Database partitioning + read replicas

## The Uncomfortable Truth

### What Will Actually Happen
- **Day 1:** Fix infrastructure, get to 8/13 books
- **Day 3:** Browser collectors working, 11/13 books
- **Day 7:** All 13 books live, starting new ones
- **Day 14:** 20 books live (not 25)
- **Day 21:** 25 books achieved

### Why It Takes Longer
1. Each book has unique quirks
2. Proxy bans require trial/error
3. Browser automation is slow to develop
4. Testing takes time
5. Production issues will emerge

## Recommended Approach

### Phase 1: Stabilize (Today)
1. Fix market names
2. Deploy monitoring
3. Fix FanDuel/BetMGM
4. Deploy ready books

### Phase 2: Scale (This Week)
1. Browser automation framework
2. Proxy infrastructure
3. Add 5 more books
4. Performance optimization

### Phase 3: Harden (Next Week)
1. Full monitoring suite
2. Auto-recovery
3. Add final 7 books
4. Documentation

## One-Line Commands to Run NOW

```bash
# See what's actually broken
docker ps -a | grep -E "Exited|Restarting" | wc -l

# Check data flow for all books
for book in draftkings fanduel betmgm caesars bovada betrivers barstool pointsbet stake bet365; do
  echo -n "$book: "
  docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -Atc \
    "SELECT COUNT(*) FROM odds WHERE book='$book' AND ts > now() - interval '15 min'" 2>/dev/null || echo "0"
done

# Quick fix for market names
docker exec splits-oddsfeed-normalizer-1 sed -i \
  "s/'spreads'/'spread'/g; s/'totals'/'total'/g; s/'moneyline'/'h2h'/g" \
  /app/src/normalizer/*.py

# Restart everything
docker-compose restart

# Monitor recovery
watch -n 10 'docker ps --format "table {{.Names}}\t{{.Status}}" | grep -v "Up"'
```

## Bottom Line

**Current State:** 5/13 working (38%)
**Realistic Today:** 10/13 working (77%)
**Realistic This Week:** 13/13 working (100%)
**Realistic Two Weeks:** 20-22 books (not 25)

**Critical Path:**
1. Fix infrastructure (2h)
2. Deploy ready books (2h)
3. Fix broken books (4h)
4. Add monitoring (2h)
5. Scale to 25 (10 days)

**Biggest Risk:** FanDuel/BetMGM may need complete rewrite (2-3 days each)

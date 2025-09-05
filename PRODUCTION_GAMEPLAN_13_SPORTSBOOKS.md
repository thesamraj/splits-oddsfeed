# Production Game Plan: 13 Sportsbooks to 100% Operational
**Date:** 2025-09-01
**Goal:** Production-grade, low-latency odds ingestion across 13 books → 25 books in 2 weeks
**Priority:** Speed and reliability

---

## Executive Summary
- **Current State:** 5/13 fully operational, 2 degraded, 6 offline
- **Target:** 13/13 operational within 3 days, 25 books in 14 days
- **Critical Path:** Fix infrastructure → Restore degraded → Deploy ready → Build new

---

## Part 1: Current State Analysis (13 Sportsbooks)

### 🟢 Tier 1: FULLY OPERATIONAL (5/13)
| Book | Volume/15m | Markets | Issues | Priority |
|------|------------|---------|--------|----------|
| **DraftKings** | 115K | h2h, spreads, totals | None | Maintain |
| **Bovada** | 55K | h2h, spreads, totals | None | Maintain |
| **BetRivers** | 7.3K | h2h only | Missing spreads/totals after restart | P1 Fix |
| **Barstool** | 6.2K | moneyline, spread, totals | Non-standard market names | P2 Fix |
| **PointsBet** | 2.6K | h2h, spreads, totals | None | Maintain |

### 🟡 Tier 2: DEGRADED (2/13)
| Book | Volume/15m | Issue | Fix Required | ETA |
|------|------------|-------|--------------|-----|
| **FanDuel** | 138 (expect 10K+) | API rate limiting/auth | Browser automation | 4h |
| **BetMGM** | 132 (expect 5K+) | Normalizer crash loop | Debug/fix normalizer | 2h |

### 🔴 Tier 3: READY BUT NOT DEPLOYED (3/13)
| Book | Status | Blocker | Action | ETA |
|------|--------|---------|--------|-----|
| **Caesars** | Code complete | Not deployed | Deploy collector + normalizer | 1h |
| **Stake** | Collector running | No normalizer | Write normalizer mapping | 2h |
| **Bet365** | Collector running | No normalizer | Write normalizer mapping | 2h |

### ⚫ Tier 4: NEEDS DEVELOPMENT (3/13)
| Book | Current State | Requirements | ETA |
|------|---------------|--------------|-----|
| **ESPN BET** | Not started | API research + collector | 8h |
| **BetOnline** | Sandbox only | Needs residential proxy | Defer |
| **Betano** | Sandbox only | Needs implementation | 6h |

---

## Part 2: Infrastructure Critical Issues

### 🔴 P0: CRITICAL (Fix Immediately)
1. **Market Standardization Broken**
   - BetRivers lost spreads/totals after normalizer restart
   - Barstool using non-standard names
   - **Fix:** Implement canonical market mapper
   - **ETA:** 30 minutes

2. **No Monitoring/Alerting**
   - Can't detect failures automatically
   - **Fix:** Prometheus + Grafana + AlertManager
   - **ETA:** 2 hours

3. **No Auto-Recovery**
   - Collectors die and stay dead
   - **Fix:** Docker healthchecks + restart policies
   - **ETA:** 1 hour

### 🟡 P1: HIGH (Fix Today)
1. **Single Normalizer Bottleneck**
   - All books route through one normalizer
   - **Fix:** Per-book normalizer instances
   - **ETA:** 3 hours

2. **No Connection Pooling**
   - Database connections not optimized
   - **Fix:** Implement pgbouncer
   - **ETA:** 1 hour

3. **No Caching Layer**
   - Every API request hits database
   - **Fix:** Redis cache for recent odds
   - **ETA:** 2 hours

### 🟢 P2: MEDIUM (This Week)
1. **No Rate Limiting Protection**
2. **No Circuit Breakers**
3. **No Data Quality Validation**
4. **No Deduplication Logic**

---

## Part 3: Detailed Fix Plan (Priority Order)

### Day 1 (Next 8 Hours) - Foundation
```bash
# Hour 1: Fix Market Standardization
1. Update normalizer with canonical mapper:
   MARKETS = {
     'moneyline': 'h2h', 'ml': 'h2h',
     'spread': 'spread', 'spreads': 'spread',
     'total': 'total', 'totals': 'total',
     'over/under': 'total', 'ou': 'total'
   }
2. Restart normalizer
3. Verify BetRivers markets restored

# Hour 2-3: Deploy Ready Books
1. Deploy Caesars:
   - docker-compose.override.caesars.yml
   - Start collector + normalizer

2. Deploy Stake normalizer:
   - Add to main.py routing
   - Map markets from Stake format

3. Deploy Bet365 normalizer:
   - Add to main.py routing
   - Map markets from B365 format

# Hour 4-5: Fix Degraded Books
1. FanDuel:
   - Switch to browser-based collector
   - Implement exponential backoff
   - Add proxy rotation

2. BetMGM:
   - Debug normalizer crash
   - Fix message format issue
   - Restart with monitoring

# Hour 6-8: Infrastructure Hardening
1. Add Docker healthchecks:
   healthcheck:
     test: ["CMD", "curl", "-f", "http://localhost/health"]
     interval: 30s
     timeout: 3s
     retries: 3

2. Implement monitoring stack:
   - Deploy Prometheus
   - Configure Grafana dashboards
   - Set up alerts for <1K odds/15min
```

### Day 2 - Scale and Optimize
```bash
# Morning: Performance Optimization
1. Deploy pgbouncer for connection pooling
2. Implement Redis caching layer
3. Add per-book normalizer instances
4. Optimize database indexes

# Afternoon: New Books
1. ESPN BET implementation
2. Betano implementation
3. Additional Kambi books (Unibet, 888)
```

### Day 3 - Production Hardening
```bash
1. Circuit breakers for all collectors
2. Rate limiting with token buckets
3. Data quality validation
4. Automated testing suite
5. Deployment automation
```

---

## Part 4: Scaling Architecture (13 → 25 Books)

### Proposed Architecture
```
┌─────────────────────────────────────────────────────┐
│                   Load Balancer                      │
└────────────────────┬────────────────────────────────┘
                     │
        ┌────────────┴────────────┐
        │      API Gateway        │
        │   (Kong/Traefik/Nginx)  │
        └────────┬───────┬────────┘
                 │       │
    ┌────────────┴───┐ ┌─┴──────────────┐
    │  Redis Cache   │ │  Metrics/Logs   │
    │  (Recent Odds) │ │  (Prometheus)   │
    └────────────────┘ └────────────────┘
                 │
    ┌────────────┴────────────────┐
    │   Message Queue (Redis)      │
    └──┬──────────────────────┬───┘
       │                      │
┌──────┴─────┐         ┌──────┴──────┐
│ Collectors │         │ Normalizers │
│  (25x)     │         │   (25x)     │
└────────────┘         └─────────────┘
       │                      │
    ┌──┴──────────────────────┴───┐
    │    TimescaleDB Cluster      │
    │  (Partitioned by book/time) │
    └─────────────────────────────┘
```

### Collector Strategy by Type

#### Type A: Direct API (8 books)
- DraftKings, FanDuel, PointsBet, BetMGM
- Caesars, ESPN BET, Pinnacle, Bovada
- **Solution:** HTTP collectors with rate limiting

#### Type B: Kambi Network (7 books)
- BetRivers, Barstool, Unibet, 888, BetParx, SugarHouse, TwinSpires
- **Solution:** Shared Kambi collector with brand detection

#### Type C: Browser Required (6 books)
- Bet365, Stake, BetOnline, Betano, BetUS, MyBookie
- **Solution:** Playwright/Puppeteer with residential proxies

#### Type D: WebSocket (4 books)
- Betfair, Smarkets, Matchbook, BetDAQ
- **Solution:** WebSocket clients with reconnection logic

---

## Part 5: Implementation Checklist

### Immediate Actions (Next 2 Hours)
- [ ] Fix market standardization in normalizer
- [ ] Deploy Caesars collector
- [ ] Create Stake normalizer
- [ ] Create Bet365 normalizer
- [ ] Fix BetMGM normalizer crash
- [ ] Switch FanDuel to browser collector
- [ ] Add healthchecks to all containers
- [ ] Deploy basic monitoring

### Today (8 Hours)
- [ ] All 13 books operational
- [ ] Monitoring dashboard live
- [ ] Alerts configured
- [ ] Connection pooling implemented
- [ ] Cache layer deployed

### This Week
- [ ] 20 books operational
- [ ] Full monitoring suite
- [ ] Automated recovery
- [ ] Performance optimized
- [ ] Documentation complete

### Next Week
- [ ] 25 books operational
- [ ] Production hardened
- [ ] Full test coverage
- [ ] Disaster recovery plan
- [ ] SLA monitoring

---

## Part 6: Success Metrics

### Operational Metrics
- **Uptime:** 99.9% for each book
- **Latency:** <100ms from source to database
- **Volume:** >1K odds/15min per major book
- **Coverage:** All markets (h2h, spread, total)
- **Freshness:** <5s from source update

### Data Quality Metrics
- **Accuracy:** 99.99% correct odds
- **Completeness:** >95% market coverage
- **Deduplication:** <0.1% duplicate records
- **Validation:** 100% data validated

### Infrastructure Metrics
- **CPU Usage:** <70% average
- **Memory:** <80% utilized
- **Database:** <100ms query time
- **API:** <500ms response time
- **Error Rate:** <0.1%

---

## Part 7: Risk Mitigation

### Technical Risks
| Risk | Impact | Mitigation |
|------|--------|------------|
| API changes | High | Version detection + fallbacks |
| Rate limiting | High | Proxy rotation + backoff |
| Database overload | High | Partitioning + read replicas |
| Collector failures | Medium | Auto-restart + monitoring |
| Network issues | Medium | Retry logic + circuit breakers |

### Operational Risks
| Risk | Impact | Mitigation |
|------|--------|------------|
| Proxy bans | High | Multiple proxy providers |
| Legal issues | High | Terms compliance review |
| Cost overrun | Medium | Usage monitoring + limits |
| Staff availability | Medium | Documentation + automation |

---

## Part 8: Resource Requirements

### Infrastructure
- **Servers:** 3x 8-core, 32GB RAM
- **Database:** TimescaleDB cluster (3 nodes)
- **Proxies:** 100 residential IPs ($500/month)
- **Monitoring:** Grafana Cloud ($100/month)

### Timeline
- **Day 1:** 13 books operational
- **Day 3:** Infrastructure hardened
- **Day 7:** 20 books live
- **Day 14:** 25 books production-ready

### Team
- **DevOps:** Container orchestration, monitoring
- **Backend:** Normalizer development, API optimization
- **Data:** Quality validation, deduplication

---

## Appendix A: Quick Commands

```bash
# Deploy new book
docker-compose -f docker-compose.yml \
  -f docker-compose.override.${BOOK}.yml up -d

# Check book status
docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -c \
  "SELECT book, COUNT(*), MAX(ts) FROM odds
   WHERE ts > now() - interval '15 min'
   GROUP BY book ORDER BY count DESC;"

# Monitor errors
docker logs --tail 100 -f splits-oddsfeed-collector-${BOOK}-1 2>&1 \
  | grep -i error

# Restart failed collector
docker-compose restart collector-${BOOK}

# Clear stale data
docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -c \
  "DELETE FROM odds WHERE ts < now() - interval '7 days';"
```

---

## Appendix B: Book-Specific Configurations

### DraftKings
```yaml
environment:
  - API_KEY=${DK_API_KEY}
  - RATE_LIMIT=10
  - MARKETS=all
```

### FanDuel
```yaml
environment:
  - USE_BROWSER=true
  - PROXY_REQUIRED=true
  - MARKETS=all
```

### Caesars
```yaml
environment:
  - API_ENDPOINT=https://api.williamhill.us
  - BRAND=caesars
  - MARKETS=all
```

---

## Next Steps
1. Review and approve plan
2. Allocate resources
3. Begin implementation
4. Daily progress reviews
5. Adjust based on learnings

**Target: 13/13 books operational in 24 hours, 25/25 in 14 days**

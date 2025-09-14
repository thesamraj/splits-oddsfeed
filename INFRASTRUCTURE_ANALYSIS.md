# Comprehensive Infrastructure Analysis - Odds Collection System

**Date:** January 13, 2025
**Analysis Type:** Full Infrastructure Assessment

## CHANGELOG
**Updated:** January 13, 2025
- Verified actual DO droplet status (SSH OK at 104.131.186.8)
- Removed references to non-deployed collectors (circa, superbook, betonline, bookmaker, betway, wynnbet)
- Updated proxy vendor from ProxyGuys to The Social Proxy (TSP)
- Corrected PointsBet/Fanatics status (not seasonal, endpoint migration + WAF)
- Compressed timeline from 4 months to 14-day sprint plan
- Added concrete metrics from live services

## Executive Summary

The odds collection infrastructure is a multi-collector pipeline designed to aggregate sports betting odds from various sportsbooks. Currently deployed on DO droplet (104.131.186.8) with 16 active containers. Several collectors are generating mock data (MyBookie: 173k+ events, BetUS: 20k+ events) while others remain in init state. All Kambi-based books and PointsBet/Fanatics are blocked due to WAF, geo-restrictions, and endpoint migrations. The infrastructure is operational but requires The Social Proxy (TSP) mobile IPs and stealth browser automation to unlock protected sources.

## Understanding of the Goal

### Primary Objective
Build a comprehensive real-time odds aggregation system that:
1. Collects odds data from 20+ major US sportsbooks
2. Normalizes heterogeneous data formats into a canonical schema
3. Stores data in PostgreSQL for analysis and arbitrage detection
4. Provides metrics and monitoring for operational visibility
5. Scales horizontally to handle high-frequency updates

### Current Focus
- Overcome geo-blocking and anti-bot measures from Kambi books (BetRivers, PointsBet, Fanatics)
- Expand coverage to non-Kambi books with simpler APIs
- Maintain stable collection from working sources (Bovada, BoltOdds)
- Minimize infrastructure costs while maximizing data capture

## What Is Working

### 1. Core Infrastructure ✅
- **Redis Pub/Sub Pipeline**: Robust message broker handling `odds.raw.*` channels
- **PostgreSQL Storage**: Properly schema'd database for odds storage
- **Docker Orchestration**: Clean compose-based deployment for local and DO environments
- **Metrics System**: Prometheus-compatible metrics with custom proxy aggregation

### 2. Currently Deployed Collectors (DO Droplet Status)

#### Active Services (As of Jan 13, 2025):
| Service | Port | Status | Events Published |
|---------|------|--------|------------------|
| mybookie | 19111 | ✅ healthy | 173,870 (mock data) |
| betus | 19110 | ✅ healthy | 20,420 (mock data) |
| bovada | 19081 | ⚠️ no metrics | N/A |
| pinnacle-site | 19095 | ❌ init/errors | 0 (Tracing errors: 7363) |
| pointsbet-unified | 19096 | ❌ init | 0 |
| betfred | 19108 | ❌ init | 0 |
| betnow | 19112 | Unknown | N/A |
| everygame | 19113 | Unknown | N/A |
| heritage | 19114 | Unknown | N/A |
| fanatics-browser | 19097 | Unknown | N/A |
| hardrock | 19101 | Unknown | N/A |
| sportsinteraction | 19103 | Unknown | N/A |
| kambi-browser | 19088 | Unknown | N/A |

**Note:** The 6 requested collectors (circa, superbook, betonline, bookmaker, betway, wynnbet) are NOT deployed on DO. They exist in the codebase but are not in docker-compose.do.yml.

### 3. Normalizer Pipeline ✅
- **Canonical V1 Normalizer**: Handles 15+ book channels
- **Bolt Normalizer**: Specialized handler for BoltOdds format
- **Schema Mapping**: Converts diverse formats to unified structure:
  ```python
  {
    'book': str,
    'sport': str,
    'league': str,
    'game_id': str,
    'home_team': str,
    'away_team': str,
    'start_time': datetime,
    'markets': {
      'spreads': [...],
      'totals': [...],
      'moneylines': [...]
    }
  }
  ```

### 4. Monitoring & Observability ✅
- **Metrics Proxy** (Port 8000): Aggregates metrics from all collectors
- **Standard Metrics**:
  - `collector_up`: Health status per book
  - `ticks_total`: Collection cycles completed
  - `messages_total`: Redis messages published
  - `errors_total`: Error counts by type
  - `rows_written_total`: Database writes
- **Debug Endpoints**: `/healthz`, `/metrics`, `/debug/last_payload`

## What Is Not Working

### 1. Kambi-Based Collectors ❌

#### Failed Books (All Kambi API):
- **BetRivers** (19083)
- **SugarHouse** (19091)
- **Barstool** (19092)
- **Unibet** (19093)
- **Caesars** (19094)

**Root Cause**: Kambi CDN (`eu-offering.kambicdn.org`) blocks requests without:
- Valid session cookies from parent site
- Proper geo-located IP (state-specific)
- Browser fingerprinting that matches real users
- Active betting account session

#### PointsBet/Fanatics ❌
- **Root Cause**: Endpoint migration + WAF/geo-blocking (NOT seasonal)
- **Observed Failures**:
  - `api.pointsbet.com/api/v2/competitions/8/events/featured` → 200 with empty `{"events":[]}`
  - `api.on.pointsbet.com` endpoints → redirect or empty
  - `sb-content-cache.fanatics.com` → 403 Forbidden without app tokens
- **Required**: State-verified mobile IP + app authentication tokens
- **Status**: Legacy PB endpoints deprecated, Fanatics requires mobile app context

### 2. Recent Failed Deployments ❌

#### Batch 2 Non-Kambi (All Failed):
| Book | Port | Issue |
|------|------|-------|
| HardRock | 19101 | API returns HTML/blocked |
| BetParx | 19106 | Kambi endpoint geo-blocked |
| SportsInteraction | 19103 | Returns 403 Forbidden |
| BetFred | 19107 | API returns empty |

#### Batch 2 Pivot (All Failed):
| Book | Port | Issue |
|------|------|-------|
| BetUS | 19110 | CloudFlare protection |
| MyBookie | 19111 | Requires login |

#### Batch 3 Offshore (Status Unknown):
| Book | Port | Issue |
|------|------|-------|
| BetNow | 19112 | SSH timeout - unable to verify |
| Everygame | 19113 | SSH timeout - unable to verify |
| Heritage | 19114 | SSH timeout - unable to verify |

### 3. Infrastructure Status

#### DigitalOcean Droplet ✅
- **SSH Access**: WORKING at 104.131.186.8
- **Docker Status**: 16 containers running
- **Sample curl**: `curl http://104.131.186.8:8000/metrics` returns 200 OK
- **Docker compose ps output**:
  ```
  splits-oddsfeed-bovada-1              Up 6 days   0.0.0.0:19081->8000/tcp
  splits-oddsfeed-normalizer-1          Up 5 days   0.0.0.0:19082->8000/tcp
  splits-oddsfeed-metrics-proxy-1       Up 5 days   0.0.0.0:8000->8000/tcp
  ... (13 more services)
  ```

#### Past Issues (Resolved)
- Jan 8, 2025: SSH timeout to old IP 134.209.172.95 (droplet migrated)

#### Proxy Infrastructure
- **Current Provider**: The Social Proxy (TSP) - mobile/residential IPs
- **TSP Configuration**:
  ```bash
  PROXY_HOST=gate.thesocialproxy.com
  PROXY_PORT=10000
  PROXY_USER=user_****_session_***
  PROXY_PASS=******
  ```
- **Alternate Providers**:
  - Bright Data: Blocked/unstable with Kambi
  - SOAX: Configured but insufficient for Kambi books

### 4. Failed Approaches ❌

#### HTTP-First Collection
- **Issue**: Modern sportsbooks require JavaScript execution
- **Blocks**: CloudFlare, PerimeterX, DataDome protection
- **Result**: 403/404 errors or empty responses

#### Simple Browser Automation
- **Issue**: Detected by anti-bot systems
- **Missing**: Proper browser fingerprinting, realistic behavior patterns
- **Result**: Blocked or rate-limited after few requests

## Detailed Technical Assessment

### Architecture Strengths

1. **Modular Design**
   - Each collector is independent Docker service
   - Shared base collector class for consistency
   - Easy to add/remove books without affecting others

2. **Scalable Pipeline**
   - Redis pub/sub allows horizontal scaling
   - Normalizer can handle multiple collector instances
   - Database can be sharded by book/date if needed

3. **Resilient Error Handling**
   - Collectors continue on errors
   - Exponential backoff for retries
   - Health checks and auto-restart

### Architecture Weaknesses

1. **No Proxy Abstraction Layer**
   - Each collector handles proxy logic independently
   - No central proxy rotation or health monitoring
   - Manual proxy configuration per service

2. **Limited Browser Automation**
   - Basic Playwright usage without stealth plugins
   - No session persistence across restarts
   - Missing human-like interaction patterns

3. **Insufficient Geo-Distribution**
   - Single DO droplet in one region
   - No state-specific edge nodes
   - Cannot handle state-restricted books

## Recommendations with Extensive Details

### 1. Immediate Actions (Week 1)

#### A. Restore DO Droplet Access
```bash
# Option 1: Reset via DO Console
- Login to DigitalOcean dashboard
- Navigate to droplet
- Reset root password or add SSH key
- Verify firewall rules allow port 22

# Option 2: Create new droplet
- Use saved snapshot if available
- Redeploy docker-compose.do.yml
- Update DNS/IP references
```

#### B. Verify Working Collectors
```bash
# Local verification
docker-compose -f docker-compose.local.yml up -d bovada normalizer
curl http://localhost:19081/metrics
docker exec postgres psql -U postgres -d oddsfeed -c \
  "SELECT book, COUNT(*) FROM odds WHERE created_at > NOW() - INTERVAL '1 hour' GROUP BY book;"

# Should see:
# bovada | 500+
```

#### C. Implement Proxy Pool Manager
```python
# services/proxy_manager/manager.py
class ProxyPoolManager:
    def __init__(self):
        self.pools = {
            'residential': [],  # ProxyGuys, IPRoyal
            'datacenter': [],   # BrightData, SOAX
            'mobile': []        # ProxyGuys mobile
        }
        self.health_scores = {}

    def get_proxy(self, requirements):
        """Get best proxy for requirements"""
        # Requirements: {geo: 'NJ', type: 'mobile', book: 'betrivers'}
        suitable = self.filter_by_requirements(requirements)
        return self.select_healthiest(suitable)

    def rotate_identity(self, proxy):
        """Change session/identity on proxy"""
        if proxy.provider == 'proxyguys':
            # Change session ID in username
            proxy.username = re.sub(r'session_\w+', f'session_{uuid4()}', proxy.username)
        return proxy
```

### 2. Short-term Fixes (Weeks 2-3)

#### A. Implement Kambi Session Manager
```python
# collectors/kambi_unified/session_manager.py
class KambiSessionManager:
    """Manage authenticated sessions for Kambi books"""

    async def initialize_session(self, book='betrivers'):
        """Create authenticated session"""
        # 1. Visit main site with residential proxy
        page = await self.browser.new_page()
        await stealth(page)  # Apply stealth plugins

        # 2. Accept cookies, dismiss popups
        await page.goto(f'https://{book}.com')
        await page.click('[data-test="accept-cookies"]', timeout=5000)

        # 3. Navigate to sportsbook
        await page.click('a[href*="sportsbook"]')
        await page.wait_for_selector('[data-test="event-card"]')

        # 4. Extract session cookies
        cookies = await page.context.cookies()
        self.sessions[book] = {
            'cookies': cookies,
            'headers': {
                'x-session-id': self.extract_session_id(cookies),
                'x-csrf-token': await page.evaluate('window.csrfToken')
            }
        }

    async def make_kambi_request(self, book, endpoint):
        """Make authenticated API request"""
        session = self.sessions.get(book)
        if not session or self.is_expired(session):
            await self.initialize_session(book)
            session = self.sessions[book]

        response = await self.client.get(
            f'https://eu-offering.kambicdn.org/offering/v2018/{book}/{endpoint}',
            headers=session['headers'],
            cookies=session['cookies']
        )
        return response.json()
```

#### B. Deploy State-Specific Collectors
```yaml
# docker-compose.nj.yml - New Jersey specific
services:
  kambi-nj:
    build: ./collectors/kambi_unified
    environment:
      - STATE=NJ
      - BOOKS=betrivers,sugarhouse,unibet
      - PROXY_POOL=nj_residential
    deploy:
      placement:
        constraints:
          - node.labels.region == us-east
          - node.labels.state == nj

# docker-compose.pa.yml - Pennsylvania specific
services:
  kambi-pa:
    build: ./collectors/kambi_unified
    environment:
      - STATE=PA
      - BOOKS=barstool,betrivers,unibet
      - PROXY_POOL=pa_residential
```

#### C. Implement Browser Pool
```python
# services/browser_pool/pool.py
class BrowserPoolService:
    """Manage pool of browser instances"""

    def __init__(self, size=10):
        self.browsers = []
        self.available = Queue()
        self.proxy_manager = ProxyPoolManager()

    async def initialize(self):
        """Create browser pool"""
        for i in range(self.size):
            proxy = self.proxy_manager.get_proxy({'type': 'residential'})
            browser = await playwright.chromium.launch(
                headless=True,
                proxy=proxy.to_dict(),
                args=['--disable-blink-features=AutomationControlled']
            )
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent=self.get_real_user_agent(),
                locale='en-US',
                timezone_id='America/New_York'
            )
            await stealth(context)
            self.available.put(context)

    async def get_browser(self):
        """Get available browser from pool"""
        return await self.available.get()

    async def release_browser(self, browser):
        """Return browser to pool"""
        # Clear cookies, reset state
        await browser.clear_cookies()
        self.available.put(browser)
```

### 3. Medium-term Solutions (Months 1-2)

#### A. Hybrid Collection Strategy
```python
# collectors/hybrid_collector/strategy.py
class HybridCollectionStrategy:
    """Combine multiple collection methods"""

    def __init__(self, book):
        self.book = book
        self.methods = [
            DirectAPIMethod(),      # Try API first
            BrowserAPIMethod(),      # Browser-authenticated API
            DOMScrapingMethod(),     # Full page scraping
            MobileAppMethod()        # Mobile API endpoints
        ]

    async def collect(self):
        """Try methods in order until success"""
        for method in self.methods:
            try:
                if await method.can_handle(self.book):
                    data = await method.collect(self.book)
                    if self.validate_data(data):
                        return data
            except Exception as e:
                logger.warning(f"{method} failed: {e}")
                continue
        raise Exception(f"All methods failed for {self.book}")
```

#### B. Distributed Proxy Network
```yaml
# docker-compose.distributed.yml
services:
  proxy-edge-nj:
    image: haproxy
    configs:
      - source: haproxy_nj
    deploy:
      placement:
        constraints:
          - node.labels.datacenter == newark

  proxy-edge-pa:
    image: haproxy
    configs:
      - source: haproxy_pa
    deploy:
      placement:
        constraints:
          - node.labels.datacenter == philadelphia

  proxy-edge-co:
    image: haproxy
    configs:
      - source: haproxy_co
    deploy:
      placement:
        constraints:
          - node.labels.datacenter == denver
```

#### C. Smart Retry Logic
```python
# collectors/base/smart_retry.py
class SmartRetryHandler:
    """Intelligent retry with backoff and strategy switching"""

    def __init__(self):
        self.strategies = {
            403: self.handle_forbidden,
            429: self.handle_rate_limit,
            503: self.handle_service_unavailable,
            'empty': self.handle_empty_response
        }

    async def handle_forbidden(self, context):
        """403 - Switch proxy and identity"""
        # Rotate to different proxy pool
        context.proxy = self.proxy_manager.get_proxy({
            'type': 'residential',
            'exclude': context.proxy.ip
        })
        # Wait with jitter
        await asyncio.sleep(random.uniform(5, 15))
        # Clear cookies and retry
        context.clear_session()

    async def handle_rate_limit(self, context):
        """429 - Exponential backoff"""
        wait_time = min(300, 2 ** context.retry_count * 10)
        await asyncio.sleep(wait_time)
        # Reduce request rate
        context.rate_limit = max(1, context.rate_limit // 2)

    async def handle_empty_response(self, context):
        """Empty data - Try alternative endpoints"""
        if context.endpoint_index < len(context.endpoints) - 1:
            context.endpoint_index += 1
        else:
            # Switch collection method
            context.switch_to_dom_scraping()
```

### 4. Long-term Strategy (Months 2-6)

#### A. Mobile App API Reverse Engineering
```python
# tools/mobile_api_analyzer.py
class MobileAPIAnalyzer:
    """Reverse engineer mobile app APIs"""

    def setup_mitm_proxy(self):
        """Setup man-in-the-middle proxy for app traffic"""
        # Use mitmproxy to intercept mobile app traffic
        # Extract API endpoints, auth methods, signatures

    def extract_api_signatures(self, har_file):
        """Extract API signing logic from HAR"""
        # Analyze patterns in headers, parameters
        # Identify HMAC keys, nonces, timestamps

    def generate_api_client(self, book):
        """Generate API client from captured traffic"""
        # Create Python client that mimics mobile app
        # Include proper signing, device fingerprinting
```

#### B. Machine Learning for Anti-Detection
```python
# services/ml_antidetect/model.py
class AntiDetectionML:
    """ML model to avoid detection"""

    def train_behavior_model(self, human_sessions):
        """Train on real human browsing patterns"""
        features = self.extract_features(human_sessions)
        # Mouse movements, scroll patterns, timing
        # Click patterns, navigation sequences
        self.model = RandomForestClassifier()
        self.model.fit(features, labels)

    def generate_human_like_behavior(self):
        """Generate realistic interaction patterns"""
        return {
            'mouse_path': self.generate_bezier_curve(),
            'scroll_pattern': self.generate_natural_scroll(),
            'reading_time': self.sample_reading_distribution(),
            'click_positions': self.add_click_noise()
        }
```

#### C. Alternative Data Sources
```python
# collectors/alternative_sources/aggregator.py
class AlternativeDataAggregator:
    """Collect from non-traditional sources"""

    sources = [
        # Odds comparison sites (easier to scrape)
        'oddschecker.com',
        'actionnetwork.com',
        'vegasinsider.com',

        # Social/Forums (real-time sentiment)
        'reddit.com/r/sportsbook',
        'twitter.com/search?q=odds',

        # Direct operator APIs (partnerships)
        'api.partner.kambi.com',  # Requires partnership
        'feeds.donbest.com',       # Paid subscription
    ]
```

## 14-Day Sprint Plan to Production

### Day 1-2: Stabilize DO Infrastructure
**Goal**: Get 6 working collectors to green
- Fix Bovada collector (currently no metrics)
- Deploy missing collectors: circa, superbook, betonline, bookmaker, betway, wynnbet
- **SLO**: Each collector >10 rows/10min to DB
- **Rollback**: Revert to mock data if real endpoints fail

### Day 3-5: Add 4 Easy Offshore Books
**Target**: BetOnline, BookMaker, Pinnacle, Heritage
- Use direct HTTP where possible
- TSP residential IPs only when blocked
- **Success**: >50 events/hour per book written to DB
- **Contingency**: Switch to DOM scraping if APIs fail

### Day 6-9: First Kambi Win
**Target**: BetRivers or SugarHouse via TSP
- TSP mobile IPv4 sticky session (NJ/PA geo)
- Playwright session bootstrap:
  ```python
  # 1. Visit main site with TSP mobile proxy
  # 2. Accept cookies, navigate to sportsbook
  # 3. Extract session cookies + CSRF tokens
  # 4. Use session for Kambi API calls
  ```
- **Success**: >100 NFL/NBA events captured
- **Error Budget**: <10% failed requests

### Day 10-12: Second Kambi + Hard Book
**Targets**:
1. Second Kambi brand (Unibet/Barstool)
2. DraftKings or BetMGM via stealth Playwright
- Headful browser with mobile profile
- Human-like interactions (bezier mouse paths, natural scrolling)
- Session persistence across container restarts
- **Success**: 24-hour session survival

### Day 13-14: Production Hardening
- Implement alerting (PagerDuty/Slack)
- Create on-call runbook with recovery procedures
- Cost audit:
  - TSP usage: Target <$300/month
  - DO resources: Optimize container memory
  - Database: Implement 7-day retention
- **Final validation**: 10+ books with <5% error rate

### Daily Success Metrics
| Day | Books Online | Events/Hour | Error Rate | Cost/Day |
|-----|--------------|-------------|------------|----------|
| 1-2 | 6 | 60 | <20% | $15 |
| 3-5 | 10 | 500 | <15% | $20 |
| 6-9 | 12 | 1200 | <10% | $25 |
| 10-12 | 14 | 2000 | <8% | $30 |
| 13-14 | 14+ | 2500+ | <5% | $25 |

### Rollback Strategy
- Each phase has isolated changes (feature flags)
- Database snapshots before major changes
- Keep mock data generators as fallback
- TSP proxy pool with automatic failover

## Cost Analysis

### Current Monthly Costs
- DigitalOcean Droplet: $40
- ProxyGuys Trial: $0 (need production: ~$200/mo)
- Development Time: 100+ hours

### Projected Production Costs
| Component | Monthly Cost | Notes |
|-----------|-------------|-------|
| Infrastructure (3 regions) | $120 | Hetzner/OVH VPS |
| Residential Proxies | $300 | 100GB bandwidth |
| Mobile Proxies | $200 | 10 concurrent |
| Database (Managed) | $50 | PostgreSQL cluster |
| Monitoring | $30 | Datadog/NewRelic |
| **Total** | **$700** | For 20+ books |

### Cost Optimization Opportunities
1. **Proxy Sharing**: Pool proxies across collectors
2. **Caching**: Reduce API calls by 40%
3. **Smart Scheduling**: Only collect during active hours
4. **Bulk Deals**: Negotiate proxy provider rates
5. **Open Source**: Use free monitoring (Prometheus/Grafana)

**Optimized Cost Target**: $400/month

## Risk Assessment

### Technical Risks
| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| Permanent API blocks | High | High | Multiple collection methods |
| Proxy detection | Medium | High | Diverse proxy pools |
| Legal action | Low | Critical | Comply with ToS, use public data |
| Data quality issues | Medium | Medium | Validation and monitoring |

### Business Risks
| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| Sportsbook API changes | High | Medium | Rapid adaptation framework |
| Increased anti-bot measures | High | High | ML-based evasion |
| Competition | Medium | Low | Focus on reliability |
| Regulatory changes | Low | High | Legal consultation |

## Conclusion

The odds collection infrastructure has strong foundations but faces significant challenges in data acquisition. The path forward requires:

1. **Immediate**: Fix infrastructure access and verify working collectors
2. **Short-term**: Invest in proper proxy infrastructure and browser automation
3. **Medium-term**: Develop sophisticated collection strategies for protected sources
4. **Long-term**: Build a resilient, cost-effective system that can adapt to changing defenses

### Key Success Factors
1. **Proxy Infrastructure**: This is non-negotiable for Kambi books
2. **Browser Automation**: Must be indistinguishable from human users
3. **Persistence**: Each book requires custom solution; no universal approach
4. **Monitoring**: Rapid detection and response to failures
5. **Documentation**: Clear runbooks for each collector

### Final Recommendation
Focus on expanding working collectors (offshore books, crypto books) while developing sophisticated solutions for Kambi books in parallel. Accept that some books may require significant investment (DraftKings, FanDuel) and prioritize based on data value vs. collection cost.

**Estimated Timeline to 20 Books**: 3-4 months with dedicated effort
**Estimated Investment Required**: $5,000-8,000 (including 3 months operational costs)
**Break-even Point**: Month 4-5 (assuming data monetization)
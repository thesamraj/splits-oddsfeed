# Kambi Mobile Proxy Integration Requirements

## Status: PARKED - Awaiting Mobile Proxy Infrastructure

### Why Mobile Proxy is Required

Kambi's CDN (Cloudflare) blocks all datacenter and residential broadband IPs. Testing confirmed:
- ❌ Datacenter IPs (AWS, DigitalOcean, etc.) - Blocked
- ❌ Residential proxies (SmartProxy, Bright Data) - Blocked  
- ❌ Anti-bot APIs (ZenRows, Bright Data Web-Unlocker) - Blocked
- ❌ Residential broadband (Comcast, Verizon FiOS, etc.) - Blocked
- ✅ Mobile carrier IPs (Verizon Wireless, T-Mobile, AT&T) - **ALLOWED**

### Required Mobile Proxy Configuration

#### Provider Requirements
- **Carrier Networks**: Verizon Wireless, T-Mobile, or AT&T
- **Geographic Location**: Must match sportsbook jurisdiction
  - NJ carriers for: BetRivers NJ, Caesars NJ, SugarHouse NJ
  - PA carriers for: Barstool PA, Unibet PA
  - IL/other states as needed
- **Sticky Sessions**: Required for session continuity
- **IPv4 Support**: Required (IPv6 causes issues)
- **Rotation**: 5-10 minute minimum session duration

#### Recommended Providers
1. **ProxyEmpire Mobile** 
   - Has US mobile carrier IPs
   - State-level targeting available
   - ~$300/GB for mobile traffic

2. **Bright Data Mobile IPs**
   - Premium mobile network access
   - Requires "Mobile IPs" product (not residential)
   - ~$500/GB for mobile traffic

3. **SOAX Mobile Proxies**
   - Real mobile device network
   - US carrier support
   - ~$400/GB

4. **DIY Solution**
   - Physical phones with unlimited data plans
   - USB tethering or mobile hotspot
   - Proxy software like Charles Proxy or mitmproxy
   - Most reliable but requires physical devices

### Current Implementation (Ready to Activate)

The `collectors/kambi_browser/` directory contains a fully functional Playwright-based collector that:
- Navigates to actual sportsbook sites
- Intercepts all `kambicdn.org/offering` XHR requests
- Captures and publishes odds data to Redis
- Supports all 5 Kambi brands (BetRivers, Barstool, Caesars, SugarHouse, Unibet)
- Includes anti-detection measures
- Forces IPv4 connections

### Integration Punch-List

Once mobile proxy credentials are obtained:

#### 1. Environment Configuration
```bash
# Add to .env
MOBILE_PROXY_HOST=your-mobile-proxy.com
MOBILE_PROXY_PORT=8080
MOBILE_PROXY_USER=username
MOBILE_PROXY_PASS=password
MOBILE_PROXY_STICKY_SESSION=session123  # For session persistence
```

#### 2. Update Collector Configuration
```python
# In collectors/kambi_browser/main.py, update setup_browser():
if all([MOBILE_PROXY_HOST, MOBILE_PROXY_PORT, MOBILE_PROXY_USER, MOBILE_PROXY_PASS]):
    launch_args["proxy"] = {
        "server": f"http://{MOBILE_PROXY_HOST}:{MOBILE_PROXY_PORT}",
        "username": f"{MOBILE_PROXY_USER}-session-{MOBILE_PROXY_STICKY_SESSION}",
        "password": MOBILE_PROXY_PASS
    }
```

#### 3. Deploy Steps
```bash
# 1. Switch back to kambi-mobile-proxy branch
git checkout kambi-mobile-proxy

# 2. Update .env with mobile proxy credentials
vim .env

# 3. Test locally first
docker compose -f docker-compose.local.yml up kambi-browser

# 4. Verify collection working
curl http://localhost:19088/metrics | grep collector_up
# Should show: collector_up{book="betrivers"} 1.0

# 5. Check intercepted URLs
curl http://localhost:19088/intercepted

# 6. If working, deploy to production
docker compose up -d kambi-browser
```

#### 4. Monitoring Integration
- Add Kambi brands back to Grafana dashboards
- Update Prometheus alerts for Kambi collectors
- Monitor mobile proxy usage/costs

#### 5. Cost Optimization
- Implement caching to reduce requests
- Use HEAD requests where possible  
- Batch multiple brands per session
- Monitor GB usage closely (mobile data is expensive)

### Testing Checklist

Before going live:

- [ ] Test each brand individually with mobile proxy
- [ ] Verify odds data structure matches normalizer expectations
- [ ] Confirm Redis pub/sub working
- [ ] Check Prometheus metrics updating
- [ ] Validate data in PostgreSQL
- [ ] Test failover behavior
- [ ] Monitor proxy costs for 24 hours
- [ ] Set up usage alerts

### Fallback Options

If mobile proxies prove too expensive/unreliable:

1. **Partner API Access**: Negotiate official API access with Kambi
2. **White-label Access**: Some Kambi brands offer affiliate APIs
3. **Alternative Data Source**: Consider providers like OddsJam or SportsDataIO
4. **Hybrid Approach**: Mobile proxy for critical games only

### Files to Review

- `collectors/kambi_browser/main.py` - Main collector implementation
- `collectors/kambi_browser/Dockerfile` - Container configuration
- `scripts/kambi_direct_probe.py` - Testing tool
- `services/kambi_browser/` - Previous Bright Data attempts (for reference)
- `BRIGHT_DATA_SUMMARY.md` - Detailed testing results

### Contact for Questions

Current implementation by: Sam (2025-01-07)
Testing performed on: Kambi CDN endpoints for all 5 brands
Last successful collection: With phone hotspot (mobile carrier IP)
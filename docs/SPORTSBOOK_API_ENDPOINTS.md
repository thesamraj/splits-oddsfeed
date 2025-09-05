# Sportsbook API Endpoints Documentation

*Last Updated: 2025-09-01*

## Working Sportsbooks (No Proxy Required)

### 1. DraftKings
- **Status**: ✅ Fully Working
- **Architecture**: WebSocket + HTTP
- **WebSocket**: `wss://sportsbook-ws-ca-on.draftkings.com/websocket`
- **HTTP**: Scrapes `__INITIAL_STATE__` from public pages
- **Rate Limit**: 30 second polling
- **Notes**: No blocking, real-time data available

### 2. BetRivers
- **Status**: ✅ Fully Working
- **Architecture**: Kambi Platform WebSocket
- **Endpoints**: Multiple Kambi collectors
- **Markets**: All markets with enhanced mapper
- **Notes**: Uses Kambi white-label platform

### 3. Barstool (ESPN BET)
- **Status**: ✅ Fully Working
- **Architecture**: ESPN Public API
- **Endpoints**:
  - NFL: `https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard`
  - NBA: `https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard`
  - MLB: `https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard`
- **Rate Limit**: 30 seconds
- **Notes**: Rebranded from Barstool to ESPN BET

### 4. PointsBet
- **Status**: ✅ Fully Working
- **Architecture**: Host execution (Docker blocked)
- **Latency**: 7 seconds (optimized)
- **Notes**: Requires host execution to bypass Docker detection

### 5. Stake.com
- **Status**: 🟡 Test Data Only
- **Attempted APIs**:
  - ❌ `https://api.stake.com/sports/tree` (404)
  - ❌ `https://mediumgame.com` (DNS failure)
- **Current**: Publishing test data
- **Notes**: Crypto-friendly, needs correct API discovery

### 6. Betano
- **Status**: 🟡 Test Data Only
- **Attempted APIs**:
  - ❌ `https://api.us.betano.com/api/sports/{id}/events/prematch`
  - ❌ `https://us.betano.com/api/events/prematch`
  - 🟡 `https://www.betano.com/api/sports/soccer/events/live`
- **Current**: Publishing test data
- **Notes**: European operator, US version endpoints unclear

### 7. BetOnline
- **Status**: 🟡 Test Data Only
- **Attempted APIs**:
  - ❌ `https://www.betonline.ag/sportsbook/api/{sport}/odds`
  - ❌ `https://api.betonline.ag/api/v2/odds/nfl`
- **Current**: Publishing test data
- **Notes**: Offshore book, API returns HTML not JSON

### 8. Bovada
- **Status**: 🟡 Test Data Only
- **Attempted APIs**:
  - 🟡 `https://www.bovada.lv/services/sports/event/v2/events/A/description/football/nfl`
- **Current**: Publishing test data
- **Notes**: Major offshore, API accessible but needs parsing

## Proxy Required Sportsbooks

### 1. FanDuel
- **Status**: ❌ Blocked
- **Issue**: Cloudflare Enterprise (403 Forbidden)
- **Solution**: Residential proxy ($500-1000/month)
- **Notes**: Most aggressive anti-bot protection

### 2. BetMGM
- **Status**: ❌ Blocked
- **Issue**: Cloudflare protection
- **Solution**: Residential proxy required
- **Notes**: Previously attempted, needs proxy

### 3. Caesars
- **Status**: ❌ Blocked
- **Issue**: Advanced bot detection
- **Solution**: Residential proxy required
- **Notes**: Enterprise-grade protection

## Troubleshooting Guide

### Common Issues and Solutions

#### 1. API Returns 404
- **Cause**: Wrong endpoint or API changed
- **Solution**:
  - Check browser DevTools Network tab on live site
  - Look for XHR/Fetch requests to API endpoints
  - Try alternate subdomains (api., www., etc.)

#### 2. DNS Resolution Failure
- **Cause**: Domain doesn't exist or internal only
- **Solution**:
  - Verify domain exists: `nslookup domain.com`
  - Check if site requires VPN/geo-location
  - Try from browser first

#### 3. Getting HTML Instead of JSON
- **Cause**: Wrong endpoint or missing headers
- **Solution**:
  - Add proper Accept headers: `application/json`
  - Check Content-Type in response
  - May need session cookies

#### 4. Cloudflare 403 Forbidden
- **Cause**: Bot detection triggered
- **Solution**:
  - Requires residential proxy
  - Options: Bright Data, SmartProxy, Oxylabs
  - Cost: $500-1000/month

#### 5. Docker Detection
- **Cause**: Site detects containerized environment
- **Solution**:
  - Run collector on host machine
  - Use stealth browser automation
  - Consider residential proxy

## Proxy Services Comparison

| Service | Price/Month | IPs | Best For |
|---------|------------|-----|----------|
| Bright Data | $500+ | Millions | Enterprise, highest success rate |
| SmartProxy | $300+ | 40M+ | Good balance of price/performance |
| Oxylabs | $600+ | 100M+ | Premium, excellent support |
| IPRoyal | $200+ | 2M+ | Budget option, less reliable |

## Redis Channel Naming

All collectors publish to: `odds.raw.{bookname}`

Examples:
- `odds.raw.draftkings`
- `odds.raw.betrivers` (uses kambi)
- `odds.raw.barstool`
- `odds.raw.stake`

## Message Format

Standard format for all collectors:
```json
{
  "timestamp": "2025-09-01T00:00:00Z",
  "source": "bookname",
  "events": [
    {
      "event_id": "unique_id",
      "sport": "nfl",
      "home_team": "Team A",
      "away_team": "Team B",
      "market": "h2h",
      "price_home": 1.95,
      "price_away": 1.85
    }
  ]
}
```

## Next Steps

1. **Priority**: Get residential proxy for FanDuel, BetMGM, Caesars
2. **Research**: Find correct APIs for Stake, Betano, BetOnline
3. **Optimize**: Implement WebSocket for more books
4. **Scale**: Add more offshore books (no proxy needed)

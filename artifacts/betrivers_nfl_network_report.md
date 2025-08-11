# BetRivers (Kambi) Network Analysis Report

**Generated:** 2025-08-10T02:15:23Z
**Target URL:** https://pa.betrivers.com / https://co.betrivers.com
**Analysis Method:** Manual analysis based on Kambi API patterns

## Kambi Endpoint Analysis

Based on typical Kambi integrations and BetRivers patterns:

### Base URLs
- **https://eu-offering.kambicdn.org/offering/v2018**
- **https://c3-api.kambi.com/offering/v2018**

### Brand Slugs
- **betrivers** (most likely)
- **rushstreetinteractive** (alternative)

### Locales
- **en_US** (United States English)
- **en_CA** (Canadian English, for some regions)

### NFL-Related Endpoints

Based on standard Kambi API patterns for American Football NFL:

- List View: `/listView/american_football/nfl/all/matches`
- Event Details: `/betoffer/listView/american_football/nfl/all/matches`
- Live Events: `/betoffer/live/american_football/nfl`

### Typical Query Parameters

- `locale=en_US`
- `market=US` (or specific state codes)
- `client_id=2` (common value)
- `channel_id=1` (web channel)
- `ncid=1000` (cache control)
- `useCombined=true`

### Example Full URLs

```
https://eu-offering.kambicdn.org/offering/v2018/betrivers/listView/american_football/nfl/all/matches.json?lang=en_US&market=US&client_id=2&channel_id=1&ncid=1000
https://eu-offering.kambicdn.org/offering/v2018/betrivers/betoffer/listView/american_football/nfl/all/matches.json?lang=en_US&market=US&client_id=2&channel_id=1&ncid=1000
```

## Implementation Values

For collector-kambi implementation:

- **KAMBI_BASE_URL**: `https://eu-offering.kambicdn.org/offering/v2018`
- **KAMBI_BRAND**: `betrivers`
- **KAMBI_LOCALE**: `en_US`
- **Primary Endpoint**: `/listView/american_football/nfl/all/matches.json`
- **Bet Offers Endpoint**: `/betoffer/listView/american_football/nfl/all/matches.json`

## Notes

- Kambi APIs typically return comprehensive JSON with event metadata and betting markets
- ETag/If-None-Match headers are supported for caching
- Rate limiting may apply - recommend 3-5 second intervals
- No authentication typically required for basic event listings
- Market-specific filtering available via query parameters

# Kambi Collectors - FROZEN

**Status:** Not part of current deployment
**Branch:** Tracked separately in kambi-specific branches

## Overview

All Kambi-based sportsbooks (BetRivers, SugarHouse, Barstool, Unibet, Caesars) are **FROZEN** and not included in the main deployment pipeline.

## Requirements for Kambi

Kambi books require sophisticated infrastructure that is not currently deployed:

1. **Dedicated Mobile IP**: State-specific (NJ/PA/IL) mobile carrier IPs
2. **Sticky Sessions**: Maintain same IP/identity for 24+ hours
3. **Stealth Playwright**: Undetected browser automation with:
   - Real browser fingerprints
   - Human-like interaction patterns
   - Cookie persistence
4. **Session Bootstrap**: Initial manual navigation to establish trust:
   - Accept cookies
   - Navigate through UI
   - Extract CSRF tokens
   - Maintain WebSocket connections

## Why Frozen

- **Cost**: Mobile proxies cost $200-500/month
- **Complexity**: Requires dedicated session management service
- **Risk**: High detection rate without proper stealth
- **Maintenance**: Constant updates needed as detection evolves

## Current State

- Kambi services commented out in `docker-compose.do.yml`
- Code exists in `collectors/kambi_unified/`
- Previous attempts all failed with 403/empty responses

## Activation Plan

When ready to tackle Kambi:
1. Provision TSP mobile proxies with state targeting
2. Deploy session management service
3. Implement stealth browser pool
4. Test with single brand (BetRivers) first
5. Scale to other brands only after stable

## DO NOT

- Enable Kambi services in production
- Mix Kambi with non-Kambi deployments
- Use datacenter proxies for Kambi
- Attempt without mobile IPs

This is tracked in separate branches and not part of the main odds pipeline.
# BetParx Non-Kambi Evidence & De-scoping

**Date**: 2025-08-26 00:57:31 EDT
**Decision**: Exclude BetParx from Kambi onboarding - confirmed non-Kambi architecture

## Technical Evidence

### WebSocket Analysis
**BetParx connects to Pennsylvania Gaming Platform endpoints, NOT Kambi:**

```
Active WebSocket Connections:
wss://mpsnare.iesnare.com/star
wss://oapi.penshared.com/socket.io/1/websocket/2b3834ed-82bd-4948-946f-d6bd290c9e83
wss://oapi.penshared.com/socket.io/1/websocket/d0f9a627-2af5-4534-b88e-67b523370ee5
wss://oapi.penshared.com/socket.io/1/websocket/74b9ef53-5356-4cdf-a5f1-ee1f8868c792
wss://oapi.penshared.com/socket.io/1/websocket/6498a2ee-41a9-49cf-be00-609346db9535
```

### Expected vs Actual
- **Expected Kambi**: `wss://*.kambicdn.com/*` or `offering-api.kambicdn.com/offering/v2018/*`
- **Actual BetParx**: `oapi.penshared.com` (Pennsylvania gaming platform)
- **Frame Analysis**: 32+ frames processed, 0 odds-like JSON structures found

### Bridge Metrics
```json
{
  "status": "active",
  "frames_seen": 32,
  "candidate_json": 0,
  "published": 0,
  "bytes_seen": 1351,
  "ws_connections": 3
}
```

## Actions Taken

### 1. Service Cleanup
- Stopped BetParx Kambi collectors:
  - `splits-oddsfeed-collector-kambi-push-betparx-ws-1`
  - `splits-oddsfeed-collector-kambi-push-betparx-mirror-1`
  - `splits-oddsfeed-collector-kambi-push-betparx-1`
  - `collector-kambi-wsbridge-parx`

### 2. API Layer Updates
- Modified Kambi book filters to exclude `betparx` brand
- Prevents pollution of Kambi metrics/alignment calculations
- Preserves brand mapping for future non-Kambi integration

## Rollback Commands

If BetParx needs to be re-investigated:

```bash
# Restart BetParx services
docker compose up -d splits-oddsfeed-collector-kambi-push-betparx-1
docker compose up -d splits-oddsfeed-collector-kambi-push-betparx-ws-1
docker compose up -d splits-oddsfeed-collector-kambi-push-betparx-mirror-1

# Re-enable in API filters
# (Revert API changes in brand filtering logic)
```

## Conclusion

BetParx uses Pennsylvania-specific gaming infrastructure (`oapi.penshared.com`) rather than Kambi's standardized platform. This confirms BetParx should be excluded from Kambi onboarding and handled via separate non-Kambi integration path.

**Kambi Mission Scope**: BetRivers + SugarHouse (polite) + Unibet

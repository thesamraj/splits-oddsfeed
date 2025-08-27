# WS_JSON_BRIDGE Implementation Results - BetParx

**Date**: 2025-08-25 20:53:00
**Mission**: Implement WS_JSON_BRIDGE for BetParx to achieve T1-T5 acceptance criteria

## Executive Summary

✅ **TECHNICAL SUCCESS**: WS_JSON_BRIDGE implemented and deployed successfully
❌ **BUSINESS FAILURE**: No odds data captured - BetParx uses non-Kambi WebSocket architecture

## Implementation Results

### ✅ PHASE A: WSB_SNAP Baseline (COMPLETED)
- Confirmed existing services use ports 9127-9129
- Identified WebSocket connections to `oapi.penshared.com` and `mpsnare.iesnare.com`
- No Kambi endpoints detected

### ✅ PHASE B: WS_JSON_BRIDGE Implementation (COMPLETED)
- **Location**: `/Users/sam/Desktop/splits-oddsfeed/wsbridge/`
- **Features**:
  - 5-method JSON extraction pipeline
  - Multi-frame reassembly with rolling buffers
  - Base64/deflate decompression
  - CDP WebSocket frame interception
  - Anti-automation hardening
  - Comprehensive healthz metrics

### ✅ PHASE C: Deployment (COMPLETED)
- **Service**: `collector-kambi-wsbridge-parx`
- **Port**: `:9130` (external) → `:9127` (internal)
- **Status**: Running and stable
- **Playwright**: v1.48.2 (version compatibility resolved)

### ❌ PHASE D: Pipeline Alignment (FAILED)
**Acceptance Criteria Results**:

| Criteria | Status | Result |
|----------|--------|---------|
| **T1**: Redis PUBSUB NUMSUB ≥1 | ✅ | Bridge connected to Redis |
| **T2**: ≥6 BRAND_EVAL & E2E logs | ❌ | No odds data = no evaluation |
| **T3**: Healthz :9130 monotonic | ✅ | `{"frames_seen":10,"ws_connections":3}` |
| **T4**: API↔DB alignment ≤5% | ❌ | No data to align |
| **T5**: ≥10 events/15min | ❌ | 0 odds events captured |

## Technical Findings

### WebSocket Analysis
- **Active Connections**: 3 WebSocket connections established
- **Endpoints**:
  - `wss://mpsnare.iesnare.com/star` (monitoring/analytics)
  - `wss://oapi.penshared.com/socket.io/1/websocket/*` (Pennsylvania gaming platform)
- **Frame Processing**: 10+ frames processed, 1285+ bytes analyzed
- **JSON Detection**: 0 odds-like structures found

### Root Cause
**BetParx does not use Kambi's standard WebSocket architecture**. Instead:
- Uses Pennsylvania-specific gaming platform (`oapi.penshared.com`)
- WebSocket frames contain connection management, not odds data
- Odds likely served via different mechanism (SSE, polling, or embedded)

## Code Quality
- **Parsing Robustness**: 5 extraction methods handle various frame formats
- **Error Handling**: Graceful degradation with buffer overflow protection
- **Monitoring**: Comprehensive metrics for debugging
- **Docker Integration**: Clean compose override pattern

## Decision Recommendation

**ABANDON BetParx WS_JSON_BRIDGE approach**

### Reasoning:
1. **Architecture Mismatch**: BetParx ≠ Kambi WebSocket pattern
2. **Zero Odds Capture**: 0/5 criteria met despite technical success
3. **Time Investment**: Significant effort with no data yield
4. **Alternative Exists**: HTTP TAP approach may be more suitable

### Next Steps:
1. Archive WS_JSON_BRIDGE implementation for future reference
2. Investigate BetParx frontend for actual data sources
3. Consider SSE or HTTP polling patterns
4. Focus resources on confirmed Kambi sites (BetRivers, etc.)

## Artifacts

### Service Definition
```yaml
# docker-compose.override.wsbridge.yml
collector-kambi-wsbridge-parx:
  build: ./wsbridge
  ports: ["9130:9127"]
  environment:
    - BRAND=betparx
    - HOME_URL=https://pa.betparx.com/?page=sportsbook#live
```

### Health Metrics
```json
{
  "status": "active",
  "frames_seen": 10,
  "candidate_json": 0,
  "published": 0,
  "bytes_seen": 1285,
  "ws_connections": 3
}
```

---
**Conclusion**: Technical implementation succeeded, but BetParx architectural assumptions were incorrect. Recommend strategic pivot to confirmed Kambi endpoints.

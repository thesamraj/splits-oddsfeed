# BoltOdds Protocol Teardown Report

**Date**: 2025-09-07 (Saturday Night Prime Time)  
**Capture Duration**: ~10 minutes  
**Status**: NO DATA CAPTURED  

## Executive Summary

Despite attempting capture during Saturday night prime time with broad subscription filters across 50+ sports and 22 sportsbooks, **zero actual odds data frames were received**. Only connection acknowledgments and keepalive pings were captured. The WebSocket connection was stable but appears to be non-functional for data delivery.

## Capture Statistics

- **Total Frames**: 37
- **Data Frames**: 0
- **Message Types**:
  - `ping`: 16 (keepalive)
  - `socket_connected`: 2 (connection acks)
  - `subscribe`: 2 (our subscription messages)
- **Subscription Attempts**: 2 (rotated filters after 5 min of no data)

## Verified Components

### 1. Info Endpoint ✅
- **URL**: `https://spro.agency/api/get_info?key={TOKEN}`
- **Response**: Successfully returned 124 sports and 22 sportsbooks
- **Sample Sports**: NHL, NBA, NFL, NFL Pre, NCAAF, NBA Summer, NCAAB, Bundesliga, MLS, Ligue 1
- **Sample Books**: draftkings, neobet, thescore, espnbet, betmgm, sportsinteraction, bwin

### 2. WebSocket Connection ✅
- **URL**: `wss://spro.agency/api?key={TOKEN}`
- **Connection**: Successful with SSL (self-signed cert)
- **Stability**: 100% uptime, no disconnects
- **Keepalive**: Ping every ~20 seconds

## Subscription Attempts

### Attempt 1 (First 5 minutes)
```json
{
  "action": "subscribe",
  "filters": {
    "sports": [50 sports including NHL, NBA, NFL, etc.],
    "sportsbooks": [all 22 available books],
    "games": [],
    "markets": []
  }
}
```

### Attempt 2 (After rotation)
```json
{
  "action": "subscribe",
  "filters": {
    "sports": ["NFL", "NBA", "NHL", "MLB", "NCAAF", "NCAAB", "MLS", "WNBA"],
    "sportsbooks": [15 major US books],
    "games": [],
    "markets": ["moneyline", "spread", "total"]
  }
}
```

## Protocol Observations

### Message Flow
1. Connect → Receive `{"action": "socket_connected"}`
2. Send subscription → No acknowledgment
3. Receive pings every ~20s → Connection maintained
4. **No data frames ever received**

### Inferred Schema (Control Messages Only)
```json
{
  "action": "socket_connected" | "ping" | "pong"
}
```

## Questions for BoltOdds Founder

Given the complete absence of data despite valid connection and subscription, we need clarification on the following:

### 1. Example Data Frame
**Q**: Can you provide one real example of an odds data message (with sensitive data redacted)?  
**Why**: We need to understand the actual structure since no data was captured.

### 2. Exact Subscribe Payload
**Q**: What is the exact `subscribe` message format that will trigger data flow?  
**Current attempt**:
```json
{
  "action": "subscribe",
  "filters": {
    "sports": [...],
    "sportsbooks": [...],
    "games": [],
    "markets": []
  }
}
```
**Why**: Our subscription may be malformed or missing required fields.

### 3. Snapshot vs Delta Updates
**Q**: When data does flow, are messages full snapshots or incremental deltas?  
**Why**: Architecture decision for our normalizer.

### 4. Update Frequency
**Q**: During live games, what is the typical message frequency per game?  
**Why**: Capacity planning for our infrastructure.

### 5. Rate Limits
**Q**: Are there any per-connection rate limits or message throttling?  
**Why**: The lack of data suggests possible throttling or access restrictions.

### 6. Additional Requirements
**Q**: Are there any additional requirements beyond the API key?
- Specific headers required?
- IP whitelist?
- Account activation needed?
- Separate data subscription plan?  
**Why**: Connection succeeds but data doesn't flow, suggesting an authorization or configuration issue.

## Additional Troubleshooting Questions

7. **Time Windows**: Are there specific time windows when data is available?
8. **Sport Availability**: Is there currently any live data for the sports we subscribed to?
9. **Test Mode**: Is there a test/sandbox mode with sample data we can use?
10. **Error Messages**: Should we receive error messages if subscription fails?

## Technical Details

### Files Generated
- `docs/bolt_teardown/bolt_sniff.py` - WebSocket sniffer
- `docs/bolt_teardown/analyze_frames.py` - Frame analyzer  
- `docs/bolt_teardown/info_snapshot.json` - Available sports/books
- `docs/bolt_teardown/SCHEMA.json` - Inferred schema (control messages only)
- `data/bolt/raw/frames_*.jsonl` - Raw captured frames

### Environment
- Token stored in `.env.local` (not committed)
- SSL verification disabled for self-signed certificate
- Multiple subscription filter combinations attempted
- Capture during Saturday night prime time window

## Conclusion

**Result**: CAPTURE FAILED - No data frames received

The WebSocket infrastructure appears functional (connection, keepalive, info endpoint all work), but no actual odds data is being delivered. This suggests either:

1. **Configuration Issue**: Missing subscription parameters or incorrect format
2. **Authorization Issue**: API key may not have data access permissions
3. **Timing Issue**: No live data available during capture window
4. **Account Issue**: Account may require activation or additional setup

**Recommendation**: Direct communication with BoltOdds technical team is required to resolve the data delivery issue before any integration can proceed.
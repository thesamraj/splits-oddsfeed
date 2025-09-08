# BoltOdds Protocol Analysis Report

**Generated**: 2025-09-07  
**Capture Duration**: 5 minutes  
**Frames Captured**: 17 (1 connection ack + 16 pings)  
**Data Frames**: 0 (no odds updates received)  

## Executive Summary

Successfully connected to BoltOdds WebSocket and maintained stable connection for 5 minutes. However, no actual odds data was received during the capture window, only keepalive messages. This suggests either no live updates for subscribed sports/books or need for different subscription parameters.

## 1. Verified Endpoints

| Endpoint | URL | Status |
|----------|-----|--------|
| Info API | `https://spro.agency/api/get_info?key={TOKEN}` | ✅ Working |
| WebSocket | `wss://spro.agency/api?key={TOKEN}` | ✅ Connected |

## 2. Info API Response

Successfully fetched available sports and sportsbooks:

```json
{
  "sports": [124 total sports available],
  "sportsbooks": [22 total sportsbooks available]
}
```

Full snapshot saved to: `docs/bolt_teardown/info_snapshot.json`

### Available Sports (sample)
- NHL, NBA, NFL, NFL Pre, NCAAF, NBA Summer, NCAAB
- Bundesliga, MLS, Ligue 1, EPL, La Liga, Serie A
- Plus 111 more...

### Available Sportsbooks
- draftkings, neobet, thescore, espnbet, betmgm
- sportsinteraction, bwin, partysports, bet99, fanduel
- caesars, pointsbet, betrivers, unibet, williamhill
- Plus 7 more...

## 3. WebSocket Protocol

### Connection Flow
1. **Connect**: `wss://spro.agency/api?key={TOKEN}`
2. **Receive**: `{"action": "socket_connected"}`
3. **Send**: Subscription message
4. **Receive**: Periodic pings every ~15-20 seconds

### Subscription Message Used
```json
{
  "action": "subscribe",
  "filters": {
    "sports": ["NFL", "NBA", "NHL"],
    "sportsbooks": [
      "draftkings", "betmgm", "espnbet", "thescore", "bet365",
      "pointsbet", "betway", "superbook", "wynnbet", "bookmaker",
      "betonline", "circa", "pinnacle", "unibet", "betrivers"
    ],
    "games": [],
    "markets": []
  }
}
```

### Keepalive Protocol
- **Ping Frequency**: Every 15-20 seconds
- **Message Format**: `{"action": "ping"}`
- **Connection Stability**: 100% (no disconnects in 5 minutes)

## 4. Message Types Observed

| Message Type | Count | Purpose |
|--------------|-------|---------|
| socket_connected | 1 | Initial connection acknowledgment |
| ping | 16 | Keepalive messages |
| **odds data** | **0** | **No odds updates received** |

## 5. Expected Data Schema (Inferred)

Based on standard odds feed patterns and the subscription structure, we expect:

```json
{
  "game_id": "string",
  "sport": "string", 
  "teams": {
    "home": "string",
    "away": "string"
  },
  "start_time": "ISO8601",
  "sportsbooks": [
    {
      "name": "string",
      "markets": [
        {
          "type": "string",
          "outcomes": [
            {
              "name": "string",
              "price": "number",
              "line": "number"
            }
          ]
        }
      ]
    }
  ]
}
```

## 6. Key Findings

### ✅ Working
- WebSocket connection stable with token auth
- Keepalive mechanism functioning
- SSL/TLS with self-signed certificate accepted
- Info API returns comprehensive lists

### ⚠️ Issues/Questions
1. **No Data Frames**: Zero odds updates received despite valid subscription
2. **Possible Causes**:
   - No live games for NFL/NBA/NHL at capture time
   - Subscription filters may need adjustment
   - May require specific game IDs in filter
   - Could be rate-limited or require additional auth

### 🔄 Reconnection Behavior
- Not tested (connection remained stable)
- Ping/pong keepalive suggests automatic reconnect would work

## 7. Architecture Notes for Replication

To replicate this WebSocket feed architecture:

1. **Authentication**: Token-based via query parameter
2. **Connection**: Direct WebSocket with SSL
3. **Subscription**: Send filters immediately after connection
4. **Keepalive**: Handle ping messages every 15-20s
5. **Data Flow**: Expect full snapshots (not confirmed due to no data)

### Components Needed
- WebSocket server with token auth
- Info endpoint for available filters
- Subscription-based filtering
- Ping/pong keepalive mechanism
- JSON message protocol

## 8. Open Questions

1. **Why no data?** - Are there active games? Do we need different sports?
2. **Game IDs** - Should we populate the `games` filter array?
3. **Markets** - What are valid values for `markets` filter?
4. **Rate Limits** - Any connection or message rate limits?
5. **Delta vs Snapshot** - Are updates incremental or full state?
6. **Sequence Numbers** - Is there message ordering/versioning?

## 9. Next Steps

To get actual odds data:

1. **Try during live game windows** (evenings/weekends)
2. **Expand sports list** to include more active leagues
3. **Test with specific game IDs** if available
4. **Monitor longer duration** (15-30 minutes)
5. **Check different times of day** for activity

## 10. Files Generated

- `docs/bolt_teardown/bolt_sniff.py` - WebSocket sniffer (token redacted)
- `docs/bolt_teardown/analyze_frames.py` - Frame analyzer
- `docs/bolt_teardown/info_snapshot.json` - Available sports/books
- `data/bolt/raw/frames_*.jsonl` - Raw captured frames
- `docs/bolt_teardown/ANALYSIS.json` - Analysis results

## Conclusion

**PARTIAL SUCCESS**: WebSocket connection established and stable, but no actual odds data captured. The protocol structure is clear (connection → subscription → data stream), but requires testing during active game periods to capture real odds messages for complete schema analysis.
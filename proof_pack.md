# Normalizer Fix Success - Proof Pack

## ✅ BRAND_EVAL Logs Working
```
normalizer-1  | BRAND_EVAL brand=betrivers url=https://eu1.offering-api.kambicdn.com/offering/v2018/rsi2uspa/event/live/open.json
normalizer-1  | BRAND_EVAL brand=kambi url=wss://e0-api.kambi.com/subscribe/unibet page_url=https://pa.unibet.com/
normalizer-1  | BRAND_EVAL brand=kambi url=wss://e0-api.kambi.com/subscribe/betparx page_url=https://pa.betparx.com/
```

## ✅ E2E Latency < 1.0s
```
E2E: Observed 0.006s latency for 24 rows
E2E: Observed 0.009s latency for 24 rows
E2E: Observed 0.014s latency for 18 rows
E2E: Observed 0.021s latency for 24 rows
E2E: Observed 0.038s latency for 20 rows
E2E: Observed 0.479s latency for 252 rows (worst case, still < 1s)
```

## ✅ Normalizer Running Stable
- Up for 3+ minutes without restart loop
- No syntax errors
- Processing messages from all books (kambi, draftkings, mgm, pinnacle, bet365, agg)

## 🔧 Brand Attribution Status
- ✅ BetRivers: Correctly extracted from rsi2uspa token
- ⚠️ BetParx/Unibet: WebSocket URLs defaulting to "kambi" (needs token extraction improvement)
- ✅ Token-based extraction working for HTTP API URLs

## Files Modified
1. `/opt/splits-oddsfeed/normalizer/src/normalizer/main.py` - Restored from backup and added logging
2. Brand extraction logic embedded in main.py (lines ~374-390)

## Environment Variables Set
- LOG_E2E_ALWAYS=1
- BRAND_DEBUG=1

## Next Steps (if needed)
1. Improve WebSocket URL brand extraction to handle /subscribe/{brand} pattern
2. Wait for SugarHouse collector data to verify sg2us* token extraction

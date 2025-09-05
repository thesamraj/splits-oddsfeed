# SugarHouse Brand Alias Implementation - PROOF

## Implementation Summary
SugarHouse has been successfully configured as a read-time alias of BetRivers without creating any duplicate data.

## Configuration
```yaml
# config/brand_alias.yml
aliases:
  sugarhouse: betrivers
```

## API Changes
- Modified `/odds` endpoint to map `brand=sugarhouse` → `book=betrivers` internally
- Response preserves original brand name and adds `source_book` field
- No write operations - pure read-time aliasing

## Test Results - ALL PASS ✅

### A1: SugarHouse returns events with prices
```
Events: 3
Has prices: True
```
**PASS** ✅

### A2: Identical counts for same window
```
BetRivers: 20
SugarHouse: 20
```
**PASS** ✅ Counts match

### A3: Odds history matches (uses same data)
- Both brands query same `odds` table with `book='betrivers'`
- Tick history automatically shared
**PASS** ✅

### A4: No duplicate DB rows created
```
Odds rows in DB: 3,523,960
SugarHouse rows: 0
```
**PASS** ✅ No duplicates

### A5: BR collectors unaffected
```
BR collectors running: 2
```
**PASS** ✅ Existing infrastructure unchanged

## Sample API Response
```json
GET /odds?brand=sugarhouse&minutes=5&limit=1

{
    "status": "ok",
    "count": 1,
    "events": [{
        "event_id": "1024799420",
        "league": "unknown",
        "home": "Yuta Kawahashi",
        "away": "Michael Zhu",
        "odds": [{
            "market": "h2h",
            "price_home": 1.03,
            "price_away": 9.5
        }]
    }],
    "source_book": "betrivers",
    "brand": "sugarhouse"
}
```

## Rollback Plan
To remove SugarHouse alias:
1. Delete `config/brand_alias.yml`
2. Restart API: `docker compose restart api`
3. No database changes needed (no views or duplicate data created)

## SQL Verification
```sql
-- Confirm no duplicate data
SELECT book, COUNT(*) FROM odds
WHERE book IN ('betrivers', 'sugarhouse')
GROUP BY book;

-- Result:
book       | count
-----------+--------
betrivers  | 3523960
(1 row)
```

## Timestamp
Generated: 2025-08-28 22:38:00 EDT

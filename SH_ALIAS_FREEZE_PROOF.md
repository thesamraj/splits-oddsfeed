# SugarHouse Alias Freeze Proof

## Freeze Tag
`sh-alias-freeze-20250829_1259`

## Current State
- **BetRivers**: 3,523,960+ odds rows
- **SugarHouse**: Aliased to BetRivers (0 duplicate rows)
- **API Status**: Healthy
- **Parity Monitor**: Running (24h)

## Latest Verification
```bash
# Test commands
curl -s "http://localhost:8080/odds?brand=betrivers&minutes=10" | jq '.count'
# Result: 20

curl -s "http://localhost:8080/odds?brand=sugarhouse&minutes=10" | jq '.count'
# Result: 20 (identical)
```

## Sample Response
```json
GET /odds?brand=sugarhouse&minutes=5&limit=1
{
    "status": "ok",
    "count": 1,
    "events": [...],
    "source_book": "betrivers",
    "brand": "sugarhouse"
}
```

## Rollback Commands
```bash
# To rollback alias configuration
git checkout sh-alias-freeze-20250829_1259
docker compose restart api

# To remove alias completely
rm config/brand_alias.yml
docker compose restart api
```

## Files Committed
- `config/brand_alias.yml` - Brand alias configuration
- `api/src/api/main.py` - API with alias support
- `scripts/sh_parity_monitor.py` - 24h monitoring script

## Timestamp
Generated: 2025-08-29 02:59:00 UTC

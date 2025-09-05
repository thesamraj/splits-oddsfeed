# REAL-ONLY EXPAND AUTOPILOT - Rollback Documentation

## Mission Status: COMPLETE ✅

### Acceptance Criteria Results
- ✅ **Bovada Real Data**: 23,056 odds from 104 events in 15-min window
- ✅ **No Test Data**: 0 test/dummy records in database
- ✅ **Existing Books**: Untouched (BetRivers operational)
- ✅ **BetOnline Sandbox**: Discovery mode, no DB writes
- ✅ **Guardrails**: Test data rejection active in normalizer

## Changes Made

### 1. Disabled Test Publishers
```bash
# Killed containers:
docker kill stake-test betano-test betonline-test bovada-test
```

### 2. Implemented Real Bovada Collector
- **File**: `/collectors/bovada_real/bovada_real.py`
- **Channel**: `odds.raw.bovada`
- **Status**: Publishing 1,600+ odds per cycle from 100+ events

### 3. Added Normalizer Support
- **File**: `/normalizer/src/normalizer/main.py`
- **Changes**:
  - Added Bovada to multibook handler
  - Converts odds items to events structure
  - Added test data guard

### 4. BetOnline Sandbox
- **File**: `/collectors/betonline_sandbox/betonline_discovery.py`
- **Mode**: Log-only, no database writes
- **Channel**: `odds.raw.betonline.sandbox`

## Rollback Procedures

### Full Rollback
```bash
# Stop Bovada collector
docker stop bovada-real

# Revert normalizer changes
git checkout 371ba9a -- normalizer/src/normalizer/main.py
docker-compose build normalizer
docker-compose up -d normalizer

# Remove Bovada data
docker exec splits-oddsfeed-store-1 sh -c "PGPASSWORD=odds psql -h localhost -U odds oddsfeed -c \"DELETE FROM odds WHERE book = 'bovada'\""
docker exec splits-oddsfeed-store-1 sh -c "PGPASSWORD=odds psql -h localhost -U odds oddsfeed -c \"DELETE FROM events WHERE id LIKE 'bovada_%'\""
```

### Partial Rollback (Keep Bovada, Remove Guards)
```bash
# Remove test data guard only
sed -i '/is_test_data/,/return False/d' normalizer/src/normalizer/main.py
docker-compose build normalizer
docker-compose up -d normalizer
```

### Emergency Stop
```bash
# Immediate stop of new data
docker stop bovada-real betonline-sandbox
```

## Monitoring Commands

### Check Bovada Status
```bash
# Collector health
docker logs bovada-real --tail 20

# Database stats
docker exec splits-oddsfeed-store-1 sh -c "PGPASSWORD=odds psql -h localhost -U odds oddsfeed -c \"
  SELECT
    COUNT(*) as odds_count,
    COUNT(DISTINCT event_id) as event_count,
    MAX(ts) as latest_update
  FROM odds
  WHERE book = 'bovada'
  AND ts > NOW() - INTERVAL '15 minutes'
\""
```

### Run Smoke Test
```bash
bash /Users/sam/Desktop/splits-oddsfeed/scripts/smoke_all.sh
```

## Key Files Modified
1. `/normalizer/src/normalizer/main.py` - Added Bovada support and test guards
2. `/collectors/bovada_real/bovada_real.py` - Real Bovada collector
3. `/collectors/betonline_sandbox/betonline_discovery.py` - BetOnline discovery
4. `/scripts/smoke_all.sh` - Acceptance criteria checks

## Artifacts
- Snapshot: `/artifacts/REAL_ONLY_20250831_230135/`
- Docker state before changes preserved
- Git commit before changes: `371ba9a`

## Support
For issues, check:
1. Bovada collector logs: `docker logs bovada-real`
2. Normalizer logs: `docker logs splits-oddsfeed-normalizer-1`
3. Database connection: Verify with smoke test

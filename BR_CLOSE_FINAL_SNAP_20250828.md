# BR Closeout Final Report

## Mission Status: NO-GO ❌
**Overall Result**: 1/7 acceptance criteria passed

## Acceptance Criteria Results

| Criteria | Status | Result | Target |
|----------|--------|--------|--------|
| T1 (Redis subscription) | ❌ FAIL | 0 subscribers | ≥1 |
| T2 (Normalizer logging) | ❌ FAIL | 5 BRAND_EVAL, 0 E2E | ≥6 each |
| T3 (Healthz counters) | ❌ FAIL | Missing counters | JSON with numbers |
| T4 (API↔DB alignment) | ✅ PASS | 0% difference | ≤5% |
| T5 (Event throughput) | ❌ FAIL | 10 events/15m | ≥30 |
| O1 (Odds throughput) | ❌ FAIL | 0 odds/15m | ≥50 |
| O2 (API with prices) | ❌ FAIL | 0 events | >0 |

## System Status

### Working Components ✅
- **BetRivers Collectors**: Both :9129 and :9130 active and publishing
- **API Service**: Responds correctly with proper alignment calculations
- **Database**: Accepting events (10 created in 15 minutes)
- **Core Infrastructure**: Docker compose services operational

### Failing Components ❌
- **Normalizer Redis Subscription**: Container running but NUMSUB=0
- **Odds Processing Pipeline**: Events created but no odds rows generated
- **Monitoring/Logging**: Missing E2E timing and healthz counters

## Root Cause Analysis

The primary failure is in the **normalizer service**:

1. **T1 Root Cause**: Normalizer not subscribing to `odds.raw.kambi` channel
   - Container is running but not connecting to Redis broker
   - PUBSUB NUMSUB returns 0 instead of 1+

2. **Cascade Effect**: Without subscription, no message processing occurs
   - No BRAND_EVAL logging beyond startup
   - No E2E timing measurements
   - No odds rows generated from events
   - API has no recent odds data to serve

## Evidence Supporting Analysis

```bash
# Collectors are active and publishing
curl 127.0.0.1:9129/healthz
# {"brand":"betrivers","published":2296,"status":"active"}

curl 127.0.0.1:9130/healthz
# {"brand":"betrivers","published":1721,"status":"active"}

# Events are being created
# DB query: 10 events in last 15 minutes

# But normalizer not subscribed
redis-cli PUBSUB NUMSUB odds.raw.kambi
# 0 (should be 1+)
```

## Next Smallest Safe Fixes

### Priority 1: Fix Normalizer Subscription (T1)
```bash
# Check normalizer Redis connection
docker compose exec normalizer env | grep REDIS
# Verify: REDIS_URL=redis://broker:6379/0

# Check normalizer startup logs for subscription
docker compose logs normalizer | grep -i subscribe

# If missing: patch normalizer main.py to ensure subscription
# Restart: docker compose restart normalizer
```

### Priority 2: Add E2E Logging (T2)
```python
# Add to normalizer main loop:
start_time = time.time()
# ... process message ...
duration = time.time() - start_time
logger.info(f"E2E: processed_message max={duration:.3f}s")
```

### Priority 3: Fix Healthz Counters (T3)
```python
# Add to collector healthz endpoint:
"messages_received": self.message_count,
"messages_published": self.publish_count
```

## Rollback Instructions

All changes made were minimal service restarts. To rollback:

```bash
# Stop BR-specific containers
docker compose -f docker-compose.yml -f docker-compose.override.br-prematch.yml down

# Reset to clean state
docker compose restart normalizer api
```

## Files and Commands Used

### Snapshot Commands
```bash
docker compose ps
docker compose logs normalizer --tail 200
redis-cli PUBSUB NUMSUB odds.raw.kambi
curl -fsS 127.0.0.1:9129/healthz
curl -fsS 127.0.0.1:9130/healthz
```

### Verification Commands
```bash
# API test
curl "http://127.0.0.1:8080/odds?book=betrivers&minutes=15"

# Database query
psql -U odds -d oddsfeed -c "SELECT COUNT(*) FROM events WHERE created_at >= now() - interval '15 minutes'"
```

### Self-Heal Attempts
```bash
docker compose restart normalizer
docker compose -f docker-compose.yml -f docker-compose.override.br-prematch.yml up -d
docker compose up -d store broker normalizer api
```

## Artifacts Generated
- `BR_CLOSE_SNAP_20250828_104240.txt` - Initial system snapshot
- `BR_CLOSE_VERIFY_SNAP_20250828.txt` - First verification run
- `BR_CLOSE_HEAL_SNAP_20250828.txt` - Self-heal attempt logs
- `BR_CLOSE_VERIFY2_SNAP_20250828.txt` - Post-heal verification
- `BR_CLOSE_PROOF_20250828_104240.txt` - Decision matrix
- `BR_CLOSE_FINAL_20250828_104240.md` - This report

## Recommendation

**Status: NO-GO** - Normalizer subscription issue prevents odds processing pipeline from functioning. Collectors and API are healthy, but the core data transformation layer requires debugging and repair.

The smallest next fix is to diagnose and repair the normalizer's Redis subscription mechanism, which should cascade into resolving T5/O1/O2 throughput issues.

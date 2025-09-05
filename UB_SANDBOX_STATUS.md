# Unibet Sandbox Implementation Status

## Summary
Successfully launched Unibet sandbox with automatic HTTP→CDP fallback mechanism while maintaining frozen BetRivers state.

## Architecture

### Component Overview
```
┌──────────────────────┐
│   UB Orchestrator    │ Port: 9136
│  (Management Layer)  │
└──────┬───────────────┘
       │
┌──────▼───────────────┐     ┌─────────────────────┐
│   UB HTTP Collector  │────►│   UB CDP Collector  │
│     Port: 9134      │     │     Port: 9135      │
│   (Primary Mode)    │     │  (Fallback Mode)    │
└──────────────────────┘     └─────────────────────┘
           │                           │
           ▼                           ▼
    ┌─────────────────────────────────────┐
    │         Redis Pub/Sub               │
    │    Channel: odds.raw.unibet         │
    └─────────────────────────────────────┘
```

### Auto-Fallback Logic
1. **HTTP Mode (Default)**
   - Attempts direct API calls to Kambi endpoints
   - Monitors success rate over sliding window
   - Tracks consecutive failures

2. **Fallback Triggers**
   - 6+ consecutive failures
   - Error rate > 50% over last 10 requests
   - No success in 15 minutes

3. **CDP Mode (Fallback)**
   - Activates on fallback signal
   - Uses Playwright with stealth settings
   - Scrapes data via browser automation

4. **Recovery Mechanism**
   - Attempts HTTP recovery every 5 minutes
   - Clears fallback signal for retry
   - Returns to HTTP mode on success

## Deployment

### Files Created
```
api/collectors/
├── ub_http/
│   ├── Dockerfile
│   └── ub_http_collector.py
├── ub_cdp/
│   ├── Dockerfile
│   └── ub_cdp_collector.py
└── ub_orchestrator/
    ├── Dockerfile
    └── ub_orchestrator.py

docker-compose.override.ub-sandbox.yml
```

### Launch Commands
```bash
# Start Unibet sandbox (with BR frozen)
docker compose -f docker-compose.yml \
  -f docker-compose.override.br-only.yml \
  -f docker-compose.override.ub-sandbox.yml \
  up -d ub-orchestrator

# Check status
curl http://localhost:9136/healthz
```

## Current Status

### Health Endpoints
- **Orchestrator**: `http://localhost:9136/healthz` ✅ ACTIVE
- **HTTP Collector**: `http://localhost:9134/healthz`
- **CDP Collector**: `http://localhost:9135/healthz`

### Latest Check
```json
{
  "status": "active",
  "consecutive_failures": 0,
  "last_success": "2025-08-29T13:10:54.724545"
}
```

## Safety Features

### 1. Isolation
- Runs in separate Docker network namespace
- Independent Redis channels (odds.raw.unibet)
- No interference with BR/SH streams

### 2. Resource Management
- HTTP collector: 60s polling interval (normal)
- CDP collector: 120s scraping interval (fallback)
- Orchestrator reduces polling to 5min in fallback mode

### 3. Auto-Recovery
- Automatic failover to CDP on HTTP blocks
- Periodic recovery attempts every 5 minutes
- Graceful degradation with reduced frequency

### 4. Self-Healing
- Auto-restarts crashed processes
- Rotates user agents on rate limiting
- Clears stale fallback signals

## Monitoring

### Key Metrics
- `consecutive_failures`: HTTP failure count
- `error_rate`: Percentage of failed requests
- `last_success`: Timestamp of last successful fetch
- `mode`: Current operating mode (http/fallback)
- `recovery_attempts`: Number of recovery tries

### Log Locations
```bash
# View orchestrator logs
docker logs splits-oddsfeed-ub-orchestrator-1

# Monitor Redis traffic
docker compose exec broker redis-cli PSUBSCRIBE "odds.raw.unibet"
```

## Rollback Plan

### To Stop Unibet Sandbox
```bash
# Stop UB orchestrator
docker compose -f docker-compose.yml \
  -f docker-compose.override.br-only.yml \
  -f docker-compose.override.ub-sandbox.yml \
  stop ub-orchestrator

# Remove UB containers
docker compose -f docker-compose.yml \
  -f docker-compose.override.br-only.yml \
  -f docker-compose.override.ub-sandbox.yml \
  rm -f ub-orchestrator
```

### Complete Removal
```bash
# Remove all UB files
rm -rf api/collectors/ub_*
rm docker-compose.override.ub-sandbox.yml
```

## Next Steps

### Production Readiness
- [ ] Add Unibet normalizer configuration
- [ ] Configure API endpoints for UB data
- [ ] Set up monitoring dashboards
- [ ] Implement alerting thresholds

### Optimization
- [ ] Tune fallback thresholds based on patterns
- [ ] Add more Kambi endpoints for coverage
- [ ] Implement proxy rotation for resilience

## Timestamp
Generated: 2025-08-29 09:11:00 EDT

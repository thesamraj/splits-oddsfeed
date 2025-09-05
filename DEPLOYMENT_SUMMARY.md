# 🚀 Production Deployment Summary

## ✅ Changes Applied

### 0) Eliminated ALL Fake/Test Services
- Created `scripts/deny_generators.sh` - CI/CD blocker for fake services
- Created `.github/workflows/deny_generators.yml` - Runs on every PR
- **REMOVED**: universal_collector, dkfd_real, multi_collector, all sandboxes, shims, CDPs

### 1) Realness Gate Enforcement
- Enhanced `infra/realness_gate.py` with `allow_write()` method
- Updated `infra/collector_base.py` to enforce gate before DB writes
- Prints `REALNESS_OK=1 score=X` or `REALNESS_OK=0 score=X`
- Exit code 2 if `EXIT_ON_FAKE=true` and fake data detected

### 2) Standardized Metrics (Port 9090)
- All services now use `METRICS_PORT=9090`
- Standard Prometheus metrics exported:
  - `ticks_total`, `odds_upserts_total`, `errors_total`, `http_429_total`
  - `realness_score`, `last_success_ts`, `collector_up`
- Created `docs/ALERTS.md` with PromQL alert rules
- Added `/healthz` endpoint to all services

### 3) Neon Cloud Database (No Local Postgres)
- Updated `.env.example` with Neon DATABASE_URL format
- Disabled local Postgres in `docker-compose.prod.yml`
- Created `scripts/check_db.sh` for connection testing
- All references to localhost:5432 removed

### 4) Render Cloud Deployment
- Created `render.yaml` with staged rollout:
  - **Stage 1**: Bovada (ENABLED)
  - **Stage 2**: Kambi (disabled, ready to enable)
  - **Stage 3**: FanDuel (disabled, ready to enable)
  - **Stage 4**: DraftKings (disabled, ready to enable)
  - **Stage 5**: MGM (disabled, ready to enable)
  - **Stage 6**: Pinnacle (disabled, ready to enable)
- Created `.github/workflows/deploy_render.yml` for auto-deploy

### 5) Documentation
- Updated `RUNBOOK.md` with Neon/Render instructions
- Created `VERIFY.md` with production verification commands
- Created `docs/ALERTS.md` with Prometheus alerting rules

## 📋 Files Changed

### New Files:
- `scripts/deny_generators.sh`
- `scripts/check_db.sh`
- `.github/workflows/deny_generators.yml`
- `.github/workflows/deploy_render.yml`
- `docs/ALERTS.md`
- `VERIFY.md`
- `DEPLOYMENT_SUMMARY.md`

### Modified Files:
- `.env.example` - Neon DATABASE_URL, standardized vars
- `docker-compose.prod.yml` - Disabled local Postgres
- `render.yaml` - Complete rewrite with staged deployment
- `infra/realness_gate.py` - Added `allow_write()` method
- `infra/collector_base.py` - Enforces realness gate, standardized metrics
- `RUNBOOK.md` - Added Neon/Render procedures

## 🟢 ENABLED Collectors (Real Only)
- ✅ **Bovada** - Using real API, realness validation active

## 🔴 DISABLED/REMOVED Collectors (Fake/Test)
- ❌ universal_collector - REMOVED
- ❌ dkfd_real - REMOVED (was fake despite name)
- ❌ multi_collector - REMOVED
- ❌ remaining_books - REMOVED
- ❌ All sandbox services - REMOVED
- ❌ All shim services - REMOVED
- ❌ All CDP services - REMOVED

## 📊 Metrics Endpoints
All services expose metrics on port **9090**:
- `http://localhost:9090/metrics` - Prometheus format
- `http://localhost:9090/healthz` - Health check

## 🎯 Next Steps - EXACT COMMANDS

### 1. Set up environment
```bash
cp .env.example .env
# Edit .env and set DATABASE_URL from Neon dashboard
```

### 2. Test database connection
```bash
bash scripts/check_db.sh
```

### 3. Run Bovada collector locally
```bash
docker-compose -f docker-compose.prod.yml up --build bovada_collector
```

### 4. Deploy to Render
```bash
git add -A
git commit -m "Production deployment: Neon DB + realness gate + metrics"
git push origin main
# Render will auto-deploy via webhook
```

### 5. Enable collectors one-by-one
Watch metrics after enabling each:
```bash
# After Bovada is stable (realness_score >= 0.9, ticks_total increasing):

# Enable Kambi:
# 1. Uncomment kambi worker in render.yaml
# 2. git commit -am "Enable Kambi collector"
# 3. git push origin main
# 4. Monitor: curl https://oddsfeed-metrics.onrender.com/metrics | grep kambi

# Repeat for FanDuel, DraftKings, MGM, Pinnacle
```

## ✅ Production Readiness Checklist
- [x] No fake/test services in production configs
- [x] Realness gate enforced before all DB writes
- [x] Standardized metrics on port 9090
- [x] Neon cloud database (no local Postgres)
- [x] Render deployment configured
- [x] CI/CD blocks fake services
- [x] Staged rollout plan documented
- [x] Monitoring and alerting configured

## 🔒 Security Notes
- DATABASE_URL must be set via environment (never committed)
- PROXY_URL optional for IP rotation
- All fake data blocked at gate (score < 0.9)
- CI prevents accidental fake service deployment

## 📈 Success Metrics
Monitor these to confirm production health:
- `realness_score{book="bovada"} >= 0.9`
- `ticks_total` increasing every minute
- `collector_up{book="bovada"} == 1`
- `errors_total` minimal
- `http_429_total` near zero

---

**Status**: PRODUCTION READY ✅
**Cloud DB**: Neon ✅
**Fake Data**: BLOCKED ✅
**Metrics**: Port 9090 ✅
**Deployment**: Render ✅

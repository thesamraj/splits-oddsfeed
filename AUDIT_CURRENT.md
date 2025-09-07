# Splits Oddsfeed Repository Audit

**Date**: 2025-01-07  
**Status**: ⚠️ OPERATIONAL WITH ISSUES

## 1. Service Matrix

| Service | Stack | Code Path | Health | Metrics | Env Required | Build | Runtime | Last Activity | Blockers |
|---------|-------|-----------|--------|---------|--------------|-------|---------|---------------|----------|
| **Bovada** | HTTP/Real | `collectors/bovada_real/` | `/health` | `:19081/metrics` | DATABASE_URL, REDIS_URL | ✅ Has Dockerfile | ❌ Not responding | Unknown | Docker daemon issues |
| **Normalizer** | Python | `normalizer/` | `/health` | `:19082/metrics` | DATABASE_URL, REDIS_URL | ✅ Has Dockerfile | ❌ Not responding | Unknown | Docker daemon issues |
| **Metrics Proxy** | Python | `services/metrics_proxy/` | `/health` | `:8000/metrics` | METRICS_TARGETS | ✅ Has Dockerfile | ❌ Not responding | Unknown | Docker daemon issues |
| **Kambi Browser** | Playwright | `collectors/kambi_browser/` | `/health` | `:19088/metrics` | MOBILE_PROXY_* | ✅ Has Dockerfile | 🔒 FROZEN | N/A | Requires mobile proxy |
| **Kambi Unified** | HTTP | `collectors/kambi_unified/` | `/health` | `/metrics` | PROXY_URL | ✅ Has Dockerfile | 🔒 FROZEN | N/A | Requires mobile proxy |
| **DraftKings WS** | WebSocket | `collectors/draftkings_ws/` | `/health` | `/metrics` | PROXY_URL | ✅ Has Dockerfile | ❌ Not configured | N/A | No env setup |
| **FanDuel WS** | WebSocket | `collectors/fanduel_ws/` | `/health` | `/metrics` | PROXY_URL | ✅ Has Dockerfile | ❌ Not configured | N/A | No env setup |
| **BetMGM Browser** | Playwright | `collectors/betmgm_browser/` | `/health` | `/metrics` | PROXY_URL | ✅ Has Dockerfile | ❌ Not configured | N/A | No env setup |
| **Pinnacle** | API | `collectors/pinnacle/` | `/health` | `/metrics` | PIN_USER, PIN_PASS | ✅ Has Dockerfile | ❌ Not configured | N/A | No credentials |

## 2. Environment Summary

### ✅ Configured
- `REAL_ONLY=true`
- `REALNESS_THRESHOLD=0.85`
- `LIGHT_SOCCER=true`
- `PROXY_HOST`, `PROXY_PORT`, `PROXY_USER`, `PROXY_PASS` (SmartProxy)
- `ZENROWS_API_KEY` (configured but blocked by Kambi)
- `BRIGHTDATA_API_KEY` (configured but blocked by Kambi)

### ⚠️ Missing/Issues
- `DATABASE_URL` - Not loaded in shell environment
- `REDIS_URL` - Not loaded in shell environment
- Mobile proxy credentials for Kambi (SOAX tested but blocked)

## 3. Runtime Checks (Local)

### Docker Status
- **Docker Desktop**: Running (PID 31892)
- **Docker daemon**: ⚠️ Hanging/unresponsive (docker ps timeout)
- **Compose validation**: ✅ Config valid (warning about obsolete 'version' attribute)

### Service Health
- **Metrics Proxy (8000)**: ❌ Not responding
- **Bovada (19081)**: ❌ Not responding  
- **Normalizer (19082)**: ❌ Not responding
- **Kambi Browser**: 🔒 Intentionally disabled

## 4. Data Plane

### PostgreSQL
- **Status**: ❌ No DATABASE_URL in environment
- **Required for**: Storing normalized odds data
- **Tables expected**: `odds`, `ticks`, `events`

### Redis
- **Status**: ❌ No REDIS_URL in environment
- **Required for**: Pub/sub between collectors and normalizer
- **Channels**: `odds:bovada`, `odds:betrivers`, etc.

## 5. Fix-First Checklist

1. **🔴 Restart Docker Desktop**
   ```bash
   # Docker daemon is hanging - restart Docker Desktop app
   # Then verify: docker ps
   ```

2. **🔴 Load environment variables**
   ```bash
   source .env
   export DATABASE_URL REDIS_URL
   ```

3. **🟡 Start core services**
   ```bash
   docker compose -f docker-compose.local.yml up -d bovada normalizer metrics-proxy
   ```

4. **🟡 Verify Bovada collection**
   ```bash
   curl http://localhost:19081/metrics | grep collector_up
   # Should show: collector_up{book="bovada"} 1.0
   ```

5. **🟢 Check data flow**
   ```bash
   # Watch ticks growing
   curl http://localhost:8000/metrics | grep ticks_total
   sleep 30
   curl http://localhost:8000/metrics | grep ticks_total
   ```

6. **🟢 Validate database writes**
   ```bash
   psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM odds WHERE brand='bovada' AND created_at > NOW() - INTERVAL '5 minutes'"
   ```

7. **🔵 Clean up old containers**
   ```bash
   docker system prune -f
   docker volume prune -f
   ```

8. **🔵 Fix compose file warning**
   ```bash
   # Remove 'version: 3.8' line from docker-compose.local.yml
   ```

9. **⚪ Document SOAX proxy issue**
   ```bash
   # SOAX mobile proxy (AT&T Wireless) confirmed working but Kambi still blocks
   # Need alternative: physical phone hotspot or different provider
   ```

10. **⚪ Prepare for production**
    ```bash
    # Use render.yaml for Render.com deployment (Bovada-only)
    # Main branch ready, kambi-mobile-proxy branch parked
    ```

## Key Findings

### Critical Issues
1. **Docker daemon hanging** - Preventing all container operations
2. **Environment not loaded** - DATABASE_URL and REDIS_URL not in shell

### Operational Issues  
1. **No services running** - Due to Docker issues
2. **Kambi blocked** - Even with legitimate AT&T Wireless mobile proxy

### Ready to Deploy
1. **Bovada collector** - Code complete, just needs Docker working
2. **Normalizer** - Ready to process data
3. **Render.com config** - render.yaml configured for production

## Recommendations

**Immediate Action**: Restart Docker Desktop and start Bovada-only stack

**Next Steps**: 
- Deploy to Render.com for stable production environment
- Continue investigating alternative mobile proxy providers for Kambi
- Consider physical device with mobile hotspot as most reliable option

---

*Generated by scripts/audit_repo.sh*
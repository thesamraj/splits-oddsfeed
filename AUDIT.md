# Splits OddsFeed Repository Audit

**Generated**: 2025-09-07  
**Repository**: ~/Desktop/splits-oddsfeed  
**Status**: ⚠️ **PARTIAL FAILURE** - Docker daemon hanging locally, remote services operational

## Service Matrix

| Service | Stack | Code Path | Health | Metrics | Env Needed | Build | Runtime | Last Activity | Blockers |
|---------|-------|-----------|--------|---------|------------|-------|---------|---------------|----------|
| **bovada** | HTTP/Real | collectors/bovada_real/ | :19081/health | :19081/metrics | DATABASE_URL, REDIS_URL, REALNESS_THRESHOLD | ✅ Exists | ⚠️ Port open, no response | N/A | Docker daemon hang |
| **normalizer** | Service | normalizer/ | :19082/health | :19082/metrics | DATABASE_URL, REDIS_URL | ✅ Exists | ⚠️ Port open, no response | N/A | Docker daemon hang |
| **metrics-proxy** | Service | services/metrics_proxy/ | N/A | :8000/metrics | METRICS_PORT | ✅ Exists | ⚠️ Port open, no response | N/A | Docker daemon hang |
| kambi_unified | Kambi/FROZEN | collectors/kambi_unified/ | N/A | N/A | KAMBI_* | ❄️ FROZEN | ❄️ FROZEN | N/A | Frozen per directive |
| kambi_browser | Browser/FROZEN | collectors/kambi_browser/ | N/A | N/A | KAMBI_* | ❄️ FROZEN | ❄️ FROZEN | N/A | Frozen per directive |
| kambi_fix | Kambi/FROZEN | collectors/kambi_fix/ | N/A | N/A | KAMBI_* | ❄️ FROZEN | ❄️ FROZEN | N/A | Frozen per directive |
| draftkings_api | HTTP | collectors/draftkings_api/ | N/A | N/A | DK_* | 🔍 Not in compose | N/A | N/A | Not configured |
| fanduel_api | HTTP | collectors/fanduel_api/ | N/A | N/A | FD_* | 🔍 Not in compose | N/A | N/A | Not configured |
| betmgm | HTTP | collectors/betmgm/ | N/A | N/A | MGM_* | 🔍 Not in compose | N/A | N/A | Not configured |

## Environment Summary

### ✅ Present
- DATABASE_URL (Neon: ep-solitary-hat-aejfw99z-pooler.c-2.us-east-2.aws.neon.tech)
- REDIS_URL (configured)
- REALNESS_THRESHOLD=0.85
- REAL_ONLY=true
- LIGHT_SOCCER=true
- BRIGHTDATA_* credentials (configured)
- ZENROWS_API_KEY (configured)
- PROXY_* settings (configured)

### ⚠️ Missing/Unknown
- Kambi credentials (FROZEN - not needed)
- DraftKings/FanDuel/BetMGM specific configs

## Runtime Checks (Local)

### Docker Status
- **Docker Daemon**: ❌ HANGING - All docker commands timeout
- **Ports Status**:
  - 8000 (metrics-proxy): ✅ Open but unresponsive
  - 19081 (bovada): ✅ Open but unresponsive  
  - 19082 (normalizer): ✅ Open but unresponsive

### Compose Validation
- **docker-compose.local.yml**: ✅ Valid structure
- **Services defined**: bovada, normalizer, metrics-proxy
- **Build capability**: Cannot verify (Docker hanging)

## Data Plane

### Database
- **Type**: PostgreSQL (Neon)
- **Host**: ep-solitary-hat-aejfw99z-pooler.c-2.us-east-2.aws.neon.tech
- **Status**: ⚠️ Cannot test locally (Docker required)

### Redis
- **Type**: Upstash
- **Status**: ⚠️ Cannot test locally (Docker required)

## Remote Status (DO Droplet 104.131.186.8)

✅ **OPERATIONAL** - Services running on remote:
- splits-oddsfeed-bovada-1 (Active)
- splits-oddsfeed-normalizer-1 (Active)
- Data flowing to Neon DB (confirmed in previous session)

## Fix-First Checklist

1. **🔴 Restart Docker Desktop** - Docker daemon is completely hung on local machine
   ```bash
   osascript -e 'quit app "Docker"'
   sleep 5
   open -a Docker
   # Wait for Docker to fully start (30-60 seconds)
   ```

2. **🟠 Clear Docker system** - After Docker restart, clean up:
   ```bash
   docker system prune -af --volumes
   docker compose -f docker-compose.local.yml down -v
   ```

3. **🟡 Rebuild and start local services** - Fresh start with compose:
   ```bash
   cd ~/Desktop/splits-oddsfeed
   docker compose -f docker-compose.local.yml build --no-cache
   docker compose -f docker-compose.local.yml up -d
   ```

4. **🟡 Verify health endpoints** - Confirm services are responding:
   ```bash
   curl -s http://localhost:19081/health | jq .
   curl -s http://localhost:19082/health | jq .
   curl -s http://localhost:8000/metrics | head -20
   ```

5. **🟢 Add LIGHT_SOCCER to local compose** - Reduce Bovada payload sizes:
   ```yaml
   # In docker-compose.local.yml under bovada environment
   - LIGHT_SOCCER=true
   ```

6. **🟢 Lower REALNESS_THRESHOLD locally** - Match remote config:
   ```bash
   # In .env
   REALNESS_THRESHOLD=0.30
   ```

7. **🟢 Add database monitoring** - Create simple health check:
   ```bash
   echo "SELECT COUNT(*) FROM odds WHERE ts > NOW() - INTERVAL '5 minutes';" | \
     docker compose run --rm normalizer psql $DATABASE_URL
   ```

8. **🔵 Document remote fallback** - Since remote is working:
   ```bash
   # Add to README or runbook:
   # If local Docker fails, use remote for testing:
   ssh root@104.131.186.8 'docker logs splits-oddsfeed-bovada-1 --tail 50'
   ```

9. **🔵 Create Docker health script** - Prevent future hangs:
   ```bash
   # scripts/check_docker.sh
   timeout 5 docker version > /dev/null 2>&1 || \
     (echo "Docker hung, restarting..." && osascript -e 'quit app "Docker"')
   ```

10. **⚪ Archive unused collectors** - Clean repo structure:
    ```bash
    mkdir -p collectors/_archive
    # Move unused: draftkings_*, fanduel_*, betmgm_*, etc.
    ```

## Summary

**Local Environment**: Docker daemon is completely hung, preventing all container operations. Services have ports open but are unresponsive, indicating containers may be running but frozen.

**Remote Environment**: Fully operational on DigitalOcean droplet with Bovada→Normalizer→Neon DB pipeline working.

**Critical Issue**: Local Docker Desktop needs restart. This is blocking all local development and testing.

**Recommendation**: Restart Docker Desktop immediately, then follow the fix-first checklist in order.
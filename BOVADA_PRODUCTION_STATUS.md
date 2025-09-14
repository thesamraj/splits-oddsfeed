# Production Status: Bovada-Only Deployment

## Current Status: ✅ PRODUCTION READY

### Active Components
- **Bovada Collector**: Fully operational, collecting NFL/NBA/NHL/MLB odds
- **Normalizer**: Processing Bovada data and storing in PostgreSQL
- **Metrics Proxy**: Aggregating health metrics from all services
- **API Service**: Serving odds data via REST endpoints

### Deployment Environments

#### Local Development (docker-compose.local.yml)
```bash
# Start Bovada-only stack locally
docker compose -f docker-compose.local.yml up -d

# Check health
curl http://localhost:8000/health
curl http://localhost:19081/metrics  # Bovada metrics
curl http://localhost:19082/metrics  # Normalizer metrics
```

#### Production (Render.com)
- **Metrics**: https://oddsfeed-metrics-proxy.onrender.com/metrics
- **Health**: https://oddsfeed-metrics-proxy.onrender.com/health
- **Bovada**: https://oddsfeed-bovada.onrender.com/metrics
- **Normalizer**: https://oddsfeed-normalizer.onrender.com/metrics

#### DigitalOcean (Alternative)
```bash
# Deploy using docker-compose.yml
docker compose up -d
```

### Monitoring Dashboards

#### Prometheus Queries
```promql
# Bovada collection rate
rate(ticks_total{book="bovada"}[5m])

# Success rate
collector_up{book="bovada"}

# Events collected (15min window)
odds_15m{book="bovada"}

# Error rate
rate(errors_total{book="bovada"}[5m])
```

#### Grafana Dashboard JSON
Location: `monitoring/grafana/dashboards/bovada-only.json`

Key panels:
- Collection Status (collector_up)
- Events/Minute (rate of ticks_total)
- 15-Minute Rolling Count (odds_15m)
- Error Types (errors_total by type)
- Response Times (request_duration_seconds)

### Database Schema

Bovada data is stored in normalized format:
- `odds` table: Current odds for all events
- `ticks` table: Historical odds changes
- `events` table: Event metadata

Query examples:
```sql
-- Current NFL odds
SELECT * FROM odds 
WHERE brand = 'bovada' 
  AND sport = 'american_football'
  AND league = 'nfl'
ORDER BY game_time;

-- Recent ticks
SELECT * FROM ticks
WHERE brand = 'bovada'
  AND created_at > NOW() - INTERVAL '15 minutes'
ORDER BY created_at DESC;
```

### Health Checks

#### Service-Level Health
- `/health` - Basic health check
- `/healthz` - Kubernetes-style health
- `/metrics` - Prometheus metrics

#### System Health Script
```bash
#!/bin/bash
# check_bovada_health.sh

echo "=== Bovada Collection Health ==="
curl -s http://localhost:8000/metrics | grep -E "collector_up.*bovada"
echo

echo "=== Events Collected (15min) ==="
curl -s http://localhost:8000/metrics | grep -E "odds_15m.*bovada"
echo

echo "=== Error Count ==="
curl -s http://localhost:8000/metrics | grep -E "errors_total.*bovada"
```

### Troubleshooting

#### Common Issues

1. **No data collected**
   - Check: `docker logs bovada-container-name`
   - Verify: Bovada.lv is accessible
   - Solution: Restart collector

2. **High error rate**
   - Check: Network connectivity
   - Verify: No rate limiting
   - Solution: Adjust POLL_INTERVAL_SEC

3. **Database connection issues**
   - Check: DATABASE_URL in .env
   - Verify: PostgreSQL is running
   - Solution: Check connection string

### Performance Metrics

Current production performance:
- **Collection Interval**: 30 seconds
- **Average Events/Cycle**: 500-1500 (varies by time)
- **Success Rate**: >95%
- **Resource Usage**: 
  - CPU: ~200m
  - Memory: ~256MB
  - Network: ~10MB/hour

### Cost Analysis

Monthly costs (Render.com):
- Bovada Collector: $7 (Starter)
- Normalizer: $7 (Starter)
- Metrics Proxy: $7 (Starter)
- Redis: $10 (Starter)
- **Total**: ~$31/month

Database (external):
- Neon PostgreSQL: Free tier sufficient
- Alternative: Supabase free tier

### Future Enhancements

1. **Add More Bovada Sports**
   - Currently: NFL, NBA, NHL, MLB
   - Planned: Soccer, Tennis, Golf

2. **Optimize Collection**
   - Implement differential updates
   - Add caching layer
   - Compress Redis messages

3. **Enhanced Monitoring**
   - Add Grafana alerts
   - Implement PagerDuty integration
   - Add SLA tracking

### Kambi Integration (When Ready)

The Kambi collectors are fully implemented but require mobile proxy infrastructure.
See `KAMBI_MOBILE_PROXY_INTEGRATION.md` for:
- Mobile proxy requirements
- Integration punch-list
- Cost estimates
- Testing procedures

To activate Kambi:
```bash
git checkout kambi-mobile-proxy
# Follow integration guide
```

### Support Contacts

- **Repository**: ~/Desktop/splits-oddsfeed
- **Branch**: 
  - Production: `main` or `master`
  - Kambi work: `kambi-mobile-proxy`
- **Documentation**: This file and KAMBI_MOBILE_PROXY_INTEGRATION.md

---

Last Updated: 2025-01-07
Status: Production Ready (Bovada-only)
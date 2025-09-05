# Production Runbook

## Enabling Next Sportsbook

### Pre-flight Checks
1. Verify current book is stable:
   ```bash
   bash scripts/verify_book.sh <current_book>
   ```
2. Check metrics show realness_score >= 0.9 for at least 1 hour
3. Confirm no 429 rate limits or errors in last hour

### Enable Book in Render
1. Go to Render dashboard
2. Find the service (e.g., `oddsfeed-betrivers`)
3. Settings → Change `autoDeploy` from `false` to `true`
4. Deploy → Manual Deploy
5. Watch logs for first 5 minutes

### Post-Enable Verification
```bash
# Wait 5 minutes for data to flow, then:
bash scripts/verify_book.sh <new_book>

# Monitor for 10 minutes
watch -n 60 'curl -s localhost:9090/metrics | grep "<new_book>"'
```

### Gate and Alerts to Watch
- Gate policy: maintain `realness_score ≥ 0.9` continuously for 30–60 minutes before enabling the next book.
- Watch these metrics (per book): `realness_score`, `odds_15m`, `collector_up`, `http_429_total`.
- If any regress, pause rollout and investigate using `/realness/report` and logs.

## Alert Runbooks

### Realness Score < 0.9
**Alert**: `realness_score{book="X"} < 0.9`

**Response**:
1. Check if book's website changed:
   ```bash
   docker logs $(docker ps -qf name=X-collector) --tail 100
   ```
2. If schema changed, disable collector immediately
3. Update mapper in normalizer
4. Re-enable after fix verified locally

### No Odds in 15 Minutes
**Alert**: `odds_15m{book="X"} == 0`

**Response**:
1. Check collector health:
   ```bash
   curl localhost:9090/metrics | grep "collector_up{book=\"X\"}"
   ```
2. Check for 429s:
   ```bash
   curl localhost:9090/metrics | grep "http_429_total{book=\"X\"}"
   ```
3. If 429s, increase backoff in collector config
4. If collector down, restart:
   ```bash
   docker restart X-collector
   ```

## Common Failure Modes

### 429 Rate Limits
- **Symptoms**: HTTP 429 responses, odds stop updating
- **Fix**: Increase TOKEN_BUCKET_RATE or add jitter
- **Prevention**: Monitor http_429_total metric

### Schema Changes
- **Symptoms**: Normalizer errors, realness score drops
- **Fix**: Update mapper, test locally first
- **Prevention**: Daily schema validation checks

### Proxy Blocks
- **Symptoms**: Connection refused, timeouts
- **Fix**: Rotate proxy, use residential IPs
- **Prevention**: Randomize User-Agent, add delays

### Memory Leaks
- **Symptoms**: Container restarts, OOM kills
- **Fix**: Add memory limits, fix leak in code
- **Prevention**: Monitor memory_usage_bytes metric

## Staged Rollout Schedule

| Stage | Books | Enable When | Notes |
|-------|-------|------------|-------|
| 0 | Bovada | NOW | Already live |
| 1 | BetRivers | Bovada stable 24h | Enable first, verify |
| 1 | Barstool | BetRivers stable 1h | Same infra as BetRivers |
| 1 | Caesars | Barstool stable 1h | Monitor for brand differences |
| 1 | SugarHouse | Caesars stable 1h | Watch for PA geo-blocks |
| 1 | Unibet | SugarHouse stable 1h | International, may need proxy |
| 2 | FanDuel | All Kambi stable 24h | WebSocket, watch memory |
| 2 | DraftKings | FanDuel stable 24h | Complex WS protocol |
| 3 | BetMGM | WS books stable 48h | Playwright browser automation |
| 3 | Pinnacle | BetMGM stable 24h | Requires API credentials |
| 3 | Bet365 | Pinnacle stable 24h | May need UK proxy |
| 3 | Stake | Bet365 stable 24h | Crypto-focused |
| 3 | PointsBet | Stake stable 24h | Australian, needs proxy |

### Cloud Deployment (AWS/GCP/Azure)
```bash
# Build and push images
docker-compose -f docker-compose.prod.yml build
docker-compose -f docker-compose.prod.yml push

# Deploy using your cloud provider's container service
# - AWS: ECS/Fargate
# - GCP: Cloud Run
# - Azure: Container Instances
```

## 📊 Monitoring

### Prometheus Metrics
- Endpoint: `http://<host>:9090/metrics`
- Key metrics:
  - `oddsfeed_books_active` - Number of active sportsbooks
  - `oddsfeed_realness_score` - Data validation scores
  - `oddsfeed_collector_up` - Collector health status
  - `oddsfeed_odds_records_rate` - Ingestion rate

### Grafana Setup
1. Add Prometheus data source: `http://metrics_server:9090`
2. Import dashboard from metrics server homepage
3. Set refresh to 30s

### Health Checks
```bash
# Check system health
curl http://localhost:8000/health
curl http://localhost:9090/health

# Check active books
curl http://localhost:8000/api/books/status

# Check realness validation
docker-compose -f docker-compose.prod.yml logs normalizer | grep "realness"
```

## 🔧 Operations

### Adding a New Real Collector

1. Create collector using base class:
```python
from infra.collector_base import RealCollectorBase

class NewBookCollector(RealCollectorBase):
    def __init__(self):
        super().__init__('bookname', strict_validation=True)

    def collect_data(self):
        # Implement real data collection
        pass
```

2. Add to docker-compose.prod.yml
3. Test realness validation passes
4. Deploy

### Database Maintenance

```bash
# Check database size
docker exec -it splits-oddsfeed_store_1 psql -U odds -d oddsfeed -c "
  SELECT pg_size_pretty(pg_database_size('oddsfeed'));
"

# Manual cleanup if needed
docker exec -it splits-oddsfeed_store_1 psql -U odds -d oddsfeed -c "
  DELETE FROM odds WHERE ts < NOW() - INTERVAL '24 hours';
"

# Backup database
docker exec splits-oddsfeed_store_1 pg_dump -U odds oddsfeed | gzip > backup_$(date +%Y%m%d).sql.gz
```

### Scaling

```bash
# Scale collectors
docker-compose -f docker-compose.prod.yml up -d --scale bovada_collector=2

# Monitor resource usage
docker stats

# Adjust resource limits in docker-compose.prod.yml
```

## 🚑 Troubleshooting

### Collector Not Working

1. Check logs:
```bash
docker-compose -f docker-compose.prod.yml logs <collector_name>
```

2. Check realness validation:
```bash
docker-compose -f docker-compose.prod.yml logs <collector_name> | grep "realness"
```

3. Test collector manually:
```bash
docker-compose -f docker-compose.prod.yml exec <collector_name> python -c "
from collector import Collector
c = Collector()
print(c.collect_data())
"
```

### High Memory Usage

1. Check retention settings:
```bash
grep RETENTION_HOURS docker-compose.prod.yml
```

2. Force cleanup:
```bash
docker-compose -f docker-compose.prod.yml restart db_cleaner
```

3. Check for memory leaks:
```bash
docker stats --no-stream
```

### Realness Gate Blocking Valid Data

1. Check realness scores:
```bash
docker-compose -f docker-compose.prod.yml logs | grep "score="
```

2. Temporarily switch to permissive mode:
```bash
# Set STRICT_VALIDATION=false in environment
docker-compose -f docker-compose.prod.yml up -d <collector_name>
```

3. Review blocked data samples in debug logs

### Database Connection Issues

1. Check connectivity:
```bash
docker-compose -f docker-compose.prod.yml exec api ping store
```

2. Verify credentials:
```bash
docker-compose -f docker-compose.prod.yml exec store psql -U odds -d oddsfeed -c "SELECT 1;"
```

3. Restart services:
```bash
docker-compose -f docker-compose.prod.yml restart
```

## 📈 Performance Tuning

### Optimize Collection Intervals
```yaml
# In docker-compose.prod.yml
environment:
  COLLECTION_INTERVAL: 60  # Adjust based on rate limits
```

### Database Indexes
```sql
-- Add indexes for common queries
CREATE INDEX idx_odds_book_ts ON odds(book, ts DESC);
CREATE INDEX idx_odds_event_id ON odds(event_id);
CREATE INDEX idx_events_sport ON events(sport);
```

### Redis Memory Management
```bash
# Check memory usage
docker exec splits-oddsfeed_broker_1 redis-cli INFO memory

# Set max memory
docker exec splits-oddsfeed_broker_1 redis-cli CONFIG SET maxmemory 512mb
```

## 🔐 Security

### Environment Variables
```bash
# Generate secure passwords
openssl rand -base64 32

# Set in production
export DB_PASSWORD=<secure_password>
export REDIS_PASSWORD=<secure_password>
```

### Network Security
- Use private networks for internal services
- Only expose API and metrics ports
- Enable TLS for production endpoints
- Implement rate limiting on API

### Monitoring for Anomalies
```bash
# Check for suspicious activity
docker-compose -f docker-compose.prod.yml logs | grep -E "403|401|attack|suspicious"

# Monitor realness gate blocks
docker-compose -f docker-compose.prod.yml logs | grep "Blocked"
```

## 📝 Logs

### Centralized Logging
```bash
# Aggregate all logs
docker-compose -f docker-compose.prod.yml logs > logs_$(date +%Y%m%d).txt

# Stream to logging service
docker-compose -f docker-compose.prod.yml logs -f | tee >(nc logserver.example.com 514)
```

### Log Rotation
```yaml
# Add to docker-compose.prod.yml
logging:
  driver: json-file
  options:
    max-size: "10m"
    max-file: "3"
```

## 🔄 Updates and Rollbacks

### Rolling Update
```bash
# Pull latest changes
git pull

# Rebuild specific service
docker-compose -f docker-compose.prod.yml build <service>

# Rolling restart
docker-compose -f docker-compose.prod.yml up -d --no-deps <service>
```

### Rollback
```bash
# Revert to previous version
git checkout <previous_commit>

# Rebuild and deploy
docker-compose -f docker-compose.prod.yml build
docker-compose -f docker-compose.prod.yml up -d
```

## 📞 Support Contacts

- **On-Call Engineer**: Check PagerDuty
- **Database Admin**: #database-team
- **Security Team**: security@company.com
- **Metrics/Monitoring**: #observability

## 🎯 SLOs

- **Availability**: 99.9% uptime
- **Latency**: p99 < 1s for API requests
- **Data Freshness**: Odds updated within 60s
- **Realness Score**: > 0.8 for all production data

## ⚡ Quick Commands

```bash
# Full restart
docker-compose -f docker-compose.prod.yml down && docker-compose -f docker-compose.prod.yml up -d

# Check everything
./scripts/deny_generators.sh && docker ps && curl localhost:9090/health

# Emergency stop
docker-compose -f docker-compose.prod.yml down

# View real-time metrics
watch -n 5 'curl -s localhost:9090/metrics | grep odds'
```

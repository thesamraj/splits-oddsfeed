# Production Hardening Verification

Run these commands to verify production hardening is complete.

## Reading the Realness Report

The realness report provides explainable scoring with 5 features:

### Feature Scores (0.0 - 1.0)
- **event_diversity**: Unique matchups ratio. Low = repeated games
- **team_entropy**: Distribution variety. Low = limited team coverage
- **price_variance**: Odds variation. Low = static or fake prices
- **time_spread**: Game time distribution. Low = all games at same time
- **duplicate_ratio**: Unique content ratio. Low = many duplicates

### Composite Score
Weighted average of all features. Must be ≥ 0.9 for production.

### Warm-up Period
- First 200 samples or 10 minutes = warm-up mode
- During warm-up: Data passes but metrics tagged `warmup=true`
- After warm-up: Strict enforcement, sub-threshold data quarantined

### Troubleshooting Low Scores

#### event_diversity < 0.5
- Parser not extracting unique event IDs
- Check: Are home/away teams properly parsed?
- Fix: Improve parse_event_id() and parse_teams()

#### duplicate_ratio < 0.5
- Same events being processed multiple times
- Check: Is dedup ring buffer working?
- Fix: Check dedup_dropped_total metric

## Common Low-Score Causes and Fixes

| Issue | Score Impact | Root Cause | Fix |
|-------|-------------|------------|-----|
| time_spread < 0.3 | -0.15 | All games at same time or using fetch time instead of start_time | Use event.startTime (epoch ms) not current time |
| duplicate_ratio < 0.5 | -0.10 | Same market published multiple times with price jitter | Round prices to 2 decimals in dedup key |
| event_diversity < 0.4 | -0.15 | Parser not extracting unique event IDs | Ensure parse_event_id uses stable Bovada ID |
| team_entropy < 0.5 | -0.10 | Team names not normalized | Apply parse_teams() with alias mapping |
| price_variance < 0.5 | -0.12 | Static/fake prices or parsing errors | Verify american odds extraction from price object |
| Warm-up mode | N/A | < 200 samples or < 10 minutes | Wait for warm-up to complete, monitor progress |
| Soccer endpoint | N/A | Response > 50MB causing incomplete reads | Skip soccer or increase timeout |
| Snapshot reuse | -0.20 | Same exact data polled repeatedly | Check snapshot_hash and skip if identical within 30s |
| Price jitter | -0.15 | Same market with ±1 cent price changes | Band prices to 5-cent intervals for dedup |
| Missing native IDs | -0.10 | Not using Bovada's event/market/outcome IDs | Prefer native IDs when present |

### Exact Curl Snippets

- Realness report (JSON):
```bash
curl -s localhost:9091/realness/report | jq '.composite_score, .top_failure_reasons, .feature_scores'
```

- Realness sample (sanitized):
```bash
curl -s localhost:9091/realness/sample | jq
```

- Feature metrics (Prometheus):
```bash
curl -s localhost:9090/metrics | grep -E 'realness_feat_|realness_score|dedup_dropped_total'
```

## Quick Verification Commands

### 1. Check for banned fake/generator services
```bash
grep -R -nE 'remaining_books|universal|dkfd_real|multi_collector|sandbox|shim|cdp|fake_data|test_data' docker-compose.* || echo "✅ CLEAN - No fake services found"
```

### 2. Verify realness gate is enforced
```bash
grep -R -n "allow_write(" collectors/ || echo "⚠️ MISSING GATE - Add allow_write() checks"
```

### 3. Confirm no local database references
```bash
grep -R -n "splits-oddsfeed-store-1\|localhost:5432" . --exclude-dir=.git || echo "✅ NO LOCAL DB - Using Neon only"
```

### 4. Test health endpoint
```bash
curl -sf localhost:9090/healthz && echo "✅ METRICS OK" || echo "❌ METRICS FAIL"
```

### 5. Verify metrics port standardization
```bash
grep -R "METRICS_PORT" . | grep -v "9090" || echo "✅ All using port 9090"
```

### 6. Check realness scores in logs
```bash
docker-compose logs | grep "REALNESS_OK=" | tail -5
```

### 7. Verify Neon connection
```bash
source .env && psql "$DATABASE_URL" -c "SELECT now();" && echo "✅ Neon DB connected"
```

### 8. List running containers (should be minimal)
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

## Full Production Readiness Check

```bash
#!/bin/bash
echo "🔍 Running full production verification..."

# Check for fakes
./scripts/deny_generators.sh || exit 1

# Check database
bash scripts/check_db.sh || exit 1

# Check metrics
curl -sf localhost:9090/metrics > /dev/null && echo "✅ Metrics server running" || echo "❌ Metrics server down"

# Check realness in logs
if docker-compose logs 2>/dev/null | grep -q "REALNESS_OK=1"; then
  echo "✅ Realness validation active"
else
  echo "⚠️ No realness validation seen in logs"
fi

echo "✅ Production verification complete"
```

## Security Verification

### Secret Scanning
```bash
echo "== Secret scan =="
bash scripts/verify_no_secrets.sh && echo "NO_SECRETS_OK"
```

### Denylist Check
```bash
echo "== Denylist check (prod paths) =="
bash scripts/deny_generators.sh --dry-run && echo "DENYLIST_OK" || echo "VIOLATIONS_FOUND"
```

### Sandbox References
```bash
echo "== Sandbox refs in compose (should be none in prod files) =="
grep -R -nE 'sandbox|cdp|shim|dkfd_real|remaining_books|multi_collector' docker-compose.prod.yml || echo "CLEAN_COMPOSE"
```

### Port Configuration
```bash
echo "== Ports sanity (9090 metrics, 9091 health) =="
grep -nE 'METRICS_PORT|HEALTH_PORT' .env.example collectors/base/*.py normalizer/src/normalizer/base*.py | head -10
```

### Render Health Checks
```bash
echo "== Render workers healthCheck (should not exist for workers) =="
grep -B2 -A2 "type: worker" render.yaml | grep "healthCheckPath" || echo "NO_WORKER_HEALTHCHECKS"
```

## Expected State When Correctly Configured

1. **No local Postgres running** - All data in Neon
2. **Only real collectors active** - Bovada first, then staged rollout
3. **All services expose metrics on 9090** - Standard port everywhere
4. **Realness scores >= 0.9** - No fake data passes through
5. **CI blocks fake services** - deny_generators.yml prevents accidents
6. **No secrets in repository** - All credentials in .env or cloud dashboard
7. **Secret scanners active** - Gitleaks + TruffleHog in CI

## Metrics to Monitor

```bash
# Key metrics to watch
curl -s localhost:9090/metrics | grep -E "realness_score|ticks_total|collector_up|http_429_total|errors_total"
```

Should see:
- `realness_score{book="bovada"} >= 0.9`
- `ticks_total{book="bovada"}` increasing
- `collector_up{book="bovada"} 1`
- `http_429_total` near 0
- `errors_total` minimal

## Data Retention Policies

### Automated Cleanup
- **Neon Retention**: GitHub Actions workflow runs weekly
  - Ticks older than 45 days are deleted
  - Odds older than 90 days are deleted
  - Schedule: Every Sunday at 4:05 AM UTC

### Manual Cleanup
```bash
# Run retention manually
psql "$DATABASE_URL" -c "DELETE FROM odds_ticks WHERE ts < now() - interval '45 days';"
psql "$DATABASE_URL" -c "DELETE FROM odds WHERE ts < now() - interval '90 days';"
```

### Monitoring
- Check retention job status: Actions tab in GitHub
- Slack notifications on failure (if SLACK_WEBHOOK configured)
- Database statistics reported after each run

## Sportsbook Verification

Run these commands to verify each book is operating correctly:

| Book | Verify Command | Status |
|------|---------------|--------|
| Bovada | `bash scripts/verify_book.sh bovada` | Stage 0 (LIVE) |
| BetRivers | `bash scripts/verify_book.sh betrivers` | Stage 1 (Kambi) |
| Barstool | `bash scripts/verify_book.sh barstool` | Stage 1 (Kambi) |
| Caesars | `bash scripts/verify_book.sh caesars` | Stage 1 (Kambi) |
| SugarHouse | `bash scripts/verify_book.sh sugarhouse` | Stage 1 (Kambi) |
| Unibet | `bash scripts/verify_book.sh unibet` | Stage 1 (Kambi) |
| FanDuel | `bash scripts/verify_book.sh fanduel` | Stage 2 (WS) |
| DraftKings | `bash scripts/verify_book.sh draftkings` | Stage 2 (WS) |
| BetMGM | `bash scripts/verify_book.sh betmgm` | Stage 3 |
| Pinnacle | `bash scripts/verify_book.sh pinnacle` | Stage 3 |
| Bet365 | `bash scripts/verify_book.sh bet365` | Stage 3 |
| Stake | `bash scripts/verify_book.sh stake` | Stage 3 |
| PointsBet | `bash scripts/verify_book.sh pointsbet` | Stage 3 |

### Quick Metrics Check
```bash
# Check all books at once
for book in bovada betrivers barstool caesars sugarhouse unibet fanduel draftkings betmgm pinnacle bet365 stake pointsbet; do
  echo -n "$book: "
  curl -s localhost:9090/metrics | grep "realness_score{book=\"$book\"}" | awk '{print "realness="$2}' | head -1 || echo "not active"
done
```

### 10-Minute Soak Test
```bash
# Monitor odds growth over 10 minutes
for i in {1..10}; do
  echo "=== Minute $i ==="
  curl -s localhost:9090/metrics | grep "odds_15m" | grep -v "#"
  sleep 60
done
```

### Kambi Brand Parity Check
```bash
# Compare Kambi brand counts
psql "$DATABASE_URL" -c "
  SELECT book, market_type, COUNT(*) as count
  FROM odds
  WHERE book IN ('betrivers', 'barstool', 'caesars', 'sugarhouse', 'unibet')
  AND ts > now() - interval '15 minutes'
  GROUP BY book, market_type
  ORDER BY book, market_type;
"
```

## Progressive Rollout

### Dry Run Test
```bash
# Test progressive rollout logic (dry run)
gh workflow run progressive_rollout -f dry_run=true

# Check workflow status
gh run list --workflow=progressive_rollout --limit=1

# View logs
gh run view $(gh run list --workflow=progressive_rollout --limit=1 --json databaseId -q '.[0].databaseId')
```

### Enable Auto-Rollout
To enable automatic progressive rollout:
1. Set GitHub secrets:
   - `METRICS_URL`: Your public metrics proxy URL
   - `DATABASE_URL`: Neon database connection string
   - `RENDER_API_KEY`: Render API key
   - `RENDER_OWNER_ID`: Render account owner ID
   - `SERVICE_BETRIVERS`, `SERVICE_BARSTOOL`, etc: Render service IDs

2. The workflow will run every 10 minutes and:
   - Check Bovada stability (realness ≥ 0.9, odds_15m > 0)
   - Enable BetRivers after 30m of Bovada stability
   - Enable each Kambi brand after 1h of previous stability
   - Enable FanDuel after 24h of all Kambi stability
   - Continue through all 13 books

### Manual Verification
```bash
# Check which books are ready
make verify
BOOKS="bovada" RUN_ONCE=true bash scripts/alert_monitor.sh

# Test metrics proxy
curl http://localhost:8000/metrics | grep realness_score
curl http://localhost:8000/healthz
```

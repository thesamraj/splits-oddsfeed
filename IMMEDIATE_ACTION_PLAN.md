# Immediate Action Plan: Next 4 Hours

## Hour 1: Fix Critical Infrastructure (0-60 min)

### 1.1 Fix Market Standardization (15 min)
```python
# normalizer/src/normalizer/main.py
CANONICAL_MARKETS = {
    'moneyline': 'h2h',
    'ml': 'h2h',
    'money_line': 'h2h',
    'h2h': 'h2h',
    'head2head': 'h2h',
    'spread': 'spread',
    'spreads': 'spread',
    'handicap': 'spread',
    'line': 'spread',
    'total': 'total',
    'totals': 'total',
    'over/under': 'total',
    'over_under': 'total',
    'ou': 'total'
}

def normalize_market(market_name):
    return CANONICAL_MARKETS.get(str(market_name).lower(), market_name)
```

### 1.2 Add Container Health Checks (15 min)
```yaml
# docker-compose.yml additions
healthcheck:
  test: ["CMD", "python", "-c", "import requests; requests.get('http://localhost:9200/health')"]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 40s

restart: unless-stopped  # Add to all collectors
```

### 1.3 Fix BetRivers Markets (15 min)
```bash
# Restart BR+ collector properly
docker-compose -f docker-compose.yml \
  -f docker-compose.override.br-plus.yml \
  up -d --build collector-br-http-plus

# Verify markets flowing
watch -n 5 "docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -c \
  \"SELECT market, COUNT(*) FROM odds WHERE book='betrivers' \
  AND ts > now() - interval '5 min' GROUP BY market;\""
```

### 1.4 Quick Monitoring Setup (15 min)
```bash
# Create simple monitoring script
cat > monitor.sh << 'EOF'
#!/bin/bash
while true; do
  clear
  echo "=== SPORTSBOOK MONITOR $(date) ==="
  docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -t -c "
    SELECT
      book,
      COUNT(*) as cnt,
      ROUND(EXTRACT(EPOCH FROM (now()-MAX(ts)))) as sec_ago
    FROM odds
    WHERE ts > now() - interval '15 min'
    GROUP BY book
    ORDER BY cnt DESC;"

  # Alert if any book < 100 odds
  LOW_BOOKS=$(docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -t -c "
    SELECT book FROM (
      SELECT book, COUNT(*) as cnt
      FROM odds WHERE ts > now() - interval '15 min'
      GROUP BY book
    ) t WHERE cnt < 100;")

  if [ ! -z "$LOW_BOOKS" ]; then
    echo "⚠️  ALERT: Low volume books: $LOW_BOOKS"
  fi

  sleep 30
done
EOF
chmod +x monitor.sh
```

---

## Hour 2: Deploy Ready Books (60-120 min)

### 2.1 Deploy Caesars (20 min)
```yaml
# docker-compose.override.caesars.yml
services:
  collector-caesars:
    image: python:3.11-slim
    command: python /app/caesars_collector.py
    environment:
      - REDIS_URL=redis://broker:6379
      - API_URL=https://api.williamhill.us/v2/
      - POLL_INTERVAL=30
    volumes:
      - ./collectors/caesars:/app
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "python", "-c", "print('ok')"]
      interval: 30s
```

```python
# collectors/caesars/caesars_collector.py
import asyncio
import aiohttp
import redis
import json
import time

async def collect():
    r = redis.from_url('redis://broker:6379')
    async with aiohttp.ClientSession() as session:
        url = "https://api.williamhill.us/v2/sports/americanfootball/events"
        async with session.get(url) as resp:
            data = await resp.json()
            # Transform to standard format
            events = []
            for event in data.get('events', []):
                events.append({
                    'event_id': event['id'],
                    'home_team': event['home'],
                    'away_team': event['away'],
                    'odds': extract_odds(event)
                })

            payload = {
                'book': 'caesars',
                'brand': 'caesars',
                'events': events,
                'ts': int(time.time() * 1000)
            }
            r.publish('odds.raw.caesars', json.dumps(payload))

asyncio.run(collect())
```

### 2.2 Create Stake Normalizer (20 min)
```python
# Add to normalizer/src/normalizer/main.py
elif book == "stake":
    await self.process_stake_message(book, payload)

async def process_stake_message(self, book: str, payload: dict):
    """Process Stake odds format"""
    events = payload.get('events', [])

    for event in events:
        normalized = {
            'event_id': event.get('id'),
            'book': 'stake',
            'sport': event.get('sport', 'unknown'),
            'home_team': event.get('home'),
            'away_team': event.get('away'),
            'odds': []
        }

        # Extract markets
        for market in event.get('markets', []):
            market_type = normalize_market(market.get('type'))

            if market_type == 'h2h':
                normalized['odds'].append({
                    'market': 'h2h',
                    'price_home': market.get('home_odds'),
                    'price_away': market.get('away_odds')
                })
            # Add spread, total handling

        await self.store_event(normalized)
```

### 2.3 Create Bet365 Normalizer (20 min)
```python
# Similar to Stake, add B365 format handling
elif book == "bet365":
    await self.process_bet365_message(book, payload)
```

---

## Hour 3: Fix Degraded Books (120-180 min)

### 3.1 Fix FanDuel with Browser Collector (30 min)
```javascript
// collectors/fanduel_browser/collector.js
const playwright = require('playwright');

async function collectFanDuel() {
    const browser = await playwright.chromium.launch({
        headless: true,
        args: ['--disable-blink-features=AutomationControlled']
    });

    const page = await browser.newPage();
    await page.goto('https://sportsbook.fanduel.com/navigation/nfl');

    // Wait for odds to load
    await page.waitForSelector('[data-test="odds-value"]');

    // Extract odds
    const odds = await page.evaluate(() => {
        const events = [];
        document.querySelectorAll('[data-test="event-card"]').forEach(card => {
            events.push({
                home: card.querySelector('.home-team').innerText,
                away: card.querySelector('.away-team').innerText,
                odds: {
                    home: card.querySelector('.home-odds').innerText,
                    away: card.querySelector('.away-odds').innerText
                }
            });
        });
        return events;
    });

    // Publish to Redis
    await publishToRedis('odds.raw.fanduel', odds);
    await browser.close();
}
```

### 3.2 Fix BetMGM Normalizer (30 min)
```bash
# Check why normalizer is crashing
docker logs betmgm-normalizer --tail 100

# Fix the issue (likely format mismatch)
# Restart with proper error handling
docker-compose restart betmgm-normalizer
```

---

## Hour 4: Infrastructure Hardening (180-240 min)

### 4.1 Deploy Prometheus + Grafana (30 min)
```yaml
# docker-compose.monitoring.yml
services:
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
```

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'collectors'
    static_configs:
      - targets: ['collector-dk:9200', 'collector-fd:9201']
    scrape_interval: 15s
```

### 4.2 Add Connection Pooling (20 min)
```yaml
# Deploy pgbouncer
services:
  pgbouncer:
    image: edoburu/pgbouncer:latest
    environment:
      - DATABASES_HOST=store
      - DATABASES_PORT=5432
      - DATABASES_DBNAME=oddsfeed
      - POOL_MODE=transaction
      - MAX_CLIENT_CONN=1000
      - DEFAULT_POOL_SIZE=25
    ports:
      - "6432:6432"
```

### 4.3 Implement Redis Cache (10 min)
```python
# api/main.py additions
import redis
cache = redis.from_url('redis://broker:6379/1')

@app.get("/odds")
async def get_odds(book: str, minutes: int = 15):
    # Check cache first
    cache_key = f"odds:{book}:{minutes}"
    cached = cache.get(cache_key)
    if cached:
        return json.loads(cached)

    # Query database
    result = await db.fetch_odds(book, minutes)

    # Cache for 30 seconds
    cache.setex(cache_key, 30, json.dumps(result))
    return result
```

---

## Validation Checklist

After each hour, verify:

### Hour 1 Validation
- [ ] BetRivers showing spreads/totals
- [ ] Market names standardized
- [ ] Health checks working
- [ ] Monitor script running

### Hour 2 Validation
- [ ] Caesars collector running
- [ ] Stake normalizer processing
- [ ] Bet365 normalizer processing
- [ ] All producing >100 odds/15min

### Hour 3 Validation
- [ ] FanDuel >1K odds/15min
- [ ] BetMGM >500 odds/15min
- [ ] No normalizer crashes
- [ ] All markets present

### Hour 4 Validation
- [ ] Grafana dashboards showing data
- [ ] Alerts configured
- [ ] API response <500ms
- [ ] Cache hit rate >50%

---

## Emergency Rollback Commands

```bash
# If something breaks, quick rollback:

# Stop all collectors
docker-compose stop

# Restore previous version
git checkout HEAD~1
docker-compose up -d

# Clear bad data
docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -c \
  "DELETE FROM odds WHERE ts > now() - interval '1 hour' AND book='${PROBLEM_BOOK}';"
```

---

## Success Criteria

By end of 4 hours:
- ✅ 10/13 books operational
- ✅ All books >100 odds/15min
- ✅ All markets (h2h/spread/total) present
- ✅ Monitoring dashboard live
- ✅ Alerts configured
- ✅ <500ms API response
- ✅ Auto-recovery enabled

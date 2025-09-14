# BoltOdds Collector

Collector for BoltOdds API with automatic endpoint discovery and normalization.

## Environment Variables

- `BOLT_BASE_URL`: Base URL for BoltOdds API (required)
- `BOLT_API_TOKEN`: API authentication token (default provided)
- `REDIS_URL`: Redis connection URL
- `POLL_INTERVAL`: Polling interval in seconds (default: 30)
- `PORT`: HTTP server port for health/metrics (default: 8000)

## Discovery Logic

1. Checks for OpenAPI/Swagger spec at common paths
2. If not found, probes common odds endpoint patterns
3. Automatically detects auth method (Bearer vs X-API-Key)
4. Discovers and caches available endpoints

## Data Flow

1. Fetches odds data from discovered endpoints
2. Normalizes to internal schema (book, event_id, teams, markets, etc.)
3. Publishes to Redis channel: `odds.raw.bolt.staging`
4. To switch to production: change channel to `odds.raw.bolt`

## Endpoints

- `/healthz`: Health check with last fetch timestamp
- `/metrics`: Prometheus metrics (collector_up, ticks_total, last_fetch_seconds)

## Local Testing

```bash
docker compose -f docker-compose.local.yml build boltodds
docker compose -f docker-compose.local.yml up -d boltodds
curl http://localhost:19098/healthz
```
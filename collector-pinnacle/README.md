# Pinnacle Sports Collector

Official odds collector for Pinnacle Sports API integration with splits-oddsfeed.

## Features

- **Dual Mode Operation**: Mock mode (no credentials required) and real API mode
- **Rate Limiting**: Configurable rate limiting with daily budget controls
- **ETag Caching**: Efficient caching to minimize API calls
- **Prometheus Metrics**: Comprehensive monitoring and observability
- **Docker Ready**: Multi-arch container builds (linux/amd64, linux/arm64)

## Configuration

All configuration is done via environment variables with the `PIN_` prefix:

### Core Settings
- `PIN_USE_MOCK`: Set to `true` for mock mode, `false` for real API (default: `true`)
- `PIN_USERNAME`: Pinnacle API username (required for real mode)
- `PIN_PASSWORD`: Pinnacle API password (required for real mode)
- `PIN_BASE_URL`: Pinnacle API base URL (default: `https://api.pinnacle.com`)

### Data Settings
- `PIN_SPORT_ID`: Sport ID to collect (default: `29` for NFL)
- `PIN_LEAGUE_IDS`: Comma-separated league IDs to filter (optional)
- `PIN_MARKETS`: Markets to collect (default: `moneyline,spreads,totals`)

### Performance Settings
- `PIN_INTERVAL`: Collection interval in seconds (default: `5`)
- `PIN_TIMEOUT_CONNECT`: HTTP connect timeout (default: `4`)
- `PIN_TIMEOUT_READ`: HTTP read timeout (default: `6`)
- `PIN_RATE_LIMIT_SLEEP_MS`: Rate limiting sleep in milliseconds (default: `250`)

### Budget Controls
- `PIN_DAILY_REQUEST_BUDGET`: Daily API request limit (default: `10000`)
- `PIN_BUDGET_MIN_REMAINING`: Minimum remaining requests before throttling (default: `100`)

### Caching
- `PIN_ETAG_CACHE`: Enable ETag caching (default: `true`)

### Monitoring
- `PIN_PROM_PORT`: Prometheus metrics port (default: `9110`)

## Usage

### Mock Mode (No Credentials Required)
```bash
PIN_USE_MOCK=true docker run ghcr.io/thesamraj/splits-oddsfeed/collector-pinnacle:latest
```

### Real API Mode
```bash
PIN_USE_MOCK=false \
PIN_USERNAME=your_username \
PIN_PASSWORD=your_password \
docker run ghcr.io/thesamraj/splits-oddsfeed/collector-pinnacle:latest
```

### With Docker Compose
```bash
# Mock mode
make pinnacle-mock

# Real mode (requires PIN_USERNAME and PIN_PASSWORD environment variables)
make pinnacle-real
```

## Metrics

The collector exposes Prometheus metrics on port 9110:

- `collector_fetch_total`: Total API fetch attempts by status
- `collector_budget_daily_total`: Current daily request count
- `collector_budget_remaining`: Remaining requests in daily budget
- `collector_last_success_timestamp_seconds`: Timestamp of last successful fetch

## Data Flow

1. **Collection**: Fetches fixtures and odds from Pinnacle API (or generates mock data)
2. **Publishing**: Publishes raw data to Redis channel `odds.raw.pinnacle`
3. **Normalization**: Data flows through normalizer to unified schema
4. **Storage**: Normalized data stored in TimescaleDB
5. **API**: Data accessible via REST API with filtering

## Development

### Running Tests
```bash
cd collector-pinnacle
poetry install
poetry run pytest tests/ -v
```

### Code Quality
```bash
# Lint
poetry run ruff check .

# Format
poetry run black .

# Type checking
poetry run mypy .
```

### Local Development
```bash
# Install dependencies
poetry install

# Run in mock mode
poetry run python -m collector_pinnacle.main
```

## API Compliance

- Respects Pinnacle's rate limits and ToS
- Implements proper authentication
- Uses ETag caching to minimize requests
- Tracks daily request budgets
- Handles API errors gracefully

## Integration

This collector integrates seamlessly with the splits-oddsfeed ecosystem:

- **Redis**: Publishes to `odds.raw.pinnacle` channel
- **Normalizer**: Automatic data normalization via `pinnacle_mapper.py`
- **Database**: Events and odds stored in TimescaleDB
- **API**: Data queryable via `/odds?book=pin` endpoint
- **Monitoring**: Grafana dashboards for operational visibility

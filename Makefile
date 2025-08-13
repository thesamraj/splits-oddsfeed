.PHONY: up down tail lint fmt typecheck test latency ingest-mock pinnacle-mock pinnacle-real migrate db-status help

# Default target
help:
	@echo "Available targets:"
	@echo "  up         - Start all services with docker-compose"
	@echo "  down       - Stop all services and remove volumes"
	@echo "  tail       - Follow logs from all services"
	@echo "  lint       - Run ruff linter on all services"
	@echo "  fmt        - Format code with black on all services"
	@echo "  typecheck  - Run mypy type checking on all services"
	@echo "  test       - Run pytest tests"
	@echo "  latency    - Placeholder for latency testing"
	@echo "  ingest-mock - Run aggregator collector in mock mode"
	@echo "  pinnacle-mock - Run Pinnacle collector in mock mode"
	@echo "  pinnacle-real - Run Pinnacle collector in real mode (requires PIN_USERNAME/PIN_PASSWORD)"
	@echo "  migrate    - Apply database migrations"
	@echo "  db-status  - Show database hypertable and policy status"

# Docker operations
up:
	cp .env.example .env
	docker-compose up -d --build

down:
	docker-compose down -v

tail:
	docker-compose logs -f --tail=200

# Code quality
lint:
	@echo "Running ruff on all services..."
	@for service in api normalizer collector-dk collector-fd collector-mgm collector-b365 collector-stake collector-agg collector-pinnacle proxy-manager; do \
		echo "Linting $$service..."; \
		cd $$service && poetry run ruff check . && cd ..; \
	done

fmt:
	@echo "Running black on all services..."
	@for service in api normalizer collector-dk collector-fd collector-mgm collector-b365 collector-stake collector-agg collector-pinnacle proxy-manager; do \
		echo "Formatting $$service..."; \
		cd $$service && poetry run black . && cd ..; \
	done

typecheck:
	@echo "Running mypy on all services..."
	@for service in api normalizer collector-dk collector-fd collector-mgm collector-b365 collector-stake collector-agg collector-pinnacle proxy-manager; do \
		echo "Type checking $$service..."; \
		cd $$service && poetry run mypy . && cd ..; \
	done

# Testing
test:
	@echo "Running tests..."
	@if [ ! -f .env ]; then cp .env.example .env; fi
	PYTHONPATH=. poetry run pytest tests/ -v

# Placeholder for latency testing
latency:
	@echo "Latency testing not yet implemented"

# Mock mode testing
ingest-mock:
	@echo "Starting aggregator in mock mode..."
	@if [ ! -f .env ]; then cp .env.example .env; fi
	@echo "AGG_USE_MOCK=true" >> .env
	docker-compose up -d broker store
	@echo "Waiting for dependencies..."
	sleep 10
	docker-compose up --build collector-agg

# Pinnacle smoke tests
pinnacle-mock:
	@echo "Starting Pinnacle collector in mock mode..."
	@if [ ! -f .env ]; then cp .env.example .env; fi
	@echo "PIN_USE_MOCK=true" >> .env
	docker-compose up -d broker store normalizer
	@echo "Waiting for dependencies..."
	sleep 10
	docker-compose up --build collector-pinnacle

pinnacle-real:
	@echo "Starting Pinnacle collector in real mode..."
	@if [ ! -f .env ]; then cp .env.example .env; fi
	@echo "PIN_USE_MOCK=false" >> .env
	@test -n "$$PIN_USERNAME" || (echo "PIN_USERNAME environment variable required"; exit 1)
	@test -n "$$PIN_PASSWORD" || (echo "PIN_PASSWORD environment variable required"; exit 1)
	@echo "PIN_USERNAME=$$PIN_USERNAME" >> .env
	@echo "PIN_PASSWORD=$$PIN_PASSWORD" >> .env
	docker-compose up -d broker store normalizer
	@echo "Waiting for dependencies..."
	sleep 10
	docker-compose up --build collector-pinnacle

release: ## release a new version: make release VER=v0.1.X
	@test -n "$(VER)" || (echo "VER required"; exit 1)
	git fetch --tags && git tag $(VER) && git push origin $(VER)

wait-ghcr: ## wait for GHCR manifest (amd64+arm64)
	@test -n "$(VER)" || (echo "VER required"; exit 1)
	IMG=ghcr.io/thesamraj/splits-oddsfeed/api; \
	until docker buildx imagetools inspect $$IMG:$(VER) 2>/dev/null | grep -q "linux/arm64"; do echo "waiting for $$IMG:$(VER)"; sleep 8; done

deploy: ## deploy released version locally
	@test -n "$(VER)" || (echo "VER required"; exit 1)
	$(MAKE) wait-ghcr VER=$(VER)
	printf "services:\n  api:\n    image: ghcr.io/thesamraj/splits-oddsfeed/api:$(VER)\n" > docker-compose.override.yml
	docker compose pull api && docker compose up -d api

migrate:
	docker compose up -d store
	chmod +x bin/migrate.sh
	bin/migrate.sh

db-status:
	docker compose exec -T store psql -U odds -d oddsfeed -c "SELECT hypertable_name, compression_enabled FROM timescaledb_information.hypertables WHERE hypertable_name='odds';"
	docker compose exec -T store psql -U odds -d oddsfeed -c "SELECT job_id, proc_name, hypertable_name, config FROM timescaledb_information.jobs WHERE hypertable_name='odds' ORDER BY proc_name;"
	docker compose exec -T store psql -U odds -d oddsfeed -Atc "SELECT indexname FROM pg_indexes WHERE tablename='odds' ORDER BY indexname;"

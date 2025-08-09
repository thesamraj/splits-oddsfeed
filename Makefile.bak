.PHONY: up down tail lint fmt typecheck test latency help

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
	@for service in api normalizer collector-dk collector-fd collector-mgm collector-b365 collector-stake proxy-manager; do \
		echo "Linting $$service..."; \
		cd $$service && poetry run ruff check . && cd ..; \
	done

fmt:
	@echo "Running black on all services..."
	@for service in api normalizer collector-dk collector-fd collector-mgm collector-b365 collector-stake proxy-manager; do \
		echo "Formatting $$service..."; \
		cd $$service && poetry run black . && cd ..; \
	done

typecheck:
	@echo "Running mypy on all services..."
	@for service in api normalizer collector-dk collector-fd collector-mgm collector-b365 collector-stake proxy-manager; do \
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

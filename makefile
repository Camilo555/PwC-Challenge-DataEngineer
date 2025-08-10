.PHONY: help install dev-install test lint format type-check clean run-api run-etl docker-up docker-down

# Variables
PYTHON := poetry run python
PYTEST := poetry run pytest
RUFF := poetry run ruff
BLACK := poetry run black
MYPY := poetry run mypy

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	poetry install --only main

dev-install: ## Install all dependencies including dev
	poetry install

test: ## Run all tests
	$(PYTEST) tests/ -v --cov=src/de_challenge --cov-report=term-missing

test-unit: ## Run unit tests only
	$(PYTEST) tests/unit/ -v

test-integration: ## Run integration tests only
	$(PYTEST) tests/integration/ -v

lint: ## Run linting with ruff
	$(RUFF) check src/ tests/

format: ## Format code with black and ruff
	$(BLACK) src/ tests/
	$(RUFF) check --fix src/ tests/

type-check: ## Run type checking with mypy
	$(MYPY) src/

clean: ## Clean generated files and caches
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf dist/ build/

# Database operations
db-init: ## Initialize database schema
	$(PYTHON) scripts/init_db.py

db-migrate: ## Run database migrations
	poetry run alembic upgrade head

db-seed: ## Seed database with initial data
	$(PYTHON) scripts/seed_data.py

# ETL operations
etl-bronze: ## Run Bronze layer ETL
	$(PYTHON) -m src.de_challenge.etl.bronze.ingest_structured

etl-silver: ## Run Silver layer ETL
	$(PYTHON) -m src.de_challenge.etl.silver.clean_transactions

etl-gold: ## Run Gold layer ETL
	$(PYTHON) -m src.de_challenge.etl.gold.build_dimensions

etl-full: ## Run complete ETL pipeline
	$(PYTHON) scripts/run_etl.py

# API operations
run-api: ## Run FastAPI application
	poetry run uvicorn de_challenge.api.main:app --host 0.0.0.0 --port 8000 --reload

run-api-prod: ## Run FastAPI in production mode
	poetry run uvicorn de_challenge.api.main:app --host 0.0.0.0 --port 8000 --workers 4

# Docker operations
docker-build: ## Build Docker images
	docker-compose build

docker-up: ## Start all services with Docker Compose
	docker-compose up -d

docker-down: ## Stop all Docker services
	docker-compose down

docker-logs: ## Show Docker logs
	docker-compose logs -f

# Vector search operations
vector-index: ## Build vector search index
	$(PYTHON) -m src.de_challenge.vector_search.indexer

# Development shortcuts
dev: dev-install ## Setup development environment
	cp .env.example .env
	mkdir -p data/{raw,bronze,silver,gold,warehouse}
	mkdir -p logs

check: lint type-check test ## Run all checks (lint, type-check, test)

all: clean dev-install check ## Clean, install, and run all checks
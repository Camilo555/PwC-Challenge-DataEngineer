.PHONY: help install test lint format clean run-etl api-dev run-api etl-full etl-bronze etl-silver etl-gold docker-up docker-down

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install all dependencies with Poetry (including dev tools)
	poetry install --with dev
	poetry run pre-commit install

test: ## Run test suite with coverage
	poetry run pytest tests/ -v --cov=src/de_challenge --cov-report=term-missing

test-unit: ## Run only unit tests
	poetry run pytest tests/unit/ -v

test-integration: ## Run only integration tests
	poetry run pytest tests/integration/ -v

lint: ## Run linting with ruff
	poetry run ruff check src/ tests/
	poetry run mypy src/

format: ## Format code with black and ruff
	poetry run black src/ tests/
	poetry run ruff check --fix src/ tests/

clean: ## Clean up generated files
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info

run-etl: ## Run the complete ETL pipeline (Bronze -> Silver -> Gold)
	poetry run python scripts/run_etl.py

run-bronze: ## Run only Bronze layer ETL
	poetry run python scripts/run_bronze.py

run-silver: ## Run only Silver layer ETL
	poetry run python scripts/run_silver.py

run-gold: ## Run only Gold layer ETL
	poetry run python scripts/run_gold.py

api-dev: ## Start FastAPI in development mode
	poetry run uvicorn de_challenge.api.main:app --reload --host 0.0.0.0 --port 8000

run-api: ## Alias: start FastAPI in development mode
	$(MAKE) api-dev

api-prod: ## Start FastAPI in production mode
	poetry run gunicorn de_challenge.api.main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000

docker-build: ## Build Docker images
	docker-compose build

docker-up: ## Start all services with Docker Compose
	docker-compose up -d

docker-down: ## Stop all Docker services
	docker-compose down

docker-logs: ## Show Docker logs
	docker-compose logs -f

init-db: ## Initialize database schema
	poetry run python scripts/init_db.py

seed-data: ## Seed database with sample data
	poetry run python scripts/seed_data.py

quality-check: ## Run all quality checks
	make lint
	make test
	@echo "✅ All quality checks passed!"

setup-dev: ## Complete development environment setup
	make install
	cp .env.example .env
	make init-db
	@echo "✅ Development environment ready!"

# Aliases matching README
etl-full: ## Alias: Run the complete ETL pipeline
	$(MAKE) run-etl

etl-bronze: ## Alias: Run only Bronze layer ETL
	$(MAKE) run-bronze

etl-silver: ## Alias: Run only Silver layer ETL
	$(MAKE) run-silver

etl-gold: ## Alias: Run only Gold layer ETL
	$(MAKE) run-gold
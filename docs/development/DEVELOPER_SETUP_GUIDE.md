# PwC Retail Data Platform - Developer Setup Guide

## Table of Contents

1. [Quick Start](#quick-start)
2. [Development Environment Setup](#development-environment-setup)
3. [Code Standards & Guidelines](#code-standards--guidelines)
4. [Testing Framework](#testing-framework)
5. [Debugging & Profiling](#debugging--profiling)
6. [Contributing Guidelines](#contributing-guidelines)
7. [IDE Configuration](#ide-configuration)
8. [Troubleshooting](#troubleshooting)

## Quick Start

### Prerequisites

Ensure you have the following installed:

```bash
# Required Software
Python 3.10 or 3.11
Git
Docker & Docker Compose
Node.js 18+ (for frontend tooling)
Poetry (Python dependency manager)

# Optional but Recommended
VS Code with Python extension
PyCharm Professional (or Community)
pgAdmin or DBeaver (database GUI)
```

### 5-Minute Setup

```bash
# 1. Clone repository
git clone https://github.com/company/pwc-retail-data-platform.git
cd pwc-retail-data-platform

# 2. Setup Python environment
poetry install
poetry shell

# 3. Copy environment file
cp .env.example .env

# 4. Start local services
docker-compose up -d postgres redis elasticsearch typesense

# 5. Initialize database
poetry run python scripts/init_db.py

# 6. Load sample data
poetry run python scripts/seed_data.py

# 7. Start development server
poetry run uvicorn src.api.main:app --reload --port 8000

# 8. Verify setup
curl http://localhost:8000/health
```

### Verify Installation

```bash
# Check all services are running
docker-compose ps

# Run tests to ensure everything works
poetry run pytest tests/unit/ -v

# Check code quality
poetry run ruff check src/
poetry run black --check src/
poetry run mypy src/
```

## Development Environment Setup

### Environment Configuration

#### .env.development (Complete Example)

```bash
# ======================
# Core Configuration
# ======================
ENVIRONMENT=development
DEBUG=true
LOG_LEVEL=DEBUG
API_HOST=0.0.0.0
API_PORT=8000

# ======================
# Database Configuration
# ======================
# PostgreSQL (Primary Database)
DATABASE_URL=postgresql://postgres:password@localhost:5432/retail_dev
DATABASE_POOL_SIZE=10
DATABASE_POOL_MAX_OVERFLOW=20
DATABASE_POOL_PRE_PING=true
DATABASE_ECHO=false  # Set to true for SQL query logging

# Database Connection Testing
DATABASE_CONNECT_TIMEOUT=10
DATABASE_QUERY_TIMEOUT=30

# ======================
# Cache Configuration
# ======================
# Redis
REDIS_URL=redis://localhost:6379/0
REDIS_MAX_CONNECTIONS=20
REDIS_SOCKET_KEEPALIVE=true
REDIS_HEALTH_CHECK_INTERVAL=30

# ======================
# Security Configuration
# ======================
# JWT Settings
JWT_SECRET_KEY=dev-secret-key-change-in-production-b8f2e9d1c4a7
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_DAYS=7

# CORS Settings
CORS_ALLOWED_ORIGINS=["http://localhost:3000", "http://localhost:8080"]
CORS_ALLOW_CREDENTIALS=true

# Rate Limiting
RATE_LIMITING_ENABLED=true
DEFAULT_RATE_LIMIT=100  # requests per minute
BURST_RATE_LIMIT=150

# ======================
# Search Services
# ======================
# Elasticsearch
ELASTICSEARCH_URL=http://localhost:9200
ELASTICSEARCH_INDEX_PREFIX=pwc_retail_dev
ELASTICSEARCH_REQUEST_TIMEOUT=30

# Typesense (Vector Search)
TYPESENSE_HOST=localhost
TYPESENSE_PORT=8108
TYPESENSE_PROTOCOL=http
TYPESENSE_API_KEY=dev-typesense-key-123456
TYPESENSE_CONNECTION_TIMEOUT=10

# ======================
# External APIs
# ======================
# Country Data API
COUNTRIES_API_URL=https://restcountries.com/v3.1
COUNTRIES_API_TIMEOUT=10

# Currency Exchange API  
EXCHANGE_RATES_API_URL=https://api.exchangerate-api.com/v4
EXCHANGE_RATES_API_KEY=your-api-key-here

# Product Enrichment API
PRODUCT_API_URL=https://api.products.com/v1
PRODUCT_API_KEY=your-product-api-key

# ======================
# Data Processing
# ======================
# Spark Configuration
SPARK_MASTER_URL=local[*]
SPARK_APP_NAME=PwC_Retail_Dev
SPARK_SQL_WAREHOUSE_DIR=/tmp/spark-warehouse
SPARK_SERIALIZER=org.apache.spark.serializer.KryoSerializer

# DBT Configuration
DBT_PROFILES_DIR=./dbt/profiles
DBT_TARGET=dev
DBT_THREADS=4

# ======================
# File Storage
# ======================
# Local Development Storage
DATA_LAKE_PATH=./data
UPLOAD_PATH=./data/uploads
TEMP_PATH=./data/temp
LOG_PATH=./logs

# AWS S3 (for testing cloud features locally)
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_ENDPOINT_URL=http://localhost:9000  # MinIO for local S3 testing
AWS_S3_BUCKET=pwc-retail-dev-data
AWS_REGION=us-east-1

# ======================
# Monitoring & Observability
# ======================
# Prometheus
PROMETHEUS_ENABLED=true
PROMETHEUS_PORT=8000  # Metrics endpoint on same port as API

# Distributed Tracing
JAEGER_ENABLED=false  # Disable in dev unless testing tracing
JAEGER_ENDPOINT=http://localhost:14268/api/traces

# Logging
LOG_FORMAT=detailed  # Options: simple, detailed, json
LOG_FILE_ENABLED=true
LOG_FILE_PATH=./logs/app.log
LOG_ROTATION_SIZE=10MB
LOG_RETENTION_DAYS=7

# ======================
# Feature Flags
# ======================
# Core Features
ENABLE_API_V2=true
ENABLE_GRAPHQL=true
ENABLE_VECTOR_SEARCH=true
ENABLE_ADVANCED_ANALYTICS=true

# Development Features
ENABLE_API_DOCS=true
ENABLE_DEBUG_TOOLBAR=true
ENABLE_PROFILING=false
ENABLE_MOCK_EXTERNAL_APIS=true

# Experimental Features
ENABLE_ML_PREDICTIONS=false
ENABLE_REAL_TIME_ANALYTICS=false
ENABLE_STREAMING_ETL=false

# ======================
# Development Tools
# ======================
# Hot Reloading
RELOAD_ON_CHANGE=true
RELOAD_DIRS=["src", "tests"]

# Database Development
ENABLE_SQL_ECHO=false  # Set to true to see all SQL queries
ENABLE_DB_FIXTURES=true  # Enable test data fixtures

# Testing
PYTEST_ASYNCIO_MODE=auto
TEST_DATABASE_URL=postgresql://postgres:password@localhost:5432/retail_test

# ======================
# Performance Tuning (Dev)
# ======================
# API Performance
GUNICORN_WORKERS=1  # Single worker for easier debugging
GUNICORN_THREADS=1
GUNICORN_TIMEOUT=300  # Longer timeout for debugging
PRELOAD_APP=false    # Disable preloading for easier debugging

# Cache Performance
CACHE_DEFAULT_TTL=300  # 5 minutes for development
CACHE_ENABLED=true
CACHE_PREFIX=pwc_retail_dev

# Database Performance
DB_STATEMENT_TIMEOUT=30000  # 30 seconds
DB_LOCK_TIMEOUT=10000       # 10 seconds
```

### Docker Development Stack

#### docker-compose.dev.yml (Enhanced)

```yaml
version: '3.8'

services:
  # ======================
  # Core Services
  # ======================
  
  # PostgreSQL Database
  postgres:
    image: postgres:15-alpine
    container_name: pwc-retail-postgres-dev
    environment:
      POSTGRES_DB: retail_dev
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
      POSTGRES_INITDB_ARGS: "--auth-host=scram-sha-256"
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./docker/init-db-dev.sql:/docker-entrypoint-initdb.d/01-init.sql
      - ./docker/init-test-db.sql:/docker-entrypoint-initdb.d/02-test-db.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - dev-network

  # Redis Cache
  redis:
    image: redis:7-alpine
    container_name: pwc-retail-redis-dev
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
      - ./docker/redis/redis-dev.conf:/usr/local/etc/redis/redis.conf
    command: redis-server /usr/local/etc/redis/redis.conf
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 3
    networks:
      - dev-network

  # ======================
  # Search Services
  # ======================
  
  # Elasticsearch
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    container_name: pwc-retail-elasticsearch-dev
    environment:
      - node.name=es-node-dev
      - cluster.name=pwc-retail-dev-cluster
      - discovery.type=single-node
      - bootstrap.memory_lock=true
      - xpack.security.enabled=false
      - xpack.security.enrollment.enabled=false
      - "ES_JAVA_OPTS=-Xms1g -Xmx1g"
    ulimits:
      memlock:
        soft: -1
        hard: -1
    ports:
      - "9200:9200"
      - "9300:9300"
    volumes:
      - elasticsearch_data:/usr/share/elasticsearch/data
      - ./docker/elasticsearch/elasticsearch-dev.yml:/usr/share/elasticsearch/config/elasticsearch.yml
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:9200/_cluster/health || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5
    networks:
      - dev-network

  # Kibana (for Elasticsearch debugging)
  kibana:
    image: docker.elastic.co/kibana/kibana:8.11.0
    container_name: pwc-retail-kibana-dev
    environment:
      - ELASTICSEARCH_HOSTS=http://elasticsearch:9200
      - xpack.security.enabled=false
    ports:
      - "5601:5601"
    depends_on:
      elasticsearch:
        condition: service_healthy
    networks:
      - dev-network

  # Typesense (Vector Search)
  typesense:
    image: typesense/typesense:0.25.1
    container_name: pwc-retail-typesense-dev
    environment:
      TYPESENSE_DATA_DIR: /data
      TYPESENSE_API_KEY: dev-typesense-key-123456
      TYPESENSE_ENABLE_CORS: true
    ports:
      - "8108:8108"
    volumes:
      - typesense_data:/data
    networks:
      - dev-network

  # ======================
  # Development Tools
  # ======================
  
  # MinIO (Local S3 Compatible Storage)
  minio:
    image: minio/minio:latest
    container_name: pwc-retail-minio-dev
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio_data:/data
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 20s
      retries: 3
    networks:
      - dev-network

  # pgAdmin (Database Management UI)
  pgadmin:
    image: dpage/pgadmin4:latest
    container_name: pwc-retail-pgadmin-dev
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@pwc-retail.local
      PGADMIN_DEFAULT_PASSWORD: admin123
      PGADMIN_CONFIG_SERVER_MODE: 'False'
    ports:
      - "5050:80"
    volumes:
      - pgadmin_data:/var/lib/pgadmin
      - ./docker/pgadmin/servers.json:/pgadmin4/servers.json
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - dev-network

  # RedisInsight (Redis Management UI)
  redis-insight:
    image: redislabs/redisinsight:latest
    container_name: pwc-retail-redis-insight-dev
    ports:
      - "8001:8001"
    volumes:
      - redis_insight_data:/db
    networks:
      - dev-network

  # ======================
  # Monitoring Stack (Optional)
  # ======================
  
  # Prometheus (Metrics Collection)
  prometheus:
    image: prom/prometheus:latest
    container_name: pwc-retail-prometheus-dev
    ports:
      - "9090:9090"
    volumes:
      - ./docker/prometheus/dev.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
      - '--storage.tsdb.retention.time=7d'
      - '--web.enable-lifecycle'
    profiles: ["monitoring"]
    networks:
      - dev-network

  # Grafana (Metrics Visualization)
  grafana:
    image: grafana/grafana:latest
    container_name: pwc-retail-grafana-dev
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin123
      - GF_USERS_ALLOW_SIGN_UP=false
      - GF_INSTALL_PLUGINS=redis-datasource,grafana-piechart-panel
    ports:
      - "3001:3000"
    volumes:
      - grafana_data:/var/lib/grafana
      - ./docker/grafana/provisioning:/etc/grafana/provisioning
    profiles: ["monitoring"]
    depends_on:
      - prometheus
    networks:
      - dev-network

  # Jaeger (Distributed Tracing)
  jaeger:
    image: jaegertracing/all-in-one:latest
    container_name: pwc-retail-jaeger-dev
    environment:
      - COLLECTOR_OTLP_ENABLED=true
    ports:
      - "16686:16686"  # Jaeger UI
      - "14250:14250"  # gRPC
      - "14268:14268"  # HTTP
      - "6831:6831/udp"  # UDP
      - "6832:6832/udp"  # UDP
    profiles: ["tracing"]
    networks:
      - dev-network

# ======================
# Volumes
# ======================
volumes:
  postgres_data:
    driver: local
  redis_data:
    driver: local
  elasticsearch_data:
    driver: local
  typesense_data:
    driver: local
  minio_data:
    driver: local
  pgadmin_data:
    driver: local
  redis_insight_data:
    driver: local
  prometheus_data:
    driver: local
  grafana_data:
    driver: local

# ======================
# Networks
# ======================
networks:
  dev-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.21.0.0/16
```

### Development Scripts

#### scripts/dev-setup.sh

```bash
#!/bin/bash
# Enhanced development environment setup script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Main setup function
main() {
    log_info "🚀 Setting up PwC Retail Data Platform development environment"
    echo "=========================================================="
    
    cd "$PROJECT_ROOT"
    
    # Check prerequisites
    check_prerequisites
    
    # Setup Python environment
    setup_python_environment
    
    # Setup environment configuration
    setup_environment_config
    
    # Setup Docker services
    setup_docker_services
    
    # Initialize database
    initialize_database
    
    # Load sample data
    load_sample_data
    
    # Setup Git hooks
    setup_git_hooks
    
    # Setup IDE configuration
    setup_ide_configuration
    
    # Verify setup
    verify_setup
    
    log_success "🎉 Development environment setup completed successfully!"
    print_next_steps
}

check_prerequisites() {
    log_info "Checking prerequisites..."
    
    local missing_deps=()
    
    # Check required commands
    local required_commands=("git" "docker" "docker-compose" "python3" "poetry" "node" "npm")
    
    for cmd in "${required_commands[@]}"; do
        if ! command_exists "$cmd"; then
            missing_deps+=("$cmd")
        else
            log_success "✓ $cmd is installed"
        fi
    done
    
    # Check Python version
    if command_exists python3; then
        PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        if [[ "$PYTHON_VERSION" < "3.10" ]]; then
            log_error "Python 3.10+ is required. Current version: $PYTHON_VERSION"
            missing_deps+=("python3.10+")
        else
            log_success "✓ Python $PYTHON_VERSION is compatible"
        fi
    fi
    
    # Check Docker daemon
    if ! docker ps >/dev/null 2>&1; then
        log_error "Docker daemon is not running"
        missing_deps+=("docker-daemon")
    else
        log_success "✓ Docker daemon is running"
    fi
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        log_error "Missing dependencies: ${missing_deps[*]}"
        log_error "Please install missing dependencies and run again"
        exit 1
    fi
}

setup_python_environment() {
    log_info "Setting up Python environment..."
    
    # Install/update poetry
    if ! command_exists poetry; then
        log_info "Installing Poetry..."
        curl -sSL https://install.python-poetry.org | python3 -
    fi
    
    # Configure poetry
    poetry config virtualenvs.in-project true
    poetry config virtualenvs.prefer-active-python true
    
    # Install dependencies
    log_info "Installing Python dependencies..."
    poetry install --with dev,test
    
    # Install pre-commit hooks
    log_info "Setting up pre-commit hooks..."
    poetry run pre-commit install
    
    log_success "✓ Python environment setup completed"
}

setup_environment_config() {
    log_info "Setting up environment configuration..."
    
    # Copy environment file if it doesn't exist
    if [ ! -f ".env" ]; then
        if [ -f ".env.example" ]; then
            cp .env.example .env
            log_success "✓ Created .env from .env.example"
        else
            log_warning "⚠ .env.example not found, creating basic .env"
            create_basic_env_file
        fi
    else
        log_info "✓ .env file already exists"
    fi
    
    # Create necessary directories
    local directories=("logs" "data/raw" "data/bronze" "data/silver" "data/gold" "data/uploads" "data/temp")
    
    for dir in "${directories[@]}"; do
        mkdir -p "$dir"
        log_success "✓ Created directory: $dir"
    done
}

create_basic_env_file() {
    cat > .env << EOF
# Basic development configuration
ENVIRONMENT=development
DEBUG=true
LOG_LEVEL=DEBUG

# Database
DATABASE_URL=postgresql://postgres:password@localhost:5432/retail_dev

# Redis
REDIS_URL=redis://localhost:6379/0

# Security
JWT_SECRET_KEY=dev-secret-key-change-in-production

# Search Services
ELASTICSEARCH_URL=http://localhost:9200
TYPESENSE_HOST=localhost
TYPESENSE_PORT=8108
TYPESENSE_API_KEY=dev-typesense-key

# Feature Flags
ENABLE_API_DOCS=true
ENABLE_VECTOR_SEARCH=true
ENABLE_ADVANCED_ANALYTICS=true
EOF
}

setup_docker_services() {
    log_info "Setting up Docker services..."
    
    # Pull images
    log_info "Pulling Docker images..."
    docker-compose -f docker-compose.dev.yml pull
    
    # Start core services
    log_info "Starting core services..."
    docker-compose -f docker-compose.dev.yml up -d postgres redis elasticsearch typesense
    
    # Wait for services to be ready
    log_info "Waiting for services to be ready..."
    
    local max_attempts=30
    local attempt=1
    
    # Wait for PostgreSQL
    while ! docker-compose -f docker-compose.dev.yml exec -T postgres pg_isready -U postgres >/dev/null 2>&1; do
        if [ $attempt -eq $max_attempts ]; then
            log_error "PostgreSQL failed to start within timeout"
            exit 1
        fi
        log_info "Waiting for PostgreSQL... (attempt $attempt/$max_attempts)"
        sleep 2
        ((attempt++))
    done
    log_success "✓ PostgreSQL is ready"
    
    # Wait for Redis
    attempt=1
    while ! docker-compose -f docker-compose.dev.yml exec -T redis redis-cli ping >/dev/null 2>&1; do
        if [ $attempt -eq $max_attempts ]; then
            log_error "Redis failed to start within timeout"
            exit 1
        fi
        log_info "Waiting for Redis... (attempt $attempt/$max_attempts)"
        sleep 2
        ((attempt++))
    done
    log_success "✓ Redis is ready"
    
    # Wait for Elasticsearch
    attempt=1
    while ! curl -s http://localhost:9200/_cluster/health >/dev/null 2>&1; do
        if [ $attempt -eq $max_attempts ]; then
            log_error "Elasticsearch failed to start within timeout"
            exit 1
        fi
        log_info "Waiting for Elasticsearch... (attempt $attempt/$max_attempts)"
        sleep 3
        ((attempt++))
    done
    log_success "✓ Elasticsearch is ready"
    
    log_success "✓ All Docker services are running"
}

initialize_database() {
    log_info "Initializing database..."
    
    # Run database initialization script
    if [ -f "scripts/init_db.py" ]; then
        poetry run python scripts/init_db.py
        log_success "✓ Database initialized"
    else
        log_warning "⚠ Database initialization script not found"
    fi
    
    # Run database migrations (if using Alembic)
    if [ -d "alembic" ]; then
        log_info "Running database migrations..."
        poetry run alembic upgrade head
        log_success "✓ Database migrations completed"
    fi
}

load_sample_data() {
    log_info "Loading sample data..."
    
    if [ -f "scripts/seed_data.py" ]; then
        poetry run python scripts/seed_data.py
        log_success "✓ Sample data loaded"
    else
        log_warning "⚠ Sample data script not found"
    fi
}

setup_git_hooks() {
    log_info "Setting up Git hooks..."
    
    # Install pre-commit hooks
    if [ -f ".pre-commit-config.yaml" ]; then
        poetry run pre-commit install
        poetry run pre-commit install --hook-type commit-msg
        log_success "✓ Pre-commit hooks installed"
    else
        log_warning "⚠ .pre-commit-config.yaml not found"
    fi
}

setup_ide_configuration() {
    log_info "Setting up IDE configuration..."
    
    # VS Code settings
    if [ -d ".vscode" ] || command_exists code; then
        mkdir -p .vscode
        
        # Create VS Code settings
        cat > .vscode/settings.json << 'EOF'
{
    "python.defaultInterpreterPath": "./.venv/bin/python",
    "python.terminal.activateEnvironment": true,
    "python.linting.enabled": true,
    "python.linting.ruffEnabled": true,
    "python.formatting.provider": "black",
    "python.testing.pytestEnabled": true,
    "python.testing.unittestEnabled": false,
    "python.testing.pytestArgs": [
        "tests"
    ],
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
        "source.organizeImports": true
    },
    "files.exclude": {
        "**/__pycache__": true,
        "**/*.pyc": true,
        ".venv": true,
        ".pytest_cache": true,
        "*.egg-info": true
    }
}
EOF
        
        # Create launch configuration
        cat > .vscode/launch.json << 'EOF'
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "FastAPI Dev Server",
            "type": "python",
            "request": "launch",
            "program": "-m",
            "args": ["uvicorn", "src.api.main:app", "--reload", "--port", "8000"],
            "console": "integratedTerminal",
            "envFile": "${workspaceFolder}/.env",
            "cwd": "${workspaceFolder}"
        },
        {
            "name": "Python: Current File",
            "type": "python",
            "request": "launch",
            "program": "${file}",
            "console": "integratedTerminal",
            "envFile": "${workspaceFolder}/.env",
            "cwd": "${workspaceFolder}"
        }
    ]
}
EOF
        
        log_success "✓ VS Code configuration created"
    fi
    
    # PyCharm configuration
    if [ -d ".idea" ]; then
        log_info "✓ PyCharm project detected"
    fi
}

verify_setup() {
    log_info "Verifying setup..."
    
    # Test Python environment
    if poetry run python -c "import src.core.config; print('Core modules importable')" >/dev/null 2>&1; then
        log_success "✓ Python modules are importable"
    else
        log_error "✗ Python module import failed"
        return 1
    fi
    
    # Test database connection
    if poetry run python -c "
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
conn.close()
print('Database connection successful')
" >/dev/null 2>&1; then
        log_success "✓ Database connection successful"
    else
        log_error "✗ Database connection failed"
        return 1
    fi
    
    # Test Redis connection
    if poetry run python -c "
import redis
import os
from dotenv import load_dotenv
load_dotenv()
r = redis.from_url(os.getenv('REDIS_URL'))
r.ping()
print('Redis connection successful')
" >/dev/null 2>&1; then
        log_success "✓ Redis connection successful"
    else
        log_error "✗ Redis connection failed"
        return 1
    fi
    
    # Run basic tests
    log_info "Running basic tests..."
    if poetry run pytest tests/unit/ -v --tb=short; then
        log_success "✓ Unit tests passed"
    else
        log_warning "⚠ Some unit tests failed (this might be normal for a fresh setup)"
    fi
}

print_next_steps() {
    echo ""
    echo "=========================================================="
    log_success "🎉 Setup completed! Here's what you can do now:"
    echo ""
    echo "1. Start the development server:"
    echo "   poetry run uvicorn src.api.main:app --reload"
    echo ""
    echo "2. Run tests:"
    echo "   poetry run pytest"
    echo ""
    echo "3. Access services:"
    echo "   • API Documentation: http://localhost:8000/docs"
    echo "   • Database UI: http://localhost:5050 (admin@pwc-retail.local / admin123)"
    echo "   • Redis UI: http://localhost:8001"
    echo "   • Elasticsearch: http://localhost:9200"
    echo "   • Kibana: http://localhost:5601"
    echo ""
    echo "4. Development commands:"
    echo "   • Format code: poetry run black src/"
    echo "   • Lint code: poetry run ruff check src/"
    echo "   • Type check: poetry run mypy src/"
    echo "   • Run pre-commit: poetry run pre-commit run --all-files"
    echo ""
    echo "5. Docker management:"
    echo "   • View services: docker-compose -f docker-compose.dev.yml ps"
    echo "   • View logs: docker-compose -f docker-compose.dev.yml logs -f"
    echo "   • Stop services: docker-compose -f docker-compose.dev.yml down"
    echo ""
    echo "Happy coding! 🚀"
}

# Run main function
main "$@"
```

## Code Standards & Guidelines

### Python Code Style

#### Style Guide (.ruff.toml)

```toml
# Ruff configuration for code linting and formatting
target-version = "py310"
line-length = 100
indent-width = 4

[lint]
select = [
    # pycodestyle
    "E",
    "W",
    # Pyflakes
    "F",
    # pyupgrade
    "UP",
    # flake8-bugbear
    "B",
    # flake8-simplify
    "SIM",
    # isort
    "I",
    # flake8-comprehensions
    "C4",
    # flake8-bandit
    "S",
    # flake8-docstrings
    "D",
    # flake8-annotations
    "ANN",
    # flake8-async
    "ASYNC",
    # flake8-pytest-style
    "PT",
    # Perflint
    "PERF",
    # Refurb
    "FURB",
]

ignore = [
    # Missing docstring in public module
    "D100",
    # Missing docstring in public class
    "D101", 
    # Missing docstring in public method
    "D102",
    # Missing docstring in public function
    "D103",
    # Missing docstring in public package
    "D104",
    # Missing docstring in magic method
    "D105",
    # Missing docstring in public nested class
    "D106",
    # Missing docstring in __init__
    "D107",
    # Line too long (handled by black)
    "E501",
    # Do not perform function calls in argument defaults
    "B008",
    # Too complex
    "C901",
    # Magic value used in comparison
    "PLR2004",
]

[lint.per-file-ignores]
"tests/*" = [
    # Use of assert detected
    "S101",
    # Possible hardcoded password
    "S106",  
    # Test functions don't need docstrings
    "D103",
]
"scripts/*" = [
    # Scripts can use print
    "T201",
    # Scripts don't need docstrings
    "D103",
]

[lint.isort]
known-first-party = ["src", "tests"]
force-single-line = true
lines-after-imports = 2

[lint.pydocstyle]
convention = "google"

[format]
quote-style = "double"
indent-style = "space"
skip-string-normalization = false
line-ending = "auto"
```

#### Example Code with Best Practices

```python
"""
Example module demonstrating code standards and best practices.

This module shows proper structure, documentation, and patterns
for the PwC Retail Data Platform.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, validator
from sqlalchemy import Column, DateTime, String, Integer
from sqlalchemy.ext.declarative import declarative_base


# ======================
# Enums and Constants
# ======================

class ProcessingStatus(str, Enum):
    """Processing status enumeration with string values."""
    
    PENDING = "pending"
    IN_PROGRESS = "in_progress" 
    COMPLETED = "completed"
    FAILED = "failed"


# Constants should be UPPER_CASE
DEFAULT_TIMEOUT = 30
MAX_RETRY_ATTEMPTS = 3
SUPPORTED_FILE_FORMATS = ["csv", "parquet", "json"]


# ======================
# Data Models (Pydantic)
# ======================

class BaseDataModel(BaseModel):
    """Base model with common configuration."""
    
    class Config:
        """Pydantic configuration."""
        
        # Use enum values instead of names
        use_enum_values = True
        # Allow population by field name or alias
        allow_population_by_field_name = True
        # Validate assignment
        validate_assignment = True


class CustomerModel(BaseDataModel):
    """Customer data model with validation."""
    
    customer_id: str = Field(
        ..., 
        description="Unique customer identifier",
        min_length=5,
        max_length=20
    )
    email: Optional[str] = Field(
        None,
        description="Customer email address",
        regex=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    )
    country: str = Field(
        ...,
        description="Customer country",
        min_length=2
    )
    registration_date: datetime = Field(
        default_factory=datetime.now,
        description="Customer registration timestamp"
    )
    total_orders: int = Field(
        default=0,
        description="Total number of orders",
        ge=0
    )
    total_spent: float = Field(
        default=0.0,
        description="Total amount spent",
        ge=0.0
    )
    
    @validator("country")
    def validate_country(cls, value: str) -> str:
        """Validate and normalize country name."""
        if not value or len(value.strip()) < 2:
            raise ValueError("Country must be at least 2 characters")
        
        # Normalize country name
        normalized = value.strip().title()
        
        # Map common variations
        country_mappings = {
            "Usa": "United States",
            "Us": "United States", 
            "Uk": "United Kingdom",
            "Britain": "United Kingdom",
        }
        
        return country_mappings.get(normalized, normalized)
    
    @validator("total_spent")
    def validate_total_spent(cls, value: float, values: Dict[str, Any]) -> float:
        """Validate total spent against number of orders."""
        total_orders = values.get("total_orders", 0)
        
        if total_orders > 0 and value == 0.0:
            raise ValueError("Customer with orders must have positive total spent")
        
        return value


# ======================
# Database Models (SQLAlchemy)
# ======================

Base = declarative_base()


class CustomerEntity(Base):
    """Customer database entity."""
    
    __tablename__ = "customers"
    
    # Primary key
    customer_id = Column(String(20), primary_key=True)
    
    # Required fields
    email = Column(String(255), unique=True, nullable=True)
    country = Column(String(100), nullable=False, index=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, nullable=False)
    
    # Metrics
    total_orders = Column(Integer, default=0, nullable=False)
    total_spent = Column(Integer, default=0, nullable=False)  # Store as cents
    
    def __repr__(self) -> str:
        """String representation of customer."""
        return f"<Customer(id={self.customer_id}, country={self.country})>"


# ======================
# Service Classes
# ======================

class CustomerService:
    """Service class for customer operations."""
    
    def __init__(self, database_session, cache_client) -> None:
        """Initialize customer service."""
        self.db = database_session
        self.cache = cache_client
        self._logger = self._get_logger()
    
    def get_customer(self, customer_id: str) -> Optional[CustomerModel]:
        """
        Get customer by ID with caching.
        
        Args:
            customer_id: The customer identifier
            
        Returns:
            Customer model if found, None otherwise
            
        Raises:
            ValueError: If customer_id is invalid
        """
        if not customer_id or len(customer_id.strip()) < 5:
            raise ValueError("Customer ID must be at least 5 characters")
        
        # Check cache first
        cache_key = f"customer:{customer_id}"
        cached_data = self.cache.get(cache_key)
        
        if cached_data:
            self._logger.debug(f"Customer {customer_id} found in cache")
            return CustomerModel.parse_obj(cached_data)
        
        # Query database
        entity = self.db.query(CustomerEntity).filter(
            CustomerEntity.customer_id == customer_id
        ).first()
        
        if not entity:
            self._logger.info(f"Customer {customer_id} not found")
            return None
        
        # Convert to model
        model = CustomerModel(
            customer_id=entity.customer_id,
            email=entity.email,
            country=entity.country,
            registration_date=entity.created_at,
            total_orders=entity.total_orders,
            total_spent=entity.total_spent / 100.0  # Convert from cents
        )
        
        # Cache result
        self.cache.set(cache_key, model.dict(), timeout=300)
        
        self._logger.debug(f"Customer {customer_id} loaded and cached")
        return model
    
    def update_customer_metrics(self, customer_id: str, order_amount: float) -> None:
        """
        Update customer order metrics.
        
        Args:
            customer_id: The customer identifier
            order_amount: The order amount to add
            
        Raises:
            ValueError: If parameters are invalid
        """
        if order_amount < 0:
            raise ValueError("Order amount must be positive")
        
        # Update database
        entity = self.db.query(CustomerEntity).filter(
            CustomerEntity.customer_id == customer_id
        ).first()
        
        if entity:
            entity.total_orders += 1
            entity.total_spent += int(order_amount * 100)  # Convert to cents
            entity.updated_at = datetime.now()
            
            self.db.commit()
            
            # Invalidate cache
            cache_key = f"customer:{customer_id}"
            self.cache.delete(cache_key)
            
            self._logger.info(
                f"Updated metrics for customer {customer_id}: "
                f"orders={entity.total_orders}, spent=${entity.total_spent/100:.2f}"
            )
    
    @staticmethod
    def _get_logger():
        """Get logger instance."""
        import logging
        return logging.getLogger(__name__)


# ======================
# Utility Functions
# ======================

def generate_customer_id() -> str:
    """Generate a unique customer ID."""
    return f"CUST_{uuid4().hex[:8].upper()}"


def validate_email(email: str) -> bool:
    """
    Validate email address format.
    
    Args:
        email: Email address to validate
        
    Returns:
        True if valid, False otherwise
    """
    import re
    
    if not email:
        return False
    
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))


def calculate_customer_score(
    total_orders: int, 
    total_spent: float, 
    days_since_registration: int
) -> float:
    """
    Calculate customer value score.
    
    Args:
        total_orders: Total number of orders
        total_spent: Total amount spent
        days_since_registration: Days since customer registration
        
    Returns:
        Customer score between 0 and 100
    """
    if total_orders == 0 or days_since_registration <= 0:
        return 0.0
    
    # Base score from spending
    spending_score = min(total_spent / 1000.0 * 30, 30)
    
    # Frequency score
    order_frequency = total_orders / (days_since_registration / 30.0)  # Orders per month
    frequency_score = min(order_frequency * 20, 40)
    
    # Longevity score
    longevity_score = min(days_since_registration / 365.0 * 30, 30)
    
    return spending_score + frequency_score + longevity_score


# ======================
# Example Usage
# ======================

if __name__ == "__main__":
    # Example of how to use the models and services
    
    # Create a customer model
    customer_data = {
        "customer_id": "CUST12345",
        "email": "customer@example.com",
        "country": "United States",
        "total_orders": 5,
        "total_spent": 250.75
    }
    
    customer = CustomerModel(**customer_data)
    print(f"Created customer: {customer}")
    
    # Calculate customer score
    score = calculate_customer_score(
        total_orders=customer.total_orders,
        total_spent=customer.total_spent,
        days_since_registration=30
    )
    print(f"Customer score: {score:.2f}")
```

### Database Standards

#### Migration Standards (Alembic)

```python
"""
Example Alembic migration showing best practices.

Revision ID: 001_add_customer_analytics
Revises: None
Create Date: 2024-08-25 10:30:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# Revision identifiers
revision = '001_add_customer_analytics'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """
    Add customer analytics tables and indexes.
    
    This migration:
    1. Creates customer_analytics table
    2. Adds indexes for performance
    3. Creates materialized view for reporting
    """
    
    # Create customer analytics table
    op.create_table(
        'customer_analytics',
        sa.Column('customer_id', sa.String(20), nullable=False),
        sa.Column('analysis_date', sa.Date(), nullable=False),
        sa.Column('rfm_recency', sa.Integer(), nullable=False),
        sa.Column('rfm_frequency', sa.Integer(), nullable=False),
        sa.Column('rfm_monetary', sa.Integer(), nullable=False),
        sa.Column('rfm_score', sa.String(3), nullable=False),
        sa.Column('customer_segment', sa.String(50), nullable=False),
        sa.Column('lifetime_value', sa.Numeric(12, 2), nullable=False),
        sa.Column('churn_probability', sa.Numeric(5, 4), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        
        # Primary key
        sa.PrimaryKeyConstraint('customer_id', 'analysis_date'),
        
        # Foreign key
        sa.ForeignKeyConstraint(['customer_id'], ['customers.customer_id'], ondelete='CASCADE'),
        
        # Check constraints
        sa.CheckConstraint('rfm_recency BETWEEN 1 AND 5', name='check_rfm_recency'),
        sa.CheckConstraint('rfm_frequency BETWEEN 1 AND 5', name='check_rfm_frequency'),
        sa.CheckConstraint('rfm_monetary BETWEEN 1 AND 5', name='check_rfm_monetary'),
        sa.CheckConstraint('lifetime_value >= 0', name='check_lifetime_value'),
        sa.CheckConstraint('churn_probability BETWEEN 0 AND 1', name='check_churn_probability'),
        
        schema='analytics'
    )
    
    # Create indexes for performance
    op.create_index(
        'idx_customer_analytics_segment',
        'customer_analytics',
        ['customer_segment', 'analysis_date'],
        schema='analytics'
    )
    
    op.create_index(
        'idx_customer_analytics_rfm',
        'customer_analytics', 
        ['rfm_score', 'analysis_date'],
        schema='analytics'
    )
    
    op.create_index(
        'idx_customer_analytics_ltv',
        'customer_analytics',
        ['lifetime_value', 'analysis_date'],
        schema='analytics',
        postgresql_where=sa.text('lifetime_value > 100')  # Partial index
    )
    
    # Create materialized view for reporting
    op.execute("""
        CREATE MATERIALIZED VIEW analytics.customer_segments_summary AS
        SELECT 
            analysis_date,
            customer_segment,
            COUNT(*) as customer_count,
            AVG(lifetime_value) as avg_lifetime_value,
            SUM(lifetime_value) as total_lifetime_value,
            AVG(churn_probability) as avg_churn_probability,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lifetime_value) as median_ltv,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lifetime_value) as p95_ltv
        FROM analytics.customer_analytics
        GROUP BY analysis_date, customer_segment
        WITH DATA;
    """)
    
    # Create index on materialized view
    op.execute("""
        CREATE UNIQUE INDEX idx_customer_segments_summary_pk
        ON analytics.customer_segments_summary (analysis_date, customer_segment);
    """)
    
    # Create function to refresh materialized view
    op.execute("""
        CREATE OR REPLACE FUNCTION analytics.refresh_customer_segments_summary()
        RETURNS void AS $$
        BEGIN
            REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.customer_segments_summary;
        END;
        $$ LANGUAGE plpgsql;
    """)


def downgrade() -> None:
    """
    Remove customer analytics tables and objects.
    
    This rollback:
    1. Drops materialized view and function
    2. Drops indexes
    3. Drops table
    """
    
    # Drop function
    op.execute("DROP FUNCTION IF EXISTS analytics.refresh_customer_segments_summary()")
    
    # Drop materialized view
    op.execute("DROP MATERIALIZED VIEW IF EXISTS analytics.customer_segments_summary")
    
    # Drop indexes (will be automatically dropped with table, but explicit is better)
    op.drop_index('idx_customer_analytics_ltv', 'customer_analytics', schema='analytics')
    op.drop_index('idx_customer_analytics_rfm', 'customer_analytics', schema='analytics')
    op.drop_index('idx_customer_analytics_segment', 'customer_analytics', schema='analytics')
    
    # Drop table
    op.drop_table('customer_analytics', schema='analytics')
```

## Testing Framework

### Test Configuration (pytest.ini)

```ini
[tool:pytest]
minversion = 6.0
addopts = 
    -ra
    -q
    --strict-markers
    --strict-config
    --cov=src
    --cov-report=html
    --cov-report=term-missing
    --cov-report=xml
    --cov-branch
    --tb=short
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
markers =
    unit: Unit tests
    integration: Integration tests
    performance: Performance tests
    slow: Slow running tests
    external: Tests that require external services
    database: Tests that require database
    api: API endpoint tests
filterwarnings =
    ignore::DeprecationWarning
    ignore::PendingDeprecationWarning
asyncio_mode = auto
```

### Test Structure and Examples

#### Unit Tests

```python
"""
Unit tests for customer service.

These tests focus on isolated functionality without external dependencies.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
from uuid import uuid4

from src.domain.services.customer_service import CustomerService
from src.domain.entities.customer import Customer
from src.core.exceptions import CustomerNotFoundError, ValidationError


class TestCustomerService:
    """Test suite for CustomerService."""
    
    @pytest.fixture
    def mock_db_session(self):
        """Mock database session."""
        return Mock()
    
    @pytest.fixture
    def mock_cache_client(self):
        """Mock cache client."""
        cache = Mock()
        cache.get.return_value = None  # Default to cache miss
        return cache
    
    @pytest.fixture
    def customer_service(self, mock_db_session, mock_cache_client):
        """Create CustomerService instance with mocked dependencies."""
        return CustomerService(
            database_session=mock_db_session,
            cache_client=mock_cache_client
        )
    
    @pytest.fixture
    def sample_customer_data(self):
        """Sample customer data for testing."""
        return {
            "customer_id": "CUST12345",
            "email": "test@example.com",
            "country": "United States",
            "registration_date": datetime.now(),
            "total_orders": 5,
            "total_spent": 250.75
        }
    
    def test_get_customer_success(self, customer_service, mock_db_session, sample_customer_data):
        """Test successful customer retrieval."""
        # Arrange
        customer_id = sample_customer_data["customer_id"]
        mock_entity = Mock()
        mock_entity.customer_id = customer_id
        mock_entity.email = sample_customer_data["email"]
        mock_entity.country = sample_customer_data["country"]
        mock_entity.created_at = sample_customer_data["registration_date"]
        mock_entity.total_orders = sample_customer_data["total_orders"]
        mock_entity.total_spent = int(sample_customer_data["total_spent"] * 100)
        
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_entity
        
        # Act
        result = customer_service.get_customer(customer_id)
        
        # Assert
        assert result is not None
        assert result.customer_id == customer_id
        assert result.email == sample_customer_data["email"]
        assert result.total_spent == sample_customer_data["total_spent"]
        mock_db_session.query.assert_called_once()
    
    def test_get_customer_not_found(self, customer_service, mock_db_session):
        """Test customer not found scenario."""
        # Arrange
        customer_id = "NONEXISTENT"
        mock_db_session.query.return_value.filter.return_value.first.return_value = None
        
        # Act
        result = customer_service.get_customer(customer_id)
        
        # Assert
        assert result is None
    
    def test_get_customer_invalid_id(self, customer_service):
        """Test customer retrieval with invalid ID."""
        # Arrange
        invalid_ids = ["", "   ", "123", None]
        
        for invalid_id in invalid_ids:
            # Act & Assert
            with pytest.raises(ValueError, match="Customer ID must be at least 5 characters"):
                customer_service.get_customer(invalid_id)
    
    def test_get_customer_cache_hit(self, customer_service, mock_cache_client, sample_customer_data):
        """Test customer retrieval from cache."""
        # Arrange
        customer_id = sample_customer_data["customer_id"]
        cached_data = sample_customer_data.copy()
        mock_cache_client.get.return_value = cached_data
        
        # Act
        result = customer_service.get_customer(customer_id)
        
        # Assert
        assert result is not None
        assert result.customer_id == customer_id
        mock_cache_client.get.assert_called_once_with(f"customer:{customer_id}")
    
    @pytest.mark.parametrize("order_amount,expected_orders,expected_spent", [
        (100.0, 1, 10000),  # $100.00 -> 10000 cents
        (25.50, 1, 2550),   # $25.50 -> 2550 cents
        (0.01, 1, 1),       # $0.01 -> 1 cent
    ])
    def test_update_customer_metrics_success(
        self, 
        customer_service, 
        mock_db_session, 
        mock_cache_client,
        order_amount, 
        expected_orders, 
        expected_spent
    ):
        """Test successful customer metrics update."""
        # Arrange
        customer_id = "CUST12345"
        mock_entity = Mock()
        mock_entity.customer_id = customer_id
        mock_entity.total_orders = 0
        mock_entity.total_spent = 0
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_entity
        
        # Act
        customer_service.update_customer_metrics(customer_id, order_amount)
        
        # Assert
        assert mock_entity.total_orders == expected_orders
        assert mock_entity.total_spent == expected_spent
        mock_db_session.commit.assert_called_once()
        mock_cache_client.delete.assert_called_once_with(f"customer:{customer_id}")
    
    def test_update_customer_metrics_negative_amount(self, customer_service):
        """Test metrics update with negative amount."""
        # Act & Assert
        with pytest.raises(ValueError, match="Order amount must be positive"):
            customer_service.update_customer_metrics("CUST12345", -10.0)
    
    @patch('src.domain.services.customer_service.datetime')
    def test_update_customer_metrics_updates_timestamp(
        self, 
        mock_datetime, 
        customer_service, 
        mock_db_session
    ):
        """Test that customer metrics update includes timestamp update."""
        # Arrange
        fixed_time = datetime(2024, 8, 25, 12, 0, 0)
        mock_datetime.now.return_value = fixed_time
        
        customer_id = "CUST12345"
        mock_entity = Mock()
        mock_entity.customer_id = customer_id
        mock_entity.total_orders = 0
        mock_entity.total_spent = 0
        mock_db_session.query.return_value.filter.return_value.first.return_value = mock_entity
        
        # Act
        customer_service.update_customer_metrics(customer_id, 100.0)
        
        # Assert
        assert mock_entity.updated_at == fixed_time


class TestCustomerValidation:
    """Test suite for customer validation functions."""
    
    @pytest.mark.parametrize("email,expected", [
        ("valid@example.com", True),
        ("user.name+tag@example.co.uk", True),
        ("test123@subdomain.example.org", True),
        ("", False),
        ("invalid.email", False),
        ("@example.com", False),
        ("user@", False),
        ("user space@example.com", False),
        (None, False),
    ])
    def test_validate_email(self, email, expected):
        """Test email validation with various inputs."""
        from src.domain.services.customer_service import validate_email
        
        # Act
        result = validate_email(email)
        
        # Assert
        assert result == expected
    
    @pytest.mark.parametrize("orders,spent,days,expected_min,expected_max", [
        (10, 1000.0, 365, 50.0, 100.0),    # Good customer
        (1, 50.0, 30, 5.0, 25.0),          # New customer
        (0, 0.0, 100, 0.0, 0.0),           # No orders
        (100, 10000.0, 1095, 90.0, 100.0), # Excellent long-term customer
    ])
    def test_calculate_customer_score(self, orders, spent, days, expected_min, expected_max):
        """Test customer score calculation."""
        from src.domain.services.customer_service import calculate_customer_score
        
        # Act
        score = calculate_customer_score(orders, spent, days)
        
        # Assert
        assert expected_min <= score <= expected_max
        assert 0.0 <= score <= 100.0


@pytest.mark.integration
class TestCustomerServiceIntegration:
    """Integration tests that require database and cache."""
    
    @pytest.fixture
    def db_session(self):
        """Real database session for integration tests."""
        # This would create a real database session
        # using your test database configuration
        pass
    
    @pytest.fixture
    def redis_client(self):
        """Real Redis client for integration tests."""
        # This would create a real Redis client
        # using your test Redis configuration
        pass
    
    @pytest.mark.database
    def test_customer_crud_operations(self, db_session, redis_client):
        """Test complete CRUD operations on customer."""
        # This would test actual database operations
        pass
```

#### Integration Tests

```python
"""
Integration tests for the API endpoints.

These tests verify the complete request/response cycle.
"""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime
from unittest.mock import patch

from src.api.main import app
from tests.fixtures.test_data import create_sample_customers


@pytest.fixture
def client():
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def auth_headers():
    """Create authentication headers for testing."""
    # This would create valid JWT tokens for testing
    return {"Authorization": "Bearer test-token-123"}


@pytest.mark.integration
class TestCustomerAPI:
    """Integration tests for customer API endpoints."""
    
    def test_get_customer_success(self, client, auth_headers):
        """Test successful customer retrieval via API."""
        # Arrange
        customer_id = "CUST12345"
        
        # Act
        response = client.get(
            f"/api/v1/customers/{customer_id}",
            headers=auth_headers
        )
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert data["customer_id"] == customer_id
        assert "email" in data
        assert "total_orders" in data
    
    def test_get_customer_not_found(self, client, auth_headers):
        """Test customer not found response."""
        # Act
        response = client.get(
            "/api/v1/customers/NONEXISTENT",
            headers=auth_headers
        )
        
        # Assert
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()
    
    def test_get_customer_unauthorized(self, client):
        """Test unauthorized access."""
        # Act
        response = client.get("/api/v1/customers/CUST12345")
        
        # Assert
        assert response.status_code == 401
    
    @pytest.mark.parametrize("invalid_id", ["", "123", "INVALID"])
    def test_get_customer_invalid_id(self, client, auth_headers, invalid_id):
        """Test customer retrieval with invalid IDs."""
        # Act
        response = client.get(
            f"/api/v1/customers/{invalid_id}",
            headers=auth_headers
        )
        
        # Assert
        assert response.status_code in [400, 422]
    
    def test_create_customer_success(self, client, auth_headers):
        """Test successful customer creation."""
        # Arrange
        customer_data = {
            "customer_id": "CUST67890",
            "email": "newcustomer@example.com",
            "country": "Canada"
        }
        
        # Act
        response = client.post(
            "/api/v1/customers/",
            json=customer_data,
            headers=auth_headers
        )
        
        # Assert
        assert response.status_code == 201
        data = response.json()
        assert data["customer_id"] == customer_data["customer_id"]
        assert data["email"] == customer_data["email"]
    
    def test_update_customer_metrics(self, client, auth_headers):
        """Test customer metrics update."""
        # Arrange
        customer_id = "CUST12345"
        update_data = {"order_amount": 150.75}
        
        # Act
        response = client.patch(
            f"/api/v1/customers/{customer_id}/metrics",
            json=update_data,
            headers=auth_headers
        )
        
        # Assert
        assert response.status_code == 200
        data = response.json()
        assert "total_spent" in data
        assert "total_orders" in data


@pytest.mark.performance
class TestAPIPerformance:
    """Performance tests for API endpoints."""
    
    def test_customer_list_performance(self, client, auth_headers):
        """Test customer list endpoint performance."""
        import time
        
        # Act
        start_time = time.time()
        response = client.get(
            "/api/v1/customers/?limit=1000",
            headers=auth_headers
        )
        end_time = time.time()
        
        # Assert
        assert response.status_code == 200
        assert (end_time - start_time) < 2.0  # Should complete within 2 seconds
    
    @pytest.mark.slow
    def test_concurrent_requests(self, client, auth_headers):
        """Test API under concurrent load."""
        import concurrent.futures
        import time
        
        def make_request():
            return client.get("/api/v1/customers/CUST12345", headers=auth_headers)
        
        # Act
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(make_request) for _ in range(100)]
            responses = [future.result() for future in futures]
        end_time = time.time()
        
        # Assert
        assert all(r.status_code == 200 for r in responses)
        assert (end_time - start_time) < 10.0  # Should complete within 10 seconds
```

## Debugging & Profiling

### Debugging Configuration

#### VS Code Debug Configuration

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "FastAPI Development Server",
            "type": "python",
            "request": "launch",
            "module": "uvicorn",
            "args": [
                "src.api.main:app",
                "--reload",
                "--host",
                "0.0.0.0",
                "--port",
                "8000",
                "--log-level",
                "debug"
            ],
            "envFile": "${workspaceFolder}/.env",
            "console": "integratedTerminal",
            "cwd": "${workspaceFolder}",
            "python": "${workspaceFolder}/.venv/bin/python"
        },
        {
            "name": "Debug ETL Pipeline",
            "type": "python",
            "request": "launch",
            "program": "${workspaceFolder}/scripts/run_etl.py",
            "args": ["--pipeline", "bronze_ingestion", "--debug"],
            "envFile": "${workspaceFolder}/.env",
            "console": "integratedTerminal",
            "cwd": "${workspaceFolder}"
        },
        {
            "name": "Debug Tests",
            "type": "python",
            "request": "launch",
            "module": "pytest",
            "args": [
                "${file}",
                "-v",
                "--tb=short"
            ],
            "envFile": "${workspaceFolder}/.env",
            "console": "integratedTerminal",
            "cwd": "${workspaceFolder}"
        },
        {
            "name": "Debug Current File",
            "type": "python",
            "request": "launch",
            "program": "${file}",
            "envFile": "${workspaceFolder}/.env",
            "console": "integratedTerminal",
            "cwd": "${workspaceFolder}"
        }
    ]
}
```

### Profiling Tools

#### Memory Profiling

```python
"""
Memory profiling utilities for performance analysis.
"""

import functools
import tracemalloc
from memory_profiler import profile
from typing import Callable, Any


def memory_profile(func: Callable) -> Callable:
    """
    Decorator to profile memory usage of a function.
    
    Usage:
        @memory_profile
        def my_function():
            # Function implementation
            pass
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        # Start tracing
        tracemalloc.start()
        
        try:
            # Execute function
            result = func(*args, **kwargs)
            
            # Get memory statistics
            current, peak = tracemalloc.get_traced_memory()
            
            print(f"\n=== Memory Profile for {func.__name__} ===")
            print(f"Current memory usage: {current / 1024 / 1024:.2f} MB")
            print(f"Peak memory usage: {peak / 1024 / 1024:.2f} MB")
            
            return result
            
        finally:
            tracemalloc.stop()
    
    return wrapper


@profile  # Line-by-line memory profiler
def analyze_data_processing():
    """Example function with memory profiling."""
    import pandas as pd
    
    # Load data
    data = pd.read_csv("data/large_dataset.csv")
    
    # Process data
    processed = data.groupby("category").agg({
        "amount": ["sum", "mean", "count"],
        "quantity": "sum"
    })
    
    # Further processing
    result = processed.reset_index()
    
    return result


# Usage example
if __name__ == "__main__":
    # Run with: python -m memory_profiler script.py
    result = analyze_data_processing()
```

#### Performance Profiling

```python
"""
Performance profiling utilities.
"""

import cProfile
import pstats
import time
import functools
from typing import Callable, Any


def time_profile(func: Callable) -> Callable:
    """Simple timing decorator."""
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.perf_counter()
        
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            end_time = time.perf_counter()
            duration = end_time - start_time
            print(f"{func.__name__} took {duration:.4f} seconds")
    
    return wrapper


def detailed_profile(func: Callable) -> Callable:
    """Detailed profiling with cProfile."""
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        profiler = cProfile.Profile()
        profiler.enable()
        
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            profiler.disable()
            
            # Print profile stats
            stats = pstats.Stats(profiler)
            stats.sort_stats('cumulative')
            stats.print_stats(20)  # Top 20 functions
    
    return wrapper


# Example usage
@time_profile
@detailed_profile
def complex_data_operation():
    """Example of a complex data operation."""
    import pandas as pd
    import numpy as np
    
    # Generate sample data
    data = pd.DataFrame({
        'id': range(100000),
        'value': np.random.random(100000),
        'category': np.random.choice(['A', 'B', 'C'], 100000)
    })
    
    # Complex operations
    result = (data
              .groupby('category')
              .apply(lambda x: x.nlargest(1000, 'value'))
              .reset_index(drop=True))
    
    return result
```

## Contributing Guidelines

### Git Workflow

#### Branch Naming Convention

```bash
# Feature branches
feature/user-authentication
feature/customer-analytics
feature/api-v2-enhancements

# Bug fix branches
bugfix/customer-validation-error
bugfix/etl-pipeline-timeout

# Hotfix branches (for production issues)
hotfix/critical-security-patch
hotfix/database-connection-leak

# Release branches
release/v1.2.0
release/v2.0.0-beta

# Development branches
develop
main (production)
```

#### Commit Message Format

```bash
# Format: <type>(<scope>): <subject>
#
# Types: feat, fix, docs, style, refactor, perf, test, chore
# Scope: component or area of change
# Subject: brief description (imperative mood, lowercase, no period)

# Examples:
git commit -m "feat(api): add customer segmentation endpoint"
git commit -m "fix(etl): handle null values in customer data processing"
git commit -m "docs(readme): update installation instructions"
git commit -m "refactor(services): extract common validation logic"
git commit -m "perf(database): optimize customer query with indexing"
git commit -m "test(integration): add customer API integration tests"
```

#### Pull Request Template

```markdown
## Pull Request Description

**Summary**
Brief description of what this PR does.

**Type of Change**
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Performance improvement
- [ ] Documentation update
- [ ] Refactoring (no functional changes)

**Changes Made**
- List the main changes
- Include any new dependencies
- Mention configuration changes

**Testing Done**
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing completed
- [ ] Performance impact assessed

**Deployment Considerations**
- [ ] Database migrations required
- [ ] Configuration updates needed
- [ ] Infrastructure changes required
- [ ] Backward compatibility maintained

**Screenshots (if applicable)**
<!-- Add screenshots for UI changes -->

**Related Issues**
Fixes #123
Closes #456

**Checklist**
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Tests added/updated
- [ ] No merge conflicts
```

### Code Review Guidelines

#### Review Checklist

**Functionality**
- [ ] Code solves the intended problem
- [ ] Edge cases are handled appropriately
- [ ] Error handling is comprehensive
- [ ] Input validation is proper

**Code Quality**
- [ ] Code is readable and well-structured
- [ ] Functions are focused and single-purpose
- [ ] Variable and function names are descriptive
- [ ] No code duplication

**Performance**
- [ ] No obvious performance issues
- [ ] Database queries are optimized
- [ ] Memory usage is reasonable
- [ ] Caching is used appropriately

**Security**
- [ ] No sensitive data exposure
- [ ] Input sanitization is proper
- [ ] Authentication/authorization is correct
- [ ] No SQL injection vulnerabilities

**Testing**
- [ ] Tests cover new functionality
- [ ] Tests are readable and maintainable
- [ ] Edge cases are tested
- [ ] Integration tests are included where appropriate

**Documentation**
- [ ] Code is properly documented
- [ ] API changes are documented
- [ ] README is updated if necessary
- [ ] Breaking changes are highlighted

## IDE Configuration

### VS Code Extensions

Create `.vscode/extensions.json`:

```json
{
    "recommendations": [
        "ms-python.python",
        "ms-python.black-formatter",
        "charliermarsh.ruff",
        "ms-python.mypy-type-checker",
        "ms-toolsai.jupyter",
        "ms-vscode.vscode-json",
        "redhat.vscode-yaml",
        "ms-vscode.docker",
        "ms-kubernetes-tools.vscode-kubernetes-tools",
        "hashicorp.terraform",
        "ms-vscode.test-adapter-converter",
        "littlefoxteam.vscode-python-test-adapter",
        "streetsidesoftware.code-spell-checker",
        "formulahendry.auto-rename-tag",
        "bradlc.vscode-tailwindcss",
        "ms-vscode.vscode-typescript-next"
    ]
}
```

### PyCharm Configuration

For PyCharm users, configure:

1. **Python Interpreter**: Point to `.venv/bin/python`
2. **Code Style**: Import settings from `pyproject.toml`
3. **Run Configurations**: Create run configs for API, tests, and ETL
4. **Database**: Configure connection to local PostgreSQL
5. **Docker**: Configure Docker integration

## Troubleshooting

### Common Issues

#### 1. Poetry Installation Issues

```bash
# Issue: Poetry not found after installation
export PATH="$HOME/.local/bin:$PATH"

# Issue: Virtual environment not activated
poetry shell
# or
source .venv/bin/activate

# Issue: Dependencies not installing
poetry install --no-cache
```

#### 2. Database Connection Issues

```bash
# Check if PostgreSQL is running
docker-compose -f docker-compose.dev.yml ps postgres

# Check connection manually
psql -h localhost -U postgres -d retail_dev -c "SELECT version();"

# Reset database
docker-compose -f docker-compose.dev.yml down postgres
docker volume rm pwc-retail-data-platform_postgres_data
docker-compose -f docker-compose.dev.yml up -d postgres
```

#### 3. Import Issues

```bash
# Issue: Module not found
export PYTHONPATH="${PYTHONPATH}:${PWD}/src"

# Or use poetry
poetry run python -c "import src.core.config; print('OK')"

# Issue: Circular imports
# Restructure imports to avoid circular dependencies
```

#### 4. Test Issues

```bash
# Issue: Tests not discovering
pytest --collect-only

# Issue: Database tests failing
# Make sure test database is set up
poetry run pytest tests/integration/ --tb=short -v
```

#### 5. Docker Issues

```bash
# Issue: Services not starting
docker-compose -f docker-compose.dev.yml logs

# Issue: Port conflicts
docker-compose -f docker-compose.dev.yml down
# Edit ports in docker-compose.dev.yml

# Issue: Permission issues (Linux/Mac)
sudo chown -R $USER:$USER .
```

For more detailed troubleshooting, see:
- [Common Development Issues](./TROUBLESHOOTING.md)
- [Performance Debugging Guide](./PERFORMANCE_DEBUGGING.md)
- [Database Development Guide](./DATABASE_DEVELOPMENT.md)
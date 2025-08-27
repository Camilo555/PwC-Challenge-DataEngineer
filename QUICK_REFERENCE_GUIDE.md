# PwC Challenge DataEngineer - Quick Reference Guide

## 🚀 Essential Commands & Operations

This quick reference guide provides the most commonly used commands, configurations, and troubleshooting steps for developers and operators working with the PwC Challenge DataEngineer platform.

## 📋 Table of Contents

1. [Development Environment](#development-environment)
2. [API Operations](#api-operations)
3. [Data Pipeline Commands](#data-pipeline-commands)
4. [Monitoring & Health Checks](#monitoring--health-checks)
5. [Testing Commands](#testing-commands)
6. [Deployment Operations](#deployment-operations)
7. [Troubleshooting](#troubleshooting)
8. [Security Operations](#security-operations)

---

## 🛠️ Development Environment

### Quick Setup
```bash
# Clone and setup
git clone <repository>
cd pwc-challenge-dataengineer
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -e .

# Start development environment
make setup-dev
make start-local

# Verify installation
make test-quick
```

### Development Stack Commands
```bash
# Start full development stack
docker-compose -f docker-compose.dev.yml up -d

# Start specific services
docker-compose up postgres redis kafka elasticsearch

# Stop and clean
docker-compose down -v
docker system prune -f

# View logs
docker-compose logs -f [service-name]
```

### Environment Variables
```bash
# Required environment variables
export DATABASE_URL="postgresql://user:pass@localhost:5432/pwc_data"
export REDIS_URL="redis://localhost:6379/0"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export ELASTICSEARCH_URL="http://localhost:9200"
export SECRET_KEY="your-secret-key"
export JWT_SECRET="your-jwt-secret"
```

---

## 🔌 API Operations

### API Server Commands
```bash
# Start API server
uvicorn src.api.main:app --reload --port 8000

# Start with debugging
uvicorn src.api.main:app --reload --log-level debug

# Production mode
gunicorn src.api.main:app -w 4 -k uvicorn.workers.UvicornWorker
```

### API Testing
```bash
# Health check
curl http://localhost:8000/api/v1/monitoring/health

# API documentation
open http://localhost:8000/docs

# Authentication test
curl -X POST http://localhost:8000/api/v1/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username":"test","password":"test"}'

# Sales data query
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8000/api/v1/sales/transactions?limit=10
```

### Common API Endpoints
```
Authentication:
├── POST /api/v1/auth/token          # Get JWT token
├── POST /api/v1/auth/refresh        # Refresh token
└── GET  /api/v1/auth/me             # User profile

Sales Data:
├── GET  /api/v1/sales/transactions  # List transactions
├── GET  /api/v1/sales/analytics     # Sales analytics
└── GET  /api/v1/sales/search        # Search transactions

Monitoring:
├── GET  /api/v1/monitoring/health   # System health
├── GET  /api/v1/monitoring/metrics  # Platform metrics
└── GET  /api/v1/monitoring/alerts   # Active alerts
```

---

## 📊 Data Pipeline Commands

### ETL Pipeline Execution
```bash
# Run complete ETL pipeline
python scripts/run_etl.py

# Run specific layers
python scripts/run_bronze.py
python scripts/run_silver.py  
python scripts/run_gold.py

# Run with specific engine
python scripts/run_bronze_spark.py
python scripts/run_bronze_pandas.py
python scripts/run_bronze_polars.py
```

### dbt Operations
```bash
# dbt commands
dbt deps                    # Install dependencies
dbt run                     # Run all models
dbt test                    # Run all tests
dbt docs generate          # Generate documentation
dbt docs serve             # Serve documentation

# Specific model operations
dbt run --models stg_customers
dbt test --models fact_sales
dbt run --select +dim_customers+
```

### Data Quality Checks
```bash
# Run data quality validation
python -m src.domain.validators.data_quality

# Run Great Expectations
great_expectations checkpoint run retail_data_checkpoint

# Database integrity checks
python scripts/test_database_optimization.py
```

---

## 📈 Monitoring & Health Checks

### System Health Commands
```bash
# Overall system health
curl http://localhost:8000/api/v1/monitoring/health

# Component health checks
curl http://localhost:8000/api/v1/monitoring/health/database
curl http://localhost:8000/api/v1/monitoring/health/redis
curl http://localhost:8000/api/v1/monitoring/health/elasticsearch

# Performance metrics
curl http://localhost:8000/api/v1/monitoring/metrics
```

### Service Status Checks
```bash
# Check all services
docker-compose ps

# Check specific service logs
docker-compose logs -f api
docker-compose logs -f postgres
docker-compose logs -f kafka

# Resource usage
docker stats

# Process monitoring
ps aux | grep python
ps aux | grep java  # Spark processes
```

### Database Operations
```bash
# Database connection test
python -c "from src.data_access.db import get_engine; print(get_engine().execute('SELECT 1').scalar())"

# Database migrations
alembic upgrade head
alembic current
alembic history

# Database backup
pg_dump -h localhost -U postgres pwc_data > backup.sql

# Database restore
psql -h localhost -U postgres pwc_data < backup.sql
```

---

## 🧪 Testing Commands

### Test Execution
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test categories
pytest tests/unit/          # Unit tests
pytest tests/integration/   # Integration tests
pytest tests/e2e/          # End-to-end tests
pytest tests/performance/   # Performance tests
pytest tests/security/     # Security tests

# Run specific test file
pytest tests/unit/test_config.py -v

# Run tests with markers
pytest -m "not slow"       # Skip slow tests
pytest -m "security"       # Run only security tests
```

### Test Data Management
```bash
# Generate test data
python scripts/seed_data.py

# Prepare test datasets
python scripts/prepare_data.py --test

# Reset test database
python scripts/init_db.py --test-data
```

### Performance Testing
```bash
# Load testing with Locust
locust -f tests/load/locustfile.py --host=http://localhost:8000

# API performance testing
python tests/performance/test_api_performance.py

# ETL performance benchmarking
python tests/performance/test_etl_performance.py
```

---

## 🚀 Deployment Operations

### Docker Operations
```bash
# Build all images
docker-compose build

# Build specific service
docker-compose build api

# Production deployment
docker-compose -f docker-compose.prod.yml up -d

# Scaling services
docker-compose up --scale api=3 --scale worker=2

# Health check
docker-compose exec api python -c "import requests; print(requests.get('http://localhost:8000/health').json())"
```

### Kubernetes Operations
```bash
# Apply manifests
kubectl apply -f k8s/

# Check deployments
kubectl get deployments
kubectl get pods
kubectl get services

# View logs
kubectl logs -f deployment/api-deployment
kubectl logs -f -l app=data-pipeline

# Port forwarding
kubectl port-forward svc/api-service 8000:8000
kubectl port-forward svc/grafana 3000:3000

# Scale deployment
kubectl scale deployment api-deployment --replicas=5
```

### Environment Management
```bash
# Development environment
export ENV=development
docker-compose -f docker-compose.dev.yml up -d

# Staging environment  
export ENV=staging
docker-compose -f docker-compose.staging.yml up -d

# Production environment
export ENV=production
docker-compose -f docker-compose.prod.yml up -d
```

---

## 🔧 Troubleshooting

### Common Issues & Solutions

#### API Issues
```bash
# Check API logs
docker-compose logs -f api

# Check API health
curl http://localhost:8000/health

# Restart API service
docker-compose restart api

# Database connection issues
python -c "from src.data_access.db import get_engine; get_engine().execute('SELECT 1')"
```

#### Data Pipeline Issues
```bash
# Check pipeline logs
tail -f logs/app.log

# Check data quality issues
python -m src.domain.validators.data_quality --verbose

# Check disk space
df -h
du -sh data/

# Clean temporary files
rm -rf temp/
rm -rf data/bronze/*.tmp
```

#### Performance Issues
```bash
# Check system resources
htop
free -h
iostat 1 5

# Check database performance
psql -c "SELECT query, calls, mean_time FROM pg_stat_statements ORDER BY mean_time DESC LIMIT 10;"

# Check Redis performance
redis-cli info stats
redis-cli slowlog get 10

# Check Elasticsearch health
curl http://localhost:9200/_cluster/health?pretty
```

#### Container Issues
```bash
# Check container status
docker ps -a

# Check container resources
docker stats

# Check container logs
docker logs [container-id] --tail 100

# Restart containers
docker-compose restart [service-name]

# Clean up
docker system prune -f
docker volume prune -f
```

---

## 🔒 Security Operations

### Security Checks
```bash
# Run security tests
pytest tests/security/

# Security vulnerability scan
python scripts/run_security_tests.py

# Check for secrets in code
git secrets --scan

# Audit dependencies
pip-audit
safety check
```

### Authentication & Authorization
```bash
# Generate JWT token
python -c "
from src.core.security.secret_manager import SecretManager
sm = SecretManager()
print(sm.generate_jwt_token({'sub': 'user123'}))
"

# Verify JWT token
python -c "
from src.core.security.secret_manager import SecretManager
sm = SecretManager()
print(sm.verify_jwt_token('your-token'))
"

# Check user permissions
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8000/api/v1/auth/permissions
```

### Secret Management
```bash
# Initialize secrets
python scripts/secret_management.py --init

# Rotate secrets
python scripts/secret_management.py --rotate-keys

# Check secret health
python scripts/secret_management.py --health-check

# Backup secrets (encrypted)
python scripts/secret_management.py --backup
```

---

## 📱 Monitoring Dashboards

### Access URLs (Development)
```
Service Dashboards:
├── API Documentation: http://localhost:8000/docs
├── Grafana Dashboard: http://localhost:3000 (admin/admin)
├── Elasticsearch: http://localhost:9200
├── Kafka UI: http://localhost:8080
├── Redis Insight: http://localhost:8001
└── Jupyter Lab: http://localhost:8888

Health Endpoints:
├── API Health: http://localhost:8000/api/v1/monitoring/health  
├── Database Health: http://localhost:8000/api/v1/monitoring/health/database
├── Cache Health: http://localhost:8000/api/v1/monitoring/health/redis
└── Search Health: http://localhost:8000/api/v1/monitoring/health/elasticsearch
```

### Key Metrics to Monitor
```
Performance Metrics:
├── API Response Time: <250ms (95th percentile)
├── Database Connection Pool: <80% utilization
├── Memory Usage: <80% of available
├── CPU Usage: <70% average
├── Disk Space: >20% free space
└── Error Rate: <0.1%

Business Metrics:
├── Data Freshness: <5 minutes lag
├── Pipeline Success Rate: >99.9%
├── Data Quality Score: >95%
├── User Session Duration: trend monitoring
└── Transaction Volume: trend monitoring
```

---

## 🆘 Emergency Procedures

### Critical Issue Response
```bash
# System down - restart all services
docker-compose down && docker-compose up -d

# Database corruption - restore from backup
./scripts/restore_database.sh [backup-date]

# High load - scale up services
docker-compose up --scale api=5 --scale worker=3

# Security incident - rotate all secrets
python scripts/secret_management.py --emergency-rotate
```

### Contact Information
```
Emergency Contacts:
├── Platform Team: platform-team@pwc.com
├── On-call Engineer: +1-555-ONCALL-1
├── Security Team: security-incident@pwc.com
└── Manager: [manager-email]

Escalation Path:
1. Try troubleshooting steps above
2. Check recent changes in Git
3. Contact platform team
4. Escalate to on-call if critical
5. Notify security team if breach suspected
```

---

## 📋 Useful References

### Documentation Links
- [📖 Main Documentation Hub](docs/README.md)
- [🏗️ System Architecture](docs/architecture/COMPREHENSIVE_SYSTEM_ARCHITECTURE.md)
- [🔌 API Documentation](docs/api/COMPREHENSIVE_API_DOCUMENTATION.md)
- [👨‍💻 Developer Guide](docs/development/DEVELOPER_SETUP_GUIDE.md)
- [📊 Monitoring Guide](docs/monitoring/COMPREHENSIVE_MONITORING_GUIDE.md)

### Configuration Files
```
Key Configuration Files:
├── pyproject.toml          # Python dependencies and project config
├── docker-compose.yml      # Docker services configuration
├── dbt_project.yml        # dbt project configuration  
├── profiles.yml           # dbt profiles configuration
├── pytest.ini             # Test configuration
├── .env.example           # Environment variables template
└── makefile               # Development automation commands
```

---

**Last Updated**: August 27, 2025  
**Version**: 1.0.0  
**Maintained By**: Platform Engineering Team  
**Review Cycle**: Monthly

For questions or improvements to this guide, contact: platform-team@pwc.com
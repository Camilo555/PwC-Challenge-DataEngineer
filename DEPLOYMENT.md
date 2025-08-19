# Deployment Guide

This guide provides comprehensive instructions for deploying the PwC Retail ETL Pipeline to various environments using Docker and CI/CD pipelines.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Environment Configuration](#environment-configuration)
- [Deployment Methods](#deployment-methods)
- [CI/CD Pipeline](#cicd-pipeline)
- [Monitoring and Validation](#monitoring-and-validation)
- [Troubleshooting](#troubleshooting)

## Overview

The deployment infrastructure supports:

- **Containerized deployment** with Docker and Docker Compose
- **Multi-environment support** (development, staging, production)
- **CI/CD automation** with GitHub Actions
- **Comprehensive monitoring** with Prometheus, Grafana, and custom health checks
- **Security best practices** with secrets management and authentication
- **Scalable architecture** with Spark clusters and orchestration tools

## Prerequisites

### System Requirements

- **Docker** 20.10+ with Docker Compose
- **Git** for version control
- **Bash** shell for deployment scripts
- **curl** for health checks
- **Minimum Resources:**
  - 8GB RAM
  - 50GB disk space
  - 4 CPU cores

### Required Software

```bash
# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify installation
docker --version
docker-compose --version
```

## Environment Configuration

### 1. Copy Environment Template

```bash
cp .env.example .env.staging    # For staging
cp .env.example .env.production # For production
```

### 2. Configure Environment Variables

Edit the environment files with your specific values:

#### Critical Settings

```bash
# Database
POSTGRES_PASSWORD=your_secure_password_here
DATABASE_URL=postgresql://postgres:password@postgres:5432/retail_db

# Security
SECRET_KEY=your_32_character_secret_key_here
BASIC_AUTH_PASSWORD=secure_api_password

# External Services
TYPESENSE_API_KEY=your_typesense_api_key
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL

# Monitoring
GRAFANA_PASSWORD=secure_grafana_password
```

#### Generate Secure Keys

```bash
# Generate SECRET_KEY
openssl rand -hex 32

# Generate TYPESENSE_API_KEY
openssl rand -hex 16

# Generate Fernet key for Airflow
openssl rand -base64 32
```

## Deployment Methods

### Method 1: Manual Deployment

#### Staging Deployment

```bash
# Deploy to staging
./scripts/deploy_staging.sh

# Validate deployment
./scripts/validate_deployment.sh
```

#### Production Deployment

```bash
# Set required environment variables
export POSTGRES_PASSWORD="your_production_password"
export SECRET_KEY="your_production_secret"
export BASIC_AUTH_PASSWORD="your_production_api_password"
export TYPESENSE_API_KEY="your_production_typesense_key"

# Deploy to production
./scripts/deploy_production.sh

# Validate deployment
./scripts/validate_deployment.sh
```

### Method 2: Docker Compose Profiles

#### Start Core Services

```bash
docker-compose -f docker-compose.production.yml up -d postgres redis typesense api
```

#### Start with Monitoring

```bash
docker-compose -f docker-compose.production.yml --profile monitoring up -d
```

#### Start with Orchestration

```bash
# Dagster
docker-compose -f docker-compose.production.yml --profile dagster up -d

# Airflow
docker-compose -f docker-compose.production.yml --profile airflow up -d
```

#### Start Full Stack

```bash
docker-compose -f docker-compose.production.yml --profile monitoring --profile dagster up -d
```

### Method 3: Individual Services

```bash
# API only
docker-compose -f docker-compose.production.yml up -d postgres redis api

# ETL processing
docker-compose -f docker-compose.production.yml --profile etl run etl-spark

# Vector search indexing
docker-compose -f docker-compose.production.yml run indexer
```

## CI/CD Pipeline

### GitHub Actions Workflow

The repository includes three main workflows:

1. **Continuous Integration** (`.github/workflows/ci.yml`)
   - Runs tests and linting
   - Validates code quality
   - Security scanning

2. **Docker Build** (`.github/workflows/docker.yml`)
   - Builds and pushes Docker images
   - Multi-architecture support
   - Vulnerability scanning

3. **Deployment** (`.github/workflows/deploy.yml`)
   - Automated staging deployment
   - Manual production deployment
   - Rollback capabilities

### Setting Up GitHub Actions

1. **Configure Secrets** in your GitHub repository:

```bash
# Required secrets
POSTGRES_PASSWORD
SECRET_KEY
BASIC_AUTH_PASSWORD
TYPESENSE_API_KEY
GRAFANA_PASSWORD
SLACK_WEBHOOK_URL

# Optional secrets
STAGING_DATABASE_URL
PRODUCTION_DATABASE_URL
```

2. **Enable GitHub Container Registry:**

```bash
# Login to GitHub Container Registry
echo $GITHUB_TOKEN | docker login ghcr.io -u USERNAME --password-stdin
```

3. **Trigger Deployment:**

```bash
# Push to main branch for staging
git push origin main

# Create release for production
git tag -a v1.0.0 -m "Production release v1.0.0"
git push origin v1.0.0
```

## Monitoring and Validation

### Access Points

After successful deployment, access the services at:

- **API:** http://localhost:8000
- **API Documentation:** http://localhost:8000/docs
- **Health Check:** http://localhost:8000/api/v1/health
- **Dagster:** http://localhost:3000
- **Prometheus:** http://localhost:9090
- **Grafana:** http://localhost:3001
- **Typesense:** http://localhost:8108

### Health Checks

```bash
# Automated validation
./scripts/validate_deployment.sh

# Manual health checks
curl http://localhost:8000/api/v1/health
curl http://localhost:8108/health
curl http://localhost:9090/-/healthy
```

### Log Monitoring

```bash
# View all logs
docker-compose -f docker-compose.production.yml logs -f

# Specific service logs
docker-compose -f docker-compose.production.yml logs -f api
docker-compose -f docker-compose.production.yml logs -f etl-spark

# Follow logs with timestamps
docker-compose -f docker-compose.production.yml logs -f -t
```

### Performance Monitoring

- **Grafana Dashboards:** Pre-configured dashboards for system metrics
- **Prometheus Metrics:** Custom application metrics
- **Health Check Endpoints:** Automated monitoring integration

## Advanced Configuration

### Scaling Services

```bash
# Scale API instances
docker-compose -f docker-compose.production.yml up -d --scale api=3

# Scale Spark workers
docker-compose -f docker-compose.production.yml up -d --scale spark-worker=4
```

### Custom Spark Configuration

```bash
# Override Spark settings
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g
export SPARK_EXECUTOR_CORES=2

docker-compose -f docker-compose.production.yml up -d spark-master spark-worker
```

### SSL/TLS Configuration

1. **Generate SSL Certificates:**

```bash
mkdir -p docker/nginx/ssl
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout docker/nginx/ssl/nginx.key \
  -out docker/nginx/ssl/nginx.crt
```

2. **Configure Nginx:** Update `docker/nginx/default.conf` with SSL settings

### External Database Configuration

For managed database services:

```bash
# Set external database URL
export DATABASE_URL="postgresql://user:pass@external-db.com:5432/retail_db"

# Disable local PostgreSQL
docker-compose -f docker-compose.production.yml up -d --scale postgres=0
```

## Backup and Recovery

### Database Backup

```bash
# Create backup
docker-compose -f docker-compose.production.yml exec postgres pg_dump -U postgres retail_db > backup-$(date +%Y%m%d).sql

# Restore backup
docker-compose -f docker-compose.production.yml exec -T postgres psql -U postgres retail_db < backup-20240101.sql
```

### Application Data Backup

```bash
# Backup data volumes
docker run --rm -v pwc-retail-etl_postgres_data:/source -v $(pwd):/backup alpine tar -czf /backup/postgres-backup.tar.gz -C /source .

# Restore data volumes
docker run --rm -v pwc-retail-etl_postgres_data:/target -v $(pwd):/backup alpine tar -xzf /backup/postgres-backup.tar.gz -C /target
```

## Troubleshooting

### Common Issues

#### Port Conflicts

```bash
# Check port usage
netstat -tlnp | grep :8000

# Use different ports
export API_PORT=8001
docker-compose -f docker-compose.production.yml up -d
```

#### Memory Issues

```bash
# Check memory usage
docker stats

# Adjust memory limits in docker-compose.yml
deploy:
  resources:
    limits:
      memory: 2G
```

#### Permission Issues

```bash
# Fix data directory permissions
sudo chown -R $(id -u):$(id -g) data/ logs/

# Fix Docker socket permissions
sudo usermod -aG docker $USER
```

### Service-Specific Troubleshooting

#### API Service Issues

```bash
# Check API logs
docker-compose logs -f api

# Test API connectivity
curl -v http://localhost:8000/api/v1/health

# Check database connectivity from API
docker-compose exec api python -c "from data_access.db import get_database_url; print('DB OK')"
```

#### Database Issues

```bash
# Check PostgreSQL logs
docker-compose logs -f postgres

# Test database connection
docker-compose exec postgres pg_isready -U postgres

# Reset database
docker-compose down -v
docker-compose up -d postgres
```

#### Spark Issues

```bash
# Check Spark master logs
docker-compose logs -f spark-master

# Access Spark UI
open http://localhost:8080

# Test Spark job
docker-compose run --rm etl-spark python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate(); print('Spark OK')"
```

### Performance Optimization

#### Database Performance

```bash
# Analyze database performance
docker-compose exec postgres psql -U postgres -c "SELECT * FROM pg_stat_activity;"

# Update database statistics
docker-compose exec postgres psql -U postgres -c "ANALYZE;"
```

#### API Performance

```bash
# Monitor API performance
curl -w "@curl-format.txt" -o /dev/null -s http://localhost:8000/api/v1/health

# Scale API instances
docker-compose up -d --scale api=3
```

### Recovery Procedures

#### Complete System Recovery

```bash
# Stop all services
docker-compose -f docker-compose.production.yml down

# Remove all containers and volumes (DESTRUCTIVE)
docker-compose -f docker-compose.production.yml down -v

# Restore from backup
# ... restore database and data volumes ...

# Restart services
docker-compose -f docker-compose.production.yml up -d
```

#### Rollback Deployment

```bash
# Automatic rollback (from deployment script)
./scripts/deploy_production.sh # Will rollback on failure

# Manual rollback
git checkout previous-tag
docker-compose -f docker-compose.production.yml up -d
```

## Security Considerations

### Production Security Checklist

- [ ] Change all default passwords
- [ ] Use strong, unique API keys
- [ ] Enable SSL/TLS certificates
- [ ] Configure firewall rules
- [ ] Enable audit logging
- [ ] Regular security updates
- [ ] Backup encryption
- [ ] Network segmentation

### Secrets Management

```bash
# Use environment files with restricted permissions
chmod 600 .env.production

# Consider using external secrets management
# - HashiCorp Vault
# - AWS Secrets Manager
# - Azure Key Vault
# - Google Secret Manager
```

## Support and Maintenance

### Regular Maintenance Tasks

1. **Update Dependencies:** Monthly security updates
2. **Database Maintenance:** Weekly VACUUM and ANALYZE
3. **Log Rotation:** Configure logrotate for container logs
4. **Backup Verification:** Weekly backup restore tests
5. **Performance Review:** Monthly performance analysis

### Getting Help

- **GitHub Issues:** Report bugs and feature requests
- **Documentation:** Check inline code documentation
- **Logs:** Always include relevant logs when seeking help
- **Monitoring:** Use Grafana dashboards for system insights

---

For additional help or questions, please refer to the main README.md or create an issue in the repository.
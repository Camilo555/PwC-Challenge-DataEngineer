# 🚀 PWC Retail Data Platform - Multi-Environment Deployment Guide

## 📋 Overview

This guide covers deploying the PWC Retail Data Platform across TEST, DEV, and PROD environments using Docker Compose with environment-specific configurations.

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Multi-Environment Setup                     │
├─────────────────┬─────────────────┬─────────────────────────────┤
│   TEST ENV      │    DEV ENV      │       PROD ENV              │
│   Port: 8001    │   Port: 8002    │      Port: 8000             │
│   Lightweight   │   Full Stack    │   Enterprise Grade          │
│   Fast Testing  │   Development   │   High Availability         │
└─────────────────┴─────────────────┴─────────────────────────────┘
```

## 🗂️ File Structure

```
PWC-Challenge-DataEngineer/
├── docker-compose.base.yml      # Base configuration (shared)
├── docker-compose.test.yml      # TEST environment overrides
├── docker-compose.dev.yml       # DEV environment overrides  
├── docker-compose.prod.yml      # PROD environment overrides
├── .env.test                    # TEST environment variables
├── .env.dev                     # DEV environment variables
├── .env.prod                    # PROD environment variables
├── scripts/
│   ├── deploy.sh               # Unix deployment script
│   └── deploy.bat              # Windows deployment script
└── docker/
    ├── rabbitmq/
    ├── prometheus/
    ├── grafana/
    └── nginx/
```

## 🌍 Environment Configurations

### 🧪 TEST Environment
- **Purpose**: Automated testing, CI/CD, integration tests
- **Resources**: Minimal (512MB-1GB per service)
- **Features**: 
  - Lightweight components
  - Single partition Kafka
  - Pandas processing engine
  - In-memory configurations
  - Test databases with mock data

### 🛠️ DEV Environment  
- **Purpose**: Development, debugging, feature testing
- **Resources**: Moderate (1-3GB per service)
- **Features**:
  - Full Spark cluster
  - Development tools (Jupyter, PgAdmin)
  - Live code reload
  - Monitoring stack
  - Debug logging

### 🏭 PROD Environment
- **Purpose**: Production workloads, high availability
- **Resources**: High (2-6GB per service)
- **Features**:
  - Multi-node Kafka cluster
  - Multiple Spark workers
  - Load balancers
  - SSL/TLS encryption
  - Backup services
  - Enterprise monitoring

## 🚀 Quick Start

### Prerequisites

1. **Docker & Docker Compose**
   ```bash
   # Verify installation
   docker --version
   docker-compose --version
   ```

2. **System Requirements**
   - **TEST**: 4GB RAM, 2 CPU cores
   - **DEV**: 8GB RAM, 4 CPU cores  
   - **PROD**: 16GB+ RAM, 8+ CPU cores

3. **Network Ports** (ensure these are available):
   ```
   TEST:  8001 (API), 5433 (DB), 9093 (Kafka), 8109 (Typesense)
   DEV:   8002 (API), 5434 (DB), 9094 (Kafka), 8110 (Typesense)
   PROD:  8000 (API), 5432 (DB), 9092 (Kafka), 8108 (Typesense)
   ```

### 🎯 Deploy TEST Environment

```bash
# Unix/Linux/Mac
./scripts/deploy.sh test

# Windows
scripts\deploy.bat test
```

### 🛠️ Deploy DEV Environment

```bash
# Unix/Linux/Mac
./scripts/deploy.sh dev --profiles api,etl,orchestration,dev-tools

# Windows  
scripts\deploy.bat dev /p api,etl,orchestration,dev-tools
```

### 🏭 Deploy PROD Environment

```bash
# First, configure production secrets in .env.prod

# Unix/Linux/Mac
./scripts/deploy.sh prod --profiles api,etl,orchestration,monitoring

# Windows
scripts\deploy.bat prod /p api,etl,orchestration,monitoring
```

## 📝 Detailed Deployment Instructions

### 1. Environment Setup

#### TEST Environment
```bash
# 1. Deploy with minimal profile
./scripts/deploy.sh test --detach

# 2. Run integration tests
./scripts/deploy.sh test --profiles ci,integration-test

# 3. Check status
./scripts/deploy.sh test --status
```

#### DEV Environment
```bash
# 1. Deploy with development tools
./scripts/deploy.sh dev --profiles api,etl,orchestration,dev-tools,dev-monitoring --build

# 2. Access development services
echo "API: http://localhost:8002"
echo "Dagster: http://localhost:3003" 
echo "PgAdmin: http://localhost:5050"
echo "Jupyter: http://localhost:8888"
echo "Grafana: http://localhost:3002"
```

#### PROD Environment
```bash
# 1. Configure production secrets in .env.prod
# Set strong passwords for:
# - POSTGRES_PASSWORD
# - RABBITMQ_PASSWORD  
# - SECRET_KEY
# - GRAFANA_ADMIN_PASSWORD

# 2. Create data directories
sudo mkdir -p /opt/pwc-data/{postgres,rabbitmq,kafka,spark,typesense}
sudo chown -R $(whoami) /opt/pwc-data

# 3. Deploy production stack
./scripts/deploy.sh prod --profiles api,etl,orchestration,monitoring,kafka-cluster

# 4. Verify deployment
./scripts/deploy.sh prod --status
```

### 2. Service Profiles

Use `--profiles` to deploy specific service groups:

| Profile | Services | Description |
|---------|----------|-------------|
| `api` | API, Database, Message Queue | Core API services |
| `etl` | ETL Bronze/Silver/Gold | Data processing pipeline |
| `orchestration` | Dagster | Workflow orchestration |
| `dagster` | Dagster only | Asset-based orchestration |  
| `airflow` | Airflow stack | Traditional workflow management |
| `monitoring` | Datadog, Prometheus, Grafana | Observability stack |
| `dev-tools` | PgAdmin, Jupyter | Development utilities |
| `kafka-cluster` | Multi-broker Kafka | High-availability streaming |
| `backup` | Backup services | Automated backups (prod only) |

### 3. Common Operations

#### View Logs
```bash
# All services
./scripts/deploy.sh dev --logs

# Specific service
./scripts/deploy.sh prod --logs api
```

#### Stop Environment
```bash
# Stop and remove containers
./scripts/deploy.sh dev --down
```

#### Update Services
```bash
# Rebuild and update
./scripts/deploy.sh prod --build --profiles api
```

## 🔧 Configuration

### Environment Variables

Each environment has its own `.env` file with appropriate configurations for TEST, DEV, and PROD deployments.

### Resource Limits

| Environment | Memory/Service | CPU/Service | Total RAM |
|-------------|----------------|-------------|-----------|
| TEST | 256MB - 1GB | 0.25 - 1.0 | ~4GB |
| DEV | 512MB - 3GB | 0.5 - 2.0 | ~8GB |
| PROD | 1GB - 6GB | 1.0 - 4.0 | 16GB+ |

## 🔒 Security

### Production Security Checklist

- [ ] **Strong Passwords**: All default passwords changed
- [ ] **SSL/TLS**: HTTPS enabled with valid certificates  
- [ ] **Network Isolation**: Internal networks configured
- [ ] **Secret Management**: No secrets in environment files
- [ ] **Container Security**: Non-root users, no new privileges
- [ ] **Firewall**: Only required ports exposed
- [ ] **Monitoring**: Security monitoring enabled
- [ ] **Backups**: Automated backup strategy implemented

## 📊 Monitoring & Observability

### Monitoring Endpoints

| Environment | Grafana | Prometheus | Dagster | API Health |
|-------------|---------|------------|---------|------------|
| TEST | :3001 | :9091 | :3001 | :8001/health |
| DEV | :3002 | :9091 | :3003 | :8002/health |
| PROD | :3001 | :9090 | :3000 | :8000/health |

## 🛠️ Troubleshooting

### Common Issues

#### Port Conflicts
```bash
# Find processes using ports
netstat -tulpn | grep :8000

# Kill process
sudo kill -9 <PID>
```

#### Memory Issues
```bash
# Check container memory usage
docker stats

# Clean up unused resources
docker system prune -f
```

#### Service Won't Start
```bash
# Check service logs
./scripts/deploy.sh env --logs service-name

# Restart specific service
docker-compose restart service-name
```

## 🎯 Usage Examples

### Example 1: Deploy DEV environment with full monitoring
```bash
./scripts/deploy.sh dev --profiles api,etl,orchestration,dev-tools,dev-monitoring --build --detach
```

### Example 2: Deploy PROD with high availability
```bash
./scripts/deploy.sh prod --profiles api,etl,orchestration,monitoring,kafka-cluster,backup
```

### Example 3: Run integration tests in TEST
```bash
./scripts/deploy.sh test --profiles ci,integration-test --build
```

### Example 4: Scale production API
```bash
# Scale API to 3 instances
docker-compose -f docker-compose.base.yml -f docker-compose.prod.yml --env-file .env.prod up -d --scale api=3
```

---

**Enterprise Data Platform** | **Multi-Environment Ready** | **Production Tested**

For more information, see the main [README.md](./README.md) or open an issue for support.

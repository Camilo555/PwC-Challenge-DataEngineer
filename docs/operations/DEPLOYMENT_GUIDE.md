# PwC Retail Data Platform - Deployment Guide

## Table of Contents

1. [Overview](#overview)
2. [Infrastructure Prerequisites](#infrastructure-prerequisites)
3. [Environment Setup](#environment-setup)
4. [Docker Deployment](#docker-deployment)
5. [Kubernetes Deployment](#kubernetes-deployment)
6. [Terraform Infrastructure](#terraform-infrastructure)
7. [CI/CD Pipeline Setup](#cicd-pipeline-setup)
8. [Security Configuration](#security-configuration)
9. [Monitoring & Observability](#monitoring--observability)
10. [Troubleshooting](#troubleshooting)

## Overview

The PwC Retail Data Platform supports multiple deployment strategies to accommodate different environments and use cases. This guide covers deployment across development, staging, and production environments using modern DevOps practices.

### Deployment Architecture

```mermaid
graph TB
    subgraph "Development Environment"
        DEV_DOCKER[Docker Compose<br/>Local Development]
        DEV_DB[(PostgreSQL<br/>Local)]
        DEV_CACHE[(Redis<br/>Local)]
    end
    
    subgraph "Staging Environment"
        STAGE_K8S[Kubernetes Cluster<br/>Staging]
        STAGE_DB[(Cloud PostgreSQL<br/>Staging)]
        STAGE_CACHE[(Cloud Redis<br/>Staging)]
        STAGE_STORAGE[(Cloud Storage<br/>Staging)]
    end
    
    subgraph "Production Environment"
        PROD_K8S[Kubernetes Cluster<br/>Production]
        PROD_DB[(Cloud PostgreSQL<br/>Production)]
        PROD_CACHE[(Cloud Redis<br/>Production)]
        PROD_STORAGE[(Cloud Storage<br/>Production)]
        PROD_CDN[CDN<br/>Global Distribution]
    end
    
    subgraph "Infrastructure as Code"
        TERRAFORM[Terraform<br/>Multi-Cloud IaC]
        HELM[Helm Charts<br/>K8s Deployment]
        ANSIBLE[Ansible<br/>Configuration Management]
    end
    
    subgraph "CI/CD Pipeline"
        GIT[Git Repository]
        CI[GitHub Actions<br/>CI Pipeline]
        CD[ArgoCD<br/>GitOps Deployment]
        REGISTRY[Container Registry]
    end
    
    %% Flow
    GIT --> CI
    CI --> REGISTRY
    CI --> STAGE_K8S
    CD --> PROD_K8S
    
    TERRAFORM --> STAGE_K8S
    TERRAFORM --> PROD_K8S
    HELM --> STAGE_K8S
    HELM --> PROD_K8S
```

### Deployment Strategies

| Environment | Strategy | Infrastructure | Automation Level |
|-------------|----------|---------------|-------------------|
| **Development** | Docker Compose | Local/Laptop | Manual |
| **Staging** | Kubernetes | Cloud (Single Region) | Semi-Automated |
| **Production** | Kubernetes | Multi-Cloud/Multi-Region | Fully Automated |

## Infrastructure Prerequisites

### Minimum Requirements

#### Development Environment
- **CPU**: 4 cores minimum, 8 cores recommended
- **Memory**: 16 GB RAM minimum, 32 GB recommended
- **Storage**: 100 GB available space
- **Network**: Stable internet connection for package downloads

#### Staging Environment
- **Kubernetes Cluster**: 3 nodes minimum
- **Node Specifications**: 4 vCPU, 16 GB RAM, 100 GB SSD per node
- **Database**: Managed PostgreSQL (e.g., AWS RDS, Azure Database)
- **Cache**: Managed Redis (e.g., AWS ElastiCache, Azure Cache)
- **Storage**: Object storage (S3, Azure Blob, GCS)

#### Production Environment
- **Kubernetes Cluster**: 5+ nodes across multiple AZs
- **Node Specifications**: 8 vCPU, 32 GB RAM, 500 GB SSD per node
- **Database**: High-availability managed PostgreSQL with read replicas
- **Cache**: Redis cluster with failover
- **Storage**: Multi-region object storage with replication
- **Monitoring**: Prometheus, Grafana, AlertManager
- **Logging**: ELK stack or managed logging solution

### Software Dependencies

```bash
# Required Software Versions
Docker >= 24.0
Docker Compose >= 2.20
Kubernetes >= 1.28
Helm >= 3.12
Terraform >= 1.6
kubectl >= 1.28
Python >= 3.10
Node.js >= 18.0 (for frontend)
```

## Environment Setup

### Environment Variables

Create environment-specific configuration files:

#### .env.development
```bash
# Database Configuration
DATABASE_URL=postgresql://postgres:password@localhost:5432/retail_dev
DATABASE_POOL_SIZE=10
DATABASE_MAX_OVERFLOW=20

# Redis Configuration  
REDIS_URL=redis://localhost:6379/0
REDIS_MAX_CONNECTIONS=20

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=true
LOG_LEVEL=DEBUG

# Security
JWT_SECRET_KEY=dev-secret-key-change-in-production
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30

# External Services
ELASTICSEARCH_URL=http://localhost:9200
TYPESENSE_HOST=localhost
TYPESENSE_PORT=8108
TYPESENSE_API_KEY=dev-api-key

# Data Processing
SPARK_MASTER_URL=local[*]
DBT_PROFILES_DIR=./dbt/profiles

# Feature Flags
ENABLE_VECTOR_SEARCH=true
ENABLE_ADVANCED_ANALYTICS=true
ENABLE_CACHING=true
```

#### .env.staging
```bash
# Database Configuration
DATABASE_URL=postgresql://user:pass@staging-db.region.rds.amazonaws.com:5432/retail_staging
DATABASE_POOL_SIZE=20
DATABASE_MAX_OVERFLOW=40

# Redis Configuration
REDIS_URL=redis://staging-cache.region.cache.amazonaws.com:6379
REDIS_MAX_CONNECTIONS=50

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=false
LOG_LEVEL=INFO

# Security
JWT_SECRET_KEY=${JWT_SECRET_FROM_VAULT}
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30

# External Services
ELASTICSEARCH_URL=https://staging-search.region.es.amazonaws.com
TYPESENSE_HOST=staging-typesense.company.com
TYPESENSE_PORT=443
TYPESENSE_API_KEY=${TYPESENSE_API_KEY_FROM_VAULT}

# Cloud Storage
AWS_S3_BUCKET=pwc-retail-staging-data
AWS_REGION=us-east-1

# Monitoring
PROMETHEUS_URL=http://prometheus.monitoring.svc.cluster.local:9090
GRAFANA_URL=http://grafana.monitoring.svc.cluster.local:3000

# Feature Flags
ENABLE_VECTOR_SEARCH=true
ENABLE_ADVANCED_ANALYTICS=true
ENABLE_CACHING=true
```

#### .env.production
```bash
# Database Configuration (Read from secrets)
DATABASE_URL=${DATABASE_URL_FROM_VAULT}
DATABASE_POOL_SIZE=50
DATABASE_MAX_OVERFLOW=100

# Redis Configuration
REDIS_URL=${REDIS_URL_FROM_VAULT}
REDIS_MAX_CONNECTIONS=100

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
DEBUG=false
LOG_LEVEL=WARNING

# Security
JWT_SECRET_KEY=${JWT_SECRET_FROM_VAULT}
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=15  # Shorter for production

# External Services
ELASTICSEARCH_URL=${ELASTICSEARCH_URL_FROM_VAULT}
TYPESENSE_HOST=${TYPESENSE_HOST_FROM_VAULT}
TYPESENSE_PORT=443
TYPESENSE_API_KEY=${TYPESENSE_API_KEY_FROM_VAULT}

# Cloud Storage
AWS_S3_BUCKET=pwc-retail-prod-data
AWS_REGION=us-east-1

# Monitoring
PROMETHEUS_URL=http://prometheus.monitoring.svc.cluster.local:9090
GRAFANA_URL=http://grafana.monitoring.svc.cluster.local:3000

# Feature Flags
ENABLE_VECTOR_SEARCH=true
ENABLE_ADVANCED_ANALYTICS=true
ENABLE_CACHING=true

# Production Optimizations
GUNICORN_WORKERS=4
GUNICORN_THREADS=2
PRELOAD_APP=true
```

## Docker Deployment

### Development Deployment

#### Quick Start
```bash
# Clone repository
git clone <repository-url>
cd pwc-retail-data-platform

# Copy environment file
cp .env.example .env

# Start all services
docker-compose up -d

# Initialize database
docker-compose exec api python scripts/init_db.py

# Load sample data
docker-compose exec api python scripts/seed_data.py

# Access the application
open http://localhost:8000/docs
```

#### Complete Docker Compose Configuration

```yaml
# docker-compose.yml (Production-Ready)
version: '3.8'

services:
  # API Service
  api:
    build:
      context: .
      dockerfile: docker/Dockerfile.api
      target: production
    ports:
      - "${API_PORT:-8000}:8000"
    environment:
      - DATABASE_URL=${DATABASE_URL}
      - REDIS_URL=${REDIS_URL}
      - JWT_SECRET_KEY=${JWT_SECRET_KEY}
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    volumes:
      - ./logs:/app/logs
      - ./data:/app/data
    networks:
      - app-network
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 40s

  # Database
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: ${POSTGRES_DB:-retail}
      POSTGRES_USER: ${POSTGRES_USER:-postgres}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-password}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./docker/init-db.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "5432:5432"
    networks:
      - app-network
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-postgres}"]
      interval: 10s
      timeout: 5s
      retries: 5

  # Redis Cache
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    networks:
      - app-network
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 3

  # Elasticsearch
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - bootstrap.memory_lock=true
      - "ES_JAVA_OPTS=-Xms2g -Xmx2g"
    ulimits:
      memlock:
        soft: -1
        hard: -1
    volumes:
      - elasticsearch_data:/usr/share/elasticsearch/data
    ports:
      - "9200:9200"
    networks:
      - app-network
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:9200/_cluster/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  # Typesense Vector Search
  typesense:
    image: typesense/typesense:0.25.1
    environment:
      TYPESENSE_DATA_DIR: /data
      TYPESENSE_API_KEY: ${TYPESENSE_API_KEY}
    volumes:
      - typesense_data:/data
    ports:
      - "8108:8108"
    networks:
      - app-network
    restart: unless-stopped

  # ETL Services
  etl-scheduler:
    build:
      context: .
      dockerfile: docker/Dockerfile.etl
    environment:
      - DATABASE_URL=${DATABASE_URL}
      - DAGSTER_HOME=/app/dagster_home
    volumes:
      - ./data:/app/data
      - ./dagster_home:/app/dagster_home
    depends_on:
      - postgres
      - redis
    networks:
      - app-network
    restart: unless-stopped
    command: ["dagster", "daemon", "run"]

  # Monitoring - Prometheus
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./docker/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    networks:
      - app-network
    restart: unless-stopped
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'

  # Monitoring - Grafana
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD:-admin}
    volumes:
      - grafana_data:/var/lib/grafana
      - ./docker/grafana/provisioning:/etc/grafana/provisioning
    networks:
      - app-network
    restart: unless-stopped

volumes:
  postgres_data:
    driver: local
  redis_data:
    driver: local
  elasticsearch_data:
    driver: local
  typesense_data:
    driver: local
  prometheus_data:
    driver: local
  grafana_data:
    driver: local

networks:
  app-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16
```

#### Container Health Checks

```bash
# Health check script
#!/bin/bash
# docker/healthcheck.sh

set -e

# Check API health
curl -f http://localhost:8000/health || exit 1

# Check database connectivity
python -c "
import psycopg2
import os
try:
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    conn.close()
    print('Database: OK')
except Exception as e:
    print(f'Database: FAIL - {e}')
    exit(1)
"

# Check Redis connectivity
python -c "
import redis
import os
try:
    r = redis.from_url(os.environ['REDIS_URL'])
    r.ping()
    print('Redis: OK')
except Exception as e:
    print(f'Redis: FAIL - {e}')
    exit(1)
"

echo "All health checks passed"
```

## Kubernetes Deployment

### Kubernetes Manifests

#### Namespace and Configuration

```yaml
# k8s/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: pwc-retail
  labels:
    name: pwc-retail
    environment: production

---
# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: pwc-retail
data:
  DATABASE_POOL_SIZE: "50"
  REDIS_MAX_CONNECTIONS: "100"
  LOG_LEVEL: "INFO"
  PROMETHEUS_URL: "http://prometheus.monitoring.svc.cluster.local:9090"
  ENABLE_VECTOR_SEARCH: "true"
  ENABLE_ADVANCED_ANALYTICS: "true"
```

#### Secrets Management

```yaml
# k8s/secrets.yaml
apiVersion: v1
kind: Secret
metadata:
  name: app-secrets
  namespace: pwc-retail
type: Opaque
stringData:
  database-url: postgresql://user:password@postgres.database.svc.cluster.local:5432/retail
  redis-url: redis://redis.cache.svc.cluster.local:6379
  jwt-secret-key: super-secret-jwt-key
  typesense-api-key: typesense-api-key
  elasticsearch-url: https://elasticsearch.search.svc.cluster.local:9200

---
# External Secrets Operator integration
apiVersion: external-secrets.io/v1beta1
kind: SecretStore
metadata:
  name: vault-secret-store
  namespace: pwc-retail
spec:
  provider:
    vault:
      server: "https://vault.company.com"
      path: "secret"
      version: "v2"
      auth:
        kubernetes:
          mountPath: "kubernetes"
          role: "pwc-retail-role"

---
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: app-external-secrets
  namespace: pwc-retail
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-secret-store
    kind: SecretStore
  target:
    name: app-secrets
    creationPolicy: Owner
  data:
  - secretKey: database-url
    remoteRef:
      key: pwc-retail/database
      property: url
  - secretKey: jwt-secret-key
    remoteRef:
      key: pwc-retail/jwt
      property: secret
```

#### API Service Deployment

```yaml
# k8s/api-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-service
  namespace: pwc-retail
  labels:
    app: api-service
    version: v1
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 1
  selector:
    matchLabels:
      app: api-service
  template:
    metadata:
      labels:
        app: api-service
        version: v1
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8000"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: api-service-sa
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 2000
      containers:
      - name: api
        image: pwc-retail/api:latest
        imagePullPolicy: Always
        ports:
        - containerPort: 8000
          name: http
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: app-secrets
              key: database-url
        - name: REDIS_URL
          valueFrom:
            secretKeyRef:
              name: app-secrets
              key: redis-url
        - name: JWT_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: app-secrets
              key: jwt-secret-key
        envFrom:
        - configMapRef:
            name: app-config
        resources:
          requests:
            cpu: 500m
            memory: 1Gi
          limits:
            cpu: 2000m
            memory: 4Gi
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 3
        volumeMounts:
        - name: app-logs
          mountPath: /app/logs
        - name: temp-data
          mountPath: /app/temp
      volumes:
      - name: app-logs
        emptyDir: {}
      - name: temp-data
        emptyDir:
          sizeLimit: 10Gi

---
# Service Account
apiVersion: v1
kind: ServiceAccount
metadata:
  name: api-service-sa
  namespace: pwc-retail
automountServiceAccountToken: false

---
# Service
apiVersion: v1
kind: Service
metadata:
  name: api-service
  namespace: pwc-retail
  labels:
    app: api-service
spec:
  type: ClusterIP
  ports:
  - port: 80
    targetPort: 8000
    protocol: TCP
    name: http
  selector:
    app: api-service
```

#### Horizontal Pod Autoscaler

```yaml
# k8s/hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: api-service-hpa
  namespace: pwc-retail
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: api-service
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 100
        periodSeconds: 15
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
```

#### Ingress Configuration

```yaml
# k8s/ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: api-ingress
  namespace: pwc-retail
  annotations:
    kubernetes.io/ingress.class: nginx
    cert-manager.io/cluster-issuer: letsencrypt-prod
    nginx.ingress.kubernetes.io/rate-limit: "100"
    nginx.ingress.kubernetes.io/rate-limit-window: "1m"
    nginx.ingress.kubernetes.io/proxy-body-size: "100m"
    nginx.ingress.kubernetes.io/cors-allow-origin: "https://app.pwc-retail.com"
    nginx.ingress.kubernetes.io/enable-cors: "true"
spec:
  tls:
  - hosts:
    - api.pwc-retail.com
    secretName: api-tls-secret
  rules:
  - host: api.pwc-retail.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: api-service
            port:
              number: 80

---
# Network Policy for Security
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: api-network-policy
  namespace: pwc-retail
spec:
  podSelector:
    matchLabels:
      app: api-service
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: nginx-ingress
    ports:
    - protocol: TCP
      port: 8000
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: database
    ports:
    - protocol: TCP
      port: 5432
  - to:
    - namespaceSelector:
        matchLabels:
          name: cache
    ports:
    - protocol: TCP
      port: 6379
```

### Helm Chart Deployment

#### Chart Structure
```
helm/
├── Chart.yaml
├── values.yaml
├── values-staging.yaml
├── values-production.yaml
└── templates/
    ├── deployment.yaml
    ├── service.yaml
    ├── ingress.yaml
    ├── configmap.yaml
    ├── secrets.yaml
    ├── hpa.yaml
    ├── pdb.yaml
    └── servicemonitor.yaml
```

#### Helm Values Configuration

```yaml
# helm/values-production.yaml
global:
  environment: production
  imageRegistry: registry.company.com
  imagePullSecrets:
    - name: registry-secret

image:
  repository: pwc-retail/api
  tag: "1.0.0"
  pullPolicy: Always

replicaCount: 3
minReplicas: 3
maxReplicas: 20

resources:
  requests:
    cpu: 500m
    memory: 1Gi
  limits:
    cpu: 2000m
    memory: 4Gi

autoscaling:
  enabled: true
  targetCPUUtilizationPercentage: 70
  targetMemoryUtilizationPercentage: 80

service:
  type: ClusterIP
  port: 80
  targetPort: 8000

ingress:
  enabled: true
  className: nginx
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
    nginx.ingress.kubernetes.io/rate-limit: "100"
  hosts:
    - host: api.pwc-retail.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: api-tls
      hosts:
        - api.pwc-retail.com

database:
  host: prod-postgres.database.svc.cluster.local
  port: 5432
  name: retail
  poolSize: 50

redis:
  host: prod-redis.cache.svc.cluster.local
  port: 6379
  maxConnections: 100

monitoring:
  prometheus:
    enabled: true
    path: /metrics
    port: 8000
  grafana:
    enabled: true

security:
  networkPolicies:
    enabled: true
  podSecurityPolicy:
    enabled: true
  runAsNonRoot: true
  runAsUser: 1000

persistence:
  logs:
    enabled: true
    size: 10Gi
    storageClass: fast-ssd
```

#### Helm Deployment Commands

```bash
# Install Helm chart in staging
helm upgrade --install pwc-retail ./helm \
  --namespace pwc-retail \
  --create-namespace \
  --values helm/values-staging.yaml

# Install Helm chart in production
helm upgrade --install pwc-retail ./helm \
  --namespace pwc-retail \
  --create-namespace \
  --values helm/values-production.yaml \
  --wait \
  --timeout 10m

# Rollback deployment
helm rollback pwc-retail 1 --namespace pwc-retail

# Uninstall deployment
helm uninstall pwc-retail --namespace pwc-retail
```

## Terraform Infrastructure

### Multi-Cloud Infrastructure

#### Main Terraform Configuration

```hcl
# terraform/main.tf
terraform {
  required_version = ">= 1.6.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.70"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
  }

  backend "s3" {
    bucket         = "pwc-retail-terraform-state"
    key            = "infrastructure/prod/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "pwc-retail-terraform-locks"
    encrypt        = true
  }
}

# Local values for consistent naming
locals {
  project_name = "pwc-retail-platform"
  environment  = var.environment
  region       = var.region
  
  common_tags = {
    Project     = "PwC-Retail-Data-Platform"
    Environment = var.environment
    ManagedBy   = "Terraform"
    Owner       = "Data-Engineering-Team"
    CostCenter  = var.cost_center
  }
}

# Data sources
data "aws_caller_identity" "current" {}
data "aws_availability_zones" "available" {
  state = "available"
}

# VPC Module
module "vpc" {
  source = "./modules/networking"
  
  project_name = local.project_name
  environment  = local.environment
  region       = local.region
  
  vpc_cidr             = var.vpc_cidr
  availability_zones   = data.aws_availability_zones.available.names
  enable_nat_gateway   = true
  enable_vpn_gateway   = false
  enable_flow_logs     = true
  
  tags = local.common_tags
}

# EKS Cluster
module "eks" {
  source = "./modules/kubernetes"
  
  cluster_name    = "${local.project_name}-${local.environment}"
  cluster_version = var.kubernetes_version
  
  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnet_ids
  
  node_groups = {
    main = {
      desired_capacity = var.eks_node_group_desired_capacity
      max_capacity     = var.eks_node_group_max_capacity
      min_capacity     = var.eks_node_group_min_capacity
      
      instance_types = ["m5.large", "m5.xlarge"]
      capacity_type  = "ON_DEMAND"
      
      k8s_labels = {
        Environment = local.environment
        NodeGroup   = "main"
      }
    }
    
    spot = {
      desired_capacity = 2
      max_capacity     = 10
      min_capacity     = 0
      
      instance_types = ["m5.large", "m5.xlarge", "m5.2xlarge"]
      capacity_type  = "SPOT"
      
      k8s_labels = {
        Environment = local.environment
        NodeGroup   = "spot"
      }
      
      taints = [{
        key    = "spot"
        value  = "true"
        effect = "NO_SCHEDULE"
      }]
    }
  }
  
  tags = local.common_tags
}

# RDS Database
module "database" {
  source = "./modules/database"
  
  identifier = "${local.project_name}-${local.environment}"
  
  engine         = "postgres"
  engine_version = "15.4"
  instance_class = var.database_instance_class
  
  allocated_storage     = var.database_allocated_storage
  max_allocated_storage = var.database_max_allocated_storage
  storage_encrypted     = true
  
  database_name = var.database_name
  username      = var.database_username
  
  vpc_id                = module.vpc.vpc_id
  subnet_ids            = module.vpc.database_subnet_ids
  vpc_security_group_ids = [module.vpc.database_security_group_id]
  
  backup_retention_period = var.database_backup_retention_period
  backup_window          = "03:00-04:00"
  maintenance_window     = "sun:04:00-sun:05:00"
  
  performance_insights_enabled = true
  monitoring_interval         = 60
  
  tags = local.common_tags
}

# ElastiCache Redis
module "redis" {
  source = "./modules/cache"
  
  cluster_id = "${local.project_name}-${local.environment}"
  
  engine_version    = "7.0"
  node_type        = var.redis_node_type
  num_cache_nodes  = var.redis_num_nodes
  
  parameter_group_name = "default.redis7"
  
  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.cache_subnet_ids
  
  security_group_ids = [module.vpc.cache_security_group_id]
  
  tags = local.common_tags
}

# S3 Data Lake
module "data_lake" {
  source = "./modules/storage"
  
  project_name = local.project_name
  environment  = local.environment
  
  enable_versioning     = true
  enable_encryption     = true
  enable_lifecycle      = true
  enable_cross_region   = var.enable_cross_region_replication
  
  lifecycle_rules = [
    {
      id     = "data_lifecycle"
      status = "Enabled"
      
      transitions = [
        {
          days          = 30
          storage_class = "STANDARD_IA"
        },
        {
          days          = 90
          storage_class = "GLACIER"
        },
        {
          days          = 365
          storage_class = "DEEP_ARCHIVE"
        }
      ]
    }
  ]
  
  tags = local.common_tags
}

# Monitoring Stack
module "monitoring" {
  source = "./modules/monitoring"
  
  project_name = local.project_name
  environment  = local.environment
  
  cluster_name = module.eks.cluster_name
  
  prometheus_config = {
    retention_days = 30
    storage_size   = "50Gi"
  }
  
  grafana_config = {
    admin_password = var.grafana_admin_password
    storage_size   = "10Gi"
  }
  
  alertmanager_config = {
    slack_webhook = var.slack_webhook_url
    email_config  = var.email_config
  }
  
  tags = local.common_tags
}
```

#### Terraform Variables

```hcl
# terraform/variables.tf
variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "cost_center" {
  description = "Cost center for billing"
  type        = string
  default     = "data-engineering"
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "kubernetes_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.28"
}

variable "eks_node_group_desired_capacity" {
  description = "Desired capacity for EKS node group"
  type        = number
  default     = 3
}

variable "eks_node_group_max_capacity" {
  description = "Maximum capacity for EKS node group"
  type        = number
  default     = 10
}

variable "eks_node_group_min_capacity" {
  description = "Minimum capacity for EKS node group"
  type        = number
  default     = 1
}

variable "database_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.medium"
}

variable "database_allocated_storage" {
  description = "RDS allocated storage in GB"
  type        = number
  default     = 100
}

variable "database_max_allocated_storage" {
  description = "RDS maximum allocated storage in GB"
  type        = number
  default     = 1000
}

variable "database_backup_retention_period" {
  description = "RDS backup retention period in days"
  type        = number
  default     = 7
}

variable "redis_node_type" {
  description = "ElastiCache Redis node type"
  type        = string
  default     = "cache.t3.medium"
}

variable "redis_num_nodes" {
  description = "Number of Redis nodes"
  type        = number
  default     = 2
}
```

#### Terraform Deployment Commands

```bash
# Initialize Terraform
terraform init

# Plan infrastructure changes
terraform plan -var-file="environments/prod.tfvars"

# Apply infrastructure changes
terraform apply -var-file="environments/prod.tfvars" -auto-approve

# Destroy infrastructure (BE CAREFUL!)
terraform destroy -var-file="environments/prod.tfvars"

# Format Terraform files
terraform fmt -recursive

# Validate Terraform configuration
terraform validate

# Show current state
terraform show

# Import existing resources
terraform import module.database.aws_db_instance.main db-instance-id
```

## CI/CD Pipeline Setup

### GitHub Actions Workflow

```yaml
# .github/workflows/deploy.yml
name: Deploy PwC Retail Platform

on:
  push:
    branches:
      - main
      - develop
    tags:
      - 'v*'
  pull_request:
    branches:
      - main

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.10, 3.11]
    
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install Poetry
      uses: snok/install-poetry@v1
      with:
        version: latest
        virtualenvs-create: true
        virtualenvs-in-project: true
    
    - name: Load cached venv
      id: cached-poetry-dependencies
      uses: actions/cache@v3
      with:
        path: .venv
        key: venv-${{ runner.os }}-${{ matrix.python-version }}-${{ hashFiles('**/poetry.lock') }}
    
    - name: Install dependencies
      if: steps.cached-poetry-dependencies.outputs.cache-hit != 'true'
      run: poetry install --no-interaction --no-root
    
    - name: Install project
      run: poetry install --no-interaction
    
    - name: Run tests
      run: |
        poetry run pytest tests/ --cov=src --cov-report=xml --cov-report=html
    
    - name: Upload coverage to Codecov
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
        fail_ci_if_error: true

  security-scan:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    
    - name: Run Trivy vulnerability scanner
      uses: aquasecurity/trivy-action@master
      with:
        scan-type: 'fs'
        scan-ref: '.'
        format: 'sarif'
        output: 'trivy-results.sarif'
    
    - name: Upload Trivy scan results
      uses: github/codeql-action/upload-sarif@v2
      with:
        sarif_file: 'trivy-results.sarif'

  build:
    needs: [test, security-scan]
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
    
    steps:
    - name: Checkout repository
      uses: actions/checkout@v4
    
    - name: Log in to Container Registry
      uses: docker/login-action@v2
      with:
        registry: ${{ env.REGISTRY }}
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}
    
    - name: Extract metadata
      id: meta
      uses: docker/metadata-action@v4
      with:
        images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
        tags: |
          type=ref,event=branch
          type=ref,event=pr
          type=semver,pattern={{version}}
          type=semver,pattern={{major}}.{{minor}}
          type=sha,prefix={{branch}}-
    
    - name: Build and push Docker image
      uses: docker/build-push-action@v4
      with:
        context: .
        file: docker/Dockerfile.api
        push: true
        tags: ${{ steps.meta.outputs.tags }}
        labels: ${{ steps.meta.outputs.labels }}
        cache-from: type=gha
        cache-to: type=gha,mode=max

  deploy-staging:
    if: github.ref == 'refs/heads/develop'
    needs: build
    runs-on: ubuntu-latest
    environment: staging
    
    steps:
    - name: Checkout repository
      uses: actions/checkout@v4
    
    - name: Configure AWS credentials
      uses: aws-actions/configure-aws-credentials@v2
      with:
        aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
        aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
        aws-region: us-east-1
    
    - name: Update kubeconfig
      run: |
        aws eks update-kubeconfig --name pwc-retail-staging --region us-east-1
    
    - name: Deploy to Staging
      run: |
        helm upgrade --install pwc-retail ./helm \
          --namespace pwc-retail-staging \
          --create-namespace \
          --values helm/values-staging.yaml \
          --set image.tag=${{ github.sha }} \
          --wait \
          --timeout 10m
    
    - name: Run integration tests
      run: |
        kubectl wait --for=condition=available --timeout=300s deployment/api-service -n pwc-retail-staging
        poetry run pytest tests/integration/ --base-url=https://staging-api.pwc-retail.com

  deploy-production:
    if: startsWith(github.ref, 'refs/tags/v')
    needs: build
    runs-on: ubuntu-latest
    environment: production
    
    steps:
    - name: Checkout repository
      uses: actions/checkout@v4
    
    - name: Configure AWS credentials
      uses: aws-actions/configure-aws-credentials@v2
      with:
        aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
        aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
        aws-region: us-east-1
    
    - name: Update kubeconfig
      run: |
        aws eks update-kubeconfig --name pwc-retail-prod --region us-east-1
    
    - name: Deploy to Production
      run: |
        helm upgrade --install pwc-retail ./helm \
          --namespace pwc-retail \
          --create-namespace \
          --values helm/values-production.yaml \
          --set image.tag=${{ github.ref_name }} \
          --wait \
          --timeout 15m
    
    - name: Run smoke tests
      run: |
        kubectl wait --for=condition=available --timeout=300s deployment/api-service -n pwc-retail
        poetry run pytest tests/smoke/ --base-url=https://api.pwc-retail.com
    
    - name: Notify deployment
      uses: 8398a7/action-slack@v3
      with:
        status: ${{ job.status }}
        channel: '#deployments'
        webhook_url: ${{ secrets.SLACK_WEBHOOK }}
        text: |
          🚀 Production deployment completed successfully!
          Version: ${{ github.ref_name }}
          Commit: ${{ github.sha }}
```

### GitOps with ArgoCD

```yaml
# argocd/application.yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: pwc-retail-prod
  namespace: argocd
  finalizers:
    - resources-finalizer.argocd.argoproj.io
spec:
  project: default
  
  source:
    repoURL: https://github.com/company/pwc-retail-platform
    targetRevision: main
    path: helm
    helm:
      valueFiles:
        - values-production.yaml
      parameters:
        - name: image.tag
          value: "1.0.0"
  
  destination:
    server: https://kubernetes.default.svc
    namespace: pwc-retail
  
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
      allowEmpty: false
    syncOptions:
      - CreateNamespace=true
      - PrunePropagationPolicy=foreground
    retry:
      limit: 5
      backoff:
        duration: 5s
        factor: 2
        maxDuration: 3m

---
# ArgoCD AppProject for enhanced security
apiVersion: argoproj.io/v1alpha1
kind: AppProject
metadata:
  name: pwc-retail
  namespace: argocd
spec:
  description: PwC Retail Data Platform
  
  sourceRepos:
    - 'https://github.com/company/pwc-retail-platform'
  
  destinations:
    - namespace: 'pwc-retail*'
      server: https://kubernetes.default.svc
  
  clusterResourceWhitelist:
    - group: ''
      kind: Namespace
    - group: 'rbac.authorization.k8s.io'
      kind: ClusterRole
    - group: 'rbac.authorization.k8s.io'
      kind: ClusterRoleBinding
  
  namespaceResourceWhitelist:
    - group: ''
      kind: ConfigMap
    - group: ''
      kind: Secret
    - group: ''
      kind: Service
    - group: 'apps'
      kind: Deployment
    - group: 'apps'
      kind: ReplicaSet
    - group: 'networking.k8s.io'
      kind: Ingress
  
  roles:
    - name: developers
      description: Developer access to staging
      policies:
        - p, proj:pwc-retail:developers, applications, get, pwc-retail/staging, allow
        - p, proj:pwc-retail:developers, applications, sync, pwc-retail/staging, allow
      groups:
        - company:developers
    
    - name: ops
      description: Operations team full access
      policies:
        - p, proj:pwc-retail:ops, applications, *, pwc-retail/*, allow
      groups:
        - company:ops
```

## Security Configuration

### Security Hardening Checklist

#### Container Security

```dockerfile
# docker/Dockerfile.api (Security Hardened)
FROM python:3.10-slim as base

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Install security updates
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Set up application directory
WORKDIR /app
RUN chown appuser:appuser /app

# Copy requirements first for better caching
COPY --chown=appuser:appuser pyproject.toml poetry.lock ./

# Install Python dependencies
RUN pip install --no-cache-dir poetry==1.6.1 && \
    poetry config virtualenvs.create false && \
    poetry install --only=main --no-interaction --no-ansi

# Copy application code
COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser scripts/ ./scripts/

# Security: Remove unnecessary packages and files
RUN pip uninstall -y poetry && \
    rm -rf /root/.cache /tmp/* /var/tmp/*

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Expose port
EXPOSE 8000

# Run application
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "4", "--timeout", "120", "src.api.main:app"]
```

#### Pod Security Standards

```yaml
# k8s/pod-security-policy.yaml
apiVersion: policy/v1beta1
kind: PodSecurityPolicy
metadata:
  name: pwc-retail-psp
spec:
  privileged: false
  allowPrivilegeEscalation: false
  requiredDropCapabilities:
    - ALL
  volumes:
    - 'configMap'
    - 'emptyDir'
    - 'projected'
    - 'secret'
    - 'downwardAPI'
    - 'persistentVolumeClaim'
  runAsUser:
    rule: 'MustRunAsNonRoot'
  seLinux:
    rule: 'RunAsAny'
  fsGroup:
    rule: 'RunAsAny'

---
# Security Context Constraints (OpenShift)
apiVersion: security.openshift.io/v1
kind: SecurityContextConstraints
metadata:
  name: pwc-retail-scc
allowHostDirVolumePlugin: false
allowHostIPC: false
allowHostNetwork: false
allowHostPID: false
allowHostPorts: false
allowPrivilegedContainer: false
allowedCapabilities: null
defaultAddCapabilities: null
requiredDropCapabilities:
  - ALL
fsGroup:
  type: MustRunAs
  ranges:
    - min: 1000
      max: 65535
runAsUser:
  type: MustRunAs
  uid: 1000
seLinuxContext:
  type: MustRunAs
volumes:
  - configMap
  - downwardAPI
  - emptyDir
  - persistentVolumeClaim
  - projected
  - secret
```

#### Network Security

```yaml
# k8s/network-policies.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: deny-all-ingress
  namespace: pwc-retail
spec:
  podSelector: {}
  policyTypes:
  - Ingress

---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-api-ingress
  namespace: pwc-retail
spec:
  podSelector:
    matchLabels:
      app: api-service
  policyTypes:
  - Ingress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: nginx-ingress
    ports:
    - protocol: TCP
      port: 8000

---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-database-egress
  namespace: pwc-retail
spec:
  podSelector:
    matchLabels:
      app: api-service
  policyTypes:
  - Egress
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: database
    ports:
    - protocol: TCP
      port: 5432
  - to: []
    ports:
    - protocol: UDP
      port: 53
```

## Monitoring & Observability

### Prometheus Configuration

```yaml
# docker/prometheus/prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  external_labels:
    cluster: 'pwc-retail-prod'
    environment: 'production'

alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - alertmanager:9093

rule_files:
  - "alert-rules.yml"

scrape_configs:
  # API Service
  - job_name: 'api-service'
    static_configs:
      - targets: ['api-service:8000']
    metrics_path: '/metrics'
    scrape_interval: 10s
    scrape_timeout: 5s

  # Database
  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres-exporter:9187']

  # Redis
  - job_name: 'redis'
    static_configs:
      - targets: ['redis-exporter:9121']

  # Kubernetes
  - job_name: 'kubernetes-nodes'
    kubernetes_sd_configs:
      - role: node
    relabel_configs:
      - source_labels: [__address__]
        regex: '(.*):10250'
        replacement: '${1}:9100'
        target_label: __address__

  # ETL Pipelines
  - job_name: 'dagster'
    static_configs:
      - targets: ['dagster-daemon:3001']
```

### Alert Rules

```yaml
# docker/prometheus/alert-rules.yml
groups:
  - name: api-service
    rules:
      - alert: APIHighErrorRate
        expr: rate(http_requests_total{status=~"5.."}[5m]) > 0.1
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High error rate detected"
          description: "API error rate is {{ $value }} errors per second"

      - alert: APIHighLatency
        expr: histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m])) > 0.5
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "High API latency detected"
          description: "95th percentile latency is {{ $value }}s"

  - name: database
    rules:
      - alert: DatabaseConnectionsHigh
        expr: pg_stat_database_numbackends / pg_settings_max_connections > 0.8
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Database connections high"
          description: "Database connections at {{ $value }}% of maximum"

  - name: etl-pipelines
    rules:
      - alert: ETLPipelineFailure
        expr: increase(dagster_pipeline_failure_total[1h]) > 0
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "ETL pipeline failure"
          description: "Pipeline {{ $labels.pipeline }} has failed"
```

### Grafana Dashboards

```json
{
  "dashboard": {
    "id": null,
    "title": "PwC Retail Platform - API Overview",
    "tags": ["pwc-retail", "api"],
    "timezone": "browser",
    "panels": [
      {
        "title": "Request Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(http_requests_total[5m])",
            "legendFormat": "{{method}} {{endpoint}}"
          }
        ],
        "yAxes": [
          {
            "label": "Requests per second"
          }
        ]
      },
      {
        "title": "Response Time",
        "type": "graph", 
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))",
            "legendFormat": "95th percentile"
          },
          {
            "expr": "histogram_quantile(0.50, rate(http_request_duration_seconds_bucket[5m]))",
            "legendFormat": "50th percentile"
          }
        ]
      }
    ]
  }
}
```

## Troubleshooting

### Common Deployment Issues

#### 1. Pod Startup Issues

**Problem**: Pods failing to start with `CrashLoopBackOff`

```bash
# Diagnostic commands
kubectl get pods -n pwc-retail
kubectl describe pod <pod-name> -n pwc-retail
kubectl logs <pod-name> -n pwc-retail --previous
```

**Common Solutions**:
```bash
# Check resource limits
kubectl describe pod <pod-name> -n pwc-retail | grep -A 5 "Limits"

# Check secrets and configmaps
kubectl get secrets -n pwc-retail
kubectl get configmaps -n pwc-retail

# Check node resources
kubectl top nodes
kubectl describe node <node-name>
```

#### 2. Database Connection Issues

**Problem**: Application can't connect to database

```bash
# Test database connectivity
kubectl run -it --rm debug --image=postgres:15 --restart=Never -- bash
psql -h <database-host> -U <username> -d <database> -c "SELECT version();"

# Check database secrets
kubectl get secret app-secrets -n pwc-retail -o yaml

# Test from within cluster
kubectl exec -it <pod-name> -n pwc-retail -- python -c "
import psycopg2
import os
conn = psycopg2.connect(os.environ['DATABASE_URL'])
print('Database connection successful')
"
```

#### 3. Ingress/Load Balancer Issues

**Problem**: External traffic not reaching services

```bash
# Check ingress status
kubectl get ingress -n pwc-retail
kubectl describe ingress <ingress-name> -n pwc-retail

# Check service endpoints
kubectl get endpoints -n pwc-retail
kubectl describe service <service-name> -n pwc-retail

# Test internal connectivity
kubectl port-forward service/<service-name> 8080:80 -n pwc-retail
curl http://localhost:8080/health
```

### Deployment Verification Checklist

```bash
#!/bin/bash
# scripts/verify-deployment.sh

set -e

NAMESPACE="pwc-retail"
SERVICE_NAME="api-service"

echo "🔍 Verifying deployment..."

# Check namespace exists
if ! kubectl get namespace $NAMESPACE >/dev/null 2>&1; then
    echo "❌ Namespace $NAMESPACE does not exist"
    exit 1
fi
echo "✅ Namespace exists"

# Check all pods are running
PENDING_PODS=$(kubectl get pods -n $NAMESPACE --field-selector=status.phase!=Running -o name | wc -l)
if [ $PENDING_PODS -gt 0 ]; then
    echo "❌ Found $PENDING_PODS non-running pods"
    kubectl get pods -n $NAMESPACE --field-selector=status.phase!=Running
    exit 1
fi
echo "✅ All pods are running"

# Check service is accessible
SERVICE_IP=$(kubectl get service $SERVICE_NAME -n $NAMESPACE -o jsonpath='{.spec.clusterIP}')
if ! kubectl run -it --rm debug-$RANDOM --image=curlimages/curl --restart=Never -- curl -f http://$SERVICE_IP/health >/dev/null 2>&1; then
    echo "❌ Service health check failed"
    exit 1
fi
echo "✅ Service is healthy"

# Check ingress is configured
if ! kubectl get ingress -n $NAMESPACE >/dev/null 2>&1; then
    echo "⚠️  No ingress found (might be expected for internal services)"
else
    echo "✅ Ingress configured"
fi

# Check HPA is working
HPA_COUNT=$(kubectl get hpa -n $NAMESPACE -o name | wc -l)
if [ $HPA_COUNT -eq 0 ]; then
    echo "⚠️  No HPA found"
else
    echo "✅ HPA configured"
fi

echo "🎉 Deployment verification complete!"
```

### Performance Tuning

#### JVM/Python Optimization

```bash
# Production environment variables for optimal performance
export PYTHONUNBUFFERED=1
export PYTHONDONTWRITEBYTECODE=1
export PYTHONHASHSEED=random

# Gunicorn optimization
export GUNICORN_WORKERS=4
export GUNICORN_THREADS=2
export GUNICORN_MAX_REQUESTS=1000
export GUNICORN_MAX_REQUESTS_JITTER=100
export GUNICORN_PRELOAD=true
export GUNICORN_TIMEOUT=120
export GUNICORN_KEEPALIVE=5

# Memory optimization
export MALLOC_ARENA_MAX=2
```

#### Database Performance

```sql
-- Production database optimization
-- Connection pooling
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';

-- Memory settings
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;

-- Logging for monitoring
ALTER SYSTEM SET log_min_duration_statement = 1000;
ALTER SYSTEM SET log_checkpoints = on;
ALTER SYSTEM SET log_connections = on;
ALTER SYSTEM SET log_disconnections = on;

SELECT pg_reload_conf();
```

For detailed troubleshooting scenarios, see our [Operations Runbooks](./runbooks/README.md).
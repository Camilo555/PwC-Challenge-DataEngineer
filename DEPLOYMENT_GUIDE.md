# Production Deployment Guide

## Overview
This guide provides comprehensive instructions for deploying the PwC Data Engineering Challenge retail ETL pipeline to production environments.

## Prerequisites

### System Requirements
- **Python**: 3.10 (exactly - project uses Poetry with strict version requirement)
- **Java**: JDK 11+ (required for Spark/Delta Lake)
- **Memory**: Minimum 8GB RAM (16GB recommended for Spark operations)
- **Storage**: Minimum 50GB available disk space
- **Network**: Access to external APIs (exchangerate-api.com, restcountries.com, etc.)

### Infrastructure Requirements
- **Database**: PostgreSQL 13+ or Supabase instance
- **Message Queue**: Optional (for future scaling)
- **Container Platform**: Docker/Kubernetes (recommended)
- **Monitoring**: Prometheus/Grafana (optional but recommended)

## Installation

### 1. Environment Setup

```bash
# Install Python 3.10 (use pyenv for version management)
pyenv install 3.10.11
pyenv local 3.10.11

# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -

# Clone and setup project
git clone <repository-url>
cd PwC-Challenge-DataEngineer
poetry install --only=main  # Production dependencies only
```

### 2. Configuration

Create production environment file:

```bash
# Copy example environment
cp .env.example .env.production

# Edit with production values
nano .env.production
```

Required production environment variables:

```env
# Core Configuration
ENVIRONMENT=production
DATABASE_URL=postgresql://user:password@host:port/database
API_PORT=8000

# Security (CRITICAL: Generate secure values)
SECRET_KEY=your-secure-32-character-secret-key-here
BASIC_AUTH_USERNAME=admin
BASIC_AUTH_PASSWORD=secure-password-min-12-chars
TYPESENSE_API_KEY=your-typesense-api-key

# External API Keys
CURRENCY_API_KEY=your-exchangerate-api-key
COUNTRY_API_KEY=optional-restcountries-api-key
PRODUCT_API_KEY=optional-product-api-key

# Data Storage Paths
RAW_DATA_PATH=/data/raw
BRONZE_PATH=/data/bronze
SILVER_PATH=/data/silver  
GOLD_PATH=/data/gold

# Supabase (if using)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your-anon-key
SUPABASE_SERVICE_KEY=your-service-key

# Spark Configuration
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g
SPARK_SQL_ADAPTIVE_ENABLED=true
SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED=true
```

### 3. Database Setup

```bash
# Initialize database
poetry run alembic upgrade head

# Create initial data directories
mkdir -p /data/{raw,bronze,silver,gold}
chown -R app:app /data/
```

### 4. Security Hardening

Generate secure credentials:

```bash
# Generate secure secret key
python -c "import secrets; print(secrets.token_urlsafe(32))"

# Generate secure password
python -c "import secrets; import string; chars = string.ascii_letters + string.digits + '!@#$%^&*'; print(''.join(secrets.choice(chars) for _ in range(16)))"
```

## Deployment Options

### Option 1: Docker Deployment (Recommended)

Create production Dockerfile:

```dockerfile
FROM python:3.10-slim

# System dependencies
RUN apt-get update && apt-get install -y \\
    openjdk-11-jre-headless \\
    curl \\
    && rm -rf /var/lib/apt/lists/*

# Python environment
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN pip install poetry && \\
    poetry config virtualenvs.create false && \\
    poetry install --only=main --no-dev

# Application code
COPY src/ ./src/
COPY .env.production ./.env

# Run application
CMD ["poetry", "run", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

Deploy with Docker Compose:

```yaml
version: '3.8'

services:
  app:
    build: .
    ports:
      - "8000:8000"
    environment:
      - ENVIRONMENT=production
    volumes:
      - ./data:/data
      - ./logs:/app/logs
    depends_on:
      - postgres
    restart: unless-stopped

  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: retail_etl
      POSTGRES_USER: app
      POSTGRES_PASSWORD: secure_password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    restart: unless-stopped

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - ./ssl:/etc/nginx/ssl
    depends_on:
      - app
    restart: unless-stopped

volumes:
  postgres_data:
```

### Option 2: Kubernetes Deployment

Create Kubernetes manifests:

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: retail-etl

---
# deployment.yaml  
apiVersion: apps/v1
kind: Deployment
metadata:
  name: retail-etl-app
  namespace: retail-etl
spec:
  replicas: 3
  selector:
    matchLabels:
      app: retail-etl-app
  template:
    metadata:
      labels:
        app: retail-etl-app
    spec:
      containers:
      - name: app
        image: retail-etl:latest
        ports:
        - containerPort: 8000
        env:
        - name: ENVIRONMENT
          value: "production"
        resources:
          requests:
            memory: "2Gi"
            cpu: "500m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        volumeMounts:
        - name: data-storage
          mountPath: /data
      volumes:
      - name: data-storage
        persistentVolumeClaim:
          claimName: retail-etl-pvc

---
# service.yaml
apiVersion: v1
kind: Service
metadata:
  name: retail-etl-service
  namespace: retail-etl
spec:
  selector:
    app: retail-etl-app
  ports:
  - port: 80
    targetPort: 8000
  type: LoadBalancer
```

### Option 3: Traditional Server Deployment

```bash
# Setup systemd service
sudo tee /etc/systemd/system/retail-etl.service > /dev/null <<EOF
[Unit]
Description=Retail ETL Pipeline API
After=network.target

[Service]
Type=exec
User=app
Group=app
WorkingDirectory=/opt/retail-etl
Environment=PATH=/opt/retail-etl/.venv/bin
ExecStart=/opt/retail-etl/.venv/bin/uvicorn api.main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
sudo systemctl enable retail-etl
sudo systemctl start retail-etl
```

## Orchestration Deployment

### Dagster (Recommended)

```bash
# Start Dagster daemon and webserver
poetry run dagster dev --host 0.0.0.0 --port 3000

# Or with systemd services
sudo systemctl enable dagster-daemon
sudo systemctl enable dagster-webserver
```

### Apache Airflow (Alternative)

```bash
# Install Airflow separately (due to dependency conflicts)
pip install apache-airflow==2.10.4

# Initialize database
airflow db init

# Create admin user
airflow users create \\
    --username admin \\
    --firstname Admin \\
    --lastname User \\
    --role Admin \\
    --email admin@company.com

# Copy DAGs
cp -r src/airflow_dags/* $AIRFLOW_HOME/dags/

# Start services
airflow webserver --port 8080 &
airflow scheduler &
```

## Monitoring and Observability

### Health Checks

Configure health check endpoints:

```bash
# API health check
curl -f http://localhost:8000/health || exit 1

# External API health check
curl -f http://localhost:8000/api/v1/external-apis/health || exit 1

# Database health check
curl -f http://localhost:8000/health/database || exit 1
```

### Logging Configuration

Production logging setup:

```python
# In production environment
LOGGING_LEVEL=INFO
LOGGING_FORMAT=json
LOGGING_FILE=/var/log/retail-etl/app.log
LOGGING_MAX_SIZE=100MB
LOGGING_BACKUP_COUNT=5
```

### Metrics and Monitoring

Optional Prometheus metrics:

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'retail-etl'
    static_configs:
      - targets: ['localhost:8000']
    metrics_path: '/metrics'
```

## Backup and Recovery

### Database Backups

```bash
# Daily backup script
#!/bin/bash
BACKUP_DIR="/backups/postgres"
DATE=$(date +%Y%m%d_%H%M%S)
pg_dump $DATABASE_URL > "$BACKUP_DIR/retail_etl_$DATE.sql"

# Keep only last 30 days
find $BACKUP_DIR -name "retail_etl_*.sql" -mtime +30 -delete
```

### Data Backups

```bash
# Backup processed data
#!/bin/bash
BACKUP_DIR="/backups/data"
DATE=$(date +%Y%m%d)
tar -czf "$BACKUP_DIR/processed_data_$DATE.tar.gz" /data/gold/

# Sync to cloud storage (example with AWS S3)
aws s3 sync /data/gold/ s3://company-retail-etl-backups/gold/
```

## Security Considerations

### Network Security
- Use HTTPS/TLS for all external communications
- Implement proper firewall rules
- Use VPN for sensitive operations
- Enable database SSL/TLS connections

### Access Control
- Implement role-based access control (RBAC)
- Use strong authentication (consider SSO/OIDC)
- Regular access reviews and key rotation
- Audit logging for all data access

### Data Security
- Encrypt data at rest and in transit
- Implement proper data retention policies
- PII/sensitive data masking
- Regular security scanning

## Performance Optimization

### Spark Optimization

```python
# Production Spark configuration
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

### Database Optimization

```sql
-- Create indexes for common queries
CREATE INDEX CONCURRENTLY idx_fact_sales_date ON fact_sales(sale_date);
CREATE INDEX CONCURRENTLY idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX CONCURRENTLY idx_fact_sales_product ON fact_sales(product_key);

-- Analyze tables for query optimization
ANALYZE fact_sales;
ANALYZE dim_customer;
ANALYZE dim_product;
```

## Troubleshooting

### Common Issues

1. **Memory Issues**
   ```bash
   # Increase Spark memory allocation
   export SPARK_DRIVER_MEMORY=8g
   export SPARK_EXECUTOR_MEMORY=8g
   ```

2. **Permission Issues**
   ```bash
   # Fix data directory permissions
   sudo chown -R app:app /data/
   sudo chmod -R 755 /data/
   ```

3. **Database Connection Issues**
   ```bash
   # Test database connectivity
   psql $DATABASE_URL -c "SELECT 1;"
   ```

4. **External API Issues**
   ```bash
   # Test API connectivity
   curl -f "https://v6.exchangerate-api.com/v6/supported-codes"
   ```

### Log Analysis

```bash
# Application logs
tail -f /var/log/retail-etl/app.log

# Spark logs
tail -f /var/log/spark/spark-*.log

# System logs
journalctl -u retail-etl -f
```

## Maintenance

### Regular Tasks

1. **Daily**: Monitor system health and performance
2. **Weekly**: Review logs and error reports
3. **Monthly**: Security updates and dependency updates
4. **Quarterly**: Performance optimization review
5. **Semi-annually**: Security audit and penetration testing

### Updates

```bash
# Update dependencies
poetry update

# Database migrations
poetry run alembic upgrade head

# Restart services
sudo systemctl restart retail-etl
```

This deployment guide provides a comprehensive foundation for deploying the retail ETL pipeline to production with proper security, monitoring, and maintenance considerations.
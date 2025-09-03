# Deployment and Environment Setup Guide

## Overview

This guide provides comprehensive instructions for setting up and deploying the PwC Data Engineering Challenge platform across different environments (development, staging, production).

## System Requirements

### Minimum Hardware Requirements

#### Development Environment
- **CPU**: 4 cores (Intel i5 or equivalent)
- **RAM**: 8GB minimum, 16GB recommended
- **Storage**: 50GB SSD space
- **Network**: Stable internet connection

#### Staging Environment
- **CPU**: 8 cores
- **RAM**: 16GB minimum, 32GB recommended
- **Storage**: 100GB SSD space
- **Network**: 1Gbps connection

#### Production Environment
- **CPU**: 16+ cores
- **RAM**: 32GB minimum, 64GB recommended
- **Storage**: 500GB SSD space (with backup strategy)
- **Network**: 10Gbps connection with redundancy

### Software Requirements

#### Core Dependencies
- **Python**: 3.10.x (exact version required)
- **Java**: JDK 11 or 17 (for Spark)
- **Docker**: 20.10+ with Docker Compose
- **Git**: Latest version

#### Database Options
- **SQLite**: Development/testing (included)
- **PostgreSQL**: 14+ (staging/production)
- **Supabase**: Cloud PostgreSQL option

#### Additional Services
- **Redis**: 7.0+ (caching and rate limiting)
- **Kafka**: 3.0+ (event streaming)
- **RabbitMQ**: 3.11+ (task queuing)

## Installation Methods

### Method 1: Local Development Setup (Recommended for Development)

#### Step 1: Clone Repository
```bash
git clone https://github.com/Camilo555/PwC-Challenge-DataEngineer.git
cd PwC-Challenge-DataEngineer
```

#### Step 2: Install Python and Poetry
```bash
# Install Python 3.10 (using pyenv recommended)
pyenv install 3.10.12
pyenv local 3.10.12

# Install Poetry
curl -sSL https://install.python-poetry.org | python3 -
```

#### Step 3: Install Dependencies
```bash
# Install all dependencies
poetry install

# Activate virtual environment
poetry shell

# Verify installation
python --version  # Should show 3.10.x
```

#### Step 4: Environment Configuration
```bash
# Copy environment template
cp .env.template .env

# Edit environment variables
nano .env
```

**Required Environment Variables:**
```bash
# Application Settings
ENVIRONMENT=development
SECRET_KEY=your-secret-key-min-32-chars
BASIC_AUTH_PASSWORD=secure-password-min-12-chars

# Database Configuration
DATABASE_TYPE=sqlite
DATABASE_URL=sqlite:///./data/warehouse/retail.db

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000

# Security Configuration (optional in development)
SKIP_SECURITY_VALIDATION=true

# Monitoring (optional)
ENABLE_MONITORING=false
ENABLE_VECTOR_SEARCH=true
```

#### Step 5: Initialize Database and Data Directories
```bash
# Create data directories
mkdir -p data/{raw,bronze,silver,gold,warehouse,logs}

# Initialize database (if using SQLite)
python -c "from core.database.db_manager import create_database; create_database()"
```

#### Step 6: Start Development Server
```bash
# Start API server
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

# Alternative: Using poetry
poetry run uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

### Method 2: Docker Development Setup

#### Step 1: Docker Compose Setup
```bash
# Clone repository
git clone https://github.com/Camilo555/PwC-Challenge-DataEngineer.git
cd PwC-Challenge-DataEngineer

# Create environment file
cp .env.template .env
# Edit .env with appropriate values

# Build and start services
docker-compose up -d --build
```

#### Docker Compose Configuration
```yaml
# docker-compose.yml
version: '3.8'

services:
  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - ENVIRONMENT=development
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    depends_on:
      - postgres
      - redis

  postgres:
    image: postgres:14
    environment:
      POSTGRES_DB: retail_dwh
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

volumes:
  postgres_data:
```

### Method 3: Production Deployment

#### Prerequisites for Production
1. **SSL Certificate**: Valid SSL certificate for HTTPS
2. **Domain Name**: Configured DNS pointing to your server
3. **Firewall**: Properly configured firewall rules
4. **Backup Strategy**: Automated backup solution
5. **Monitoring**: Comprehensive monitoring setup

#### Production Environment Variables
```bash
# Production .env
ENVIRONMENT=production
SECRET_KEY=32-char-secure-random-key
BASIC_AUTH_PASSWORD=secure-production-password

# Database (PostgreSQL required)
DATABASE_TYPE=postgresql
DATABASE_URL=postgresql://user:password@localhost:5432/retail_dwh

# Security (all required in production)
HTTPS_ENABLED=true
JWT_AUTH_ENABLED=true
CORS_ALLOWED_ORIGINS=["https://yourdomain.com"]

# External Services
TYPESENSE_API_KEY=your-typesense-api-key
CURRENCY_API_KEY=your-currency-api-key

# Monitoring
ENABLE_MONITORING=true
DATADOG_API_KEY=your-datadog-api-key
```

#### Production Deployment Steps
```bash
# 1. Server preparation
sudo apt update && sudo apt upgrade -y
sudo apt install -y docker.io docker-compose nginx certbot

# 2. Clone and setup
git clone https://github.com/Camilo555/PwC-Challenge-DataEngineer.git
cd PwC-Challenge-DataEngineer

# 3. Production environment setup
cp .env.template .env
# Configure production values in .env

# 4. SSL certificate setup
sudo certbot --nginx -d yourdomain.com

# 5. Start production services
docker-compose -f docker-compose.prod.yml up -d

# 6. Verify deployment
curl https://yourdomain.com/health
```

## Environment-Specific Configurations

### Development Environment
```python
# config/development.py
class DevelopmentConfig:
    DEBUG = True
    TESTING = False
    LOG_LEVEL = "DEBUG"
    
    # Relaxed security for development
    SKIP_SECURITY_VALIDATION = True
    BASIC_AUTH_ENABLED = True
    JWT_AUTH_ENABLED = False
    
    # Local services
    DATABASE_URL = "sqlite:///./data/warehouse/retail.db"
    REDIS_URL = "redis://localhost:6379/0"
```

### Staging Environment
```python
# config/staging.py
class StagingConfig:
    DEBUG = False
    TESTING = True
    LOG_LEVEL = "INFO"
    
    # Enhanced security
    JWT_AUTH_ENABLED = True
    BASIC_AUTH_ENABLED = False
    
    # External services
    DATABASE_URL = os.getenv("DATABASE_URL")
    REDIS_URL = os.getenv("REDIS_URL")
    
    # Monitoring
    ENABLE_MONITORING = True
```

### Production Environment
```python
# config/production.py
class ProductionConfig:
    DEBUG = False
    TESTING = False
    LOG_LEVEL = "WARNING"
    
    # Maximum security
    HTTPS_ENABLED = True
    JWT_AUTH_ENABLED = True
    BASIC_AUTH_ENABLED = False
    
    # Enterprise features
    DLP_ENABLED = True
    COMPLIANCE_MONITORING = True
    REAL_TIME_MONITORING = True
    
    # External services (required)
    DATABASE_URL = os.getenv("DATABASE_URL")
    REDIS_URL = os.getenv("REDIS_URL")
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
```

## Database Setup

### SQLite Setup (Development Only)
```bash
# SQLite is automatically created
# Location: ./data/warehouse/retail.db
# No additional setup required
```

### PostgreSQL Setup

#### Local PostgreSQL
```bash
# Install PostgreSQL
sudo apt install postgresql postgresql-contrib

# Create database and user
sudo -u postgres psql
CREATE DATABASE retail_dwh;
CREATE USER retail_user WITH ENCRYPTED PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE retail_dwh TO retail_user;
\q

# Update connection string
DATABASE_URL=postgresql://retail_user:secure_password@localhost:5432/retail_dwh
```

#### Supabase Setup
```bash
# Create Supabase project at https://supabase.com
# Get your project URL and keys

# Environment variables
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-key
SUPABASE_SERVICE_KEY=your-service-key
DATABASE_TYPE=postgresql
DATABASE_URL=postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT].supabase.co:5432/postgres
```

### Database Migration
```bash
# Run database migrations
alembic upgrade head

# Create sample data (optional)
python scripts/create_sample_data.py
```

## External Services Setup

### Typesense (Vector Search)
```bash
# Local Typesense with Docker
docker run -d -p 8108:8108 -v typesense-data:/data \
  typesense/typesense:0.25.0 \
  --data-dir /data \
  --api-key=your-api-key \
  --enable-cors

# Environment variable
TYPESENSE_API_KEY=your-api-key
TYPESENSE_HOST=localhost
TYPESENSE_PORT=8108
```

### Redis (Caching & Rate Limiting)
```bash
# Local Redis
docker run -d -p 6379:6379 redis:7-alpine

# Or install locally
sudo apt install redis-server
sudo systemctl start redis-server

# Environment variable
REDIS_URL=redis://localhost:6379/0
```

### RabbitMQ (Task Queue)
```bash
# RabbitMQ with Docker
docker run -d -p 5672:5672 -p 15672:15672 \
  --name rabbitmq \
  -e RABBITMQ_DEFAULT_USER=admin \
  -e RABBITMQ_DEFAULT_PASS=password \
  rabbitmq:3-management

# Environment variables
RABBITMQ_URL=amqp://admin:password@localhost:5672
```

### Kafka (Event Streaming)
```bash
# Kafka with Docker Compose
# See docker-compose.kafka.yml for complete setup

# Environment variables
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_AUTO_CREATE_TOPICS=true
```

## Security Setup

### SSL/TLS Configuration (Production)
```nginx
# /etc/nginx/sites-available/pwc-api
server {
    listen 80;
    server_name yourdomain.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name yourdomain.com;
    
    ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;
    
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### Firewall Configuration
```bash
# UFW firewall setup
sudo ufw enable
sudo ufw allow ssh
sudo ufw allow 80
sudo ufw allow 443
sudo ufw allow from trusted_ip to any port 5432  # Database access
```

### Security Headers
```python
# Automatic security headers in production
SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "X-XSS-Protection": "1; mode=block",
    "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
    "Content-Security-Policy": "default-src 'self'"
}
```

## Monitoring Setup

### DataDog Integration
```bash
# DataDog agent installation
DD_AGENT_MAJOR_VERSION=7 DD_API_KEY=your-api-key \
DD_SITE="datadoghq.com" \
bash -c "$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script.sh)"

# Configure custom metrics
# See src/monitoring/datadog_custom_metrics_advanced.py
```

### Logging Configuration
```python
# Production logging setup
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'json': {
            'format': '{"time": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}'
        }
    },
    'handlers': {
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': '/var/log/pwc-api/app.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5,
            'formatter': 'json'
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['file']
    }
}
```

## Performance Tuning

### Application Tuning
```python
# uvicorn production configuration
uvicorn api.main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --access-log \
  --error-log
```

### Database Optimization
```sql
-- PostgreSQL optimization
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '16MB';
SELECT pg_reload_conf();
```

### Spark Configuration (ETL)
```python
spark_config = {
    "spark.executor.memory": "4g",
    "spark.driver.memory": "2g",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
}
```

## Backup and Recovery

### Database Backup
```bash
# Automated PostgreSQL backup
#!/bin/bash
# backup_db.sh
DB_NAME="retail_dwh"
BACKUP_DIR="/opt/backups"
DATE=$(date +%Y%m%d_%H%M%S)

pg_dump $DB_NAME > "$BACKUP_DIR/retail_dwh_$DATE.sql"
gzip "$BACKUP_DIR/retail_dwh_$DATE.sql"

# Keep only last 7 days
find $BACKUP_DIR -name "retail_dwh_*.sql.gz" -mtime +7 -delete
```

### Data Backup
```bash
# Delta Lake backup
#!/bin/bash
# backup_data.sh
DATA_DIR="/opt/data"
BACKUP_DIR="/opt/backups/data"
DATE=$(date +%Y%m%d_%H%M%S)

rsync -av --exclude='*.tmp' $DATA_DIR/ $BACKUP_DIR/$DATE/

# Keep only last 30 days
find $BACKUP_DIR -type d -mtime +30 -exec rm -rf {} +
```

## Health Checks and Monitoring

### Health Check Endpoints
```bash
# API health check
curl http://localhost:8000/health

# Detailed security status (requires auth)
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8000/api/v1/security/status
```

### System Monitoring
```bash
# Resource monitoring script
#!/bin/bash
# monitor_system.sh
echo "=== System Resources ==="
echo "CPU Usage: $(top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1)%"
echo "Memory Usage: $(free | grep Mem | awk '{printf("%.1f%%", $3/$2 * 100.0)}')"
echo "Disk Usage: $(df -h / | awk 'NR==2{print $5}')"
echo "Load Average: $(uptime | awk '{print $10 $11 $12}')"
```

## Troubleshooting

### Common Issues

#### 1. Port Already in Use
```bash
# Find and kill process using port 8000
sudo lsof -ti:8000 | xargs kill -9

# Or use different port
export API_PORT=8001
```

#### 2. Permission Denied Errors
```bash
# Fix data directory permissions
sudo chown -R $USER:$USER ./data
chmod -R 755 ./data
```

#### 3. Database Connection Issues
```bash
# Test PostgreSQL connection
psql -h localhost -U retail_user -d retail_dwh -c "SELECT version();"

# Check if PostgreSQL is running
sudo systemctl status postgresql
```

#### 4. Memory Issues
```bash
# Increase Docker memory limit
# In Docker Desktop: Settings > Resources > Memory > 8GB+

# Or reduce Spark memory usage
export SPARK_DRIVER_MEMORY=1g
export SPARK_EXECUTOR_MEMORY=2g
```

### Log Analysis
```bash
# Application logs
tail -f logs/app.log

# Docker logs
docker logs -f container_name

# System logs
journalctl -u your-service -f
```

## Deployment Checklist

### Pre-deployment
- [ ] All environment variables configured
- [ ] Database migrations completed
- [ ] SSL certificates valid and installed
- [ ] Security configurations reviewed
- [ ] Backup strategy implemented
- [ ] Monitoring configured
- [ ] Load testing completed

### Post-deployment
- [ ] Health checks passing
- [ ] Security assessment completed
- [ ] Performance monitoring active
- [ ] Backup verification
- [ ] Documentation updated
- [ ] Team notification sent

## Scaling Considerations

### Horizontal Scaling
- Load balancer configuration
- Database connection pooling
- Session state management
- Cache distribution

### Vertical Scaling
- Resource monitoring
- Performance bottleneck identification
- Hardware upgrade planning
- Cost optimization

## Support and Maintenance

### Regular Maintenance Tasks
- [ ] Security updates (weekly)
- [ ] Dependency updates (monthly)
- [ ] Performance optimization review (monthly)
- [ ] Backup verification (weekly)
- [ ] Log rotation (daily)
- [ ] Certificate renewal (as needed)

### Emergency Procedures
1. **System Down**: Follow incident response plan
2. **Data Corruption**: Restore from latest backup
3. **Security Breach**: Activate security response team
4. **Performance Issues**: Scale resources immediately

## Contact Information

**Technical Support**: support@pwc-retail.com  
**Security Issues**: security@pwc-retail.com  
**Documentation**: https://docs.pwc-retail.com
# PwC Data Engineering Platform - Deployment Guide

## 🚀 Overview

This guide provides comprehensive instructions for deploying the PwC Data Engineering Platform across different environments. The platform supports containerized deployment with Docker and Docker Compose, featuring enterprise-grade monitoring, caching, and scaling capabilities.

## 📋 Prerequisites

### System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| RAM | 8GB | 16GB+ |
| CPU | 4 cores | 8+ cores |
| Disk Space | 50GB | 100GB+ |
| Network | 100 Mbps | 1 Gbps |

### Software Dependencies

- **Docker**: 24.0+ with Docker Compose v2
- **Git**: For source code management
- **Bash**: For deployment scripts (Windows users: use WSL or Git Bash)
- **curl**: For health checks

### Optional Dependencies

- **DataDog API Key**: For enterprise monitoring
- **SSL Certificates**: For production HTTPS deployment

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Load Balancer (Nginx)                    │
├─────────────────────────────────────────────────────────────────┤
│  PwC Data Platform    │  Monitoring Stack  │  Message Queue     │
│  ┌─────────────────┐  │  ┌──────────────┐  │  ┌──────────────┐ │
│  │   Application   │  │  │  Grafana     │  │  │  RabbitMQ    │ │
│  │   (FastAPI)     │  │  │  Dashboard   │  │  │              │ │
│  └─────────────────┘  │  └──────────────┘  │  └──────────────┘ │
│  ┌─────────────────┐  │  ┌──────────────┐  │  ┌──────────────┐ │
│  │   Redis Cache   │  │  │  Prometheus  │  │  │    Kafka     │ │
│  │                 │  │  │              │  │  │  (Optional)  │ │
│  └─────────────────┘  │  └──────────────┘  │  └──────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│                    Spark Cluster (Optional)                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │  Spark Master   │  │  Spark Worker   │  │  Spark Worker   │ │
│  │                 │  │                 │  │                 │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## 🌍 Deployment Environments

### 1. Development Environment

**Purpose**: Local development and testing
**Features**: Hot reload, debug mode, simplified stack

```bash
# Quick development deployment
./deployment/deploy.sh -e development --skip-tests

# With specific worker count
./deployment/deploy.sh -e development -w 1
```

**Services**: Application, Redis, Prometheus, Grafana
**Resource Usage**: ~2GB RAM, 2 CPU cores

### 2. Staging Environment

**Purpose**: Pre-production testing and validation
**Features**: Production-like setup with monitoring

```bash
# Staging deployment
./deployment/deploy.sh -e staging -v v1.2.3

# Clean staging deployment
./deployment/deploy.sh -e staging -c --datadog-key=your_key_here
```

**Services**: Full stack with reduced resources
**Resource Usage**: ~6GB RAM, 4 CPU cores

### 3. Production Environment

**Purpose**: Live production workloads
**Features**: Full enterprise stack, monitoring, scaling

```bash
# Production deployment
./deployment/deploy.sh -e production -v v1.2.3 -w 3 --datadog-key=your_key_here

# Production with custom registry
./deployment/deploy.sh -e production -r registry.company.com -v v1.2.3
```

**Services**: Complete platform with all components
**Resource Usage**: ~12GB RAM, 8+ CPU cores

## 🚢 Deployment Methods

### Method 1: Automated Deployment Script

The recommended approach using our deployment script:

```bash
# Clone repository
git clone <repository_url>
cd PwC-Challenge-DataEngineer

# Make script executable (Linux/Mac)
chmod +x deployment/deploy.sh

# Deploy to production
./deployment/deploy.sh -e production -v latest --datadog-key=YOUR_KEY
```

### Method 2: Manual Docker Compose

For custom deployments or troubleshooting:

```bash
# Set environment variables
export DD_API_KEY=your_datadog_key
export IMAGE_TAG=pwc-data-platform:latest

# Build images
docker build -f deployment/Dockerfile -t pwc-data-platform:latest .

# Deploy services
docker-compose -f deployment/docker-compose.production.yml up -d

# Scale workers
docker-compose -f deployment/docker-compose.production.yml up -d --scale spark-worker=3
```

### Method 3: Kubernetes (Advanced)

For Kubernetes deployment, see [Kubernetes Deployment Guide](./k8s/README.md).

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `ENV` | Environment name | production | No |
| `DATABASE_URL` | Database connection | sqlite:///./data/warehouse/retail.db | No |
| `REDIS_URL` | Redis connection | redis://redis:6379 | No |
| `RABBITMQ_URL` | RabbitMQ connection | amqp://guest:guest@rabbitmq:5672// | No |
| `DD_API_KEY` | DataDog API key | - | No |
| `LOG_LEVEL` | Logging level | INFO | No |
| `ENABLE_MONITORING` | Enable monitoring | true | No |
| `ENABLE_CACHING` | Enable Redis caching | true | No |
| `PROCESSING_ENGINE` | Data processing engine | spark | No |

### Service Configuration

#### Application (FastAPI)
- **Port**: 8000
- **Health Check**: `/health`
- **Metrics**: `/metrics`
- **Documentation**: `/docs`

#### Monitoring Stack
- **Grafana**: http://localhost:3000 (admin/admin123)
- **Prometheus**: http://localhost:9090
- **Alert Manager**: http://localhost:9093

#### Message Queue
- **RabbitMQ Management**: http://localhost:15672 (admin/admin123)
- **Kafka UI**: http://localhost:8080 (if enabled)

#### Big Data Processing
- **Spark Master UI**: http://localhost:8080
- **Spark History Server**: http://localhost:18080

## 🔍 Health Checks & Monitoring

### Application Health

```bash
# Check application health
curl http://localhost:8000/health

# Check detailed metrics
curl http://localhost:8000/metrics

# View application logs
docker-compose logs -f pipeline
```

### Service Status

```bash
# Check all services
docker-compose ps

# Check specific service logs
docker-compose logs -f redis
docker-compose logs -f rabbitmq
docker-compose logs -f prometheus
```

### Performance Monitoring

1. **Grafana Dashboards**: http://localhost:3000
   - Application Performance
   - Infrastructure Metrics
   - Data Pipeline Status
   - Business Metrics

2. **Prometheus Metrics**: http://localhost:9090
   - Query custom metrics
   - Set up alerting rules
   - Monitor resource usage

3. **DataDog Integration** (if configured):
   - APM traces
   - Log aggregation
   - Custom dashboards
   - Alert notifications

## 🔧 Maintenance & Operations

### Backup Procedures

```bash
# Backup database
docker exec pwc-pipeline sqlite3 /app/data/warehouse/retail.db ".backup /app/data/backup/retail_$(date +%Y%m%d_%H%M%S).db"

# Backup configuration
tar -czf config_backup_$(date +%Y%m%d).tar.gz config/

# Backup logs
tar -czf logs_backup_$(date +%Y%m%d).tar.gz logs/
```

### Update Deployment

```bash
# Update to new version
./deployment/deploy.sh -e production -v v1.3.0 --datadog-key=YOUR_KEY

# Rolling update (zero downtime)
docker-compose up -d --no-deps pipeline
```

### Scaling Operations

```bash
# Scale Spark workers
docker-compose up -d --scale spark-worker=5

# Scale application instances (with load balancer)
docker-compose up -d --scale pipeline=3
```

### Log Management

```bash
# View recent logs
docker-compose logs --tail=100 -f

# Export logs
docker-compose logs --no-color > platform_logs_$(date +%Y%m%d).log

# Clean up old logs
docker system prune -f --volumes
```

## 🚨 Troubleshooting

### Common Issues

#### 1. Port Conflicts
```bash
# Check what's using ports
netstat -tulpn | grep :8000
netstat -tulpn | grep :3000

# Stop conflicting services
sudo systemctl stop apache2
sudo systemctl stop nginx
```

#### 2. Memory Issues
```bash
# Check memory usage
docker stats

# Increase Docker memory limit
# Docker Desktop: Settings > Resources > Memory
```

#### 3. Permission Issues
```bash
# Fix file permissions
sudo chown -R $USER:$USER data/ logs/

# Fix Docker permissions (Linux)
sudo usermod -aG docker $USER
```

#### 4. Database Connection Issues
```bash
# Check database file
ls -la data/warehouse/

# Test database connection
docker exec pwc-pipeline sqlite3 /app/data/warehouse/retail.db ".tables"
```

### Service-Specific Troubleshooting

#### Redis Issues
```bash
# Check Redis connectivity
docker exec pwc-redis redis-cli ping

# View Redis logs
docker-compose logs redis

# Flush Redis cache
docker exec pwc-redis redis-cli flushall
```

#### RabbitMQ Issues
```bash
# Check RabbitMQ status
docker exec pwc-rabbitmq rabbitmqctl status

# View queues
docker exec pwc-rabbitmq rabbitmqctl list_queues

# Reset RabbitMQ
docker-compose restart rabbitmq
```

#### Monitoring Issues
```bash
# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# Restart monitoring stack
docker-compose restart prometheus grafana
```

## 🔐 Security Considerations

### Production Security Checklist

- [ ] Change default passwords
- [ ] Enable HTTPS with valid SSL certificates
- [ ] Configure firewall rules
- [ ] Set up VPN access for monitoring interfaces
- [ ] Enable authentication for all admin interfaces
- [ ] Regular security updates
- [ ] Monitor access logs
- [ ] Set up intrusion detection

### Network Security

```bash
# Configure firewall (Linux)
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw deny 3000/tcp  # Restrict Grafana access
sudo ufw deny 9090/tcp  # Restrict Prometheus access
```

### SSL/TLS Configuration

1. Obtain SSL certificates (Let's Encrypt recommended)
2. Update `deployment/nginx/nginx.conf`
3. Uncomment HTTPS server block
4. Restart Nginx service

## 📊 Performance Optimization

### Database Optimization
- Enable connection pooling
- Configure query caching
- Monitor slow queries
- Regular database maintenance

### Application Optimization
- Enable Redis caching
- Configure appropriate worker counts
- Monitor memory usage
- Tune garbage collection

### Infrastructure Optimization
- Use SSD storage
- Configure appropriate resource limits
- Monitor network latency
- Implement CDN for static assets

## 🆘 Support & Escalation

### Log Collection
```bash
# Collect all logs for support
./deployment/collect_logs.sh

# Generate diagnostic report
./deployment/diagnostic_report.sh
```

### Emergency Procedures
1. **Service Outage**: Run health checks, restart services
2. **Data Loss**: Restore from backup
3. **Security Breach**: Isolate system, review logs
4. **Performance Issues**: Check resources, scale services

### Contact Information
- **Technical Support**: engineering-team@company.com
- **Emergency Escalation**: +1-xxx-xxx-xxxx
- **Documentation**: https://docs.company.com/data-platform

---

## 📚 Additional Resources

- [API Documentation](../docs/api/)
- [Architecture Guide](../docs/architecture/)
- [Development Guide](../docs/development/)
- [Security Guide](../docs/security/)
- [Monitoring Guide](../docs/monitoring/)

---

*Last updated: $(date)*
*Version: v1.0.0*
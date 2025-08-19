# CI/CD Pipeline Documentation

## Overview

This document describes the comprehensive CI/CD pipeline implementation for the PwC Data Engineering Challenge project. The pipeline includes automated testing, security scanning, deployment, and monitoring workflows.

## 🚀 Workflow Overview

### 1. **Continuous Integration (`ci.yml`)**
**Trigger:** Push/PR to main/develop branches
**Purpose:** Core testing, linting, and quality assurance

#### Features:
- **Multi-Python Version Testing** (3.10, 3.11)
- **Enhanced Environment Setup** with test configurations
- **Comprehensive Testing Suite**:
  - Core imports and configuration validation
  - Airflow DAG syntax checking
  - Supabase client initialization testing
  - Pytest with coverage reporting
- **Code Quality Checks**:
  - Ruff linting and formatting
  - MyPy type checking
  - Security scanning with Safety and Bandit
- **Dependency Vulnerability Scanning**
- **Coverage Reporting** with Codecov integration

#### Environment Variables:
```yaml
ENVIRONMENT: test
DATABASE_TYPE: sqlite
DATABASE_URL: sqlite:///./test.db
API_PORT: 8000
ENABLE_EXTERNAL_ENRICHMENT: false
ENABLE_VECTOR_SEARCH: false
```

### 2. **Docker Build & Test (`docker.yml`)**
**Trigger:** Push/PR to main/develop + releases
**Purpose:** Container building, testing, and security scanning

#### Features:
- **Multi-Architecture Builds** (AMD64, ARM64)
- **Multi-Service Support** (API, ETL containers)
- **Automated Container Testing** with Typesense integration
- **Security Scanning** with Trivy
- **Container Registry** integration (GitHub Packages)
- **Layer Caching** optimization

#### Services Tested:
- API container with health checks
- ETL container functionality
- Typesense search integration
- Database connectivity

### 3. **Integration Tests (`integration-tests.yml`)**
**Trigger:** Push/PR + daily schedule (2 AM UTC)
**Purpose:** End-to-end testing of all system components

#### Test Suites:
- **API Integration Tests**:
  - Full API server startup and health checks
  - Authentication and authorization
  - Database connection and schema creation
  - Supabase integration testing
- **Airflow DAG Validation**:
  - DAG syntax and import validation
  - Configuration loading tests
  - Dependency validation
- **Data Quality Tests**:
  - Quality scoring algorithms
  - Issue detection and reporting
  - Threshold validation
- **Performance Tests**:
  - Large dataset processing (10K records)
  - Response time benchmarking
  - Memory usage optimization
  - I/O performance validation

#### Service Dependencies:
- PostgreSQL database
- Typesense search engine
- Mock external APIs

### 4. **Deployment (`deployment.yml`)**
**Trigger:** Releases + manual workflow dispatch
**Purpose:** Production deployment with rollback capabilities

#### Deployment Stages:
- **Pre-deployment Validation**
- **Environment-specific Deployments** (Staging/Production)
- **Database Migration Management**
- **Airflow DAG Deployment**
- **Post-deployment Verification**

#### Environments:
- **Staging**: `staging.pwc-retail-etl.com`
- **Production**: `pwc-retail-etl.com`

#### Safety Features:
- Blue-green deployment strategy
- Automatic rollback on failure
- Smoke test validation
- Deployment status tracking

### 5. **Monitoring & Health Checks (`monitoring.yml`)**
**Trigger:** Hourly during business hours + daily full checks
**Purpose:** Continuous system monitoring and alerting

#### Monitoring Components:
- **Health Checks**: API endpoints, database connectivity, Airflow DAGs
- **Performance Monitoring**: Response times, resource usage, throughput
- **Data Quality Monitoring**: Automated quality scoring and validation
- **Security Monitoring**: SSL/TLS, authentication, vulnerability scanning
- **Alerting System**: Slack/email notifications, escalation procedures

### 6. **Claude Code Integration (`claude-code.yml`)**
**Trigger:** Push/PR to main/develop
**Purpose:** AI-powered code analysis and quality assessment

#### Features:
- Automated code review with AI insights
- Code quality recommendations
- Architecture analysis
- Best practices validation

## 🔧 Configuration Requirements

### Required Secrets

#### Production Deployment:
```bash
# Database Configuration
DATABASE_URL_STAGING=postgresql://user:pass@staging-db.supabase.co:5432/postgres
DATABASE_URL_PRODUCTION=postgresql://user:pass@prod-db.supabase.co:5432/postgres

# API Authentication
API_USERNAME=admin
API_PASSWORD=secure_password_here

# Claude Code Integration
CLAUDE_CODE_OAUTH_TOKEN=your_claude_code_token

# Container Registry
GITHUB_TOKEN=automatically_provided

# Monitoring & Alerting (Optional)
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
PAGERDUTY_API_KEY=your_pagerduty_key
```

### Environment-Specific Variables

#### Staging Environment:
```bash
ENVIRONMENT=staging
DATABASE_URL=postgresql://staging_db_connection
API_HOST=staging.pwc-retail-etl.com
ENABLE_DEBUG_LOGGING=true
```

#### Production Environment:
```bash
ENVIRONMENT=production
DATABASE_URL=postgresql://production_db_connection
API_HOST=pwc-retail-etl.com
ENABLE_DEBUG_LOGGING=false
ENABLE_MONITORING=true
```

## 🚦 Pipeline Flow

### Pull Request Flow:
```
Code Push → CI Tests → Docker Build → Integration Tests → Claude Code Review → Merge Approval
```

### Release Flow:
```
Tag Creation → Full CI/CD → Docker Build → Security Scan → Deploy Staging → Deploy Production → Monitoring
```

### Daily Flow:
```
Scheduled Trigger → Full Integration Tests → Performance Monitoring → Data Quality Checks → Alert Summary
```

## 📊 Quality Gates

### CI Pipeline Gates:
1. **Code Quality**: Ruff linting must pass
2. **Type Safety**: MyPy validation (warnings allowed)
3. **Security**: No high-severity vulnerabilities
4. **Test Coverage**: Minimum 80% coverage maintained
5. **Integration**: All core components must initialize

### Deployment Gates:
1. **All CI Checks**: Must pass completely
2. **Container Security**: Trivy scan passes
3. **Integration Tests**: Full suite passes
4. **Performance**: Response times within thresholds
5. **Manual Approval**: Required for production

### Monitoring Gates:
1. **Health Check**: All endpoints respond (< 2s)
2. **Performance**: P95 response time < 1000ms
3. **Data Quality**: Overall score > 85%
4. **Security**: SSL/TLS valid, no security alerts

## 🔍 Troubleshooting Guide

### Common Issues:

#### 1. **CI Test Failures**
```bash
# Check test logs
git checkout <branch>
poetry install --with dev
export PYTHONPATH=src:$PYTHONPATH
poetry run pytest tests/ -v

# Fix common issues:
# - Missing environment variables
# - Import path issues
# - Test data dependencies
```

#### 2. **Docker Build Failures**
```bash
# Test locally
docker build -f docker/Dockerfile.api -t test-api .
docker run --rm test-api

# Common fixes:
# - Update base image versions
# - Fix dependency conflicts
# - Check file paths in COPY commands
```

#### 3. **Integration Test Failures**
```bash
# Run integration tests locally
docker-compose -f docker-compose.test.yml up -d
# Wait for services to be ready
poetry run pytest tests/integration/ -v

# Common fixes:
# - Service startup timing
# - Port conflicts
# - Database permissions
```

#### 4. **Deployment Failures**
```bash
# Check deployment status
kubectl get deployments
kubectl logs -l app=pwc-retail-api

# Rollback if needed
kubectl rollout undo deployment/pwc-retail-api
```

### Monitoring and Alerts:

#### Health Check Failures:
- **Check**: Service logs and metrics
- **Action**: Restart failed services
- **Escalate**: If multiple services affected

#### Performance Degradation:
- **Check**: Database query performance
- **Action**: Scale resources if needed
- **Monitor**: Response time trends

#### Data Quality Issues:
- **Check**: ETL pipeline logs
- **Action**: Review data sources
- **Alert**: Data team for investigation

## 📈 Performance Benchmarks

### Expected Performance Metrics:

| Component | Metric | Target | Alert Threshold |
|-----------|--------|--------|-----------------|
| API Response | P95 | < 500ms | > 1000ms |
| Data Processing | 10K records | < 15s | > 30s |
| Health Checks | All endpoints | < 2s | > 5s |
| Integration Tests | Full suite | < 10min | > 20min |
| Docker Build | Multi-arch | < 5min | > 15min |

### Resource Usage:
- **Memory**: API < 512MB, ETL < 2GB
- **CPU**: Average < 50%, Peak < 80%
- **Disk I/O**: < 100MB/s sustained
- **Network**: < 50 concurrent connections

## 🔄 Maintenance

### Weekly Tasks:
- Review monitoring alerts and trends
- Update dependency vulnerabilities
- Check performance metrics
- Validate backup procedures

### Monthly Tasks:
- Update base images and dependencies
- Review and optimize workflow performance
- Security audit and penetration testing
- Disaster recovery testing

### Quarterly Tasks:
- Full architecture review
- Capacity planning and scaling
- Technology stack evaluation
- Team training and documentation updates

## 📚 Additional Resources

### Documentation:
- [GitHub Actions Documentation](https://docs.github.com/actions)
- [Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/)
- [Airflow Operations](https://airflow.apache.org/docs/apache-airflow/stable/concepts/index.html)
- [Supabase Management](https://supabase.com/docs/guides/platform)

### Monitoring Tools:
- **Logs**: GitHub Actions logs, Container logs
- **Metrics**: Prometheus/Grafana (if configured)
- **Alerts**: Slack, Email, PagerDuty
- **Status Page**: For public service status

### Support:
- **Development Team**: For code-related issues
- **DevOps Team**: For infrastructure and deployment
- **Data Team**: For data quality and pipeline issues
- **Security Team**: For vulnerability and security concerns

---

**Last Updated**: $(date +'%Y-%m-%d')
**Pipeline Version**: 2.0
**Maintained By**: PwC Data Engineering Team
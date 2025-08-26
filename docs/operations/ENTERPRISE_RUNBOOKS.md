# Enterprise Operations Runbooks

---
title: Enterprise Operations Runbooks
description: Comprehensive operational procedures for the PwC Enterprise Data Platform
audience: [operators, sre, support, on-call]
last_updated: 2025-01-25
version: 2.0.0
owner: SRE Team
reviewers: [Platform Team, Security Team]
tags: [operations, runbooks, procedures, incident-response, troubleshooting]
---

## Table of Contents

1. [Runbook Overview](#runbook-overview)
2. [Incident Response Procedures](#incident-response-procedures)
3. [System Health Checks](#system-health-checks)
4. [Service Recovery Procedures](#service-recovery-procedures)
5. [Performance Troubleshooting](#performance-troubleshooting)
6. [Database Operations](#database-operations)
7. [Deployment Procedures](#deployment-procedures)
8. [Security Incident Response](#security-incident-response)
9. [Disaster Recovery](#disaster-recovery)
10. [Escalation Procedures](#escalation-procedures)

## Runbook Overview

### Purpose
This document provides step-by-step operational procedures for the PwC Enterprise Data Platform. These runbooks are designed to enable rapid incident response and consistent operational procedures.

### How to Use This Document
- Each runbook follows a standard format: Context, Symptoms, Investigation, Resolution, Prevention
- Time estimates are provided for each procedure
- All commands include safety checks and verification steps
- Escalation procedures are clearly defined

### Emergency Contacts
```
Critical Issues (P1): +1-555-ONCALL-1 or critical-alerts@pwc.pagerduty.com
Platform Team: platform-team@pwc.com
Security Team: security-incident@pwc.com  
Database Team: database-team@pwc.com
Network Operations: network-ops@pwc.com
```

## Incident Response Procedures

### P1: Service Completely Down

**Context:** Complete service outage affecting all users
**Expected Response Time:** 15 minutes
**Severity:** Critical

#### Symptoms
- Health check endpoints returning 503/504 errors
- No successful API responses
- Zero active connections
- Monitoring dashboards showing all instances down

#### Investigation Steps

1. **Check Service Status** (2 minutes)
```bash
# Check service health across all instances
curl -f https://api.pwc-data.com/api/v1/monitoring/health
curl -f https://api-backup.pwc-data.com/api/v1/monitoring/health

# Check load balancer status
aws elbv2 describe-target-health --target-group-arn $TARGET_GROUP_ARN

# Check service registry
curl -H "Authorization: Bearer $SERVICE_TOKEN" \
  https://api.pwc-data.com/api/v1/enterprise/services/sales-service
```

2. **Check Infrastructure** (3 minutes)
```bash
# Check Kubernetes pods
kubectl get pods -n production -l app=sales-service
kubectl describe pods -n production -l app=sales-service

# Check recent deployments
kubectl rollout history deployment/sales-service -n production

# Check resource usage
kubectl top pods -n production -l app=sales-service
```

3. **Check Dependencies** (2 minutes)
```bash
# Check database connectivity
pg_isready -h $DB_HOST -p $DB_PORT -U $DB_USER

# Check message queue
rabbitmq-diagnostics -q ping

# Check cache
redis-cli -h $REDIS_HOST ping

# Check external API dependencies
curl -f https://external-api.example.com/health
```

#### Resolution Steps

**Option A: Quick Restart** (5 minutes)
```bash
# If infrastructure is healthy, restart services
kubectl rollout restart deployment/sales-service -n production

# Wait for rollout to complete
kubectl rollout status deployment/sales-service -n production --timeout=300s

# Verify service health
curl -f https://api.pwc-data.com/health
```

**Option B: Scale Up New Instances** (3 minutes)
```bash
# Scale up additional instances
kubectl scale deployment/sales-service --replicas=6 -n production

# Wait for new instances to be ready
kubectl wait --for=condition=ready pod -l app=sales-service -n production --timeout=180s
```

**Option C: Rollback Deployment** (4 minutes)
```bash
# If recent deployment caused the issue
kubectl rollout undo deployment/sales-service -n production

# Monitor rollback
kubectl rollout status deployment/sales-service -n production --timeout=300s
```

#### Verification
```bash
# Verify service is responding
curl -f https://api.pwc-data.com/api/v1/sales/health

# Check error rates in last 5 minutes
curl -H "Authorization: Bearer $TOKEN" \
  "https://api.pwc-data.com/api/v1/monitoring/metrics/timeseries/api_error_rate?hours=1"

# Verify all instances are healthy
kubectl get pods -n production -l app=sales-service
```

#### Communication Template
```
INCIDENT UPDATE - P1 Service Outage

Status: [INVESTIGATING/IDENTIFIED/MONITORING/RESOLVED]
Start Time: [UTC timestamp]
Services Affected: [Service names]
User Impact: [Description]

Current Status:
- [Status description]
- ETA for resolution: [Time estimate]

Next Update: [Time for next update]

Incident Commander: [Name]
Communication Channel: #incident-[number]
```

### P2: High Error Rate

**Context:** Service responding but with elevated error rate (>5%)
**Expected Response Time:** 30 minutes
**Severity:** High

#### Symptoms
- API error rate above threshold (5%)
- Increased 5xx status codes
- User complaints about failures
- Alert: "High API Error Rate"

#### Investigation Steps

1. **Analyze Error Patterns** (5 minutes)
```bash
# Get recent error breakdown
curl -H "Authorization: Bearer $TOKEN" \
  "https://api.pwc-data.com/api/v1/monitoring/insights/error-analysis?hours=1"

# Check specific endpoints
curl -H "Authorization: Bearer $TOKEN" \
  "https://api.pwc-data.com/api/v1/monitoring/metrics?metric_names=api_error_rate&hours=2"

# Get error logs
kubectl logs -n production -l app=sales-service --since=30m | grep ERROR | tail -50
```

2. **Check System Resources** (3 minutes)
```bash
# Check CPU and memory usage
kubectl top pods -n production -l app=sales-service

# Check database performance
psql -h $DB_HOST -c "SELECT * FROM pg_stat_activity WHERE state = 'active';"

# Check connection pools
kubectl logs -n production -l app=sales-service | grep "connection pool" | tail -20
```

3. **Check Dependencies** (2 minutes)
```bash
# Check external API status
curl -w "%{http_code}" https://external-api.example.com/status

# Check message queue health
rabbitmq-diagnostics check_running

# Check cache hit rates
redis-cli info stats | grep keyspace_hits
```

#### Resolution Steps

**Option A: Scale Resources** (10 minutes)
```bash
# Increase CPU/memory limits if resource constrained
kubectl patch deployment sales-service -n production -p \
  '{"spec":{"template":{"spec":{"containers":[{"name":"sales-service","resources":{"limits":{"cpu":"1000m","memory":"2Gi"}}}]}}}}'

# Scale horizontally if needed
kubectl scale deployment/sales-service --replicas=8 -n production
```

**Option B: Database Optimization** (15 minutes)
```bash
# Check for long-running queries
psql -h $DB_HOST -c "SELECT query, state, query_start FROM pg_stat_activity WHERE state = 'active' AND query_start < NOW() - INTERVAL '30 seconds';"

# Kill problematic queries if safe
psql -h $DB_HOST -c "SELECT pg_terminate_backend([pid]);"

# Check connection count
psql -h $DB_HOST -c "SELECT count(*) FROM pg_stat_activity;"
```

**Option C: Circuit Breaker Reset** (2 minutes)
```bash
# Reset circuit breakers if they're preventing recovery
curl -X POST -H "Authorization: Bearer $SERVICE_TOKEN" \
  https://api.pwc-data.com/api/v1/enterprise/circuit-breakers/reset/external-service
```

### P3: High Response Time

**Context:** API response time above SLA (>1000ms for 95th percentile)
**Expected Response Time:** 1 hour
**Severity:** Medium

#### Investigation Steps

1. **Identify Slow Endpoints** (10 minutes)
```bash
# Get response time breakdown by endpoint
curl -H "Authorization: Bearer $TOKEN" \
  "https://api.pwc-data.com/api/v1/monitoring/metrics/timeseries/api_response_time_p95?hours=2"

# Check database query performance
psql -h $DB_HOST -c "SELECT query, mean_time, calls FROM pg_stat_statements ORDER BY mean_time DESC LIMIT 10;"

# Check for memory pressure
kubectl top pods -n production -l app=sales-service
```

2. **Analyze Database Performance** (10 minutes)
```bash
# Check for blocking queries
psql -h $DB_HOST -c "SELECT * FROM pg_locks WHERE NOT granted;"

# Check index usage
psql -h $DB_HOST -c "SELECT schemaname, tablename, attname, n_distinct, correlation FROM pg_stats WHERE schemaname = 'public' ORDER BY n_distinct DESC;"

# Check cache hit ratios
psql -h $DB_HOST -c "SELECT datname, blks_hit, blks_read, round(blks_hit*100.0/(blks_hit+blks_read), 2) AS hit_ratio FROM pg_stat_database WHERE datname = 'sales_db';"
```

#### Resolution Steps

**Option A: Database Optimization** (30 minutes)
```bash
# Analyze and create missing indexes
psql -h $DB_HOST -d sales_db -c "SELECT * FROM pg_stat_user_tables WHERE n_tup_ins + n_tup_upd + n_tup_del > 0 ORDER BY seq_scan DESC;"

# Update table statistics
psql -h $DB_HOST -d sales_db -c "ANALYZE;"

# Consider connection pooling optimization
kubectl edit configmap sales-service-config -n production
# Update database connection pool settings
```

**Option B: Caching Implementation** (20 minutes)
```bash
# Enable Redis caching for slow queries
kubectl edit deployment sales-service -n production
# Add Redis cache configuration

# Verify cache configuration
redis-cli config get "maxmemory*"
```

## System Health Checks

### Daily Health Check Routine

**Frequency:** Daily at 9:00 AM UTC
**Duration:** 15 minutes
**Responsible:** On-call SRE

#### Checklist

1. **Overall System Status** (2 minutes)
```bash
# Check high-level system health
curl https://api.pwc-data.com/api/v1/monitoring/health

# Get SLO compliance status
curl -H "Authorization: Bearer $TOKEN" \
  https://api.pwc-data.com/api/v1/monitoring/dashboard/overview/data
```

2. **Service Health** (5 minutes)
```bash
# Check all critical services
for service in sales-service analytics-service datamart-service; do
  echo "Checking $service..."
  kubectl get pods -n production -l app=$service
  curl -f https://api.pwc-data.com/api/v1/enterprise/services/$service
done
```

3. **Resource Utilization** (3 minutes)
```bash
# Check cluster resources
kubectl top nodes
kubectl top pods -n production --sort-by=cpu

# Check database connections
psql -h $DB_HOST -c "SELECT count(*) as active_connections FROM pg_stat_activity WHERE state = 'active';"
```

4. **Error Rates** (2 minutes)
```bash
# Check 24-hour error rates
curl -H "Authorization: Bearer $TOKEN" \
  "https://api.pwc-data.com/api/v1/monitoring/metrics?metric_names=api_error_rate&hours=24&aggregation=avg"
```

5. **Alert Status** (3 minutes)
```bash
# Check active alerts
curl -H "Authorization: Bearer $TOKEN" \
  https://api.pwc-data.com/api/v1/monitoring/alerts?active_only=true

# Review resolved alerts from last 24 hours
curl -H "Authorization: Bearer $TOKEN" \
  "https://api.pwc-data.com/api/v1/monitoring/alerts?resolved_since=24h"
```

#### Health Check Report Template
```
DAILY HEALTH CHECK REPORT
Date: [YYYY-MM-DD]
Operator: [Name]

SYSTEM OVERVIEW
- Overall Status: [GREEN/YELLOW/RED]
- SLO Compliance: [X.X%]
- Active Alerts: [X critical, X error, X warning]

SERVICE STATUS
- sales-service: [X/X pods healthy, avg response time: XXXms]
- analytics-service: [X/X pods healthy, avg response time: XXXms] 
- datamart-service: [X/X pods healthy, avg response time: XXXms]

RESOURCE UTILIZATION
- CPU Usage: [XX%] 
- Memory Usage: [XX%]
- Database Connections: [XX/XX]

ISSUES IDENTIFIED
- [List any issues found]
- [Include severity and planned actions]

ACTIONS TAKEN
- [List any corrective actions]

Next Check: [Next daily check time]
```

## Database Operations

### Database Maintenance Runbook

**Context:** Regular database maintenance tasks
**Frequency:** Weekly (Sundays 2:00 AM UTC)
**Duration:** 45 minutes

#### Pre-Maintenance Checks (5 minutes)
```bash
# Verify backup status
pg_dump --version
pg_basebackup --help

# Check database size and growth
psql -h $DB_HOST -c "SELECT pg_database.datname, pg_size_pretty(pg_database_size(pg_database.datname)) AS size FROM pg_database;"

# Check replication status
psql -h $DB_HOST -c "SELECT * FROM pg_stat_replication;"
```

#### Maintenance Tasks (35 minutes)

1. **Database Statistics Update** (10 minutes)
```bash
# Update query planner statistics
psql -h $DB_HOST -d sales_db -c "ANALYZE VERBOSE;"

# Check for unused indexes
psql -h $DB_HOST -d sales_db -c "SELECT schemaname, tablename, indexname, idx_tup_read, idx_tup_fetch FROM pg_stat_user_indexes WHERE idx_tup_read = 0 AND idx_tup_fetch = 0 ORDER BY schemaname, tablename;"
```

2. **Vacuum and Reindex** (20 minutes)
```bash
# Vacuum analyze critical tables
for table in sales transactions customers products; do
  psql -h $DB_HOST -d sales_db -c "VACUUM ANALYZE $table;" 
done

# Reindex if needed (during low-traffic period)
psql -h $DB_HOST -d sales_db -c "REINDEX INDEX CONCURRENTLY idx_sales_date;"
```

3. **Connection Cleanup** (5 minutes)
```bash
# Check for idle connections
psql -h $DB_HOST -c "SELECT count(*) FROM pg_stat_activity WHERE state = 'idle' AND query_start < now() - interval '1 hour';"

# Terminate old idle connections if necessary
psql -h $DB_HOST -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle' AND query_start < now() - interval '2 hours';"
```

#### Post-Maintenance Verification (5 minutes)
```bash
# Verify database health
psql -h $DB_HOST -c "SELECT version();"
psql -h $DB_HOST -c "SELECT pg_is_in_recovery();"

# Check performance impact
curl -H "Authorization: Bearer $TOKEN" \
  "https://api.pwc-data.com/api/v1/monitoring/metrics/timeseries/database_response_time?hours=1"

# Verify application connectivity
curl -f https://api.pwc-data.com/api/v1/sales/health
```

## Deployment Procedures

### Standard Application Deployment

**Context:** Deploying new application versions
**Duration:** 20 minutes
**Risk Level:** Medium

#### Pre-Deployment Checklist (5 minutes)
```bash
# Verify staging deployment success
kubectl get pods -n staging -l app=sales-service
curl -f https://api-staging.pwc-data.com/health

# Check current production status
kubectl get pods -n production -l app=sales-service
curl -f https://api.pwc-data.com/api/v1/monitoring/health

# Verify database migrations (if any)
kubectl logs -n staging deployment/sales-service | grep migration
```

#### Deployment Steps (10 minutes)

1. **Deploy with Rolling Update** (5 minutes)
```bash
# Set new image version
kubectl set image deployment/sales-service sales-service=pwc/sales-service:v2.1.1 -n production

# Monitor rollout progress
kubectl rollout status deployment/sales-service -n production --timeout=300s

# Verify pod status
kubectl get pods -n production -l app=sales-service
```

2. **Health Verification** (3 minutes)
```bash
# Check service health
for i in {1..5}; do
  curl -f https://api.pwc-data.com/api/v1/sales/health
  sleep 10
done

# Verify metrics
curl -H "Authorization: Bearer $TOKEN" \
  "https://api.pwc-data.com/api/v1/monitoring/metrics?metric_names=api_error_rate&hours=1"
```

3. **Load Testing** (2 minutes)
```bash
# Run basic load test
ab -n 100 -c 10 https://api.pwc-data.com/api/v1/sales/transactions

# Monitor response times during test
curl -H "Authorization: Bearer $TOKEN" \
  "https://api.pwc-data.com/api/v1/monitoring/metrics/timeseries/api_response_time_p95?hours=1"
```

#### Post-Deployment Monitoring (5 minutes)
```bash
# Monitor for 15 minutes after deployment
for i in {1..3}; do
  echo "Check $i at $(date)"
  
  # Error rate check
  curl -H "Authorization: Bearer $TOKEN" \
    "https://api.pwc-data.com/api/v1/monitoring/metrics?metric_names=api_error_rate&hours=1"
  
  # Response time check
  curl -H "Authorization: Bearer $TOKEN" \
    "https://api.pwc-data.com/api/v1/monitoring/metrics?metric_names=api_response_time_p95&hours=1"
  
  sleep 300  # Wait 5 minutes
done
```

#### Rollback Procedure (Emergency)
```bash
# If deployment fails, immediate rollback
kubectl rollout undo deployment/sales-service -n production

# Wait for rollback completion
kubectl rollout status deployment/sales-service -n production --timeout=180s

# Verify rollback success
kubectl get pods -n production -l app=sales-service
curl -f https://api.pwc-data.com/api/v1/sales/health
```

## Security Incident Response

### Security Incident Classification

| Level | Description | Response Time | Actions |
|-------|-------------|---------------|---------|
| **P1** | Active attack, data breach | 15 minutes | Immediate containment, law enforcement |
| **P2** | Suspected breach, vuln exploitation | 1 hour | Investigation, containment |
| **P3** | Security policy violation | 4 hours | Review, remediation |
| **P4** | Security awareness issue | 24 hours | Training, documentation |

### P1 Security Incident Response

**Context:** Active security attack or confirmed data breach
**Response Time:** 15 minutes
**Team:** Security Response Team + Management

#### Immediate Actions (5 minutes)
```bash
# Document incident start time
echo "Security incident start: $(date -u)" >> /var/log/security-incident.log

# Capture system state
kubectl get pods --all-namespaces -o wide > /tmp/incident-pods-$(date +%s).log
netstat -tuln > /tmp/incident-network-$(date +%s).log
ps aux > /tmp/incident-processes-$(date +%s).log
```

#### Containment (10 minutes)
```bash
# Option A: Complete isolation (if attack is active)
# Block all external traffic
kubectl patch service api-gateway -p '{"spec":{"type":"ClusterIP"}}'

# Option B: Selective blocking (if specific threat identified)  
# Block suspicious IPs
kubectl apply -f - <<EOF
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: block-suspicious-ips
spec:
  podSelector:
    matchLabels:
      app: sales-service
  policyTypes:
  - Ingress
  ingress:
  - from:
    - ipBlock:
        cidr: 0.0.0.0/0
        except:
        - [SUSPICIOUS_IP]/32
EOF

# Option C: Account lockdown (if compromised credentials)
# Disable compromised user accounts
curl -X POST -H "Authorization: Bearer $ADMIN_TOKEN" \
  https://api.pwc-data.com/api/v1/auth/users/[USER_ID]/disable
```

#### Evidence Preservation (Ongoing)
```bash
# Create forensic disk images (if required)
dd if=/dev/sda of=/forensics/disk-image-$(date +%s).img

# Preserve logs
kubectl logs --all-containers --prefix -n production > /forensics/k8s-logs-$(date +%s).log

# Database audit trail
pg_dump -h $DB_HOST -d sales_db --table=audit_log > /forensics/audit-$(date +%s).sql
```

#### Communication (Immediate)
```bash
# Send security alert
curl -X POST https://hooks.slack.com/services/YOUR/SECURITY/WEBHOOK \
  -H 'Content-type: application/json' \
  --data '{"text":"🚨 P1 SECURITY INCIDENT DECLARED - Check #security-incident channel"}'

# Email security team
echo "P1 Security incident declared at $(date -u). Incident ID: SEC-$(date +%s)" | \
  mail -s "P1 SECURITY INCIDENT" security-incident@pwc.com
```

## Disaster Recovery

### Data Center Failover

**Context:** Primary data center unavailable
**RTO:** 4 hours (Recovery Time Objective)
**RPO:** 1 hour (Recovery Point Objective)

#### Failover Triggers
- Primary data center network outage > 30 minutes
- Infrastructure failure affecting >50% of services
- Natural disaster or security incident
- Management directive

#### Failover Procedure (240 minutes)

1. **Declare Disaster (15 minutes)**
```bash
# Document disaster declaration
echo "DISASTER DECLARED: $(date -u) - Initiating DC failover" | \
  tee /var/log/disaster-recovery.log

# Notify stakeholders
curl -X POST $PAGERDUTY_WEBHOOK \
  -d '{"routing_key":"disaster-recovery","event_action":"trigger","payload":{"summary":"Data center failover initiated"}}'
```

2. **Assess Secondary Site (30 minutes)**
```bash
# Check secondary infrastructure status
curl -f https://api-dr.pwc-data.com/health

# Verify database replication status
psql -h $DR_DB_HOST -c "SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn();"

# Check data freshness
psql -h $DR_DB_HOST -c "SELECT max(created_at) FROM sales WHERE created_at > NOW() - INTERVAL '2 hours';"
```

3. **Promote Secondary Systems (60 minutes)**
```bash
# Promote read-replica to primary
pg_promote -D $DR_DB_DATA_DIR

# Update DNS records to point to DR site
aws route53 change-resource-record-sets --hosted-zone-id $ZONE_ID --change-batch file://dr-dns-change.json

# Scale up DR services to production capacity
kubectl scale deployment --all --replicas=3 -n production-dr
```

4. **Verify Recovery (45 minutes)**
```bash
# Test critical user journeys
./scripts/smoke-test-dr.sh

# Verify data integrity
./scripts/data-integrity-check.sh

# Load testing
ab -n 1000 -c 50 https://api.pwc-data.com/api/v1/sales/health
```

5. **Monitor and Stabilize (90 minutes)**
```bash
# Monitor error rates and response times
watch -n 30 'curl -s https://api.pwc-data.com/api/v1/monitoring/health | jq .status'

# Check database performance
watch -n 60 'psql -h $DR_DB_HOST -c "SELECT count(*) FROM pg_stat_activity WHERE state = \"active\";"'

# Monitor resource usage
watch kubectl top pods -n production-dr
```

#### Failback Procedure (When Primary Restored)
```bash
# Sync data from DR to primary
pg_basebackup -h $DR_DB_HOST -D $PRIMARY_DB_DATA_DIR

# Switch traffic back to primary
aws route53 change-resource-record-sets --hosted-zone-id $ZONE_ID --change-batch file://primary-dns-change.json

# Verify primary site health
curl -f https://api.pwc-data.com/health

# Scale down DR site
kubectl scale deployment --all --replicas=1 -n production-dr
```

## Escalation Procedures

### Escalation Matrix

| Issue Type | L1 Response | L2 Escalation | L3 Escalation | Management |
|------------|-------------|---------------|---------------|------------|
| **Service Down** | 15 min | 30 min | 45 min | 1 hour |
| **Performance** | 30 min | 1 hour | 2 hours | 4 hours |
| **Security** | Immediate | 15 min | 30 min | 45 min |
| **Data Issues** | 1 hour | 2 hours | 4 hours | 8 hours |

### Escalation Contacts

```yaml
L1_Support:
  - name: "SRE On-Call"
    phone: "+1-555-ONCALL-1"
    pagerduty: "sre-oncall@pwc.pagerduty.com"

L2_Engineering:
  - name: "Platform Team Lead"
    phone: "+1-555-PLATFORM"
    slack: "@platform-lead"
    
L3_Architecture:
  - name: "Principal Architect"
    phone: "+1-555-ARCHITECT"
    email: "principal-architect@pwc.com"
    
Management:
  - name: "VP Engineering"
    phone: "+1-555-VP-ENG"
    email: "vp-engineering@pwc.com"
```

### Escalation Triggers

#### Automatic Escalation
- P1 incident not resolved within time limit
- Multiple services affected
- Customer-facing impact exceeds SLA
- Security incident requiring legal notification

#### Manual Escalation
- Technical complexity beyond current skill level
- Resource authorization needed
- Customer communication required
- External vendor coordination needed

---

## Quick Reference Cards

### Emergency Commands
```bash
# Service restart
kubectl rollout restart deployment/[SERVICE] -n production

# Scale up quickly  
kubectl scale deployment/[SERVICE] --replicas=10 -n production

# Emergency rollback
kubectl rollout undo deployment/[SERVICE] -n production

# Check service health
curl -f https://api.pwc-data.com/api/v1/monitoring/health

# Get active alerts
curl -H "Authorization: Bearer $TOKEN" \
  https://api.pwc-data.com/api/v1/monitoring/alerts?active_only=true

# Emergency maintenance mode
kubectl patch ingress api-gateway -p '{"metadata":{"annotations":{"nginx.ingress.kubernetes.io/default-backend":"maintenance-page"}}}'
```

### Service Dependencies
```
API Gateway -> Sales Service -> Database
           -> Analytics Service -> Database + Kafka
           -> Search Service -> Elasticsearch
           -> Auth Service -> Database + Redis

All Services -> RabbitMQ (messaging)
            -> Redis (caching)
            -> Monitoring (observability)
```

### SLA Targets
- **API Availability:** 99.9%
- **API Response Time:** <500ms (95th percentile)
- **ETL Success Rate:** 99.5%
- **Data Freshness:** <15 minutes
- **Alert Response:** <15 minutes (P1)

---

*These runbooks are living documents and should be updated after each incident to incorporate lessons learned.*
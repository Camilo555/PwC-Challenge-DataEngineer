# PwC Retail Data Platform - Operations Runbook

## Table of Contents

1. [Overview](#overview)
2. [Emergency Response](#emergency-response)
3. [System Health Monitoring](#system-health-monitoring)
4. [Common Incident Responses](#common-incident-responses)
5. [ETL Pipeline Operations](#etl-pipeline-operations)
6. [Database Operations](#database-operations)
7. [Performance Troubleshooting](#performance-troubleshooting)
8. [Security Incident Response](#security-incident-response)
9. [Backup & Recovery](#backup--recovery)
10. [Escalation Procedures](#escalation-procedures)

## Overview

This runbook provides step-by-step procedures for managing the PwC Retail Data Platform in production. It covers emergency response, routine maintenance, and troubleshooting procedures for operations teams.

### Key Contacts

| Role | Name | Primary Contact | Secondary Contact | Escalation |
|------|------|-----------------|-------------------|------------|
| **Platform Owner** | Data Engineering Lead | +1-555-0001 | lead@company.com | CTO |
| **On-Call Engineer** | Rotating Schedule | +1-555-0002 | oncall@company.com | Platform Owner |
| **Database Admin** | DBA Team | +1-555-0003 | dba@company.com | On-Call Engineer |
| **Security Team** | InfoSec | +1-555-0004 | security@company.com | CISO |
| **Business Owner** | Analytics Director | +1-555-0005 | analytics@company.com | VP Data |

### Service Level Objectives (SLOs)

| Metric | Target | Measurement Window |
|--------|--------|--------------------|
| **API Availability** | 99.9% | 30 days |
| **API Response Time (P95)** | < 500ms | 5 minutes |
| **ETL Pipeline Success Rate** | 99.5% | 7 days |
| **Data Freshness** | < 6 hours | Real-time |
| **Recovery Time Objective (RTO)** | < 4 hours | Per incident |
| **Recovery Point Objective (RPO)** | < 15 minutes | Per incident |

## Emergency Response

### Incident Classification

#### Severity Levels

**P0 - Critical (Emergency)**
- Complete system outage
- Data corruption or loss
- Security breach
- Response Time: < 15 minutes
- Notification: Immediate to all stakeholders

**P1 - High (Urgent)**
- Significant functionality impact
- Performance degradation affecting users
- Failed critical ETL pipelines
- Response Time: < 1 hour
- Notification: On-call team + management

**P2 - Medium (Normal)**
- Partial functionality impact
- Non-critical pipeline failures
- Performance issues not affecting users
- Response Time: < 4 hours
- Notification: On-call team

**P3 - Low (Planned)**
- Minor issues
- Enhancement requests
- Scheduled maintenance
- Response Time: < 24 hours
- Notification: Standard channels

### Emergency Response Checklist

#### Immediate Actions (First 15 minutes)

```bash
# 1. Acknowledge the incident
echo "$(date): Incident acknowledged by $(whoami)" >> /var/log/incidents/current.log

# 2. Assess system status
kubectl get pods -n pwc-retail --field-selector=status.phase!=Running
kubectl get nodes --field-selector=spec.unschedulable=false

# 3. Check monitoring dashboards
# - Grafana: https://monitoring.pwc-retail.com/dashboards
# - Prometheus alerts: https://monitoring.pwc-retail.com/alerts

# 4. Join incident response channel
# Slack: #incident-response

# 5. Create incident ticket
# JIRA: https://company.atlassian.net/servicedesk
```

#### Communication Template

```markdown
**INCIDENT ALERT - P[0/1/2/3]**

**Summary**: [Brief description of the issue]
**Impact**: [What systems/users are affected]
**Current Status**: [What's happening now]
**ETA for Resolution**: [Best estimate]
**Incident Commander**: [Name and contact]
**Next Update**: [When the next update will be provided]

**Affected Services**:
- [ ] API (api.pwc-retail.com)
- [ ] ETL Pipelines
- [ ] Data Warehouse
- [ ] Analytics Dashboard
- [ ] Search Services

**Response Actions Taken**:
- [x] [Action taken with timestamp]
- [ ] [Planned action]

**Monitoring Links**:
- [Grafana Dashboard](https://monitoring.pwc-retail.com)
- [Incident Details](https://company.atlassian.net/incident-123)
```

## System Health Monitoring

### Health Check Procedures

#### API Health Check

```bash
#!/bin/bash
# scripts/health-check-api.sh

API_URL="https://api.pwc-retail.com"
TIMEOUT=10

echo "🏥 API Health Check - $(date)"
echo "=================================="

# Basic health endpoint
if curl -f -s --max-time $TIMEOUT "$API_URL/health" > /dev/null; then
    echo "✅ Health endpoint: OK"
else
    echo "❌ Health endpoint: FAIL"
    exit 1
fi

# Database connectivity
HEALTH_RESPONSE=$(curl -s --max-time $TIMEOUT "$API_URL/health")
DB_STATUS=$(echo $HEALTH_RESPONSE | jq -r '.services.database')

if [ "$DB_STATUS" = "healthy" ]; then
    echo "✅ Database connectivity: OK"
else
    echo "❌ Database connectivity: FAIL"
    echo "Response: $HEALTH_RESPONSE"
    exit 1
fi

# Redis connectivity
REDIS_STATUS=$(echo $HEALTH_RESPONSE | jq -r '.services.redis')

if [ "$REDIS_STATUS" = "healthy" ]; then
    echo "✅ Redis connectivity: OK"
else
    echo "❌ Redis connectivity: FAIL"
fi

# Performance check
RESPONSE_TIME=$(curl -o /dev/null -s -w '%{time_total}\n' --max-time $TIMEOUT "$API_URL/health")
if (( $(echo "$RESPONSE_TIME < 1.0" | bc -l) )); then
    echo "✅ Response time: ${RESPONSE_TIME}s"
else
    echo "⚠️  Response time: ${RESPONSE_TIME}s (slow)"
fi

echo "✅ API health check completed"
```

#### ETL Pipeline Health Check

```bash
#!/bin/bash
# scripts/health-check-etl.sh

DAGSTER_HOST="http://dagster.pwc-retail.svc.cluster.local:3000"

echo "🔧 ETL Pipeline Health Check - $(date)"
echo "=================================="

# Check Dagster daemon status
if curl -f -s --max-time 10 "$DAGSTER_HOST/server_info" > /dev/null; then
    echo "✅ Dagster daemon: Running"
else
    echo "❌ Dagster daemon: Not responding"
    exit 1
fi

# Check recent pipeline runs
RUNS_JSON=$(curl -s "$DAGSTER_HOST/api/runs" | jq '.data[:5]')
FAILED_RUNS=$(echo $RUNS_JSON | jq '[.[] | select(.status == "FAILURE")] | length')

if [ "$FAILED_RUNS" -eq 0 ]; then
    echo "✅ Recent pipeline runs: All successful"
else
    echo "⚠️  Recent pipeline runs: $FAILED_RUNS failures detected"
    echo $RUNS_JSON | jq '.[] | select(.status == "FAILURE") | {pipeline: .pipelineName, status: .status, startTime: .startTime}'
fi

# Check data freshness
LAST_SUCCESS=$(curl -s "$DAGSTER_HOST/api/runs" | jq -r '.data[] | select(.status == "SUCCESS") | .startTime' | head -1)
CURRENT_TIME=$(date +%s)
LAST_SUCCESS_TIME=$(date -d "$LAST_SUCCESS" +%s 2>/dev/null || echo $CURRENT_TIME)
TIME_DIFF=$((CURRENT_TIME - LAST_SUCCESS_TIME))

if [ $TIME_DIFF -lt 21600 ]; then # 6 hours
    echo "✅ Data freshness: Last successful run $(($TIME_DIFF / 3600)) hours ago"
else
    echo "❌ Data freshness: Last successful run $(($TIME_DIFF / 3600)) hours ago (stale)"
    exit 1
fi

echo "✅ ETL pipeline health check completed"
```

### Automated Monitoring Setup

#### Prometheus Alert Rules

```yaml
# monitoring/alert-rules.yml
groups:
  - name: platform-health
    rules:
      - alert: APIDown
        expr: up{job="api-service"} == 0
        for: 1m
        labels:
          severity: critical
          team: platform
        annotations:
          summary: "API service is down"
          description: "API service has been down for more than 1 minute"
          runbook_url: "https://runbook.pwc-retail.com/api-down"

      - alert: HighErrorRate
        expr: rate(http_requests_total{status=~"5.."}[5m]) > 0.1
        for: 5m
        labels:
          severity: critical
          team: platform
        annotations:
          summary: "High error rate detected"
          description: "Error rate is {{ $value }} errors per second"
          runbook_url: "https://runbook.pwc-retail.com/high-error-rate"

      - alert: DatabaseConnectionsHigh
        expr: pg_stat_database_numbackends / pg_settings_max_connections > 0.8
        for: 5m
        labels:
          severity: warning
          team: database
        annotations:
          summary: "Database connections high"
          description: "Database connections at {{ $value }}% of maximum"
          runbook_url: "https://runbook.pwc-retail.com/database-connections"

      - alert: ETLPipelineFailure
        expr: increase(dagster_pipeline_failure_total[1h]) > 0
        for: 0m
        labels:
          severity: critical
          team: data-engineering
        annotations:
          summary: "ETL pipeline failure"
          description: "Pipeline {{ $labels.pipeline }} has failed"
          runbook_url: "https://runbook.pwc-retail.com/etl-failure"
```

## Common Incident Responses

### API Service Down

**Symptoms**: 
- Health checks failing
- 5xx errors from load balancer
- Users cannot access application

**Investigation Steps**:

```bash
# 1. Check pod status
kubectl get pods -n pwc-retail -l app=api-service

# 2. Check recent events
kubectl get events -n pwc-retail --sort-by='.lastTimestamp' | tail -20

# 3. Check service and endpoints
kubectl describe service api-service -n pwc-retail
kubectl get endpoints api-service -n pwc-retail

# 4. Check pod logs
kubectl logs -l app=api-service -n pwc-retail --tail=100 --timestamps

# 5. Check resource utilization
kubectl top pods -n pwc-retail
kubectl describe node $(kubectl get pods -l app=api-service -n pwc-retail -o jsonpath='{.items[0].spec.nodeName}')
```

**Resolution Steps**:

```bash
# Option 1: Restart pods
kubectl rollout restart deployment/api-service -n pwc-retail
kubectl rollout status deployment/api-service -n pwc-retail

# Option 2: Scale up replicas
kubectl scale deployment api-service --replicas=5 -n pwc-retail

# Option 3: Emergency rollback (if related to recent deployment)
kubectl rollout undo deployment/api-service -n pwc-retail

# Option 4: Check and fix configuration
kubectl edit configmap app-config -n pwc-retail
kubectl edit secret app-secrets -n pwc-retail

# Verification
kubectl get pods -n pwc-retail -l app=api-service
curl -f https://api.pwc-retail.com/health
```

### Database Connection Issues

**Symptoms**:
- Connection timeout errors
- "Too many connections" errors
- Slow query responses

**Investigation Steps**:

```bash
# 1. Check database pod status (if self-hosted)
kubectl get pods -n database -l app=postgres

# 2. Check connection count
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT count(*) as active_connections, 
       max_conn, 
       max_conn - count(*) as available_connections
FROM pg_stat_activity, 
     (SELECT setting::int as max_conn FROM pg_settings WHERE name = 'max_connections') mc;
"

# 3. Check for blocked queries
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT pid, state, query_start, query 
FROM pg_stat_activity 
WHERE state != 'idle' 
ORDER BY query_start;
"

# 4. Check slow queries
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT query, calls, total_time, mean_time 
FROM pg_stat_statements 
ORDER BY mean_time DESC 
LIMIT 10;
"
```

**Resolution Steps**:

```bash
# Option 1: Kill long-running queries
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE state != 'idle' 
AND query_start < now() - interval '5 minutes';
"

# Option 2: Restart connection pool
kubectl rollout restart deployment/api-service -n pwc-retail

# Option 3: Scale up database (managed services)
# AWS RDS: Modify instance class
# Azure: Scale up the service tier

# Option 4: Increase connection limits (temporary)
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
ALTER SYSTEM SET max_connections = 300;
SELECT pg_reload_conf();
"
```

### High Memory Usage

**Symptoms**:
- Pods being OOMKilled
- Slow response times
- Node resource pressure

**Investigation Steps**:

```bash
# 1. Check pod resource usage
kubectl top pods -n pwc-retail --sort-by=memory

# 2. Check node resource usage
kubectl top nodes

# 3. Check pod resource limits
kubectl describe pod $(kubectl get pods -l app=api-service -n pwc-retail -o name | head -1) | grep -A 10 "Limits"

# 4. Check memory usage over time (if monitoring available)
# Query Prometheus: container_memory_usage_bytes{pod=~"api-service.*"}
```

**Resolution Steps**:

```bash
# Option 1: Scale horizontally
kubectl scale deployment api-service --replicas=6 -n pwc-retail

# Option 2: Increase memory limits
kubectl patch deployment api-service -n pwc-retail -p='
{
  "spec": {
    "template": {
      "spec": {
        "containers": [
          {
            "name": "api",
            "resources": {
              "limits": {
                "memory": "6Gi"
              },
              "requests": {
                "memory": "2Gi"
              }
            }
          }
        ]
      }
    }
  }
}'

# Option 3: Restart pods to clear memory
kubectl delete pods -l app=api-service -n pwc-retail

# Option 4: Add more nodes to cluster (if using cluster autoscaler)
kubectl get nodes -o wide
```

### ETL Pipeline Failures

**Symptoms**:
- Failed pipeline runs in Dagster
- Data freshness alerts
- Missing data in analytics

**Investigation Steps**:

```bash
# 1. Check Dagster UI for failed runs
# Navigate to: http://dagster.pwc-retail.com

# 2. Get recent failed runs via API
curl -s "http://dagster.pwc-retail.svc.cluster.local:3000/api/runs?filter=FAILURE" | jq '.data[:5]'

# 3. Check specific run logs
RUN_ID="your-run-id"
curl -s "http://dagster.pwc-retail.svc.cluster.local:3000/api/runs/$RUN_ID/logs" | jq '.data.logs'

# 4. Check data quality in source systems
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
    COUNT(*) as row_count,
    MAX(created_at) as latest_record,
    COUNT(DISTINCT customer_id) as unique_customers
FROM silver.retail_transactions 
WHERE created_at >= CURRENT_DATE - INTERVAL '1 day';
"

# 5. Check Spark job status (if applicable)
kubectl logs -n pwc-retail -l app=spark-driver --tail=100
```

**Resolution Steps**:

```bash
# Option 1: Retry failed run
curl -X POST "http://dagster.pwc-retail.svc.cluster.local:3000/api/runs/$RUN_ID/retry"

# Option 2: Run manual pipeline execution
# Via Dagster UI: Select pipeline > Launch Run

# Option 3: Check and fix data quality issues
# Investigate source data quality problems

# Option 4: Restart Dagster daemon
kubectl rollout restart deployment/dagster-daemon -n pwc-retail

# Option 5: Clear corrupted intermediate data
# Remove problematic files from S3/data lake
aws s3 rm s3://pwc-retail-data/bronze/problematic_batch/ --recursive
```

## ETL Pipeline Operations

### Pipeline Monitoring

#### Real-time Pipeline Status

```bash
#!/bin/bash
# scripts/pipeline-status.sh

DAGSTER_API="http://dagster.pwc-retail.svc.cluster.local:3000/api"

echo "📊 Pipeline Status Dashboard - $(date)"
echo "=================================="

# Get active runs
ACTIVE_RUNS=$(curl -s "$DAGSTER_API/runs?filter=IN_PROGRESS" | jq -r '.data | length')
echo "🏃 Active Runs: $ACTIVE_RUNS"

# Get recent failures
RECENT_FAILURES=$(curl -s "$DAGSTER_API/runs?filter=FAILURE&limit=10" | jq '.data')
FAILURE_COUNT=$(echo $RECENT_FAILURES | jq 'length')
echo "❌ Recent Failures: $FAILURE_COUNT"

if [ $FAILURE_COUNT -gt 0 ]; then
    echo "📋 Failed Pipelines:"
    echo $RECENT_FAILURES | jq -r '.[] | "  - \(.pipelineName) at \(.startTime)"'
fi

# Get pipeline success rate (last 24 hours)
SUCCESS_RATE=$(curl -s "$DAGSTER_API/runs?limit=50" | jq '[.data[] | select(.startTime > (now - 86400))] | group_by(.status) | map({status: .[0].status, count: length}) | from_entries | ((.SUCCESS // 0) / ((.SUCCESS // 0) + (.FAILURE // 0))) * 100')
echo "📈 24h Success Rate: ${SUCCESS_RATE}%"

# Check data freshness
LAST_SUCCESS=$(curl -s "$DAGSTER_API/runs?filter=SUCCESS&limit=1" | jq -r '.data[0].endTime')
if [ "$LAST_SUCCESS" != "null" ]; then
    LAST_SUCCESS_HOURS=$(( ($(date +%s) - $(date -d "$LAST_SUCCESS" +%s)) / 3600 ))
    echo "🕐 Data Freshness: ${LAST_SUCCESS_HOURS} hours since last success"
else
    echo "⚠️  No recent successful runs found"
fi
```

### Manual Pipeline Operations

#### Trigger Emergency Data Refresh

```bash
#!/bin/bash
# scripts/emergency-data-refresh.sh

set -e

NAMESPACE="pwc-retail"
DAGSTER_API="http://dagster.pwc-retail.svc.cluster.local:3000/api"

echo "🚨 Emergency Data Refresh - $(date)"
echo "=================================="

# 1. Stop current running pipelines
echo "Stopping active pipelines..."
ACTIVE_RUNS=$(curl -s "$DAGSTER_API/runs?filter=IN_PROGRESS" | jq -r '.data[].runId')
for run_id in $ACTIVE_RUNS; do
    echo "  Stopping run: $run_id"
    curl -X POST "$DAGSTER_API/runs/$run_id/terminate"
done

# 2. Clear Redis cache
echo "Clearing cache..."
kubectl exec -n $NAMESPACE deployment/redis -- redis-cli FLUSHALL

# 3. Trigger full data refresh
echo "Triggering full data refresh..."

# Bronze layer refresh
curl -X POST "$DAGSTER_API/runs" -H "Content-Type: application/json" -d '{
    "pipeline_name": "bronze_ingestion_pipeline",
    "mode": "default",
    "run_config": {
        "resources": {
            "spark": {
                "config": {
                    "spark_conf": {
                        "spark.sql.execution.arrow.pyspark.enabled": "true"
                    }
                }
            }
        },
        "execution": {
            "multiprocess": {
                "config": {
                    "max_concurrent": 4
                }
            }
        }
    },
    "tags": {
        "emergency_refresh": "true",
        "triggered_by": "'$USER'",
        "timestamp": "'$(date -Iseconds)'"
    }
}'

sleep 30  # Wait for bronze to start

# Silver layer refresh
curl -X POST "$DAGSTER_API/runs" -H "Content-Type: application/json" -d '{
    "pipeline_name": "silver_processing_pipeline",
    "mode": "default",
    "tags": {
        "emergency_refresh": "true",
        "triggered_by": "'$USER'"
    }
}'

sleep 30  # Wait for silver to start

# Gold layer refresh
curl -X POST "$DAGSTER_API/runs" -H "Content-Type: application/json" -d '{
    "pipeline_name": "gold_analytics_pipeline", 
    "mode": "default",
    "tags": {
        "emergency_refresh": "true",
        "triggered_by": "'$USER'"
    }
}'

echo "✅ Emergency refresh triggered. Monitor progress at:"
echo "   Dagster UI: http://dagster.pwc-retail.com"
echo "   Grafana: https://monitoring.pwc-retail.com/d/etl-overview"
```

#### Data Quality Remediation

```bash
#!/bin/bash
# scripts/data-quality-remediation.sh

DB_HOST="postgres.database.svc.cluster.local"
DB_USER="postgres"
DB_NAME="retail"

echo "🔍 Data Quality Remediation - $(date)"
echo "=================================="

# 1. Identify quality issues
echo "Analyzing data quality issues..."

QUALITY_REPORT=$(psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "
WITH quality_summary AS (
    SELECT 
        'silver.retail_transactions' as table_name,
        COUNT(*) as total_records,
        COUNT(*) FILTER (WHERE data_quality_score < 0.8) as low_quality_records,
        AVG(data_quality_score) as avg_quality_score,
        COUNT(*) FILTER (WHERE customer_id IS NULL) as missing_customer_ids,
        COUNT(*) FILTER (WHERE unit_price <= 0) as invalid_prices,
        COUNT(*) FILTER (WHERE quantity = 0) as zero_quantities
    FROM silver.retail_transactions 
    WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
)
SELECT json_agg(quality_summary) FROM quality_summary;
")

echo "Quality Report: $QUALITY_REPORT"

# 2. Fix common quality issues
echo "Fixing common quality issues..."

# Fix missing customer IDs (assign placeholder)
FIXED_CUSTOMER_IDS=$(psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "
UPDATE silver.retail_transactions 
SET customer_id = 'UNKNOWN_' || transaction_id::text
WHERE customer_id IS NULL 
AND created_at >= CURRENT_DATE - INTERVAL '1 day'
RETURNING COUNT(*);
")
echo "Fixed missing customer IDs: $FIXED_CUSTOMER_IDS records"

# Fix invalid prices (use median price for product)
FIXED_PRICES=$(psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "
WITH median_prices AS (
    SELECT 
        stock_code,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY unit_price) as median_price
    FROM silver.retail_transactions 
    WHERE unit_price > 0
    GROUP BY stock_code
)
UPDATE silver.retail_transactions t
SET unit_price = mp.median_price,
    data_quality_score = GREATEST(data_quality_score - 0.1, 0.0)
FROM median_prices mp
WHERE t.stock_code = mp.stock_code
AND t.unit_price <= 0
AND t.created_at >= CURRENT_DATE - INTERVAL '1 day'
RETURNING COUNT(*);
")
echo "Fixed invalid prices: $FIXED_PRICES records"

# 3. Recalculate quality scores
echo "Recalculating quality scores..."
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
UPDATE silver.retail_transactions 
SET data_quality_score = (
    CASE WHEN customer_id IS NOT NULL THEN 0.25 ELSE 0 END +
    CASE WHEN unit_price > 0 THEN 0.25 ELSE 0 END +
    CASE WHEN quantity != 0 THEN 0.25 ELSE 0 END +
    CASE WHEN invoice_date IS NOT NULL THEN 0.25 ELSE 0 END
),
updated_at = CURRENT_TIMESTAMP
WHERE created_at >= CURRENT_DATE - INTERVAL '1 day';
"

# 4. Generate updated quality report
UPDATED_QUALITY=$(psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "
SELECT 
    COUNT(*) as total_records,
    AVG(data_quality_score) as avg_quality_score,
    COUNT(*) FILTER (WHERE data_quality_score >= 0.8) as high_quality_records
FROM silver.retail_transactions 
WHERE created_at >= CURRENT_DATE - INTERVAL '1 day';
")

echo "✅ Quality remediation completed"
echo "Updated metrics: $UPDATED_QUALITY"

# 5. Trigger downstream processing
echo "Triggering downstream processing..."
curl -X POST "http://dagster.pwc-retail.svc.cluster.local:3000/api/runs" \
    -H "Content-Type: application/json" \
    -d '{
        "pipeline_name": "gold_analytics_pipeline",
        "tags": {"trigger": "quality_remediation"}
    }'

echo "🎉 Data quality remediation process completed"
```

## Database Operations

### Database Maintenance

#### Automated Vacuum and Analyze

```bash
#!/bin/bash
# scripts/database-maintenance.sh

DB_HOST="postgres.database.svc.cluster.local"
DB_USER="postgres" 
DB_NAME="retail"

echo "🗃️  Database Maintenance - $(date)"
echo "=================================="

# 1. Check database statistics
echo "Checking database statistics..."
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
    schemaname,
    tablename,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables 
ORDER BY n_tup_ins + n_tup_upd + n_tup_del DESC
LIMIT 20;
"

# 2. Identify tables needing vacuum
TABLES_TO_VACUUM=$(psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "
SELECT schemaname || '.' || tablename
FROM pg_stat_user_tables 
WHERE (n_tup_ins + n_tup_upd + n_tup_del) > 1000
AND (last_vacuum IS NULL OR last_vacuum < NOW() - INTERVAL '7 days')
ORDER BY n_tup_ins + n_tup_upd + n_tup_del DESC;
")

# 3. Run vacuum analyze on identified tables
if [ -n "$TABLES_TO_VACUUM" ]; then
    echo "Running vacuum analyze on tables:"
    for table in $TABLES_TO_VACUUM; do
        echo "  Processing: $table"
        psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "VACUUM ANALYZE $table;"
    done
else
    echo "No tables require vacuum"
fi

# 4. Update table statistics
echo "Updating table statistics..."
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "ANALYZE;"

# 5. Check for bloat
echo "Checking for table bloat..."
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
WITH bloat AS (
    SELECT 
        schemaname, 
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
        pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS table_size,
        ROUND(100 * pg_relation_size(schemaname||'.'||tablename) / 
              NULLIF(pg_total_relation_size(schemaname||'.'||tablename), 0), 2) AS table_bloat_ratio
    FROM pg_stat_user_tables
    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
)
SELECT * FROM bloat LIMIT 10;
"

# 6. Reindex if necessary
echo "Checking index usage..."
UNUSED_INDEXES=$(psql -h $DB_HOST -U $DB_USER -d $DB_NAME -t -c "
SELECT schemaname||'.'||indexname
FROM pg_stat_user_indexes 
WHERE idx_scan = 0 
AND schemaname NOT IN ('information_schema', 'pg_catalog')
LIMIT 5;
")

if [ -n "$UNUSED_INDEXES" ]; then
    echo "⚠️  Found unused indexes (consider dropping):"
    for index in $UNUSED_INDEXES; do
        echo "  $index"
    done
fi

echo "✅ Database maintenance completed"
```

### Performance Monitoring

#### Query Performance Analysis

```sql
-- scripts/query-performance-analysis.sql

-- 1. Slow queries (requires pg_stat_statements extension)
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    stddev_time,
    rows,
    100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
FROM pg_stat_statements 
WHERE calls > 100
ORDER BY mean_time DESC 
LIMIT 20;

-- 2. Most time-consuming queries
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    (total_time / sum(total_time) OVER()) * 100 AS percentage_total
FROM pg_stat_statements 
WHERE calls > 10
ORDER BY total_time DESC 
LIMIT 10;

-- 3. Index usage statistics
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_tup_read,
    idx_tup_fetch,
    idx_scan,
    CASE WHEN idx_scan = 0 THEN 'UNUSED' 
         WHEN idx_scan < 10 THEN 'LOW_USAGE'
         ELSE 'ACTIVE' 
    END as usage_category
FROM pg_stat_user_indexes 
ORDER BY idx_scan DESC;

-- 4. Table scan vs index scan ratio
SELECT 
    schemaname,
    tablename,
    seq_scan,
    seq_tup_read,
    idx_scan,
    idx_tup_fetch,
    CASE WHEN seq_scan + idx_scan = 0 THEN 0
         ELSE ROUND(100.0 * seq_scan / (seq_scan + idx_scan), 2)
    END AS seq_scan_percent
FROM pg_stat_user_tables 
WHERE seq_scan + idx_scan > 0
ORDER BY seq_scan_percent DESC;

-- 5. Connection and lock analysis
SELECT 
    state,
    count(*) as connection_count,
    max(extract(epoch from (now() - query_start))) as max_duration_seconds
FROM pg_stat_activity 
GROUP BY state
ORDER BY connection_count DESC;

-- 6. Blocking queries
SELECT 
    blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    blocked_activity.query AS blocked_statement,
    blocking_activity.query AS blocking_statement
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks 
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;
```

## Performance Troubleshooting

### API Performance Issues

#### High Response Times

**Investigation Checklist**:

```bash
#!/bin/bash
# scripts/api-performance-investigation.sh

echo "🔍 API Performance Investigation - $(date)"
echo "=================================="

# 1. Check current response times
echo "Checking current response times..."
for endpoint in "/health" "/api/v1/sales" "/api/v2/analytics/customer-segmentation"; do
    RESPONSE_TIME=$(curl -o /dev/null -s -w '%{time_total}\n' "https://api.pwc-retail.com$endpoint")
    echo "  $endpoint: ${RESPONSE_TIME}s"
done

# 2. Check pod resource utilization  
echo "Checking pod resource utilization..."
kubectl top pods -n pwc-retail -l app=api-service

# 3. Check database connection pool
echo "Checking database connections..."
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
    count(*) as active_connections,
    count(*) FILTER (WHERE state = 'active') as active_queries,
    count(*) FILTER (WHERE state = 'idle') as idle_connections,
    count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction
FROM pg_stat_activity;
"

# 4. Check slow queries
echo "Checking slow queries..."
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT pid, state, query_start, query 
FROM pg_stat_activity 
WHERE state = 'active' 
AND query_start < now() - interval '30 seconds'
ORDER BY query_start;
"

# 5. Check cache hit rate
echo "Checking Redis cache status..."
kubectl exec -n pwc-retail deployment/redis -- redis-cli INFO stats | grep -E "keyspace_hits|keyspace_misses"

# 6. Check application metrics
echo "Checking application metrics..."
curl -s https://api.pwc-retail.com/metrics | grep -E "http_requests_total|http_request_duration"
```

#### Memory Leaks

**Detection and Resolution**:

```bash
#!/bin/bash
# scripts/memory-leak-detection.sh

NAMESPACE="pwc-retail"
APP_LABEL="app=api-service"

echo "🧠 Memory Leak Detection - $(date)"
echo "=================================="

# 1. Monitor memory usage over time
echo "Memory usage trend (last 6 hours):"
kubectl get pods -n $NAMESPACE -l $APP_LABEL -o jsonpath='{.items[*].metadata.name}' | \
while read pod; do
    echo "Pod: $pod"
    # This would typically query Prometheus for historical data
    # kubectl exec $pod -n $NAMESPACE -- ps -eo pid,ppid,cmd,%mem,%cpu --sort=-%mem | head -10
done

# 2. Check for memory growth patterns
echo "Current memory usage by process:"
kubectl exec -n $NAMESPACE deployment/api-service -- ps aux --sort=-%mem | head -10

# 3. Generate heap dump (if using Python memory profiling)
echo "Generating memory profile..."
kubectl exec -n $NAMESPACE deployment/api-service -- python -c "
import psutil
import os
process = psutil.Process(os.getpid())
memory_info = process.memory_info()
print(f'RSS: {memory_info.rss / 1024 / 1024:.2f} MB')
print(f'VMS: {memory_info.vms / 1024 / 1024:.2f} MB')
"

# 4. Check for potential memory leaks in application
echo "Checking for common memory leak patterns..."
kubectl logs -n $NAMESPACE -l $APP_LABEL --tail=1000 | grep -i "memory\|leak\|oom"

# 5. Restart pods if memory usage is critical
MEMORY_USAGE=$(kubectl top pods -n $NAMESPACE -l $APP_LABEL --no-headers | awk '{sum+=$3} END {print sum}' | sed 's/Mi//')
if [ "$MEMORY_USAGE" -gt 3000 ]; then  # > 3GB total
    echo "⚠️  High memory usage detected ($MEMORY_USAGE Mi). Restarting pods..."
    kubectl rollout restart deployment/api-service -n $NAMESPACE
fi
```

## Security Incident Response

### Security Breach Protocol

#### Immediate Response (First 30 minutes)

```bash
#!/bin/bash
# scripts/security-incident-response.sh

INCIDENT_ID="SEC-$(date +%Y%m%d-%H%M%S)"
INCIDENT_LOG="/var/log/security/incident-$INCIDENT_ID.log"

echo "🚨 SECURITY INCIDENT RESPONSE - $INCIDENT_ID" | tee -a $INCIDENT_LOG
echo "Started at: $(date)" | tee -a $INCIDENT_LOG
echo "Responder: $(whoami)" | tee -a $INCIDENT_LOG
echo "=======================================" | tee -a $INCIDENT_LOG

# 1. Isolate affected systems
echo "STEP 1: Isolating affected systems..." | tee -a $INCIDENT_LOG

# Stop all external traffic
echo "  - Stopping external traffic..." | tee -a $INCIDENT_LOG
kubectl patch ingress api-ingress -n pwc-retail -p '{"spec":{"rules":[]}}'

# Block suspicious IPs (example)
# kubectl exec -n nginx-ingress deployment/nginx-ingress-controller -- nginx -s reload

# 2. Preserve evidence
echo "STEP 2: Preserving evidence..." | tee -a $INCIDENT_LOG

# Capture system state
kubectl get events -A --sort-by='.lastTimestamp' > "/tmp/k8s-events-$INCIDENT_ID.log"
kubectl get pods -A -o wide > "/tmp/pods-state-$INCIDENT_ID.log" 

# Capture application logs
kubectl logs -n pwc-retail -l app=api-service --since=2h > "/tmp/api-logs-$INCIDENT_ID.log"

# Database access logs (if available)
# psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "SELECT * FROM pg_stat_activity;" > "/tmp/db-activity-$INCIDENT_ID.log"

# 3. Assess impact
echo "STEP 3: Assessing impact..." | tee -a $INCIDENT_LOG

# Check for data exfiltration
echo "  - Checking for unusual data access patterns..." | tee -a $INCIDENT_LOG
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
    usename, 
    application_name,
    client_addr,
    query_start,
    state,
    query
FROM pg_stat_activity 
WHERE state != 'idle' 
ORDER BY query_start DESC;
" >> $INCIDENT_LOG

# Check authentication logs
kubectl logs -n pwc-retail -l app=api-service --since=2h | grep -i "auth\|login\|token" >> $INCIDENT_LOG

# 4. Notify stakeholders
echo "STEP 4: Notifying stakeholders..." | tee -a $INCIDENT_LOG

# Send alert to security team
curl -X POST $SLACK_SECURITY_WEBHOOK -H 'Content-type: application/json' --data "{
    \"text\": \"🚨 SECURITY INCIDENT DETECTED - $INCIDENT_ID\",
    \"attachments\": [{
        \"color\": \"danger\",
        \"fields\": [{
            \"title\": \"Incident ID\",
            \"value\": \"$INCIDENT_ID\",
            \"short\": true
        }, {
            \"title\": \"Status\",
            \"value\": \"Investigation In Progress\",
            \"short\": true
        }]
    }]
}"

echo "✅ Immediate response completed. Incident ID: $INCIDENT_ID" | tee -a $INCIDENT_LOG
echo "Next steps: Detailed forensic analysis and remediation" | tee -a $INCIDENT_LOG
```

### Access Review and Remediation

```bash
#!/bin/bash
# scripts/access-audit.sh

echo "🔒 Access Audit and Remediation - $(date)"
echo "=================================="

# 1. Review Kubernetes RBAC
echo "Reviewing Kubernetes access..."
kubectl get rolebindings,clusterrolebindings -A -o wide

# 2. Review database access
echo "Reviewing database access..."
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
    rolname,
    rolsuper,
    rolinherit,
    rolcreaterole,
    rolcreatedb,
    rolcanlogin,
    rolconnlimit,
    valuntil
FROM pg_roles 
WHERE rolcanlogin = true;
"

# 3. Review API tokens (if stored in database)
echo "Reviewing active API tokens..."
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
SELECT 
    user_id,
    token_name,
    created_at,
    last_used_at,
    expires_at
FROM api_tokens 
WHERE expires_at > NOW() 
ORDER BY last_used_at DESC;
" || echo "API tokens table not found"

# 4. Revoke suspicious access
echo "Revoking suspicious access..."
# Example: Revoke tokens that haven't been used in 30+ days
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
UPDATE api_tokens 
SET revoked_at = NOW() 
WHERE last_used_at < NOW() - INTERVAL '30 days' 
AND revoked_at IS NULL;
" || echo "No API tokens to revoke"

# 5. Force password reset for all users
echo "Initiating password reset for all users..."
# This would depend on your authentication system
# Example: Set password_reset_required flag
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c "
UPDATE users 
SET password_reset_required = true, 
    updated_at = NOW();
" || echo "Users table not found"

echo "✅ Access audit and remediation completed"
```

## Backup & Recovery

### Automated Backup Verification

```bash
#!/bin/bash
# scripts/backup-verification.sh

BACKUP_BUCKET="pwc-retail-backups"
VERIFICATION_LOG="/var/log/backup-verification.log"

echo "💾 Backup Verification - $(date)" | tee -a $VERIFICATION_LOG
echo "=======================================" | tee -a $VERIFICATION_LOG

# 1. Verify database backup
echo "Verifying database backup..." | tee -a $VERIFICATION_LOG

LATEST_DB_BACKUP=$(aws s3 ls s3://$BACKUP_BUCKET/database/ --recursive | sort | tail -1 | awk '{print $4}')
if [ -n "$LATEST_DB_BACKUP" ]; then
    BACKUP_SIZE=$(aws s3 ls s3://$BACKUP_BUCKET/$LATEST_DB_BACKUP | awk '{print $3}')
    BACKUP_DATE=$(aws s3 ls s3://$BACKUP_BUCKET/$LATEST_DB_BACKUP | awk '{print $1, $2}')
    echo "  Latest backup: $LATEST_DB_BACKUP" | tee -a $VERIFICATION_LOG
    echo "  Size: $BACKUP_SIZE bytes" | tee -a $VERIFICATION_LOG
    echo "  Date: $BACKUP_DATE" | tee -a $VERIFICATION_LOG
    
    # Verify backup is recent (within 24 hours)
    BACKUP_EPOCH=$(date -d "$BACKUP_DATE" +%s)
    CURRENT_EPOCH=$(date +%s)
    HOURS_DIFF=$(( (CURRENT_EPOCH - BACKUP_EPOCH) / 3600 ))
    
    if [ $HOURS_DIFF -lt 24 ]; then
        echo "  ✅ Database backup is current ($HOURS_DIFF hours old)" | tee -a $VERIFICATION_LOG
    else
        echo "  ❌ Database backup is stale ($HOURS_DIFF hours old)" | tee -a $VERIFICATION_LOG
    fi
else
    echo "  ❌ No database backup found" | tee -a $VERIFICATION_LOG
fi

# 2. Verify data lake backup
echo "Verifying data lake backup..." | tee -a $VERIFICATION_LOG

DATA_LAKE_OBJECTS=$(aws s3 ls s3://$BACKUP_BUCKET/data-lake/ --recursive | wc -l)
if [ $DATA_LAKE_OBJECTS -gt 0 ]; then
    echo "  ✅ Data lake backup contains $DATA_LAKE_OBJECTS objects" | tee -a $VERIFICATION_LOG
else
    echo "  ❌ No data lake backup objects found" | tee -a $VERIFICATION_LOG
fi

# 3. Verify configuration backup
echo "Verifying configuration backup..." | tee -a $VERIFICATION_LOG

LATEST_CONFIG_BACKUP=$(aws s3 ls s3://$BACKUP_BUCKET/config/ --recursive | sort | tail -1 | awk '{print $4}')
if [ -n "$LATEST_CONFIG_BACKUP" ]; then
    echo "  ✅ Latest configuration backup: $LATEST_CONFIG_BACKUP" | tee -a $VERIFICATION_LOG
else
    echo "  ❌ No configuration backup found" | tee -a $VERIFICATION_LOG
fi

# 4. Test backup restore (on test database)
echo "Testing backup restore..." | tee -a $VERIFICATION_LOG

TEST_DB_HOST="test-postgres.database.svc.cluster.local"
if nc -z $TEST_DB_HOST 5432; then
    echo "  Testing restore on test database..." | tee -a $VERIFICATION_LOG
    
    # Download and restore latest backup to test database
    aws s3 cp s3://$BACKUP_BUCKET/$LATEST_DB_BACKUP /tmp/test-backup.sql.gz
    gunzip /tmp/test-backup.sql.gz
    
    # Create test database
    psql -h $TEST_DB_HOST -U postgres -c "DROP DATABASE IF EXISTS backup_test;"
    psql -h $TEST_DB_HOST -U postgres -c "CREATE DATABASE backup_test;"
    
    # Restore backup
    psql -h $TEST_DB_HOST -U postgres -d backup_test -f /tmp/test-backup.sql > /tmp/restore-test.log 2>&1
    
    # Verify restore
    RESTORED_TABLES=$(psql -h $TEST_DB_HOST -U postgres -d backup_test -t -c "SELECT count(*) FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog');")
    
    if [ $RESTORED_TABLES -gt 0 ]; then
        echo "  ✅ Backup restore test successful ($RESTORED_TABLES tables restored)" | tee -a $VERIFICATION_LOG
    else
        echo "  ❌ Backup restore test failed" | tee -a $VERIFICATION_LOG
        cat /tmp/restore-test.log | tee -a $VERIFICATION_LOG
    fi
    
    # Cleanup
    rm -f /tmp/test-backup.sql /tmp/restore-test.log
    psql -h $TEST_DB_HOST -U postgres -c "DROP DATABASE backup_test;"
else
    echo "  ⚠️  Test database not available, skipping restore test" | tee -a $VERIFICATION_LOG
fi

echo "✅ Backup verification completed" | tee -a $VERIFICATION_LOG
```

### Disaster Recovery Procedure

```bash
#!/bin/bash
# scripts/disaster-recovery.sh

DR_LOG="/var/log/disaster-recovery-$(date +%Y%m%d-%H%M%S).log"
BACKUP_BUCKET="pwc-retail-backups"

echo "🚨 DISASTER RECOVERY PROCEDURE" | tee -a $DR_LOG
echo "Started at: $(date)" | tee -a $DR_LOG
echo "Operator: $(whoami)" | tee -a $DR_LOG
echo "=======================================" | tee -a $DR_LOG

# Confirm disaster recovery
read -p "Are you sure you want to initiate disaster recovery? (yes/no): " confirm
if [ "$confirm" != "yes" ]; then
    echo "Disaster recovery cancelled" | tee -a $DR_LOG
    exit 1
fi

# 1. Restore database from backup
echo "STEP 1: Restoring database from backup..." | tee -a $DR_LOG

LATEST_DB_BACKUP=$(aws s3 ls s3://$BACKUP_BUCKET/database/ --recursive | sort | tail -1 | awk '{print $4}')
if [ -n "$LATEST_DB_BACKUP" ]; then
    echo "  Downloading backup: $LATEST_DB_BACKUP" | tee -a $DR_LOG
    aws s3 cp s3://$BACKUP_BUCKET/$LATEST_DB_BACKUP /tmp/restore-backup.sql.gz
    gunzip /tmp/restore-backup.sql.gz
    
    echo "  Restoring database..." | tee -a $DR_LOG
    psql -h $DB_HOST -U $DB_USER -c "DROP DATABASE IF EXISTS retail_backup;"
    psql -h $DB_HOST -U $DB_USER -c "CREATE DATABASE retail_backup;"
    psql -h $DB_HOST -U $DB_USER -d retail_backup -f /tmp/restore-backup.sql
    
    echo "  Switching to restored database..." | tee -a $DR_LOG
    psql -h $DB_HOST -U $DB_USER -c "ALTER DATABASE retail RENAME TO retail_corrupted;"
    psql -h $DB_HOST -U $DB_USER -c "ALTER DATABASE retail_backup RENAME TO retail;"
    
    echo "  ✅ Database restoration completed" | tee -a $DR_LOG
else
    echo "  ❌ No database backup found" | tee -a $DR_LOG
    exit 1
fi

# 2. Restore data lake
echo "STEP 2: Restoring data lake..." | tee -a $DR_LOG

aws s3 sync s3://$BACKUP_BUCKET/data-lake/ s3://pwc-retail-data/ --delete
echo "  ✅ Data lake restoration completed" | tee -a $DR_LOG

# 3. Redeploy applications
echo "STEP 3: Redeploying applications..." | tee -a $DR_LOG

# Redeploy API service
kubectl rollout restart deployment/api-service -n pwc-retail
kubectl rollout status deployment/api-service -n pwc-retail

# Redeploy ETL services
kubectl rollout restart deployment/dagster-daemon -n pwc-retail
kubectl rollout status deployment/dagster-daemon -n pwc-retail

echo "  ✅ Application redeployment completed" | tee -a $DR_LOG

# 4. Verify system health
echo "STEP 4: Verifying system health..." | tee -a $DR_LOG

# Wait for services to be ready
sleep 60

# Check API health
if curl -f -s https://api.pwc-retail.com/health > /dev/null; then
    echo "  ✅ API service is healthy" | tee -a $DR_LOG
else
    echo "  ❌ API service health check failed" | tee -a $DR_LOG
fi

# Check database connectivity
if psql -h $DB_HOST -U $DB_USER -d retail -c "SELECT 1;" > /dev/null; then
    echo "  ✅ Database is accessible" | tee -a $DR_LOG
else
    echo "  ❌ Database connectivity failed" | tee -a $DR_LOG
fi

# Check data integrity
ROW_COUNT=$(psql -h $DB_HOST -U $DB_USER -d retail -t -c "SELECT count(*) FROM silver.retail_transactions;")
echo "  Data integrity check: $ROW_COUNT rows in main table" | tee -a $DR_LOG

# 5. Restart ETL pipelines
echo "STEP 5: Restarting ETL pipelines..." | tee -a $DR_LOG

curl -X POST "http://dagster.pwc-retail.svc.cluster.local:3000/api/runs" \
    -H "Content-Type: application/json" \
    -d '{
        "pipeline_name": "full_data_refresh_pipeline",
        "tags": {"trigger": "disaster_recovery"}
    }'

echo "  ✅ ETL pipelines restarted" | tee -a $DR_LOG

# 6. Notify stakeholders
echo "STEP 6: Notifying stakeholders..." | tee -a $DR_LOG

curl -X POST $SLACK_WEBHOOK -H 'Content-type: application/json' --data "{
    \"text\": \"🟢 DISASTER RECOVERY COMPLETED\",
    \"attachments\": [{
        \"color\": \"good\",
        \"fields\": [{
            \"title\": \"Status\",
            \"value\": \"System restored and operational\",
            \"short\": true
        }, {
            \"title\": \"Recovery Time\",
            \"value\": \"$(date)\",
            \"short\": true
        }]
    }]
}"

echo "🎉 DISASTER RECOVERY COMPLETED SUCCESSFULLY" | tee -a $DR_LOG
echo "Recovery completed at: $(date)" | tee -a $DR_LOG

# Cleanup
rm -f /tmp/restore-backup.sql
```

## Escalation Procedures

### Escalation Matrix

| Incident Severity | Initial Response | Escalation Level 1 | Escalation Level 2 | Escalation Level 3 |
|-------------------|------------------|--------------------|--------------------|-------------------|
| **P0 - Critical** | On-Call Engineer | Platform Owner | CTO | CEO |
| **P1 - High** | On-Call Engineer | Platform Owner | VP Engineering | - |
| **P2 - Medium** | On-Call Engineer | Team Lead | - | - |
| **P3 - Low** | Assigned Engineer | - | - | - |

### Escalation Triggers

#### Automatic Escalation

```bash
#!/bin/bash
# scripts/auto-escalation.sh

INCIDENT_ID=$1
SEVERITY=$2
START_TIME=$3

CURRENT_TIME=$(date +%s)
ELAPSED_TIME=$((CURRENT_TIME - START_TIME))

case $SEVERITY in
    "P0")
        if [ $ELAPSED_TIME -gt 1800 ]; then  # 30 minutes
            echo "P0 incident auto-escalating to Level 2 after 30 minutes"
            # Notify CTO
            curl -X POST $CTO_NOTIFICATION_WEBHOOK --data "{\"incident_id\": \"$INCIDENT_ID\", \"severity\": \"$SEVERITY\", \"escalation_level\": 2}"
        fi
        ;;
    "P1")
        if [ $ELAPSED_TIME -gt 3600 ]; then  # 1 hour
            echo "P1 incident auto-escalating to Level 2 after 1 hour"
            # Notify VP Engineering
            curl -X POST $VP_ENG_NOTIFICATION_WEBHOOK --data "{\"incident_id\": \"$INCIDENT_ID\", \"severity\": \"$SEVERITY\", \"escalation_level\": 2}"
        fi
        ;;
esac
```

### Communication Templates

#### Incident Update Template

```markdown
**INCIDENT UPDATE - P[0/1/2/3] - [INCIDENT_ID]**

**Time**: [Current Time]
**Duration**: [Time since incident start]
**Status**: [In Progress/Resolved/Investigating]

**Summary**: [Brief update on what's happening]

**Actions Taken**:
- [List of actions completed]
- [Include timestamps]

**Current Hypothesis**: [What we think is causing the issue]

**Next Steps**:
- [What we're doing next]
- [ETA for next update]

**Impact Assessment**:
- Affected Users: [Number/percentage]
- Services Down: [List of affected services]
- Revenue Impact: [If applicable]

**Incident Commander**: [Name]
**Next Update**: [Time]
```

#### Resolution Template

```markdown
**INCIDENT RESOLVED - P[0/1/2/3] - [INCIDENT_ID]**

**Resolution Time**: [Time]
**Total Duration**: [Duration from start to resolution]
**Final Status**: RESOLVED

**Root Cause**: [Detailed explanation of what caused the incident]

**Resolution Actions**:
- [List of actions that resolved the issue]
- [Include who did what and when]

**Prevention Measures**:
- [What we're doing to prevent this in the future]
- [Any process improvements]

**Follow-up Items**:
- [ ] Post-mortem meeting scheduled
- [ ] Documentation updates
- [ ] Process improvements
- [ ] Technical improvements

**Incident Commander**: [Name]
**Post-mortem**: [Date/time of post-mortem meeting]
```

For additional operational procedures and troubleshooting guides, see:
- [Database Operations Runbook](./DATABASE_OPERATIONS.md)
- [Monitoring and Alerting Guide](./MONITORING_GUIDE.md)
- [Security Incident Response](./SECURITY_INCIDENT_RESPONSE.md)
# Supabase Integration Guide

Complete guide for integrating your PwC Data Engineering Challenge project with Supabase (PostgreSQL).

## Table of Contents
- [Prerequisites](#prerequisites)
- [Supabase Project Setup](#supabase-project-setup)
- [Environment Configuration](#environment-configuration)
- [Database Schema Creation](#database-schema-creation)
- [API Endpoints](#api-endpoints)
- [Data Migration](#data-migration)
- [Security & Performance](#security--performance)
- [Troubleshooting](#troubleshooting)

## Prerequisites

1. **Supabase Account**: Sign up at [supabase.com](https://supabase.com)
2. **Project Dependencies**: Ensure your project has PostgreSQL driver
```bash
poetry install  # Already includes psycopg2-binary
```

## Supabase Project Setup

### 1. Create New Project
1. Go to [Supabase Dashboard](https://app.supabase.com)
2. Click "New Project"
3. Choose your organization
4. Set project details:
   - **Name**: `pwc-retail-etl`
   - **Database Password**: Generate strong password
   - **Region**: Choose closest to your location
5. Wait for project creation (2-3 minutes)

### 2. Get Project Credentials
After project creation, navigate to **Settings > API**:

- **Project URL**: `https://[project-id].supabase.co`
- **Project API Key** (anon public): `eyJ...` (for client connections)
- **Service Role Key**: `eyJ...` (for server operations)

Navigate to **Settings > Database**:
- **Connection String**: `postgresql://postgres:[password]@db.[project-id].supabase.co:5432/postgres`

## Environment Configuration

### 1. Update `.env` File

Create or update your `.env` file with Supabase configuration:

```bash
# Environment
ENVIRONMENT=production

# Database Configuration - Switch to PostgreSQL
DATABASE_TYPE=postgresql
DATABASE_URL=postgresql://postgres:[YOUR_PASSWORD]@db.[PROJECT_ID].supabase.co:5432/postgres?sslmode=require

# Supabase Integration
SUPABASE_URL=https://[PROJECT_ID].supabase.co
SUPABASE_KEY=eyJ[YOUR_ANON_KEY]
SUPABASE_SERVICE_KEY=eyJ[YOUR_SERVICE_KEY]
SUPABASE_SCHEMA=retail_dwh
ENABLE_SUPABASE_RLS=true

# Keep existing configuration
BASIC_AUTH_USERNAME=admin
BASIC_AUTH_PASSWORD=your_secure_password
TYPESENSE_API_KEY=your_typesense_key

# Spark Configuration (optimized for cloud)
SPARK_MASTER=local[2]
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=2g
SPARK_SQL_SHUFFLE_PARTITIONS=4
```

### 2. Verify Connection

Test your Supabase connection:

```bash
# Start the API
poetry run uvicorn de_challenge.api.main:app --host 0.0.0.0 --port 8000

# Test Supabase endpoints
curl -u admin:your_password "http://localhost:8000/api/v1/supabase/connection"
curl -u admin:your_password "http://localhost:8000/api/v1/supabase/health"
```

## Database Schema Creation

### 1. Automatic Schema Creation

The API automatically creates the star schema on startup. You can also trigger it manually:

```bash
# Create schema
curl -u admin:your_password -X POST "http://localhost:8000/api/v1/supabase/schema/create"

# Create all tables
curl -u admin:your_password -X POST "http://localhost:8000/api/v1/supabase/tables/create"
```

### 2. Manual Schema Creation (Optional)

If you prefer manual setup, connect to your Supabase project using SQL Editor:

```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS retail_dwh;
SET search_path TO retail_dwh, public;

-- Enable Row Level Security (optional but recommended)
ALTER DEFAULT PRIVILEGES IN SCHEMA retail_dwh 
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO authenticated;
```

The star schema tables will be created automatically by SQLModel when the API starts.

## API Endpoints

### Supabase Management Endpoints

All endpoints require Basic Authentication:

```bash
# Health check with comprehensive database info
GET /api/v1/supabase/health

# Test basic connection
GET /api/v1/supabase/connection

# Get table statistics
GET /api/v1/supabase/statistics

# Validate data integrity
POST /api/v1/supabase/integrity/validate

# Create database schema
POST /api/v1/supabase/schema/create

# Create all star schema tables
POST /api/v1/supabase/tables/create

# Get current configuration
GET /api/v1/supabase/config
```

### Example API Usage

```bash
# Comprehensive health check
curl -u admin:password "http://localhost:8000/api/v1/supabase/health" | jq

# Response example:
{
  "status": "healthy",
  "connection": {
    "status": "connected",
    "database": "postgres",
    "user": "postgres",
    "version": "PostgreSQL 15.1",
    "ssl_enabled": true
  },
  "tables": {
    "fact_sale": {"row_count": 1000, "total_size": "128 kB"},
    "dim_product": {"row_count": 50, "total_size": "16 kB"},
    "dim_customer": {"row_count": 200, "total_size": "32 kB"},
    "dim_country": {"row_count": 10, "total_size": "8192 bytes"},
    "dim_invoice": {"row_count": 300, "total_size": "24 kB"},
    "dim_date": {"row_count": 365, "total_size": "40 kB"}
  },
  "integrity": {
    "status": "passed",
    "checks": [
      "Orphaned fact records (missing product): PASSED",
      "Orphaned fact records (missing customer): PASSED",
      "Zero or negative prices: PASSED"
    ]
  }
}
```

## Data Migration

### 1. ETL Pipeline with Supabase

The ETL pipeline automatically detects Supabase and optimizes accordingly:

```bash
# Run complete ETL pipeline
poetry run python scripts/run_etl.py

# The pipeline will:
# 1. Process bronze layer (local files)
# 2. Clean data in silver layer
# 3. Load star schema to Supabase (gold layer)
```

### 2. Manual Data Loading

If you have existing data to migrate:

```python
from de_challenge.data_access.supabase_client import get_supabase_client

# Example: Load dimension data
client = get_supabase_client()
async with client.transaction() as session:
    # Your data loading logic here
    pass
```

### 3. Verify Data Loading

```bash
# Check table statistics after loading
curl -u admin:password "http://localhost:8000/api/v1/supabase/statistics"

# Validate data integrity
curl -u admin:password -X POST "http://localhost:8000/api/v1/supabase/integrity/validate"
```

## Security & Performance

### 1. Row Level Security (RLS)

Enable RLS for production environments:

```sql
-- In Supabase SQL Editor
ALTER TABLE retail_dwh.fact_sale ENABLE ROW LEVEL SECURITY;
ALTER TABLE retail_dwh.dim_product ENABLE ROW LEVEL SECURITY;
ALTER TABLE retail_dwh.dim_customer ENABLE ROW LEVEL SECURITY;
ALTER TABLE retail_dwh.dim_country ENABLE ROW LEVEL SECURITY;
ALTER TABLE retail_dwh.dim_invoice ENABLE ROW LEVEL SECURITY;
ALTER TABLE retail_dwh.dim_date ENABLE ROW LEVEL SECURITY;

-- Create policies as needed
CREATE POLICY "Public read access" ON retail_dwh.fact_sale
    FOR SELECT USING (true);
```

### 2. Connection Pooling

The Supabase client includes connection pooling:

```python
# Automatic connection pooling configuration
pool_size=10,
max_overflow=20,
pool_pre_ping=True,
pool_recycle=3600
```

### 3. Performance Indexes

Indexes are created automatically for optimal query performance:

```sql
-- Fact table indexes (created automatically)
CREATE INDEX idx_fact_sale_product_key ON fact_sale(product_key);
CREATE INDEX idx_fact_sale_customer_key ON fact_sale(customer_key);
-- ... and more
```

### 4. SSL Configuration

SSL is enforced for all Supabase connections:

```bash
DATABASE_URL=postgresql://user:pass@host:5432/db?sslmode=require
```

## Troubleshooting

### Common Issues

#### 1. Connection Timeout
```bash
# Error: Connection timeout
# Solution: Check your network and Supabase project status
curl -u admin:password "http://localhost:8000/api/v1/supabase/connection"
```

#### 2. SSL Certificate Issues
```bash
# Error: SSL certificate verify failed
# Solution: Ensure sslmode=require in DATABASE_URL
DATABASE_URL=postgresql://user:pass@host:5432/db?sslmode=require
```

#### 3. Schema Permission Issues
```bash
# Error: permission denied for schema
# Solution: Use service role key for admin operations
SUPABASE_SERVICE_KEY=your_service_role_key
```

#### 4. Row Level Security Blocking Access
```sql
-- Disable RLS temporarily for testing
ALTER TABLE retail_dwh.fact_sale DISABLE ROW LEVEL SECURITY;
```

### Debug Commands

```bash
# Test configuration
curl -u admin:password "http://localhost:8000/api/v1/supabase/config"

# Check connection details
curl -u admin:password "http://localhost:8000/api/v1/supabase/connection"

# Validate data integrity
curl -u admin:password -X POST "http://localhost:8000/api/v1/supabase/integrity/validate"

# Check API logs
docker compose logs -f api
```

### Performance Monitoring

```bash
# Monitor table sizes and performance
curl -u admin:password "http://localhost:8000/api/v1/supabase/statistics" | jq '.tables'

# Check query performance in Supabase Dashboard
# Go to Database > Logs to see slow queries
```

## Migration from SQLite

To migrate from SQLite to Supabase:

1. **Backup existing data**:
```bash
cp data/warehouse/retail.db data/warehouse/retail_backup.db
```

2. **Update environment**:
```bash
# Change DATABASE_TYPE from sqlite to postgresql
DATABASE_TYPE=postgresql
DATABASE_URL=postgresql://postgres:password@db.project.supabase.co:5432/postgres?sslmode=require
```

3. **Restart API and verify**:
```bash
docker compose restart api
curl -u admin:password "http://localhost:8000/api/v1/supabase/health"
```

4. **Re-run ETL if needed**:
```bash
poetry run python scripts/run_etl.py
```

## Production Checklist

- [ ] Strong database password set
- [ ] Service role key secured
- [ ] Row Level Security policies configured
- [ ] SSL mode enabled (`sslmode=require`)
- [ ] Connection pooling configured
- [ ] Performance indexes created
- [ ] Data integrity validated
- [ ] Backup strategy implemented
- [ ] Monitoring alerts configured
- [ ] API authentication secured

## Support

- **Supabase Documentation**: [docs.supabase.com](https://docs.supabase.com)
- **PostgreSQL Documentation**: [postgresql.org/docs](https://www.postgresql.org/docs/)
- **Project Issues**: Check API logs and use health check endpoints

---

Your PwC Data Engineering Challenge project is now enterprise-ready with Supabase integration! 🚀
-- PWC Retail Data Platform - PRODUCTION Database Initialization

-- Include base initialization
\i /docker-entrypoint-initdb.d/init-db.sql

-- Production-specific configurations
SET shared_preload_libraries = 'pg_stat_statements';
SET log_min_duration_statement = 1000;  -- Log slow queries (>1s)
SET log_checkpoints = on;
SET log_connections = on;
SET log_disconnections = on;

-- Performance tuning for production
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET maintenance_work_mem = '64MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.7;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = 100;

-- Create production user with limited privileges
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'prod_app_user') THEN
        CREATE ROLE prod_app_user WITH LOGIN PASSWORD 'CHANGE_IN_PRODUCTION';
    END IF;
END
$$;

-- Grant specific permissions for production user
GRANT USAGE ON SCHEMA bronze TO prod_app_user;
GRANT USAGE ON SCHEMA silver TO prod_app_user;
GRANT USAGE ON SCHEMA gold TO prod_app_user;
GRANT USAGE ON SCHEMA metrics TO prod_app_user;
GRANT USAGE ON SCHEMA audit TO prod_app_user;

-- Grant table-level permissions (more restrictive)
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA bronze TO prod_app_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA silver TO prod_app_user;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO prod_app_user;  -- Read-only for gold
GRANT INSERT ON ALL TABLES IN SCHEMA metrics TO prod_app_user;
GRANT INSERT ON ALL TABLES IN SCHEMA audit TO prod_app_user;

-- Create production monitoring views
CREATE OR REPLACE VIEW metrics.production_health AS
SELECT 
    'etl_pipeline' as component,
    CASE 
        WHEN COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '1 hour' AND status = 'failed') > 0 
        THEN 'unhealthy'
        WHEN COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') = 0
        THEN 'no_activity'
        ELSE 'healthy'
    END as status,
    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') as runs_24h,
    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours' AND status = 'failed') as failures_24h
FROM metrics.etl_runs;

-- Create production data quality monitoring
CREATE TABLE IF NOT EXISTS metrics.data_quality_checks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR(255) NOT NULL,
    check_name VARCHAR(255) NOT NULL,
    check_result BOOLEAN NOT NULL,
    check_value NUMERIC,
    threshold_value NUMERIC,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for production performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_runs_created_at_status 
ON metrics.etl_runs(created_at, status);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_data_lineage_created_at 
ON audit.data_lineage(created_at);

-- Create production backup verification
CREATE OR REPLACE FUNCTION prod.verify_backup_integrity() RETURNS TABLE (
    backup_type TEXT,
    last_backup TIMESTAMP,
    status TEXT
) AS $$
BEGIN
    -- This would integrate with your backup system
    RETURN QUERY
    SELECT 
        'database'::TEXT,
        NOW() - INTERVAL '1 day'::TIMESTAMP,  -- Placeholder
        'needs_implementation'::TEXT;
END;
$$ LANGUAGE plpgsql;

-- Security: Revoke public schema access in production
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO prod_app_user;
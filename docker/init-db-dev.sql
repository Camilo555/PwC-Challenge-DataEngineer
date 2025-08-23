-- PWC Retail Data Platform - DEV Database Initialization

-- Include base initialization
\i /docker-entrypoint-initdb.d/init-db.sql

-- Development-specific configurations
SET shared_preload_libraries = 'pg_stat_statements';
SET log_statement = 'all';  -- Log all statements for debugging
SET log_duration = on;      -- Log query durations

-- Create development user
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'dev_user') THEN
        CREATE ROLE dev_user WITH LOGIN PASSWORD 'dev_password' CREATEDB;
    END IF;
END
$$;

-- Grant permissions for dev user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO dev_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO dev_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO dev_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA metrics TO dev_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA audit TO dev_user;

-- Create development views for easier debugging
CREATE OR REPLACE VIEW metrics.etl_summary AS
SELECT 
    pipeline_name,
    stage,
    COUNT(*) as total_runs,
    COUNT(*) FILTER (WHERE status = 'success') as successful_runs,
    COUNT(*) FILTER (WHERE status = 'failed') as failed_runs,
    AVG(duration_seconds) as avg_duration,
    MAX(created_at) as last_run
FROM metrics.etl_runs 
GROUP BY pipeline_name, stage
ORDER BY pipeline_name, stage;

-- Create function for resetting dev data
CREATE OR REPLACE FUNCTION dev.reset_test_data() RETURNS VOID AS $$
BEGIN
    TRUNCATE TABLE bronze.test_retail_data CASCADE;
    TRUNCATE TABLE metrics.etl_runs CASCADE;
    TRUNCATE TABLE audit.data_lineage CASCADE;
    
    -- Re-insert sample data
    INSERT INTO bronze.test_retail_data (
        invoice_no, stock_code, description, quantity, 
        invoice_date, unit_price, customer_id, country
    ) VALUES 
        ('DEV001', 'ITEM001', 'Dev Product 1', 100, '2024-01-01 10:00:00', 9.99, 'CUST001', 'United Kingdom'),
        ('DEV002', 'ITEM002', 'Dev Product 2', 50, '2024-01-01 11:00:00', 19.99, 'CUST002', 'Germany'),
        ('DEV003', 'ITEM003', 'Dev Product 3', 25, '2024-01-01 12:00:00', 29.99, 'CUST003', 'France');
        
    RAISE NOTICE 'Development test data reset successfully';
END;
$$ LANGUAGE plpgsql;
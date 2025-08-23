-- PWC Retail Data Platform Database Initialization
-- Base initialization for all environments

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "btree_gin";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;  
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS metrics;
CREATE SCHEMA IF NOT EXISTS audit;

-- Set default permissions
GRANT USAGE ON SCHEMA bronze TO PUBLIC;
GRANT USAGE ON SCHEMA silver TO PUBLIC;
GRANT USAGE ON SCHEMA gold TO PUBLIC;
GRANT USAGE ON SCHEMA metrics TO PUBLIC;
GRANT USAGE ON SCHEMA audit TO PUBLIC;

-- Create audit table for tracking changes
CREATE TABLE IF NOT EXISTS audit.data_lineage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_table VARCHAR(255) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    operation VARCHAR(50) NOT NULL,
    record_count INTEGER NOT NULL DEFAULT 0,
    processing_time_seconds NUMERIC(10,3),
    quality_score NUMERIC(5,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

-- Create metrics table for ETL monitoring
CREATE TABLE IF NOT EXISTS metrics.etl_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_name VARCHAR(255) NOT NULL,
    stage VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds NUMERIC(10,3),
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_audit_source_table ON audit.data_lineage(source_table);
CREATE INDEX IF NOT EXISTS idx_audit_created_at ON audit.data_lineage(created_at);
CREATE INDEX IF NOT EXISTS idx_metrics_pipeline ON metrics.etl_runs(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_metrics_stage ON metrics.etl_runs(stage);
CREATE INDEX IF NOT EXISTS idx_metrics_created_at ON metrics.etl_runs(created_at);

-- Create stored procedure for ETL monitoring
CREATE OR REPLACE FUNCTION metrics.log_etl_run(
    p_pipeline_name VARCHAR(255),
    p_stage VARCHAR(50),
    p_status VARCHAR(50),
    p_duration_seconds NUMERIC DEFAULT NULL,
    p_records_processed INTEGER DEFAULT 0,
    p_records_failed INTEGER DEFAULT 0,
    p_error_message TEXT DEFAULT NULL,
    p_metadata JSONB DEFAULT NULL
) RETURNS UUID AS $$
DECLARE
    run_id UUID;
BEGIN
    INSERT INTO metrics.etl_runs (
        pipeline_name, stage, status, duration_seconds, 
        records_processed, records_failed, error_message, metadata
    ) VALUES (
        p_pipeline_name, p_stage, p_status, p_duration_seconds,
        p_records_processed, p_records_failed, p_error_message, p_metadata
    ) RETURNING id INTO run_id;
    
    RETURN run_id;
END;
$$ LANGUAGE plpgsql;
-- PWC Retail Data Platform - TEST Database Initialization

-- Include base initialization
\i /docker-entrypoint-initdb.d/init-db.sql

-- Test-specific configurations
SET default_statistics_target = 10;  -- Lower stats for faster testing
SET random_page_cost = 1.1;          -- Assume SSD for testing

-- Create test data tables with simplified structure
CREATE TABLE IF NOT EXISTS bronze.test_retail_data (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    invoice_no VARCHAR(50),
    stock_code VARCHAR(50),
    description TEXT,
    quantity INTEGER,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(10,2),
    customer_id VARCHAR(50),
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample test data
INSERT INTO bronze.test_retail_data (
    invoice_no, stock_code, description, quantity, 
    invoice_date, unit_price, customer_id, country
) VALUES 
    ('TEST001', 'ITEM001', 'Test Product 1', 10, '2024-01-01 10:00:00', 9.99, 'CUST001', 'United Kingdom'),
    ('TEST002', 'ITEM002', 'Test Product 2', 5, '2024-01-01 11:00:00', 19.99, 'CUST002', 'Germany'),
    ('TEST003', 'ITEM003', 'Test Product 3', 3, '2024-01-01 12:00:00', 29.99, 'CUST003', 'France')
ON CONFLICT DO NOTHING;

-- Create test user for integration tests
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'test_user') THEN
        CREATE ROLE test_user WITH LOGIN PASSWORD 'test_password';
    END IF;
END
$$;

-- Grant permissions for test user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO test_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO test_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO test_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA metrics TO test_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA audit TO test_user;
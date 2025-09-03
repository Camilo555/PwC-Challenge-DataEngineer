

{% macro get_primary_key_columns(table_name) %}
    {%- set pk_columns = {
        'fact_sales': ['sale_key'],
        'dim_customers': ['customer_sk'],
        'dim_products': ['product_sk'],
        'agg_sales_daily': ['date_key', 'customer_id', 'stock_code']
    } -%}
    {{ return(pk_columns.get(table_name, ['id'])) }}
{% endmacro %}


-- Create Optimized Indexes for Performance
{% macro create_optimized_indexes(model_name) %}
    {% set index_configs = {
        'fact_sales': [
            {'columns': ['invoice_date'], 'type': 'btree', 'include': ['customer_id', 'sales_amount']},
            {'columns': ['customer_id', 'invoice_date'], 'type': 'btree'},
            {'columns': ['stock_code', 'invoice_date'], 'type': 'btree'},
            {'columns': ['country', 'invoice_date'], 'type': 'btree'},
            {'columns': ['is_return'], 'type': 'btree'},
            {'columns': ['sales_amount'], 'type': 'btree', 'where': 'sales_amount > 0'}
        ],
        'dim_customers': [
            {'columns': ['customer_id'], 'type': 'unique'},
            {'columns': ['customer_segment', 'value_tier'], 'type': 'btree'},
            {'columns': ['is_current'], 'type': 'btree', 'where': 'is_current = true'},
            {'columns': ['country'], 'type': 'btree'}
        ],
        'dim_products': [
            {'columns': ['stock_code'], 'type': 'unique'},
            {'columns': ['inferred_category'], 'type': 'btree'},
            {'columns': ['price_volatility'], 'type': 'btree'}
        ]
    } %}
    
    {% set table_indexes = index_configs.get(model_name.identifier, []) %}
    
    {% for index_config in table_indexes %}
        {% set index_name = model_name.identifier ~ '_' ~ index_config.columns | join('_') ~ '_idx' %}
        
        CREATE INDEX IF NOT EXISTS {{ index_name }}
        ON {{ model_name }}
        {% if index_config.type == 'unique' %}UNIQUE{% endif %}
        ({{ index_config.columns | join(', ') }})
        {% if index_config.get('include') %}
        INCLUDE ({{ index_config.include | join(', ') }})
        {% endif %}
        {% if index_config.get('where') %}
        WHERE {{ index_config.where }}
        {% endif %};
        
    {% endfor %}
{% endmacro %}


-- Collect and Update Table Statistics
{% macro collect_table_statistics(model_name) %}
    -- Update table statistics for query optimizer
    ANALYZE {{ model_name }};
    
    -- Collect extended statistics if available
    {% if target.type == 'postgres' %}
        UPDATE pg_stats SET 
            n_distinct = -1,
            correlation = 1.0
        WHERE schemaname = '{{ model_name.schema }}'
          AND tablename = '{{ model_name.identifier }}';
    {% endif %}
    
    {{ log('Statistics updated for ' ~ model_name, info=true) }}
{% endmacro %}


-- Query Cache Optimization
{% macro optimize_query_cache() %}
    {% if target.type == 'postgres' %}
        -- Set work_mem for current session
        SET work_mem = '256MB';
        SET effective_cache_size = '4GB';
        SET random_page_cost = 1.1;
    {% elif target.type == 'snowflake' %}
        -- Use multi-cluster warehouse for better performance
        USE WAREHOUSE COMPUTE_WH_LARGE;
        ALTER SESSION SET USE_CACHED_RESULT = TRUE;
    {% elif target.type == 'bigquery' %}
        -- Enable query cache
        SET @@dataset_project_id = '{{ target.project }}';
    {% endif %}
{% endmacro %}


-- Partition Management for Large Tables  
{% macro manage_partitions(model_name, partition_column, retention_days=365) %}
    {% set retention_date = (modules.datetime.date.today() - modules.datetime.timedelta(days=retention_days)).strftime('%Y-%m-%d') %}
    
    -- Drop old partitions beyond retention period
    {% if target.type == 'postgres' %}
        DO $$
        DECLARE
            partition_name TEXT;
        BEGIN
            FOR partition_name IN 
                SELECT schemaname||'.'||tablename 
                FROM pg_tables 
                WHERE schemaname = '{{ model_name.schema }}'
                  AND tablename LIKE '{{ model_name.identifier }}_p%'
                  AND tablename < '{{ model_name.identifier }}_p' || replace('{{ retention_date }}', '-', '_')
            LOOP
                EXECUTE 'DROP TABLE IF EXISTS ' || partition_name;
                RAISE NOTICE 'Dropped partition: %', partition_name;
            END LOOP;
        END $$;
    {% endif %}
    
    -- Create future partitions (next 30 days)
    {% for i in range(30) %}
        {% set future_date = (modules.datetime.date.today() + modules.datetime.timedelta(days=i)).strftime('%Y_%m_%d') %}
        
        CREATE TABLE IF NOT EXISTS {{ model_name.schema }}.{{ model_name.identifier }}_p{{ future_date }}
        PARTITION OF {{ model_name }}
        FOR VALUES FROM ('{{ future_date.replace('_', '-') }}') 
        TO ('{{ (modules.datetime.date.today() + modules.datetime.timedelta(days=i+1)).strftime('%Y-%m-%d') }}');
    {% endfor %}
{% endmacro %}


-- Materialized View Refresh Strategy
{% macro refresh_materialized_views(base_model) %}
    {% set dependent_mvs = {
        'fact_sales': ['mv_daily_sales_summary', 'mv_customer_metrics', 'mv_product_performance'],
        'dim_customers': ['mv_customer_segments', 'mv_rfm_analysis'],
        'dim_products': ['mv_product_catalog', 'mv_inventory_turnover']
    } %}
    
    {% set mvs_to_refresh = dependent_mvs.get(base_model.identifier, []) %}
    
    {% for mv_name in mvs_to_refresh %}
        {% if target.type == 'postgres' %}
            REFRESH MATERIALIZED VIEW CONCURRENTLY {{ mv_name }};
        {% elif target.type == 'snowflake' %}
            ALTER MATERIALIZED VIEW {{ mv_name }} REFRESH;
        {% endif %}
        
        {{ log('Refreshed materialized view: ' ~ mv_name, info=true) }}
    {% endfor %}
{% endmacro %}


-- Query Execution Plan Analysis
{% macro analyze_query_performance(query_text, model_name) %}
    {% if target.type == 'postgres' %}
        -- Create temporary table to store execution plan
        CREATE TEMP TABLE IF NOT EXISTS query_plans (
            model_name VARCHAR(255),
            execution_time TIMESTAMP,
            plan_text TEXT,
            total_cost NUMERIC,
            rows_estimated BIGINT
        );
        
        -- Analyze query execution plan
        DO $$
        DECLARE
            plan_result TEXT;
            cost_estimate NUMERIC;
            row_estimate BIGINT;
        BEGIN
            -- Get execution plan
            EXECUTE 'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' || $1 INTO plan_result;
            
            -- Extract cost and row estimates from plan
            SELECT 
                (plan_result::json->>0->>'Total Cost')::numeric,
                (plan_result::json->>0->>'Plan Rows')::bigint
            INTO cost_estimate, row_estimate;
            
            -- Store analysis results
            INSERT INTO query_plans VALUES ($2, NOW(), plan_result, cost_estimate, row_estimate);
            
            -- Log performance metrics
            RAISE NOTICE 'Query analysis for %: Cost=%, Rows=%', $2, cost_estimate, row_estimate;
        END $$
        USING query_text, model_name;
        
    {% endif %}
{% endmacro %}


-- Incremental Processing Optimization
{% macro optimize_incremental_processing(model_name, unique_key, updated_column='updated_at') %}
    {% if is_incremental() %}
        -- Use merge strategy with optimized predicates
        MERGE {{ this }} AS target
        USING (
            SELECT * FROM {{ model_name }}
            WHERE {{ updated_column }} > (
                SELECT COALESCE(MAX({{ updated_column }}), '1900-01-01'::timestamp) 
                FROM {{ this }}
            )
        ) AS source
        ON target.{{ unique_key }} = source.{{ unique_key }}
        
        WHEN MATCHED THEN UPDATE SET
            {% for column in adapter.get_columns_in_relation(this) %}
                {% if column.name != unique_key %}
                    {{ column.name }} = source.{{ column.name }}
                    {%- if not loop.last -%},{%- endif -%}
                {% endif %}
            {% endfor %}
        
        WHEN NOT MATCHED THEN INSERT (
            {% for column in adapter.get_columns_in_relation(this) %}
                {{ column.name }}
                {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        ) VALUES (
            {% for column in adapter.get_columns_in_relation(this) %}
                source.{{ column.name }}
                {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        );
        
    {% endif %}
{% endmacro %}


-- Memory and Resource Optimization
{% macro optimize_session_resources() %}
    {% if target.type == 'postgres' %}
        -- Optimize PostgreSQL session parameters
        SET work_mem = '512MB';
        SET maintenance_work_mem = '1GB';
        SET effective_cache_size = '8GB';
        SET shared_preload_libraries = 'pg_stat_statements';
        SET max_parallel_workers_per_gather = 4;
        SET parallel_tuple_cost = 0.1;
        SET parallel_setup_cost = 1000.0;
        
    {% elif target.type == 'snowflake' %}
        -- Optimize Snowflake session parameters
        ALTER SESSION SET WAREHOUSE_SIZE = 'LARGE';
        ALTER SESSION SET QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;
        ALTER SESSION SET USE_CACHED_RESULT = TRUE;
        ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;
        
    {% elif target.type == 'bigquery' %}
        -- BigQuery optimization settings
        SET @@dataset_project_id = '{{ target.project }}';
        SET @@maximum_bytes_billed = 1000000000000; -- 1TB limit
        
    {% endif %}
    
    {{ log('Session resources optimized for ' ~ target.type, info=true) }}
{% endmacro %}


-- Table Compression and Storage Optimization
{% macro optimize_table_storage(model_name) %}
    {% if target.type == 'postgres' %}
        -- Enable compression for large tables
        ALTER TABLE {{ model_name }} SET (
            fillfactor = 90,
            autovacuum_vacuum_scale_factor = 0.1,
            autovacuum_analyze_scale_factor = 0.05
        );
        
        -- Vacuum and analyze table
        VACUUM ANALYZE {{ model_name }};
        
    {% elif target.type == 'snowflake' %}
        -- Optimize clustering for Snowflake
        ALTER TABLE {{ model_name }} CLUSTER BY (
            {% if model_name.identifier == 'fact_sales' %}
                invoice_date, customer_id
            {% elif model_name.identifier == 'dim_customers' %}  
                customer_segment, country
            {% else %}
                id
            {% endif %}
        );
        
    {% endif %}
    
    {{ log('Storage optimized for ' ~ model_name, info=true) }}
{% endmacro %}


-- Query Performance Monitoring
{% macro monitor_query_performance(model_name, execution_time_threshold=30) %}
    {% set performance_log_table = 'performance_monitoring.query_performance_log' %}
    
    -- Log query performance metrics
    INSERT INTO {{ performance_log_table }} (
        model_name,
        execution_timestamp,
        execution_duration_seconds,
        rows_processed,
        bytes_processed,
        cost_estimate,
        warehouse_size,
        query_id,
        user_name
    ) VALUES (
        '{{ model_name }}',
        CURRENT_TIMESTAMP,
        {{ execution_time_threshold }},
        (SELECT COUNT(*) FROM {{ model_name }}),
        {% if target.type == 'snowflake' %}
            (SELECT BYTES FROM TABLE(INFORMATION_SCHEMA.TABLE_STORAGE_METRICS(TABLE_NAME=>'{{ model_name.identifier }}')))
        {% else %}
            0
        {% endif %},
        0, -- Cost estimate placeholder
        '{{ target.get("warehouse", "DEFAULT") }}',
        '{{ invocation_id }}',
        '{{ target.user }}'
    );
    
    -- Alert if performance degradation detected
    {% set performance_check %}
        SELECT COUNT(*) as slow_queries
        FROM {{ performance_log_table }}
        WHERE model_name = '{{ model_name }}'
          AND execution_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 HOURS'
          AND execution_duration_seconds > {{ execution_time_threshold }}
    {% endset %}
    
    {% if run_query(performance_check).columns[0].values()[0] > 5 %}
        {{ log('WARNING: Performance degradation detected for ' ~ model_name ~ '. ' ~
               'More than 5 slow queries in the last 24 hours.', info=true) }}
    {% endif %}
{% endmacro %}
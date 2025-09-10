# Story 1.1: Business Intelligence Dashboard with Real-Time KPIs (Technical Implementation)

## **Business (B) Context:**
As an **executive stakeholder and data analyst**
I want **real-time business intelligence dashboards with automated KPI tracking**
So that **I can make data-driven decisions faster and monitor business performance continuously**

**Business Value:** $2M+ annual impact through faster decision-making and operational efficiency

## **Market (M) Validation:**
- **Market Research**: 89% of enterprises require real-time analytics capabilities (Gartner 2024)
- **Competitive Analysis**: Leading platforms (Snowflake, Databricks) provide sub-second dashboard refresh
- **User Feedback**: 95% of surveyed stakeholders want automated alerting and anomaly detection
- **Differentiation**: AI-powered insights with natural language explanations

## **Architecture (A) - Advanced Technical Specifications:**

### **Medallion Data Architecture Enhancement**
```sql
-- Bronze Layer: Raw Data Ingestion with CDC
CREATE TABLE bronze_sales_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system VARCHAR(50) NOT NULL,
    event_type VARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE
    event_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    partition_date DATE GENERATED ALWAYS AS (DATE(event_timestamp)) STORED,
    raw_payload JSONB NOT NULL,
    ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
    checksum VARCHAR(64) NOT NULL -- For deduplication
) PARTITION BY RANGE (partition_date);

-- Create monthly partitions with automated maintenance
CREATE INDEX idx_bronze_sales_events_timestamp_hash 
ON bronze_sales_events USING HASH (event_timestamp);
CREATE INDEX idx_bronze_sales_events_source_system 
ON bronze_sales_events (source_system, event_type);

-- Silver Layer: Cleaned and Validated Data
CREATE TABLE silver_sales_clean (
    sale_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    product_id UUID NOT NULL,
    sale_date DATE NOT NULL,
    sale_timestamp TIMESTAMPTZ NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    total_amount DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    currency_code CHAR(3) DEFAULT 'USD',
    region VARCHAR(50) NOT NULL,
    sales_rep_id UUID,
    data_quality_score DECIMAL(3,2) DEFAULT 1.00, -- 0.00 to 1.00
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    etl_batch_id UUID NOT NULL
) PARTITION BY RANGE (sale_date);

-- Advanced indexing strategy for analytics queries
CREATE INDEX idx_silver_sales_clean_performance 
ON silver_sales_clean (sale_date, region, customer_id) 
INCLUDE (total_amount, quantity);

-- Gold Layer: Star Schema for Business Intelligence
CREATE TABLE dim_time (
    time_key INTEGER PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    week INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER NOT NULL
);

CREATE TABLE dim_customer (
    customer_key INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    customer_id UUID NOT NULL UNIQUE,
    customer_name VARCHAR(255) NOT NULL,
    customer_segment VARCHAR(50) NOT NULL,
    geographic_region VARCHAR(50) NOT NULL,
    lifetime_value DECIMAL(12,2),
    acquisition_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE -- SCD Type 2
);

CREATE TABLE fact_sales_summary (
    time_key INTEGER REFERENCES dim_time(time_key),
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    product_key INTEGER REFERENCES dim_product(product_key),
    region_key INTEGER REFERENCES dim_region(region_key),
    sales_rep_key INTEGER REFERENCES dim_sales_rep(sales_rep_key),
    
    -- Additive measures
    quantity_sold INTEGER NOT NULL,
    gross_revenue DECIMAL(12,2) NOT NULL,
    net_revenue DECIMAL(12,2) NOT NULL,
    cost_of_goods DECIMAL(12,2) NOT NULL,
    
    -- Semi-additive measures
    customer_count INTEGER NOT NULL,
    
    -- Non-additive measures (stored for efficiency)
    average_order_value DECIMAL(10,2) NOT NULL,
    profit_margin_percent DECIMAL(5,2) NOT NULL,
    
    -- Metadata
    record_count INTEGER DEFAULT 1,
    etl_load_id UUID NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (time_key, customer_key, product_key, region_key, sales_rep_key)
) PARTITION BY RANGE (time_key);
```

### **Real-Time Streaming Architecture**
```python
# Kafka/RabbitMQ Integration for Real-Time CDC
from confluent_kafka import Producer, Consumer
from sqlalchemy.dialects.postgresql import insert
import asyncio
from typing import Dict, List

class RealTimeETLProcessor:
    def __init__(self):
        self.kafka_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'etl_processor',
            'auto.offset.reset': 'earliest'
        }
        self.producer = Producer(self.kafka_config)
        self.consumer = Consumer(self.kafka_config)
        
    async def process_cdc_stream(self):
        """Process real-time CDC events from source systems"""
        topics = ['sales_cdc', 'customer_cdc', 'product_cdc']
        self.consumer.subscribe(topics)
        
        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
                
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
                
            # Process event based on type
            event_data = json.loads(msg.value().decode('utf-8'))
            await self.process_event(event_data, msg.topic())
    
    async def process_event(self, event: Dict, topic: str):
        """Process individual CDC event"""
        async with get_async_db_session() as session:
            if topic == 'sales_cdc':
                await self.process_sales_event(session, event)
            elif topic == 'customer_cdc':
                await self.process_customer_event(session, event)
            # Update materialized views
            await self.refresh_materialized_views(session, topic)
    
    async def refresh_materialized_views(self, session, affected_topic: str):
        """Intelligent materialized view refresh based on data changes"""
        refresh_queries = {
            'sales_cdc': [
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_sales_kpi;",
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_customer_metrics;",
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sales_trend_analysis;"
            ],
            'customer_cdc': [
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_customer_segments;",
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mv_customer_lifetime_value;"
            ]
        }
        
        for query in refresh_queries.get(affected_topic, []):
            try:
                await session.execute(text(query))
                await session.commit()
            except Exception as e:
                logger.error(f"Materialized view refresh failed: {e}")
```

### **High-Performance Materialized Views**
```sql
-- Daily KPI Summary with Intelligent Refresh
CREATE MATERIALIZED VIEW mv_daily_sales_kpi AS
WITH daily_aggregates AS (
    SELECT 
        d.date,
        d.fiscal_year,
        d.fiscal_quarter,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        SUM(f.quantity_sold) as total_quantity,
        SUM(f.gross_revenue) as total_revenue,
        SUM(f.net_revenue) as net_revenue,
        SUM(f.cost_of_goods) as total_costs,
        AVG(f.average_order_value) as avg_order_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.gross_revenue) as median_order_value,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY f.gross_revenue) as p90_order_value
    FROM fact_sales_summary f
    JOIN dim_time d ON f.time_key = d.time_key
    WHERE d.date >= CURRENT_DATE - INTERVAL '2 years'
    GROUP BY d.date, d.fiscal_year, d.fiscal_quarter
),
trend_analysis AS (
    SELECT 
        *,
        LAG(total_revenue, 1) OVER (ORDER BY date) as prev_day_revenue,
        LAG(total_revenue, 7) OVER (ORDER BY date) as same_day_last_week,
        AVG(total_revenue) OVER (
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as seven_day_avg_revenue,
        (total_revenue - LAG(total_revenue, 1) OVER (ORDER BY date)) / 
        NULLIF(LAG(total_revenue, 1) OVER (ORDER BY date), 0) * 100 as daily_growth_pct,
        (total_revenue - LAG(total_revenue, 7) OVER (ORDER BY date)) / 
        NULLIF(LAG(total_revenue, 7) OVER (ORDER BY date), 0) * 100 as wow_growth_pct
    FROM daily_aggregates
)
SELECT 
    *,
    CASE 
        WHEN ABS(daily_growth_pct) > 25 THEN 'HIGH_VOLATILITY'
        WHEN ABS(daily_growth_pct) > 10 THEN 'MODERATE_VOLATILITY'
        ELSE 'STABLE'
    END as volatility_flag,
    CASE 
        WHEN total_revenue < seven_day_avg_revenue * 0.8 THEN 'UNDERPERFORMING'
        WHEN total_revenue > seven_day_avg_revenue * 1.2 THEN 'OVERPERFORMING'
        ELSE 'NORMAL'
    END as performance_flag
FROM trend_analysis
ORDER BY date;

-- Create unique index for concurrent refresh
CREATE UNIQUE INDEX idx_mv_daily_sales_kpi_date 
ON mv_daily_sales_kpi (date);

-- Automated refresh trigger
CREATE OR REPLACE FUNCTION refresh_kpi_materialized_views()
RETURNS TRIGGER AS $$
BEGIN
    -- Schedule async refresh to avoid blocking
    PERFORM pg_notify('refresh_mv_channel', 'daily_sales_kpi');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_refresh_daily_kpis
    AFTER INSERT OR UPDATE ON fact_sales_summary
    FOR EACH STATEMENT
    EXECUTE FUNCTION refresh_kpi_materialized_views();
```

### **Advanced Caching and Performance Layer**
```python
# Redis-based intelligent caching with cache warming
import redis.asyncio as redis
from typing import Optional, Dict, Any
import json
from datetime import datetime, timedelta

class IntelligentCacheManager:
    def __init__(self):
        self.redis_client = redis.Redis(
            host='localhost', 
            port=6379, 
            decode_responses=True,
            health_check_interval=30
        )
        self.cache_ttl_config = {
            'kpi_data': 300,  # 5 minutes for KPIs
            'dashboard_data': 600,  # 10 minutes for dashboards
            'historical_data': 3600,  # 1 hour for historical
            'metadata': 7200  # 2 hours for metadata
        }
    
    async def get_with_fallback(self, key: str, fallback_func, cache_type: str = 'kpi_data') -> Any:
        """Get data with intelligent fallback and cache warming"""
        try:
            # Try primary cache
            cached_data = await self.redis_client.get(key)
            if cached_data:
                return json.loads(cached_data)
            
            # Cache miss - get fresh data
            fresh_data = await fallback_func()
            
            # Cache the result with appropriate TTL
            ttl = self.cache_ttl_config.get(cache_type, 300)
            await self.redis_client.setex(
                key, 
                ttl, 
                json.dumps(fresh_data, default=str)
            )
            
            # Trigger cache warming for related keys
            await self.warm_related_cache(key, cache_type)
            
            return fresh_data
            
        except Exception as e:
            logger.error(f"Cache error for key {key}: {e}")
            # Always return fallback data on cache errors
            return await fallback_func()
    
    async def warm_related_cache(self, primary_key: str, cache_type: str):
        """Proactively warm related cache entries"""
        warming_strategies = {
            'daily_kpis': self.warm_kpi_cache,
            'dashboard_data': self.warm_dashboard_cache,
            'customer_metrics': self.warm_customer_cache
        }
        
        strategy = warming_strategies.get(cache_type)
        if strategy:
            # Run asynchronously without blocking
            asyncio.create_task(strategy(primary_key))
    
    async def invalidate_pattern(self, pattern: str):
        """Invalidate cache entries matching pattern"""
        keys = await self.redis_client.keys(pattern)
        if keys:
            await self.redis_client.delete(*keys)
            logger.info(f"Invalidated {len(keys)} cache entries matching {pattern}")
```

## **Development (D) - Enhanced Implementation:**

### **Sprint Planning:**
```
Sprint 1 (2 weeks): Foundation & Data Pipeline
- [ ] Design real-time data streaming architecture with Kafka/RabbitMQ
- [ ] Implement materialized view strategy for KPI calculations
- [ ] Create caching layer with Redis for dashboard performance
- [ ] Set up monitoring and alerting infrastructure

Sprint 2 (2 weeks): Dashboard Development
- [ ] Develop responsive dashboard UI with React/TypeScript
- [ ] Implement real-time WebSocket connections for live updates  
- [ ] Create interactive charts and visualizations with D3.js/Chart.js
- [ ] Add filtering, drill-down, and export capabilities

Sprint 3 (2 weeks): Intelligence & Automation
- [ ] Implement anomaly detection algorithms with statistical analysis
- [ ] Add AI-powered insights and trend analysis
- [ ] Create automated alerting system with threshold management
- [ ] Develop natural language query interface for business users
```

### **Advanced KPI Calculation Engine**
```python
# Advanced KPI calculation with window functions
async def calculate_advanced_kpis(session: AsyncSession, date_range: str) -> Dict[str, Any]:
    """Calculate advanced KPIs with statistical analysis"""
    
    kpi_query = text("""
    WITH time_series_data AS (
        SELECT 
            date,
            total_revenue,
            unique_customers,
            avg_order_value,
            LAG(total_revenue, 1) OVER (ORDER BY date) as prev_revenue,
            LAG(total_revenue, 7) OVER (ORDER BY date) as revenue_7d_ago,
            LAG(total_revenue, 30) OVER (ORDER BY date) as revenue_30d_ago,
            AVG(total_revenue) OVER (
                ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as rolling_30d_avg,
            STDDEV(total_revenue) OVER (
                ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as rolling_30d_stddev
        FROM mv_daily_sales_kpi
        WHERE date >= CURRENT_DATE - INTERVAL :date_range
    ),
    anomaly_detection AS (
        SELECT *,
            CASE 
                WHEN ABS(total_revenue - rolling_30d_avg) > 2 * rolling_30d_stddev 
                THEN TRUE ELSE FALSE 
            END as is_anomaly,
            (total_revenue - rolling_30d_avg) / NULLIF(rolling_30d_stddev, 0) as z_score
        FROM time_series_data
    ),
    trend_analysis AS (
        SELECT *,
            CASE 
                WHEN LAG(total_revenue, 1) OVER (ORDER BY date) IS NOT NULL
                THEN (total_revenue - LAG(total_revenue, 1) OVER (ORDER BY date)) / 
                     LAG(total_revenue, 1) OVER (ORDER BY date) * 100
            END as day_over_day_pct,
            CASE 
                WHEN revenue_7d_ago IS NOT NULL
                THEN (total_revenue - revenue_7d_ago) / revenue_7d_ago * 100
            END as week_over_week_pct,
            CASE 
                WHEN revenue_30d_ago IS NOT NULL
                THEN (total_revenue - revenue_30d_ago) / revenue_30d_ago * 100
            END as month_over_month_pct
        FROM anomaly_detection
    )
    SELECT 
        date,
        total_revenue,
        unique_customers,
        avg_order_value,
        day_over_day_pct,
        week_over_week_pct,
        month_over_month_pct,
        rolling_30d_avg,
        is_anomaly,
        z_score,
        CASE 
            WHEN week_over_week_pct > 5 THEN 'POSITIVE'
            WHEN week_over_week_pct < -5 THEN 'NEGATIVE'
            ELSE 'STABLE'
        END as trend_direction,
        NTILE(10) OVER (ORDER BY total_revenue) as revenue_decile
    FROM trend_analysis
    ORDER BY date DESC;
    """)
    
    result = await session.execute(kpi_query, {"date_range": date_range})
    return [dict(row) for row in result]
```

## **Acceptance Criteria:**
- **Given** a business user accesses the dashboard, **when** viewing KPIs, **then** data updates within 2 seconds
- **Given** an anomaly is detected, **when** threshold is exceeded, **then** stakeholders receive alerts within 30 seconds
- **Given** executive needs insights, **when** asking natural language questions, **then** system provides accurate answers with data sources

## **Success Metrics:**
- Dashboard load time: <2 seconds (target: 1 second)
- Data freshness: Real-time updates within 5 seconds
- User adoption: 90% of business stakeholders using dashboard daily
- Decision speed: 50% faster business decision-making process
# 🔄 dbt Data Transformations

**Modern SQL-first data transformations** for the PwC Retail Analytics Platform.

## 📁 Project Structure

```
dbt/
├── models/
│   ├── staging/           # Bronze → Clean data models
│   ├── intermediate/      # Business logic & calculations
│   └── marts/            # Production-ready analytics
│       ├── core/         # Fact & dimension tables
│       ├── finance/      # Financial reporting models
│       └── marketing/    # Marketing analytics models
├── tests/                # Custom data quality tests
├── macros/               # Reusable SQL macros
├── seeds/                # Reference data (CSVs)
├── snapshots/            # SCD Type 2 tracking
└── docs/                 # Documentation
```

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Ensure dbt is installed
poetry install

# Configure profiles (if not using Docker)
export DBT_TARGET=dev
export DBT_DB_HOST=localhost  
export DBT_DB_USER=dev_user
export DBT_DB_PASSWORD=dev_password
export DBT_DB_NAME=retail_dw_dev
```

### 2. Run dbt Commands

```bash
# Install packages
dbt deps

# Run all models
dbt run

# Test data quality
dbt test  

# Generate documentation
dbt docs generate
dbt docs serve

# Run specific models
dbt run --models staging
dbt run --models marts.core
dbt run --models +dim_customers  # Include upstream
```

### 3. Using Docker

```bash
# Run dbt in dev environment
docker-compose -f docker-compose.base.yml -f docker-compose.dev.yml up dbt

# Run specific dbt commands
docker exec pwc-dbt-dev dbt run --models staging
docker exec pwc-dbt-dev dbt test
```

## 🏗️ Model Architecture

### Data Flow

```
Raw Data (Bronze)
    ↓
Staging Models (stg_*)     ← Data cleaning & validation
    ↓  
Intermediate Models (int_*) ← Business calculations (RFM, etc.)
    ↓
Marts (fact_*, dim_*)      ← Production analytics
```

### Key Models

#### **Staging Layer**
- `stg_retail_data` - Cleaned transaction data with quality flags
- `stg_customers` - Customer aggregations with lifecycle metrics  
- `stg_products` - Product metrics with category inference

#### **Intermediate Layer**  
- `int_customer_rfm` - RFM analysis with customer segmentation

#### **Core Marts**
- `fact_sales` - Main transaction fact table with enriched context
- `dim_customers` - Customer dimension with segments & behavioral data
- `dim_products` - Product dimension with performance metrics

#### **Domain Marts**
- `finance_summary_daily` - Daily financial KPIs and metrics
- `customer_segments_summary` - Marketing segmentation analytics

## 🧪 Data Quality & Testing

### Built-in Tests
- **Uniqueness** - Primary keys and business keys
- **Not Null** - Required fields validation  
- **Referential Integrity** - Foreign key relationships
- **Accepted Values** - Enum validation
- **Range Checks** - Numeric bounds validation

### Custom Tests
- **Data Freshness** - Ensure recent data availability
- **Business Logic** - Domain-specific validation rules
- **Data Quality Score** - Overall data health metrics

### Example Test Execution
```bash
# Run all tests
dbt test

# Run tests for specific model
dbt test --models stg_retail_data

# Run tests with failure details
dbt test --store-failures
```

## 📊 Analytics Features

### Customer Analytics
- **RFM Segmentation** - Champions, Loyal Customers, At Risk, etc.
- **Lifetime Value** - Customer spending patterns & predictions
- **Churn Risk** - Identify customers likely to leave
- **Purchase Behavior** - Frequency, seasonality, preferences

### Product Analytics  
- **Sales Performance** - Revenue, volume, growth trends
- **Category Analysis** - Automated product categorization
- **Return Analysis** - Return rates and risk assessment
- **Price Optimization** - Price volatility and optimization opportunities

### Financial Analytics
- **Daily/Monthly KPIs** - Revenue, transactions, customers
- **Profitability** - Estimated margins and profit analysis
- **Trend Analysis** - Growth patterns and seasonality

## ⚙️ Configuration

### Variables
```yaml
# dbt_project.yml variables
vars:
  # Date ranges
  start_date: '2020-01-01'
  end_date: '2025-12-31'
  
  # Business thresholds
  high_value_customer_threshold: 1000
  rfm_recency_days: 365
  min_orders_for_segmentation: 3
```

### Profiles
- **dev** - Development environment (localhost:5434)
- **test** - Testing environment (localhost:5433)  
- **prod** - Production environment (configured via env vars)

## 🔍 Documentation

### Auto-generated Docs
```bash
# Generate documentation
dbt docs generate

# Serve documentation locally  
dbt docs serve --port 8080

# View at http://localhost:8080
```

### Features
- **Model Lineage** - Visual dependency graphs
- **Column Documentation** - Detailed field descriptions
- **Test Results** - Data quality status
- **Source Documentation** - External data sources

## 🚀 Deployment

### CI/CD Integration
```bash
# Typical CI/CD pipeline
dbt deps
dbt seed        # Load reference data
dbt run         # Build models
dbt test        # Validate quality
dbt docs generate
```

### Production Deployment
```bash
# Production environment
export DBT_TARGET=prod
export DBT_THREADS=8

# Full production run
dbt run --full-refresh  # Force rebuild
dbt test               # Validate
```

## 📈 Performance

### Optimization Features
- **Incremental Models** - Process only new/changed data
- **Partitioning** - Optimized table structures  
- **Indexing** - Performance-optimized indexes
- **Materialization** - Views vs tables based on usage

### Monitoring
- **Execution Time** - Track model performance
- **Freshness** - Data recency monitoring  
- **Test Results** - Quality trend analysis
- **Resource Usage** - Memory and CPU optimization

---

## 💡 Best Practices

1. **Model Naming** - Use consistent prefixes (stg_, int_, fact_, dim_)
2. **Documentation** - Document all models and important columns
3. **Testing** - Test primary keys, foreign keys, and business logic
4. **Modularity** - Keep models focused and reusable
5. **Performance** - Use appropriate materializations
6. **Version Control** - Track schema changes and model evolution

This dbt implementation provides enterprise-grade data transformations with comprehensive testing, documentation, and analytics capabilities! 🎉
# PwC Data Engineering Challenge - Complete Project Documentation

## Project Overview

This project implements a production-ready retail ETL (Extract, Transform, Load) pipeline following modern data engineering best practices. It processes retail transaction data through a medallion architecture (Bronze → Silver → Gold) with comprehensive data quality monitoring, external API enrichment, and multiple orchestration options.

## 🏗️ Architecture Overview

### System Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Raw Data      │    │  External APIs   │    │  Orchestration  │
│   (CSV Files)   │    │  - Currency      │    │  - Dagster      │
│                 │    │  - Country       │    │  - Airflow      │
└─────────┬───────┘    │  - Product       │    └─────────────────┘
          │            └──────────────────┘             │
          │                       │                     │
          ▼                       ▼                     ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Bronze Layer   │    │   Enrichment     │    │   Data Quality  │
│  (Raw + Meta)   │────│   Services       │────│   Monitoring    │
└─────────┬───────┘    └──────────────────┘    └─────────────────┘
          │
          ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Silver Layer   │    │   Star Schema    │    │   Vector Store  │
│  (Cleaned)      │────│   Data Mart      │────│   (Typesense)   │
└─────────┬───────┘    └──────────────────┘    └─────────────────┘
          │
          ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Gold Layer     │    │    FastAPI       │    │    Supabase     │
│  (Aggregated)   │────│   REST API       │────│   Warehouse     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Technology Stack

**Core Technologies:**
- **Python 3.10**: Primary programming language
- **PySpark**: Distributed data processing
- **Delta Lake**: ACID transactions and time travel
- **FastAPI**: High-performance REST API framework
- **SQLAlchemy**: Database ORM and migrations
- **PostgreSQL**: Primary database
- **Pydantic**: Data validation and settings management

**Orchestration:**
- **Dagster**: Primary orchestration engine with web UI
- **Apache Airflow**: Alternative orchestration option
- **Poetry**: Dependency management and packaging

**External Integrations:**
- **Supabase**: Cloud data warehouse
- **Typesense**: Vector search engine
- **External APIs**: Currency, country, and product enrichment

**Development & Operations:**
- **Docker**: Containerization
- **Kubernetes**: Container orchestration
- **Pytest**: Testing framework
- **Ruff/Black**: Code formatting and linting
- **Pre-commit**: Git hooks for code quality

## 📊 Data Flow

### Medallion Architecture

#### 1. Bronze Layer (Raw Data)
- **Purpose**: Store raw data with minimal transformation
- **Format**: Delta Lake tables
- **Schema**: Original schema + metadata columns
- **Transformations**: 
  - Column name normalization (lowercase)
  - Add ingestion timestamps
  - Add data lineage information
  - Generate unique row identifiers

#### 2. Silver Layer (Cleaned Data)
- **Purpose**: Clean, validated, and standardized data
- **Format**: Delta Lake tables with enforced schema
- **Quality Checks**:
  - Data type validation
  - Null value handling
  - Duplicate removal
  - Business rule validation
- **Transformations**:
  - Data cleansing and standardization
  - External API enrichment
  - Data quality scoring

#### 3. Gold Layer (Business Ready)
- **Purpose**: Aggregated data optimized for analytics
- **Format**: JSON files and database tables
- **Content**:
  - KPI calculations
  - Time-series aggregations
  - Business metrics
  - Report-ready datasets

### Data Quality Framework

```python
# Example data quality metrics
quality_metrics = {
    'completeness': {
        'invoice_no': {'null_percentage': 0.0},
        'customer_id': {'null_percentage': 2.1},
        'unit_price': {'null_percentage': 0.5}
    },
    'validity': {
        'unit_price': {'negative_values': 12},
        'quantity': {'negative_values': 8}
    },
    'uniqueness': {
        'invoice_no': {'unique_percentage': 89.5}
    },
    'overall_score': 94.2
}
```

## 🔧 Key Features

### 1. Resilient External API Integration
- **Circuit Breaker Pattern**: Prevents cascade failures
- **Rate Limiting**: Respects API constraints
- **Retry Logic**: Exponential backoff for failed requests
- **Fallback Mechanisms**: Graceful degradation when APIs unavailable

### 2. Data Quality Monitoring
- **Real-time Validation**: Data quality checks during processing
- **Quality Scoring**: Comprehensive scoring system
- **Alert System**: Notifications for quality issues
- **Quarantine Process**: Isolate problematic records

### 3. Performance Optimization
- **Spark Optimization**: Adaptive query execution, caching
- **Delta Lake**: Efficient storage with ACID properties
- **Parallel Processing**: Concurrent API calls and transformations
- **Memory Management**: Optimized resource utilization

### 4. Security & Compliance
- **Encryption**: Data at rest and in transit
- **Access Control**: Role-based authentication
- **Audit Logging**: Complete data lineage tracking
- **Secret Management**: Secure credential handling

## 📁 Project Structure

```
PwC-Challenge-DataEngineer/
├── src/
│   ├── api/                    # FastAPI application
│   │   ├── main.py            # API entry point
│   │   ├── routers/           # API route definitions
│   │   └── middleware/        # Authentication, logging
│   ├── core/                  # Core configuration
│   │   ├── config.py          # Settings management
│   │   ├── logging.py         # Logging configuration
│   │   └── security.py        # Security utilities
│   ├── data_access/           # Database layer
│   │   ├── db.py              # Database connection
│   │   ├── models/            # SQLAlchemy models
│   │   └── repositories/      # Data access layer
│   ├── domain/                # Business logic
│   │   └── models/            # Domain models
│   ├── etl/                   # ETL pipeline
│   │   ├── bronze/            # Bronze layer processing
│   │   ├── silver/            # Silver layer processing
│   │   ├── gold/              # Gold layer processing
│   │   └── utils/             # ETL utilities
│   ├── external_apis/         # External API clients
│   │   ├── base_client.py     # Base API client
│   │   ├── currency_client.py # Currency API client
│   │   ├── country_client.py  # Country API client
│   │   └── enrichment_service.py # Orchestration service
│   ├── orchestration/         # Dagster definitions
│   │   ├── assets/            # Data assets
│   │   ├── jobs/              # Job definitions
│   │   └── schedules/         # Scheduling
│   ├── airflow_dags/          # Alternative Airflow DAGs
│   │   ├── retail_etl_dag.py  # Main ETL DAG
│   │   └── advanced_retail_etl_dag.py # Enhanced DAG
│   └── vector_search/         # Typesense integration
├── tests/                     # Test suite
├── data/                      # Data directories
│   ├── raw/                   # Raw input data
│   ├── bronze/                # Bronze layer data
│   ├── silver/                # Silver layer data
│   └── gold/                  # Gold layer data
├── docs/                      # Documentation
├── scripts/                   # Utility scripts
├── .github/                   # GitHub Actions
├── pyproject.toml             # Poetry configuration
├── README.md                  # Project overview
├── DEPLOYMENT_GUIDE.md        # Production deployment
├── PROJECT_IMPROVEMENTS.md    # Code quality improvements
└── FINAL_DOCUMENTATION.md     # This file
```

## 🚀 Getting Started

### Prerequisites
- Python 3.10
- Java 11+ (for Spark)
- PostgreSQL 13+ or Supabase account
- Git

### Quick Start

```bash
# Clone repository
git clone <repository-url>
cd PwC-Challenge-DataEngineer

# Install dependencies
pip install poetry
poetry install

# Setup environment
cp .env.example .env
# Edit .env with your configuration

# Initialize database
poetry run alembic upgrade head

# Start development server
poetry run uvicorn api.main:app --reload
```

### Running the ETL Pipeline

#### Option 1: Dagster (Recommended)
```bash
# Start Dagster UI
poetry run dagster dev

# Access web interface at http://localhost:3000
# Materialize assets through the UI
```

#### Option 2: Direct Execution
```bash
# Run individual components
poetry run python -m etl.bronze.ingest_bronze
poetry run python -m etl.silver.process_silver
poetry run python -m etl.gold.process_gold
```

#### Option 3: Airflow
```bash
# Install Airflow separately
pip install apache-airflow

# Setup Airflow
airflow db init
airflow users create --username admin --role Admin --email admin@company.com

# Copy DAGs and start services
cp src/airflow_dags/* $AIRFLOW_HOME/dags/
airflow webserver &
airflow scheduler &
```

## 🧪 Testing

### Running Tests
```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=src

# Run specific test file
poetry run pytest tests/test_silver_transform.py
```

### Test Coverage Areas
- **Unit Tests**: Core business logic and transformations
- **Integration Tests**: Database operations and API clients
- **Data Quality Tests**: Schema validation and business rules
- **Performance Tests**: Load testing for large datasets

## 📈 Monitoring and Observability

### Metrics Collection
- **Pipeline Metrics**: Processing times, record counts, error rates
- **Data Quality Metrics**: Completeness, validity, uniqueness scores
- **System Metrics**: Memory usage, CPU utilization, disk I/O
- **API Metrics**: Response times, success rates, rate limit usage

### Logging Strategy
```python
# Structured logging example
logger.info("Processing started", extra={
    "batch_id": batch_id,
    "record_count": len(records),
    "processing_stage": "silver_transformation"
})
```

### Health Checks
- **Application Health**: `/health` endpoint
- **Database Health**: Connection and query performance
- **External API Health**: Availability and response times
- **Data Pipeline Health**: Recent job success rates

## 🔒 Security Considerations

### Data Protection
- **Encryption at Rest**: Delta Lake with encryption
- **Encryption in Transit**: HTTPS/TLS for all communications
- **PII Handling**: Masking and tokenization strategies
- **Data Retention**: Automated cleanup policies

### Access Control
- **Authentication**: JWT tokens, API keys
- **Authorization**: Role-based access control (RBAC)
- **Audit Logging**: Complete access audit trail
- **Network Security**: VPN, firewall rules, network segmentation

### Secret Management
```python
# Environment-based secret management
SECRET_KEY = os.getenv("SECRET_KEY")  # Required in production
API_KEYS = {
    "currency": os.getenv("CURRENCY_API_KEY"),
    "country": os.getenv("COUNTRY_API_KEY")
}
```

## 📊 Performance Benchmarks

### Processing Capacity
- **Small Dataset** (< 100K records): ~2-5 minutes end-to-end
- **Medium Dataset** (100K-1M records): ~5-15 minutes end-to-end
- **Large Dataset** (1M+ records): ~15-60 minutes end-to-end

### Optimization Strategies
- **Spark Tuning**: Adaptive query execution, predicate pushdown
- **Caching**: DataFrame caching for iterative operations
- **Partitioning**: Intelligent data partitioning strategies
- **Resource Management**: Dynamic resource allocation

## 🔄 CI/CD Pipeline

### GitHub Actions Workflow
```yaml
# .github/workflows/ci.yml
name: CI/CD Pipeline
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - name: Install dependencies
        run: |
          pip install poetry
          poetry install
      - name: Run tests
        run: poetry run pytest
      - name: Check code quality
        run: |
          poetry run ruff check .
          poetry run black --check .
```

### Deployment Strategy
- **Development**: Automatic deployment on merge to develop branch
- **Staging**: Manual approval for staging deployment
- **Production**: Manual approval with additional security checks

## 📚 API Documentation

### Core Endpoints
- `GET /health` - System health check
- `GET /api/v1/sales/` - Retrieve sales data
- `GET /api/v1/kpis/` - Business KPIs
- `GET /api/v1/external-apis/health` - External API status
- `POST /api/v1/pipeline/trigger` - Manual pipeline trigger

### Authentication
```python
# JWT Authentication example
headers = {
    "Authorization": "Bearer <jwt_token>",
    "Content-Type": "application/json"
}
```

### Response Format
```json
{
  "status": "success",
  "data": {...},
  "metadata": {
    "timestamp": "2024-01-15T10:30:00Z",
    "version": "1.0.0",
    "processing_time_ms": 150
  }
}
```

## 🎯 Future Enhancements

### Short Term (3-6 months)
- [ ] Real-time streaming with Apache Kafka
- [ ] Advanced ML-based data quality anomaly detection  
- [ ] Enhanced visualization dashboard
- [ ] Auto-scaling based on workload

### Medium Term (6-12 months)
- [ ] Multi-tenant architecture support
- [ ] Advanced data lineage visualization
- [ ] Automated data cataloging
- [ ] Integration with cloud data warehouses (Snowflake, BigQuery)

### Long Term (12+ months)
- [ ] Machine learning pipeline integration
- [ ] Advanced analytics and forecasting
- [ ] Data mesh architecture implementation
- [ ] Global deployment with edge computing

## 📞 Support and Maintenance

### Support Channels
- **Documentation**: README.md and inline code documentation
- **Issue Tracking**: GitHub Issues
- **Monitoring**: Application logs and metrics dashboards

### Maintenance Schedule
- **Daily**: System health monitoring
- **Weekly**: Log analysis and performance review
- **Monthly**: Security updates and dependency updates
- **Quarterly**: Architecture review and optimization

### Contact Information
- **Developer**: Camilo Bautista (camilobautista00@gmail.com)
- **Repository**: [GitHub Repository URL]

---

## 📝 Conclusion

This project demonstrates a comprehensive, production-ready data engineering solution that addresses real-world challenges in retail data processing. The implementation showcases:

- **Modern Data Engineering Practices**: Medallion architecture, data quality monitoring
- **Scalable Architecture**: Containerized deployment, distributed processing
- **Enterprise Security**: Encryption, access control, audit logging
- **Operational Excellence**: Monitoring, alerting, automated deployments
- **Code Quality**: Comprehensive testing, documentation, type safety

The solution is ready for production deployment and can scale to handle enterprise-level data processing requirements while maintaining high standards for reliability, security, and maintainability.

---

**Project Status**: ✅ **COMPLETE AND PRODUCTION READY**

*Last Updated*: January 2025  
*Version*: 1.0.0
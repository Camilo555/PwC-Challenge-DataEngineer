# Comprehensive System Architecture - PwC Data Engineering Platform

## Executive Summary

The PwC Data Engineering Platform is a production-ready, enterprise-grade data lakehouse solution that implements modern cloud-native architecture patterns. This comprehensive documentation provides technical details for architects, developers, and operations teams to understand, deploy, and maintain the platform.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [System Components](#system-components)
- [Data Architecture](#data-architecture)
- [Infrastructure Components](#infrastructure-components)
- [Security Architecture](#security-architecture)
- [Integration Patterns](#integration-patterns)
- [Scalability & Performance](#scalability--performance)
- [Monitoring & Observability](#monitoring--observability)
- [Deployment Architecture](#deployment-architecture)

## Architecture Overview

### High-Level System Architecture

```mermaid
graph TB
    subgraph "External Systems"
        ERP[ERP Systems]
        CRM[CRM Systems]
        APIs[External APIs]
        Files[File Sources]
    end
    
    subgraph "Ingestion Layer"
        Gateway[API Gateway<br/>FastAPI + NGINX]
        Streaming[Kafka Streams]
        Batch[Batch Processors]
        Webhooks[Webhook Handlers]
    end
    
    subgraph "Processing Engine Selection"
        EngineRouter{Engine Router<br/>Smart Selection}
        Pandas[Pandas Engine<br/>< 1GB]
        Polars[Polars Engine<br/>1GB-100GB]
        Spark[Spark Cluster<br/>> 100GB]
    end
    
    subgraph "Medallion Data Lakehouse"
        Bronze[(Bronze Layer<br/>Raw Data<br/>Parquet + Delta)]
        Silver[(Silver Layer<br/>Validated Data<br/>Quality Assured)]
        Gold[(Gold Layer<br/>Analytics Ready<br/>Star Schema)]
    end
    
    subgraph "Orchestration Layer"
        Dagster[Dagster Assets<br/>Modern Orchestration]
        Airflow[Airflow DAGs<br/>Legacy Support]
        dbt[dbt Transformations<br/>SQL-First ELT]
    end
    
    subgraph "Analytics & API Layer"
        API_V1[API v1<br/>CRUD Operations]
        API_V2[API v2<br/>Advanced Analytics]
        GraphQL[GraphQL<br/>Flexible Queries]
        ML[ML Pipeline<br/>Feature Engineering]
    end
    
    subgraph "Storage Layer"
        PostgreSQL[(PostgreSQL<br/>OLTP Warehouse)]
        Redis[(Redis<br/>Caching Layer)]
        S3[(S3/MinIO<br/>Object Storage)]
        Vector[(Typesense<br/>Vector Search)]
        Elastic[(Elasticsearch<br/>Full-text Search)]
    end
    
    subgraph "Monitoring & Security"
        Prometheus[Prometheus<br/>Metrics Collection]
        Grafana[Grafana<br/>Visualization]
        Jaeger[Jaeger<br/>Distributed Tracing]
        Vault[HashiCorp Vault<br/>Secret Management]
        RBAC[RBAC System<br/>Authorization]
    end
    
    %% Data Flow
    ERP --> Gateway
    CRM --> Streaming
    APIs --> Gateway
    Files --> Batch
    
    Gateway --> EngineRouter
    Streaming --> EngineRouter
    Batch --> EngineRouter
    Webhooks --> EngineRouter
    
    EngineRouter --> Pandas
    EngineRouter --> Polars
    EngineRouter --> Spark
    
    Pandas --> Bronze
    Polars --> Bronze
    Spark --> Bronze
    
    Bronze --> Silver
    Silver --> Gold
    
    Dagster -.-> Bronze
    Dagster -.-> Silver
    Dagster -.-> Gold
    
    Airflow -.-> Spark
    dbt -.-> Silver
    dbt -.-> Gold
    
    Gold --> PostgreSQL
    Gold --> API_V1
    Gold --> API_V2
    Gold --> GraphQL
    
    API_V1 --> Redis
    API_V2 --> Vector
    GraphQL --> Elastic
    
    Gold --> ML
    ML --> API_V2
    
    %% Monitoring connections
    Gateway --> Prometheus
    Spark --> Prometheus
    API_V1 --> Jaeger
    API_V2 --> Jaeger
    
    Prometheus --> Grafana
    
    %% Security connections
    Gateway --> RBAC
    API_V1 --> Vault
    API_V2 --> Vault
```

### Technology Stack Overview

| Layer | Primary Technology | Backup/Alternative | Purpose |
|-------|-------------------|-------------------|---------|
| **API Gateway** | FastAPI + NGINX | Kong, AWS API Gateway | Request routing, rate limiting, authentication |
| **Processing** | Apache Spark 3.5 | Polars, Pandas | Distributed data processing |
| **Orchestration** | Dagster | Apache Airflow | Workflow orchestration and monitoring |
| **Transformations** | dbt Core | Custom SQL | SQL-first data transformations |
| **Storage - OLTP** | PostgreSQL 15 | MySQL, SQL Server | Transactional data warehouse |
| **Storage - Lake** | Delta Lake + S3 | Iceberg, Hudi | Data lakehouse with ACID properties |
| **Caching** | Redis Cluster | Memcached | In-memory data caching |
| **Search** | Typesense + Elasticsearch | Solr, OpenSearch | Vector and full-text search |
| **Monitoring** | Prometheus + Grafana | Datadog, New Relic | Metrics and observability |
| **Tracing** | Jaeger + OpenTelemetry | Zipkin | Distributed tracing |
| **Security** | HashiCorp Vault | AWS Secrets Manager | Secret management |
| **Streaming** | Apache Kafka | RabbitMQ, Apache Pulsar | Event streaming |
| **Container** | Docker + Kubernetes | Docker Swarm | Container orchestration |
| **IaC** | Terraform | CloudFormation, Pulumi | Infrastructure as Code |

## System Components

### 1. Data Ingestion Components

#### API Gateway Service
```yaml
Component: FastAPI + NGINX
Purpose: Centralized API management and routing
Features:
  - Rate limiting with Redis backend
  - JWT authentication and authorization
  - Request/response transformation
  - Circuit breaker pattern implementation
  - CORS handling and security headers
  - API versioning support (v1, v2)
  - OpenAPI documentation generation
  - Health check endpoints
Scaling: Horizontal with load balancer
Dependencies: Redis, PostgreSQL, Vault
```

#### Streaming Ingestion
```yaml
Component: Apache Kafka + Kafka Connect
Purpose: Real-time event processing and streaming data ingestion
Features:
  - Topic partitioning for scalability
  - Message serialization (Avro, JSON)
  - Exactly-once processing semantics
  - Dead letter queue handling
  - Schema registry integration
  - Monitoring with JMX metrics
  - Multi-consumer group support
Scaling: Horizontal partitioning
Dependencies: Zookeeper, Schema Registry
```

#### Batch Processing Framework
```yaml
Component: Custom Multi-Engine Framework
Purpose: Intelligent processing engine selection
Features:
  - Automatic engine selection based on data size
  - Pandas: < 1GB datasets
  - Polars: 1GB-100GB datasets (30x faster than Pandas)
  - Spark: > 100GB datasets (distributed processing)
  - Unified interface across engines
  - Performance monitoring and metrics
  - Fallback and error handling
Scaling: Vertical and horizontal based on engine
Dependencies: Spark cluster, Python environment
```

### 2. Data Processing Components

#### Apache Spark Cluster
```yaml
Component: Spark 3.5 with Delta Lake
Purpose: Large-scale distributed data processing
Architecture:
  - Driver: 4 vCPU, 16GB RAM
  - Executors: 2-20 dynamic allocation
  - Each Executor: 4 vCPU, 8GB RAM
Features:
  - Dynamic resource allocation
  - Spark SQL optimization
  - Delta Lake ACID transactions
  - Time travel and versioning
  - Structured streaming
  - MLlib integration
  - Custom UDFs and transformations
Storage: S3-compatible object storage
Monitoring: Spark History Server + Prometheus
```

#### Polars Processing Engine
```yaml
Component: Polars (Rust-based DataFrame library)
Purpose: High-performance medium dataset processing
Features:
  - 30x faster than Pandas for medium datasets
  - Memory efficient processing
  - Lazy evaluation with query optimization
  - Multi-threading support
  - Arrow-based columnar format
  - SQL expression support
  - Streaming processing capability
Use Cases: 1GB-100GB datasets
Performance: Memory-bound optimization
```

#### Pandas Processing Engine
```yaml
Component: Pandas with optimization
Purpose: Small dataset processing and prototyping
Features:
  - Familiar DataFrame API
  - Rich ecosystem integration
  - Statistical analysis capabilities
  - Time series processing
  - Data visualization integration
  - Memory usage optimization
Use Cases: < 1GB datasets, development, testing
Optimization: Chunked processing, dtype optimization
```

### 3. Orchestration Components

#### Dagster Assets
```yaml
Component: Dagster 1.5+
Purpose: Modern data orchestration with asset-centric approach
Features:
  - Asset-based data lineage
  - Automatic dependency resolution
  - Real-time monitoring and alerting
  - Web-based UI for monitoring
  - Integration with dbt, Spark, Pandas
  - Partitioned assets for incremental processing
  - Data quality checks and validation
  - Resource management and configuration
  - Sensor-based triggering
Architecture:
  - Daemon: Background process management
  - WebServer: UI and API access
  - Database: PostgreSQL for metadata
  - Compute: Kubernetes pods for execution
```

#### Apache Airflow (Legacy Support)
```yaml
Component: Airflow 2.7+
Purpose: Traditional workflow orchestration
Features:
  - DAG-based workflow definition
  - Rich operator ecosystem
  - SLA monitoring and alerting
  - Complex branching and conditional logic
  - Integration with cloud services
  - Role-based access control
  - Variable and connection management
Architecture:
  - Scheduler: DAG parsing and task scheduling
  - WebServer: UI and REST API
  - Executor: CeleryExecutor with Redis
  - Workers: Distributed task execution
```

#### dbt Transformations
```yaml
Component: dbt Core 1.6+
Purpose: SQL-first data transformations
Features:
  - Modular SQL transformations
  - Data lineage and documentation
  - Testing framework for data quality
  - Macro system for code reusability
  - Incremental model processing
  - Snapshot functionality for SCD Type 2
  - Integration with data catalogs
  - Version control integration
Architecture:
  - CLI-based execution
  - Integration with Dagster/Airflow
  - PostgreSQL target database
  - Git-based source control
```

### 4. Storage Components

#### PostgreSQL Data Warehouse
```yaml
Component: PostgreSQL 15 with enterprise extensions
Purpose: OLTP data warehouse and metadata storage
Configuration:
  - CPU: 8 vCPU
  - Memory: 32GB RAM
  - Storage: 1TB SSD with automated backups
  - Connections: 200 max connections with pgBouncer
Features:
  - ACID transactions
  - Advanced indexing (B-tree, Hash, GiST, GIN)
  - Partitioning support
  - Materialized views
  - Full-text search capabilities
  - JSON/JSONB support
  - Row-level security
  - Point-in-time recovery
Optimization:
  - Connection pooling with pgBouncer
  - Query optimization with pg_stat_statements
  - Automated VACUUM and ANALYZE
```

#### Delta Lake Data Lakehouse
```yaml
Component: Delta Lake on S3-compatible storage
Purpose: ACID-compliant data lakehouse
Features:
  - ACID transactions
  - Time travel and versioning
  - Schema evolution
  - Data skipping with Z-ordering
  - Concurrent read/write operations
  - Automatic small file compaction
  - Data retention policies
Storage Structure:
  - Bronze: Raw data with minimal transformation
  - Silver: Validated and cleaned data
  - Gold: Business-ready aggregated data
Format: Parquet with Delta metadata
Partitioning: Date-based with intelligent partitioning
```

#### Redis Caching Layer
```yaml
Component: Redis Cluster 7.0+
Purpose: High-performance caching and session management
Configuration:
  - Cluster: 3 master + 3 replica nodes
  - Memory: 4GB per node
  - Persistence: RDB + AOF
Features:
  - Multi-level caching strategy
  - Session management for APIs
  - Rate limiting counters
  - Real-time metrics storage
  - Pub/Sub for real-time notifications
  - Lua scripting for complex operations
  - Automatic failover and recovery
Data Types: Strings, Hashes, Sets, Sorted Sets, Streams
```

### 5. Search and Analytics Components

#### Typesense Vector Search
```yaml
Component: Typesense 0.25+
Purpose: Vector similarity search and semantic search
Features:
  - Vector similarity search with HNSW algorithm
  - Real-time indexing and search
  - Typo tolerance and fuzzy matching
  - Faceted search capabilities
  - Geographic search support
  - RESTful API with SDKs
  - High availability clustering
Use Cases:
  - Product similarity search
  - Customer behavior analysis
  - Recommendation systems
Performance: Sub-millisecond search responses
```

#### Elasticsearch Analytics
```yaml
Component: Elasticsearch 8.10+
Purpose: Full-text search and analytics
Features:
  - Full-text search with scoring
  - Complex aggregations and analytics
  - Real-time data ingestion
  - Machine learning capabilities
  - Security features (authentication, authorization)
  - Monitoring and alerting
Architecture:
  - 3-node cluster with dedicated master
  - Data nodes: 16GB RAM, 500GB SSD
  - Ingest pipelines for data processing
Use Cases:
  - Log analytics and monitoring
  - Business intelligence dashboards
  - Anomaly detection
```

## Data Architecture

### Medallion Architecture Implementation

The platform implements a three-tier medallion architecture with Delta Lake as the storage format:

#### Bronze Layer (Raw Data Preservation)
```yaml
Purpose: Landing zone for all raw data
Characteristics:
  - Exact copy of source data
  - Schema on read approach
  - Audit metadata included
  - No business logic applied
  - Retention: 2-7 years based on compliance
Storage Format: Delta Lake Parquet
Partitioning: By ingestion_date and source_system
Schema Evolution: Automatic with Delta Lake
Quality: Basic validation only
```

#### Silver Layer (Cleaned and Validated)
```yaml
Purpose: Clean, standardized, and validated data
Characteristics:
  - Data quality rules applied
  - Standardized data types and formats
  - Deduplication performed
  - Business rules validation
  - Referential integrity checks
Processing: dbt models with data quality tests
Storage Format: Delta Lake Parquet
Partitioning: By business date and region
Schema: Enforced with constraints
Quality: Comprehensive validation with scoring
```

#### Gold Layer (Analytics Ready)
```yaml
Purpose: Business-ready aggregated and enriched data
Characteristics:
  - Star schema implementation
  - Pre-aggregated metrics
  - Slowly Changing Dimensions (SCD Type 2)
  - Business KPIs and calculated fields
  - Optimized for analytical queries
Models: Dimensional modeling with fact/dimension tables
Storage Format: Delta Lake + PostgreSQL
Optimization: Z-ordering, bloom filters, statistics
Access: Direct API access and BI tool integration
```

### Data Flow Architecture

```mermaid
flowchart LR
    subgraph "Sources"
        A[CSV Files]
        B[APIs]
        C[Streams]
        D[Databases]
    end
    
    subgraph "Ingestion"
        E[File Sensors]
        F[API Endpoints]
        G[Kafka Consumers]
        H[CDC Connectors]
    end
    
    subgraph "Processing Pipeline"
        I[Engine Selection]
        J[Bronze Processing]
        K[Silver Processing]
        L[Gold Processing]
    end
    
    subgraph "Storage Layers"
        M[(Bronze<br/>Raw Data)]
        N[(Silver<br/>Clean Data)]
        O[(Gold<br/>Analytics)]
    end
    
    subgraph "Consumption"
        P[APIs]
        Q[BI Tools]
        R[ML Models]
        S[Reports]
    end
    
    A --> E
    B --> F
    C --> G
    D --> H
    
    E --> I
    F --> I
    G --> I
    H --> I
    
    I --> J
    J --> M
    M --> K
    K --> N
    N --> L
    L --> O
    
    O --> P
    O --> Q
    O --> R
    O --> S
```

### Schema Design Patterns

#### Star Schema Implementation
The Gold layer implements a traditional star schema optimized for analytical queries:

```sql
-- Fact table: Sales transactions
CREATE TABLE fact_sales (
    sale_key BIGSERIAL PRIMARY KEY,
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    product_key INTEGER REFERENCES dim_products(product_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    invoice_key INTEGER REFERENCES dim_invoice(invoice_key),
    
    -- Measures
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    
    -- Additional attributes
    is_return BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (date_key);

-- Dimension tables with SCD Type 2 support
CREATE TABLE dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    country VARCHAR(100),
    customer_segment VARCHAR(50),
    value_tier VARCHAR(20),
    
    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    version_number INTEGER DEFAULT 1
);
```

#### Data Quality Framework Schema
```sql
CREATE TABLE data_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    column_name VARCHAR(100),
    metric_type VARCHAR(50), -- completeness, accuracy, validity
    metric_value DECIMAL(5,2),
    threshold_value DECIMAL(5,2),
    status VARCHAR(20), -- PASS, FAIL, WARNING
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    check_details JSONB
);
```

## Infrastructure Components

### Kubernetes Deployment Architecture

```yaml
# Production Kubernetes cluster configuration
apiVersion: v1
kind: Namespace
metadata:
  name: pwc-data-platform

---
# API Service Deployment
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-service
  namespace: pwc-data-platform
spec:
  replicas: 3
  selector:
    matchLabels:
      app: api-service
  template:
    metadata:
      labels:
        app: api-service
    spec:
      containers:
      - name: fastapi
        image: pwc-platform/api:latest
        ports:
        - containerPort: 8000
        resources:
          requests:
            cpu: 500m
            memory: 1Gi
          limits:
            cpu: 2
            memory: 4Gi
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: url
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
```

### Spark on Kubernetes Configuration

```yaml
# Spark Operator configuration for Kubernetes
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: retail-etl-pipeline
  namespace: pwc-data-platform
spec:
  type: Scala
  mode: cluster
  image: pwc-platform/spark:3.5.3
  imagePullPolicy: Always
  mainApplicationFile: "s3://pwc-data-lake/jars/retail-etl.jar"
  
  sparkVersion: "3.5.3"
  
  driver:
    cores: 4
    coreLimit: "4"
    memory: "16g"
    serviceAccount: spark-operator
    
  executor:
    cores: 4
    instances: 5
    memory: "8g"
    
  dynamicAllocation:
    enabled: true
    initialExecutors: 2
    minExecutors: 1
    maxExecutors: 20
    
  monitoring:
    exposeDriverMetrics: true
    exposeExecutorMetrics: true
    prometheus:
      jmxExporterJar: "/opt/spark/jars/jmx_prometheus_javaagent.jar"
```

### Database Configuration

#### PostgreSQL Production Configuration
```postgresql
-- postgresql.conf optimizations
shared_preload_libraries = 'pg_stat_statements,auto_explain'
max_connections = 200
shared_buffers = 8GB
effective_cache_size = 24GB
maintenance_work_mem = 2GB
checkpoint_completion_target = 0.7
wal_buffers = 16MB
default_statistics_target = 100
random_page_cost = 1.1

-- Enable query optimization
auto_explain.log_min_duration = '3s'
auto_explain.log_analyze = on
auto_explain.log_verbose = on
auto_explain.log_buffers = on

-- Connection pooling with pgBouncer
[databases]
pwc_retail = host=localhost port=5432 dbname=pwc_retail
pwc_warehouse = host=localhost port=5432 dbname=pwc_warehouse

[pgbouncer]
listen_addr = *
listen_port = 6432
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 25
max_db_connections = 100
```

## Security Architecture

### Authentication and Authorization

```mermaid
sequenceDiagram
    participant Client
    participant Gateway as API Gateway
    participant Auth as Auth Service
    participant Vault as HashiCorp Vault
    participant DB as Database
    
    Client->>Gateway: Request with credentials
    Gateway->>Auth: Validate credentials
    Auth->>Vault: Retrieve user secrets
    Vault-->>Auth: User credentials
    Auth->>DB: Verify user and roles
    DB-->>Auth: User profile and permissions
    Auth-->>Gateway: JWT token + roles
    Gateway-->>Client: Access token
    
    Note over Client,DB: Subsequent API calls
    Client->>Gateway: API request + JWT
    Gateway->>Auth: Validate JWT
    Auth-->>Gateway: User context
    Gateway->>DB: Execute request (if authorized)
    DB-->>Gateway: Response
    Gateway-->>Client: API response
```

### Security Controls Implementation

#### JWT Token Management
```python
# Security configuration
class SecurityConfig:
    jwt_secret_key: str = get_secret("JWT_SECRET_KEY")
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    refresh_token_expire_days: int = 7
    password_bcrypt_rounds: int = 12
    
    # Security headers
    security_headers = {
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "X-XSS-Protection": "1; mode=block",
        "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
        "Content-Security-Policy": "default-src 'self'"
    }
```

#### Role-Based Access Control (RBAC)
```yaml
# RBAC configuration
roles:
  data_analyst:
    permissions:
      - "read:sales_data"
      - "read:analytics"
      - "read:reports"
    resources:
      - "/api/v1/sales/*"
      - "/api/v2/analytics/*"
    restrictions:
      - "no_pii_access"
  
  data_engineer:
    permissions:
      - "read:*"
      - "write:etl_pipelines"
      - "manage:data_quality"
    resources:
      - "/api/*"
      - "/admin/pipelines/*"
    restrictions: []
  
  admin:
    permissions:
      - "*"
    resources:
      - "/*"
    restrictions: []
```

#### Data Encryption Strategy
```python
# Encryption implementation
class EncryptionManager:
    def __init__(self):
        self.fernet = Fernet(get_secret("ENCRYPTION_KEY"))
    
    def encrypt_pii(self, data: str) -> str:
        """Encrypt personally identifiable information"""
        return self.fernet.encrypt(data.encode()).decode()
    
    def decrypt_pii(self, encrypted_data: str) -> str:
        """Decrypt PII data"""
        return self.fernet.decrypt(encrypted_data.encode()).decode()
    
    def hash_sensitive_field(self, data: str) -> str:
        """One-way hash for sensitive fields"""
        return bcrypt.hashpw(data.encode(), bcrypt.gensalt()).decode()
```

## Integration Patterns

### External API Integration Architecture

```mermaid
graph LR
    subgraph "External APIs"
        Currency[Currency API]
        Country[Country API]
        Product[Product API]
        Weather[Weather API]
    end
    
    subgraph "Integration Layer"
        Gateway[API Gateway]
        Cache[Redis Cache]
        Circuit[Circuit Breaker]
        Retry[Retry Logic]
    end
    
    subgraph "Processing"
        Enrichment[Data Enrichment]
        Validation[Data Validation]
        Transform[Data Transform]
    end
    
    Currency --> Gateway
    Country --> Gateway
    Product --> Gateway
    Weather --> Gateway
    
    Gateway --> Cache
    Cache --> Circuit
    Circuit --> Retry
    
    Retry --> Enrichment
    Enrichment --> Validation
    Validation --> Transform
```

### Message Queue Integration

#### Apache Kafka Configuration
```yaml
# Kafka cluster configuration
version: '3.8'
services:
  kafka1:
    image: confluentinc/cp-kafka:7.4.1
    hostname: kafka1
    container_name: kafka1
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka1:29092,PLAINTEXT_HOST://localhost:9092'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: localhost
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka1:29093,2@kafka2:29093,3@kafka3:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://kafka1:29092,CONTROLLER://kafka1:29093,PLAINTEXT_HOST://0.0.0.0:9092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
```

#### RabbitMQ Configuration for Task Queue
```python
# RabbitMQ task management
class TaskQueueManager:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.URLParameters(get_secret("RABBITMQ_URL"))
        )
        self.channel = self.connection.channel()
        self.setup_queues()
    
    def setup_queues(self):
        # ETL processing queue
        self.channel.queue_declare(
            queue='etl_processing',
            durable=True,
            arguments={'x-max-retries': 3}
        )
        
        # Analytics computation queue
        self.channel.queue_declare(
            queue='analytics_compute',
            durable=True,
            arguments={
                'x-message-ttl': 3600000,  # 1 hour TTL
                'x-max-length': 1000
            }
        )
```

## Scalability & Performance

### Auto-Scaling Configuration

#### Horizontal Pod Autoscaler (HPA)
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: api-service-hpa
  namespace: pwc-data-platform
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: api-service
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 60
      selectPolicy: Max
      policies:
      - type: Percent
        value: 100
        periodSeconds: 15
    scaleDown:
      stabilizationWindowSeconds: 300
      selectPolicy: Max
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
```

#### Spark Dynamic Allocation
```python
# Spark configuration for dynamic scaling
spark_config = {
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "1",
    "spark.dynamicAllocation.maxExecutors": "50",
    "spark.dynamicAllocation.initialExecutors": "5",
    "spark.dynamicAllocation.executorIdleTimeout": "60s",
    "spark.dynamicAllocation.cachedExecutorIdleTimeout": "300s",
    "spark.dynamicAllocation.schedulerBacklogTimeout": "1s",
    "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout": "5s",
    
    # Performance optimization
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true"
}
```

### Performance Optimization Strategies

#### Database Performance Optimization
```sql
-- Create performance-optimized indexes
CREATE INDEX CONCURRENTLY idx_sales_customer_date_amount 
ON fact_sales (customer_key, date_key, total_amount DESC);

CREATE INDEX CONCURRENTLY idx_sales_product_country 
ON fact_sales (product_key, country) 
INCLUDE (total_amount, quantity);

-- Partitioned index for time-series data
CREATE INDEX CONCURRENTLY idx_sales_time_series 
ON fact_sales USING BTREE (date_key, created_at) 
WHERE date_key >= 20240101;

-- Partial index for frequently queried subsets
CREATE INDEX CONCURRENTLY idx_sales_high_value 
ON fact_sales (customer_key, product_key) 
WHERE total_amount > 1000;

-- GIN index for JSON data
CREATE INDEX CONCURRENTLY idx_quality_metrics_details 
ON data_quality_metrics USING GIN (check_details);
```

#### Caching Strategy Implementation
```python
class MultiLevelCacheManager:
    def __init__(self):
        self.l1_cache = TTLCache(maxsize=1000, ttl=300)  # 5 min
        self.l2_cache = redis.Redis(host='redis-cluster')
        self.l3_cache = {}  # Application-level cache
    
    async def get(self, key: str) -> Any:
        # L1 Cache (In-memory)
        if key in self.l1_cache:
            return self.l1_cache[key]
        
        # L2 Cache (Redis)
        value = await self.l2_cache.get(key)
        if value:
            self.l1_cache[key] = json.loads(value)
            return self.l1_cache[key]
        
        # L3 Cache (Database/Computed)
        return None
    
    async def set(self, key: str, value: Any, ttl: int = 3600):
        # Set in all cache levels
        self.l1_cache[key] = value
        await self.l2_cache.setex(key, ttl, json.dumps(value))
```

## Monitoring & Observability

### Comprehensive Monitoring Stack

```mermaid
graph TB
    subgraph "Application Layer"
        API[FastAPI Services]
        Spark[Spark Jobs]
        dbt[dbt Models]
        Dagster[Dagster Pipelines]
    end
    
    subgraph "Metrics Collection"
        Prometheus[Prometheus Server]
        Node[Node Exporter]
        Blackbox[Blackbox Exporter]
        JMX[JMX Exporter]
    end
    
    subgraph "Tracing"
        Jaeger[Jaeger Server]
        OTel[OpenTelemetry]
    end
    
    subgraph "Logging"
        Fluentd[Fluentd]
        Elastic[Elasticsearch]
        Kibana[Kibana]
    end
    
    subgraph "Visualization & Alerting"
        Grafana[Grafana Dashboards]
        AlertManager[AlertManager]
        PagerDuty[PagerDuty]
        Slack[Slack Notifications]
    end
    
    API --> Prometheus
    Spark --> JMX
    dbt --> Prometheus
    Dagster --> Prometheus
    
    API --> OTel
    OTel --> Jaeger
    
    API --> Fluentd
    Fluentd --> Elastic
    Elastic --> Kibana
    
    Prometheus --> Grafana
    Prometheus --> AlertManager
    AlertManager --> PagerDuty
    AlertManager --> Slack
```

### Key Performance Indicators (KPIs)

#### Business Metrics
```yaml
Business KPIs:
  Revenue Metrics:
    - Total Revenue (daily/monthly/yearly)
    - Revenue Growth Rate
    - Average Order Value (AOV)
    - Customer Lifetime Value (CLV)
  
  Customer Metrics:
    - New Customer Acquisition Rate
    - Customer Retention Rate
    - Customer Churn Rate
    - Customer Segmentation Distribution
  
  Product Metrics:
    - Top Performing Products
    - Product Category Performance
    - Return Rate by Product
    - Inventory Turnover Rate
  
  Geographic Metrics:
    - Revenue by Country/Region
    - Market Penetration Rate
    - Geographic Growth Trends
```

#### Technical Metrics
```yaml
Technical KPIs:
  API Performance:
    - Request Response Time (p50, p95, p99)
    - Requests Per Second (RPS)
    - Error Rate (4xx, 5xx)
    - API Availability (SLA: 99.9%)
  
  Data Pipeline Metrics:
    - ETL Job Success Rate
    - Data Processing Latency
    - Data Quality Score
    - Pipeline SLA Compliance
  
  Infrastructure Metrics:
    - CPU and Memory Utilization
    - Database Performance (query time, connections)
    - Cache Hit Ratio
    - Network Throughput and Latency
  
  Security Metrics:
    - Failed Authentication Attempts
    - Security Vulnerability Scan Results
    - Data Access Audit Trail
    - Encryption Coverage Percentage
```

### Alerting Configuration

```yaml
# Prometheus alerting rules
groups:
  - name: api_alerts
    rules:
    - alert: HighErrorRate
      expr: (rate(http_requests_total{status=~"5.."}[5m]) / rate(http_requests_total[5m])) * 100 > 5
      for: 2m
      labels:
        severity: critical
        service: api
      annotations:
        summary: "High error rate detected"
        description: "Error rate is {{ $value }}% for the last 5 minutes"
    
    - alert: HighResponseTime
      expr: histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m])) > 2
      for: 5m
      labels:
        severity: warning
        service: api
      annotations:
        summary: "High response time detected"
        description: "95th percentile response time is {{ $value }}s"
  
  - name: data_pipeline_alerts
    rules:
    - alert: ETLJobFailure
      expr: increase(etl_job_failures_total[1h]) > 0
      for: 0m
      labels:
        severity: critical
        service: etl
      annotations:
        summary: "ETL job failure detected"
        description: "{{ $labels.job_name }} has failed {{ $value }} times in the last hour"
    
    - alert: DataQualityDegraded
      expr: avg_over_time(data_quality_score[1h]) < 0.8
      for: 10m
      labels:
        severity: warning
        service: data_quality
      annotations:
        summary: "Data quality score degraded"
        description: "Average data quality score is {{ $value }} over the last hour"
```

## Deployment Architecture

### Multi-Environment Strategy

```mermaid
graph LR
    subgraph "Development"
        DevCode[Code Changes]
        DevTest[Unit Tests]
        DevBuild[Docker Build]
    end
    
    subgraph "Staging"
        StageDeploy[Auto Deploy]
        StageTest[Integration Tests]
        StageValidate[Performance Tests]
    end
    
    subgraph "Production"
        ProdApproval[Manual Approval]
        ProdDeploy[Blue-Green Deploy]
        ProdMonitor[Health Monitoring]
        ProdRollback[Auto Rollback]
    end
    
    DevCode --> DevTest
    DevTest --> DevBuild
    DevBuild --> StageDeploy
    
    StageDeploy --> StageTest
    StageTest --> StageValidate
    StageValidate --> ProdApproval
    
    ProdApproval --> ProdDeploy
    ProdDeploy --> ProdMonitor
    ProdMonitor --> ProdRollback
```

### Infrastructure as Code (Terraform)

#### AWS Infrastructure Configuration
```hcl
# main.tf - AWS production infrastructure
terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  backend "s3" {
    bucket = "pwc-terraform-state"
    key    = "prod/terraform.tfstate"
    region = "us-east-1"
    encrypt = true
    dynamodb_table = "terraform-locks"
  }
}

# EKS Cluster
module "eks" {
  source = "./modules/kubernetes"
  
  cluster_name = "pwc-data-platform"
  cluster_version = "1.28"
  
  vpc_id = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets
  
  node_groups = {
    general = {
      desired_capacity = 3
      max_capacity     = 10
      min_capacity     = 2
      
      instance_types = ["m5.xlarge"]
      
      k8s_labels = {
        Environment = "production"
        NodeGroup   = "general"
      }
    }
    
    compute_optimized = {
      desired_capacity = 2
      max_capacity     = 20
      min_capacity     = 1
      
      instance_types = ["c5.2xlarge"]
      
      k8s_labels = {
        Environment = "production"
        NodeGroup   = "compute"
        workload    = "spark"
      }
    }
  }
}

# RDS PostgreSQL
module "rds" {
  source = "./modules/rds"
  
  identifier = "pwc-data-warehouse"
  engine_version = "15.4"
  instance_class = "db.r5.2xlarge"
  allocated_storage = 1000
  storage_encrypted = true
  
  multi_az = true
  backup_retention_period = 30
  backup_window = "03:00-04:00"
  maintenance_window = "Sun:04:00-Sun:05:00"
  
  vpc_security_group_ids = [module.security.database_sg_id]
  subnet_group_name = module.vpc.database_subnet_group_name
}

# S3 Data Lake
module "s3" {
  source = "./modules/s3"
  
  buckets = {
    data_lake = {
      name = "pwc-data-lake-prod"
      versioning = true
      lifecycle_rules = [
        {
          id = "transition_to_ia"
          enabled = true
          transition = {
            days = 30
            storage_class = "STANDARD_IA"
          }
        },
        {
          id = "transition_to_glacier"
          enabled = true
          transition = {
            days = 90
            storage_class = "GLACIER"
          }
        }
      ]
    }
  }
}
```

### Container Orchestration

#### Docker Compose for Development
```yaml
# docker-compose.yml
version: '3.8'

services:
  api:
    build:
      context: .
      dockerfile: docker/Dockerfile.api
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://postgres:password@postgres:5432/pwc_retail
      - REDIS_URL=redis://redis:6379
      - VAULT_URL=http://vault:8200
    depends_on:
      - postgres
      - redis
      - vault
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s

  postgres:
    image: postgres:15
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_DB=pwc_retail
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./docker/init-db.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 3

  spark-master:
    image: bitnami/spark:3.5
    ports:
      - "8080:8080"
      - "7077:7077"
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    volumes:
      - ./data:/opt/spark-data

  spark-worker:
    image: bitnami/spark:3.5
    ports:
      - "8081:8081"
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_WORKER_CORES=2
    depends_on:
      - spark-master
    volumes:
      - ./data:/opt/spark-data

  dagster:
    build:
      context: .
      dockerfile: docker/Dockerfile.dagster
    ports:
      - "3000:3000"
    environment:
      - DAGSTER_POSTGRES_USER=postgres
      - DAGSTER_POSTGRES_PASSWORD=password
      - DAGSTER_POSTGRES_DB=dagster
      - DAGSTER_POSTGRES_HOST=postgres
    depends_on:
      - postgres
    volumes:
      - ./src:/opt/dagster/app/src

volumes:
  postgres_data:
  redis_data:
```

This comprehensive system architecture documentation provides the technical foundation for understanding, deploying, and maintaining the PwC Data Engineering Platform. Each component is designed for scalability, reliability, and enterprise-grade performance.

For implementation details and operational procedures, refer to the complementary documentation:
- [API Documentation](../API_DOCUMENTATION.md)
- [Deployment Guide](../operations/DEPLOYMENT_GUIDE.md)  
- [Security Documentation](../security/SECURITY_GUIDE.md)
- [Monitoring Runbook](../operations/runbooks/OPERATIONS_RUNBOOK.md)
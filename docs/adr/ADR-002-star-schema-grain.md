# ADR-002: Star Schema Grain Strategy

## Status
Accepted

## Context

Our data warehouse needs to support analytical queries across sales data with varying levels of granularity. The choice of fact table grain (level of detail) fundamentally impacts query performance, storage requirements, and analytical capabilities.

### Business Requirements

1. **Sales Analysis**: Support detailed sales analysis at transaction line level
2. **Customer Analytics**: Enable customer behavior analysis and segmentation
3. **Product Performance**: Track product sales performance over time
4. **Store Operations**: Support store-level operational reporting
5. **Financial Reporting**: Enable accurate financial consolidations and reporting

### Current State Challenges

1. **Multiple Grain Levels**: Business users need both detailed and summarized views
2. **Performance Trade-offs**: Detailed grain impacts query performance for aggregate queries
3. **Storage Costs**: Fine-grained fact tables can become very large
4. **Data Quality**: Lower grain increases complexity of data quality validation
5. **Historical Analysis**: Need to support historical trend analysis and comparisons

### Analytical Use Cases

#### Primary Use Cases
- **Transaction Analysis**: Line-by-line transaction details for auditing and analysis
- **Product Performance**: Which products are selling well by store, time period
- **Customer Journey**: Track customer purchase patterns over time
- **Inventory Planning**: Understand demand patterns for inventory optimization

#### Secondary Use Cases
- **Financial Reconciliation**: Match sales data to accounting systems
- **Fraud Detection**: Identify unusual patterns in transaction data
- **Marketing Attribution**: Track campaign effectiveness to transaction level
- **Operational Analytics**: Store performance, staff productivity analysis

## Decision

We will implement a **multi-grain star schema approach** with the primary fact table at **invoice line item grain** and supporting aggregate fact tables for performance.

### Primary Fact Table: `fact_sales` (Invoice Line Grain)

**Grain Definition**: One record per invoice line item
**Grain Columns**: `invoice_id` + `line_number`

This grain provides maximum flexibility for analysis while maintaining referential integrity with source systems.

### Dimensional Model Structure

```
    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
    │   dim_date  │    │ dim_product │    │dim_customer │
    │             │    │             │    │             │
    │ date_key    │◄───┤ product_key │    │customer_key │
    │ date        │    │ product_id  │    │customer_id  │
    │ year        │    │ name        │    │ name        │
    │ quarter     │    │ category    │    │ segment     │
    │ month       │    │ price       │◄───┤ country     │
    │ fiscal_year │    │ is_current  │    │ is_current  │
    └─────────────┘    └─────────────┘    └─────────────┘
           │                   │                   │
           │                   │                   │
           ▼                   ▼                   ▼
    ┌─────────────────────────────────────────────────────┐
    │                fact_sales                           │
    │                                                     │
    │ invoice_id          (Grain)                         │
    │ line_number         (Grain)                         │
    │ date_key           ─────────────────────────────────┤
    │ product_key        ─────────────────────────────────┤
    │ customer_key       ─────────────────────────────────┤
    │ store_key          ─────────────────────────────────┤
    │                                                     │
    │ quantity           (Additive Measure)               │
    │ unit_price         (Non-additive Measure)           │
    │ discount_amount    (Additive Measure)               │
    │ tax_amount         (Additive Measure)               │
    │ total_amount       (Additive Measure)               │
    │ gross_amount       (Calculated Measure)             │
    │ net_amount         (Calculated Measure)             │
    │ profit_margin      (Calculated Measure)             │
    └─────────────────────────────────────────────────────┘
           ▲
           │
    ┌─────────────┐
    │  dim_store  │
    │             │
    │ store_key   │
    │ store_id    │
    │ name        │
    │ address     │
    │ region      │
    │ manager     │
    └─────────────┘
```

## Rationale

### Why Invoice Line Grain?

1. **Maximum Analytical Flexibility**: Supports all aggregation levels needed by business
2. **Source System Alignment**: Matches natural grain of transactional systems
3. **Auditability**: Maintains traceability to source transactions
4. **Data Quality**: Easier to validate at atomic level
5. **Future Requirements**: Accommodates unforeseen analytical needs

### Grain Comparison Analysis

| Grain Level | Storage Size | Query Performance | Analytical Flexibility | Data Quality |
|-------------|--------------|------------------|----------------------|--------------|
| **Line Item** | ⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Invoice Header | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| Daily Summary | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐ |

## Implementation Strategy

### Phase 1: Core Fact Table ✅
- [x] Implement `fact_sales` at invoice line grain
- [x] Build supporting dimensions with SCD Type 2
- [x] Establish referential integrity constraints
- [x] Implement data quality validations

### Phase 2: Performance Optimization
- [ ] Create materialized aggregate tables
- [ ] Implement automatic aggregation refresh
- [ ] Add query performance monitoring
- [ ] Optimize partition strategy

### Phase 3: Advanced Analytics
- [ ] Add calculated measures and KPIs
- [ ] Implement slowly changing dimension tracking
- [ ] Add data lineage tracking
- [ ] Create business-friendly views

### Fact Table Schema Definition

```sql
CREATE TABLE fact_sales (
    -- Grain columns
    invoice_id          BIGINT NOT NULL,
    line_number         INTEGER NOT NULL,
    
    -- Dimension foreign keys
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_key),
    product_key         BIGINT NOT NULL REFERENCES dim_product(product_key),
    customer_key        BIGINT NOT NULL REFERENCES dim_customer(customer_key),
    store_key           BIGINT NOT NULL REFERENCES dim_store(store_key),
    
    -- Measures (additive)
    quantity            DECIMAL(10,3) NOT NULL,
    unit_price          DECIMAL(10,2) NOT NULL,
    discount_amount     DECIMAL(10,2) DEFAULT 0,
    tax_amount          DECIMAL(10,2) DEFAULT 0,
    total_amount        DECIMAL(10,2) NOT NULL,
    
    -- Calculated measures
    gross_amount        DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price),
    net_amount          DECIMAL(10,2) GENERATED ALWAYS AS (gross_amount - discount_amount),
    profit_margin       DECIMAL(5,4) GENERATED ALWAYS AS (
        CASE WHEN gross_amount > 0 
             THEN (gross_amount - discount_amount) / gross_amount 
             ELSE 0 END
    ),
    
    -- Audit columns
    etl_created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    etl_updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    etl_batch_id        VARCHAR(50) NOT NULL,
    
    -- Constraints
    PRIMARY KEY (invoice_id, line_number),
    CHECK (quantity > 0),
    CHECK (unit_price >= 0),
    CHECK (total_amount >= 0)
);
```

### Partitioning Strategy

```python
# Partition by date for time-based query optimization
PARTITION_COLUMNS = ['date_key']

# Clustering for commonly filtered columns (engine-specific)
CLUSTER_COLUMNS = ['customer_key', 'product_key']
```

## Performance Considerations

### Query Optimization Strategies

1. **Partitioning**: Partition by date_key for time-based queries
2. **Indexing**: Create indexes on commonly filtered dimension keys
3. **Materialized Views**: Pre-aggregate common query patterns
4. **Compression**: Use columnar storage and compression for large fact tables

### Expected Query Patterns

#### High-Frequency Queries (Optimized)
```sql
-- Customer purchase summary by month
SELECT 
    c.customer_name,
    d.year, d.month,
    SUM(f.total_amount) as monthly_total
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2024 AND c.is_current = TRUE
GROUP BY c.customer_name, d.year, d.month;

-- Product performance by category
SELECT 
    p.category,
    COUNT(*) as transaction_count,
    SUM(f.quantity) as total_quantity,
    SUM(f.total_amount) as total_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.category;
```

### Aggregate Fact Tables (Future)

For performance optimization, we plan to create aggregate fact tables:

#### `fact_sales_daily`
- **Grain**: One record per product per store per day
- **Use Case**: Daily reporting dashboards
- **Aggregation**: SUM of measures, COUNT of transactions

#### `fact_sales_monthly` 
- **Grain**: One record per product per customer per month
- **Use Case**: Monthly business reviews
- **Aggregation**: Monthly totals and averages

## Data Quality Rules

### Grain-Specific Validations

1. **Uniqueness**: Each (invoice_id, line_number) combination must be unique
2. **Referential Integrity**: All dimension keys must exist in respective dimensions
3. **Business Rules**: 
   - Quantity must be positive
   - Unit price must be non-negative
   - Total amount should equal (quantity * unit_price) - discount_amount + tax_amount
4. **Completeness**: No NULL values in required measures
5. **Consistency**: Cross-validation with source systems

### Data Quality Monitoring

```python
# Grain validation
def validate_fact_sales_grain(df):
    # Check uniqueness of grain
    grain_cols = ['invoice_id', 'line_number']
    duplicates = df.group_by(grain_cols).count().filter(pl.col('count') > 1)
    
    assert duplicates.height == 0, f"Duplicate grain found: {duplicates}"
    
    # Check referential integrity
    orphaned_products = df.join(
        dim_product, 
        on='product_key', 
        how='anti'
    )
    assert orphaned_products.height == 0, "Orphaned product keys found"
```

## Consequences

### Positive Consequences

1. **Analytical Flexibility**: Supports all current and future analytical requirements
2. **Data Accuracy**: Maintains full precision and auditability
3. **Business Alignment**: Matches natural business process grain
4. **Future-Proof**: Can accommodate new requirements without redesign
5. **Source System Alignment**: Direct mapping to transactional systems

### Negative Consequences

1. **Storage Requirements**: Larger storage footprint than aggregated grains
2. **Query Performance**: Some aggregate queries may be slower initially
3. **ETL Complexity**: More complex data quality validation
4. **Maintenance Overhead**: More records to process and maintain

### Risk Mitigation

1. **Performance**: Implement partitioning, indexing, and aggregate tables
2. **Storage Costs**: Use compression and archiving strategies
3. **Query Complexity**: Provide business-friendly views and reports
4. **Data Volume**: Monitor growth and implement retention policies

## Success Metrics

1. **Query Performance**: <5 second response time for 95% of analytical queries
2. **Data Quality**: >99.9% data quality score for all fact table validations
3. **Storage Efficiency**: <20% storage overhead compared to source systems
4. **User Satisfaction**: >90% satisfaction score from business analysts
5. **System Reliability**: 99.9% uptime for data warehouse queries

## Alternative Grains Considered

### Alternative 1: Invoice Header Grain
**Decision**: One record per invoice (aggregated line items)
**Rejected Because**:
- Loses line-item detail needed for product analysis
- Cannot support item-level discount analysis
- Aggregation logic becomes complex for mixed-item invoices

### Alternative 2: Daily Aggregate Grain  
**Decision**: One record per product per customer per day
**Rejected Because**:
- Loses transaction-level detail needed for operational analysis
- Cannot support time-of-day analysis
- Makes fraud detection and auditing difficult

### Alternative 3: Multiple Fact Tables
**Decision**: Separate fact tables for different grains
**Rejected Because**:
- Increases ETL complexity and maintenance overhead
- Creates data consistency challenges
- Confuses business users about which table to use

## Related Decisions

- [ADR-001: DataFrame Engine Abstraction Strategy](ADR-001-engine-strategy.md)
- [ADR-003: Data Quality Framework](ADR-003-data-quality-framework.md) (planned)
- [ADR-004: SCD Type 2 Implementation Strategy](ADR-004-scd-strategy.md) (planned)

## References

- [The Data Warehouse Toolkit - Kimball & Ross](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/)
- [Star Schema Complete Reference - Christopher Adamson](https://www.oreilly.com/library/view/star-schema-the/9780071744324/)
- [Dimensional Data Warehousing with Apache Spark](https://spark.apache.org/docs/latest/sql-data-sources-delta.html)
- [Modern Data Warehouse Design - Inmon vs Kimball](https://www.integrate.io/blog/inmon-vs-kimball/)
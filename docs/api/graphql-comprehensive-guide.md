# GraphQL API Comprehensive Guide

---
title: GraphQL API Documentation
description: Complete GraphQL schema documentation with queries, mutations, and examples
audience: [developers, integrators, data-analysts]
last_updated: 2025-01-25
version: 3.0.0
owner: API Team
reviewers: [Platform Team, Data Engineering Team]
tags: [graphql, api, schema, queries, mutations, real-time]
---

## Table of Contents

1. [GraphQL Overview](#graphql-overview)
2. [Schema Documentation](#schema-documentation)
3. [Query Examples](#query-examples)
4. [Mutation Examples](#mutation-examples)
5. [Subscription Examples](#subscription-examples)
6. [Advanced Patterns](#advanced-patterns)
7. [Performance Optimization](#performance-optimization)
8. [Integration Guide](#integration-guide)
9. [Best Practices](#best-practices)
10. [Troubleshooting](#troubleshooting)

## GraphQL Overview

The PwC Enterprise Data Engineering Platform provides a comprehensive GraphQL API built with **Strawberry GraphQL** framework, offering:

### 🎯 Core Features

- **Type-Safe Schema**: Fully typed schema with comprehensive validation
- **Real-Time Subscriptions**: Live data updates via WebSocket subscriptions
- **Advanced Filtering**: Complex query filtering with multiple criteria
- **Pagination Support**: Efficient cursor-based and offset-based pagination
- **Data Loaders**: Optimized N+1 query resolution with intelligent batching
- **Authentication**: JWT-based security with role-based access control
- **Performance Monitoring**: Comprehensive query performance tracking

### 🏗️ Architecture

```mermaid
graph LR
    subgraph "Client Layer"
        WEB[Web Apps]
        MOBILE[Mobile Apps]
        DESKTOP[Desktop Apps]
    end
    
    subgraph "GraphQL Layer"
        ENDPOINT[/api/graphql]
        PLAYGROUND[GraphiQL Playground]
        SCHEMA[Schema Registry]
    end
    
    subgraph "Resolver Layer"
        QUERY[Query Resolvers]
        MUTATION[Mutation Resolvers]
        SUBSCRIPTION[Subscription Resolvers]
        DATALOADER[Data Loaders]
    end
    
    subgraph "Data Layer"
        STARSCHEMA[(Star Schema)]
        CACHE[(Redis Cache)]
        SEARCH[(Search Engine)]
    end
    
    WEB --> ENDPOINT
    MOBILE --> ENDPOINT
    DESKTOP --> ENDPOINT
    
    ENDPOINT --> QUERY
    ENDPOINT --> MUTATION
    ENDPOINT --> SUBSCRIPTION
    
    QUERY --> DATALOADER
    MUTATION --> STARSCHEMA
    SUBSCRIPTION --> CACHE
    
    DATALOADER --> STARSCHEMA
    DATALOADER --> CACHE
```

### 🚀 Quick Start

**Endpoint**: `https://api.pwc-data.com/api/graphql`  
**Playground**: `https://api.pwc-data.com/api/graphql` (Interactive GraphiQL interface)  
**Local Development**: `http://localhost:8000/api/graphql`

## Schema Documentation

### Core Types

#### Sale Type

The primary transaction entity representing retail sales data.

```graphql
type Sale {
  """Unique sale identifier"""
  sale_id: String!
  
  """Quantity of items purchased"""
  quantity: Int!
  
  """Unit price of the item"""
  unit_price: Float!
  
  """Total transaction amount"""
  total_amount: Float!
  
  """Applied discount amount"""
  discount_amount: Float!
  
  """Tax amount charged"""
  tax_amount: Float!
  
  """Profit amount (if calculated)"""
  profit_amount: Float
  
  """Profit margin percentage"""
  margin_percentage: Float
  
  """Transaction creation timestamp"""
  created_at: DateTime!
  
  """Associated customer (nullable for guest purchases)"""
  customer: Customer
  
  """Product information"""
  product: Product!
  
  """Country information"""
  country: Country!
}
```

#### Customer Type

Comprehensive customer information with analytics and segmentation.

```graphql
type Customer {
  """Internal customer key"""
  customer_key: Int!
  
  """External customer identifier"""
  customer_id: String
  
  """Customer segment classification"""
  customer_segment: String
  
  """Total lifetime value"""
  lifetime_value: Float
  
  """Total number of orders"""
  total_orders: Int
  
  """Total amount spent"""
  total_spent: Float
  
  """Average order value"""
  avg_order_value: Float
  
  """RFM Analysis Scores"""
  recency_score: Int
  frequency_score: Int
  monetary_score: Int
  
  """RFM segment classification"""
  rfm_segment: String
}
```

#### Product Type

Product catalog information with categorization and pricing.

```graphql
type Product {
  """Internal product key"""
  product_key: Int!
  
  """Product stock code"""
  stock_code: String!
  
  """Product description"""
  description: String
  
  """Product category"""
  category: String
  
  """Product subcategory"""
  subcategory: String
  
  """Product brand"""
  brand: String
  
  """Unit cost for the product"""
  unit_cost: Float
  
  """Recommended retail price"""
  recommended_price: Float
}
```

#### Country Type

Geographic information with economic indicators.

```graphql
type Country {
  """Internal country key"""
  country_key: Int!
  
  """ISO country code"""
  country_code: String!
  
  """Full country name"""
  country_name: String!
  
  """Geographic region"""
  region: String
  
  """Continent"""
  continent: String
  
  """Currency code"""
  currency_code: String
  
  """GDP per capita"""
  gdp_per_capita: Float
  
  """Population count"""
  population: Int
}
```

### Analytics Types

#### SalesAnalytics Type

Aggregated sales performance metrics over time periods.

```graphql
type SalesAnalytics {
  """Time period identifier"""
  period: String!
  
  """Total revenue for the period"""
  total_revenue: Float!
  
  """Total quantity sold"""
  total_quantity: Int!
  
  """Number of transactions"""
  transaction_count: Int!
  
  """Unique customers count"""
  unique_customers: Int!
  
  """Average order value"""
  avg_order_value: Float!
}
```

#### CustomerSegment Type

Customer segmentation analytics with performance metrics.

```graphql
type CustomerSegment {
  """Segment name/classification"""
  segment_name: String!
  
  """Number of customers in segment"""
  customer_count: Int!
  
  """Average lifetime value for segment"""
  avg_lifetime_value: Float!
  
  """Average order value for segment"""
  avg_order_value: Float!
  
  """Average number of orders per customer"""
  avg_total_orders: Float!
}
```

#### ProductPerformance Type

Product performance metrics and analytics.

```graphql
type ProductPerformance {
  """Product stock code"""
  stock_code: String!
  
  """Product description"""
  description: String
  
  """Product category"""
  category: String
  
  """Total revenue generated"""
  total_revenue: Float!
  
  """Total quantity sold"""
  total_quantity: Int!
  
  """Number of transactions"""
  transaction_count: Int!
}
```

#### BusinessMetrics Type

High-level business KPIs and metrics.

```graphql
type BusinessMetrics {
  """Total revenue across all transactions"""
  total_revenue: Float!
  
  """Total number of transactions"""
  total_transactions: Int!
  
  """Unique customers count"""
  unique_customers: Int!
  
  """Average order value"""
  avg_order_value: Float!
  
  """Total products in catalog"""
  total_products: Int!
  
  """Active countries with sales"""
  active_countries: Int!
}
```

### Input Types

#### SalesFilters Input

Comprehensive filtering options for sales queries.

```graphql
input SalesFilters {
  """Filter by start date (YYYY-MM-DD)"""
  date_from: Date
  
  """Filter by end date (YYYY-MM-DD)"""
  date_to: Date
  
  """Filter by country name"""
  country: String
  
  """Filter by product category"""
  product_category: String
  
  """Filter by customer segment"""
  customer_segment: String
  
  """Minimum transaction amount"""
  min_amount: Float
  
  """Maximum transaction amount"""
  max_amount: Float
}
```

#### PaginationInput Input

Pagination parameters for large result sets.

```graphql
input PaginationInput {
  """Page number (1-based)"""
  page: Int = 1
  
  """Number of items per page"""
  page_size: Int = 20
}
```

### Enums

#### TimeGranularity Enum

Time period options for analytics aggregations.

```graphql
enum TimeGranularity {
  """Daily aggregation"""
  DAILY
  
  """Weekly aggregation"""
  WEEKLY
  
  """Monthly aggregation"""
  MONTHLY
  
  """Quarterly aggregation"""
  QUARTERLY
  
  """Yearly aggregation"""
  YEARLY
}
```

#### MetricType Enum

Available metric types for product performance analysis.

```graphql
enum MetricType {
  """Revenue-based metrics"""
  REVENUE
  
  """Quantity-based metrics"""
  QUANTITY
  
  """Profit-based metrics"""
  PROFIT
  
  """Margin-based metrics"""
  MARGIN
}
```

## Query Examples

### Basic Sales Query

Retrieve sales data with basic filtering and pagination.

```graphql
query GetSales($filters: SalesFilters, $pagination: PaginationInput) {
  sales(filters: $filters, pagination: $pagination) {
    items {
      sale_id
      quantity
      unit_price
      total_amount
      created_at
      product {
        stock_code
        description
        category
      }
      customer {
        customer_id
        customer_segment
        rfm_segment
      }
      country {
        country_name
        region
      }
    }
    total_count
    page
    page_size
    has_next
    has_previous
  }
}
```

**Variables**:
```json
{
  "filters": {
    "date_from": "2010-12-01",
    "date_to": "2011-12-31",
    "country": "United Kingdom",
    "min_amount": 10.0
  },
  "pagination": {
    "page": 1,
    "page_size": 50
  }
}
```

### Advanced Sales Analytics Query

Complex analytics query with multiple aggregations.

```graphql
query GetSalesAnalytics($granularity: TimeGranularity!) {
  salesAnalytics(granularity: $granularity) {
    period
    total_revenue
    total_quantity
    transaction_count
    unique_customers
    avg_order_value
  }
  
  customerSegments {
    segment_name
    customer_count
    avg_lifetime_value
    avg_order_value
    avg_total_orders
  }
  
  topProductsByRevenue(limit: 10) {
    stock_code
    description
    category
    total_revenue
    total_quantity
    transaction_count
  }
  
  businessMetrics {
    total_revenue
    total_transactions
    unique_customers
    avg_order_value
    total_products
    active_countries
  }
}
```

**Variables**:
```json
{
  "granularity": "MONTHLY"
}
```

### Customer Segmentation Analysis

Detailed customer analysis with RFM segmentation.

```graphql
query GetCustomerAnalysis {
  customerSegments {
    segment_name
    customer_count
    avg_lifetime_value
    avg_order_value
    avg_total_orders
  }
  
  customers(
    filters: { customer_segment: "High Value" }
    pagination: { page: 1, page_size: 20 }
  ) {
    items {
      customer_key
      customer_id
      customer_segment
      lifetime_value
      total_orders
      total_spent
      avg_order_value
      recency_score
      frequency_score
      monetary_score
      rfm_segment
    }
    total_count
  }
}
```

### Product Performance Query

Comprehensive product performance analysis.

```graphql
query GetProductPerformance($metricType: MetricType!) {
  productPerformance(
    metricType: $metricType
    limit: 25
    categoryFilter: "Electronics"
  ) {
    stock_code
    description
    category
    total_revenue
    total_quantity
    transaction_count
  }
  
  products(filters: { category: "Electronics" }) {
    items {
      product_key
      stock_code
      description
      category
      subcategory
      brand
      unit_cost
      recommended_price
    }
  }
}
```

**Variables**:
```json
{
  "metricType": "REVENUE"
}
```

### Geographic Sales Analysis

Sales performance by geographic regions.

```graphql
query GetGeographicAnalysis {
  salesByCountry(limit: 15) {
    country {
      country_name
      country_code
      region
      continent
      currency_code
      gdp_per_capita
      population
    }
    total_revenue
    transaction_count
    unique_customers
    avg_order_value
  }
  
  salesByRegion {
    region
    total_revenue
    transaction_count
    country_count
    avg_revenue_per_country
  }
}
```

## Mutation Examples

### Async Task Submission

Submit background tasks for complex analytics processing.

```graphql
mutation SubmitAnalyticsTask($input: TaskSubmissionInput!) {
  submitTask(input: $input) {
    task_id
    task_name
    status
    submitted_at
    progress
  }
}
```

**Variables**:
```json
{
  "input": {
    "task_name": "comprehensive_sales_analysis",
    "parameters": "{\"date_range\": {\"start\": \"2010-01-01\", \"end\": \"2011-12-31\"}, \"include_predictions\": true}"
  }
}
```

### Data Refresh Trigger

Trigger data mart refresh operations.

```graphql
mutation TriggerDataRefresh($scope: String!) {
  triggerDataRefresh(scope: $scope) {
    task_id
    status
    estimated_completion
  }
}
```

**Variables**:
```json
{
  "scope": "gold_layer_refresh"
}
```

## Subscription Examples

### Real-Time Sales Monitoring

Subscribe to live sales data updates.

```graphql
subscription LiveSalesUpdates($filters: SalesFilters) {
  liveTransactions(filters: $filters) {
    sale_id
    total_amount
    created_at
    product {
      stock_code
      description
    }
    country {
      country_name
    }
  }
}
```

### Task Progress Updates

Monitor async task progress in real-time.

```graphql
subscription TaskProgressUpdates($taskId: String!) {
  taskProgress(taskId: $taskId) {
    task_id
    task_name
    status
    progress
    result
    error
    last_updated: DateTime!
  }
}
```

### Business Metrics Dashboard

Real-time business metrics for dashboard applications.

```graphql
subscription LiveBusinessMetrics {
  liveMetrics {
    timestamp
    total_revenue_today
    transactions_today
    active_users
    avg_order_value_24h
    top_selling_products {
      stock_code
      sales_count
    }
  }
}
```

## Advanced Patterns

### Complex Nested Queries

Leverage GraphQL's nested query capabilities for complex data relationships.

```graphql
query ComplexNestedAnalysis {
  customers(
    filters: { customer_segment: "High Value" }
    pagination: { page: 1, page_size: 10 }
  ) {
    items {
      customer_id
      customer_segment
      lifetime_value
      
      # Nested sales for each customer
      recentSales(limit: 5) {
        sale_id
        total_amount
        created_at
        
        # Product details for each sale
        product {
          stock_code
          description
          category
          
          # Similar products
          similarProducts(limit: 3) {
            stock_code
            description
            similarity_score
          }
        }
      }
      
      # Customer analytics
      monthlySpending(months: 12) {
        month
        total_spent
        order_count
      }
    }
  }
}
```

### Dynamic Field Selection

Use fragments for reusable field selections.

```graphql
fragment BasicProductInfo on Product {
  stock_code
  description
  category
  unit_cost
  recommended_price
}

fragment CustomerMetrics on Customer {
  customer_id
  lifetime_value
  total_orders
  avg_order_value
  rfm_segment
}

query ProductsWithCustomerMetrics {
  products(limit: 20) {
    items {
      ...BasicProductInfo
      
      topCustomers(limit: 5) {
        ...CustomerMetrics
      }
    }
  }
}
```

### Conditional Queries with Directives

Use GraphQL directives for conditional field inclusion.

```graphql
query ConditionalSalesQuery($includeCustomer: Boolean!, $includeAnalytics: Boolean!) {
  sales(pagination: { page: 1, page_size: 10 }) {
    items {
      sale_id
      total_amount
      created_at
      
      customer @include(if: $includeCustomer) {
        customer_id
        customer_segment
      }
      
      product {
        stock_code
        description
        
        analytics @include(if: $includeAnalytics) {
          total_revenue
          popularity_rank
        }
      }
    }
  }
}
```

## Performance Optimization

### Data Loader Usage

The API implements intelligent data loaders to solve N+1 query problems:

```python
# Example of optimized nested query execution
# Single query for sales, batched queries for related entities
query {
  sales(limit: 100) {
    items {
      sale_id
      customer {  # Batched via DataLoader
        customer_id
      }
      product {   # Batched via DataLoader
        description
      }
      country {   # Batched via DataLoader
        country_name
      }
    }
  }
}
```

### Query Complexity Analysis

The API implements query complexity analysis to prevent expensive operations:

```graphql
# This query has high complexity due to nested relations
query ExpensiveQuery {
  customers(limit: 1000) {  # High limit = higher complexity
    items {
      recentSales(limit: 100) {  # Nested high limit = very high complexity
        product {
          similarProducts(limit: 50) {  # Triple nesting = extreme complexity
            # ... fields
          }
        }
      }
    }
  }
}
```

**Best Practices**:
- Use pagination effectively with reasonable page sizes
- Limit nested collection sizes
- Request only needed fields
- Use fragments to avoid field duplication

### Caching Strategies

**Query-Level Caching**:
```graphql
# Cached queries (marked with @cached directive)
query CachedBusinessMetrics @cached(ttl: 300) {
  businessMetrics {
    total_revenue
    total_transactions
  }
}
```

**Field-Level Caching**:
```graphql
query MixedCachingQuery {
  products(limit: 10) {
    items {
      stock_code
      description
      
      # This field has longer cache TTL
      analytics @cached(ttl: 3600) {
        total_revenue
        popularity_rank
      }
    }
  }
}
```

## Integration Guide

### Authentication Setup

All GraphQL queries require proper authentication:

```javascript
// JavaScript/TypeScript client setup
const client = new ApolloClient({
  uri: 'https://api.pwc-data.com/api/graphql',
  headers: {
    Authorization: `Bearer ${accessToken}`
  },
  cache: new InMemoryCache()
});
```

### Python Client Example

```python
import httpx
import json

class GraphQLClient:
    def __init__(self, endpoint: str, token: str):
        self.endpoint = endpoint
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
    
    async def query(self, query: str, variables: dict = None):
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.endpoint,
                json={
                    'query': query,
                    'variables': variables or {}
                },
                headers=self.headers
            )
            return response.json()

# Usage example
client = GraphQLClient('https://api.pwc-data.com/api/graphql', access_token)

query = """
query GetSales($limit: Int!) {
  sales(pagination: {page_size: $limit}) {
    items {
      sale_id
      total_amount
      product {
        description
      }
    }
  }
}
"""

result = await client.query(query, {'limit': 10})
```

### React/Apollo Client Example

```typescript
import { useQuery, gql } from '@apollo/client';

const GET_SALES = gql`
  query GetSales($filters: SalesFilters, $pagination: PaginationInput) {
    sales(filters: $filters, pagination: $pagination) {
      items {
        sale_id
        total_amount
        created_at
        product {
          stock_code
          description
        }
        customer {
          customer_segment
        }
      }
      total_count
      has_next
    }
  }
`;

function SalesList() {
  const { loading, error, data } = useQuery(GET_SALES, {
    variables: {
      filters: {
        date_from: '2010-12-01',
        date_to: '2011-12-31'
      },
      pagination: {
        page: 1,
        page_size: 20
      }
    }
  });

  if (loading) return <div>Loading...</div>;
  if (error) return <div>Error: {error.message}</div>;

  return (
    <div>
      {data.sales.items.map((sale) => (
        <div key={sale.sale_id}>
          <p>Sale ID: {sale.sale_id}</p>
          <p>Amount: ${sale.total_amount}</p>
          <p>Product: {sale.product.description}</p>
        </div>
      ))}
    </div>
  );
}
```

### WebSocket Subscriptions

```typescript
import { createClient } from 'graphql-ws';

const wsClient = createClient({
  url: 'wss://api.pwc-data.com/api/graphql',
  connectionParams: {
    Authorization: `Bearer ${accessToken}`,
  },
});

// Subscribe to live updates
const subscription = wsClient.iterate({
  query: `
    subscription LiveSalesUpdates {
      liveTransactions {
        sale_id
        total_amount
        created_at
      }
    }
  `
});

for await (const result of subscription) {
  console.log('New transaction:', result.data.liveTransactions);
}
```

## Best Practices

### Query Design

1. **Use Fragments**: Avoid field duplication
```graphql
fragment SaleInfo on Sale {
  sale_id
  total_amount
  created_at
}

query MultipleSaleQueries {
  recentSales: sales(pagination: {page_size: 10}) {
    items {
      ...SaleInfo
    }
  }
  
  highValueSales: sales(filters: {min_amount: 1000}) {
    items {
      ...SaleInfo
    }
  }
}
```

2. **Pagination Strategy**: Always use pagination for large datasets
```graphql
query PaginatedQuery {
  sales(pagination: {page: 1, page_size: 20}) {
    items {
      # ... fields
    }
    total_count
    has_next
    has_previous
  }
}
```

3. **Selective Field Queries**: Request only needed fields
```graphql
# Good: Specific fields
query EfficientQuery {
  sales(limit: 10) {
    items {
      sale_id
      total_amount
    }
  }
}

# Avoid: Over-fetching
query InefficientQuery {
  sales(limit: 10) {
    items {
      sale_id
      quantity
      unit_price
      total_amount
      discount_amount
      tax_amount
      profit_amount
      margin_percentage
      created_at
      # ... many more fields
    }
  }
}
```

### Error Handling

```typescript
import { ApolloError } from '@apollo/client';

function handleGraphQLError(error: ApolloError) {
  if (error.networkError) {
    console.error('Network error:', error.networkError);
    // Handle network connectivity issues
  }
  
  if (error.graphQLErrors) {
    error.graphQLErrors.forEach(({ message, locations, path }) => {
      console.error(
        `GraphQL error: ${message}`,
        `Location: ${locations}`,
        `Path: ${path}`
      );
    });
    // Handle GraphQL-specific errors
  }
}
```

### Performance Monitoring

```typescript
// Apollo Client with performance monitoring
const client = new ApolloClient({
  uri: 'https://api.pwc-data.com/api/graphql',
  cache: new InMemoryCache(),
  plugins: [
    {
      requestDidStart() {
        return {
          didResolveOperation(requestContext) {
            console.log('Query:', requestContext.request.query);
          },
          didReceiveResponse(requestContext) {
            console.log('Response time:', requestContext.response.http?.status);
          }
        };
      }
    }
  ]
});
```

## Troubleshooting

### Common Issues

#### Authentication Errors
```json
{
  "errors": [
    {
      "message": "Authentication required",
      "extensions": {
        "code": "UNAUTHENTICATED"
      }
    }
  ]
}
```

**Solution**: Ensure valid JWT token in Authorization header:
```
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

#### Query Complexity Errors
```json
{
  "errors": [
    {
      "message": "Query complexity exceeds maximum allowed complexity",
      "extensions": {
        "code": "QUERY_COMPLEXITY_TOO_HIGH",
        "complexity": 1500,
        "maxComplexity": 1000
      }
    }
  ]
}
```

**Solution**: Reduce query complexity by:
- Using smaller pagination limits
- Reducing nested query depth
- Requesting fewer fields

#### Rate Limiting
```json
{
  "errors": [
    {
      "message": "Rate limit exceeded",
      "extensions": {
        "code": "RATE_LIMIT_EXCEEDED",
        "retryAfter": 60
      }
    }
  ]
}
```

**Solution**: Implement exponential backoff retry logic.

### Performance Issues

#### Slow Query Resolution
1. **Check Query Complexity**: Use introspection to analyze complexity
2. **Optimize Field Selection**: Request only necessary fields
3. **Use Proper Indexes**: Ensure database indexes support your filters
4. **Consider Caching**: Use cached queries for repeated operations

#### Memory Issues
1. **Pagination**: Always use pagination for large result sets
2. **Connection Pooling**: Configure proper database connection limits
3. **Query Timeout**: Set reasonable timeouts for long-running queries

### Debugging Tools

#### GraphiQL Playground
Access the interactive GraphiQL interface at:
- Production: `https://api.pwc-data.com/api/graphql`
- Local: `http://localhost:8000/api/graphql`

#### Query Analysis
```graphql
# Use introspection to analyze schema
query IntrospectionQuery {
  __schema {
    queryType {
      fields {
        name
        description
        type {
          name
        }
      }
    }
  }
}
```

#### Performance Profiling
Enable query profiling in development:
```python
# In resolver
import time
start_time = time.time()
# ... resolver logic
logger.info(f"Query resolved in {time.time() - start_time:.2f}s")
```

## Advanced Features

### Custom Scalars

The API includes custom scalar types for enhanced type safety:

```graphql
# Custom DateTime scalar with timezone support
scalar DateTime

# Custom JSON scalar for flexible data structures  
scalar JSON

# Custom UUID scalar with validation
scalar UUID
```

### Batch Operations via GraphQL

```graphql
mutation BatchCreateSales($sales: [SaleInput!]!) {
  batchCreateSales(sales: $sales) {
    successful_count
    failed_count
    errors {
      index
      message
    }
  }
}
```

### File Upload Support

```graphql
scalar Upload

mutation UploadSalesData($file: Upload!) {
  uploadSalesFile(file: $file) {
    filename
    size
    processing_status
  }
}
```

## API Versioning

The GraphQL API supports versioning through field deprecation and schema evolution:

```graphql
type Product {
  stock_code: String!
  description: String
  
  # Deprecated field
  old_category: String @deprecated(reason: "Use 'category' instead")
  
  # New field with better structure
  category: String
}
```

---

**For additional support and advanced use cases, consult our [Developer Portal](https://docs.pwc-data.com) or contact the API team.**
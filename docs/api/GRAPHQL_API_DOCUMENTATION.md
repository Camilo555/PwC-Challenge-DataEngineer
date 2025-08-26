# PwC Retail Data Platform - GraphQL API Documentation

## Table of Contents

1. [Overview](#overview)
2. [Getting Started](#getting-started)
3. [Authentication](#authentication)
4. [Schema Overview](#schema-overview)
5. [Query Examples](#query-examples)
6. [Mutations](#mutations)
7. [Subscriptions](#subscriptions)
8. [Error Handling](#error-handling)
9. [Performance & Optimization](#performance--optimization)
10. [SDL Schema Reference](#sdl-schema-reference)

## Overview

The PwC Retail Data Platform provides a powerful GraphQL API that complements our REST API, offering flexible data querying capabilities with a single endpoint. Built with Strawberry GraphQL, it provides type-safe, efficient data access with real-time capabilities.

### GraphQL Endpoint
- **Development**: `http://localhost:8000/api/graphql`
- **Staging**: `https://staging-api.pwc-retail-platform.com/api/graphql`
- **Production**: `https://api.pwc-retail-platform.com/api/graphql`

### GraphQL Playground
- **Development**: `http://localhost:8000/api/graphql` (Interactive query builder)
- **Schema Explorer**: Built-in schema documentation and query builder

### Key Features
- **Single endpoint** for all data access needs
- **Flexible queries** - fetch exactly the data you need
- **Real-time subscriptions** for live data updates
- **Strong type system** with auto-generated documentation
- **Query optimization** with automatic batching and caching
- **Federation ready** for microservices architecture

## Getting Started

### GraphQL Playground

Access the interactive GraphQL playground at `/api/graphql` to explore the schema and test queries:

![GraphQL Playground Interface](https://docs.pwc-retail-platform.com/images/graphql-playground.png)

### Basic Query Structure

All GraphQL requests use POST method to the GraphQL endpoint:

```bash
curl -X POST \
  https://api.pwc-retail-platform.com/api/graphql \
  -H 'Authorization: Bearer <your_jwt_token>' \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "query { sales(limit: 5) { transactionId, totalAmount, customer { name } } }"
  }'
```

### Client Libraries

#### JavaScript/TypeScript
```javascript
import { GraphQLClient } from 'graphql-request';

const client = new GraphQLClient('https://api.pwc-retail-platform.com/api/graphql', {
  headers: {
    Authorization: `Bearer ${token}`,
  },
});

const query = `
  query GetSalesData($limit: Int) {
    sales(limit: $limit) {
      transactionId
      totalAmount
      invoiceDate
      customer {
        customerId
        country
      }
    }
  }
`;

const data = await client.request(query, { limit: 10 });
```

#### Python
```python
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport

transport = RequestsHTTPTransport(
    url="https://api.pwc-retail-platform.com/api/graphql",
    headers={"Authorization": f"Bearer {token}"}
)

client = Client(transport=transport)

query = gql("""
    query GetSalesData($limit: Int) {
        sales(limit: $limit) {
            transactionId
            totalAmount
            invoiceDate
            customer {
                customerId
                country
            }
        }
    }
""")

result = client.execute(query, variable_values={"limit": 10})
```

## Authentication

GraphQL API uses the same JWT authentication as the REST API:

```javascript
const headers = {
  'Authorization': `Bearer ${jwtToken}`,
  'Content-Type': 'application/json'
};
```

For authentication queries, see the [REST API Authentication documentation](./REST_API_DOCUMENTATION.md#authentication).

## Schema Overview

### Root Types

```graphql
type Query {
  # Sales data queries
  sales(filter: SalesFilter, pagination: PaginationInput): [Sale!]!
  sale(transactionId: ID!): Sale
  
  # Customer queries
  customers(filter: CustomerFilter, pagination: PaginationInput): [Customer!]!
  customer(customerId: ID!): Customer
  
  # Product queries
  products(filter: ProductFilter, pagination: PaginationInput): [Product!]!
  product(stockCode: String!): Product
  
  # Analytics queries
  salesAnalytics(input: AnalyticsInput!): SalesAnalytics!
  customerSegmentation(method: SegmentationMethod!): [CustomerSegment!]!
  cohortAnalysis(input: CohortAnalysisInput!): CohortAnalysis!
  
  # Search queries
  searchProducts(query: String!, limit: Int = 20): [ProductSearchResult!]!
  searchSemantic(query: String!, k: Int = 10): [SemanticSearchResult!]!
  
  # ETL and system queries
  pipelineStatus: [Pipeline!]!
  systemHealth: SystemHealth!
}

type Mutation {
  # Data operations
  createSale(input: CreateSaleInput!): Sale!
  updateSale(id: ID!, input: UpdateSaleInput!): Sale!
  deleteSale(id: ID!): Boolean!
  
  # ETL operations
  triggerPipeline(pipelineId: String!, parameters: JSON): PipelineRun!
  
  # Analytics operations
  refreshAnalytics(type: AnalyticsType!): Boolean!
}

type Subscription {
  # Real-time data updates
  salesUpdates(filter: SalesFilter): Sale!
  pipelineUpdates(pipelineId: String): PipelineStatus!
  systemAlerts: SystemAlert!
}
```

### Core Data Types

#### Sale Type
```graphql
type Sale {
  transactionId: ID!
  invoice: String!
  stockCode: String!
  description: String
  quantity: Int!
  invoiceDate: Date!
  unitPrice: Float!
  totalAmount: Float!
  customerId: String!
  country: String!
  
  # Related data
  customer: Customer!
  product: Product!
  
  # Computed fields
  isReturn: Boolean!
  profitMargin: Float
  
  # Metadata
  createdAt: DateTime!
  updatedAt: DateTime!
  dataQualityScore: Float
}
```

#### Customer Type
```graphql
type Customer {
  customerId: ID!
  country: String!
  
  # Aggregated data
  totalTransactions: Int!
  totalSpent: Float!
  avgOrderValue: Float!
  firstPurchaseDate: Date
  lastPurchaseDate: Date
  
  # Behavioral data
  rfmScore: RFMScore
  segment: String
  lifetimeValue: Float
  churnProbability: Float
  
  # Related data
  sales(filter: SalesFilter, pagination: PaginationInput): [Sale!]!
  
  # Computed fields
  daysSinceLastPurchase: Int
  purchaseFrequency: Float
}
```

#### Product Type
```graphql
type Product {
  stockCode: ID!
  description: String!
  category: String
  unitPrice: Float
  
  # Sales performance
  totalSold: Int!
  totalRevenue: Float!
  avgRating: Float
  popularityScore: Float
  
  # Related data
  sales(filter: SalesFilter, pagination: PaginationInput): [Sale!]!
  
  # Analytics
  seasonalTrends: [SeasonalTrend!]!
  crossSellProducts: [Product!]!
}
```

### Analytics Types

#### Sales Analytics
```graphql
type SalesAnalytics {
  totalRevenue: Float!
  totalTransactions: Int!
  uniqueCustomers: Int!
  avgOrderValue: Float!
  
  # Time series data
  revenueByPeriod: [TimeSeries!]!
  transactionsByPeriod: [TimeSeries!]!
  
  # Geographic breakdown
  revenueByCountry: [CountryRevenue!]!
  
  # Product performance
  topProducts: [ProductPerformance!]!
  
  # Customer insights
  newVsReturningCustomers: CustomerBreakdown!
  
  # Growth metrics
  monthOverMonthGrowth: Float!
  yearOverYearGrowth: Float!
}
```

#### Customer Segmentation
```graphql
type CustomerSegment {
  segmentId: String!
  name: String!
  description: String
  customerCount: Int!
  percentage: Float!
  
  # RFM characteristics
  avgRecency: Float!
  avgFrequency: Float!
  avgMonetaryValue: Float!
  
  # Business metrics
  totalRevenue: Float!
  avgLifetimeValue: Float!
  churnRate: Float
  
  # Recommendations
  recommendedActions: [String!]!
  marketingStrategies: [String!]!
}
```

### Input Types

#### Filters
```graphql
input SalesFilter {
  dateRange: DateRangeInput
  countries: [String!]
  customerIds: [String!]
  stockCodes: [String!]
  minAmount: Float
  maxAmount: Float
  isReturn: Boolean
}

input DateRangeInput {
  startDate: Date!
  endDate: Date!
}

input PaginationInput {
  limit: Int = 100
  offset: Int = 0
}
```

#### Analytics Inputs
```graphql
input AnalyticsInput {
  dateRange: DateRangeInput!
  groupBy: GroupByDimension!
  metrics: [AnalyticsMetric!]!
  filters: SalesFilter
}

enum GroupByDimension {
  DAY
  WEEK
  MONTH
  QUARTER
  YEAR
  COUNTRY
  PRODUCT
  CUSTOMER_SEGMENT
}

enum AnalyticsMetric {
  REVENUE
  TRANSACTIONS
  CUSTOMERS
  AVG_ORDER_VALUE
  PROFIT_MARGIN
}
```

## Query Examples

### Basic Sales Query

```graphql
query GetRecentSales {
  sales(
    filter: {
      dateRange: {
        startDate: "2024-08-01"
        endDate: "2024-08-25"
      }
    }
    pagination: { limit: 10 }
  ) {
    transactionId
    invoice
    totalAmount
    invoiceDate
    customer {
      customerId
      country
    }
    product {
      stockCode
      description
    }
  }
}
```

**Response:**
```json
{
  "data": {
    "sales": [
      {
        "transactionId": "txn_123",
        "invoice": "536365",
        "totalAmount": 15.30,
        "invoiceDate": "2024-08-01",
        "customer": {
          "customerId": "17850",
          "country": "United Kingdom"
        },
        "product": {
          "stockCode": "85123A",
          "description": "CREAM HANGING HEART T-LIGHT HOLDER"
        }
      }
    ]
  }
}
```

### Advanced Analytics Query

```graphql
query GetSalesAnalytics {
  salesAnalytics(
    input: {
      dateRange: {
        startDate: "2024-01-01"
        endDate: "2024-08-25"
      }
      groupBy: MONTH
      metrics: [REVENUE, TRANSACTIONS, AVG_ORDER_VALUE]
      filters: {
        countries: ["United Kingdom", "Germany", "France"]
      }
    }
  ) {
    totalRevenue
    totalTransactions
    avgOrderValue
    monthOverMonthGrowth
    
    revenueByPeriod {
      period
      value
    }
    
    revenueByCountry {
      country
      revenue
      percentage
    }
    
    topProducts(limit: 5) {
      product {
        stockCode
        description
      }
      revenue
      unitsSold
    }
  }
}
```

### Customer Analysis Query

```graphql
query GetCustomerInsights($customerId: ID!) {
  customer(customerId: $customerId) {
    customerId
    country
    totalSpent
    totalTransactions
    avgOrderValue
    
    rfmScore {
      recency
      frequency
      monetary
      score
    }
    
    segment
    lifetimeValue
    churnProbability
    
    sales(
      filter: {
        dateRange: {
          startDate: "2024-01-01"
          endDate: "2024-08-25"
        }
      }
      pagination: { limit: 10 }
    ) {
      transactionId
      totalAmount
      invoiceDate
      product {
        description
      }
    }
  }
}
```

### Product Performance Query

```graphql
query GetProductPerformance {
  products(
    filter: {
      minRevenue: 1000
    }
    pagination: { limit: 20 }
  ) {
    stockCode
    description
    totalRevenue
    totalSold
    popularityScore
    
    seasonalTrends {
      month
      averageSales
      trendDirection
    }
    
    crossSellProducts(limit: 3) {
      stockCode
      description
      associationStrength
    }
  }
}
```

### Customer Segmentation Query

```graphql
query GetCustomerSegments {
  customerSegmentation(method: RFM) {
    segmentId
    name
    customerCount
    percentage
    avgRecency
    avgFrequency
    avgMonetaryValue
    totalRevenue
    recommendedActions
  }
}
```

### Cohort Analysis Query

```graphql
query GetCohortAnalysis {
  cohortAnalysis(
    input: {
      startDate: "2024-01-01"
      endDate: "2024-08-25"
      periods: 6
    }
  ) {
    cohorts {
      cohortMonth
      customerCount
      retentionRates {
        period
        rate
      }
      revenuePerCohort {
        period
        revenue
      }
    }
    
    predictions {
      nextPeriodRetention
      confidenceInterval {
        lower
        upper
      }
    }
  }
}
```

## Mutations

### Create Sale

```graphql
mutation CreateSale($input: CreateSaleInput!) {
  createSale(input: $input) {
    transactionId
    totalAmount
    customer {
      customerId
    }
    product {
      stockCode
    }
  }
}
```

**Variables:**
```json
{
  "input": {
    "invoice": "536366",
    "stockCode": "85123A",
    "quantity": 2,
    "unitPrice": 2.55,
    "customerId": "17850",
    "invoiceDate": "2024-08-25"
  }
}
```

### Trigger ETL Pipeline

```graphql
mutation TriggerPipeline($pipelineId: String!, $parameters: JSON) {
  triggerPipeline(pipelineId: $pipelineId, parameters: $parameters) {
    runId
    status
    estimatedDuration
    queuePosition
  }
}
```

**Variables:**
```json
{
  "pipelineId": "retail_bronze_ingestion",
  "parameters": {
    "forceFullRefresh": false,
    "dateRange": {
      "start": "2024-08-20",
      "end": "2024-08-25"
    }
  }
}
```

## Subscriptions

### Real-time Sales Updates

```graphql
subscription SalesUpdates($filter: SalesFilter) {
  salesUpdates(filter: $filter) {
    transactionId
    totalAmount
    invoiceDate
    customer {
      customerId
      country
    }
    product {
      description
    }
  }
}
```

### Pipeline Status Updates

```graphql
subscription PipelineUpdates($pipelineId: String) {
  pipelineUpdates(pipelineId: $pipelineId) {
    pipelineId
    status
    progress
    currentStep
    estimatedTimeRemaining
  }
}
```

### System Alerts

```graphql
subscription SystemAlerts {
  systemAlerts {
    alertId
    severity
    message
    component
    timestamp
    resolved
  }
}
```

## Error Handling

### GraphQL Error Format

GraphQL errors follow the standard GraphQL error format:

```json
{
  "data": null,
  "errors": [
    {
      "message": "Customer not found",
      "locations": [
        {
          "line": 3,
          "column": 5
        }
      ],
      "path": ["customer"],
      "extensions": {
        "code": "NOT_FOUND",
        "customerId": "invalid_id",
        "timestamp": "2024-08-25T14:30:00Z"
      }
    }
  ]
}
```

### Common Error Codes

| Code | Description | HTTP Status |
|------|-------------|-------------|
| `AUTHENTICATION_REQUIRED` | JWT token missing or invalid | 401 |
| `FORBIDDEN` | Insufficient permissions | 403 |
| `NOT_FOUND` | Requested resource not found | 404 |
| `VALIDATION_ERROR` | Input validation failed | 400 |
| `RATE_LIMIT_EXCEEDED` | Too many requests | 429 |
| `INTERNAL_ERROR` | Server error | 500 |

### Partial Errors

GraphQL can return partial data with errors:

```json
{
  "data": {
    "sales": [
      {
        "transactionId": "txn_123",
        "totalAmount": 15.30,
        "customer": null
      }
    ]
  },
  "errors": [
    {
      "message": "Customer data temporarily unavailable",
      "path": ["sales", 0, "customer"],
      "extensions": {
        "code": "SERVICE_UNAVAILABLE"
      }
    }
  ]
}
```

## Performance & Optimization

### Query Complexity Analysis

The API implements query complexity analysis to prevent expensive queries:

```graphql
# This query might be rejected if too complex
query ExpensiveQuery {
  sales {  # Complexity: 1000+ if no limit
    customer {  # +1 for each sale
      sales {  # N+1 problem - very expensive
        customer {  # Nested customers
          sales {  # Even more expensive
            # ...
          }
        }
      }
    }
  }
}
```

### Query Depth Limiting

Maximum query depth is limited to prevent deeply nested queries:

```graphql
# This query exceeds max depth (7 levels)
query TooDeepQuery {
  customer {           # Level 1
    sales {           # Level 2
      product {       # Level 3
        sales {       # Level 4
          customer {  # Level 5
            sales {   # Level 6
              product { # Level 7
                sales { # Level 8 - REJECTED
                  # ...
                }
              }
            }
          }
        }
      }
    }
  }
}
```

### Pagination Best Practices

Always use pagination for list queries:

```graphql
query OptimizedSalesQuery {
  sales(
    pagination: { limit: 50, offset: 0 }
    filter: { 
      dateRange: { 
        startDate: "2024-08-01", 
        endDate: "2024-08-25" 
      } 
    }
  ) {
    transactionId
    totalAmount
    # Only request fields you need
  }
}
```

### Field Selection Optimization

Only request the fields you need:

```graphql
# Good - specific fields
query OptimizedQuery {
  customer(customerId: "123") {
    customerId
    totalSpent
    segment
  }
}

# Bad - requesting everything
query UnoptimizedQuery {
  customer(customerId: "123") {
    customerId
    country
    totalTransactions
    totalSpent
    avgOrderValue
    firstPurchaseDate
    lastPurchaseDate
    rfmScore {
      recency
      frequency
      monetary
      score
    }
    segment
    lifetimeValue
    churnProbability
    sales {
      transactionId
      invoice
      # ... many more fields
    }
  }
}
```

### Caching Strategies

#### Query-level Caching
```javascript
const client = new GraphQLClient(endpoint, {
  cache: new InMemoryCache({
    typePolicies: {
      Customer: {
        fields: {
          sales: {
            merge: false  // Replace instead of merge
          }
        }
      }
    }
  })
});
```

#### Field-level Caching
```graphql
# Cached fields are marked in schema
type Customer {
  customerId: ID!
  totalSpent: Float! @cached(ttl: 300)  # 5 minutes
  segment: String @cached(ttl: 3600)    # 1 hour
}
```

## SDL Schema Reference

### Complete Schema Definition

```graphql
# Scalars
scalar Date
scalar DateTime
scalar JSON

# Enums
enum SegmentationMethod {
  RFM
  CLUSTERING
  BEHAVIORAL
}

enum AnalyticsMetric {
  REVENUE
  TRANSACTIONS
  CUSTOMERS
  AVG_ORDER_VALUE
  PROFIT_MARGIN
}

enum GroupByDimension {
  DAY
  WEEK
  MONTH
  QUARTER
  YEAR
  COUNTRY
  PRODUCT
  CUSTOMER_SEGMENT
}

# Input Types
input SalesFilter {
  dateRange: DateRangeInput
  countries: [String!]
  customerIds: [String!]
  stockCodes: [String!]
  minAmount: Float
  maxAmount: Float
  isReturn: Boolean
}

input DateRangeInput {
  startDate: Date!
  endDate: Date!
}

input PaginationInput {
  limit: Int = 100
  offset: Int = 0
}

input AnalyticsInput {
  dateRange: DateRangeInput!
  groupBy: GroupByDimension!
  metrics: [AnalyticsMetric!]!
  filters: SalesFilter
}

input CreateSaleInput {
  invoice: String!
  stockCode: String!
  description: String
  quantity: Int!
  unitPrice: Float!
  customerId: String!
  invoiceDate: Date!
}

# Main Types
type Sale {
  transactionId: ID!
  invoice: String!
  stockCode: String!
  description: String
  quantity: Int!
  invoiceDate: Date!
  unitPrice: Float!
  totalAmount: Float!
  customerId: String!
  country: String!
  
  # Relations
  customer: Customer!
  product: Product!
  
  # Computed
  isReturn: Boolean!
  profitMargin: Float
  
  # Metadata
  createdAt: DateTime!
  updatedAt: DateTime!
  dataQualityScore: Float
}

type Customer {
  customerId: ID!
  country: String!
  
  # Aggregates
  totalTransactions: Int!
  totalSpent: Float!
  avgOrderValue: Float!
  firstPurchaseDate: Date
  lastPurchaseDate: Date
  
  # Analytics
  rfmScore: RFMScore
  segment: String
  lifetimeValue: Float
  churnProbability: Float
  
  # Relations
  sales(filter: SalesFilter, pagination: PaginationInput): [Sale!]!
  
  # Computed
  daysSinceLastPurchase: Int
  purchaseFrequency: Float
}

type Product {
  stockCode: ID!
  description: String!
  category: String
  unitPrice: Float
  
  # Performance metrics
  totalSold: Int!
  totalRevenue: Float!
  avgRating: Float
  popularityScore: Float
  
  # Relations
  sales(filter: SalesFilter, pagination: PaginationInput): [Sale!]!
  
  # Analytics
  seasonalTrends: [SeasonalTrend!]!
  crossSellProducts: [Product!]!
}

# Analytics Types
type RFMScore {
  recency: Int!
  frequency: Int!
  monetary: Int!
  score: String!
}

type SalesAnalytics {
  totalRevenue: Float!
  totalTransactions: Int!
  uniqueCustomers: Int!
  avgOrderValue: Float!
  
  revenueByPeriod: [TimeSeries!]!
  transactionsByPeriod: [TimeSeries!]!
  revenueByCountry: [CountryRevenue!]!
  topProducts: [ProductPerformance!]!
  newVsReturningCustomers: CustomerBreakdown!
  
  monthOverMonthGrowth: Float!
  yearOverYearGrowth: Float!
}

type CustomerSegment {
  segmentId: String!
  name: String!
  description: String
  customerCount: Int!
  percentage: Float!
  
  avgRecency: Float!
  avgFrequency: Float!
  avgMonetaryValue: Float!
  
  totalRevenue: Float!
  avgLifetimeValue: Float!
  churnRate: Float
  
  recommendedActions: [String!]!
  marketingStrategies: [String!]!
}

# Helper Types
type TimeSeries {
  period: String!
  value: Float!
  label: String
}

type CountryRevenue {
  country: String!
  revenue: Float!
  percentage: Float!
  customerCount: Int!
}

type ProductPerformance {
  product: Product!
  revenue: Float!
  unitsSold: Int!
  profitMargin: Float
}

# Root Types
type Query {
  # Core data
  sales(filter: SalesFilter, pagination: PaginationInput): [Sale!]!
  sale(transactionId: ID!): Sale
  customers(filter: CustomerFilter, pagination: PaginationInput): [Customer!]!
  customer(customerId: ID!): Customer
  products(filter: ProductFilter, pagination: PaginationInput): [Product!]!
  product(stockCode: String!): Product
  
  # Analytics
  salesAnalytics(input: AnalyticsInput!): SalesAnalytics!
  customerSegmentation(method: SegmentationMethod!): [CustomerSegment!]!
  cohortAnalysis(input: CohortAnalysisInput!): CohortAnalysis!
  
  # Search
  searchProducts(query: String!, limit: Int = 20): [ProductSearchResult!]!
  searchSemantic(query: String!, k: Int = 10): [SemanticSearchResult!]!
  
  # System
  pipelineStatus: [Pipeline!]!
  systemHealth: SystemHealth!
}

type Mutation {
  createSale(input: CreateSaleInput!): Sale!
  updateSale(id: ID!, input: UpdateSaleInput!): Sale!
  deleteSale(id: ID!): Boolean!
  triggerPipeline(pipelineId: String!, parameters: JSON): PipelineRun!
  refreshAnalytics(type: AnalyticsType!): Boolean!
}

type Subscription {
  salesUpdates(filter: SalesFilter): Sale!
  pipelineUpdates(pipelineId: String): PipelineStatus!
  systemAlerts: SystemAlert!
}
```

---

## Advanced Features

### GraphQL Federation

The API is federation-ready for microservices architecture:

```graphql
# Customer service schema
extend type Customer @key(fields: "customerId") {
  customerId: ID! @external
  preferences: CustomerPreferences
}

# Sales service schema  
type Sale @key(fields: "transactionId") {
  transactionId: ID!
  customer: Customer @provides(fields: "customerId")
}
```

### Custom Directives

```graphql
# Rate limiting directive
type Query {
  expensiveAnalytics: Analytics @rateLimit(max: 5, window: 60)
}

# Authentication directive
type Mutation {
  deleteSale(id: ID!): Boolean @auth(requires: "admin")
}

# Caching directive
type Customer {
  totalSpent: Float @cached(ttl: 300)
}
```

### DataLoader Integration

Automatic N+1 query optimization:

```python
# Automatic batching of customer lookups
@strawberry.field
def customer(self) -> Customer:
    return customer_loader.load(self.customer_id)
```

This ensures that even if you request customers for 100 sales records, only one database query is executed.

### Real-time Features

The GraphQL subscriptions use WebSocket connections for real-time updates. The connection automatically handles:

- **Authentication**: JWT token validation
- **Authorization**: Permission checking for subscription data
- **Connection management**: Automatic reconnection and heartbeat
- **Filtering**: Server-side filtering of subscription data

For detailed subscription examples and WebSocket client setup, see our [Real-time Features Guide](https://docs.pwc-retail-platform.com/realtime).

---

## Migration from REST

### Query Equivalents

| REST Endpoint | GraphQL Query |
|---------------|---------------|
| `GET /api/v1/sales?limit=10` | `{ sales(pagination: {limit: 10}) { ... } }` |
| `GET /api/v1/sales/analytics` | `{ salesAnalytics(...) { ... } }` |
| `GET /api/v1/customers/123` | `{ customer(customerId: "123") { ... } }` |

### Benefits of GraphQL

1. **Single Request**: Get all needed data in one request
2. **No Over-fetching**: Request exactly the fields you need  
3. **Strong Typing**: Built-in validation and documentation
4. **Real-time**: WebSocket subscriptions for live updates
5. **Tooling**: Rich ecosystem of GraphQL tools and libraries

### Best Practices for Migration

1. **Start Small**: Begin with read-only queries
2. **Use Fragments**: Create reusable query fragments
3. **Monitor Performance**: Track query complexity and execution time
4. **Cache Strategically**: Implement appropriate caching layers
5. **Handle Errors**: Plan for partial failures and error states

For a complete migration guide, see our [REST to GraphQL Migration Guide](https://docs.pwc-retail-platform.com/migration/rest-to-graphql).
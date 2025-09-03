# PwC Data Engineering Challenge - Enterprise API

**Version:** 4.0.0  
**Generated:** 2025-09-02 16:28:10

Enterprise-grade high-performance REST API

---

## Overview

This API provides comprehensive endpoints for the PwC Data Engineering Challenge platform, featuring:

- **Enterprise Security**: Advanced authentication, authorization, and data protection
- **High Performance**: Optimized for high-throughput data processing  
- **Scalability**: Microservices architecture with service discovery
- **Compliance**: GDPR, HIPAA, PCI-DSS, SOX compliance
- **Real-time Features**: WebSocket support for live updates

### Key Features

- RESTful design following OpenAPI 3.0 specification
- JSON-based request/response format with performance optimization
- Comprehensive authentication (JWT, API Keys, OAuth2/OIDC)
- Role-based access control (RBAC) with fine-grained permissions
- Rate limiting and throttling for API protection
- Real-time WebSocket connections for live data updates
- GraphQL endpoint for flexible data querying
- Comprehensive error handling with detailed messages

---

## Authentication

The API supports multiple authentication methods:

### JWT Bearer Tokens (Recommended)
```http
Authorization: Bearer <jwt_token>
```

### API Keys  
```http
Authorization: Bearer pwc_<api_key>
```

### OAuth2/OIDC
Supports OAuth2 authorization code flow with PKCE.

---

## Base URL

- Production: `https://api.pwc-challenge.com`
- Staging: `https://staging-api.pwc-challenge.com`  
- Development: `http://localhost:8000`

---

## API Endpoints

Total Endpoints: **8**

### API

#### 🟢 GET `/api/v1/limits`

**Summary:** Get information about batch operation limits and constraints.

🔓 **Public Endpoint**

**Example Request:**

```bash
curl -X GET "$API_BASE_URL/api/v1/limits" \
  -H "Content-Type: application/json"
```

---


### Search

#### 🟢 GET `/api/v1/typesense`

**Summary:** Vector search endpoint with mandatory filters.

MANDATORY: At least one filter must be implemented per challenge requirements.
Available filters:
- country: Filter by specific country
- price_min/price_max: Filter by price range

🔓 **Public Endpoint**

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `q` | string | ✅ | Parameter q |
| `limit` | integer | ✅ | Parameter limit |
| `country` | string | ✅ | Parameter country |
| `price_min` | string | ✅ | Parameter price_min |
| `price_max` | string | ✅ | Parameter price_max |

**Example Request:**

```bash
curl -X GET "$API_BASE_URL/api/v1/typesense" \
  -H "Content-Type: application/json"
```

---

#### 🟢 GET `/api/v1/enhanced`

**Summary:** Enhanced vector search with comprehensive filtering - REQUIREMENT FULFILLED.

This endpoint implements multiple filter types as required:
- Category filtering
- Price range filtering
- Country filtering
- Date range filtering
- Customer segment filtering

🔓 **Public Endpoint**

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `q` | string | ✅ | Parameter q |
| `category` | string | ✅ | Parameter category |
| `price_min` | string | ✅ | Parameter price_min |
| `price_max` | string | ✅ | Parameter price_max |
| `country` | string | ✅ | Parameter country |
| `date_from` | string | ✅ | Parameter date_from |
| `date_to` | string | ✅ | Parameter date_to |
| `customer_segment` | string | ✅ | Parameter customer_segment |
| `per_page` | integer | ✅ | Parameter per_page |
| `page` | integer | ✅ | Parameter page |

**Example Request:**

```bash
curl -X GET "$API_BASE_URL/api/v1/enhanced" \
  -H "Content-Type: application/json"
```

---

#### 🟢 GET `/api/v1/faceted`

**Summary:** Faceted search to get available filter options.

Returns facet counts for dynamic filter UI generation.

🔓 **Public Endpoint**

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `q` | string | ✅ | Parameter q |
| `facet_fields` | string | ✅ | Parameter facet_fields |
| `max_facet_values` | integer | ✅ | Parameter max_facet_values |

**Example Request:**

```bash
curl -X GET "$API_BASE_URL/api/v1/faceted" \
  -H "Content-Type: application/json"
```

---

#### 🟢 GET `/api/v1/geographic`

**Summary:** Geographic search with country-based filtering.

Specialized endpoint for location-based searches.

🔓 **Public Endpoint**

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `q` | string | ✅ | Parameter q |
| `country` | string | ✅ | Parameter country |
| `per_page` | integer | ✅ | Parameter per_page |

**Example Request:**

```bash
curl -X GET "$API_BASE_URL/api/v1/geographic" \
  -H "Content-Type: application/json"
```

---

#### 🔵 POST `/api/v1/advanced`

**Summary:** Advanced search with complex filtering.

Accepts JSON body with complex filter structures:
{
    "total_range": {"min": 10, "max": 1000},
    "date_range": {"from": "2023-01-01", "to": "2023-12-31"},
    "categories": ["Electronics", "Books"],
    "countries": ["United Kingdom", "Germany"]
}

🔓 **Public Endpoint**

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `query` | string | ✅ | Parameter query |
| `filters` | string | ✅ | Parameter filters |
| `sort_by` | string | ✅ | Parameter sort_by |
| `per_page` | integer | ✅ | Parameter per_page |
| `page` | integer | ✅ | Parameter page |

**Example Request:**

```bash
curl -X POST "$API_BASE_URL/api/v1/advanced" \
  -H "Content-Type: application/json"
```

---

#### 🟢 GET `/api/v1/suggestions/{field}`

**Summary:** Get filter suggestions for a specific field.

Useful for autocomplete in search UIs.

🔓 **Public Endpoint**

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `field` | string | ✅ | Parameter field |
| `query` | string | ✅ | Parameter query |

**Example Request:**

```bash
curl -X GET "$API_BASE_URL/api/v1/suggestions/{field}" \
  -H "Content-Type: application/json"
```

---

#### 🟢 GET `/api/v1/filters/available`

**Summary:** Get information about available filters and their types.

Returns metadata about what filters are supported.

🔓 **Public Endpoint**

**Example Request:**

```bash
curl -X GET "$API_BASE_URL/api/v1/filters/available" \
  -H "Content-Type: application/json"
```

---


## Data Models

### SaleItem

Schema for SaleItem

**Properties:**

| Field | Type | Description |
|-------|------|--------------|
| `invoice_no` | string | Field invoice_no |
| `stock_code` | string | Field stock_code |
| `description` | string | Field description |
| `quantity` | integer | Field quantity |
| `invoice_date` | string | Field invoice_date |
| `unit_price` | number | Field unit_price |
| `customer_id` | string | Field customer_id |
| `country` | string | Field country |
| `total` | number | Field total |
| `total_str` | string | Field total_str |

### PaginatedSales

Schema for PaginatedSales

**Properties:**

| Field | Type | Description |
|-------|------|--------------|
| `items` | array | Field items |
| `total` | integer | Field total |
| `page` | integer | Field page |
| `size` | integer | Field size |

### EnhancedSaleItem

Schema for EnhancedSaleItem

**Properties:**

| Field | Type | Description |
|-------|------|--------------|
| `sale_id` | string | Field sale_id |
| `invoice_no` | string | Field invoice_no |
| `stock_code` | string | Field stock_code |
| `description` | string | Field description |
| `quantity` | integer | Field quantity |
| `unit_price` | string | Field unit_price |
| `total_amount` | string | Field total_amount |
| `discount_amount` | string | Field discount_amount |
| `tax_amount` | string | Field tax_amount |
| `profit_amount` | string | Field profit_amount |
| `margin_percentage` | string | Field margin_percentage |
| `customer_id` | string | Field customer_id |
| `customer_segment` | string | Field customer_segment |
| `customer_lifetime_value` | string | Field customer_lifetime_value |
| `country` | string | Field country |
| `country_code` | string | Field country_code |
| `region` | string | Field region |
| `continent` | string | Field continent |
| `product_category` | string | Field product_category |
| `product_brand` | string | Field product_brand |
| `invoice_date` | string | Field invoice_date |
| `fiscal_year` | integer | Field fiscal_year |
| `fiscal_quarter` | integer | Field fiscal_quarter |
| `is_weekend` | boolean | Field is_weekend |
| `is_holiday` | boolean | Field is_holiday |
| `revenue_per_unit` | string | Field revenue_per_unit |
| `profitability_score` | number | Field profitability_score |
| `customer_value_tier` | string | Field customer_value_tier |

### SalesAggregation

Schema for SalesAggregation

**Properties:**

| Field | Type | Description |
|-------|------|--------------|
| `dimension` | string | Field dimension |
| `dimension_value` | string | Field dimension_value |
| `total_revenue` | string | Field total_revenue |
| `total_quantity` | integer | Field total_quantity |
| `transaction_count` | integer | Field transaction_count |
| `unique_customers` | integer | Field unique_customers |
| `avg_order_value` | string | Field avg_order_value |
| `profit_margin` | string | Field profit_margin |
| `growth_rate` | number | Field growth_rate |

### TimeSeriesPoint

Schema for TimeSeriesPoint

**Properties:**

| Field | Type | Description |
|-------|------|--------------|
| `period` | string | Field period |
| `revenue` | string | Field revenue |
| `quantity` | integer | Field quantity |
| `transactions` | integer | Field transactions |
| `unique_customers` | integer | Field unique_customers |
| `avg_order_value` | string | Field avg_order_value |
| `cumulative_revenue` | string | Field cumulative_revenue |
| `period_growth` | number | Field period_growth |

### EnhancedSalesAnalytics

Schema for EnhancedSalesAnalytics

**Properties:**

| Field | Type | Description |
|-------|------|--------------|
| `summary` | object | Field summary |
| `time_series` | integer | Field time_series |
| `top_products` | array | Field top_products |
| `top_customers` | array | Field top_customers |
| `geographic_breakdown` | array | Field geographic_breakdown |
| `category_performance` | array | Field category_performance |
| `seasonal_insights` | object | Field seasonal_insights |
| `forecasting` | object | Field forecasting |

### PaginatedEnhancedSales

Schema for PaginatedEnhancedSales

**Properties:**

| Field | Type | Description |
|-------|------|--------------|
| `items` | array | Field items |
| `total` | integer | Field total |
| `page` | integer | Field page |
| `size` | integer | Field size |
| `pages` | integer | Field pages |
| `has_next` | boolean | Field has_next |
| `has_previous` | boolean | Field has_previous |
| `aggregations` | object | Field aggregations |
| `filters_applied` | object | Field filters_applied |
| `response_time_ms` | number | Field response_time_ms |
| `data_freshness` | string | Field data_freshness |
| `quality_score` | number | Field quality_score |

### SalesFiltersV2

Schema for SalesFiltersV2

**Properties:**

| Field | Type | Description |
|-------|------|--------------|
| `date_from` | string | Field date_from |
| `date_to` | string | Field date_to |
| `fiscal_year` | integer | Field fiscal_year |
| `fiscal_quarter` | integer | Field fiscal_quarter |
| `countries` | string | Field countries |
| `regions` | string | Field regions |
| `continents` | string | Field continents |
| `categories` | string | Field categories |
| `brands` | string | Field brands |
| `stock_codes` | string | Field stock_codes |
| `customer_segments` | string | Field customer_segments |
| `customer_value_tiers` | string | Field customer_value_tiers |
| `min_customer_ltv` | string | Field min_customer_ltv |
| `min_amount` | string | Field min_amount |
| `max_amount` | string | Field max_amount |
| `min_margin` | string | Field min_margin |
| `has_profit_data` | boolean | Field has_profit_data |
| `exclude_cancelled` | boolean | Field exclude_cancelled |
| `exclude_refunds` | boolean | Field exclude_refunds |
| `include_weekends` | boolean | Field include_weekends |
| `include_holidays` | boolean | Field include_holidays |

### SalesExportRequest

Schema for SalesExportRequest

**Properties:**

| Field | Type | Description |
|-------|------|--------------|
| `format` | string | Field format |
| `filters` | string | Field filters |
| `include_analytics` | boolean | Field include_analytics |
| `include_forecasting` | boolean | Field include_forecasting |
| `compression` | string | Field compression |
| `email_notification` | boolean | Field email_notification |
| `notification_email` | string | Field notification_email |

### SalesExportResponse

Schema for SalesExportResponse

**Properties:**

| Field | Type | Description |
|-------|------|--------------|
| `export_id` | string | Field export_id |
| `status` | string | Field status |
| `estimated_completion` | string | Field estimated_completion |
| `download_url` | string | Field download_url |
| `file_size_mb` | number | Field file_size_mb |
| `record_count` | integer | Field record_count |

### CustomerEntity

Schema for CustomerEntity

**Properties:**

| Field | Type | Description |
|-------|------|--------------|
| `customer_key` | integer | Field customer_key |
| `customer_id` | string | Field customer_id |
| `customer_segment` | string | Field customer_segment |
| `customer_value_tier` | string | Field customer_value_tier |
| `status` | string | Field status |
| `lifetime_value` | string | Field lifetime_value |
| `avg_order_value` | string | Field avg_order_value |
| `order_count` | integer | Field order_count |
| `first_order_date` | string | Field first_order_date |
| `last_order_date` | string | Field last_order_date |
| `created_at` | string | Field created_at |
| `is_active` | boolean | Field is_active |
| `try` | number | Field try |
| `Decimal` | number | Field Decimal |
| `datetime` | string | Field datetime |
| `date` | string | Field date |
| `UUID` | string | Field UUID |

### SalesTransactionUpdate

Schema for SalesTransactionUpdate

**Properties:**

| Field | Type | Description |
|-------|------|--------------|
| `description` | string | Field description |
| `status` | string | Field status |
| `payment_method` | string | Field payment_method |

### SalesTransactionFilter

Schema for SalesTransactionFilter

**Properties:**

| Field | Type | Description |
|-------|------|--------------|
| `invoice_no` | string | Field invoice_no |
| `stock_code` | string | Field stock_code |
| `customer_id` | string | Field customer_id |
| `country` | string | Field country |
| `status` | string | Field status |
| `date_from` | string | Field date_from |
| `date_to` | string | Field date_to |
| `min_amount` | string | Field min_amount |
| `max_amount` | string | Field max_amount |
| `min_quantity` | integer | Field min_quantity |
| `max_quantity` | integer | Field max_quantity |

## Common Response Codes

| Code | Status | Description |
|------|--------|-------------|
| 200 | OK | Request successful |
| 201 | Created | Resource created successfully |
| 400 | Bad Request | Invalid request parameters |
| 401 | Unauthorized | Authentication required |
| 403 | Forbidden | Insufficient permissions |
| 404 | Not Found | Resource not found |
| 422 | Unprocessable Entity | Validation error |
| 429 | Too Many Requests | Rate limit exceeded |
| 500 | Internal Server Error | Server error |

## Rate Limiting

- **Default Limit:** 100 requests per minute
- **Authentication Endpoints:** 5 requests per minute
- **Analytics Endpoints:** 20 requests per minute (burst to 30)

Rate limit information is provided in response headers:
- `X-RateLimit-Limit`: Maximum requests allowed
- `X-RateLimit-Remaining`: Remaining requests in current window
- `X-RateLimit-Reset`: Time when the rate limit resets

## Support

For API support and questions:
- **Documentation:** [API Documentation Portal](https://docs.pwc-challenge.com)
- **Issues:** [GitHub Issues](https://github.com/pwc-challenge/issues)
- **Contact:** api-support@pwc-challenge.com

---

*Generated by PwC Data Engineering Challenge API Documentation Generator*

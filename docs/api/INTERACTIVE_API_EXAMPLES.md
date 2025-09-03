# Interactive API Examples - PwC Data Engineering Platform

## 🎯 Introduction

This guide provides practical, executable examples for all API endpoints in the PwC Data Engineering Platform. Each example includes request/response samples, error handling, and integration patterns.

## 🚀 Getting Started with API Testing

### Prerequisites
```bash
# Install HTTP client tools
pip install httpx requests-toolbelt

# Or use curl (pre-installed on most systems)
curl --version

# Set environment variables
export API_BASE_URL="http://localhost:8000"
export API_TOKEN="your-jwt-token-here"
```

### Authentication Setup
```python
import httpx
import asyncio
from datetime import datetime, timedelta

# Get authentication token
async def get_auth_token():
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{API_BASE_URL}/auth/login",
            json={
                "username": "demo@example.com",
                "password": "demo_password"
            }
        )
        return response.json()["access_token"]

# Use token in subsequent requests
token = asyncio.run(get_auth_token())
headers = {"Authorization": f"Bearer {token}"}
```

## 📊 Core API Examples

### 1. Health Check & System Status

#### Basic Health Check
```bash
# Curl example
curl -X GET "${API_BASE_URL}/health" \
  -H "Accept: application/json"
```

```python
# Python example
import httpx

async def check_health():
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{API_BASE_URL}/health")
        return response.json()

# Expected Response:
{
    "status": "healthy",
    "timestamp": "2025-09-01T10:00:00Z",
    "version": "3.0.0",
    "services": {
        "database": "healthy",
        "redis": "healthy",
        "elasticsearch": "healthy"
    }
}
```

#### Detailed System Status
```bash
curl -X GET "${API_BASE_URL}/status" \
  -H "Authorization: Bearer ${API_TOKEN}"
```

```python
async def get_system_status():
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{API_BASE_URL}/status",
            headers=headers
        )
        return response.json()

# Expected Response:
{
    "system": {
        "uptime": 86400,
        "load_average": [0.5, 0.6, 0.7],
        "memory_usage": {
            "total": "16GB",
            "used": "8GB",
            "free": "8GB"
        }
    },
    "services": {
        "api_server": {"status": "running", "port": 8000},
        "etl_workers": {"status": "running", "active_jobs": 3},
        "scheduler": {"status": "running", "next_run": "2025-09-01T11:00:00Z"}
    }
}
```

### 2. Customer Management APIs

#### Create Customer
```python
import httpx
from pydantic import BaseModel
from typing import Optional

class CustomerCreate(BaseModel):
    customer_id: str
    email: str
    first_name: str
    last_name: str
    phone: Optional[str] = None
    address: Optional[dict] = None

async def create_customer(customer_data: CustomerCreate):
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{API_BASE_URL}/api/v1/customers",
            headers=headers,
            json=customer_data.dict()
        )
        if response.status_code == 201:
            return response.json()
        else:
            raise Exception(f"Failed to create customer: {response.text}")

# Example usage
customer_data = CustomerCreate(
    customer_id="CUST-001",
    email="john.doe@example.com",
    first_name="John",
    last_name="Doe",
    phone="+1-555-0123",
    address={
        "street": "123 Main St",
        "city": "New York",
        "state": "NY",
        "zip_code": "10001",
        "country": "US"
    }
)

result = await create_customer(customer_data)
```

#### Bulk Customer Import
```python
async def bulk_import_customers(customers: list[CustomerCreate]):
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            f"{API_BASE_URL}/api/v1/customers/bulk",
            headers=headers,
            json=[customer.dict() for customer in customers]
        )
        return response.json()

# Example: Import 1000 customers
customers = [
    CustomerCreate(
        customer_id=f"CUST-{i:04d}",
        email=f"customer{i}@example.com",
        first_name=f"Customer",
        last_name=f"{i}",
    )
    for i in range(1, 1001)
]

result = await bulk_import_customers(customers)
# Response includes success count, error details, and processing time
```

#### Search Customers with Advanced Filters
```bash
# Search by email pattern
curl -X GET "${API_BASE_URL}/api/v1/customers/search" \
  -H "Authorization: Bearer ${API_TOKEN}" \
  -G -d "email_pattern=%.com" \
  -d "limit=50" \
  -d "offset=0"

# Search by date range
curl -X GET "${API_BASE_URL}/api/v1/customers/search" \
  -H "Authorization: Bearer ${API_TOKEN}" \
  -G -d "created_after=2025-01-01" \
  -d "created_before=2025-12-31" \
  -d "sort_by=created_at" \
  -d "sort_order=desc"
```

```python
from datetime import datetime, date

async def search_customers(
    email_pattern: str = None,
    created_after: date = None,
    created_before: date = None,
    limit: int = 50,
    offset: int = 0
):
    params = {"limit": limit, "offset": offset}
    
    if email_pattern:
        params["email_pattern"] = email_pattern
    if created_after:
        params["created_after"] = created_after.isoformat()
    if created_before:
        params["created_before"] = created_before.isoformat()
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{API_BASE_URL}/api/v1/customers/search",
            headers=headers,
            params=params
        )
        return response.json()

# Example usage
results = await search_customers(
    email_pattern="%.gmail.com",
    created_after=date(2025, 1, 1),
    limit=100
)
```

### 3. Product Management APIs

#### Product CRUD Operations
```python
class ProductCreate(BaseModel):
    product_id: str
    name: str
    description: Optional[str] = None
    category: str
    price: float
    stock_quantity: int
    supplier_id: Optional[str] = None

async def manage_products():
    # Create product
    product_data = ProductCreate(
        product_id="PROD-001",
        name="Premium Headphones",
        description="High-quality wireless headphones with noise cancellation",
        category="Electronics",
        price=299.99,
        stock_quantity=100
    )
    
    async with httpx.AsyncClient() as client:
        # CREATE
        create_response = await client.post(
            f"{API_BASE_URL}/api/v1/products",
            headers=headers,
            json=product_data.dict()
        )
        
        # READ
        get_response = await client.get(
            f"{API_BASE_URL}/api/v1/products/PROD-001",
            headers=headers
        )
        
        # UPDATE
        update_data = {"price": 279.99, "stock_quantity": 95}
        update_response = await client.patch(
            f"{API_BASE_URL}/api/v1/products/PROD-001",
            headers=headers,
            json=update_data
        )
        
        # DELETE
        delete_response = await client.delete(
            f"{API_BASE_URL}/api/v1/products/PROD-001",
            headers=headers
        )
        
    return {
        "created": create_response.json(),
        "retrieved": get_response.json(),
        "updated": update_response.json(),
        "deleted": delete_response.status_code == 204
    }
```

#### Product Analytics
```python
async def get_product_analytics(product_id: str, days: int = 30):
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{API_BASE_URL}/api/v1/products/{product_id}/analytics",
            headers=headers,
            params={"days": days}
        )
        return response.json()

# Example response
{
    "product_id": "PROD-001",
    "period": "30_days",
    "metrics": {
        "total_sales": 1250,
        "revenue": 37497.50,
        "average_order_value": 29.99,
        "customer_satisfaction": 4.7,
        "return_rate": 0.02
    },
    "trends": {
        "sales_trend": "increasing",
        "revenue_growth": 0.15,
        "seasonal_pattern": "holiday_spike"
    }
}
```

### 4. Order Processing APIs

#### Complete Order Workflow
```python
class OrderItem(BaseModel):
    product_id: str
    quantity: int
    unit_price: float

class OrderCreate(BaseModel):
    customer_id: str
    items: list[OrderItem]
    shipping_address: dict
    payment_method: str

async def complete_order_workflow():
    # Create order
    order_data = OrderCreate(
        customer_id="CUST-001",
        items=[
            OrderItem(product_id="PROD-001", quantity=2, unit_price=299.99),
            OrderItem(product_id="PROD-002", quantity=1, unit_price=199.99)
        ],
        shipping_address={
            "street": "123 Main St",
            "city": "New York",
            "state": "NY",
            "zip_code": "10001"
        },
        payment_method="credit_card"
    )
    
    async with httpx.AsyncClient() as client:
        # Step 1: Create order
        create_response = await client.post(
            f"{API_BASE_URL}/api/v1/orders",
            headers=headers,
            json=order_data.dict()
        )
        order = create_response.json()
        order_id = order["order_id"]
        
        # Step 2: Process payment
        payment_response = await client.post(
            f"{API_BASE_URL}/api/v1/orders/{order_id}/payment",
            headers=headers,
            json={
                "payment_method": "credit_card",
                "card_token": "tok_visa_4242",
                "amount": order["total_amount"]
            }
        )
        
        # Step 3: Update inventory
        inventory_response = await client.post(
            f"{API_BASE_URL}/api/v1/orders/{order_id}/inventory",
            headers=headers
        )
        
        # Step 4: Generate shipping label
        shipping_response = await client.post(
            f"{API_BASE_URL}/api/v1/orders/{order_id}/shipping",
            headers=headers,
            json={
                "carrier": "UPS",
                "service_type": "ground"
            }
        )
        
        return {
            "order": order,
            "payment_status": payment_response.json(),
            "inventory_updated": inventory_response.json(),
            "shipping_label": shipping_response.json()
        }
```

### 5. Analytics and Reporting APIs

#### Sales Analytics Dashboard
```python
async def get_sales_dashboard(date_range: str = "30d"):
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{API_BASE_URL}/api/v1/analytics/sales-dashboard",
            headers=headers,
            params={"range": date_range}
        )
        return response.json()

# Example response
{
    "period": "30_days",
    "summary": {
        "total_revenue": 1250000.00,
        "total_orders": 5000,
        "average_order_value": 250.00,
        "customer_acquisition_cost": 45.00
    },
    "trends": {
        "revenue_growth": 0.15,
        "order_growth": 0.12,
        "customer_retention": 0.85
    },
    "top_products": [
        {"product_id": "PROD-001", "revenue": 50000, "units_sold": 200},
        {"product_id": "PROD-002", "revenue": 45000, "units_sold": 180}
    ],
    "geographic_distribution": {
        "US": 0.60,
        "CA": 0.20,
        "UK": 0.15,
        "Other": 0.05
    }
}
```

#### Customer Segmentation Analysis
```python
async def get_customer_segments():
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{API_BASE_URL}/api/v1/analytics/customer-segments",
            headers=headers
        )
        return response.json()

# RFM Analysis Response
{
    "segments": {
        "champions": {
            "count": 1250,
            "percentage": 0.25,
            "avg_revenue": 500.00,
            "characteristics": ["High frequency", "Recent purchases", "High value"]
        },
        "loyal_customers": {
            "count": 1500,
            "percentage": 0.30,
            "avg_revenue": 350.00,
            "characteristics": ["Regular purchases", "Moderate value"]
        },
        "at_risk": {
            "count": 750,
            "percentage": 0.15,
            "avg_revenue": 400.00,
            "characteristics": ["High value", "Long time since purchase"]
        }
    },
    "recommendations": {
        "champions": "Reward and retain",
        "loyal_customers": "Upsell opportunities",
        "at_risk": "Win-back campaigns"
    }
}
```

## 🔍 Advanced API Features

### 1. Real-time Data Streaming
```python
import asyncio
import websockets
import json

async def stream_real_time_events():
    uri = f"ws://localhost:8000/ws/events"
    
    async with websockets.connect(uri) as websocket:
        # Subscribe to specific event types
        await websocket.send(json.dumps({
            "action": "subscribe",
            "events": ["order_created", "payment_processed", "inventory_updated"]
        }))
        
        # Listen for events
        async for message in websocket:
            event = json.loads(message)
            print(f"Received event: {event['type']} - {event['data']}")
            
            if event["type"] == "order_created":
                await handle_new_order(event["data"])
            elif event["type"] == "payment_processed":
                await handle_payment_confirmation(event["data"])

# Example event handling
async def handle_new_order(order_data):
    print(f"Processing new order: {order_data['order_id']}")
    # Trigger inventory check, send confirmation email, etc.
```

### 2. Batch Operations with Progress Tracking
```python
async def bulk_data_import_with_progress():
    # Start bulk import job
    async with httpx.AsyncClient() as client:
        start_response = await client.post(
            f"{API_BASE_URL}/api/v1/bulk/import",
            headers=headers,
            json={
                "operation": "customer_import",
                "data_source": "s3://bucket/customers.csv",
                "options": {
                    "batch_size": 1000,
                    "validate_data": True,
                    "skip_duplicates": True
                }
            }
        )
        
        job_id = start_response.json()["job_id"]
        
        # Poll for progress
        while True:
            progress_response = await client.get(
                f"{API_BASE_URL}/api/v1/bulk/jobs/{job_id}",
                headers=headers
            )
            
            progress = progress_response.json()
            print(f"Progress: {progress['percent_complete']}% - {progress['status']}")
            
            if progress["status"] in ["completed", "failed"]:
                break
                
            await asyncio.sleep(5)
        
        return progress

# Example progress response
{
    "job_id": "job_123456",
    "status": "processing",
    "percent_complete": 65,
    "records_processed": 6500,
    "total_records": 10000,
    "errors": 5,
    "estimated_completion": "2025-09-01T10:15:00Z"
}
```

### 3. GraphQL Query Examples
```python
import httpx

async def graphql_query(query: str, variables: dict = None):
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{API_BASE_URL}/graphql",
            headers=headers,
            json={
                "query": query,
                "variables": variables or {}
            }
        )
        return response.json()

# Complex nested query
customer_orders_query = """
query GetCustomerWithOrders($customerId: ID!, $orderLimit: Int = 10) {
    customer(id: $customerId) {
        id
        email
        firstName
        lastName
        profile {
            preferredCategories
            lifetimeValue
            segmentType
        }
        orders(limit: $orderLimit, orderBy: {field: CREATED_AT, direction: DESC}) {
            edges {
                node {
                    id
                    status
                    totalAmount
                    createdAt
                    items {
                        product {
                            id
                            name
                            category
                        }
                        quantity
                        unitPrice
                    }
                }
            }
            totalCount
        }
        analytics {
            totalOrders
            averageOrderValue
            lastOrderDate
            favoriteCategory
        }
    }
}
"""

result = await graphql_query(
    customer_orders_query, 
    {"customerId": "CUST-001", "orderLimit": 5}
)
```

### 4. Machine Learning Model Integration
```python
async def get_product_recommendations(customer_id: str, limit: int = 10):
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{API_BASE_URL}/api/v1/ml/recommendations",
            headers=headers,
            json={
                "customer_id": customer_id,
                "algorithm": "collaborative_filtering",
                "limit": limit,
                "include_explanations": True
            }
        )
        return response.json()

# ML-powered customer insights
async def analyze_customer_behavior(customer_id: str):
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{API_BASE_URL}/api/v1/ml/customer-analysis",
            headers=headers,
            json={
                "customer_id": customer_id,
                "models": ["churn_prediction", "lifetime_value", "next_purchase"]
            }
        )
        return response.json()

# Example response
{
    "customer_id": "CUST-001",
    "predictions": {
        "churn_probability": 0.15,
        "predicted_ltv": 2500.00,
        "next_purchase_category": "Electronics",
        "next_purchase_probability": 0.78,
        "days_to_next_purchase": 14
    },
    "recommendations": [
        {
            "product_id": "PROD-123",
            "confidence": 0.85,
            "reason": "Similar customers also bought this"
        }
    ]
}
```

## 📊 API Performance and Monitoring

### Response Time Monitoring
```python
import time
import statistics

async def benchmark_api_endpoint(endpoint: str, iterations: int = 100):
    times = []
    
    async with httpx.AsyncClient() as client:
        for i in range(iterations):
            start_time = time.time()
            response = await client.get(f"{API_BASE_URL}{endpoint}", headers=headers)
            end_time = time.time()
            
            times.append(end_time - start_time)
            
            if i % 10 == 0:
                print(f"Completed {i}/{iterations} requests")
    
    return {
        "endpoint": endpoint,
        "iterations": iterations,
        "avg_response_time": statistics.mean(times),
        "median_response_time": statistics.median(times),
        "p95_response_time": sorted(times)[int(0.95 * len(times))],
        "min_response_time": min(times),
        "max_response_time": max(times)
    }

# Benchmark critical endpoints
endpoints = [
    "/api/v1/customers/CUST-001",
    "/api/v1/products/search?category=Electronics",
    "/api/v1/orders/recent?limit=50"
]

for endpoint in endpoints:
    benchmark_result = await benchmark_api_endpoint(endpoint)
    print(f"Benchmark results for {endpoint}: {benchmark_result}")
```

### Rate Limiting Testing
```python
async def test_rate_limits():
    async with httpx.AsyncClient() as client:
        # Test rate limiting
        requests_made = 0
        start_time = time.time()
        
        while True:
            response = await client.get(f"{API_BASE_URL}/api/v1/health", headers=headers)
            requests_made += 1
            
            if response.status_code == 429:  # Too Many Requests
                elapsed = time.time() - start_time
                print(f"Rate limit reached after {requests_made} requests in {elapsed:.2f} seconds")
                print(f"Rate limit headers: {dict(response.headers)}")
                break
                
            if requests_made > 1000:  # Safety limit
                break
                
            await asyncio.sleep(0.01)  # Small delay between requests
```

## 🛠️ Testing and Validation Tools

### API Integration Test Suite
```python
import pytest
import httpx
import asyncio

class APITestSuite:
    def __init__(self, base_url: str, auth_token: str):
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {auth_token}"}
        
    async def test_customer_lifecycle(self):
        """Test complete customer creation, update, and deletion"""
        async with httpx.AsyncClient() as client:
            # Create customer
            customer_data = {
                "customer_id": "TEST-001",
                "email": "test@example.com",
                "first_name": "Test",
                "last_name": "User"
            }
            
            create_response = await client.post(
                f"{self.base_url}/api/v1/customers",
                headers=self.headers,
                json=customer_data
            )
            assert create_response.status_code == 201
            
            # Verify customer exists
            get_response = await client.get(
                f"{self.base_url}/api/v1/customers/TEST-001",
                headers=self.headers
            )
            assert get_response.status_code == 200
            customer = get_response.json()
            assert customer["email"] == "test@example.com"
            
            # Update customer
            update_response = await client.patch(
                f"{self.base_url}/api/v1/customers/TEST-001",
                headers=self.headers,
                json={"phone": "+1-555-9999"}
            )
            assert update_response.status_code == 200
            
            # Delete customer
            delete_response = await client.delete(
                f"{self.base_url}/api/v1/customers/TEST-001",
                headers=self.headers
            )
            assert delete_response.status_code == 204

# Usage
test_suite = APITestSuite("http://localhost:8000", token)
await test_suite.test_customer_lifecycle()
```

### Data Validation Examples
```python
from pydantic import BaseModel, validator, Field
from typing import List, Optional
from datetime import datetime

class APIResponseValidator(BaseModel):
    """Validate API response structure and data types"""
    
    class Config:
        extra = "forbid"  # Reject unknown fields
        
class CustomerResponse(APIResponseValidator):
    customer_id: str = Field(..., regex=r"CUST-\d{3,}")
    email: str = Field(..., regex=r".+@.+\..+")
    first_name: str = Field(..., min_length=1, max_length=50)
    last_name: str = Field(..., min_length=1, max_length=50)
    created_at: datetime
    updated_at: datetime
    
    @validator('email')
    def validate_email_domain(cls, v):
        allowed_domains = ['example.com', 'test.com', 'company.com']
        domain = v.split('@')[1]
        if domain not in allowed_domains:
            raise ValueError(f'Email domain {domain} not allowed')
        return v

# Validate API responses
async def validate_api_response():
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{API_BASE_URL}/api/v1/customers/CUST-001",
            headers=headers
        )
        
        # Validate response structure
        customer = CustomerResponse(**response.json())
        print(f"Validated customer: {customer.customer_id}")
        
        return customer
```

## 🔒 Security Testing Examples

### Authentication Testing
```python
async def test_authentication_flows():
    """Test various authentication scenarios"""
    
    async with httpx.AsyncClient() as client:
        # Test invalid token
        invalid_headers = {"Authorization": "Bearer invalid_token"}
        response = await client.get(
            f"{API_BASE_URL}/api/v1/customers",
            headers=invalid_headers
        )
        assert response.status_code == 401
        
        # Test expired token
        expired_headers = {"Authorization": "Bearer expired_token"}
        response = await client.get(
            f"{API_BASE_URL}/api/v1/customers",
            headers=expired_headers
        )
        assert response.status_code == 401
        
        # Test missing token
        response = await client.get(f"{API_BASE_URL}/api/v1/customers")
        assert response.status_code == 401
        
        print("Authentication tests passed!")

await test_authentication_flows()
```

### Input Validation Testing
```python
async def test_input_validation():
    """Test API input validation and error handling"""
    
    test_cases = [
        # Invalid email format
        {"email": "invalid-email", "expected_status": 422},
        # Missing required fields
        {"first_name": "John", "expected_status": 422},
        # SQL injection attempt
        {"email": "user@example.com'; DROP TABLE customers; --", "expected_status": 422},
        # XSS attempt
        {"first_name": "<script>alert('xss')</script>", "expected_status": 422},
    ]
    
    async with httpx.AsyncClient() as client:
        for test_case in test_cases:
            expected_status = test_case.pop("expected_status")
            
            response = await client.post(
                f"{API_BASE_URL}/api/v1/customers",
                headers=headers,
                json=test_case
            )
            
            assert response.status_code == expected_status
            print(f"Validation test passed for: {test_case}")
```

---

## 📚 Additional Resources

### API Documentation Links
- **OpenAPI Specification**: `http://localhost:8000/openapi.json`
- **Interactive Swagger UI**: `http://localhost:8000/docs`
- **ReDoc Documentation**: `http://localhost:8000/redoc`
- **GraphQL Playground**: `http://localhost:8000/graphql`

### Example Collections
- **Postman Collection**: [Download](../examples/postman_collection.json)
- **Insomnia Collection**: [Download](../examples/insomnia_collection.json)
- **HTTPie Examples**: [View Examples](../examples/httpie_examples.md)

### SDK and Client Libraries
- **Python SDK**: `pip install pwc-data-platform-sdk`
- **JavaScript SDK**: `npm install @pwc/data-platform-client`
- **Go Client**: [GitHub Repository](https://github.com/pwc/data-platform-go-client)

---

**Last Updated**: September 1, 2025  
**API Version**: v1.0.0  
**Maintained By**: API Development Team  
**Support**: Create issue or contact api-team@company.com
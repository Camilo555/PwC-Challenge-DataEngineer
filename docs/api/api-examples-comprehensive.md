# PwC Enterprise API - Comprehensive Examples Guide

This guide provides complete examples for integrating with the PwC Enterprise Data Engineering API across multiple programming languages and scenarios.

## Table of Contents

1. [Authentication Examples](#authentication-examples)
2. [Sales Analytics Examples](#sales-analytics-examples)
3. [AI/ML Integration Examples](#aiml-integration-examples)
4. [Search & Discovery Examples](#search--discovery-examples)
5. [Real-time Analytics Examples](#real-time-analytics-examples)
6. [Error Handling Examples](#error-handling-examples)
7. [Performance Optimization Examples](#performance-optimization-examples)
8. [SDK Usage Examples](#sdk-usage-examples)

---

## Authentication Examples

### JWT Authentication

#### Python Example
```python
import requests
import json
from datetime import datetime, timedelta

class PwCAPIClient:
    def __init__(self, base_url="http://localhost:8000"):
        self.base_url = base_url
        self.access_token = None
        self.refresh_token = None
        self.token_expires_at = None

    def authenticate(self, username, password, remember_me=False):
        """Authenticate with username and password"""
        auth_data = {
            "username": username,
            "password": password,
            "remember_me": remember_me
        }

        response = requests.post(
            f"{self.base_url}/api/v1/auth/login",
            json=auth_data,
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            auth_result = response.json()
            self.access_token = auth_result["access_token"]
            self.refresh_token = auth_result.get("refresh_token")

            # Calculate token expiration
            expires_in = auth_result.get("expires_in", 86400)  # Default 24 hours
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)

            print(f"Authentication successful. Token expires at: {self.token_expires_at}")
            return True
        else:
            print(f"Authentication failed: {response.text}")
            return False

    def refresh_access_token(self):
        """Refresh the access token using refresh token"""
        if not self.refresh_token:
            print("No refresh token available")
            return False

        response = requests.post(
            f"{self.base_url}/api/v1/auth/refresh",
            json={"refresh_token": self.refresh_token}
        )

        if response.status_code == 200:
            auth_result = response.json()
            self.access_token = auth_result["access_token"]
            expires_in = auth_result.get("expires_in", 86400)
            self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
            return True
        else:
            print(f"Token refresh failed: {response.text}")
            return False

    def get_headers(self):
        """Get headers with authentication"""
        if self.is_token_expired():
            self.refresh_access_token()

        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "User-Agent": "PwC-API-Client/3.1.0"
        }

    def is_token_expired(self):
        """Check if token is expired or about to expire"""
        if not self.token_expires_at:
            return True
        return datetime.now() >= (self.token_expires_at - timedelta(minutes=5))

# Usage example
client = PwCAPIClient()
if client.authenticate("data_engineer", "SecurePass123!"):
    headers = client.get_headers()
    print("Ready to make authenticated API calls")
```

#### JavaScript/TypeScript Example
```typescript
interface AuthResponse {
  access_token: string;
  refresh_token?: string;
  token_type: string;
  expires_in: number;
  user_info: {
    user_id: string;
    username: string;
    roles: string[];
  };
}

class PwCAPIClient {
  private baseUrl: string;
  private accessToken: string | null = null;
  private refreshToken: string | null = null;
  private tokenExpiresAt: Date | null = null;

  constructor(baseUrl: string = "http://localhost:8000") {
    this.baseUrl = baseUrl;
  }

  async authenticate(username: string, password: string, rememberMe: boolean = false): Promise<boolean> {
    try {
      const response = await fetch(`${this.baseUrl}/api/v1/auth/login`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          username,
          password,
          remember_me: rememberMe
        })
      });

      if (response.ok) {
        const authResult: AuthResponse = await response.json();
        this.accessToken = authResult.access_token;
        this.refreshToken = authResult.refresh_token || null;

        // Calculate token expiration
        const expiresIn = authResult.expires_in || 86400; // Default 24 hours
        this.tokenExpiresAt = new Date(Date.now() + (expiresIn * 1000));

        console.log(`Authentication successful. Token expires at: ${this.tokenExpiresAt}`);
        return true;
      } else {
        const error = await response.text();
        console.error(`Authentication failed: ${error}`);
        return false;
      }
    } catch (error) {
      console.error(`Authentication error: ${error}`);
      return false;
    }
  }

  async refreshAccessToken(): Promise<boolean> {
    if (!this.refreshToken) {
      console.error("No refresh token available");
      return false;
    }

    try {
      const response = await fetch(`${this.baseUrl}/api/v1/auth/refresh`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          refresh_token: this.refreshToken
        })
      });

      if (response.ok) {
        const authResult: AuthResponse = await response.json();
        this.accessToken = authResult.access_token;
        const expiresIn = authResult.expires_in || 86400;
        this.tokenExpiresAt = new Date(Date.now() + (expiresIn * 1000));
        return true;
      } else {
        console.error(`Token refresh failed: ${await response.text()}`);
        return false;
      }
    } catch (error) {
      console.error(`Token refresh error: ${error}`);
      return false;
    }
  }

  async getHeaders(): Promise<Record<string, string>> {
    if (this.isTokenExpired()) {
      await this.refreshAccessToken();
    }

    return {
      'Authorization': `Bearer ${this.accessToken}`,
      'Content-Type': 'application/json',
      'User-Agent': 'PwC-API-Client/3.1.0'
    };
  }

  private isTokenExpired(): boolean {
    if (!this.tokenExpiresAt) {
      return true;
    }
    // Check if token expires in the next 5 minutes
    return new Date() >= new Date(this.tokenExpiresAt.getTime() - (5 * 60 * 1000));
  }
}

// Usage example
async function main() {
  const client = new PwCAPIClient();

  if (await client.authenticate("data_engineer", "SecurePass123!")) {
    const headers = await client.getHeaders();
    console.log("Ready to make authenticated API calls");

    // Example API call
    const response = await fetch("http://localhost:8000/api/v1/sales", {
      headers: headers
    });

    if (response.ok) {
      const salesData = await response.json();
      console.log(`Retrieved ${salesData.items.length} sales records`);
    }
  }
}

main().catch(console.error);
```

### API Key Authentication

#### cURL Example
```bash
#!/bin/bash

# Set your API key
API_KEY="pwc_your_api_key_here"
BASE_URL="http://localhost:8000"

# Function to make authenticated API calls
api_call() {
    local method=$1
    local endpoint=$2
    local data=$3

    curl -X $method \
         -H "Authorization: Bearer $API_KEY" \
         -H "Content-Type: application/json" \
         -H "User-Agent: PwC-API-Client/bash-3.1.0" \
         ${data:+-d "$data"} \
         "$BASE_URL$endpoint"
}

# Example: Get sales data
echo "Fetching sales data..."
api_call GET "/api/v1/sales?country=United%20Kingdom&page=1&size=20"

# Example: Get analytics
echo -e "\n\nFetching analytics..."
api_call GET "/api/v1/analytics/revenue-summary"

# Example: Test ML prediction
echo -e "\n\nTesting ML prediction..."
PREDICTION_DATA='{
    "model_id": "customer_churn_v2",
    "features": {
        "tenure": 12,
        "monthly_charges": 65.50,
        "total_charges": 786.00
    },
    "options": {
        "explain": true,
        "confidence": true
    }
}'
api_call POST "/api/v1/ml-analytics/predict" "$PREDICTION_DATA"
```

---

## Sales Analytics Examples

### Basic Sales Data Retrieval

#### Python with Pandas
```python
import pandas as pd
import requests
from datetime import datetime, timedelta

def get_sales_data(client, filters=None, page=1, size=100):
    """Retrieve sales data with optional filters"""
    params = {
        "page": page,
        "size": size
    }

    if filters:
        params.update(filters)

    response = requests.get(
        f"{client.base_url}/api/v1/sales",
        headers=client.get_headers(),
        params=params
    )

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API call failed: {response.text}")

def sales_to_dataframe(sales_response):
    """Convert sales API response to pandas DataFrame"""
    items = sales_response.get("items", [])
    df = pd.DataFrame(items)

    # Convert date columns
    if 'invoice_date' in df.columns:
        df['invoice_date'] = pd.to_datetime(df['invoice_date'])

    # Calculate derived metrics
    if all(col in df.columns for col in ['quantity', 'unit_price']):
        df['total_amount'] = df['quantity'] * df['unit_price']

    return df

# Usage example
client = PwCAPIClient()
client.authenticate("data_engineer", "SecurePass123!")

# Get sales data for analysis
filters = {
    "country": "United Kingdom",
    "date_from": "2010-12-01",
    "date_to": "2011-12-31"
}

sales_response = get_sales_data(client, filters)
sales_df = sales_to_dataframe(sales_response)

# Basic analysis
print(f"Total records: {len(sales_df)}")
print(f"Total revenue: ${sales_df['total_amount'].sum():,.2f}")
print(f"Average order value: ${sales_df['total_amount'].mean():.2f}")
print(f"Date range: {sales_df['invoice_date'].min()} to {sales_df['invoice_date'].max()}")

# Top products by revenue
top_products = sales_df.groupby('description')['total_amount'].sum().sort_values(ascending=False).head(10)
print("\nTop 10 products by revenue:")
print(top_products)
```

### Advanced Analytics with Time Series

#### Python with Advanced Analytics
```python
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta

def get_advanced_analytics(client, start_date, end_date, metrics=None, dimensions=None):
    """Get advanced analytics data"""
    analytics_request = {
        "start_date": start_date,
        "end_date": end_date,
        "metrics": metrics or ["revenue", "transactions", "unique_customers"],
        "dimensions": dimensions or ["date", "country", "product_category"],
        "granularity": "daily",
        "filters": {
            "min_amount": 10.0
        }
    }

    response = requests.post(
        f"{client.base_url}/api/v2/analytics/advanced-analytics",
        headers=client.get_headers(),
        json=analytics_request
    )

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Analytics API call failed: {response.text}")

def create_revenue_trend_chart(analytics_data):
    """Create revenue trend visualization"""
    # Extract time series data
    time_series = analytics_data.get("data", {}).get("time_series", [])

    if not time_series:
        print("No time series data available")
        return

    df = pd.DataFrame(time_series)
    df['date'] = pd.to_datetime(df['date'])

    # Create the plot
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['revenue'], linewidth=2, marker='o', markersize=4)
    plt.title('Daily Revenue Trend', fontsize=16, fontweight='bold')
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Revenue ($)', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)

    # Format y-axis as currency
    ax = plt.gca()
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))

    plt.tight_layout()
    plt.show()

    # Print summary statistics
    print(f"Revenue Statistics:")
    print(f"  Total Revenue: ${df['revenue'].sum():,.2f}")
    print(f"  Average Daily Revenue: ${df['revenue'].mean():,.2f}")
    print(f"  Highest Day: ${df['revenue'].max():,.2f}")
    print(f"  Growth Rate: {((df['revenue'].iloc[-1] / df['revenue'].iloc[0]) - 1) * 100:.1f}%")

# Usage example
client = PwCAPIClient()
client.authenticate("data_engineer", "SecurePass123!")

# Get advanced analytics
analytics_data = get_advanced_analytics(
    client,
    start_date="2010-12-01T00:00:00Z",
    end_date="2011-12-31T23:59:59Z"
)

print(f"Analytics Request ID: {analytics_data.get('request_id')}")
print(f"Processing Time: {analytics_data.get('processing_time_ms')}ms")

# Create visualizations
create_revenue_trend_chart(analytics_data)
```

---

## AI/ML Integration Examples

### Machine Learning Predictions

#### Python ML Integration
```python
import json
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional

class MLPredictionClient:
    def __init__(self, api_client):
        self.api_client = api_client

    def predict_customer_churn(self, customer_features: Dict[str, Any]) -> Dict[str, Any]:
        """Predict customer churn probability"""
        prediction_request = {
            "model_id": "customer_churn_v2",
            "features": customer_features,
            "options": {
                "explain": True,
                "confidence": True,
                "probability": True
            }
        }

        response = requests.post(
            f"{self.api_client.base_url}/api/v1/ml-analytics/predict",
            headers=self.api_client.get_headers(),
            json=prediction_request
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"ML prediction failed: {response.text}")

    def batch_predict(self, model_id: str, features_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Batch prediction for multiple records"""
        batch_request = {
            "model_id": model_id,
            "batch_features": features_list,
            "options": {
                "explain": False,  # Disable for batch to improve performance
                "confidence": True
            }
        }

        response = requests.post(
            f"{self.api_client.base_url}/api/v1/ml-analytics/batch-predict",
            headers=self.api_client.get_headers(),
            json=batch_request
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Batch prediction failed: {response.text}")

    def get_model_info(self, model_id: str) -> Dict[str, Any]:
        """Get information about a specific model"""
        response = requests.get(
            f"{self.api_client.base_url}/api/v1/ml-analytics/models/{model_id}",
            headers=self.api_client.get_headers()
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to get model info: {response.text}")

# Usage example
client = PwCAPIClient()
client.authenticate("data_engineer", "SecurePass123!")

ml_client = MLPredictionClient(client)

# Single prediction
customer_data = {
    "tenure": 12,
    "monthly_charges": 65.50,
    "total_charges": 786.00,
    "contract": "Month-to-month",
    "payment_method": "Electronic check",
    "internet_service": "DSL"
}

churn_prediction = ml_client.predict_customer_churn(customer_data)
print(f"Churn Prediction: {churn_prediction['prediction']}")
print(f"Confidence: {churn_prediction['confidence']:.2%}")
print(f"Probability: {churn_prediction['probability']:.2%}")

if churn_prediction.get('explanation'):
    print("\nFeature Importance:")
    for feature, importance in churn_prediction['explanation']['feature_importance'].items():
        print(f"  {feature}: {importance:.3f}")

# Batch prediction example
customers_list = [
    {"tenure": 24, "monthly_charges": 80.00, "total_charges": 1920.00},
    {"tenure": 6, "monthly_charges": 45.00, "total_charges": 270.00},
    {"tenure": 36, "monthly_charges": 95.00, "total_charges": 3420.00}
]

batch_results = ml_client.batch_predict("customer_churn_v2", customers_list)
print(f"\nBatch Prediction Results ({len(batch_results['predictions'])} customers):")
for i, result in enumerate(batch_results['predictions']):
    print(f"  Customer {i+1}: {result['prediction']} (confidence: {result['confidence']:.2%})")
```

### Natural Language Query Processing

#### AI Conversational Analytics
```python
def natural_language_query(client, query: str, context: Dict[str, Any] = None):
    """Process natural language queries using AI"""
    nl_request = {
        "query": query,
        "context": context or {
            "data_sources": ["sales", "customers", "products"],
            "preferred_format": "table",
            "include_visualization": True
        },
        "options": {
            "explain_reasoning": True,
            "suggest_followup": True,
            "confidence_threshold": 0.8
        }
    }

    response = requests.post(
        f"{client.base_url}/api/v1/ai-conversational-analytics/query",
        headers=client.get_headers(),
        json=nl_request
    )

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Natural language query failed: {response.text}")

# Usage examples
client = PwCAPIClient()
client.authenticate("data_engineer", "SecurePass123!")

# Example queries
queries = [
    "Show me the top 10 customers by revenue in the UK for 2024",
    "What are the trending products this month?",
    "Compare sales performance between Q1 and Q2",
    "Which customer segments have the highest churn risk?",
    "Find seasonal patterns in our product sales"
]

for query in queries:
    print(f"\nQuery: {query}")
    result = natural_language_query(client, query)

    print(f"Interpretation: {result['interpretation']}")
    print(f"Confidence: {result['confidence']:.2%}")

    if result['data']:
        print(f"Results: {len(result['data'])} records found")

    if result.get('suggested_followup'):
        print(f"Suggested followup: {result['suggested_followup']}")
```

---

## Search & Discovery Examples

### Vector Similarity Search

#### Advanced Search Implementation
```python
class SearchClient:
    def __init__(self, api_client):
        self.api_client = api_client

    def vector_search(self, query: str, **kwargs) -> Dict[str, Any]:
        """Perform vector similarity search"""
        search_request = {
            "query": query,
            "embedding_model": kwargs.get("embedding_model", "text-embedding-ada-002"),
            "top_k": kwargs.get("top_k", 10),
            "filters": kwargs.get("filters", {}),
            "include_metadata": kwargs.get("include_metadata", True),
            "threshold": kwargs.get("threshold", 0.7)
        }

        response = requests.post(
            f"{self.api_client.base_url}/api/v1/search/vector",
            headers=self.api_client.get_headers(),
            json=search_request
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Vector search failed: {response.text}")

    def hybrid_search(self, query: str, **kwargs) -> Dict[str, Any]:
        """Perform hybrid search combining vector and text search"""
        search_request = {
            "query": query,
            "search_types": ["vector", "elasticsearch"],
            "weights": {
                "vector": 0.7,
                "elasticsearch": 0.3
            },
            "top_k": kwargs.get("top_k", 20),
            "filters": kwargs.get("filters", {})
        }

        response = requests.post(
            f"{self.api_client.base_url}/api/v1/search/hybrid",
            headers=self.api_client.get_headers(),
            json=search_request
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Hybrid search failed: {response.text}")

# Usage examples
client = PwCAPIClient()
client.authenticate("data_engineer", "SecurePass123!")

search_client = SearchClient(client)

# Vector search for similar content
search_queries = [
    "customer retention strategies",
    "revenue optimization techniques",
    "product recommendation algorithms",
    "market segmentation analysis"
]

for query in search_queries:
    print(f"\nSearching for: {query}")

    # Vector search
    vector_results = search_client.vector_search(
        query=query,
        top_k=5,
        filters={"category": "analytics", "type": "documentation"}
    )

    print(f"Vector search found {len(vector_results['results'])} results:")
    for i, result in enumerate(vector_results['results'][:3]):
        print(f"  {i+1}. {result['title']} (score: {result['score']:.3f})")

    # Hybrid search
    hybrid_results = search_client.hybrid_search(
        query=query,
        top_k=5
    )

    print(f"Hybrid search found {len(hybrid_results['results'])} results:")
    for i, result in enumerate(hybrid_results['results'][:3]):
        print(f"  {i+1}. {result['title']} (combined score: {result['combined_score']:.3f})")
```

---

## Real-time Analytics Examples

### WebSocket Integration

#### Real-time Dashboard Updates
```javascript
class RealTimeAnalytics {
    constructor(baseUrl, accessToken) {
        this.baseUrl = baseUrl.replace('http', 'ws');
        this.accessToken = accessToken;
        this.connections = new Map();
        this.messageHandlers = new Map();
    }

    connect(endpoint, messageHandler) {
        const wsUrl = `${this.baseUrl}${endpoint}?token=${this.accessToken}`;
        const ws = new WebSocket(wsUrl);

        ws.onopen = () => {
            console.log(`Connected to ${endpoint}`);
            this.connections.set(endpoint, ws);
        };

        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                if (messageHandler) {
                    messageHandler(data);
                }

                // Store the handler for later use
                this.messageHandlers.set(endpoint, messageHandler);
            } catch (error) {
                console.error('Error parsing WebSocket message:', error);
            }
        };

        ws.onclose = () => {
            console.log(`Disconnected from ${endpoint}`);
            this.connections.delete(endpoint);

            // Attempt to reconnect after 5 seconds
            setTimeout(() => {
                console.log(`Attempting to reconnect to ${endpoint}`);
                this.connect(endpoint, this.messageHandlers.get(endpoint));
            }, 5000);
        };

        ws.onerror = (error) => {
            console.error(`WebSocket error on ${endpoint}:`, error);
        };

        return ws;
    }

    disconnect(endpoint) {
        const ws = this.connections.get(endpoint);
        if (ws) {
            ws.close();
            this.connections.delete(endpoint);
            this.messageHandlers.delete(endpoint);
        }
    }

    disconnectAll() {
        for (const [endpoint, ws] of this.connections) {
            ws.close();
        }
        this.connections.clear();
        this.messageHandlers.clear();
    }
}

// Usage example
async function setupRealTimeDashboard() {
    // Authenticate first
    const client = new PwCAPIClient();
    await client.authenticate("data_engineer", "SecurePass123!");

    // Setup real-time connections
    const realTime = new RealTimeAnalytics("ws://localhost:8000", client.accessToken);

    // Sales updates
    realTime.connect("/ws/sales/updates", (data) => {
        console.log("Sales update received:", data);
        updateSalesChart(data);
    });

    // Security alerts
    realTime.connect("/ws/security/alerts", (data) => {
        console.log("Security alert:", data);
        showSecurityAlert(data);
    });

    // System health updates
    realTime.connect("/ws/system/health", (data) => {
        console.log("System health update:", data);
        updateHealthIndicators(data);
    });
}

function updateSalesChart(salesData) {
    // Update sales chart with real-time data
    const chart = document.getElementById('sales-chart');
    if (chart && chart.chart) {
        chart.chart.data.datasets[0].data.push({
            x: new Date(salesData.timestamp),
            y: salesData.revenue
        });
        chart.chart.update('none'); // No animation for real-time updates
    }
}

function showSecurityAlert(alertData) {
    // Display security alert
    const alertContainer = document.getElementById('security-alerts');
    const alertElement = document.createElement('div');
    alertElement.className = `alert alert-${alertData.severity}`;
    alertElement.innerHTML = `
        <strong>Security Alert:</strong> ${alertData.message}
        <br><small>Time: ${new Date(alertData.timestamp).toLocaleString()}</small>
    `;
    alertContainer.prepend(alertElement);

    // Auto-remove after 30 seconds
    setTimeout(() => {
        alertElement.remove();
    }, 30000);
}

function updateHealthIndicators(healthData) {
    // Update system health indicators
    const indicators = document.querySelectorAll('.health-indicator');
    indicators.forEach(indicator => {
        const service = indicator.dataset.service;
        if (healthData.services && healthData.services[service]) {
            const status = healthData.services[service].status;
            indicator.className = `health-indicator status-${status}`;
            indicator.textContent = status.toUpperCase();
        }
    });
}

// Initialize real-time dashboard
setupRealTimeDashboard().catch(console.error);
```

---

## Error Handling Examples

### Comprehensive Error Handling

#### Python Error Handling
```python
import time
import logging
from typing import Optional, Dict, Any
from requests.exceptions import RequestException, Timeout, ConnectionError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class APIError(Exception):
    """Custom API error class"""
    def __init__(self, message: str, status_code: int = None, correlation_id: str = None):
        self.message = message
        self.status_code = status_code
        self.correlation_id = correlation_id
        super().__init__(self.message)

class RobustAPIClient:
    def __init__(self, base_url: str, max_retries: int = 3, retry_delay: float = 1.0):
        self.base_url = base_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.access_token = None

    def make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make API request with comprehensive error handling and retries"""
        url = f"{self.base_url}{endpoint}"

        for attempt in range(self.max_retries + 1):
            try:
                # Prepare headers
                headers = kwargs.get('headers', {})
                if self.access_token:
                    headers['Authorization'] = f"Bearer {self.access_token}"

                # Add correlation ID for tracking
                import uuid
                correlation_id = str(uuid.uuid4())
                headers['X-Correlation-ID'] = correlation_id

                kwargs['headers'] = headers
                kwargs['timeout'] = kwargs.get('timeout', 30)

                logger.info(f"Making {method} request to {endpoint} (attempt {attempt + 1})")

                response = requests.request(method, url, **kwargs)

                # Handle different response status codes
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 401:
                    # Token expired or invalid
                    logger.warning("Authentication token expired, attempting refresh")
                    if self.refresh_token():
                        continue  # Retry with new token
                    else:
                        raise APIError("Authentication failed", 401, correlation_id)
                elif response.status_code == 429:
                    # Rate limited
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited, waiting {retry_after} seconds")
                    time.sleep(retry_after)
                    continue
                elif response.status_code >= 500:
                    # Server error - retry
                    logger.warning(f"Server error ({response.status_code}), retrying...")
                    if attempt < self.max_retries:
                        time.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                        continue
                    else:
                        raise APIError(f"Server error after {self.max_retries} retries",
                                     response.status_code, correlation_id)
                else:
                    # Client error - don't retry
                    error_detail = "Unknown error"
                    try:
                        error_response = response.json()
                        error_detail = error_response.get('error', {}).get('message', error_detail)
                    except:
                        error_detail = response.text

                    raise APIError(f"Request failed: {error_detail}",
                                 response.status_code, correlation_id)

            except (ConnectionError, Timeout) as e:
                logger.warning(f"Network error: {e}")
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay * (2 ** attempt))
                    continue
                else:
                    raise APIError(f"Network error after {self.max_retries} retries: {e}")

            except RequestException as e:
                logger.error(f"Request exception: {e}")
                raise APIError(f"Request failed: {e}")

        raise APIError("Maximum retries exceeded")

    def get_sales_data(self, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get sales data with error handling"""
        try:
            params = filters or {}
            response = self.make_request('GET', '/api/v1/sales', params=params)

            # Validate response structure
            if 'items' not in response:
                raise APIError("Invalid response format: missing 'items' field")

            logger.info(f"Successfully retrieved {len(response['items'])} sales records")
            return response

        except APIError as e:
            logger.error(f"Sales data request failed: {e.message}")
            if e.correlation_id:
                logger.error(f"Correlation ID: {e.correlation_id}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_sales_data: {e}")
            raise APIError(f"Unexpected error: {e}")

# Usage example with error handling
def main():
    client = RobustAPIClient("http://localhost:8000")

    try:
        # Authenticate
        auth_response = client.make_request('POST', '/api/v1/auth/login', json={
            "username": "data_engineer",
            "password": "SecurePass123!"
        })
        client.access_token = auth_response.get('access_token')

        # Get sales data
        sales_data = client.get_sales_data({
            "country": "United Kingdom",
            "page": 1,
            "size": 100
        })

        print(f"Successfully retrieved {len(sales_data['items'])} sales records")

    except APIError as e:
        print(f"API Error: {e.message}")
        if e.status_code:
            print(f"Status Code: {e.status_code}")
        if e.correlation_id:
            print(f"Correlation ID: {e.correlation_id}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
```

---

## Performance Optimization Examples

### Caching and Pagination

#### Efficient Data Retrieval
```python
import asyncio
import aiohttp
from typing import AsyncGenerator, List, Dict, Any
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class CacheEntry:
    data: Any
    timestamp: datetime
    ttl: int

    def is_expired(self) -> bool:
        return datetime.now() > (self.timestamp + timedelta(seconds=self.ttl))

class OptimizedAPIClient:
    def __init__(self, base_url: str, cache_ttl: int = 300):
        self.base_url = base_url
        self.cache = {}
        self.cache_ttl = cache_ttl
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def _get_cache_key(self, method: str, endpoint: str, params: Dict = None) -> str:
        """Generate cache key for request"""
        params_str = "&".join(f"{k}={v}" for k, v in sorted((params or {}).items()))
        return f"{method}:{endpoint}:{params_str}"

    def _get_from_cache(self, cache_key: str) -> Any:
        """Get data from cache if valid"""
        if cache_key in self.cache:
            entry = self.cache[cache_key]
            if not entry.is_expired():
                return entry.data
            else:
                del self.cache[cache_key]
        return None

    def _set_cache(self, cache_key: str, data: Any, ttl: int = None):
        """Store data in cache"""
        self.cache[cache_key] = CacheEntry(
            data=data,
            timestamp=datetime.now(),
            ttl=ttl or self.cache_ttl
        )

    async def make_request(self, method: str, endpoint: str, use_cache: bool = True, **kwargs) -> Dict[str, Any]:
        """Make async API request with caching"""
        cache_key = self._get_cache_key(method, endpoint, kwargs.get('params'))

        # Try cache first for GET requests
        if method == 'GET' and use_cache:
            cached_data = self._get_from_cache(cache_key)
            if cached_data:
                print(f"Cache hit for {endpoint}")
                return cached_data

        # Make actual request
        url = f"{self.base_url}{endpoint}"
        async with self.session.request(method, url, **kwargs) as response:
            if response.status == 200:
                data = await response.json()

                # Cache GET requests
                if method == 'GET' and use_cache:
                    self._set_cache(cache_key, data)
                    print(f"Cached response for {endpoint}")

                return data
            else:
                error_text = await response.text()
                raise Exception(f"Request failed ({response.status}): {error_text}")

    async def paginate_all(self, endpoint: str, params: Dict = None, page_size: int = 100) -> AsyncGenerator[Dict[str, Any], None]:
        """Efficiently paginate through all results"""
        page = 1
        total_retrieved = 0

        base_params = params or {}
        base_params['size'] = page_size

        while True:
            base_params['page'] = page

            response = await self.make_request('GET', endpoint, params=base_params.copy())
            items = response.get('items', [])

            if not items:
                break

            for item in items:
                yield item
                total_retrieved += 1

            # Check if we've retrieved all available records
            meta = response.get('meta', {})
            total_available = meta.get('total', 0)
            has_next = meta.get('has_next', False)

            print(f"Retrieved page {page}, {len(items)} items ({total_retrieved}/{total_available} total)")

            if not has_next or total_retrieved >= total_available:
                break

            page += 1

    async def batch_request(self, requests: List[Dict[str, Any]], max_concurrent: int = 10) -> List[Dict[str, Any]]:
        """Make multiple requests concurrently with rate limiting"""
        semaphore = asyncio.Semaphore(max_concurrent)

        async def limited_request(request_config):
            async with semaphore:
                return await self.make_request(**request_config)

        tasks = [limited_request(req) for req in requests]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and log them
        successful_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Request {i} failed: {result}")
            else:
                successful_results.append(result)

        return successful_results

# Usage example
async def optimize_data_retrieval():
    async with OptimizedAPIClient("http://localhost:8000") as client:
        # Authenticate
        auth_response = await client.make_request('POST', '/api/v1/auth/login',
                                                use_cache=False, json={
            "username": "data_engineer",
            "password": "SecurePass123!"
        })

        # Set authorization header for subsequent requests
        headers = {"Authorization": f"Bearer {auth_response['access_token']}"}
        client.session.headers.update(headers)

        # Example 1: Efficient pagination
        print("Retrieving all sales data efficiently...")
        sales_count = 0
        async for sale in client.paginate_all('/api/v1/sales',
                                            params={"country": "United Kingdom"}):
            sales_count += 1
            if sales_count % 1000 == 0:
                print(f"Processed {sales_count} sales records...")

        print(f"Total sales processed: {sales_count}")

        # Example 2: Concurrent requests for analytics
        analytics_requests = [
            {"method": "GET", "endpoint": "/api/v1/analytics/revenue-summary"},
            {"method": "GET", "endpoint": "/api/v1/analytics/customer-segments"},
            {"method": "GET", "endpoint": "/api/v1/analytics/product-performance"},
            {"method": "GET", "endpoint": "/api/v1/analytics/geographic-analysis"}
        ]

        print("\nMaking concurrent analytics requests...")
        analytics_results = await client.batch_request(analytics_requests, max_concurrent=4)

        for i, result in enumerate(analytics_results):
            print(f"Analytics request {i+1}: {len(str(result))} characters of data")

        # Example 3: Cached requests
        print("\nTesting cache performance...")

        # First request (cache miss)
        start_time = asyncio.get_event_loop().time()
        response1 = await client.make_request('GET', '/api/v1/sales',
                                            params={"page": 1, "size": 50})
        first_request_time = asyncio.get_event_loop().time() - start_time

        # Second request (cache hit)
        start_time = asyncio.get_event_loop().time()
        response2 = await client.make_request('GET', '/api/v1/sales',
                                            params={"page": 1, "size": 50})
        second_request_time = asyncio.get_event_loop().time() - start_time

        print(f"First request time: {first_request_time:.3f}s")
        print(f"Second request time (cached): {second_request_time:.3f}s")
        print(f"Cache speedup: {first_request_time / second_request_time:.1f}x")

# Run the optimization example
asyncio.run(optimize_data_retrieval())
```

---

This comprehensive examples guide demonstrates advanced integration patterns, error handling, performance optimization, and real-time capabilities of the PwC Enterprise Data Engineering API. Each example includes production-ready code with proper error handling, logging, and performance considerations.

For additional examples and updates, visit our [developer documentation](https://docs.pwc-data.com) or contact our support team at api-support@pwc-data.com.
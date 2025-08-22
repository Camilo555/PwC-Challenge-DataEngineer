# Distributed Tracing with OpenTelemetry

## Overview

The application includes a comprehensive distributed tracing system built on OpenTelemetry, providing end-to-end observability across all components of the retail analytics pipeline.

## Features

### 🔍 **Core Tracing Capabilities**
- **OpenTelemetry Integration**: Full OTEL compatibility with industry standards
- **Correlation ID Propagation**: Automatic correlation across service boundaries  
- **Multi-Backend Support**: Jaeger, OTLP, Zipkin, Console, and more
- **Automatic Instrumentation**: Zero-code instrumentation for popular libraries
- **Performance Monitoring**: Request latency, throughput, and error tracking

### 🎯 **Supported Components**
- **FastAPI Applications**: Automatic request/response tracing
- **Database Operations**: SQLAlchemy, PostgreSQL, SQLite instrumentation
- **HTTP Clients**: Requests, HTTPX, urllib instrumentation  
- **Cache Operations**: Redis instrumentation
- **ETL Pipelines**: Custom tracing for data processing workflows
- **Background Tasks**: Async task and job tracing

### 📊 **Observability Features**
- **Distributed Traces**: End-to-end request flow visualization
- **Span Attributes**: Rich metadata and context information
- **Error Tracking**: Exception capture and error rate monitoring
- **Performance Metrics**: Latency percentiles and SLA monitoring
- **Service Maps**: Automatic service dependency discovery

## Quick Start

### Environment Configuration

Set these environment variables to configure tracing:

```bash
# Enable tracing
export OTEL_ENABLED=true
export OTEL_SERVICE_NAME=retail-etl-pipeline
export OTEL_SERVICE_VERSION=1.0.0
export ENVIRONMENT=development

# Backend configuration  
export OTEL_BACKEND=jaeger
export OTEL_EXPORTER_ENDPOINT=http://localhost:14268/api/traces

# Sampling (1.0 = 100%, 0.1 = 10%)
export OTEL_SAMPLING_RATE=1.0

# Auto-instrumentation
export OTEL_AUTO_INSTRUMENT=true
```

### Application Integration

The tracing system initializes automatically during application startup:

```python
from core.startup import startup_application

# This will configure tracing automatically
await startup_application()
```

### Manual Tracing

Use the tracing decorators for custom instrumentation:

```python
from core.tracing import trace_function, trace_async_function, create_span

# Synchronous function tracing
@trace_function(name="process_data", kind="internal")
def process_data(data):
    return transform(data)

# Asynchronous function tracing  
@trace_async_function(name="fetch_data", kind="client")
async def fetch_data(url):
    return await http_client.get(url)

# Manual span creation
with create_span("custom_operation", attributes={"type": "etl"}) as span:
    result = do_work()
    span.set_attribute("result.count", len(result))
```

## Configuration

### TracingConfig Options

```python
from core.tracing import TracingConfig, TracingBackend

config = TracingConfig(
    enabled=True,
    service_name="retail-etl-pipeline",
    service_version="1.0.0",
    environment="production",
    
    # Backend selection
    backend=TracingBackend.JAEGER,
    endpoint="http://jaeger:14268/api/traces",
    
    # Sampling configuration
    sampling_rate=0.1,  # 10% in production
    
    # Export settings
    export_timeout_seconds=30,
    export_max_batch_size=512,
    
    # Instrumentation
    auto_instrument=True,
    instrument_db=True,
    instrument_http=True,
    
    # Resource attributes
    resource_attributes={
        "deployment.environment": "production",
        "service.namespace": "retail-analytics"
    }
)
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_ENABLED` | `true` | Enable/disable tracing |
| `OTEL_SERVICE_NAME` | `retail-etl-pipeline` | Service name |
| `OTEL_SERVICE_VERSION` | `1.0.0` | Service version |
| `OTEL_BACKEND` | `console` | Backend (console, jaeger, otlp) |
| `OTEL_EXPORTER_ENDPOINT` | - | Backend endpoint URL |
| `OTEL_SAMPLING_RATE` | `1.0` | Sampling rate (0.0-1.0) |
| `OTEL_AUTO_INSTRUMENT` | `true` | Auto-instrument libraries |
| `OTEL_PROPAGATORS` | `tracecontext,baggage,b3,jaeger` | Propagation formats |

## Deployment Scenarios

### Development Environment

```bash
# Console output for development
export OTEL_ENABLED=true
export OTEL_BACKEND=console
export OTEL_SAMPLING_RATE=1.0

python -m api.main
```

### Production with Jaeger

```bash
# Jaeger backend
export OTEL_ENABLED=true
export OTEL_BACKEND=jaeger
export OTEL_EXPORTER_ENDPOINT=http://jaeger:14268/api/traces
export OTEL_SAMPLING_RATE=0.1

python -m api.main
```

### Production with OTLP

```bash
# OTLP backend (e.g., for Grafana, DataDog)
export OTEL_ENABLED=true
export OTEL_BACKEND=otlp
export OTEL_EXPORTER_ENDPOINT=http://otel-collector:4317
export OTEL_SAMPLING_RATE=0.1

python -m api.main
```

### Docker Deployment

```dockerfile
FROM python:3.10-slim

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application
COPY src/ /app/src/
WORKDIR /app

# Configure tracing
ENV OTEL_ENABLED=true
ENV OTEL_SERVICE_NAME=retail-etl-api
ENV OTEL_BACKEND=jaeger
ENV OTEL_EXPORTER_ENDPOINT=http://jaeger:14268/api/traces

# Start application
CMD ["python", "-m", "api.main"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: retail-etl-api
spec:
  template:
    spec:
      containers:
      - name: api
        image: retail-etl:latest
        env:
        - name: OTEL_ENABLED
          value: "true"
        - name: OTEL_SERVICE_NAME
          value: "retail-etl-api"
        - name: OTEL_BACKEND
          value: "otlp"
        - name: OTEL_EXPORTER_ENDPOINT
          value: "http://jaeger-collector:14268/api/traces"
        - name: OTEL_SAMPLING_RATE
          value: "0.1"
        - name: OTEL_RESOURCE_ATTRIBUTES
          value: "deployment.environment=production,k8s.cluster.name=retail-cluster"
```

## Backend Configuration

### Jaeger

```bash
# Run Jaeger with Docker
docker run -d --name jaeger \
  -p 16686:16686 \
  -p 14268:14268 \
  jaegertracing/all-in-one:latest
```

Access Jaeger UI at: http://localhost:16686

### OTLP with OpenTelemetry Collector

```yaml
# otel-collector-config.yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:

exporters:
  jaeger:
    endpoint: jaeger:14250
    tls:
      insecure: true

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [jaeger]
```

## API Reference

### Core Tracing Functions

```python
from core.tracing import (
    get_tracer,
    create_span,
    set_span_attribute,
    set_span_status,
    trace_function,
    trace_async_function
)

# Get tracer instance
tracer = get_tracer("my-service")

# Create spans
with create_span("operation_name") as span:
    # Your code here
    set_span_attribute("key", "value")
    set_span_status("ok")

# Function decorators
@trace_function("my_function")
def sync_function():
    pass

@trace_async_function("my_async_function")
async def async_function():
    pass
```

### Correlation Context

```python
from core.tracing import correlation_context, get_correlation_id

# Create correlation context
with correlation_context(
    correlation_id="123e4567-e89b-12d3-a456-426614174000",
    user_id="user123",
    operation="data_processing"
) as ctx:
    # All operations within this context will be correlated
    process_data()

# Get current correlation ID
correlation_id = get_correlation_id()
```

### FastAPI Integration

```python
from fastapi import FastAPI
from core.tracing.fastapi_integration import setup_tracing_middleware

app = FastAPI()

# Setup tracing middleware
setup_tracing_middleware(
    app,
    service_name="my-api",
    include_request_body=True,
    exclude_paths=["/health", "/metrics"]
)
```

### ETL Tracing

```python
from core.tracing import trace_etl_operation

@trace_etl_operation("bronze_ingestion", stage="bronze")
def ingest_bronze_data(data_source):
    # ETL operation
    return processed_data

@trace_etl_operation("silver_transformation", stage="silver")  
async def transform_silver_data(bronze_data):
    # Async ETL operation
    return transformed_data
```

## Monitoring and Alerting

### Key Metrics to Monitor

1. **Trace Volume**: Number of traces per minute
2. **Error Rate**: Percentage of traces with errors
3. **Latency Percentiles**: P50, P95, P99 response times
4. **Service Dependencies**: Service-to-service call patterns
5. **Sampling Rate**: Actual vs configured sampling

### Alerting Examples

```yaml
# Grafana Alert Rules
groups:
- name: tracing-alerts
  rules:
  - alert: HighErrorRate
    expr: rate(traces_total{status="error"}[5m]) / rate(traces_total[5m]) > 0.05
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High error rate in traces"
      
  - alert: HighLatency
    expr: histogram_quantile(0.95, rate(duration_bucket[5m])) > 1000
    for: 5m
    labels:
      severity: critical
    annotations:
      summary: "High P95 latency detected"
```

### Service Map Queries

```promql
# Service dependency graph
sum(rate(traces_total[5m])) by (service_name, target_service)

# Error rate by service
sum(rate(traces_total{status="error"}[5m])) by (service_name)

# Average latency by endpoint
avg(duration) by (http_route, service_name)
```

## Troubleshooting

### Common Issues

1. **No traces appearing**
   ```bash
   # Check configuration
   export OTEL_ENABLED=true
   export OTEL_BACKEND=console
   
   # Verify endpoint connectivity
   curl -v http://jaeger:14268/api/traces
   ```

2. **Missing spans**
   ```python
   # Ensure auto-instrumentation is enabled
   export OTEL_AUTO_INSTRUMENT=true
   
   # Check instrumentation status
   from core.tracing import get_instrumentation_status
   print(get_instrumentation_status())
   ```

3. **High overhead**
   ```bash
   # Reduce sampling rate
   export OTEL_SAMPLING_RATE=0.1
   
   # Disable unnecessary instrumentation
   export OTEL_INSTRUMENT_LOGGING=false
   ```

### Debug Mode

Enable debug logging for tracing issues:

```bash
export LOG_LEVEL=DEBUG
export OTEL_LOG_LEVEL=DEBUG

python -m api.main
```

### Health Check

```python
from core.tracing.startup import get_tracing_health

# Get tracing system status
health = get_tracing_health()
print(f"Tracing enabled: {health['tracing_enabled']}")
print(f"Instrumented libraries: {health.get('current_instrumentation', {}).get('instrumented_libraries', [])}")
```

## Best Practices

### 1. Sampling Strategy
- **Development**: 100% sampling for complete visibility
- **Staging**: 50% sampling for testing
- **Production**: 1-10% sampling to reduce overhead

### 2. Span Attributes
```python
# Good attributes
span.set_attribute("user.id", user_id)
span.set_attribute("operation.type", "database_query")
span.set_attribute("db.table", "sales_transactions")
span.set_attribute("records.count", record_count)

# Avoid high-cardinality attributes
# span.set_attribute("request.body", large_json)  # BAD
# span.set_attribute("timestamp", datetime.now())  # BAD
```

### 3. Error Handling
```python
try:
    result = risky_operation()
    set_span_status("ok")
except Exception as e:
    tracer.record_exception(e)
    set_span_status("error", str(e))
    raise
```

### 4. Performance Considerations
- Use async spans for I/O operations
- Limit span attribute size (< 1KB)
- Avoid creating spans in tight loops
- Use sampling to control overhead

### 5. Correlation Best Practices
- Always propagate correlation IDs across service calls
- Include correlation IDs in all log messages
- Use consistent header names across services
- Validate correlation ID format

## Integration Examples

### Database Operations
```python
from core.tracing import trace_database_operation

@trace_database_operation("insert", "sales_transactions")
async def insert_sales_data(sales_data):
    async with get_database_session() as session:
        await session.execute(insert_query, sales_data)
        await session.commit()
```

### External API Calls
```python
@trace_function("external_api_call", kind="client")
async def call_external_api(url, data):
    # Correlation headers are automatically propagated
    response = await http_client.post(url, json=data)
    set_span_attribute("http.status_code", response.status_code)
    return response.json()
```

### Background Jobs
```python
from core.tracing import correlation_context

async def process_background_job(job_data):
    # Create new correlation context for background job
    with correlation_context(
        operation="background_job",
        job_id=job_data["id"]
    ):
        await process_job(job_data)
```

This distributed tracing system provides comprehensive observability for the retail analytics pipeline, enabling effective debugging, performance monitoring, and system understanding across all components.
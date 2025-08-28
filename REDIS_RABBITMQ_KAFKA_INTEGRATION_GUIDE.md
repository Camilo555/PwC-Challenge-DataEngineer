# Redis, RabbitMQ & Kafka Integration Guide

## Enterprise Infrastructure Integration

This guide covers the comprehensive integration of Redis (caching), RabbitMQ (messaging), and Kafka (streaming) into the PwC Challenge DataEngineer platform.

## Architecture Overview

### Hybrid Messaging Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│      Redis      │    │    RabbitMQ     │    │     Kafka       │
│   (Caching)     │    │  (Messaging)    │    │  (Streaming)    │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • Model Cache   │    │ • Commands      │    │ • Events        │
│ • Session Data  │    │ • Notifications │    │ • Metrics       │
│ • Query Results │    │ • Job Queue     │    │ • Audit Logs    │
│ • Feature Store │    │ • Alerts        │    │ • Real-time     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### When to Use Each Technology

- **Redis**: Fast data access, session storage, ML model caching
- **RabbitMQ**: Reliable command processing, job queues, notifications
- **Kafka**: High-throughput event streaming, audit logs, analytics

## Implementation Examples

### Redis Caching

```python
from src.core.caching.redis_enhanced import get_cache_manager

# Initialize cache manager
cache_manager = get_cache_manager()

# Cache ML predictions
await cache_manager.ml_models.cache_prediction(
    model_id="sales_model_v1",
    features={"user_id": 123, "product_id": 456},
    prediction=0.85,
    ttl=1800
)

# Retrieve cached prediction
cached_result = await cache_manager.ml_models.get_prediction(
    model_id="sales_model_v1", 
    features={"user_id": 123, "product_id": 456}
)
```

### RabbitMQ Messaging

```python
from src.messaging.enterprise_rabbitmq import get_rabbitmq_manager

# Initialize messaging
rabbitmq = get_rabbitmq_manager()

# Send ML training job
await rabbitmq.ml_handler.submit_training_job({
    "model_type": "classification",
    "dataset": "customer_data",
    "hyperparameters": {"n_estimators": 100}
})

# Send alert notification
await rabbitmq.reliability.send_alert({
    "severity": "high",
    "message": "Model drift detected",
    "model_id": "sales_model_v1"
})
```

### Kafka Streaming

```python
from src.streaming.enhanced_kafka import EnhancedKafkaManager

# Initialize Kafka
kafka_manager = EnhancedKafkaManager()

# Stream ML events
await kafka_manager.event_sourcing.publish_event({
    "event_type": "model_training_completed",
    "model_id": "sales_model_v1",
    "accuracy": 0.92,
    "timestamp": datetime.utcnow()
})

# Process streaming events
async for event in kafka_manager.stream_processor.process_events("ml_events"):
    if event["event_type"] == "prediction_request":
        # Handle prediction with caching
        result = await handle_prediction_with_cache(event)
```

## Monitoring and Health Checks

### System Health Monitoring

```python
from src.infrastructure.monitoring.health_monitor import InfrastructureHealthMonitor

# Initialize monitoring
health_monitor = InfrastructureHealthMonitor()

# Check overall health
health_status = await health_monitor.get_overall_health()

# Individual component checks
redis_health = await health_monitor.check_redis_health()
rabbitmq_health = await health_monitor.check_rabbitmq_health() 
kafka_health = await health_monitor.check_kafka_health()
```

### API Endpoints

```bash
# System health
GET /api/v1/infrastructure/health

# Component-specific health
GET /api/v1/infrastructure/redis/health
GET /api/v1/infrastructure/rabbitmq/health
GET /api/v1/infrastructure/kafka/health

# Performance metrics
GET /api/v1/infrastructure/metrics
```

## Configuration

### Docker Compose Setup

```yaml
version: '3.8'
services:
  redis-cluster:
    image: redis:7-alpine
    ports: ["6379:6379"]
    
  rabbitmq:
    image: rabbitmq:3-management
    ports: ["5672:5672", "15672:15672"]
    environment:
      RABBITMQ_DEFAULT_USER: admin
      RABBITMQ_DEFAULT_PASS: password
      
  kafka:
    image: confluentinc/cp-kafka:7.4.0
    ports: ["9092:9092"]
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
```

### Environment Configuration

```bash
# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=your_password
REDIS_CLUSTER_NODES=redis-1:6379,redis-2:6379,redis-3:6379

# RabbitMQ Configuration  
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=admin
RABBITMQ_PASSWORD=password
RABBITMQ_VIRTUAL_HOST=/

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SCHEMA_REGISTRY_URL=http://localhost:8081
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
```

## Performance Optimization

### Redis Optimization
- Use appropriate data structures (hash vs string)
- Set optimal TTL values for different data types
- Monitor memory usage and implement eviction policies
- Use Redis clustering for horizontal scaling

### RabbitMQ Optimization
- Configure appropriate queue types (classic vs quorum)
- Use message persistence only when necessary
- Optimize prefetch settings for consumers
- Monitor queue depths and consumer performance

### Kafka Optimization
- Use appropriate partitioning strategies
- Configure batch sizes and compression
- Tune consumer group settings
- Monitor consumer lag and rebalancing

## Troubleshooting

### Common Issues

**Redis Connection Issues:**
```bash
# Check Redis connectivity
redis-cli ping

# Monitor Redis logs
docker logs redis-container

# Check memory usage
redis-cli info memory
```

**RabbitMQ Queue Problems:**
```bash
# Check queue status
rabbitmqctl list_queues

# Monitor connections
rabbitmqctl list_connections

# Check cluster status
rabbitmqctl cluster_status
```

**Kafka Consumer Lag:**
```bash
# Check consumer groups
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Check lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group group-name
```

## Migration Guide

### Step 1: Infrastructure Setup
1. Deploy Redis cluster
2. Setup RabbitMQ with required exchanges
3. Configure Kafka topics and partitions

### Step 2: Application Updates
1. Update components to use new caching layer
2. Integrate RabbitMQ messaging patterns
3. Enable Kafka event streaming

### Step 3: Testing and Validation
1. Run integration tests
2. Perform load testing
3. Validate monitoring and alerting

### Step 4: Production Deployment
1. Rolling deployment of updated components
2. Monitor system performance
3. Validate data consistency

## Best Practices

### Caching Strategy
- Cache frequently accessed data with appropriate TTLs
- Use cache-aside pattern for read-heavy workloads
- Implement cache warming for critical data
- Monitor cache hit ratios and optimize accordingly

### Messaging Patterns
- Use RabbitMQ for command/response patterns
- Use Kafka for high-volume event streaming
- Implement proper error handling and retries
- Use dead letter queues for failed messages

### Monitoring
- Set up comprehensive health checks
- Monitor key performance metrics
- Implement automated alerting
- Use distributed tracing for troubleshooting

## Security Considerations

- Enable TLS encryption for all connections
- Implement proper authentication and authorization
- Use network security groups and firewalls
- Regular security updates and patching
- Monitor for security anomalies

## Compliance

- Implement audit logging for all operations
- Ensure data encryption at rest and in transit
- Maintain data lineage and governance
- Regular compliance audits and reporting

---

This integration provides enterprise-grade caching, messaging, and streaming capabilities that significantly improve the performance, reliability, and scalability of the data platform.
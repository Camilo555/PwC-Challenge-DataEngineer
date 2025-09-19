"""
Comprehensive Logging Examples
=============================

Examples demonstrating the advanced logging capabilities of the PwC
Data Engineering platform including structured logging, security features,
performance monitoring, and business metrics extraction.
"""

import asyncio
import logging
import time
import uuid
from datetime import datetime
from typing import Dict, Any

# Import the comprehensive logging system
from src.core.logging import (
    initialize_enterprise_logging,
    JSONFormatter,
    ColoredFormatter,
    StructuredFormatter,
    PerformanceFilter,
    SecurityFilter,
    EnvironmentFilter,
    DatabaseLogHandler,
    SecurityLogHandler,
    MetricsLogHandler
)


def example_basic_structured_logging():
    """Example of basic structured logging setup."""
    print("\n=== Basic Structured Logging Example ===")

    # Initialize enterprise logging
    config = initialize_enterprise_logging(
        environment='development',
        log_level='INFO',
        log_format='colored',
        enable_performance_monitoring=True,
        enable_security_logging=True,
        enable_metrics_collection=True
    )

    # Get logger
    logger = logging.getLogger('example.basic')

    # Basic logging with structured data
    logger.info("Application started", extra={
        'service': 'data-platform',
        'version': '1.0.0',
        'environment': 'development'
    })

    # Log with correlation ID
    correlation_id = str(uuid.uuid4())
    logger.info("Processing request", extra={
        'correlation_id': correlation_id,
        'user_id': 'user123',
        'operation': 'data_query'
    })

    # Log with performance metrics
    start_time = time.time()
    time.sleep(0.1)  # Simulate work
    duration = (time.time() - start_time) * 1000

    logger.info("Request completed", extra={
        'correlation_id': correlation_id,
        'duration_ms': duration,
        'status': 'success',
        'records_processed': 1500
    })


def example_advanced_formatting():
    """Example of advanced formatting capabilities."""
    print("\n=== Advanced Formatting Example ===")

    # Create logger with different formatters
    logger = logging.getLogger('example.formatting')
    logger.setLevel(logging.DEBUG)

    # Remove existing handlers
    logger.handlers.clear()

    # Add console handler with colored formatter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColoredFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    logger.addHandler(console_handler)

    # Add file handler with JSON formatter
    file_handler = logging.FileHandler('logs/formatted_example.log')
    file_handler.setFormatter(JSONFormatter(
        include_timestamp=True,
        include_process_info=True,
        include_thread_info=True,
        redact_sensitive=True
    ))
    logger.addHandler(file_handler)

    # Log various types of messages
    logger.debug("Debug message with sensitive data", extra={
        'user_id': 'user123',
        'password': 'secret123',  # This will be redacted
        'api_key': 'sk-abcd1234',  # This will be redacted
        'credit_card': '4111-1111-1111-1111'  # This will be redacted
    })

    logger.info("Info message with performance data", extra={
        'operation': 'database_query',
        'duration_ms': 45.2,
        'memory_usage': 128.5,
        'cpu_percent': 15.3
    })

    logger.warning("Warning with business context", extra={
        'event_type': 'low_inventory',
        'product_id': 'PROD-123',
        'current_stock': 5,
        'threshold': 10
    })

    # Log exception with full context
    try:
        raise ValueError("Example error for demonstration")
    except Exception as e:
        logger.error("Error occurred during processing", extra={
            'error_type': type(e).__name__,
            'operation': 'data_validation',
            'input_file': 'data/input.csv'
        }, exc_info=True)


def example_security_logging():
    """Example of security-aware logging."""
    print("\n=== Security Logging Example ===")

    # Create security logger
    security_logger = logging.getLogger('security.example')
    security_logger.setLevel(logging.INFO)
    security_logger.handlers.clear()

    # Add security handler
    security_handler = SecurityLogHandler(
        security_log_path='logs/security_example.log',
        audit_log_path='logs/audit_example.log',
        compliance_mode=True
    )
    security_handler.setLevel(logging.INFO)
    security_logger.addHandler(security_handler)

    # Add security filter
    security_filter = SecurityFilter(
        compliance_mode=True
    )
    security_handler.addFilter(security_filter)

    # Log authentication events
    security_logger.info("User login attempt", extra={
        'user_id': 'user123',
        'client_ip': '192.168.1.100',
        'user_agent': 'Mozilla/5.0...',
        'session_id': str(uuid.uuid4()),
        'authentication': 'success'
    })

    security_logger.warning("Failed login attempt", extra={
        'user_id': 'unknown',
        'client_ip': '10.0.0.50',
        'failed_reason': 'invalid_password',
        'authentication': 'failed'
    })

    # Log authorization events
    security_logger.info("Resource access granted", extra={
        'user_id': 'user123',
        'resource': '/api/v1/sensitive-data',
        'action': 'READ',
        'authorization': 'granted',
        'role': 'data_analyst'
    })

    security_logger.error("Unauthorized access attempt", extra={
        'user_id': 'user456',
        'resource': '/api/v1/admin/users',
        'action': 'DELETE',
        'authorization': 'denied',
        'reason': 'insufficient_privileges'
    })

    # Log suspicious activity
    security_logger.critical("Potential security incident", extra={
        'incident_type': 'brute_force_attack',
        'client_ip': '192.168.1.200',
        'attempts_count': 50,
        'time_window': '5 minutes',
        'action_taken': 'ip_blocked'
    })


def example_performance_monitoring():
    """Example of performance monitoring and metrics."""
    print("\n=== Performance Monitoring Example ===")

    # Create performance logger
    perf_logger = logging.getLogger('performance.example')
    perf_logger.setLevel(logging.INFO)
    perf_logger.handlers.clear()

    # Add console handler with performance filter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(StructuredFormatter())

    perf_filter = PerformanceFilter(
        slow_threshold=1.0,  # 1 second
        very_slow_threshold=5.0,  # 5 seconds
        track_memory=True,
        track_cpu=True
    )
    console_handler.addFilter(perf_filter)
    perf_logger.addHandler(console_handler)

    # Simulate various operations with different performance characteristics
    operations = [
        ('fast_operation', 0.05),
        ('normal_operation', 0.5),
        ('slow_operation', 1.5),
        ('very_slow_operation', 6.0)
    ]

    for operation_name, duration in operations:
        # Start operation
        request_id = str(uuid.uuid4())
        perf_logger.info(f"Starting {operation_name}", extra={
            'request_id': request_id,
            'request_start': True,
            'operation': operation_name
        })

        # Simulate work
        time.sleep(duration)

        # End operation
        perf_logger.info(f"Completed {operation_name}", extra={
            'request_id': request_id,
            'operation': operation_name,
            'records_processed': int(1000 / duration),  # Simulate throughput
            'success': True
        })

    # Get performance statistics
    stats = perf_filter.get_performance_stats()
    print(f"\nPerformance Statistics: {stats}")


def example_business_metrics():
    """Example of business metrics extraction from logs."""
    print("\n=== Business Metrics Example ===")

    # Create business logger
    business_logger = logging.getLogger('business.example')
    business_logger.setLevel(logging.INFO)
    business_logger.handlers.clear()

    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColoredFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    business_logger.addHandler(console_handler)

    # Add business metrics filter
    from src.core.logging.filters import BusinessMetricsFilter
    metrics_filter = BusinessMetricsFilter()
    console_handler.addFilter(metrics_filter)

    # Log business events
    business_logger.info("User registered successfully", extra={
        'user_id': 'user789',
        'email': 'user@example.com',
        'subscription_tier': 'premium',
        'referral_code': 'REF123'
    })

    business_logger.info("Payment processed successfully", extra={
        'payment_id': 'pay_123456',
        'user_id': 'user789',
        'amount': 99.99,
        'currency': 'USD',
        'payment_method': 'credit_card'
    })

    business_logger.info("Order completed with revenue $2,450.00", extra={
        'order_id': 'ORD-789',
        'customer_id': 'CUST-456',
        'items_count': 5,
        'shipping_cost': 15.00
    })

    business_logger.info("Processed 15,000 transactions with average response time 25ms", extra={
        'batch_id': 'BATCH-001',
        'success_rate': 99.8,
        'error_count': 30
    })

    business_logger.info("Daily active users: 5,420 users", extra={
        'date': datetime.now().strftime('%Y-%m-%d'),
        'platform': 'web',
        'new_users': 120
    })

    # Get business metrics summary
    metrics_summary = metrics_filter.get_business_metrics_summary()
    print(f"\nBusiness Metrics Summary: {metrics_summary}")


async def example_async_logging():
    """Example of logging in async contexts."""
    print("\n=== Async Logging Example ===")

    # Create async logger
    async_logger = logging.getLogger('async.example')

    async def async_operation(operation_id: str, duration: float):
        """Simulate async operation with logging."""
        correlation_id = str(uuid.uuid4())

        async_logger.info("Starting async operation", extra={
            'operation_id': operation_id,
            'correlation_id': correlation_id,
            'start_time': datetime.utcnow().isoformat()
        })

        try:
            # Simulate async work
            await asyncio.sleep(duration)

            # Simulate some processing
            if operation_id == 'op_3':
                raise ValueError("Simulated error in async operation")

            async_logger.info("Async operation completed", extra={
                'operation_id': operation_id,
                'correlation_id': correlation_id,
                'duration_ms': duration * 1000,
                'status': 'success'
            })

        except Exception as e:
            async_logger.error("Async operation failed", extra={
                'operation_id': operation_id,
                'correlation_id': correlation_id,
                'error': str(e),
                'status': 'failed'
            }, exc_info=True)

    # Run multiple async operations concurrently
    tasks = [
        async_operation('op_1', 0.1),
        async_operation('op_2', 0.2),
        async_operation('op_3', 0.3),  # This will fail
        async_operation('op_4', 0.05)
    ]

    await asyncio.gather(*tasks, return_exceptions=True)


def example_database_logging():
    """Example of database logging integration."""
    print("\n=== Database Logging Example ===")

    # Create database logger
    db_logger = logging.getLogger('database.example')
    db_logger.setLevel(logging.INFO)
    db_logger.handlers.clear()

    # Add database handler
    db_handler = DatabaseLogHandler(
        database_url='sqlite:///logs/logging_example.db',
        table_name='log_records',
        max_batch_size=10,
        flush_interval=2.0
    )
    db_logger.addHandler(db_handler)

    # Add console handler for immediate feedback
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColoredFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    db_logger.addHandler(console_handler)

    # Log various events that will be stored in database
    for i in range(15):
        db_logger.info(f"Database operation {i+1}", extra={
            'operation_id': f'db_op_{i+1}',
            'table_name': f'table_{i % 3 + 1}',
            'rows_affected': (i + 1) * 10,
            'execution_time_ms': (i + 1) * 5.5,
            'query_type': ['SELECT', 'INSERT', 'UPDATE', 'DELETE'][i % 4]
        })

        # Add some variety with warnings and errors
        if i % 7 == 0:
            db_logger.warning(f"Slow query detected in operation {i+1}", extra={
                'operation_id': f'db_op_{i+1}',
                'slow_query_threshold': 100,
                'actual_duration': 150.5
            })

        if i % 11 == 0:
            try:
                raise ConnectionError("Database connection timeout")
            except Exception as e:
                db_logger.error(f"Database error in operation {i+1}", extra={
                    'operation_id': f'db_op_{i+1}',
                    'error_code': 'CONNECTION_TIMEOUT'
                }, exc_info=True)

    print("Database logging complete. Check logs/logging_example.db for stored logs.")

    # Wait for batch flush
    time.sleep(3)


def example_correlation_tracking():
    """Example of correlation ID tracking across operations."""
    print("\n=== Correlation Tracking Example ===")

    # Create correlation logger
    corr_logger = logging.getLogger('correlation.example')
    corr_logger.setLevel(logging.INFO)
    corr_logger.handlers.clear()

    # Add console handler with correlation filter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(StructuredFormatter())

    from src.core.logging.filters import CorrelationFilter
    corr_filter = CorrelationFilter()
    console_handler.addFilter(corr_filter)
    corr_logger.addHandler(console_handler)

    def simulate_request_flow(correlation_id: str):
        """Simulate a request flow with correlation tracking."""
        # Set correlation context
        corr_filter.set_correlation_context(correlation_id, {
            'user_id': 'user123',
            'session_id': 'sess_456',
            'request_type': 'data_query'
        })

        # API Gateway
        corr_logger.info("Request received at API Gateway", extra={
            'correlation_id': correlation_id,
            'component': 'api_gateway',
            'endpoint': '/api/v1/data/query'
        })

        # Authentication Service
        corr_logger.info("Authentication check", extra={
            'correlation_id': correlation_id,
            'component': 'auth_service',
            'result': 'authenticated'
        })

        # Business Logic
        corr_logger.info("Processing business logic", extra={
            'correlation_id': correlation_id,
            'component': 'business_service',
            'operation': 'data_aggregation'
        })

        # Database Query
        corr_logger.info("Executing database query", extra={
            'correlation_id': correlation_id,
            'component': 'database',
            'query_type': 'SELECT',
            'table': 'sales_data'
        })

        # Response
        corr_logger.info("Response sent", extra={
            'correlation_id': correlation_id,
            'component': 'api_gateway',
            'status_code': 200,
            'response_size_bytes': 2048
        })

        # Clean up correlation context
        corr_filter.clear_correlation_context(correlation_id)

    # Simulate multiple concurrent requests
    for i in range(3):
        correlation_id = f"req_{uuid.uuid4().hex[:8]}"
        simulate_request_flow(correlation_id)
        print()  # Separator for readability


def run_all_examples():
    """Run all logging examples."""
    print("🚀 PwC Data Platform - Comprehensive Logging Examples")
    print("=" * 60)

    try:
        example_basic_structured_logging()
        example_advanced_formatting()
        example_security_logging()
        example_performance_monitoring()
        example_business_metrics()

        # Run async example
        print("\n=== Running Async Example ===")
        asyncio.run(example_async_logging())

        example_database_logging()
        example_correlation_tracking()

        print("\n✅ All logging examples completed successfully!")
        print("\nCheck the 'logs/' directory for generated log files:")
        print("- logs/formatted_example.log - JSON formatted logs")
        print("- logs/security_example.log - Security events")
        print("- logs/audit_example.log - Audit trail")
        print("- logs/logging_example.db - SQLite database with logs")

    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Ensure logs directory exists
    import os
    os.makedirs('logs', exist_ok=True)

    # Run all examples
    run_all_examples()
"""
RabbitMQ Enterprise Messaging System

This package provides a comprehensive RabbitMQ messaging system for enterprise data platforms
with advanced features including:

- High availability connection management with SSL/TLS support
- Advanced message patterns (pub/sub, RPC, work queues, topic routing)
- ML pipeline and data quality message handlers
- Reliability features (dead letter queues, circuit breakers, deduplication)
- Integration services for ML, user sessions, and real-time analytics
- Comprehensive monitoring, metrics collection, and performance optimization
"""

from .enterprise_rabbitmq_manager import (
    EnterpriseRabbitMQManager,
    EnterpriseMessage,
    MessageMetadata,
    QueueType,
    MessagePriority,
    DeliveryMode,
    ConnectionConfig,
    get_rabbitmq_manager,
    set_rabbitmq_manager
)

from .message_patterns import (
    PublisherSubscriber,
    RequestResponse,
    WorkQueue,
    TopicRouter,
    MessagePatternFactory,
    PatternType,
    RPCRequest,
    RPCResponse,
    get_publisher_subscriber,
    get_request_response,
    get_work_queue,
    get_topic_router
)

from .ml_message_handlers import (
    MLMessageHandler,
    MLTrainingRequest,
    MLTrainingResponse,
    MLInferenceRequest,
    MLInferenceResponse,
    FeaturePipelineEvent,
    MLJobType,
    MLJobStatus,
    ModelType,
    get_ml_message_handler,
    set_ml_message_handler
)

from .data_quality_handlers import (
    DataQualityMessageHandler,
    DataQualityValidationRequest,
    DataQualityValidationResult,
    DataLineageEvent,
    DataGovernanceEvent,
    DataQualityStatus,
    ValidationSeverity,
    DataQualityRule,
    get_dq_message_handler,
    set_dq_message_handler
)

from .reliability_features import (
    MessageReliabilityHandler,
    RetryConfig,
    RetryStrategy,
    CircuitBreakerConfig,
    DeduplicationConfig,
    AdvancedCircuitBreaker,
    PriorityQueueManager,
    get_reliability_handler,
    set_reliability_handler
)

from .integration_services import (
    MLPipelineIntegrationService,
    UserSessionIntegrationService,
    RealtimeDashboardIntegrationService,
    MLPipelineIntegrationEvent,
    UserSessionEvent,
    DashboardUpdateEvent,
    IntegrationEventType,
    get_ml_integration_service,
    get_session_integration_service,
    get_dashboard_integration_service
)

from .monitoring_and_metrics import (
    MetricsCollector,
    PerformanceOptimizer,
    HealthCheckService,
    MetricPoint,
    QueueMetrics,
    SystemHealthCheck,
    PerformanceAlert,
    MetricType,
    AlertSeverity,
    get_metrics_collector,
    get_performance_optimizer,
    get_health_check_service
)

# Version information
__version__ = "1.0.0"
__author__ = "Enterprise Data Platform Team"
__description__ = "Comprehensive RabbitMQ messaging system for enterprise data platforms"

# Export all public APIs
__all__ = [
    # Core manager
    "EnterpriseRabbitMQManager",
    "EnterpriseMessage",
    "MessageMetadata", 
    "QueueType",
    "MessagePriority",
    "DeliveryMode",
    "ConnectionConfig",
    "get_rabbitmq_manager",
    "set_rabbitmq_manager",
    
    # Message patterns
    "PublisherSubscriber",
    "RequestResponse", 
    "WorkQueue",
    "TopicRouter",
    "MessagePatternFactory",
    "PatternType",
    "RPCRequest",
    "RPCResponse",
    "get_publisher_subscriber",
    "get_request_response",
    "get_work_queue",
    "get_topic_router",
    
    # ML message handlers
    "MLMessageHandler",
    "MLTrainingRequest",
    "MLTrainingResponse",
    "MLInferenceRequest", 
    "MLInferenceResponse",
    "FeaturePipelineEvent",
    "MLJobType",
    "MLJobStatus",
    "ModelType",
    "get_ml_message_handler",
    "set_ml_message_handler",
    
    # Data quality handlers
    "DataQualityMessageHandler",
    "DataQualityValidationRequest",
    "DataQualityValidationResult",
    "DataLineageEvent",
    "DataGovernanceEvent", 
    "DataQualityStatus",
    "ValidationSeverity",
    "DataQualityRule",
    "get_dq_message_handler",
    "set_dq_message_handler",
    
    # Reliability features
    "MessageReliabilityHandler",
    "RetryConfig",
    "RetryStrategy",
    "CircuitBreakerConfig",
    "DeduplicationConfig",
    "AdvancedCircuitBreaker",
    "PriorityQueueManager",
    "get_reliability_handler", 
    "set_reliability_handler",
    
    # Integration services
    "MLPipelineIntegrationService",
    "UserSessionIntegrationService",
    "RealtimeDashboardIntegrationService",
    "MLPipelineIntegrationEvent",
    "UserSessionEvent",
    "DashboardUpdateEvent",
    "IntegrationEventType",
    "get_ml_integration_service",
    "get_session_integration_service", 
    "get_dashboard_integration_service",
    
    # Monitoring and metrics
    "MetricsCollector",
    "PerformanceOptimizer",
    "HealthCheckService",
    "MetricPoint",
    "QueueMetrics",
    "SystemHealthCheck",
    "PerformanceAlert",
    "MetricType",
    "AlertSeverity",
    "get_metrics_collector",
    "get_performance_optimizer",
    "get_health_check_service"
]


def initialize_messaging_system(
    connection_config: Optional[ConnectionConfig] = None,
    enable_metrics: bool = True,
    enable_health_checks: bool = True,
    enable_ml_integration: bool = True,
    enable_dq_integration: bool = True,
    enable_session_integration: bool = True,
    enable_dashboard_integration: bool = True
) -> dict:
    """
    Initialize the complete RabbitMQ messaging system with all components.
    
    Args:
        connection_config: RabbitMQ connection configuration
        enable_metrics: Enable metrics collection
        enable_health_checks: Enable health check service
        enable_ml_integration: Enable ML pipeline integration
        enable_dq_integration: Enable data quality integration
        enable_session_integration: Enable user session integration
        enable_dashboard_integration: Enable dashboard integration
        
    Returns:
        Dictionary containing all initialized components
    """
    components = {}
    
    try:
        # Initialize core RabbitMQ manager
        if connection_config:
            rabbitmq_manager = EnterpriseRabbitMQManager(connection_config)
        else:
            rabbitmq_manager = get_rabbitmq_manager()
        
        components["rabbitmq_manager"] = rabbitmq_manager
        
        # Initialize reliability handler
        reliability_handler = get_reliability_handler()
        components["reliability_handler"] = reliability_handler
        
        # Initialize message patterns
        components["message_patterns"] = {
            "publisher_subscriber": get_publisher_subscriber(),
            "request_response": get_request_response(),
            "work_queue": get_work_queue(),
            "topic_router": get_topic_router()
        }
        
        # Initialize ML integration
        if enable_ml_integration:
            ml_handler = get_ml_message_handler()
            ml_integration = get_ml_integration_service()
            components["ml_handler"] = ml_handler
            components["ml_integration"] = ml_integration
            
            # Start ML workers
            ml_handler.start_ml_workers(num_workers=3)
        
        # Initialize data quality integration
        if enable_dq_integration:
            dq_handler = get_dq_message_handler()
            components["dq_handler"] = dq_handler
            
            # Start DQ workers
            dq_handler.start_dq_workers(num_workers=3)
        
        # Initialize session integration
        if enable_session_integration:
            session_integration = get_session_integration_service()
            components["session_integration"] = session_integration
        
        # Initialize dashboard integration
        if enable_dashboard_integration:
            dashboard_integration = get_dashboard_integration_service()
            components["dashboard_integration"] = dashboard_integration
        
        # Initialize metrics collection
        if enable_metrics:
            metrics_collector = get_metrics_collector()
            performance_optimizer = get_performance_optimizer()
            components["metrics_collector"] = metrics_collector
            components["performance_optimizer"] = performance_optimizer
            
            # Start metrics collection
            metrics_collector.start_collection()
        
        # Initialize health checks
        if enable_health_checks:
            health_check_service = get_health_check_service()
            components["health_check_service"] = health_check_service
        
        return {
            "status": "success",
            "message": "RabbitMQ messaging system initialized successfully",
            "components": components,
            "enabled_features": {
                "metrics": enable_metrics,
                "health_checks": enable_health_checks,
                "ml_integration": enable_ml_integration,
                "dq_integration": enable_dq_integration,
                "session_integration": enable_session_integration,
                "dashboard_integration": enable_dashboard_integration
            }
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to initialize messaging system: {str(e)}",
            "components": components
        }


def shutdown_messaging_system():
    """
    Gracefully shutdown the RabbitMQ messaging system.
    """
    try:
        # Stop metrics collection
        metrics_collector = get_metrics_collector()
        metrics_collector.stop_collection()
        
        # Close RabbitMQ connections
        rabbitmq_manager = get_rabbitmq_manager()
        rabbitmq_manager.close()
        
        return {
            "status": "success",
            "message": "Messaging system shutdown successfully"
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": f"Error during shutdown: {str(e)}"
        }


def get_system_status() -> dict:
    """
    Get comprehensive status of the messaging system.
    
    Returns:
        Dictionary containing system status information
    """
    try:
        status_info = {}
        
        # RabbitMQ health
        rabbitmq_manager = get_rabbitmq_manager()
        rabbitmq_health = rabbitmq_manager.health_check()
        status_info["rabbitmq"] = rabbitmq_health
        
        # Metrics collector status
        try:
            metrics_collector = get_metrics_collector()
            metrics_status = metrics_collector.get_current_metrics()
            status_info["metrics"] = metrics_status
        except:
            status_info["metrics"] = {"status": "not_initialized"}
        
        # Performance summary
        try:
            performance_optimizer = get_performance_optimizer()
            performance_summary = performance_optimizer.analyze_performance()
            status_info["performance"] = performance_summary
        except:
            status_info["performance"] = {"status": "not_initialized"}
        
        # Health checks
        try:
            health_service = get_health_check_service()
            health_status = health_service.perform_health_check()
            status_info["health_checks"] = health_status
        except:
            status_info["health_checks"] = {"status": "not_initialized"}
        
        return {
            "status": "success",
            "system_status": status_info,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error getting system status: {str(e)}",
            "timestamp": datetime.utcnow().isoformat()
        }


# Quick start example
def quick_start_example():
    """
    Quick start example demonstrating basic usage
    """
    print("RabbitMQ Enterprise Messaging System - Quick Start Example")
    print("=" * 60)
    
    try:
        # Initialize system
        print("1. Initializing messaging system...")
        init_result = initialize_messaging_system()
        
        if init_result["status"] == "success":
            print("✅ Messaging system initialized successfully")
            
            # Get components
            components = init_result["components"]
            rabbitmq_manager = components["rabbitmq_manager"]
            
            # Test message publishing
            print("\n2. Testing message publishing...")
            test_message = EnterpriseMessage(
                queue_type=QueueType.TASK_QUEUE,
                message_type="test_message",
                payload={
                    "message": "Hello from RabbitMQ Enterprise!",
                    "timestamp": datetime.utcnow().isoformat()
                },
                metadata=MessageMetadata(
                    priority=MessagePriority.NORMAL
                )
            )
            
            success = rabbitmq_manager.publish_message(test_message)
            if success:
                print("✅ Test message published successfully")
            else:
                print("❌ Failed to publish test message")
            
            # Test ML integration
            if "ml_integration" in components:
                print("\n3. Testing ML integration...")
                ml_integration = components["ml_integration"]
                
                pipeline_config = {
                    "model_name": "test_model",
                    "data_preparation": {"enabled": True},
                    "model_training": {"enabled": True, "model_type": "sklearn"}
                }
                
                pipeline_id = ml_integration.start_ml_pipeline("test_pipeline", pipeline_config)
                print(f"✅ ML pipeline started with ID: {pipeline_id}")
            
            # Get system status
            print("\n4. Checking system status...")
            status = get_system_status()
            if status["status"] == "success":
                print("✅ System status check completed")
                rabbitmq_status = status["system_status"]["rabbitmq"]["status"]
                print(f"   RabbitMQ Status: {rabbitmq_status}")
            
            print("\n✅ Quick start example completed successfully!")
            print("\nNext steps:")
            print("- Explore the ML pipeline integration features")
            print("- Set up data quality validation workflows") 
            print("- Configure monitoring and alerting")
            print("- Review performance optimization recommendations")
            
        else:
            print(f"❌ Failed to initialize: {init_result['message']}")
            
    except Exception as e:
        print(f"❌ Error in quick start example: {e}")


if __name__ == "__main__":
    quick_start_example()
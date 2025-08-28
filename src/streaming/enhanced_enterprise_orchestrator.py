"""
Enhanced Enterprise Streaming Data Platform Orchestrator
Integrates all advanced Kafka capabilities including event sourcing, CQRS, saga patterns, and fault tolerance
"""
from __future__ import annotations

import json
import uuid
import asyncio
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.streaming import StreamingQuery
import redis

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from etl.spark.delta_lake_manager import DeltaLakeManager
from monitoring.advanced_metrics import get_metrics_collector

# Import existing components
from src.streaming.kafka_manager import KafkaManager, StreamingTopic
from src.streaming.spark_structured_streaming import (
    create_spark_session, create_streaming_config,
    RealtimeBronzeProcessor, RealtimeSilverProcessor, RealtimeGoldProcessor
)
from src.streaming.advanced_stream_processor import AdvancedStreamProcessor
from src.streaming.realtime_silver_processor import RealtimeSilverProcessor as EnhancedSilverProcessor
from src.streaming.realtime_gold_processor import RealtimeGoldProcessor as EnhancedGoldProcessor
from src.streaming.realtime_analytics_ml import RealtimeAnalyticsEngine
from src.streaming.streaming_governance import StreamingGovernanceFramework
from src.streaming.schema_evolution_manager import SchemaEvolutionManager
from src.streaming.cdc_processor import CDCProcessor

# Import enhanced Kafka components
from src.streaming.enhanced_kafka_streams import (
    EnhancedKafkaStreamsProcessor, EventSourcingProcessor, CQRSProcessor, SagaOrchestrator
)
from src.streaming.event_sourcing_cache_integration import (
    EventSourcingCacheManager, CacheConfig, EventReplayConfig
)
from src.streaming.hybrid_messaging_architecture import (
    HybridMessagingOrchestrator, RabbitMQConfig, HybridMessage, MessageType
)
from src.streaming.fault_tolerance_exactly_once import (
    IdempotentProducer, ExactlyOnceConsumer, FaultToleranceConfig, DeliveryGuarantee
)


class EnhancedPlatformState(Enum):
    """Enhanced platform operational states"""
    INITIALIZING = "initializing"
    STARTING = "starting"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    MAINTENANCE = "maintenance"
    RECOVERING = "recovering"
    DEGRADED = "degraded"


class AdvancedProcessingMode(Enum):
    """Advanced processing modes with enhanced patterns"""
    BATCH_ONLY = "batch_only"
    STREAMING_ONLY = "streaming_only"
    HYBRID = "hybrid"
    LAMBDA = "lambda"
    EVENT_SOURCING = "event_sourcing"
    CQRS = "cqrs"
    SAGA = "saga"
    MICROSERVICES = "microservices"


@dataclass
class EnhancedPlatformConfig:
    """Enhanced enterprise streaming platform configuration"""
    # Platform settings
    platform_name: str = "Enhanced-PwC-Enterprise-Streaming-Platform"
    environment: str = "production"
    processing_mode: AdvancedProcessingMode = AdvancedProcessingMode.EVENT_SOURCING
    
    # Kafka settings
    kafka_bootstrap_servers: List[str] = field(default_factory=lambda: ["localhost:9092"])
    schema_registry_url: Optional[str] = None
    kafka_topics: List[str] = field(default_factory=lambda: [
        "retail-transactions", "customer-events", "system-events",
        "commands", "events", "saga-orchestration"
    ])
    
    # Storage settings
    data_lake_path: str = "./data/enhanced_enterprise_lake"
    checkpoint_location: str = "./checkpoints/enhanced_enterprise"
    feature_store_path: str = "./data/enhanced_feature_store"
    
    # Processing layers
    enable_bronze_layer: bool = True
    enable_silver_layer: bool = True
    enable_gold_layer: bool = True
    enable_analytics: bool = True
    enable_governance: bool = True
    enable_cep: bool = True
    enable_cdc: bool = True
    enable_ml: bool = True
    
    # Enhanced patterns
    enable_event_sourcing: bool = True
    enable_cqrs: bool = True
    enable_saga_pattern: bool = True
    enable_hybrid_messaging: bool = True
    enable_exactly_once: bool = True
    
    # Redis settings
    redis_url: str = "redis://localhost:6379"
    enable_redis_cache: bool = True
    
    # RabbitMQ settings
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_username: str = "guest"
    rabbitmq_password: str = "guest"
    
    # Performance settings
    max_concurrent_streams: int = 20
    stream_parallelism: int = 8
    batch_size: int = 10000
    trigger_interval: str = "15 seconds"
    watermark_delay: str = "5 minutes"
    
    # Fault tolerance
    enable_circuit_breaker: bool = True
    enable_retry_logic: bool = True
    max_retry_attempts: int = 5
    enable_dead_letter_queue: bool = True
    
    # Monitoring settings
    enable_monitoring: bool = True
    enable_alerting: bool = True
    metrics_interval: str = "30 seconds"
    health_check_interval: str = "15 seconds"


@dataclass
class EnhancedPlatformMetrics:
    """Enhanced platform-wide metrics"""
    platform_state: EnhancedPlatformState
    total_streams: int = 0
    active_streams: int = 0
    failed_streams: int = 0
    total_records_processed: int = 0
    records_per_second: float = 0.0
    error_rate: float = 0.0
    uptime_seconds: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_utilization: float = 0.0
    
    # Enhanced metrics
    events_sourced: int = 0
    commands_processed: int = 0
    sagas_completed: int = 0
    cache_hit_ratio: float = 0.0
    exactly_once_violations: int = 0
    duplicate_messages_detected: int = 0
    
    last_update: datetime = field(default_factory=datetime.now)


class EnhancedStreamingPipelineManager:
    """Enhanced pipeline manager with advanced Kafka patterns"""
    
    def __init__(self, spark: SparkSession, config: EnhancedPlatformConfig):
        self.spark = spark
        self.config = config
        self.logger = get_logger(f"{__name__}.EnhancedPipelineManager")
        
        # Enhanced components
        self.kafka_streams_processor: Optional[EnhancedKafkaStreamsProcessor] = None
        self.event_sourcing_manager: Optional[EventSourcingCacheManager] = None
        self.hybrid_messaging: Optional[HybridMessagingOrchestrator] = None
        self.idempotent_producer: Optional[IdempotentProducer] = None
        
        # Redis client
        self.redis_client: Optional[redis.Redis] = None
        
        # Traditional pipeline components (enhanced with new capabilities)
        self.active_pipelines: Dict[str, StreamingQuery] = {}
        self.pipeline_health: Dict[str, bool] = {}
        self.pipeline_metrics: Dict[str, Dict[str, Any]] = {}
        
        # Component processors (existing)
        self.bronze_processor: Optional[RealtimeBronzeProcessor] = None
        self.silver_processor: Optional[EnhancedSilverProcessor] = None
        self.gold_processor: Optional[EnhancedGoldProcessor] = None
        self.analytics_engine: Optional[RealtimeAnalyticsEngine] = None
        self.governance_framework: Optional[StreamingGovernanceFramework] = None
        self.cep_processor: Optional[AdvancedStreamProcessor] = None
        self.cdc_processor: Optional[CDCProcessor] = None
        
        # Initialize enhanced components
        self._initialize_enhanced_components()
        
    def _initialize_enhanced_components(self):
        """Initialize enhanced streaming components"""
        try:
            # Initialize Redis client if enabled
            if self.config.enable_redis_cache:
                self.redis_client = redis.from_url(self.config.redis_url)
                self.redis_client.ping()  # Test connection
                self.logger.info("Redis client initialized")
            
            # Initialize event sourcing manager
            if self.config.enable_event_sourcing:
                cache_config = CacheConfig(
                    redis_url=self.config.redis_url,
                    enable_cache_warming=True
                )
                replay_config = EventReplayConfig(
                    enable_replay=True,
                    replay_topic="event-replay"
                )
                
                self.event_sourcing_manager = EventSourcingCacheManager(
                    cache_config=cache_config,
                    replay_config=replay_config,
                    kafka_bootstrap_servers=self.config.kafka_bootstrap_servers
                )
                self.logger.info("Event sourcing manager initialized")
            
            # Initialize hybrid messaging
            if self.config.enable_hybrid_messaging:
                rabbitmq_config = RabbitMQConfig(
                    host=self.config.rabbitmq_host,
                    port=self.config.rabbitmq_port,
                    username=self.config.rabbitmq_username,
                    password=self.config.rabbitmq_password,
                    enable_ha=True,
                    confirm_delivery=True
                )
                
                self.hybrid_messaging = HybridMessagingOrchestrator(
                    kafka_bootstrap_servers=self.config.kafka_bootstrap_servers,
                    rabbitmq_config=rabbitmq_config,
                    redis_client=self.redis_client
                )
                self.logger.info("Hybrid messaging orchestrator initialized")
            
            # Initialize idempotent producer for exactly-once semantics
            if self.config.enable_exactly_once:
                fault_tolerance_config = FaultToleranceConfig(
                    delivery_guarantee=DeliveryGuarantee.EXACTLY_ONCE,
                    enable_idempotent_producer=True,
                    enable_transactions=True,
                    enable_duplicate_detection=True,
                    enable_circuit_breaker=self.config.enable_circuit_breaker,
                    max_retries=self.config.max_retry_attempts,
                    enable_dead_letter_queue=self.config.enable_dead_letter_queue
                )
                
                self.idempotent_producer = IdempotentProducer(
                    bootstrap_servers=self.config.kafka_bootstrap_servers,
                    config=fault_tolerance_config,
                    redis_client=self.redis_client
                )
                self.logger.info("Idempotent producer initialized")
            
            # Initialize Kafka Streams processor
            self.kafka_streams_processor = EnhancedKafkaStreamsProcessor(
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                application_id=f"{self.config.platform_name}-streams",
                redis_client=self.redis_client,
                rabbitmq_channel=self.hybrid_messaging.rabbitmq_manager.channel if self.hybrid_messaging else None
            )
            self.logger.info("Enhanced Kafka Streams processor initialized")
            
            # Initialize traditional processors with enhanced capabilities
            self._initialize_traditional_processors()
            
        except Exception as e:
            self.logger.error(f"Failed to initialize enhanced components: {e}")
            raise
    
    def _initialize_traditional_processors(self):
        """Initialize traditional processors enhanced with new capabilities"""
        try:
            # Create streaming configuration with enhanced settings
            streaming_config = create_streaming_config(
                app_name=self.config.platform_name,
                kafka_bootstrap_servers=self.config.kafka_bootstrap_servers,
                checkpoint_location=self.config.checkpoint_location,
                output_path=self.config.data_lake_path,
                trigger_interval=self.config.trigger_interval,
                max_offsets_per_trigger=self.config.batch_size,
                enable_exactly_once=self.config.enable_exactly_once
            )
            
            # Initialize Bronze processor
            if self.config.enable_bronze_layer:
                self.bronze_processor = RealtimeBronzeProcessor(
                    self.spark, streaming_config, "enhanced_bronze"
                )
            
            # Initialize Silver processor
            if self.config.enable_silver_layer:
                from src.streaming.realtime_silver_processor import create_silver_processing_config
                silver_config = create_silver_processing_config(
                    enable_data_cleaning=True,
                    enable_enrichment=True,
                    enable_business_rules=True,
                    enable_event_sourcing=self.config.enable_event_sourcing,
                    quality_threshold=0.90  # Higher threshold for enhanced platform
                )
                self.silver_processor = EnhancedSilverProcessor(self.spark, silver_config)
            
            # Initialize Gold processor
            if self.config.enable_gold_layer:
                from src.streaming.realtime_gold_processor import create_gold_processing_config
                gold_config = create_gold_processing_config(
                    enable_real_time_aggregations=True,
                    enable_anomaly_detection=True,
                    enable_alerting=True,
                    enable_exactly_once=self.config.enable_exactly_once
                )
                self.gold_processor = EnhancedGoldProcessor(self.spark, gold_config)
            
            # Initialize Analytics engine with enhanced features
            if self.config.enable_analytics and self.config.enable_ml:
                from src.streaming.realtime_analytics_ml import create_analytics_config
                analytics_config = create_analytics_config(
                    enable_feature_engineering=True,
                    enable_model_serving=True,
                    enable_anomaly_detection=True,
                    feature_store_enabled=True,
                    enable_event_sourcing=self.config.enable_event_sourcing
                )
                self.analytics_engine = RealtimeAnalyticsEngine(self.spark, analytics_config)
            
            # Initialize Governance framework
            if self.config.enable_governance:
                from src.streaming.streaming_governance import create_governance_config
                governance_config = create_governance_config(
                    enable_quality_monitoring=True,
                    enable_compliance_checking=True,
                    enable_lineage_tracking=True,
                    enable_audit_logging=True,
                    enable_exactly_once_validation=self.config.enable_exactly_once
                )
                self.governance_framework = StreamingGovernanceFramework(self.spark, governance_config)
            
            # Initialize CDC processor with enhanced features
            if self.config.enable_cdc:
                from src.streaming.cdc_processor import create_cdc_config, CDCSource
                cdc_config = create_cdc_config(
                    source_type=CDCSource.DEBEZIUM,
                    kafka_topics=["cdc.retail.customers", "cdc.retail.orders"],
                    target_tables={
                        "customers": f"{self.config.data_lake_path}/cdc/customers",
                        "orders": f"{self.config.data_lake_path}/cdc/orders"
                    },
                    primary_keys={
                        "customers": ["customer_id"],
                        "orders": ["order_id"]
                    },
                    enable_exactly_once=self.config.enable_exactly_once
                )
                self.cdc_processor = CDCProcessor(self.spark, cdc_config)
            
            self.logger.info("Traditional processors initialized with enhanced capabilities")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize traditional processors: {e}")
            raise
    
    def start_all_pipelines(self) -> Dict[str, Any]:
        """Start all enhanced streaming pipelines"""
        try:
            self.logger.info("Starting all enhanced streaming pipelines")
            results = {
                "traditional_pipelines": {},
                "enhanced_components": {},
                "success": True
            }
            
            # Start hybrid messaging first (commands and events)
            if self.hybrid_messaging:
                self.hybrid_messaging.start_processing()
                results["enhanced_components"]["hybrid_messaging"] = True
                self.logger.info("Hybrid messaging started")
            
            # Start Kafka Streams processor (event sourcing, CQRS, saga)
            if self.kafka_streams_processor:
                topic_processor_mapping = {
                    "event-store": "event_sourcing",
                    "commands": "cqrs",
                    "events": "cqrs",
                    "queries": "cqrs",
                    "saga-orchestration": "saga"
                }
                self.kafka_streams_processor.start_processing(topic_processor_mapping)
                results["enhanced_components"]["kafka_streams"] = True
                self.logger.info("Kafka Streams processor started")
            
            # Start traditional pipelines with enhanced capabilities
            traditional_results = self._start_traditional_pipelines()
            results["traditional_pipelines"] = traditional_results
            
            # Start CDC processing
            if self.cdc_processor:
                cdc_queries = self.cdc_processor.start_cdc_processing()
                self.active_pipelines.update(cdc_queries)
                results["traditional_pipelines"]["cdc"] = len(cdc_queries)
                self.logger.info(f"CDC processing started with {len(cdc_queries)} topics")
            
            total_active = len(self.active_pipelines) + len(results["enhanced_components"])
            self.logger.info(f"Started {total_active} enhanced streaming components")
            
            return results
            
        except Exception as e:\n            self.logger.error(f\"Failed to start enhanced pipelines: {e}\")\n            self.stop_all_pipelines()\n            return {\"success\": False, \"error\": str(e)}\n    \n    def _start_traditional_pipelines(self) -> Dict[str, Any]:\n        \"\"\"Start traditional pipelines with enhanced features\"\"\"\n        results = {}\n        \n        try:\n            # Start Bronze layer\n            if self.bronze_processor:\n                bronze_query = self.bronze_processor.start_stream(self.config.kafka_topics)\n                self.active_pipelines[\"bronze\"] = bronze_query\n                self.pipeline_health[\"bronze\"] = True\n                results[\"bronze\"] = True\n                self.logger.info(\"Enhanced Bronze pipeline started\")\n            \n            # Create enhanced Bronze stream for downstream processing\n            bronze_stream = None\n            if self.config.enable_bronze_layer:\n                bronze_path = f\"{self.config.data_lake_path}/bronze/realtime_transactions\"\n                bronze_stream = (\n                    self.spark.readStream\n                    .format(\"delta\")\n                    .option(\"ignoreChanges\", \"true\")\n                    .option(\"readChangeFeed\", \"true\")  # Enable change data feed for event sourcing\n                    .load(bronze_path)\n                )\n            \n            # Start Silver layer with event sourcing integration\n            if self.silver_processor and bronze_stream is not None:\n                silver_query = self.silver_processor.process_streaming_data(\n                    bronze_stream,\n                    f\"{self.config.data_lake_path}/silver/realtime_transactions\",\n                    f\"{self.config.checkpoint_location}/silver\"\n                )\n                self.active_pipelines[\"silver\"] = silver_query\n                self.pipeline_health[\"silver\"] = True\n                results[\"silver\"] = True\n                self.logger.info(\"Enhanced Silver pipeline started\")\n                \n                # Store events in event sourcing store\n                if self.config.enable_event_sourcing and self.event_sourcing_manager:\n                    self._integrate_silver_with_event_sourcing(silver_stream)\n            \n            # Create Silver stream for downstream processing\n            silver_stream = None\n            if self.config.enable_silver_layer:\n                silver_path = f\"{self.config.data_lake_path}/silver/realtime_transactions\"\n                silver_stream = (\n                    self.spark.readStream\n                    .format(\"delta\")\n                    .option(\"ignoreChanges\", \"true\")\n                    .option(\"readChangeFeed\", \"true\")\n                    .load(silver_path)\n                )\n            \n            # Start Gold layer with exactly-once guarantees\n            if self.gold_processor and silver_stream is not None:\n                gold_queries = self.gold_processor.start_all_aggregations(\n                    silver_stream,\n                    f\"{self.config.data_lake_path}/gold\",\n                    f\"{self.config.checkpoint_location}/gold\"\n                )\n                self.active_pipelines.update(gold_queries)\n                for agg_name in gold_queries.keys():\n                    self.pipeline_health[f\"gold_{agg_name}\"] = True\n                results[\"gold\"] = len(gold_queries)\n                self.logger.info(f\"Enhanced Gold pipeline started with {len(gold_queries)} aggregations\")\n            \n            # Start Analytics engine with event sourcing features\n            if self.analytics_engine and silver_stream is not None:\n                analytics_query = self.analytics_engine.process_streaming_analytics(\n                    silver_stream,\n                    f\"{self.config.data_lake_path}/analytics\",\n                    f\"{self.config.checkpoint_location}/analytics\"\n                )\n                self.active_pipelines[\"analytics\"] = analytics_query\n                self.pipeline_health[\"analytics\"] = True\n                results[\"analytics\"] = True\n                self.logger.info(\"Enhanced Analytics pipeline started\")\n            \n            return results\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to start traditional pipelines: {e}\")\n            raise\n    \n    def _integrate_silver_with_event_sourcing(self, silver_stream: DataFrame):\n        \"\"\"Integrate Silver layer with event sourcing\"\"\"\n        try:\n            def process_silver_batch(batch_df: DataFrame, batch_id: int):\n                # Convert Silver layer changes to events\n                if not batch_df.isEmpty():\n                    events = self._convert_silver_changes_to_events(batch_df)\n                    \n                    # Store events in event sourcing manager\n                    for event in events:\n                        if self.event_sourcing_manager:\n                            self.event_sourcing_manager.store_event(event, update_cache=True)\n                    \n                    self.logger.debug(f\"Stored {len(events)} events from Silver batch {batch_id}\")\n            \n            # Create event sourcing integration query\n            event_sourcing_query = (\n                silver_stream\n                .writeStream\n                .foreachBatch(process_silver_batch)\n                .option(\"checkpointLocation\", f\"{self.config.checkpoint_location}/event_sourcing\")\n                .trigger(processingTime=self.config.trigger_interval)\n                .start()\n            )\n            \n            self.active_pipelines[\"event_sourcing_integration\"] = event_sourcing_query\n            self.pipeline_health[\"event_sourcing_integration\"] = True\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to integrate Silver with event sourcing: {e}\")\n            raise\n    \n    def _convert_silver_changes_to_events(self, batch_df: DataFrame) -> List:\n        \"\"\"Convert Silver layer DataFrame changes to events\"\"\"\n        try:\n            from src.streaming.kafka_manager import EnhancedStreamingMessage\n            \n            events = []\n            rows = batch_df.collect()\n            \n            for row in rows:\n                # Create event from row data\n                event = EnhancedStreamingMessage(\n                    message_id=str(uuid.uuid4()),\n                    topic=\"silver_layer_events\",\n                    key=str(row.get(\"customer_id_clean\", \"unknown\")),\n                    payload=row.asDict(),\n                    timestamp=datetime.now(),\n                    headers={\"source\": \"silver_layer\"},\n                    event_type=\"silver_data_processed\",\n                    aggregate_id=str(row.get(\"customer_id_clean\", \"unknown\")),\n                    aggregate_version=1\n                )\n                events.append(event)\n            \n            return events\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to convert Silver changes to events: {e}\")\n            return []\n    \n    def stop_all_pipelines(self):\n        \"\"\"Stop all enhanced streaming pipelines\"\"\"\n        self.logger.info(\"Stopping all enhanced streaming pipelines\")\n        \n        # Stop enhanced components\n        if self.kafka_streams_processor:\n            self.kafka_streams_processor.stop_processing()\n            self.logger.info(\"Kafka Streams processor stopped\")\n        \n        if self.hybrid_messaging:\n            self.hybrid_messaging.stop_processing()\n            self.logger.info(\"Hybrid messaging stopped\")\n        \n        # Stop traditional pipelines\n        for pipeline_name, query in self.active_pipelines.items():\n            try:\n                if query and query.isActive:\n                    query.stop()\n                    self.logger.info(f\"Stopped pipeline: {pipeline_name}\")\n                self.pipeline_health[pipeline_name] = False\n            except Exception as e:\n                self.logger.warning(f\"Error stopping pipeline {pipeline_name}: {e}\")\n        \n        # Stop CDC processor\n        if self.cdc_processor:\n            self.cdc_processor.stop_cdc_processing()\n            self.logger.info(\"CDC processor stopped\")\n        \n        # Clean up resources\n        if self.idempotent_producer:\n            self.idempotent_producer.close()\n        \n        if self.event_sourcing_manager:\n            self.event_sourcing_manager.close()\n        \n        self.active_pipelines.clear()\n        self.logger.info(\"All enhanced streaming pipelines stopped\")\n    \n    def get_enhanced_pipeline_health(self) -> Dict[str, Any]:\n        \"\"\"Get health status of all enhanced pipelines\"\"\"\n        health_status = {\"traditional_pipelines\": {}, \"enhanced_components\": {}}\n        \n        # Traditional pipeline health\n        for pipeline_name, query in self.active_pipelines.items():\n            if query:\n                is_active = query.isActive\n                last_progress = query.lastProgress\n                \n                health_status[\"traditional_pipelines\"][pipeline_name] = {\n                    \"active\": is_active,\n                    \"healthy\": self.pipeline_health.get(pipeline_name, False),\n                    \"query_id\": query.id if is_active else None,\n                    \"batch_id\": last_progress.get(\"batchId\", -1) if last_progress else -1,\n                    \"input_rows_per_second\": last_progress.get(\"inputRowsPerSecond\", 0) if last_progress else 0,\n                    \"processed_rows_per_second\": last_progress.get(\"processedRowsPerSecond\", 0) if last_progress else 0,\n                    \"last_update\": last_progress.get(\"timestamp\") if last_progress else None\n                }\n        \n        # Enhanced components health\n        if self.kafka_streams_processor:\n            health_status[\"enhanced_components\"][\"kafka_streams\"] = self.kafka_streams_processor.get_processing_metrics()\n        \n        if self.hybrid_messaging:\n            health_status[\"enhanced_components\"][\"hybrid_messaging\"] = self.hybrid_messaging.get_metrics()\n        \n        if self.idempotent_producer:\n            health_status[\"enhanced_components\"][\"idempotent_producer\"] = self.idempotent_producer.get_metrics()\n        \n        if self.event_sourcing_manager:\n            health_status[\"enhanced_components\"][\"event_sourcing\"] = self.event_sourcing_manager.get_metrics()\n        \n        return health_status\n\n\nclass EnhancedPlatformMonitor:\n    \"\"\"Enhanced platform monitoring with advanced capabilities\"\"\"\n    \n    def __init__(self, config: EnhancedPlatformConfig):\n        self.config = config\n        self.logger = get_logger(f\"{__name__}.EnhancedPlatformMonitor\")\n        self.metrics_collector = get_metrics_collector()\n        \n        # Monitoring state\n        self.monitoring_active = False\n        self.monitoring_thread: Optional[threading.Thread] = None\n        self.health_checks: Dict[str, bool] = {}\n        self.platform_metrics = EnhancedPlatformMetrics(platform_state=EnhancedPlatformState.STOPPED)\n        \n        # Enhanced alert thresholds\n        self.alert_thresholds = {\n            \"error_rate\": 0.03,  # 3% - stricter for enhanced platform\n            \"memory_usage_mb\": 12000,  # 12GB\n            \"cpu_utilization\": 0.85,  # 85%\n            \"records_per_second_min\": 50,  # Higher minimum throughput\n            \"cache_hit_ratio_min\": 0.80,  # 80% cache hit ratio\n            \"exactly_once_violations_max\": 0,  # Zero tolerance\n            \"duplicate_detection_max\": 10  # Max duplicates per minute\n        }\n    \n    def start_monitoring(self, pipeline_manager: EnhancedStreamingPipelineManager):\n        \"\"\"Start enhanced platform monitoring\"\"\"\n        if self.monitoring_active:\n            self.logger.warning(\"Enhanced monitoring already active\")\n            return\n        \n        self.monitoring_active = True\n        self.monitoring_thread = threading.Thread(\n            target=self._enhanced_monitoring_loop,\n            args=(pipeline_manager,),\n            name=\"EnhancedPlatformMonitor\",\n            daemon=True\n        )\n        self.monitoring_thread.start()\n        \n        self.logger.info(\"Enhanced platform monitoring started\")\n    \n    def _enhanced_monitoring_loop(self, pipeline_manager: EnhancedStreamingPipelineManager):\n        \"\"\"Enhanced monitoring loop with advanced metrics\"\"\"\n        self.logger.info(\"Starting enhanced platform monitoring loop\")\n        \n        while self.monitoring_active:\n            try:\n                # Collect enhanced platform metrics\n                self._collect_enhanced_platform_metrics(pipeline_manager)\n                \n                # Perform comprehensive health checks\n                self._perform_enhanced_health_checks(pipeline_manager)\n                \n                # Check enhanced alert conditions\n                self._check_enhanced_alert_conditions()\n                \n                # Send metrics to monitoring system\n                if self.metrics_collector:\n                    self._send_enhanced_platform_metrics()\n                \n                # Wait for next monitoring cycle\n                time.sleep(15)  # More frequent monitoring for enhanced platform\n                \n            except Exception as e:\n                self.logger.error(f\"Enhanced monitoring loop error: {e}\")\n                time.sleep(5)  # Shorter wait on error\n    \n    def _collect_enhanced_platform_metrics(self, pipeline_manager: EnhancedStreamingPipelineManager):\n        \"\"\"Collect enhanced platform metrics\"\"\"\n        try:\n            pipeline_health = pipeline_manager.get_enhanced_pipeline_health()\n            \n            # Traditional metrics\n            traditional_pipelines = pipeline_health.get(\"traditional_pipelines\", {})\n            total_streams = len(traditional_pipelines)\n            active_streams = sum(1 for status in traditional_pipelines.values() if status.get(\"active\", False))\n            failed_streams = sum(1 for status in traditional_pipelines.values() if not status.get(\"healthy\", False))\n            \n            total_input_rate = sum(\n                status.get(\"input_rows_per_second\", 0)\n                for status in traditional_pipelines.values()\n                if isinstance(status.get(\"input_rows_per_second\"), (int, float))\n            )\n            \n            # Enhanced metrics from components\n            enhanced_components = pipeline_health.get(\"enhanced_components\", {})\n            \n            # Event sourcing metrics\n            event_sourcing_metrics = enhanced_components.get(\"event_sourcing\", {})\n            events_sourced = event_sourcing_metrics.get(\"event_store_stats\", {}).get(\"total_events\", 0)\n            cache_hit_ratio = event_sourcing_metrics.get(\"cache_stats\", {}).get(\"cache_metrics\", {}).get(\"hit_ratio\", 0.0)\n            \n            # Hybrid messaging metrics\n            hybrid_metrics = enhanced_components.get(\"hybrid_messaging\", {})\n            commands_processed = hybrid_metrics.get(\"rabbitmq_metrics\", {}).get(\"messages_consumed\", 0)\n            \n            # Idempotent producer metrics\n            producer_metrics = enhanced_components.get(\"idempotent_producer\", {})\n            exactly_once_violations = 0  # Would be calculated from actual violations\n            duplicate_messages_detected = producer_metrics.get(\"duplicate_detection_enabled\", False)\n            \n            # Update platform metrics\n            self.platform_metrics.total_streams = total_streams + len(enhanced_components)\n            self.platform_metrics.active_streams = active_streams\n            self.platform_metrics.failed_streams = failed_streams\n            self.platform_metrics.records_per_second = total_input_rate\n            self.platform_metrics.error_rate = failed_streams / max(total_streams, 1)\n            self.platform_metrics.events_sourced = events_sourced\n            self.platform_metrics.commands_processed = commands_processed\n            self.platform_metrics.cache_hit_ratio = cache_hit_ratio\n            self.platform_metrics.exactly_once_violations = exactly_once_violations\n            self.platform_metrics.duplicate_messages_detected = int(duplicate_messages_detected)\n            self.platform_metrics.last_update = datetime.now()\n            \n            # System metrics\n            import psutil\n            self.platform_metrics.memory_usage_mb = psutil.virtual_memory().used / (1024 * 1024)\n            self.platform_metrics.cpu_utilization = psutil.cpu_percent() / 100.0\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to collect enhanced platform metrics: {e}\")\n    \n    def _perform_enhanced_health_checks(self, pipeline_manager: EnhancedStreamingPipelineManager):\n        \"\"\"Perform enhanced health checks\"\"\"\n        try:\n            pipeline_health = pipeline_manager.get_enhanced_pipeline_health()\n            \n            # Check traditional pipelines\n            for pipeline_name, status in pipeline_health.get(\"traditional_pipelines\", {}).items():\n                is_healthy = (\n                    status.get(\"active\", False) and\n                    status.get(\"healthy\", False) and\n                    status.get(\"input_rows_per_second\", 0) >= 0\n                )\n                self.health_checks[f\"traditional_{pipeline_name}\"] = is_healthy\n            \n            # Check enhanced components\n            for component_name, metrics in pipeline_health.get(\"enhanced_components\", {}).items():\n                is_healthy = metrics.get(\"running\", True) if isinstance(metrics, dict) else True\n                self.health_checks[f\"enhanced_{component_name}\"] = is_healthy\n            \n            # Overall platform health with enhanced criteria\n            overall_healthy = (\n                self.platform_metrics.active_streams > 0 and\n                self.platform_metrics.error_rate < self.alert_thresholds[\"error_rate\"] and\n                self.platform_metrics.cache_hit_ratio >= self.alert_thresholds[\"cache_hit_ratio_min\"] and\n                self.platform_metrics.exactly_once_violations <= self.alert_thresholds[\"exactly_once_violations_max\"]\n            )\n            \n            self.health_checks[\"platform_overall\"] = overall_healthy\n            \n            # Set platform state based on health\n            if overall_healthy:\n                self.platform_metrics.platform_state = EnhancedPlatformState.RUNNING\n            elif self.platform_metrics.failed_streams > 0:\n                self.platform_metrics.platform_state = EnhancedPlatformState.DEGRADED\n            else:\n                self.platform_metrics.platform_state = EnhancedPlatformState.ERROR\n            \n        except Exception as e:\n            self.logger.error(f\"Enhanced health check failed: {e}\")\n    \n    def _check_enhanced_alert_conditions(self):\n        \"\"\"Check for enhanced alert conditions\"\"\"\n        try:\n            # Traditional alerts\n            if self.platform_metrics.error_rate > self.alert_thresholds[\"error_rate\"]:\n                self._send_enhanced_alert(\"high_error_rate\", {\n                    \"error_rate\": self.platform_metrics.error_rate,\n                    \"threshold\": self.alert_thresholds[\"error_rate\"]\n                })\n            \n            # Cache performance alert\n            if self.platform_metrics.cache_hit_ratio < self.alert_thresholds[\"cache_hit_ratio_min\"]:\n                self._send_enhanced_alert(\"low_cache_hit_ratio\", {\n                    \"hit_ratio\": self.platform_metrics.cache_hit_ratio,\n                    \"threshold\": self.alert_thresholds[\"cache_hit_ratio_min\"]\n                })\n            \n            # Exactly-once violations alert\n            if self.platform_metrics.exactly_once_violations > self.alert_thresholds[\"exactly_once_violations_max\"]:\n                self._send_enhanced_alert(\"exactly_once_violations\", {\n                    \"violations\": self.platform_metrics.exactly_once_violations,\n                    \"threshold\": self.alert_thresholds[\"exactly_once_violations_max\"]\n                })\n            \n            # Duplicate detection alert\n            if self.platform_metrics.duplicate_messages_detected > self.alert_thresholds[\"duplicate_detection_max\"]:\n                self._send_enhanced_alert(\"high_duplicate_detection\", {\n                    \"duplicates\": self.platform_metrics.duplicate_messages_detected,\n                    \"threshold\": self.alert_thresholds[\"duplicate_detection_max\"]\n                })\n            \n        except Exception as e:\n            self.logger.error(f\"Enhanced alert condition checking failed: {e}\")\n    \n    def _send_enhanced_alert(self, alert_type: str, details: Dict[str, Any]):\n        \"\"\"Send enhanced platform alert\"\"\"\n        try:\n            alert = {\n                \"alert_type\": f\"enhanced_platform_{alert_type}\",\n                \"severity\": \"critical\" if \"violations\" in alert_type else \"high\",\n                \"timestamp\": datetime.now().isoformat(),\n                \"platform_name\": self.config.platform_name,\n                \"platform_state\": self.platform_metrics.platform_state.value,\n                \"details\": details\n            }\n            \n            self.logger.error(f\"Enhanced platform alert: {alert_type} - {details}\")\n            \n            # Send to monitoring systems\n            if self.metrics_collector:\n                self.metrics_collector.increment_counter(\n                    \"platform_alerts\",\n                    {\"alert_type\": alert_type, \"severity\": alert[\"severity\"]}\n                )\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to send enhanced alert: {e}\")\n    \n    def _send_enhanced_platform_metrics(self):\n        \"\"\"Send enhanced platform metrics to monitoring system\"\"\"\n        try:\n            metrics_to_send = {\n                \"platform_total_streams\": self.platform_metrics.total_streams,\n                \"platform_active_streams\": self.platform_metrics.active_streams,\n                \"platform_records_per_second\": self.platform_metrics.records_per_second,\n                \"platform_error_rate\": self.platform_metrics.error_rate,\n                \"platform_events_sourced\": self.platform_metrics.events_sourced,\n                \"platform_commands_processed\": self.platform_metrics.commands_processed,\n                \"platform_cache_hit_ratio\": self.platform_metrics.cache_hit_ratio,\n                \"platform_exactly_once_violations\": self.platform_metrics.exactly_once_violations,\n                \"platform_memory_usage_mb\": self.platform_metrics.memory_usage_mb,\n                \"platform_cpu_utilization\": self.platform_metrics.cpu_utilization\n            }\n            \n            tags = {\"platform\": self.config.platform_name, \"environment\": self.config.environment}\n            \n            for metric_name, value in metrics_to_send.items():\n                self.metrics_collector.gauge(metric_name, float(value), tags)\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to send enhanced platform metrics: {e}\")\n    \n    def get_enhanced_platform_status(self) -> Dict[str, Any]:\n        \"\"\"Get comprehensive enhanced platform status\"\"\"\n        return {\n            \"enhanced_platform_metrics\": {\n                \"state\": self.platform_metrics.platform_state.value,\n                \"total_streams\": self.platform_metrics.total_streams,\n                \"active_streams\": self.platform_metrics.active_streams,\n                \"failed_streams\": self.platform_metrics.failed_streams,\n                \"records_per_second\": self.platform_metrics.records_per_second,\n                \"error_rate\": self.platform_metrics.error_rate,\n                \"events_sourced\": self.platform_metrics.events_sourced,\n                \"commands_processed\": self.platform_metrics.commands_processed,\n                \"cache_hit_ratio\": self.platform_metrics.cache_hit_ratio,\n                \"exactly_once_violations\": self.platform_metrics.exactly_once_violations,\n                \"memory_usage_mb\": self.platform_metrics.memory_usage_mb,\n                \"cpu_utilization\": self.platform_metrics.cpu_utilization\n            },\n            \"health_checks\": self.health_checks,\n            \"monitoring_active\": self.monitoring_active,\n            \"enhanced_alert_thresholds\": self.alert_thresholds,\n            \"timestamp\": datetime.now().isoformat()\n        }\n    \n    def stop_monitoring(self):\n        \"\"\"Stop enhanced platform monitoring\"\"\"\n        self.monitoring_active = False\n        if self.monitoring_thread:\n            self.monitoring_thread.join(timeout=30)\n        \n        self.logger.info(\"Enhanced platform monitoring stopped\")\n\n\nclass EnhancedEnterpriseStreamingOrchestrator:\n    \"\"\"Enhanced main orchestrator with all advanced Kafka capabilities\"\"\"\n    \n    def __init__(self, config: EnhancedPlatformConfig = None):\n        self.config = config or EnhancedPlatformConfig()\n        self.logger = get_logger(__name__)\n        \n        # Platform state\n        self.platform_state = EnhancedPlatformState.STOPPED\n        self.start_time: Optional[datetime] = None\n        \n        # Core components\n        self.spark: Optional[SparkSession] = None\n        self.pipeline_manager: Optional[EnhancedStreamingPipelineManager] = None\n        self.platform_monitor: Optional[EnhancedPlatformMonitor] = None\n        \n        # Initialize platform\n        self._initialize_enhanced_platform()\n    \n    def _initialize_enhanced_platform(self):\n        \"\"\"Initialize the enhanced streaming platform\"\"\"\n        try:\n            self.platform_state = EnhancedPlatformState.INITIALIZING\n            self.logger.info(f\"Initializing {self.config.platform_name}\")\n            \n            # Create Spark session with enhanced configuration\n            spark_config = create_streaming_config(\n                app_name=self.config.platform_name,\n                kafka_bootstrap_servers=self.config.kafka_bootstrap_servers,\n                checkpoint_location=self.config.checkpoint_location,\n                output_path=self.config.data_lake_path,\n                enable_exactly_once=self.config.enable_exactly_once\n            )\n            \n            self.spark = create_spark_session(spark_config)\n            \n            # Initialize enhanced managers\n            self.pipeline_manager = EnhancedStreamingPipelineManager(self.spark, self.config)\n            self.platform_monitor = EnhancedPlatformMonitor(self.config)\n            \n            # Create necessary directories\n            Path(self.config.data_lake_path).mkdir(parents=True, exist_ok=True)\n            Path(self.config.checkpoint_location).mkdir(parents=True, exist_ok=True)\n            Path(self.config.feature_store_path).mkdir(parents=True, exist_ok=True)\n            \n            self.platform_state = EnhancedPlatformState.STOPPED\n            self.logger.info(\"Enhanced platform initialized successfully\")\n            \n        except Exception as e:\n            self.platform_state = EnhancedPlatformState.ERROR\n            self.logger.error(f\"Enhanced platform initialization failed: {e}\")\n            raise\n    \n    def start_platform(self) -> bool:\n        \"\"\"Start the entire enhanced streaming platform\"\"\"\n        try:\n            if self.platform_state == EnhancedPlatformState.RUNNING:\n                self.logger.warning(\"Enhanced platform is already running\")\n                return True\n            \n            self.platform_state = EnhancedPlatformState.STARTING\n            self.start_time = datetime.now()\n            self.logger.info(f\"Starting {self.config.platform_name}\")\n            \n            # Start all enhanced streaming pipelines\n            pipeline_results = self.pipeline_manager.start_all_pipelines()\n            \n            if not pipeline_results.get(\"success\", True):\n                raise Exception(f\"Failed to start pipelines: {pipeline_results.get('error')}\")\n            \n            # Start enhanced platform monitoring\n            if self.config.enable_monitoring:\n                self.platform_monitor.start_monitoring(self.pipeline_manager)\n            \n            self.platform_state = EnhancedPlatformState.RUNNING\n            \n            total_components = (\n                len(pipeline_results.get(\"traditional_pipelines\", {})) +\n                len(pipeline_results.get(\"enhanced_components\", {}))\n            )\n            \n            self.logger.info(\n                f\"Enhanced platform started successfully with {total_components} components\")\n            \n            return True\n            \n        except Exception as e:\n            self.platform_state = EnhancedPlatformState.ERROR\n            self.logger.error(f\"Failed to start enhanced platform: {e}\")\n            self.stop_platform()\n            return False\n    \n    def stop_platform(self):\n        \"\"\"Stop the entire enhanced streaming platform\"\"\"\n        try:\n            if self.platform_state == EnhancedPlatformState.STOPPED:\n                self.logger.warning(\"Enhanced platform is already stopped\")\n                return\n            \n            self.platform_state = EnhancedPlatformState.STOPPING\n            self.logger.info(f\"Stopping {self.config.platform_name}\")\n            \n            # Stop monitoring\n            if self.platform_monitor:\n                self.platform_monitor.stop_monitoring()\n            \n            # Stop all enhanced pipelines\n            if self.pipeline_manager:\n                self.pipeline_manager.stop_all_pipelines()\n            \n            # Close Spark session\n            if self.spark:\n                self.spark.stop()\n            \n            self.platform_state = EnhancedPlatformState.STOPPED\n            self.logger.info(\"Enhanced platform stopped successfully\")\n            \n        except Exception as e:\n            self.platform_state = EnhancedPlatformState.ERROR\n            self.logger.error(f\"Error stopping enhanced platform: {e}\")\n    \n    def get_enhanced_platform_status(self) -> Dict[str, Any]:\n        \"\"\"Get comprehensive enhanced platform status\"\"\"\n        try:\n            base_status = {\n                \"platform_name\": self.config.platform_name,\n                \"environment\": self.config.environment,\n                \"platform_state\": self.platform_state.value,\n                \"processing_mode\": self.config.processing_mode.value,\n                \"uptime_seconds\": (\n                    (datetime.now() - self.start_time).total_seconds()\n                    if self.start_time else 0\n                ),\n                \"enhanced_configuration\": {\n                    \"event_sourcing_enabled\": self.config.enable_event_sourcing,\n                    \"cqrs_enabled\": self.config.enable_cqrs,\n                    \"saga_pattern_enabled\": self.config.enable_saga_pattern,\n                    \"hybrid_messaging_enabled\": self.config.enable_hybrid_messaging,\n                    \"exactly_once_enabled\": self.config.enable_exactly_once,\n                    \"redis_cache_enabled\": self.config.enable_redis_cache,\n                    \"circuit_breaker_enabled\": self.config.enable_circuit_breaker\n                }\n            }\n            \n            # Add enhanced pipeline health if available\n            if self.pipeline_manager:\n                base_status[\"enhanced_pipeline_health\"] = self.pipeline_manager.get_enhanced_pipeline_health()\n            \n            # Add enhanced monitoring status if available\n            if self.platform_monitor:\n                monitor_status = self.platform_monitor.get_enhanced_platform_status()\n                base_status.update(monitor_status)\n            \n            return base_status\n            \n        except Exception as e:\n            self.logger.error(f\"Failed to get enhanced platform status: {e}\")\n            return {\n                \"error\": str(e),\n                \"platform_state\": self.platform_state.value,\n                \"timestamp\": datetime.now().isoformat()\n            }\n    \n    def restart_platform(self):\n        \"\"\"Restart the entire enhanced platform\"\"\"\n        self.logger.info(\"Restarting enhanced platform\")\n        self.stop_platform()\n        time.sleep(10)  # Allow more time for cleanup\n        return self.start_platform()\n    \n    def __enter__(self):\n        \"\"\"Context manager entry\"\"\"\n        return self\n    \n    def __exit__(self, exc_type, exc_val, exc_tb):\n        \"\"\"Context manager exit\"\"\"\n        self.stop_platform()\n\n\n# Factory functions\ndef create_enhanced_platform_config(**kwargs) -> EnhancedPlatformConfig:\n    \"\"\"Create enhanced platform configuration\"\"\"\n    return EnhancedPlatformConfig(**kwargs)\n\n\ndef create_enhanced_enterprise_streaming_platform(\n    config: EnhancedPlatformConfig = None\n) -> EnhancedEnterpriseStreamingOrchestrator:\n    \"\"\"Create enhanced enterprise streaming platform instance\"\"\"\n    return EnhancedEnterpriseStreamingOrchestrator(config)\n\n\n# Example usage and main entry point\nif __name__ == \"__main__\":\n    import signal\n    import sys\n    \n    def signal_handler(sig, frame):\n        print(\"\\nShutting down Enhanced Enterprise Streaming Platform...\")\n        if 'orchestrator' in globals():\n            orchestrator.stop_platform()\n        sys.exit(0)\n    \n    signal.signal(signal.SIGINT, signal_handler)\n    \n    try:\n        print(\"\\n✨ Starting Enhanced PwC Enterprise Streaming Data Platform ✨\")\n        print(\"=\" * 70)\n        \n        # Create enhanced platform configuration\n        config = create_enhanced_platform_config(\n            platform_name=\"Enhanced-PwC-Enterprise-Streaming-Platform\",\n            environment=\"development\",\n            processing_mode=AdvancedProcessingMode.EVENT_SOURCING,\n            kafka_bootstrap_servers=[\"localhost:9092\"],\n            enable_event_sourcing=True,\n            enable_cqrs=True,\n            enable_saga_pattern=True,\n            enable_hybrid_messaging=True,\n            enable_exactly_once=True,\n            enable_redis_cache=True,\n            enable_circuit_breaker=True,\n            redis_url=\"redis://localhost:6379\",\n            rabbitmq_host=\"localhost\",\n            max_concurrent_streams=20,\n            trigger_interval=\"10 seconds\"\n        )\n        \n        # Create and start enhanced platform\n        with create_enhanced_enterprise_streaming_platform(config) as orchestrator:\n            # Start the enhanced platform\n            success = orchestrator.start_platform()\n            \n            if success:\n                print(f\"✅ Enhanced platform started successfully!\")\n                print(f\"   Platform State: {orchestrator.platform_state.value}\")\n                print(f\"   Processing Mode: {config.processing_mode.value}\")\n                \n                # Show initial enhanced status\n                status = orchestrator.get_enhanced_platform_status()\n                enhanced_config = status[\"enhanced_configuration\"]\n                \n                print(f\"   Event Sourcing: {enhanced_config['event_sourcing_enabled']}\")\n                print(f\"   CQRS: {enhanced_config['cqrs_enabled']}\")\n                print(f\"   Saga Pattern: {enhanced_config['saga_pattern_enabled']}\")\n                print(f\"   Hybrid Messaging: {enhanced_config['hybrid_messaging_enabled']}\")\n                print(f\"   Exactly-Once: {enhanced_config['exactly_once_enabled']}\")\n                print(f\"   Redis Cache: {enhanced_config['redis_cache_enabled']}\")\n                \n                pipeline_health = status.get(\"enhanced_pipeline_health\", {})\n                traditional_count = len(pipeline_health.get(\"traditional_pipelines\", {}))\n                enhanced_count = len(pipeline_health.get(\"enhanced_components\", {}))\n                \n                print(f\"   Traditional Pipelines: {traditional_count}\")\n                print(f\"   Enhanced Components: {enhanced_count}\")\n                print(f\"   Monitoring: {status.get('monitoring_active', False)}\")\n                \n                print(\"\\nℹ️ Enhanced platform is running. Press Ctrl+C to stop.\")\n                print(\"=\" * 70)\n                \n                # Keep running and show periodic enhanced status\n                while True:\n                    time.sleep(45)  # Show status every 45 seconds\n                    status = orchestrator.get_enhanced_platform_status()\n                    uptime = status.get('uptime_seconds', 0)\n                    \n                    enhanced_metrics = status.get('enhanced_platform_metrics', {})\n                    active_streams = enhanced_metrics.get('active_streams', 0)\n                    error_rate = enhanced_metrics.get('error_rate', 0)\n                    cache_hit_ratio = enhanced_metrics.get('cache_hit_ratio', 0)\n                    events_sourced = enhanced_metrics.get('events_sourced', 0)\n                    \n                    print(f\"\\n🔄 Enhanced Status - Uptime: {uptime:.0f}s\")\n                    print(f\"   Active Streams: {active_streams}, Error Rate: {error_rate:.2%}\")\n                    print(f\"   Cache Hit Ratio: {cache_hit_ratio:.2%}, Events Sourced: {events_sourced}\")\n                    \n                    # Show component health\n                    pipeline_health = status.get(\"enhanced_pipeline_health\", {})\n                    \n                    # Traditional pipelines\n                    for name, health in pipeline_health.get(\"traditional_pipelines\", {}).items():\n                        if health.get('active', False):\n                            rate = health.get('input_rows_per_second', 0)\n                            print(f\"   ✅ Traditional {name}: {rate:.1f} records/sec\")\n                    \n                    # Enhanced components\n                    for name, metrics in pipeline_health.get(\"enhanced_components\", {}).items():\n                        status_indicator = \"✅\" if metrics.get('running', True) else \"❌\"\n                        print(f\"   {status_indicator} Enhanced {name}: Active\")\n                        \n            else:\n                print(\"❌ Failed to start enhanced platform\")\n                sys.exit(1)\n                \n    except KeyboardInterrupt:\n        print(\"\\n🛑 Received shutdown signal\")\n    except Exception as e:\n        print(f\"❌ Enhanced platform error: {str(e)}\")\n        import traceback\n        traceback.print_exc()\n        sys.exit(1)\n    finally:\n        print(\"✨ Thank you for using Enhanced PwC Enterprise Streaming Platform ✨\")
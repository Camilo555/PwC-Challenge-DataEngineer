"""
Enterprise Streaming Data Platform Orchestrator
Comprehensive orchestration framework that integrates all streaming components into a unified platform
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

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from etl.spark.delta_lake_manager import DeltaLakeManager
from monitoring.advanced_metrics import get_metrics_collector
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


class PlatformState(Enum):
    """Platform operational states"""
    INITIALIZING = "initializing"
    STARTING = "starting"
    RUNNING = "running"
    PAUSING = "pausing"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
    MAINTENANCE = "maintenance"


class StreamingLayer(Enum):
    """Streaming processing layers"""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    ANALYTICS = "analytics"
    GOVERNANCE = "governance"


class ProcessingMode(Enum):
    """Processing modes"""
    BATCH_ONLY = "batch_only"
    STREAMING_ONLY = "streaming_only"
    HYBRID = "hybrid"
    LAMBDA = "lambda"  # Both batch and streaming for same data


@dataclass
class PlatformConfig:
    """Enterprise streaming platform configuration"""
    # Platform settings
    platform_name: str = "PwC-Enterprise-Streaming-Platform"
    environment: str = "production"
    processing_mode: ProcessingMode = ProcessingMode.HYBRID
    
    # Kafka settings
    kafka_bootstrap_servers: List[str] = field(default_factory=lambda: ["localhost:9092"])
    kafka_topics: List[str] = field(default_factory=lambda: [
        "retail-transactions", "customer-events", "system-events"
    ])
    
    # Storage settings
    data_lake_path: str = "./data/enterprise_lake"
    checkpoint_location: str = "./checkpoints/enterprise"
    feature_store_path: str = "./data/feature_store"
    
    # Processing settings
    enable_bronze_layer: bool = True
    enable_silver_layer: bool = True
    enable_gold_layer: bool = True
    enable_analytics: bool = True
    enable_governance: bool = True
    enable_cep: bool = True
    enable_cdc: bool = True
    enable_ml: bool = True
    
    # Performance settings
    max_concurrent_streams: int = 10
    stream_parallelism: int = 4
    batch_size: int = 10000
    trigger_interval: str = "30 seconds"
    watermark_delay: str = "10 minutes"
    
    # Monitoring settings
    enable_monitoring: bool = True
    enable_alerting: bool = True
    metrics_interval: str = "1 minute"
    health_check_interval: str = "30 seconds"
    
    # Recovery settings
    enable_auto_recovery: bool = True
    max_retry_attempts: int = 3
    retry_delay_seconds: int = 60
    circuit_breaker_threshold: int = 5


@dataclass
class PlatformMetrics:
    """Platform-wide metrics"""
    platform_state: PlatformState
    total_streams: int = 0
    active_streams: int = 0
    failed_streams: int = 0
    total_records_processed: int = 0
    records_per_second: float = 0.0
    error_rate: float = 0.0
    uptime_seconds: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_utilization: float = 0.0
    last_update: datetime = field(default_factory=datetime.now)


class StreamingPipelineManager:
    """Manages individual streaming pipelines"""
    
    def __init__(self, spark: SparkSession, config: PlatformConfig):
        self.spark = spark
        self.config = config
        self.logger = get_logger(f"{__name__}.PipelineManager")
        
        # Pipeline components
        self.active_pipelines: Dict[str, StreamingQuery] = {}
        self.pipeline_health: Dict[str, bool] = {}
        self.pipeline_metrics: Dict[str, Dict[str, Any]] = {}
        
        # Component processors
        self.bronze_processor: Optional[RealtimeBronzeProcessor] = None
        self.silver_processor: Optional[EnhancedSilverProcessor] = None
        self.gold_processor: Optional[EnhancedGoldProcessor] = None
        self.analytics_engine: Optional[RealtimeAnalyticsEngine] = None
        self.governance_framework: Optional[StreamingGovernanceFramework] = None
        self.cep_processor: Optional[AdvancedStreamProcessor] = None
        self.cdc_processor: Optional[CDCProcessor] = None
        
        # Initialize components
        self._initialize_processors()
    
    def _initialize_processors(self):
        """Initialize all streaming processors"""
        try:
            # Create streaming configuration
            streaming_config = create_streaming_config(
                app_name=self.config.platform_name,
                kafka_bootstrap_servers=self.config.kafka_bootstrap_servers,
                checkpoint_location=self.config.checkpoint_location,
                output_path=self.config.data_lake_path,
                trigger_interval=self.config.trigger_interval,
                max_offsets_per_trigger=self.config.batch_size
            )
            
            # Initialize Bronze processor
            if self.config.enable_bronze_layer:
                self.bronze_processor = RealtimeBronzeProcessor(
                    self.spark, streaming_config, "enterprise_bronze"
                )
            
            # Initialize Silver processor
            if self.config.enable_silver_layer:
                from src.streaming.realtime_silver_processor import create_silver_processing_config
                silver_config = create_silver_processing_config(
                    enable_data_cleaning=True,
                    enable_enrichment=True,
                    enable_business_rules=True,
                    quality_threshold=0.85
                )
                self.silver_processor = EnhancedSilverProcessor(self.spark, silver_config)
            
            # Initialize Gold processor
            if self.config.enable_gold_layer:
                from src.streaming.realtime_gold_processor import create_gold_processing_config
                gold_config = create_gold_processing_config(
                    enable_real_time_aggregations=True,
                    enable_anomaly_detection=True,
                    enable_alerting=True
                )
                self.gold_processor = EnhancedGoldProcessor(self.spark, gold_config)
            
            # Initialize Analytics engine
            if self.config.enable_analytics and self.config.enable_ml:
                from src.streaming.realtime_analytics_ml import create_analytics_config
                analytics_config = create_analytics_config(
                    enable_feature_engineering=True,
                    enable_model_serving=True,
                    enable_anomaly_detection=True,
                    feature_store_enabled=True
                )
                self.analytics_engine = RealtimeAnalyticsEngine(self.spark, analytics_config)
            
            # Initialize Governance framework
            if self.config.enable_governance:
                from src.streaming.streaming_governance import create_governance_config
                governance_config = create_governance_config(
                    enable_quality_monitoring=True,
                    enable_compliance_checking=True,
                    enable_lineage_tracking=True,
                    enable_audit_logging=True
                )
                self.governance_framework = StreamingGovernanceFramework(self.spark, governance_config)
            
            # Initialize CEP processor
            if self.config.enable_cep:
                from src.streaming.advanced_stream_processor import create_advanced_stream_processor
                self.cep_processor = create_advanced_stream_processor({
                    "enable_monitoring": True,
                    "batch_size": self.config.batch_size
                })
            
            self.logger.info("All streaming processors initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize processors: {e}")
            raise
    
    def start_all_pipelines(self) -> Dict[str, StreamingQuery]:
        """Start all configured streaming pipelines"""
        try:
            self.logger.info("Starting all streaming pipelines")
            
            # Start Bronze layer
            if self.bronze_processor:
                bronze_query = self.bronze_processor.start_stream(self.config.kafka_topics)
                self.active_pipelines["bronze"] = bronze_query
                self.pipeline_health["bronze"] = True
                self.logger.info("Bronze pipeline started")
            
            # Create Bronze Delta stream for downstream processing
            bronze_stream = None
            if self.config.enable_bronze_layer:
                bronze_path = f"{self.config.data_lake_path}/bronze/realtime_transactions"
                bronze_stream = (
                    self.spark.readStream
                    .format("delta")
                    .option("ignoreChanges", "true")
                    .load(bronze_path)
                )
            
            # Start Silver layer
            if self.silver_processor and bronze_stream is not None:
                silver_query = self.silver_processor.process_streaming_data(
                    bronze_stream,
                    f"{self.config.data_lake_path}/silver/realtime_transactions",
                    f"{self.config.checkpoint_location}/silver"
                )
                self.active_pipelines["silver"] = silver_query
                self.pipeline_health["silver"] = True
                self.logger.info("Silver pipeline started")
            
            # Create Silver stream for downstream processing
            silver_stream = None
            if self.config.enable_silver_layer:
                silver_path = f"{self.config.data_lake_path}/silver/realtime_transactions"
                silver_stream = (
                    self.spark.readStream
                    .format("delta")
                    .option("ignoreChanges", "true")
                    .load(silver_path)
                )
            
            # Start Gold layer
            if self.gold_processor and silver_stream is not None:
                gold_queries = self.gold_processor.start_all_aggregations(
                    silver_stream,
                    f"{self.config.data_lake_path}/gold",
                    f"{self.config.checkpoint_location}/gold"
                )
                self.active_pipelines.update(gold_queries)
                for agg_name in gold_queries.keys():
                    self.pipeline_health[f"gold_{agg_name}"] = True
                self.logger.info(f"Gold pipeline started with {len(gold_queries)} aggregations")
            
            # Start Analytics engine
            if self.analytics_engine and silver_stream is not None:
                analytics_query = self.analytics_engine.process_streaming_analytics(
                    silver_stream,
                    f"{self.config.data_lake_path}/analytics",
                    f"{self.config.checkpoint_location}/analytics"
                )
                self.active_pipelines["analytics"] = analytics_query
                self.pipeline_health["analytics"] = True
                self.logger.info("Analytics pipeline started")
            
            # Start CEP processor
            if self.cep_processor:
                self.cep_processor.initialize()
                self.cep_processor.start_processing(self.config.kafka_topics)
                self.pipeline_health["cep"] = True
                self.logger.info("CEP processor started")
            
            self.logger.info(f"Started {len(self.active_pipelines)} streaming pipelines")
            return self.active_pipelines
            
        except Exception as e:
            self.logger.error(f"Failed to start pipelines: {e}")
            self.stop_all_pipelines()
            raise
    
    def stop_all_pipelines(self):
        """Stop all active streaming pipelines"""
        self.logger.info("Stopping all streaming pipelines")
        
        # Stop Spark streaming queries
        for pipeline_name, query in self.active_pipelines.items():
            try:
                if query and query.isActive:
                    query.stop()
                    self.logger.info(f"Stopped pipeline: {pipeline_name}")
                self.pipeline_health[pipeline_name] = False
            except Exception as e:
                self.logger.warning(f"Error stopping pipeline {pipeline_name}: {e}")
        
        # Stop CEP processor
        if self.cep_processor:
            try:
                self.cep_processor.stop_processing()
                self.pipeline_health["cep"] = False
                self.logger.info("Stopped CEP processor")
            except Exception as e:
                self.logger.warning(f"Error stopping CEP processor: {e}")
        
        self.active_pipelines.clear()
    
    def get_pipeline_health(self) -> Dict[str, Any]:
        """Get health status of all pipelines"""
        health_status = {}
        
        for pipeline_name, query in self.active_pipelines.items():
            if query:
                is_active = query.isActive
                last_progress = query.lastProgress
                
                health_status[pipeline_name] = {
                    "active": is_active,
                    "healthy": self.pipeline_health.get(pipeline_name, False),
                    "query_id": query.id if is_active else None,
                    "batch_id": last_progress.get("batchId", -1) if last_progress else -1,
                    "input_rows_per_second": last_progress.get("inputRowsPerSecond", 0) if last_progress else 0,
                    "processed_rows_per_second": last_progress.get("processedRowsPerSecond", 0) if last_progress else 0,
                    "last_update": last_progress.get("timestamp") if last_progress else None
                }
        
        # Add CEP processor status
        if self.cep_processor:
            cep_stats = self.cep_processor.get_processing_stats()
            health_status["cep"] = {
                "active": cep_stats["running"],
                "healthy": self.pipeline_health.get("cep", False),
                "processed_events": cep_stats["processed_events"],
                "events_per_second": cep_stats["events_per_second"],
                "active_windows": cep_stats["active_windows"]
            }
        
        return health_status


class PlatformMonitor:
    """Platform-wide monitoring and health management"""
    
    def __init__(self, config: PlatformConfig):
        self.config = config
        self.logger = get_logger(f"{__name__}.PlatformMonitor")
        self.metrics_collector = get_metrics_collector()
        
        # Monitoring state
        self.monitoring_active = False
        self.monitoring_thread: Optional[threading.Thread] = None
        self.health_checks: Dict[str, bool] = {}
        self.platform_metrics = PlatformMetrics(platform_state=PlatformState.STOPPED)
        
        # Alert thresholds
        self.alert_thresholds = {
            "error_rate": 0.05,  # 5%
            "memory_usage_mb": 8000,  # 8GB
            "cpu_utilization": 0.8,  # 80%
            "records_per_second_min": 10
        }
    
    def start_monitoring(self, pipeline_manager: StreamingPipelineManager):
        """Start platform monitoring"""
        if self.monitoring_active:
            self.logger.warning("Monitoring already active")
            return
        
        self.monitoring_active = True
        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            args=(pipeline_manager,),
            name="PlatformMonitor",
            daemon=True
        )
        self.monitoring_thread.start()
        
        self.logger.info("Platform monitoring started")
    
    def stop_monitoring(self):
        """Stop platform monitoring"""
        self.monitoring_active = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=30)
        
        self.logger.info("Platform monitoring stopped")
    
    def _monitoring_loop(self, pipeline_manager: StreamingPipelineManager):
        """Main monitoring loop"""
        self.logger.info("Starting platform monitoring loop")
        
        while self.monitoring_active:
            try:
                # Collect platform metrics
                self._collect_platform_metrics(pipeline_manager)
                
                # Perform health checks
                self._perform_health_checks(pipeline_manager)
                
                # Check alert conditions
                self._check_alert_conditions()
                
                # Send metrics to monitoring system
                if self.metrics_collector:
                    self._send_platform_metrics()
                
                # Wait for next monitoring cycle
                time.sleep(30)  # Monitor every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Monitoring loop error: {e}")
                time.sleep(10)  # Shorter wait on error
    
    def _collect_platform_metrics(self, pipeline_manager: StreamingPipelineManager):
        """Collect platform-wide metrics"""
        try:
            pipeline_health = pipeline_manager.get_pipeline_health()
            
            # Calculate metrics
            total_streams = len(pipeline_health)
            active_streams = sum(1 for status in pipeline_health.values() if status.get("active", False))
            failed_streams = sum(1 for status in pipeline_health.values() if not status.get("healthy", False))
            
            # Calculate processing rates
            total_input_rate = sum(
                status.get("input_rows_per_second", 0)
                for status in pipeline_health.values()
                if isinstance(status.get("input_rows_per_second"), (int, float))
            )
            
            # Update platform metrics
            self.platform_metrics.total_streams = total_streams
            self.platform_metrics.active_streams = active_streams
            self.platform_metrics.failed_streams = failed_streams
            self.platform_metrics.records_per_second = total_input_rate
            self.platform_metrics.error_rate = failed_streams / max(total_streams, 1)
            self.platform_metrics.last_update = datetime.now()
            
            # Get system metrics (simplified - in production use proper monitoring)
            import psutil
            self.platform_metrics.memory_usage_mb = psutil.virtual_memory().used / (1024 * 1024)
            self.platform_metrics.cpu_utilization = psutil.cpu_percent() / 100.0
            
        except Exception as e:
            self.logger.error(f"Failed to collect platform metrics: {e}")
    
    def _perform_health_checks(self, pipeline_manager: StreamingPipelineManager):
        """Perform comprehensive health checks"""
        try:
            pipeline_health = pipeline_manager.get_pipeline_health()
            
            # Check individual pipeline health
            for pipeline_name, status in pipeline_health.items():
                is_healthy = (
                    status.get("active", False) and
                    status.get("healthy", False) and
                    status.get("input_rows_per_second", 0) >= 0
                )
                self.health_checks[pipeline_name] = is_healthy
            
            # Check overall platform health
            overall_healthy = (
                self.platform_metrics.active_streams > 0 and
                self.platform_metrics.error_rate < self.alert_thresholds["error_rate"] and
                self.platform_metrics.memory_usage_mb < self.alert_thresholds["memory_usage_mb"] and
                self.platform_metrics.cpu_utilization < self.alert_thresholds["cpu_utilization"]
            )
            
            self.health_checks["platform_overall"] = overall_healthy
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
    
    def _check_alert_conditions(self):
        """Check for alert conditions"""
        try:
            # Check error rate
            if self.platform_metrics.error_rate > self.alert_thresholds["error_rate"]:
                self._send_alert("high_error_rate", {
                    "error_rate": self.platform_metrics.error_rate,
                    "threshold": self.alert_thresholds["error_rate"]
                })
            
            # Check memory usage
            if self.platform_metrics.memory_usage_mb > self.alert_thresholds["memory_usage_mb"]:
                self._send_alert("high_memory_usage", {
                    "memory_usage_mb": self.platform_metrics.memory_usage_mb,
                    "threshold": self.alert_thresholds["memory_usage_mb"]
                })
            
            # Check processing rate
            if self.platform_metrics.records_per_second < self.alert_thresholds["records_per_second_min"]:
                self._send_alert("low_processing_rate", {
                    "records_per_second": self.platform_metrics.records_per_second,
                    "threshold": self.alert_thresholds["records_per_second_min"]
                })
            
        except Exception as e:
            self.logger.error(f"Alert condition checking failed: {e}")
    
    def _send_alert(self, alert_type: str, details: Dict[str, Any]):
        """Send platform alert"""
        try:
            alert = {
                "alert_type": f"platform_{alert_type}",
                "severity": "high",
                "timestamp": datetime.now().isoformat(),
                "platform_name": self.config.platform_name,
                "details": details
            }
            
            self.logger.warning(f"Platform alert: {alert_type} - {details}")
            
            # In production, this would send to alerting systems
            # self.kafka_manager.produce_data_quality_alert(alert)
            
        except Exception as e:
            self.logger.error(f"Failed to send alert: {e}")
    
    def _send_platform_metrics(self):
        """Send platform metrics to monitoring system"""
        try:
            self.metrics_collector.gauge(
                "platform_total_streams", 
                self.platform_metrics.total_streams,
                {"platform": self.config.platform_name}
            )
            self.metrics_collector.gauge(
                "platform_active_streams", 
                self.platform_metrics.active_streams,
                {"platform": self.config.platform_name}
            )
            self.metrics_collector.gauge(
                "platform_records_per_second", 
                self.platform_metrics.records_per_second,
                {"platform": self.config.platform_name}
            )
            self.metrics_collector.gauge(
                "platform_error_rate", 
                self.platform_metrics.error_rate,
                {"platform": self.config.platform_name}
            )
            self.metrics_collector.gauge(
                "platform_memory_usage_mb", 
                self.platform_metrics.memory_usage_mb,
                {"platform": self.config.platform_name}
            )
            self.metrics_collector.gauge(
                "platform_cpu_utilization", 
                self.platform_metrics.cpu_utilization,
                {"platform": self.config.platform_name}
            )
            
        except Exception as e:
            self.logger.error(f"Failed to send platform metrics: {e}")
    
    def get_platform_status(self) -> Dict[str, Any]:
        """Get comprehensive platform status"""
        return {
            "platform_metrics": {
                "state": self.platform_metrics.platform_state.value,
                "total_streams": self.platform_metrics.total_streams,
                "active_streams": self.platform_metrics.active_streams,
                "failed_streams": self.platform_metrics.failed_streams,
                "records_per_second": self.platform_metrics.records_per_second,
                "error_rate": self.platform_metrics.error_rate,
                "memory_usage_mb": self.platform_metrics.memory_usage_mb,
                "cpu_utilization": self.platform_metrics.cpu_utilization
            },
            "health_checks": self.health_checks,
            "monitoring_active": self.monitoring_active,
            "alert_thresholds": self.alert_thresholds,
            "timestamp": datetime.now().isoformat()
        }


class EnterpriseStreamingOrchestrator:
    """Main orchestrator for the enterprise streaming platform"""
    
    def __init__(self, config: PlatformConfig = None):
        self.config = config or PlatformConfig()
        self.logger = get_logger(__name__)
        
        # Platform state
        self.platform_state = PlatformState.STOPPED
        self.start_time: Optional[datetime] = None
        
        # Core components
        self.spark: Optional[SparkSession] = None
        self.kafka_manager: Optional[KafkaManager] = None
        self.delta_manager: Optional[DeltaLakeManager] = None
        self.pipeline_manager: Optional[StreamingPipelineManager] = None
        self.platform_monitor: Optional[PlatformMonitor] = None
        
        # Initialize platform
        self._initialize_platform()
        
    def _initialize_platform(self):
        """Initialize the streaming platform"""
        try:
            self.platform_state = PlatformState.INITIALIZING
            self.logger.info(f"Initializing {self.config.platform_name}")
            
            # Create Spark session
            self.spark = create_spark_session(create_streaming_config(
                app_name=self.config.platform_name,
                kafka_bootstrap_servers=self.config.kafka_bootstrap_servers,
                checkpoint_location=self.config.checkpoint_location,
                output_path=self.config.data_lake_path
            ))
            
            # Initialize managers
            self.kafka_manager = KafkaManager()
            self.delta_manager = DeltaLakeManager(self.spark)
            self.pipeline_manager = StreamingPipelineManager(self.spark, self.config)
            self.platform_monitor = PlatformMonitor(self.config)
            
            # Create necessary directories
            Path(self.config.data_lake_path).mkdir(parents=True, exist_ok=True)
            Path(self.config.checkpoint_location).mkdir(parents=True, exist_ok=True)
            Path(self.config.feature_store_path).mkdir(parents=True, exist_ok=True)
            
            self.platform_state = PlatformState.STOPPED
            self.logger.info("Platform initialized successfully")
            
        except Exception as e:
            self.platform_state = PlatformState.ERROR
            self.logger.error(f"Platform initialization failed: {e}")
            raise
    
    def start_platform(self) -> bool:
        """Start the entire streaming platform"""
        try:
            if self.platform_state == PlatformState.RUNNING:
                self.logger.warning("Platform is already running")
                return True
            
            self.platform_state = PlatformState.STARTING
            self.start_time = datetime.now()
            self.logger.info(f"Starting {self.config.platform_name}")
            
            # Start all streaming pipelines
            active_pipelines = self.pipeline_manager.start_all_pipelines()
            
            # Start platform monitoring
            if self.config.enable_monitoring:
                self.platform_monitor.start_monitoring(self.pipeline_manager)
            
            self.platform_state = PlatformState.RUNNING
            self.logger.info(
                f"Platform started successfully with {len(active_pipelines)} pipelines"
            )
            
            return True
            
        except Exception as e:
            self.platform_state = PlatformState.ERROR
            self.logger.error(f"Failed to start platform: {e}")
            self.stop_platform()
            return False
    
    def stop_platform(self):
        """Stop the entire streaming platform"""
        try:
            if self.platform_state == PlatformState.STOPPED:
                self.logger.warning("Platform is already stopped")
                return
            
            self.platform_state = PlatformState.STOPPING
            self.logger.info(f"Stopping {self.config.platform_name}")
            
            # Stop monitoring
            if self.platform_monitor:
                self.platform_monitor.stop_monitoring()
            
            # Stop all pipelines
            if self.pipeline_manager:
                self.pipeline_manager.stop_all_pipelines()
            
            # Close Spark session
            if self.spark:
                self.spark.stop()
            
            self.platform_state = PlatformState.STOPPED
            self.logger.info("Platform stopped successfully")
            
        except Exception as e:
            self.platform_state = PlatformState.ERROR
            self.logger.error(f"Error stopping platform: {e}")
    
    def get_platform_status(self) -> Dict[str, Any]:
        """Get comprehensive platform status"""
        try:
            base_status = {
                "platform_name": self.config.platform_name,
                "environment": self.config.environment,
                "platform_state": self.platform_state.value,
                "uptime_seconds": (
                    (datetime.now() - self.start_time).total_seconds()
                    if self.start_time else 0
                ),
                "configuration": {
                    "processing_mode": self.config.processing_mode.value,
                    "kafka_topics": self.config.kafka_topics,
                    "max_concurrent_streams": self.config.max_concurrent_streams,
                    "enable_layers": {
                        "bronze": self.config.enable_bronze_layer,
                        "silver": self.config.enable_silver_layer,
                        "gold": self.config.enable_gold_layer,
                        "analytics": self.config.enable_analytics,
                        "governance": self.config.enable_governance
                    }
                }
            }
            
            # Add pipeline health if available
            if self.pipeline_manager:
                base_status["pipeline_health"] = self.pipeline_manager.get_pipeline_health()
            
            # Add monitoring status if available
            if self.platform_monitor:
                monitor_status = self.platform_monitor.get_platform_status()
                base_status.update(monitor_status)
            
            return base_status
            
        except Exception as e:
            self.logger.error(f"Failed to get platform status: {e}")
            return {
                "error": str(e),
                "platform_state": self.platform_state.value,
                "timestamp": datetime.now().isoformat()
            }
    
    def pause_platform(self):
        """Pause platform operations"""
        # Implementation would pause all streams
        self.platform_state = PlatformState.PAUSED
        self.logger.info("Platform paused")
    
    def resume_platform(self):
        """Resume platform operations"""
        # Implementation would resume all streams
        self.platform_state = PlatformState.RUNNING
        self.logger.info("Platform resumed")
    
    def restart_platform(self):
        """Restart the entire platform"""
        self.logger.info("Restarting platform")
        self.stop_platform()
        time.sleep(5)  # Brief pause
        return self.start_platform()
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop_platform()


# Factory functions
def create_platform_config(**kwargs) -> PlatformConfig:
    """Create platform configuration"""
    return PlatformConfig(**kwargs)


def create_enterprise_streaming_platform(
    config: PlatformConfig = None
) -> EnterpriseStreamingOrchestrator:
    """Create enterprise streaming platform instance"""
    return EnterpriseStreamingOrchestrator(config)


# Example usage and main entry point
if __name__ == "__main__":
    import signal
    import sys
    
    def signal_handler(sig, frame):
        print("\nShutting down Enterprise Streaming Platform...")
        if 'orchestrator' in globals():
            orchestrator.stop_platform()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        print("\n✨ Starting PwC Enterprise Streaming Data Platform ✨")
        print("=" * 60)
        
        # Create platform configuration
        config = create_platform_config(
            platform_name="PwC-Enterprise-Streaming-Platform",
            environment="development",
            processing_mode=ProcessingMode.HYBRID,
            kafka_bootstrap_servers=["localhost:9092"],
            enable_bronze_layer=True,
            enable_silver_layer=True,
            enable_gold_layer=True,
            enable_analytics=True,
            enable_governance=True,
            enable_monitoring=True,
            max_concurrent_streams=8,
            trigger_interval="15 seconds"
        )
        
        # Create and start platform
        with create_enterprise_streaming_platform(config) as orchestrator:
            # Start the platform
            success = orchestrator.start_platform()
            
            if success:
                print(f"✅ Platform started successfully!")
                print(f"   Platform State: {orchestrator.platform_state.value}")
                
                # Show initial status
                status = orchestrator.get_platform_status()
                print(f"   Active Streams: {len(status.get('pipeline_health', {}))}")
                print(f"   Configuration: {status['configuration']['processing_mode']}")
                print(f"   Monitoring: {status.get('monitoring_active', False)}")
                
                print("\nℹ️ Platform is running. Press Ctrl+C to stop.")
                print("=" * 60)
                
                # Keep running and show periodic status
                while True:
                    time.sleep(30)
                    status = orchestrator.get_platform_status()
                    uptime = status.get('uptime_seconds', 0)
                    health = status.get('pipeline_health', {})
                    active_count = sum(1 for h in health.values() if h.get('active', False))
                    
                    print(f"\n🔄 Status Update - Uptime: {uptime:.0f}s, Active Pipelines: {active_count}")
                    
                    # Show pipeline details
                    for name, pipeline_health in health.items():
                        if pipeline_health.get('active', False):
                            rate = pipeline_health.get('input_rows_per_second', 0)
                            print(f"   ✅ {name}: {rate:.1f} records/sec")
                        else:
                            print(f"   ❌ {name}: Inactive")
            else:
                print("❌ Failed to start platform")
                sys.exit(1)
                
    except KeyboardInterrupt:
        print("\n🛹 Received shutdown signal")
    except Exception as e:
        print(f"❌ Platform error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        print("✨ Thank you for using PwC Enterprise Streaming Platform ✨")

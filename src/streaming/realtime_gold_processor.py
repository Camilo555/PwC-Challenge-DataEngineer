"""
Advanced Real-time Gold Layer Processing
Provides comprehensive business-ready aggregations, analytics, and real-time insights
"""
from __future__ import annotations

import json
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, expr, lit, struct, when, coalesce, isnull,
    window, session_window, count, countDistinct, sum as spark_sum, avg, 
    max as spark_max, min as spark_min, stddev, variance, percentile_approx,
    first, last, collect_list, collect_set, array_distinct, size,
    year, month, day, hour, minute, dayofweek, date_format, date_trunc,
    lag, lead, rank, dense_rank, row_number, ntile,
    from_unixtime, unix_timestamp, to_timestamp,
    regexp_replace, split, explode, posexplode,
    broadcast, bucketBy
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    StringType, StructField, StructType, TimestampType,
    DoubleType, IntegerType, BooleanType, LongType, ArrayType, 
    MapType, DecimalType
)
from pyspark.sql.window import Window

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from etl.spark.delta_lake_manager import DeltaLakeManager
from monitoring.advanced_metrics import get_metrics_collector
from src.streaming.kafka_manager import KafkaManager, StreamingTopic
from src.streaming.hybrid_messaging_architecture import (
    HybridMessagingArchitecture, RabbitMQManager, RabbitMQConfig,
    HybridMessage, MessageType, MessagePriority
)
from src.streaming.event_sourcing_cache_integration import (
    EventCache, CacheConfig, CacheStrategy, ConsistencyLevel
)


class AggregationType(Enum):
    """Types of aggregations"""
    TUMBLING_WINDOW = "tumbling_window"
    SLIDING_WINDOW = "sliding_window"
    SESSION_WINDOW = "session_window"
    GLOBAL_AGGREGATE = "global_aggregate"
    CUSTOM_WINDOW = "custom_window"


class MetricType(Enum):
    """Types of business metrics"""
    REVENUE = "revenue"
    VOLUME = "volume"
    CUSTOMER = "customer"
    PRODUCT = "product"
    OPERATIONAL = "operational"
    QUALITY = "quality"
    BEHAVIORAL = "behavioral"
    ANOMALY = "anomaly"


class AlertSeverity(Enum):
    """Alert severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class AggregationDefinition:
    """Definition for a streaming aggregation"""
    name: str
    aggregation_type: AggregationType
    metric_type: MetricType
    window_duration: str
    slide_duration: Optional[str] = None
    group_by_columns: List[str] = field(default_factory=list)
    aggregation_expressions: Dict[str, str] = field(default_factory=dict)
    filter_condition: Optional[str] = None
    watermark_column: str = "silver_processed_at"
    watermark_delay: str = "10 minutes"
    output_mode: str = "update"
    trigger_interval: str = "30 seconds"
    enabled: bool = True
    priority: int = 1


@dataclass
class AlertRule:
    """Real-time alert rule definition"""
    rule_id: str
    name: str
    metric_name: str
    threshold_value: float
    comparison_operator: str  # >, <, >=, <=, ==, !=
    severity: AlertSeverity
    cooldown_minutes: int = 5
    enabled: bool = True
    notification_channels: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GoldProcessingConfig:
    """Configuration for Gold layer processing with enhanced infrastructure"""
    enable_real_time_aggregations: bool = True
    enable_anomaly_detection: bool = True
    enable_alerting: bool = True
    enable_ml_features: bool = True
    aggregation_checkpoint_interval: str = "1 minute"
    state_timeout: str = "2 hours"
    max_events_per_trigger: int = 500000
    parallelism: int = 4
    enable_optimization: bool = True
    
    # Enhanced caching configuration
    enable_gold_caching: bool = True
    redis_url: str = "redis://localhost:6379"
    aggregation_cache_ttl: int = 3600  # 1 hour
    alert_cache_ttl: int = 900  # 15 minutes
    metric_cache_ttl: int = 1800  # 30 minutes
    
    # RabbitMQ configuration for processing coordination
    enable_gold_messaging: bool = True
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    
    # CQRS pattern configuration
    enable_cqrs: bool = True
    read_model_cache_ttl: int = 1200  # 20 minutes
    
    # Enhanced Kafka settings for optimized partitioning
    kafka_partitioning_strategy: str = "hash"  # hash, range, custom
    kafka_replication_factor: int = 3
    kafka_min_in_sync_replicas: int = 2


class RealtimeAggregationEngine:
    """Engine for real-time aggregations and analytics with caching"""
    
    def __init__(self, spark: SparkSession, config: GoldProcessingConfig, cache_manager: Optional[EventCache] = None, messaging_manager: Optional[HybridMessagingArchitecture] = None):
        self.spark = spark
        self.config = config
        self.cache_manager = cache_manager
        self.messaging_manager = messaging_manager
        self.logger = get_logger(__name__)
        self.metrics_collector = get_metrics_collector()
        
        # Aggregation definitions registry
        self.aggregations: Dict[str, AggregationDefinition] = {}
        self.alert_rules: Dict[str, AlertRule] = {}
        
        # Load built-in aggregations and alerts
        self._load_builtin_aggregations()
        self._load_builtin_alerts()
        
    def _load_builtin_aggregations(self):
        """Load built-in aggregation definitions"""
        
        # Revenue aggregations
        revenue_aggregations = [
            AggregationDefinition(
                name="hourly_revenue_by_country",
                aggregation_type=AggregationType.TUMBLING_WINDOW,
                metric_type=MetricType.REVENUE,
                window_duration="1 hour",
                group_by_columns=["country_clean", "currency_code"],
                aggregation_expressions={
                    "total_revenue": "sum(line_total)",
                    "transaction_count": "count(*)",
                    "avg_transaction_value": "avg(line_total)",
                    "max_transaction_value": "max(line_total)",
                    "unique_customers": "countDistinct(customer_id_clean)",
                    "unique_products": "countDistinct(stock_code)",
                    "return_count": "sum(case when is_return then 1 else 0 end)",
                    "return_rate": "avg(case when is_return then 1.0 else 0.0 end)"
                },
                filter_condition="silver_quality_score >= 0.8"
            ),
            AggregationDefinition(
                name="minute_revenue_sliding",
                aggregation_type=AggregationType.SLIDING_WINDOW,
                metric_type=MetricType.REVENUE,
                window_duration="5 minutes",
                slide_duration="1 minute",
                group_by_columns=[],
                aggregation_expressions={
                    "total_revenue_5min": "sum(line_total)",
                    "transaction_count_5min": "count(*)",
                    "revenue_per_minute": "sum(line_total) / 5.0",
                    "transactions_per_minute": "count(*) / 5.0"
                }
            )
        ]
        
        # Customer behavior aggregations
        customer_aggregations = [
            AggregationDefinition(
                name="customer_session_analysis",
                aggregation_type=AggregationType.SESSION_WINDOW,
                metric_type=MetricType.CUSTOMER,
                window_duration="30 minutes",  # Session timeout
                group_by_columns=["customer_id_clean"],
                aggregation_expressions={
                    "session_revenue": "sum(line_total)",
                    "session_transaction_count": "count(*)",
                    "session_duration_minutes": "(max(unix_timestamp(silver_processed_at)) - min(unix_timestamp(silver_processed_at))) / 60.0",
                    "unique_products_in_session": "countDistinct(stock_code)",
                    "avg_time_between_transactions": "(max(unix_timestamp(silver_processed_at)) - min(unix_timestamp(silver_processed_at))) / (count(*) - 1)",
                    "session_start_time": "min(silver_processed_at)",
                    "session_end_time": "max(silver_processed_at)"
                },
                filter_condition="customer_id_clean != 'ANONYMOUS' and silver_quality_score >= 0.7",
                watermark_delay="5 minutes"
            ),
            AggregationDefinition(
                name="customer_segment_performance",
                aggregation_type=AggregationType.TUMBLING_WINDOW,
                metric_type=MetricType.CUSTOMER,
                window_duration="15 minutes",
                group_by_columns=["customer_segment", "country_clean"],
                aggregation_expressions={
                    "segment_revenue": "sum(line_total)",
                    "segment_customer_count": "countDistinct(customer_id_clean)",
                    "segment_avg_order_value": "avg(line_total)",
                    "segment_transaction_count": "count(*)",
                    "segment_revenue_per_customer": "sum(line_total) / countDistinct(customer_id_clean)"
                }
            )
        ]
        
        # Product performance aggregations
        product_aggregations = [
            AggregationDefinition(
                name="product_popularity_realtime",
                aggregation_type=AggregationType.SLIDING_WINDOW,
                metric_type=MetricType.PRODUCT,
                window_duration="10 minutes",
                slide_duration="2 minutes",
                group_by_columns=["stock_code", "description_clean"],
                aggregation_expressions={
                    "product_sales_count": "sum(quantity)",
                    "product_revenue": "sum(line_total)",
                    "product_transaction_count": "count(*)",
                    "product_unique_customers": "countDistinct(customer_id_clean)",
                    "product_avg_price": "avg(unit_price)",
                    "product_price_variance": "variance(unit_price)",
                    "product_countries_count": "countDistinct(country_clean)"
                },
                filter_condition="quantity > 0 and unit_price > 0"
            )
        ]
        
        # Operational metrics
        operational_aggregations = [
            AggregationDefinition(
                name="data_quality_monitoring",
                aggregation_type=AggregationType.TUMBLING_WINDOW,
                metric_type=MetricType.QUALITY,
                window_duration="5 minutes",
                group_by_columns=["quality_tier"],
                aggregation_expressions={
                    "record_count": "count(*)",
                    "avg_quality_score": "avg(silver_quality_score)",
                    "min_quality_score": "min(silver_quality_score)",
                    "quality_score_stddev": "stddev(silver_quality_score)",
                    "high_risk_count": "sum(case when risk_score > 0.7 then 1 else 0 end)"
                }
            ),
            AggregationDefinition(
                name="processing_performance_metrics",
                aggregation_type=AggregationType.TUMBLING_WINDOW,
                metric_type=MetricType.OPERATIONAL,
                window_duration="2 minutes",
                group_by_columns=[],
                aggregation_expressions={
                    "total_records_processed": "count(*)",
                    "processing_latency_avg": "avg(unix_timestamp(silver_processed_at) - unix_timestamp(kafka_timestamp))",
                    "processing_latency_max": "max(unix_timestamp(silver_processed_at) - unix_timestamp(kafka_timestamp))",
                    "records_per_second": "count(*) / 120.0",
                    "duplicate_removal_rate": "avg(case when is_duplicate_removed then 1.0 else 0.0 end)"
                }
            )
        ]
        
        # Anomaly detection aggregations
        anomaly_aggregations = [
            AggregationDefinition(
                name="anomaly_detection_metrics",
                aggregation_type=AggregationType.SLIDING_WINDOW,
                metric_type=MetricType.ANOMALY,
                window_duration="30 minutes",
                slide_duration="5 minutes",
                group_by_columns=["country_clean"],
                aggregation_expressions={
                    "revenue_current": "sum(line_total)",
                    "transaction_count_current": "count(*)",
                    "avg_transaction_value_current": "avg(line_total)",
                    "unique_customers_current": "countDistinct(customer_id_clean)",
                    "high_value_transactions": "sum(case when line_total > 1000 then 1 else 0 end)",
                    "return_rate_current": "avg(case when is_return then 1.0 else 0.0 end)",
                    "anonymous_transaction_rate": "avg(case when customer_id_clean = 'ANONYMOUS' then 1.0 else 0.0 end)"
                },
                output_mode="complete"  # For anomaly detection comparisons
            )
        ]
        
        # Register all aggregations
        all_aggregations = (
            revenue_aggregations + 
            customer_aggregations + 
            product_aggregations + 
            operational_aggregations + 
            anomaly_aggregations
        )
        
        for agg in all_aggregations:
            self.aggregations[agg.name] = agg
        
        self.logger.info(f"Loaded {len(all_aggregations)} built-in aggregations")
    
    def _load_builtin_alerts(self):
        """Load built-in alert rules"""
        
        alert_rules = [
            AlertRule(
                rule_id="high_revenue_spike",
                name="High Revenue Spike Alert",
                metric_name="total_revenue",
                threshold_value=10000.0,
                comparison_operator=">",
                severity=AlertSeverity.HIGH,
                cooldown_minutes=10,
                notification_channels=["email", "slack"],
                metadata={"business_impact": "high", "category": "revenue"}
            ),
            AlertRule(
                rule_id="transaction_volume_drop",
                name="Transaction Volume Drop Alert",
                metric_name="transaction_count",
                threshold_value=10.0,
                comparison_operator="<",
                severity=AlertSeverity.MEDIUM,
                cooldown_minutes=15,
                notification_channels=["email"],
                metadata={"business_impact": "medium", "category": "volume"}
            ),
            AlertRule(
                rule_id="high_return_rate",
                name="High Return Rate Alert",
                metric_name="return_rate",
                threshold_value=0.3,
                comparison_operator=">",
                severity=AlertSeverity.HIGH,
                cooldown_minutes=30,
                notification_channels=["email", "pagerduty"],
                metadata={"business_impact": "high", "category": "quality"}
            ),
            AlertRule(
                rule_id="data_quality_degradation",
                name="Data Quality Degradation Alert",
                metric_name="avg_quality_score",
                threshold_value=0.7,
                comparison_operator="<",
                severity=AlertSeverity.CRITICAL,
                cooldown_minutes=5,
                notification_channels=["email", "slack", "pagerduty"],
                metadata={"business_impact": "critical", "category": "quality"}
            ),
            AlertRule(
                rule_id="processing_latency_high",
                name="Processing Latency High Alert",
                metric_name="processing_latency_avg",
                threshold_value=300.0,  # 5 minutes
                comparison_operator=">",
                severity=AlertSeverity.HIGH,
                cooldown_minutes=10,
                notification_channels=["slack"],
                metadata={"business_impact": "medium", "category": "performance"}
            )
        ]
        
        for rule in alert_rules:
            self.alert_rules[rule.rule_id] = rule
        
        self.logger.info(f"Loaded {len(alert_rules)} built-in alert rules")
    
    def create_aggregation_stream(
        self, 
        df: DataFrame, 
        aggregation_name: str,
        use_cache: bool = True
    ) -> DataFrame:
        """Create streaming aggregation based on definition with caching support"""
        try:
            if aggregation_name not in self.aggregations:
                raise ValueError(f"Aggregation '{aggregation_name}' not found")
            
            agg_def = self.aggregations[aggregation_name]
            
            if not agg_def.enabled:
                self.logger.info(f"Aggregation '{aggregation_name}' is disabled")
                return df.limit(0)  # Return empty DataFrame
            
            # Try to get cached aggregation metadata
            if use_cache and self.cache_manager:
                cache_key = f"aggregation_metadata:{aggregation_name}"
                cached_metadata = self.cache_manager.get(cache_key)
                if cached_metadata:
                    self.logger.debug(f"Using cached metadata for aggregation: {aggregation_name}")
            
            # Apply filter if specified
            filtered_df = df
            if agg_def.filter_condition:
                filtered_df = df.filter(expr(agg_def.filter_condition))
            
            # Add watermark
            watermarked_df = filtered_df.withWatermark(
                agg_def.watermark_column, 
                agg_def.watermark_delay
            )
            
            # Create aggregation based on type
            if agg_def.aggregation_type == AggregationType.TUMBLING_WINDOW:
                result_df = self._create_tumbling_window_aggregation(
                    watermarked_df, agg_def
                )
            elif agg_def.aggregation_type == AggregationType.SLIDING_WINDOW:
                result_df = self._create_sliding_window_aggregation(
                    watermarked_df, agg_def
                )
            elif agg_def.aggregation_type == AggregationType.SESSION_WINDOW:
                result_df = self._create_session_window_aggregation(
                    watermarked_df, agg_def
                )
            elif agg_def.aggregation_type == AggregationType.GLOBAL_AGGREGATE:
                result_df = self._create_global_aggregation(
                    watermarked_df, agg_def
                )
            else:
                raise ValueError(f"Unsupported aggregation type: {agg_def.aggregation_type}")
            
            # Add metadata
            result_df = (
                result_df
                .withColumn("aggregation_name", lit(aggregation_name))
                .withColumn("metric_type", lit(agg_def.metric_type.value))
                .withColumn("aggregation_timestamp", current_timestamp())
                .withColumn("gold_id", expr("uuid()"))
                .withColumn("cache_enabled", lit(use_cache and self.cache_manager is not None))
            )
            
            # Cache aggregation metadata
            if self.cache_manager:
                agg_metadata = {
                    "aggregation_name": aggregation_name,
                    "aggregation_type": agg_def.aggregation_type.value,
                    "metric_type": agg_def.metric_type.value,
                    "window_duration": agg_def.window_duration,
                    "group_by_columns": agg_def.group_by_columns,
                    "created_at": datetime.now().isoformat()
                }
                self.cache_manager.set(
                    f"aggregation_metadata:{aggregation_name}",
                    json.dumps(agg_metadata),
                    ttl=self.config.aggregation_cache_ttl
                )
            
            self.logger.debug(f"Created aggregation stream: {aggregation_name}")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Failed to create aggregation stream '{aggregation_name}': {e}")
            raise
    
    def _create_tumbling_window_aggregation(
        self, 
        df: DataFrame, 
        agg_def: AggregationDefinition
    ) -> DataFrame:
        """Create tumbling window aggregation"""
        group_cols = [window(col(agg_def.watermark_column), agg_def.window_duration)]
        group_cols.extend([col(c) for c in agg_def.group_by_columns])
        
        # Build aggregation expressions
        agg_exprs = {}
        for alias, expression in agg_def.aggregation_expressions.items():
            agg_exprs[alias] = expr(expression)
        
        result_df = (
            df.groupBy(*group_cols)
            .agg(agg_exprs)
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                *[col(c) for c in agg_def.group_by_columns],
                *[col(alias) for alias in agg_def.aggregation_expressions.keys()]
            )
        )
        
        return result_df
    
    def _create_sliding_window_aggregation(
        self, 
        df: DataFrame, 
        agg_def: AggregationDefinition
    ) -> DataFrame:
        """Create sliding window aggregation"""
        if not agg_def.slide_duration:
            raise ValueError("Slide duration required for sliding window")
        
        group_cols = [
            window(
                col(agg_def.watermark_column), 
                agg_def.window_duration, 
                agg_def.slide_duration
            )
        ]
        group_cols.extend([col(c) for c in agg_def.group_by_columns])
        
        # Build aggregation expressions
        agg_exprs = {}
        for alias, expression in agg_def.aggregation_expressions.items():
            agg_exprs[alias] = expr(expression)
        
        result_df = (
            df.groupBy(*group_cols)
            .agg(agg_exprs)
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                *[col(c) for c in agg_def.group_by_columns],
                *[col(alias) for alias in agg_def.aggregation_expressions.keys()]
            )
        )
        
        return result_df
    
    def _create_session_window_aggregation(
        self, 
        df: DataFrame, 
        agg_def: AggregationDefinition
    ) -> DataFrame:
        """Create session window aggregation"""
        group_cols = [
            session_window(
                col(agg_def.watermark_column), 
                agg_def.window_duration
            )
        ]
        group_cols.extend([col(c) for c in agg_def.group_by_columns])
        
        # Build aggregation expressions
        agg_exprs = {}
        for alias, expression in agg_def.aggregation_expressions.items():
            agg_exprs[alias] = expr(expression)
        
        result_df = (
            df.groupBy(*group_cols)
            .agg(agg_exprs)
            .select(
                col("session_window.start").alias("session_start"),
                col("session_window.end").alias("session_end"),
                *[col(c) for c in agg_def.group_by_columns],
                *[col(alias) for alias in agg_def.aggregation_expressions.keys()]
            )
        )
        
        return result_df
    
    def _create_global_aggregation(
        self, 
        df: DataFrame, 
        agg_def: AggregationDefinition
    ) -> DataFrame:
        """Create global aggregation (no windowing)"""
        if agg_def.group_by_columns:
            group_cols = [col(c) for c in agg_def.group_by_columns]
        else:
            group_cols = []
        
        # Build aggregation expressions
        agg_exprs = {}
        for alias, expression in agg_def.aggregation_expressions.items():
            agg_exprs[alias] = expr(expression)
        
        if group_cols:
            result_df = df.groupBy(*group_cols).agg(agg_exprs)
        else:
            result_df = df.agg(agg_exprs)
        
        # Add timestamp for global aggregations
        result_df = result_df.withColumn("aggregation_time", current_timestamp())
        
        return result_df
    
    def check_alert_conditions(self, metrics_df: DataFrame, use_cache: bool = True) -> List[Dict[str, Any]]:
        """Check alert conditions against metrics with caching support"""
        triggered_alerts = []
        
        try:
            # Collect current metrics
            if metrics_df.isEmpty():
                return triggered_alerts
            
            # Check cached alert state to avoid duplicate alerts
            cached_alerts = set()
            if use_cache and self.cache_manager:
                cached_alert_data = self.cache_manager.get("recent_alerts")
                if cached_alert_data:
                    cached_alerts = set(json.loads(cached_alert_data))
                    self.logger.debug(f"Found {len(cached_alerts)} cached recent alerts")
            
            metrics_data = metrics_df.collect()
            
            for row in metrics_data:
                row_dict = row.asDict()
                
                # Check each alert rule
                for rule_id, alert_rule in self.alert_rules.items():
                    if not alert_rule.enabled:
                        continue
                    
                    if alert_rule.metric_name not in row_dict:
                        continue
                    
                    metric_value = row_dict[alert_rule.metric_name]
                    threshold = alert_rule.threshold_value
                    operator = alert_rule.comparison_operator
                    
                    # Evaluate condition
                    condition_met = False
                    if operator == ">" and metric_value > threshold:
                        condition_met = True
                    elif operator == "<" and metric_value < threshold:
                        condition_met = True
                    elif operator == ">=" and metric_value >= threshold:
                        condition_met = True
                    elif operator == "<=" and metric_value <= threshold:
                        condition_met = True
                    elif operator == "==" and metric_value == threshold:
                        condition_met = True
                    elif operator == "!=" and metric_value != threshold:
                        condition_met = True
                    
                    if condition_met:
                        alert_id = f"{rule_id}_{alert_rule.metric_name}_{int(datetime.now().timestamp())}"
                        
                        # Check if we already sent this alert recently
                        if alert_id not in cached_alerts:
                            alert = {
                                "alert_id": alert_id,
                                "rule_id": rule_id,
                                "rule_name": alert_rule.name,
                                "metric_name": alert_rule.metric_name,
                                "metric_value": metric_value,
                                "threshold_value": threshold,
                                "severity": alert_rule.severity.value,
                                "timestamp": datetime.now().isoformat(),
                                "metadata": alert_rule.metadata,
                                "additional_context": row_dict,
                                "cache_deduplication": True
                            }
                            triggered_alerts.append(alert)
                            cached_alerts.add(alert_id)
            
            if triggered_alerts:
                self.logger.warning(f"Triggered {len(triggered_alerts)} alerts")
                
                # Cache the recent alerts to avoid duplicates
                if self.cache_manager:
                    self.cache_manager.set(
                        "recent_alerts",
                        json.dumps(list(cached_alerts)),
                        ttl=self.config.alert_cache_ttl
                    )
            
            return triggered_alerts
            
        except Exception as e:
            self.logger.error(f"Alert condition checking failed: {e}")
            return []
    
    def add_custom_aggregation(self, aggregation: AggregationDefinition):
        """Add custom aggregation definition"""
        self.aggregations[aggregation.name] = aggregation
        self.logger.info(f"Added custom aggregation: {aggregation.name}")
    
    def add_alert_rule(self, alert_rule: AlertRule):
        """Add custom alert rule"""
        self.alert_rules[alert_rule.rule_id] = alert_rule
        self.logger.info(f"Added alert rule: {alert_rule.name}")


class RealtimeGoldProcessor:
    """Comprehensive real-time Gold layer processor with enhanced infrastructure"""
    
    def __init__(
        self, 
        spark: SparkSession, 
        config: GoldProcessingConfig = None
    ):
        self.spark = spark
        self.config = config or GoldProcessingConfig()
        self.logger = get_logger(__name__)
        self.metrics_collector = get_metrics_collector()
        
        # Enhanced infrastructure components
        self.cache_manager: Optional[EventCache] = None
        self.messaging_manager: Optional[HybridMessagingArchitecture] = None
        self.rabbitmq_manager: Optional[RabbitMQManager] = None
        
        # Initialize enhanced infrastructure
        if config.enable_gold_caching:
            self._initialize_cache_manager()
        if config.enable_gold_messaging:
            self._initialize_messaging_manager()
        
        # Managers and services
        self.kafka_manager = KafkaManager()
        self.delta_manager = DeltaLakeManager(spark)
        
        # Aggregation engine with enhanced features
        self.aggregation_engine = RealtimeAggregationEngine(spark, config, self.cache_manager, self.messaging_manager)
        
        # Active queries
        self.active_queries: Dict[str, StreamingQuery] = {}
        
        # Metrics
        self.processed_batches = 0
        self.total_aggregations = 0
        self.alerts_triggered = 0
        
        self.logger.info("Real-time Gold Processor initialized with enhanced infrastructure")
    
    def _initialize_cache_manager(self):
        """Initialize Gold-specific cache manager"""
        try:
            cache_config = CacheConfig(
                redis_url=self.config.redis_url,
                default_ttl=self.config.aggregation_cache_ttl,
                cache_strategy=CacheStrategy.CACHE_ASIDE,
                consistency_level=ConsistencyLevel.EVENTUAL,
                enable_cache_warming=True
            )
            self.cache_manager = EventCache(cache_config)
            self.logger.info("Gold cache manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Gold cache manager: {e}")
    
    def _initialize_messaging_manager(self):
        """Initialize messaging for Gold processing coordination"""
        try:
            rabbitmq_config = RabbitMQConfig(
                host=self.config.rabbitmq_host,
                port=self.config.rabbitmq_port,
                enable_dead_letter=True
            )
            self.rabbitmq_manager = RabbitMQManager(rabbitmq_config)
            self.messaging_manager = HybridMessagingArchitecture(
                kafka_manager=self.kafka_manager,
                rabbitmq_manager=self.rabbitmq_manager,
                cache_manager=self.cache_manager
            )
            self.logger.info("Gold messaging manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Gold messaging manager: {e}")
    
    def start_all_aggregations(
        self,
        silver_stream: DataFrame,
        output_base_path: str,
        checkpoint_base_path: str
    ) -> Dict[str, StreamingQuery]:
        """Start all configured aggregations"""
        try:
            self.logger.info("Starting all Gold layer aggregations")
            
            queries = {}
            
            for agg_name in self.aggregation_engine.aggregations.keys():
                try:
                    query = self.start_single_aggregation(
                        silver_stream, 
                        agg_name, 
                        f"{output_base_path}/{agg_name}",
                        f"{checkpoint_base_path}/{agg_name}"
                    )
                    queries[agg_name] = query
                    self.logger.info(f"Started aggregation: {agg_name}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to start aggregation {agg_name}: {e}")
            
            self.active_queries.update(queries)
            self.logger.info(f"Started {len(queries)} aggregation streams")
            
            return queries
            
        except Exception as e:
            self.logger.error(f"Failed to start aggregations: {e}")
            raise
    
    def start_single_aggregation(
        self,
        silver_stream: DataFrame,
        aggregation_name: str,
        output_path: str,
        checkpoint_path: str
    ) -> StreamingQuery:
        """Start a single aggregation stream"""
        try:
            # Create aggregation stream
            agg_stream = self.aggregation_engine.create_aggregation_stream(
                silver_stream, aggregation_name
            )
            
            # Get aggregation definition for configuration
            agg_def = self.aggregation_engine.aggregations[aggregation_name]
            
            # Configure and start the stream
            query = (
                agg_stream
                .writeStream
                .format("delta")
                .outputMode(agg_def.output_mode)
                .option("checkpointLocation", checkpoint_path)
                .option("mergeSchema", "true")
                .trigger(processingTime=agg_def.trigger_interval)
                .foreachBatch(lambda batch_df, batch_id: 
                             self._process_gold_batch(
                                 batch_df, batch_id, aggregation_name, output_path
                             ))
                .start()
            )
            
            return query
            
        except Exception as e:
            self.logger.error(f"Failed to start aggregation {aggregation_name}: {e}")
            raise
    
    def _cache_gold_results(self, aggregation_name: str, batch_id: int, results: Dict[str, Any]):
        """Cache Gold processing results for CQRS pattern"""
        try:
            if self.cache_manager and self.config.enable_cqrs:
                cache_key = f"gold_results:{aggregation_name}:batch:{batch_id}"
                self.cache_manager.set(
                    cache_key,
                    json.dumps(results),
                    ttl=self.config.read_model_cache_ttl
                )
                self.logger.debug(f"Cached Gold results for {aggregation_name}, batch {batch_id}")
        except Exception as e:
            self.logger.warning(f"Failed to cache Gold results: {e}")
    
    def _send_gold_notification(self, event_type: str, aggregation_name: str, batch_id: int, data: Dict[str, Any]):
        """Send Gold processing notifications via RabbitMQ"""
        try:
            if self.messaging_manager:
                message = HybridMessage(
                    message_id=str(uuid.uuid4()),
                    message_type=MessageType.EVENT,
                    routing_key=f"gold.processing.{event_type}",
                    payload={
                        "aggregation_name": aggregation_name,
                        "batch_id": batch_id,
                        "event_type": event_type,
                        "data": data,
                        "timestamp": datetime.now().isoformat()
                    },
                    priority=MessagePriority.HIGH if event_type == "error" else MessagePriority.NORMAL
                )
                self.messaging_manager.send_event(message)
        except Exception as e:
            self.logger.warning(f"Failed to send Gold notification: {e}")
    
    def _process_gold_batch(
        self, 
        batch_df: DataFrame, 
        batch_id: int, 
        aggregation_name: str,
        output_path: str
    ):
        """Process each aggregation batch"""
        try:
            if batch_df.isEmpty():
                return
            
            batch_count = batch_df.count()
            self.logger.info(
                f"Processing Gold batch {batch_id} for {aggregation_name}: {batch_count} records"
            )
            
            # Send batch start notification
            self._send_gold_notification(
                "batch_started",
                aggregation_name,
                batch_id,
                {"record_count": batch_count}
            )
            
            # Write to Delta Lake with optimized partitioning
            writer = (
                batch_df
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
            )
            
            # Apply partitioning strategy based on configuration
            if self.config.kafka_partitioning_strategy == "hash":
                # Use hash partitioning for better distribution
                writer = writer.option("dataChange", "true")
            
            writer.save(output_path)
            
            # Cache processing results for CQRS
            processing_results = {
                "batch_id": batch_id,
                "record_count": batch_count,
                "aggregation_name": aggregation_name,
                "processing_timestamp": datetime.now().isoformat(),
                "partitioning_strategy": self.config.kafka_partitioning_strategy
            }
            self._cache_gold_results(aggregation_name, batch_id, processing_results)
            
            # Check for alert conditions if enabled
            if self.config.enable_alerting:
                alerts = self.aggregation_engine.check_alert_conditions(batch_df, use_cache=True)
                
                if alerts:
                    self._handle_alerts(alerts, aggregation_name)
                    self.alerts_triggered += len(alerts)
                    
                    # Send alert notification
                    self._send_gold_notification(
                        "alerts_triggered",
                        aggregation_name,
                        batch_id,
                        {
                            "alert_count": len(alerts),
                            "high_severity_count": sum(1 for alert in alerts if alert.get("severity") in ["high", "critical"])
                        }
                    )
            
            # Update metrics
            self.processed_batches += 1
            self.total_aggregations += batch_count
            
            # Send metrics
            if self.metrics_collector:
                self.metrics_collector.increment_counter(
                    "gold_aggregations_processed",
                    {"aggregation_name": aggregation_name}
                )
                
                if alerts:
                    self.metrics_collector.increment_counter(
                        "gold_alerts_triggered",
                        {"aggregation_name": aggregation_name, "alert_count": str(len(alerts))}
                    )
            
            # Send batch completion notification
            self._send_gold_notification(
                "batch_completed",
                aggregation_name,
                batch_id,
                {
                    "record_count": batch_count,
                    "alerts_triggered": len(alerts) if 'alerts' in locals() else 0
                }
            )
            
            self.logger.debug(
                f"Batch {batch_id} for {aggregation_name} processed successfully"
            )
            
        except Exception as e:
            self.logger.error(
                f"Gold batch processing failed for {aggregation_name}, batch {batch_id}: {e}"
            )
            
            # Send error notification
            self._send_gold_notification(
                "error",
                aggregation_name,
                batch_id,
                {"error": str(e)}
            )
            raise
    
    def _handle_alerts(self, alerts: List[Dict[str, Any]], aggregation_name: str):
        """Handle triggered alerts with enhanced messaging"""
        try:
            for alert in alerts:
                # Log alert
                self.logger.warning(
                    f"ALERT TRIGGERED - {alert['rule_name']}: "
                    f"{alert['metric_name']} = {alert['metric_value']} "
                    f"({alert['threshold_value']} threshold)"
                )
                
                # Send to Kafka topic for alert processing
                alert_message = {
                    "alert_id": str(uuid.uuid4()),
                    "aggregation_name": aggregation_name,
                    "alert_data": alert,
                    "timestamp": datetime.now().isoformat()
                }
                
                self.kafka_manager.produce_data_quality_alert(alert_message)
                
                # Also send via RabbitMQ for immediate processing
                if self.messaging_manager:
                    rabbitmq_message = HybridMessage(
                        message_id=str(uuid.uuid4()),
                        message_type=MessageType.COMMAND,
                        routing_key="alerts.gold.immediate",
                        payload=alert_message,
                        priority=MessagePriority.CRITICAL if alert.get("severity") == "critical" else MessagePriority.HIGH
                    )
                    self.messaging_manager.send_command(rabbitmq_message)
                
                # Additional alert handling logic could go here
                # (e.g., sending emails, Slack notifications, etc.)
            
        except Exception as e:
            self.logger.error(f"Alert handling failed: {e}")
    
    def stop_all_aggregations(self):
        """Stop all active aggregation streams"""
        self.logger.info("Stopping all Gold aggregation streams")
        
        for agg_name, query in self.active_queries.items():
            try:
                if query and query.isActive:
                    query.stop()
                    self.logger.info(f"Stopped aggregation: {agg_name}")
            except Exception as e:
                self.logger.warning(f"Error stopping aggregation {agg_name}: {e}")
        
        self.active_queries.clear()
    
    def get_cache_metrics(self) -> Dict[str, Any]:
        """Get Gold cache performance metrics"""
        cache_metrics = {}
        if self.cache_manager:
            cache_metrics = self.cache_manager.metrics.get_metrics_summary()
        return cache_metrics
    
    def get_cached_results(self, aggregation_name: str, batch_id: int) -> Optional[Dict[str, Any]]:
        """Get cached Gold processing results for CQRS read model"""
        try:
            if self.cache_manager:
                cache_key = f"gold_results:{aggregation_name}:batch:{batch_id}"
                cached_data = self.cache_manager.get(cache_key)
                if cached_data:
                    return json.loads(cached_data)
        except Exception as e:
            self.logger.warning(f"Failed to get cached results for {aggregation_name}, batch {batch_id}: {e}")
        return None
    
    def get_aggregation_status(self) -> Dict[str, Any]:
        """Get status of all aggregations with enhanced info"""
        status = {}
        
        for agg_name, query in self.active_queries.items():
            if query and query.isActive:
                last_progress = query.lastProgress
                status[agg_name] = {
                    "active": True,
                    "query_id": query.id,
                    "batch_id": last_progress.get("batchId", -1) if last_progress else -1,
                    "input_rows_per_second": last_progress.get("inputRowsPerSecond", 0) if last_progress else 0,
                    "processed_rows_per_second": last_progress.get("processedRowsPerSecond", 0) if last_progress else 0
                }
            else:
                status[agg_name] = {"active": False}
        
        return {
            "aggregation_status": status,
            "total_active_aggregations": sum(1 for s in status.values() if s.get("active", False)),
            "processed_batches": self.processed_batches,
            "total_aggregations": self.total_aggregations,
            "alerts_triggered": self.alerts_triggered,
            "timestamp": datetime.now().isoformat(),
            "cache_metrics": self.get_cache_metrics(),
            "messaging_enabled": self.messaging_manager is not None,
            "infrastructure_status": {
                "caching_enabled": self.config.enable_gold_caching,
                "messaging_enabled": self.config.enable_gold_messaging,
                "cqrs_enabled": self.config.enable_cqrs,
                "cache_hit_ratio": self.get_cache_metrics().get("hit_ratio", 0.0) if self.cache_manager else 0.0,
                "partitioning_strategy": self.config.kafka_partitioning_strategy
            }
        }


# Factory functions
def create_gold_processing_config(**kwargs) -> GoldProcessingConfig:
    """Create Gold processing configuration"""
    return GoldProcessingConfig(**kwargs)


def create_realtime_gold_processor(
    spark: SparkSession,
    config: GoldProcessingConfig = None
) -> RealtimeGoldProcessor:
    """Create real-time Gold processor instance"""
    return RealtimeGoldProcessor(spark, config)


# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    import time
    
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("RealtimeGoldProcessor")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.stateStore.providerClass", 
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
        .getOrCreate()
    )
    
    try:
        print("Testing Real-time Gold Processor...")
        
        # Create configuration
        config = create_gold_processing_config(
            enable_real_time_aggregations=True,
            enable_anomaly_detection=True,
            enable_alerting=True,
            aggregation_checkpoint_interval="30 seconds"
        )
        
        # Create processor
        processor = create_realtime_gold_processor(spark, config)
        
        # Get aggregation names
        agg_names = list(processor.aggregation_engine.aggregations.keys())
        print(f"✅ Created Gold processor with {len(agg_names)} aggregations")
        print(f"   Aggregations: {', '.join(agg_names[:3])}...")
        
        # Get status
        status = processor.get_aggregation_status()
        print(f"Initial status: {status['total_active_aggregations']} active aggregations")
        
        # Show alert rules
        alert_count = len(processor.aggregation_engine.alert_rules)
        print(f"Loaded {alert_count} alert rules")
        
        print("✅ Real-time Gold Processor testing completed")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

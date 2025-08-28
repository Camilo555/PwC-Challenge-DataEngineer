"""
Advanced Real-time Silver Layer Processing
Provides comprehensive data cleaning, enrichment, and quality assurance for streaming data
"""
from __future__ import annotations

import json
import uuid
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, expr, lit, struct, when, coalesce, isnull, isnan,
    upper, lower, trim, regexp_replace, regexp_extract, split, length,
    date_format, unix_timestamp, from_unixtime, to_timestamp, datediff,
    round as spark_round, floor, ceil, abs as spark_abs, sqrt, log, 
    array, array_contains, element_at, size, sort_array,
    map_keys, map_values, map_from_arrays, create_map,
    hash, crc32, md5, sha1, sha2,
    rand, randn, monotonically_increasing_id,
    broadcast, bucketBy, sortBy
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    StringType, StructField, StructType, TimestampType,
    DoubleType, IntegerType, BooleanType, LongType, ArrayType, 
    MapType, DecimalType, FloatType
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


class DataQualityLevel(Enum):
    """Data quality levels"""
    EXCELLENT = "excellent"  # 0.95-1.0
    GOOD = "good"           # 0.85-0.95
    FAIR = "fair"           # 0.70-0.85
    POOR = "poor"           # 0.50-0.70
    UNACCEPTABLE = "unacceptable"  # < 0.50


class EnrichmentType(Enum):
    """Types of data enrichment"""
    CURRENCY_CONVERSION = "currency_conversion"
    COUNTRY_MAPPING = "country_mapping"
    PRODUCT_CATEGORIZATION = "product_categorization"
    CUSTOMER_SEGMENTATION = "customer_segmentation"
    TEMPORAL_FEATURES = "temporal_features"
    GEOLOCATION = "geolocation"
    REFERENCE_DATA = "reference_data"


class ProcessingRule(Enum):
    """Data processing rule types"""
    VALIDATION = "validation"
    CLEANING = "cleaning"
    TRANSFORMATION = "transformation"
    ENRICHMENT = "enrichment"
    BUSINESS_LOGIC = "business_logic"


@dataclass
class QualityRule:
    """Data quality rule definition"""
    rule_id: str
    name: str
    rule_type: ProcessingRule
    condition: str
    severity: str  # critical, high, medium, low
    action: str    # reject, flag, fix, ignore
    description: str = ""
    weight: float = 1.0
    enabled: bool = True


@dataclass
class EnrichmentRule:
    """Data enrichment rule definition"""
    rule_id: str
    name: str
    enrichment_type: EnrichmentType
    source_columns: List[str]
    target_columns: List[str]
    transformation_logic: str
    lookup_table: Optional[str] = None
    cache_ttl: int = 3600  # seconds
    enabled: bool = True


@dataclass
class SilverProcessingConfig:
    """Configuration for Silver layer processing with enhanced infrastructure"""
    enable_data_cleaning: bool = True
    enable_enrichment: bool = True
    enable_business_rules: bool = True
    enable_deduplication: bool = True
    enable_schema_evolution: bool = True
    quality_threshold: float = 0.8
    duplicate_detection_columns: List[str] = field(default_factory=lambda: [
        "invoice_no", "stock_code", "customer_id", "kafka_timestamp"
    ])
    watermark_delay: str = "10 minutes"
    checkpoint_interval: str = "30 seconds"
    state_timeout: str = "1 hour"
    max_records_per_batch: int = 100000
    
    # Enhanced caching configuration
    enable_silver_caching: bool = True
    redis_url: str = "redis://localhost:6379"
    processing_cache_ttl: int = 1800  # 30 minutes
    quality_cache_ttl: int = 3600  # 1 hour
    reference_data_cache_ttl: int = 7200  # 2 hours
    
    # RabbitMQ configuration for processing coordination
    enable_silver_messaging: bool = True
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    
    # CQRS pattern configuration
    enable_cqrs: bool = True
    read_model_cache_ttl: int = 900  # 15 minutes


class DataQualityEngine:
    """Engine for data quality assessment and improvement"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger(__name__)
        
        # Quality rules registry
        self.quality_rules: Dict[str, QualityRule] = {}
        
        # Load built-in quality rules
        self._load_builtin_quality_rules()
        
    def _load_builtin_quality_rules(self):
        """Load built-in data quality rules"""
        
        rules = [
            # Critical validation rules
            QualityRule(
                rule_id="non_null_invoice",
                name="Invoice Number Not Null",
                rule_type=ProcessingRule.VALIDATION,
                condition="invoice_no is not null and trim(invoice_no) != ''",
                severity="critical",
                action="reject",
                description="Invoice number must be present",
                weight=2.0
            ),
            QualityRule(
                rule_id="positive_quantity",
                name="Positive Quantity for Sales",
                rule_type=ProcessingRule.VALIDATION,
                condition="quantity > 0 or description like '%CANCELLED%' or description like '%RETURN%'",
                severity="high",
                action="flag",
                description="Quantity should be positive for regular sales",
                weight=1.5
            ),
            QualityRule(
                rule_id="positive_unit_price",
                name="Positive Unit Price",
                rule_type=ProcessingRule.VALIDATION,
                condition="unit_price > 0 or description like '%MANUAL%' or description like '%ADJUSTMENT%'",
                severity="high",
                action="flag",
                description="Unit price should be positive",
                weight=1.5
            ),
            QualityRule(
                rule_id="valid_customer_id",
                name="Valid Customer ID Format",
                rule_type=ProcessingRule.VALIDATION,
                condition="customer_id is not null and (regexp_like(customer_id, '^[0-9]+$') or customer_id = 'GUEST')",
                severity="medium",
                action="fix",
                description="Customer ID should be numeric or GUEST",
                weight=1.0
            ),
            QualityRule(
                rule_id="reasonable_line_total",
                name="Reasonable Line Total",
                rule_type=ProcessingRule.VALIDATION,
                condition="abs(line_total) <= 100000",
                severity="medium",
                action="flag",
                description="Line total should be within reasonable bounds",
                weight=1.0
            ),
            QualityRule(
                rule_id="valid_date_range",
                name="Valid Transaction Date Range",
                rule_type=ProcessingRule.VALIDATION,
                condition="kafka_timestamp >= '2010-01-01' and kafka_timestamp <= current_timestamp()",
                severity="high",
                action="reject",
                description="Transaction date should be within valid range",
                weight=1.5
            ),
            QualityRule(
                rule_id="stock_code_format",
                name="Valid Stock Code Format",
                rule_type=ProcessingRule.VALIDATION,
                condition="stock_code is not null and length(trim(stock_code)) >= 1",
                severity="medium",
                action="flag",
                description="Stock code should not be empty",
                weight=1.0
            ),
            QualityRule(
                rule_id="country_not_null",
                name="Country Not Null",
                rule_type=ProcessingRule.VALIDATION,
                condition="country is not null and trim(country) != ''",
                severity="low",
                action="fix",
                description="Country should be present",
                weight=0.5
            )
        ]
        
        for rule in rules:
            self.quality_rules[rule.rule_id] = rule
        
        self.logger.info(f"Loaded {len(rules)} built-in quality rules")
    
    def assess_quality(self, df: DataFrame) -> DataFrame:
        """Assess data quality for each record"""
        try:
            result_df = df
            quality_scores = []
            quality_flags = []
            
            # Apply each quality rule
            for rule_id, rule in self.quality_rules.items():
                if not rule.enabled:
                    continue
                
                try:
                    # Create rule check column
                    rule_column = f"rule_{rule_id}"
                    result_df = result_df.withColumn(
                        rule_column,
                        when(expr(rule.condition), lit(rule.weight)).otherwise(lit(0.0))
                    )
                    quality_scores.append(col(rule_column))
                    
                    # Create rule flag column
                    flag_column = f"flag_{rule_id}"
                    result_df = result_df.withColumn(
                        flag_column,
                        when(expr(rule.condition), lit(False)).otherwise(lit(True))
                    )
                    quality_flags.append(col(flag_column))
                    
                except Exception as e:
                    self.logger.error(f"Failed to apply rule {rule_id}: {e}")
            
            # Calculate overall quality score
            total_weight = sum(rule.weight for rule in self.quality_rules.values() if rule.enabled)
            if quality_scores:
                quality_score_expr = sum(quality_scores) / lit(total_weight)
            else:
                quality_score_expr = lit(1.0)
            
            # Add quality assessment columns
            result_df = (
                result_df
                .withColumn("silver_quality_score", quality_score_expr)
                .withColumn("quality_flags_count", sum([when(flag, lit(1)).otherwise(lit(0)) for flag in quality_flags]))
                .withColumn(
                    "quality_tier",
                    when(col("silver_quality_score") >= 0.95, lit(DataQualityLevel.EXCELLENT.value))
                    .when(col("silver_quality_score") >= 0.85, lit(DataQualityLevel.GOOD.value))
                    .when(col("silver_quality_score") >= 0.70, lit(DataQualityLevel.FAIR.value))
                    .when(col("silver_quality_score") >= 0.50, lit(DataQualityLevel.POOR.value))
                    .otherwise(lit(DataQualityLevel.UNACCEPTABLE.value))
                )
                .withColumn(
                    "quality_issues",
                    array([
                        when(col(f"flag_{rule_id}"), lit(rule_id)).otherwise(lit(None))
                        for rule_id in self.quality_rules.keys()
                        if self.quality_rules[rule_id].enabled
                    ])
                )
            )
            
            # Clean up temporary rule columns
            columns_to_drop = [f"rule_{rule_id}" for rule_id in self.quality_rules.keys()] + \
                             [f"flag_{rule_id}" for rule_id in self.quality_rules.keys()]
            
            for col_name in columns_to_drop:
                if col_name in result_df.columns:
                    result_df = result_df.drop(col_name)
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Quality assessment failed: {e}")
            return df.withColumn("silver_quality_score", lit(0.0))
    
    def add_quality_rule(self, rule: QualityRule):
        """Add custom quality rule"""
        self.quality_rules[rule.rule_id] = rule
        self.logger.info(f"Added quality rule: {rule.name}")


class DataEnrichmentEngine:
    """Engine for data enrichment and transformation"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger(__name__)
        
        # Enrichment rules registry
        self.enrichment_rules: Dict[str, EnrichmentRule] = {}
        
        # Reference data caches
        self.reference_data: Dict[str, DataFrame] = {}
        
        # Load built-in enrichment rules
        self._load_builtin_enrichment_rules()
        self._load_reference_data()
        
    def _load_builtin_enrichment_rules(self):
        """Load built-in enrichment rules"""
        
        rules = [
            EnrichmentRule(
                rule_id="clean_customer_id",
                name="Clean Customer ID",
                enrichment_type=EnrichmentType.CUSTOMER_SEGMENTATION,
                source_columns=["customer_id"],
                target_columns=["customer_id_clean"],
                transformation_logic="""
                case 
                    when customer_id is null or trim(customer_id) = '' then 'ANONYMOUS'
                    when regexp_like(customer_id, '^[0-9]+$') then customer_id
                    else 'GUEST'
                end
                """
            ),
            EnrichmentRule(
                rule_id="clean_country",
                name="Clean and Standardize Country",
                enrichment_type=EnrichmentType.COUNTRY_MAPPING,
                source_columns=["country"],
                target_columns=["country_clean"],
                transformation_logic="""
                case
                    when upper(trim(country)) in ('UK', 'UNITED KINGDOM', 'GREAT BRITAIN', 'GB') then 'United Kingdom'
                    when upper(trim(country)) in ('USA', 'US', 'UNITED STATES', 'AMERICA') then 'United States'
                    when upper(trim(country)) in ('FRANCE', 'FR') then 'France'
                    when upper(trim(country)) in ('GERMANY', 'DE', 'DEUTSCHLAND') then 'Germany'
                    when upper(trim(country)) in ('SPAIN', 'ES', 'ESPANA') then 'Spain'
                    when upper(trim(country)) in ('ITALY', 'IT', 'ITALIA') then 'Italy'
                    when country is null or trim(country) = '' then 'Unknown'
                    else initcap(trim(country))
                end
                """
            ),
            EnrichmentRule(
                rule_id="clean_description",
                name="Clean Product Description",
                enrichment_type=EnrichmentType.PRODUCT_CATEGORIZATION,
                source_columns=["description"],
                target_columns=["description_clean"],
                transformation_logic="""
                case
                    when description is null then 'Unknown Product'
                    else regexp_replace(
                        regexp_replace(
                            regexp_replace(upper(trim(description)), '[^A-Z0-9 ]', ' '),
                            '\\s+', ' '
                        ),
                        '^\\s+|\\s+$', ''
                    )
                end
                """
            ),
            EnrichmentRule(
                rule_id="derive_line_total",
                name="Calculate Line Total",
                enrichment_type=EnrichmentType.REFERENCE_DATA,
                source_columns=["quantity", "unit_price"],
                target_columns=["line_total"],
                transformation_logic="coalesce(quantity * unit_price, 0.0)"
            ),
            EnrichmentRule(
                rule_id="detect_returns",
                name="Detect Return Transactions",
                enrichment_type=EnrichmentType.BUSINESS_LOGIC,
                source_columns=["quantity", "description"],
                target_columns=["is_return"],
                transformation_logic="""
                case
                    when quantity < 0 then true
                    when upper(description) like '%CANCEL%' then true
                    when upper(description) like '%RETURN%' then true
                    when upper(description) like '%REFUND%' then true
                    else false
                end
                """
            ),
            EnrichmentRule(
                rule_id="currency_detection",
                name="Detect Currency Code",
                enrichment_type=EnrichmentType.CURRENCY_CONVERSION,
                source_columns=["country_clean"],
                target_columns=["currency_code"],
                transformation_logic="""
                case
                    when country_clean in ('United Kingdom', 'UK') then 'GBP'
                    when country_clean = 'United States' then 'USD'
                    when country_clean in ('Germany', 'France', 'Spain', 'Italy', 'Netherlands', 'Belgium') then 'EUR'
                    when country_clean = 'Japan' then 'JPY'
                    when country_clean = 'Canada' then 'CAD'
                    when country_clean = 'Australia' then 'AUD'
                    when country_clean = 'Switzerland' then 'CHF'
                    else 'GBP'
                end
                """
            ),
            EnrichmentRule(
                rule_id="temporal_features",
                name="Extract Temporal Features",
                enrichment_type=EnrichmentType.TEMPORAL_FEATURES,
                source_columns=["kafka_timestamp"],
                target_columns=["processing_date", "hour_of_day", "day_of_week", "month_of_year", "is_weekend"],
                transformation_logic="multiple"  # Special marker for multiple column transformation
            ),
            EnrichmentRule(
                rule_id="risk_scoring",
                name="Calculate Risk Score",
                enrichment_type=EnrichmentType.BUSINESS_LOGIC,
                source_columns=["quantity", "unit_price", "customer_id_clean", "is_return"],
                target_columns=["risk_score"],
                transformation_logic="""
                case
                    when abs(quantity) > 1000 then 0.8
                    when unit_price > 500 then 0.7
                    when customer_id_clean = 'ANONYMOUS' and abs(line_total) > 100 then 0.6
                    when is_return and abs(line_total) > 200 then 0.5
                    when abs(line_total) > 1000 then 0.4
                    else 0.1
                end
                """
            )
        ]
        
        for rule in rules:
            self.enrichment_rules[rule.rule_id] = rule
        
        self.logger.info(f"Loaded {len(rules)} built-in enrichment rules")
    
    def _load_reference_data(self):
        """Load reference data for enrichment"""
        try:
            # Country mapping data
            country_data = [
                ("UK", "United Kingdom", "GBP", "Europe"),
                ("US", "United States", "USD", "North America"),
                ("FR", "France", "EUR", "Europe"),
                ("DE", "Germany", "EUR", "Europe"),
                ("ES", "Spain", "EUR", "Europe"),
                ("IT", "Italy", "EUR", "Europe"),
                ("JP", "Japan", "JPY", "Asia"),
                ("CA", "Canada", "CAD", "North America"),
                ("AU", "Australia", "AUD", "Oceania"),
                ("CH", "Switzerland", "CHF", "Europe")
            ]
            
            country_schema = StructType([
                StructField("country_code", StringType(), True),
                StructField("country_name", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("region", StringType(), True)
            ])
            
            self.reference_data["countries"] = self.spark.createDataFrame(
                country_data, country_schema
            )
            
            self.logger.info("Loaded reference data for enrichment")
            
        except Exception as e:
            self.logger.warning(f"Failed to load reference data: {e}")
    
    def apply_enrichment(self, df: DataFrame) -> DataFrame:
        """Apply all enabled enrichment rules"""
        try:
            result_df = df
            
            # Apply enrichment rules in order
            for rule_id, rule in self.enrichment_rules.items():
                if not rule.enabled:
                    continue
                
                try:
                    result_df = self._apply_single_enrichment(result_df, rule)
                except Exception as e:
                    self.logger.error(f"Failed to apply enrichment rule {rule_id}: {e}")
            
            # Add enrichment metadata
            result_df = (
                result_df
                .withColumn("silver_processed_at", current_timestamp())
                .withColumn("enrichment_version", lit("2.0"))
                .withColumn("processing_date", date_format(current_timestamp(), "yyyy-MM-dd"))
            )
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Enrichment failed: {e}")
            return df
    
    def _apply_single_enrichment(self, df: DataFrame, rule: EnrichmentRule) -> DataFrame:
        """Apply a single enrichment rule"""
        try:
            if rule.rule_id == "temporal_features":
                # Special handling for temporal features
                return (
                    df
                    .withColumn("processing_date", date_format(col("kafka_timestamp"), "yyyy-MM-dd"))
                    .withColumn("hour_of_day", expr("hour(kafka_timestamp)"))
                    .withColumn("day_of_week", expr("dayofweek(kafka_timestamp)"))
                    .withColumn("month_of_year", expr("month(kafka_timestamp)"))
                    .withColumn("is_weekend", 
                               when(col("day_of_week").isin([1, 7]), True).otherwise(False))
                )
            
            # Standard single-column transformation
            target_column = rule.target_columns[0]
            
            return df.withColumn(
                target_column,
                expr(rule.transformation_logic)
            )
            
        except Exception as e:
            self.logger.error(f"Single enrichment failed for rule {rule.rule_id}: {e}")
            return df
    
    def add_enrichment_rule(self, rule: EnrichmentRule):
        """Add custom enrichment rule"""
        self.enrichment_rules[rule.rule_id] = rule
        self.logger.info(f"Added enrichment rule: {rule.name}")


class DeduplicationEngine:
    """Engine for detecting and handling duplicate records"""
    
    def __init__(self, spark: SparkSession, dedup_columns: List[str]):
        self.spark = spark
        self.dedup_columns = dedup_columns
        self.logger = get_logger(__name__)
        
    def deduplicate_stream(self, df: DataFrame) -> DataFrame:
        """Remove duplicates from streaming DataFrame"""
        try:
            # Add deduplication logic with watermark
            window_spec = (
                Window
                .partitionBy(*self.dedup_columns)
                .orderBy(col("kafka_timestamp").desc())
            )
            
            result_df = (
                df
                .withColumn("row_number", expr("row_number() over (partition by {} order by kafka_timestamp desc)".format(
                    ", ".join(self.dedup_columns)
                )))
                .withColumn("is_duplicate_removed", col("row_number") > 1)
                .filter(col("row_number") == 1)
                .drop("row_number")
            )
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Deduplication failed: {e}")
            return df.withColumn("is_duplicate_removed", lit(False))


class RealtimeSilverProcessor:
    """Comprehensive real-time Silver layer processor with enhanced infrastructure"""
    
    def __init__(
        self, 
        spark: SparkSession, 
        config: SilverProcessingConfig = None
    ):
        self.spark = spark
        self.config = config or SilverProcessingConfig()
        self.logger = get_logger(__name__)
        self.metrics_collector = get_metrics_collector()
        
        # Enhanced infrastructure components
        self.cache_manager: Optional[EventCache] = None
        self.messaging_manager: Optional[HybridMessagingArchitecture] = None
        self.rabbitmq_manager: Optional[RabbitMQManager] = None
        
        # Initialize enhanced infrastructure
        if config.enable_silver_caching:
            self._initialize_cache_manager()
        if config.enable_silver_messaging:
            self._initialize_messaging_manager()
        
        # Initialize processing engines with enhanced features
        self.quality_engine = DataQualityEngine(spark, self.cache_manager)
        self.enrichment_engine = DataEnrichmentEngine(spark, self.cache_manager)
        self.deduplication_engine = DeduplicationEngine(
            spark, self.config.duplicate_detection_columns
        )
        
        # Managers and services
        self.kafka_manager = KafkaManager()
        self.delta_manager = DeltaLakeManager(spark)
        
        # Processing metrics
        self.processed_batches = 0
        self.total_records_processed = 0
        self.quality_improvements = 0
        self.duplicates_removed = 0
        
        self.logger.info("Real-time Silver Processor initialized with enhanced infrastructure")
    
    def _initialize_cache_manager(self):
        """Initialize Silver-specific cache manager"""
        try:
            cache_config = CacheConfig(
                redis_url=self.config.redis_url,
                default_ttl=self.config.processing_cache_ttl,
                cache_strategy=CacheStrategy.CACHE_ASIDE,
                consistency_level=ConsistencyLevel.EVENTUAL,
                enable_cache_warming=True
            )
            self.cache_manager = EventCache(cache_config)
            self.logger.info("Silver cache manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Silver cache manager: {e}")
    
    def _initialize_messaging_manager(self):
        """Initialize messaging for Silver processing coordination"""
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
            self.logger.info("Silver messaging manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Silver messaging manager: {e}")
    
    def process_streaming_data(
        self,
        bronze_stream: DataFrame,
        output_path: str,
        checkpoint_path: str
    ) -> StreamingQuery:
        """Process streaming data through Silver layer pipeline"""
        try:
            self.logger.info("Starting Silver layer processing")
            
            # Add watermark for late data handling
            watermarked_stream = bronze_stream.withWatermark(
                "kafka_timestamp", 
                self.config.watermark_delay
            )
            
            # Apply Silver transformations
            processed_stream = self._apply_silver_transformations(watermarked_stream)
            
            # Configure and start streaming query
            query = (
                processed_stream
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", checkpoint_path)
                .option("mergeSchema", "true")
                .trigger(processingTime=self.config.checkpoint_interval)
                .foreachBatch(lambda batch_df, batch_id: 
                             self._process_silver_batch(batch_df, batch_id, output_path))
                .start()
            )
            
            return query
            
        except Exception as e:
            self.logger.error(f"Failed to start Silver processing: {e}")
            raise
    
    def _apply_silver_transformations(self, df: DataFrame) -> DataFrame:
        """Apply all Silver layer transformations"""
        try:
            result_df = df
            
            # 1. Data cleaning and standardization
            if self.config.enable_data_cleaning:
                result_df = self._apply_data_cleaning(result_df)
            
            # 2. Data enrichment
            if self.config.enable_enrichment:
                result_df = self.enrichment_engine.apply_enrichment(result_df)
            
            # 3. Deduplication
            if self.config.enable_deduplication:
                result_df = self.deduplication_engine.deduplicate_stream(result_df)
            
            # 4. Quality assessment
            result_df = self.quality_engine.assess_quality(result_df)
            
            # 5. Business rules validation
            if self.config.enable_business_rules:
                result_df = self._apply_business_rules(result_df)
            
            # 6. Add final metadata
            result_df = self._add_silver_metadata(result_df)
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Silver transformations failed: {e}")
            return df
    
    def _apply_data_cleaning(self, df: DataFrame) -> DataFrame:
        """Apply data cleaning transformations"""
        try:
            result_df = (
                df
                # Clean string fields
                .withColumn("description", 
                           when(col("description").isNull(), lit("Unknown"))
                           .otherwise(trim(col("description"))))
                .withColumn("country", 
                           when(col("country").isNull(), lit("Unknown"))
                           .otherwise(trim(col("country"))))
                
                # Handle numeric nulls and negatives appropriately
                .withColumn("quantity", 
                           when(col("quantity").isNull(), lit(0))
                           .otherwise(col("quantity")))
                .withColumn("unit_price", 
                           when(col("unit_price").isNull(), lit(0.0))
                           .when(col("unit_price") < 0, lit(0.0))
                           .otherwise(spark_round(col("unit_price"), 2)))
                
                # Clean customer ID
                .withColumn("customer_id", 
                           when(col("customer_id").isNull(), lit(""))
                           .otherwise(trim(col("customer_id"))))
                
                # Clean stock code
                .withColumn("stock_code", 
                           when(col("stock_code").isNull(), lit("UNKNOWN"))
                           .otherwise(trim(upper(col("stock_code")))))
                
                # Clean invoice number
                .withColumn("invoice_no", 
                           when(col("invoice_no").isNull(), lit(""))
                           .otherwise(trim(col("invoice_no"))))
            )
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Data cleaning failed: {e}")
            return df
    
    def _apply_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply business-specific validation rules"""
        try:
            result_df = (
                df
                # Mark suspicious transactions
                .withColumn(
                    "is_suspicious",
                    (col("risk_score") > 0.7) |
                    (col("silver_quality_score") < 0.5) |
                    (abs(col("line_total")) > 10000)
                )
                
                # Customer segment classification
                .withColumn(
                    "customer_segment",
                    when(col("customer_id_clean") == "ANONYMOUS", "anonymous")
                    .when(col("customer_id_clean") == "GUEST", "guest")
                    .when(col("line_total") > 100, "premium")
                    .otherwise("standard")
                )
                
                # Transaction category
                .withColumn(
                    "transaction_category",
                    when(col("is_return"), "return")
                    .when(col("quantity") == 0, "cancellation")
                    .when(col("line_total") > 500, "high_value")
                    .when(col("line_total") < 5, "low_value")
                    .otherwise("standard")
                )
                
                # Processing priority
                .withColumn(
                    "processing_priority",
                    when(col("is_suspicious"), "high")
                    .when(col("customer_segment") == "premium", "high")
                    .when(col("silver_quality_score") < 0.7, "medium")
                    .otherwise("standard")
                )
            )
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Business rules application failed: {e}")
            return df
    
    def _add_silver_metadata(self, df: DataFrame) -> DataFrame:
        """Add Silver layer metadata"""
        return (
            df
            .withColumn("silver_id", expr("uuid()"))
            .withColumn("silver_processed_at", current_timestamp())
            .withColumn("silver_version", lit("2.0"))
            .withColumn("data_lineage", lit("bronze -> silver"))
        )
    
    def _process_silver_batch(
        self, 
        batch_df: DataFrame, 
        batch_id: int, 
        output_path: str
    ):
        """Process each Silver batch"""
        try:
            if batch_df.isEmpty():
                return
            
            batch_count = batch_df.count()
            self.logger.info(f"Processing Silver batch {batch_id}: {batch_count} records")
            
            # Calculate quality metrics for this batch
            quality_stats = self._calculate_batch_quality_metrics(batch_df)
            
            # Write to Delta Lake
            (
                batch_df
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("processing_date", "country_clean")
                .save(output_path)
            )
            
            # Update processing metrics
            self.processed_batches += 1
            self.total_records_processed += batch_count
            
            # Update quality metrics
            duplicates_count = batch_df.filter(col("is_duplicate_removed")).count()
            self.duplicates_removed += duplicates_count
            
            # Send metrics
            if self.metrics_collector:
                self.metrics_collector.increment_counter(
                    "silver_records_processed", 
                    {"batch_id": str(batch_id)}
                )
                self.metrics_collector.gauge(
                    "silver_quality_score_avg", 
                    quality_stats["avg_quality_score"],
                    {"batch_id": str(batch_id)}
                )
                self.metrics_collector.gauge(
                    "silver_duplicates_removed", 
                    float(duplicates_count),
                    {"batch_id": str(batch_id)}
                )
            
            # Send quality alerts if needed
            if quality_stats["avg_quality_score"] < self.config.quality_threshold:
                self._send_quality_alert(quality_stats, batch_id)
            
            self.logger.info(
                f"Silver batch {batch_id} processed - "
                f"Records: {batch_count}, Quality Score: {quality_stats['avg_quality_score']:.3f}, "
                f"Duplicates Removed: {duplicates_count}"
            )
            
        except Exception as e:
            self.logger.error(f"Silver batch {batch_id} processing failed: {e}")
            raise
    
    def _calculate_batch_quality_metrics(self, batch_df: DataFrame) -> Dict[str, Any]:
        """Calculate quality metrics for a batch"""
        try:
            stats = batch_df.agg(
                expr("avg(silver_quality_score)").alias("avg_quality_score"),
                expr("min(silver_quality_score)").alias("min_quality_score"),
                expr("max(silver_quality_score)").alias("max_quality_score"),
                expr("count(*)").alias("total_records"),
                expr("sum(case when silver_quality_score < 0.5 then 1 else 0 end)").alias("poor_quality_count"),
                expr("avg(quality_flags_count)").alias("avg_flags_per_record")
            ).collect()[0]
            
            return {
                "avg_quality_score": float(stats["avg_quality_score"] or 0),
                "min_quality_score": float(stats["min_quality_score"] or 0),
                "max_quality_score": float(stats["max_quality_score"] or 0),
                "total_records": int(stats["total_records"] or 0),
                "poor_quality_count": int(stats["poor_quality_count"] or 0),
                "avg_flags_per_record": float(stats["avg_flags_per_record"] or 0)
            }
            
        except Exception as e:
            self.logger.error(f"Quality metrics calculation failed: {e}")
            return {"avg_quality_score": 0.0}
    
    def _send_quality_alert(self, quality_stats: Dict[str, Any], batch_id: int):
        """Send quality alert for poor quality batch"""
        try:
            alert_data = {
                "alert_type": "silver_quality_degradation",
                "batch_id": batch_id,
                "avg_quality_score": quality_stats["avg_quality_score"],
                "threshold": self.config.quality_threshold,
                "poor_quality_count": quality_stats.get("poor_quality_count", 0),
                "total_records": quality_stats.get("total_records", 0),
                "severity": "high" if quality_stats["avg_quality_score"] < 0.5 else "medium",
                "timestamp": datetime.now().isoformat()
            }
            
            self.kafka_manager.produce_data_quality_alert(alert_data)
            self.logger.warning(f"Quality alert sent for batch {batch_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to send quality alert: {e}")
    
    def get_cache_metrics(self) -> Dict[str, Any]:
        """Get Silver cache performance metrics"""
        cache_metrics = {}
        if self.cache_manager:
            cache_metrics = self.cache_manager.metrics.get_metrics_summary()
        return cache_metrics
    
    def get_processing_metrics(self) -> Dict[str, Any]:
        """Get Silver processing metrics with enhanced info"""
        return {
            "silver_metrics": {
                "processed_batches": self.processed_batches,
                "total_records_processed": self.total_records_processed,
                "duplicates_removed": self.duplicates_removed,
                "quality_rules_count": len(self.quality_engine.quality_rules),
                "enrichment_rules_count": len(self.enrichment_engine.enrichment_rules)
            },
            "configuration": {
                "quality_threshold": self.config.quality_threshold,
                "enable_data_cleaning": self.config.enable_data_cleaning,
                "enable_enrichment": self.config.enable_enrichment,
                "enable_business_rules": self.config.enable_business_rules,
                "enable_deduplication": self.config.enable_deduplication
            },
            "timestamp": datetime.now().isoformat(),
            "cache_metrics": self.get_cache_metrics(),
            "messaging_enabled": self.messaging_manager is not None,
            "infrastructure_status": {
                "caching_enabled": self.config.enable_silver_caching,
                "messaging_enabled": self.config.enable_silver_messaging,
                "cache_hit_ratio": self.get_cache_metrics().get("hit_ratio", 0.0) if self.cache_manager else 0.0
            }
        }


# Factory functions
def create_silver_processing_config(**kwargs) -> SilverProcessingConfig:
    """Create Silver processing configuration"""
    return SilverProcessingConfig(**kwargs)


def create_realtime_silver_processor(
    spark: SparkSession,
    config: SilverProcessingConfig = None
) -> RealtimeSilverProcessor:
    """Create real-time Silver processor instance"""
    return RealtimeSilverProcessor(spark, config)


# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("RealtimeSilverProcessor")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    try:
        print("Testing Real-time Silver Processor...")
        
        # Create configuration
        config = create_silver_processing_config(
            enable_data_cleaning=True,
            enable_enrichment=True,
            enable_business_rules=True,
            enable_deduplication=True,
            quality_threshold=0.8
        )
        
        # Create processor
        processor = create_realtime_silver_processor(spark, config)
        
        # Get metrics
        metrics = processor.get_processing_metrics()
        print(f"✅ Created Silver processor with {metrics['silver_metrics']['quality_rules_count']} quality rules")
        print(f"   Enrichment rules: {metrics['silver_metrics']['enrichment_rules_count']}")
        print(f"   Quality threshold: {metrics['configuration']['quality_threshold']}")
        
        # Show data quality rules
        quality_rules = list(processor.quality_engine.quality_rules.keys())
        print(f"   Quality rules: {', '.join(quality_rules[:3])}...")
        
        # Show enrichment rules
        enrichment_rules = list(processor.enrichment_engine.enrichment_rules.keys())
        print(f"   Enrichment rules: {', '.join(enrichment_rules[:3])}...")
        
        print("✅ Real-time Silver Processor testing completed")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
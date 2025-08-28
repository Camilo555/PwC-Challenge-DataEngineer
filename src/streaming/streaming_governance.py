"""
Comprehensive Streaming Data Quality and Governance Framework
Provides real-time data quality monitoring, governance, compliance, and lineage tracking
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
    col, current_timestamp, expr, lit, struct, when, coalesce, isnull, isnan,
    count, countDistinct, sum as spark_sum, avg, max as spark_max, min as spark_min,
    stddev, variance, percentile_approx, regexp_extract, length, size,
    hash, sha2, encrypt, decrypt, array, map_from_arrays,
    year, month, day, hour, date_format
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    StringType, StructField, StructType, TimestampType,
    DoubleType, IntegerType, BooleanType, LongType, ArrayType, MapType
)

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


class DataQualityDimension(Enum):
    """Data quality dimensions"""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"
    INTEGRITY = "integrity"
    CONFORMITY = "conformity"


class ComplianceFramework(Enum):
    """Compliance frameworks"""
    GDPR = "gdpr"
    CCPA = "ccpa"
    HIPAA = "hipaa"
    SOX = "sox"
    PCI_DSS = "pci_dss"
    CUSTOM = "custom"


class DataClassification(Enum):
    """Data classification levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    TOP_SECRET = "top_secret"


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class QualityRule:
    """Data quality rule definition"""
    rule_id: str
    name: str
    dimension: DataQualityDimension
    description: str
    columns: List[str]
    condition: str
    threshold: float
    severity: AlertSeverity
    enabled: bool = True
    business_impact: str = "medium"
    remediation_action: str = "alert"
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class ComplianceRule:
    """Compliance rule definition"""
    rule_id: str
    name: str
    framework: ComplianceFramework
    description: str
    data_types: List[str]
    retention_period_days: Optional[int] = None
    anonymization_required: bool = False
    encryption_required: bool = False
    access_controls: Dict[str, Any] = field(default_factory=dict)
    audit_required: bool = True
    enabled: bool = True


@dataclass
class DataLineageRecord:
    """Data lineage tracking record"""
    record_id: str
    source_system: str
    source_table: str
    target_system: str
    target_table: str
    transformation_type: str
    transformation_description: str
    processing_timestamp: datetime
    data_classification: DataClassification
    compliance_flags: Dict[str, bool] = field(default_factory=dict)
    quality_scores: Dict[str, float] = field(default_factory=dict)


@dataclass
class GovernanceConfig:
    """Configuration for data governance with enhanced infrastructure"""
    enable_quality_monitoring: bool = True
    enable_compliance_checking: bool = True
    enable_lineage_tracking: bool = True
    enable_data_masking: bool = True
    enable_audit_logging: bool = True
    quality_check_frequency: str = "1 minute"
    compliance_check_frequency: str = "5 minutes"
    lineage_batch_size: int = 1000
    audit_retention_days: int = 90
    alert_threshold_violations: int = 5
    
    # Enhanced caching configuration
    enable_governance_caching: bool = True
    redis_url: str = "redis://localhost:6379"
    policy_cache_ttl: int = 3600  # 1 hour
    audit_cache_ttl: int = 1800  # 30 minutes
    compliance_cache_ttl: int = 7200  # 2 hours
    
    # RabbitMQ configuration for policy enforcement
    enable_governance_messaging: bool = True
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    
    # Real-time compliance monitoring
    enable_real_time_monitoring: bool = True
    monitoring_window_duration: str = "5 minutes"
    violation_threshold: int = 10


class DataQualityMonitor:
    """Real-time data quality monitoring with caching"""
    
    def __init__(self, spark: SparkSession, config: GovernanceConfig, cache_manager: Optional[EventCache] = None, messaging_manager: Optional[HybridMessagingArchitecture] = None):
        self.spark = spark
        self.config = config
        self.cache_manager = cache_manager
        self.messaging_manager = messaging_manager
        self.logger = get_logger(__name__)
        self.metrics_collector = get_metrics_collector()
        
        # Quality rules registry
        self.quality_rules: Dict[str, QualityRule] = {}
        
        # Quality metrics storage
        self.quality_metrics_path = Path("./data/governance/quality_metrics")
        self.quality_metrics_path.mkdir(parents=True, exist_ok=True)
        
        self._load_builtin_quality_rules()
        
    def _load_builtin_quality_rules(self):
        """Load built-in data quality rules"""
        
        rules = [
            # Completeness rules
            QualityRule(
                rule_id="invoice_no_completeness",
                name="Invoice Number Completeness",
                dimension=DataQualityDimension.COMPLETENESS,
                description="Check that invoice numbers are not null or empty",
                columns=["invoice_no"],
                condition="invoice_no is not null and trim(invoice_no) != ''",
                threshold=0.99,
                severity=AlertSeverity.HIGH,
                business_impact="high"
            ),
            QualityRule(
                rule_id="customer_id_completeness",
                name="Customer ID Completeness",
                dimension=DataQualityDimension.COMPLETENESS,
                description="Check customer ID completeness",
                columns=["customer_id"],
                condition="customer_id is not null",
                threshold=0.85,
                severity=AlertSeverity.MEDIUM,
                business_impact="medium"
            ),
            
            # Validity rules
            QualityRule(
                rule_id="quantity_validity",
                name="Quantity Value Validity",
                dimension=DataQualityDimension.VALIDITY,
                description="Check that quantity values are within reasonable range",
                columns=["quantity"],
                condition="quantity between -10000 and 100000",
                threshold=0.95,
                severity=AlertSeverity.HIGH,
                business_impact="high"
            ),
            QualityRule(
                rule_id="unit_price_validity",
                name="Unit Price Validity",
                dimension=DataQualityDimension.VALIDITY,
                description="Check that unit prices are positive and reasonable",
                columns=["unit_price"],
                condition="unit_price >= 0 and unit_price < 50000",
                threshold=0.98,
                severity=AlertSeverity.HIGH,
                business_impact="high"
            ),
            
            # Consistency rules
            QualityRule(
                rule_id="line_total_consistency",
                name="Line Total Consistency",
                dimension=DataQualityDimension.CONSISTENCY,
                description="Check that line total equals quantity * unit price",
                columns=["quantity", "unit_price", "line_total"],
                condition="abs(line_total - (quantity * unit_price)) < 0.01",
                threshold=0.90,
                severity=AlertSeverity.MEDIUM,
                business_impact="medium"
            ),
            
            # Uniqueness rules
            QualityRule(
                rule_id="transaction_uniqueness",
                name="Transaction Uniqueness",
                dimension=DataQualityDimension.UNIQUENESS,
                description="Check for duplicate transactions within processing window",
                columns=["invoice_no", "stock_code", "kafka_timestamp"],
                condition="count(*) over (partition by invoice_no, stock_code order by kafka_timestamp range between interval 1 hour preceding and current row) = 1",
                threshold=0.95,
                severity=AlertSeverity.MEDIUM,
                business_impact="medium"
            ),
            
            # Timeliness rules
            QualityRule(
                rule_id="data_freshness",
                name="Data Freshness",
                dimension=DataQualityDimension.TIMELINESS,
                description="Check data processing latency",
                columns=["kafka_timestamp", "processing_timestamp"],
                condition="unix_timestamp(processing_timestamp) - unix_timestamp(kafka_timestamp) < 3600", # 1 hour
                threshold=0.90,
                severity=AlertSeverity.MEDIUM,
                business_impact="low"
            )
        ]
        
        for rule in rules:
            self.quality_rules[rule.rule_id] = rule
        
        self.logger.info(f"Loaded {len(rules)} built-in quality rules")
    
    def evaluate_quality_rules(self, df: DataFrame, use_cache: bool = True) -> DataFrame:
        """Evaluate all quality rules against DataFrame with caching"""
        try:
            # Try to get cached quality rules compilation
            if use_cache and self.cache_manager:
                cached_rules = self.cache_manager.get("quality_rules:compiled")
                if cached_rules:
                    self.logger.debug("Using cached quality rules compilation")
            
            result_df = df
            quality_scores = []
            
            # Add quality evaluation timestamp
            result_df = result_df.withColumn("quality_evaluation_timestamp", current_timestamp())
            
            for rule_id, rule in self.quality_rules.items():
                if not rule.enabled:
                    continue
                
                try:
                    # Check if required columns exist
                    missing_columns = [col for col in rule.columns if col not in df.columns]
                    if missing_columns:
                        self.logger.warning(f"Rule {rule_id} skipped: missing columns {missing_columns}")
                        continue
                    
                    # Add rule evaluation column
                    rule_column = f"dq_{rule_id}_passed"
                    result_df = result_df.withColumn(
                        rule_column,
                        when(expr(rule.condition), True).otherwise(False)
                    )
                    
                    quality_scores.append(rule_column)
                    
                    self.logger.debug(f"Applied quality rule: {rule_id}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to apply quality rule {rule_id}: {e}")
            
            # Calculate overall quality score
            if quality_scores:
                quality_expr = " + ".join([f"cast({col} as double)" for col in quality_scores])
                result_df = result_df.withColumn(
                    "overall_quality_score",
                    expr(f"({quality_expr}) / {len(quality_scores)}")
                )
                
                # Add quality tier
                result_df = result_df.withColumn(
                    "quality_tier",
                    when(col("overall_quality_score") >= 0.95, "EXCELLENT")
                    .when(col("overall_quality_score") >= 0.85, "GOOD")
                    .when(col("overall_quality_score") >= 0.70, "FAIR")
                    .otherwise("POOR")
                )
            else:
                result_df = result_df.withColumn("overall_quality_score", lit(1.0))
                result_df = result_df.withColumn("quality_tier", lit("UNKNOWN"))
            
            # Add quality metadata
            applied_rules = [rule_id for rule_id in self.quality_rules.keys() if self.quality_rules[rule_id].enabled]
            result_df = result_df.withColumn(
                "quality_rules_applied",
                lit(json.dumps(applied_rules))
            )
            
            # Cache quality rules compilation for future use
            if self.cache_manager:
                quality_compilation = {
                    "applied_rules": applied_rules,
                    "total_rules": len(self.quality_rules),
                    "compilation_timestamp": datetime.now().isoformat()
                }
                self.cache_manager.set(
                    "quality_rules:compiled",
                    json.dumps(quality_compilation),
                    ttl=1800  # 30 minutes
                )
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Quality rule evaluation failed: {e}")
            return df
    
    def generate_quality_report(self, df: DataFrame, report_name: str, use_cache: bool = True) -> Dict[str, Any]:
        """Generate comprehensive quality report with caching"""
        try:
            total_records = df.count()
            if total_records == 0:
                return {"error": "No data to analyze"}
            
            # Calculate rule-specific metrics
            rule_metrics = {}
            for rule_id, rule in self.quality_rules.items():
                if not rule.enabled:
                    continue
                
                rule_column = f"dq_{rule_id}_passed"
                if rule_column in df.columns:
                    passed_count = df.filter(col(rule_column)).count()
                    pass_rate = passed_count / total_records
                    
                    rule_metrics[rule_id] = {
                        "rule_name": rule.name,
                        "dimension": rule.dimension.value,
                        "total_records": total_records,
                        "passed_records": passed_count,
                        "failed_records": total_records - passed_count,
                        "pass_rate": pass_rate,
                        "threshold": rule.threshold,
                        "threshold_met": pass_rate >= rule.threshold,
                        "severity": rule.severity.value,
                        "business_impact": rule.business_impact
                    }
            
            # Calculate overall metrics
            overall_stats = df.agg(
                avg("overall_quality_score").alias("avg_quality_score"),
                min("overall_quality_score").alias("min_quality_score"),
                spark_max("overall_quality_score").alias("max_quality_score"),
                stddev("overall_quality_score").alias("quality_score_stddev")
            ).collect()[0]
            
            # Quality tier distribution
            tier_distribution = (
                df.groupBy("quality_tier")
                .count()
                .orderBy("quality_tier")
                .collect()
            )
            
            tier_stats = {row["quality_tier"]: row["count"] for row in tier_distribution}
            
            # Generate report
            report = {
                "report_name": report_name,
                "timestamp": datetime.now().isoformat(),
                "total_records": total_records,
                "overall_statistics": {
                    "avg_quality_score": float(overall_stats["avg_quality_score"] or 0),
                    "min_quality_score": float(overall_stats["min_quality_score"] or 0),
                    "max_quality_score": float(overall_stats["max_quality_score"] or 0),
                    "quality_score_stddev": float(overall_stats["quality_score_stddev"] or 0)
                },
                "quality_tier_distribution": tier_stats,
                "rule_metrics": rule_metrics,
                "rules_evaluated": len(rule_metrics),
                "threshold_violations": sum(
                    1 for metric in rule_metrics.values() if not metric["threshold_met"]
                )
            }
            
            # Save report
            report_file = self.quality_metrics_path / f"{report_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            # Cache report for quick access
            if self.cache_manager:
                cache_key = f"quality_report:{report_name}:latest"
                self.cache_manager.set(
                    cache_key,
                    json.dumps(report),
                    ttl=self.config.audit_cache_ttl
                )
            
            self.logger.info(f"Quality report generated: {report_file}")
            return report
            
        except Exception as e:
            self.logger.error(f"Quality report generation failed: {e}")
            return {"error": str(e)}
    
    def add_quality_rule(self, rule: QualityRule):
        """Add custom quality rule"""
        self.quality_rules[rule.rule_id] = rule
        self.logger.info(f"Added quality rule: {rule.name}")


class ComplianceMonitor:
    """Compliance monitoring and enforcement with caching"""
    
    def __init__(self, spark: SparkSession, config: GovernanceConfig, cache_manager: Optional[EventCache] = None, messaging_manager: Optional[HybridMessagingArchitecture] = None):
        self.spark = spark
        self.config = config
        self.cache_manager = cache_manager
        self.messaging_manager = messaging_manager
        self.logger = get_logger(__name__)
        
        # Compliance rules registry
        self.compliance_rules: Dict[str, ComplianceRule] = {}
        
        # Audit log path
        self.audit_log_path = Path("./data/governance/audit_logs")
        self.audit_log_path.mkdir(parents=True, exist_ok=True)
        
        self._load_builtin_compliance_rules()
        
    def _load_builtin_compliance_rules(self):
        """Load built-in compliance rules"""
        
        rules = [
            ComplianceRule(
                rule_id="gdpr_customer_data",
                name="GDPR Customer Data Protection",
                framework=ComplianceFramework.GDPR,
                description="GDPR compliance for customer personal data",
                data_types=["customer_id", "email", "phone", "address"],
                retention_period_days=2555,  # 7 years
                anonymization_required=True,
                encryption_required=True,
                access_controls={"roles": ["data_analyst", "data_scientist"], "approval_required": True},
                audit_required=True
            ),
            ComplianceRule(
                rule_id="ccpa_personal_info",
                name="CCPA Personal Information Protection",
                framework=ComplianceFramework.CCPA,
                description="CCPA compliance for California residents",
                data_types=["customer_id", "location", "purchase_history"],
                retention_period_days=1095,  # 3 years
                anonymization_required=True,
                access_controls={"geo_restriction": "california", "opt_out_respected": True}
            ),
            ComplianceRule(
                rule_id="pci_dss_payment_data",
                name="PCI DSS Payment Data Security",
                framework=ComplianceFramework.PCI_DSS,
                description="PCI DSS compliance for payment card data",
                data_types=["card_number", "cvv", "expiry_date"],
                retention_period_days=365,  # 1 year
                encryption_required=True,
                access_controls={"roles": ["payment_processor"], "mfa_required": True}
            )
        ]
        
        for rule in rules:
            self.compliance_rules[rule.rule_id] = rule
        
        self.logger.info(f"Loaded {len(rules)} built-in compliance rules")
    
    def apply_compliance_controls(self, df: DataFrame, use_cache: bool = True) -> DataFrame:
        """Apply compliance controls to DataFrame with caching"""
        try:
            # Try to get cached compliance policies
            if use_cache and self.cache_manager:
                cached_policies = self.cache_manager.get("compliance_policies:active")
                if cached_policies:
                    self.logger.debug("Using cached compliance policies")
            
            result_df = df
            compliance_flags = {}
            
            # Add compliance evaluation timestamp
            result_df = result_df.withColumn("compliance_evaluation_timestamp", current_timestamp())
            
            for rule_id, rule in self.compliance_rules.items():
                if not rule.enabled:
                    continue
                
                try:
                    # Check if rule applies to any columns in the DataFrame
                    applicable_columns = [col for col in rule.data_types if col in df.columns]
                    
                    if not applicable_columns:
                        continue
                    
                    # Apply anonymization if required
                    if rule.anonymization_required:
                        result_df = self._apply_anonymization(result_df, applicable_columns, rule_id)
                    
                    # Apply encryption if required
                    if rule.encryption_required:
                        result_df = self._apply_encryption(result_df, applicable_columns, rule_id)
                    
                    # Add compliance flag
                    compliance_flags[rule_id] = {
                        "framework": rule.framework.value,
                        "applicable_columns": applicable_columns,
                        "anonymization_applied": rule.anonymization_required,
                        "encryption_applied": rule.encryption_required
                    }
                    
                    self.logger.debug(f"Applied compliance rule: {rule_id}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to apply compliance rule {rule_id}: {e}")
            
            # Add compliance metadata
            result_df = result_df.withColumn(
                "compliance_flags",
                lit(json.dumps(compliance_flags))
            )
            
            # Cache active compliance policies
            if self.cache_manager:
                active_policies = {
                    "applied_rules": list(compliance_flags.keys()),
                    "total_rules": len(self.compliance_rules),
                    "cache_timestamp": datetime.now().isoformat()
                }
                self.cache_manager.set(
                    "compliance_policies:active",
                    json.dumps(active_policies),
                    ttl=self.config.compliance_cache_ttl
                )
            
            # Log compliance application
            self._log_compliance_action("compliance_controls_applied", {
                "rules_applied": list(compliance_flags.keys()),
                "record_count": df.count()
            })
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Compliance controls application failed: {e}")
            return df
    
    def _apply_anonymization(self, df: DataFrame, columns: List[str], rule_id: str) -> DataFrame:
        """Apply data anonymization"""
        try:
            result_df = df
            
            for column in columns:
                if column in df.columns:
                    # Simple anonymization using hash (in production, use proper anonymization)
                    anonymized_column = f"{column}_anonymized"
                    result_df = result_df.withColumn(
                        anonymized_column,
                        sha2(col(column).cast("string"), 256)
                    )
                    
                    # Optionally drop original column for sensitive data
                    if rule_id in ["gdpr_customer_data", "pci_dss_payment_data"]:
                        result_df = result_df.drop(column)
                        result_df = result_df.withColumnRenamed(anonymized_column, column)
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Anonymization failed: {e}")
            return df
    
    def _apply_encryption(self, df: DataFrame, columns: List[str], rule_id: str) -> DataFrame:
        """Apply data encryption (placeholder - would use proper encryption in production)"""
        try:
            # In production, this would use proper encryption libraries
            # For demonstration, we'll use a simple encoding
            result_df = df
            
            for column in columns:
                if column in df.columns:
                    encrypted_column = f"{column}_encrypted"
                    result_df = result_df.withColumn(
                        encrypted_column,
                        expr(f"base64(cast({column} as binary))")
                    )
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Encryption failed: {e}")
            return df
    
    def _log_compliance_action(self, action: str, metadata: Dict[str, Any]):
        """Log compliance action for audit trail"""
        try:
            if not self.config.enable_audit_logging:
                return
            
            audit_record = {
                "audit_id": str(uuid.uuid4()),
                "timestamp": datetime.now().isoformat(),
                "action": action,
                "metadata": metadata,
                "system": "streaming_governance",
                "user": "system"
            }
            
            # Save to audit log
            audit_file = self.audit_log_path / f"compliance_audit_{datetime.now().strftime('%Y%m%d')}.jsonl"
            
            with open(audit_file, 'a') as f:
                f.write(json.dumps(audit_record) + '\n')
            
        except Exception as e:
            self.logger.error(f"Audit logging failed: {e}")
    
    def add_compliance_rule(self, rule: ComplianceRule):
        """Add custom compliance rule"""
        self.compliance_rules[rule.rule_id] = rule
        self.logger.info(f"Added compliance rule: {rule.name}")


class DataLineageTracker:
    """Data lineage tracking and management with caching"""
    
    def __init__(self, spark: SparkSession, config: GovernanceConfig, cache_manager: Optional[EventCache] = None, messaging_manager: Optional[HybridMessagingArchitecture] = None):
        self.spark = spark
        self.config = config
        self.cache_manager = cache_manager
        self.messaging_manager = messaging_manager
        self.logger = get_logger(__name__)
        
        # Lineage storage
        self.lineage_path = Path("./data/governance/lineage")
        self.lineage_path.mkdir(parents=True, exist_ok=True)
        
    def track_lineage(
        self, 
        df: DataFrame, 
        source_info: Dict[str, str], 
        target_info: Dict[str, str],
        transformation_type: str,
        transformation_description: str
    ) -> DataFrame:
        """Track data lineage for a transformation"""
        try:
            # Create lineage record
            lineage_id = str(uuid.uuid4())
            
            lineage_record = DataLineageRecord(
                record_id=lineage_id,
                source_system=source_info.get("system", "unknown"),
                source_table=source_info.get("table", "unknown"),
                target_system=target_info.get("system", "unknown"),
                target_table=target_info.get("table", "unknown"),
                transformation_type=transformation_type,
                transformation_description=transformation_description,
                processing_timestamp=datetime.now(),
                data_classification=DataClassification(source_info.get("classification", "internal"))
            )
            
            # Add lineage metadata to DataFrame
            result_df = (
                df
                .withColumn("lineage_id", lit(lineage_id))
                .withColumn("source_system", lit(lineage_record.source_system))
                .withColumn("source_table", lit(lineage_record.source_table))
                .withColumn("target_system", lit(lineage_record.target_system))
                .withColumn("target_table", lit(lineage_record.target_table))
                .withColumn("transformation_type", lit(transformation_type))
                .withColumn("data_classification", lit(lineage_record.data_classification.value))
                .withColumn("lineage_timestamp", current_timestamp())
            )
            
            # Store lineage record
            self._store_lineage_record(lineage_record)
            
            # Cache lineage metadata for quick queries
            if self.cache_manager:
                lineage_cache_key = f"lineage:{source_info.get('table', 'unknown')}:{target_info.get('table', 'unknown')}"
                lineage_metadata = {
                    "lineage_id": lineage_id,
                    "transformation_type": transformation_type,
                    "data_classification": lineage_record.data_classification.value,
                    "processing_timestamp": lineage_record.processing_timestamp.isoformat()
                }
                self.cache_manager.set(
                    lineage_cache_key,
                    json.dumps(lineage_metadata),
                    ttl=3600  # 1 hour
                )
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Lineage tracking failed: {e}")
            return df
    
    def _store_lineage_record(self, record: DataLineageRecord):
        """Store lineage record for audit and analysis"""
        try:
            lineage_data = {
                "record_id": record.record_id,
                "source_system": record.source_system,
                "source_table": record.source_table,
                "target_system": record.target_system,
                "target_table": record.target_table,
                "transformation_type": record.transformation_type,
                "transformation_description": record.transformation_description,
                "processing_timestamp": record.processing_timestamp.isoformat(),
                "data_classification": record.data_classification.value,
                "compliance_flags": record.compliance_flags,
                "quality_scores": record.quality_scores
            }
            
            # Save to lineage file
            lineage_file = self.lineage_path / f"lineage_{datetime.now().strftime('%Y%m%d')}.jsonl"
            
            with open(lineage_file, 'a') as f:
                f.write(json.dumps(lineage_data) + '\n')
            
            # Send lineage tracking event
            if self.messaging_manager:
                message = HybridMessage(
                    message_id=str(uuid.uuid4()),
                    message_type=MessageType.EVENT,
                    routing_key="governance.lineage.tracked",
                    payload=lineage_data,
                    priority=MessagePriority.NORMAL
                )
                self.messaging_manager.send_event(message)
            
        except Exception as e:
            self.logger.error(f"Lineage record storage failed: {e}")


class StreamingGovernanceFramework:
    """Comprehensive streaming data governance framework with enhanced infrastructure"""
    
    def __init__(self, spark: SparkSession, config: GovernanceConfig = None):
        self.spark = spark
        self.config = config or GovernanceConfig()
        self.logger = get_logger(__name__)
        self.metrics_collector = get_metrics_collector()
        
        # Enhanced infrastructure components
        self.cache_manager: Optional[EventCache] = None
        self.messaging_manager: Optional[HybridMessagingArchitecture] = None
        self.rabbitmq_manager: Optional[RabbitMQManager] = None
        
        # Initialize enhanced infrastructure
        if config.enable_governance_caching:
            self._initialize_cache_manager()
        if config.enable_governance_messaging:
            self._initialize_messaging_manager()
        
        # Initialize components with enhanced features
        self.quality_monitor = DataQualityMonitor(spark, self.config, self.cache_manager, self.messaging_manager) if self.config.enable_quality_monitoring else None
        self.compliance_monitor = ComplianceMonitor(spark, self.config, self.cache_manager, self.messaging_manager) if self.config.enable_compliance_checking else None
        self.lineage_tracker = DataLineageTracker(spark, self.config, self.cache_manager, self.messaging_manager) if self.config.enable_lineage_tracking else None
        
        # Kafka for alerts
        self.kafka_manager = KafkaManager()
        
        # Processing metrics
        self.quality_checks_performed = 0
        self.compliance_violations = 0
        self.lineage_records_created = 0
        
        self.logger.info("Streaming Governance Framework initialized with enhanced infrastructure")
    
    def _initialize_cache_manager(self):
        """Initialize governance-specific cache manager"""
        try:
            cache_config = CacheConfig(
                redis_url=self.config.redis_url,
                default_ttl=self.config.policy_cache_ttl,
                cache_strategy=CacheStrategy.CACHE_ASIDE,
                consistency_level=ConsistencyLevel.STRONG,  # Strong consistency for governance policies
                enable_cache_warming=True
            )
            self.cache_manager = EventCache(cache_config)
            self.logger.info("Governance cache manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize governance cache manager: {e}")
    
    def _initialize_messaging_manager(self):
        """Initialize messaging for governance notifications"""
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
            self.logger.info("Governance messaging manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize governance messaging manager: {e}")
    
    def apply_governance(
        self,
        df: DataFrame,
        source_info: Dict[str, str],
        target_info: Dict[str, str],
        transformation_type: str = "streaming_etl"
    ) -> DataFrame:
        """Apply comprehensive governance to streaming data"""
        try:
            result_df = df
            governance_metadata = {}
            
            # Apply data quality monitoring with caching
            if self.quality_monitor:
                result_df = self.quality_monitor.evaluate_quality_rules(result_df, use_cache=True)
                governance_metadata["quality_monitoring"] = True
                self.quality_checks_performed += 1
            
            # Apply compliance controls with caching
            if self.compliance_monitor:
                result_df = self.compliance_monitor.apply_compliance_controls(result_df, use_cache=True)
                governance_metadata["compliance_monitoring"] = True
            
            # Track data lineage
            if self.lineage_tracker:
                result_df = self.lineage_tracker.track_lineage(
                    result_df, source_info, target_info, 
                    transformation_type, "Real-time streaming transformation"
                )
                governance_metadata["lineage_tracking"] = True
                self.lineage_records_created += 1
            
            # Add governance metadata
            result_df = (
                result_df
                .withColumn("governance_applied", lit(True))
                .withColumn("governance_timestamp", current_timestamp())
                .withColumn("governance_metadata", lit(json.dumps(governance_metadata)))
            )
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Governance application failed: {e}")
            return df
    
    def process_governance_batch(
        self,
        batch_df: DataFrame,
        batch_id: int,
        source_info: Dict[str, str],
        target_info: Dict[str, str]
    ):
        """Process governance for a streaming batch"""
        try:
            if batch_df.isEmpty():
                return
            
            # Apply governance
            governed_df = self.apply_governance(
                batch_df, source_info, target_info, "batch_streaming"
            )
            
            # Generate quality report with caching
            if self.quality_monitor:
                quality_report = self.quality_monitor.generate_quality_report(
                    governed_df, f"batch_{batch_id}", use_cache=True
                )
                
                # Check for quality violations
                violations = quality_report.get("threshold_violations", 0)
                if violations > 0:
                    self._send_quality_alert(quality_report, batch_id)
                    self.compliance_violations += violations
                    
                    # Send violation notification via RabbitMQ
                    if self.messaging_manager:
                        violation_message = HybridMessage(
                            message_id=str(uuid.uuid4()),
                            message_type=MessageType.COMMAND,
                            routing_key="governance.violation.immediate",
                            payload={
                                "batch_id": batch_id,
                                "violation_count": violations,
                                "severity": "high" if violations >= self.config.violation_threshold else "medium",
                                "timestamp": datetime.now().isoformat()
                            },
                            priority=MessagePriority.CRITICAL if violations >= self.config.violation_threshold else MessagePriority.HIGH
                        )
                        self.messaging_manager.send_command(violation_message)
            
            # Send governance metrics
            if self.metrics_collector:
                self.metrics_collector.increment_counter(
                    "governance_batches_processed",
                    {"batch_id": str(batch_id)}
                )
                
                if self.quality_monitor and "overall_statistics" in quality_report:
                    self.metrics_collector.gauge(
                        "governance_quality_score",
                        quality_report["overall_statistics"]["avg_quality_score"],
                        {"batch_id": str(batch_id)}
                    )
            
            self.logger.info(f"Governance applied to batch {batch_id}: {governed_df.count()} records")
            
        except Exception as e:
            self.logger.error(f"Governance batch processing failed for batch {batch_id}: {e}")
    
    def _send_quality_alert(self, quality_report: Dict[str, Any], batch_id: int):
        """Send quality alert for violations"""
        try:
            alert_data = {
                "alert_type": "data_quality_violation",
                "batch_id": batch_id,
                "total_violations": quality_report["threshold_violations"],
                "avg_quality_score": quality_report["overall_statistics"]["avg_quality_score"],
                "violated_rules": [
                    rule_id for rule_id, metrics in quality_report["rule_metrics"].items()
                    if not metrics["threshold_met"]
                ],
                "severity": "high" if quality_report["threshold_violations"] >= 3 else "medium",
                "timestamp": datetime.now().isoformat()
            }
            
            self.kafka_manager.produce_data_quality_alert(alert_data)
            
            # Also send via RabbitMQ for immediate processing
            if self.messaging_manager:
                alert_message = HybridMessage(
                    message_id=str(uuid.uuid4()),
                    message_type=MessageType.COMMAND,
                    routing_key="governance.quality.alert",
                    payload=alert_data,
                    priority=MessagePriority.CRITICAL
                )
                self.messaging_manager.send_command(alert_message)
            
            self.logger.warning(f"Sent quality alert for batch {batch_id}: {quality_report['threshold_violations']} violations")
            
        except Exception as e:
            self.logger.error(f"Failed to send quality alert: {e}")
    
    def get_cache_metrics(self) -> Dict[str, Any]:
        """Get governance cache performance metrics"""
        cache_metrics = {}
        if self.cache_manager:
            cache_metrics = self.cache_manager.metrics.get_metrics_summary()
        return cache_metrics
    
    def get_cached_policies(self) -> Dict[str, Any]:
        """Get cached governance policies"""
        cached_policies = {}
        if self.cache_manager:
            try:
                quality_policies = self.cache_manager.get("quality_rules:compiled")
                compliance_policies = self.cache_manager.get("compliance_policies:active")
                
                cached_policies = {
                    "quality_policies": json.loads(quality_policies) if quality_policies else None,
                    "compliance_policies": json.loads(compliance_policies) if compliance_policies else None,
                    "cache_timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                self.logger.warning(f"Failed to retrieve cached policies: {e}")
        return cached_policies
    
    def get_governance_metrics(self) -> Dict[str, Any]:
        """Get governance processing metrics with enhanced info"""
        return {
            "governance_metrics": {
                "quality_checks_performed": self.quality_checks_performed,
                "compliance_violations": self.compliance_violations,
                "lineage_records_created": self.lineage_records_created
            },
            "configuration": {
                "quality_monitoring_enabled": self.config.enable_quality_monitoring,
                "compliance_checking_enabled": self.config.enable_compliance_checking,
                "lineage_tracking_enabled": self.config.enable_lineage_tracking,
                "audit_logging_enabled": self.config.enable_audit_logging
            },
            "component_status": {
                "quality_monitor": self.quality_monitor is not None,
                "compliance_monitor": self.compliance_monitor is not None,
                "lineage_tracker": self.lineage_tracker is not None
            },
            "timestamp": datetime.now().isoformat(),
            "cache_metrics": self.get_cache_metrics(),
            "messaging_enabled": self.messaging_manager is not None,
            "cached_policies": self.get_cached_policies(),
            "infrastructure_status": {
                "caching_enabled": self.config.enable_governance_caching,
                "messaging_enabled": self.config.enable_governance_messaging,
                "real_time_monitoring": self.config.enable_real_time_monitoring,
                "cache_hit_ratio": self.get_cache_metrics().get("hit_ratio", 0.0) if self.cache_manager else 0.0
            }
        }


# Factory functions
def create_governance_config(**kwargs) -> GovernanceConfig:
    """Create governance configuration"""
    return GovernanceConfig(**kwargs)


def create_streaming_governance_framework(
    spark: SparkSession,
    config: GovernanceConfig = None
) -> StreamingGovernanceFramework:
    """Create streaming governance framework instance"""
    return StreamingGovernanceFramework(spark, config)


# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("StreamingGovernanceFramework")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    try:
        print("Testing Streaming Governance Framework...")
        
        # Create configuration
        config = create_governance_config(
            enable_quality_monitoring=True,
            enable_compliance_checking=True,
            enable_lineage_tracking=True,
            enable_audit_logging=True
        )
        
        # Create governance framework
        governance = create_streaming_governance_framework(spark, config)
        
        # Get metrics
        metrics = governance.get_governance_metrics()
        print(f"✅ Created governance framework")
        print(f"   Quality monitoring: {metrics['component_status']['quality_monitor']}")
        print(f"   Compliance monitoring: {metrics['component_status']['compliance_monitor']}")
        print(f"   Lineage tracking: {metrics['component_status']['lineage_tracker']}")
        
        # Show loaded rules
        if governance.quality_monitor:
            quality_rules = len(governance.quality_monitor.quality_rules)
            print(f"   Quality rules loaded: {quality_rules}")
        
        if governance.compliance_monitor:
            compliance_rules = len(governance.compliance_monitor.compliance_rules)
            print(f"   Compliance rules loaded: {compliance_rules}")
        
        print("✅ Streaming Governance Framework testing completed")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

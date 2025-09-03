"""
Comprehensive ETL Pipeline Observability System
Provides deep observability into ETL operations with real-time monitoring,
data quality tracking, performance analysis, and predictive insights.
"""

import asyncio
import threading
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd

from core.distributed_tracing import get_tracing_manager
from core.logging import get_logger
from core.structured_logging import (
    BusinessEvent,
    LogContext,
    PerformanceEvent,
    StructuredLogger,
    log_context,
)
from monitoring.advanced_health_checks import get_health_check_manager
from monitoring.datadog_custom_metrics_advanced import get_custom_metrics_advanced

logger = get_logger(__name__)
structured_logger = StructuredLogger.get_logger(__name__)


class ETLStage(Enum):
    """ETL pipeline stages"""

    EXTRACTION = "extraction"
    TRANSFORMATION = "transformation"
    LOADING = "loading"
    VALIDATION = "validation"
    ENRICHMENT = "enrichment"
    AGGREGATION = "aggregation"
    CLEANUP = "cleanup"


class DataQualityDimension(Enum):
    """Data quality dimensions"""

    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"
    RELEVANCE = "relevance"


class AlertSeverity(Enum):
    """Alert severity levels"""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ETLMetrics:
    """ETL operation metrics"""

    pipeline_name: str
    stage: ETLStage
    start_time: datetime
    end_time: datetime | None = None
    duration_ms: float | None = None
    records_processed: int = 0
    records_failed: int = 0
    records_skipped: int = 0
    bytes_processed: int = 0
    memory_peak_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    io_read_mb: float = 0.0
    io_write_mb: float = 0.0
    data_quality_scores: dict[str, float] = field(default_factory=dict)
    error_details: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DataQualityResult:
    """Data quality assessment result"""

    dimension: DataQualityDimension
    score: float  # 0.0 to 1.0
    total_records: int
    failed_records: int
    details: dict[str, Any] = field(default_factory=dict)
    recommendations: list[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class PipelineHealthScore:
    """Pipeline health scoring"""

    pipeline_name: str
    overall_score: float  # 0.0 to 1.0
    performance_score: float
    quality_score: float
    reliability_score: float
    efficiency_score: float
    trend: str  # improving, stable, degrading
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ETLAlert:
    """ETL monitoring alert"""

    alert_id: str
    pipeline_name: str
    stage: ETLStage | None
    severity: AlertSeverity
    title: str
    description: str
    metric_name: str | None = None
    threshold_value: float | None = None
    actual_value: float | None = None
    remediation_steps: list[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    acknowledged: bool = False


class DataQualityChecker:
    """Comprehensive data quality assessment"""

    def __init__(self):
        self.logger = get_logger(f"{__name__}.data_quality")
        self.quality_rules = self._load_quality_rules()

    def _load_quality_rules(self) -> dict[str, Any]:
        """Load data quality rules configuration"""
        return {
            "completeness": {
                "required_columns": [],
                "null_threshold": 0.05,  # Max 5% nulls allowed
                "empty_string_threshold": 0.02,
            },
            "accuracy": {
                "numeric_range_checks": {},
                "date_range_checks": {},
                "categorical_value_checks": {},
            },
            "consistency": {"cross_column_checks": [], "referential_integrity_checks": []},
            "validity": {"data_type_checks": {}, "format_checks": {}, "business_rule_checks": []},
            "uniqueness": {"unique_columns": [], "composite_unique_keys": []},
            "timeliness": {"max_staleness_hours": 24, "expected_update_frequency": "daily"},
        }

    def assess_completeness(self, df: pd.DataFrame) -> DataQualityResult:
        """Assess data completeness"""
        try:
            total_records = len(df)
            if total_records == 0:
                return DataQualityResult(
                    dimension=DataQualityDimension.COMPLETENESS,
                    score=0.0,
                    total_records=0,
                    failed_records=0,
                    details={"error": "Empty dataset"},
                )

            # Check for null values
            null_counts = df.isnull().sum()
            total_nulls = null_counts.sum()
            null_percentage = total_nulls / (total_records * len(df.columns))

            # Check for empty strings
            empty_string_count = 0
            for col in df.select_dtypes(include=["object"]).columns:
                empty_string_count += (df[col] == "").sum()

            empty_string_percentage = empty_string_count / (total_records * len(df.columns))

            # Calculate completeness score
            completeness_score = max(0.0, 1.0 - null_percentage - empty_string_percentage)

            # Determine failed records (records with significant missing data)
            missing_threshold = 0.5  # If more than 50% of fields are missing
            rows_with_excessive_nulls = (
                df.isnull().sum(axis=1) / len(df.columns)
            ) > missing_threshold
            failed_records = rows_with_excessive_nulls.sum()

            details = {
                "null_percentage": round(null_percentage * 100, 2),
                "empty_string_percentage": round(empty_string_percentage * 100, 2),
                "columns_with_nulls": null_counts[null_counts > 0].to_dict(),
                "rows_with_excessive_nulls": int(failed_records),
            }

            recommendations = []
            if null_percentage > 0.05:  # More than 5% nulls
                recommendations.append("Investigate high null value percentage")
                recommendations.append("Implement data validation at source")

            return DataQualityResult(
                dimension=DataQualityDimension.COMPLETENESS,
                score=completeness_score,
                total_records=total_records,
                failed_records=int(failed_records),
                details=details,
                recommendations=recommendations,
            )

        except Exception as e:
            self.logger.error(f"Completeness assessment failed: {str(e)}")
            return DataQualityResult(
                dimension=DataQualityDimension.COMPLETENESS,
                score=0.0,
                total_records=0,
                failed_records=0,
                details={"error": str(e)},
            )

    def assess_accuracy(
        self, df: pd.DataFrame, column_constraints: dict[str, Any] = None
    ) -> DataQualityResult:
        """Assess data accuracy based on constraints"""
        try:
            total_records = len(df)
            if total_records == 0:
                return DataQualityResult(
                    dimension=DataQualityDimension.ACCURACY,
                    score=0.0,
                    total_records=0,
                    failed_records=0,
                )

            accuracy_violations = 0
            violation_details = {}

            # Default constraints if none provided
            if column_constraints is None:
                column_constraints = {}

            # Check numeric ranges
            for column in df.select_dtypes(include=[np.number]).columns:
                if column in column_constraints and "range" in column_constraints[column]:
                    min_val, max_val = column_constraints[column]["range"]
                    violations = ((df[column] < min_val) | (df[column] > max_val)).sum()
                    if violations > 0:
                        accuracy_violations += violations
                        violation_details[f"{column}_range"] = int(violations)

            # Check for negative values where they shouldn't be
            for column in ["amount", "price", "quantity", "revenue"]:
                if column in df.columns and df[column].dtype in ["int64", "float64"]:
                    negative_count = (df[column] < 0).sum()
                    if negative_count > 0:
                        accuracy_violations += negative_count
                        violation_details[f"{column}_negative"] = int(negative_count)

            # Check date validity
            for column in df.select_dtypes(include=["datetime64"]).columns:
                # Future dates where they shouldn't be
                if "created" in column.lower() or "order" in column.lower():
                    future_dates = (df[column] > datetime.now()).sum()
                    if future_dates > 0:
                        accuracy_violations += future_dates
                        violation_details[f"{column}_future"] = int(future_dates)

            # Calculate accuracy score
            accuracy_score = max(0.0, 1.0 - (accuracy_violations / total_records))

            details = {
                "total_violations": accuracy_violations,
                "violation_percentage": round((accuracy_violations / total_records) * 100, 2),
                "violation_details": violation_details,
            }

            recommendations = []
            if accuracy_score < 0.9:
                recommendations.append("Implement data validation rules at ingestion")
                recommendations.append("Add business rule validation")
                recommendations.append("Review data source quality")

            return DataQualityResult(
                dimension=DataQualityDimension.ACCURACY,
                score=accuracy_score,
                total_records=total_records,
                failed_records=accuracy_violations,
                details=details,
                recommendations=recommendations,
            )

        except Exception as e:
            self.logger.error(f"Accuracy assessment failed: {str(e)}")
            return DataQualityResult(
                dimension=DataQualityDimension.ACCURACY,
                score=0.0,
                total_records=0,
                failed_records=0,
                details={"error": str(e)},
            )

    def assess_uniqueness(
        self, df: pd.DataFrame, unique_columns: list[str] = None
    ) -> DataQualityResult:
        """Assess data uniqueness"""
        try:
            total_records = len(df)
            if total_records == 0:
                return DataQualityResult(
                    dimension=DataQualityDimension.UNIQUENESS,
                    score=0.0,
                    total_records=0,
                    failed_records=0,
                )

            duplicate_count = 0
            duplicate_details = {}

            # Check for duplicate rows
            full_duplicates = df.duplicated().sum()
            duplicate_count += full_duplicates
            duplicate_details["full_duplicates"] = int(full_duplicates)

            # Check specific columns for uniqueness
            if unique_columns:
                for column in unique_columns:
                    if column in df.columns:
                        col_duplicates = df[column].duplicated().sum()
                        duplicate_count += col_duplicates
                        duplicate_details[f"{column}_duplicates"] = int(col_duplicates)
            else:
                # Check likely unique columns
                for column in ["id", "customer_id", "order_id", "transaction_id"]:
                    if column in df.columns:
                        col_duplicates = df[column].duplicated().sum()
                        if col_duplicates > 0:
                            duplicate_count += col_duplicates
                            duplicate_details[f"{column}_duplicates"] = int(col_duplicates)

            # Calculate uniqueness score
            uniqueness_score = max(0.0, 1.0 - (duplicate_count / total_records))

            details = {
                "duplicate_count": duplicate_count,
                "duplicate_percentage": round((duplicate_count / total_records) * 100, 2),
                "duplicate_details": duplicate_details,
                "unique_records": total_records - full_duplicates,
            }

            recommendations = []
            if uniqueness_score < 0.95:
                recommendations.append("Implement duplicate detection and removal")
                recommendations.append("Add unique constraints to source systems")
                recommendations.append("Review data ingestion process for duplicates")

            return DataQualityResult(
                dimension=DataQualityDimension.UNIQUENESS,
                score=uniqueness_score,
                total_records=total_records,
                failed_records=duplicate_count,
                details=details,
                recommendations=recommendations,
            )

        except Exception as e:
            self.logger.error(f"Uniqueness assessment failed: {str(e)}")
            return DataQualityResult(
                dimension=DataQualityDimension.UNIQUENESS,
                score=0.0,
                total_records=0,
                failed_records=0,
                details={"error": str(e)},
            )

    def assess_all_dimensions(self, df: pd.DataFrame, **kwargs) -> dict[str, DataQualityResult]:
        """Assess all data quality dimensions"""
        results = {}

        # Run all assessments
        results["completeness"] = self.assess_completeness(df)
        results["accuracy"] = self.assess_accuracy(df, kwargs.get("column_constraints"))
        results["uniqueness"] = self.assess_uniqueness(df, kwargs.get("unique_columns"))

        # Log aggregate results
        overall_score = sum(r.score for r in results.values()) / len(results)
        self.logger.info(
            f"Data quality assessment completed with overall score: {overall_score:.3f}"
        )

        return results


class ETLObservabilityManager:
    """Central manager for ETL pipeline observability"""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.structured_logger = StructuredLogger.get_logger(__name__)
        self.tracer = get_tracing_manager()
        self.custom_metrics = get_custom_metrics_advanced()
        self.health_manager = get_health_check_manager()
        self.data_quality_checker = DataQualityChecker()

        # Monitoring state
        self.active_pipelines: dict[str, ETLMetrics] = {}
        self.pipeline_history: defaultdict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.quality_history: defaultdict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        self.alert_queue: deque = deque(maxlen=1000)
        self.performance_baselines: dict[str, dict[str, float]] = {}

        # Threading for background tasks
        self._monitoring_active = False
        self._monitoring_thread: threading.Thread | None = None

        # Alert thresholds
        self.alert_thresholds = {
            "duration_percentile_95": 300000,  # 5 minutes in ms
            "error_rate": 0.05,  # 5%
            "quality_score": 0.9,  # 90%
            "memory_usage_mb": 4096,  # 4GB
            "records_per_second": 100,  # Minimum processing rate
        }

        self._start_background_monitoring()

    def _start_background_monitoring(self):
        """Start background monitoring thread"""
        self._monitoring_active = True
        self._monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self._monitoring_thread.start()
        self.logger.info("ETL observability background monitoring started")

    def _monitoring_loop(self):
        """Background monitoring loop"""
        while self._monitoring_active:
            try:
                # Check for stale pipelines
                self._check_stale_pipelines()

                # Calculate performance baselines
                self._update_performance_baselines()

                # Check for anomalies
                self._check_anomalies()

                # Clean up old data
                self._cleanup_old_data()

                time.sleep(60)  # Check every minute

            except Exception as e:
                self.logger.error(f"Monitoring loop error: {str(e)}")
                time.sleep(60)

    @contextmanager
    def monitor_pipeline(self, pipeline_name: str, stage: ETLStage, **metadata):
        """Context manager for monitoring ETL pipeline execution"""

        # Create metrics tracking
        metrics = ETLMetrics(
            pipeline_name=pipeline_name,
            stage=stage,
            start_time=datetime.utcnow(),
            metadata=metadata,
        )

        # Store active pipeline
        pipeline_key = f"{pipeline_name}_{stage.value}"
        self.active_pipelines[pipeline_key] = metrics

        # Create logging context
        context = LogContext(
            operation=f"etl_{pipeline_name}_{stage.value}", component="etl_pipeline"
        )

        # Start distributed tracing
        with (
            log_context(context),
            self.tracer.trace_etl_operation(stage.value, "etl", pipeline_name) as span,
        ):
            start_time = time.time()
            self._get_memory_usage()

            try:
                # Set span attributes
                if hasattr(span, "set_attribute"):
                    span.set_attribute("etl.pipeline_name", pipeline_name)
                    span.set_attribute("etl.stage", stage.value)
                    for key, value in metadata.items():
                        span.set_attribute(f"etl.metadata.{key}", str(value))

                yield metrics

                # Success case
                end_time = time.time()
                metrics.end_time = datetime.utcnow()
                metrics.duration_ms = (end_time - start_time) * 1000
                metrics.memory_peak_mb = max(metrics.memory_peak_mb, self._get_memory_usage())

                # Log success
                self._log_pipeline_completion(metrics, success=True)

                # Send metrics to DataDog
                self._send_metrics_to_datadog(metrics)

                # Check for alerts
                self._check_pipeline_alerts(metrics)

            except Exception as e:
                # Failure case
                end_time = time.time()
                metrics.end_time = datetime.utcnow()
                metrics.duration_ms = (end_time - start_time) * 1000
                metrics.error_details.append(str(e))

                # Log failure
                self._log_pipeline_completion(metrics, success=False, error=str(e))

                # Send error metrics
                self._send_error_metrics(metrics, str(e))

                # Create alert
                self._create_alert(
                    pipeline_name=pipeline_name,
                    stage=stage,
                    severity=AlertSeverity.ERROR,
                    title=f"ETL Pipeline Failure: {pipeline_name}",
                    description=f"Stage {stage.value} failed: {str(e)}",
                    remediation_steps=[
                        "Check pipeline logs for detailed error information",
                        "Verify data source availability",
                        "Check system resources",
                        "Validate data format and schema",
                    ],
                )

                raise

            finally:
                # Store in history
                self.pipeline_history[pipeline_key].append(metrics)

                # Remove from active pipelines
                if pipeline_key in self.active_pipelines:
                    del self.active_pipelines[pipeline_key]

    def assess_data_quality(
        self, df: pd.DataFrame, pipeline_name: str, stage: ETLStage, **quality_params
    ) -> dict[str, DataQualityResult]:
        """Assess data quality and store results"""

        quality_results = self.data_quality_checker.assess_all_dimensions(df, **quality_params)

        # Store quality history
        quality_key = f"{pipeline_name}_{stage.value}"
        self.quality_history[quality_key].extend(quality_results.values())

        # Submit quality metrics to custom metrics system
        for dimension, result in quality_results.items():
            asyncio.create_task(
                self.custom_metrics.submit_metric(
                    f"data_quality_{dimension}_score",
                    result.score,
                    dimensions={
                        "pipeline": pipeline_name,
                        "stage": stage.value,
                        "dimension": dimension,
                    },
                    metadata={
                        "total_records": result.total_records,
                        "failed_records": result.failed_records,
                    },
                )
            )

        # Check quality thresholds and create alerts
        overall_quality = sum(r.score for r in quality_results.values()) / len(quality_results)
        if overall_quality < self.alert_thresholds["quality_score"]:
            self._create_alert(
                pipeline_name=pipeline_name,
                stage=stage,
                severity=AlertSeverity.WARNING if overall_quality > 0.8 else AlertSeverity.ERROR,
                title=f"Data Quality Alert: {pipeline_name}",
                description=f"Overall quality score {overall_quality:.3f} below threshold {self.alert_thresholds['quality_score']}",
                metric_name="data_quality_score",
                threshold_value=self.alert_thresholds["quality_score"],
                actual_value=overall_quality,
                remediation_steps=[
                    "Review data quality assessment details",
                    "Investigate data source quality",
                    "Implement data cleansing procedures",
                    "Update data validation rules",
                ],
            )

        # Log quality assessment
        self.structured_logger.info(
            f"Data quality assessment completed for {pipeline_name} {stage.value}",
            extra={
                "category": "etl",
                "event_type": "data_quality_assessment",
                "pipeline": pipeline_name,
                "stage": stage.value,
                "overall_score": overall_quality,
                "dimension_scores": {dim: result.score for dim, result in quality_results.items()},
            },
        )

        return quality_results

    def calculate_pipeline_health_score(self, pipeline_name: str) -> PipelineHealthScore:
        """Calculate comprehensive pipeline health score"""

        # Get recent pipeline history (last 10 runs)
        recent_metrics = []
        for key in self.pipeline_history.keys():
            if key.startswith(pipeline_name):
                recent_metrics.extend(list(self.pipeline_history[key])[-10:])

        if not recent_metrics:
            return PipelineHealthScore(
                pipeline_name=pipeline_name,
                overall_score=0.0,
                performance_score=0.0,
                quality_score=0.0,
                reliability_score=0.0,
                efficiency_score=0.0,
                trend="unknown",
            )

        # Calculate performance score (based on duration and throughput)
        durations = [m.duration_ms for m in recent_metrics if m.duration_ms]
        if durations:
            avg_duration = sum(durations) / len(durations)
            baseline_duration = self.performance_baselines.get(pipeline_name, {}).get(
                "duration_ms", avg_duration
            )
            performance_score = max(0.0, min(1.0, baseline_duration / avg_duration))
        else:
            performance_score = 0.0

        # Calculate quality score (recent data quality assessments)
        quality_scores = []
        for key in self.quality_history.keys():
            if key.startswith(pipeline_name):
                recent_quality = list(self.quality_history[key])[-5:]  # Last 5 assessments
                quality_scores.extend([q.score for q in recent_quality])

        quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 1.0

        # Calculate reliability score (success rate)
        successful_runs = len([m for m in recent_metrics if not m.error_details])
        reliability_score = successful_runs / len(recent_metrics) if recent_metrics else 0.0

        # Calculate efficiency score (resource utilization)
        memory_usage = [m.memory_peak_mb for m in recent_metrics if m.memory_peak_mb > 0]
        if memory_usage:
            avg_memory = sum(memory_usage) / len(memory_usage)
            # Efficiency based on memory usage (lower is better, up to a point)
            efficiency_score = max(0.0, min(1.0, 1000 / max(avg_memory, 100)))  # Ideal around 1GB
        else:
            efficiency_score = 1.0

        # Calculate overall score (weighted average)
        overall_score = (
            performance_score * 0.3
            + quality_score * 0.3
            + reliability_score * 0.3
            + efficiency_score * 0.1
        )

        # Determine trend
        if len(recent_metrics) >= 5:
            recent_scores = [
                (
                    0.3 * (baseline_duration / (m.duration_ms or baseline_duration))
                    + 0.4 * (1.0 if not m.error_details else 0.0)
                    + 0.3 * min(1.0, 1000 / max(m.memory_peak_mb or 100, 100))
                )
                for m in recent_metrics[-5:]
            ]

            if recent_scores[-1] > recent_scores[0] * 1.1:
                trend = "improving"
            elif recent_scores[-1] < recent_scores[0] * 0.9:
                trend = "degrading"
            else:
                trend = "stable"
        else:
            trend = "insufficient_data"

        return PipelineHealthScore(
            pipeline_name=pipeline_name,
            overall_score=overall_score,
            performance_score=performance_score,
            quality_score=quality_score,
            reliability_score=reliability_score,
            efficiency_score=efficiency_score,
            trend=trend,
        )

    def get_pipeline_metrics_summary(self, pipeline_name: str, hours: int = 24) -> dict[str, Any]:
        """Get comprehensive metrics summary for a pipeline"""

        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        # Collect recent metrics
        recent_metrics = []
        for key in self.pipeline_history.keys():
            if key.startswith(pipeline_name):
                recent = [m for m in self.pipeline_history[key] if m.start_time >= cutoff_time]
                recent_metrics.extend(recent)

        if not recent_metrics:
            return {"error": f"No recent metrics found for pipeline: {pipeline_name}"}

        # Calculate statistics
        total_runs = len(recent_metrics)
        successful_runs = len([m for m in recent_metrics if not m.error_details])
        failed_runs = total_runs - successful_runs

        durations = [m.duration_ms for m in recent_metrics if m.duration_ms]
        records_processed = sum(m.records_processed for m in recent_metrics)
        records_failed = sum(m.records_failed for m in recent_metrics)

        # Performance metrics
        avg_duration = sum(durations) / len(durations) if durations else 0
        p95_duration = np.percentile(durations, 95) if durations else 0

        # Throughput
        total_duration_seconds = sum(durations) / 1000 if durations else 0
        avg_throughput = (
            records_processed / total_duration_seconds if total_duration_seconds > 0 else 0
        )

        # Memory usage
        memory_usage = [m.memory_peak_mb for m in recent_metrics if m.memory_peak_mb > 0]
        avg_memory = sum(memory_usage) / len(memory_usage) if memory_usage else 0

        # Quality metrics
        quality_scores = []
        for key in self.quality_history.keys():
            if key.startswith(pipeline_name):
                recent_quality = [
                    q for q in self.quality_history[key] if q.timestamp >= cutoff_time
                ]
                quality_scores.extend([q.score for q in recent_quality])

        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0

        # Health score
        health_score = self.calculate_pipeline_health_score(pipeline_name)

        return {
            "pipeline_name": pipeline_name,
            "time_window_hours": hours,
            "execution_summary": {
                "total_runs": total_runs,
                "successful_runs": successful_runs,
                "failed_runs": failed_runs,
                "success_rate": round(successful_runs / total_runs, 3) if total_runs > 0 else 0,
            },
            "performance_metrics": {
                "avg_duration_ms": round(avg_duration, 2),
                "p95_duration_ms": round(p95_duration, 2),
                "avg_throughput_records_per_second": round(avg_throughput, 2),
                "total_records_processed": records_processed,
                "total_records_failed": records_failed,
                "error_rate": round(records_failed / (records_processed + records_failed), 3)
                if (records_processed + records_failed) > 0
                else 0,
            },
            "resource_utilization": {
                "avg_memory_usage_mb": round(avg_memory, 2),
                "peak_memory_usage_mb": max(memory_usage) if memory_usage else 0,
            },
            "quality_metrics": {
                "avg_quality_score": round(avg_quality, 3),
                "quality_assessments_count": len(quality_scores),
            },
            "health_score": {
                "overall_score": round(health_score.overall_score, 3),
                "performance_score": round(health_score.performance_score, 3),
                "quality_score": round(health_score.quality_score, 3),
                "reliability_score": round(health_score.reliability_score, 3),
                "efficiency_score": round(health_score.efficiency_score, 3),
                "trend": health_score.trend,
            },
            "alerts": {"recent_alerts": self._get_pipeline_alerts(pipeline_name, hours)},
            "timestamp": datetime.utcnow().isoformat(),
        }

    def _log_pipeline_completion(self, metrics: ETLMetrics, success: bool, error: str = None):
        """Log pipeline completion with structured logging"""

        # Create performance event
        perf_event = PerformanceEvent(
            operation=f"etl_{metrics.pipeline_name}_{metrics.stage.value}",
            duration_ms=metrics.duration_ms or 0,
            component="etl_pipeline",
            metadata={
                "pipeline_name": metrics.pipeline_name,
                "stage": metrics.stage.value,
                "records_processed": metrics.records_processed,
                "records_failed": metrics.records_failed,
                "success": success,
                "memory_peak_mb": metrics.memory_peak_mb,
            },
        )

        self.structured_logger.log_performance(perf_event)

        # Create business event for successful processing
        if success and metrics.records_processed > 0:
            business_event = BusinessEvent(
                event_type="etl_completion",
                entity_type="pipeline",
                entity_id=metrics.pipeline_name,
                action="process_data",
                metadata={
                    "stage": metrics.stage.value,
                    "records_processed": metrics.records_processed,
                    "duration_ms": metrics.duration_ms,
                    "quality_scores": metrics.data_quality_scores,
                },
            )

            self.structured_logger.log_business(business_event)

        # Log with appropriate level
        if success:
            self.structured_logger.info(
                f"ETL pipeline {metrics.pipeline_name} {metrics.stage.value} completed successfully",
                extra={
                    "category": "etl",
                    "event_type": "pipeline_completion",
                    "success": True,
                    "duration_ms": metrics.duration_ms,
                    "records_processed": metrics.records_processed,
                },
            )
        else:
            self.structured_logger.error(
                f"ETL pipeline {metrics.pipeline_name} {metrics.stage.value} failed: {error}",
                extra={
                    "category": "etl",
                    "event_type": "pipeline_failure",
                    "success": False,
                    "error": error,
                    "duration_ms": metrics.duration_ms,
                },
            )

    def _send_metrics_to_datadog(self, metrics: ETLMetrics):
        """Send ETL metrics to DataDog"""
        try:
            # Send to custom metrics system
            asyncio.create_task(
                self.custom_metrics.submit_metric(
                    "etl_pipeline_duration_ms",
                    metrics.duration_ms or 0,
                    dimensions={"pipeline": metrics.pipeline_name, "stage": metrics.stage.value},
                )
            )

            asyncio.create_task(
                self.custom_metrics.submit_metric(
                    "etl_records_processed",
                    metrics.records_processed,
                    dimensions={"pipeline": metrics.pipeline_name, "stage": metrics.stage.value},
                )
            )

            if metrics.records_failed > 0:
                asyncio.create_task(
                    self.custom_metrics.submit_metric(
                        "etl_records_failed",
                        metrics.records_failed,
                        dimensions={
                            "pipeline": metrics.pipeline_name,
                            "stage": metrics.stage.value,
                        },
                    )
                )

            if metrics.memory_peak_mb > 0:
                asyncio.create_task(
                    self.custom_metrics.submit_metric(
                        "etl_memory_peak_mb",
                        metrics.memory_peak_mb,
                        dimensions={
                            "pipeline": metrics.pipeline_name,
                            "stage": metrics.stage.value,
                        },
                    )
                )

        except Exception as e:
            self.logger.error(f"Failed to send ETL metrics to DataDog: {str(e)}")

    def _send_error_metrics(self, metrics: ETLMetrics, error: str):
        """Send error metrics to DataDog"""
        try:
            asyncio.create_task(
                self.custom_metrics.submit_metric(
                    "etl_pipeline_errors",
                    1,
                    dimensions={
                        "pipeline": metrics.pipeline_name,
                        "stage": metrics.stage.value,
                        "error_type": type(Exception(error)).__name__,
                    },
                )
            )

        except Exception as e:
            self.logger.error(f"Failed to send error metrics: {str(e)}")

    def _create_alert(
        self,
        pipeline_name: str,
        stage: ETLStage | None,
        severity: AlertSeverity,
        title: str,
        description: str,
        metric_name: str = None,
        threshold_value: float = None,
        actual_value: float = None,
        remediation_steps: list[str] = None,
    ):
        """Create and queue an alert"""

        alert = ETLAlert(
            alert_id=f"{pipeline_name}_{int(time.time())}_{len(self.alert_queue)}",
            pipeline_name=pipeline_name,
            stage=stage,
            severity=severity,
            title=title,
            description=description,
            metric_name=metric_name,
            threshold_value=threshold_value,
            actual_value=actual_value,
            remediation_steps=remediation_steps or [],
        )

        self.alert_queue.append(alert)

        # Log alert
        self.structured_logger.warning(
            f"ETL Alert: {title}",
            extra={
                "category": "etl",
                "event_type": "alert",
                "alert_id": alert.alert_id,
                "severity": severity.value,
                "pipeline": pipeline_name,
                "stage": stage.value if stage else None,
            },
        )

        self.logger.warning(f"ETL Alert created: {title} - {description}")

    def _check_pipeline_alerts(self, metrics: ETLMetrics):
        """Check metrics against alert thresholds"""

        # Duration threshold
        if (
            metrics.duration_ms
            and metrics.duration_ms > self.alert_thresholds["duration_percentile_95"]
        ):
            self._create_alert(
                pipeline_name=metrics.pipeline_name,
                stage=metrics.stage,
                severity=AlertSeverity.WARNING,
                title=f"ETL Duration Alert: {metrics.pipeline_name}",
                description=f"Pipeline execution took {metrics.duration_ms / 1000:.1f}s, exceeding threshold",
                metric_name="duration_ms",
                threshold_value=self.alert_thresholds["duration_percentile_95"],
                actual_value=metrics.duration_ms,
                remediation_steps=[
                    "Check system resource utilization",
                    "Optimize ETL queries and transformations",
                    "Consider data partitioning",
                    "Review data volumes",
                ],
            )

        # Error rate threshold
        if metrics.records_processed > 0:
            error_rate = metrics.records_failed / (
                metrics.records_processed + metrics.records_failed
            )
            if error_rate > self.alert_thresholds["error_rate"]:
                self._create_alert(
                    pipeline_name=metrics.pipeline_name,
                    stage=metrics.stage,
                    severity=AlertSeverity.ERROR,
                    title=f"ETL Error Rate Alert: {metrics.pipeline_name}",
                    description=f"Error rate {error_rate:.1%} exceeds threshold {self.alert_thresholds['error_rate']:.1%}",
                    metric_name="error_rate",
                    threshold_value=self.alert_thresholds["error_rate"],
                    actual_value=error_rate,
                    remediation_steps=[
                        "Review error details in pipeline logs",
                        "Check data source quality",
                        "Validate data schema and format",
                        "Implement error handling improvements",
                    ],
                )

        # Memory usage threshold
        if metrics.memory_peak_mb > self.alert_thresholds["memory_usage_mb"]:
            self._create_alert(
                pipeline_name=metrics.pipeline_name,
                stage=metrics.stage,
                severity=AlertSeverity.WARNING,
                title=f"ETL Memory Usage Alert: {metrics.pipeline_name}",
                description=f"Peak memory usage {metrics.memory_peak_mb:.0f}MB exceeds threshold",
                metric_name="memory_peak_mb",
                threshold_value=self.alert_thresholds["memory_usage_mb"],
                actual_value=metrics.memory_peak_mb,
                remediation_steps=[
                    "Optimize memory usage in transformations",
                    "Implement data chunking",
                    "Review data processing batch sizes",
                    "Consider increasing system memory",
                ],
            )

    def _get_pipeline_alerts(self, pipeline_name: str, hours: int = 24) -> list[dict[str, Any]]:
        """Get recent alerts for a pipeline"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        recent_alerts = [
            asdict(alert)
            for alert in self.alert_queue
            if alert.pipeline_name == pipeline_name and alert.timestamp >= cutoff_time
        ]

        return recent_alerts

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        try:
            import psutil

            process = psutil.Process()
            return process.memory_info().rss / (1024 * 1024)
        except:
            return 0.0

    def _check_stale_pipelines(self):
        """Check for pipelines that have been running too long"""
        current_time = datetime.utcnow()
        stale_threshold = timedelta(hours=6)  # 6 hours

        for _key, metrics in list(self.active_pipelines.items()):
            if current_time - metrics.start_time > stale_threshold:
                self._create_alert(
                    pipeline_name=metrics.pipeline_name,
                    stage=metrics.stage,
                    severity=AlertSeverity.CRITICAL,
                    title=f"Stale Pipeline Alert: {metrics.pipeline_name}",
                    description=f"Pipeline has been running for {current_time - metrics.start_time}",
                    remediation_steps=[
                        "Check if pipeline is stuck or hanging",
                        "Review system resources",
                        "Consider killing and restarting pipeline",
                        "Investigate data processing bottlenecks",
                    ],
                )

    def _update_performance_baselines(self):
        """Update performance baselines for pipelines"""
        for pipeline_name in {key.split("_")[0] for key in self.pipeline_history.keys()}:
            # Calculate baseline from successful runs
            successful_metrics = []
            for key in self.pipeline_history.keys():
                if key.startswith(pipeline_name):
                    successful = [m for m in self.pipeline_history[key] if not m.error_details]
                    successful_metrics.extend(successful)

            if len(successful_metrics) >= 10:  # Need at least 10 successful runs
                durations = [m.duration_ms for m in successful_metrics if m.duration_ms]
                if durations:
                    # Use 75th percentile as baseline (allows for some variance)
                    baseline_duration = np.percentile(durations, 75)
                    self.performance_baselines[pipeline_name] = {
                        "duration_ms": baseline_duration,
                        "updated": datetime.utcnow().isoformat(),
                    }

    def _check_anomalies(self):
        """Check for performance and quality anomalies"""
        # This is a simplified anomaly detection - in production, you'd use more sophisticated methods
        for pipeline_name in {key.split("_")[0] for key in self.pipeline_history.keys()}:
            recent_metrics = []
            for key in self.pipeline_history.keys():
                if key.startswith(pipeline_name):
                    recent_metrics.extend(list(self.pipeline_history[key])[-5:])  # Last 5 runs

            if len(recent_metrics) >= 3:
                # Check for duration anomalies
                durations = [m.duration_ms for m in recent_metrics if m.duration_ms]
                if len(durations) >= 3:
                    avg_duration = sum(durations) / len(durations)
                    baseline = self.performance_baselines.get(pipeline_name, {}).get(
                        "duration_ms", avg_duration
                    )

                    # If current average is 2x baseline, flag as anomaly
                    if avg_duration > baseline * 2:
                        self._create_alert(
                            pipeline_name=pipeline_name,
                            stage=None,
                            severity=AlertSeverity.WARNING,
                            title=f"Performance Anomaly: {pipeline_name}",
                            description=f"Recent average duration {avg_duration / 1000:.1f}s is significantly higher than baseline {baseline / 1000:.1f}s",
                            remediation_steps=[
                                "Investigate recent changes to pipeline or data",
                                "Check system resource utilization",
                                "Review data volume trends",
                                "Analyze query performance",
                            ],
                        )

    def _cleanup_old_data(self):
        """Clean up old monitoring data"""
        # This is handled by the deque maxlen, but we could add more sophisticated cleanup here
        pass

    def get_active_pipelines_status(self) -> dict[str, Any]:
        """Get status of currently active pipelines"""
        active_status = {}

        for key, metrics in self.active_pipelines.items():
            runtime = datetime.utcnow() - metrics.start_time
            active_status[key] = {
                "pipeline_name": metrics.pipeline_name,
                "stage": metrics.stage.value,
                "runtime_seconds": int(runtime.total_seconds()),
                "records_processed": metrics.records_processed,
                "records_failed": metrics.records_failed,
                "memory_peak_mb": metrics.memory_peak_mb,
                "metadata": metrics.metadata,
            }

        return {
            "active_pipelines": active_status,
            "total_active": len(active_status),
            "timestamp": datetime.utcnow().isoformat(),
        }

    def get_recent_alerts(
        self, hours: int = 24, severity: AlertSeverity = None
    ) -> list[dict[str, Any]]:
        """Get recent alerts"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        alerts = [asdict(alert) for alert in self.alert_queue if alert.timestamp >= cutoff_time]

        if severity:
            alerts = [alert for alert in alerts if alert["severity"] == severity.value]

        # Sort by timestamp (most recent first)
        alerts.sort(key=lambda x: x["timestamp"], reverse=True)

        return alerts

    def shutdown(self):
        """Shutdown the observability manager"""
        self._monitoring_active = False
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=10)

        self.logger.info("ETL observability manager shutdown completed")


# Global observability manager instance
_etl_observability_manager: ETLObservabilityManager | None = None


def get_etl_observability_manager() -> ETLObservabilityManager:
    """Get global ETL observability manager instance"""
    global _etl_observability_manager
    if _etl_observability_manager is None:
        _etl_observability_manager = ETLObservabilityManager()
    return _etl_observability_manager


# Convenience functions and decorators
def monitor_etl_stage(pipeline_name: str, stage: ETLStage, **metadata):
    """Decorator to monitor ETL stage execution"""

    def decorator(func):
        def wrapper(*args, **kwargs):
            manager = get_etl_observability_manager()

            with manager.monitor_pipeline(pipeline_name, stage, **metadata) as metrics:
                # Execute the function
                result = func(*args, **kwargs)

                # If result contains metrics, update them
                if isinstance(result, dict) and "records_processed" in result:
                    metrics.records_processed = result.get("records_processed", 0)
                    metrics.records_failed = result.get("records_failed", 0)
                    metrics.records_skipped = result.get("records_skipped", 0)
                    metrics.bytes_processed = result.get("bytes_processed", 0)

                return result

        return wrapper

    return decorator


@asynccontextmanager
async def etl_observability_context(pipeline_name: str, stage: ETLStage, **metadata):
    """Async context manager for ETL observability"""
    manager = get_etl_observability_manager()

    with manager.monitor_pipeline(pipeline_name, stage, **metadata) as metrics:
        yield metrics


if __name__ == "__main__":
    # Example usage
    import pandas as pd

    # Create sample data for testing
    sample_data = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 1],  # Duplicate ID
            "name": ["Alice", "Bob", "", "David", None, "Eve"],  # Missing values
            "amount": [100.0, -50.0, 75.0, 200.0, 150.0, 300.0],  # Negative value
            "created_date": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-03", "2025-01-01", "2024-01-05", "2024-01-06"]
            ),  # Future date
        }
    )

    # Example ETL pipeline monitoring
    def example_etl_pipeline():
        manager = get_etl_observability_manager()

        # Monitor extraction stage
        with manager.monitor_pipeline(
            "retail_etl", ETLStage.EXTRACTION, source="database"
        ) as metrics:
            print("Extracting data...")
            time.sleep(0.1)  # Simulate work
            metrics.records_processed = 1000
            metrics.bytes_processed = 50000

        # Monitor transformation stage with data quality assessment
        with manager.monitor_pipeline(
            "retail_etl", ETLStage.TRANSFORMATION, transformations=["clean", "normalize"]
        ) as metrics:
            print("Transforming data...")
            time.sleep(0.2)  # Simulate work

            # Assess data quality
            quality_results = manager.assess_data_quality(
                sample_data, "retail_etl", ETLStage.TRANSFORMATION
            )

            print("Data Quality Results:")
            for dimension, result in quality_results.items():
                print(
                    f"  {dimension}: {result.score:.3f} (Failed: {result.failed_records}/{result.total_records})"
                )

            metrics.records_processed = 950  # Some records failed transformation
            metrics.records_failed = 50

            # Store quality scores in metrics
            metrics.data_quality_scores = {
                dim: result.score for dim, result in quality_results.items()
            }

        # Monitor loading stage
        with manager.monitor_pipeline(
            "retail_etl", ETLStage.LOADING, target="warehouse"
        ) as metrics:
            print("Loading data...")
            time.sleep(0.1)  # Simulate work
            metrics.records_processed = 950

        # Get pipeline summary
        summary = manager.get_pipeline_metrics_summary("retail_etl", hours=1)
        print("\nPipeline Summary:")
        print(f"Success Rate: {summary['execution_summary']['success_rate']:.1%}")
        print(f"Avg Duration: {summary['performance_metrics']['avg_duration_ms']:.0f}ms")
        print(f"Health Score: {summary['health_score']['overall_score']:.3f}")

        # Check for alerts
        alerts = manager.get_recent_alerts(hours=1)
        if alerts:
            print(f"\nRecent Alerts: {len(alerts)}")
            for alert in alerts[:3]:  # Show first 3
                print(f"  {alert['severity']}: {alert['title']}")

    # Run example
    example_etl_pipeline()

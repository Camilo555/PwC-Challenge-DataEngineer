"""
Comprehensive ETL Pipeline Monitoring
Advanced monitoring system for ETL operations with data quality tracking,
performance analysis, and intelligent alerting.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union
from uuid import uuid4

import pandas as pd
from prometheus_client import Counter, Gauge, Histogram, Summary

from core.logging import get_logger
from monitoring.enterprise_prometheus_metrics import enterprise_metrics
from monitoring.comprehensive_distributed_tracing import distributed_tracer, SpanType

logger = get_logger(__name__)


class ETLStage(Enum):
    """ETL pipeline stages."""
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    VALIDATE = "validate"
    CLEANUP = "cleanup"
    POST_PROCESS = "post_process"


class DataQualityRule(Enum):
    """Data quality validation rules."""
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE_CHECK = "range_check"
    FORMAT_CHECK = "format_check"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    BUSINESS_RULE = "business_rule"
    SCHEMA_VALIDATION = "schema_validation"
    DUPLICATE_CHECK = "duplicate_check"


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class DataQualityCheck:
    """Data quality check result."""
    rule_name: str
    rule_type: DataQualityRule
    column_name: Optional[str] = None
    table_name: Optional[str] = None
    passed: bool = True
    total_records: int = 0
    failed_records: int = 0
    pass_rate: float = 100.0
    error_message: Optional[str] = None
    sample_failures: List[Any] = field(default_factory=list)
    execution_time_ms: float = 0.0


@dataclass
class ETLStageMetrics:
    """Metrics for individual ETL stage."""
    stage_name: str
    stage_type: ETLStage
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_ms: float = 0.0
    status: str = "running"
    
    # Data metrics
    input_records: int = 0
    output_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    skipped_records: int = 0
    
    # Performance metrics
    throughput_records_per_second: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    
    # Data size metrics
    input_size_mb: float = 0.0
    output_size_mb: float = 0.0
    compression_ratio: float = 0.0
    
    # Quality metrics
    data_quality_score: float = 100.0
    quality_checks: List[DataQualityCheck] = field(default_factory=list)
    
    # Error tracking
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class ETLJobMetrics:
    """Complete ETL job metrics."""
    job_id: str
    job_name: str
    pipeline_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_ms: float = 0.0
    status: str = "running"
    
    # Stage metrics
    stages: Dict[str, ETLStageMetrics] = field(default_factory=dict)
    
    # Overall metrics
    total_input_records: int = 0
    total_output_records: int = 0
    total_failed_records: int = 0
    overall_success_rate: float = 100.0
    overall_data_quality_score: float = 100.0
    
    # Performance metrics
    peak_memory_usage_mb: float = 0.0
    average_cpu_usage_percent: float = 0.0
    total_data_processed_mb: float = 0.0
    average_throughput_records_per_second: float = 0.0
    
    # Business context
    business_impact: Optional[str] = None
    cost_estimate: float = 0.0
    
    # Dependencies
    upstream_dependencies: List[str] = field(default_factory=list)
    downstream_dependencies: List[str] = field(default_factory=list)


@dataclass
class PipelineHealth:
    """Overall pipeline health metrics."""
    pipeline_name: str
    total_jobs: int = 0
    successful_jobs: int = 0
    failed_jobs: int = 0
    running_jobs: int = 0
    success_rate: float = 100.0
    
    average_job_duration_ms: float = 0.0
    average_throughput: float = 0.0
    average_data_quality_score: float = 100.0
    
    last_successful_run: Optional[datetime] = None
    last_failed_run: Optional[datetime] = None
    
    total_data_processed_gb: float = 0.0
    total_records_processed: int = 0
    
    # Resource utilization
    average_memory_usage_mb: float = 0.0
    peak_memory_usage_mb: float = 0.0
    average_cpu_usage_percent: float = 0.0
    
    # Data quality trends
    quality_trend_7d: List[float] = field(default_factory=list)
    performance_trend_7d: List[float] = field(default_factory=list)


class ComprehensiveETLMonitor:
    """Advanced ETL pipeline monitoring system."""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        
        # Job tracking
        self.active_jobs: Dict[str, ETLJobMetrics] = {}
        self.completed_jobs: deque[ETLJobMetrics] = deque(maxlen=1000)
        self.job_history: Dict[str, deque[ETLJobMetrics]] = defaultdict(lambda: deque(maxlen=100))
        
        # Pipeline health tracking
        self.pipeline_health: Dict[str, PipelineHealth] = {}
        
        # Performance tracking
        self.performance_history: deque = deque(maxlen=10000)
        self.quality_history: deque = deque(maxlen=10000)
        
        # Alert thresholds
        self.alert_thresholds = self._initialize_alert_thresholds()
        
        # Data quality rules
        self.quality_rules = self._initialize_quality_rules()
        
    def _initialize_alert_thresholds(self) -> Dict[str, Any]:
        """Initialize alert thresholds for ETL monitoring."""
        return {
            "job_duration_minutes": 120,        # 2 hours max job duration
            "success_rate_threshold": 95.0,     # 95% success rate
            "data_quality_threshold": 90.0,     # 90% data quality score
            "throughput_degradation": 50.0,     # 50% throughput degradation
            "memory_usage_threshold": 8192,     # 8GB memory usage
            "error_rate_threshold": 5.0,        # 5% error rate
            "data_freshness_hours": 6,          # Data should be fresh within 6 hours
            "queue_length_threshold": 10,       # Pipeline queue threshold
            "resource_utilization": 85.0,       # 85% resource utilization
            "sla_breach_minutes": 30            # SLA breach threshold
        }
    
    def _initialize_quality_rules(self) -> Dict[str, Any]:
        """Initialize default data quality rules."""
        return {
            "completeness_threshold": 95.0,     # 95% data completeness
            "accuracy_threshold": 98.0,         # 98% data accuracy
            "consistency_threshold": 99.0,      # 99% data consistency
            "uniqueness_threshold": 100.0,      # 100% uniqueness where required
            "validity_threshold": 95.0,         # 95% format validity
            "freshness_threshold": 24,          # Data should be within 24 hours
            "referential_integrity": 100.0,     # 100% referential integrity
            "business_rules_compliance": 90.0   # 90% business rules compliance
        }
    
    def start_job(
        self,
        job_name: str,
        pipeline_name: str,
        business_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Start tracking an ETL job."""
        
        job_id = str(uuid4())
        
        job_metrics = ETLJobMetrics(
            job_id=job_id,
            job_name=job_name,
            pipeline_name=pipeline_name,
            start_time=datetime.utcnow()
        )
        
        # Add business context
        if business_context:
            job_metrics.business_impact = business_context.get("business_impact")
            job_metrics.cost_estimate = business_context.get("cost_estimate", 0.0)
            job_metrics.upstream_dependencies = business_context.get("upstream_dependencies", [])
            job_metrics.downstream_dependencies = business_context.get("downstream_dependencies", [])
        
        self.active_jobs[job_id] = job_metrics
        
        # Update pipeline health
        if pipeline_name not in self.pipeline_health:
            self.pipeline_health[pipeline_name] = PipelineHealth(pipeline_name=pipeline_name)
        
        self.pipeline_health[pipeline_name].total_jobs += 1
        self.pipeline_health[pipeline_name].running_jobs += 1
        
        # Start distributed tracing
        business_tracing_context = {
            "job_name": job_name,
            "pipeline": pipeline_name,
            "job_id": job_id,
            "business_value": "1.8M"  # Data Quality story value
        }
        
        distributed_tracer.trace_etl_operation(
            job_name=job_name,
            stage="job_start",
            duration_ms=0.0
        )
        
        # Update Prometheus metrics
        enterprise_metrics.etl_records_processed.labels(
            job_name=job_name,
            table="all",
            operation="start"
        ).inc(0)
        
        self.logger.info(f"Started ETL job tracking: {job_name} (ID: {job_id})")
        return job_id
    
    def start_stage(
        self,
        job_id: str,
        stage_name: str,
        stage_type: ETLStage,
        input_records: int = 0,
        input_size_mb: float = 0.0
    ) -> str:
        """Start tracking an ETL stage."""
        
        if job_id not in self.active_jobs:
            raise ValueError(f"Job {job_id} not found")
        
        job = self.active_jobs[job_id]
        
        stage_metrics = ETLStageMetrics(
            stage_name=stage_name,
            stage_type=stage_type,
            start_time=datetime.utcnow(),
            input_records=input_records,
            input_size_mb=input_size_mb
        )
        
        job.stages[stage_name] = stage_metrics
        
        # Start tracing for this stage
        distributed_tracer.trace_etl_operation(
            job_name=job.job_name,
            stage=stage_name,
            records_processed=input_records,
            data_size_mb=input_size_mb,
            duration_ms=0.0
        )
        
        self.logger.debug(f"Started ETL stage: {stage_name} for job {job.job_name}")
        return stage_name
    
    def update_stage_progress(
        self,
        job_id: str,
        stage_name: str,
        processed_records: Optional[int] = None,
        failed_records: Optional[int] = None,
        memory_usage_mb: Optional[float] = None,
        cpu_usage_percent: Optional[float] = None
    ):
        """Update progress for an ETL stage."""
        
        if job_id not in self.active_jobs:
            return
        
        job = self.active_jobs[job_id]
        if stage_name not in job.stages:
            return
        
        stage = job.stages[stage_name]
        
        if processed_records is not None:
            stage.processed_records = processed_records
            
            # Calculate throughput
            if stage.start_time:
                elapsed_seconds = (datetime.utcnow() - stage.start_time).total_seconds()
                if elapsed_seconds > 0:
                    stage.throughput_records_per_second = processed_records / elapsed_seconds
        
        if failed_records is not None:
            stage.failed_records = failed_records
        
        if memory_usage_mb is not None:
            stage.memory_usage_mb = memory_usage_mb
            job.peak_memory_usage_mb = max(job.peak_memory_usage_mb, memory_usage_mb)
        
        if cpu_usage_percent is not None:
            stage.cpu_usage_percent = cpu_usage_percent
            
            # Update job average
            cpu_values = [s.cpu_usage_percent for s in job.stages.values() if s.cpu_usage_percent > 0]
            if cpu_values:
                job.average_cpu_usage_percent = sum(cpu_values) / len(cpu_values)
    
    def add_quality_check(
        self,
        job_id: str,
        stage_name: str,
        quality_check: DataQualityCheck
    ):
        """Add a data quality check result to a stage."""
        
        if job_id not in self.active_jobs:
            return
        
        job = self.active_jobs[job_id]
        if stage_name not in job.stages:
            return
        
        stage = job.stages[stage_name]
        stage.quality_checks.append(quality_check)
        
        # Update stage quality score
        if stage.quality_checks:
            total_pass_rate = sum(check.pass_rate for check in stage.quality_checks)
            stage.data_quality_score = total_pass_rate / len(stage.quality_checks)
        
        # Record quality check metrics
        self.quality_history.append({
            'timestamp': datetime.utcnow(),
            'job_id': job_id,
            'stage_name': stage_name,
            'rule_name': quality_check.rule_name,
            'rule_type': quality_check.rule_type.value,
            'pass_rate': quality_check.pass_rate,
            'failed_records': quality_check.failed_records,
            'total_records': quality_check.total_records
        })
        
        # Update Prometheus metrics
        enterprise_metrics.etl_data_quality_score.labels(
            job_name=job.job_name,
            table=quality_check.table_name or 'unknown',
            quality_rule=quality_check.rule_type.value
        ).set(quality_check.pass_rate)
        
        if not quality_check.passed:
            self.logger.warning(
                f"Data quality check failed: {quality_check.rule_name} "
                f"({quality_check.pass_rate:.1f}% pass rate)"
            )
    
    def finish_stage(
        self,
        job_id: str,
        stage_name: str,
        output_records: Optional[int] = None,
        output_size_mb: Optional[float] = None,
        status: str = "completed",
        errors: Optional[List[str]] = None
    ):
        """Finish tracking an ETL stage."""
        
        if job_id not in self.active_jobs:
            return
        
        job = self.active_jobs[job_id]
        if stage_name not in job.stages:
            return
        
        stage = job.stages[stage_name]
        stage.end_time = datetime.utcnow()
        stage.duration_ms = (stage.end_time - stage.start_time).total_seconds() * 1000
        stage.status = status
        
        if output_records is not None:
            stage.output_records = output_records
        
        if output_size_mb is not None:
            stage.output_size_mb = output_size_mb
            
            # Calculate compression ratio
            if stage.input_size_mb > 0:
                stage.compression_ratio = stage.input_size_mb / stage.output_size_mb
        
        if errors:
            stage.errors.extend(errors)
        
        # Final throughput calculation
        if stage.duration_ms > 0 and stage.processed_records > 0:
            stage.throughput_records_per_second = (stage.processed_records * 1000) / stage.duration_ms
        
        # Record performance metrics
        self.performance_history.append({
            'timestamp': datetime.utcnow(),
            'job_id': job_id,
            'stage_name': stage_name,
            'stage_type': stage.stage_type.value,
            'duration_ms': stage.duration_ms,
            'throughput': stage.throughput_records_per_second,
            'input_records': stage.input_records,
            'output_records': stage.output_records,
            'memory_usage_mb': stage.memory_usage_mb,
            'data_quality_score': stage.data_quality_score
        })
        
        # Update Prometheus metrics
        enterprise_metrics.etl_job_duration.labels(
            job_name=job.job_name,
            stage=stage_name,
            status=status
        ).observe(stage.duration_ms / 1000)  # Convert to seconds
        
        enterprise_metrics.etl_throughput.labels(
            job_name=job.job_name,
            stage=stage_name
        ).set(stage.throughput_records_per_second)
        
        distributed_tracer.trace_etl_operation(
            job_name=job.job_name,
            stage=stage_name,
            records_processed=stage.processed_records,
            records_failed=stage.failed_records,
            data_size_mb=stage.output_size_mb,
            duration_ms=stage.duration_ms
        )
        
        self.logger.info(
            f"Completed ETL stage: {stage_name} for job {job.job_name} "
            f"({stage.duration_ms:.0f}ms, {stage.throughput_records_per_second:.1f} records/s)"
        )
    
    def finish_job(
        self,
        job_id: str,
        status: str = "completed",
        total_output_records: Optional[int] = None,
        errors: Optional[List[str]] = None
    ):
        """Finish tracking an ETL job."""
        
        if job_id not in self.active_jobs:
            return
        
        job = self.active_jobs[job_id]
        job.end_time = datetime.utcnow()
        job.duration_ms = (job.end_time - job.start_time).total_seconds() * 1000
        job.status = status
        
        if total_output_records is not None:
            job.total_output_records = total_output_records
        
        # Calculate overall metrics from stages
        job.total_input_records = sum(stage.input_records for stage in job.stages.values())
        job.total_failed_records = sum(stage.failed_records for stage in job.stages.values())
        
        if job.total_input_records > 0:
            job.overall_success_rate = ((job.total_input_records - job.total_failed_records) 
                                      / job.total_input_records) * 100
        
        # Calculate overall data quality score
        quality_scores = [stage.data_quality_score for stage in job.stages.values() 
                         if stage.quality_checks]
        if quality_scores:
            job.overall_data_quality_score = sum(quality_scores) / len(quality_scores)
        
        # Calculate average throughput
        throughputs = [stage.throughput_records_per_second for stage in job.stages.values() 
                      if stage.throughput_records_per_second > 0]
        if throughputs:
            job.average_throughput_records_per_second = sum(throughputs) / len(throughputs)
        
        # Calculate total data processed
        job.total_data_processed_mb = sum(stage.output_size_mb for stage in job.stages.values())
        
        # Update pipeline health
        pipeline = self.pipeline_health[job.pipeline_name]
        pipeline.running_jobs -= 1
        
        if status == "completed":
            pipeline.successful_jobs += 1
            pipeline.last_successful_run = job.end_time
        else:
            pipeline.failed_jobs += 1
            pipeline.last_failed_run = job.end_time
        
        pipeline.success_rate = (pipeline.successful_jobs / pipeline.total_jobs) * 100
        pipeline.total_data_processed_gb += job.total_data_processed_mb / 1024
        pipeline.total_records_processed += job.total_output_records or 0
        
        # Update running averages
        completed_jobs = self.job_history[job.pipeline_name]
        if completed_jobs:
            durations = [j.duration_ms for j in completed_jobs]
            throughputs = [j.average_throughput_records_per_second for j in completed_jobs if j.average_throughput_records_per_second > 0]
            quality_scores = [j.overall_data_quality_score for j in completed_jobs]
            
            pipeline.average_job_duration_ms = sum(durations) / len(durations) if durations else 0
            pipeline.average_throughput = sum(throughputs) / len(throughputs) if throughputs else 0
            pipeline.average_data_quality_score = sum(quality_scores) / len(quality_scores) if quality_scores else 100
        
        # Move job to completed
        self.completed_jobs.append(job)
        self.job_history[job.pipeline_name].append(job)
        del self.active_jobs[job_id]
        
        # Update final Prometheus metrics
        enterprise_metrics.etl_records_processed.labels(
            job_name=job.job_name,
            table="all",
            operation="completed"
        ).inc(job.total_output_records or 0)
        
        self.logger.info(
            f"Completed ETL job: {job.job_name} ({job.duration_ms:.0f}ms, "
            f"{job.overall_success_rate:.1f}% success rate, "
            f"{job.overall_data_quality_score:.1f}% quality score)"
        )
    
    async def run_data_quality_checks(
        self,
        data: pd.DataFrame,
        table_name: str,
        quality_rules: Optional[Dict[str, Any]] = None
    ) -> List[DataQualityCheck]:
        """Run comprehensive data quality checks on a DataFrame."""
        
        checks = []
        rules = quality_rules or self.quality_rules
        
        try:
            # Completeness checks
            for column in data.columns:
                null_count = data[column].isnull().sum()
                total_count = len(data)
                pass_rate = ((total_count - null_count) / total_count) * 100 if total_count > 0 else 100
                
                check = DataQualityCheck(
                    rule_name=f"{column}_completeness",
                    rule_type=DataQualityRule.NOT_NULL,
                    column_name=column,
                    table_name=table_name,
                    passed=pass_rate >= rules.get("completeness_threshold", 95),
                    total_records=total_count,
                    failed_records=null_count,
                    pass_rate=pass_rate
                )
                
                if null_count > 0:
                    check.error_message = f"{null_count} null values found in {column}"
                
                checks.append(check)
            
            # Uniqueness checks for columns that should be unique
            for column in data.columns:
                if column.lower().endswith('_id') or column.lower() in ['id', 'email', 'username']:
                    duplicate_count = data[column].duplicated().sum()
                    total_count = len(data)
                    pass_rate = ((total_count - duplicate_count) / total_count) * 100 if total_count > 0 else 100
                    
                    check = DataQualityCheck(
                        rule_name=f"{column}_uniqueness",
                        rule_type=DataQualityRule.UNIQUE,
                        column_name=column,
                        table_name=table_name,
                        passed=pass_rate >= rules.get("uniqueness_threshold", 100),
                        total_records=total_count,
                        failed_records=duplicate_count,
                        pass_rate=pass_rate
                    )
                    
                    if duplicate_count > 0:
                        check.error_message = f"{duplicate_count} duplicate values found in {column}"
                        check.sample_failures = data[data[column].duplicated()][column].head(5).tolist()
                    
                    checks.append(check)
            
            # Format validation for common data types
            for column in data.columns:
                if data[column].dtype == 'object':  # String columns
                    # Email format check
                    if 'email' in column.lower():
                        import re
                        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                        valid_emails = data[column].str.match(email_pattern, na=False)
                        invalid_count = (~valid_emails).sum()
                        total_count = data[column].notna().sum()
                        pass_rate = ((total_count - invalid_count) / total_count) * 100 if total_count > 0 else 100
                        
                        check = DataQualityCheck(
                            rule_name=f"{column}_email_format",
                            rule_type=DataQualityRule.FORMAT_CHECK,
                            column_name=column,
                            table_name=table_name,
                            passed=pass_rate >= rules.get("validity_threshold", 95),
                            total_records=total_count,
                            failed_records=invalid_count,
                            pass_rate=pass_rate
                        )
                        
                        if invalid_count > 0:
                            check.error_message = f"{invalid_count} invalid email formats found"
                            check.sample_failures = data[~valid_emails][column].head(3).tolist()
                        
                        checks.append(check)
            
            # Range checks for numeric columns
            for column in data.select_dtypes(include=['number']).columns:
                # Check for reasonable ranges based on column name
                if 'age' in column.lower():
                    out_of_range = (data[column] < 0) | (data[column] > 150)
                    invalid_count = out_of_range.sum()
                    total_count = data[column].notna().sum()
                    pass_rate = ((total_count - invalid_count) / total_count) * 100 if total_count > 0 else 100
                    
                    check = DataQualityCheck(
                        rule_name=f"{column}_age_range",
                        rule_type=DataQualityRule.RANGE_CHECK,
                        column_name=column,
                        table_name=table_name,
                        passed=pass_rate >= rules.get("validity_threshold", 95),
                        total_records=total_count,
                        failed_records=invalid_count,
                        pass_rate=pass_rate
                    )
                    
                    if invalid_count > 0:
                        check.error_message = f"{invalid_count} age values outside valid range (0-150)"
                        check.sample_failures = data[out_of_range][column].head(3).tolist()
                    
                    checks.append(check)
            
            # Overall duplicate record check
            duplicate_rows = data.duplicated().sum()
            total_rows = len(data)
            pass_rate = ((total_rows - duplicate_rows) / total_rows) * 100 if total_rows > 0 else 100
            
            check = DataQualityCheck(
                rule_name="row_uniqueness",
                rule_type=DataQualityRule.DUPLICATE_CHECK,
                table_name=table_name,
                passed=pass_rate >= rules.get("uniqueness_threshold", 100),
                total_records=total_rows,
                failed_records=duplicate_rows,
                pass_rate=pass_rate
            )
            
            if duplicate_rows > 0:
                check.error_message = f"{duplicate_rows} duplicate rows found"
            
            checks.append(check)
            
        except Exception as e:
            self.logger.error(f"Error running data quality checks: {e}")
            
            # Create an error check
            error_check = DataQualityCheck(
                rule_name="quality_check_error",
                rule_type=DataQualityRule.SCHEMA_VALIDATION,
                table_name=table_name,
                passed=False,
                error_message=str(e)
            )
            checks.append(error_check)
        
        return checks
    
    async def detect_performance_anomalies(self) -> List[Dict[str, Any]]:
        """Detect performance anomalies in ETL pipelines."""
        
        anomalies = []
        
        try:
            # Analyze recent performance history
            recent_window = datetime.utcnow() - timedelta(hours=24)
            recent_performance = [
                p for p in self.performance_history 
                if p['timestamp'] > recent_window
            ]
            
            if len(recent_performance) < 10:  # Need sufficient data
                return anomalies
            
            # Group by job and stage
            job_performance = defaultdict(list)
            for perf in recent_performance:
                key = f"{perf.get('job_id', 'unknown')}_{perf.get('stage_name', 'unknown')}"
                job_performance[key].append(perf)
            
            # Detect throughput anomalies
            for key, performances in job_performance.items():
                if len(performances) < 5:  # Need sufficient samples
                    continue
                
                throughputs = [p['throughput'] for p in performances if p['throughput'] > 0]
                if len(throughputs) < 3:
                    continue
                
                # Calculate statistics
                avg_throughput = sum(throughputs) / len(throughputs)
                recent_throughputs = throughputs[-3:]  # Last 3 measurements
                recent_avg = sum(recent_throughputs) / len(recent_throughputs)
                
                # Check for significant degradation
                degradation_percent = ((avg_throughput - recent_avg) / avg_throughput) * 100
                if degradation_percent > self.alert_thresholds["throughput_degradation"]:
                    anomalies.append({
                        "type": "throughput_degradation",
                        "severity": AlertLevel.WARNING.value,
                        "job_stage": key,
                        "degradation_percent": degradation_percent,
                        "average_throughput": avg_throughput,
                        "recent_throughput": recent_avg,
                        "message": f"Throughput degraded by {degradation_percent:.1f}% for {key}"
                    })
            
            # Detect duration anomalies
            for key, performances in job_performance.items():
                durations = [p['duration_ms'] for p in performances]
                if len(durations) < 5:
                    continue
                
                avg_duration = sum(durations) / len(durations)
                recent_durations = durations[-3:]
                recent_avg = sum(recent_durations) / len(recent_durations)
                
                # Check for significant increase in duration
                increase_percent = ((recent_avg - avg_duration) / avg_duration) * 100
                if increase_percent > 50:  # 50% increase threshold
                    anomalies.append({
                        "type": "duration_increase",
                        "severity": AlertLevel.WARNING.value,
                        "job_stage": key,
                        "increase_percent": increase_percent,
                        "average_duration_ms": avg_duration,
                        "recent_duration_ms": recent_avg,
                        "message": f"Duration increased by {increase_percent:.1f}% for {key}"
                    })
            
            # Check pipeline-level anomalies
            for pipeline_name, health in self.pipeline_health.items():
                # Success rate anomaly
                if health.success_rate < self.alert_thresholds["success_rate_threshold"]:
                    anomalies.append({
                        "type": "low_success_rate",
                        "severity": AlertLevel.ERROR.value,
                        "pipeline": pipeline_name,
                        "success_rate": health.success_rate,
                        "threshold": self.alert_thresholds["success_rate_threshold"],
                        "message": f"Success rate {health.success_rate:.1f}% below threshold for {pipeline_name}"
                    })
                
                # Data quality anomaly
                if health.average_data_quality_score < self.alert_thresholds["data_quality_threshold"]:
                    anomalies.append({
                        "type": "low_data_quality",
                        "severity": AlertLevel.WARNING.value,
                        "pipeline": pipeline_name,
                        "quality_score": health.average_data_quality_score,
                        "threshold": self.alert_thresholds["data_quality_threshold"],
                        "message": f"Data quality score {health.average_data_quality_score:.1f}% below threshold for {pipeline_name}"
                    })
            
            # Check for long-running jobs
            for job_id, job in self.active_jobs.items():
                job_duration_minutes = (datetime.utcnow() - job.start_time).total_seconds() / 60
                if job_duration_minutes > self.alert_thresholds["job_duration_minutes"]:
                    anomalies.append({
                        "type": "long_running_job",
                        "severity": AlertLevel.WARNING.value,
                        "job_id": job_id,
                        "job_name": job.job_name,
                        "duration_minutes": job_duration_minutes,
                        "threshold": self.alert_thresholds["job_duration_minutes"],
                        "message": f"Job {job.job_name} running for {job_duration_minutes:.1f} minutes"
                    })
            
        except Exception as e:
            self.logger.error(f"Error detecting performance anomalies: {e}")
        
        return anomalies
    
    def get_pipeline_summary(self, pipeline_name: str) -> Dict[str, Any]:
        """Get comprehensive summary for a specific pipeline."""
        
        if pipeline_name not in self.pipeline_health:
            return {"error": f"Pipeline {pipeline_name} not found"}
        
        health = self.pipeline_health[pipeline_name]
        
        # Get recent jobs for this pipeline
        recent_jobs = [
            {
                "job_id": job.job_id,
                "job_name": job.job_name,
                "status": job.status,
                "duration_ms": job.duration_ms,
                "success_rate": job.overall_success_rate,
                "data_quality_score": job.overall_data_quality_score,
                "start_time": job.start_time.isoformat(),
                "end_time": job.end_time.isoformat() if job.end_time else None
            }
            for job in self.job_history[pipeline_name]
        ]
        
        # Get active jobs for this pipeline
        active_jobs = [
            {
                "job_id": job.job_id,
                "job_name": job.job_name,
                "status": job.status,
                "start_time": job.start_time.isoformat(),
                "stages": list(job.stages.keys()),
                "duration_minutes": (datetime.utcnow() - job.start_time).total_seconds() / 60
            }
            for job in self.active_jobs.values()
            if job.pipeline_name == pipeline_name
        ]
        
        return {
            "pipeline_name": pipeline_name,
            "health": {
                "success_rate": health.success_rate,
                "average_data_quality_score": health.average_data_quality_score,
                "average_job_duration_ms": health.average_job_duration_ms,
                "average_throughput": health.average_throughput,
                "total_data_processed_gb": health.total_data_processed_gb,
                "total_records_processed": health.total_records_processed
            },
            "job_counts": {
                "total": health.total_jobs,
                "successful": health.successful_jobs,
                "failed": health.failed_jobs,
                "running": health.running_jobs
            },
            "last_runs": {
                "last_successful": health.last_successful_run.isoformat() if health.last_successful_run else None,
                "last_failed": health.last_failed_run.isoformat() if health.last_failed_run else None
            },
            "recent_jobs": recent_jobs[-10:],  # Last 10 jobs
            "active_jobs": active_jobs,
            "resource_usage": {
                "average_memory_mb": health.average_memory_usage_mb,
                "peak_memory_mb": health.peak_memory_usage_mb,
                "average_cpu_percent": health.average_cpu_usage_percent
            }
        }
    
    def get_monitoring_dashboard_data(self) -> Dict[str, Any]:
        """Get data for monitoring dashboard."""
        
        # Overall statistics
        total_active = len(self.active_jobs)
        total_pipelines = len(self.pipeline_health)
        
        # Pipeline health summary
        pipeline_summaries = []
        for pipeline_name, health in self.pipeline_health.items():
            pipeline_summaries.append({
                "name": pipeline_name,
                "success_rate": health.success_rate,
                "running_jobs": health.running_jobs,
                "data_quality_score": health.average_data_quality_score,
                "last_successful": health.last_successful_run.isoformat() if health.last_successful_run else None
            })
        
        # Recent performance trends
        recent_performance = list(self.performance_history)[-100:]  # Last 100 entries
        recent_quality = list(self.quality_history)[-100:]
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "overview": {
                "total_active_jobs": total_active,
                "total_pipelines": total_pipelines,
                "total_completed_jobs": len(self.completed_jobs)
            },
            "pipelines": pipeline_summaries,
            "recent_performance": recent_performance,
            "recent_quality": recent_quality,
            "alert_thresholds": self.alert_thresholds,
            "active_jobs_summary": [
                {
                    "job_id": job.job_id,
                    "job_name": job.job_name,
                    "pipeline": job.pipeline_name,
                    "duration_minutes": (datetime.utcnow() - job.start_time).total_seconds() / 60,
                    "stages_completed": len([s for s in job.stages.values() if s.status == "completed"])
                }
                for job in self.active_jobs.values()
            ]
        }


# Global ETL monitor instance
etl_monitor = ComprehensiveETLMonitor()


# Context manager for automatic ETL job tracking
class ETLJobTracker:
    """Context manager for automatic ETL job tracking."""
    
    def __init__(
        self,
        job_name: str,
        pipeline_name: str,
        business_context: Optional[Dict[str, Any]] = None
    ):
        self.job_name = job_name
        self.pipeline_name = pipeline_name
        self.business_context = business_context
        self.job_id: Optional[str] = None
    
    def __enter__(self):
        self.job_id = etl_monitor.start_job(
            self.job_name,
            self.pipeline_name,
            self.business_context
        )
        return self.job_id
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.job_id:
            status = "completed" if exc_type is None else "failed"
            errors = [str(exc_val)] if exc_val else None
            etl_monitor.finish_job(self.job_id, status, errors=errors)


class ETLStageTracker:
    """Context manager for automatic ETL stage tracking."""
    
    def __init__(
        self,
        job_id: str,
        stage_name: str,
        stage_type: ETLStage,
        input_records: int = 0,
        input_size_mb: float = 0.0
    ):
        self.job_id = job_id
        self.stage_name = stage_name
        self.stage_type = stage_type
        self.input_records = input_records
        self.input_size_mb = input_size_mb
    
    def __enter__(self):
        etl_monitor.start_stage(
            self.job_id,
            self.stage_name,
            self.stage_type,
            self.input_records,
            self.input_size_mb
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        status = "completed" if exc_type is None else "failed"
        errors = [str(exc_val)] if exc_val else None
        etl_monitor.finish_stage(
            self.job_id,
            self.stage_name,
            status=status,
            errors=errors
        )
    
    def update_progress(
        self,
        processed_records: Optional[int] = None,
        failed_records: Optional[int] = None,
        memory_usage_mb: Optional[float] = None,
        cpu_usage_percent: Optional[float] = None
    ):
        """Update stage progress."""
        etl_monitor.update_stage_progress(
            self.job_id,
            self.stage_name,
            processed_records,
            failed_records,
            memory_usage_mb,
            cpu_usage_percent
        )
    
    def add_quality_check(self, quality_check: DataQualityCheck):
        """Add quality check to stage."""
        etl_monitor.add_quality_check(self.job_id, self.stage_name, quality_check)


__all__ = [
    'ComprehensiveETLMonitor',
    'etl_monitor',
    'ETLJobTracker',
    'ETLStageTracker',
    'ETLStage',
    'DataQualityRule',
    'DataQualityCheck',
    'ETLStageMetrics',
    'ETLJobMetrics',
    'PipelineHealth',
    'AlertLevel'
]
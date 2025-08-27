"""
Advanced ETL Performance Monitoring System
Provides comprehensive monitoring for data pipeline performance, data quality,
and ETL success rates with automated alerting and performance optimization insights.
"""

import asyncio
import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set
from pathlib import Path
import sqlite3
import threading

from core.logging import get_logger
from monitoring.prometheus_metrics import get_prometheus_collector
from monitoring.enterprise_observability import get_observability_platform

logger = get_logger(__name__)


class ETLStage(Enum):
    """ETL pipeline stages."""
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    VALIDATE = "validate"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class DataQualityStatus(Enum):
    """Data quality status levels."""
    EXCELLENT = "excellent"  # >95% quality score
    GOOD = "good"           # 80-95% quality score
    FAIR = "fair"           # 60-80% quality score
    POOR = "poor"           # <60% quality score
    FAILED = "failed"       # Validation failed


@dataclass
class ETLPerformanceMetrics:
    """ETL performance metrics for a single run."""
    job_id: str
    job_name: str
    stage: ETLStage
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    records_processed: int = 0
    records_failed: int = 0
    records_skipped: int = 0
    throughput_records_per_second: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    cpu_usage_percent: Optional[float] = None
    data_size_mb: Optional[float] = None
    success: bool = True
    error_message: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class DataQualityMetrics:
    """Data quality assessment metrics."""
    dataset_name: str
    table_name: str
    stage: ETLStage
    total_records: int
    valid_records: int
    invalid_records: int
    duplicate_records: int
    null_percentage: float
    completeness_score: float  # 0-100
    accuracy_score: float     # 0-100
    consistency_score: float  # 0-100
    timeliness_score: float   # 0-100
    overall_quality_score: float  # 0-100
    quality_status: DataQualityStatus
    quality_issues: List[str] = field(default_factory=list)
    check_timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ETLJobStatus:
    """Overall ETL job status and metrics."""
    job_name: str
    job_id: str
    pipeline_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_duration_seconds: Optional[float] = None
    stages_completed: List[str] = field(default_factory=list)
    stages_failed: List[str] = field(default_factory=list)
    total_records_processed: int = 0
    total_records_failed: int = 0
    success_rate_percent: float = 100.0
    performance_score: float = 100.0  # 0-100 based on SLA targets
    data_quality_score: float = 100.0  # 0-100 overall data quality
    sla_breach: bool = False
    cost_estimate_dollars: Optional[float] = None
    environment: str = "production"


@dataclass
class PipelineHealthMetrics:
    """Pipeline health and trend analysis."""
    pipeline_name: str
    success_rate_24h: float
    success_rate_7d: float
    average_duration_24h: float
    average_duration_7d: float
    data_quality_trend: str  # improving, stable, degrading
    performance_trend: str   # improving, stable, degrading
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    failure_count_24h: int = 0
    sla_compliance_percent: float = 100.0
    recommendations: List[str] = field(default_factory=list)


class ETLPerformanceMonitor:
    """Advanced ETL performance monitoring and analysis system."""

    def __init__(self):
        self.prometheus = get_prometheus_collector()
        self.observability = get_observability_platform()
        self.logger = get_logger(self.__class__.__name__)
        
        # Monitoring configuration
        self.sla_targets = {
            "bronze_max_duration_minutes": 30,
            "silver_max_duration_minutes": 45,
            "gold_max_duration_minutes": 60,
            "min_success_rate_percent": 95.0,
            "max_data_freshness_hours": 2,
            "min_data_quality_score": 90.0
        }
        
        # In-memory tracking
        self.active_jobs: Dict[str, ETLJobStatus] = {}
        self.job_history: Dict[str, List[ETLJobStatus]] = {}
        self.performance_metrics: Dict[str, List[ETLPerformanceMetrics]] = {}
        self.data_quality_history: Dict[str, List[DataQualityMetrics]] = {}
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Initialize storage
        self._init_storage()

    def _init_storage(self):
        """Initialize SQLite storage for ETL monitoring data."""
        try:
            self.db_path = Path("./data/metrics/etl_monitoring.db")
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            with sqlite3.connect(str(self.db_path)) as conn:
                # ETL job status table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS etl_job_status (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        job_id TEXT NOT NULL,
                        job_name TEXT NOT NULL,
                        pipeline_name TEXT NOT NULL,
                        start_time TEXT NOT NULL,
                        end_time TEXT,
                        total_duration_seconds REAL,
                        stages_completed TEXT,
                        stages_failed TEXT,
                        total_records_processed INTEGER,
                        total_records_failed INTEGER,
                        success_rate_percent REAL,
                        performance_score REAL,
                        data_quality_score REAL,
                        sla_breach BOOLEAN,
                        environment TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # ETL performance metrics table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS etl_performance_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        job_id TEXT NOT NULL,
                        job_name TEXT NOT NULL,
                        stage TEXT NOT NULL,
                        start_time TEXT NOT NULL,
                        end_time TEXT,
                        duration_seconds REAL,
                        records_processed INTEGER,
                        records_failed INTEGER,
                        records_skipped INTEGER,
                        throughput_records_per_second REAL,
                        memory_usage_mb REAL,
                        cpu_usage_percent REAL,
                        data_size_mb REAL,
                        success BOOLEAN,
                        error_message TEXT,
                        tags TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Data quality metrics table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS data_quality_metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        dataset_name TEXT NOT NULL,
                        table_name TEXT NOT NULL,
                        stage TEXT NOT NULL,
                        total_records INTEGER,
                        valid_records INTEGER,
                        invalid_records INTEGER,
                        duplicate_records INTEGER,
                        null_percentage REAL,
                        completeness_score REAL,
                        accuracy_score REAL,
                        consistency_score REAL,
                        timeliness_score REAL,
                        overall_quality_score REAL,
                        quality_status TEXT,
                        quality_issues TEXT,
                        check_timestamp TEXT,
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Pipeline health trends table
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS pipeline_health_trends (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pipeline_name TEXT NOT NULL,
                        success_rate_24h REAL,
                        success_rate_7d REAL,
                        average_duration_24h REAL,
                        average_duration_7d REAL,
                        data_quality_trend TEXT,
                        performance_trend TEXT,
                        last_success TEXT,
                        last_failure TEXT,
                        failure_count_24h INTEGER,
                        sla_compliance_percent REAL,
                        recommendations TEXT,
                        snapshot_time TEXT DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.commit()
                
        except Exception as e:
            self.logger.error(f"Failed to initialize ETL monitoring storage: {e}")

    def start_etl_job(self, job_name: str, pipeline_name: str, 
                     environment: str = "production", **tags) -> str:
        """Start tracking an ETL job."""
        job_id = f"{job_name}_{int(time.time())}_{hash(pipeline_name) % 1000:03d}"
        
        job_status = ETLJobStatus(
            job_name=job_name,
            job_id=job_id,
            pipeline_name=pipeline_name,
            start_time=datetime.utcnow(),
            environment=environment
        )
        
        with self.lock:
            self.active_jobs[job_id] = job_status
        
        # Record start metrics
        self.prometheus.increment_counter(
            "etl_jobs_started_total",
            1,
            {"job_name": job_name, "pipeline": pipeline_name, "environment": environment}
        )
        
        # Record business metrics
        self.observability.record_metric(
            "etl_job_started",
            1,
            self.observability.MetricCategory.BUSINESS,
            {"job_name": job_name, "pipeline": pipeline_name, **tags}
        )
        
        self.logger.info(f"Started ETL job tracking: {job_id} ({job_name})")
        return job_id

    def start_stage(self, job_id: str, stage: ETLStage, **tags) -> ETLPerformanceMetrics:
        """Start tracking an ETL stage."""
        if job_id not in self.active_jobs:
            raise ValueError(f"Job {job_id} not found in active jobs")
        
        job = self.active_jobs[job_id]
        
        stage_metrics = ETLPerformanceMetrics(
            job_id=job_id,
            job_name=job.job_name,
            stage=stage,
            start_time=datetime.utcnow(),
            tags=tags
        )
        
        # Store stage metrics
        stage_key = f"{job_id}_{stage.value}"
        with self.lock:
            if job.job_name not in self.performance_metrics:
                self.performance_metrics[job.job_name] = []
            self.performance_metrics[job.job_name].append(stage_metrics)
        
        # Record stage start metrics
        self.prometheus.increment_counter(
            "etl_stage_started_total",
            1,
            {"job_name": job.job_name, "stage": stage.value, "pipeline": job.pipeline_name}
        )
        
        self.logger.info(f"Started ETL stage: {stage.value} for job {job_id}")
        return stage_metrics

    def complete_stage(self, job_id: str, stage: ETLStage, 
                      records_processed: int = 0, records_failed: int = 0,
                      records_skipped: int = 0, memory_usage_mb: Optional[float] = None,
                      cpu_usage_percent: Optional[float] = None, 
                      data_size_mb: Optional[float] = None,
                      success: bool = True, error_message: Optional[str] = None):
        """Complete tracking of an ETL stage."""
        if job_id not in self.active_jobs:
            raise ValueError(f"Job {job_id} not found in active jobs")
        
        job = self.active_jobs[job_id]
        
        # Find the corresponding stage metrics
        stage_metrics = None
        with self.lock:
            if job.job_name in self.performance_metrics:
                for metrics in reversed(self.performance_metrics[job.job_name]):
                    if metrics.job_id == job_id and metrics.stage == stage and metrics.end_time is None:
                        stage_metrics = metrics
                        break
        
        if not stage_metrics:
            self.logger.warning(f"No active stage metrics found for {job_id} stage {stage.value}")
            return
        
        # Complete stage metrics
        end_time = datetime.utcnow()
        stage_metrics.end_time = end_time
        stage_metrics.duration_seconds = (end_time - stage_metrics.start_time).total_seconds()
        stage_metrics.records_processed = records_processed
        stage_metrics.records_failed = records_failed
        stage_metrics.records_skipped = records_skipped
        stage_metrics.memory_usage_mb = memory_usage_mb
        stage_metrics.cpu_usage_percent = cpu_usage_percent
        stage_metrics.data_size_mb = data_size_mb
        stage_metrics.success = success
        stage_metrics.error_message = error_message
        
        # Calculate throughput
        if stage_metrics.duration_seconds > 0:
            stage_metrics.throughput_records_per_second = records_processed / stage_metrics.duration_seconds
        
        # Update job status
        with self.lock:
            if success:
                job.stages_completed.append(stage.value)
            else:
                job.stages_failed.append(stage.value)
            
            job.total_records_processed += records_processed
            job.total_records_failed += records_failed
        
        # Record completion metrics
        self.prometheus.observe_histogram(
            "etl_stage_duration_seconds",
            stage_metrics.duration_seconds,
            {"job_name": job.job_name, "stage": stage.value, "status": "success" if success else "failed"}
        )
        
        self.prometheus.increment_counter(
            "etl_records_processed_total",
            records_processed,
            {"job_name": job.job_name, "stage": stage.value, "status": "processed"}
        )
        
        if records_failed > 0:
            self.prometheus.increment_counter(
                "etl_records_processed_total",
                records_failed,
                {"job_name": job.job_name, "stage": stage.value, "status": "failed"}
            )
        
        # Store to database
        self._store_stage_metrics(stage_metrics)
        
        self.logger.info(f"Completed ETL stage: {stage.value} for job {job_id} "
                        f"({records_processed} records, {stage_metrics.duration_seconds:.2f}s)")

    def complete_job(self, job_id: str, success: bool = True):
        """Complete tracking of an ETL job."""
        if job_id not in self.active_jobs:
            raise ValueError(f"Job {job_id} not found in active jobs")
        
        with self.lock:
            job = self.active_jobs[job_id]
            job.end_time = datetime.utcnow()
            job.total_duration_seconds = (job.end_time - job.start_time).total_seconds()
            
            # Calculate success rate
            total_records = job.total_records_processed + job.total_records_failed
            if total_records > 0:
                job.success_rate_percent = (job.total_records_processed / total_records) * 100
            
            # Calculate performance score based on SLA targets
            job.performance_score = self._calculate_performance_score(job)
            
            # Check SLA compliance
            job.sla_breach = self._check_sla_compliance(job)
            
            # Move to history
            if job.job_name not in self.job_history:
                self.job_history[job.job_name] = []
            self.job_history[job.job_name].append(job)
            
            # Remove from active jobs
            del self.active_jobs[job_id]
        
        # Record completion metrics
        status = "success" if success else "failed"
        self.prometheus.increment_counter(
            "etl_jobs_completed_total",
            1,
            {"job_name": job.job_name, "pipeline": job.pipeline_name, "status": status}
        )
        
        self.prometheus.observe_histogram(
            "etl_job_duration_seconds",
            job.total_duration_seconds,
            {"job_name": job.job_name, "pipeline": job.pipeline_name}
        )
        
        # Record business metrics
        self.observability.record_metric(
            "etl_job_completed",
            1,
            self.observability.MetricCategory.BUSINESS,
            {
                "job_name": job.job_name, 
                "pipeline": job.pipeline_name,
                "status": status,
                "performance_score": str(job.performance_score)
            }
        )
        
        # Store to database
        self._store_job_status(job)
        
        # Generate alerts if needed
        if job.sla_breach or not success:
            self._generate_job_alert(job, success)
        
        self.logger.info(f"Completed ETL job: {job_id} ({job.job_name}) - "
                        f"Duration: {job.total_duration_seconds:.2f}s, "
                        f"Success: {success}, Performance Score: {job.performance_score:.1f}")

    def record_data_quality_metrics(self, dataset_name: str, table_name: str, 
                                   stage: ETLStage, quality_metrics: DataQualityMetrics):
        """Record data quality metrics for a dataset."""
        quality_metrics.dataset_name = dataset_name
        quality_metrics.table_name = table_name
        quality_metrics.stage = stage
        quality_metrics.check_timestamp = datetime.utcnow()
        
        # Calculate overall quality score
        scores = [
            quality_metrics.completeness_score,
            quality_metrics.accuracy_score,
            quality_metrics.consistency_score,
            quality_metrics.timeliness_score
        ]
        quality_metrics.overall_quality_score = sum(scores) / len(scores)
        
        # Determine quality status
        if quality_metrics.overall_quality_score >= 95:
            quality_metrics.quality_status = DataQualityStatus.EXCELLENT
        elif quality_metrics.overall_quality_score >= 80:
            quality_metrics.quality_status = DataQualityStatus.GOOD
        elif quality_metrics.overall_quality_score >= 60:
            quality_metrics.quality_status = DataQualityStatus.FAIR
        else:
            quality_metrics.quality_status = DataQualityStatus.POOR
        
        # Store metrics
        with self.lock:
            key = f"{dataset_name}_{table_name}"
            if key not in self.data_quality_history:
                self.data_quality_history[key] = []
            self.data_quality_history[key].append(quality_metrics)
        
        # Record Prometheus metrics
        self.prometheus.set_gauge(
            "etl_data_quality_score",
            quality_metrics.overall_quality_score,
            {
                "dataset": dataset_name,
                "table": table_name,
                "stage": stage.value
            }
        )
        
        self.prometheus.set_gauge(
            "etl_data_completeness_score",
            quality_metrics.completeness_score,
            {
                "dataset": dataset_name,
                "table": table_name,
                "stage": stage.value
            }
        )
        
        # Store to database
        self._store_data_quality_metrics(quality_metrics)
        
        # Generate alerts for poor data quality
        if quality_metrics.quality_status in [DataQualityStatus.POOR, DataQualityStatus.FAILED]:
            self._generate_data_quality_alert(quality_metrics)
        
        self.logger.info(f"Recorded data quality metrics for {dataset_name}.{table_name}: "
                        f"Score: {quality_metrics.overall_quality_score:.1f} "
                        f"({quality_metrics.quality_status.value})")

    def get_pipeline_health_metrics(self, pipeline_name: str) -> PipelineHealthMetrics:
        """Get comprehensive health metrics for a pipeline."""
        now = datetime.utcnow()
        cutoff_24h = now - timedelta(hours=24)
        cutoff_7d = now - timedelta(days=7)
        
        # Get job history for this pipeline
        pipeline_jobs = []
        with self.lock:
            for job_list in self.job_history.values():
                pipeline_jobs.extend([job for job in job_list if job.pipeline_name == pipeline_name])
        
        # Filter by time periods
        jobs_24h = [job for job in pipeline_jobs if job.start_time >= cutoff_24h]
        jobs_7d = [job for job in pipeline_jobs if job.start_time >= cutoff_7d]
        
        # Calculate success rates
        success_rate_24h = self._calculate_success_rate(jobs_24h)
        success_rate_7d = self._calculate_success_rate(jobs_7d)
        
        # Calculate average durations
        avg_duration_24h = self._calculate_average_duration(jobs_24h)
        avg_duration_7d = self._calculate_average_duration(jobs_7d)
        
        # Determine trends
        data_quality_trend = self._calculate_data_quality_trend(pipeline_name)
        performance_trend = self._calculate_performance_trend(jobs_24h, jobs_7d)
        
        # Find last success and failure
        successful_jobs = [job for job in pipeline_jobs if job.success_rate_percent >= 95]
        failed_jobs = [job for job in pipeline_jobs if job.success_rate_percent < 95]
        
        last_success = max(successful_jobs, key=lambda x: x.start_time).start_time if successful_jobs else None
        last_failure = max(failed_jobs, key=lambda x: x.start_time).start_time if failed_jobs else None
        
        # Count failures in last 24h
        failure_count_24h = len([job for job in jobs_24h if job.success_rate_percent < 95])
        
        # Calculate SLA compliance
        sla_compliant_jobs = [job for job in jobs_24h if not job.sla_breach]
        sla_compliance = (len(sla_compliant_jobs) / len(jobs_24h) * 100) if jobs_24h else 100
        
        # Generate recommendations
        recommendations = self._generate_pipeline_recommendations(
            pipeline_name, success_rate_24h, avg_duration_24h, failure_count_24h
        )
        
        health_metrics = PipelineHealthMetrics(
            pipeline_name=pipeline_name,
            success_rate_24h=success_rate_24h,
            success_rate_7d=success_rate_7d,
            average_duration_24h=avg_duration_24h,
            average_duration_7d=avg_duration_7d,
            data_quality_trend=data_quality_trend,
            performance_trend=performance_trend,
            last_success=last_success,
            last_failure=last_failure,
            failure_count_24h=failure_count_24h,
            sla_compliance_percent=sla_compliance,
            recommendations=recommendations
        )
        
        # Store pipeline health snapshot
        self._store_pipeline_health_metrics(health_metrics)
        
        return health_metrics

    def get_etl_dashboard_data(self) -> Dict[str, Any]:
        """Get comprehensive ETL dashboard data."""
        now = datetime.utcnow()
        cutoff_24h = now - timedelta(hours=24)
        
        # Get active jobs
        active_jobs_data = []
        with self.lock:
            for job_id, job in self.active_jobs.items():
                active_jobs_data.append({
                    "job_id": job_id,
                    "job_name": job.job_name,
                    "pipeline_name": job.pipeline_name,
                    "start_time": job.start_time.isoformat(),
                    "duration_seconds": (now - job.start_time).total_seconds(),
                    "stages_completed": job.stages_completed,
                    "records_processed": job.total_records_processed
                })
        
        # Get recent job completions
        recent_jobs = []
        with self.lock:
            for job_list in self.job_history.values():
                for job in job_list:
                    if job.start_time >= cutoff_24h:
                        recent_jobs.append(job)
        
        recent_jobs.sort(key=lambda x: x.start_time, reverse=True)
        
        # Calculate summary metrics
        total_jobs_24h = len(recent_jobs)
        successful_jobs_24h = len([job for job in recent_jobs if job.success_rate_percent >= 95])
        failed_jobs_24h = total_jobs_24h - successful_jobs_24h
        
        success_rate_24h = (successful_jobs_24h / total_jobs_24h * 100) if total_jobs_24h > 0 else 100
        
        total_records_processed_24h = sum(job.total_records_processed for job in recent_jobs)
        total_records_failed_24h = sum(job.total_records_failed for job in recent_jobs)
        
        avg_job_duration_24h = (
            sum(job.total_duration_seconds for job in recent_jobs if job.total_duration_seconds) 
            / len([job for job in recent_jobs if job.total_duration_seconds])
        ) if recent_jobs else 0
        
        # Get pipeline health
        pipelines = list(set(job.pipeline_name for job in recent_jobs))
        pipeline_health = {}
        for pipeline in pipelines:
            try:
                health = self.get_pipeline_health_metrics(pipeline)
                pipeline_health[pipeline] = asdict(health)
            except Exception as e:
                self.logger.warning(f"Failed to get health metrics for pipeline {pipeline}: {e}")
        
        # Get data quality summary
        data_quality_summary = self._get_data_quality_summary()
        
        return {
            "timestamp": now.isoformat(),
            "active_jobs": {
                "count": len(active_jobs_data),
                "jobs": active_jobs_data[:10]  # Limit to 10 most recent
            },
            "recent_performance_24h": {
                "total_jobs": total_jobs_24h,
                "successful_jobs": successful_jobs_24h,
                "failed_jobs": failed_jobs_24h,
                "success_rate_percent": round(success_rate_24h, 1),
                "total_records_processed": total_records_processed_24h,
                "total_records_failed": total_records_failed_24h,
                "average_job_duration_seconds": round(avg_job_duration_24h, 2)
            },
            "pipeline_health": pipeline_health,
            "data_quality_summary": data_quality_summary,
            "recent_jobs": [asdict(job) for job in recent_jobs[:20]],  # Last 20 jobs
            "sla_targets": self.sla_targets
        }

    def _calculate_performance_score(self, job: ETLJobStatus) -> float:
        """Calculate performance score based on SLA targets."""
        score = 100.0
        
        # Duration penalty
        if job.total_duration_seconds:
            target_duration = self.sla_targets.get(f"{job.pipeline_name}_max_duration_minutes", 60) * 60
            if job.total_duration_seconds > target_duration:
                duration_penalty = min(50, (job.total_duration_seconds - target_duration) / target_duration * 25)
                score -= duration_penalty
        
        # Success rate penalty
        if job.success_rate_percent < self.sla_targets["min_success_rate_percent"]:
            success_penalty = (self.sla_targets["min_success_rate_percent"] - job.success_rate_percent) * 0.5
            score -= success_penalty
        
        # Failed stages penalty
        if job.stages_failed:
            score -= len(job.stages_failed) * 10
        
        return max(0, score)

    def _check_sla_compliance(self, job: ETLJobStatus) -> bool:
        """Check if job violates SLA targets."""
        # Check duration SLA
        if job.total_duration_seconds:
            target_duration = self.sla_targets.get(f"{job.pipeline_name}_max_duration_minutes", 60) * 60
            if job.total_duration_seconds > target_duration:
                return True
        
        # Check success rate SLA
        if job.success_rate_percent < self.sla_targets["min_success_rate_percent"]:
            return True
        
        # Check failed stages
        if job.stages_failed:
            return True
        
        return False

    def _calculate_success_rate(self, jobs: List[ETLJobStatus]) -> float:
        """Calculate success rate for a list of jobs."""
        if not jobs:
            return 100.0
        
        successful_jobs = len([job for job in jobs if job.success_rate_percent >= 95])
        return (successful_jobs / len(jobs)) * 100

    def _calculate_average_duration(self, jobs: List[ETLJobStatus]) -> float:
        """Calculate average duration for a list of jobs."""
        if not jobs:
            return 0.0
        
        durations = [job.total_duration_seconds for job in jobs if job.total_duration_seconds]
        return sum(durations) / len(durations) if durations else 0.0

    def _calculate_data_quality_trend(self, pipeline_name: str) -> str:
        """Calculate data quality trend for a pipeline."""
        # This is a simplified implementation
        # In a real system, you'd analyze historical data quality metrics
        return "stable"

    def _calculate_performance_trend(self, jobs_24h: List[ETLJobStatus], 
                                   jobs_7d: List[ETLJobStatus]) -> str:
        """Calculate performance trend."""
        if len(jobs_24h) < 2 or len(jobs_7d) < 2:
            return "stable"
        
        avg_duration_24h = self._calculate_average_duration(jobs_24h)
        avg_duration_7d = self._calculate_average_duration(jobs_7d)
        
        if avg_duration_24h < avg_duration_7d * 0.9:
            return "improving"
        elif avg_duration_24h > avg_duration_7d * 1.1:
            return "degrading"
        else:
            return "stable"

    def _generate_pipeline_recommendations(self, pipeline_name: str, success_rate: float,
                                         avg_duration: float, failure_count: int) -> List[str]:
        """Generate recommendations for pipeline optimization."""
        recommendations = []
        
        if success_rate < 95:
            recommendations.append(f"Success rate ({success_rate:.1f}%) below target. Review error handling and data validation.")
        
        if avg_duration > 3600:  # 1 hour
            recommendations.append("Long average duration detected. Consider performance optimization and parallelization.")
        
        if failure_count > 3:
            recommendations.append("High failure rate detected. Review pipeline stability and monitoring.")
        
        if not recommendations:
            recommendations.append("Pipeline performance is within acceptable limits.")
        
        return recommendations

    def _get_data_quality_summary(self) -> Dict[str, Any]:
        """Get summary of data quality metrics."""
        now = datetime.utcnow()
        cutoff_24h = now - timedelta(hours=24)
        
        recent_quality_metrics = []
        with self.lock:
            for metrics_list in self.data_quality_history.values():
                for metrics in metrics_list:
                    if metrics.check_timestamp >= cutoff_24h:
                        recent_quality_metrics.append(metrics)
        
        if not recent_quality_metrics:
            return {"message": "No data quality metrics available"}
        
        # Calculate averages
        avg_completeness = sum(m.completeness_score for m in recent_quality_metrics) / len(recent_quality_metrics)
        avg_accuracy = sum(m.accuracy_score for m in recent_quality_metrics) / len(recent_quality_metrics)
        avg_consistency = sum(m.consistency_score for m in recent_quality_metrics) / len(recent_quality_metrics)
        avg_overall = sum(m.overall_quality_score for m in recent_quality_metrics) / len(recent_quality_metrics)
        
        # Count by status
        status_counts = {}
        for status in DataQualityStatus:
            status_counts[status.value] = len([m for m in recent_quality_metrics if m.quality_status == status])
        
        return {
            "total_checks_24h": len(recent_quality_metrics),
            "average_scores": {
                "completeness": round(avg_completeness, 1),
                "accuracy": round(avg_accuracy, 1),
                "consistency": round(avg_consistency, 1),
                "overall": round(avg_overall, 1)
            },
            "status_distribution": status_counts,
            "datasets_monitored": len(set(m.dataset_name for m in recent_quality_metrics))
        }

    def _store_job_status(self, job: ETLJobStatus):
        """Store job status to database."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("""
                    INSERT INTO etl_job_status (
                        job_id, job_name, pipeline_name, start_time, end_time,
                        total_duration_seconds, stages_completed, stages_failed,
                        total_records_processed, total_records_failed, success_rate_percent,
                        performance_score, data_quality_score, sla_breach, environment
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    job.job_id, job.job_name, job.pipeline_name,
                    job.start_time.isoformat(),
                    job.end_time.isoformat() if job.end_time else None,
                    job.total_duration_seconds,
                    json.dumps(job.stages_completed),
                    json.dumps(job.stages_failed),
                    job.total_records_processed,
                    job.total_records_failed,
                    job.success_rate_percent,
                    job.performance_score,
                    job.data_quality_score,
                    job.sla_breach,
                    job.environment
                ))
        except Exception as e:
            self.logger.error(f"Failed to store job status: {e}")

    def _store_stage_metrics(self, metrics: ETLPerformanceMetrics):
        """Store stage metrics to database."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("""
                    INSERT INTO etl_performance_metrics (
                        job_id, job_name, stage, start_time, end_time, duration_seconds,
                        records_processed, records_failed, records_skipped,
                        throughput_records_per_second, memory_usage_mb, cpu_usage_percent,
                        data_size_mb, success, error_message, tags
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    metrics.job_id, metrics.job_name, metrics.stage.value,
                    metrics.start_time.isoformat(),
                    metrics.end_time.isoformat() if metrics.end_time else None,
                    metrics.duration_seconds, metrics.records_processed,
                    metrics.records_failed, metrics.records_skipped,
                    metrics.throughput_records_per_second,
                    metrics.memory_usage_mb, metrics.cpu_usage_percent,
                    metrics.data_size_mb, metrics.success, metrics.error_message,
                    json.dumps(metrics.tags)
                ))
        except Exception as e:
            self.logger.error(f"Failed to store stage metrics: {e}")

    def _store_data_quality_metrics(self, metrics: DataQualityMetrics):
        """Store data quality metrics to database."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("""
                    INSERT INTO data_quality_metrics (
                        dataset_name, table_name, stage, total_records, valid_records,
                        invalid_records, duplicate_records, null_percentage,
                        completeness_score, accuracy_score, consistency_score,
                        timeliness_score, overall_quality_score, quality_status,
                        quality_issues, check_timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    metrics.dataset_name, metrics.table_name, metrics.stage.value,
                    metrics.total_records, metrics.valid_records, metrics.invalid_records,
                    metrics.duplicate_records, metrics.null_percentage,
                    metrics.completeness_score, metrics.accuracy_score,
                    metrics.consistency_score, metrics.timeliness_score,
                    metrics.overall_quality_score, metrics.quality_status.value,
                    json.dumps(metrics.quality_issues), metrics.check_timestamp.isoformat()
                ))
        except Exception as e:
            self.logger.error(f"Failed to store data quality metrics: {e}")

    def _store_pipeline_health_metrics(self, metrics: PipelineHealthMetrics):
        """Store pipeline health metrics to database."""
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
                conn.execute("""
                    INSERT INTO pipeline_health_trends (
                        pipeline_name, success_rate_24h, success_rate_7d,
                        average_duration_24h, average_duration_7d, data_quality_trend,
                        performance_trend, last_success, last_failure, failure_count_24h,
                        sla_compliance_percent, recommendations
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    metrics.pipeline_name, metrics.success_rate_24h, metrics.success_rate_7d,
                    metrics.average_duration_24h, metrics.average_duration_7d,
                    metrics.data_quality_trend, metrics.performance_trend,
                    metrics.last_success.isoformat() if metrics.last_success else None,
                    metrics.last_failure.isoformat() if metrics.last_failure else None,
                    metrics.failure_count_24h, metrics.sla_compliance_percent,
                    json.dumps(metrics.recommendations)
                ))
        except Exception as e:
            self.logger.error(f"Failed to store pipeline health metrics: {e}")

    def _generate_job_alert(self, job: ETLJobStatus, success: bool):
        """Generate alert for job completion issues."""
        if not success or job.sla_breach:
            severity = "critical" if not success else "warning"
            message = f"ETL job {job.job_name} "
            
            if not success:
                message += "failed"
            elif job.sla_breach:
                message += "breached SLA"
            
            message += f". Duration: {job.total_duration_seconds:.2f}s, "
            message += f"Success rate: {job.success_rate_percent:.1f}%, "
            message += f"Performance score: {job.performance_score:.1f}"
            
            # In a real implementation, you'd send this to your alerting system
            self.logger.warning(f"ETL Alert [{severity.upper()}]: {message}")

    def _generate_data_quality_alert(self, metrics: DataQualityMetrics):
        """Generate alert for data quality issues."""
        message = f"Data quality issue in {metrics.dataset_name}.{metrics.table_name} "
        message += f"({metrics.stage.value}): Score {metrics.overall_quality_score:.1f}% "
        message += f"({metrics.quality_status.value})"
        
        if metrics.quality_issues:
            message += f". Issues: {', '.join(metrics.quality_issues)}"
        
        # In a real implementation, you'd send this to your alerting system
        self.logger.warning(f"Data Quality Alert: {message}")


# Global ETL performance monitor instance
_etl_monitor: Optional[ETLPerformanceMonitor] = None


def get_etl_monitor() -> ETLPerformanceMonitor:
    """Get global ETL performance monitor instance."""
    global _etl_monitor
    if _etl_monitor is None:
        _etl_monitor = ETLPerformanceMonitor()
    return _etl_monitor


# Context manager for ETL job tracking
class ETLJobTracker:
    """Context manager for automatic ETL job tracking."""
    
    def __init__(self, job_name: str, pipeline_name: str, environment: str = "production", **tags):
        self.job_name = job_name
        self.pipeline_name = pipeline_name
        self.environment = environment
        self.tags = tags
        self.job_id = None
        self.monitor = get_etl_monitor()
    
    def __enter__(self):
        self.job_id = self.monitor.start_etl_job(
            self.job_name, self.pipeline_name, self.environment, **self.tags
        )
        return self.job_id
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.job_id:
            success = exc_type is None
            self.monitor.complete_job(self.job_id, success)


# Context manager for ETL stage tracking  
class ETLStageTracker:
    """Context manager for automatic ETL stage tracking."""
    
    def __init__(self, job_id: str, stage: ETLStage, **tags):
        self.job_id = job_id
        self.stage = stage
        self.tags = tags
        self.monitor = get_etl_monitor()
        self.records_processed = 0
        self.records_failed = 0
        self.records_skipped = 0
    
    def __enter__(self):
        self.stage_metrics = self.monitor.start_stage(self.job_id, self.stage, **self.tags)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        success = exc_type is None
        error_message = str(exc_val) if exc_val else None
        
        self.monitor.complete_stage(
            self.job_id, self.stage,
            records_processed=self.records_processed,
            records_failed=self.records_failed, 
            records_skipped=self.records_skipped,
            success=success,
            error_message=error_message
        )
    
    def add_records(self, processed: int = 0, failed: int = 0, skipped: int = 0):
        """Add record counts to the stage tracker."""
        self.records_processed += processed
        self.records_failed += failed
        self.records_skipped += skipped
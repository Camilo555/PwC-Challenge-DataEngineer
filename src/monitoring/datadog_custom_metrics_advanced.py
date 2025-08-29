"""
Advanced DataDog Custom Metrics Implementation
Provides comprehensive custom metrics management with intelligent aggregation,
data quality tracking, business KPIs, and automated metric generation
"""

import asyncio
import json
import time
import statistics
import numpy as np
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from dataclasses import dataclass, asdict, field
from enum import Enum
from collections import defaultdict, deque

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring, MetricType

logger = get_logger(__name__)


class MetricCategory(Enum):
    """Categories for custom metrics"""
    BUSINESS = "business"
    TECHNICAL = "technical"
    OPERATIONAL = "operational"
    QUALITY = "quality"
    PERFORMANCE = "performance"
    SECURITY = "security"
    COST = "cost"
    USER_EXPERIENCE = "user_experience"


class AggregationType(Enum):
    """Types of metric aggregations"""
    SUM = "sum"
    AVERAGE = "average"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    PERCENTILE = "percentile"
    RATE = "rate"
    HISTOGRAM = "histogram"
    UNIQUE_COUNT = "unique_count"
    WEIGHTED_AVERAGE = "weighted_average"


class MetricFrequency(Enum):
    """Metric collection frequencies"""
    REAL_TIME = "real_time"  # < 1 second
    HIGH = "high"  # 1-10 seconds
    MEDIUM = "medium"  # 10-60 seconds
    LOW = "low"  # 1-5 minutes
    PERIODIC = "periodic"  # 5+ minutes
    ON_DEMAND = "on_demand"


@dataclass
class CustomMetricDefinition:
    """Advanced custom metric definition"""
    name: str
    display_name: str
    description: str
    category: MetricCategory
    metric_type: MetricType
    unit: str
    frequency: MetricFrequency
    aggregation_type: AggregationType
    dimensions: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    
    # Thresholds and alerting
    warning_threshold: Optional[float] = None
    critical_threshold: Optional[float] = None
    higher_is_better: bool = True
    
    # Data quality settings
    expected_range: Optional[Tuple[float, float]] = None
    outlier_detection: bool = False
    data_validation: bool = True
    
    # Aggregation settings
    window_size: int = 60  # seconds
    percentile: float = 95.0  # for percentile aggregations
    retention_days: int = 90
    
    # Business context
    business_impact: str = "medium"  # low, medium, high, critical
    cost_per_datapoint: float = 0.0
    sla_critical: bool = False
    
    # Automation settings
    auto_baseline: bool = False
    anomaly_detection: bool = False
    forecast_enabled: bool = False


@dataclass
class MetricDataPoint:
    """Individual metric data point"""
    timestamp: datetime
    value: float
    dimensions: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    quality_score: float = 1.0
    is_anomaly: bool = False


@dataclass
class MetricAggregation:
    """Aggregated metric result"""
    metric_name: str
    aggregation_type: AggregationType
    time_window: timedelta
    value: float
    data_points: int
    dimensions: Dict[str, str] = field(default_factory=dict)
    confidence: float = 1.0
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class BusinessKPI:
    """Business KPI definition with automatic calculation"""
    kpi_id: str
    name: str
    description: str
    formula: str  # Mathematical formula using metric names
    target_value: Optional[float] = None
    unit: str = "number"
    calculation_frequency: MetricFrequency = MetricFrequency.MEDIUM
    dependent_metrics: List[str] = field(default_factory=list)
    business_owner: Optional[str] = None
    board_level: bool = False


class DataDogCustomMetricsAdvanced:
    """
    Advanced Custom Metrics Management System
    
    Features:
    - Intelligent metric aggregation and windowing
    - Business KPI automatic calculation
    - Data quality monitoring and validation
    - Infrastructure efficiency metrics
    - Real-time anomaly detection
    - Cost optimization tracking
    - Automated baseline establishment
    - Dynamic threshold adjustment
    - Multi-dimensional metric analysis
    """
    
    def __init__(self, service_name: str = "custom-metrics-advanced",
                 datadog_monitoring: Optional[DatadogMonitoring] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Metric registry and storage
        self.metric_definitions: Dict[str, CustomMetricDefinition] = {}
        self.business_kpis: Dict[str, BusinessKPI] = {}
        self.metric_data: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.aggregated_metrics: Dict[str, List[MetricAggregation]] = defaultdict(list)
        
        # Real-time processing
        self.processing_queue: asyncio.Queue = asyncio.Queue()
        self.aggregation_tasks: Dict[str, asyncio.Task] = {}
        self.baseline_values: Dict[str, float] = {}
        
        # Quality tracking
        self.quality_scores: Dict[str, List[float]] = defaultdict(list)
        self.anomaly_counts: Dict[str, int] = defaultdict(int)
        self.data_validation_errors: List[Dict[str, Any]] = []
        
        # Performance tracking
        self.metrics_processed = 0
        self.processing_times = deque(maxlen=1000)
        self.cost_tracking = {"total_datapoints": 0, "estimated_cost": 0.0}
        
        # Initialize standard metrics
        self._initialize_standard_metrics()
        self._initialize_business_kpis()
        
        # Start background processing
        asyncio.create_task(self._start_background_processing())
        
        self.logger.info(f"Advanced custom metrics system initialized for {service_name}")
    
    def _initialize_standard_metrics(self):
        """Initialize standard enterprise metrics"""
        
        standard_metrics = [
            # Business Metrics
            CustomMetricDefinition(
                name="revenue_per_customer",
                display_name="Revenue per Customer",
                description="Average revenue generated per customer",
                category=MetricCategory.BUSINESS,
                metric_type=MetricType.GAUGE,
                unit="currency",
                frequency=MetricFrequency.MEDIUM,
                aggregation_type=AggregationType.AVERAGE,
                dimensions=["customer_segment", "region", "channel"],
                warning_threshold=800.0,
                critical_threshold=600.0,
                business_impact="high",
                sla_critical=True,
                auto_baseline=True,
                anomaly_detection=True
            ),
            CustomMetricDefinition(
                name="customer_acquisition_cost",
                display_name="Customer Acquisition Cost",
                description="Cost to acquire a new customer",
                category=MetricCategory.BUSINESS,
                metric_type=MetricType.GAUGE,
                unit="currency",
                frequency=MetricFrequency.MEDIUM,
                aggregation_type=AggregationType.AVERAGE,
                dimensions=["channel", "campaign", "region"],
                warning_threshold=150.0,
                critical_threshold=200.0,
                higher_is_better=False,
                business_impact="high"
            ),
            CustomMetricDefinition(
                name="customer_lifetime_value",
                display_name="Customer Lifetime Value",
                description="Predicted lifetime value of customers",
                category=MetricCategory.BUSINESS,
                metric_type=MetricType.GAUGE,
                unit="currency",
                frequency=MetricFrequency.LOW,
                aggregation_type=AggregationType.AVERAGE,
                dimensions=["customer_segment", "acquisition_channel"],
                warning_threshold=2000.0,
                critical_threshold=1500.0,
                business_impact="critical",
                forecast_enabled=True
            ),
            
            # Data Quality Metrics
            CustomMetricDefinition(
                name="data_completeness_score",
                display_name="Data Completeness Score",
                description="Percentage of complete data records",
                category=MetricCategory.QUALITY,
                metric_type=MetricType.GAUGE,
                unit="percentage",
                frequency=MetricFrequency.HIGH,
                aggregation_type=AggregationType.AVERAGE,
                dimensions=["dataset", "source", "layer"],
                expected_range=(0.0, 100.0),
                warning_threshold=95.0,
                critical_threshold=90.0,
                outlier_detection=True,
                sla_critical=True
            ),
            CustomMetricDefinition(
                name="data_accuracy_score",
                display_name="Data Accuracy Score", 
                description="Percentage of accurate data records",
                category=MetricCategory.QUALITY,
                metric_type=MetricType.GAUGE,
                unit="percentage",
                frequency=MetricFrequency.HIGH,
                aggregation_type=AggregationType.AVERAGE,
                dimensions=["dataset", "validation_rule"],
                expected_range=(0.0, 100.0),
                warning_threshold=98.0,
                critical_threshold=95.0,
                anomaly_detection=True
            ),
            CustomMetricDefinition(
                name="data_freshness_hours",
                display_name="Data Freshness",
                description="Hours since data was last updated",
                category=MetricCategory.QUALITY,
                metric_type=MetricType.GAUGE,
                unit="hours",
                frequency=MetricFrequency.HIGH,
                aggregation_type=AggregationType.MAX,
                dimensions=["dataset", "source"],
                warning_threshold=2.0,
                critical_threshold=6.0,
                higher_is_better=False
            ),
            
            # Infrastructure Efficiency Metrics
            CustomMetricDefinition(
                name="cost_per_transaction",
                display_name="Cost per Transaction",
                description="Infrastructure cost per business transaction",
                category=MetricCategory.COST,
                metric_type=MetricType.GAUGE,
                unit="currency",
                frequency=MetricFrequency.MEDIUM,
                aggregation_type=AggregationType.WEIGHTED_AVERAGE,
                dimensions=["service", "environment", "region"],
                warning_threshold=0.10,
                critical_threshold=0.25,
                higher_is_better=False,
                business_impact="medium"
            ),
            CustomMetricDefinition(
                name="resource_utilization_efficiency",
                display_name="Resource Utilization Efficiency",
                description="Efficiency of resource utilization (work done / resources used)",
                category=MetricCategory.PERFORMANCE,
                metric_type=MetricType.GAUGE,
                unit="percentage",
                frequency=MetricFrequency.HIGH,
                aggregation_type=AggregationType.AVERAGE,
                dimensions=["resource_type", "service", "environment"],
                expected_range=(0.0, 100.0),
                warning_threshold=70.0,
                critical_threshold=50.0,
                auto_baseline=True
            ),
            CustomMetricDefinition(
                name="ml_model_inference_latency",
                display_name="ML Model Inference Latency",
                description="Time taken for ML model inference",
                category=MetricCategory.PERFORMANCE,
                metric_type=MetricType.HISTOGRAM,
                unit="milliseconds",
                frequency=MetricFrequency.REAL_TIME,
                aggregation_type=AggregationType.PERCENTILE,
                percentile=95.0,
                dimensions=["model_id", "model_version", "environment"],
                warning_threshold=200.0,
                critical_threshold=500.0,
                higher_is_better=False,
                sla_critical=True
            ),
            
            # User Experience Metrics
            CustomMetricDefinition(
                name="user_engagement_score",
                display_name="User Engagement Score",
                description="Composite score of user engagement metrics",
                category=MetricCategory.USER_EXPERIENCE,
                metric_type=MetricType.GAUGE,
                unit="score",
                frequency=MetricFrequency.MEDIUM,
                aggregation_type=AggregationType.WEIGHTED_AVERAGE,
                dimensions=["user_segment", "feature", "platform"],
                expected_range=(0.0, 10.0),
                warning_threshold=6.0,
                critical_threshold=4.0,
                business_impact="high",
                anomaly_detection=True
            ),
            CustomMetricDefinition(
                name="page_load_performance_score",
                display_name="Page Load Performance Score",
                description="User-perceived page loading performance",
                category=MetricCategory.USER_EXPERIENCE,
                metric_type=MetricType.GAUGE,
                unit="score",
                frequency=MetricFrequency.HIGH,
                aggregation_type=AggregationType.PERCENTILE,
                percentile=75.0,
                dimensions=["page", "device_type", "region"],
                expected_range=(0.0, 100.0),
                warning_threshold=70.0,
                critical_threshold=50.0,
                sla_critical=True
            )
        ]
        
        for metric in standard_metrics:
            self.metric_definitions[metric.name] = metric
        
        self.logger.info(f"Initialized {len(standard_metrics)} standard metrics")
    
    def _initialize_business_kpis(self):
        """Initialize business KPIs with automatic calculation"""
        
        business_kpis = [
            BusinessKPI(
                kpi_id="customer_roi",
                name="Customer Return on Investment",
                description="ROI calculated from customer lifetime value vs acquisition cost",
                formula="customer_lifetime_value / customer_acquisition_cost",
                target_value=5.0,
                unit="ratio",
                dependent_metrics=["customer_lifetime_value", "customer_acquisition_cost"],
                business_owner="CMO",
                board_level=True
            ),
            BusinessKPI(
                kpi_id="data_quality_index",
                name="Overall Data Quality Index",
                description="Composite index of data completeness and accuracy",
                formula="(data_completeness_score + data_accuracy_score) / 2",
                target_value=97.0,
                unit="percentage",
                dependent_metrics=["data_completeness_score", "data_accuracy_score"],
                business_owner="CDO",
                board_level=True
            ),
            BusinessKPI(
                kpi_id="operational_efficiency_ratio",
                name="Operational Efficiency Ratio",
                description="Efficiency calculated from resource utilization and cost per transaction",
                formula="resource_utilization_efficiency / (cost_per_transaction * 100)",
                target_value=0.8,
                unit="ratio",
                dependent_metrics=["resource_utilization_efficiency", "cost_per_transaction"],
                business_owner="COO"
            ),
            BusinessKPI(
                kpi_id="user_satisfaction_composite",
                name="User Satisfaction Composite Score",
                description="Combined user engagement and performance satisfaction",
                formula="(user_engagement_score * 10 + page_load_performance_score) / 2",
                target_value=75.0,
                unit="score",
                dependent_metrics=["user_engagement_score", "page_load_performance_score"],
                business_owner="CPO",
                board_level=True
            )
        ]
        
        for kpi in business_kpis:
            self.business_kpis[kpi.kpi_id] = kpi
        
        self.logger.info(f"Initialized {len(business_kpis)} business KPIs")
    
    async def _start_background_processing(self):
        """Start background processing tasks"""
        
        try:
            # Start metric processing worker
            asyncio.create_task(self._metric_processing_worker())
            
            # Start aggregation tasks for each metric
            for metric_name, definition in self.metric_definitions.items():
                if definition.frequency != MetricFrequency.ON_DEMAND:
                    task = asyncio.create_task(
                        self._metric_aggregation_worker(metric_name, definition)
                    )
                    self.aggregation_tasks[metric_name] = task
            
            # Start KPI calculation task
            asyncio.create_task(self._kpi_calculation_worker())
            
            # Start quality monitoring task
            asyncio.create_task(self._quality_monitoring_worker())
            
            self.logger.info("Background processing tasks started")
            
        except Exception as e:
            self.logger.error(f"Failed to start background processing: {str(e)}")
    
    async def submit_metric(self, metric_name: str, value: float,
                          dimensions: Optional[Dict[str, str]] = None,
                          metadata: Optional[Dict[str, Any]] = None,
                          timestamp: Optional[datetime] = None) -> bool:
        """Submit a metric data point"""
        
        try:
            if metric_name not in self.metric_definitions:
                self.logger.warning(f"Unknown metric: {metric_name}")
                return False
            
            definition = self.metric_definitions[metric_name]
            
            # Create data point
            data_point = MetricDataPoint(
                timestamp=timestamp or datetime.utcnow(),
                value=value,
                dimensions=dimensions or {},
                metadata=metadata or {}
            )
            
            # Validate data point
            if definition.data_validation:
                data_point.quality_score = await self._validate_data_point(definition, data_point)
            
            # Detect anomalies
            if definition.anomaly_detection:
                data_point.is_anomaly = await self._detect_anomaly(definition, data_point)
            
            # Add to processing queue
            await self.processing_queue.put((metric_name, data_point))
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to submit metric {metric_name}: {str(e)}")
            return False
    
    async def _validate_data_point(self, definition: CustomMetricDefinition, 
                                 data_point: MetricDataPoint) -> float:
        """Validate data point and return quality score"""
        
        try:
            quality_score = 1.0
            
            # Check expected range
            if definition.expected_range:
                min_val, max_val = definition.expected_range
                if not (min_val <= data_point.value <= max_val):
                    quality_score -= 0.3
                    self.data_validation_errors.append({
                        "timestamp": data_point.timestamp.isoformat(),
                        "metric": definition.name,
                        "error": "value_out_of_range",
                        "value": data_point.value,
                        "expected_range": definition.expected_range
                    })
            
            # Check for required dimensions
            missing_dimensions = set(definition.dimensions) - set(data_point.dimensions.keys())
            if missing_dimensions:
                quality_score -= 0.2 * len(missing_dimensions) / len(definition.dimensions)
            
            # Check data freshness (timestamp not too old)
            age_hours = (datetime.utcnow() - data_point.timestamp).total_seconds() / 3600
            if age_hours > 24:  # Data older than 24 hours
                quality_score -= 0.1
            
            return max(0.0, quality_score)
            
        except Exception as e:
            self.logger.error(f"Failed to validate data point: {str(e)}")
            return 0.5
    
    async def _detect_anomaly(self, definition: CustomMetricDefinition,
                            data_point: MetricDataPoint) -> bool:
        """Detect if data point is anomalous"""
        
        try:
            metric_name = definition.name
            
            # Get historical data
            if metric_name not in self.metric_data:
                return False
            
            historical_values = [dp.value for dp in list(self.metric_data[metric_name])[-100:]]
            
            if len(historical_values) < 10:
                return False
            
            # Simple statistical anomaly detection
            mean_val = statistics.mean(historical_values)
            std_val = statistics.stdev(historical_values)
            
            # Z-score based detection
            z_score = abs(data_point.value - mean_val) / std_val if std_val > 0 else 0
            
            is_anomaly = z_score > 3.0  # 3 standard deviations
            
            if is_anomaly:
                self.anomaly_counts[metric_name] += 1
            
            return is_anomaly
            
        except Exception as e:
            self.logger.error(f"Failed to detect anomaly: {str(e)}")
            return False
    
    async def _metric_processing_worker(self):
        """Background worker for processing metric data points"""
        
        while True:
            try:
                # Get metric from queue
                metric_name, data_point = await self.processing_queue.get()
                
                start_time = time.time()
                
                # Store data point
                self.metric_data[metric_name].append(data_point)
                
                # Track quality score
                self.quality_scores[metric_name].append(data_point.quality_score)
                if len(self.quality_scores[metric_name]) > 1000:
                    self.quality_scores[metric_name] = self.quality_scores[metric_name][-1000:]
                
                # Send to DataDog
                if self.datadog_monitoring:
                    await self._send_to_datadog(metric_name, data_point)
                
                # Update performance tracking
                processing_time = time.time() - start_time
                self.processing_times.append(processing_time)
                self.metrics_processed += 1
                
                # Update cost tracking
                definition = self.metric_definitions[metric_name]
                self.cost_tracking["total_datapoints"] += 1
                self.cost_tracking["estimated_cost"] += definition.cost_per_datapoint
                
                # Mark task as done
                self.processing_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"Error in metric processing worker: {str(e)}")
                await asyncio.sleep(1)
    
    async def _send_to_datadog(self, metric_name: str, data_point: MetricDataPoint):
        """Send metric data point to DataDog"""
        
        try:
            definition = self.metric_definitions[metric_name]
            
            # Prepare tags
            tags = [f"metric:{metric_name}", f"category:{definition.category.value}"]
            
            # Add dimension tags
            for dim_key, dim_value in data_point.dimensions.items():
                tags.append(f"{dim_key}:{dim_value}")
            
            # Add definition tags
            for tag_key, tag_value in definition.tags.items():
                tags.append(f"{tag_key}:{tag_value}")
            
            # Add quality and anomaly tags
            tags.append(f"quality_score:{data_point.quality_score:.2f}")
            tags.append(f"is_anomaly:{data_point.is_anomaly}")
            tags.append(f"business_impact:{definition.business_impact}")
            
            # Send metric based on type
            if definition.metric_type == MetricType.GAUGE:
                self.datadog_monitoring.gauge(
                    f"custom.{metric_name}",
                    data_point.value,
                    tags=tags
                )
            elif definition.metric_type == MetricType.COUNT:
                self.datadog_monitoring.counter(
                    f"custom.{metric_name}",
                    data_point.value,
                    tags=tags
                )
            elif definition.metric_type == MetricType.HISTOGRAM:
                self.datadog_monitoring.histogram(
                    f"custom.{metric_name}",
                    data_point.value,
                    tags=tags
                )
            elif definition.metric_type == MetricType.DISTRIBUTION:
                self.datadog_monitoring.distribution(
                    f"custom.{metric_name}",
                    data_point.value,
                    tags=tags
                )
            
            # Send quality metrics
            self.datadog_monitoring.gauge(
                f"custom.metric_quality.{metric_name}",
                data_point.quality_score,
                tags=tags
            )
            
            # Send anomaly counter
            if data_point.is_anomaly:
                self.datadog_monitoring.counter(
                    f"custom.anomaly.{metric_name}",
                    tags=tags
                )
                
        except Exception as e:
            self.logger.error(f"Failed to send metric to DataDog: {str(e)}")
    
    async def _metric_aggregation_worker(self, metric_name: str, 
                                       definition: CustomMetricDefinition):
        """Background worker for metric aggregation"""
        
        # Calculate sleep interval based on frequency
        frequency_intervals = {
            MetricFrequency.REAL_TIME: 1,
            MetricFrequency.HIGH: 10,
            MetricFrequency.MEDIUM: 30,
            MetricFrequency.LOW: 60,
            MetricFrequency.PERIODIC: 300
        }
        
        interval = frequency_intervals.get(definition.frequency, 60)
        
        while True:
            try:
                await asyncio.sleep(interval)
                
                # Get recent data points
                if metric_name not in self.metric_data:
                    continue
                
                cutoff_time = datetime.utcnow() - timedelta(seconds=definition.window_size)
                recent_data = [
                    dp for dp in self.metric_data[metric_name]
                    if dp.timestamp >= cutoff_time
                ]
                
                if not recent_data:
                    continue
                
                # Perform aggregation
                aggregation = await self._calculate_aggregation(
                    metric_name, definition, recent_data
                )
                
                if aggregation:
                    # Store aggregation
                    self.aggregated_metrics[metric_name].append(aggregation)
                    
                    # Keep only recent aggregations
                    if len(self.aggregated_metrics[metric_name]) > 1000:
                        self.aggregated_metrics[metric_name] = self.aggregated_metrics[metric_name][-1000:]
                    
                    # Send aggregated metric to DataDog
                    if self.datadog_monitoring:
                        await self._send_aggregation_to_datadog(aggregation)
                
            except Exception as e:
                self.logger.error(f"Error in aggregation worker for {metric_name}: {str(e)}")
                await asyncio.sleep(interval)
    
    async def _calculate_aggregation(self, metric_name: str, 
                                   definition: CustomMetricDefinition,
                                   data_points: List[MetricDataPoint]) -> Optional[MetricAggregation]:
        """Calculate metric aggregation"""
        
        try:
            if not data_points:
                return None
            
            values = [dp.value for dp in data_points]
            window_size = timedelta(seconds=definition.window_size)
            
            # Calculate aggregated value based on type
            if definition.aggregation_type == AggregationType.SUM:
                agg_value = sum(values)
            elif definition.aggregation_type == AggregationType.AVERAGE:
                agg_value = statistics.mean(values)
            elif definition.aggregation_type == AggregationType.MIN:
                agg_value = min(values)
            elif definition.aggregation_type == AggregationType.MAX:
                agg_value = max(values)
            elif definition.aggregation_type == AggregationType.COUNT:
                agg_value = len(values)
            elif definition.aggregation_type == AggregationType.PERCENTILE:
                agg_value = np.percentile(values, definition.percentile)
            elif definition.aggregation_type == AggregationType.UNIQUE_COUNT:
                agg_value = len(set(values))
            elif definition.aggregation_type == AggregationType.RATE:
                # Calculate rate per second
                time_span = (max(dp.timestamp for dp in data_points) - 
                           min(dp.timestamp for dp in data_points)).total_seconds()
                agg_value = len(values) / max(time_span, 1)
            elif definition.aggregation_type == AggregationType.WEIGHTED_AVERAGE:
                # Weight by quality score
                weights = [dp.quality_score for dp in data_points]
                agg_value = np.average(values, weights=weights)
            else:
                agg_value = statistics.mean(values)
            
            # Calculate confidence based on data quality
            quality_scores = [dp.quality_score for dp in data_points]
            confidence = statistics.mean(quality_scores)
            
            # Aggregate dimensions (take most common values)
            dimension_counts = defaultdict(lambda: defaultdict(int))
            for dp in data_points:
                for dim_key, dim_value in dp.dimensions.items():
                    dimension_counts[dim_key][dim_value] += 1
            
            aggregated_dimensions = {}
            for dim_key, value_counts in dimension_counts.items():
                if value_counts:
                    aggregated_dimensions[dim_key] = max(value_counts.items(), key=lambda x: x[1])[0]
            
            return MetricAggregation(
                metric_name=metric_name,
                aggregation_type=definition.aggregation_type,
                time_window=window_size,
                value=agg_value,
                data_points=len(data_points),
                dimensions=aggregated_dimensions,
                confidence=confidence
            )
            
        except Exception as e:
            self.logger.error(f"Failed to calculate aggregation: {str(e)}")
            return None
    
    async def _send_aggregation_to_datadog(self, aggregation: MetricAggregation):
        """Send aggregated metric to DataDog"""
        
        try:
            tags = [
                f"metric:{aggregation.metric_name}",
                f"aggregation:{aggregation.aggregation_type.value}",
                f"window_seconds:{int(aggregation.time_window.total_seconds())}",
                f"data_points:{aggregation.data_points}",
                f"confidence:{aggregation.confidence:.2f}"
            ]
            
            # Add dimension tags
            for dim_key, dim_value in aggregation.dimensions.items():
                tags.append(f"{dim_key}:{dim_value}")
            
            self.datadog_monitoring.gauge(
                f"custom.aggregated.{aggregation.metric_name}",
                aggregation.value,
                tags=tags
            )
            
        except Exception as e:
            self.logger.error(f"Failed to send aggregation to DataDog: {str(e)}")
    
    async def _kpi_calculation_worker(self):
        """Background worker for calculating business KPIs"""
        
        while True:
            try:
                await asyncio.sleep(60)  # Calculate KPIs every minute
                
                for kpi_id, kpi in self.business_kpis.items():
                    try:
                        # Get latest values for dependent metrics
                        metric_values = {}
                        
                        for metric_name in kpi.dependent_metrics:
                            if metric_name in self.aggregated_metrics:
                                recent_aggs = self.aggregated_metrics[metric_name][-10:]  # Last 10 aggregations
                                if recent_aggs:
                                    # Use most recent aggregation
                                    metric_values[metric_name] = recent_aggs[-1].value
                        
                        # Calculate KPI if all dependent metrics are available
                        if len(metric_values) == len(kpi.dependent_metrics):
                            kpi_value = await self._evaluate_kpi_formula(kpi.formula, metric_values)
                            
                            if kpi_value is not None:
                                # Submit KPI as metric
                                await self.submit_metric(
                                    f"kpi_{kpi_id}",
                                    kpi_value,
                                    metadata={
                                        "kpi_name": kpi.name,
                                        "business_owner": kpi.business_owner,
                                        "board_level": kpi.board_level,
                                        "target_value": kpi.target_value
                                    }
                                )
                    
                    except Exception as e:
                        self.logger.error(f"Failed to calculate KPI {kpi_id}: {str(e)}")
                
            except Exception as e:
                self.logger.error(f"Error in KPI calculation worker: {str(e)}")
                await asyncio.sleep(60)
    
    async def _evaluate_kpi_formula(self, formula: str, metric_values: Dict[str, float]) -> Optional[float]:
        """Safely evaluate KPI formula"""
        
        try:
            # Simple formula evaluation - in production, use a safer expression evaluator
            # This is a basic implementation for demonstration
            
            evaluated_formula = formula
            for metric_name, value in metric_values.items():
                evaluated_formula = evaluated_formula.replace(metric_name, str(value))
            
            # Only allow basic mathematical operations
            allowed_chars = set('0123456789+-*/(). ')
            if all(c in allowed_chars for c in evaluated_formula):
                result = eval(evaluated_formula)
                return float(result)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to evaluate KPI formula: {str(e)}")
            return None
    
    async def _quality_monitoring_worker(self):
        """Background worker for monitoring data quality"""
        
        while True:
            try:
                await asyncio.sleep(300)  # Monitor quality every 5 minutes
                
                current_time = datetime.utcnow()
                
                # Calculate quality metrics for each metric
                for metric_name, definition in self.metric_definitions.items():
                    try:
                        # Calculate average quality score
                        if metric_name in self.quality_scores and self.quality_scores[metric_name]:
                            avg_quality = statistics.mean(self.quality_scores[metric_name][-100:])  # Last 100 points
                            
                            # Send quality metric
                            if self.datadog_monitoring:
                                self.datadog_monitoring.gauge(
                                    f"custom.quality.average_score",
                                    avg_quality,
                                    tags=[f"metric:{metric_name}", f"service:{self.service_name}"]
                                )
                        
                        # Track anomaly rate
                        if metric_name in self.anomaly_counts:
                            total_points = len(self.metric_data[metric_name])
                            anomaly_rate = self.anomaly_counts[metric_name] / max(total_points, 1) * 100
                            
                            if self.datadog_monitoring:
                                self.datadog_monitoring.gauge(
                                    f"custom.anomaly.rate",
                                    anomaly_rate,
                                    tags=[f"metric:{metric_name}", f"service:{self.service_name}"]
                                )
                    
                    except Exception as e:
                        self.logger.error(f"Failed to monitor quality for {metric_name}: {str(e)}")
                
                # Track validation errors
                recent_errors = [
                    error for error in self.data_validation_errors
                    if datetime.fromisoformat(error["timestamp"]) >= current_time - timedelta(minutes=5)
                ]
                
                if self.datadog_monitoring:
                    self.datadog_monitoring.gauge(
                        f"custom.validation.errors_rate",
                        len(recent_errors),
                        tags=[f"service:{self.service_name}"]
                    )
                
            except Exception as e:
                self.logger.error(f"Error in quality monitoring worker: {str(e)}")
                await asyncio.sleep(300)
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        
        try:
            current_time = datetime.utcnow()
            
            # Calculate summary statistics
            total_metrics = len(self.metric_definitions)
            metrics_with_data = len([m for m in self.metric_data.keys() if self.metric_data[m]])
            
            # Processing performance
            avg_processing_time = statistics.mean(self.processing_times) if self.processing_times else 0
            
            # Quality summary
            overall_quality_scores = []
            for metric_scores in self.quality_scores.values():
                if metric_scores:
                    overall_quality_scores.extend(metric_scores[-10:])  # Last 10 per metric
            
            avg_quality_score = statistics.mean(overall_quality_scores) if overall_quality_scores else 0
            
            # Category breakdown
            category_counts = defaultdict(int)
            for definition in self.metric_definitions.values():
                category_counts[definition.category.value] += 1
            
            # Business impact summary
            business_impact_counts = defaultdict(int)
            for definition in self.metric_definitions.values():
                business_impact_counts[definition.business_impact] += 1
            
            return {
                "timestamp": current_time.isoformat(),
                "service_name": self.service_name,
                "metrics": {
                    "total_defined": total_metrics,
                    "with_data": metrics_with_data,
                    "total_processed": self.metrics_processed,
                    "processing_rate_per_minute": self.metrics_processed / max(1, (current_time - datetime.utcnow()).total_seconds() / 60)
                },
                "performance": {
                    "avg_processing_time_ms": avg_processing_time * 1000,
                    "queue_size": self.processing_queue.qsize(),
                    "active_aggregation_tasks": len(self.aggregation_tasks)
                },
                "quality": {
                    "avg_quality_score": avg_quality_score,
                    "total_anomalies": sum(self.anomaly_counts.values()),
                    "validation_errors_last_hour": len([
                        e for e in self.data_validation_errors
                        if datetime.fromisoformat(e["timestamp"]) >= current_time - timedelta(hours=1)
                    ])
                },
                "business_kpis": {
                    "total_defined": len(self.business_kpis),
                    "board_level_kpis": len([k for k in self.business_kpis.values() if k.board_level])
                },
                "categories": dict(category_counts),
                "business_impact": dict(business_impact_counts),
                "cost_tracking": self.cost_tracking.copy()
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate metrics summary: {str(e)}")
            return {}


# Global custom metrics instance
_custom_metrics_advanced: Optional[DataDogCustomMetricsAdvanced] = None


def get_custom_metrics_advanced(service_name: str = "custom-metrics-advanced",
                              datadog_monitoring: Optional[DatadogMonitoring] = None) -> DataDogCustomMetricsAdvanced:
    """Get or create advanced custom metrics instance"""
    global _custom_metrics_advanced
    
    if _custom_metrics_advanced is None:
        _custom_metrics_advanced = DataDogCustomMetricsAdvanced(service_name, datadog_monitoring)
    
    return _custom_metrics_advanced


# Convenience functions

async def submit_business_metric(metric_name: str, value: float, 
                               dimensions: Optional[Dict[str, str]] = None) -> bool:
    """Convenience function for submitting business metrics"""
    metrics_system = get_custom_metrics_advanced()
    return await metrics_system.submit_metric(metric_name, value, dimensions)


async def submit_quality_metric(metric_name: str, value: float,
                              dataset: str, source: str) -> bool:
    """Convenience function for submitting data quality metrics"""
    metrics_system = get_custom_metrics_advanced()
    return await metrics_system.submit_metric(
        metric_name, value, 
        dimensions={"dataset": dataset, "source": source}
    )


async def submit_performance_metric(metric_name: str, value: float,
                                  service: str, environment: str) -> bool:
    """Convenience function for submitting performance metrics"""
    metrics_system = get_custom_metrics_advanced()
    return await metrics_system.submit_metric(
        metric_name, value,
        dimensions={"service": service, "environment": environment}
    )


def get_metrics_summary() -> Dict[str, Any]:
    """Convenience function for getting metrics summary"""
    metrics_system = get_custom_metrics_advanced()
    return metrics_system.get_metrics_summary()
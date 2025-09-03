"""
Advanced DataDog Custom Metrics Implementation
Provides comprehensive custom metrics management with intelligent aggregation,
data quality tracking, business KPIs, and automated metric generation
"""

import asyncio
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import numpy as np

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
    dimensions: list[str] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)

    # Thresholds and alerting
    warning_threshold: float | None = None
    critical_threshold: float | None = None
    higher_is_better: bool = True

    # Data quality settings
    expected_range: tuple[float, float] | None = None
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
    dimensions: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
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
    dimensions: dict[str, str] = field(default_factory=dict)
    confidence: float = 1.0
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class BusinessKPI:
    """Business KPI definition with automatic calculation"""

    kpi_id: str
    name: str
    description: str
    formula: str  # Mathematical formula using metric names
    target_value: float | None = None
    unit: str = "number"
    calculation_frequency: MetricFrequency = MetricFrequency.MEDIUM
    dependent_metrics: list[str] = field(default_factory=list)
    business_owner: str | None = None
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

    def __init__(
        self,
        service_name: str = "custom-metrics-advanced",
        datadog_monitoring: DatadogMonitoring | None = None,
    ):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")

        # Metric registry and storage
        self.metric_definitions: dict[str, CustomMetricDefinition] = {}
        self.business_kpis: dict[str, BusinessKPI] = {}
        self.metric_data: dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.aggregated_metrics: dict[str, list[MetricAggregation]] = defaultdict(list)

        # Real-time processing
        self.processing_queue: asyncio.Queue = asyncio.Queue()
        self.aggregation_tasks: dict[str, asyncio.Task] = {}
        self.baseline_values: dict[str, float] = {}

        # Quality tracking
        self.quality_scores: dict[str, list[float]] = defaultdict(list)
        self.anomaly_counts: dict[str, int] = defaultdict(int)
        self.data_validation_errors: list[dict[str, Any]] = []

        # Enhanced caching for expensive calculations
        self._health_score_cache = None
        self._health_score_cache_time = 0
        self._health_score_cache_ttl = 60  # 1 minute TTL for health score

        # Performance monitoring for the monitoring system
        self._calculation_times = deque(maxlen=100)
        self._cache_hit_rate = deque(maxlen=100)

        # Performance tracking
        self.metrics_processed = 0
        self.processing_times = deque(maxlen=1000)
        self.cost_tracking = {"total_datapoints": 0, "estimated_cost": 0.0}

        # Batch processing optimization
        self._metric_batch = []
        self._batch_size = 100
        self._last_batch_send = time.time()
        self._batch_interval = 30  # Send batch every 30 seconds

        # Rate limiting
        self._api_calls_per_minute = deque(maxlen=60)
        self._max_api_calls_per_minute = 300  # DataDog API limit

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
                anomaly_detection=True,
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
                business_impact="high",
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
                forecast_enabled=True,
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
                sla_critical=True,
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
                anomaly_detection=True,
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
                higher_is_better=False,
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
                business_impact="medium",
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
                auto_baseline=True,
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
                sla_critical=True,
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
                anomaly_detection=True,
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
                sla_critical=True,
            ),
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
                board_level=True,
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
                board_level=True,
            ),
            BusinessKPI(
                kpi_id="operational_efficiency_ratio",
                name="Operational Efficiency Ratio",
                description="Efficiency calculated from resource utilization and cost per transaction",
                formula="resource_utilization_efficiency / (cost_per_transaction * 100)",
                target_value=0.8,
                unit="ratio",
                dependent_metrics=["resource_utilization_efficiency", "cost_per_transaction"],
                business_owner="COO",
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
                board_level=True,
            ),
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

    async def submit_metric(
        self,
        metric_name: str,
        value: float,
        dimensions: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None,
        timestamp: datetime | None = None,
    ) -> bool:
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
                metadata=metadata or {},
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

    async def _validate_data_point(
        self, definition: CustomMetricDefinition, data_point: MetricDataPoint
    ) -> float:
        """Validate data point and return quality score"""

        try:
            quality_score = 1.0

            # Check expected range
            if definition.expected_range:
                min_val, max_val = definition.expected_range
                if not (min_val <= data_point.value <= max_val):
                    quality_score -= 0.3
                    self.data_validation_errors.append(
                        {
                            "timestamp": data_point.timestamp.isoformat(),
                            "metric": definition.name,
                            "error": "value_out_of_range",
                            "value": data_point.value,
                            "expected_range": definition.expected_range,
                        }
                    )

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

    async def _detect_anomaly(
        self, definition: CustomMetricDefinition, data_point: MetricDataPoint
    ) -> bool:
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
                self.datadog_monitoring.gauge(f"custom.{metric_name}", data_point.value, tags=tags)
            elif definition.metric_type == MetricType.COUNT:
                self.datadog_monitoring.counter(
                    f"custom.{metric_name}", data_point.value, tags=tags
                )
            elif definition.metric_type == MetricType.HISTOGRAM:
                self.datadog_monitoring.histogram(
                    f"custom.{metric_name}", data_point.value, tags=tags
                )
            elif definition.metric_type == MetricType.DISTRIBUTION:
                self.datadog_monitoring.distribution(
                    f"custom.{metric_name}", data_point.value, tags=tags
                )

            # Send quality metrics
            self.datadog_monitoring.gauge(
                f"custom.metric_quality.{metric_name}", data_point.quality_score, tags=tags
            )

            # Send anomaly counter
            if data_point.is_anomaly:
                self.datadog_monitoring.counter(f"custom.anomaly.{metric_name}", tags=tags)

        except Exception as e:
            self.logger.error(f"Failed to send metric to DataDog: {str(e)}")

    async def _metric_aggregation_worker(
        self, metric_name: str, definition: CustomMetricDefinition
    ):
        """Background worker for metric aggregation"""

        # Calculate sleep interval based on frequency
        frequency_intervals = {
            MetricFrequency.REAL_TIME: 1,
            MetricFrequency.HIGH: 10,
            MetricFrequency.MEDIUM: 30,
            MetricFrequency.LOW: 60,
            MetricFrequency.PERIODIC: 300,
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
                    dp for dp in self.metric_data[metric_name] if dp.timestamp >= cutoff_time
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
                        self.aggregated_metrics[metric_name] = self.aggregated_metrics[metric_name][
                            -1000:
                        ]

                    # Send aggregated metric to DataDog
                    if self.datadog_monitoring:
                        await self._send_aggregation_to_datadog(aggregation)

            except Exception as e:
                self.logger.error(f"Error in aggregation worker for {metric_name}: {str(e)}")
                await asyncio.sleep(interval)

    async def _calculate_aggregation(
        self,
        metric_name: str,
        definition: CustomMetricDefinition,
        data_points: list[MetricDataPoint],
    ) -> MetricAggregation | None:
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
                time_span = (
                    max(dp.timestamp for dp in data_points)
                    - min(dp.timestamp for dp in data_points)
                ).total_seconds()
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
                    aggregated_dimensions[dim_key] = max(value_counts.items(), key=lambda x: x[1])[
                        0
                    ]

            return MetricAggregation(
                metric_name=metric_name,
                aggregation_type=definition.aggregation_type,
                time_window=window_size,
                value=agg_value,
                data_points=len(data_points),
                dimensions=aggregated_dimensions,
                confidence=confidence,
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
                f"confidence:{aggregation.confidence:.2f}",
            ]

            # Add dimension tags
            for dim_key, dim_value in aggregation.dimensions.items():
                tags.append(f"{dim_key}:{dim_value}")

            self.datadog_monitoring.gauge(
                f"custom.aggregated.{aggregation.metric_name}", aggregation.value, tags=tags
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
                                recent_aggs = self.aggregated_metrics[metric_name][
                                    -10:
                                ]  # Last 10 aggregations
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
                                        "target_value": kpi.target_value,
                                    },
                                )

                    except Exception as e:
                        self.logger.error(f"Failed to calculate KPI {kpi_id}: {str(e)}")

            except Exception as e:
                self.logger.error(f"Error in KPI calculation worker: {str(e)}")
                await asyncio.sleep(60)

    async def _evaluate_kpi_formula(
        self, formula: str, metric_values: dict[str, float]
    ) -> float | None:
        """Safely evaluate KPI formula using ast-based expression evaluator"""

        try:
            import ast
            import operator

            # Safe operators allowed in expressions
            safe_operators = {
                ast.Add: operator.add,
                ast.Sub: operator.sub,
                ast.Mult: operator.mul,
                ast.Div: operator.truediv,
                ast.USub: operator.neg,
                ast.UAdd: operator.pos,
            }

            def safe_eval_expr(node):
                if isinstance(node, ast.Expression):
                    return safe_eval_expr(node.body)
                elif isinstance(node, ast.Num):  # Python < 3.8
                    return node.n
                elif isinstance(node, ast.Constant):  # Python >= 3.8
                    return node.value
                elif isinstance(node, ast.BinOp):
                    left = safe_eval_expr(node.left)
                    right = safe_eval_expr(node.right)
                    op = safe_operators.get(type(node.op))
                    if op is None:
                        raise ValueError(f"Unsupported operator: {type(node.op)}")
                    return op(left, right)
                elif isinstance(node, ast.UnaryOp):
                    operand = safe_eval_expr(node.operand)
                    op = safe_operators.get(type(node.op))
                    if op is None:
                        raise ValueError(f"Unsupported operator: {type(node.op)}")
                    return op(operand)
                elif isinstance(node, ast.Name):
                    # Handle variable substitution
                    if node.id in metric_values:
                        return metric_values[node.id]
                    raise ValueError(f"Unknown variable: {node.id}")
                else:
                    raise ValueError(f"Unsupported node type: {type(node)}")

            # Parse and evaluate the formula safely
            try:
                tree = ast.parse(formula, mode="eval")
                result = safe_eval_expr(tree)
                return float(result)
            except (SyntaxError, ValueError) as e:
                self.logger.warning(f"Invalid formula syntax: {formula} - {str(e)}")
                return None

        except Exception as e:
            self.logger.error(f"Failed to evaluate KPI formula: {str(e)}")
            return None

    async def _quality_monitoring_worker(self):
        """Background worker for monitoring data quality with enhanced SLI/SLO tracking"""

        while True:
            try:
                await asyncio.sleep(300)  # Monitor quality every 5 minutes

                current_time = datetime.utcnow()

                # Calculate quality metrics for each metric
                for metric_name, definition in self.metric_definitions.items():
                    try:
                        # Calculate average quality score
                        if metric_name in self.quality_scores and self.quality_scores[metric_name]:
                            avg_quality = statistics.mean(
                                self.quality_scores[metric_name][-100:]
                            )  # Last 100 points

                            # Send quality metric
                            if self.datadog_monitoring:
                                self.datadog_monitoring.gauge(
                                    "custom.quality.average_score",
                                    avg_quality,
                                    tags=[
                                        f"metric:{metric_name}",
                                        f"service:{self.service_name}",
                                        f"category:{definition.category.value}",
                                        f"sla_critical:{definition.sla_critical}",
                                    ],
                                )

                                # Track SLO compliance
                                if definition.sla_critical:
                                    slo_threshold = 0.95  # 95% SLO for critical metrics
                                    slo_compliance = 1.0 if avg_quality >= slo_threshold else 0.0

                                    self.datadog_monitoring.gauge(
                                        "custom.slo.compliance",
                                        slo_compliance,
                                        tags=[
                                            f"metric:{metric_name}",
                                            f"service:{self.service_name}",
                                            f"threshold:{slo_threshold}",
                                        ],
                                    )

                        # Track anomaly rate and trends
                        if metric_name in self.anomaly_counts:
                            total_points = len(self.metric_data[metric_name])
                            anomaly_rate = (
                                self.anomaly_counts[metric_name] / max(total_points, 1) * 100
                            )

                            if self.datadog_monitoring:
                                self.datadog_monitoring.gauge(
                                    "custom.anomaly.rate",
                                    anomaly_rate,
                                    tags=[
                                        f"metric:{metric_name}",
                                        f"service:{self.service_name}",
                                        f"business_impact:{definition.business_impact}",
                                    ],
                                )

                                # Track anomaly trend (increasing/decreasing)
                                recent_anomalies = sum(
                                    1
                                    for dp in list(self.metric_data[metric_name])[-50:]
                                    if dp.is_anomaly
                                )
                                older_anomalies = sum(
                                    1
                                    for dp in list(self.metric_data[metric_name])[-100:-50]
                                    if dp.is_anomaly
                                )

                                anomaly_trend = (
                                    "increasing"
                                    if recent_anomalies > older_anomalies
                                    else "decreasing"
                                )
                                self.datadog_monitoring.gauge(
                                    "custom.anomaly.trend",
                                    1.0 if anomaly_trend == "increasing" else 0.0,
                                    tags=[
                                        f"metric:{metric_name}",
                                        f"service:{self.service_name}",
                                        f"trend:{anomaly_trend}",
                                    ],
                                )

                        # Track data freshness and staleness
                        if metric_name in self.metric_data and self.metric_data[metric_name]:
                            latest_timestamp = max(
                                dp.timestamp for dp in self.metric_data[metric_name]
                            )
                            staleness_minutes = (
                                current_time - latest_timestamp
                            ).total_seconds() / 60

                            if self.datadog_monitoring:
                                self.datadog_monitoring.gauge(
                                    "custom.data.staleness_minutes",
                                    staleness_minutes,
                                    tags=[f"metric:{metric_name}", f"service:{self.service_name}"],
                                )

                    except Exception as e:
                        self.logger.error(f"Failed to monitor quality for {metric_name}: {str(e)}")

                # Track validation errors with severity classification
                recent_errors = [
                    error
                    for error in self.data_validation_errors
                    if datetime.fromisoformat(error["timestamp"])
                    >= current_time - timedelta(minutes=5)
                ]

                # Classify errors by severity
                critical_errors = [
                    e for e in recent_errors if "critical" in e.get("error", "").lower()
                ]
                warning_errors = [e for e in recent_errors if e not in critical_errors]

                if self.datadog_monitoring:
                    self.datadog_monitoring.gauge(
                        "custom.validation.errors_rate",
                        len(recent_errors),
                        tags=[f"service:{self.service_name}"],
                    )

                    self.datadog_monitoring.gauge(
                        "custom.validation.critical_errors",
                        len(critical_errors),
                        tags=[f"service:{self.service_name}"],
                    )

                    self.datadog_monitoring.gauge(
                        "custom.validation.warning_errors",
                        len(warning_errors),
                        tags=[f"service:{self.service_name}"],
                    )

                # Calculate overall system health score
                system_health_score = self._calculate_system_health_score()
                if self.datadog_monitoring:
                    self.datadog_monitoring.gauge(
                        "custom.system.health_score",
                        system_health_score,
                        tags=[f"service:{self.service_name}"],
                    )

            except Exception as e:
                self.logger.error(f"Error in quality monitoring worker: {str(e)}")
                await asyncio.sleep(300)

    def _calculate_system_health_score(self) -> float:
        """Calculate overall system health score based on multiple factors with caching"""
        current_time = time.time()

        # Check cache first
        if (
            self._health_score_cache is not None
            and current_time - self._health_score_cache_time < self._health_score_cache_ttl
        ):
            self._cache_hit_rate.append(1)  # Cache hit
            return self._health_score_cache

        self._cache_hit_rate.append(0)  # Cache miss
        start_time = time.time()

        try:
            health_factors = []

            # Quality score factor (40% weight) - optimized calculation
            overall_quality_scores = []
            for metric_scores in self.quality_scores.values():
                if metric_scores:
                    # Use only last 5 instead of 10 for faster calculation
                    overall_quality_scores.extend(metric_scores[-5:])

            if overall_quality_scores:
                avg_quality = sum(overall_quality_scores) / len(
                    overall_quality_scores
                )  # Faster than statistics.mean
                health_factors.append((avg_quality, 0.4))

            # Anomaly rate factor (30% weight) - optimized calculation
            total_points = sum(len(data) for data in self.metric_data.values())
            total_anomalies = sum(self.anomaly_counts.values())

            if total_points > 0:
                anomaly_rate = total_anomalies / total_points
                anomaly_score = max(0, 1.0 - (anomaly_rate * 10))
                health_factors.append((anomaly_score, 0.3))

            # Processing performance factor (20% weight) - optimized calculation
            if self.processing_times:
                # Use last 10 samples instead of all for faster calculation
                recent_times = list(self.processing_times)[-10:]
                avg_processing_time = sum(recent_times) / len(recent_times)
                perf_score = max(0, min(1.0, 1.0 - (avg_processing_time / 0.01)))
                health_factors.append((perf_score, 0.2))

            # Error rate factor (10% weight) - optimized calculation
            cutoff_time = datetime.utcnow() - timedelta(hours=1)
            recent_errors = sum(
                1
                for e in self.data_validation_errors[-50:]  # Only check last 50 errors
                if datetime.fromisoformat(e["timestamp"]) >= cutoff_time
            )
            error_score = max(0, 1.0 - (recent_errors / 100))
            health_factors.append((error_score, 0.1))

            # Calculate weighted average
            if health_factors:
                weighted_sum = sum(score * weight for score, weight in health_factors)
                total_weight = sum(weight for _, weight in health_factors)
                result = weighted_sum / total_weight if total_weight > 0 else 0.0
            else:
                result = 1.0  # Default to healthy if no data

            # Cache the result
            self._health_score_cache = result
            self._health_score_cache_time = current_time

            # Track calculation time
            calculation_time = time.time() - start_time
            self._calculation_times.append(calculation_time)

            return result

        except Exception as e:
            self.logger.error(f"Failed to calculate system health score: {str(e)}")
            return 0.5

    def get_cache_performance_stats(self) -> dict[str, Any]:
        """Get performance statistics for monitoring system caching"""
        if not self._cache_hit_rate:
            return {"cache_hit_rate": 0.0, "avg_calculation_time": 0.0, "cache_status": "no_data"}

        cache_hit_rate = (
            sum(self._cache_hit_rate) / len(self._cache_hit_rate) if self._cache_hit_rate else 0.0
        )
        avg_calc_time = (
            sum(self._calculation_times) / len(self._calculation_times)
            if self._calculation_times
            else 0.0
        )

        return {
            "cache_hit_rate": cache_hit_rate,
            "avg_calculation_time_ms": avg_calc_time * 1000,
            "cache_status": "optimal"
            if cache_hit_rate > 0.8
            else "suboptimal"
            if cache_hit_rate > 0.5
            else "poor",
            "total_calculations": len(self._calculation_times),
            "health_score_cache_ttl": self._health_score_cache_ttl,
            "batch_size": len(self._metric_batch),
            "api_rate_limit_status": self._get_api_rate_limit_status(),
        }

    def _get_api_rate_limit_status(self) -> dict[str, Any]:
        """Get API rate limit status"""
        current_minute_calls = len(self._api_calls_per_minute)
        usage_percent = (current_minute_calls / self._max_api_calls_per_minute) * 100

        return {
            "calls_this_minute": current_minute_calls,
            "max_calls_per_minute": self._max_api_calls_per_minute,
            "usage_percent": usage_percent,
            "status": "critical"
            if usage_percent > 90
            else "warning"
            if usage_percent > 70
            else "healthy",
        }

    async def _add_metric_to_batch(
        self, metric_name: str, value: float, tags: dict = None, timestamp: float = None
    ):
        """Add metric to batch for efficient API calls"""
        current_time = time.time()

        metric_data = {
            "metric": metric_name,
            "value": value,
            "timestamp": timestamp or current_time,
            "tags": tags or {},
        }

        self._metric_batch.append(metric_data)

        # Send batch if size limit reached or time interval exceeded
        if (
            len(self._metric_batch) >= self._batch_size
            or current_time - self._last_batch_send >= self._batch_interval
        ):
            await self._send_metric_batch()

    async def _send_metric_batch(self):
        """Send batched metrics to DataDog API with rate limiting"""
        if not self._metric_batch:
            return

        # Check rate limiting
        if not self._can_make_api_call():
            self.logger.warning("API rate limit reached, delaying batch send")
            return

        try:
            start_time = time.time()

            # Group metrics by type for efficient batching
            batched_metrics = self._group_metrics_for_batch(self._metric_batch)

            # Send batched metrics
            for batch_group in batched_metrics:
                if self.datadog_monitoring:
                    await self._send_batch_to_datadog(batch_group)
                    self._record_api_call()

            # Track batch processing performance
            batch_time = time.time() - start_time
            self._processing_times.append(batch_time)

            self.logger.debug(
                f"Sent batch of {len(self._metric_batch)} metrics in {batch_time:.3f}s"
            )

            # Clear batch and update timestamp
            self._metric_batch.clear()
            self._last_batch_send = time.time()

        except Exception as e:
            self.logger.error(f"Failed to send metric batch: {e}")
            # Don't clear batch on failure - will retry next time

    def _group_metrics_for_batch(self, metrics: list) -> list[list]:
        """Group metrics into optimal batches for DataDog API"""
        # DataDog API accepts up to 100 metrics per call
        api_batch_size = 100
        return [metrics[i : i + api_batch_size] for i in range(0, len(metrics), api_batch_size)]

    async def _send_batch_to_datadog(self, batch: list):
        """Send a batch of metrics to DataDog"""
        if not batch:
            return

        # Format metrics for DataDog API
        formatted_metrics = []
        for metric in batch:
            formatted_metrics.append(
                {
                    "metric": metric["metric"],
                    "points": [[metric["timestamp"], metric["value"]]],
                    "tags": [f"{k}:{v}" for k, v in metric["tags"].items()],
                }
            )

        # Send to DataDog (using existing datadog_monitoring instance)
        if self.datadog_monitoring:
            # Simulate API call - replace with actual DataDog API call
            await asyncio.sleep(0.1)  # Simulate API latency
            self.cost_tracking["total_datapoints"] += len(formatted_metrics)

    def _can_make_api_call(self) -> bool:
        """Check if API call can be made within rate limits"""
        current_time = time.time()

        # Remove calls older than 1 minute
        while self._api_calls_per_minute and self._api_calls_per_minute[0] < current_time - 60:
            self._api_calls_per_minute.popleft()

        return len(self._api_calls_per_minute) < self._max_api_calls_per_minute

    def _record_api_call(self):
        """Record an API call for rate limiting"""
        self._api_calls_per_minute.append(time.time())

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get comprehensive metrics summary"""

        try:
            current_time = datetime.utcnow()

            # Calculate summary statistics
            total_metrics = len(self.metric_definitions)
            metrics_with_data = len([m for m in self.metric_data.keys() if self.metric_data[m]])

            # Processing performance
            avg_processing_time = (
                statistics.mean(self.processing_times) if self.processing_times else 0
            )

            # Quality summary
            overall_quality_scores = []
            for metric_scores in self.quality_scores.values():
                if metric_scores:
                    overall_quality_scores.extend(metric_scores[-10:])  # Last 10 per metric

            avg_quality_score = (
                statistics.mean(overall_quality_scores) if overall_quality_scores else 0
            )

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
                    "processing_rate_per_minute": self.metrics_processed
                    / max(1, (current_time - datetime.utcnow()).total_seconds() / 60),
                },
                "performance": {
                    "avg_processing_time_ms": avg_processing_time * 1000,
                    "queue_size": self.processing_queue.qsize(),
                    "active_aggregation_tasks": len(self.aggregation_tasks),
                },
                "quality": {
                    "avg_quality_score": avg_quality_score,
                    "total_anomalies": sum(self.anomaly_counts.values()),
                    "validation_errors_last_hour": len(
                        [
                            e
                            for e in self.data_validation_errors
                            if datetime.fromisoformat(e["timestamp"])
                            >= current_time - timedelta(hours=1)
                        ]
                    ),
                },
                "business_kpis": {
                    "total_defined": len(self.business_kpis),
                    "board_level_kpis": len(
                        [k for k in self.business_kpis.values() if k.board_level]
                    ),
                },
                "categories": dict(category_counts),
                "business_impact": dict(business_impact_counts),
                "cost_tracking": self.cost_tracking.copy(),
                "slo_compliance": self._calculate_slo_compliance(),
                "predictive_insights": self._generate_predictive_insights(),
            }

        except Exception as e:
            self.logger.error(f"Failed to generate metrics summary: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}

    def _calculate_slo_compliance(self) -> dict[str, Any]:
        """Calculate SLO compliance for critical metrics"""
        try:
            slo_results = {}
            critical_metrics = [
                name for name, def_ in self.metric_definitions.items() if def_.sla_critical
            ]

            for metric_name in critical_metrics:
                if metric_name in self.quality_scores and self.quality_scores[metric_name]:
                    recent_scores = self.quality_scores[metric_name][-100:]  # Last 100 points
                    slo_threshold = 0.95

                    compliant_points = sum(1 for score in recent_scores if score >= slo_threshold)
                    total_points = len(recent_scores)

                    compliance_rate = compliant_points / total_points if total_points > 0 else 0

                    slo_results[metric_name] = {
                        "compliance_rate": compliance_rate,
                        "threshold": slo_threshold,
                        "compliant_points": compliant_points,
                        "total_points": total_points,
                        "status": "healthy"
                        if compliance_rate >= 0.99
                        else "warning"
                        if compliance_rate >= 0.95
                        else "critical",
                    }

            return slo_results

        except Exception as e:
            self.logger.error(f"Failed to calculate SLO compliance: {str(e)}")
            return {}

    def _generate_predictive_insights(self) -> dict[str, Any]:
        """Generate predictive insights for capacity planning and anomaly forecasting"""
        try:
            insights = {}

            # Analyze trends in metric values
            for metric_name, data_points in self.metric_data.items():
                if len(data_points) < 10:
                    continue

                recent_values = [dp.value for dp in list(data_points)[-50:]]  # Last 50 points

                if len(recent_values) >= 10:
                    # Simple trend analysis
                    first_half = recent_values[: len(recent_values) // 2]
                    second_half = recent_values[len(recent_values) // 2 :]

                    avg_first = statistics.mean(first_half)
                    avg_second = statistics.mean(second_half)

                    trend = (
                        "increasing"
                        if avg_second > avg_first * 1.05
                        else "decreasing"
                        if avg_second < avg_first * 0.95
                        else "stable"
                    )
                    trend_magnitude = (
                        abs(avg_second - avg_first) / avg_first if avg_first != 0 else 0
                    )

                    # Predict capacity needs
                    definition = self.metric_definitions.get(metric_name)
                    if definition and definition.warning_threshold:
                        current_avg = statistics.mean(recent_values)
                        threshold_ratio = current_avg / definition.warning_threshold

                        if trend == "increasing" and threshold_ratio > 0.7:
                            time_to_threshold = (
                                "soon"
                                if threshold_ratio > 0.9
                                else "medium"
                                if threshold_ratio > 0.8
                                else "later"
                            )
                        else:
                            time_to_threshold = "stable"
                    else:
                        time_to_threshold = "unknown"

                    insights[metric_name] = {
                        "trend": trend,
                        "trend_magnitude": round(trend_magnitude, 3),
                        "capacity_forecast": time_to_threshold,
                        "anomaly_likelihood": "high"
                        if trend_magnitude > 0.2
                        else "medium"
                        if trend_magnitude > 0.1
                        else "low",
                    }

            return insights

        except Exception as e:
            self.logger.error(f"Failed to generate predictive insights: {str(e)}")
            return {}


# Global custom metrics instance
_custom_metrics_advanced: DataDogCustomMetricsAdvanced | None = None


def get_custom_metrics_advanced(
    service_name: str = "custom-metrics-advanced",
    datadog_monitoring: DatadogMonitoring | None = None,
) -> DataDogCustomMetricsAdvanced:
    """Get or create advanced custom metrics instance"""
    global _custom_metrics_advanced

    if _custom_metrics_advanced is None:
        _custom_metrics_advanced = DataDogCustomMetricsAdvanced(service_name, datadog_monitoring)

    return _custom_metrics_advanced


# Convenience functions


async def submit_business_metric(
    metric_name: str, value: float, dimensions: dict[str, str] | None = None
) -> bool:
    """Convenience function for submitting business metrics"""
    metrics_system = get_custom_metrics_advanced()
    return await metrics_system.submit_metric(metric_name, value, dimensions)


async def submit_quality_metric(metric_name: str, value: float, dataset: str, source: str) -> bool:
    """Convenience function for submitting data quality metrics"""
    metrics_system = get_custom_metrics_advanced()
    return await metrics_system.submit_metric(
        metric_name, value, dimensions={"dataset": dataset, "source": source}
    )


async def submit_performance_metric(
    metric_name: str, value: float, service: str, environment: str
) -> bool:
    """Convenience function for submitting performance metrics"""
    metrics_system = get_custom_metrics_advanced()
    return await metrics_system.submit_metric(
        metric_name, value, dimensions={"service": service, "environment": environment}
    )


def get_metrics_summary() -> dict[str, Any]:
    """Convenience function for getting metrics summary"""
    metrics_system = get_custom_metrics_advanced()
    return metrics_system.get_metrics_summary()

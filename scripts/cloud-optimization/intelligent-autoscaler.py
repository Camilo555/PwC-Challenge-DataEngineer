#!/usr/bin/env python3
"""
Intelligent Cloud Auto-Scaling Engine
AI-powered resource optimization across AWS, Azure, and GCP
"""

import asyncio
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import boto3
import joblib
import numpy as np
import pandas as pd
import redis
from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from google.cloud import compute_v1, monitoring_v3
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)

class ScalingAction(Enum):
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    SCALE_OUT = "scale_out"
    SCALE_IN = "scale_in"
    NO_ACTION = "no_action"

class ResourceType(Enum):
    COMPUTE = "compute"
    STORAGE = "storage"
    DATABASE = "database"
    CACHE = "cache"
    NETWORK = "network"

@dataclass
class MetricData:
    """Cloud resource metrics"""
    timestamp: datetime
    resource_id: str
    resource_type: ResourceType
    cloud_provider: str
    cpu_utilization: float
    memory_utilization: float
    network_io: float
    disk_io: float
    request_count: int
    response_time_ms: float
    error_rate: float
    cost_per_hour: float

@dataclass
class ScalingRecommendation:
    """Auto-scaling recommendation"""
    resource_id: str
    current_capacity: int
    recommended_capacity: int
    scaling_action: ScalingAction
    confidence_score: float
    estimated_cost_impact: float
    estimated_performance_impact: float
    justification: str
    urgency: str  # low, medium, high, critical

@dataclass
class ResourcePrediction:
    """Resource demand prediction"""
    resource_id: str
    prediction_horizon_hours: int
    predicted_load: list[float]
    confidence_intervals: list[tuple[float, float]]
    seasonal_patterns: dict[str, float]
    anomaly_score: float

class IntelligentAutoScaler:
    """AI-powered cloud resource auto-scaler"""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.redis_client = None
        self.ml_models = {}
        self.setup_clients()
        self.setup_ml_pipeline()

    def setup_clients(self):
        """Initialize cloud provider clients"""
        try:
            # Redis for caching and coordination
            self.redis_client = redis.Redis(
                host=self.config.get('redis', {}).get('host', 'localhost'),
                port=self.config.get('redis', {}).get('port', 6379),
                decode_responses=True
            )

            # AWS CloudWatch
            if self.config.get('aws', {}).get('enabled', False):
                self.aws_cloudwatch = boto3.client(
                    'cloudwatch',
                    region_name=self.config['aws'].get('region', 'us-east-1'),
                    aws_access_key_id=self.config['aws'].get('access_key'),
                    aws_secret_access_key=self.config['aws'].get('secret_key')
                )
                self.aws_autoscaling = boto3.client(
                    'application-autoscaling',
                    region_name=self.config['aws'].get('region', 'us-east-1'),
                    aws_access_key_id=self.config['aws'].get('access_key'),
                    aws_secret_access_key=self.config['aws'].get('secret_key')
                )

            # Azure Monitor
            if self.config.get('azure', {}).get('enabled', False):
                credential = DefaultAzureCredential()
                self.azure_monitor = MonitorManagementClient(
                    credential,
                    subscription_id=self.config['azure'].get('subscription_id')
                )
                self.azure_compute = ComputeManagementClient(
                    credential,
                    subscription_id=self.config['azure'].get('subscription_id')
                )

            # GCP Monitoring
            if self.config.get('gcp', {}).get('enabled', False):
                self.gcp_monitoring = monitoring_v3.MetricServiceClient()
                self.gcp_compute = compute_v1.InstancesClient()
                self.gcp_project_id = self.config['gcp'].get('project_id')

        except Exception as e:
            logger.error(f"Error setting up cloud clients: {e}")

    def setup_ml_pipeline(self):
        """Initialize ML models for prediction and optimization"""
        # Load pre-trained models or initialize new ones
        model_path = self.config.get('ml', {}).get('model_path', './models')

        for resource_type in ResourceType:
            model_file = f"{model_path}/{resource_type.value}_predictor.joblib"
            try:
                self.ml_models[resource_type] = joblib.load(model_file)
                logger.info(f"Loaded pre-trained model for {resource_type.value}")
            except FileNotFoundError:
                # Initialize new model
                self.ml_models[resource_type] = RandomForestRegressor(
                    n_estimators=100,
                    random_state=42,
                    n_jobs=-1
                )
                logger.info(f"Initialized new model for {resource_type.value}")

    async def analyze_resource_utilization(
        self,
        time_window_hours: int = 24
    ) -> list[MetricData]:
        """Collect and analyze resource utilization across clouds"""
        logger.info(f"Analyzing resource utilization for the last {time_window_hours} hours")

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=time_window_hours)

        all_metrics = []

        # Collect metrics from all enabled cloud providers
        tasks = []
        if self.config.get('aws', {}).get('enabled', False):
            tasks.append(self._collect_aws_metrics(start_time, end_time))
        if self.config.get('azure', {}).get('enabled', False):
            tasks.append(self._collect_azure_metrics(start_time, end_time))
        if self.config.get('gcp', {}).get('enabled', False):
            tasks.append(self._collect_gcp_metrics(start_time, end_time))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, list):
                    all_metrics.extend(result)
                else:
                    logger.error(f"Error collecting metrics: {result}")

        logger.info(f"Collected {len(all_metrics)} metric data points")
        return all_metrics

    async def _collect_aws_metrics(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> list[MetricData]:
        """Collect metrics from AWS CloudWatch"""
        metrics = []

        try:
            # Get list of EC2 instances
            ec2 = boto3.client(
                'ec2',
                region_name=self.config['aws'].get('region', 'us-east-1'),
                aws_access_key_id=self.config['aws'].get('access_key'),
                aws_secret_access_key=self.config['aws'].get('secret_key')
            )

            instances_response = ec2.describe_instances()

            for reservation in instances_response['Reservations']:
                for instance in reservation['Instances']:
                    if instance['State']['Name'] == 'running':
                        instance_id = instance['InstanceId']

                        # Get CloudWatch metrics for this instance
                        metric_data = await self._get_aws_instance_metrics(
                            instance_id, start_time, end_time
                        )
                        metrics.extend(metric_data)

        except Exception as e:
            logger.error(f"Error collecting AWS metrics: {e}")

        return metrics

    async def _get_aws_instance_metrics(
        self,
        instance_id: str,
        start_time: datetime,
        end_time: datetime
    ) -> list[MetricData]:
        """Get detailed metrics for an AWS EC2 instance"""
        metrics = []

        metric_queries = [
            ('CPUUtilization', 'AWS/EC2', 'Average'),
            ('NetworkIn', 'AWS/EC2', 'Average'),
            ('NetworkOut', 'AWS/EC2', 'Average'),
            ('DiskReadOps', 'AWS/EBS', 'Average'),
            ('DiskWriteOps', 'AWS/EBS', 'Average')
        ]

        try:
            for metric_name, namespace, statistic in metric_queries:
                response = self.aws_cloudwatch.get_metric_statistics(
                    Namespace=namespace,
                    MetricName=metric_name,
                    Dimensions=[
                        {
                            'Name': 'InstanceId',
                            'Value': instance_id
                        }
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=300,  # 5-minute intervals
                    Statistics=[statistic]
                )

                for datapoint in response['Datapoints']:
                    # Create metric data point
                    # Note: This is simplified - in practice, you'd combine all metrics per timestamp
                    metric_data = MetricData(
                        timestamp=datapoint['Timestamp'],
                        resource_id=instance_id,
                        resource_type=ResourceType.COMPUTE,
                        cloud_provider='aws',
                        cpu_utilization=datapoint.get('Average', 0) if metric_name == 'CPUUtilization' else 0,
                        memory_utilization=0,  # Would need custom CloudWatch agent
                        network_io=datapoint.get('Average', 0) if 'Network' in metric_name else 0,
                        disk_io=datapoint.get('Average', 0) if 'Disk' in metric_name else 0,
                        request_count=0,  # Would come from application metrics
                        response_time_ms=0,  # Would come from application metrics
                        error_rate=0,  # Would come from application metrics
                        cost_per_hour=self._estimate_aws_instance_cost(instance_id)
                    )
                    metrics.append(metric_data)

        except Exception as e:
            logger.error(f"Error getting AWS instance metrics for {instance_id}: {e}")

        return metrics

    async def _collect_azure_metrics(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> list[MetricData]:
        """Collect metrics from Azure Monitor"""
        metrics = []

        try:
            # Get list of Azure VMs
            vms = self.azure_compute.virtual_machines.list_all()

            for vm in vms:
                vm_id = vm.id

                # Get metrics for this VM
                metric_data = await self._get_azure_vm_metrics(
                    vm_id, start_time, end_time
                )
                metrics.extend(metric_data)

        except Exception as e:
            logger.error(f"Error collecting Azure metrics: {e}")

        return metrics

    async def _get_azure_vm_metrics(
        self,
        vm_id: str,
        start_time: datetime,
        end_time: datetime
    ) -> list[MetricData]:
        """Get detailed metrics for an Azure VM"""
        metrics = []

        metric_names = [
            'Percentage CPU',
            'Network In Total',
            'Network Out Total',
            'Disk Read Operations/Sec',
            'Disk Write Operations/Sec'
        ]

        try:
            for metric_name in metric_names:
                metric_response = self.azure_monitor.metrics.list(
                    resource_uri=vm_id,
                    metricnames=metric_name,
                    timespan=f"{start_time.isoformat()}/{end_time.isoformat()}",
                    interval='PT5M',  # 5-minute intervals
                    aggregation='Average'
                )

                for metric in metric_response.value:
                    for timeseries in metric.timeseries:
                        for datapoint in timeseries.data:
                            if datapoint.average is not None:
                                metric_data = MetricData(
                                    timestamp=datapoint.time_stamp,
                                    resource_id=vm_id,
                                    resource_type=ResourceType.COMPUTE,
                                    cloud_provider='azure',
                                    cpu_utilization=datapoint.average if metric_name == 'Percentage CPU' else 0,
                                    memory_utilization=0,  # Would need additional metrics
                                    network_io=datapoint.average if 'Network' in metric_name else 0,
                                    disk_io=datapoint.average if 'Disk' in metric_name else 0,
                                    request_count=0,
                                    response_time_ms=0,
                                    error_rate=0,
                                    cost_per_hour=self._estimate_azure_vm_cost(vm_id)
                                )
                                metrics.append(metric_data)

        except Exception as e:
            logger.error(f"Error getting Azure VM metrics for {vm_id}: {e}")

        return metrics

    async def _collect_gcp_metrics(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> list[MetricData]:
        """Collect metrics from GCP Monitoring"""
        metrics = []

        try:
            # Get list of GCP instances

            # List all zones (simplified - in practice, might want to filter)
            zones_client = compute_v1.ZonesClient()
            zones = zones_client.list(project=self.gcp_project_id)

            for zone in zones:
                instances = self.gcp_compute.list(
                    project=self.gcp_project_id,
                    zone=zone.name
                )

                for instance in instances:
                    if instance.status == 'RUNNING':
                        # Get metrics for this instance
                        metric_data = await self._get_gcp_instance_metrics(
                            instance.name, zone.name, start_time, end_time
                        )
                        metrics.extend(metric_data)

        except Exception as e:
            logger.error(f"Error collecting GCP metrics: {e}")

        return metrics

    async def _get_gcp_instance_metrics(
        self,
        instance_name: str,
        zone: str,
        start_time: datetime,
        end_time: datetime
    ) -> list[MetricData]:
        """Get detailed metrics for a GCP instance"""
        metrics = []

        project_name = f"projects/{self.gcp_project_id}"

        metric_types = [
            'compute.googleapis.com/instance/cpu/utilization',
            'compute.googleapis.com/instance/network/received_bytes_count',
            'compute.googleapis.com/instance/network/sent_bytes_count',
            'compute.googleapis.com/instance/disk/read_ops_count',
            'compute.googleapis.com/instance/disk/write_ops_count'
        ]

        try:
            for metric_type in metric_types:
                interval = monitoring_v3.TimeInterval(
                    {
                        "end_time": {"seconds": int(end_time.timestamp())},
                        "start_time": {"seconds": int(start_time.timestamp())},
                    }
                )

                results = self.gcp_monitoring.list_time_series(
                    request={
                        "name": project_name,
                        "filter": f'metric.type="{metric_type}" AND resource.labels.instance_name="{instance_name}"',
                        "interval": interval,
                        "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                    }
                )

                for result in results:
                    for point in result.points:
                        metric_data = MetricData(
                            timestamp=datetime.fromtimestamp(
                                point.interval.end_time.seconds
                            ),
                            resource_id=f"{zone}/{instance_name}",
                            resource_type=ResourceType.COMPUTE,
                            cloud_provider='gcp',
                            cpu_utilization=point.value.double_value if 'cpu/utilization' in metric_type else 0,
                            memory_utilization=0,  # Would need additional metrics
                            network_io=point.value.int64_value if 'network' in metric_type else 0,
                            disk_io=point.value.int64_value if 'disk' in metric_type else 0,
                            request_count=0,
                            response_time_ms=0,
                            error_rate=0,
                            cost_per_hour=self._estimate_gcp_instance_cost(instance_name, zone)
                        )
                        metrics.append(metric_data)

        except Exception as e:
            logger.error(f"Error getting GCP instance metrics for {instance_name}: {e}")

        return metrics

    async def generate_scaling_recommendations(
        self,
        metrics: list[MetricData]
    ) -> list[ScalingRecommendation]:
        """Generate intelligent scaling recommendations using ML"""
        logger.info("Generating scaling recommendations using ML models")

        recommendations = []

        # Group metrics by resource
        resource_metrics = {}
        for metric in metrics:
            if metric.resource_id not in resource_metrics:
                resource_metrics[metric.resource_id] = []
            resource_metrics[metric.resource_id].append(metric)

        # Generate recommendations for each resource
        for resource_id, resource_metric_list in resource_metrics.items():
            if len(resource_metric_list) < 10:  # Need sufficient data points
                continue

            try:
                recommendation = await self._analyze_resource_scaling(
                    resource_id, resource_metric_list
                )
                if recommendation:
                    recommendations.append(recommendation)

            except Exception as e:
                logger.error(f"Error generating recommendation for {resource_id}: {e}")

        # Sort recommendations by urgency and confidence
        recommendations.sort(
            key=lambda r: (
                {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}[r.urgency],
                r.confidence_score
            ),
            reverse=True
        )

        logger.info(f"Generated {len(recommendations)} scaling recommendations")
        return recommendations

    async def _analyze_resource_scaling(
        self,
        resource_id: str,
        metrics: list[MetricData]
    ) -> ScalingRecommendation | None:
        """Analyze individual resource for scaling needs"""

        if not metrics:
            return None

        # Convert metrics to DataFrame for analysis
        df = pd.DataFrame([asdict(m) for m in metrics])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')

        # Calculate statistical features
        df['cpu_utilization'].mean()
        cpu_max = df['cpu_utilization'].max()
        df['cpu_utilization'].std()
        cpu_p95 = df['cpu_utilization'].quantile(0.95)

        df['memory_utilization'].mean()
        memory_max = df['memory_utilization'].max()
        memory_p95 = df['memory_utilization'].quantile(0.95)

        # Response time analysis
        response_time_mean = df['response_time_ms'].mean()
        response_time_p95 = df['response_time_ms'].quantile(0.95)

        # Error rate analysis
        error_rate_mean = df['error_rate'].mean()

        # Determine scaling action based on thresholds and ML prediction
        # resource_type = metrics[0].resource_type  # Not used currently
        current_capacity = 1  # Simplified - would get actual capacity

        # High utilization thresholds
        cpu_scale_up_threshold = self.config.get('thresholds', {}).get('cpu_scale_up', 80)
        cpu_scale_down_threshold = self.config.get('thresholds', {}).get('cpu_scale_down', 20)
        memory_scale_up_threshold = self.config.get('thresholds', {}).get('memory_scale_up', 80)
        response_time_threshold = self.config.get('thresholds', {}).get('response_time_ms', 1000)

        # Determine scaling action
        scaling_action = ScalingAction.NO_ACTION
        recommended_capacity = current_capacity
        confidence_score = 0.0
        justification = "No scaling needed"
        urgency = "low"

        # Scale up conditions
        if (cpu_p95 > cpu_scale_up_threshold or
            memory_p95 > memory_scale_up_threshold or
            response_time_p95 > response_time_threshold or
            error_rate_mean > 0.05):  # 5% error rate threshold

            scaling_action = ScalingAction.SCALE_UP
            recommended_capacity = current_capacity + 1
            confidence_score = min(95.0, cpu_p95 + memory_p95) / 100

            reasons = []
            if cpu_p95 > cpu_scale_up_threshold:
                reasons.append(f"High CPU utilization (P95: {cpu_p95:.1f}%)")
            if memory_p95 > memory_scale_up_threshold:
                reasons.append(f"High memory utilization (P95: {memory_p95:.1f}%)")
            if response_time_p95 > response_time_threshold:
                reasons.append(f"High response time (P95: {response_time_p95:.1f}ms)")
            if error_rate_mean > 0.05:
                reasons.append(f"High error rate ({error_rate_mean*100:.1f}%)")

            justification = "Scale up recommended: " + "; ".join(reasons)

            # Determine urgency
            if cpu_p95 > 90 or memory_p95 > 90 or error_rate_mean > 0.1:
                urgency = "critical"
            elif cpu_p95 > 85 or memory_p95 > 85 or response_time_p95 > 2000:
                urgency = "high"
            else:
                urgency = "medium"

        # Scale down conditions
        elif (cpu_max < cpu_scale_down_threshold and
              memory_max < cpu_scale_down_threshold and
              response_time_mean < response_time_threshold / 2 and
              error_rate_mean < 0.01):

            scaling_action = ScalingAction.SCALE_DOWN
            recommended_capacity = max(1, current_capacity - 1)
            confidence_score = (100 - max(cpu_max, memory_max)) / 100

            justification = f"Scale down opportunity: Low utilization (CPU max: {cpu_max:.1f}%, Memory max: {memory_max:.1f}%)"
            urgency = "low"

        # Estimate cost impact
        current_cost = df['cost_per_hour'].iloc[-1] if len(df) > 0 else 0
        estimated_cost_impact = (recommended_capacity - current_capacity) * current_cost

        # Estimate performance impact (simplified)
        estimated_performance_impact = (recommended_capacity / current_capacity - 1) * 100

        return ScalingRecommendation(
            resource_id=resource_id,
            current_capacity=current_capacity,
            recommended_capacity=recommended_capacity,
            scaling_action=scaling_action,
            confidence_score=confidence_score,
            estimated_cost_impact=estimated_cost_impact,
            estimated_performance_impact=estimated_performance_impact,
            justification=justification,
            urgency=urgency
        )

    async def predict_resource_demand(
        self,
        resource_id: str,
        metrics: list[MetricData],
        prediction_horizon_hours: int = 24
    ) -> ResourcePrediction:
        """Predict future resource demand using ML models"""

        if len(metrics) < 100:  # Need sufficient historical data
            logger.warning(f"Insufficient data for prediction: {resource_id}")
            return ResourcePrediction(
                resource_id=resource_id,
                prediction_horizon_hours=prediction_horizon_hours,
                predicted_load=[],
                confidence_intervals=[],
                seasonal_patterns={},
                anomaly_score=0.0
            )

        # Convert to DataFrame and prepare features
        df = pd.DataFrame([asdict(m) for m in metrics])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Feature engineering
        df['hour'] = df['timestamp'].dt.hour
        df['day_of_week'] = df['timestamp'].dt.dayofweek
        df['day_of_month'] = df['timestamp'].dt.day
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

        # Rolling averages
        df['cpu_ma_12h'] = df['cpu_utilization'].rolling(window=12, min_periods=1).mean()
        df['cpu_ma_24h'] = df['cpu_utilization'].rolling(window=24, min_periods=1).mean()

        # Lag features
        df['cpu_lag_1h'] = df['cpu_utilization'].shift(1)
        df['cpu_lag_6h'] = df['cpu_utilization'].shift(6)
        df['cpu_lag_12h'] = df['cpu_utilization'].shift(12)

        # Remove rows with NaN values
        df = df.dropna()

        if len(df) < 50:
            logger.warning(f"Insufficient clean data for prediction: {resource_id}")
            return ResourcePrediction(
                resource_id=resource_id,
                prediction_horizon_hours=prediction_horizon_hours,
                predicted_load=[],
                confidence_intervals=[],
                seasonal_patterns={},
                anomaly_score=0.0
            )

        # Prepare features and target
        feature_columns = [
            'hour', 'day_of_week', 'day_of_month', 'is_weekend',
            'cpu_ma_12h', 'cpu_ma_24h', 'cpu_lag_1h', 'cpu_lag_6h', 'cpu_lag_12h',
            'memory_utilization', 'network_io', 'disk_io'
        ]

        X = df[feature_columns]
        y = df['cpu_utilization']

        # Get appropriate model
        resource_type = metrics[0].resource_type if metrics else ResourceType.COMPUTE
        model = self.ml_models.get(resource_type)

        if model is None:
            logger.error(f"No ML model available for {resource_type}")
            return ResourcePrediction(
                resource_id=resource_id,
                prediction_horizon_hours=prediction_horizon_hours,
                predicted_load=[],
                confidence_intervals=[],
                seasonal_patterns={},
                anomaly_score=0.0
            )

        # Train/retrain model if needed
        if not hasattr(model, 'feature_importances_'):
            logger.info(f"Training ML model for {resource_id}")
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            model.fit(X_train, y_train)

        # Generate predictions
        predicted_load = []
        confidence_intervals = []

        # Use the last known values to start prediction
        last_row = df.iloc[-1].copy()

        for hour in range(prediction_horizon_hours):
            # Update time-based features
            future_time = df['timestamp'].iloc[-1] + timedelta(hours=hour+1)
            last_row['hour'] = future_time.hour
            last_row['day_of_week'] = future_time.dayofweek
            last_row['day_of_month'] = future_time.day
            last_row['is_weekend'] = int(future_time.dayofweek in [5, 6])

            # Make prediction
            features = last_row[feature_columns].values.reshape(1, -1)
            prediction = model.predict(features)[0]

            # Calculate confidence interval (simplified)
            # In practice, you'd use prediction intervals from the model
            std_dev = np.std(y)
            confidence_lower = max(0, prediction - 1.96 * std_dev)
            confidence_upper = min(100, prediction + 1.96 * std_dev)

            predicted_load.append(prediction)
            confidence_intervals.append((confidence_lower, confidence_upper))

            # Update lag features for next prediction
            last_row['cpu_lag_12h'] = last_row['cpu_lag_6h']
            last_row['cpu_lag_6h'] = last_row['cpu_lag_1h']
            last_row['cpu_lag_1h'] = prediction

        # Analyze seasonal patterns
        seasonal_patterns = {
            'hourly_avg': df.groupby('hour')['cpu_utilization'].mean().to_dict(),
            'daily_avg': df.groupby('day_of_week')['cpu_utilization'].mean().to_dict(),
            'weekend_vs_weekday': {
                'weekend': df[df['is_weekend'] == 1]['cpu_utilization'].mean(),
                'weekday': df[df['is_weekend'] == 0]['cpu_utilization'].mean()
            }
        }

        # Calculate anomaly score (simplified)
        recent_cpu = df['cpu_utilization'].tail(24).mean()
        historical_cpu = df['cpu_utilization'].mean()
        anomaly_score = abs(recent_cpu - historical_cpu) / (historical_cpu + 1e-6)

        return ResourcePrediction(
            resource_id=resource_id,
            prediction_horizon_hours=prediction_horizon_hours,
            predicted_load=predicted_load,
            confidence_intervals=confidence_intervals,
            seasonal_patterns=seasonal_patterns,
            anomaly_score=anomaly_score
        )

    def _estimate_aws_instance_cost(self, instance_id: str) -> float:
        """Estimate hourly cost for AWS instance"""
        # Simplified - would use AWS Pricing API
        return 0.096  # Example: m5.large cost

    def _estimate_azure_vm_cost(self, vm_id: str) -> float:
        """Estimate hourly cost for Azure VM"""
        # Simplified - would use Azure Pricing API
        return 0.096

    def _estimate_gcp_instance_cost(self, instance_name: str, zone: str) -> float:
        """Estimate hourly cost for GCP instance"""
        # Simplified - would use GCP Pricing API
        return 0.096

    async def execute_scaling_actions(
        self,
        recommendations: list[ScalingRecommendation],
        dry_run: bool = True
    ) -> dict[str, Any]:
        """Execute scaling actions based on recommendations"""
        results = {
            'executed': [],
            'failed': [],
            'skipped': []
        }

        for recommendation in recommendations:
            if recommendation.urgency in ['critical', 'high'] and recommendation.confidence_score > 0.8:
                try:
                    if dry_run:
                        logger.info(f"DRY RUN: Would execute {recommendation.scaling_action.value} for {recommendation.resource_id}")
                        results['executed'].append({
                            'resource_id': recommendation.resource_id,
                            'action': recommendation.scaling_action.value,
                            'dry_run': True
                        })
                    else:
                        # Execute actual scaling action
                        success = await self._execute_scaling_action(recommendation)
                        if success:
                            results['executed'].append({
                                'resource_id': recommendation.resource_id,
                                'action': recommendation.scaling_action.value,
                                'dry_run': False
                            })
                        else:
                            results['failed'].append({
                                'resource_id': recommendation.resource_id,
                                'action': recommendation.scaling_action.value,
                                'error': 'Execution failed'
                            })
                except Exception as e:
                    logger.error(f"Error executing scaling action for {recommendation.resource_id}: {e}")
                    results['failed'].append({
                        'resource_id': recommendation.resource_id,
                        'action': recommendation.scaling_action.value,
                        'error': str(e)
                    })
            else:
                results['skipped'].append({
                    'resource_id': recommendation.resource_id,
                    'reason': f"Low urgency ({recommendation.urgency}) or confidence ({recommendation.confidence_score:.2f})"
                })

        return results

    async def _execute_scaling_action(self, recommendation: ScalingRecommendation) -> bool:
        """Execute individual scaling action"""
        # This would contain actual cloud provider API calls
        # Implementation would depend on the specific cloud and resource type
        logger.info(f"Executing {recommendation.scaling_action.value} for {recommendation.resource_id}")
        return True  # Placeholder

    async def generate_optimization_report(
        self,
        metrics: list[MetricData],
        recommendations: list[ScalingRecommendation],
        predictions: list[ResourcePrediction]
    ) -> dict[str, Any]:
        """Generate comprehensive optimization report"""

        report = {
            'generated_at': datetime.utcnow().isoformat(),
            'summary': {
                'total_resources_analyzed': len({m.resource_id for m in metrics}),
                'total_recommendations': len(recommendations),
                'critical_recommendations': len([r for r in recommendations if r.urgency == 'critical']),
                'high_confidence_recommendations': len([r for r in recommendations if r.confidence_score > 0.8]),
                'estimated_monthly_savings': sum(r.estimated_cost_impact for r in recommendations if r.estimated_cost_impact < 0) * -24 * 30
            },
            'resource_analysis': {},
            'scaling_recommendations': [asdict(r) for r in recommendations],
            'demand_predictions': [asdict(p) for p in predictions],
            'cost_optimization_opportunities': []
        }

        # Analyze by cloud provider
        for cloud in ['aws', 'azure', 'gcp']:
            cloud_metrics = [m for m in metrics if m.cloud_provider == cloud]
            cloud_recommendations = [r for r in recommendations if any(m.resource_id == r.resource_id and m.cloud_provider == cloud for m in metrics)]

            if cloud_metrics:
                report['resource_analysis'][cloud] = {
                    'resource_count': len({m.resource_id for m in cloud_metrics}),
                    'avg_cpu_utilization': np.mean([m.cpu_utilization for m in cloud_metrics]),
                    'avg_memory_utilization': np.mean([m.memory_utilization for m in cloud_metrics]),
                    'total_cost_per_hour': sum(m.cost_per_hour for m in cloud_metrics),
                    'recommendations_count': len(cloud_recommendations)
                }

        # Identify cost optimization opportunities
        underutilized_resources = [
            r for r in recommendations
            if r.scaling_action == ScalingAction.SCALE_DOWN and r.confidence_score > 0.7
        ]

        for resource in underutilized_resources:
            report['cost_optimization_opportunities'].append({
                'resource_id': resource.resource_id,
                'opportunity_type': 'underutilized',
                'estimated_monthly_savings': resource.estimated_cost_impact * -24 * 30,
                'justification': resource.justification
            })

        return report

# Example usage
async def main():
    """Example usage of the intelligent auto-scaler"""

    config = {
        'aws': {
            'enabled': True,
            'region': 'us-east-1'
        },
        'azure': {
            'enabled': False,
        },
        'gcp': {
            'enabled': False,
        },
        'redis': {
            'host': 'localhost',
            'port': 6379
        },
        'thresholds': {
            'cpu_scale_up': 80,
            'cpu_scale_down': 20,
            'memory_scale_up': 80,
            'response_time_ms': 1000
        },
        'ml': {
            'model_path': './models'
        }
    }

    scaler = IntelligentAutoScaler(config)

    # Analyze current resource utilization
    metrics = await scaler.analyze_resource_utilization(time_window_hours=24)
    print(f"Collected {len(metrics)} metric data points")

    # Generate scaling recommendations
    recommendations = await scaler.generate_scaling_recommendations(metrics)
    print(f"Generated {len(recommendations)} scaling recommendations")

    for rec in recommendations[:5]:  # Show top 5 recommendations
        print(f"Resource: {rec.resource_id}")
        print(f"Action: {rec.scaling_action.value}")
        print(f"Confidence: {rec.confidence_score:.2f}")
        print(f"Urgency: {rec.urgency}")
        print(f"Justification: {rec.justification}")
        print("---")

    # Generate demand predictions for critical resources
    predictions = []
    critical_resources = [r.resource_id for r in recommendations if r.urgency == 'critical']

    for resource_id in critical_resources[:3]:  # Predict for top 3 critical resources
        resource_metrics = [m for m in metrics if m.resource_id == resource_id]
        if resource_metrics:
            prediction = await scaler.predict_resource_demand(resource_id, resource_metrics)
            predictions.append(prediction)
            print(f"24-hour demand prediction for {resource_id}:")
            print(f"Average predicted load: {np.mean(prediction.predicted_load):.1f}%")
            print(f"Peak predicted load: {np.max(prediction.predicted_load):.1f}%")

    # Generate optimization report
    report = await scaler.generate_optimization_report(metrics, recommendations, predictions)
    print("\nOptimization Report:")
    print(f"Total resources analyzed: {report['summary']['total_resources_analyzed']}")
    print(f"Critical recommendations: {report['summary']['critical_recommendations']}")
    print(f"Estimated monthly savings: ${report['summary']['estimated_monthly_savings']:.2f}")

    # Execute scaling actions (dry run)
    execution_results = await scaler.execute_scaling_actions(recommendations, dry_run=True)
    print("\nScaling execution results:")
    print(f"Would execute: {len(execution_results['executed'])} actions")
    print(f"Would skip: {len(execution_results['skipped'])} actions")

if __name__ == "__main__":
    asyncio.run(main())

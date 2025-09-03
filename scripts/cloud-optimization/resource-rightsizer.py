#!/usr/bin/env python3
"""
Multi-Cloud Resource Rightsizing Engine
Intelligent resource optimization based on actual usage patterns and performance metrics
"""

import asyncio
import json
import logging
import warnings
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import boto3
import numpy as np
from azure.identity import DefaultAzureCredential
from azure.mgmt.monitor import MonitorManagementClient
from google.cloud import monitoring_v3

warnings.filterwarnings('ignore')

logger = logging.getLogger(__name__)

class OptimizationAction(Enum):
    DOWNSIZE = "downsize"
    UPSIZE = "upsize"
    RIGHTTSIZE = "rightsize"
    TERMINATE = "terminate"
    SCHEDULE = "schedule"
    CONSOLIDATE = "consolidate"

@dataclass
class ResourceMetrics:
    """Resource usage metrics over time"""
    resource_id: str
    resource_type: str
    cloud_provider: str
    region: str

    # CPU metrics
    cpu_average: float
    cpu_max: float
    cpu_min: float
    cpu_p95: float

    # Memory metrics
    memory_average: float
    memory_max: float
    memory_min: float
    memory_p95: float

    # Storage metrics
    storage_used_gb: float
    storage_allocated_gb: float
    storage_utilization: float

    # Network metrics
    network_in_gb: float
    network_out_gb: float

    # Cost metrics
    current_hourly_cost: float
    current_monthly_cost: float

    # Utilization period
    measurement_period_days: int
    data_points: int

@dataclass
class RightsizingRecommendation:
    """Rightsizing recommendation with cost impact"""
    resource_id: str
    current_instance_type: str
    recommended_instance_type: str
    action: OptimizationAction
    confidence_score: float

    # Cost impact
    current_monthly_cost: float
    recommended_monthly_cost: float
    monthly_savings: float
    annual_savings: float

    # Performance impact
    performance_risk: str  # low, medium, high
    downtime_required: bool
    recommended_timing: str

    # Justification
    reason: str
    supporting_metrics: dict[str, float]

class MultiCloudResourceRightsizer:
    """Intelligent resource rightsizing across AWS, Azure, and GCP"""

    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.aws_clients = {}
        self.azure_clients = {}
        self.gcp_clients = {}
        self.instance_families = {}
        self.pricing_data = {}
        self.setup_cloud_clients()
        self.load_instance_families()

    def setup_cloud_clients(self):
        """Initialize cloud provider clients for monitoring and compute services"""
        try:
            # AWS clients
            if self.config.get('aws', {}).get('enabled', False):
                self.aws_clients = {
                    'cloudwatch': boto3.client('cloudwatch', region_name=self.config['aws'].get('region', 'us-east-1')),
                    'ec2': boto3.client('ec2', region_name=self.config['aws'].get('region', 'us-east-1')),
                    'ce': boto3.client('ce', region_name='us-east-1'),  # Cost Explorer only available in us-east-1
                    'rds': boto3.client('rds', region_name=self.config['aws'].get('region', 'us-east-1')),
                    'auto_scaling': boto3.client('autoscaling', region_name=self.config['aws'].get('region', 'us-east-1'))
                }

            # Azure clients
            if self.config.get('azure', {}).get('enabled', False):
                credential = DefaultAzureCredential()
                subscription_id = self.config['azure'].get('subscription_id')
                self.azure_clients = {
                    'monitor': MonitorManagementClient(credential, subscription_id),
                    # Add other Azure clients as needed
                }

            # GCP clients
            if self.config.get('gcp', {}).get('enabled', False):
                self.gcp_clients = {
                    'monitoring': monitoring_v3.MetricServiceClient(),
                    # Add other GCP clients as needed
                }

        except Exception as e:
            logger.error(f"Error setting up cloud clients: {e}")

    def load_instance_families(self):
        """Load instance family information for intelligent rightsizing"""
        self.instance_families = {
            'aws': {
                # General purpose
                't3': {'type': 'burstable', 'cpu_to_memory_ratio': 4, 'network': 'up_to_5gbps'},
                't3a': {'type': 'burstable', 'cpu_to_memory_ratio': 4, 'network': 'up_to_5gbps'},
                'm5': {'type': 'balanced', 'cpu_to_memory_ratio': 4, 'network': 'up_to_25gbps'},
                'm5a': {'type': 'balanced', 'cpu_to_memory_ratio': 4, 'network': 'up_to_20gbps'},
                'm5n': {'type': 'balanced', 'cpu_to_memory_ratio': 4, 'network': 'up_to_100gbps'},

                # Compute optimized
                'c5': {'type': 'compute', 'cpu_to_memory_ratio': 2, 'network': 'up_to_25gbps'},
                'c5n': {'type': 'compute', 'cpu_to_memory_ratio': 2, 'network': 'up_to_100gbps'},
                'c6i': {'type': 'compute', 'cpu_to_memory_ratio': 2, 'network': 'up_to_50gbps'},

                # Memory optimized
                'r5': {'type': 'memory', 'cpu_to_memory_ratio': 8, 'network': 'up_to_25gbps'},
                'r5a': {'type': 'memory', 'cpu_to_memory_ratio': 8, 'network': 'up_to_20gbps'},
                'r5n': {'type': 'memory', 'cpu_to_memory_ratio': 8, 'network': 'up_to_100gbps'},
                'x1e': {'type': 'memory', 'cpu_to_memory_ratio': 16, 'network': 'up_to_25gbps'},

                # Storage optimized
                'i3': {'type': 'storage', 'cpu_to_memory_ratio': 4, 'network': 'up_to_25gbps'},
                'i3en': {'type': 'storage', 'cpu_to_memory_ratio': 8, 'network': 'up_to_100gbps'},
            },
            'azure': {
                # General purpose
                'B': {'type': 'burstable', 'cpu_to_memory_ratio': 4, 'network': 'moderate'},
                'D': {'type': 'balanced', 'cpu_to_memory_ratio': 4, 'network': 'high'},
                'A': {'type': 'balanced', 'cpu_to_memory_ratio': 7, 'network': 'high'},

                # Compute optimized
                'F': {'type': 'compute', 'cpu_to_memory_ratio': 2, 'network': 'high'},

                # Memory optimized
                'E': {'type': 'memory', 'cpu_to_memory_ratio': 8, 'network': 'high'},
                'M': {'type': 'memory', 'cpu_to_memory_ratio': 14, 'network': 'high'},

                # Storage optimized
                'L': {'type': 'storage', 'cpu_to_memory_ratio': 4, 'network': 'high'},
            },
            'gcp': {
                # General purpose
                'e2': {'type': 'balanced', 'cpu_to_memory_ratio': 4, 'network': 'up_to_16gbps'},
                'n1': {'type': 'balanced', 'cpu_to_memory_ratio': 3.75, 'network': 'up_to_32gbps'},
                'n2': {'type': 'balanced', 'cpu_to_memory_ratio': 4, 'network': 'up_to_32gbps'},

                # Compute optimized
                'c2': {'type': 'compute', 'cpu_to_memory_ratio': 1, 'network': 'up_to_32gbps'},

                # Memory optimized
                'n1-highmem': {'type': 'memory', 'cpu_to_memory_ratio': 6.5, 'network': 'up_to_32gbps'},
                'n2-highmem': {'type': 'memory', 'cpu_to_memory_ratio': 8, 'network': 'up_to_32gbps'},
                'm1': {'type': 'memory', 'cpu_to_memory_ratio': 14.9, 'network': 'up_to_32gbps'},
                'm2': {'type': 'memory', 'cpu_to_memory_ratio': 24, 'network': 'up_to_32gbps'},
            }
        }

    async def analyze_resource_usage(self, resource_ids: list[str], days: int = 30) -> list[ResourceMetrics]:
        """Analyze resource usage patterns over specified time period"""
        logger.info(f"Analyzing usage for {len(resource_ids)} resources over {days} days")

        metrics = []
        tasks = []

        for resource_id in resource_ids:
            cloud_provider = self._identify_cloud_provider(resource_id)

            if cloud_provider == 'aws':
                tasks.append(self._get_aws_metrics(resource_id, days))
            elif cloud_provider == 'azure':
                tasks.append(self._get_azure_metrics(resource_id, days))
            elif cloud_provider == 'gcp':
                tasks.append(self._get_gcp_metrics(resource_id, days))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, ResourceMetrics):
                    metrics.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Error getting metrics: {result}")

        return metrics

    async def generate_rightsizing_recommendations(
        self,
        resource_metrics: list[ResourceMetrics],
        optimization_goals: dict[str, float] = None
    ) -> list[RightsizingRecommendation]:
        """Generate rightsizing recommendations based on usage patterns"""

        if optimization_goals is None:
            optimization_goals = {
                'cost_weight': 0.6,
                'performance_weight': 0.3,
                'risk_weight': 0.1,
                'cpu_target_utilization': 70.0,
                'memory_target_utilization': 80.0,
                'minimum_savings_threshold': 10.0  # Minimum 10% savings to recommend
            }

        recommendations = []

        for metrics in resource_metrics:
            try:
                recommendation = await self._analyze_single_resource(metrics, optimization_goals)
                if recommendation:
                    recommendations.append(recommendation)
            except Exception as e:
                logger.error(f"Error analyzing resource {metrics.resource_id}: {e}")

        # Sort by potential savings (descending)
        recommendations.sort(key=lambda x: x.annual_savings, reverse=True)

        return recommendations

    async def _analyze_single_resource(
        self,
        metrics: ResourceMetrics,
        optimization_goals: dict[str, float]
    ) -> RightsizingRecommendation | None:
        """Analyze single resource and generate recommendation"""

        # Determine optimization action based on utilization patterns
        action, reason = self._determine_optimization_action(metrics, optimization_goals)

        if action == OptimizationAction.RIGHTTSIZE:
            return None  # Already properly sized

        # Get recommended instance type
        recommended_instance = await self._get_recommended_instance(metrics, action, optimization_goals)

        if not recommended_instance:
            return None

        # Calculate cost impact
        recommended_cost = await self._get_instance_cost(
            metrics.cloud_provider,
            metrics.region,
            recommended_instance
        )

        monthly_savings = metrics.current_monthly_cost - recommended_cost
        savings_percentage = (monthly_savings / metrics.current_monthly_cost) * 100

        # Skip if savings don't meet threshold
        if savings_percentage < optimization_goals['minimum_savings_threshold']:
            return None

        # Calculate confidence score
        confidence_score = self._calculate_confidence_score(metrics, action)

        # Assess performance risk
        performance_risk = self._assess_performance_risk(metrics, recommended_instance, action)

        return RightsizingRecommendation(
            resource_id=metrics.resource_id,
            current_instance_type=self._get_current_instance_type(metrics.resource_id),
            recommended_instance_type=recommended_instance,
            action=action,
            confidence_score=confidence_score,
            current_monthly_cost=metrics.current_monthly_cost,
            recommended_monthly_cost=recommended_cost,
            monthly_savings=monthly_savings,
            annual_savings=monthly_savings * 12,
            performance_risk=performance_risk,
            downtime_required=self._requires_downtime(action),
            recommended_timing=self._get_recommended_timing(action, performance_risk),
            reason=reason,
            supporting_metrics={
                'avg_cpu_utilization': metrics.cpu_average,
                'avg_memory_utilization': metrics.memory_average,
                'max_cpu_utilization': metrics.cpu_max,
                'max_memory_utilization': metrics.memory_max,
                'savings_percentage': savings_percentage
            }
        )

    def _determine_optimization_action(
        self,
        metrics: ResourceMetrics,
        optimization_goals: dict[str, float]
    ) -> tuple[OptimizationAction, str]:
        """Determine what optimization action to take"""

        cpu_target = optimization_goals['cpu_target_utilization']
        memory_target = optimization_goals['memory_target_utilization']

        # Check if resource is underutilized
        if (metrics.cpu_average < cpu_target * 0.5 and
            metrics.memory_average < memory_target * 0.5 and
            metrics.cpu_max < cpu_target * 0.8):

            if metrics.cpu_average < 5.0 and metrics.network_in_gb + metrics.network_out_gb < 1.0:
                return OptimizationAction.TERMINATE, "Resource shows minimal usage and can be terminated"
            else:
                return OptimizationAction.DOWNSIZE, "Resource is consistently underutilized"

        # Check if resource is overutilized
        elif (metrics.cpu_p95 > 90.0 or
              metrics.memory_p95 > 90.0 or
              metrics.cpu_max > 95.0):
            return OptimizationAction.UPSIZE, "Resource shows signs of performance bottlenecks"

        # Check for scheduling opportunities (low usage patterns)
        elif self._has_predictable_schedule(metrics):
            return OptimizationAction.SCHEDULE, "Resource shows predictable usage patterns suitable for scheduling"

        # Check if properly sized
        elif (cpu_target * 0.7 <= metrics.cpu_average <= cpu_target * 1.1 and
              memory_target * 0.7 <= metrics.memory_average <= memory_target * 1.1):
            return OptimizationAction.RIGHTTSIZE, "Resource is properly sized"

        # Default to rightsize if usage is within acceptable bounds
        else:
            return OptimizationAction.RIGHTTSIZE, "Resource utilization is within acceptable bounds"

    async def _get_recommended_instance(
        self,
        metrics: ResourceMetrics,
        action: OptimizationAction,
        optimization_goals: dict[str, float]
    ) -> str | None:
        """Get recommended instance type based on metrics and action"""

        current_instance = self._get_current_instance_type(metrics.resource_id)
        current_specs = await self._get_instance_specs(
            metrics.cloud_provider,
            current_instance
        )

        if action == OptimizationAction.DOWNSIZE:
            # Find smaller instance that still meets peak requirements
            target_cpu = max(2, int(metrics.cpu_p95 * 1.2))  # 20% buffer above P95
            target_memory = max(2, int(metrics.memory_p95 * 1.2))

            return await self._find_instance_by_specs(
                metrics.cloud_provider,
                target_cpu,
                target_memory,
                current_specs['network_performance']
            )

        elif action == OptimizationAction.UPSIZE:
            # Find larger instance to handle peak loads
            target_cpu = int(current_specs['cpu_cores'] * 1.5)
            target_memory = int(current_specs['memory_gb'] * 1.5)

            return await self._find_instance_by_specs(
                metrics.cloud_provider,
                target_cpu,
                target_memory,
                current_specs['network_performance']
            )

        elif action == OptimizationAction.SCHEDULE:
            # For scheduled resources, might recommend smaller instance
            # since it will be stopped during off-hours
            target_cpu = max(1, int(metrics.cpu_average * 1.5))
            target_memory = max(1, int(metrics.memory_average * 1.5))

            return await self._find_instance_by_specs(
                metrics.cloud_provider,
                target_cpu,
                target_memory,
                'low'  # Lower network performance acceptable for scheduled workloads
            )

        return None

    def _calculate_confidence_score(self, metrics: ResourceMetrics, action: OptimizationAction) -> float:
        """Calculate confidence score for recommendation (0-100)"""

        base_score = 50.0

        # More data points = higher confidence
        if metrics.data_points > 1000:
            base_score += 20
        elif metrics.data_points > 500:
            base_score += 10
        elif metrics.data_points < 100:
            base_score -= 20

        # Longer measurement period = higher confidence
        if metrics.measurement_period_days >= 30:
            base_score += 15
        elif metrics.measurement_period_days >= 14:
            base_score += 10
        elif metrics.measurement_period_days < 7:
            base_score -= 15

        # Clear usage patterns = higher confidence
        cpu_variance = (metrics.cpu_max - metrics.cpu_min) / metrics.cpu_average if metrics.cpu_average > 0 else 0
        memory_variance = (metrics.memory_max - metrics.memory_min) / metrics.memory_average if metrics.memory_average > 0 else 0

        if cpu_variance < 0.5 and memory_variance < 0.5:
            base_score += 10  # Stable usage pattern
        elif cpu_variance > 2.0 or memory_variance > 2.0:
            base_score -= 10  # Highly variable usage

        # Action-specific adjustments
        if action == OptimizationAction.DOWNSIZE:
            # Higher confidence if consistently low usage
            if metrics.cpu_max < 50 and metrics.memory_max < 60:
                base_score += 15
        elif action == OptimizationAction.UPSIZE:
            # Higher confidence if consistently high usage
            if metrics.cpu_p95 > 85 and metrics.memory_p95 > 85:
                base_score += 15

        return min(100.0, max(0.0, base_score))

    def _assess_performance_risk(
        self,
        metrics: ResourceMetrics,
        recommended_instance: str,
        action: OptimizationAction
    ) -> str:
        """Assess performance risk of the recommendation"""

        if action == OptimizationAction.UPSIZE:
            return "low"  # Upsizing typically has low risk

        elif action == OptimizationAction.DOWNSIZE:
            # Check how close we are to peak usage
            cpu_headroom = 100 - metrics.cpu_p95
            memory_headroom = 100 - metrics.memory_p95

            if cpu_headroom < 10 or memory_headroom < 10:
                return "high"
            elif cpu_headroom < 25 or memory_headroom < 25:
                return "medium"
            else:
                return "low"

        elif action == OptimizationAction.SCHEDULE:
            return "medium"  # Scheduling has some operational risk

        elif action == OptimizationAction.TERMINATE:
            return "high"  # Termination has highest risk

        return "low"

    def _requires_downtime(self, action: OptimizationAction) -> bool:
        """Check if the optimization action requires downtime"""
        return action in [OptimizationAction.UPSIZE, OptimizationAction.DOWNSIZE]

    def _get_recommended_timing(self, action: OptimizationAction, performance_risk: str) -> str:
        """Get recommended timing for implementing the change"""

        if performance_risk == "high":
            return "maintenance_window"
        elif performance_risk == "medium":
            return "low_traffic_period"
        else:
            return "any_time"

    def _has_predictable_schedule(self, metrics: ResourceMetrics) -> bool:
        """Check if resource has predictable usage patterns suitable for scheduling"""
        # Simplified logic - in practice would analyze hourly/daily patterns
        # This would require more detailed time-series data
        return (metrics.cpu_min < 10 and
                metrics.cpu_average < 30 and
                (metrics.cpu_max - metrics.cpu_min) / metrics.cpu_average > 2)

    # Cloud-specific metric collection methods
    async def _get_aws_metrics(self, instance_id: str, days: int) -> ResourceMetrics:
        """Get AWS CloudWatch metrics for EC2 instance"""

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=days)

        try:
            # Get CPU utilization
            cpu_response = self.aws_clients['cloudwatch'].get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='CPUUtilization',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1 hour periods
                Statistics=['Average', 'Maximum', 'Minimum']
            )

            cpu_data = [point['Average'] for point in cpu_response['Datapoints']]
            cpu_max_data = [point['Maximum'] for point in cpu_response['Datapoints']]
            cpu_min_data = [point['Minimum'] for point in cpu_response['Datapoints']]

            # Get memory utilization (requires CloudWatch agent)
            try:
                memory_response = self.aws_clients['cloudwatch'].get_metric_statistics(
                    Namespace='CWAgent',
                    MetricName='mem_used_percent',
                    Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=3600,
                    Statistics=['Average', 'Maximum', 'Minimum']
                )
                memory_data = [point['Average'] for point in memory_response['Datapoints']]
                memory_max_data = [point['Maximum'] for point in memory_response['Datapoints']]
                memory_min_data = [point['Minimum'] for point in memory_response['Datapoints']]
            except Exception:
                # Fallback if memory metrics not available
                memory_data = [50.0] * len(cpu_data)  # Assume 50% utilization
                memory_max_data = [60.0] * len(cpu_data)
                memory_min_data = [40.0] * len(cpu_data)

            # Get network metrics
            network_in = self.aws_clients['cloudwatch'].get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='NetworkIn',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Sum']
            )

            network_out = self.aws_clients['cloudwatch'].get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='NetworkOut',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Sum']
            )

            # Get current cost (simplified)
            current_cost = await self._get_instance_cost('aws', self._get_instance_region(instance_id),
                                                       self._get_current_instance_type(instance_id))

            return ResourceMetrics(
                resource_id=instance_id,
                resource_type='ec2_instance',
                cloud_provider='aws',
                region=self._get_instance_region(instance_id),
                cpu_average=np.mean(cpu_data) if cpu_data else 0,
                cpu_max=max(cpu_max_data) if cpu_max_data else 0,
                cpu_min=min(cpu_min_data) if cpu_min_data else 0,
                cpu_p95=np.percentile(cpu_data, 95) if cpu_data else 0,
                memory_average=np.mean(memory_data) if memory_data else 0,
                memory_max=max(memory_max_data) if memory_max_data else 0,
                memory_min=min(memory_min_data) if memory_min_data else 0,
                memory_p95=np.percentile(memory_data, 95) if memory_data else 0,
                storage_used_gb=50,  # Simplified
                storage_allocated_gb=100,  # Simplified
                storage_utilization=50,
                network_in_gb=sum([point['Sum'] for point in network_in['Datapoints']]) / (1024**3) if network_in['Datapoints'] else 0,
                network_out_gb=sum([point['Sum'] for point in network_out['Datapoints']]) / (1024**3) if network_out['Datapoints'] else 0,
                current_hourly_cost=current_cost / (24 * 30),
                current_monthly_cost=current_cost,
                measurement_period_days=days,
                data_points=len(cpu_data)
            )

        except Exception as e:
            logger.error(f"Error getting AWS metrics for {instance_id}: {e}")
            raise

    async def _get_azure_metrics(self, resource_id: str, days: int) -> ResourceMetrics:
        """Get Azure Monitor metrics"""
        # Implementation would use Azure Monitor APIs
        # Simplified placeholder
        return ResourceMetrics(
            resource_id=resource_id,
            resource_type='vm',
            cloud_provider='azure',
            region='eastus',
            cpu_average=45.0,
            cpu_max=80.0,
            cpu_min=10.0,
            cpu_p95=75.0,
            memory_average=60.0,
            memory_max=85.0,
            memory_min=30.0,
            memory_p95=80.0,
            storage_used_gb=60,
            storage_allocated_gb=100,
            storage_utilization=60,
            network_in_gb=5.0,
            network_out_gb=3.0,
            current_hourly_cost=0.15,
            current_monthly_cost=108.0,
            measurement_period_days=days,
            data_points=720
        )

    async def _get_gcp_metrics(self, resource_id: str, days: int) -> ResourceMetrics:
        """Get GCP Cloud Monitoring metrics"""
        # Implementation would use GCP Monitoring APIs
        # Simplified placeholder
        return ResourceMetrics(
            resource_id=resource_id,
            resource_type='compute_instance',
            cloud_provider='gcp',
            region='us-central1',
            cpu_average=35.0,
            cpu_max=70.0,
            cpu_min=5.0,
            cpu_p95=65.0,
            memory_average=55.0,
            memory_max=75.0,
            memory_min=25.0,
            memory_p95=70.0,
            storage_used_gb=40,
            storage_allocated_gb=100,
            storage_utilization=40,
            network_in_gb=3.0,
            network_out_gb=2.0,
            current_hourly_cost=0.12,
            current_monthly_cost=86.4,
            measurement_period_days=days,
            data_points=720
        )

    # Helper methods (simplified implementations)
    def _identify_cloud_provider(self, resource_id: str) -> str:
        """Identify cloud provider from resource ID format"""
        if resource_id.startswith('i-'):
            return 'aws'
        elif resource_id.startswith('/subscriptions/'):
            return 'azure'
        elif 'projects/' in resource_id:
            return 'gcp'
        return 'unknown'

    def _get_current_instance_type(self, resource_id: str) -> str:
        """Get current instance type (simplified)"""
        return 't3.medium'  # Placeholder

    def _get_instance_region(self, resource_id: str) -> str:
        """Get instance region (simplified)"""
        return 'us-east-1'  # Placeholder

    async def _get_instance_cost(self, cloud_provider: str, region: str, instance_type: str) -> float:
        """Get monthly cost for instance type (simplified)"""
        # Simplified pricing - in practice would use cloud pricing APIs
        base_costs = {
            'aws': {
                't3.micro': 7.5, 't3.small': 15.0, 't3.medium': 30.0,
                't3.large': 60.0, 'm5.large': 70.0, 'm5.xlarge': 140.0
            },
            'azure': {'Standard_B1s': 8.0, 'Standard_B2s': 30.0, 'Standard_D2s_v3': 70.0},
            'gcp': {'e2-micro': 6.0, 'e2-small': 12.0, 'e2-medium': 24.0}
        }

        return base_costs.get(cloud_provider, {}).get(instance_type, 100.0)

    async def _get_instance_specs(self, cloud_provider: str, instance_type: str) -> dict[str, Any]:
        """Get instance specifications (simplified)"""
        return {
            'cpu_cores': 2,
            'memory_gb': 4,
            'network_performance': 'moderate'
        }

    async def _find_instance_by_specs(
        self,
        cloud_provider: str,
        cpu_cores: int,
        memory_gb: int,
        network_performance: str
    ) -> str:
        """Find instance type matching specifications (simplified)"""

        if cloud_provider == 'aws':
            if cpu_cores <= 1 and memory_gb <= 1:
                return 't3.micro'
            elif cpu_cores <= 2 and memory_gb <= 2:
                return 't3.small'
            elif cpu_cores <= 2 and memory_gb <= 4:
                return 't3.medium'
            elif cpu_cores <= 2 and memory_gb <= 8:
                return 't3.large'
            else:
                return 'm5.large'

        return 'm5.large'  # Default fallback

# Example usage and batch processing
async def batch_rightsizing_analysis(
    optimizer: MultiCloudResourceRightsizer,
    resource_groups: dict[str, list[str]]
) -> dict[str, Any]:
    """Perform batch rightsizing analysis across resource groups"""

    report = {
        'analysis_timestamp': datetime.utcnow().isoformat(),
        'resource_groups': {},
        'summary': {
            'total_resources_analyzed': 0,
            'total_recommendations': 0,
            'total_potential_monthly_savings': 0.0,
            'total_potential_annual_savings': 0.0
        },
        'recommendations_by_action': {
            'downsize': [],
            'upsize': [],
            'terminate': [],
            'schedule': []
        }
    }

    for group_name, resource_ids in resource_groups.items():
        logger.info(f"Analyzing resource group: {group_name}")

        # Analyze usage patterns
        metrics = await optimizer.analyze_resource_usage(resource_ids)

        # Generate recommendations
        recommendations = await optimizer.generate_rightsizing_recommendations(metrics)

        # Aggregate results
        group_summary = {
            'resources_analyzed': len(metrics),
            'recommendations_count': len(recommendations),
            'potential_monthly_savings': sum(r.monthly_savings for r in recommendations),
            'potential_annual_savings': sum(r.annual_savings for r in recommendations),
            'recommendations': [asdict(r) for r in recommendations]
        }

        report['resource_groups'][group_name] = group_summary

        # Update overall summary
        report['summary']['total_resources_analyzed'] += len(metrics)
        report['summary']['total_recommendations'] += len(recommendations)
        report['summary']['total_potential_monthly_savings'] += group_summary['potential_monthly_savings']
        report['summary']['total_potential_annual_savings'] += group_summary['potential_annual_savings']

        # Categorize recommendations by action
        for recommendation in recommendations:
            action_key = recommendation.action.value
            if action_key in report['recommendations_by_action']:
                report['recommendations_by_action'][action_key].append(asdict(recommendation))

    return report

# Example usage
async def main():
    """Example usage of the resource rightsizer"""

    config = {
        'aws': {
            'enabled': True,
            'region': 'us-east-1'
        },
        'azure': {
            'enabled': True,
            'subscription_id': 'your-subscription-id'
        },
        'gcp': {
            'enabled': True,
            'project_id': 'your-project-id'
        }
    }

    rightsizer = MultiCloudResourceRightsizer(config)

    # Example resource groups
    resource_groups = {
        'web_servers': ['i-1234567890abcdef0', 'i-0987654321fedcba0'],
        'database_servers': ['i-abcdef1234567890a'],
        'batch_processing': ['i-fedcba0987654321f']
    }

    # Generate comprehensive rightsizing report
    report = await batch_rightsizing_analysis(rightsizer, resource_groups)

    print("Rightsizing Analysis Report")
    print(f"Total resources analyzed: {report['summary']['total_resources_analyzed']}")
    print(f"Total recommendations: {report['summary']['total_recommendations']}")
    print(f"Potential monthly savings: ${report['summary']['total_potential_monthly_savings']:.2f}")
    print(f"Potential annual savings: ${report['summary']['total_potential_annual_savings']:.2f}")

    # Save report
    with open(f"rightsizing_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
        json.dump(report, f, indent=2)

if __name__ == "__main__":
    asyncio.run(main())

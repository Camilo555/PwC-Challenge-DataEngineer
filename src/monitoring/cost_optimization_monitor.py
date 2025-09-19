"""
Cost Optimization Monitor
========================

Enterprise-grade cost optimization monitoring for cloud services:
- Real-time cost tracking and budget alerting
- Resource utilization analysis and optimization recommendations
- Cost anomaly detection with ML-powered forecasting
- Multi-cloud cost management (AWS, Azure, GCP)
- Department/project-level cost allocation and chargeback
- Automated cost optimization actions and governance
"""
from __future__ import annotations

import asyncio
import json
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Callable
from uuid import UUID, uuid4

from core.logging import get_logger

logger = get_logger(__name__)


class CostSeverity(str, Enum):
    """Cost alert severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class OptimizationStrategy(str, Enum):
    """Cost optimization strategies."""
    RIGHT_SIZING = "right_sizing"
    RESERVED_INSTANCES = "reserved_instances"
    SPOT_INSTANCES = "spot_instances"
    AUTO_SCALING = "auto_scaling"
    STORAGE_OPTIMIZATION = "storage_optimization"
    RESOURCE_SCHEDULING = "resource_scheduling"
    UNUSED_RESOURCES = "unused_resources"


class CloudProvider(str, Enum):
    """Supported cloud providers."""
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"
    MULTI_CLOUD = "multi_cloud"


@dataclass
class CostMetrics:
    """Cost metrics for monitoring and analysis."""
    provider: CloudProvider
    service_name: str
    resource_id: str
    current_cost_usd: float
    daily_cost_trend: List[float]
    monthly_budget_usd: float
    cost_per_unit: float
    utilization_percent: float
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @property
    def budget_utilization_percent(self) -> float:
        """Calculate budget utilization percentage."""
        monthly_cost = sum(self.daily_cost_trend[-30:]) if len(self.daily_cost_trend) >= 30 else sum(self.daily_cost_trend) * 30 / len(self.daily_cost_trend)
        return (monthly_cost / self.monthly_budget_usd) * 100 if self.monthly_budget_usd > 0 else 0


@dataclass
class OptimizationRecommendation:
    """Cost optimization recommendation."""
    recommendation_id: str
    strategy: OptimizationStrategy
    resource_id: str
    provider: CloudProvider
    service_name: str
    current_cost_usd: float
    estimated_savings_usd: float
    confidence_score: float
    implementation_effort: str
    risk_level: str
    description: str
    action_required: str
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @property
    def roi_percent(self) -> float:
        """Calculate return on investment percentage."""
        return (self.estimated_savings_usd / self.current_cost_usd) * 100 if self.current_cost_usd > 0 else 0


@dataclass
class CostAlert:
    """Cost alert for threshold violations."""
    alert_id: str
    severity: CostSeverity
    provider: CloudProvider
    service_name: str
    resource_id: str
    threshold_type: str
    threshold_value: float
    current_value: float
    message: str
    recommended_action: str
    timestamp: datetime = field(default_factory=datetime.utcnow)


class CostOptimizationMonitor:
    """
    Enterprise cost optimization monitor with real-time tracking and ML-powered recommendations.

    Features:
    - Multi-cloud cost tracking and analysis
    - Budget monitoring with proactive alerting
    - Resource utilization analysis and optimization
    - Automated cost anomaly detection
    - Department/project cost allocation
    - Cost forecasting and trend analysis
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.cost_metrics: Dict[str, CostMetrics] = {}
        self.optimization_recommendations: Dict[str, OptimizationRecommendation] = {}
        self.cost_alerts: Dict[str, CostAlert] = {}

        # Historical data for trend analysis
        self.cost_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.utilization_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))

        # Budget and thresholds
        self.budget_thresholds = {
            "warning": 80.0,  # 80% of budget
            "critical": 95.0,  # 95% of budget
            "emergency": 100.0  # 100% of budget
        }

        # Optimization settings
        self.optimization_enabled = self.config.get("optimization_enabled", True)
        self.auto_scaling_enabled = self.config.get("auto_scaling_enabled", False)

        # Provider-specific settings
        self.provider_configs = {
            CloudProvider.AWS: {
                "regions": ["us-east-1", "us-west-2", "eu-west-1"],
                "services": ["ec2", "rds", "s3", "lambda", "ecs"],
                "cost_allocation_tags": ["Environment", "Project", "Department"]
            },
            CloudProvider.AZURE: {
                "regions": ["eastus", "westus2", "westeurope"],
                "services": ["compute", "storage", "database", "functions"],
                "cost_allocation_tags": ["Environment", "Project", "CostCenter"]
            },
            CloudProvider.GCP: {
                "regions": ["us-central1", "us-east1", "europe-west1"],
                "services": ["compute", "storage", "sql", "functions"],
                "cost_allocation_tags": ["env", "project", "team"]
            }
        }

        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None

    async def start_monitoring(self) -> None:
        """Start the cost optimization monitoring service."""
        if self._running:
            logger.warning("Cost monitoring is already running")
            return

        logger.info("Starting cost optimization monitoring...")
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitoring_loop())

        await self._initialize_baseline_metrics()
        logger.info("Cost optimization monitoring started successfully")

    async def stop_monitoring(self) -> None:
        """Stop the cost optimization monitoring service."""
        if not self._running:
            return

        logger.info("Stopping cost optimization monitoring...")
        self._running = False

        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

        logger.info("Cost optimization monitoring stopped")

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop for cost optimization."""
        while self._running:
            try:
                # Collect cost metrics from all providers
                await self._collect_cost_metrics()

                # Analyze cost trends and detect anomalies
                await self._analyze_cost_trends()

                # Generate optimization recommendations
                await self._generate_optimization_recommendations()

                # Check budget thresholds and generate alerts
                await self._check_budget_thresholds()

                # Perform automated optimizations if enabled
                if self.optimization_enabled:
                    await self._perform_automated_optimizations()

                # Wait before next monitoring cycle
                await asyncio.sleep(300)  # 5 minutes

            except Exception as e:
                logger.error(f"Error in cost monitoring loop: {e}")
                await asyncio.sleep(60)  # Wait 1 minute on error

    async def _initialize_baseline_metrics(self) -> None:
        """Initialize baseline cost metrics for all cloud providers."""
        logger.info("Initializing baseline cost metrics...")

        # Initialize metrics for each provider and service
        for provider in CloudProvider:
            if provider == CloudProvider.MULTI_CLOUD:
                continue

            provider_config = self.provider_configs.get(provider, {})
            services = provider_config.get("services", [])

            for service in services:
                for region in provider_config.get("regions", ["default"]):
                    resource_id = f"{provider.value}-{service}-{region}"

                    # Create baseline metrics
                    metrics = CostMetrics(
                        provider=provider,
                        service_name=service,
                        resource_id=resource_id,
                        current_cost_usd=self._simulate_current_cost(provider, service),
                        daily_cost_trend=self._simulate_cost_trend(),
                        monthly_budget_usd=self._get_monthly_budget(provider, service),
                        cost_per_unit=self._simulate_cost_per_unit(service),
                        utilization_percent=self._simulate_utilization()
                    )

                    self.cost_metrics[resource_id] = metrics

        logger.info(f"Initialized {len(self.cost_metrics)} baseline cost metrics")

    async def _collect_cost_metrics(self) -> None:
        """Collect current cost metrics from all cloud providers."""
        current_time = datetime.utcnow()

        for resource_id, metrics in self.cost_metrics.items():
            # Simulate real-time cost data collection
            new_cost = self._simulate_current_cost(metrics.provider, metrics.service_name)
            new_utilization = self._simulate_utilization()

            # Update metrics
            metrics.current_cost_usd = new_cost
            metrics.utilization_percent = new_utilization
            metrics.timestamp = current_time

            # Update historical data
            self.cost_history[resource_id].append({
                "timestamp": current_time,
                "cost": new_cost,
                "utilization": new_utilization
            })

            # Update daily cost trend (last 30 days)
            if len(metrics.daily_cost_trend) >= 30:
                metrics.daily_cost_trend.pop(0)
            metrics.daily_cost_trend.append(new_cost)

    async def _analyze_cost_trends(self) -> None:
        """Analyze cost trends and detect anomalies."""
        for resource_id, metrics in self.cost_metrics.items():
            if len(metrics.daily_cost_trend) < 7:  # Need at least a week of data
                continue

            # Calculate trend indicators
            recent_costs = metrics.daily_cost_trend[-7:]  # Last 7 days
            previous_costs = metrics.daily_cost_trend[-14:-7] if len(metrics.daily_cost_trend) >= 14 else recent_costs

            recent_avg = statistics.mean(recent_costs)
            previous_avg = statistics.mean(previous_costs)

            # Detect cost anomalies
            if recent_avg > previous_avg * 1.2:  # 20% increase
                await self._create_cost_alert(
                    resource_id=resource_id,
                    severity=CostSeverity.HIGH,
                    threshold_type="cost_anomaly",
                    threshold_value=previous_avg * 1.2,
                    current_value=recent_avg,
                    message=f"Cost anomaly detected: {((recent_avg - previous_avg) / previous_avg * 100):.1f}% increase"
                )

    async def _generate_optimization_recommendations(self) -> None:
        """Generate cost optimization recommendations based on current metrics."""
        for resource_id, metrics in self.cost_metrics.items():
            recommendations = []

            # Right-sizing recommendation
            if metrics.utilization_percent < 30:  # Low utilization
                recommendation = OptimizationRecommendation(
                    recommendation_id=str(uuid4()),
                    strategy=OptimizationStrategy.RIGHT_SIZING,
                    resource_id=resource_id,
                    provider=metrics.provider,
                    service_name=metrics.service_name,
                    current_cost_usd=metrics.current_cost_usd,
                    estimated_savings_usd=metrics.current_cost_usd * 0.3,  # 30% savings
                    confidence_score=0.85,
                    implementation_effort="Medium",
                    risk_level="Low",
                    description=f"Resource {resource_id} has low utilization ({metrics.utilization_percent:.1f}%). Consider right-sizing.",
                    action_required="Review and downsize resource capacity"
                )
                recommendations.append(recommendation)

            # Unused resources recommendation
            if metrics.utilization_percent < 5:  # Very low utilization
                recommendation = OptimizationRecommendation(
                    recommendation_id=str(uuid4()),
                    strategy=OptimizationStrategy.UNUSED_RESOURCES,
                    resource_id=resource_id,
                    provider=metrics.provider,
                    service_name=metrics.service_name,
                    current_cost_usd=metrics.current_cost_usd,
                    estimated_savings_usd=metrics.current_cost_usd * 0.95,  # 95% savings
                    confidence_score=0.95,
                    implementation_effort="Low",
                    risk_level="Low",
                    description=f"Resource {resource_id} appears to be unused ({metrics.utilization_percent:.1f}% utilization).",
                    action_required="Consider decommissioning or stopping this resource"
                )
                recommendations.append(recommendation)

            # Store recommendations
            for rec in recommendations:
                self.optimization_recommendations[rec.recommendation_id] = rec

    async def _check_budget_thresholds(self) -> None:
        """Check budget thresholds and generate alerts."""
        for resource_id, metrics in self.cost_metrics.items():
            budget_utilization = metrics.budget_utilization_percent

            # Check budget thresholds
            if budget_utilization >= self.budget_thresholds["emergency"]:
                severity = CostSeverity.CRITICAL
                message = f"Budget exceeded! Current utilization: {budget_utilization:.1f}%"
            elif budget_utilization >= self.budget_thresholds["critical"]:
                severity = CostSeverity.HIGH
                message = f"Budget critical! Current utilization: {budget_utilization:.1f}%"
            elif budget_utilization >= self.budget_thresholds["warning"]:
                severity = CostSeverity.MEDIUM
                message = f"Budget warning! Current utilization: {budget_utilization:.1f}%"
            else:
                continue  # No alert needed

            await self._create_cost_alert(
                resource_id=resource_id,
                severity=severity,
                threshold_type="budget_utilization",
                threshold_value=self.budget_thresholds["warning"],
                current_value=budget_utilization,
                message=message
            )

    async def _create_cost_alert(self, resource_id: str, severity: CostSeverity,
                               threshold_type: str, threshold_value: float,
                               current_value: float, message: str) -> None:
        """Create a cost alert."""
        metrics = self.cost_metrics.get(resource_id)
        if not metrics:
            return

        alert = CostAlert(
            alert_id=str(uuid4()),
            severity=severity,
            provider=metrics.provider,
            service_name=metrics.service_name,
            resource_id=resource_id,
            threshold_type=threshold_type,
            threshold_value=threshold_value,
            current_value=current_value,
            message=message,
            recommended_action=self._get_recommended_action(severity, threshold_type)
        )

        self.cost_alerts[alert.alert_id] = alert

        logger.warning(f"Cost alert generated: {alert.severity.value} - {alert.message}")

    async def _perform_automated_optimizations(self) -> None:
        """Perform automated cost optimizations based on recommendations."""
        if not self.optimization_enabled:
            return

        for rec_id, recommendation in self.optimization_recommendations.items():
            # Only perform low-risk optimizations automatically
            if recommendation.risk_level.lower() == "low" and recommendation.confidence_score >= 0.9:
                logger.info(f"Performing automated optimization: {recommendation.strategy.value} for {recommendation.resource_id}")

                # Simulate optimization action
                await self._execute_optimization(recommendation)

    async def _execute_optimization(self, recommendation: OptimizationRecommendation) -> None:
        """Execute a cost optimization recommendation."""
        logger.info(f"Executing optimization: {recommendation.strategy.value}")

        # Simulate optimization execution
        if recommendation.strategy == OptimizationStrategy.RIGHT_SIZING:
            logger.info(f"Right-sizing resource {recommendation.resource_id}")
        elif recommendation.strategy == OptimizationStrategy.UNUSED_RESOURCES:
            logger.info(f"Marking resource {recommendation.resource_id} for decommissioning")
        elif recommendation.strategy == OptimizationStrategy.AUTO_SCALING:
            logger.info(f"Enabling auto-scaling for {recommendation.resource_id}")

        # Update metrics to reflect optimization
        if recommendation.resource_id in self.cost_metrics:
            metrics = self.cost_metrics[recommendation.resource_id]
            metrics.current_cost_usd *= (1 - recommendation.estimated_savings_usd / recommendation.current_cost_usd)

    def get_cost_summary(self) -> Dict[str, Any]:
        """Get comprehensive cost summary and metrics."""
        total_cost = sum(metrics.current_cost_usd for metrics in self.cost_metrics.values())
        total_budget = sum(metrics.monthly_budget_usd for metrics in self.cost_metrics.values())

        # Calculate provider breakdown
        provider_costs = defaultdict(float)
        for metrics in self.cost_metrics.values():
            provider_costs[metrics.provider.value] += metrics.current_cost_usd

        # Calculate service breakdown
        service_costs = defaultdict(float)
        for metrics in self.cost_metrics.values():
            service_costs[metrics.service_name] += metrics.current_cost_usd

        # Calculate optimization potential
        potential_savings = sum(rec.estimated_savings_usd for rec in self.optimization_recommendations.values())

        # Active alerts by severity
        alert_counts = defaultdict(int)
        for alert in self.cost_alerts.values():
            alert_counts[alert.severity.value] += 1

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "cost_overview": {
                "total_monthly_cost_usd": total_cost,
                "total_monthly_budget_usd": total_budget,
                "budget_utilization_percent": (total_cost / total_budget * 100) if total_budget > 0 else 0,
                "cost_trend": "stable"  # Simplified
            },
            "provider_breakdown": dict(provider_costs),
            "service_breakdown": dict(service_costs),
            "optimization_summary": {
                "total_recommendations": len(self.optimization_recommendations),
                "potential_monthly_savings_usd": potential_savings,
                "savings_opportunity_percent": (potential_savings / total_cost * 100) if total_cost > 0 else 0
            },
            "alerts_summary": {
                "total_active_alerts": len(self.cost_alerts),
                "alerts_by_severity": dict(alert_counts)
            },
            "top_cost_resources": sorted(
                [
                    {
                        "resource_id": metrics.resource_id,
                        "provider": metrics.provider.value,
                        "service": metrics.service_name,
                        "monthly_cost_usd": metrics.current_cost_usd,
                        "utilization_percent": metrics.utilization_percent
                    }
                    for metrics in self.cost_metrics.values()
                ],
                key=lambda x: x["monthly_cost_usd"],
                reverse=True
            )[:10]
        }

    def get_optimization_recommendations(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get top cost optimization recommendations."""
        recommendations = sorted(
            self.optimization_recommendations.values(),
            key=lambda x: x.estimated_savings_usd,
            reverse=True
        )[:limit]

        return [
            {
                "recommendation_id": rec.recommendation_id,
                "strategy": rec.strategy.value,
                "resource_id": rec.resource_id,
                "provider": rec.provider.value,
                "service": rec.service_name,
                "current_cost_usd": rec.current_cost_usd,
                "estimated_savings_usd": rec.estimated_savings_usd,
                "roi_percent": rec.roi_percent,
                "confidence_score": rec.confidence_score,
                "implementation_effort": rec.implementation_effort,
                "risk_level": rec.risk_level,
                "description": rec.description,
                "action_required": rec.action_required,
                "timestamp": rec.timestamp.isoformat()
            }
            for rec in recommendations
        ]

    def get_cost_alerts(self, severity: Optional[CostSeverity] = None) -> List[Dict[str, Any]]:
        """Get current cost alerts, optionally filtered by severity."""
        alerts = list(self.cost_alerts.values())

        if severity:
            alerts = [alert for alert in alerts if alert.severity == severity]

        return [
            {
                "alert_id": alert.alert_id,
                "severity": alert.severity.value,
                "provider": alert.provider.value,
                "service": alert.service_name,
                "resource_id": alert.resource_id,
                "threshold_type": alert.threshold_type,
                "threshold_value": alert.threshold_value,
                "current_value": alert.current_value,
                "message": alert.message,
                "recommended_action": alert.recommended_action,
                "timestamp": alert.timestamp.isoformat()
            }
            for alert in sorted(alerts, key=lambda x: x.timestamp, reverse=True)
        ]

    # Simulation methods for development/testing
    def _simulate_current_cost(self, provider: CloudProvider, service: str) -> float:
        """Simulate current cost for a service."""
        base_costs = {
            "ec2": 150.0, "rds": 200.0, "s3": 50.0, "lambda": 25.0, "ecs": 100.0,
            "compute": 180.0, "storage": 60.0, "database": 250.0, "functions": 30.0,
            "sql": 220.0
        }
        base_cost = base_costs.get(service, 100.0)

        # Add some randomness
        import random
        variation = random.uniform(0.8, 1.2)
        return base_cost * variation

    def _simulate_cost_trend(self) -> List[float]:
        """Simulate 30-day cost trend."""
        import random
        base_cost = random.uniform(50, 300)
        trend = []
        for i in range(30):
            # Simulate daily cost with some variation
            daily_cost = base_cost * random.uniform(0.8, 1.2)
            trend.append(daily_cost)
        return trend

    def _get_monthly_budget(self, provider: CloudProvider, service: str) -> float:
        """Get monthly budget for a service."""
        base_budgets = {
            "ec2": 1000.0, "rds": 1500.0, "s3": 300.0, "lambda": 200.0, "ecs": 800.0,
            "compute": 1200.0, "storage": 400.0, "database": 2000.0, "functions": 250.0,
            "sql": 1800.0
        }
        return base_budgets.get(service, 500.0)

    def _simulate_cost_per_unit(self, service: str) -> float:
        """Simulate cost per unit for a service."""
        unit_costs = {
            "ec2": 0.10, "rds": 0.25, "s3": 0.023, "lambda": 0.0000002, "ecs": 0.15,
            "compute": 0.12, "storage": 0.02, "database": 0.30, "functions": 0.0000003,
            "sql": 0.28
        }
        return unit_costs.get(service, 0.10)

    def _simulate_utilization(self) -> float:
        """Simulate resource utilization."""
        import random
        return random.uniform(5, 95)

    def _get_recommended_action(self, severity: CostSeverity, threshold_type: str) -> str:
        """Get recommended action for an alert."""
        actions = {
            CostSeverity.CRITICAL: "Immediate action required - Review and optimize resources",
            CostSeverity.HIGH: "Urgent review needed - Implement cost controls",
            CostSeverity.MEDIUM: "Monitor closely and plan optimization",
            CostSeverity.LOW: "Schedule routine optimization review"
        }
        return actions.get(severity, "Review resource usage")


# Global cost monitor instance
cost_monitor = CostOptimizationMonitor()
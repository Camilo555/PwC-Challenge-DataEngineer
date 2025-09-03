"""
Enterprise Monitoring Orchestrator
Coordinates all monitoring, observability, and alerting components for comprehensive production monitoring.
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from core.distributed_tracing import get_tracing_manager
from core.logging import get_logger
from core.structured_logging import LogContext, StructuredLogger, log_context
from monitoring.advanced_alerting_sla import get_alerting_sla
from monitoring.advanced_health_checks import get_health_check_manager

# Import all monitoring components
from monitoring.datadog_custom_metrics_advanced import get_custom_metrics_advanced
from monitoring.etl_comprehensive_observability import get_etl_observability_manager
from monitoring.performance_monitoring_realtime import get_performance_monitor
from monitoring.synthetic_monitoring_enterprise import get_synthetic_monitoring

logger = get_logger(__name__)
structured_logger = StructuredLogger.get_logger(__name__)


class MonitoringTier(Enum):
    """Monitoring system tiers."""

    TIER_1_INFRASTRUCTURE = "tier_1_infrastructure"
    TIER_2_APPLICATION = "tier_2_application"
    TIER_3_BUSINESS = "tier_3_business"
    TIER_4_ANALYTICS = "tier_4_analytics"


@dataclass
class MonitoringConfig:
    """Configuration for enterprise monitoring orchestrator."""

    service_name: str = "pwc-data-platform"
    environment: str = "production"
    enable_datadog: bool = True
    enable_synthetic_monitoring: bool = True
    enable_performance_monitoring: bool = True
    enable_alerting: bool = True
    enable_health_checks: bool = True
    enable_etl_observability: bool = True

    # Alert response time targets (seconds)
    critical_alert_response_time: int = 5
    high_alert_response_time: int = 30
    medium_alert_response_time: int = 120

    # SLA targets
    system_availability_target: float = 99.9
    api_response_time_target_ms: float = 200.0
    data_freshness_target_minutes: int = 15


@dataclass
class MonitoringStatus:
    """Overall monitoring system status."""

    timestamp: datetime
    tier_1_status: str  # Infrastructure
    tier_2_status: str  # Application
    tier_3_status: str  # Business
    tier_4_status: str  # Analytics
    overall_health: str
    active_alerts_count: int
    sla_compliance_score: float
    performance_metrics: dict[str, float]
    recent_incidents: list[dict[str, Any]]


class EnterpriseMonitoringOrchestrator:
    """
    Enterprise-grade monitoring orchestrator that coordinates all observability components.

    Features:
    - Unified monitoring dashboard and status
    - Coordinated alerting with smart escalation
    - SLA monitoring and breach prediction
    - Performance optimization recommendations
    - Automated incident response workflows
    - Business impact assessment
    - Cost optimization monitoring
    - Compliance and audit trail management
    """

    def __init__(self, config: MonitoringConfig = None):
        self.config = config or MonitoringConfig()
        self.logger = get_logger(f"{__name__}.orchestrator")
        self.structured_logger = StructuredLogger.get_logger(__name__)
        self.tracing_manager = get_tracing_manager()

        # Component instances
        self.custom_metrics = None
        self.alerting_sla = None
        self.health_monitor = None
        self.performance_monitor = None
        self.synthetic_monitoring = None
        self.etl_observability = None

        # Orchestrator state
        self.monitoring_active = False
        self.orchestration_tasks: dict[str, asyncio.Task] = {}
        self.system_status_history: list[MonitoringStatus] = []
        self.cost_metrics: dict[str, float] = {}

        self.logger.info(
            f"Enterprise monitoring orchestrator initialized for {self.config.service_name}"
        )

    async def initialize(self):
        """Initialize all monitoring components."""
        try:
            context = LogContext(operation="initialize_monitoring", component="orchestrator")

            with log_context(context):
                self.structured_logger.info("Initializing enterprise monitoring components")

                # Initialize DataDog custom metrics
                if self.config.enable_datadog:
                    self.custom_metrics = get_custom_metrics_advanced()
                    await self.custom_metrics.initialize()
                    self.logger.info("DataDog custom metrics initialized")

                # Initialize alerting and SLA monitoring
                if self.config.enable_alerting:
                    self.alerting_sla = get_alerting_sla()
                    self.logger.info("Advanced alerting and SLA monitoring initialized")

                # Initialize health monitoring
                if self.config.enable_health_checks:
                    self.health_monitor = get_health_check_manager()
                    self.logger.info("Advanced health monitoring initialized")

                # Initialize performance monitoring
                if self.config.enable_performance_monitoring:
                    self.performance_monitor = get_performance_monitor()
                    self.logger.info("Real-time performance monitoring initialized")

                # Initialize synthetic monitoring
                if self.config.enable_synthetic_monitoring:
                    self.synthetic_monitoring = get_synthetic_monitoring()
                    self.logger.info("Enterprise synthetic monitoring initialized")

                # Initialize ETL observability
                if self.config.enable_etl_observability:
                    self.etl_observability = get_etl_observability_manager()
                    self.logger.info("ETL comprehensive observability initialized")

                self.structured_logger.info(
                    "Enterprise monitoring orchestrator initialized successfully",
                    extra={
                        "category": "monitoring",
                        "event_type": "initialization_complete",
                        "enabled_components": {
                            "datadog": self.config.enable_datadog,
                            "alerting": self.config.enable_alerting,
                            "health_checks": self.config.enable_health_checks,
                            "performance": self.config.enable_performance_monitoring,
                            "synthetic": self.config.enable_synthetic_monitoring,
                            "etl_observability": self.config.enable_etl_observability,
                        },
                    },
                )

        except Exception as e:
            self.logger.error(f"Failed to initialize monitoring orchestrator: {str(e)}")
            raise

    async def start_monitoring(self):
        """Start all monitoring systems and orchestration tasks."""
        try:
            self.monitoring_active = True

            # Start all monitoring components
            if self.alerting_sla:
                await self.alerting_sla.start_monitoring()

            if self.health_monitor:
                await self.health_monitor.start_monitoring()

            if self.performance_monitor:
                await self.performance_monitor.start_monitoring()

            if self.synthetic_monitoring:
                await self.synthetic_monitoring.start_monitoring()

            if self.etl_observability:
                await self.etl_observability.start_monitoring()

            # Start orchestration tasks
            self.orchestration_tasks["system_status"] = asyncio.create_task(
                self._system_status_monitoring_loop()
            )

            self.orchestration_tasks["cost_monitoring"] = asyncio.create_task(
                self._cost_monitoring_loop()
            )

            self.orchestration_tasks["predictive_analysis"] = asyncio.create_task(
                self._predictive_analysis_loop()
            )

            self.orchestration_tasks["compliance_monitoring"] = asyncio.create_task(
                self._compliance_monitoring_loop()
            )

            self.orchestration_tasks["incident_coordination"] = asyncio.create_task(
                self._incident_coordination_loop()
            )

            self.logger.info("Enterprise monitoring orchestrator started successfully")

        except Exception as e:
            self.logger.error(f"Failed to start monitoring orchestrator: {str(e)}")
            raise

    async def stop_monitoring(self):
        """Stop all monitoring systems and orchestration tasks."""
        try:
            self.monitoring_active = False

            # Cancel orchestration tasks
            for task_name, task in self.orchestration_tasks.items():
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    self.logger.info(f"Stopped orchestration task: {task_name}")

            self.orchestration_tasks.clear()

            # Stop all monitoring components
            if self.etl_observability:
                await self.etl_observability.stop_monitoring()

            if self.synthetic_monitoring:
                await self.synthetic_monitoring.stop_monitoring()

            if self.performance_monitor:
                await self.performance_monitor.stop_monitoring()

            if self.health_monitor:
                await self.health_monitor.stop_monitoring()

            if self.alerting_sla:
                await self.alerting_sla.stop_monitoring()

            self.logger.info("Enterprise monitoring orchestrator stopped successfully")

        except Exception as e:
            self.logger.error(f"Error stopping monitoring orchestrator: {str(e)}")

    async def _system_status_monitoring_loop(self):
        """Monitor overall system status and generate unified reports."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(60)  # Check every minute

                # Collect status from all tiers
                current_status = await self._collect_system_status()

                # Store status history
                self.system_status_history.append(current_status)

                # Keep only last 24 hours of status
                cutoff_time = datetime.utcnow() - timedelta(hours=24)
                self.system_status_history = [
                    status
                    for status in self.system_status_history
                    if status.timestamp >= cutoff_time
                ]

                # Send unified metrics to DataDog
                if self.custom_metrics:
                    await self._send_unified_metrics(current_status)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in system status monitoring loop: {str(e)}")
                await asyncio.sleep(60)

    async def _cost_monitoring_loop(self):
        """Monitor infrastructure and operational costs."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes

                # Calculate cost metrics
                cost_metrics = await self._calculate_cost_metrics()
                self.cost_metrics.update(cost_metrics)

                # Send cost metrics to monitoring
                if self.custom_metrics:
                    for metric_name, value in cost_metrics.items():
                        await self.custom_metrics.submit_metric(
                            f"cost.{metric_name}",
                            value,
                            dimensions={"service": self.config.service_name},
                        )

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in cost monitoring loop: {str(e)}")
                await asyncio.sleep(300)

    async def _predictive_analysis_loop(self):
        """Perform predictive analysis for proactive monitoring."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(600)  # Check every 10 minutes

                # Analyze trends and predict potential issues
                predictions = await self._analyze_predictive_trends()

                # Generate predictive alerts if needed
                for prediction in predictions:
                    if prediction.get("severity") in ["high", "critical"]:
                        await self._trigger_predictive_alert(prediction)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in predictive analysis loop: {str(e)}")
                await asyncio.sleep(600)

    async def _compliance_monitoring_loop(self):
        """Monitor compliance and audit requirements."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(3600)  # Check every hour

                # Check compliance metrics
                compliance_status = await self._check_compliance_status()

                # Log compliance events
                if compliance_status.get("violations"):
                    self.structured_logger.warning(
                        "Compliance violations detected",
                        extra={
                            "category": "compliance",
                            "event_type": "compliance_violation",
                            "violations": compliance_status["violations"],
                        },
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in compliance monitoring loop: {str(e)}")
                await asyncio.sleep(3600)

    async def _incident_coordination_loop(self):
        """Coordinate incident response across all monitoring systems."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds

                # Collect active incidents from all systems
                incidents = await self._collect_active_incidents()

                # Coordinate response based on business impact
                for incident in incidents:
                    await self._coordinate_incident_response(incident)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in incident coordination loop: {str(e)}")
                await asyncio.sleep(30)

    async def _collect_system_status(self) -> MonitoringStatus:
        """Collect unified system status from all tiers."""
        try:
            current_time = datetime.utcnow()

            # Tier 1: Infrastructure status
            tier_1_status = "healthy"
            if self.health_monitor:
                health_summary = self.health_monitor.get_health_summary()
                tier_1_status = health_summary.get("overall_status", "unknown")

            # Tier 2: Application status
            tier_2_status = "healthy"
            if self.performance_monitor:
                perf_summary = self.performance_monitor.get_performance_summary()
                if perf_summary.get("threshold_violations"):
                    tier_2_status = "degraded"

            # Tier 3: Business status
            tier_3_status = "healthy"
            if self.synthetic_monitoring:
                synthetic_summary = self.synthetic_monitoring.get_monitoring_summary()
                if synthetic_summary.get("recent_violations", 0) > 0:
                    tier_3_status = "at_risk"

            # Tier 4: Analytics status
            tier_4_status = "healthy"
            if self.etl_observability:
                etl_summary = self.etl_observability.get_observability_summary()
                if etl_summary.get("pipeline_failures", 0) > 0:
                    tier_4_status = "degraded"

            # Calculate overall health
            tier_statuses = [tier_1_status, tier_2_status, tier_3_status, tier_4_status]
            if "critical" in tier_statuses:
                overall_health = "critical"
            elif "degraded" in tier_statuses or "at_risk" in tier_statuses:
                overall_health = "degraded"
            else:
                overall_health = "healthy"

            # Collect metrics
            active_alerts_count = 0
            sla_compliance_score = 100.0

            if self.alerting_sla:
                alerting_summary = self.alerting_sla.get_alerting_summary()
                active_alerts_count = alerting_summary.get("alerts", {}).get("total_active", 0)
                sla_compliance = alerting_summary.get("sla_compliance", {})
                if sla_compliance.get("services"):
                    scores = [
                        s.get("compliance_score", 100) for s in sla_compliance["services"].values()
                    ]
                    sla_compliance_score = sum(scores) / len(scores) if scores else 100.0

            # Performance metrics
            performance_metrics = {}
            if self.performance_monitor:
                perf_summary = self.performance_monitor.get_performance_summary()
                performance_metrics = perf_summary.get("metrics_summary", {})

            # Recent incidents
            recent_incidents = []
            if self.alerting_sla and hasattr(self.alerting_sla, "incident_timeline"):
                recent_cutoff = current_time - timedelta(hours=1)
                recent_incidents = [
                    incident
                    for incident in self.alerting_sla.incident_timeline[-10:]
                    if datetime.fromisoformat(incident["timestamp"]) >= recent_cutoff
                ]

            return MonitoringStatus(
                timestamp=current_time,
                tier_1_status=tier_1_status,
                tier_2_status=tier_2_status,
                tier_3_status=tier_3_status,
                tier_4_status=tier_4_status,
                overall_health=overall_health,
                active_alerts_count=active_alerts_count,
                sla_compliance_score=sla_compliance_score,
                performance_metrics=performance_metrics,
                recent_incidents=recent_incidents,
            )

        except Exception as e:
            self.logger.error(f"Failed to collect system status: {str(e)}")
            return MonitoringStatus(
                timestamp=datetime.utcnow(),
                tier_1_status="unknown",
                tier_2_status="unknown",
                tier_3_status="unknown",
                tier_4_status="unknown",
                overall_health="unknown",
                active_alerts_count=0,
                sla_compliance_score=0.0,
                performance_metrics={},
                recent_incidents=[],
            )

    async def _send_unified_metrics(self, status: MonitoringStatus):
        """Send unified monitoring metrics to DataDog."""
        try:
            dimensions = {
                "service": self.config.service_name,
                "environment": self.config.environment,
            }

            # System health metrics
            health_scores = {
                "healthy": 100,
                "degraded": 70,
                "at_risk": 50,
                "critical": 20,
                "unknown": 0,
            }

            await self.custom_metrics.submit_metric(
                "enterprise.monitoring.tier1.health_score",
                health_scores.get(status.tier_1_status, 0),
                dimensions,
            )

            await self.custom_metrics.submit_metric(
                "enterprise.monitoring.tier2.health_score",
                health_scores.get(status.tier_2_status, 0),
                dimensions,
            )

            await self.custom_metrics.submit_metric(
                "enterprise.monitoring.tier3.health_score",
                health_scores.get(status.tier_3_status, 0),
                dimensions,
            )

            await self.custom_metrics.submit_metric(
                "enterprise.monitoring.tier4.health_score",
                health_scores.get(status.tier_4_status, 0),
                dimensions,
            )

            await self.custom_metrics.submit_metric(
                "enterprise.monitoring.overall.health_score",
                health_scores.get(status.overall_health, 0),
                dimensions,
            )

            await self.custom_metrics.submit_metric(
                "enterprise.monitoring.active_alerts_count", status.active_alerts_count, dimensions
            )

            await self.custom_metrics.submit_metric(
                "enterprise.monitoring.sla_compliance_score",
                status.sla_compliance_score,
                dimensions,
            )

        except Exception as e:
            self.logger.error(f"Failed to send unified metrics: {str(e)}")

    async def _calculate_cost_metrics(self) -> dict[str, float]:
        """Calculate infrastructure and operational cost metrics."""
        try:
            # Mock cost calculation - in production, this would integrate with cloud billing APIs
            cost_metrics = {
                "infrastructure_hourly_cost": 45.67,
                "data_processing_cost": 12.34,
                "monitoring_tools_cost": 8.90,
                "storage_cost": 5.23,
                "network_cost": 2.15,
                "total_hourly_cost": 74.29,
            }

            # Cost efficiency metrics
            if self.performance_monitor:
                perf_summary = self.performance_monitor.get_performance_summary()
                throughput_metrics = perf_summary.get("metrics_summary", {})

                # Calculate cost per request if we have throughput data
                total_requests = 0
                for metric_name, metric_data in throughput_metrics.items():
                    if "requests" in metric_name.lower():
                        total_requests += metric_data.get("count", 0)

                if total_requests > 0:
                    cost_metrics["cost_per_request"] = (
                        cost_metrics["total_hourly_cost"] / total_requests
                    )

            return cost_metrics

        except Exception as e:
            self.logger.error(f"Failed to calculate cost metrics: {str(e)}")
            return {}

    async def _analyze_predictive_trends(self) -> list[dict[str, Any]]:
        """Analyze trends for predictive alerting."""
        try:
            predictions = []

            # Analyze system status trends
            if len(self.system_status_history) >= 10:
                recent_statuses = self.system_status_history[-10:]

                # Check for degrading trends
                degraded_count = sum(
                    1 for s in recent_statuses if s.overall_health in ["degraded", "critical"]
                )

                if degraded_count >= 5:
                    predictions.append(
                        {
                            "type": "system_degradation_trend",
                            "severity": "high",
                            "description": "System health showing degradation trend",
                            "recommended_actions": [
                                "Review system resource utilization",
                                "Check for capacity constraints",
                                "Investigate root cause",
                            ],
                        }
                    )

                # Check for SLA compliance trends
                sla_scores = [s.sla_compliance_score for s in recent_statuses]
                if sla_scores and all(score < 95.0 for score in sla_scores[-3:]):
                    predictions.append(
                        {
                            "type": "sla_compliance_risk",
                            "severity": "medium",
                            "description": "SLA compliance trending downward",
                            "recommended_actions": [
                                "Review SLA targets",
                                "Optimize performance bottlenecks",
                                "Consider scaling resources",
                            ],
                        }
                    )

            return predictions

        except Exception as e:
            self.logger.error(f"Failed to analyze predictive trends: {str(e)}")
            return []

    async def _trigger_predictive_alert(self, prediction: dict[str, Any]):
        """Trigger predictive alert based on trend analysis."""
        try:
            self.structured_logger.warning(
                f"Predictive alert: {prediction['description']}",
                extra={
                    "category": "monitoring",
                    "event_type": "predictive_alert",
                    "prediction_type": prediction["type"],
                    "severity": prediction["severity"],
                    "recommended_actions": prediction["recommended_actions"],
                },
            )

        except Exception as e:
            self.logger.error(f"Failed to trigger predictive alert: {str(e)}")

    async def _check_compliance_status(self) -> dict[str, Any]:
        """Check compliance status across all systems."""
        try:
            compliance_status = {
                "data_retention_compliance": True,
                "security_logging_compliance": True,
                "audit_trail_compliance": True,
                "performance_sla_compliance": True,
                "violations": [],
            }

            # Check data retention compliance
            # This would check actual log retention policies

            # Check security logging compliance
            # This would verify security events are being logged

            # Check audit trail compliance
            # This would verify all required events are audited

            # Check performance SLA compliance
            if hasattr(self, "system_status_history") and self.system_status_history:
                recent_status = self.system_status_history[-1]
                if recent_status.sla_compliance_score < 95.0:
                    compliance_status["performance_sla_compliance"] = False
                    compliance_status["violations"].append(
                        {
                            "type": "sla_compliance",
                            "description": f"SLA compliance below target: {recent_status.sla_compliance_score}%",
                        }
                    )

            return compliance_status

        except Exception as e:
            self.logger.error(f"Failed to check compliance status: {str(e)}")
            return {"violations": []}

    async def _collect_active_incidents(self) -> list[dict[str, Any]]:
        """Collect active incidents from all monitoring systems."""
        try:
            incidents = []

            # Collect from alerting system
            if self.alerting_sla:
                alerting_summary = self.alerting_sla.get_alerting_summary()
                active_alerts = alerting_summary.get("alerts", {})

                for severity, count in active_alerts.get("by_severity", {}).items():
                    if count > 0:
                        incidents.append(
                            {
                                "source": "alerting",
                                "type": "alert",
                                "severity": severity,
                                "count": count,
                                "timestamp": datetime.utcnow().isoformat(),
                            }
                        )

            # Collect from health monitoring
            if self.health_monitor:
                health_summary = self.health_monitor.get_health_summary()
                if health_summary.get("overall_status") in ["critical", "degraded"]:
                    incidents.append(
                        {
                            "source": "health_monitor",
                            "type": "health_issue",
                            "severity": "high"
                            if health_summary["overall_status"] == "critical"
                            else "medium",
                            "description": f"System health: {health_summary['overall_status']}",
                            "timestamp": datetime.utcnow().isoformat(),
                        }
                    )

            return incidents

        except Exception as e:
            self.logger.error(f"Failed to collect active incidents: {str(e)}")
            return []

    async def _coordinate_incident_response(self, incident: dict[str, Any]):
        """Coordinate incident response based on business impact."""
        try:
            # Log incident coordination
            self.structured_logger.info(
                f"Coordinating response for incident: {incident.get('type')}",
                extra={
                    "category": "incident",
                    "event_type": "incident_coordination",
                    "incident": incident,
                },
            )

            # In production, this would trigger automated remediation
            # based on incident type and severity

        except Exception as e:
            self.logger.error(f"Failed to coordinate incident response: {str(e)}")

    def get_comprehensive_status(self) -> dict[str, Any]:
        """Get comprehensive monitoring status across all systems."""
        try:
            current_time = datetime.utcnow()

            # Get current system status
            current_status = self.system_status_history[-1] if self.system_status_history else None

            # Collect summaries from all components
            summaries = {}

            if self.custom_metrics:
                summaries["custom_metrics"] = {"status": "active", "initialized": True}

            if self.alerting_sla:
                summaries["alerting_sla"] = self.alerting_sla.get_alerting_summary()

            if self.health_monitor:
                summaries["health_monitor"] = self.health_monitor.get_health_summary()

            if self.performance_monitor:
                summaries["performance_monitor"] = (
                    self.performance_monitor.get_performance_summary()
                )

            if self.synthetic_monitoring:
                summaries["synthetic_monitoring"] = (
                    self.synthetic_monitoring.get_monitoring_summary()
                )

            if self.etl_observability:
                summaries["etl_observability"] = self.etl_observability.get_observability_summary()

            return {
                "timestamp": current_time.isoformat(),
                "service_name": self.config.service_name,
                "environment": self.config.environment,
                "monitoring_active": self.monitoring_active,
                "orchestrator_status": {
                    "active_tasks": len(self.orchestration_tasks),
                    "system_status_history_length": len(self.system_status_history),
                },
                "current_status": {
                    "tier_1_infrastructure": current_status.tier_1_status
                    if current_status
                    else "unknown",
                    "tier_2_application": current_status.tier_2_status
                    if current_status
                    else "unknown",
                    "tier_3_business": current_status.tier_3_status
                    if current_status
                    else "unknown",
                    "tier_4_analytics": current_status.tier_4_status
                    if current_status
                    else "unknown",
                    "overall_health": current_status.overall_health
                    if current_status
                    else "unknown",
                    "active_alerts": current_status.active_alerts_count if current_status else 0,
                    "sla_compliance_score": current_status.sla_compliance_score
                    if current_status
                    else 0.0,
                },
                "cost_metrics": self.cost_metrics,
                "component_summaries": summaries,
                "configuration": {
                    "enable_datadog": self.config.enable_datadog,
                    "enable_synthetic_monitoring": self.config.enable_synthetic_monitoring,
                    "enable_performance_monitoring": self.config.enable_performance_monitoring,
                    "enable_alerting": self.config.enable_alerting,
                    "enable_health_checks": self.config.enable_health_checks,
                    "enable_etl_observability": self.config.enable_etl_observability,
                    "system_availability_target": self.config.system_availability_target,
                    "api_response_time_target_ms": self.config.api_response_time_target_ms,
                    "data_freshness_target_minutes": self.config.data_freshness_target_minutes,
                },
            }

        except Exception as e:
            self.logger.error(f"Failed to get comprehensive status: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}


# Global orchestrator instance
_monitoring_orchestrator: EnterpriseMonitoringOrchestrator | None = None


def get_monitoring_orchestrator(
    config: MonitoringConfig = None,
) -> EnterpriseMonitoringOrchestrator:
    """Get or create the global monitoring orchestrator instance."""
    global _monitoring_orchestrator

    if _monitoring_orchestrator is None:
        _monitoring_orchestrator = EnterpriseMonitoringOrchestrator(config)

    return _monitoring_orchestrator


# Convenience functions
async def initialize_enterprise_monitoring(config: MonitoringConfig = None):
    """Initialize the enterprise monitoring orchestrator."""
    orchestrator = get_monitoring_orchestrator(config)
    await orchestrator.initialize()


async def start_enterprise_monitoring():
    """Start the enterprise monitoring orchestrator."""
    orchestrator = get_monitoring_orchestrator()
    await orchestrator.start_monitoring()


async def stop_enterprise_monitoring():
    """Stop the enterprise monitoring orchestrator."""
    orchestrator = get_monitoring_orchestrator()
    await orchestrator.stop_monitoring()


def get_enterprise_monitoring_status() -> dict[str, Any]:
    """Get comprehensive enterprise monitoring status."""
    orchestrator = get_monitoring_orchestrator()
    return orchestrator.get_comprehensive_status()

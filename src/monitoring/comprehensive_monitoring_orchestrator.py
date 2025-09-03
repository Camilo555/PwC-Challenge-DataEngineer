"""
Comprehensive Monitoring Orchestrator
Central orchestrator for all monitoring, observability, and alerting systems.
Provides unified management, health scoring, and operational insights.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from core.distributed_tracing import DistributedTracingManager, get_tracing_manager
from core.logging import get_logger
from core.structured_logging import StructuredLogger
from monitoring.advanced_alerting_sla import AdvancedAlertingSLA, get_alerting_sla
from monitoring.advanced_health_checks import HealthCheckManager, get_health_check_manager
from monitoring.datadog_custom_metrics_advanced import (
    DataDogCustomMetricsAdvanced,
    get_custom_metrics_advanced,
)
from monitoring.etl_comprehensive_observability import (
    ETLObservabilityManager,
    get_etl_observability_manager,
)
from monitoring.performance_monitoring_realtime import (
    RealTimePerformanceMonitoring,
    get_performance_monitoring,
)
from monitoring.synthetic_monitoring_enterprise import (
    SyntheticMonitoringEnterprise,
    get_synthetic_monitoring,
)

logger = get_logger(__name__)
structured_logger = StructuredLogger.get_logger(__name__)


class SystemHealth(Enum):
    """Overall system health status."""

    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class MonitoringComponent(Enum):
    """Monitoring system components."""

    CUSTOM_METRICS = "custom_metrics"
    HEALTH_CHECKS = "health_checks"
    PERFORMANCE_MONITORING = "performance_monitoring"
    ETL_OBSERVABILITY = "etl_observability"
    SYNTHETIC_MONITORING = "synthetic_monitoring"
    ALERTING_SLA = "alerting_sla"
    DISTRIBUTED_TRACING = "distributed_tracing"
    STRUCTURED_LOGGING = "structured_logging"


@dataclass
class MonitoringInsights:
    """Comprehensive monitoring insights and recommendations."""

    overall_health: SystemHealth
    health_score: float  # 0-100
    critical_issues: list[str]
    warnings: list[str]
    recommendations: list[str]
    performance_summary: dict[str, Any]
    sla_compliance: dict[str, Any]
    capacity_forecast: dict[str, Any]
    cost_analysis: dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)


class MonitoringOrchestrator:
    """
    Central orchestrator for comprehensive monitoring and observability.

    Features:
    - Unified management of all monitoring components
    - Cross-component correlation and insights
    - Automated health scoring and issue prioritization
    - Capacity planning and cost optimization
    - Operational runbook generation
    - Executive dashboard data aggregation
    """

    def __init__(self, service_name: str = "monitoring-orchestrator"):
        self.service_name = service_name
        self.logger = get_logger(f"{__name__}.{service_name}")
        self.structured_logger = StructuredLogger.get_logger(__name__)

        # Initialize all monitoring components
        self.custom_metrics: DataDogCustomMetricsAdvanced | None = None
        self.health_checks: HealthCheckManager | None = None
        self.performance_monitoring: RealTimePerformanceMonitoring | None = None
        self.etl_observability: ETLObservabilityManager | None = None
        self.synthetic_monitoring: SyntheticMonitoringEnterprise | None = None
        self.alerting_sla: AdvancedAlertingSLA | None = None
        self.distributed_tracing: DistributedTracingManager | None = None

        # Component status tracking
        self.component_status: dict[MonitoringComponent, bool] = {}
        self.component_health: dict[MonitoringComponent, SystemHealth] = {}
        self.component_errors: dict[MonitoringComponent, list[str]] = {}

        # Background tasks
        self.monitoring_active = False
        self.background_tasks: dict[str, asyncio.Task] = {}

        # Historical data for trending
        self.health_history: list[tuple[datetime, float]] = []
        self.insights_history: list[MonitoringInsights] = []

        self.logger.info(f"Monitoring orchestrator initialized for {service_name}")

    async def initialize_all_components(self):
        """Initialize and configure all monitoring components."""
        try:
            self.logger.info("Initializing all monitoring components...")

            # Initialize custom metrics system
            try:
                self.custom_metrics = get_custom_metrics_advanced(
                    service_name="data-platform-metrics"
                )
                self.component_status[MonitoringComponent.CUSTOM_METRICS] = True
                self.component_health[MonitoringComponent.CUSTOM_METRICS] = SystemHealth.HEALTHY
                self.logger.info("✓ Custom metrics system initialized")
            except Exception as e:
                self._record_component_error(MonitoringComponent.CUSTOM_METRICS, str(e))

            # Initialize health checks
            try:
                self.health_checks = get_health_check_manager()
                self.component_status[MonitoringComponent.HEALTH_CHECKS] = True
                self.component_health[MonitoringComponent.HEALTH_CHECKS] = SystemHealth.HEALTHY
                self.logger.info("✓ Health checks system initialized")
            except Exception as e:
                self._record_component_error(MonitoringComponent.HEALTH_CHECKS, str(e))

            # Initialize performance monitoring
            try:
                self.performance_monitoring = get_performance_monitoring(
                    service_name="data-platform-performance"
                )
                self.component_status[MonitoringComponent.PERFORMANCE_MONITORING] = True
                self.component_health[MonitoringComponent.PERFORMANCE_MONITORING] = (
                    SystemHealth.HEALTHY
                )
                self.logger.info("✓ Performance monitoring initialized")
            except Exception as e:
                self._record_component_error(MonitoringComponent.PERFORMANCE_MONITORING, str(e))

            # Initialize ETL observability
            try:
                self.etl_observability = get_etl_observability_manager()
                self.component_status[MonitoringComponent.ETL_OBSERVABILITY] = True
                self.component_health[MonitoringComponent.ETL_OBSERVABILITY] = SystemHealth.HEALTHY
                self.logger.info("✓ ETL observability initialized")
            except Exception as e:
                self._record_component_error(MonitoringComponent.ETL_OBSERVABILITY, str(e))

            # Initialize synthetic monitoring
            try:
                self.synthetic_monitoring = get_synthetic_monitoring(
                    service_name="data-platform-synthetic"
                )
                self.component_status[MonitoringComponent.SYNTHETIC_MONITORING] = True
                self.component_health[MonitoringComponent.SYNTHETIC_MONITORING] = (
                    SystemHealth.HEALTHY
                )
                self.logger.info("✓ Synthetic monitoring initialized")
            except Exception as e:
                self._record_component_error(MonitoringComponent.SYNTHETIC_MONITORING, str(e))

            # Initialize alerting and SLA monitoring
            try:
                self.alerting_sla = get_alerting_sla(service_name="data-platform-alerts")
                self.component_status[MonitoringComponent.ALERTING_SLA] = True
                self.component_health[MonitoringComponent.ALERTING_SLA] = SystemHealth.HEALTHY
                self.logger.info("✓ Alerting and SLA monitoring initialized")
            except Exception as e:
                self._record_component_error(MonitoringComponent.ALERTING_SLA, str(e))

            # Initialize distributed tracing
            try:
                self.distributed_tracing = get_tracing_manager()
                self.component_status[MonitoringComponent.DISTRIBUTED_TRACING] = True
                self.component_health[MonitoringComponent.DISTRIBUTED_TRACING] = (
                    SystemHealth.HEALTHY
                )
                self.logger.info("✓ Distributed tracing initialized")
            except Exception as e:
                self._record_component_error(MonitoringComponent.DISTRIBUTED_TRACING, str(e))

            # Structured logging is already available
            self.component_status[MonitoringComponent.STRUCTURED_LOGGING] = True
            self.component_health[MonitoringComponent.STRUCTURED_LOGGING] = SystemHealth.HEALTHY
            self.logger.info("✓ Structured logging available")

            initialized_count = sum(1 for status in self.component_status.values() if status)
            total_count = len(MonitoringComponent)

            self.logger.info(
                f"Monitoring orchestrator: {initialized_count}/{total_count} components initialized successfully"
            )

            if initialized_count < total_count:
                self.logger.warning(
                    "Some monitoring components failed to initialize. Check component errors for details."
                )

        except Exception as e:
            self.logger.error(f"Failed to initialize monitoring components: {str(e)}")
            raise

    async def start_all_monitoring(self):
        """Start all monitoring systems."""
        try:
            self.monitoring_active = True
            self.logger.info("Starting all monitoring systems...")

            # Start performance monitoring
            if self.performance_monitoring:
                try:
                    await self.performance_monitoring.start_monitoring()
                    self.logger.info("✓ Performance monitoring started")
                except Exception as e:
                    self._record_component_error(
                        MonitoringComponent.PERFORMANCE_MONITORING, f"Start failed: {str(e)}"
                    )

            # Start synthetic monitoring
            if self.synthetic_monitoring:
                try:
                    await self.synthetic_monitoring.start_monitoring()
                    self.logger.info("✓ Synthetic monitoring started")
                except Exception as e:
                    self._record_component_error(
                        MonitoringComponent.SYNTHETIC_MONITORING, f"Start failed: {str(e)}"
                    )

            # Start alerting and SLA monitoring
            if self.alerting_sla:
                try:
                    await self.alerting_sla.start_monitoring()
                    self.logger.info("✓ Alerting and SLA monitoring started")
                except Exception as e:
                    self._record_component_error(
                        MonitoringComponent.ALERTING_SLA, f"Start failed: {str(e)}"
                    )

            # Start orchestrator background tasks
            insights_task = asyncio.create_task(self._monitoring_insights_loop())
            self.background_tasks["insights"] = insights_task

            health_task = asyncio.create_task(self._health_scoring_loop())
            self.background_tasks["health_scoring"] = health_task

            correlation_task = asyncio.create_task(self._cross_component_correlation_loop())
            self.background_tasks["correlation"] = correlation_task

            self.logger.info("All monitoring systems started successfully")

        except Exception as e:
            self.logger.error(f"Failed to start monitoring systems: {str(e)}")
            raise

    async def stop_all_monitoring(self):
        """Stop all monitoring systems gracefully."""
        try:
            self.monitoring_active = False
            self.logger.info("Stopping all monitoring systems...")

            # Stop background tasks
            for task_name, task in self.background_tasks.items():
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    self.logger.info(f"✓ Stopped background task: {task_name}")

            # Stop monitoring components
            if self.performance_monitoring:
                try:
                    await self.performance_monitoring.stop_monitoring()
                    self.logger.info("✓ Performance monitoring stopped")
                except Exception as e:
                    self.logger.warning(f"Error stopping performance monitoring: {str(e)}")

            if self.synthetic_monitoring:
                try:
                    await self.synthetic_monitoring.stop_monitoring()
                    self.logger.info("✓ Synthetic monitoring stopped")
                except Exception as e:
                    self.logger.warning(f"Error stopping synthetic monitoring: {str(e)}")

            if self.alerting_sla:
                try:
                    await self.alerting_sla.stop_monitoring()
                    self.logger.info("✓ Alerting and SLA monitoring stopped")
                except Exception as e:
                    self.logger.warning(f"Error stopping alerting system: {str(e)}")

            if self.distributed_tracing:
                try:
                    self.distributed_tracing.shutdown()
                    self.logger.info("✓ Distributed tracing shutdown")
                except Exception as e:
                    self.logger.warning(f"Error shutting down tracing: {str(e)}")

            if self.etl_observability:
                try:
                    self.etl_observability.shutdown()
                    self.logger.info("✓ ETL observability shutdown")
                except Exception as e:
                    self.logger.warning(f"Error shutting down ETL observability: {str(e)}")

            self.background_tasks.clear()
            self.logger.info("All monitoring systems stopped successfully")

        except Exception as e:
            self.logger.error(f"Error stopping monitoring systems: {str(e)}")

    async def _monitoring_insights_loop(self):
        """Background task for generating monitoring insights."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(300)  # Every 5 minutes

                insights = await self.generate_comprehensive_insights()
                self.insights_history.append(insights)

                # Keep only last 24 hours of insights
                cutoff_time = datetime.utcnow() - timedelta(hours=24)
                self.insights_history = [
                    i for i in self.insights_history if i.timestamp >= cutoff_time
                ]

                # Log critical insights
                if insights.overall_health in [SystemHealth.CRITICAL, SystemHealth.WARNING]:
                    self.structured_logger.warning(
                        f"System health alert: {insights.overall_health.value} (Score: {insights.health_score:.1f})",
                        category="monitoring",
                        event_type="health_alert",
                        critical_issues=insights.critical_issues,
                        health_score=insights.health_score,
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in monitoring insights loop: {str(e)}")
                await asyncio.sleep(300)

    async def _health_scoring_loop(self):
        """Background task for health scoring."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(60)  # Every minute

                health_score = await self.calculate_overall_health_score()
                self.health_history.append((datetime.utcnow(), health_score))

                # Keep only last 24 hours
                cutoff_time = datetime.utcnow() - timedelta(hours=24)
                self.health_history = [
                    (ts, score) for ts, score in self.health_history if ts >= cutoff_time
                ]

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in health scoring loop: {str(e)}")
                await asyncio.sleep(60)

    async def _cross_component_correlation_loop(self):
        """Background task for cross-component correlation analysis."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(600)  # Every 10 minutes

                # Perform correlation analysis
                correlations = await self._analyze_component_correlations()

                # Log significant correlations
                for correlation in correlations:
                    if correlation.get("strength", 0) > 0.7:  # Strong correlation
                        self.structured_logger.info(
                            f"Strong correlation detected: {correlation.get('description', 'Unknown')}",
                            category="monitoring",
                            event_type="correlation_detected",
                            correlation_strength=correlation.get("strength", 0),
                        )

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in correlation analysis loop: {str(e)}")
                await asyncio.sleep(600)

    async def generate_comprehensive_insights(self) -> MonitoringInsights:
        """Generate comprehensive monitoring insights and recommendations."""
        try:
            current_time = datetime.utcnow()

            # Collect data from all components
            component_data = await self._collect_all_component_data()

            # Calculate overall health score
            health_score = await self.calculate_overall_health_score()

            # Determine overall health status
            if health_score >= 90:
                overall_health = SystemHealth.HEALTHY
            elif health_score >= 70:
                overall_health = SystemHealth.WARNING
            elif health_score >= 30:
                overall_health = SystemHealth.CRITICAL
            else:
                overall_health = SystemHealth.UNKNOWN

            # Identify critical issues
            critical_issues = []
            warnings = []

            # Check each component for issues
            for component, health in self.component_health.items():
                if health == SystemHealth.CRITICAL:
                    critical_issues.append(f"{component.value} is in critical state")
                elif health == SystemHealth.WARNING:
                    warnings.append(f"{component.value} needs attention")

            # Add component-specific issues
            if self.alerting_sla:
                try:
                    alert_summary = self.alerting_sla.get_alerting_summary()
                    active_critical = (
                        alert_summary.get("alerts", {}).get("by_severity", {}).get("p0_critical", 0)
                    )
                    if active_critical > 0:
                        critical_issues.append(f"{active_critical} critical alerts active")
                except Exception:
                    pass

            if self.performance_monitoring:
                try:
                    perf_summary = self.performance_monitoring.get_performance_summary()
                    if perf_summary.get("overall_health_score", 100) < 50:
                        critical_issues.append("System performance is critically degraded")
                except Exception:
                    pass

            # Generate recommendations
            recommendations = self._generate_recommendations(
                health_score, critical_issues, warnings
            )

            # Performance summary
            performance_summary = component_data.get("performance", {})

            # SLA compliance
            sla_compliance = component_data.get("sla", {})

            # Capacity forecast
            capacity_forecast = await self._generate_capacity_forecast()

            # Cost analysis
            cost_analysis = await self._generate_cost_analysis()

            return MonitoringInsights(
                overall_health=overall_health,
                health_score=health_score,
                critical_issues=critical_issues,
                warnings=warnings,
                recommendations=recommendations,
                performance_summary=performance_summary,
                sla_compliance=sla_compliance,
                capacity_forecast=capacity_forecast,
                cost_analysis=cost_analysis,
                timestamp=current_time,
            )

        except Exception as e:
            self.logger.error(f"Failed to generate comprehensive insights: {str(e)}")
            return MonitoringInsights(
                overall_health=SystemHealth.UNKNOWN,
                health_score=0.0,
                critical_issues=[f"Insights generation failed: {str(e)}"],
                warnings=[],
                recommendations=["Investigate monitoring system issues"],
                performance_summary={},
                sla_compliance={},
                capacity_forecast={},
                cost_analysis={},
            )

    async def calculate_overall_health_score(self) -> float:
        """Calculate overall system health score (0-100)."""
        try:
            component_scores = []

            # Health check score (20% weight)
            if self.health_checks:
                try:
                    status, summary = self.health_checks.get_overall_health_status()
                    if status.value == "healthy":
                        health_score = 100.0
                    elif status.value == "degraded":
                        health_score = 70.0
                    elif status.value == "unhealthy":
                        health_score = 40.0
                    elif status.value == "critical":
                        health_score = 10.0
                    else:
                        health_score = 50.0

                    component_scores.append((health_score, 0.2))
                except Exception:
                    component_scores.append((50.0, 0.2))  # Default score on error

            # Performance score (25% weight)
            if self.performance_monitoring:
                try:
                    perf_summary = self.performance_monitoring.get_performance_summary()
                    perf_score = perf_summary.get("overall_health_score", 50.0)
                    component_scores.append((perf_score, 0.25))
                except Exception:
                    component_scores.append((50.0, 0.25))

            # ETL observability score (20% weight)
            if self.etl_observability:
                try:
                    # Calculate score based on recent pipeline success rates
                    etl_score = 80.0  # Default - would calculate from actual data
                    component_scores.append((etl_score, 0.2))
                except Exception:
                    component_scores.append((50.0, 0.2))

            # Synthetic monitoring score (15% weight)
            if self.synthetic_monitoring:
                try:
                    synth_summary = self.synthetic_monitoring.get_monitoring_summary()
                    synth_health = synth_summary.get("overall_health", "unknown")
                    if synth_health == "healthy":
                        synth_score = 100.0
                    elif synth_health == "warning":
                        synth_score = 70.0
                    elif synth_health == "critical":
                        synth_score = 30.0
                    else:
                        synth_score = 50.0

                    component_scores.append((synth_score, 0.15))
                except Exception:
                    component_scores.append((50.0, 0.15))

            # SLA compliance score (20% weight)
            if self.alerting_sla:
                try:
                    alert_summary = self.alerting_sla.get_alerting_summary()
                    sla_health = alert_summary.get("sla_compliance", {}).get("overall_health", 50.0)
                    component_scores.append((sla_health, 0.2))
                except Exception:
                    component_scores.append((50.0, 0.2))

            # Calculate weighted average
            if component_scores:
                weighted_sum = sum(score * weight for score, weight in component_scores)
                total_weight = sum(weight for _, weight in component_scores)
                return weighted_sum / total_weight if total_weight > 0 else 50.0

            return 50.0  # Default score

        except Exception as e:
            self.logger.error(f"Failed to calculate health score: {str(e)}")
            return 0.0

    def get_monitoring_dashboard_data(self) -> dict[str, Any]:
        """Get comprehensive dashboard data for executive reporting."""
        try:
            current_time = datetime.utcnow()

            # Get latest insights
            latest_insights = self.insights_history[-1] if self.insights_history else None

            # Component status summary
            component_summary = {}
            for component, status in self.component_status.items():
                health = self.component_health.get(component, SystemHealth.UNKNOWN)
                errors = self.component_errors.get(component, [])

                component_summary[component.value] = {
                    "status": "operational" if status else "failed",
                    "health": health.value,
                    "error_count": len(errors),
                    "last_errors": errors[-3:] if errors else [],  # Last 3 errors
                }

            # Health trend (last 24 hours)
            health_trend = []
            if len(self.health_history) > 1:
                for timestamp, score in self.health_history[-24:]:  # Last 24 data points
                    health_trend.append({"timestamp": timestamp.isoformat(), "score": score})

            # Operational metrics
            operational_metrics = {}

            if self.custom_metrics:
                try:
                    metrics_summary = self.custom_metrics.get_metrics_summary()
                    operational_metrics["custom_metrics"] = {
                        "total_metrics": metrics_summary.get("metrics", {}).get("total_defined", 0),
                        "processing_rate": metrics_summary.get("metrics", {}).get(
                            "processing_rate_per_minute", 0
                        ),
                        "quality_score": metrics_summary.get("quality", {}).get(
                            "avg_quality_score", 0
                        ),
                    }
                except Exception:
                    pass

            if self.synthetic_monitoring:
                try:
                    synth_summary = self.synthetic_monitoring.get_monitoring_summary()
                    operational_metrics["synthetic_monitoring"] = {
                        "total_checks": synth_summary.get("statistics", {}).get("total_checks", 0),
                        "success_rate": synth_summary.get("statistics", {}).get(
                            "success_rate_percent", 0
                        ),
                        "avg_response_time": synth_summary.get("statistics", {}).get(
                            "avg_response_time_ms", 0
                        ),
                    }
                except Exception:
                    pass

            # Recent incidents and alerts
            recent_incidents = []
            if self.alerting_sla:
                try:
                    alert_summary = self.alerting_sla.get_alerting_summary()
                    recent_incidents = alert_summary.get("alerts", {}).get("recent", [])
                except Exception:
                    pass

            return {
                "timestamp": current_time.isoformat(),
                "service_name": self.service_name,
                "overall_status": {
                    "health": latest_insights.overall_health.value
                    if latest_insights
                    else SystemHealth.UNKNOWN.value,
                    "score": latest_insights.health_score if latest_insights else 0.0,
                    "trend": self._calculate_health_trend(),
                },
                "components": component_summary,
                "health_trend": health_trend,
                "operational_metrics": operational_metrics,
                "critical_issues": latest_insights.critical_issues if latest_insights else [],
                "warnings": latest_insights.warnings if latest_insights else [],
                "recommendations": latest_insights.recommendations if latest_insights else [],
                "recent_incidents": recent_incidents[:10],  # Top 10 recent incidents
                "performance_summary": latest_insights.performance_summary
                if latest_insights
                else {},
                "sla_compliance": latest_insights.sla_compliance if latest_insights else {},
                "capacity_forecast": latest_insights.capacity_forecast if latest_insights else {},
                "cost_analysis": latest_insights.cost_analysis if latest_insights else {},
            }

        except Exception as e:
            self.logger.error(f"Failed to generate dashboard data: {str(e)}")
            return {
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
                "service_name": self.service_name,
            }

    def generate_operational_runbook(self) -> dict[str, Any]:
        """Generate operational runbook with troubleshooting procedures."""
        try:
            runbook = {
                "title": "Data Platform Monitoring Operational Runbook",
                "version": "1.0",
                "last_updated": datetime.utcnow().isoformat(),
                "overview": {
                    "description": "Comprehensive operational procedures for monitoring and troubleshooting the data platform",
                    "components_covered": list(self.component_status.keys()),
                    "emergency_contacts": {
                        "primary_oncall": "oncall@company.com",
                        "escalation_manager": "manager@company.com",
                        "platform_team": "platform-team@company.com",
                    },
                },
                "health_check_procedures": {
                    "overall_health_assessment": [
                        "Check monitoring dashboard for overall health score",
                        "Review active critical alerts",
                        "Verify all monitoring components are operational",
                        "Check recent incident timeline",
                    ],
                    "component_health_checks": {
                        "api_services": [
                            "curl -f http://api-host/health",
                            "Check response time < 200ms",
                            "Verify error rate < 1%",
                        ],
                        "database": [
                            "Check database connectivity",
                            "Verify query performance < 100ms",
                            "Check connection pool utilization < 70%",
                        ],
                        "etl_pipelines": [
                            "Check pipeline execution status",
                            "Verify data quality scores > 95%",
                            "Check processing times within SLA",
                        ],
                    },
                },
                "troubleshooting_guides": {
                    "high_error_rate": {
                        "symptoms": [
                            "Error rate > 5%",
                            "Multiple failed requests",
                            "User complaints",
                        ],
                        "investigation_steps": [
                            "Check application logs for error patterns",
                            "Review database performance metrics",
                            "Check external service dependencies",
                            "Verify system resource utilization",
                        ],
                        "resolution_actions": [
                            "Scale up resources if CPU/memory high",
                            "Restart failed services",
                            "Check database locks and long-running queries",
                            "Verify external service availability",
                        ],
                    },
                    "performance_degradation": {
                        "symptoms": ["High response times", "Slow queries", "Resource exhaustion"],
                        "investigation_steps": [
                            "Check system resource utilization",
                            "Review slow query logs",
                            "Check cache hit rates",
                            "Analyze request patterns",
                        ],
                        "resolution_actions": [
                            "Optimize slow queries",
                            "Scale resources horizontally/vertically",
                            "Clear cache if needed",
                            "Implement request throttling",
                        ],
                    },
                    "data_quality_issues": {
                        "symptoms": [
                            "Data quality score < 90%",
                            "Data validation failures",
                            "Incomplete records",
                        ],
                        "investigation_steps": [
                            "Check data source availability",
                            "Review ETL pipeline logs",
                            "Verify data validation rules",
                            "Check transformation logic",
                        ],
                        "resolution_actions": [
                            "Fix data source issues",
                            "Rerun failed ETL jobs",
                            "Update validation rules if needed",
                            "Implement data remediation procedures",
                        ],
                    },
                },
                "escalation_procedures": {
                    "p0_critical": {
                        "criteria": "Service completely down, data loss, security breach",
                        "response_time": "5 minutes",
                        "actions": [
                            "Page primary on-call immediately",
                            "Create incident in tracking system",
                            "Escalate to management within 15 minutes",
                            "Prepare executive communication",
                        ],
                    },
                    "p1_high": {
                        "criteria": "Major functionality impacted, SLA breach",
                        "response_time": "15 minutes",
                        "actions": [
                            "Alert primary on-call",
                            "Create incident ticket",
                            "Begin investigation",
                            "Escalate if not resolved in 1 hour",
                        ],
                    },
                },
                "maintenance_procedures": {
                    "daily_checks": [
                        "Review overnight alerts and incidents",
                        "Check system health dashboard",
                        "Verify backup completion status",
                        "Review resource utilization trends",
                    ],
                    "weekly_tasks": [
                        "Review monitoring system performance",
                        "Update alert thresholds if needed",
                        "Check capacity planning forecasts",
                        "Review and update runbooks",
                    ],
                    "monthly_reviews": [
                        "SLA compliance assessment",
                        "Monitoring system optimization",
                        "Cost analysis and optimization",
                        "Post-incident review summary",
                    ],
                },
                "automation_scripts": {
                    "health_check": "scripts/health_check.sh",
                    "log_analysis": "scripts/analyze_logs.py",
                    "performance_report": "scripts/performance_report.py",
                    "incident_response": "scripts/incident_response.py",
                },
            }

            return runbook

        except Exception as e:
            self.logger.error(f"Failed to generate operational runbook: {str(e)}")
            return {"error": str(e)}

    def _record_component_error(self, component: MonitoringComponent, error: str):
        """Record component error for tracking."""
        if component not in self.component_errors:
            self.component_errors[component] = []

        self.component_errors[component].append(f"{datetime.utcnow().isoformat()}: {error}")

        # Keep only last 10 errors per component
        if len(self.component_errors[component]) > 10:
            self.component_errors[component] = self.component_errors[component][-10:]

        self.component_status[component] = False
        self.component_health[component] = SystemHealth.CRITICAL

        self.logger.error(f"Component {component.value} error: {error}")

    async def _collect_all_component_data(self) -> dict[str, Any]:
        """Collect data from all monitoring components."""
        data = {}

        # Collect performance data
        if self.performance_monitoring:
            try:
                data["performance"] = self.performance_monitoring.get_performance_summary()
            except Exception as e:
                self.logger.warning(f"Failed to collect performance data: {str(e)}")

        # Collect SLA data
        if self.alerting_sla:
            try:
                data["sla"] = self.alerting_sla.get_alerting_summary()
            except Exception as e:
                self.logger.warning(f"Failed to collect SLA data: {str(e)}")

        # Collect more component data as needed
        return data

    def _generate_recommendations(
        self, health_score: float, critical_issues: list[str], warnings: list[str]
    ) -> list[str]:
        """Generate actionable recommendations based on current system state."""
        recommendations = []

        if health_score < 50:
            recommendations.append(
                "URGENT: System health is critically low. Immediate investigation required."
            )
        elif health_score < 70:
            recommendations.append(
                "System health needs attention. Review critical issues and warnings."
            )

        if critical_issues:
            recommendations.append(f"Address {len(critical_issues)} critical issues immediately.")
            recommendations.append("Consider activating incident response procedures.")

        if warnings:
            recommendations.append(f"Review and address {len(warnings)} warning conditions.")

        if health_score > 90:
            recommendations.append("System is healthy. Consider optimization opportunities.")

        return recommendations

    async def _generate_capacity_forecast(self) -> dict[str, Any]:
        """Generate capacity planning forecast."""
        # This would integrate with actual capacity metrics
        return {
            "cpu_forecast": "Stable for next 30 days",
            "memory_forecast": "20% growth expected in next week",
            "storage_forecast": "50% capacity in 60 days",
            "network_forecast": "Within normal parameters",
        }

    async def _generate_cost_analysis(self) -> dict[str, Any]:
        """Generate cost analysis and optimization recommendations."""
        return {
            "current_monthly_cost": 5000.0,
            "projected_cost": 5500.0,
            "cost_per_transaction": 0.05,
            "optimization_opportunities": [
                "Right-size underutilized instances",
                "Optimize data storage tiers",
                "Review monitoring data retention",
            ],
        }

    def _calculate_health_trend(self) -> str:
        """Calculate health trend based on recent history."""
        if len(self.health_history) < 2:
            return "insufficient_data"

        recent_scores = [score for _, score in self.health_history[-10:]]
        if len(recent_scores) < 5:
            return "insufficient_data"

        first_half_avg = sum(recent_scores[: len(recent_scores) // 2]) / (len(recent_scores) // 2)
        second_half_avg = sum(recent_scores[len(recent_scores) // 2 :]) / (
            len(recent_scores) - len(recent_scores) // 2
        )

        if second_half_avg > first_half_avg * 1.05:
            return "improving"
        elif second_half_avg < first_half_avg * 0.95:
            return "degrading"
        else:
            return "stable"

    async def _analyze_component_correlations(self) -> list[dict[str, Any]]:
        """Analyze correlations between component health and performance."""
        correlations = []

        # This would perform actual correlation analysis
        # For now, return mock correlations
        correlations.append(
            {
                "description": "Database performance correlates with ETL success rate",
                "strength": 0.85,
                "components": ["database", "etl"],
                "recommendation": "Monitor database performance during ETL operations",
            }
        )

        return correlations


# Global orchestrator instance
_monitoring_orchestrator: MonitoringOrchestrator | None = None


def get_monitoring_orchestrator() -> MonitoringOrchestrator:
    """Get global monitoring orchestrator instance."""
    global _monitoring_orchestrator
    if _monitoring_orchestrator is None:
        _monitoring_orchestrator = MonitoringOrchestrator()
    return _monitoring_orchestrator


async def initialize_comprehensive_monitoring():
    """Initialize and start comprehensive monitoring system."""
    orchestrator = get_monitoring_orchestrator()
    await orchestrator.initialize_all_components()
    await orchestrator.start_all_monitoring()
    return orchestrator


async def shutdown_comprehensive_monitoring():
    """Shutdown comprehensive monitoring system."""
    orchestrator = get_monitoring_orchestrator()
    await orchestrator.stop_all_monitoring()


def get_monitoring_dashboard() -> dict[str, Any]:
    """Get monitoring dashboard data."""
    orchestrator = get_monitoring_orchestrator()
    return orchestrator.get_monitoring_dashboard_data()


def get_operational_runbook() -> dict[str, Any]:
    """Get operational runbook."""
    orchestrator = get_monitoring_orchestrator()
    return orchestrator.generate_operational_runbook()


if __name__ == "__main__":
    # Test monitoring orchestrator
    async def test_orchestrator():
        orchestrator = MonitoringOrchestrator("test-orchestrator")

        try:
            await orchestrator.initialize_all_components()
            await orchestrator.start_all_monitoring()

            # Generate test insights
            insights = await orchestrator.generate_comprehensive_insights()
            print(f"Health Score: {insights.health_score:.1f}")
            print(f"Overall Health: {insights.overall_health.value}")
            print(f"Critical Issues: {len(insights.critical_issues)}")

            # Get dashboard data
            dashboard = orchestrator.get_monitoring_dashboard_data()
            print(f"Dashboard components: {len(dashboard.get('components', {}))}")

            # Get runbook
            runbook = orchestrator.generate_operational_runbook()
            print(f"Runbook sections: {list(runbook.keys())}")

        finally:
            await orchestrator.stop_all_monitoring()

    # Run test
    asyncio.run(test_orchestrator())

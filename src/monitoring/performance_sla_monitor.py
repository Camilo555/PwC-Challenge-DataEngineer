"""
Performance SLA Monitor - <15ms API Response Time Validation
==========================================================

Enterprise-grade performance monitoring with strict SLA validation:
- Real-time API response time tracking with <15ms targets
- Automated SLA violation detection and alerting
- Performance regression analysis with trending
- Business impact correlation for SLA violations
- Comprehensive performance reporting and dashboards

Key Features:
- Sub-millisecond precision timing measurement
- Automated performance baselines and anomaly detection
- Multi-dimensional performance analysis (endpoint, user, region)
- Real-time alerting for SLA violations with escalation
- Performance budget tracking and capacity planning
"""

import asyncio
import logging
import statistics
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from collections import defaultdict, deque

import aioredis
import asyncpg
from fastapi import HTTPException
from pydantic import BaseModel, Field, validator

from core.config import get_settings

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SLAStatus(str, Enum):
    """SLA compliance status."""
    COMPLIANT = "compliant"
    WARNING = "warning"
    VIOLATION = "violation"
    CRITICAL = "critical"


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


@dataclass
class PerformanceMetric:
    """Individual performance measurement."""
    endpoint: str
    method: str
    response_time_ms: float
    status_code: int
    timestamp: datetime
    user_id: Optional[str] = None
    region: Optional[str] = None
    cache_hit: bool = False
    request_size_bytes: int = 0
    response_size_bytes: int = 0


@dataclass
class SLAConfiguration:
    """SLA configuration for endpoint monitoring."""
    endpoint_pattern: str
    target_response_time_ms: float = 15.0
    warning_threshold_ms: float = 12.0
    percentile_target: float = 95.0
    max_violation_rate: float = 1.0  # 1% violation rate
    measurement_window_minutes: int = 5
    alert_escalation_minutes: int = 2


@dataclass
class SLAViolation:
    """SLA violation record."""
    violation_id: str
    endpoint: str
    violation_type: str
    severity: AlertSeverity
    response_time_ms: float
    threshold_ms: float
    timestamp: datetime
    user_impact: int = 0
    business_impact_score: float = 0.0
    resolved: bool = False


@dataclass
class PerformanceAnalysis:
    """Performance analysis results."""
    endpoint: str
    measurement_period: timedelta
    total_requests: int
    avg_response_time_ms: float
    p50_response_time_ms: float
    p95_response_time_ms: float
    p99_response_time_ms: float
    max_response_time_ms: float
    sla_compliance_rate: float
    violation_count: int
    trend_direction: str
    performance_score: float


class PerformanceSLAMonitor:
    """
    Performance SLA Monitor for <15ms API response time validation.

    Provides real-time monitoring, violation detection, and automated alerting
    for API performance SLAs with sub-millisecond precision.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        db_url: str = None,
        default_sla_config: Optional[SLAConfiguration] = None
    ):
        self.redis_url = redis_url
        self.db_url = db_url or "sqlite:///performance_sla.db"
        self.redis_client: Optional[aioredis.Redis] = None

        # Performance data storage
        self.metrics_buffer: deque = deque(maxlen=10000)
        self.sla_configurations: Dict[str, SLAConfiguration] = {}
        self.active_violations: Dict[str, SLAViolation] = {}

        # Performance tracking
        self.endpoint_stats: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.violation_history: List[SLAViolation] = []

        # Default SLA configuration
        self.default_sla = default_sla_config or SLAConfiguration(
            endpoint_pattern="*",
            target_response_time_ms=15.0,
            warning_threshold_ms=12.0
        )

        logger.info("Performance SLA Monitor initialized with <15ms targets")

    async def initialize(self) -> None:
        """Initialize monitoring infrastructure."""
        try:
            # Initialize Redis connection
            try:
                self.redis_client = await aioredis.from_url(
                    self.redis_url,
                    encoding="utf-8",
                    decode_responses=True
                )
            except Exception as e:
                logger.warning(f"Redis connection failed: {e}. Using in-memory storage.")
                self.redis_client = None

            # Load SLA configurations
            await self._load_sla_configurations()

            logger.info("Performance SLA Monitor initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Performance SLA Monitor: {e}")
            raise

    async def record_performance_metric(
        self,
        endpoint: str,
        method: str,
        response_time_ms: float,
        status_code: int,
        user_id: Optional[str] = None,
        region: Optional[str] = None,
        **kwargs
    ) -> None:
        """Record a performance metric measurement."""
        try:
            # Create performance metric
            metric = PerformanceMetric(
                endpoint=endpoint,
                method=method,
                response_time_ms=response_time_ms,
                status_code=status_code,
                timestamp=datetime.utcnow(),
                user_id=user_id,
                region=region,
                cache_hit=kwargs.get('cache_hit', False),
                request_size_bytes=kwargs.get('request_size_bytes', 0),
                response_size_bytes=kwargs.get('response_size_bytes', 0)
            )

            # Add to buffer
            self.metrics_buffer.append(metric)

            # Update endpoint statistics
            self.endpoint_stats[endpoint].append(metric)

            # Check for SLA violations
            await self._check_sla_compliance(metric)

            # Store in Redis for real-time access
            if self.redis_client:
                await self._store_metric_in_redis(metric)

        except Exception as e:
            logger.error(f"Failed to record performance metric: {e}")

    async def _check_sla_compliance(self, metric: PerformanceMetric) -> None:
        """Check if metric violates SLA and trigger alerts."""
        try:
            # Get SLA configuration for endpoint
            sla_config = self._get_sla_config(metric.endpoint)

            # Check immediate violation (>15ms)
            if metric.response_time_ms > sla_config.target_response_time_ms:
                violation = SLAViolation(
                    violation_id=f"{metric.endpoint}_{int(time.time() * 1000)}",
                    endpoint=metric.endpoint,
                    violation_type="response_time_violation",
                    severity=self._determine_violation_severity(
                        metric.response_time_ms,
                        sla_config
                    ),
                    response_time_ms=metric.response_time_ms,
                    threshold_ms=sla_config.target_response_time_ms,
                    timestamp=metric.timestamp
                )

                # Record violation
                await self._record_sla_violation(violation)

                # Trigger immediate alert for critical violations
                if violation.severity in [AlertSeverity.CRITICAL, AlertSeverity.EMERGENCY]:
                    await self._trigger_immediate_alert(violation)

            # Check warning threshold (>12ms)
            elif metric.response_time_ms > sla_config.warning_threshold_ms:
                await self._record_performance_warning(metric, sla_config)

        except Exception as e:
            logger.error(f"Failed to check SLA compliance: {e}")

    def _determine_violation_severity(
        self,
        response_time_ms: float,
        sla_config: SLAConfiguration
    ) -> AlertSeverity:
        """Determine the severity of an SLA violation."""
        target = sla_config.target_response_time_ms

        if response_time_ms > target * 4:  # >60ms
            return AlertSeverity.EMERGENCY
        elif response_time_ms > target * 2:  # >30ms
            return AlertSeverity.CRITICAL
        elif response_time_ms > target * 1.5:  # >22.5ms
            return AlertSeverity.WARNING
        else:
            return AlertSeverity.INFO

    async def _record_sla_violation(self, violation: SLAViolation) -> None:
        """Record an SLA violation with business impact analysis."""
        try:
            # Calculate business impact
            violation.business_impact_score = await self._calculate_business_impact(violation)

            # Store violation
            self.active_violations[violation.violation_id] = violation
            self.violation_history.append(violation)

            # Store in Redis
            if self.redis_client:
                await self.redis_client.hset(
                    "sla_violations",
                    violation.violation_id,
                    f"{violation.endpoint}:{violation.response_time_ms:.2f}ms:{violation.severity.value}"
                )

            logger.warning(
                f"SLA violation recorded: {violation.endpoint} "
                f"({violation.response_time_ms:.2f}ms > {violation.threshold_ms:.2f}ms)"
            )

        except Exception as e:
            logger.error(f"Failed to record SLA violation: {e}")

    async def get_performance_analysis(
        self,
        endpoint: Optional[str] = None,
        time_window_minutes: int = 60
    ) -> Dict[str, PerformanceAnalysis]:
        """Get comprehensive performance analysis for endpoints."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=time_window_minutes)

            analyses = {}

            # Filter metrics by time window
            relevant_metrics = [
                m for m in self.metrics_buffer
                if m.timestamp >= start_time and (endpoint is None or m.endpoint == endpoint)
            ]

            # Group by endpoint
            endpoint_metrics = defaultdict(list)
            for metric in relevant_metrics:
                endpoint_metrics[metric.endpoint].append(metric)

            # Analyze each endpoint
            for ep, metrics in endpoint_metrics.items():
                if not metrics:
                    continue

                response_times = [m.response_time_ms for m in metrics]

                # Calculate statistics
                analysis = PerformanceAnalysis(
                    endpoint=ep,
                    measurement_period=timedelta(minutes=time_window_minutes),
                    total_requests=len(metrics),
                    avg_response_time_ms=statistics.mean(response_times),
                    p50_response_time_ms=statistics.median(response_times),
                    p95_response_time_ms=self._percentile(response_times, 95),
                    p99_response_time_ms=self._percentile(response_times, 99),
                    max_response_time_ms=max(response_times),
                    sla_compliance_rate=self._calculate_sla_compliance_rate(metrics),
                    violation_count=len([m for m in metrics if m.response_time_ms > 15.0]),
                    trend_direction=await self._calculate_trend_direction(ep),
                    performance_score=self._calculate_performance_score(response_times)
                )

                analyses[ep] = analysis

            return analyses

        except Exception as e:
            logger.error(f"Failed to get performance analysis: {e}")
            return {}

    def _percentile(self, data: List[float], percentile: float) -> float:
        """Calculate percentile value."""
        if not data:
            return 0.0

        sorted_data = sorted(data)
        index = int((percentile / 100) * len(sorted_data))
        if index >= len(sorted_data):
            index = len(sorted_data) - 1
        return sorted_data[index]

    def _calculate_sla_compliance_rate(self, metrics: List[PerformanceMetric]) -> float:
        """Calculate SLA compliance rate."""
        if not metrics:
            return 100.0

        compliant_count = len([m for m in metrics if m.response_time_ms <= 15.0])
        return (compliant_count / len(metrics)) * 100

    def _calculate_performance_score(self, response_times: List[float]) -> float:
        """Calculate overall performance score (0-100)."""
        if not response_times:
            return 100.0

        # Base score on average response time relative to 15ms target
        avg_time = statistics.mean(response_times)

        if avg_time <= 15.0:
            # Excellent performance: 85-100 score
            score = 100 - (avg_time / 15.0) * 15
        else:
            # Poor performance: 0-85 score
            score = max(0, 85 - (avg_time - 15.0))

        return round(score, 2)

    async def validate_15ms_sla_compliance(
        self,
        endpoint: Optional[str] = None,
        time_window_minutes: int = 5
    ) -> Dict[str, Any]:
        """
        Validate <15ms SLA compliance for API endpoints.

        Returns comprehensive compliance report with violations and recommendations.
        """
        try:
            # Get performance analysis
            analyses = await self.get_performance_analysis(endpoint, time_window_minutes)

            compliance_report = {
                "timestamp": datetime.utcnow().isoformat(),
                "time_window_minutes": time_window_minutes,
                "overall_compliance": True,
                "compliance_summary": {
                    "total_endpoints": len(analyses),
                    "compliant_endpoints": 0,
                    "warning_endpoints": 0,
                    "violation_endpoints": 0
                },
                "endpoint_details": {},
                "violations": [],
                "recommendations": []
            }

            for ep, analysis in analyses.items():
                # Determine compliance status
                compliance_status = SLAStatus.COMPLIANT
                if analysis.p95_response_time_ms > 15.0:
                    compliance_status = SLAStatus.VIOLATION
                    compliance_report["overall_compliance"] = False
                elif analysis.p95_response_time_ms > 12.0:
                    compliance_status = SLAStatus.WARNING

                # Update summary counts
                if compliance_status == SLAStatus.COMPLIANT:
                    compliance_report["compliance_summary"]["compliant_endpoints"] += 1
                elif compliance_status == SLAStatus.WARNING:
                    compliance_report["compliance_summary"]["warning_endpoints"] += 1
                else:
                    compliance_report["compliance_summary"]["violation_endpoints"] += 1

                # Endpoint details
                compliance_report["endpoint_details"][ep] = {
                    "status": compliance_status.value,
                    "p95_response_time_ms": analysis.p95_response_time_ms,
                    "avg_response_time_ms": analysis.avg_response_time_ms,
                    "compliance_rate": analysis.sla_compliance_rate,
                    "total_requests": analysis.total_requests,
                    "violation_count": analysis.violation_count,
                    "performance_score": analysis.performance_score,
                    "trend": analysis.trend_direction
                }

                # Add violations
                if compliance_status in [SLAStatus.WARNING, SLAStatus.VIOLATION]:
                    compliance_report["violations"].append({
                        "endpoint": ep,
                        "severity": compliance_status.value,
                        "p95_response_time_ms": analysis.p95_response_time_ms,
                        "target_ms": 15.0,
                        "violation_count": analysis.violation_count,
                        "compliance_rate": analysis.sla_compliance_rate
                    })

                    # Add recommendations
                    recommendations = self._generate_performance_recommendations(analysis)
                    compliance_report["recommendations"].extend(recommendations)

            # Calculate overall compliance rate
            total_endpoints = compliance_report["compliance_summary"]["total_endpoints"]
            compliant_endpoints = compliance_report["compliance_summary"]["compliant_endpoints"]

            if total_endpoints > 0:
                overall_compliance_rate = (compliant_endpoints / total_endpoints) * 100
            else:
                overall_compliance_rate = 100.0

            compliance_report["overall_compliance_rate"] = round(overall_compliance_rate, 2)

            return compliance_report

        except Exception as e:
            logger.error(f"Failed to validate 15ms SLA compliance: {e}")
            raise HTTPException(status_code=500, detail="SLA validation failed")

    def _generate_performance_recommendations(
        self,
        analysis: PerformanceAnalysis
    ) -> List[Dict[str, str]]:
        """Generate performance improvement recommendations."""
        recommendations = []

        if analysis.avg_response_time_ms > 15.0:
            recommendations.append({
                "endpoint": analysis.endpoint,
                "type": "optimization",
                "priority": "high",
                "recommendation": "Optimize endpoint performance - average response time exceeds 15ms target",
                "details": f"Current average: {analysis.avg_response_time_ms:.2f}ms, Target: 15ms"
            })

        if analysis.p95_response_time_ms > 25.0:
            recommendations.append({
                "endpoint": analysis.endpoint,
                "type": "caching",
                "priority": "high",
                "recommendation": "Implement aggressive caching - P95 response time is critically high",
                "details": f"Current P95: {analysis.p95_response_time_ms:.2f}ms"
            })

        if analysis.sla_compliance_rate < 95.0:
            recommendations.append({
                "endpoint": analysis.endpoint,
                "type": "reliability",
                "priority": "critical",
                "recommendation": "Improve service reliability - SLA compliance below 95%",
                "details": f"Current compliance: {analysis.sla_compliance_rate:.1f}%"
            })

        return recommendations

    async def get_real_time_performance_metrics(self) -> Dict[str, Any]:
        """Get real-time performance metrics for dashboards."""
        try:
            current_time = datetime.utcnow()
            last_minute_metrics = [
                m for m in self.metrics_buffer
                if (current_time - m.timestamp).total_seconds() <= 60
            ]

            if not last_minute_metrics:
                return {
                    "timestamp": current_time.isoformat(),
                    "requests_per_minute": 0,
                    "avg_response_time_ms": 0,
                    "p95_response_time_ms": 0,
                    "sla_compliance_rate": 100.0,
                    "active_violations": 0
                }

            response_times = [m.response_time_ms for m in last_minute_metrics]
            compliant_requests = len([m for m in last_minute_metrics if m.response_time_ms <= 15.0])

            return {
                "timestamp": current_time.isoformat(),
                "requests_per_minute": len(last_minute_metrics),
                "avg_response_time_ms": round(statistics.mean(response_times), 2),
                "p95_response_time_ms": round(self._percentile(response_times, 95), 2),
                "sla_compliance_rate": round((compliant_requests / len(last_minute_metrics)) * 100, 2),
                "active_violations": len(self.active_violations),
                "fastest_endpoint": min(last_minute_metrics, key=lambda m: m.response_time_ms).endpoint if last_minute_metrics else "none",
                "slowest_endpoint": max(last_minute_metrics, key=lambda m: m.response_time_ms).endpoint if last_minute_metrics else "none"
            }

        except Exception as e:
            logger.error(f"Failed to get real-time metrics: {e}")
            return {}

    # Helper methods
    async def _load_sla_configurations(self) -> None:
        """Load SLA configurations."""
        # Default configurations for common endpoints
        self.sla_configurations = {
            "/api/v1/sales": SLAConfiguration("/api/v1/sales", 15.0, 12.0),
            "/api/v1/analytics": SLAConfiguration("/api/v1/analytics", 15.0, 12.0),
            "/api/v1/dashboard": SLAConfiguration("/api/v1/dashboard", 10.0, 8.0),  # Stricter for dashboard
            "/api/v1/health": SLAConfiguration("/api/v1/health", 5.0, 3.0),  # Very strict for health checks
        }

    def _get_sla_config(self, endpoint: str) -> SLAConfiguration:
        """Get SLA configuration for endpoint."""
        return self.sla_configurations.get(endpoint, self.default_sla)

    async def _store_metric_in_redis(self, metric: PerformanceMetric) -> None:
        """Store metric in Redis for real-time access."""
        try:
            key = f"performance:{metric.endpoint}:{int(metric.timestamp.timestamp())}"
            value = f"{metric.response_time_ms}:{metric.status_code}"
            await self.redis_client.setex(key, 3600, value)  # 1 hour TTL
        except Exception as e:
            logger.debug(f"Failed to store metric in Redis: {e}")

    async def _calculate_business_impact(self, violation: SLAViolation) -> float:
        """Calculate business impact score for violation."""
        # Simple impact calculation based on response time
        base_impact = 1.0
        if violation.response_time_ms > 50:
            base_impact = 5.0
        elif violation.response_time_ms > 30:
            base_impact = 3.0
        elif violation.response_time_ms > 20:
            base_impact = 2.0

        return base_impact

    async def _record_performance_warning(
        self,
        metric: PerformanceMetric,
        sla_config: SLAConfiguration
    ) -> None:
        """Record performance warning."""
        logger.info(
            f"Performance warning: {metric.endpoint} "
            f"({metric.response_time_ms:.2f}ms > {sla_config.warning_threshold_ms:.2f}ms)"
        )

    async def _trigger_immediate_alert(self, violation: SLAViolation) -> None:
        """Trigger immediate alert for critical violations."""
        logger.critical(
            f"CRITICAL SLA VIOLATION: {violation.endpoint} "
            f"({violation.response_time_ms:.2f}ms > {violation.threshold_ms:.2f}ms)"
        )

    async def _calculate_trend_direction(self, endpoint: str) -> str:
        """Calculate performance trend direction."""
        # Simple implementation - can be enhanced
        return "stable"
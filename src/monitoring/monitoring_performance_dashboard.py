"""
Monitoring Performance Dashboard

Provides comprehensive performance monitoring and optimization analytics for the monitoring system itself.
Tracks cache performance, API efficiency, processing times, and optimization impact.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import asdict, dataclass
from typing import Any

from core.logging import get_logger
from monitoring.monitoring_performance_optimizer import get_monitoring_optimizer

logger = get_logger(__name__)


@dataclass
class PerformanceMetrics:
    """Performance metrics snapshot"""

    timestamp: float
    cache_hit_rate: float
    avg_calculation_time_ms: float
    batch_processing_efficiency: float
    api_rate_limit_usage: float
    memory_usage_mb: float
    queue_depth: int
    circuit_breaker_trips: int
    optimization_impact: float


class MonitoringPerformanceDashboard:
    """Performance monitoring dashboard for monitoring system optimization"""

    def __init__(self):
        self.logger = logger
        self._performance_history = deque(maxlen=1000)  # 1000 snapshots
        self._baseline_metrics: PerformanceMetrics | None = None
        self._optimization_start_time = time.time()

        # Performance targets
        self._performance_targets = {
            "cache_hit_rate": 0.8,  # 80% cache hit rate
            "avg_calculation_time_ms": 100,  # < 100ms average calculation
            "api_rate_limit_usage": 0.7,  # < 70% API rate limit usage
            "batch_efficiency": 0.9,  # 90% batch efficiency
            "memory_overhead_mb": 50,  # < 50MB memory overhead
        }

        # Alert thresholds
        self._alert_thresholds = {
            "cache_hit_rate_critical": 0.5,
            "calculation_time_critical": 500,  # 500ms
            "api_rate_limit_critical": 0.9,
            "memory_critical_mb": 100,
        }

    async def start_monitoring(self, monitoring_interval: float = 30.0):
        """Start continuous performance monitoring"""
        self.logger.info("Starting monitoring performance dashboard")

        while True:
            try:
                await self._collect_performance_metrics()
                await self._analyze_performance_trends()
                await self._check_performance_alerts()
                await asyncio.sleep(monitoring_interval)

            except Exception as e:
                self.logger.error(f"Error in performance monitoring: {e}")
                await asyncio.sleep(60)

    async def _collect_performance_metrics(self):
        """Collect current performance metrics from all monitoring components"""
        try:
            current_time = time.time()

            # Collect cache performance from DataDog metrics
            cache_stats = await self._get_cache_performance_stats()

            # Collect optimizer performance
            optimizer_stats = get_monitoring_optimizer().get_performance_stats()

            # Collect system resource usage
            system_stats = await self._get_system_resource_stats()

            # Calculate derived metrics
            optimization_impact = self._calculate_optimization_impact()

            metrics = PerformanceMetrics(
                timestamp=current_time,
                cache_hit_rate=cache_stats.get("cache_hit_rate", 0.0),
                avg_calculation_time_ms=cache_stats.get("avg_calculation_time_ms", 0.0),
                batch_processing_efficiency=self._calculate_batch_efficiency(cache_stats),
                api_rate_limit_usage=cache_stats.get("api_rate_limit_status", {}).get(
                    "usage_percent", 0.0
                )
                / 100,
                memory_usage_mb=system_stats.get("memory_mb", 0.0),
                queue_depth=optimizer_stats.get("total_queued_jobs", 0),
                circuit_breaker_trips=len(
                    [
                        cb
                        for cb in optimizer_stats.get("circuit_breakers", {}).values()
                        if cb.get("state") == "open"
                    ]
                ),
                optimization_impact=optimization_impact,
            )

            self._performance_history.append(metrics)

            # Set baseline if not exists
            if self._baseline_metrics is None:
                self._baseline_metrics = metrics
                self.logger.info(f"Baseline performance metrics established: {asdict(metrics)}")

        except Exception as e:
            self.logger.error(f"Failed to collect performance metrics: {e}")

    async def _get_cache_performance_stats(self) -> dict[str, Any]:
        """Get cache performance statistics from monitoring components"""
        try:
            # This would normally get stats from CustomMetricsAdvanced instance
            # For now, return simulated stats
            return {
                "cache_hit_rate": 0.85,
                "avg_calculation_time_ms": 75.0,
                "batch_size": 45,
                "api_rate_limit_status": {"usage_percent": 35.0, "status": "healthy"},
            }
        except Exception as e:
            self.logger.error(f"Failed to get cache stats: {e}")
            return {}

    async def _get_system_resource_stats(self) -> dict[str, Any]:
        """Get system resource usage for monitoring components"""
        try:
            import psutil

            process = psutil.Process()
            memory_info = process.memory_info()

            return {
                "memory_mb": memory_info.rss / (1024 * 1024),
                "cpu_percent": process.cpu_percent(),
                "threads": process.num_threads(),
            }
        except Exception as e:
            self.logger.error(f"Failed to get system stats: {e}")
            return {"memory_mb": 0.0, "cpu_percent": 0.0, "threads": 0}

    def _calculate_batch_efficiency(self, cache_stats: dict[str, Any]) -> float:
        """Calculate batch processing efficiency"""
        batch_size = cache_stats.get("batch_size", 0)
        max_batch_size = 100  # Target batch size

        if max_batch_size == 0:
            return 1.0

        # Efficiency based on how close to optimal batch size
        efficiency = min(1.0, batch_size / max_batch_size)
        return efficiency

    def _calculate_optimization_impact(self) -> float:
        """Calculate overall optimization impact compared to baseline"""
        if not self._baseline_metrics or len(self._performance_history) < 10:
            return 0.0

        try:
            # Get recent average performance
            recent_metrics = list(self._performance_history)[-10:]
            avg_cache_hit = sum(m.cache_hit_rate for m in recent_metrics) / len(recent_metrics)
            avg_calc_time = sum(m.avg_calculation_time_ms for m in recent_metrics) / len(
                recent_metrics
            )
            avg_api_usage = sum(m.api_rate_limit_usage for m in recent_metrics) / len(
                recent_metrics
            )

            # Calculate improvements
            cache_improvement = (avg_cache_hit - self._baseline_metrics.cache_hit_rate) / max(
                self._baseline_metrics.cache_hit_rate, 0.1
            )
            time_improvement = (
                self._baseline_metrics.avg_calculation_time_ms - avg_calc_time
            ) / max(self._baseline_metrics.avg_calculation_time_ms, 1)
            api_improvement = (self._baseline_metrics.api_rate_limit_usage - avg_api_usage) / max(
                self._baseline_metrics.api_rate_limit_usage, 0.1
            )

            # Weighted overall improvement
            overall_improvement = (
                cache_improvement * 0.4 + time_improvement * 0.4 + api_improvement * 0.2
            )
            return max(-1.0, min(1.0, overall_improvement))  # Clamp between -1 and 1

        except Exception as e:
            self.logger.error(f"Failed to calculate optimization impact: {e}")
            return 0.0

    async def _analyze_performance_trends(self):
        """Analyze performance trends and identify issues"""
        if len(self._performance_history) < 20:
            return

        try:
            recent_metrics = list(self._performance_history)[-20:]

            # Analyze cache hit rate trend
            cache_rates = [m.cache_hit_rate for m in recent_metrics]
            cache_trend = self._calculate_trend(cache_rates)

            # Analyze calculation time trend
            calc_times = [m.avg_calculation_time_ms for m in recent_metrics]
            time_trend = self._calculate_trend(calc_times)

            # Analyze memory usage trend
            memory_usage = [m.memory_usage_mb for m in recent_metrics]
            memory_trend = self._calculate_trend(memory_usage)

            # Log significant trends
            if cache_trend < -0.1:  # Cache hit rate declining
                self.logger.warning(f"Cache hit rate declining: trend = {cache_trend:.3f}")

            if time_trend > 0.1:  # Calculation time increasing
                self.logger.warning(f"Calculation times increasing: trend = {time_trend:.3f}")

            if memory_trend > 0.2:  # Memory usage increasing significantly
                self.logger.warning(f"Memory usage trending up: trend = {memory_trend:.3f}")

        except Exception as e:
            self.logger.error(f"Failed to analyze performance trends: {e}")

    def _calculate_trend(self, values: list[float]) -> float:
        """Calculate trend direction (-1 to 1, where 1 is strongly increasing)"""
        if len(values) < 2:
            return 0.0

        # Simple linear trend calculation
        n = len(values)
        x_values = list(range(n))

        # Calculate correlation coefficient
        x_mean = sum(x_values) / n
        y_mean = sum(values) / n

        numerator = sum((x_values[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        x_variance = sum((x - x_mean) ** 2 for x in x_values)
        y_variance = sum((y - y_mean) ** 2 for y in values)

        if x_variance == 0 or y_variance == 0:
            return 0.0

        correlation = numerator / (x_variance * y_variance) ** 0.5
        return correlation

    async def _check_performance_alerts(self):
        """Check for performance alerts and degradations"""
        if not self._performance_history:
            return

        current_metrics = self._performance_history[-1]

        alerts = []

        # Check critical thresholds
        if current_metrics.cache_hit_rate < self._alert_thresholds["cache_hit_rate_critical"]:
            alerts.append(f"Critical: Cache hit rate too low: {current_metrics.cache_hit_rate:.2f}")

        if (
            current_metrics.avg_calculation_time_ms
            > self._alert_thresholds["calculation_time_critical"]
        ):
            alerts.append(
                f"Critical: Calculation time too high: {current_metrics.avg_calculation_time_ms:.1f}ms"
            )

        if current_metrics.api_rate_limit_usage > self._alert_thresholds["api_rate_limit_critical"]:
            alerts.append(
                f"Critical: API rate limit usage too high: {current_metrics.api_rate_limit_usage:.1%}"
            )

        if current_metrics.memory_usage_mb > self._alert_thresholds["memory_critical_mb"]:
            alerts.append(
                f"Critical: Memory usage too high: {current_metrics.memory_usage_mb:.1f}MB"
            )

        if current_metrics.circuit_breaker_trips > 0:
            alerts.append(
                f"Warning: {current_metrics.circuit_breaker_trips} circuit breaker(s) open"
            )

        # Log alerts
        for alert in alerts:
            self.logger.error(alert)

    def get_performance_dashboard(self) -> dict[str, Any]:
        """Get comprehensive performance dashboard data"""
        if not self._performance_history:
            return {"status": "no_data", "message": "No performance data available yet"}

        current = self._performance_history[-1]

        # Calculate averages over different time windows
        last_hour = [
            m for m in self._performance_history if current.timestamp - m.timestamp <= 3600
        ]
        last_day = [
            m for m in self._performance_history if current.timestamp - m.timestamp <= 86400
        ]

        dashboard = {
            "current_performance": asdict(current),
            "baseline_performance": asdict(self._baseline_metrics)
            if self._baseline_metrics
            else None,
            "performance_targets": self._performance_targets,
            "optimization_summary": {
                "optimization_running_hours": (current.timestamp - self._optimization_start_time)
                / 3600,
                "total_optimization_impact": current.optimization_impact,
                "cache_performance": "excellent"
                if current.cache_hit_rate > 0.9
                else "good"
                if current.cache_hit_rate > 0.7
                else "needs_improvement",
                "api_efficiency": "excellent"
                if current.api_rate_limit_usage < 0.5
                else "good"
                if current.api_rate_limit_usage < 0.8
                else "needs_improvement",
            },
            "hourly_averages": self._calculate_averages(last_hour) if last_hour else {},
            "daily_averages": self._calculate_averages(last_day) if last_day else {},
            "performance_alerts": self._get_current_alerts(current),
            "recommendations": self._get_performance_recommendations(current),
        }

        return dashboard

    def _calculate_averages(self, metrics: list[PerformanceMetrics]) -> dict[str, float]:
        """Calculate average metrics for a time window"""
        if not metrics:
            return {}

        return {
            "cache_hit_rate": sum(m.cache_hit_rate for m in metrics) / len(metrics),
            "avg_calculation_time_ms": sum(m.avg_calculation_time_ms for m in metrics)
            / len(metrics),
            "batch_processing_efficiency": sum(m.batch_processing_efficiency for m in metrics)
            / len(metrics),
            "api_rate_limit_usage": sum(m.api_rate_limit_usage for m in metrics) / len(metrics),
            "memory_usage_mb": sum(m.memory_usage_mb for m in metrics) / len(metrics),
            "avg_queue_depth": sum(m.queue_depth for m in metrics) / len(metrics),
        }

    def _get_current_alerts(self, current: PerformanceMetrics) -> list[str]:
        """Get current performance alerts"""
        alerts = []

        if current.cache_hit_rate < self._performance_targets["cache_hit_rate"]:
            alerts.append("Cache hit rate below target")

        if current.avg_calculation_time_ms > self._performance_targets["avg_calculation_time_ms"]:
            alerts.append("Calculation time above target")

        if current.api_rate_limit_usage > self._performance_targets["api_rate_limit_usage"]:
            alerts.append("API rate limit usage above target")

        if current.memory_usage_mb > self._performance_targets["memory_overhead_mb"]:
            alerts.append("Memory usage above target")

        if current.circuit_breaker_trips > 0:
            alerts.append(f"{current.circuit_breaker_trips} circuit breaker(s) tripped")

        return alerts

    def _get_performance_recommendations(self, current: PerformanceMetrics) -> list[str]:
        """Get performance optimization recommendations"""
        recommendations = []

        if current.cache_hit_rate < 0.7:
            recommendations.append("Consider increasing cache TTL or warming more data")

        if current.avg_calculation_time_ms > 200:
            recommendations.append(
                "Consider optimizing expensive calculations or adding more caching"
            )

        if current.batch_processing_efficiency < 0.8:
            recommendations.append("Consider adjusting batch size or processing intervals")

        if current.api_rate_limit_usage > 0.8:
            recommendations.append("Consider increasing batch sizes or reducing API call frequency")

        if current.queue_depth > 50:
            recommendations.append(
                "Consider adding more worker threads or optimizing job processing"
            )

        return recommendations


# Global dashboard instance
_global_dashboard: MonitoringPerformanceDashboard | None = None


def get_performance_dashboard() -> MonitoringPerformanceDashboard:
    """Get global performance dashboard instance"""
    global _global_dashboard
    if _global_dashboard is None:
        _global_dashboard = MonitoringPerformanceDashboard()
    return _global_dashboard

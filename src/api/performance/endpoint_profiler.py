"""
Advanced API Endpoint Performance Profiler

Comprehensive performance profiling and bottleneck identification system
for enterprise-grade API monitoring and optimization.
"""

import asyncio
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict, deque
import statistics

from fastapi import Request, Response
from sqlalchemy import text
from pydantic import BaseModel

from core.logging import get_logger
from core.config.base_config import BaseConfig

logger = get_logger(__name__)
config = BaseConfig()


class BottleneckType(Enum):
    """Types of performance bottlenecks."""
    DATABASE_QUERY = "database_query"
    EXTERNAL_API = "external_api"
    CPU_INTENSIVE = "cpu_intensive"
    MEMORY_USAGE = "memory_usage"
    NETWORK_IO = "network_io"
    CACHE_MISS = "cache_miss"
    SERIALIZATION = "serialization"
    AUTHENTICATION = "authentication"


class SeverityLevel(Enum):
    """Performance issue severity levels."""
    LOW = "low"          # <100ms impact
    MEDIUM = "medium"    # 100-500ms impact
    HIGH = "high"        # 500-1000ms impact
    CRITICAL = "critical"  # >1000ms impact


@dataclass
class PerformanceProfile:
    """Detailed performance profile for an endpoint."""
    endpoint: str
    method: str
    total_time_ms: float
    database_time_ms: float
    external_api_time_ms: float
    serialization_time_ms: float
    cache_time_ms: float
    business_logic_time_ms: float
    memory_usage_mb: float
    cpu_usage_percent: float
    bottlenecks: List[Dict[str, Any]]
    timestamp: datetime
    request_id: str
    user_id: Optional[str] = None


@dataclass
class EndpointStats:
    """Statistical analysis for an endpoint."""
    endpoint: str
    method: str
    total_requests: int
    avg_response_time_ms: float
    median_response_time_ms: float
    p95_response_time_ms: float
    p99_response_time_ms: float
    error_rate_percent: float
    throughput_rps: float
    common_bottlenecks: List[Dict[str, Any]]
    optimization_suggestions: List[str]
    last_updated: datetime


class PerformanceProfiler:
    """Advanced performance profiler with bottleneck identification."""

    def __init__(self, max_profiles: int = 10000, analysis_window_minutes: int = 60):
        self.max_profiles = max_profiles
        self.analysis_window = timedelta(minutes=analysis_window_minutes)
        self.profiles: deque = deque(maxlen=max_profiles)
        self.endpoint_stats: Dict[str, EndpointStats] = {}
        self.bottleneck_patterns: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    async def profile_endpoint(self,
                             request: Request,
                             response: Response,
                             execution_context: Dict[str, Any]) -> PerformanceProfile:
        """
        Profile an endpoint execution and identify bottlenecks.

        Args:
            request: FastAPI request object
            response: FastAPI response object
            execution_context: Context with timing and resource usage data

        Returns:
            PerformanceProfile with detailed performance analysis
        """

        endpoint_key = f"{request.method}:{request.url.path}"

        # Extract timing data
        total_time = execution_context.get("total_time_ms", 0)
        database_time = execution_context.get("database_time_ms", 0)
        external_api_time = execution_context.get("external_api_time_ms", 0)
        serialization_time = execution_context.get("serialization_time_ms", 0)
        cache_time = execution_context.get("cache_time_ms", 0)
        business_logic_time = max(0, total_time - database_time - external_api_time -
                                serialization_time - cache_time)

        # Extract resource usage
        memory_usage = execution_context.get("memory_usage_mb", 0)
        cpu_usage = execution_context.get("cpu_usage_percent", 0)

        # Identify bottlenecks
        bottlenecks = await self._identify_bottlenecks({
            "total_time_ms": total_time,
            "database_time_ms": database_time,
            "external_api_time_ms": external_api_time,
            "serialization_time_ms": serialization_time,
            "cache_time_ms": cache_time,
            "business_logic_time_ms": business_logic_time,
            "memory_usage_mb": memory_usage,
            "cpu_usage_percent": cpu_usage,
            "endpoint": endpoint_key,
            "request_size_kb": len(str(request.query_params)) / 1024,
            "response_size_kb": len(getattr(response, 'body', b'')) / 1024,
        })

        profile = PerformanceProfile(
            endpoint=request.url.path,
            method=request.method,
            total_time_ms=total_time,
            database_time_ms=database_time,
            external_api_time_ms=external_api_time,
            serialization_time_ms=serialization_time,
            cache_time_ms=cache_time,
            business_logic_time_ms=business_logic_time,
            memory_usage_mb=memory_usage,
            cpu_usage_percent=cpu_usage,
            bottlenecks=bottlenecks,
            timestamp=datetime.utcnow(),
            request_id=execution_context.get("request_id", "unknown"),
            user_id=execution_context.get("user_id")
        )

        # Store profile for analysis
        self.profiles.append(profile)

        # Update endpoint statistics
        await self._update_endpoint_stats(profile)

        # Record bottleneck patterns
        for bottleneck in bottlenecks:
            self.bottleneck_patterns[endpoint_key].append(bottleneck)

        return profile

    async def _identify_bottlenecks(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify performance bottlenecks based on metrics."""

        bottlenecks = []
        total_time = metrics["total_time_ms"]

        # Database bottleneck detection
        db_time = metrics["database_time_ms"]
        if db_time > 100:  # >100ms database time
            severity = SeverityLevel.CRITICAL if db_time > 1000 else (
                SeverityLevel.HIGH if db_time > 500 else SeverityLevel.MEDIUM
            )
            bottlenecks.append({
                "type": BottleneckType.DATABASE_QUERY.value,
                "severity": severity.value,
                "impact_ms": db_time,
                "percentage_of_total": (db_time / max(total_time, 1)) * 100,
                "description": f"Database queries taking {db_time:.1f}ms",
                "suggestions": [
                    "Add database indexes for frequently queried columns",
                    "Optimize query execution plans",
                    "Consider query result caching",
                    "Use connection pooling",
                    "Implement read replicas for read-heavy operations"
                ]
            })

        # External API bottleneck detection
        external_time = metrics["external_api_time_ms"]
        if external_time > 200:  # >200ms external API time
            severity = SeverityLevel.CRITICAL if external_time > 2000 else (
                SeverityLevel.HIGH if external_time > 1000 else SeverityLevel.MEDIUM
            )
            bottlenecks.append({
                "type": BottleneckType.EXTERNAL_API.value,
                "severity": severity.value,
                "impact_ms": external_time,
                "percentage_of_total": (external_time / max(total_time, 1)) * 100,
                "description": f"External API calls taking {external_time:.1f}ms",
                "suggestions": [
                    "Implement circuit breaker pattern",
                    "Add response caching for external API calls",
                    "Use async/parallel API calls where possible",
                    "Consider API result pre-fetching",
                    "Implement timeout and retry policies"
                ]
            })

        # CPU intensive operation detection
        cpu_usage = metrics["cpu_usage_percent"]
        if cpu_usage > 80:  # >80% CPU usage
            severity = SeverityLevel.HIGH if cpu_usage > 95 else SeverityLevel.MEDIUM
            bottlenecks.append({
                "type": BottleneckType.CPU_INTENSIVE.value,
                "severity": severity.value,
                "impact_ms": 0,  # CPU is measured in percentage
                "cpu_usage_percent": cpu_usage,
                "description": f"High CPU usage: {cpu_usage:.1f}%",
                "suggestions": [
                    "Optimize algorithm complexity",
                    "Use async processing for CPU-intensive tasks",
                    "Implement result caching",
                    "Consider background job processing",
                    "Profile and optimize hot code paths"
                ]
            })

        # Memory usage detection
        memory_usage = metrics["memory_usage_mb"]
        if memory_usage > 100:  # >100MB memory usage
            severity = SeverityLevel.HIGH if memory_usage > 500 else SeverityLevel.MEDIUM
            bottlenecks.append({
                "type": BottleneckType.MEMORY_USAGE.value,
                "severity": severity.value,
                "impact_ms": 0,
                "memory_usage_mb": memory_usage,
                "description": f"High memory usage: {memory_usage:.1f}MB",
                "suggestions": [
                    "Optimize data structures and algorithms",
                    "Implement pagination for large datasets",
                    "Use streaming for large responses",
                    "Add garbage collection optimization",
                    "Consider lazy loading patterns"
                ]
            })

        # Serialization bottleneck detection
        serialization_time = metrics["serialization_time_ms"]
        if serialization_time > 50:  # >50ms serialization time
            severity = SeverityLevel.MEDIUM if serialization_time > 200 else SeverityLevel.LOW
            bottlenecks.append({
                "type": BottleneckType.SERIALIZATION.value,
                "severity": severity.value,
                "impact_ms": serialization_time,
                "percentage_of_total": (serialization_time / max(total_time, 1)) * 100,
                "description": f"Response serialization taking {serialization_time:.1f}ms",
                "suggestions": [
                    "Use more efficient serialization libraries",
                    "Pre-serialize commonly used responses",
                    "Implement response compression",
                    "Use streaming for large responses",
                    "Optimize data structure before serialization"
                ]
            })

        # Cache efficiency analysis
        cache_time = metrics["cache_time_ms"]
        if cache_time > 10:  # Cache operations should be very fast
            bottlenecks.append({
                "type": BottleneckType.CACHE_MISS.value,
                "severity": SeverityLevel.LOW.value,
                "impact_ms": cache_time,
                "description": f"Cache operations taking {cache_time:.1f}ms",
                "suggestions": [
                    "Optimize cache key strategies",
                    "Use local cache for frequently accessed data",
                    "Implement cache warming strategies",
                    "Review cache expiration policies",
                    "Consider cache hierarchies (L1, L2)"
                ]
            })

        return bottlenecks

    async def _update_endpoint_stats(self, profile: PerformanceProfile) -> None:
        """Update statistical analysis for an endpoint."""

        endpoint_key = f"{profile.method}:{profile.endpoint}"

        # Get recent profiles for this endpoint (last hour)
        recent_profiles = [
            p for p in self.profiles
            if (f"{p.method}:{p.endpoint}" == endpoint_key and
                datetime.utcnow() - p.timestamp < self.analysis_window)
        ]

        if not recent_profiles:
            return

        # Calculate statistics
        response_times = [p.total_time_ms for p in recent_profiles]
        error_count = len([p for p in recent_profiles if p.total_time_ms > 5000])  # >5s is error

        # Common bottlenecks analysis
        all_bottlenecks = []
        for p in recent_profiles:
            all_bottlenecks.extend(p.bottlenecks)

        bottleneck_counts = defaultdict(int)
        for bottleneck in all_bottlenecks:
            bottleneck_counts[bottleneck["type"]] += 1

        common_bottlenecks = [
            {
                "type": btype,
                "frequency": count,
                "percentage": (count / len(recent_profiles)) * 100
            }
            for btype, count in bottleneck_counts.most_common(5)
        ]

        # Generate optimization suggestions
        optimization_suggestions = self._generate_optimization_suggestions(
            recent_profiles, common_bottlenecks
        )

        # Update stats
        self.endpoint_stats[endpoint_key] = EndpointStats(
            endpoint=profile.endpoint,
            method=profile.method,
            total_requests=len(recent_profiles),
            avg_response_time_ms=statistics.mean(response_times),
            median_response_time_ms=statistics.median(response_times),
            p95_response_time_ms=statistics.quantiles(response_times, n=20)[18] if len(response_times) > 10 else max(response_times),
            p99_response_time_ms=statistics.quantiles(response_times, n=100)[98] if len(response_times) > 50 else max(response_times),
            error_rate_percent=(error_count / len(recent_profiles)) * 100,
            throughput_rps=len(recent_profiles) / (self.analysis_window.total_seconds() / 3600),
            common_bottlenecks=common_bottlenecks,
            optimization_suggestions=optimization_suggestions,
            last_updated=datetime.utcnow()
        )

    def _generate_optimization_suggestions(self,
                                         profiles: List[PerformanceProfile],
                                         common_bottlenecks: List[Dict[str, Any]]) -> List[str]:
        """Generate optimization suggestions based on analysis."""

        suggestions = []

        # Analyze average times
        avg_total = statistics.mean([p.total_time_ms for p in profiles])
        avg_db = statistics.mean([p.database_time_ms for p in profiles])
        avg_external = statistics.mean([p.external_api_time_ms for p in profiles])

        if avg_total > 1000:  # >1s average response time
            suggestions.append("CRITICAL: Average response time exceeds 1 second - immediate optimization required")
        elif avg_total > 500:
            suggestions.append("HIGH: Average response time exceeds 500ms - optimization recommended")

        if avg_db > avg_total * 0.4:  # DB takes >40% of total time
            suggestions.extend([
                "Database queries are the primary bottleneck (>40% of response time)",
                "Consider implementing database query optimization",
                "Add indexes for frequently queried columns",
                "Implement query result caching"
            ])

        if avg_external > avg_total * 0.3:  # External APIs take >30% of total time
            suggestions.extend([
                "External API calls are a significant bottleneck (>30% of response time)",
                "Implement response caching for external API calls",
                "Use async/parallel processing for multiple API calls",
                "Consider implementing circuit breaker pattern"
            ])

        # Analyze common bottlenecks
        for bottleneck in common_bottlenecks:
            if bottleneck["percentage"] > 50:  # Affects >50% of requests
                suggestions.append(f"FREQUENT ISSUE: {bottleneck['type']} affects {bottleneck['percentage']:.1f}% of requests")

        return suggestions[:10]  # Limit to top 10 suggestions

    def get_endpoint_analysis(self, endpoint: str, method: str = "GET") -> Optional[EndpointStats]:
        """Get detailed analysis for a specific endpoint."""
        endpoint_key = f"{method}:{endpoint}"
        return self.endpoint_stats.get(endpoint_key)

    def get_top_slow_endpoints(self, limit: int = 10) -> List[EndpointStats]:
        """Get endpoints with highest average response times."""
        return sorted(
            self.endpoint_stats.values(),
            key=lambda x: x.avg_response_time_ms,
            reverse=True
        )[:limit]

    def get_bottleneck_summary(self) -> Dict[str, Any]:
        """Get summary of all bottlenecks across endpoints."""

        all_bottlenecks = defaultdict(int)
        total_requests = 0

        for stats in self.endpoint_stats.values():
            total_requests += stats.total_requests
            for bottleneck in stats.common_bottlenecks:
                all_bottlenecks[bottleneck["type"]] += bottleneck["frequency"]

        return {
            "total_requests_analyzed": total_requests,
            "analysis_window_minutes": self.analysis_window.total_seconds() / 60,
            "bottleneck_distribution": dict(all_bottlenecks),
            "most_common_bottleneck": max(all_bottlenecks.items(), key=lambda x: x[1])[0] if all_bottlenecks else None,
            "endpoints_analyzed": len(self.endpoint_stats)
        }

    def get_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report."""

        if not self.endpoint_stats:
            return {"message": "No performance data available"}

        # Overall statistics
        all_avg_times = [stats.avg_response_time_ms for stats in self.endpoint_stats.values()]
        all_p95_times = [stats.p95_response_time_ms for stats in self.endpoint_stats.values()]
        all_error_rates = [stats.error_rate_percent for stats in self.endpoint_stats.values()]

        slow_endpoints = self.get_top_slow_endpoints(5)
        bottleneck_summary = self.get_bottleneck_summary()

        # Performance insights
        insights = []
        if statistics.mean(all_avg_times) > 500:
            insights.append("CRITICAL: Overall API performance needs improvement (avg >500ms)")

        if max(all_error_rates) > 5:
            insights.append("HIGH: Some endpoints have high error rates (>5%)")

        if bottleneck_summary["most_common_bottleneck"] == BottleneckType.DATABASE_QUERY.value:
            insights.append("Database optimization should be the top priority")

        return {
            "summary": {
                "endpoints_analyzed": len(self.endpoint_stats),
                "total_requests": bottleneck_summary["total_requests_analyzed"],
                "avg_response_time_ms": statistics.mean(all_avg_times),
                "p95_response_time_ms": statistics.mean(all_p95_times),
                "avg_error_rate_percent": statistics.mean(all_error_rates),
                "analysis_period_minutes": bottleneck_summary["analysis_window_minutes"]
            },
            "slow_endpoints": [asdict(endpoint) for endpoint in slow_endpoints],
            "bottleneck_analysis": bottleneck_summary,
            "performance_insights": insights,
            "optimization_priority": self._get_optimization_priority()
        }

    def _get_optimization_priority(self) -> List[Dict[str, Any]]:
        """Get prioritized optimization recommendations."""

        priorities = []

        for stats in self.endpoint_stats.values():
            # Calculate priority score based on multiple factors
            impact_score = (
                stats.avg_response_time_ms / 1000 +  # Response time impact
                stats.error_rate_percent / 10 +      # Error rate impact
                stats.throughput_rps / 100            # Traffic volume impact
            )

            priorities.append({
                "endpoint": f"{stats.method} {stats.endpoint}",
                "priority_score": impact_score,
                "avg_response_time_ms": stats.avg_response_time_ms,
                "error_rate_percent": stats.error_rate_percent,
                "requests_per_hour": stats.throughput_rps,
                "top_suggestion": stats.optimization_suggestions[0] if stats.optimization_suggestions else "No specific suggestions"
            })

        return sorted(priorities, key=lambda x: x["priority_score"], reverse=True)[:10]


# Global profiler instance
_global_profiler: Optional[PerformanceProfiler] = None


def get_profiler() -> PerformanceProfiler:
    """Get or create global performance profiler instance."""
    global _global_profiler
    if _global_profiler is None:
        _global_profiler = PerformanceProfiler()
    return _global_profiler


# Middleware integration helper
async def profile_request(request: Request, call_next: Callable, profiler: PerformanceProfiler) -> Response:
    """Profile a request using the performance profiler."""

    start_time = time.time()
    request_id = f"req_{int(time.time())}_{hash(str(request.url))}"

    # Execute request
    response = await call_next(request)

    # Calculate execution context
    total_time_ms = (time.time() - start_time) * 1000

    execution_context = {
        "request_id": request_id,
        "total_time_ms": total_time_ms,
        "database_time_ms": getattr(request.state, 'db_time_ms', 0),
        "external_api_time_ms": getattr(request.state, 'api_time_ms', 0),
        "cache_time_ms": getattr(request.state, 'cache_time_ms', 0),
        "serialization_time_ms": getattr(request.state, 'serialization_time_ms', 0),
        "memory_usage_mb": getattr(request.state, 'memory_mb', 0),
        "cpu_usage_percent": getattr(request.state, 'cpu_percent', 0),
        "user_id": getattr(request.state, 'user_id', None)
    }

    # Profile the request
    profile = await profiler.profile_endpoint(request, response, execution_context)

    # Add performance headers to response
    response.headers["X-Response-Time"] = f"{total_time_ms:.2f}ms"
    response.headers["X-Request-ID"] = request_id

    if profile.bottlenecks:
        response.headers["X-Performance-Warning"] = f"Detected {len(profile.bottlenecks)} bottlenecks"

    return response
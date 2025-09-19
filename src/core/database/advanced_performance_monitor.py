"""
Advanced Database Performance Monitor
====================================

Real-time database performance monitoring with ML-powered anomaly detection,
query optimization recommendations, and predictive performance analytics.
"""
from __future__ import annotations

import asyncio
import time
import statistics
import json
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Tuple, Union
import uuid
import hashlib

from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import Pool
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from core.logging import get_logger

logger = get_logger(__name__)


class PerformanceAlertLevel(Enum):
    """Performance alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class QueryComplexity(Enum):
    """Query complexity categories."""
    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"
    VERY_COMPLEX = "very_complex"


class PerformanceMetricType(Enum):
    """Types of performance metrics."""
    LATENCY = "latency"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"
    RESOURCE_USAGE = "resource_usage"
    CONNECTION_POOL = "connection_pool"
    LOCK_CONTENTION = "lock_contention"


@dataclass
class QueryMetrics:
    """Comprehensive query performance metrics."""
    query_id: str
    query_hash: str
    sql_text: str
    execution_time: float
    rows_examined: int = 0
    rows_returned: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    cpu_time: float = 0.0
    io_cost: float = 0.0
    memory_usage: int = 0
    lock_time: float = 0.0
    index_usage: Dict[str, int] = field(default_factory=dict)
    table_scans: int = 0
    joins_performed: int = 0
    temp_tables_created: int = 0
    sort_operations: int = 0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    error_message: Optional[str] = None
    execution_plan: Optional[Dict[str, Any]] = None


@dataclass
class PerformanceAlert:
    """Performance alert information."""
    alert_id: str
    alert_type: str
    level: PerformanceAlertLevel
    title: str
    description: str
    metric_value: float
    threshold_value: float
    timestamp: datetime
    query_id: Optional[str] = None
    recommendations: List[str] = field(default_factory=list)
    affected_tables: List[str] = field(default_factory=list)
    resolved: bool = False
    resolution_time: Optional[datetime] = None


@dataclass
class PerformanceThreshold:
    """Performance monitoring threshold."""
    metric_type: PerformanceMetricType
    threshold_value: float
    comparison_operator: str  # >, <, >=, <=, ==
    alert_level: PerformanceAlertLevel
    description: str
    enabled: bool = True


@dataclass
class DatabaseHealthScore:
    """Overall database health assessment."""
    overall_score: float  # 0-100
    performance_score: float
    availability_score: float
    efficiency_score: float
    security_score: float
    capacity_score: float
    timestamp: datetime
    detailed_metrics: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)


class QueryAnalyzer:
    """Analyzes SQL queries for performance characteristics."""

    def __init__(self):
        self.query_patterns = self._initialize_patterns()
        self.complexity_keywords = {
            QueryComplexity.SIMPLE: ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            QueryComplexity.MODERATE: ['JOIN', 'GROUP BY', 'ORDER BY', 'HAVING'],
            QueryComplexity.COMPLEX: ['SUBQUERY', 'UNION', 'EXISTS', 'WINDOW'],
            QueryComplexity.VERY_COMPLEX: ['RECURSIVE', 'CTE', 'PIVOT', 'UNPIVOT']
        }

    def analyze_query(self, sql_text: str) -> Dict[str, Any]:
        """Analyze SQL query for performance characteristics."""
        sql_upper = sql_text.upper()

        analysis = {
            "complexity": self._determine_complexity(sql_upper),
            "query_type": self._determine_query_type(sql_upper),
            "estimated_cost": self._estimate_query_cost(sql_upper),
            "potential_issues": self._identify_potential_issues(sql_upper),
            "optimization_hints": self._generate_optimization_hints(sql_upper),
            "tables_involved": self._extract_tables(sql_upper),
            "joins_count": sql_upper.count('JOIN'),
            "subqueries_count": sql_upper.count('(SELECT'),
            "aggregations_count": len([kw for kw in ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN'] if kw in sql_upper])
        }

        return analysis

    def _determine_complexity(self, sql: str) -> QueryComplexity:
        """Determine query complexity based on keywords and structure."""
        complexity_score = 0

        # Count complexity indicators
        for complexity, keywords in self.complexity_keywords.items():
            for keyword in keywords:
                complexity_score += sql.count(keyword)

        # Determine complexity level
        if complexity_score <= 2:
            return QueryComplexity.SIMPLE
        elif complexity_score <= 5:
            return QueryComplexity.MODERATE
        elif complexity_score <= 10:
            return QueryComplexity.COMPLEX
        else:
            return QueryComplexity.VERY_COMPLEX

    def _determine_query_type(self, sql: str) -> str:
        """Determine the primary query type."""
        if sql.strip().startswith('SELECT'):
            return 'SELECT'
        elif sql.strip().startswith('INSERT'):
            return 'INSERT'
        elif sql.strip().startswith('UPDATE'):
            return 'UPDATE'
        elif sql.strip().startswith('DELETE'):
            return 'DELETE'
        elif sql.strip().startswith('CREATE'):
            return 'CREATE'
        elif sql.strip().startswith('ALTER'):
            return 'ALTER'
        elif sql.strip().startswith('DROP'):
            return 'DROP'
        else:
            return 'UNKNOWN'

    def _estimate_query_cost(self, sql: str) -> float:
        """Estimate relative query cost."""
        base_cost = 1.0
        cost_factors = {
            'JOIN': 2.0,
            'SUBQUERY': 1.5,
            'GROUP BY': 1.3,
            'ORDER BY': 1.2,
            'DISTINCT': 1.4,
            'UNION': 1.5,
            'EXISTS': 1.3,
            'IN (SELECT': 2.0
        }

        total_cost = base_cost
        for factor, multiplier in cost_factors.items():
            total_cost *= (1 + sql.count(factor) * (multiplier - 1))

        return min(total_cost, 100.0)  # Cap at 100

    def _identify_potential_issues(self, sql: str) -> List[str]:
        """Identify potential performance issues."""
        issues = []

        if 'SELECT *' in sql:
            issues.append("Using SELECT * - consider selecting specific columns")

        if sql.count('JOIN') > 5:
            issues.append("High number of JOINs detected - consider query optimization")

        if 'ORDER BY' in sql and 'LIMIT' not in sql:
            issues.append("ORDER BY without LIMIT - may process unnecessary rows")

        if sql.count('(SELECT') > 3:
            issues.append("Multiple subqueries detected - consider using JOINs instead")

        if 'WHERE' not in sql and any(keyword in sql for keyword in ['SELECT', 'UPDATE', 'DELETE']):
            issues.append("Query without WHERE clause - may affect performance")

        if 'LIKE %' in sql:
            issues.append("Leading wildcard in LIKE - cannot use indexes effectively")

        return issues

    def _generate_optimization_hints(self, sql: str) -> List[str]:
        """Generate query optimization hints."""
        hints = []

        if 'SELECT *' in sql:
            hints.append("Replace SELECT * with specific column names")

        if sql.count('JOIN') > 0 and 'INDEX' not in sql:
            hints.append("Ensure proper indexes on JOIN columns")

        if 'GROUP BY' in sql:
            hints.append("Consider composite indexes for GROUP BY columns")

        if 'ORDER BY' in sql:
            hints.append("Create indexes on ORDER BY columns for better performance")

        if sql.count('(SELECT') > 0:
            hints.append("Consider rewriting subqueries as JOINs where possible")

        return hints

    def _extract_tables(self, sql: str) -> List[str]:
        """Extract table names from SQL query."""
        # Simplified table extraction - would need more sophisticated parsing in production
        tables = []
        words = sql.split()

        for i, word in enumerate(words):
            if word.upper() in ['FROM', 'JOIN', 'INTO', 'UPDATE'] and i + 1 < len(words):
                table_name = words[i + 1].replace(',', '').replace('(', '').replace(')', '')
                if table_name and not table_name.upper() in ['SELECT', 'WHERE', 'ON']:
                    tables.append(table_name.lower())

        return list(set(tables))

    def _initialize_patterns(self) -> Dict[str, Any]:
        """Initialize common query patterns for analysis."""
        return {
            "slow_patterns": [
                r"SELECT.*FROM.*WHERE.*LIKE\s+'%.*%'",
                r"SELECT.*FROM.*ORDER BY.*RAND\(\)",
                r"SELECT.*FROM.*WHERE.*!=",
                r"SELECT.*FROM.*WHERE.*OR.*OR"
            ],
            "index_hints": [
                r"WHERE\s+(\w+)\s*=",
                r"JOIN\s+\w+\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)",
                r"ORDER BY\s+(\w+)",
                r"GROUP BY\s+(\w+)"
            ]
        }


class AnomalyDetector:
    """ML-powered anomaly detection for database performance."""

    def __init__(self, sensitivity: float = 2.0):
        self.sensitivity = sensitivity
        self.baseline_metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.anomaly_history: List[Dict[str, Any]] = []

    def detect_anomalies(self, metrics: QueryMetrics) -> List[Dict[str, Any]]:
        """Detect performance anomalies using statistical methods."""
        anomalies = []

        # Check execution time anomaly
        execution_anomaly = self._detect_execution_time_anomaly(metrics)
        if execution_anomaly:
            anomalies.append(execution_anomaly)

        # Check resource usage anomaly
        resource_anomaly = self._detect_resource_anomaly(metrics)
        if resource_anomaly:
            anomalies.append(resource_anomaly)

        # Check throughput anomaly
        throughput_anomaly = self._detect_throughput_anomaly(metrics)
        if throughput_anomaly:
            anomalies.append(throughput_anomaly)

        # Record baseline metrics
        self._update_baseline(metrics)

        return anomalies

    def _detect_execution_time_anomaly(self, metrics: QueryMetrics) -> Optional[Dict[str, Any]]:
        """Detect execution time anomalies."""
        query_hash = metrics.query_hash
        execution_times = self.baseline_metrics[f"{query_hash}_execution_time"]

        if len(execution_times) < 10:  # Need baseline
            return None

        mean_time = statistics.mean(execution_times)
        std_time = statistics.stdev(execution_times) if len(execution_times) > 1 else 0

        # Z-score based anomaly detection
        if std_time > 0:
            z_score = (metrics.execution_time - mean_time) / std_time

            if abs(z_score) > self.sensitivity:
                return {
                    "type": "execution_time_anomaly",
                    "severity": "high" if abs(z_score) > 3 else "medium",
                    "z_score": z_score,
                    "current_value": metrics.execution_time,
                    "baseline_mean": mean_time,
                    "baseline_std": std_time,
                    "query_id": metrics.query_id
                }

        return None

    def _detect_resource_anomaly(self, metrics: QueryMetrics) -> Optional[Dict[str, Any]]:
        """Detect resource usage anomalies."""
        # Check memory usage anomaly
        if metrics.memory_usage > 0:
            memory_baseline = self.baseline_metrics["memory_usage"]

            if len(memory_baseline) >= 10:
                mean_memory = statistics.mean(memory_baseline)
                if metrics.memory_usage > mean_memory * 3:  # 3x normal usage
                    return {
                        "type": "memory_usage_anomaly",
                        "severity": "high",
                        "current_value": metrics.memory_usage,
                        "baseline_mean": mean_memory,
                        "query_id": metrics.query_id
                    }

        return None

    def _detect_throughput_anomaly(self, metrics: QueryMetrics) -> Optional[Dict[str, Any]]:
        """Detect throughput anomalies."""
        if metrics.rows_examined > 0 and metrics.execution_time > 0:
            throughput = metrics.rows_returned / metrics.execution_time
            throughput_baseline = self.baseline_metrics["throughput"]

            if len(throughput_baseline) >= 10:
                mean_throughput = statistics.mean(throughput_baseline)
                std_throughput = statistics.stdev(throughput_baseline) if len(throughput_baseline) > 1 else 0

                if std_throughput > 0:
                    z_score = (throughput - mean_throughput) / std_throughput

                    if z_score < -self.sensitivity:  # Lower throughput is problematic
                        return {
                            "type": "throughput_anomaly",
                            "severity": "medium",
                            "z_score": z_score,
                            "current_value": throughput,
                            "baseline_mean": mean_throughput,
                            "query_id": metrics.query_id
                        }

        return None

    def _update_baseline(self, metrics: QueryMetrics):
        """Update baseline metrics for future comparisons."""
        query_hash = metrics.query_hash

        self.baseline_metrics[f"{query_hash}_execution_time"].append(metrics.execution_time)
        self.baseline_metrics["memory_usage"].append(metrics.memory_usage)

        if metrics.rows_examined > 0 and metrics.execution_time > 0:
            throughput = metrics.rows_returned / metrics.execution_time
            self.baseline_metrics["throughput"].append(throughput)


class AdvancedPerformanceMonitor:
    """Advanced database performance monitor with comprehensive analytics."""

    def __init__(self, alert_callback: Optional[Callable] = None):
        self.query_analyzer = QueryAnalyzer()
        self.anomaly_detector = AnomalyDetector()
        self.alert_callback = alert_callback

        # Metrics storage
        self.query_metrics: deque = deque(maxlen=10000)
        self.performance_alerts: List[PerformanceAlert] = []
        self.health_history: deque = deque(maxlen=1000)

        # Thresholds
        self.thresholds: List[PerformanceThreshold] = self._initialize_thresholds()

        # Aggregated metrics
        self.aggregated_metrics: Dict[str, Any] = defaultdict(list)
        self.slow_queries: deque = deque(maxlen=100)

        # Monitoring state
        self.monitoring_enabled = True
        self.last_health_check = datetime.utcnow()

    def _initialize_thresholds(self) -> List[PerformanceThreshold]:
        """Initialize default performance thresholds."""
        return [
            PerformanceThreshold(
                metric_type=PerformanceMetricType.LATENCY,
                threshold_value=5.0,  # 5 seconds
                comparison_operator=">",
                alert_level=PerformanceAlertLevel.WARNING,
                description="Query execution time exceeds 5 seconds"
            ),
            PerformanceThreshold(
                metric_type=PerformanceMetricType.LATENCY,
                threshold_value=30.0,  # 30 seconds
                comparison_operator=">",
                alert_level=PerformanceAlertLevel.CRITICAL,
                description="Query execution time exceeds 30 seconds"
            ),
            PerformanceThreshold(
                metric_type=PerformanceMetricType.ERROR_RATE,
                threshold_value=0.05,  # 5%
                comparison_operator=">",
                alert_level=PerformanceAlertLevel.WARNING,
                description="Error rate exceeds 5%"
            ),
            PerformanceThreshold(
                metric_type=PerformanceMetricType.ERROR_RATE,
                threshold_value=0.10,  # 10%
                comparison_operator=">",
                alert_level=PerformanceAlertLevel.CRITICAL,
                description="Error rate exceeds 10%"
            )
        ]

    def record_query_metrics(self, metrics: QueryMetrics):
        """Record query performance metrics."""
        if not self.monitoring_enabled:
            return

        try:
            # Store metrics
            self.query_metrics.append(metrics)

            # Analyze query
            query_analysis = self.query_analyzer.analyze_query(metrics.sql_text)
            metrics.execution_plan = query_analysis

            # Check for slow queries
            if metrics.execution_time > 5.0:  # Slow query threshold
                self.slow_queries.append(metrics)

            # Detect anomalies
            anomalies = self.anomaly_detector.detect_anomalies(metrics)
            for anomaly in anomalies:
                self._create_anomaly_alert(anomaly, metrics)

            # Check thresholds
            self._check_thresholds(metrics)

            # Update aggregated metrics
            self._update_aggregated_metrics(metrics)

            logger.debug(f"Recorded query metrics: {metrics.query_id}")

        except Exception as e:
            logger.error(f"Error recording query metrics: {e}")

    def _create_anomaly_alert(self, anomaly: Dict[str, Any], metrics: QueryMetrics):
        """Create alert for detected anomaly."""
        alert = PerformanceAlert(
            alert_id=str(uuid.uuid4()),
            alert_type="anomaly",
            level=PerformanceAlertLevel.WARNING if anomaly["severity"] == "medium" else PerformanceAlertLevel.CRITICAL,
            title=f"Performance Anomaly: {anomaly['type']}",
            description=f"Detected {anomaly['type']} with z-score {anomaly.get('z_score', 'N/A')}",
            metric_value=anomaly["current_value"],
            threshold_value=anomaly.get("baseline_mean", 0),
            timestamp=datetime.utcnow(),
            query_id=metrics.query_id,
            recommendations=self._generate_anomaly_recommendations(anomaly, metrics)
        )

        self.performance_alerts.append(alert)

        if self.alert_callback:
            try:
                self.alert_callback(alert)
            except Exception as e:
                logger.error(f"Error in alert callback: {e}")

    def _check_thresholds(self, metrics: QueryMetrics):
        """Check metrics against defined thresholds."""
        for threshold in self.thresholds:
            if not threshold.enabled:
                continue

            metric_value = None

            if threshold.metric_type == PerformanceMetricType.LATENCY:
                metric_value = metrics.execution_time
            elif threshold.metric_type == PerformanceMetricType.ERROR_RATE:
                # Calculate recent error rate
                recent_queries = list(self.query_metrics)[-100:]  # Last 100 queries
                error_count = sum(1 for q in recent_queries if q.error_message)
                metric_value = error_count / len(recent_queries) if recent_queries else 0

            if metric_value is not None and self._evaluate_threshold(metric_value, threshold):
                self._create_threshold_alert(threshold, metric_value, metrics)

    def _evaluate_threshold(self, value: float, threshold: PerformanceThreshold) -> bool:
        """Evaluate if value exceeds threshold."""
        if threshold.comparison_operator == ">":
            return value > threshold.threshold_value
        elif threshold.comparison_operator == "<":
            return value < threshold.threshold_value
        elif threshold.comparison_operator == ">=":
            return value >= threshold.threshold_value
        elif threshold.comparison_operator == "<=":
            return value <= threshold.threshold_value
        elif threshold.comparison_operator == "==":
            return value == threshold.threshold_value

        return False

    def _create_threshold_alert(
        self, threshold: PerformanceThreshold, metric_value: float, metrics: QueryMetrics
    ):
        """Create alert for threshold violation."""
        alert = PerformanceAlert(
            alert_id=str(uuid.uuid4()),
            alert_type="threshold",
            level=threshold.alert_level,
            title=f"Threshold Violation: {threshold.metric_type.value}",
            description=threshold.description,
            metric_value=metric_value,
            threshold_value=threshold.threshold_value,
            timestamp=datetime.utcnow(),
            query_id=metrics.query_id,
            recommendations=self._generate_threshold_recommendations(threshold, metrics)
        )

        self.performance_alerts.append(alert)

        if self.alert_callback:
            try:
                self.alert_callback(alert)
            except Exception as e:
                logger.error(f"Error in alert callback: {e}")

    def _generate_anomaly_recommendations(
        self, anomaly: Dict[str, Any], metrics: QueryMetrics
    ) -> List[str]:
        """Generate recommendations for anomaly resolution."""
        recommendations = []

        if anomaly["type"] == "execution_time_anomaly":
            recommendations.extend([
                "Review query execution plan for optimization opportunities",
                "Check for missing or inefficient indexes",
                "Consider query rewriting or restructuring",
                "Monitor database load and resource usage"
            ])
        elif anomaly["type"] == "memory_usage_anomaly":
            recommendations.extend([
                "Optimize query to reduce memory consumption",
                "Consider adding LIMIT clauses to large result sets",
                "Review JOIN operations and temporary table usage",
                "Monitor available system memory"
            ])
        elif anomaly["type"] == "throughput_anomaly":
            recommendations.extend([
                "Analyze query execution plan for inefficiencies",
                "Check for table locks or blocking operations",
                "Consider index optimization",
                "Review database server performance"
            ])

        return recommendations

    def _generate_threshold_recommendations(
        self, threshold: PerformanceThreshold, metrics: QueryMetrics
    ) -> List[str]:
        """Generate recommendations for threshold violations."""
        recommendations = []

        if threshold.metric_type == PerformanceMetricType.LATENCY:
            recommendations.extend([
                "Optimize slow queries using EXPLAIN PLAN",
                "Add appropriate indexes for frequently accessed columns",
                "Consider query caching for repeated operations",
                "Review database configuration parameters"
            ])
        elif threshold.metric_type == PerformanceMetricType.ERROR_RATE:
            recommendations.extend([
                "Review application error handling",
                "Check database connectivity and timeouts",
                "Monitor database server health",
                "Investigate failing query patterns"
            ])

        return recommendations

    def _update_aggregated_metrics(self, metrics: QueryMetrics):
        """Update aggregated performance metrics."""
        current_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

        # Update hourly aggregations
        hour_key = current_hour.isoformat()

        if hour_key not in self.aggregated_metrics:
            self.aggregated_metrics[hour_key] = {
                "query_count": 0,
                "total_execution_time": 0.0,
                "error_count": 0,
                "slow_query_count": 0,
                "unique_queries": set()
            }

        hour_metrics = self.aggregated_metrics[hour_key]
        hour_metrics["query_count"] += 1
        hour_metrics["total_execution_time"] += metrics.execution_time

        if metrics.error_message:
            hour_metrics["error_count"] += 1

        if metrics.execution_time > 5.0:
            hour_metrics["slow_query_count"] += 1

        hour_metrics["unique_queries"].add(metrics.query_hash)

    def calculate_health_score(self) -> DatabaseHealthScore:
        """Calculate overall database health score."""
        current_time = datetime.utcnow()

        # Get recent metrics for scoring
        recent_queries = [q for q in self.query_metrics if (current_time - q.timestamp).total_seconds() < 3600]

        if not recent_queries:
            return DatabaseHealthScore(
                overall_score=50.0,
                performance_score=50.0,
                availability_score=50.0,
                efficiency_score=50.0,
                security_score=50.0,
                capacity_score=50.0,
                timestamp=current_time,
                recommendations=["Insufficient data for health assessment"]
            )

        # Calculate component scores
        performance_score = self._calculate_performance_score(recent_queries)
        availability_score = self._calculate_availability_score(recent_queries)
        efficiency_score = self._calculate_efficiency_score(recent_queries)
        capacity_score = self._calculate_capacity_score(recent_queries)
        security_score = 85.0  # Placeholder - would require security metrics

        # Calculate overall score (weighted average)
        overall_score = (
            performance_score * 0.3 +
            availability_score * 0.25 +
            efficiency_score * 0.25 +
            capacity_score * 0.15 +
            security_score * 0.05
        )

        health_score = DatabaseHealthScore(
            overall_score=overall_score,
            performance_score=performance_score,
            availability_score=availability_score,
            efficiency_score=efficiency_score,
            security_score=security_score,
            capacity_score=capacity_score,
            timestamp=current_time,
            detailed_metrics=self._get_detailed_health_metrics(recent_queries),
            recommendations=self._generate_health_recommendations(overall_score)
        )

        self.health_history.append(health_score)
        self.last_health_check = current_time

        return health_score

    def _calculate_performance_score(self, queries: List[QueryMetrics]) -> float:
        """Calculate performance component score."""
        if not queries:
            return 50.0

        # Average execution time
        avg_execution_time = statistics.mean(q.execution_time for q in queries)

        # Slow query ratio
        slow_queries = sum(1 for q in queries if q.execution_time > 5.0)
        slow_ratio = slow_queries / len(queries)

        # Score based on performance metrics
        time_score = max(0, 100 - (avg_execution_time * 10))  # Penalize long execution times
        slow_score = max(0, 100 - (slow_ratio * 200))  # Penalize high slow query ratio

        return (time_score + slow_score) / 2

    def _calculate_availability_score(self, queries: List[QueryMetrics]) -> float:
        """Calculate availability component score."""
        if not queries:
            return 50.0

        # Error rate
        error_count = sum(1 for q in queries if q.error_message)
        error_rate = error_count / len(queries)

        # Connection success rate (simplified)
        success_rate = 1.0 - error_rate

        return success_rate * 100

    def _calculate_efficiency_score(self, queries: List[QueryMetrics]) -> float:
        """Calculate efficiency component score."""
        if not queries:
            return 50.0

        # Resource utilization efficiency
        total_rows_examined = sum(q.rows_examined for q in queries if q.rows_examined > 0)
        total_rows_returned = sum(q.rows_returned for q in queries if q.rows_returned > 0)

        if total_rows_examined > 0:
            efficiency_ratio = total_rows_returned / total_rows_examined
            return min(efficiency_ratio * 100, 100)

        return 70.0  # Default score when no data

    def _calculate_capacity_score(self, queries: List[QueryMetrics]) -> float:
        """Calculate capacity component score."""
        if not queries:
            return 50.0

        # Memory usage assessment
        memory_usage = [q.memory_usage for q in queries if q.memory_usage > 0]

        if memory_usage:
            avg_memory = statistics.mean(memory_usage)
            max_memory = max(memory_usage)

            # Assume threshold of 1GB for warning
            memory_score = max(0, 100 - (avg_memory / (1024 * 1024 * 1024)) * 50)
            return memory_score

        return 80.0  # Default score

    def _get_detailed_health_metrics(self, queries: List[QueryMetrics]) -> Dict[str, Any]:
        """Get detailed health metrics."""
        if not queries:
            return {}

        return {
            "total_queries": len(queries),
            "avg_execution_time": statistics.mean(q.execution_time for q in queries),
            "max_execution_time": max(q.execution_time for q in queries),
            "error_rate": sum(1 for q in queries if q.error_message) / len(queries),
            "slow_query_count": sum(1 for q in queries if q.execution_time > 5.0),
            "unique_query_count": len(set(q.query_hash for q in queries)),
            "avg_rows_examined": statistics.mean(q.rows_examined for q in queries if q.rows_examined > 0) or 0,
            "avg_rows_returned": statistics.mean(q.rows_returned for q in queries if q.rows_returned > 0) or 0
        }

    def _generate_health_recommendations(self, overall_score: float) -> List[str]:
        """Generate recommendations based on health score."""
        recommendations = []

        if overall_score < 60:
            recommendations.extend([
                "Database performance is below acceptable levels",
                "Immediate investigation and optimization required",
                "Consider scaling database resources",
                "Review slow queries and optimize indexes"
            ])
        elif overall_score < 80:
            recommendations.extend([
                "Database performance has room for improvement",
                "Monitor slow queries and optimize where possible",
                "Consider preventive maintenance",
                "Review query patterns for optimization opportunities"
            ])
        else:
            recommendations.extend([
                "Database performance is good",
                "Continue monitoring for any degradation",
                "Consider proactive optimization for peak performance"
            ])

        return recommendations

    def get_performance_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get comprehensive performance summary."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        recent_queries = [q for q in self.query_metrics if q.timestamp >= cutoff_time]
        recent_alerts = [a for a in self.performance_alerts if a.timestamp >= cutoff_time]

        if not recent_queries:
            return {
                "period": f"Last {hours} hours",
                "no_data": True,
                "message": "No query data available for the specified period"
            }

        # Calculate summary statistics
        execution_times = [q.execution_time for q in recent_queries]
        error_count = sum(1 for q in recent_queries if q.error_message)

        summary = {
            "period": f"Last {hours} hours",
            "total_queries": len(recent_queries),
            "unique_queries": len(set(q.query_hash for q in recent_queries)),
            "avg_execution_time": statistics.mean(execution_times),
            "median_execution_time": statistics.median(execution_times),
            "max_execution_time": max(execution_times),
            "min_execution_time": min(execution_times),
            "error_count": error_count,
            "error_rate": error_count / len(recent_queries),
            "slow_query_count": sum(1 for q in recent_queries if q.execution_time > 5.0),
            "alert_count": len(recent_alerts),
            "critical_alerts": len([a for a in recent_alerts if a.level == PerformanceAlertLevel.CRITICAL]),
            "health_score": self.calculate_health_score().overall_score,
            "top_slow_queries": [
                {
                    "query_id": q.query_id,
                    "execution_time": q.execution_time,
                    "sql_text": q.sql_text[:200] + "..." if len(q.sql_text) > 200 else q.sql_text
                }
                for q in sorted(recent_queries, key=lambda x: x.execution_time, reverse=True)[:5]
            ],
            "recent_alerts": [
                {
                    "alert_id": a.alert_id,
                    "level": a.level.value,
                    "title": a.title,
                    "timestamp": a.timestamp.isoformat()
                }
                for a in recent_alerts[-10:]  # Last 10 alerts
            ]
        }

        return summary

    def get_query_recommendations(self, query_hash: str) -> List[str]:
        """Get optimization recommendations for a specific query."""
        # Find queries with this hash
        matching_queries = [q for q in self.query_metrics if q.query_hash == query_hash]

        if not matching_queries:
            return ["No data available for this query"]

        # Analyze query performance
        latest_query = matching_queries[-1]
        query_analysis = self.query_analyzer.analyze_query(latest_query.sql_text)

        recommendations = []
        recommendations.extend(query_analysis.get("optimization_hints", []))

        # Add performance-based recommendations
        avg_execution_time = statistics.mean(q.execution_time for q in matching_queries)

        if avg_execution_time > 10.0:
            recommendations.append("Query execution time is very high - consider major optimization")
        elif avg_execution_time > 5.0:
            recommendations.append("Query execution time is elevated - optimization recommended")

        if query_analysis.get("complexity") == QueryComplexity.VERY_COMPLEX:
            recommendations.append("Query is very complex - consider breaking into simpler operations")

        return recommendations


# Event listener setup for SQLAlchemy integration
def setup_performance_monitoring(engine: Engine, monitor: AdvancedPerformanceMonitor):
    """Set up SQLAlchemy event listeners for performance monitoring."""

    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        context._query_start_time = time.time()
        context._query_id = str(uuid.uuid4())

    @event.listens_for(engine, "after_cursor_execute")
    def receive_after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        if hasattr(context, '_query_start_time'):
            execution_time = time.time() - context._query_start_time
            query_id = getattr(context, '_query_id', str(uuid.uuid4()))

            # Create query hash
            query_hash = hashlib.md5(statement.encode()).hexdigest()

            # Create metrics object
            metrics = QueryMetrics(
                query_id=query_id,
                query_hash=query_hash,
                sql_text=statement,
                execution_time=execution_time,
                rows_returned=cursor.rowcount if cursor.rowcount > 0 else 0
            )

            # Record metrics
            monitor.record_query_metrics(metrics)

    @event.listens_for(engine, "dbapi_error")
    def receive_dbapi_error(exception, connection, cursor, statement, parameters, context, is_disconnect):
        query_id = getattr(context, '_query_id', str(uuid.uuid4()))
        execution_time = time.time() - getattr(context, '_query_start_time', time.time())

        # Create query hash
        query_hash = hashlib.md5(statement.encode()).hexdigest()

        # Create metrics object for failed query
        metrics = QueryMetrics(
            query_id=query_id,
            query_hash=query_hash,
            sql_text=statement,
            execution_time=execution_time,
            error_message=str(exception)
        )

        # Record metrics
        monitor.record_query_metrics(metrics)


# Factory function
def create_performance_monitor(alert_callback: Optional[Callable] = None) -> AdvancedPerformanceMonitor:
    """Create advanced performance monitor with default configuration."""
    return AdvancedPerformanceMonitor(alert_callback=alert_callback)
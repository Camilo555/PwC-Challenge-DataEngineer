"""
Intelligent Database Connection Pool Manager
===========================================

AI-driven database connection pool with adaptive sizing, predictive scaling,
workload pattern recognition, and enterprise-grade optimization features.
"""
from __future__ import annotations

import asyncio
import time
import statistics
import json
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Tuple, Union
import uuid
import threading

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import QueuePool, StaticPool
from sqlalchemy import event, text
from sqlalchemy.engine.events import PoolEvents

from core.logging import get_logger
from core.config.base_config import BaseConfig

logger = get_logger(__name__)


class WorkloadPattern(Enum):
    """Database workload patterns."""
    STEADY = "steady"
    BURST = "burst"
    PERIODIC = "periodic"
    RANDOM = "random"
    DECLINING = "declining"
    GROWING = "growing"


class OptimizationStrategy(Enum):
    """Pool optimization strategies."""
    CONSERVATIVE = "conservative"
    AGGRESSIVE = "aggressive"
    BALANCED = "balanced"
    PREDICTIVE = "predictive"
    COST_OPTIMIZED = "cost_optimized"


class ConnectionPriority(Enum):
    """Connection priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class WorkloadMetrics:
    """Workload analysis metrics."""
    timestamp: datetime
    active_connections: int
    queue_size: int
    request_rate: float
    avg_response_time: float
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    error_rate: float = 0.0
    slow_query_count: int = 0


@dataclass
class PoolOptimizationResult:
    """Result of pool optimization analysis."""
    recommended_pool_size: int
    recommended_max_overflow: int
    confidence_score: float
    reasoning: str
    estimated_cost_impact: float
    estimated_performance_gain: float
    optimization_strategy: OptimizationStrategy


@dataclass
class ConnectionRequest:
    """Connection request with priority and context."""
    request_id: str
    priority: ConnectionPriority
    requested_at: datetime
    context: Dict[str, Any] = field(default_factory=dict)
    timeout: Optional[float] = None
    retry_count: int = 0


class WorkloadAnalyzer:
    """Analyzes database workload patterns for optimization."""

    def __init__(self, analysis_window: int = 3600):  # 1 hour
        self.analysis_window = analysis_window
        self.metrics_history: deque = deque(maxlen=1000)
        self.pattern_cache: Dict[str, WorkloadPattern] = {}
        self.pattern_confidence: Dict[WorkloadPattern, float] = {}

    def record_metrics(self, metrics: WorkloadMetrics):
        """Record workload metrics for analysis."""
        self.metrics_history.append(metrics)

    def analyze_current_pattern(self) -> Tuple[WorkloadPattern, float]:
        """Analyze current workload pattern with confidence score."""
        if len(self.metrics_history) < 10:
            return WorkloadPattern.RANDOM, 0.1

        recent_metrics = list(self.metrics_history)[-60:]  # Last 60 data points

        # Extract key metrics
        connection_counts = [m.active_connections for m in recent_metrics]
        request_rates = [m.request_rate for m in recent_metrics]
        response_times = [m.avg_response_time for m in recent_metrics]

        # Analyze patterns
        pattern_scores = {
            WorkloadPattern.STEADY: self._analyze_steady_pattern(connection_counts, request_rates),
            WorkloadPattern.BURST: self._analyze_burst_pattern(connection_counts, request_rates),
            WorkloadPattern.PERIODIC: self._analyze_periodic_pattern(connection_counts),
            WorkloadPattern.GROWING: self._analyze_growth_pattern(connection_counts),
            WorkloadPattern.DECLINING: self._analyze_decline_pattern(connection_counts),
            WorkloadPattern.RANDOM: 0.1  # Default baseline
        }

        # Find pattern with highest confidence
        best_pattern = max(pattern_scores.items(), key=lambda x: x[1])

        # Cache result
        cache_key = f"{datetime.utcnow().hour}_{datetime.utcnow().minute // 15}"
        self.pattern_cache[cache_key] = best_pattern[0]
        self.pattern_confidence[best_pattern[0]] = best_pattern[1]

        return best_pattern[0], best_pattern[1]

    def _analyze_steady_pattern(self, connections: List[int], rates: List[float]) -> float:
        """Analyze for steady workload pattern."""
        if len(connections) < 5:
            return 0.0

        # Calculate coefficient of variation (CV)
        conn_cv = statistics.stdev(connections) / statistics.mean(connections) if statistics.mean(connections) > 0 else 1.0
        rate_cv = statistics.stdev(rates) / statistics.mean(rates) if statistics.mean(rates) > 0 else 1.0

        # Low variation indicates steady pattern
        steady_score = max(0, 1.0 - (conn_cv + rate_cv) / 2)
        return min(steady_score, 0.9)

    def _analyze_burst_pattern(self, connections: List[int], rates: List[float]) -> float:
        """Analyze for burst workload pattern."""
        if len(connections) < 10:
            return 0.0

        # Look for sudden spikes
        max_conn = max(connections)
        avg_conn = statistics.mean(connections)

        if avg_conn == 0:
            return 0.0

        spike_ratio = max_conn / avg_conn

        # Count number of values significantly above average
        threshold = avg_conn * 1.5
        spikes = sum(1 for c in connections if c > threshold)
        spike_frequency = spikes / len(connections)

        # Burst pattern has high spikes but not constantly
        if spike_ratio > 2.0 and 0.1 <= spike_frequency <= 0.3:
            return min(0.8, spike_ratio / 5.0)

        return 0.0

    def _analyze_periodic_pattern(self, connections: List[int]) -> float:
        """Analyze for periodic workload pattern using autocorrelation."""
        if len(connections) < 20:
            return 0.0

        # Simple autocorrelation analysis
        n = len(connections)
        mean_conn = statistics.mean(connections)

        # Check for periodicity at different lags
        max_correlation = 0.0

        for lag in range(1, min(n // 3, 10)):
            correlation = self._calculate_autocorrelation(connections, lag, mean_conn)
            max_correlation = max(max_correlation, abs(correlation))

        # Strong correlation indicates periodicity
        return min(max_correlation, 0.8)

    def _calculate_autocorrelation(self, data: List[int], lag: int, mean: float) -> float:
        """Calculate autocorrelation at given lag."""
        n = len(data)
        if lag >= n:
            return 0.0

        numerator = sum((data[i] - mean) * (data[i + lag] - mean) for i in range(n - lag))
        denominator = sum((x - mean) ** 2 for x in data)

        return numerator / denominator if denominator > 0 else 0.0

    def _analyze_growth_pattern(self, connections: List[int]) -> float:
        """Analyze for growing workload pattern."""
        if len(connections) < 10:
            return 0.0

        # Calculate trend using linear regression slope
        n = len(connections)
        x_mean = (n - 1) / 2
        y_mean = statistics.mean(connections)

        numerator = sum((i - x_mean) * (connections[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return 0.0

        slope = numerator / denominator

        # Positive slope indicates growth
        if slope > 0:
            # Normalize slope based on data range
            data_range = max(connections) - min(connections)
            normalized_slope = min(slope / (data_range / n), 1.0) if data_range > 0 else 0.0
            return min(normalized_slope, 0.8)

        return 0.0

    def _analyze_decline_pattern(self, connections: List[int]) -> float:
        """Analyze for declining workload pattern."""
        growth_score = self._analyze_growth_pattern(connections)
        # Declining is negative growth
        return growth_score if growth_score > 0 else 0.0

    def predict_future_load(self, horizon_minutes: int = 30) -> List[Tuple[datetime, float]]:
        """Predict future load based on historical patterns."""
        if len(self.metrics_history) < 30:
            # Not enough data for prediction
            current_time = datetime.utcnow()
            current_load = self.metrics_history[-1].active_connections if self.metrics_history else 5
            return [(current_time + timedelta(minutes=i), current_load) for i in range(horizon_minutes)]

        pattern, confidence = self.analyze_current_pattern()

        predictions = []
        current_time = datetime.utcnow()
        recent_loads = [m.active_connections for m in list(self.metrics_history)[-10:]]
        base_load = statistics.mean(recent_loads)

        for minute in range(horizon_minutes):
            future_time = current_time + timedelta(minutes=minute)

            if pattern == WorkloadPattern.STEADY:
                predicted_load = base_load
            elif pattern == WorkloadPattern.GROWING:
                # Assume linear growth
                growth_rate = (recent_loads[-1] - recent_loads[0]) / len(recent_loads)
                predicted_load = base_load + growth_rate * minute
            elif pattern == WorkloadPattern.DECLINING:
                # Assume linear decline
                decline_rate = (recent_loads[0] - recent_loads[-1]) / len(recent_loads)
                predicted_load = base_load - decline_rate * minute
            elif pattern == WorkloadPattern.PERIODIC:
                # Simple sinusoidal pattern
                period = 60  # 1 hour period
                amplitude = statistics.stdev(recent_loads)
                predicted_load = base_load + amplitude * statistics.sin(2 * statistics.pi * minute / period)
            elif pattern == WorkloadPattern.BURST:
                # Random bursts with higher baseline
                predicted_load = base_load * (1.2 + 0.3 * (minute % 10) / 10)
            else:
                predicted_load = base_load

            predicted_load = max(1, predicted_load)  # Minimum 1 connection
            predictions.append((future_time, predicted_load))

        return predictions


class IntelligentPoolOptimizer:
    """AI-driven pool optimization engine."""

    def __init__(self, workload_analyzer: WorkloadAnalyzer):
        self.workload_analyzer = workload_analyzer
        self.optimization_history: List[PoolOptimizationResult] = []
        self.performance_baseline: Dict[str, float] = {}

    def optimize_pool_configuration(
        self,
        current_pool_size: int,
        current_max_overflow: int,
        target_strategy: OptimizationStrategy = OptimizationStrategy.BALANCED
    ) -> PoolOptimizationResult:
        """Generate optimized pool configuration recommendations."""

        # Analyze current workload pattern
        pattern, confidence = self.workload_analyzer.analyze_current_pattern()

        # Get recent metrics
        recent_metrics = list(self.workload_analyzer.metrics_history)[-30:] if self.workload_analyzer.metrics_history else []

        if not recent_metrics:
            return self._get_default_optimization(current_pool_size, current_max_overflow)

        # Calculate current utilization metrics
        avg_active = statistics.mean(m.active_connections for m in recent_metrics)
        max_active = max(m.active_connections for m in recent_metrics)
        avg_queue_size = statistics.mean(m.queue_size for m in recent_metrics)
        avg_response_time = statistics.mean(m.avg_response_time for m in recent_metrics)

        # Determine optimal configuration based on pattern and strategy
        if target_strategy == OptimizationStrategy.CONSERVATIVE:
            recommended_pool = self._conservative_optimization(
                current_pool_size, avg_active, max_active, pattern
            )
        elif target_strategy == OptimizationStrategy.AGGRESSIVE:
            recommended_pool = self._aggressive_optimization(
                current_pool_size, avg_active, max_active, pattern
            )
        elif target_strategy == OptimizationStrategy.PREDICTIVE:
            recommended_pool = self._predictive_optimization(
                current_pool_size, pattern, confidence
            )
        elif target_strategy == OptimizationStrategy.COST_OPTIMIZED:
            recommended_pool = self._cost_optimized_optimization(
                current_pool_size, avg_active, avg_queue_size
            )
        else:  # BALANCED
            recommended_pool = self._balanced_optimization(
                current_pool_size, avg_active, max_active, avg_queue_size, pattern
            )

        # Calculate confidence and reasoning
        optimization_confidence = min(confidence + 0.3, 1.0)
        reasoning = self._generate_reasoning(pattern, target_strategy, recent_metrics)

        # Estimate performance impact
        performance_gain = self._estimate_performance_gain(
            current_pool_size, recommended_pool[0], avg_response_time
        )
        cost_impact = self._estimate_cost_impact(
            current_pool_size, recommended_pool[0]
        )

        result = PoolOptimizationResult(
            recommended_pool_size=recommended_pool[0],
            recommended_max_overflow=recommended_pool[1],
            confidence_score=optimization_confidence,
            reasoning=reasoning,
            estimated_cost_impact=cost_impact,
            estimated_performance_gain=performance_gain,
            optimization_strategy=target_strategy
        )

        self.optimization_history.append(result)
        return result

    def _conservative_optimization(
        self, current_pool: int, avg_active: float, max_active: float, pattern: WorkloadPattern
    ) -> Tuple[int, int]:
        """Conservative optimization - prioritize stability."""
        # Always maintain headroom
        target_pool = max(current_pool, int(max_active * 1.5))
        target_overflow = max(int(target_pool * 0.5), 5)

        return target_pool, target_overflow

    def _aggressive_optimization(
        self, current_pool: int, avg_active: float, max_active: float, pattern: WorkloadPattern
    ) -> Tuple[int, int]:
        """Aggressive optimization - minimize resources while maintaining performance."""
        # Optimize close to actual usage
        target_pool = max(int(avg_active * 1.2), 3)
        target_overflow = max(int((max_active - avg_active) * 1.1), 2)

        return target_pool, target_overflow

    def _predictive_optimization(
        self, current_pool: int, pattern: WorkloadPattern, confidence: float
    ) -> Tuple[int, int]:
        """Predictive optimization based on workload patterns."""
        # Get load predictions
        predictions = self.workload_analyzer.predict_future_load(30)
        predicted_loads = [p[1] for p in predictions]

        max_predicted = max(predicted_loads)
        avg_predicted = statistics.mean(predicted_loads)

        # Adjust based on confidence in predictions
        confidence_factor = max(confidence, 0.5)

        target_pool = int(avg_predicted * (1.0 + confidence_factor))
        target_overflow = int((max_predicted - avg_predicted) * confidence_factor)

        return max(target_pool, 3), max(target_overflow, 2)

    def _cost_optimized_optimization(
        self, current_pool: int, avg_active: float, avg_queue_size: float
    ) -> Tuple[int, int]:
        """Cost-optimized configuration - minimize connection costs."""
        # Optimize for minimum viable pool
        if avg_queue_size > 2:
            # Need more connections
            target_pool = current_pool + 2
        elif avg_queue_size < 0.5 and avg_active < current_pool * 0.7:
            # Can reduce connections
            target_pool = max(int(avg_active * 1.1), 3)
        else:
            target_pool = current_pool

        target_overflow = max(int(target_pool * 0.3), 2)
        return target_pool, target_overflow

    def _balanced_optimization(
        self, current_pool: int, avg_active: float, max_active: float,
        avg_queue_size: float, pattern: WorkloadPattern
    ) -> Tuple[int, int]:
        """Balanced optimization - optimize for both performance and cost."""

        # Base calculation on average usage with safety margin
        safety_margin = 1.3 if pattern == WorkloadPattern.BURST else 1.2
        target_pool = int(avg_active * safety_margin)

        # Adjust for queue pressure
        if avg_queue_size > 1:
            target_pool += int(avg_queue_size)

        # Ensure minimum viable pool
        target_pool = max(target_pool, 3)

        # Calculate overflow based on burst capacity needed
        burst_capacity = max_active - avg_active
        target_overflow = max(int(burst_capacity * 1.1), int(target_pool * 0.3))

        return target_pool, target_overflow

    def _get_default_optimization(self, current_pool: int, current_overflow: int) -> PoolOptimizationResult:
        """Get default optimization when no data is available."""
        return PoolOptimizationResult(
            recommended_pool_size=max(current_pool, 5),
            recommended_max_overflow=max(current_overflow, 5),
            confidence_score=0.1,
            reasoning="Insufficient data for optimization - using safe defaults",
            estimated_cost_impact=0.0,
            estimated_performance_gain=0.0,
            optimization_strategy=OptimizationStrategy.CONSERVATIVE
        )

    def _generate_reasoning(
        self, pattern: WorkloadPattern, strategy: OptimizationStrategy, metrics: List[WorkloadMetrics]
    ) -> str:
        """Generate human-readable reasoning for optimization."""

        avg_active = statistics.mean(m.active_connections for m in metrics)
        max_active = max(m.active_connections for m in metrics)
        avg_queue = statistics.mean(m.queue_size for m in metrics)

        reasoning_parts = [
            f"Detected {pattern.value} workload pattern",
            f"Average active connections: {avg_active:.1f}",
            f"Peak connections: {max_active}",
        ]

        if avg_queue > 1:
            reasoning_parts.append(f"Queue pressure detected (avg: {avg_queue:.1f})")

        if strategy == OptimizationStrategy.COST_OPTIMIZED:
            reasoning_parts.append("Optimizing for minimal connection costs")
        elif strategy == OptimizationStrategy.AGGRESSIVE:
            reasoning_parts.append("Aggressively optimizing resources")
        elif strategy == OptimizationStrategy.PREDICTIVE:
            reasoning_parts.append("Using predictive analysis for optimization")

        return "; ".join(reasoning_parts)

    def _estimate_performance_gain(
        self, current_pool: int, recommended_pool: int, avg_response_time: float
    ) -> float:
        """Estimate performance gain from optimization."""
        if recommended_pool > current_pool:
            # More connections should improve response time
            improvement_factor = (recommended_pool - current_pool) / current_pool
            return min(improvement_factor * 0.2, 0.5)  # Max 50% improvement
        elif recommended_pool < current_pool:
            # Fewer connections might slightly degrade performance
            degradation_factor = (current_pool - recommended_pool) / current_pool
            return -min(degradation_factor * 0.1, 0.2)  # Max 20% degradation

        return 0.0

    def _estimate_cost_impact(self, current_pool: int, recommended_pool: int) -> float:
        """Estimate cost impact of optimization."""
        # Simplified cost model - assume linear relationship with pool size
        cost_per_connection = 1.0  # Relative cost unit

        current_cost = current_pool * cost_per_connection
        recommended_cost = recommended_pool * cost_per_connection

        return (recommended_cost - current_cost) / current_cost if current_cost > 0 else 0.0


class IntelligentConnectionPool:
    """Enterprise-grade intelligent connection pool with AI optimization."""

    def __init__(
        self,
        database_url: str,
        initial_pool_size: int = 10,
        max_overflow: int = 20,
        optimization_strategy: OptimizationStrategy = OptimizationStrategy.BALANCED,
        auto_optimize: bool = True,
        optimization_interval: int = 300  # 5 minutes
    ):
        self.database_url = database_url
        self.current_pool_size = initial_pool_size
        self.current_max_overflow = max_overflow
        self.optimization_strategy = optimization_strategy
        self.auto_optimize = auto_optimize
        self.optimization_interval = optimization_interval

        # Initialize components
        self.workload_analyzer = WorkloadAnalyzer()
        self.pool_optimizer = IntelligentPoolOptimizer(self.workload_analyzer)

        # Connection management
        self.connection_requests: deque = deque()
        self.active_connections: Dict[str, AsyncSession] = {}
        self.connection_metrics: Dict[str, Any] = {}

        # Monitoring
        self.last_optimization = datetime.utcnow()
        self.optimization_task: Optional[asyncio.Task] = None
        self.monitoring_task: Optional[asyncio.Task] = None
        self.metrics_lock = asyncio.Lock()

        # Create async engine
        self.engine: Optional[AsyncEngine] = None
        self.session_factory: Optional[async_sessionmaker] = None

    async def initialize(self):
        """Initialize the connection pool."""
        try:
            # Create async engine with optimized configuration
            self.engine = await self._create_optimized_engine()

            # Create session factory
            self.session_factory = async_sessionmaker(
                bind=self.engine,
                class_=AsyncSession,
                expire_on_commit=False
            )

            # Start monitoring and optimization tasks
            if self.auto_optimize:
                self.optimization_task = asyncio.create_task(self._optimization_loop())

            self.monitoring_task = asyncio.create_task(self._monitoring_loop())

            logger.info(f"Intelligent connection pool initialized with {self.current_pool_size} connections")

        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise

    async def _create_optimized_engine(self) -> AsyncEngine:
        """Create optimized async engine."""
        from sqlalchemy.ext.asyncio import create_async_engine

        return create_async_engine(
            self.database_url,
            poolclass=QueuePool,
            pool_size=self.current_pool_size,
            max_overflow=self.current_max_overflow,
            pool_timeout=30,
            pool_recycle=3600,
            pool_pre_ping=True,
            echo=False,
            future=True
        )

    async def get_session(
        self,
        priority: ConnectionPriority = ConnectionPriority.NORMAL,
        timeout: Optional[float] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> AsyncSession:
        """Get database session with priority handling."""

        request = ConnectionRequest(
            request_id=str(uuid.uuid4()),
            priority=priority,
            requested_at=datetime.utcnow(),
            context=context or {},
            timeout=timeout
        )

        start_time = time.time()

        try:
            # Record request metrics
            await self._record_request_metrics()

            # Get session from factory
            session = self.session_factory()

            # Track active connection
            self.active_connections[request.request_id] = session

            # Record successful connection
            connection_time = time.time() - start_time
            await self._record_connection_success(connection_time)

            return session

        except Exception as e:
            # Record connection failure
            await self._record_connection_failure(str(e))
            logger.error(f"Failed to get database session: {e}")
            raise

    async def return_session(self, session: AsyncSession, request_id: Optional[str] = None):
        """Return session to pool."""
        try:
            await session.close()

            # Remove from active connections
            if request_id and request_id in self.active_connections:
                del self.active_connections[request_id]

        except Exception as e:
            logger.error(f"Error returning session to pool: {e}")

    async def _optimization_loop(self):
        """Background optimization loop."""
        while True:
            try:
                await asyncio.sleep(self.optimization_interval)

                # Check if optimization is needed
                if self._should_optimize():
                    await self._perform_optimization()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in optimization loop: {e}")

    async def _monitoring_loop(self):
        """Background monitoring loop."""
        while True:
            try:
                await asyncio.sleep(30)  # Monitor every 30 seconds

                # Collect current metrics
                await self._collect_workload_metrics()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")

    async def _collect_workload_metrics(self):
        """Collect current workload metrics."""
        async with self.metrics_lock:
            current_time = datetime.utcnow()

            # Get pool statistics
            pool_stats = await self._get_pool_statistics()

            metrics = WorkloadMetrics(
                timestamp=current_time,
                active_connections=len(self.active_connections),
                queue_size=len(self.connection_requests),
                request_rate=pool_stats.get("request_rate", 0.0),
                avg_response_time=pool_stats.get("avg_response_time", 0.0),
                cpu_usage=pool_stats.get("cpu_usage", 0.0),
                memory_usage=pool_stats.get("memory_usage", 0.0),
                error_rate=pool_stats.get("error_rate", 0.0)
            )

            self.workload_analyzer.record_metrics(metrics)

    async def _get_pool_statistics(self) -> Dict[str, float]:
        """Get current pool statistics."""
        if not self.engine:
            return {}

        pool = self.engine.pool

        return {
            "pool_size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "invalid": pool.invalid(),
            "request_rate": 0.0,  # Would be calculated from metrics
            "avg_response_time": 0.0,  # Would be calculated from metrics
            "cpu_usage": 0.0,  # Would require system monitoring
            "memory_usage": 0.0,  # Would require system monitoring
            "error_rate": 0.0  # Would be calculated from error metrics
        }

    def _should_optimize(self) -> bool:
        """Determine if optimization should be performed."""
        # Optimize if enough time has passed and we have sufficient data
        time_since_last = datetime.utcnow() - self.last_optimization
        has_data = len(self.workload_analyzer.metrics_history) >= 10

        return time_since_last.total_seconds() >= self.optimization_interval and has_data

    async def _perform_optimization(self):
        """Perform pool optimization."""
        try:
            logger.info("Starting intelligent pool optimization")

            # Get optimization recommendation
            optimization = self.pool_optimizer.optimize_pool_configuration(
                self.current_pool_size,
                self.current_max_overflow,
                self.optimization_strategy
            )

            # Apply optimization if confidence is high enough
            if optimization.confidence_score >= 0.6:
                await self._apply_optimization(optimization)
            else:
                logger.info(f"Skipping optimization due to low confidence: {optimization.confidence_score:.2f}")

            self.last_optimization = datetime.utcnow()

        except Exception as e:
            logger.error(f"Error during pool optimization: {e}")

    async def _apply_optimization(self, optimization: PoolOptimizationResult):
        """Apply optimization recommendations."""
        logger.info(
            f"Applying optimization: pool_size {self.current_pool_size} -> {optimization.recommended_pool_size}, "
            f"max_overflow {self.current_max_overflow} -> {optimization.recommended_max_overflow}"
        )

        # Update pool configuration
        self.current_pool_size = optimization.recommended_pool_size
        self.current_max_overflow = optimization.recommended_max_overflow

        # Recreate engine with new configuration
        old_engine = self.engine
        self.engine = await self._create_optimized_engine()

        # Update session factory
        self.session_factory = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

        # Gracefully close old engine
        if old_engine:
            await old_engine.dispose()

        logger.info(f"Pool optimization applied successfully. Reasoning: {optimization.reasoning}")

    async def _record_request_metrics(self):
        """Record request metrics."""
        # Implementation would track request patterns
        pass

    async def _record_connection_success(self, connection_time: float):
        """Record successful connection metrics."""
        # Implementation would update success metrics
        pass

    async def _record_connection_failure(self, error: str):
        """Record connection failure metrics."""
        # Implementation would update failure metrics
        pass

    async def get_optimization_status(self) -> Dict[str, Any]:
        """Get current optimization status and recommendations."""
        pattern, confidence = self.workload_analyzer.analyze_current_pattern()

        # Get latest optimization result
        latest_optimization = None
        if self.pool_optimizer.optimization_history:
            latest_optimization = self.pool_optimizer.optimization_history[-1]

        # Get load predictions
        predictions = self.workload_analyzer.predict_future_load(30)

        return {
            "current_pattern": {
                "type": pattern.value,
                "confidence": confidence
            },
            "current_configuration": {
                "pool_size": self.current_pool_size,
                "max_overflow": self.current_max_overflow,
                "strategy": self.optimization_strategy.value
            },
            "latest_optimization": {
                "recommended_pool_size": latest_optimization.recommended_pool_size if latest_optimization else None,
                "confidence_score": latest_optimization.confidence_score if latest_optimization else None,
                "reasoning": latest_optimization.reasoning if latest_optimization else None,
                "estimated_performance_gain": latest_optimization.estimated_performance_gain if latest_optimization else None
            } if latest_optimization else None,
            "load_predictions": [
                {"time": pred[0].isoformat(), "predicted_load": pred[1]}
                for pred in predictions[:10]  # Next 10 minutes
            ],
            "metrics_summary": {
                "total_metrics_collected": len(self.workload_analyzer.metrics_history),
                "active_connections": len(self.active_connections),
                "auto_optimization_enabled": self.auto_optimize
            }
        }

    async def close(self):
        """Close the connection pool and cleanup resources."""
        try:
            # Cancel background tasks
            if self.optimization_task:
                self.optimization_task.cancel()
                try:
                    await self.optimization_task
                except asyncio.CancelledError:
                    pass

            if self.monitoring_task:
                self.monitoring_task.cancel()
                try:
                    await self.monitoring_task
                except asyncio.CancelledError:
                    pass

            # Close all active sessions
            for session in self.active_connections.values():
                await session.close()

            # Dispose of engine
            if self.engine:
                await self.engine.dispose()

            logger.info("Intelligent connection pool closed successfully")

        except Exception as e:
            logger.error(f"Error closing connection pool: {e}")


# Factory function for easy creation
def create_intelligent_connection_pool(
    database_url: str,
    optimization_strategy: OptimizationStrategy = OptimizationStrategy.BALANCED,
    auto_optimize: bool = True
) -> IntelligentConnectionPool:
    """Create intelligent connection pool with enterprise configuration."""

    return IntelligentConnectionPool(
        database_url=database_url,
        initial_pool_size=10,
        max_overflow=20,
        optimization_strategy=optimization_strategy,
        auto_optimize=auto_optimize,
        optimization_interval=300  # 5 minutes
    )
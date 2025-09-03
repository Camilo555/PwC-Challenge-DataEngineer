"""
Monitoring System Performance Optimizer

Provides intelligent optimization for monitoring components including:
- Async processing for non-critical metrics
- Circuit breakers for expensive operations
- Intelligent sampling strategies
- Performance monitoring for monitoring system itself
- Batch processing optimization
"""

from __future__ import annotations

import asyncio
import functools
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from core.logging import get_logger

logger = get_logger(__name__)


class MetricPriority(Enum):
    """Metric calculation priority levels"""

    CRITICAL = "critical"  # Real-time, no delay
    HIGH = "high"  # < 5 second delay
    NORMAL = "normal"  # < 30 second delay
    LOW = "low"  # < 5 minute delay


@dataclass
class CircuitBreakerState:
    """Circuit breaker state for expensive operations"""

    failure_count: int = 0
    last_failure_time: float = 0
    state: str = "closed"  # closed, open, half-open
    failure_threshold: int = 5
    recovery_timeout: float = 60  # seconds


@dataclass
class MetricCalculationJob:
    """Job for async metric calculation"""

    metric_name: str
    calculation_func: Callable[[], Awaitable[Any]]
    priority: MetricPriority
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0
    max_retries: int = 3


class MonitoringPerformanceOptimizer:
    """Performance optimizer for monitoring system components"""

    def __init__(self, max_concurrent_jobs: int = 10, enable_circuit_breakers: bool = True):
        self.max_concurrent_jobs = max_concurrent_jobs
        self.enable_circuit_breakers = enable_circuit_breakers
        self.logger = logger

        # Async processing queues
        self._priority_queues: dict[MetricPriority, asyncio.Queue] = {
            priority: asyncio.Queue() for priority in MetricPriority
        }

        # Circuit breakers for expensive operations
        self._circuit_breakers: dict[str, CircuitBreakerState] = {}

        # Performance tracking
        self._processing_times = deque(maxlen=1000)
        self._queue_sizes = deque(maxlen=100)
        self._cache_performance = deque(maxlen=100)

        # Sampling configuration
        self._sampling_rates = {
            MetricPriority.CRITICAL: 1.0,  # No sampling
            MetricPriority.HIGH: 1.0,  # No sampling
            MetricPriority.NORMAL: 0.8,  # Sample 80%
            MetricPriority.LOW: 0.3,  # Sample 30%
        }

        # Worker tasks
        self._worker_tasks: list[asyncio.Task] = []
        self._running = False

    async def start(self):
        """Start the performance optimizer workers"""
        if self._running:
            return

        self._running = True
        self.logger.info("Starting monitoring performance optimizer")

        # Start priority-based workers
        for priority in MetricPriority:
            worker_count = self._get_worker_count_for_priority(priority)
            for i in range(worker_count):
                task = asyncio.create_task(
                    self._process_priority_queue(priority, f"{priority.value}-worker-{i}")
                )
                self._worker_tasks.append(task)

        # Start monitoring task
        monitor_task = asyncio.create_task(self._monitor_performance())
        self._worker_tasks.append(monitor_task)

        self.logger.info(f"Started {len(self._worker_tasks)} optimizer workers")

    async def stop(self):
        """Stop the performance optimizer"""
        if not self._running:
            return

        self._running = False
        self.logger.info("Stopping monitoring performance optimizer")

        # Cancel all worker tasks
        for task in self._worker_tasks:
            task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks.clear()

        self.logger.info("Monitoring performance optimizer stopped")

    async def submit_calculation(
        self,
        metric_name: str,
        calculation_func: Callable[[], Awaitable[Any]],
        priority: MetricPriority = MetricPriority.NORMAL,
    ) -> bool:
        """Submit a metric calculation job for async processing"""

        # Apply intelligent sampling
        if not self._should_process_metric(metric_name, priority):
            return False

        # Check if we should skip due to circuit breaker
        if self.enable_circuit_breakers and not self._can_process_metric(metric_name):
            self.logger.debug(f"Circuit breaker open for {metric_name}, skipping")
            return False

        job = MetricCalculationJob(
            metric_name=metric_name, calculation_func=calculation_func, priority=priority
        )

        try:
            await self._priority_queues[priority].put(job)
            return True
        except Exception as e:
            self.logger.error(f"Failed to submit calculation job for {metric_name}: {e}")
            return False

    def circuit_breaker(
        self, operation_name: str, failure_threshold: int = 5, recovery_timeout: float = 60
    ):
        """Decorator for circuit breaker pattern on expensive operations"""

        def decorator(func):
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                if not self.enable_circuit_breakers:
                    return await func(*args, **kwargs)

                breaker = self._get_circuit_breaker(
                    operation_name, failure_threshold, recovery_timeout
                )

                # Check circuit breaker state
                if breaker.state == "open":
                    if time.time() - breaker.last_failure_time > breaker.recovery_timeout:
                        breaker.state = "half-open"
                        self.logger.info(f"Circuit breaker for {operation_name} moved to half-open")
                    else:
                        raise Exception(f"Circuit breaker open for {operation_name}")

                try:
                    start_time = time.time()
                    result = await func(*args, **kwargs)

                    # Success - reset circuit breaker
                    if breaker.state == "half-open":
                        breaker.state = "closed"
                        breaker.failure_count = 0
                        self.logger.info(f"Circuit breaker for {operation_name} closed")

                    # Track performance
                    self._processing_times.append(time.time() - start_time)

                    return result

                except Exception as e:
                    # Handle failure
                    breaker.failure_count += 1
                    breaker.last_failure_time = time.time()

                    if breaker.failure_count >= breaker.failure_threshold:
                        breaker.state = "open"
                        self.logger.warning(
                            f"Circuit breaker opened for {operation_name} after {breaker.failure_count} failures"
                        )

                    raise e

            return wrapper

        return decorator

    def get_performance_stats(self) -> dict[str, Any]:
        """Get performance statistics for the optimizer"""
        queue_stats = {
            priority.value: self._priority_queues[priority].qsize() for priority in MetricPriority
        }

        circuit_breaker_stats = {
            name: {
                "state": breaker.state,
                "failure_count": breaker.failure_count,
                "last_failure": breaker.last_failure_time,
            }
            for name, breaker in self._circuit_breakers.items()
        }

        return {
            "queue_sizes": queue_stats,
            "total_queued_jobs": sum(queue_stats.values()),
            "avg_processing_time_ms": (
                sum(self._processing_times) / len(self._processing_times) * 1000
            )
            if self._processing_times
            else 0,
            "circuit_breakers": circuit_breaker_stats,
            "worker_count": len(self._worker_tasks),
            "running": self._running,
            "sampling_rates": {p.value: rate for p, rate in self._sampling_rates.items()},
        }

    async def _process_priority_queue(self, priority: MetricPriority, worker_name: str):
        """Process jobs from a priority queue"""
        queue = self._priority_queues[priority]

        while self._running:
            try:
                # Wait for job with timeout based on priority
                timeout = self._get_timeout_for_priority(priority)
                job = await asyncio.wait_for(queue.get(), timeout=timeout)

                await self._process_job(job, worker_name)

            except asyncio.TimeoutError:
                # No jobs available, continue
                continue
            except Exception as e:
                self.logger.error(f"Error in {worker_name}: {e}")
                await asyncio.sleep(1)

    async def _process_job(self, job: MetricCalculationJob, worker_name: str):
        """Process a single metric calculation job"""
        start_time = time.time()

        try:
            self.logger.debug(f"{worker_name} processing {job.metric_name}")

            # Execute the calculation
            await job.calculation_func()

            # Track success
            processing_time = time.time() - start_time
            self._processing_times.append(processing_time)

            self.logger.debug(
                f"{worker_name} completed {job.metric_name} in {processing_time:.3f}s"
            )

        except Exception as e:
            self.logger.error(f"{worker_name} failed to process {job.metric_name}: {e}")

            # Handle circuit breaker failure
            if self.enable_circuit_breakers:
                breaker = self._get_circuit_breaker(job.metric_name)
                breaker.failure_count += 1
                breaker.last_failure_time = time.time()

            # Retry if configured
            if job.retry_count < job.max_retries:
                job.retry_count += 1
                await self._priority_queues[job.priority].put(job)
                self.logger.info(
                    f"Retrying {job.metric_name} ({job.retry_count}/{job.max_retries})"
                )

    def _should_process_metric(self, metric_name: str, priority: MetricPriority) -> bool:
        """Determine if metric should be processed based on sampling rate"""
        sampling_rate = self._sampling_rates.get(priority, 1.0)

        # Critical metrics are always processed
        if priority == MetricPriority.CRITICAL:
            return True

        # Use hash-based sampling for consistency
        import hashlib

        hash_value = int(
            hashlib.md5(f"{metric_name}-{int(time.time() / 10)}".encode()).hexdigest(), 16
        )
        return (hash_value % 100) / 100 < sampling_rate

    def _can_process_metric(self, metric_name: str) -> bool:
        """Check if metric can be processed (circuit breaker check)"""
        if not self.enable_circuit_breakers:
            return True

        breaker = self._circuit_breakers.get(metric_name)
        if not breaker:
            return True

        if breaker.state == "open":
            # Check if recovery timeout has passed
            if time.time() - breaker.last_failure_time > breaker.recovery_timeout:
                breaker.state = "half-open"
                return True
            return False

        return True

    def _get_circuit_breaker(
        self, name: str, failure_threshold: int = 5, recovery_timeout: float = 60
    ) -> CircuitBreakerState:
        """Get or create circuit breaker for operation"""
        if name not in self._circuit_breakers:
            self._circuit_breakers[name] = CircuitBreakerState(
                failure_threshold=failure_threshold, recovery_timeout=recovery_timeout
            )
        return self._circuit_breakers[name]

    def _get_worker_count_for_priority(self, priority: MetricPriority) -> int:
        """Get number of workers for priority level"""
        if priority == MetricPriority.CRITICAL:
            return max(2, self.max_concurrent_jobs // 4)
        elif priority == MetricPriority.HIGH:
            return max(2, self.max_concurrent_jobs // 4)
        elif priority == MetricPriority.NORMAL:
            return max(1, self.max_concurrent_jobs // 3)
        else:  # LOW
            return 1

    def _get_timeout_for_priority(self, priority: MetricPriority) -> float:
        """Get queue timeout for priority level"""
        timeouts = {
            MetricPriority.CRITICAL: 1.0,
            MetricPriority.HIGH: 5.0,
            MetricPriority.NORMAL: 30.0,
            MetricPriority.LOW: 300.0,
        }
        return timeouts.get(priority, 30.0)

    async def _monitor_performance(self):
        """Monitor optimizer performance and adjust parameters"""
        while self._running:
            try:
                # Track queue sizes
                total_queued = sum(q.qsize() for q in self._priority_queues.values())
                self._queue_sizes.append(total_queued)

                # Adjust sampling rates based on queue pressure
                if total_queued > self.max_concurrent_jobs * 2:
                    # High pressure - reduce sampling for low priority
                    self._sampling_rates[MetricPriority.LOW] = max(
                        0.1, self._sampling_rates[MetricPriority.LOW] * 0.8
                    )
                    self._sampling_rates[MetricPriority.NORMAL] = max(
                        0.5, self._sampling_rates[MetricPriority.NORMAL] * 0.9
                    )
                elif total_queued < self.max_concurrent_jobs // 2:
                    # Low pressure - increase sampling
                    self._sampling_rates[MetricPriority.LOW] = min(
                        0.3, self._sampling_rates[MetricPriority.LOW] * 1.1
                    )
                    self._sampling_rates[MetricPriority.NORMAL] = min(
                        0.8, self._sampling_rates[MetricPriority.NORMAL] * 1.05
                    )

                # Log performance stats periodically
                if len(self._queue_sizes) % 12 == 0:  # Every 5 minutes
                    stats = self.get_performance_stats()
                    self.logger.info(f"Monitoring optimizer stats: {stats}")

                await asyncio.sleep(25)  # Check every 25 seconds

            except Exception as e:
                self.logger.error(f"Error in performance monitor: {e}")
                await asyncio.sleep(60)


# Global optimizer instance
_global_optimizer: MonitoringPerformanceOptimizer | None = None


def get_monitoring_optimizer() -> MonitoringPerformanceOptimizer:
    """Get global monitoring performance optimizer instance"""
    global _global_optimizer
    if _global_optimizer is None:
        _global_optimizer = MonitoringPerformanceOptimizer()
    return _global_optimizer


async def optimize_metric_calculation(
    metric_name: str,
    calculation_func: Callable[[], Awaitable[Any]],
    priority: MetricPriority = MetricPriority.NORMAL,
) -> bool:
    """Submit a metric calculation for optimized async processing"""
    optimizer = get_monitoring_optimizer()
    return await optimizer.submit_calculation(metric_name, calculation_func, priority)


def circuit_breaker(operation_name: str, failure_threshold: int = 5, recovery_timeout: float = 60):
    """Decorator for circuit breaker pattern on expensive operations"""
    optimizer = get_monitoring_optimizer()
    return optimizer.circuit_breaker(operation_name, failure_threshold, recovery_timeout)

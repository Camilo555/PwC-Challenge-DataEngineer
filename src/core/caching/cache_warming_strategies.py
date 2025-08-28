"""
Cache Warming and Invalidation Strategies

Advanced cache warming and invalidation strategies for optimal performance
across all cache types with intelligent scheduling and dependency management.
"""

import asyncio
import json
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable, Coroutine

from core.logging import get_logger
from core.caching.redis_cache_manager import RedisCacheManager, get_cache_manager
from core.caching.redis_streams import publish_cache_warming, publish_cache_invalidation, EventType

logger = get_logger(__name__)


class WarmingStrategy(Enum):
    """Cache warming strategies."""
    IMMEDIATE = "immediate"  # Warm cache immediately
    SCHEDULED = "scheduled"  # Warm cache on schedule
    PREDICTIVE = "predictive"  # Warm based on usage patterns
    LAZY = "lazy"  # Warm on first miss
    BATCH = "batch"  # Warm in batches
    PRIORITY_BASED = "priority_based"  # Warm high-priority items first


class InvalidationStrategy(Enum):
    """Cache invalidation strategies."""
    IMMEDIATE = "immediate"  # Invalidate immediately
    CASCADING = "cascading"  # Invalidate dependent caches
    TIME_BASED = "time_based"  # Invalidate based on TTL
    EVENT_DRIVEN = "event_driven"  # Invalidate on specific events
    DEPENDENCY_BASED = "dependency_based"  # Invalidate based on dependencies
    SMART_INVALIDATION = "smart_invalidation"  # Use ML to predict what to invalidate


class WarmingPriority(Enum):
    """Cache warming priorities."""
    CRITICAL = "critical"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"
    BACKGROUND = "background"


@dataclass
class WarmingTask:
    """Cache warming task definition."""
    task_id: str
    cache_key: str
    cache_namespace: str
    loader_function: Callable
    loader_params: Dict[str, Any]
    priority: WarmingPriority
    strategy: WarmingStrategy
    scheduled_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    retry_count: int = 0
    max_retries: int = 3
    dependencies: List[str] = None
    tags: List[str] = None
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        if self.tags is None:
            self.tags = []


@dataclass
class InvalidationRule:
    """Cache invalidation rule definition."""
    rule_id: str
    name: str
    strategy: InvalidationStrategy
    trigger_patterns: List[str]
    target_patterns: List[str]
    conditions: Dict[str, Any]
    priority: int = 0
    enabled: bool = True
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()


class BaseWarmingEngine(ABC):
    """Base class for cache warming engines."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None):
        self.cache_manager = cache_manager
        
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    @abstractmethod
    async def warm_cache(self, task: WarmingTask) -> bool:
        """Warm cache based on task definition."""
        pass
    
    @abstractmethod
    async def get_warming_candidates(self, **kwargs) -> List[WarmingTask]:
        """Get candidates for cache warming."""
        pass


class PredictiveWarmingEngine(BaseWarmingEngine):
    """Predictive cache warming based on usage patterns."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None):
        super().__init__(cache_manager)
        self.usage_tracker = {}  # Track access patterns
        self.prediction_threshold = 0.7  # Confidence threshold for predictions
        
    async def warm_cache(self, task: WarmingTask) -> bool:
        """Warm cache using predictive strategy."""
        try:
            cache_manager = await self._get_cache_manager()
            
            # Check if already cached
            if await cache_manager.exists(task.cache_key, task.cache_namespace):
                return True
            
            # Execute loader function
            if asyncio.iscoroutinefunction(task.loader_function):
                result = await task.loader_function(**task.loader_params)
            else:
                result = task.loader_function(**task.loader_params)
            
            if result is not None:
                # Calculate TTL based on prediction confidence
                ttl = self._calculate_predictive_ttl(task)
                
                success = await cache_manager.set(
                    task.cache_key, result, ttl, task.cache_namespace
                )
                
                if success:
                    logger.debug(f"Predictively warmed cache: {task.cache_key}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Predictive warming error for task {task.task_id}: {e}")
            return False
    
    def _calculate_predictive_ttl(self, task: WarmingTask) -> int:
        """Calculate TTL based on prediction confidence."""
        # Default TTL
        base_ttl = 3600  # 1 hour
        
        # Adjust based on priority
        priority_multipliers = {
            WarmingPriority.CRITICAL: 2.0,
            WarmingPriority.HIGH: 1.5,
            WarmingPriority.NORMAL: 1.0,
            WarmingPriority.LOW: 0.7,
            WarmingPriority.BACKGROUND: 0.5
        }
        
        multiplier = priority_multipliers.get(task.priority, 1.0)
        return int(base_ttl * multiplier)
    
    async def get_warming_candidates(self, **kwargs) -> List[WarmingTask]:
        """Get candidates for predictive warming."""
        candidates = []
        
        # This would analyze usage patterns and predict what needs warming
        # For now, return empty list - would be implemented based on specific needs
        
        return candidates


class ScheduledWarmingEngine(BaseWarmingEngine):
    """Scheduled cache warming at specific times."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None):
        super().__init__(cache_manager)
        self.scheduled_tasks: List[WarmingTask] = []
        
    async def warm_cache(self, task: WarmingTask) -> bool:
        """Warm cache using scheduled strategy."""
        try:
            # Check if it's time to warm
            if task.scheduled_at and datetime.utcnow() < task.scheduled_at:
                return False
            
            cache_manager = await self._get_cache_manager()
            
            # Execute loader function
            if asyncio.iscoroutinefunction(task.loader_function):
                result = await task.loader_function(**task.loader_params)
            else:
                result = task.loader_function(**task.loader_params)
            
            if result is not None:
                ttl = self._calculate_scheduled_ttl(task)
                
                success = await cache_manager.set(
                    task.cache_key, result, ttl, task.cache_namespace
                )
                
                if success:
                    logger.info(f"Scheduled cache warming completed: {task.cache_key}")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Scheduled warming error for task {task.task_id}: {e}")
            return False
    
    def _calculate_scheduled_ttl(self, task: WarmingTask) -> int:
        """Calculate TTL for scheduled warming."""
        if task.expires_at:
            return int((task.expires_at - datetime.utcnow()).total_seconds())
        return 3600  # Default 1 hour
    
    async def get_warming_candidates(self, **kwargs) -> List[WarmingTask]:
        """Get scheduled warming candidates."""
        current_time = datetime.utcnow()
        
        candidates = [
            task for task in self.scheduled_tasks
            if task.scheduled_at and current_time >= task.scheduled_at
        ]
        
        return candidates


class BatchWarmingEngine(BaseWarmingEngine):
    """Batch cache warming for efficient resource usage."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None,
                 batch_size: int = 50):
        super().__init__(cache_manager)
        self.batch_size = batch_size
        
    async def warm_cache_batch(self, tasks: List[WarmingTask]) -> Dict[str, bool]:
        """Warm multiple cache entries in batch."""
        results = {}
        
        # Group tasks by loader function for efficiency
        grouped_tasks = defaultdict(list)
        for task in tasks:
            grouped_tasks[task.loader_function].append(task)
        
        for loader_func, task_group in grouped_tasks.items():
            batch_results = await self._warm_batch_with_loader(loader_func, task_group)
            results.update(batch_results)
        
        return results
    
    async def _warm_batch_with_loader(self, loader_func: Callable, 
                                    tasks: List[WarmingTask]) -> Dict[str, bool]:
        """Warm batch of tasks using same loader function."""
        cache_manager = await self._get_cache_manager()
        results = {}
        
        try:
            # Prepare batch parameters
            batch_params = [task.loader_params for task in tasks]
            
            # Execute batch loader
            if asyncio.iscoroutinefunction(loader_func):
                batch_results = await loader_func(batch_params)
            else:
                batch_results = loader_func(batch_params)
            
            # Cache batch results
            if isinstance(batch_results, dict):
                cache_data = {}
                
                for i, task in enumerate(tasks):
                    if task.cache_key in batch_results:
                        result = batch_results[task.cache_key]
                        cache_data[task.cache_key] = result
                        results[task.task_id] = True
                    else:
                        results[task.task_id] = False
                
                # Batch set to Redis
                if cache_data:
                    success = await cache_manager.set_multi(
                        cache_data, namespace=tasks[0].cache_namespace
                    )
                    
                    if success:
                        logger.info(f"Batch warmed {len(cache_data)} cache entries")
            
        except Exception as e:
            logger.error(f"Batch warming error: {e}")
            for task in tasks:
                results[task.task_id] = False
        
        return results
    
    async def warm_cache(self, task: WarmingTask) -> bool:
        """Single cache warming (delegates to batch)."""
        results = await self.warm_cache_batch([task])
        return results.get(task.task_id, False)
    
    async def get_warming_candidates(self, **kwargs) -> List[WarmingTask]:
        """Get candidates for batch warming."""
        # This would return tasks suitable for batch processing
        return []


class CacheWarmingOrchestrator:
    """Orchestrates cache warming across different strategies."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None):
        self.cache_manager = cache_manager
        self.namespace = "cache_warming"
        
        # Initialize warming engines
        self.warming_engines = {
            WarmingStrategy.PREDICTIVE: PredictiveWarmingEngine(cache_manager),
            WarmingStrategy.SCHEDULED: ScheduledWarmingEngine(cache_manager),
            WarmingStrategy.BATCH: BatchWarmingEngine(cache_manager)
        }
        
        # Task management
        self.pending_tasks: deque = deque()
        self.running_tasks: Dict[str, WarmingTask] = {}
        self.completed_tasks: Dict[str, bool] = {}
        
        # Priority queues
        self.priority_queues = {
            priority: deque() for priority in WarmingPriority
        }
        
        # Statistics
        self.stats = {
            "tasks_completed": 0,
            "tasks_failed": 0,
            "total_warming_time": 0.0,
            "cache_hits_after_warming": 0
        }
        
        # Background task for processing
        self._processor_task = None
        self._running = False
    
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    async def start_orchestrator(self):
        """Start the cache warming orchestrator."""
        if not self._running:
            self._running = True
            self._processor_task = asyncio.create_task(self._process_warming_tasks())
            logger.info("Cache warming orchestrator started")
    
    async def stop_orchestrator(self):
        """Stop the cache warming orchestrator."""
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
        logger.info("Cache warming orchestrator stopped")
    
    async def schedule_warming_task(self, task: WarmingTask) -> bool:
        """Schedule a cache warming task."""
        try:
            # Validate task
            if not task.cache_key or not task.loader_function:
                raise ValueError("Invalid warming task: missing cache_key or loader_function")
            
            # Add to appropriate queue based on priority
            self.priority_queues[task.priority].append(task)
            
            # Store task metadata
            await self._store_task_metadata(task)
            
            logger.debug(f"Scheduled warming task: {task.task_id} (priority: {task.priority.value})")
            return True
            
        except Exception as e:
            logger.error(f"Error scheduling warming task: {e}")
            return False
    
    async def _process_warming_tasks(self):
        """Background task processor for warming tasks."""
        while self._running:
            try:
                # Process tasks by priority
                task = await self._get_next_task()
                
                if task:
                    await self._execute_warming_task(task)
                else:
                    # No tasks, wait a bit
                    await asyncio.sleep(1)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in task processor: {e}")
                await asyncio.sleep(1)
    
    async def _get_next_task(self) -> Optional[WarmingTask]:
        """Get the next task to process based on priority."""
        # Process by priority order
        for priority in [WarmingPriority.CRITICAL, WarmingPriority.HIGH, 
                        WarmingPriority.NORMAL, WarmingPriority.LOW, WarmingPriority.BACKGROUND]:
            queue = self.priority_queues[priority]
            if queue:
                return queue.popleft()
        
        return None
    
    async def _execute_warming_task(self, task: WarmingTask):
        """Execute a warming task."""
        start_time = time.time()
        self.running_tasks[task.task_id] = task
        
        try:
            # Get appropriate warming engine
            engine = self.warming_engines.get(task.strategy)
            if not engine:
                raise ValueError(f"Unknown warming strategy: {task.strategy}")
            
            # Execute warming
            success = await engine.warm_cache(task)
            
            # Update statistics
            execution_time = time.time() - start_time
            self.stats["total_warming_time"] += execution_time
            
            if success:
                self.stats["tasks_completed"] += 1
                self.completed_tasks[task.task_id] = True
                
                # Publish warming event
                await publish_cache_warming(
                    task.cache_key, 
                    {"strategy": task.strategy.value, "execution_time": execution_time},
                    source="warming_orchestrator"
                )
                
                logger.debug(f"Warming task completed: {task.task_id} ({execution_time:.2f}s)")
            else:
                self.stats["tasks_failed"] += 1
                self.completed_tasks[task.task_id] = False
                
                # Retry if allowed
                if task.retry_count < task.max_retries:
                    task.retry_count += 1
                    await asyncio.sleep(2 ** task.retry_count)  # Exponential backoff
                    self.priority_queues[task.priority].append(task)
                    logger.debug(f"Retrying warming task: {task.task_id} (attempt {task.retry_count})")
                else:
                    logger.error(f"Warming task failed after max retries: {task.task_id}")
        
        except Exception as e:
            logger.error(f"Error executing warming task {task.task_id}: {e}")
            self.stats["tasks_failed"] += 1
            self.completed_tasks[task.task_id] = False
        
        finally:
            self.running_tasks.pop(task.task_id, None)
    
    async def _store_task_metadata(self, task: WarmingTask):
        """Store task metadata in cache."""
        cache_manager = await self._get_cache_manager()
        
        metadata = {
            "task_id": task.task_id,
            "cache_key": task.cache_key,
            "strategy": task.strategy.value,
            "priority": task.priority.value,
            "scheduled_at": task.scheduled_at.isoformat() if task.scheduled_at else None,
            "created_at": datetime.utcnow().isoformat(),
            "tags": task.tags
        }
        
        await cache_manager.set(
            f"task_meta:{task.task_id}", metadata, 3600, self.namespace
        )
    
    async def get_warming_statistics(self) -> Dict[str, Any]:
        """Get comprehensive warming statistics."""
        return {
            "orchestrator_stats": self.stats.copy(),
            "task_queues": {
                priority.value: len(queue) 
                for priority, queue in self.priority_queues.items()
            },
            "running_tasks": len(self.running_tasks),
            "completed_tasks": len(self.completed_tasks),
            "success_rate": (
                self.stats["tasks_completed"] / 
                (self.stats["tasks_completed"] + self.stats["tasks_failed"])
                if (self.stats["tasks_completed"] + self.stats["tasks_failed"]) > 0 else 0
            ),
            "avg_warming_time": (
                self.stats["total_warming_time"] / self.stats["tasks_completed"]
                if self.stats["tasks_completed"] > 0 else 0
            )
        }


class CacheInvalidationManager:
    """Advanced cache invalidation with dependency tracking."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None):
        self.cache_manager = cache_manager
        self.namespace = "cache_invalidation"
        
        # Invalidation rules
        self.invalidation_rules: Dict[str, InvalidationRule] = {}
        self.dependency_graph: Dict[str, Set[str]] = defaultdict(set)
        
        # Statistics
        self.stats = {
            "invalidations_triggered": 0,
            "cache_entries_invalidated": 0,
            "cascading_invalidations": 0,
            "rules_processed": 0
        }
    
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    def register_invalidation_rule(self, rule: InvalidationRule):
        """Register a cache invalidation rule."""
        self.invalidation_rules[rule.rule_id] = rule
        logger.info(f"Registered invalidation rule: {rule.name}")
    
    def add_cache_dependency(self, dependent_key: str, dependency_key: str):
        """Add cache dependency relationship."""
        self.dependency_graph[dependency_key].add(dependent_key)
        logger.debug(f"Added dependency: {dependent_key} -> {dependency_key}")
    
    async def invalidate_cache(self, trigger: str, context: Dict[str, Any] = None) -> int:
        """Trigger cache invalidation based on rules."""
        total_invalidated = 0
        
        try:
            # Find matching rules
            matching_rules = [
                rule for rule in self.invalidation_rules.values()
                if rule.enabled and self._rule_matches_trigger(rule, trigger, context)
            ]
            
            # Sort by priority
            matching_rules.sort(key=lambda r: r.priority, reverse=True)
            
            for rule in matching_rules:
                count = await self._execute_invalidation_rule(rule, trigger, context)
                total_invalidated += count
                self.stats["rules_processed"] += 1
            
            self.stats["invalidations_triggered"] += 1
            logger.info(f"Invalidation triggered by '{trigger}': {total_invalidated} entries invalidated")
            
        except Exception as e:
            logger.error(f"Error in cache invalidation: {e}")
        
        return total_invalidated
    
    def _rule_matches_trigger(self, rule: InvalidationRule, trigger: str, 
                            context: Dict[str, Any] = None) -> bool:
        """Check if invalidation rule matches trigger."""
        # Check trigger patterns
        for pattern in rule.trigger_patterns:
            if self._matches_pattern(trigger, pattern):
                # Check additional conditions if any
                if rule.conditions:
                    return self._evaluate_conditions(rule.conditions, context or {})
                return True
        return False
    
    def _matches_pattern(self, value: str, pattern: str) -> bool:
        """Check if value matches pattern (supports wildcards)."""
        import fnmatch
        return fnmatch.fnmatch(value, pattern)
    
    def _evaluate_conditions(self, conditions: Dict[str, Any], context: Dict[str, Any]) -> bool:
        """Evaluate rule conditions against context."""
        for key, expected_value in conditions.items():
            if key not in context:
                return False
            
            context_value = context[key]
            
            if isinstance(expected_value, dict):
                # Complex condition (e.g., {"operator": "gt", "value": 100})
                operator = expected_value.get("operator", "eq")
                value = expected_value.get("value")
                
                if operator == "eq" and context_value != value:
                    return False
                elif operator == "gt" and context_value <= value:
                    return False
                elif operator == "lt" and context_value >= value:
                    return False
                # Add more operators as needed
            else:
                # Simple equality check
                if context_value != expected_value:
                    return False
        
        return True
    
    async def _execute_invalidation_rule(self, rule: InvalidationRule, trigger: str,
                                       context: Dict[str, Any] = None) -> int:
        """Execute a specific invalidation rule."""
        cache_manager = await self._get_cache_manager()
        total_invalidated = 0
        
        try:
            for pattern in rule.target_patterns:
                # Apply context substitutions to pattern
                if context:
                    for key, value in context.items():
                        pattern = pattern.replace(f"{{{key}}}", str(value))
                
                if rule.strategy == InvalidationStrategy.CASCADING:
                    # Invalidate with cascading
                    count = await self._cascading_invalidation(pattern)
                elif rule.strategy == InvalidationStrategy.DEPENDENCY_BASED:
                    # Invalidate based on dependencies
                    count = await self._dependency_based_invalidation(pattern)
                else:
                    # Standard pattern invalidation
                    count = await cache_manager.invalidate_pattern(pattern)
                
                total_invalidated += count
                
                # Publish invalidation event
                await publish_cache_invalidation(
                    pattern, source="invalidation_manager"
                )
            
            self.stats["cache_entries_invalidated"] += total_invalidated
            
        except Exception as e:
            logger.error(f"Error executing invalidation rule {rule.rule_id}: {e}")
        
        return total_invalidated
    
    async def _cascading_invalidation(self, pattern: str) -> int:
        """Perform cascading invalidation following dependencies."""
        cache_manager = await self._get_cache_manager()
        total_invalidated = 0
        
        # Get keys matching pattern
        if cache_manager.is_cluster:
            all_keys = []
            for node in cache_manager.async_redis_client.get_nodes():
                keys = await node.keys(pattern)
                if keys:
                    all_keys.extend(keys)
        else:
            all_keys = await cache_manager.async_redis_client.keys(pattern)
        
        # Track processed keys to avoid cycles
        processed_keys = set()
        
        async def invalidate_with_dependencies(key: str):
            if key in processed_keys:
                return 0
            
            processed_keys.add(key)
            count = 0
            
            # Invalidate the key
            deleted = await cache_manager.async_redis_client.delete(key)
            if deleted:
                count += 1
            
            # Invalidate dependent keys
            if key in self.dependency_graph:
                for dependent_key in self.dependency_graph[key]:
                    count += await invalidate_with_dependencies(dependent_key)
            
            return count
        
        # Process all matching keys
        for key in all_keys:
            total_invalidated += await invalidate_with_dependencies(key)
        
        self.stats["cascading_invalidations"] += 1
        return total_invalidated
    
    async def _dependency_based_invalidation(self, pattern: str) -> int:
        """Invalidate based on dependency relationships."""
        # Similar to cascading but with more sophisticated dependency resolution
        return await self._cascading_invalidation(pattern)
    
    async def get_invalidation_statistics(self) -> Dict[str, Any]:
        """Get comprehensive invalidation statistics."""
        return {
            "invalidation_stats": self.stats.copy(),
            "active_rules": len([r for r in self.invalidation_rules.values() if r.enabled]),
            "total_rules": len(self.invalidation_rules),
            "dependency_relationships": sum(len(deps) for deps in self.dependency_graph.values()),
            "avg_invalidations_per_trigger": (
                self.stats["cache_entries_invalidated"] / self.stats["invalidations_triggered"]
                if self.stats["invalidations_triggered"] > 0 else 0
            )
        }


# Global instances
_warming_orchestrator: Optional[CacheWarmingOrchestrator] = None
_invalidation_manager: Optional[CacheInvalidationManager] = None


async def get_warming_orchestrator() -> CacheWarmingOrchestrator:
    """Get or create global warming orchestrator instance."""
    global _warming_orchestrator
    if _warming_orchestrator is None:
        _warming_orchestrator = CacheWarmingOrchestrator()
        await _warming_orchestrator.start_orchestrator()
    return _warming_orchestrator


async def get_invalidation_manager() -> CacheInvalidationManager:
    """Get or create global invalidation manager instance."""
    global _invalidation_manager
    if _invalidation_manager is None:
        _invalidation_manager = CacheInvalidationManager()
    return _invalidation_manager


# Convenience functions
async def warm_cache_key(cache_key: str, loader_function: Callable,
                        loader_params: Dict[str, Any] = None,
                        priority: WarmingPriority = WarmingPriority.NORMAL,
                        strategy: WarmingStrategy = WarmingStrategy.IMMEDIATE) -> bool:
    """Convenience function to warm a single cache key."""
    orchestrator = await get_warming_orchestrator()
    
    task = WarmingTask(
        task_id=f"warm_{cache_key}_{int(time.time())}",
        cache_key=cache_key,
        cache_namespace="default",
        loader_function=loader_function,
        loader_params=loader_params or {},
        priority=priority,
        strategy=strategy
    )
    
    return await orchestrator.schedule_warming_task(task)


async def invalidate_by_trigger(trigger: str, context: Dict[str, Any] = None) -> int:
    """Convenience function to trigger cache invalidation."""
    invalidation_manager = await get_invalidation_manager()
    return await invalidation_manager.invalidate_cache(trigger, context)
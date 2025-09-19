"""
Optimized Database Connection Pool with Health Monitoring

High-performance connection pool with advanced features:
- Adaptive pool sizing based on load
- Connection health monitoring and automatic failover
- Query performance tracking and optimization
- Read/write replica support
- Connection lifecycle management
"""

import asyncio
import time
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union, Callable, AsyncContextManager
from urllib.parse import urlparse
from collections import defaultdict, deque

import asyncpg
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import QueuePool, NullPool
from sqlalchemy.engine.events import event
from sqlalchemy import text

from core.logging import get_logger

logger = get_logger(__name__)


class ConnectionPoolStrategy(Enum):
    """Connection pool strategies."""
    FIXED = "fixed"
    ADAPTIVE = "adaptive"
    LOAD_BALANCED = "load_balanced"


class ConnectionState(Enum):
    """Connection states."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    RECOVERING = "recovering"


@dataclass
class ConnectionMetrics:
    """Connection performance metrics."""
    connection_id: str
    created_at: datetime
    last_used: datetime
    total_queries: int = 0
    failed_queries: int = 0
    avg_query_time: float = 0.0
    current_state: ConnectionState = ConnectionState.HEALTHY
    error_count: int = 0
    last_error: Optional[str] = None
    last_health_check: Optional[datetime] = None
    
    def record_query(self, execution_time: float, success: bool = True):
        """Record query execution metrics."""
        self.total_queries += 1
        self.last_used = datetime.utcnow()
        
        if success:
            # Update rolling average
            if self.avg_query_time == 0:
                self.avg_query_time = execution_time
            else:
                # Exponential moving average
                alpha = 0.1
                self.avg_query_time = alpha * execution_time + (1 - alpha) * self.avg_query_time
        else:
            self.failed_queries += 1
            self.error_count += 1
    
    def update_state(self, state: ConnectionState, error: Optional[str] = None):
        """Update connection state."""
        self.current_state = state
        if error:
            self.last_error = error
            self.error_count += 1
    
    @property
    def success_rate(self) -> float:
        """Calculate query success rate."""
        if self.total_queries == 0:
            return 1.0
        return (self.total_queries - self.failed_queries) / self.total_queries
    
    @property
    def is_healthy(self) -> bool:
        """Check if connection is healthy."""
        return (
            self.current_state == ConnectionState.HEALTHY and
            self.success_rate >= 0.95 and
            self.error_count < 5
        )


@dataclass
class PoolConfiguration:
    """Database pool configuration."""
    # Basic pool settings
    min_size: int = 5
    max_size: int = 20
    max_overflow: int = 10
    
    # Connection settings
    connection_timeout: float = 30.0
    checkout_timeout: float = 10.0
    recycle_time: int = 3600  # 1 hour
    
    # Health monitoring
    health_check_interval: float = 60.0  # seconds
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # Performance settings
    statement_cache_size: int = 1000
    prepared_statement_cache_size: int = 100
    query_timeout: float = 30.0
    
    # Adaptive settings
    enable_adaptive_sizing: bool = True
    load_threshold: float = 0.8  # Scale up when 80% utilization
    scale_up_factor: float = 1.5
    scale_down_factor: float = 0.7
    
    # Replica settings
    read_replica_urls: List[str] = field(default_factory=list)
    read_write_ratio: float = 0.7  # 70% read, 30% write
    
    # Monitoring
    enable_query_logging: bool = True
    slow_query_threshold: float = 1.0  # seconds
    enable_metrics_collection: bool = True


class OptimizedConnectionPool:
    """Optimized database connection pool with advanced features."""
    
    def __init__(self, database_url: str, config: Optional[PoolConfiguration] = None):
        self.database_url = database_url
        self.config = config or PoolConfiguration()
        
        # Connection tracking
        self.connections: Dict[str, ConnectionMetrics] = {}
        self.connection_lock = asyncio.Lock()
        
        # Pool state
        self.current_pool_size = self.config.min_size
        self.active_connections = 0
        self.total_requests = 0
        self.pool_created_at = datetime.utcnow()
        
        # Health monitoring
        self.health_check_task: Optional[asyncio.Task] = None
        self.is_healthy = True
        self.last_health_check = datetime.utcnow()
        
        # Performance metrics
        self.query_metrics = {
            "total_queries": 0,
            "slow_queries": 0,
            "failed_queries": 0,
            "avg_query_time": 0.0,
            "peak_connections": 0
        }
        
        # SQLAlchemy engines
        self.write_engine = None
        self.read_engines: List[Any] = []
        self.session_factory = None
        
        # Async initialization flag
        self._initialized = False
        
        # Enhanced concurrent access optimization
        self.connection_semaphore = asyncio.BoundedSemaphore(self.config.max_size + self.config.max_overflow)
        self.query_queue = asyncio.Queue(maxsize=1000)  # Queue for query requests
        self.connection_distribution_lock = asyncio.RLock()  # Re-entrant lock for connection distribution
        
        # Load balancing for read replicas
        self.read_replica_weights: Dict[str, float] = {}  # Weight-based load balancing
        self.read_replica_health: Dict[str, bool] = {}  # Health status of read replicas
        self.connection_affinity: Dict[str, str] = {}  # Session to preferred engine mapping
        
        # Performance-based connection selection
        self.engine_performance_metrics: Dict[str, Dict[str, float]] = {}
        self.adaptive_routing_enabled = True
        
        # Enhanced monitoring
        self.concurrent_request_histogram = defaultdict(int)  # Track concurrent request patterns
        self.connection_wait_times = deque(maxlen=1000)  # Track wait times for connections
    
    async def initialize(self):
        """Initialize the connection pool."""
        if self._initialized:
            return
        
        try:
            # Create write engine (primary database)
            self.write_engine = create_async_engine(
                self.database_url,
                poolclass=QueuePool,
                pool_size=self.config.min_size,
                max_overflow=self.config.max_overflow,
                pool_timeout=self.config.checkout_timeout,
                pool_recycle=self.config.recycle_time,
                pool_pre_ping=True,  # Validate connections before use
                echo=self.config.enable_query_logging,
                connect_args={
                    "statement_cache_size": self.config.statement_cache_size,
                    "prepared_statement_cache_size": self.config.prepared_statement_cache_size,
                    "command_timeout": self.config.query_timeout
                }
            )
            
            # Create read replica engines
            for read_url in self.config.read_replica_urls:
                read_engine = create_async_engine(
                    read_url,
                    poolclass=QueuePool,
                    pool_size=max(2, self.config.min_size // 2),
                    max_overflow=self.config.max_overflow // 2,
                    pool_timeout=self.config.checkout_timeout,
                    pool_recycle=self.config.recycle_time,
                    pool_pre_ping=True,
                    echo=self.config.enable_query_logging
                )
                self.read_engines.append(read_engine)
            
            # Create session factory
            self.session_factory = async_sessionmaker(
                self.write_engine,
                expire_on_commit=False,
                class_=AsyncSession
            )
            
            # Set up event listeners for monitoring
            self._setup_event_listeners()
            
            # Start health monitoring
            if self.config.health_check_interval > 0:
                self.health_check_task = asyncio.create_task(self._health_monitor())
            
            self._initialized = True
            logger.info(f"Optimized connection pool initialized with {self.config.min_size}-{self.config.max_size} connections")
            
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise
    
    def _setup_event_listeners(self):
        """Set up SQLAlchemy event listeners for monitoring."""
        
        @event.listens_for(self.write_engine.sync_engine, "before_cursor_execute")
        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            context._query_start_time = time.time()
        
        @event.listens_for(self.write_engine.sync_engine, "after_cursor_execute")
        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            if hasattr(context, '_query_start_time'):
                execution_time = time.time() - context._query_start_time
                asyncio.create_task(self._record_query_metrics(execution_time, True))
        
        @event.listens_for(self.write_engine.sync_engine, "handle_error")
        def handle_error(exception_context):
            asyncio.create_task(self._record_query_metrics(0, False))
    
    async def _record_query_metrics(self, execution_time: float, success: bool):
        """Record query execution metrics."""
        self.query_metrics["total_queries"] += 1
        
        if success:
            # Update average query time
            if self.query_metrics["avg_query_time"] == 0:
                self.query_metrics["avg_query_time"] = execution_time
            else:
                alpha = 0.1
                self.query_metrics["avg_query_time"] = (
                    alpha * execution_time + 
                    (1 - alpha) * self.query_metrics["avg_query_time"]
                )
            
            # Check for slow queries
            if execution_time > self.config.slow_query_threshold:
                self.query_metrics["slow_queries"] += 1
                logger.warning(f"Slow query detected: {execution_time:.3f}s")
        else:
            self.query_metrics["failed_queries"] += 1
    
    @asynccontextmanager
    async def get_session(self, read_only: bool = False, session_id: str = None, preferred_engine: str = None) -> AsyncContextManager[AsyncSession]:
        """
        Get database session with advanced connection management and optimization.
        
        Args:
            read_only: Whether this is a read-only session
            session_id: Optional session identifier for connection affinity
            preferred_engine: Preferred engine for this session
        
        Returns:
            Async context manager for database session
        """
        if not self._initialized:
            await self.initialize()
        
        # Acquire connection semaphore for concurrent access control
        connection_wait_start = time.time()
        await self.connection_semaphore.acquire()
        connection_wait_time = time.time() - connection_wait_start
        self.connection_wait_times.append(connection_wait_time)
        
        if connection_wait_time > 0.1:  # Log slow connection waits
            logger.warning(f"Slow connection acquisition: {connection_wait_time:.3f}s")
        
        try:
            # Enhanced engine selection with performance-based routing
            engine = await self._select_optimal_engine(read_only, session_id, preferred_engine)
            session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
            
            session = session_factory()
            connection_id = f"conn_{int(time.time()*1000)}_{id(session)}_{session_id or 'anonymous'}"
        
        try:
            # Track connection
            async with self.connection_lock:
                self.active_connections += 1
                self.total_requests += 1
                
                if self.active_connections > self.query_metrics["peak_connections"]:
                    self.query_metrics["peak_connections"] = self.active_connections
                
                self.connections[connection_id] = ConnectionMetrics(
                    connection_id=connection_id,
                    created_at=datetime.utcnow(),
                    last_used=datetime.utcnow()
                )
            
            # Check if pool scaling is needed
            if self.config.enable_adaptive_sizing:
                await self._check_adaptive_scaling()
            
            # Update concurrent request tracking
            current_concurrent = len([c for c in self.connections.values() if c.current_state == ConnectionState.HEALTHY])
            self.concurrent_request_histogram[min(current_concurrent, 50)] += 1  # Cap at 50 for histogram
            
            yield session
            
        except Exception as e:
            # Record connection error
            if connection_id in self.connections:
                self.connections[connection_id].update_state(ConnectionState.FAILED, str(e))
            logger.error(f"Database session error: {e}")
            raise
        finally:
            try:
                await session.close()
            except Exception as e:
                logger.error(f"Error closing session: {e}")
            finally:
                # Update connection tracking
                async with self.connection_lock:
                    self.active_connections -= 1
                    if connection_id in self.connections:
                        del self.connections[connection_id]
                
                # Release connection semaphore
                self.connection_semaphore.release()
    
    async def _select_optimal_engine(self, read_only: bool = False, session_id: str = None, preferred_engine: str = None):
        """
        Select optimal engine based on performance metrics, health, and load balancing.
        
        Enhanced selection algorithm considering:
        - Engine health and response times
        - Current load distribution
        - Session affinity for consistent reads
        - Performance-based routing
        """
        async with self.connection_distribution_lock:
            # Check for session affinity
            if session_id and session_id in self.connection_affinity:
                preferred_engine = self.connection_affinity[session_id]
            
            # For read-only queries, use advanced read replica selection
            if read_only and self.read_engines:
                selected_engine = await self._select_optimal_read_engine(session_id)
                
                # Store session affinity for consistent reads
                if session_id:
                    self.connection_affinity[session_id] = str(id(selected_engine))
                    
                return selected_engine
            
            # For write operations, always use write engine but track performance
            await self._update_engine_performance_metrics(self.write_engine, "write")
            return self.write_engine
    
    async def _select_optimal_read_engine(self, session_id: str = None):
        """Select optimal read replica based on performance and health."""
        if not self.read_engines:
            return self.write_engine
        
        healthy_engines = []
        
        # Filter healthy engines and collect performance metrics
        for i, engine in enumerate(self.read_engines):
            engine_key = f"read_{i}"
            
            # Check engine health
            is_healthy = self.read_replica_health.get(engine_key, True)
            
            if is_healthy:
                # Get performance metrics
                metrics = self.engine_performance_metrics.get(engine_key, {
                    'avg_response_time': 0.1,
                    'active_connections': 0,
                    'error_rate': 0.0,
                    'weight': 1.0
                })
                
                healthy_engines.append((engine, engine_key, metrics))
        
        if not healthy_engines:
            logger.warning("No healthy read replicas available, falling back to write engine")
            return self.write_engine
        
        # Performance-based selection
        if self.adaptive_routing_enabled:
            return await self._select_by_performance(healthy_engines)
        else:
            # Weighted round-robin selection
            return await self._select_by_weight(healthy_engines)
    
    async def _select_by_performance(self, healthy_engines):
        """Select engine based on performance metrics."""
        best_engine = None
        best_score = float('inf')
        
        for engine, engine_key, metrics in healthy_engines:
            # Calculate composite performance score
            # Lower score is better
            score = (
                metrics['avg_response_time'] * 10 +  # Response time weight: 10x
                metrics['active_connections'] * 0.1 +  # Connection load weight: 0.1x
                metrics['error_rate'] * 100  # Error rate weight: 100x
            )
            
            # Adjust score by engine weight (higher weight = better)
            score = score / max(metrics['weight'], 0.1)
            
            if score < best_score:
                best_score = score
                best_engine = engine
        
        return best_engine or healthy_engines[0][0]
    
    async def _select_by_weight(self, healthy_engines):
        """Select engine using weighted round-robin."""
        total_weight = sum(metrics['weight'] for _, _, metrics in healthy_engines)
        
        if total_weight == 0:
            return healthy_engines[0][0]
        
        # Use request count to implement weighted round-robin
        weight_position = self.total_requests % int(total_weight * 10)
        current_weight = 0
        
        for engine, engine_key, metrics in healthy_engines:
            current_weight += metrics['weight'] * 10
            if weight_position < current_weight:
                return engine
        
        # Fallback to first engine
        return healthy_engines[0][0]
    
    async def _update_engine_performance_metrics(self, engine, engine_type: str):
        """Update performance metrics for an engine."""
        engine_key = f"{engine_type}_{id(engine)}"
        
        if engine_key not in self.engine_performance_metrics:
            self.engine_performance_metrics[engine_key] = {
                'avg_response_time': 0.1,
                'active_connections': 0,
                'error_rate': 0.0,
                'weight': 1.0,
                'last_updated': time.time()
            }
        
        # Update active connections count
        active_count = len([c for c in self.connections.values() 
                          if c.current_state == ConnectionState.HEALTHY])
        
        self.engine_performance_metrics[engine_key]['active_connections'] = active_count
        self.engine_performance_metrics[engine_key]['last_updated'] = time.time()
    
    async def _check_adaptive_scaling(self):
        """Check if pool should be scaled up or down."""
        if not self.config.enable_adaptive_sizing:
            return
        
        utilization = self.active_connections / self.current_pool_size
        
        # Scale up if utilization is high
        if utilization > self.config.load_threshold:
            new_size = min(
                int(self.current_pool_size * self.config.scale_up_factor),
                self.config.max_size
            )
            if new_size > self.current_pool_size:
                await self._scale_pool(new_size)
                logger.info(f"Scaled pool up to {new_size} connections (utilization: {utilization:.2f})")
        
        # Scale down if utilization is low
        elif utilization < (self.config.load_threshold / 2):
            new_size = max(
                int(self.current_pool_size * self.config.scale_down_factor),
                self.config.min_size
            )
            if new_size < self.current_pool_size:
                await self._scale_pool(new_size)
                logger.info(f"Scaled pool down to {new_size} connections (utilization: {utilization:.2f})")
    
    async def _scale_pool(self, new_size: int):
        """Scale the connection pool to new size."""
        try:
            # Update engine pool size
            self.write_engine.pool._pool.maxsize = new_size
            self.current_pool_size = new_size
            
            # Update read replica pool sizes proportionally
            for engine in self.read_engines:
                engine.pool._pool.maxsize = max(2, new_size // 2)
                
        except Exception as e:
            logger.error(f"Failed to scale connection pool: {e}")
    
    async def _health_monitor(self):
        """Background task for health monitoring."""
        while True:
            try:
                await asyncio.sleep(self.config.health_check_interval)
                await self._perform_health_check()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health monitor error: {e}")
    
    async def _perform_health_check(self):
        """Perform comprehensive health check."""
        try:
            start_time = time.time()
            
            # Test write engine
            async with self.get_session() as session:
                result = await session.execute(text("SELECT 1"))
                await result.fetchone()
            
            # Test read engines
            for engine in self.read_engines:
                try:
                    async with engine.begin() as conn:
                        await conn.execute(text("SELECT 1"))
                except Exception as e:
                    logger.warning(f"Read replica health check failed: {e}")
            
            health_check_time = time.time() - start_time
            self.is_healthy = health_check_time < 1.0  # Consider healthy if responds in <1s
            self.last_health_check = datetime.utcnow()
            
            if health_check_time > 0.5:
                logger.warning(f"Slow health check: {health_check_time:.3f}s")
            
        except Exception as e:
            self.is_healthy = False
            logger.error(f"Health check failed: {e}")
    
    async def execute_query(self, query: str, parameters: Optional[Dict] = None, 
                          read_only: bool = False) -> Any:
        """Execute a query with performance monitoring."""
        start_time = time.time()
        
        try:
            async with self.get_session(read_only=read_only) as session:
                if parameters:
                    result = await session.execute(text(query), parameters)
                else:
                    result = await session.execute(text(query))
                
                if query.strip().upper().startswith(('SELECT', 'WITH')):
                    return await result.fetchall()
                else:
                    await session.commit()
                    return result.rowcount
                    
        except Exception as e:
            execution_time = time.time() - start_time
            await self._record_query_metrics(execution_time, False)
            raise
        finally:
            execution_time = time.time() - start_time
            await self._record_query_metrics(execution_time, True)
    
    async def get_pool_stats(self) -> Dict[str, Any]:
        """Get comprehensive pool statistics."""
        uptime = datetime.utcnow() - self.pool_created_at
        
        # Calculate connection utilization
        utilization = self.active_connections / self.current_pool_size if self.current_pool_size > 0 else 0
        
        # Calculate success rate
        total_queries = self.query_metrics["total_queries"]
        failed_queries = self.query_metrics["failed_queries"]
        success_rate = (total_queries - failed_queries) / total_queries if total_queries > 0 else 1.0
        
        return {
            "pool_health": {
                "is_healthy": self.is_healthy,
                "last_health_check": self.last_health_check.isoformat() if self.last_health_check else None,
                "uptime_seconds": uptime.total_seconds()
            },
            "pool_size": {
                "current_size": self.current_pool_size,
                "min_size": self.config.min_size,
                "max_size": self.config.max_size,
                "active_connections": self.active_connections,
                "utilization": utilization
            },
            "performance_metrics": {
                "total_requests": self.total_requests,
                "total_queries": total_queries,
                "failed_queries": failed_queries,
                "success_rate": success_rate,
                "avg_query_time": self.query_metrics["avg_query_time"],
                "slow_queries": self.query_metrics["slow_queries"],
                "peak_connections": self.query_metrics["peak_connections"]
            },
            "configuration": {
                "adaptive_sizing_enabled": self.config.enable_adaptive_sizing,
                "read_replicas": len(self.read_engines),
                "health_check_interval": self.config.health_check_interval,
                "query_timeout": self.config.query_timeout
            }
        }
    
    async def close(self):
        """Close the connection pool and cleanup resources."""
        try:
            # Cancel health monitoring
            if self.health_check_task:
                self.health_check_task.cancel()
                try:
                    await self.health_check_task
                except asyncio.CancelledError:
                    pass
            
            # Close engines
            if self.write_engine:
                await self.write_engine.dispose()
            
            for engine in self.read_engines:
                await engine.dispose()
            
            self._initialized = False
            logger.info("Connection pool closed successfully")
            
        except Exception as e:
            logger.error(f"Error closing connection pool: {e}")


# Global connection pool instance
_connection_pool: Optional[OptimizedConnectionPool] = None


async def get_optimized_pool(database_url: str, config: Optional[PoolConfiguration] = None) -> OptimizedConnectionPool:
    """Get or create optimized connection pool."""
    global _connection_pool
    
    if _connection_pool is None:
        _connection_pool = OptimizedConnectionPool(database_url, config)
        await _connection_pool.initialize()
    
    return _connection_pool


async def close_pool():
    """Close the global connection pool."""
    global _connection_pool
    if _connection_pool:
        await _connection_pool.close()
        _connection_pool = None


# Context manager for automatic pool management
@asynccontextmanager
async def managed_pool(database_url: str, config: Optional[PoolConfiguration] = None):
    """Context manager for automatic pool lifecycle management."""
    pool = None
    try:
        pool = OptimizedConnectionPool(database_url, config)
        await pool.initialize()
        yield pool
    finally:
        if pool:
            await pool.close()
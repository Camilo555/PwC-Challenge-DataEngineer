"""
Redis Monitoring and Performance Optimization

Comprehensive monitoring, alerting, and performance optimization system
for Redis cache infrastructure with real-time metrics and automated tuning.
"""

import asyncio
import json
import time
import warnings
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable

import psutil

from core.logging import get_logger
from core.caching.redis_cache_manager import RedisCacheManager, get_cache_manager
from core.caching.redis_streams import publish_cache_invalidation, EventType

logger = get_logger(__name__)


class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class MetricType(Enum):
    """Types of Redis metrics."""
    PERFORMANCE = "performance"
    MEMORY = "memory"
    CONNECTION = "connection"
    REPLICATION = "replication"
    PERSISTENCE = "persistence"
    CLUSTER = "cluster"
    CACHE_EFFICIENCY = "cache_efficiency"


@dataclass
class RedisMetric:
    """Redis metric data point."""
    name: str
    value: Union[int, float, str]
    metric_type: MetricType
    timestamp: datetime
    instance: str = "default"
    tags: Dict[str, str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = {}


@dataclass
class Alert:
    """Redis monitoring alert."""
    alert_id: str
    name: str
    level: AlertLevel
    message: str
    metric_name: str
    current_value: Union[int, float]
    threshold: Union[int, float]
    created_at: datetime
    instance: str = "default"
    resolved_at: Optional[datetime] = None
    
    def is_resolved(self) -> bool:
        return self.resolved_at is not None


class PerformanceAnalyzer:
    """Analyzes Redis performance patterns and suggests optimizations."""
    
    def __init__(self):
        self.analysis_window = timedelta(minutes=30)
        self.metric_history = defaultdict(deque)
        
    def add_metric(self, metric: RedisMetric):
        """Add metric to analysis history."""
        # Keep only recent metrics
        cutoff_time = datetime.utcnow() - self.analysis_window
        
        history = self.metric_history[metric.name]
        history.append(metric)
        
        # Remove old metrics
        while history and history[0].timestamp < cutoff_time:
            history.popleft()
    
    def analyze_cache_hit_ratio(self, hit_ratio: float) -> List[str]:
        """Analyze cache hit ratio and provide recommendations."""
        recommendations = []
        
        if hit_ratio < 0.5:
            recommendations.extend([
                "Cache hit ratio is critically low (<50%)",
                "Consider increasing cache memory allocation",
                "Review cache key TTL settings",
                "Implement better cache warming strategies"
            ])
        elif hit_ratio < 0.7:
            recommendations.extend([
                "Cache hit ratio could be improved (<70%)",
                "Review cache eviction policies",
                "Consider implementing cache warming for hot keys"
            ])
        elif hit_ratio < 0.9:
            recommendations.append("Cache performance is good, minor optimizations possible")
        
        return recommendations
    
    def analyze_memory_usage(self, used_memory: int, max_memory: int) -> List[str]:
        """Analyze memory usage patterns."""
        recommendations = []
        usage_ratio = used_memory / max_memory if max_memory > 0 else 0
        
        if usage_ratio > 0.9:
            recommendations.extend([
                "Memory usage is critically high (>90%)",
                "Consider increasing max memory limit",
                "Review key expiration policies",
                "Implement more aggressive eviction policies"
            ])
        elif usage_ratio > 0.8:
            recommendations.extend([
                "Memory usage is high (>80%)",
                "Monitor for potential memory issues",
                "Consider cleanup of expired keys"
            ])
        
        return recommendations
    
    def analyze_connection_patterns(self, connected_clients: int, 
                                  max_clients: int) -> List[str]:
        """Analyze connection usage patterns."""
        recommendations = []
        
        if connected_clients > max_clients * 0.8:
            recommendations.extend([
                "High connection usage detected",
                "Consider connection pooling",
                "Review client connection management",
                "Monitor for connection leaks"
            ])
        
        return recommendations
    
    def analyze_slow_queries(self, slow_queries: List[Dict]) -> List[str]:
        """Analyze slow query patterns."""
        recommendations = []
        
        if len(slow_queries) > 10:
            recommendations.extend([
                "High number of slow queries detected",
                "Review query patterns and optimize",
                "Consider data structure optimizations",
                "Implement query result caching"
            ])
        
        # Analyze query patterns
        command_counts = defaultdict(int)
        for query in slow_queries:
            command = query.get('command', '').split()[0].upper()
            command_counts[command] += 1
        
        # Find most problematic commands
        sorted_commands = sorted(command_counts.items(), key=lambda x: x[1], reverse=True)
        
        if sorted_commands:
            top_command = sorted_commands[0]
            if top_command[1] > 5:
                recommendations.append(
                    f"Command '{top_command[0]}' is frequently slow ({top_command[1]} occurrences)"
                )
        
        return recommendations


class RedisMonitor:
    """Comprehensive Redis monitoring system."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None,
                 monitoring_interval: int = 60):  # 1 minute
        self.cache_manager = cache_manager
        self.monitoring_interval = monitoring_interval
        self.namespace = "redis_monitoring"
        
        # Monitoring state
        self.is_monitoring = False
        self.monitor_task = None
        
        # Alert thresholds
        self.alert_thresholds = {
            'memory_usage_percent': {'warning': 80, 'critical': 90},
            'hit_rate': {'warning': 0.7, 'critical': 0.5},
            'connected_clients_percent': {'warning': 80, 'critical': 95},
            'evicted_keys_per_second': {'warning': 10, 'critical': 50},
            'expired_keys_per_second': {'warning': 100, 'critical': 500},
            'blocked_clients': {'warning': 10, 'critical': 50},
            'replication_lag_seconds': {'warning': 5, 'critical': 10},
            'rdb_last_save_hours': {'warning': 2, 'critical': 4}
        }
        
        # Metrics storage
        self.metrics_history = defaultdict(deque)
        self.active_alerts = {}
        
        # Performance analyzer
        self.performance_analyzer = PerformanceAnalyzer()
        
        # Statistics
        self.stats = {
            'metrics_collected': 0,
            'alerts_triggered': 0,
            'alerts_resolved': 0,
            'monitoring_uptime': 0.0
        }
        
        self.monitoring_start_time = None
    
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    async def start_monitoring(self):
        """Start Redis monitoring."""
        if not self.is_monitoring:
            self.is_monitoring = True
            self.monitoring_start_time = time.time()
            self.monitor_task = asyncio.create_task(self._monitoring_loop())
            logger.info("Redis monitoring started")
    
    async def stop_monitoring(self):
        """Stop Redis monitoring."""
        self.is_monitoring = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("Redis monitoring stopped")
    
    async def _monitoring_loop(self):
        """Main monitoring loop."""
        while self.is_monitoring:
            try:
                # Collect metrics
                metrics = await self.collect_metrics()
                
                # Store metrics
                await self._store_metrics(metrics)
                
                # Check alerts
                await self._check_alerts(metrics)
                
                # Update statistics
                self.stats['metrics_collected'] += len(metrics)
                
                if self.monitoring_start_time:
                    self.stats['monitoring_uptime'] = time.time() - self.monitoring_start_time
                
                # Wait for next cycle
                await asyncio.sleep(self.monitoring_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(self.monitoring_interval)
    
    async def collect_metrics(self) -> List[RedisMetric]:
        """Collect comprehensive Redis metrics."""
        cache_manager = await self._get_cache_manager()
        metrics = []
        timestamp = datetime.utcnow()
        
        try:
            # Get Redis INFO
            info = await cache_manager.async_redis_client.info()
            
            # Memory metrics
            metrics.extend([
                RedisMetric("used_memory", info.get('used_memory', 0), 
                           MetricType.MEMORY, timestamp),
                RedisMetric("used_memory_rss", info.get('used_memory_rss', 0), 
                           MetricType.MEMORY, timestamp),
                RedisMetric("used_memory_peak", info.get('used_memory_peak', 0), 
                           MetricType.MEMORY, timestamp),
                RedisMetric("maxmemory", info.get('maxmemory', 0), 
                           MetricType.MEMORY, timestamp),
                RedisMetric("mem_fragmentation_ratio", info.get('mem_fragmentation_ratio', 0), 
                           MetricType.MEMORY, timestamp)
            ])
            
            # Performance metrics
            total_commands = info.get('total_commands_processed', 0)
            total_connections = info.get('total_connections_received', 0)
            keyspace_hits = info.get('keyspace_hits', 0)
            keyspace_misses = info.get('keyspace_misses', 0)
            
            hit_rate = (keyspace_hits / (keyspace_hits + keyspace_misses) 
                       if (keyspace_hits + keyspace_misses) > 0 else 0)
            
            metrics.extend([
                RedisMetric("total_commands_processed", total_commands, 
                           MetricType.PERFORMANCE, timestamp),
                RedisMetric("instantaneous_ops_per_sec", info.get('instantaneous_ops_per_sec', 0), 
                           MetricType.PERFORMANCE, timestamp),
                RedisMetric("keyspace_hits", keyspace_hits, 
                           MetricType.CACHE_EFFICIENCY, timestamp),
                RedisMetric("keyspace_misses", keyspace_misses, 
                           MetricType.CACHE_EFFICIENCY, timestamp),
                RedisMetric("hit_rate", hit_rate, 
                           MetricType.CACHE_EFFICIENCY, timestamp),
                RedisMetric("expired_keys", info.get('expired_keys', 0), 
                           MetricType.CACHE_EFFICIENCY, timestamp),
                RedisMetric("evicted_keys", info.get('evicted_keys', 0), 
                           MetricType.CACHE_EFFICIENCY, timestamp)
            ])
            
            # Connection metrics
            metrics.extend([
                RedisMetric("connected_clients", info.get('connected_clients', 0), 
                           MetricType.CONNECTION, timestamp),
                RedisMetric("blocked_clients", info.get('blocked_clients', 0), 
                           MetricType.CONNECTION, timestamp),
                RedisMetric("total_connections_received", total_connections, 
                           MetricType.CONNECTION, timestamp),
                RedisMetric("rejected_connections", info.get('rejected_connections', 0), 
                           MetricType.CONNECTION, timestamp)
            ])
            
            # Replication metrics (if applicable)
            if info.get('role') == 'master':
                metrics.extend([
                    RedisMetric("connected_slaves", info.get('connected_slaves', 0), 
                               MetricType.REPLICATION, timestamp),
                    RedisMetric("master_repl_offset", info.get('master_repl_offset', 0), 
                               MetricType.REPLICATION, timestamp)
                ])
            elif info.get('role') == 'slave':
                metrics.extend([
                    RedisMetric("slave_repl_offset", info.get('slave_repl_offset', 0), 
                               MetricType.REPLICATION, timestamp),
                    RedisMetric("slave_lag", info.get('master_last_io_seconds_ago', 0), 
                               MetricType.REPLICATION, timestamp)
                ])
            
            # Persistence metrics
            metrics.extend([
                RedisMetric("rdb_changes_since_last_save", info.get('rdb_changes_since_last_save', 0), 
                           MetricType.PERSISTENCE, timestamp),
                RedisMetric("rdb_last_save_time", info.get('rdb_last_save_time', 0), 
                           MetricType.PERSISTENCE, timestamp),
                RedisMetric("aof_rewrite_in_progress", info.get('aof_rewrite_in_progress', 0), 
                           MetricType.PERSISTENCE, timestamp)
            ])
            
            # System metrics
            system_metrics = await self._collect_system_metrics(timestamp)
            metrics.extend(system_metrics)
            
            # Cluster metrics (if applicable)
            if cache_manager.is_cluster:
                cluster_metrics = await self._collect_cluster_metrics(timestamp)
                metrics.extend(cluster_metrics)
            
        except Exception as e:
            logger.error(f"Error collecting Redis metrics: {e}")
        
        return metrics
    
    async def _collect_system_metrics(self, timestamp: datetime) -> List[RedisMetric]:
        """Collect system-level metrics."""
        metrics = []
        
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent()
            metrics.append(RedisMetric("system_cpu_percent", cpu_percent, 
                                     MetricType.PERFORMANCE, timestamp))
            
            # Memory usage
            memory = psutil.virtual_memory()
            metrics.extend([
                RedisMetric("system_memory_total", memory.total, 
                           MetricType.MEMORY, timestamp),
                RedisMetric("system_memory_available", memory.available, 
                           MetricType.MEMORY, timestamp),
                RedisMetric("system_memory_percent", memory.percent, 
                           MetricType.MEMORY, timestamp)
            ])
            
            # Disk I/O
            disk_io = psutil.disk_io_counters()
            if disk_io:
                metrics.extend([
                    RedisMetric("disk_read_bytes", disk_io.read_bytes, 
                               MetricType.PERFORMANCE, timestamp),
                    RedisMetric("disk_write_bytes", disk_io.write_bytes, 
                               MetricType.PERFORMANCE, timestamp)
                ])
            
            # Network I/O
            net_io = psutil.net_io_counters()
            if net_io:
                metrics.extend([
                    RedisMetric("network_bytes_sent", net_io.bytes_sent, 
                               MetricType.PERFORMANCE, timestamp),
                    RedisMetric("network_bytes_recv", net_io.bytes_recv, 
                               MetricType.PERFORMANCE, timestamp)
                ])
        
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
        
        return metrics
    
    async def _collect_cluster_metrics(self, timestamp: datetime) -> List[RedisMetric]:
        """Collect Redis cluster metrics."""
        metrics = []
        cache_manager = await self._get_cache_manager()
        
        try:
            # Get cluster info
            cluster_info = await cache_manager.async_redis_client.cluster_info()
            
            if cluster_info:
                metrics.extend([
                    RedisMetric("cluster_state", cluster_info.get('cluster_state', 'unknown'), 
                               MetricType.CLUSTER, timestamp),
                    RedisMetric("cluster_slots_assigned", cluster_info.get('cluster_slots_assigned', 0), 
                               MetricType.CLUSTER, timestamp),
                    RedisMetric("cluster_slots_ok", cluster_info.get('cluster_slots_ok', 0), 
                               MetricType.CLUSTER, timestamp),
                    RedisMetric("cluster_known_nodes", cluster_info.get('cluster_known_nodes', 0), 
                               MetricType.CLUSTER, timestamp)
                ])
        
        except Exception as e:
            logger.error(f"Error collecting cluster metrics: {e}")
        
        return metrics
    
    async def _store_metrics(self, metrics: List[RedisMetric]):
        """Store metrics for analysis."""
        cache_manager = await self._get_cache_manager()
        
        # Store in Redis for persistence
        try:
            metrics_data = [asdict(metric) for metric in metrics]
            
            # Convert datetime to ISO string
            for metric_data in metrics_data:
                metric_data['timestamp'] = metric_data['timestamp'].isoformat()
                metric_data['metric_type'] = metric_data['metric_type'].value
            
            # Store with TTL (keep for 24 hours)
            await cache_manager.lpush(
                "metrics_history", *metrics_data, 
                namespace=self.namespace
            )
            
            # Keep only recent metrics (last 1440 = 24 hours worth)
            await cache_manager.async_redis_client.ltrim(
                cache_manager._build_key("metrics_history", self.namespace),
                0, 1440
            )
            
        except Exception as e:
            logger.error(f"Error storing metrics: {e}")
        
        # Store in memory for analysis
        for metric in metrics:
            self.performance_analyzer.add_metric(metric)
            
            # Keep limited history in memory
            history = self.metrics_history[metric.name]
            history.append(metric)
            
            # Keep only last 100 data points per metric
            while len(history) > 100:
                history.popleft()
    
    async def _check_alerts(self, metrics: List[RedisMetric]):
        """Check metrics against alert thresholds."""
        current_time = datetime.utcnow()
        
        for metric in metrics:
            if not isinstance(metric.value, (int, float)):
                continue
                
            metric_name = metric.name
            
            # Check if we have thresholds for this metric
            if metric_name not in self.alert_thresholds:
                continue
            
            thresholds = self.alert_thresholds[metric_name]
            current_value = float(metric.value)
            
            # Check for critical alert
            if 'critical' in thresholds and current_value >= thresholds['critical']:
                await self._trigger_alert(
                    metric_name, AlertLevel.CRITICAL, current_value, 
                    thresholds['critical'], current_time
                )
            # Check for warning alert
            elif 'warning' in thresholds and current_value >= thresholds['warning']:
                await self._trigger_alert(
                    metric_name, AlertLevel.WARNING, current_value, 
                    thresholds['warning'], current_time
                )
            else:
                # Check if we need to resolve an existing alert
                await self._resolve_alert(metric_name, current_time)
    
    async def _trigger_alert(self, metric_name: str, level: AlertLevel, 
                           current_value: float, threshold: float,
                           timestamp: datetime):
        """Trigger an alert."""
        alert_key = f"{metric_name}_{level.value}"
        
        # Check if alert is already active
        if alert_key in self.active_alerts:
            return
        
        alert_id = f"alert_{int(timestamp.timestamp())}_{alert_key}"
        
        alert = Alert(
            alert_id=alert_id,
            name=f"Redis {metric_name} {level.value}",
            level=level,
            message=f"Redis metric '{metric_name}' is {current_value}, exceeding {level.value} threshold of {threshold}",
            metric_name=metric_name,
            current_value=current_value,
            threshold=threshold,
            created_at=timestamp
        )
        
        self.active_alerts[alert_key] = alert
        self.stats['alerts_triggered'] += 1
        
        # Log alert
        log_method = logger.critical if level == AlertLevel.CRITICAL else \
                    logger.error if level == AlertLevel.ERROR else \
                    logger.warning
        
        log_method(f"Redis Alert: {alert.message}")
        
        # Store alert
        await self._store_alert(alert)
    
    async def _resolve_alert(self, metric_name: str, timestamp: datetime):
        """Resolve active alerts for a metric."""
        resolved_alerts = []
        
        for level in AlertLevel:
            alert_key = f"{metric_name}_{level.value}"
            if alert_key in self.active_alerts:
                alert = self.active_alerts[alert_key]
                alert.resolved_at = timestamp
                resolved_alerts.append(alert)
                del self.active_alerts[alert_key]
                self.stats['alerts_resolved'] += 1
        
        if resolved_alerts:
            logger.info(f"Resolved {len(resolved_alerts)} alerts for metric: {metric_name}")
            
            # Store resolved alerts
            for alert in resolved_alerts:
                await self._store_alert(alert)
    
    async def _store_alert(self, alert: Alert):
        """Store alert in cache."""
        cache_manager = await self._get_cache_manager()
        
        try:
            alert_data = asdict(alert)
            alert_data['created_at'] = alert_data['created_at'].isoformat()
            alert_data['level'] = alert_data['level'].value
            
            if alert_data['resolved_at']:
                alert_data['resolved_at'] = alert_data['resolved_at'].isoformat()
            
            # Store alert (keep for 7 days)
            await cache_manager.set(
                f"alert:{alert.alert_id}", alert_data, 
                604800, self.namespace  # 7 days
            )
            
            # Add to alerts list
            await cache_manager.lpush(
                "alerts_history", alert.alert_id,
                namespace=self.namespace
            )
            
            # Keep only recent alerts (last 1000)
            await cache_manager.async_redis_client.ltrim(
                cache_manager._build_key("alerts_history", self.namespace),
                0, 999
            )
            
        except Exception as e:
            logger.error(f"Error storing alert: {e}")
    
    def set_alert_threshold(self, metric_name: str, level: str, threshold: float):
        """Set alert threshold for a metric."""
        if metric_name not in self.alert_thresholds:
            self.alert_thresholds[metric_name] = {}
        
        self.alert_thresholds[metric_name][level] = threshold
        logger.info(f"Set {level} threshold for {metric_name}: {threshold}")
    
    async def get_current_metrics(self) -> Dict[str, RedisMetric]:
        """Get the most recent metrics."""
        latest_metrics = {}
        
        for metric_name, history in self.metrics_history.items():
            if history:
                latest_metrics[metric_name] = history[-1]
        
        return latest_metrics
    
    async def get_metric_history(self, metric_name: str, 
                               duration: timedelta = timedelta(hours=1)) -> List[RedisMetric]:
        """Get metric history for a specific period."""
        if metric_name not in self.metrics_history:
            return []
        
        cutoff_time = datetime.utcnow() - duration
        history = self.metrics_history[metric_name]
        
        return [metric for metric in history if metric.timestamp >= cutoff_time]
    
    async def get_active_alerts(self) -> List[Alert]:
        """Get currently active alerts."""
        return list(self.active_alerts.values())
    
    async def get_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report."""
        current_metrics = await self.get_current_metrics()
        
        # Get key metrics
        hit_rate = current_metrics.get('hit_rate')
        used_memory = current_metrics.get('used_memory')
        maxmemory = current_metrics.get('maxmemory')
        connected_clients = current_metrics.get('connected_clients')
        
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'overview': {
                'hit_rate': hit_rate.value if hit_rate else 0,
                'memory_usage_mb': used_memory.value / 1024 / 1024 if used_memory else 0,
                'memory_usage_percent': (
                    (used_memory.value / maxmemory.value * 100) 
                    if used_memory and maxmemory and maxmemory.value > 0 else 0
                ),
                'connected_clients': connected_clients.value if connected_clients else 0
            },
            'recommendations': [],
            'alerts': {
                'active': len(self.active_alerts),
                'total_triggered': self.stats['alerts_triggered'],
                'total_resolved': self.stats['alerts_resolved']
            },
            'monitoring_stats': self.stats.copy()
        }
        
        # Generate recommendations
        if hit_rate:
            recommendations = self.performance_analyzer.analyze_cache_hit_ratio(hit_rate.value)
            report['recommendations'].extend(recommendations)
        
        if used_memory and maxmemory:
            recommendations = self.performance_analyzer.analyze_memory_usage(
                int(used_memory.value), int(maxmemory.value)
            )
            report['recommendations'].extend(recommendations)
        
        if connected_clients:
            # Assume max clients is 10000 if not available
            max_clients = current_metrics.get('maxclients')
            max_clients_value = max_clients.value if max_clients else 10000
            
            recommendations = self.performance_analyzer.analyze_connection_patterns(
                int(connected_clients.value), int(max_clients_value)
            )
            report['recommendations'].extend(recommendations)
        
        return report
    
    async def get_monitoring_statistics(self) -> Dict[str, Any]:
        """Get comprehensive monitoring statistics."""
        return {
            'monitoring_stats': self.stats.copy(),
            'alert_thresholds': self.alert_thresholds.copy(),
            'metrics_tracked': len(self.metrics_history),
            'active_alerts': len(self.active_alerts),
            'monitoring_interval': self.monitoring_interval,
            'is_monitoring': self.is_monitoring
        }


# Global Redis monitor instance
_redis_monitor: Optional[RedisMonitor] = None


async def get_redis_monitor() -> RedisMonitor:
    """Get or create global Redis monitor instance."""
    global _redis_monitor
    if _redis_monitor is None:
        _redis_monitor = RedisMonitor()
        await _redis_monitor.start_monitoring()
    return _redis_monitor


# Performance optimization utilities
class RedisOptimizer:
    """Redis performance optimization utilities."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None):
        self.cache_manager = cache_manager
    
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    async def analyze_key_patterns(self) -> Dict[str, Any]:
        """Analyze key patterns for optimization opportunities."""
        cache_manager = await self._get_cache_manager()
        
        try:
            # Sample keys for analysis
            sample_keys = []
            
            if cache_manager.is_cluster:
                # For cluster, sample from each node
                for node in cache_manager.async_redis_client.get_nodes():
                    keys = await node.keys("*")
                    if keys:
                        sample_keys.extend(keys[:100])  # Limit per node
            else:
                keys = await cache_manager.async_redis_client.keys("*")
                sample_keys = keys[:500]  # Limit total keys
            
            # Analyze patterns
            namespace_counts = defaultdict(int)
            key_lengths = []
            ttl_analysis = defaultdict(list)
            
            for key in sample_keys:
                # Namespace analysis
                if ":" in key:
                    namespace = key.split(":")[0]
                    namespace_counts[namespace] += 1
                
                # Key length analysis
                key_lengths.append(len(key))
                
                # TTL analysis
                try:
                    ttl = await cache_manager.async_redis_client.ttl(key)
                    if ttl > 0:
                        ttl_analysis[namespace].append(ttl)
                except:
                    pass
            
            # Generate analysis
            analysis = {
                'total_keys_analyzed': len(sample_keys),
                'namespace_distribution': dict(namespace_counts),
                'key_length_stats': {
                    'avg': sum(key_lengths) / len(key_lengths) if key_lengths else 0,
                    'min': min(key_lengths) if key_lengths else 0,
                    'max': max(key_lengths) if key_lengths else 0
                },
                'ttl_analysis': {
                    namespace: {
                        'avg_ttl': sum(ttls) / len(ttls) if ttls else 0,
                        'min_ttl': min(ttls) if ttls else 0,
                        'max_ttl': max(ttls) if ttls else 0,
                        'count': len(ttls)
                    } for namespace, ttls in ttl_analysis.items()
                },
                'recommendations': []
            }
            
            # Generate recommendations
            if key_lengths and max(key_lengths) > 500:
                analysis['recommendations'].append(
                    "Some keys are very long (>500 chars), consider shorter key names"
                )
            
            if len(namespace_counts) > 50:
                analysis['recommendations'].append(
                    "High number of namespaces detected, consider consolidation"
                )
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing key patterns: {e}")
            return {'error': str(e)}
    
    async def suggest_memory_optimizations(self) -> List[str]:
        """Suggest memory optimization strategies."""
        cache_manager = await self._get_cache_manager()
        suggestions = []
        
        try:
            info = await cache_manager.async_redis_client.info()
            
            # Check fragmentation ratio
            fragmentation_ratio = info.get('mem_fragmentation_ratio', 1.0)
            if fragmentation_ratio > 1.5:
                suggestions.extend([
                    "High memory fragmentation detected",
                    "Consider running MEMORY PURGE command",
                    "Review data structure usage patterns"
                ])
            
            # Check for expired keys
            expired_keys = info.get('expired_keys', 0)
            if expired_keys > 10000:
                suggestions.append(
                    "High number of expired keys, consider more aggressive cleanup"
                )
            
            # Check evicted keys
            evicted_keys = info.get('evicted_keys', 0)
            if evicted_keys > 1000:
                suggestions.extend([
                    "Keys are being evicted due to memory pressure",
                    "Consider increasing maxmemory limit",
                    "Review key expiration policies"
                ])
            
        except Exception as e:
            logger.error(f"Error generating memory optimization suggestions: {e}")
        
        return suggestions
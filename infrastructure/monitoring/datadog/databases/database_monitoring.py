"""
DataDog Database Monitoring
Comprehensive database monitoring for PostgreSQL, Redis, and Elasticsearch
"""

import logging
from typing import Dict, List, Any, Optional
from datadog import initialize, statsd
from datadog.api.metrics import Metrics
from datetime import datetime
import psycopg2
import redis
import elasticsearch
import asyncio
import time
from contextlib import asynccontextmanager
import json
from concurrent.futures import ThreadPoolExecutor

# Initialize DataDog
initialize(
    api_key="<your-api-key>",
    app_key="<your-app-key>",
    host_name="enterprise-data-platform"
)

logger = logging.getLogger(__name__)

class DataDogDatabaseMonitor:
    """Comprehensive database monitoring for enterprise data platform"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.executor = ThreadPoolExecutor(max_workers=10)
        
        # Database connections
        self.postgres_pools = {}
        self.redis_pools = {}
        self.elasticsearch_clients = {}
        
        # Monitoring configuration
        self.monitoring_interval = config.get('monitoring_interval', 60)
        self.alert_thresholds = config.get('alert_thresholds', {})
        
        # Performance baselines
        self.performance_baselines = {
            'postgres': {
                'query_time_p95': 100,  # ms
                'connections_max': 100,
                'cache_hit_ratio_min': 0.95
            },
            'redis': {
                'memory_usage_max': 0.8,  # 80% of max memory
                'keyspace_hit_ratio_min': 0.95,
                'connected_clients_max': 1000
            },
            'elasticsearch': {
                'query_time_p95': 50,  # ms
                'indexing_rate_min': 1000,  # docs/sec
                'search_rate_max': 10000   # queries/sec
            }
        }
        
        self._setup_database_connections()
    
    def _setup_database_connections(self):
        """Setup database connections for monitoring"""
        try:
            # PostgreSQL connections
            for db_name, db_config in self.config.get('postgresql', {}).items():
                self.postgres_pools[db_name] = {
                    'host': db_config['host'],
                    'port': db_config['port'],
                    'database': db_config['database'],
                    'user': db_config['user'],
                    'password': db_config['password']
                }
            
            # Redis connections
            for redis_name, redis_config in self.config.get('redis', {}).items():
                self.redis_pools[redis_name] = redis.Redis(
                    host=redis_config['host'],
                    port=redis_config['port'],
                    password=redis_config.get('password'),
                    decode_responses=True
                )
            
            # Elasticsearch connections
            for es_name, es_config in self.config.get('elasticsearch', {}).items():
                self.elasticsearch_clients[es_name] = elasticsearch.Elasticsearch(
                    hosts=[{
                        'host': es_config['host'],
                        'port': es_config['port']
                    }],
                    http_auth=es_config.get('auth')
                )
            
            logger.info("Database connections initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database connections: {e}")
            raise
    
    async def monitor_postgresql(self, db_name: str) -> Dict[str, Any]:
        """Monitor PostgreSQL database metrics"""
        try:
            db_config = self.postgres_pools[db_name]
            
            # Connect to database
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            
            metrics = {}
            
            # Basic connection metrics
            cursor.execute("""
                SELECT count(*) as active_connections
                FROM pg_stat_activity 
                WHERE state = 'active';
            """)
            metrics['active_connections'] = cursor.fetchone()[0]
            
            # Database statistics
            cursor.execute("""
                SELECT 
                    pg_database_size(current_database()) as db_size,
                    numbackends as connections,
                    xact_commit as commits,
                    xact_rollback as rollbacks,
                    blks_read as blocks_read,
                    blks_hit as blocks_hit,
                    tup_returned as tuples_returned,
                    tup_fetched as tuples_fetched,
                    tup_inserted as tuples_inserted,
                    tup_updated as tuples_updated,
                    tup_deleted as tuples_deleted
                FROM pg_stat_database 
                WHERE datname = current_database();
            """)
            
            db_stats = cursor.fetchone()
            if db_stats:
                metrics.update({
                    'database_size': db_stats[0],
                    'connections': db_stats[1],
                    'commits': db_stats[2],
                    'rollbacks': db_stats[3],
                    'blocks_read': db_stats[4],
                    'blocks_hit': db_stats[5],
                    'tuples_returned': db_stats[6],
                    'tuples_fetched': db_stats[7],
                    'tuples_inserted': db_stats[8],
                    'tuples_updated': db_stats[9],
                    'tuples_deleted': db_stats[10]
                })
            
            # Cache hit ratio
            if metrics.get('blocks_read', 0) + metrics.get('blocks_hit', 0) > 0:
                cache_hit_ratio = metrics['blocks_hit'] / (metrics['blocks_read'] + metrics['blocks_hit'])
                metrics['cache_hit_ratio'] = cache_hit_ratio
            
            # Slow queries
            cursor.execute("""
                SELECT 
                    query,
                    calls,
                    total_time,
                    mean_time,
                    max_time,
                    rows
                FROM pg_stat_statements 
                WHERE mean_time > 100  -- queries slower than 100ms
                ORDER BY mean_time DESC 
                LIMIT 10;
            """)
            
            slow_queries = cursor.fetchall()
            metrics['slow_queries'] = len(slow_queries)
            
            # Table statistics
            cursor.execute("""
                SELECT 
                    schemaname,
                    tablename,
                    n_tup_ins as inserts,
                    n_tup_upd as updates,
                    n_tup_del as deletes,
                    n_live_tup as live_tuples,
                    n_dead_tup as dead_tuples,
                    last_vacuum,
                    last_autovacuum,
                    last_analyze,
                    last_autoanalyze
                FROM pg_stat_user_tables
                ORDER BY n_live_tup DESC
                LIMIT 20;
            """)
            
            table_stats = cursor.fetchall()
            metrics['monitored_tables'] = len(table_stats)
            
            # Lock statistics
            cursor.execute("""
                SELECT 
                    mode,
                    count(*) as lock_count
                FROM pg_locks
                WHERE granted = true
                GROUP BY mode;
            """)
            
            lock_stats = cursor.fetchall()
            metrics['active_locks'] = sum(lock[1] for lock in lock_stats)
            
            # Send metrics to DataDog
            await self._send_postgresql_metrics(db_name, metrics)
            
            cursor.close()
            conn.close()
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error monitoring PostgreSQL {db_name}: {e}")
            statsd.increment('postgresql.monitoring.error', tags=[f'database:{db_name}'])
            return {}
    
    async def monitor_redis(self, redis_name: str) -> Dict[str, Any]:
        """Monitor Redis database metrics"""
        try:
            redis_client = self.redis_pools[redis_name]
            
            # Get Redis info
            info = redis_client.info()
            
            metrics = {
                'connected_clients': info.get('connected_clients', 0),
                'used_memory': info.get('used_memory', 0),
                'used_memory_rss': info.get('used_memory_rss', 0),
                'used_memory_peak': info.get('used_memory_peak', 0),
                'maxmemory': info.get('maxmemory', 0),
                'keyspace_hits': info.get('keyspace_hits', 0),
                'keyspace_misses': info.get('keyspace_misses', 0),
                'instantaneous_ops_per_sec': info.get('instantaneous_ops_per_sec', 0),
                'total_commands_processed': info.get('total_commands_processed', 0),
                'total_connections_received': info.get('total_connections_received', 0),
                'rejected_connections': info.get('rejected_connections', 0),
                'expired_keys': info.get('expired_keys', 0),
                'evicted_keys': info.get('evicted_keys', 0),
                'uptime_in_seconds': info.get('uptime_in_seconds', 0)
            }
            
            # Calculate derived metrics
            if metrics['keyspace_hits'] + metrics['keyspace_misses'] > 0:
                hit_ratio = metrics['keyspace_hits'] / (metrics['keyspace_hits'] + metrics['keyspace_misses'])
                metrics['keyspace_hit_ratio'] = hit_ratio
            
            if metrics['maxmemory'] > 0:
                memory_usage_ratio = metrics['used_memory'] / metrics['maxmemory']
                metrics['memory_usage_ratio'] = memory_usage_ratio
            
            # Get keyspace information
            keyspace_info = redis_client.info('keyspace')
            total_keys = 0
            for db_key, db_info in keyspace_info.items():
                if db_key.startswith('db'):
                    db_stats = dict(item.split('=') for item in db_info.split(','))
                    total_keys += int(db_stats.get('keys', 0))
            
            metrics['total_keys'] = total_keys
            
            # Get slowlog
            slowlog = redis_client.slowlog_get(10)
            metrics['slow_queries'] = len(slowlog)
            
            # Send metrics to DataDog
            await self._send_redis_metrics(redis_name, metrics)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error monitoring Redis {redis_name}: {e}")
            statsd.increment('redis.monitoring.error', tags=[f'redis:{redis_name}'])
            return {}
    
    async def monitor_elasticsearch(self, es_name: str) -> Dict[str, Any]:
        """Monitor Elasticsearch cluster metrics"""
        try:
            es_client = self.elasticsearch_clients[es_name]
            
            # Cluster health
            cluster_health = es_client.cluster.health()
            
            # Cluster stats
            cluster_stats = es_client.cluster.stats()
            
            # Node stats
            node_stats = es_client.nodes.stats()
            
            metrics = {
                # Cluster health
                'cluster_status': cluster_health.get('status', 'unknown'),
                'number_of_nodes': cluster_health.get('number_of_nodes', 0),
                'number_of_data_nodes': cluster_health.get('number_of_data_nodes', 0),
                'active_primary_shards': cluster_health.get('active_primary_shards', 0),
                'active_shards': cluster_health.get('active_shards', 0),
                'relocating_shards': cluster_health.get('relocating_shards', 0),
                'initializing_shards': cluster_health.get('initializing_shards', 0),
                'unassigned_shards': cluster_health.get('unassigned_shards', 0),
                
                # Cluster stats
                'indices_count': cluster_stats['indices']['count'],
                'documents_count': cluster_stats['indices']['docs']['count'],
                'documents_deleted': cluster_stats['indices']['docs']['deleted'],
                'store_size_bytes': cluster_stats['indices']['store']['size_in_bytes'],
                'segments_count': cluster_stats['indices']['segments']['count'],
                'field_data_memory_size': cluster_stats['indices']['fielddata']['memory_size_in_bytes'],
                'query_cache_memory_size': cluster_stats['indices']['query_cache']['memory_size_in_bytes'],
            }
            
            # Node-level metrics aggregation
            total_heap_used = 0
            total_heap_max = 0
            total_disk_used = 0
            total_disk_available = 0
            
            for node_id, node_data in node_stats['nodes'].items():
                jvm_stats = node_data.get('jvm', {})
                fs_stats = node_data.get('fs', {}).get('total', {})
                
                total_heap_used += jvm_stats.get('mem', {}).get('heap_used_in_bytes', 0)
                total_heap_max += jvm_stats.get('mem', {}).get('heap_max_in_bytes', 0)
                total_disk_used += fs_stats.get('used_in_bytes', 0)
                total_disk_available += fs_stats.get('available_in_bytes', 0)
            
            metrics.update({
                'jvm_heap_used_bytes': total_heap_used,
                'jvm_heap_max_bytes': total_heap_max,
                'fs_used_bytes': total_disk_used,
                'fs_available_bytes': total_disk_available
            })
            
            # Calculate derived metrics
            if total_heap_max > 0:
                heap_usage_ratio = total_heap_used / total_heap_max
                metrics['heap_usage_ratio'] = heap_usage_ratio
            
            if total_disk_used + total_disk_available > 0:
                disk_usage_ratio = total_disk_used / (total_disk_used + total_disk_available)
                metrics['disk_usage_ratio'] = disk_usage_ratio
            
            # Get index statistics
            index_stats = es_client.indices.stats()
            
            total_search_query_total = 0
            total_search_query_time = 0
            total_indexing_index_total = 0
            total_indexing_index_time = 0
            
            for index_name, index_data in index_stats['indices'].items():
                search_stats = index_data.get('total', {}).get('search', {})
                indexing_stats = index_data.get('total', {}).get('indexing', {})
                
                total_search_query_total += search_stats.get('query_total', 0)
                total_search_query_time += search_stats.get('query_time_in_millis', 0)
                total_indexing_index_total += indexing_stats.get('index_total', 0)
                total_indexing_index_time += indexing_stats.get('index_time_in_millis', 0)
            
            metrics.update({
                'search_query_total': total_search_query_total,
                'search_query_time_millis': total_search_query_time,
                'indexing_index_total': total_indexing_index_total,
                'indexing_index_time_millis': total_indexing_index_time
            })
            
            # Calculate average query and indexing times
            if total_search_query_total > 0:
                avg_query_time = total_search_query_time / total_search_query_total
                metrics['avg_query_time_millis'] = avg_query_time
            
            if total_indexing_index_total > 0:
                avg_indexing_time = total_indexing_index_time / total_indexing_index_total
                metrics['avg_indexing_time_millis'] = avg_indexing_time
            
            # Send metrics to DataDog
            await self._send_elasticsearch_metrics(es_name, metrics)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error monitoring Elasticsearch {es_name}: {e}")
            statsd.increment('elasticsearch.monitoring.error', tags=[f'cluster:{es_name}'])
            return {}
    
    async def _send_postgresql_metrics(self, db_name: str, metrics: Dict[str, Any]):
        """Send PostgreSQL metrics to DataDog"""
        timestamp = time.time()
        tags = [f'database:{db_name}', 'db_type:postgresql']
        
        # Connection metrics
        statsd.gauge('postgresql.connections.active', metrics.get('active_connections', 0), tags=tags)
        statsd.gauge('postgresql.connections.total', metrics.get('connections', 0), tags=tags)
        
        # Performance metrics
        statsd.gauge('postgresql.database.size', metrics.get('database_size', 0), tags=tags)
        statsd.gauge('postgresql.transactions.commits', metrics.get('commits', 0), tags=tags)
        statsd.gauge('postgresql.transactions.rollbacks', metrics.get('rollbacks', 0), tags=tags)
        
        # Cache metrics
        statsd.gauge('postgresql.cache.hit_ratio', metrics.get('cache_hit_ratio', 0), tags=tags)
        statsd.gauge('postgresql.cache.blocks_read', metrics.get('blocks_read', 0), tags=tags)
        statsd.gauge('postgresql.cache.blocks_hit', metrics.get('blocks_hit', 0), tags=tags)
        
        # Query metrics
        statsd.gauge('postgresql.queries.slow_queries', metrics.get('slow_queries', 0), tags=tags)
        statsd.gauge('postgresql.locks.active', metrics.get('active_locks', 0), tags=tags)
        
        # Tuple metrics
        statsd.gauge('postgresql.tuples.returned', metrics.get('tuples_returned', 0), tags=tags)
        statsd.gauge('postgresql.tuples.fetched', metrics.get('tuples_fetched', 0), tags=tags)
        statsd.gauge('postgresql.tuples.inserted', metrics.get('tuples_inserted', 0), tags=tags)
        statsd.gauge('postgresql.tuples.updated', metrics.get('tuples_updated', 0), tags=tags)
        statsd.gauge('postgresql.tuples.deleted', metrics.get('tuples_deleted', 0), tags=tags)
        
        # Check alert thresholds
        await self._check_postgresql_alerts(db_name, metrics, tags)
    
    async def _send_redis_metrics(self, redis_name: str, metrics: Dict[str, Any]):
        """Send Redis metrics to DataDog"""
        timestamp = time.time()
        tags = [f'redis:{redis_name}', 'db_type:redis']
        
        # Connection metrics
        statsd.gauge('redis.connections.connected_clients', metrics.get('connected_clients', 0), tags=tags)
        statsd.gauge('redis.connections.total_connections_received', metrics.get('total_connections_received', 0), tags=tags)
        statsd.gauge('redis.connections.rejected_connections', metrics.get('rejected_connections', 0), tags=tags)
        
        # Memory metrics
        statsd.gauge('redis.memory.used_memory', metrics.get('used_memory', 0), tags=tags)
        statsd.gauge('redis.memory.used_memory_rss', metrics.get('used_memory_rss', 0), tags=tags)
        statsd.gauge('redis.memory.used_memory_peak', metrics.get('used_memory_peak', 0), tags=tags)
        statsd.gauge('redis.memory.usage_ratio', metrics.get('memory_usage_ratio', 0), tags=tags)
        
        # Performance metrics
        statsd.gauge('redis.performance.keyspace_hit_ratio', metrics.get('keyspace_hit_ratio', 0), tags=tags)
        statsd.gauge('redis.performance.instantaneous_ops_per_sec', metrics.get('instantaneous_ops_per_sec', 0), tags=tags)
        statsd.gauge('redis.performance.total_commands_processed', metrics.get('total_commands_processed', 0), tags=tags)
        
        # Key metrics
        statsd.gauge('redis.keys.total_keys', metrics.get('total_keys', 0), tags=tags)
        statsd.gauge('redis.keys.expired_keys', metrics.get('expired_keys', 0), tags=tags)
        statsd.gauge('redis.keys.evicted_keys', metrics.get('evicted_keys', 0), tags=tags)
        
        # Slow queries
        statsd.gauge('redis.queries.slow_queries', metrics.get('slow_queries', 0), tags=tags)
        
        # Uptime
        statsd.gauge('redis.uptime.uptime_in_seconds', metrics.get('uptime_in_seconds', 0), tags=tags)
        
        # Check alert thresholds
        await self._check_redis_alerts(redis_name, metrics, tags)
    
    async def _send_elasticsearch_metrics(self, es_name: str, metrics: Dict[str, Any]):
        """Send Elasticsearch metrics to DataDog"""
        timestamp = time.time()
        tags = [f'cluster:{es_name}', 'db_type:elasticsearch']
        
        # Cluster health
        cluster_status_mapping = {'green': 0, 'yellow': 1, 'red': 2}
        cluster_status_value = cluster_status_mapping.get(metrics.get('cluster_status'), 3)
        statsd.gauge('elasticsearch.cluster.status', cluster_status_value, tags=tags)
        statsd.gauge('elasticsearch.cluster.nodes', metrics.get('number_of_nodes', 0), tags=tags)
        statsd.gauge('elasticsearch.cluster.data_nodes', metrics.get('number_of_data_nodes', 0), tags=tags)
        
        # Shard metrics
        statsd.gauge('elasticsearch.shards.active_primary', metrics.get('active_primary_shards', 0), tags=tags)
        statsd.gauge('elasticsearch.shards.active', metrics.get('active_shards', 0), tags=tags)
        statsd.gauge('elasticsearch.shards.relocating', metrics.get('relocating_shards', 0), tags=tags)
        statsd.gauge('elasticsearch.shards.initializing', metrics.get('initializing_shards', 0), tags=tags)
        statsd.gauge('elasticsearch.shards.unassigned', metrics.get('unassigned_shards', 0), tags=tags)
        
        # Index metrics
        statsd.gauge('elasticsearch.indices.count', metrics.get('indices_count', 0), tags=tags)
        statsd.gauge('elasticsearch.documents.count', metrics.get('documents_count', 0), tags=tags)
        statsd.gauge('elasticsearch.documents.deleted', metrics.get('documents_deleted', 0), tags=tags)
        statsd.gauge('elasticsearch.store.size_bytes', metrics.get('store_size_bytes', 0), tags=tags)
        
        # Memory metrics
        statsd.gauge('elasticsearch.jvm.heap_used_bytes', metrics.get('jvm_heap_used_bytes', 0), tags=tags)
        statsd.gauge('elasticsearch.jvm.heap_max_bytes', metrics.get('jvm_heap_max_bytes', 0), tags=tags)
        statsd.gauge('elasticsearch.jvm.heap_usage_ratio', metrics.get('heap_usage_ratio', 0), tags=tags)
        
        # Disk metrics
        statsd.gauge('elasticsearch.fs.used_bytes', metrics.get('fs_used_bytes', 0), tags=tags)
        statsd.gauge('elasticsearch.fs.available_bytes', metrics.get('fs_available_bytes', 0), tags=tags)
        statsd.gauge('elasticsearch.fs.usage_ratio', metrics.get('disk_usage_ratio', 0), tags=tags)
        
        # Search metrics
        statsd.gauge('elasticsearch.search.query_total', metrics.get('search_query_total', 0), tags=tags)
        statsd.gauge('elasticsearch.search.query_time_millis', metrics.get('search_query_time_millis', 0), tags=tags)
        statsd.gauge('elasticsearch.search.avg_query_time_millis', metrics.get('avg_query_time_millis', 0), tags=tags)
        
        # Indexing metrics
        statsd.gauge('elasticsearch.indexing.index_total', metrics.get('indexing_index_total', 0), tags=tags)
        statsd.gauge('elasticsearch.indexing.index_time_millis', metrics.get('indexing_index_time_millis', 0), tags=tags)
        statsd.gauge('elasticsearch.indexing.avg_indexing_time_millis', metrics.get('avg_indexing_time_millis', 0), tags=tags)
        
        # Check alert thresholds
        await self._check_elasticsearch_alerts(es_name, metrics, tags)
    
    async def _check_postgresql_alerts(self, db_name: str, metrics: Dict[str, Any], tags: List[str]):
        """Check PostgreSQL alert thresholds"""
        baselines = self.performance_baselines['postgres']
        
        # Check cache hit ratio
        if metrics.get('cache_hit_ratio', 1.0) < baselines['cache_hit_ratio_min']:
            statsd.event(
                title=f"PostgreSQL Cache Hit Ratio Alert - {db_name}",
                text=f"Cache hit ratio {metrics['cache_hit_ratio']:.3f} below threshold {baselines['cache_hit_ratio_min']}",
                alert_type='warning',
                tags=tags
            )
        
        # Check connection count
        if metrics.get('connections', 0) > baselines['connections_max']:
            statsd.event(
                title=f"PostgreSQL Connection Alert - {db_name}",
                text=f"Connection count {metrics['connections']} exceeds threshold {baselines['connections_max']}",
                alert_type='warning',
                tags=tags
            )
    
    async def _check_redis_alerts(self, redis_name: str, metrics: Dict[str, Any], tags: List[str]):
        """Check Redis alert thresholds"""
        baselines = self.performance_baselines['redis']
        
        # Check memory usage
        if metrics.get('memory_usage_ratio', 0) > baselines['memory_usage_max']:
            statsd.event(
                title=f"Redis Memory Usage Alert - {redis_name}",
                text=f"Memory usage {metrics['memory_usage_ratio']:.3f} exceeds threshold {baselines['memory_usage_max']}",
                alert_type='warning',
                tags=tags
            )
        
        # Check keyspace hit ratio
        if metrics.get('keyspace_hit_ratio', 1.0) < baselines['keyspace_hit_ratio_min']:
            statsd.event(
                title=f"Redis Hit Ratio Alert - {redis_name}",
                text=f"Keyspace hit ratio {metrics['keyspace_hit_ratio']:.3f} below threshold {baselines['keyspace_hit_ratio_min']}",
                alert_type='warning',
                tags=tags
            )
    
    async def _check_elasticsearch_alerts(self, es_name: str, metrics: Dict[str, Any], tags: List[str]):
        """Check Elasticsearch alert thresholds"""
        baselines = self.performance_baselines['elasticsearch']
        
        # Check cluster status
        if metrics.get('cluster_status') == 'red':
            statsd.event(
                title=f"Elasticsearch Cluster Status Alert - {es_name}",
                text=f"Cluster status is RED - immediate attention required",
                alert_type='error',
                tags=tags
            )
        elif metrics.get('cluster_status') == 'yellow':
            statsd.event(
                title=f"Elasticsearch Cluster Status Warning - {es_name}",
                text=f"Cluster status is YELLOW - monitoring required",
                alert_type='warning',
                tags=tags
            )
        
        # Check heap usage
        if metrics.get('heap_usage_ratio', 0) > 0.8:
            statsd.event(
                title=f"Elasticsearch Heap Usage Alert - {es_name}",
                text=f"JVM heap usage {metrics['heap_usage_ratio']:.3f} exceeds 80%",
                alert_type='warning',
                tags=tags
            )
        
        # Check disk usage
        if metrics.get('disk_usage_ratio', 0) > 0.9:
            statsd.event(
                title=f"Elasticsearch Disk Usage Alert - {es_name}",
                text=f"Disk usage {metrics['disk_usage_ratio']:.3f} exceeds 90%",
                alert_type='error',
                tags=tags
            )
    
    async def run_monitoring_loop(self):
        """Run continuous monitoring loop"""
        logger.info("Starting database monitoring loop")
        
        while True:
            try:
                # Monitor all databases in parallel
                tasks = []
                
                # PostgreSQL monitoring
                for db_name in self.postgres_pools.keys():
                    tasks.append(self.monitor_postgresql(db_name))
                
                # Redis monitoring
                for redis_name in self.redis_pools.keys():
                    tasks.append(self.monitor_redis(redis_name))
                
                # Elasticsearch monitoring
                for es_name in self.elasticsearch_clients.keys():
                    tasks.append(self.monitor_elasticsearch(es_name))
                
                # Execute all monitoring tasks
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Log results
                successful_monitors = sum(1 for result in results if not isinstance(result, Exception))
                failed_monitors = len(results) - successful_monitors
                
                logger.info(f"Monitoring cycle completed: {successful_monitors} successful, {failed_monitors} failed")
                
                # Send health check metric
                statsd.gauge('database.monitoring.health', 1, tags=['status:healthy'])
                
                # Wait for next monitoring interval
                await asyncio.sleep(self.monitoring_interval)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                statsd.increment('database.monitoring.loop_error')
                await asyncio.sleep(60)  # Wait 1 minute before retrying

# Configuration example
DATABASE_MONITORING_CONFIG = {
    'monitoring_interval': 60,  # seconds
    'postgresql': {
        'primary': {
            'host': 'postgres-primary.default.svc.cluster.local',
            'port': 5432,
            'database': 'postgres',
            'user': 'datadog',
            'password': 'secure-password'
        },
        'replica': {
            'host': 'postgres-replica.default.svc.cluster.local',
            'port': 5432,
            'database': 'postgres',
            'user': 'datadog',
            'password': 'secure-password'
        }
    },
    'redis': {
        'cache': {
            'host': 'redis-cache.default.svc.cluster.local',
            'port': 6379,
            'password': 'redis-password'
        },
        'session': {
            'host': 'redis-session.default.svc.cluster.local',
            'port': 6379,
            'password': 'redis-password'
        }
    },
    'elasticsearch': {
        'logs': {
            'host': 'elasticsearch-logs.default.svc.cluster.local',
            'port': 9200,
            'auth': ('elastic', 'elastic-password')
        },
        'metrics': {
            'host': 'elasticsearch-metrics.default.svc.cluster.local',
            'port': 9200,
            'auth': ('elastic', 'elastic-password')
        }
    },
    'alert_thresholds': {
        'postgresql': {
            'cache_hit_ratio_min': 0.95,
            'connections_max': 100,
            'slow_query_threshold_ms': 100
        },
        'redis': {
            'memory_usage_max': 0.8,
            'keyspace_hit_ratio_min': 0.95,
            'connected_clients_max': 1000
        },
        'elasticsearch': {
            'heap_usage_max': 0.8,
            'disk_usage_max': 0.9,
            'query_time_p95_max': 50
        }
    }
}

async def main():
    """Main entry point for database monitoring"""
    monitor = DataDogDatabaseMonitor(DATABASE_MONITORING_CONFIG)
    await monitor.run_monitoring_loop()

if __name__ == "__main__":
    asyncio.run(main())
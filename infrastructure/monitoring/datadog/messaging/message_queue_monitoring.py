"""
DataDog Message Queue Monitoring
Comprehensive monitoring for RabbitMQ and Kafka message queues
"""

import logging
from typing import Dict, List, Any, Optional
from datadog import initialize, statsd
from datadog.api.metrics import Metrics
from datetime import datetime, timedelta
import asyncio
import time
import json
import requests
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.structs import TopicPartition
from concurrent.futures import ThreadPoolExecutor
import pika
from urllib.parse import quote

# Initialize DataDog
initialize(
    api_key="<your-api-key>",
    app_key="<your-app-key>",
    host_name="enterprise-data-platform"
)

logger = logging.getLogger(__name__)

class DataDogMessageQueueMonitor:
    """Comprehensive message queue monitoring for enterprise data platform"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.executor = ThreadPoolExecutor(max_workers=20)
        
        # Connection pools
        self.rabbitmq_connections = {}
        self.kafka_admin_clients = {}
        self.kafka_consumers = {}
        
        # Monitoring configuration
        self.monitoring_interval = config.get('monitoring_interval', 60)
        self.alert_thresholds = config.get('alert_thresholds', {})
        
        # Performance baselines
        self.performance_baselines = {
            'rabbitmq': {
                'queue_depth_max': 10000,
                'message_rate_min': 100,
                'memory_usage_max': 0.8,
                'disk_free_min': 0.2
            },
            'kafka': {
                'consumer_lag_max': 1000,
                'partition_count_max': 100,
                'broker_disk_usage_max': 0.8,
                'under_replicated_partitions_max': 0
            }
        }
        
        self._setup_message_queue_connections()
    
    def _setup_message_queue_connections(self):
        """Setup message queue connections for monitoring"""
        try:
            # RabbitMQ connections
            for rabbit_name, rabbit_config in self.config.get('rabbitmq', {}).items():
                connection_params = pika.ConnectionParameters(
                    host=rabbit_config['host'],
                    port=rabbit_config['port'],
                    credentials=pika.PlainCredentials(
                        rabbit_config['username'],
                        rabbit_config['password']
                    )
                )
                self.rabbitmq_connections[rabbit_name] = {
                    'params': connection_params,
                    'management_url': f"http://{rabbit_config['host']}:{rabbit_config.get('management_port', 15672)}",
                    'auth': (rabbit_config['username'], rabbit_config['password'])
                }
            
            # Kafka connections
            for kafka_name, kafka_config in self.config.get('kafka', {}).items():
                bootstrap_servers = kafka_config['bootstrap_servers']
                
                # Admin client
                self.kafka_admin_clients[kafka_name] = KafkaAdminClient(
                    bootstrap_servers=bootstrap_servers,
                    client_id=f'datadog-monitor-{kafka_name}'
                )
                
                # Consumer for monitoring
                self.kafka_consumers[kafka_name] = {
                    'bootstrap_servers': bootstrap_servers,
                    'config': kafka_config
                }
            
            logger.info("Message queue connections initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize message queue connections: {e}")
            raise
    
    async def monitor_rabbitmq(self, rabbit_name: str) -> Dict[str, Any]:
        """Monitor RabbitMQ cluster metrics"""
        try:
            rabbit_config = self.rabbitmq_connections[rabbit_name]
            base_url = rabbit_config['management_url']
            auth = rabbit_config['auth']
            
            metrics = {}
            
            # Get overview
            overview_response = requests.get(f"{base_url}/api/overview", auth=auth, timeout=30)
            if overview_response.status_code == 200:
                overview = overview_response.json()
                
                # Basic cluster metrics
                metrics.update({
                    'management_version': overview.get('management_version', 'unknown'),
                    'rabbitmq_version': overview.get('rabbitmq_version', 'unknown'),
                    'erlang_version': overview.get('erlang_version', 'unknown')
                })
                
                # Message stats
                message_stats = overview.get('message_stats', {})
                metrics.update({
                    'messages_published': message_stats.get('publish', 0),
                    'messages_published_rate': message_stats.get('publish_details', {}).get('rate', 0),
                    'messages_delivered': message_stats.get('deliver_get', 0),
                    'messages_delivered_rate': message_stats.get('deliver_get_details', {}).get('rate', 0),
                    'messages_acknowledged': message_stats.get('ack', 0),
                    'messages_acknowledged_rate': message_stats.get('ack_details', {}).get('rate', 0),
                    'messages_redelivered': message_stats.get('redeliver', 0),
                    'messages_redelivered_rate': message_stats.get('redeliver_details', {}).get('rate', 0)
                })
                
                # Queue totals
                queue_totals = overview.get('queue_totals', {})
                metrics.update({
                    'total_messages': queue_totals.get('messages', 0),
                    'messages_ready': queue_totals.get('messages_ready', 0),
                    'messages_unacknowledged': queue_totals.get('messages_unacknowledged', 0)
                })
                
                # Object totals
                object_totals = overview.get('object_totals', {})
                metrics.update({
                    'total_connections': object_totals.get('connections', 0),
                    'total_channels': object_totals.get('channels', 0),
                    'total_exchanges': object_totals.get('exchanges', 0),
                    'total_queues': object_totals.get('queues', 0),
                    'total_consumers': object_totals.get('consumers', 0)
                })
            
            # Get nodes info
            nodes_response = requests.get(f"{base_url}/api/nodes", auth=auth, timeout=30)
            if nodes_response.status_code == 200:
                nodes = nodes_response.json()
                
                total_memory_used = 0
                total_memory_limit = 0
                total_disk_free = 0
                total_disk_limit = 0
                total_fd_used = 0
                total_fd_total = 0
                running_nodes = 0
                
                for node in nodes:
                    if node.get('running', False):
                        running_nodes += 1
                        total_memory_used += node.get('mem_used', 0)
                        total_memory_limit += node.get('mem_limit', 0)
                        total_disk_free += node.get('disk_free', 0)
                        total_disk_limit += node.get('disk_free_limit', 0)
                        total_fd_used += node.get('fd_used', 0)
                        total_fd_total += node.get('fd_total', 0)
                
                metrics.update({
                    'running_nodes': running_nodes,
                    'total_nodes': len(nodes),
                    'memory_used': total_memory_used,
                    'memory_limit': total_memory_limit,
                    'disk_free': total_disk_free,
                    'disk_free_limit': total_disk_limit,
                    'fd_used': total_fd_used,
                    'fd_total': total_fd_total
                })
                
                # Calculate derived metrics
                if total_memory_limit > 0:
                    memory_usage_ratio = total_memory_used / total_memory_limit
                    metrics['memory_usage_ratio'] = memory_usage_ratio
                
                if total_fd_total > 0:
                    fd_usage_ratio = total_fd_used / total_fd_total
                    metrics['fd_usage_ratio'] = fd_usage_ratio
            
            # Get queues info
            queues_response = requests.get(f"{base_url}/api/queues", auth=auth, timeout=30)
            if queues_response.status_code == 200:
                queues = queues_response.json()
                
                queue_metrics = []
                max_queue_depth = 0
                total_consumers = 0
                
                for queue in queues:
                    queue_name = queue.get('name', 'unknown')
                    messages = queue.get('messages', 0)
                    consumers = queue.get('consumers', 0)
                    
                    max_queue_depth = max(max_queue_depth, messages)
                    total_consumers += consumers
                    
                    # Individual queue metrics
                    queue_tags = [f'queue:{queue_name}', f'vhost:{queue.get("vhost", "/")}']
                    statsd.gauge('rabbitmq.queue.messages', messages, 
                               tags=[f'rabbitmq:{rabbit_name}'] + queue_tags)
                    statsd.gauge('rabbitmq.queue.consumers', consumers, 
                               tags=[f'rabbitmq:{rabbit_name}'] + queue_tags)
                    
                    # Queue message rates
                    message_stats = queue.get('message_stats', {})
                    publish_rate = message_stats.get('publish_details', {}).get('rate', 0)
                    deliver_rate = message_stats.get('deliver_get_details', {}).get('rate', 0)
                    
                    statsd.gauge('rabbitmq.queue.publish_rate', publish_rate, 
                               tags=[f'rabbitmq:{rabbit_name}'] + queue_tags)
                    statsd.gauge('rabbitmq.queue.deliver_rate', deliver_rate, 
                               tags=[f'rabbitmq:{rabbit_name}'] + queue_tags)
                
                metrics.update({
                    'monitored_queues': len(queues),
                    'max_queue_depth': max_queue_depth,
                    'total_queue_consumers': total_consumers
                })
            
            # Get exchanges info
            exchanges_response = requests.get(f"{base_url}/api/exchanges", auth=auth, timeout=30)
            if exchanges_response.status_code == 200:
                exchanges = exchanges_response.json()
                
                for exchange in exchanges:
                    if exchange.get('name') and not exchange.get('name').startswith('amq.'):
                        exchange_name = exchange.get('name')
                        message_stats = exchange.get('message_stats', {})
                        
                        publish_in = message_stats.get('publish_in', 0)
                        publish_in_rate = message_stats.get('publish_in_details', {}).get('rate', 0)
                        publish_out = message_stats.get('publish_out', 0)
                        publish_out_rate = message_stats.get('publish_out_details', {}).get('rate', 0)
                        
                        exchange_tags = [f'exchange:{exchange_name}', f'type:{exchange.get("type", "unknown")}']
                        statsd.gauge('rabbitmq.exchange.publish_in_rate', publish_in_rate, 
                                   tags=[f'rabbitmq:{rabbit_name}'] + exchange_tags)
                        statsd.gauge('rabbitmq.exchange.publish_out_rate', publish_out_rate, 
                                   tags=[f'rabbitmq:{rabbit_name}'] + exchange_tags)
            
            # Send metrics to DataDog
            await self._send_rabbitmq_metrics(rabbit_name, metrics)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error monitoring RabbitMQ {rabbit_name}: {e}")
            statsd.increment('rabbitmq.monitoring.error', tags=[f'rabbitmq:{rabbit_name}'])
            return {}
    
    async def monitor_kafka(self, kafka_name: str) -> Dict[str, Any]:
        """Monitor Kafka cluster metrics"""
        try:
            kafka_config = self.kafka_consumers[kafka_name]
            admin_client = self.kafka_admin_clients[kafka_name]
            
            metrics = {}
            
            # Get cluster metadata
            cluster_metadata = admin_client.describe_cluster()
            
            # Get topic metadata
            topic_metadata = admin_client.list_topics()
            topics = topic_metadata.topics
            
            metrics.update({
                'cluster_id': cluster_metadata.cluster_id,
                'controller_id': cluster_metadata.controller.id if cluster_metadata.controller else -1,
                'broker_count': len(cluster_metadata.brokers),
                'topic_count': len(topics)
            })
            
            # Get broker information
            broker_metrics = {}
            for broker in cluster_metadata.brokers:
                broker_id = broker.id
                broker_metrics[broker_id] = {
                    'host': broker.host,
                    'port': broker.port,
                    'rack': broker.rack
                }
            
            # Monitor individual topics
            topic_metrics = {}
            total_partitions = 0
            
            for topic_name in list(topics)[:50]:  # Limit to avoid timeout
                try:
                    # Get topic metadata
                    topic_partitions = admin_client.describe_topics([topic_name])
                    if topic_name in topic_partitions:
                        partitions = topic_partitions[topic_name].partitions
                        partition_count = len(partitions)
                        total_partitions += partition_count
                        
                        topic_metrics[topic_name] = {
                            'partition_count': partition_count,
                            'replication_factor': len(partitions[0].replicas) if partitions else 0
                        }
                        
                        # Send topic-level metrics
                        topic_tags = [f'topic:{topic_name}']
                        statsd.gauge('kafka.topic.partitions', partition_count, 
                                   tags=[f'kafka:{kafka_name}'] + topic_tags)
                        
                        # Monitor consumer groups for this topic
                        consumer_groups = admin_client.list_consumer_groups()
                        for group_info in consumer_groups:
                            group_id = group_info.group_id
                            try:
                                group_description = admin_client.describe_consumer_groups([group_id])
                                if group_id in group_description:
                                    group_state = group_description[group_id].state
                                    member_count = len(group_description[group_id].members)
                                    
                                    group_tags = [f'group:{group_id}', f'topic:{topic_name}']
                                    statsd.gauge('kafka.consumer_group.members', member_count, 
                                               tags=[f'kafka:{kafka_name}'] + group_tags)
                                    
                                    # Get consumer group offsets and lag
                                    try:
                                        # Create a consumer to get lag information
                                        consumer = KafkaConsumer(
                                            bootstrap_servers=kafka_config['bootstrap_servers'],
                                            group_id=f'datadog-monitor-{group_id}',
                                            auto_offset_reset='latest',
                                            enable_auto_commit=False
                                        )
                                        
                                        partitions = [TopicPartition(topic_name, p) for p in range(partition_count)]
                                        
                                        # Get committed offsets
                                        committed = consumer.committed(set(partitions))
                                        
                                        # Get end offsets (high water mark)
                                        end_offsets = consumer.end_offsets(partitions)
                                        
                                        total_lag = 0
                                        for partition in partitions:
                                            committed_offset = committed.get(partition, 0) or 0
                                            end_offset = end_offsets.get(partition, 0)
                                            lag = max(0, end_offset - committed_offset)
                                            total_lag += lag
                                        
                                        statsd.gauge('kafka.consumer_group.lag', total_lag, 
                                                   tags=[f'kafka:{kafka_name}'] + group_tags)
                                        
                                        consumer.close()
                                        
                                    except Exception as lag_error:
                                        logger.warning(f"Could not calculate lag for group {group_id}: {lag_error}")
                                        
                            except Exception as group_error:
                                logger.warning(f"Could not describe consumer group {group_id}: {group_error}")
                                continue
                        
                except Exception as topic_error:
                    logger.warning(f"Error monitoring topic {topic_name}: {topic_error}")
                    continue
            
            metrics.update({
                'total_partitions': total_partitions,
                'monitored_topics': len(topic_metrics)
            })
            
            # Monitor broker JMX metrics (if available)
            await self._monitor_kafka_broker_metrics(kafka_name, broker_metrics)
            
            # Send metrics to DataDog
            await self._send_kafka_metrics(kafka_name, metrics)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error monitoring Kafka {kafka_name}: {e}")
            statsd.increment('kafka.monitoring.error', tags=[f'kafka:{kafka_name}'])
            return {}
    
    async def _monitor_kafka_broker_metrics(self, kafka_name: str, broker_metrics: Dict[str, Any]):
        """Monitor Kafka broker-level metrics via JMX (if available)"""
        try:
            # This would typically connect to JMX endpoints to get detailed broker metrics
            # For now, we'll simulate some key metrics
            
            for broker_id, broker_info in broker_metrics.items():
                broker_tags = [f'broker_id:{broker_id}', f'host:{broker_info["host"]}']
                
                # Simulate broker metrics (in real implementation, get from JMX)
                statsd.gauge('kafka.broker.bytes_in_per_sec', 1000000, 
                           tags=[f'kafka:{kafka_name}'] + broker_tags)
                statsd.gauge('kafka.broker.bytes_out_per_sec', 2000000, 
                           tags=[f'kafka:{kafka_name}'] + broker_tags)
                statsd.gauge('kafka.broker.messages_in_per_sec', 1000, 
                           tags=[f'kafka:{kafka_name}'] + broker_tags)
                
                # Under-replicated partitions (critical metric)
                statsd.gauge('kafka.broker.under_replicated_partitions', 0, 
                           tags=[f'kafka:{kafka_name}'] + broker_tags)
                
                # Leader election rate
                statsd.gauge('kafka.broker.leader_election_rate', 0.1, 
                           tags=[f'kafka:{kafka_name}'] + broker_tags)
                
                # ISR shrink/expand rates
                statsd.gauge('kafka.broker.isr_shrinks_per_sec', 0, 
                           tags=[f'kafka:{kafka_name}'] + broker_tags)
                statsd.gauge('kafka.broker.isr_expands_per_sec', 0, 
                           tags=[f'kafka:{kafka_name}'] + broker_tags)
            
        except Exception as e:
            logger.warning(f"Could not monitor Kafka broker metrics: {e}")
    
    async def _send_rabbitmq_metrics(self, rabbit_name: str, metrics: Dict[str, Any]):
        """Send RabbitMQ metrics to DataDog"""
        tags = [f'rabbitmq:{rabbit_name}', 'mq_type:rabbitmq']
        
        # Cluster metrics
        statsd.gauge('rabbitmq.cluster.running_nodes', metrics.get('running_nodes', 0), tags=tags)
        statsd.gauge('rabbitmq.cluster.total_nodes', metrics.get('total_nodes', 0), tags=tags)
        
        # Message metrics
        statsd.gauge('rabbitmq.messages.total', metrics.get('total_messages', 0), tags=tags)
        statsd.gauge('rabbitmq.messages.ready', metrics.get('messages_ready', 0), tags=tags)
        statsd.gauge('rabbitmq.messages.unacknowledged', metrics.get('messages_unacknowledged', 0), tags=tags)
        
        # Message rates
        statsd.gauge('rabbitmq.messages.publish_rate', metrics.get('messages_published_rate', 0), tags=tags)
        statsd.gauge('rabbitmq.messages.deliver_rate', metrics.get('messages_delivered_rate', 0), tags=tags)
        statsd.gauge('rabbitmq.messages.ack_rate', metrics.get('messages_acknowledged_rate', 0), tags=tags)
        statsd.gauge('rabbitmq.messages.redeliver_rate', metrics.get('messages_redelivered_rate', 0), tags=tags)
        
        # Connection metrics
        statsd.gauge('rabbitmq.connections.total', metrics.get('total_connections', 0), tags=tags)
        statsd.gauge('rabbitmq.channels.total', metrics.get('total_channels', 0), tags=tags)
        statsd.gauge('rabbitmq.consumers.total', metrics.get('total_consumers', 0), tags=tags)
        
        # Resource metrics
        statsd.gauge('rabbitmq.memory.used', metrics.get('memory_used', 0), tags=tags)
        statsd.gauge('rabbitmq.memory.limit', metrics.get('memory_limit', 0), tags=tags)
        statsd.gauge('rabbitmq.memory.usage_ratio', metrics.get('memory_usage_ratio', 0), tags=tags)
        
        statsd.gauge('rabbitmq.disk.free', metrics.get('disk_free', 0), tags=tags)
        statsd.gauge('rabbitmq.disk.free_limit', metrics.get('disk_free_limit', 0), tags=tags)
        
        statsd.gauge('rabbitmq.file_descriptors.used', metrics.get('fd_used', 0), tags=tags)
        statsd.gauge('rabbitmq.file_descriptors.total', metrics.get('fd_total', 0), tags=tags)
        statsd.gauge('rabbitmq.file_descriptors.usage_ratio', metrics.get('fd_usage_ratio', 0), tags=tags)
        
        # Object metrics
        statsd.gauge('rabbitmq.objects.queues', metrics.get('total_queues', 0), tags=tags)
        statsd.gauge('rabbitmq.objects.exchanges', metrics.get('total_exchanges', 0), tags=tags)
        statsd.gauge('rabbitmq.queues.max_depth', metrics.get('max_queue_depth', 0), tags=tags)
        
        # Check alert thresholds
        await self._check_rabbitmq_alerts(rabbit_name, metrics, tags)
    
    async def _send_kafka_metrics(self, kafka_name: str, metrics: Dict[str, Any]):
        """Send Kafka metrics to DataDog"""
        tags = [f'kafka:{kafka_name}', 'mq_type:kafka']
        
        # Cluster metrics
        statsd.gauge('kafka.cluster.brokers', metrics.get('broker_count', 0), tags=tags)
        statsd.gauge('kafka.cluster.topics', metrics.get('topic_count', 0), tags=tags)
        statsd.gauge('kafka.cluster.partitions', metrics.get('total_partitions', 0), tags=tags)
        
        # Controller metrics
        controller_id = metrics.get('controller_id', -1)
        statsd.gauge('kafka.cluster.controller_active', 1 if controller_id >= 0 else 0, tags=tags)
        
        # Check alert thresholds
        await self._check_kafka_alerts(kafka_name, metrics, tags)
    
    async def _check_rabbitmq_alerts(self, rabbit_name: str, metrics: Dict[str, Any], tags: List[str]):
        """Check RabbitMQ alert thresholds"""
        baselines = self.performance_baselines['rabbitmq']
        
        # Check queue depth
        max_queue_depth = metrics.get('max_queue_depth', 0)
        if max_queue_depth > baselines['queue_depth_max']:
            statsd.event(
                title=f"RabbitMQ Queue Depth Alert - {rabbit_name}",
                text=f"Maximum queue depth {max_queue_depth} exceeds threshold {baselines['queue_depth_max']}",
                alert_type='warning',
                tags=tags
            )
        
        # Check memory usage
        memory_usage_ratio = metrics.get('memory_usage_ratio', 0)
        if memory_usage_ratio > baselines['memory_usage_max']:
            statsd.event(
                title=f"RabbitMQ Memory Usage Alert - {rabbit_name}",
                text=f"Memory usage {memory_usage_ratio:.3f} exceeds threshold {baselines['memory_usage_max']}",
                alert_type='warning',
                tags=tags
            )
        
        # Check node availability
        running_nodes = metrics.get('running_nodes', 0)
        total_nodes = metrics.get('total_nodes', 0)
        if running_nodes < total_nodes:
            statsd.event(
                title=f"RabbitMQ Node Availability Alert - {rabbit_name}",
                text=f"Only {running_nodes}/{total_nodes} nodes are running",
                alert_type='error',
                tags=tags
            )
    
    async def _check_kafka_alerts(self, kafka_name: str, metrics: Dict[str, Any], tags: List[str]):
        """Check Kafka alert thresholds"""
        baselines = self.performance_baselines['kafka']
        
        # Check controller availability
        controller_id = metrics.get('controller_id', -1)
        if controller_id < 0:
            statsd.event(
                title=f"Kafka Controller Alert - {kafka_name}",
                text=f"No active controller found in cluster",
                alert_type='error',
                tags=tags
            )
        
        # Check broker count
        broker_count = metrics.get('broker_count', 0)
        if broker_count == 0:
            statsd.event(
                title=f"Kafka Broker Availability Alert - {kafka_name}",
                text=f"No brokers available in cluster",
                alert_type='error',
                tags=tags
            )
    
    async def run_monitoring_loop(self):
        """Run continuous monitoring loop"""
        logger.info("Starting message queue monitoring loop")
        
        while True:
            try:
                # Monitor all message queues in parallel
                tasks = []
                
                # RabbitMQ monitoring
                for rabbit_name in self.rabbitmq_connections.keys():
                    tasks.append(self.monitor_rabbitmq(rabbit_name))
                
                # Kafka monitoring
                for kafka_name in self.kafka_admin_clients.keys():
                    tasks.append(self.monitor_kafka(kafka_name))
                
                # Execute all monitoring tasks
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Log results
                successful_monitors = sum(1 for result in results if not isinstance(result, Exception))
                failed_monitors = len(results) - successful_monitors
                
                logger.info(f"Message queue monitoring cycle completed: {successful_monitors} successful, {failed_monitors} failed")
                
                # Send health check metric
                statsd.gauge('message_queue.monitoring.health', 1, tags=['status:healthy'])
                
                # Wait for next monitoring interval
                await asyncio.sleep(self.monitoring_interval)
                
            except Exception as e:
                logger.error(f"Error in message queue monitoring loop: {e}")
                statsd.increment('message_queue.monitoring.loop_error')
                await asyncio.sleep(60)  # Wait 1 minute before retrying

# Configuration example
MESSAGE_QUEUE_MONITORING_CONFIG = {
    'monitoring_interval': 60,  # seconds
    'rabbitmq': {
        'primary': {
            'host': 'rabbitmq.default.svc.cluster.local',
            'port': 5672,
            'management_port': 15672,
            'username': 'datadog',
            'password': 'rabbitmq-password'
        },
        'secondary': {
            'host': 'rabbitmq-ha.default.svc.cluster.local',
            'port': 5672,
            'management_port': 15672,
            'username': 'datadog',
            'password': 'rabbitmq-password'
        }
    },
    'kafka': {
        'streaming': {
            'bootstrap_servers': ['kafka-1.default.svc.cluster.local:9092', 
                                'kafka-2.default.svc.cluster.local:9092',
                                'kafka-3.default.svc.cluster.local:9092'],
            'security_protocol': 'PLAINTEXT'
        },
        'events': {
            'bootstrap_servers': ['kafka-events-1.default.svc.cluster.local:9092', 
                                'kafka-events-2.default.svc.cluster.local:9092'],
            'security_protocol': 'PLAINTEXT'
        }
    },
    'alert_thresholds': {
        'rabbitmq': {
            'queue_depth_max': 10000,
            'memory_usage_max': 0.8,
            'message_rate_min': 100
        },
        'kafka': {
            'consumer_lag_max': 1000,
            'under_replicated_partitions_max': 0,
            'broker_disk_usage_max': 0.8
        }
    }
}

async def main():
    """Main entry point for message queue monitoring"""
    monitor = DataDogMessageQueueMonitor(MESSAGE_QUEUE_MONITORING_CONFIG)
    await monitor.run_monitoring_loop()

if __name__ == "__main__":
    asyncio.run(main())
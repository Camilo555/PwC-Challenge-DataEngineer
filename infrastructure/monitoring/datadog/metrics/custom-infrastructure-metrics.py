#!/usr/bin/env python3
"""
Custom Infrastructure Metrics Collection
Collects and reports business-specific infrastructure KPIs to DataDog
"""

import json
import time
import logging
import psutil
import requests
import subprocess
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import threading
from datadog import api, initialize, statsd
import kubernetes
import docker

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class InfrastructureMetric:
    """Infrastructure metric data structure"""
    name: str
    value: float
    timestamp: datetime
    tags: List[str]
    metric_type: str = 'gauge'  # gauge, counter, histogram
    unit: Optional[str] = None

class CustomInfrastructureMetrics:
    """Custom infrastructure metrics collector"""
    
    def __init__(self, config: Dict):
        """Initialize metrics collector"""
        self.config = config
        self.running = False
        
        # Initialize DataDog
        initialize(
            api_key=config.get('datadog_api_key'),
            app_key=config.get('datadog_app_key'),
            api_host=config.get('datadog_host', 'https://api.datadoghq.com')
        )
        
        # Initialize StatsD client for real-time metrics
        statsd.initialize(
            statsd_host=config.get('statsd_host', 'localhost'),
            statsd_port=config.get('statsd_port', 8125),
            statsd_namespace='infrastructure'
        )
        
        # Initialize clients
        try:
            kubernetes.config.load_incluster_config()
            self.k8s_client = kubernetes.client.ApiClient()
            self.k8s_v1 = kubernetes.client.CoreV1Api()
            self.k8s_apps_v1 = kubernetes.client.AppsV1Api()
            self.k8s_metrics = kubernetes.client.CustomObjectsApi()
        except:
            self.k8s_client = None
            logger.warning("Kubernetes client not available")
        
        try:
            self.docker_client = docker.from_env()
        except:
            self.docker_client = None
            logger.warning("Docker client not available")
        
        # Metrics configuration
        self.collection_interval = config.get('collection_interval', 60)  # seconds
        self.cost_calculation_enabled = config.get('cost_calculation_enabled', True)
        
        # Cloud provider pricing (simplified - should be loaded from API)
        self.aws_pricing = {
            'compute': {'m5.large': 0.096, 'm5.xlarge': 0.192, 'm5.2xlarge': 0.384},
            'storage': {'gp3': 0.08, 'io2': 0.125, 's3_standard': 0.023},
            'network': {'data_transfer': 0.09, 'nat_gateway': 0.045}
        }
        
        # Business KPIs configuration
        self.business_kpis = {
            'data_processing_throughput': {'target': 1000000, 'unit': 'records/hour'},
            'query_response_time': {'target': 100, 'unit': 'ms'},
            'system_availability': {'target': 99.9, 'unit': '%'},
            'cost_per_gb_processed': {'target': 0.001, 'unit': '$/GB'}
        }
        
    def start_collection(self):
        """Start metrics collection"""
        logger.info("Starting custom infrastructure metrics collection")
        self.running = True
        
        # Start collection threads
        threading.Thread(target=self._collect_capacity_metrics, daemon=True).start()
        threading.Thread(target=self._collect_performance_metrics, daemon=True).start()
        threading.Thread(target=self._collect_cost_metrics, daemon=True).start()
        threading.Thread(target=self._collect_business_kpis, daemon=True).start()
        threading.Thread(target=self._collect_efficiency_metrics, daemon=True).start()
        
    def stop_collection(self):
        """Stop metrics collection"""
        logger.info("Stopping custom infrastructure metrics collection")
        self.running = False
    
    def _collect_capacity_metrics(self):
        """Collect capacity planning metrics"""
        logger.info("Starting capacity metrics collection")
        
        while self.running:
            try:
                # System resource metrics
                cpu_count = psutil.cpu_count()
                memory_total = psutil.virtual_memory().total / (1024**3)  # GB
                disk_total = sum([d.total for d in psutil.disk_usage('/').total]) / (1024**3) if hasattr(psutil.disk_usage('/'), 'total') else psutil.disk_usage('/').total / (1024**3)
                
                # CPU utilization trend
                cpu_usage = psutil.cpu_percent(interval=1)
                memory_usage = psutil.virtual_memory().percent
                disk_usage = psutil.disk_usage('/').percent
                
                # Network usage
                net_io = psutil.net_io_counters()
                network_throughput = (net_io.bytes_sent + net_io.bytes_recv) / (1024**2)  # MB
                
                # Database storage growth rate
                db_growth_rate = self._calculate_database_growth_rate()
                
                # Send capacity metrics
                timestamp = time.time()
                
                statsd.gauge('capacity.cpu.total_cores', cpu_count, tags=['env:production'])
                statsd.gauge('capacity.memory.total_gb', memory_total, tags=['env:production'])
                statsd.gauge('capacity.disk.total_gb', disk_total, tags=['env:production'])
                
                statsd.gauge('capacity.cpu.utilization_pct', cpu_usage, tags=['env:production'])
                statsd.gauge('capacity.memory.utilization_pct', memory_usage, tags=['env:production'])
                statsd.gauge('capacity.disk.utilization_pct', disk_usage, tags=['env:production'])
                
                statsd.gauge('capacity.network.throughput_mb', network_throughput, tags=['env:production'])
                statsd.gauge('capacity.database.growth_rate_gb_day', db_growth_rate, tags=['env:production'])
                
                # Kubernetes capacity metrics
                if self.k8s_client:
                    k8s_capacity = self._get_kubernetes_capacity()
                    for metric_name, value in k8s_capacity.items():
                        statsd.gauge(f'capacity.kubernetes.{metric_name}', value, tags=['env:production'])
                
                time.sleep(self.collection_interval)
                
            except Exception as e:
                logger.error(f"Error collecting capacity metrics: {e}")
                time.sleep(60)
    
    def _collect_performance_metrics(self):
        """Collect performance benchmark metrics"""
        logger.info("Starting performance metrics collection")
        
        while self.running:
            try:
                # System performance metrics
                load_avg = psutil.getloadavg()[0] if hasattr(psutil, 'getloadavg') else 0
                context_switches = psutil.cpu_stats().ctx_switches if hasattr(psutil, 'cpu_stats') else 0
                
                # Database performance
                db_performance = self._get_database_performance()
                
                # Cache hit rates
                cache_performance = self._get_cache_performance()
                
                # Message queue performance
                queue_performance = self._get_queue_performance()
                
                # Send performance metrics
                statsd.gauge('performance.system.load_average', load_avg, tags=['env:production'])
                statsd.gauge('performance.system.context_switches', context_switches, tags=['env:production'])
                
                for db_name, metrics in db_performance.items():
                    for metric_name, value in metrics.items():
                        statsd.gauge(f'performance.database.{metric_name}', value, 
                                   tags=[f'database:{db_name}', 'env:production'])
                
                for cache_name, metrics in cache_performance.items():
                    for metric_name, value in metrics.items():
                        statsd.gauge(f'performance.cache.{metric_name}', value,
                                   tags=[f'cache:{cache_name}', 'env:production'])
                
                for queue_name, metrics in queue_performance.items():
                    for metric_name, value in metrics.items():
                        statsd.gauge(f'performance.queue.{metric_name}', value,
                                   tags=[f'queue:{queue_name}', 'env:production'])
                
                time.sleep(self.collection_interval)
                
            except Exception as e:
                logger.error(f"Error collecting performance metrics: {e}")
                time.sleep(60)
    
    def _collect_cost_metrics(self):
        """Collect cost optimization metrics"""
        logger.info("Starting cost metrics collection")
        
        while self.running:
            try:
                if not self.cost_calculation_enabled:
                    time.sleep(300)  # Check every 5 minutes
                    continue
                
                # Calculate infrastructure costs
                compute_costs = self._calculate_compute_costs()
                storage_costs = self._calculate_storage_costs()
                network_costs = self._calculate_network_costs()
                
                total_hourly_cost = compute_costs + storage_costs + network_costs
                total_monthly_cost = total_hourly_cost * 24 * 30
                
                # Cost efficiency metrics
                cost_per_request = self._calculate_cost_per_request(total_hourly_cost)
                cost_per_gb_processed = self._calculate_cost_per_gb_processed(total_hourly_cost)
                
                # Resource efficiency
                resource_efficiency = self._calculate_resource_efficiency()
                
                # Send cost metrics
                statsd.gauge('cost.compute.hourly_usd', compute_costs, tags=['env:production'])
                statsd.gauge('cost.storage.hourly_usd', storage_costs, tags=['env:production'])
                statsd.gauge('cost.network.hourly_usd', network_costs, tags=['env:production'])
                statsd.gauge('cost.total.hourly_usd', total_hourly_cost, tags=['env:production'])
                statsd.gauge('cost.total.monthly_usd', total_monthly_cost, tags=['env:production'])
                
                statsd.gauge('cost.efficiency.per_request_usd', cost_per_request, tags=['env:production'])
                statsd.gauge('cost.efficiency.per_gb_processed_usd', cost_per_gb_processed, tags=['env:production'])
                statsd.gauge('cost.efficiency.resource_utilization_score', resource_efficiency, tags=['env:production'])
                
                time.sleep(300)  # Update every 5 minutes
                
            except Exception as e:
                logger.error(f"Error collecting cost metrics: {e}")
                time.sleep(300)
    
    def _collect_business_kpis(self):
        """Collect business-specific infrastructure KPIs"""
        logger.info("Starting business KPIs collection")
        
        while self.running:
            try:
                # Data processing throughput
                processing_throughput = self._get_data_processing_throughput()
                
                # Query response times
                avg_query_time = self._get_average_query_response_time()
                
                # System availability
                system_availability = self._calculate_system_availability()
                
                # Data pipeline success rate
                pipeline_success_rate = self._get_pipeline_success_rate()
                
                # Storage growth rate
                storage_growth_rate = self._calculate_storage_growth_rate()
                
                # Send business KPIs
                statsd.gauge('business.data_processing.throughput_records_hour', processing_throughput, 
                           tags=['env:production'])
                statsd.gauge('business.query.avg_response_time_ms', avg_query_time, 
                           tags=['env:production'])
                statsd.gauge('business.system.availability_pct', system_availability, 
                           tags=['env:production'])
                statsd.gauge('business.pipeline.success_rate_pct', pipeline_success_rate, 
                           tags=['env:production'])
                statsd.gauge('business.storage.growth_rate_gb_day', storage_growth_rate, 
                           tags=['env:production'])
                
                # Calculate KPI scores against targets
                for kpi_name, config in self.business_kpis.items():
                    current_value = locals().get(kpi_name.split('_')[-1], 0)  # Simplified mapping
                    target_value = config['target']
                    
                    if kpi_name in ['query_response_time']:
                        # Lower is better
                        score = max(0, (target_value - current_value) / target_value * 100)
                    else:
                        # Higher is better
                        score = min(100, current_value / target_value * 100)
                    
                    statsd.gauge(f'business.kpi.{kpi_name}_score', score, 
                               tags=['env:production', f'target:{target_value}'])
                
                time.sleep(self.collection_interval)
                
            except Exception as e:
                logger.error(f"Error collecting business KPIs: {e}")
                time.sleep(60)
    
    def _collect_efficiency_metrics(self):
        """Collect resource efficiency metrics"""
        logger.info("Starting efficiency metrics collection")
        
        while self.running:
            try:
                # CPU efficiency (work done per CPU cycle)
                cpu_efficiency = self._calculate_cpu_efficiency()
                
                # Memory efficiency (data processed per GB)
                memory_efficiency = self._calculate_memory_efficiency()
                
                # Storage efficiency (compression ratio, deduplication)
                storage_efficiency = self._calculate_storage_efficiency()
                
                # Network efficiency (useful data vs overhead)
                network_efficiency = self._calculate_network_efficiency()
                
                # Overall infrastructure efficiency score
                overall_efficiency = (cpu_efficiency + memory_efficiency + 
                                    storage_efficiency + network_efficiency) / 4
                
                # Send efficiency metrics
                statsd.gauge('efficiency.cpu.utilization_score', cpu_efficiency, tags=['env:production'])
                statsd.gauge('efficiency.memory.utilization_score', memory_efficiency, tags=['env:production'])
                statsd.gauge('efficiency.storage.utilization_score', storage_efficiency, tags=['env:production'])
                statsd.gauge('efficiency.network.utilization_score', network_efficiency, tags=['env:production'])
                statsd.gauge('efficiency.overall.score', overall_efficiency, tags=['env:production'])
                
                time.sleep(self.collection_interval)
                
            except Exception as e:
                logger.error(f"Error collecting efficiency metrics: {e}")
                time.sleep(60)
    
    # Helper methods for metric calculations
    
    def _calculate_database_growth_rate(self) -> float:
        """Calculate database growth rate in GB/day"""
        try:
            # This would query database size metrics from the past week
            # and calculate growth rate - simplified implementation
            return 5.2  # GB/day
        except Exception:
            return 0.0
    
    def _get_kubernetes_capacity(self) -> Dict[str, float]:
        """Get Kubernetes capacity metrics"""
        if not self.k8s_client:
            return {}
        
        try:
            # Get node metrics
            nodes = self.k8s_v1.list_node()
            
            total_cpu = 0
            total_memory = 0
            total_pods = 0
            
            for node in nodes.items:
                # Parse resource capacity
                cpu_capacity = node.status.capacity.get('cpu', '0')
                memory_capacity = node.status.capacity.get('memory', '0Ki')
                pod_capacity = int(node.status.capacity.get('pods', '0'))
                
                # Convert to standard units
                total_cpu += self._parse_cpu_resource(cpu_capacity)
                total_memory += self._parse_memory_resource(memory_capacity)
                total_pods += pod_capacity
            
            return {
                'total_cpu_cores': total_cpu,
                'total_memory_gb': total_memory,
                'total_pod_capacity': total_pods,
                'node_count': len(nodes.items)
            }
        except Exception as e:
            logger.error(f"Error getting Kubernetes capacity: {e}")
            return {}
    
    def _parse_cpu_resource(self, cpu_str: str) -> float:
        """Parse Kubernetes CPU resource string"""
        if cpu_str.endswith('m'):
            return float(cpu_str[:-1]) / 1000
        return float(cpu_str)
    
    def _parse_memory_resource(self, memory_str: str) -> float:
        """Parse Kubernetes memory resource string to GB"""
        multipliers = {'Ki': 1024, 'Mi': 1024**2, 'Gi': 1024**3, 'Ti': 1024**4}
        
        for suffix, multiplier in multipliers.items():
            if memory_str.endswith(suffix):
                value = float(memory_str[:-2])
                return value * multiplier / (1024**3)  # Convert to GB
        
        return float(memory_str) / (1024**3)  # Assume bytes
    
    def _get_database_performance(self) -> Dict[str, Dict[str, float]]:
        """Get database performance metrics"""
        # Simplified implementation - would integrate with actual database metrics
        return {
            'postgresql': {
                'avg_query_time_ms': 45.2,
                'connections_active': 23,
                'cache_hit_ratio': 98.5,
                'locks_waiting': 2
            },
            'redis': {
                'ops_per_sec': 15000,
                'hit_ratio': 97.8,
                'memory_fragmentation': 1.2,
                'blocked_clients': 0
            }
        }
    
    def _get_cache_performance(self) -> Dict[str, Dict[str, float]]:
        """Get cache performance metrics"""
        return {
            'redis': {
                'hit_rate_pct': 97.8,
                'miss_rate_pct': 2.2,
                'evictions_per_sec': 12.5,
                'memory_usage_pct': 68.3
            }
        }
    
    def _get_queue_performance(self) -> Dict[str, Dict[str, float]]:
        """Get message queue performance metrics"""
        return {
            'rabbitmq': {
                'messages_per_sec': 850,
                'queue_depth': 125,
                'consumer_utilization_pct': 78.5,
                'memory_usage_mb': 245
            },
            'kafka': {
                'messages_per_sec': 2500,
                'bytes_per_sec_mb': 45.8,
                'consumer_lag': 1250,
                'partition_count': 48
            }
        }
    
    def _calculate_compute_costs(self) -> float:
        """Calculate hourly compute costs"""
        # Simplified cost calculation - would integrate with cloud provider APIs
        instance_counts = {'m5.large': 5, 'm5.xlarge': 3, 'm5.2xlarge': 2}
        
        total_cost = 0
        for instance_type, count in instance_counts.items():
            hourly_rate = self.aws_pricing['compute'].get(instance_type, 0)
            total_cost += hourly_rate * count
        
        return total_cost
    
    def _calculate_storage_costs(self) -> float:
        """Calculate hourly storage costs"""
        # Storage usage in GB
        storage_usage = {
            'gp3': 2000,    # GB
            's3_standard': 5000,  # GB
            'io2': 500      # GB
        }
        
        total_cost = 0
        for storage_type, usage_gb in storage_usage.items():
            monthly_rate = self.aws_pricing['storage'].get(storage_type, 0)
            hourly_cost = (monthly_rate * usage_gb) / (24 * 30)  # Convert to hourly
            total_cost += hourly_cost
        
        return total_cost
    
    def _calculate_network_costs(self) -> float:
        """Calculate hourly network costs"""
        # Network usage in GB/hour
        data_transfer_gb_hour = 50
        nat_gateway_hours = 24  # Always running
        
        data_transfer_cost = data_transfer_gb_hour * self.aws_pricing['network']['data_transfer']
        nat_gateway_cost = self.aws_pricing['network']['nat_gateway']
        
        return data_transfer_cost + nat_gateway_cost
    
    def _calculate_cost_per_request(self, hourly_cost: float) -> float:
        """Calculate cost per request"""
        # Estimate requests per hour
        requests_per_hour = 100000  # Would come from actual metrics
        
        if requests_per_hour > 0:
            return hourly_cost / requests_per_hour
        return 0.0
    
    def _calculate_cost_per_gb_processed(self, hourly_cost: float) -> float:
        """Calculate cost per GB of data processed"""
        # Estimate GB processed per hour
        gb_processed_per_hour = 1000  # Would come from actual data pipeline metrics
        
        if gb_processed_per_hour > 0:
            return hourly_cost / gb_processed_per_hour
        return 0.0
    
    def _calculate_resource_efficiency(self) -> float:
        """Calculate overall resource efficiency score (0-100)"""
        cpu_utilization = psutil.cpu_percent()
        memory_utilization = psutil.virtual_memory().percent
        
        # Optimal utilization is around 70-80%
        optimal_utilization = 75
        
        cpu_score = 100 - abs(cpu_utilization - optimal_utilization)
        memory_score = 100 - abs(memory_utilization - optimal_utilization)
        
        return max(0, (cpu_score + memory_score) / 2)
    
    def _get_data_processing_throughput(self) -> float:
        """Get data processing throughput in records/hour"""
        # Would integrate with actual data pipeline metrics
        return 875000  # records/hour
    
    def _get_average_query_response_time(self) -> float:
        """Get average query response time in milliseconds"""
        # Would integrate with database monitoring
        return 85.5  # milliseconds
    
    def _calculate_system_availability(self) -> float:
        """Calculate system availability percentage"""
        # Would calculate based on uptime metrics
        return 99.95  # percentage
    
    def _get_pipeline_success_rate(self) -> float:
        """Get data pipeline success rate"""
        # Would integrate with pipeline monitoring
        return 98.2  # percentage
    
    def _calculate_storage_growth_rate(self) -> float:
        """Calculate storage growth rate in GB/day"""
        # Would analyze historical storage usage
        return 12.5  # GB/day
    
    def _calculate_cpu_efficiency(self) -> float:
        """Calculate CPU efficiency score"""
        cpu_usage = psutil.cpu_percent(interval=1)
        # Efficiency is based on how well CPU usage matches workload
        # This is a simplified calculation
        return min(100, cpu_usage * 1.2)  # Scale factor for efficiency
    
    def _calculate_memory_efficiency(self) -> float:
        """Calculate memory efficiency score"""
        memory_info = psutil.virtual_memory()
        # Consider both usage and available memory
        efficiency = (memory_info.used / memory_info.total) * 100
        return min(100, efficiency * 1.1)
    
    def _calculate_storage_efficiency(self) -> float:
        """Calculate storage efficiency score"""
        # Would include compression ratios, deduplication rates, etc.
        # Simplified implementation
        disk_usage = psutil.disk_usage('/').percent
        return min(100, 120 - disk_usage)  # Inverse relationship with usage
    
    def _calculate_network_efficiency(self) -> float:
        """Calculate network efficiency score"""
        # Would analyze network utilization vs capacity
        # Simplified implementation
        return 85.0  # Static value for demo

def main():
    """Main execution function"""
    config = {
        'datadog_api_key': 'your-datadog-api-key',
        'datadog_app_key': 'your-datadog-app-key',
        'statsd_host': 'localhost',
        'statsd_port': 8125,
        'collection_interval': 60,
        'cost_calculation_enabled': True
    }
    
    metrics_collector = CustomInfrastructureMetrics(config)
    
    try:
        metrics_collector.start_collection()
        
        logger.info("Custom infrastructure metrics collection started")
        
        # Keep running
        while True:
            time.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
        metrics_collector.stop_collection()
        return 0
    except Exception as e:
        logger.error(f"Metrics collection failed: {e}")
        metrics_collector.stop_collection()
        return 1

if __name__ == '__main__':
    exit(main())
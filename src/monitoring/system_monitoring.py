"""
Enterprise System Monitoring
Comprehensive system-level monitoring with infrastructure metrics, health checks, and performance tracking.
"""
from __future__ import annotations

import asyncio
import json
import platform
import psutil
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set
import uuid

try:
    import docker
    DOCKER_AVAILABLE = True
except ImportError:
    DOCKER_AVAILABLE = False
    docker = None

try:
    from kubernetes import client, config
    KUBERNETES_AVAILABLE = True
except ImportError:
    KUBERNETES_AVAILABLE = False
    client = None
    config = None

from core.logging import get_logger
from .prometheus_metrics import get_prometheus_collector

logger = get_logger(__name__)


class SystemHealthStatus(Enum):
    """System health status levels."""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class ResourceType(Enum):
    """System resource types."""
    CPU = "cpu"
    MEMORY = "memory"
    DISK = "disk"
    NETWORK = "network"
    PROCESS = "process"
    CONTAINER = "container"
    SERVICE = "service"


@dataclass
class SystemMetric:
    """System metric data structure."""
    name: str
    value: float
    unit: str
    timestamp: datetime
    resource_type: ResourceType
    host: str
    tags: Dict[str, str] = field(default_factory=dict)
    threshold_warning: Optional[float] = None
    threshold_critical: Optional[float] = None


@dataclass
class HealthCheck:
    """Health check configuration and status."""
    name: str
    description: str
    check_function: str
    interval_seconds: int
    timeout_seconds: int
    status: SystemHealthStatus
    last_check: Optional[datetime] = None
    last_success: Optional[datetime] = None
    failure_count: int = 0
    error_message: str = ""
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class ProcessInfo:
    """Process information structure."""
    pid: int
    name: str
    cpu_percent: float
    memory_percent: float
    memory_rss: int
    memory_vms: int
    status: str
    create_time: float
    cmdline: List[str]
    connections: int = 0
    open_files: int = 0


class SystemMonitor:
    """Comprehensive system monitoring class."""

    def __init__(self, collect_interval: int = 30):
        self.collect_interval = collect_interval
        self.prometheus = get_prometheus_collector()
        self.logger = get_logger(self.__class__.__name__)
        
        # System information
        self.hostname = platform.node()
        self.platform_info = self._get_platform_info()
        
        # Health checks
        self.health_checks: Dict[str, HealthCheck] = {}
        self.health_check_results: Dict[str, List[Dict[str, Any]]] = {}
        
        # Resource thresholds
        self.resource_thresholds = {
            'cpu_usage': {'warning': 80.0, 'critical': 95.0},
            'memory_usage': {'warning': 85.0, 'critical': 95.0},
            'disk_usage': {'warning': 85.0, 'critical': 95.0},
            'load_average': {'warning': 4.0, 'critical': 8.0}
        }
        
        # Docker client if available
        self.docker_client = None
        if DOCKER_AVAILABLE:
            try:
                self.docker_client = docker.from_env()
            except Exception as e:
                self.logger.warning(f"Docker client initialization failed: {e}")
        
        # Kubernetes client if available
        self.k8s_client = None
        if KUBERNETES_AVAILABLE:
            try:
                config.load_incluster_config()
                self.k8s_client = client.CoreV1Api()
            except Exception:
                try:
                    config.load_kube_config()
                    self.k8s_client = client.CoreV1Api()
                except Exception as e:
                    self.logger.warning(f"Kubernetes client initialization failed: {e}")
        
        # Initialize default health checks
        self._initialize_health_checks()

    def _get_platform_info(self) -> Dict[str, Any]:
        """Get platform information."""
        return {
            'hostname': self.hostname,
            'platform': platform.platform(),
            'architecture': platform.architecture()[0],
            'processor': platform.processor(),
            'python_version': platform.python_version(),
            'boot_time': psutil.boot_time(),
            'logical_cpus': psutil.cpu_count(logical=True),
            'physical_cpus': psutil.cpu_count(logical=False)
        }

    def _initialize_health_checks(self):
        """Initialize default system health checks."""
        health_checks = [
            HealthCheck(
                name="cpu_usage",
                description="CPU usage percentage check",
                check_function="check_cpu_usage",
                interval_seconds=30,
                timeout_seconds=5,
                status=SystemHealthStatus.UNKNOWN
            ),
            HealthCheck(
                name="memory_usage",
                description="Memory usage percentage check",
                check_function="check_memory_usage",
                interval_seconds=30,
                timeout_seconds=5,
                status=SystemHealthStatus.UNKNOWN
            ),
            HealthCheck(
                name="disk_usage",
                description="Disk usage percentage check",
                check_function="check_disk_usage",
                interval_seconds=60,
                timeout_seconds=10,
                status=SystemHealthStatus.UNKNOWN
            ),
            HealthCheck(
                name="load_average",
                description="System load average check",
                check_function="check_load_average",
                interval_seconds=30,
                timeout_seconds=5,
                status=SystemHealthStatus.UNKNOWN
            ),
            HealthCheck(
                name="network_connectivity",
                description="Network connectivity check",
                check_function="check_network_connectivity",
                interval_seconds=60,
                timeout_seconds=10,
                status=SystemHealthStatus.UNKNOWN
            )
        ]
        
        for health_check in health_checks:
            self.register_health_check(health_check)

    def register_health_check(self, health_check: HealthCheck):
        """Register a new health check."""
        self.health_checks[health_check.name] = health_check
        self.health_check_results[health_check.name] = []
        self.logger.info(f"Registered health check: {health_check.name}")

    async def collect_system_metrics(self) -> List[SystemMetric]:
        """Collect comprehensive system metrics."""
        metrics = []
        timestamp = datetime.utcnow()

        try:
            # CPU metrics
            cpu_metrics = await self._collect_cpu_metrics(timestamp)
            metrics.extend(cpu_metrics)

            # Memory metrics
            memory_metrics = await self._collect_memory_metrics(timestamp)
            metrics.extend(memory_metrics)

            # Disk metrics
            disk_metrics = await self._collect_disk_metrics(timestamp)
            metrics.extend(disk_metrics)

            # Network metrics
            network_metrics = await self._collect_network_metrics(timestamp)
            metrics.extend(network_metrics)

            # Process metrics
            process_metrics = await self._collect_process_metrics(timestamp)
            metrics.extend(process_metrics)

            # Container metrics (if available)
            if self.docker_client:
                container_metrics = await self._collect_container_metrics(timestamp)
                metrics.extend(container_metrics)

            # Update Prometheus metrics
            self._update_prometheus_metrics(metrics)

        except Exception as e:
            self.logger.error(f"Failed to collect system metrics: {e}")

        return metrics

    async def _collect_cpu_metrics(self, timestamp: datetime) -> List[SystemMetric]:
        """Collect CPU-related metrics."""
        metrics = []
        
        # Overall CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        metrics.append(SystemMetric(
            name="cpu_usage_percent",
            value=cpu_percent,
            unit="percent",
            timestamp=timestamp,
            resource_type=ResourceType.CPU,
            host=self.hostname,
            threshold_warning=self.resource_thresholds['cpu_usage']['warning'],
            threshold_critical=self.resource_thresholds['cpu_usage']['critical']
        ))

        # Per-core CPU usage
        cpu_per_core = psutil.cpu_percent(percpu=True, interval=1)
        for i, usage in enumerate(cpu_per_core):
            metrics.append(SystemMetric(
                name="cpu_usage_percent_per_core",
                value=usage,
                unit="percent",
                timestamp=timestamp,
                resource_type=ResourceType.CPU,
                host=self.hostname,
                tags={'core': str(i)}
            ))

        # Load average (Unix-like systems)
        try:
            load_avg = psutil.getloadavg()
            for i, avg in enumerate(['1min', '5min', '15min']):
                metrics.append(SystemMetric(
                    name=f"load_average_{avg}",
                    value=load_avg[i],
                    unit="ratio",
                    timestamp=timestamp,
                    resource_type=ResourceType.CPU,
                    host=self.hostname,
                    threshold_warning=self.resource_thresholds['load_average']['warning'],
                    threshold_critical=self.resource_thresholds['load_average']['critical']
                ))
        except AttributeError:
            # Windows doesn't have load average
            pass

        # CPU frequency
        try:
            cpu_freq = psutil.cpu_freq()
            if cpu_freq:
                metrics.append(SystemMetric(
                    name="cpu_frequency_mhz",
                    value=cpu_freq.current,
                    unit="mhz",
                    timestamp=timestamp,
                    resource_type=ResourceType.CPU,
                    host=self.hostname
                ))
        except Exception:
            pass

        return metrics

    async def _collect_memory_metrics(self, timestamp: datetime) -> List[SystemMetric]:
        """Collect memory-related metrics."""
        metrics = []
        
        # Virtual memory
        vmem = psutil.virtual_memory()
        metrics.extend([
            SystemMetric(
                name="memory_usage_percent",
                value=vmem.percent,
                unit="percent",
                timestamp=timestamp,
                resource_type=ResourceType.MEMORY,
                host=self.hostname,
                threshold_warning=self.resource_thresholds['memory_usage']['warning'],
                threshold_critical=self.resource_thresholds['memory_usage']['critical']
            ),
            SystemMetric(
                name="memory_total_bytes",
                value=vmem.total,
                unit="bytes",
                timestamp=timestamp,
                resource_type=ResourceType.MEMORY,
                host=self.hostname
            ),
            SystemMetric(
                name="memory_available_bytes",
                value=vmem.available,
                unit="bytes",
                timestamp=timestamp,
                resource_type=ResourceType.MEMORY,
                host=self.hostname
            ),
            SystemMetric(
                name="memory_used_bytes",
                value=vmem.used,
                unit="bytes",
                timestamp=timestamp,
                resource_type=ResourceType.MEMORY,
                host=self.hostname
            )
        ])

        # Swap memory
        swap = psutil.swap_memory()
        metrics.extend([
            SystemMetric(
                name="swap_usage_percent",
                value=swap.percent,
                unit="percent",
                timestamp=timestamp,
                resource_type=ResourceType.MEMORY,
                host=self.hostname,
                tags={'type': 'swap'}
            ),
            SystemMetric(
                name="swap_total_bytes",
                value=swap.total,
                unit="bytes",
                timestamp=timestamp,
                resource_type=ResourceType.MEMORY,
                host=self.hostname,
                tags={'type': 'swap'}
            ),
            SystemMetric(
                name="swap_used_bytes",
                value=swap.used,
                unit="bytes",
                timestamp=timestamp,
                resource_type=ResourceType.MEMORY,
                host=self.hostname,
                tags={'type': 'swap'}
            )
        ])

        return metrics

    async def _collect_disk_metrics(self, timestamp: datetime) -> List[SystemMetric]:
        """Collect disk-related metrics."""
        metrics = []
        
        # Disk usage for each mounted filesystem
        for partition in psutil.disk_partitions():
            try:
                usage = psutil.disk_usage(partition.mountpoint)
                usage_percent = (usage.used / usage.total) * 100 if usage.total > 0 else 0
                
                metrics.extend([
                    SystemMetric(
                        name="disk_usage_percent",
                        value=usage_percent,
                        unit="percent",
                        timestamp=timestamp,
                        resource_type=ResourceType.DISK,
                        host=self.hostname,
                        tags={
                            'device': partition.device,
                            'mountpoint': partition.mountpoint,
                            'fstype': partition.fstype
                        },
                        threshold_warning=self.resource_thresholds['disk_usage']['warning'],
                        threshold_critical=self.resource_thresholds['disk_usage']['critical']
                    ),
                    SystemMetric(
                        name="disk_total_bytes",
                        value=usage.total,
                        unit="bytes",
                        timestamp=timestamp,
                        resource_type=ResourceType.DISK,
                        host=self.hostname,
                        tags={
                            'device': partition.device,
                            'mountpoint': partition.mountpoint
                        }
                    ),
                    SystemMetric(
                        name="disk_used_bytes",
                        value=usage.used,
                        unit="bytes",
                        timestamp=timestamp,
                        resource_type=ResourceType.DISK,
                        host=self.hostname,
                        tags={
                            'device': partition.device,
                            'mountpoint': partition.mountpoint
                        }
                    ),
                    SystemMetric(
                        name="disk_free_bytes",
                        value=usage.free,
                        unit="bytes",
                        timestamp=timestamp,
                        resource_type=ResourceType.DISK,
                        host=self.hostname,
                        tags={
                            'device': partition.device,
                            'mountpoint': partition.mountpoint
                        }
                    )
                ])
            except PermissionError:
                # Skip if we don't have permission to access the device
                continue
            except Exception as e:
                self.logger.warning(f"Failed to get disk usage for {partition.mountpoint}: {e}")
                continue

        # Disk I/O statistics
        try:
            disk_io = psutil.disk_io_counters()
            if disk_io:
                metrics.extend([
                    SystemMetric(
                        name="disk_read_bytes_total",
                        value=disk_io.read_bytes,
                        unit="bytes",
                        timestamp=timestamp,
                        resource_type=ResourceType.DISK,
                        host=self.hostname,
                        tags={'operation': 'read'}
                    ),
                    SystemMetric(
                        name="disk_write_bytes_total",
                        value=disk_io.write_bytes,
                        unit="bytes",
                        timestamp=timestamp,
                        resource_type=ResourceType.DISK,
                        host=self.hostname,
                        tags={'operation': 'write'}
                    ),
                    SystemMetric(
                        name="disk_read_count_total",
                        value=disk_io.read_count,
                        unit="operations",
                        timestamp=timestamp,
                        resource_type=ResourceType.DISK,
                        host=self.hostname,
                        tags={'operation': 'read'}
                    ),
                    SystemMetric(
                        name="disk_write_count_total",
                        value=disk_io.write_count,
                        unit="operations",
                        timestamp=timestamp,
                        resource_type=ResourceType.DISK,
                        host=self.hostname,
                        tags={'operation': 'write'}
                    )
                ])
        except Exception as e:
            self.logger.warning(f"Failed to get disk I/O counters: {e}")

        return metrics

    async def _collect_network_metrics(self, timestamp: datetime) -> List[SystemMetric]:
        """Collect network-related metrics."""
        metrics = []
        
        try:
            # Network I/O statistics
            net_io = psutil.net_io_counters()
            if net_io:
                metrics.extend([
                    SystemMetric(
                        name="network_bytes_sent_total",
                        value=net_io.bytes_sent,
                        unit="bytes",
                        timestamp=timestamp,
                        resource_type=ResourceType.NETWORK,
                        host=self.hostname,
                        tags={'direction': 'sent'}
                    ),
                    SystemMetric(
                        name="network_bytes_recv_total",
                        value=net_io.bytes_recv,
                        unit="bytes",
                        timestamp=timestamp,
                        resource_type=ResourceType.NETWORK,
                        host=self.hostname,
                        tags={'direction': 'received'}
                    ),
                    SystemMetric(
                        name="network_packets_sent_total",
                        value=net_io.packets_sent,
                        unit="packets",
                        timestamp=timestamp,
                        resource_type=ResourceType.NETWORK,
                        host=self.hostname,
                        tags={'direction': 'sent'}
                    ),
                    SystemMetric(
                        name="network_packets_recv_total",
                        value=net_io.packets_recv,
                        unit="packets",
                        timestamp=timestamp,
                        resource_type=ResourceType.NETWORK,
                        host=self.hostname,
                        tags={'direction': 'received'}
                    )
                ])

            # Per-interface statistics
            net_io_per_nic = psutil.net_io_counters(pernic=True)
            for interface, stats in net_io_per_nic.items():
                metrics.extend([
                    SystemMetric(
                        name="network_interface_bytes_sent",
                        value=stats.bytes_sent,
                        unit="bytes",
                        timestamp=timestamp,
                        resource_type=ResourceType.NETWORK,
                        host=self.hostname,
                        tags={'interface': interface, 'direction': 'sent'}
                    ),
                    SystemMetric(
                        name="network_interface_bytes_recv",
                        value=stats.bytes_recv,
                        unit="bytes",
                        timestamp=timestamp,
                        resource_type=ResourceType.NETWORK,
                        host=self.hostname,
                        tags={'interface': interface, 'direction': 'received'}
                    )
                ])

            # Network connections
            connections = psutil.net_connections()
            connection_states = {}
            for conn in connections:
                state = conn.status if hasattr(conn, 'status') else 'UNKNOWN'
                connection_states[state] = connection_states.get(state, 0) + 1

            for state, count in connection_states.items():
                metrics.append(SystemMetric(
                    name="network_connections_total",
                    value=count,
                    unit="connections",
                    timestamp=timestamp,
                    resource_type=ResourceType.NETWORK,
                    host=self.hostname,
                    tags={'state': state}
                ))

        except Exception as e:
            self.logger.warning(f"Failed to collect network metrics: {e}")

        return metrics

    async def _collect_process_metrics(self, timestamp: datetime) -> List[SystemMetric]:
        """Collect process-related metrics."""
        metrics = []
        
        try:
            # Total process count
            process_count = len(psutil.pids())
            metrics.append(SystemMetric(
                name="processes_total",
                value=process_count,
                unit="processes",
                timestamp=timestamp,
                resource_type=ResourceType.PROCESS,
                host=self.hostname
            ))

            # Process states
            process_states = {}
            top_processes = []

            for proc in psutil.process_iter(['pid', 'name', 'status', 'cpu_percent', 'memory_percent']):
                try:
                    pinfo = proc.info
                    status = pinfo['status']
                    process_states[status] = process_states.get(status, 0) + 1
                    
                    # Track top CPU consumers
                    if pinfo['cpu_percent'] and pinfo['cpu_percent'] > 1.0:  # Only processes using > 1% CPU
                        top_processes.append({
                            'pid': pinfo['pid'],
                            'name': pinfo['name'],
                            'cpu_percent': pinfo['cpu_percent'],
                            'memory_percent': pinfo['memory_percent']
                        })
                        
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue

            # Process state metrics
            for state, count in process_states.items():
                metrics.append(SystemMetric(
                    name="processes_by_state",
                    value=count,
                    unit="processes",
                    timestamp=timestamp,
                    resource_type=ResourceType.PROCESS,
                    host=self.hostname,
                    tags={'state': state}
                ))

            # Top processes metrics
            top_processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
            for i, proc in enumerate(top_processes[:5]):  # Top 5 CPU consumers
                metrics.extend([
                    SystemMetric(
                        name="top_process_cpu_percent",
                        value=proc['cpu_percent'],
                        unit="percent",
                        timestamp=timestamp,
                        resource_type=ResourceType.PROCESS,
                        host=self.hostname,
                        tags={
                            'rank': str(i+1),
                            'pid': str(proc['pid']),
                            'name': proc['name']
                        }
                    ),
                    SystemMetric(
                        name="top_process_memory_percent",
                        value=proc['memory_percent'],
                        unit="percent",
                        timestamp=timestamp,
                        resource_type=ResourceType.PROCESS,
                        host=self.hostname,
                        tags={
                            'rank': str(i+1),
                            'pid': str(proc['pid']),
                            'name': proc['name']
                        }
                    )
                ])

        except Exception as e:
            self.logger.warning(f"Failed to collect process metrics: {e}")

        return metrics

    async def _collect_container_metrics(self, timestamp: datetime) -> List[SystemMetric]:
        """Collect container-related metrics."""
        metrics = []
        
        if not self.docker_client:
            return metrics

        try:
            containers = self.docker_client.containers.list(all=True)
            
            # Container state counts
            container_states = {}
            for container in containers:
                state = container.status
                container_states[state] = container_states.get(state, 0) + 1

            for state, count in container_states.items():
                metrics.append(SystemMetric(
                    name="containers_total",
                    value=count,
                    unit="containers",
                    timestamp=timestamp,
                    resource_type=ResourceType.CONTAINER,
                    host=self.hostname,
                    tags={'state': state}
                ))

            # Individual container metrics
            for container in containers:
                if container.status == 'running':
                    try:
                        stats = container.stats(stream=False)
                        
                        # CPU usage
                        cpu_delta = stats['cpu_stats']['cpu_usage']['total_usage'] - \
                                  stats['precpu_stats']['cpu_usage']['total_usage']
                        system_delta = stats['cpu_stats']['system_cpu_usage'] - \
                                     stats['precpu_stats']['system_cpu_usage']
                        
                        if system_delta > 0:
                            cpu_percent = (cpu_delta / system_delta) * 100.0
                            metrics.append(SystemMetric(
                                name="container_cpu_percent",
                                value=cpu_percent,
                                unit="percent",
                                timestamp=timestamp,
                                resource_type=ResourceType.CONTAINER,
                                host=self.hostname,
                                tags={
                                    'container_name': container.name,
                                    'container_id': container.id[:12],
                                    'image': container.image.tags[0] if container.image.tags else 'unknown'
                                }
                            ))

                        # Memory usage
                        memory_usage = stats['memory_stats']['usage']
                        memory_limit = stats['memory_stats']['limit']
                        memory_percent = (memory_usage / memory_limit) * 100 if memory_limit > 0 else 0
                        
                        metrics.extend([
                            SystemMetric(
                                name="container_memory_usage_bytes",
                                value=memory_usage,
                                unit="bytes",
                                timestamp=timestamp,
                                resource_type=ResourceType.CONTAINER,
                                host=self.hostname,
                                tags={
                                    'container_name': container.name,
                                    'container_id': container.id[:12]
                                }
                            ),
                            SystemMetric(
                                name="container_memory_percent",
                                value=memory_percent,
                                unit="percent",
                                timestamp=timestamp,
                                resource_type=ResourceType.CONTAINER,
                                host=self.hostname,
                                tags={
                                    'container_name': container.name,
                                    'container_id': container.id[:12]
                                }
                            )
                        ])

                        # Network I/O
                        networks = stats.get('networks', {})
                        for network_name, network_stats in networks.items():
                            metrics.extend([
                                SystemMetric(
                                    name="container_network_bytes_received",
                                    value=network_stats['rx_bytes'],
                                    unit="bytes",
                                    timestamp=timestamp,
                                    resource_type=ResourceType.CONTAINER,
                                    host=self.hostname,
                                    tags={
                                        'container_name': container.name,
                                        'network': network_name,
                                        'direction': 'rx'
                                    }
                                ),
                                SystemMetric(
                                    name="container_network_bytes_transmitted",
                                    value=network_stats['tx_bytes'],
                                    unit="bytes",
                                    timestamp=timestamp,
                                    resource_type=ResourceType.CONTAINER,
                                    host=self.hostname,
                                    tags={
                                        'container_name': container.name,
                                        'network': network_name,
                                        'direction': 'tx'
                                    }
                                )
                            ])

                    except Exception as e:
                        self.logger.warning(f"Failed to get stats for container {container.name}: {e}")

        except Exception as e:
            self.logger.warning(f"Failed to collect container metrics: {e}")

        return metrics

    def _update_prometheus_metrics(self, metrics: List[SystemMetric]):
        """Update Prometheus metrics with collected system metrics."""
        for metric in metrics:
            labels = dict(metric.tags)
            labels['host'] = metric.host
            labels['resource_type'] = metric.resource_type.value

            # Map metric names to Prometheus metrics
            prometheus_name = f"system_{metric.name}"
            
            try:
                if metric.name.endswith('_percent'):
                    self.prometheus.set_gauge(prometheus_name, metric.value, labels)
                elif metric.name.endswith('_total') or metric.name.endswith('_count'):
                    self.prometheus.increment_counter(prometheus_name, metric.value, labels)
                elif metric.name.endswith('_bytes') or metric.name.endswith('_seconds'):
                    self.prometheus.observe_histogram(prometheus_name, metric.value, labels)
                else:
                    self.prometheus.set_gauge(prometheus_name, metric.value, labels)
            except Exception as e:
                self.logger.debug(f"Failed to update Prometheus metric {prometheus_name}: {e}")

    async def run_health_checks(self) -> Dict[str, Dict[str, Any]]:
        """Run all registered health checks."""
        results = {}
        
        for name, health_check in self.health_checks.items():
            try:
                result = await self._run_single_health_check(health_check)
                results[name] = result
                
                # Store result history
                self.health_check_results[name].append({
                    'timestamp': datetime.utcnow().isoformat(),
                    'status': result['status'],
                    'value': result.get('value'),
                    'error_message': result.get('error_message', '')
                })
                
                # Limit history size
                if len(self.health_check_results[name]) > 100:
                    self.health_check_results[name] = self.health_check_results[name][-100:]
                    
            except Exception as e:
                self.logger.error(f"Health check {name} failed: {e}")
                results[name] = {
                    'status': SystemHealthStatus.UNKNOWN.value,
                    'error_message': str(e),
                    'timestamp': datetime.utcnow().isoformat()
                }

        return results

    async def _run_single_health_check(self, health_check: HealthCheck) -> Dict[str, Any]:
        """Run a single health check."""
        start_time = time.time()
        
        try:
            # Get the check function
            check_func = getattr(self, health_check.check_function)
            result = await asyncio.wait_for(
                check_func(),
                timeout=health_check.timeout_seconds
            )
            
            duration = time.time() - start_time
            health_check.last_check = datetime.utcnow()
            
            if result['status'] == SystemHealthStatus.HEALTHY:
                health_check.last_success = datetime.utcnow()
                health_check.failure_count = 0
            else:
                health_check.failure_count += 1
                
            health_check.status = result['status']
            health_check.error_message = result.get('error_message', '')
            
            return {
                'status': result['status'].value,
                'value': result.get('value'),
                'duration_seconds': duration,
                'timestamp': health_check.last_check.isoformat(),
                'failure_count': health_check.failure_count,
                'error_message': result.get('error_message', '')
            }
            
        except asyncio.TimeoutError:
            health_check.failure_count += 1
            health_check.status = SystemHealthStatus.CRITICAL
            health_check.error_message = f"Health check timed out after {health_check.timeout_seconds}s"
            
            return {
                'status': SystemHealthStatus.CRITICAL.value,
                'duration_seconds': time.time() - start_time,
                'timestamp': datetime.utcnow().isoformat(),
                'failure_count': health_check.failure_count,
                'error_message': health_check.error_message
            }

    async def check_cpu_usage(self) -> Dict[str, Any]:
        """Health check for CPU usage."""
        cpu_percent = psutil.cpu_percent(interval=1)
        
        if cpu_percent >= self.resource_thresholds['cpu_usage']['critical']:
            status = SystemHealthStatus.CRITICAL
            message = f"CPU usage critically high: {cpu_percent:.1f}%"
        elif cpu_percent >= self.resource_thresholds['cpu_usage']['warning']:
            status = SystemHealthStatus.WARNING
            message = f"CPU usage high: {cpu_percent:.1f}%"
        else:
            status = SystemHealthStatus.HEALTHY
            message = f"CPU usage normal: {cpu_percent:.1f}%"
            
        return {
            'status': status,
            'value': cpu_percent,
            'message': message
        }

    async def check_memory_usage(self) -> Dict[str, Any]:
        """Health check for memory usage."""
        memory = psutil.virtual_memory()
        
        if memory.percent >= self.resource_thresholds['memory_usage']['critical']:
            status = SystemHealthStatus.CRITICAL
            message = f"Memory usage critically high: {memory.percent:.1f}%"
        elif memory.percent >= self.resource_thresholds['memory_usage']['warning']:
            status = SystemHealthStatus.WARNING
            message = f"Memory usage high: {memory.percent:.1f}%"
        else:
            status = SystemHealthStatus.HEALTHY
            message = f"Memory usage normal: {memory.percent:.1f}%"
            
        return {
            'status': status,
            'value': memory.percent,
            'message': message
        }

    async def check_disk_usage(self) -> Dict[str, Any]:
        """Health check for disk usage."""
        max_usage = 0
        critical_partitions = []
        warning_partitions = []
        
        for partition in psutil.disk_partitions():
            try:
                usage = psutil.disk_usage(partition.mountpoint)
                usage_percent = (usage.used / usage.total) * 100 if usage.total > 0 else 0
                max_usage = max(max_usage, usage_percent)
                
                if usage_percent >= self.resource_thresholds['disk_usage']['critical']:
                    critical_partitions.append(f"{partition.mountpoint} ({usage_percent:.1f}%)")
                elif usage_percent >= self.resource_thresholds['disk_usage']['warning']:
                    warning_partitions.append(f"{partition.mountpoint} ({usage_percent:.1f}%)")
                    
            except (PermissionError, FileNotFoundError):
                continue
        
        if critical_partitions:
            status = SystemHealthStatus.CRITICAL
            message = f"Critical disk usage on: {', '.join(critical_partitions)}"
        elif warning_partitions:
            status = SystemHealthStatus.WARNING
            message = f"High disk usage on: {', '.join(warning_partitions)}"
        else:
            status = SystemHealthStatus.HEALTHY
            message = f"Disk usage normal (max: {max_usage:.1f}%)"
            
        return {
            'status': status,
            'value': max_usage,
            'message': message
        }

    async def check_load_average(self) -> Dict[str, Any]:
        """Health check for system load average."""
        try:
            load_avg = psutil.getloadavg()[0]  # 1-minute load average
            cpu_count = psutil.cpu_count(logical=True)
            
            # Normalize load by CPU count
            normalized_load = load_avg / cpu_count if cpu_count > 0 else load_avg
            
            if normalized_load >= self.resource_thresholds['load_average']['critical'] / cpu_count:
                status = SystemHealthStatus.CRITICAL
                message = f"Load average critically high: {load_avg:.2f} (normalized: {normalized_load:.2f})"
            elif normalized_load >= self.resource_thresholds['load_average']['warning'] / cpu_count:
                status = SystemHealthStatus.WARNING
                message = f"Load average high: {load_avg:.2f} (normalized: {normalized_load:.2f})"
            else:
                status = SystemHealthStatus.HEALTHY
                message = f"Load average normal: {load_avg:.2f} (normalized: {normalized_load:.2f})"
                
            return {
                'status': status,
                'value': normalized_load,
                'message': message
            }
        except AttributeError:
            # Windows doesn't have load average
            return {
                'status': SystemHealthStatus.HEALTHY,
                'value': 0,
                'message': "Load average not available on this platform"
            }

    async def check_network_connectivity(self) -> Dict[str, Any]:
        """Health check for network connectivity."""
        try:
            # Check if we have network interfaces with traffic
            net_io = psutil.net_io_counters()
            if not net_io or (net_io.bytes_sent == 0 and net_io.bytes_recv == 0):
                return {
                    'status': SystemHealthStatus.WARNING,
                    'message': "No network activity detected"
                }
            
            # Check for active network connections
            connections = psutil.net_connections()
            active_connections = len([c for c in connections if hasattr(c, 'status') and c.status == 'ESTABLISHED'])
            
            if active_connections == 0:
                status = SystemHealthStatus.WARNING
                message = "No active network connections"
            else:
                status = SystemHealthStatus.HEALTHY
                message = f"{active_connections} active network connections"
                
            return {
                'status': status,
                'value': active_connections,
                'message': message
            }
        except Exception as e:
            return {
                'status': SystemHealthStatus.WARNING,
                'message': f"Network check failed: {str(e)}"
            }

    def get_system_overview(self) -> Dict[str, Any]:
        """Get comprehensive system overview."""
        return {
            'platform_info': self.platform_info,
            'health_status': {name: check.status.value for name, check in self.health_checks.items()},
            'resource_thresholds': self.resource_thresholds,
            'monitoring_config': {
                'collect_interval': self.collect_interval,
                'hostname': self.hostname,
                'docker_available': DOCKER_AVAILABLE and self.docker_client is not None,
                'kubernetes_available': KUBERNETES_AVAILABLE and self.k8s_client is not None
            },
            'last_check': datetime.utcnow().isoformat()
        }

    async def start_monitoring(self):
        """Start continuous system monitoring."""
        self.logger.info(f"Starting system monitoring on {self.hostname}")
        
        while True:
            try:
                # Collect metrics
                metrics = await self.collect_system_metrics()
                self.logger.debug(f"Collected {len(metrics)} system metrics")
                
                # Run health checks
                health_results = await self.run_health_checks()
                self.logger.debug(f"Ran {len(health_results)} health checks")
                
                # Sleep until next collection
                await asyncio.sleep(self.collect_interval)
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(10)  # Brief pause before retrying


# Global system monitor instance
_system_monitor: Optional[SystemMonitor] = None


def get_system_monitor() -> SystemMonitor:
    """Get global system monitor instance."""
    global _system_monitor
    if _system_monitor is None:
        _system_monitor = SystemMonitor()
    return _system_monitor


def start_system_monitoring(collect_interval: int = 30) -> SystemMonitor:
    """Initialize and start system monitoring."""
    global _system_monitor
    _system_monitor = SystemMonitor(collect_interval=collect_interval)
    return _system_monitor
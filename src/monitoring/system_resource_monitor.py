"""
Comprehensive System Resource Monitoring
Advanced system monitoring with intelligent alerting, predictive scaling,
and resource optimization recommendations.
"""
from __future__ import annotations

import asyncio
import json
import platform
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
import uuid

import psutil
from prometheus_client import Counter, Gauge, Histogram

from core.logging import get_logger
from monitoring.enterprise_prometheus_metrics import enterprise_metrics

logger = get_logger(__name__)


class ResourceType(Enum):
    """Types of system resources."""
    CPU = "cpu"
    MEMORY = "memory"
    DISK = "disk"
    NETWORK = "network"
    GPU = "gpu"
    PROCESS = "process"


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ResourceStatus(Enum):
    """Resource status levels."""
    OPTIMAL = "optimal"
    WARNING = "warning"
    CRITICAL = "critical"
    OVERLOADED = "overloaded"


@dataclass
class CPUMetrics:
    """CPU performance metrics."""
    overall_usage_percent: float = 0.0
    per_core_usage: List[float] = field(default_factory=list)
    load_average_1m: float = 0.0
    load_average_5m: float = 0.0
    load_average_15m: float = 0.0
    context_switches: int = 0
    interrupts: int = 0
    soft_interrupts: int = 0
    steal_time_percent: float = 0.0
    frequency_mhz: float = 0.0
    temperature_celsius: Optional[float] = None
    
    # Process-level CPU stats
    top_cpu_processes: List[Dict[str, Any]] = field(default_factory=list)
    cpu_time_user: float = 0.0
    cpu_time_system: float = 0.0
    cpu_time_idle: float = 0.0
    cpu_time_iowait: float = 0.0


@dataclass
class MemoryMetrics:
    """Memory performance metrics."""
    total_gb: float = 0.0
    available_gb: float = 0.0
    used_gb: float = 0.0
    usage_percent: float = 0.0
    
    # Virtual memory
    cached_gb: float = 0.0
    buffers_gb: float = 0.0
    shared_gb: float = 0.0
    
    # Swap memory
    swap_total_gb: float = 0.0
    swap_used_gb: float = 0.0
    swap_usage_percent: float = 0.0
    
    # Memory pressure indicators
    page_faults: int = 0
    page_swaps: int = 0
    memory_pressure_score: float = 0.0
    
    # Process-level memory stats
    top_memory_processes: List[Dict[str, Any]] = field(default_factory=list)
    
    # Memory growth tracking
    memory_growth_rate_mb_per_minute: float = 0.0


@dataclass
class DiskMetrics:
    """Disk performance metrics."""
    partitions: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # I/O statistics
    read_bytes_per_sec: float = 0.0
    write_bytes_per_sec: float = 0.0
    read_iops: float = 0.0
    write_iops: float = 0.0
    
    # I/O wait times
    avg_read_time_ms: float = 0.0
    avg_write_time_ms: float = 0.0
    io_wait_percent: float = 0.0
    
    # Queue depths
    avg_queue_length: float = 0.0
    
    # Top I/O processes
    top_io_processes: List[Dict[str, Any]] = field(default_factory=list)
    
    # Disk health
    total_disk_errors: int = 0


@dataclass
class NetworkMetrics:
    """Network performance metrics."""
    interfaces: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    # Overall network stats
    total_bytes_sent_per_sec: float = 0.0
    total_bytes_recv_per_sec: float = 0.0
    total_packets_sent_per_sec: float = 0.0
    total_packets_recv_per_sec: float = 0.0
    
    # Error rates
    total_errors_in: int = 0
    total_errors_out: int = 0
    total_drops_in: int = 0
    total_drops_out: int = 0
    
    # Connection statistics
    tcp_connections: Dict[str, int] = field(default_factory=dict)
    udp_connections: int = 0
    
    # Network latency (if available)
    avg_latency_ms: Optional[float] = None
    
    # Top network processes
    top_network_processes: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ProcessMetrics:
    """Process-level metrics."""
    total_processes: int = 0
    running_processes: int = 0
    sleeping_processes: int = 0
    zombie_processes: int = 0
    
    # Resource-intensive processes
    high_cpu_processes: List[Dict[str, Any]] = field(default_factory=list)
    high_memory_processes: List[Dict[str, Any]] = field(default_factory=list)
    high_io_processes: List[Dict[str, Any]] = field(default_factory=list)
    
    # Process lifecycle
    processes_started: int = 0
    processes_terminated: int = 0
    
    # Thread information
    total_threads: int = 0
    
    # File descriptors
    total_file_descriptors: int = 0
    max_file_descriptors: int = 0


@dataclass
class SystemHealth:
    """Overall system health assessment."""
    overall_status: ResourceStatus = ResourceStatus.OPTIMAL
    health_score: float = 100.0  # 0-100 scale
    
    # Resource status breakdown
    cpu_status: ResourceStatus = ResourceStatus.OPTIMAL
    memory_status: ResourceStatus = ResourceStatus.OPTIMAL
    disk_status: ResourceStatus = ResourceStatus.OPTIMAL
    network_status: ResourceStatus = ResourceStatus.OPTIMAL
    
    # Performance indicators
    performance_index: float = 100.0  # 0-100 scale
    stability_index: float = 100.0    # 0-100 scale
    efficiency_index: float = 100.0   # 0-100 scale
    
    # Predictive indicators
    predicted_issues: List[str] = field(default_factory=list)
    capacity_forecast: Dict[str, float] = field(default_factory=dict)  # Days until capacity limit
    
    # Uptime and availability
    uptime_seconds: float = 0.0
    boot_time: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ResourceAlert:
    """System resource alert."""
    alert_id: str
    alert_type: str
    severity: AlertSeverity
    resource_type: ResourceType
    metric_name: str
    current_value: float
    threshold_value: float
    message: str
    recommendation: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    resolved: bool = False
    resolution_time: Optional[datetime] = None
    business_impact: Optional[str] = None
    affected_processes: List[str] = field(default_factory=list)


class ComprehensiveSystemMonitor:
    """Advanced system resource monitoring with intelligent alerting."""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        
        # Metrics storage
        self.current_metrics = {}
        self.metrics_history: deque = deque(maxlen=10080)  # 7 days of minute-by-minute data
        self.health_history: deque[SystemHealth] = deque(maxlen=1440)  # 24 hours of minute data
        
        # Alert management
        self.active_alerts: Dict[str, ResourceAlert] = {}
        self.alert_history: deque[ResourceAlert] = deque(maxlen=10000)
        
        # Thresholds and configuration
        self.alert_thresholds = self._initialize_alert_thresholds()
        self.optimization_thresholds = self._initialize_optimization_thresholds()
        
        # Baseline performance tracking
        self.performance_baselines: Dict[str, float] = {}
        self.baseline_collection_started = datetime.utcnow()
        
        # Predictive modeling data
        self.trend_data: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1440))  # 24 hours
        
        # System information
        self.system_info = self._collect_system_info()
        
        # Monitoring state
        self.monitoring_active = False
        self.last_collection_time = datetime.utcnow()
        
    def _initialize_alert_thresholds(self) -> Dict[str, Dict[str, float]]:
        """Initialize alert thresholds for different resources."""
        return {
            "cpu": {
                "warning_percent": 75.0,
                "error_percent": 85.0,
                "critical_percent": 95.0,
                "load_warning": 0.8,  # Per core
                "load_error": 1.0,
                "load_critical": 1.5
            },
            "memory": {
                "warning_percent": 80.0,
                "error_percent": 90.0,
                "critical_percent": 95.0,
                "swap_warning_percent": 10.0,
                "swap_error_percent": 50.0,
                "swap_critical_percent": 80.0
            },
            "disk": {
                "usage_warning_percent": 80.0,
                "usage_error_percent": 90.0,
                "usage_critical_percent": 95.0,
                "io_wait_warning_percent": 20.0,
                "io_wait_error_percent": 40.0,
                "io_wait_critical_percent": 60.0
            },
            "network": {
                "error_rate_warning_percent": 1.0,
                "error_rate_error_percent": 5.0,
                "error_rate_critical_percent": 10.0,
                "utilization_warning_percent": 70.0,
                "utilization_error_percent": 85.0,
                "utilization_critical_percent": 95.0
            },
            "process": {
                "zombie_warning_count": 5,
                "zombie_error_count": 10,
                "zombie_critical_count": 20,
                "fd_usage_warning_percent": 80.0,
                "fd_usage_error_percent": 90.0,
                "fd_usage_critical_percent": 95.0
            }
        }
    
    def _initialize_optimization_thresholds(self) -> Dict[str, float]:
        """Initialize thresholds for optimization recommendations."""
        return {
            "cpu_underutilization": 20.0,     # Below 20% average CPU
            "memory_underutilization": 30.0,  # Below 30% memory usage
            "disk_space_waste": 50.0,         # Over 50% free space consistently
            "network_underutilization": 10.0, # Below 10% network usage
            "process_efficiency": 75.0,       # Process efficiency score
            "resource_balance": 0.3           # Imbalance threshold between resources
        }
    
    def _collect_system_info(self) -> Dict[str, Any]:
        """Collect static system information."""
        try:
            boot_time = datetime.fromtimestamp(psutil.boot_time())
            
            return {
                "hostname": platform.node(),
                "platform": platform.platform(),
                "architecture": platform.architecture()[0],
                "processor": platform.processor(),
                "cpu_count_physical": psutil.cpu_count(logical=False),
                "cpu_count_logical": psutil.cpu_count(logical=True),
                "memory_total_gb": psutil.virtual_memory().total / (1024**3),
                "boot_time": boot_time.isoformat(),
                "python_version": platform.python_version()
            }
        except Exception as e:
            self.logger.error(f"Error collecting system info: {e}")
            return {}
    
    async def collect_cpu_metrics(self) -> CPUMetrics:
        """Collect comprehensive CPU metrics."""
        try:
            # Overall CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_times = psutil.cpu_times()
            
            # Per-core usage
            cpu_per_core = psutil.cpu_percent(percpu=True, interval=None)
            
            # Load averages (Unix systems)
            load_avg = [0.0, 0.0, 0.0]
            try:
                load_avg = psutil.getloadavg()
            except AttributeError:
                # Windows doesn't have load average
                pass
            
            # CPU frequency
            cpu_freq = psutil.cpu_freq()
            frequency = cpu_freq.current if cpu_freq else 0.0
            
            # CPU statistics
            cpu_stats = psutil.cpu_stats()
            
            # Top CPU processes
            top_processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
                try:
                    if proc.info['cpu_percent'] and proc.info['cpu_percent'] > 1.0:
                        top_processes.append({
                            'pid': proc.info['pid'],
                            'name': proc.info['name'],
                            'cpu_percent': proc.info['cpu_percent'],
                            'memory_percent': proc.info['memory_percent']
                        })
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            # Sort by CPU usage and take top 10
            top_processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
            top_processes = top_processes[:10]
            
            return CPUMetrics(
                overall_usage_percent=cpu_percent,
                per_core_usage=cpu_per_core,
                load_average_1m=load_avg[0],
                load_average_5m=load_avg[1],
                load_average_15m=load_avg[2],
                context_switches=cpu_stats.ctx_switches,
                interrupts=cpu_stats.interrupts,
                soft_interrupts=cpu_stats.soft_interrupts,
                frequency_mhz=frequency,
                top_cpu_processes=top_processes,
                cpu_time_user=cpu_times.user,
                cpu_time_system=cpu_times.system,
                cpu_time_idle=cpu_times.idle,
                cpu_time_iowait=getattr(cpu_times, 'iowait', 0.0)
            )
            
        except Exception as e:
            self.logger.error(f"Error collecting CPU metrics: {e}")
            return CPUMetrics()
    
    async def collect_memory_metrics(self) -> MemoryMetrics:
        """Collect comprehensive memory metrics."""
        try:
            # Virtual memory
            vmem = psutil.virtual_memory()
            
            # Swap memory
            swap = psutil.swap_memory()
            
            # Top memory processes
            top_processes = []
            for proc in psutil.process_iter(['pid', 'name', 'memory_percent', 'memory_info']):
                try:
                    if proc.info['memory_percent'] and proc.info['memory_percent'] > 0.5:
                        memory_mb = proc.info['memory_info'].rss / (1024 * 1024)
                        top_processes.append({
                            'pid': proc.info['pid'],
                            'name': proc.info['name'],
                            'memory_percent': proc.info['memory_percent'],
                            'memory_mb': memory_mb
                        })
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            # Sort by memory usage and take top 10
            top_processes.sort(key=lambda x: x['memory_percent'], reverse=True)
            top_processes = top_processes[:10]
            
            # Calculate memory pressure score
            memory_pressure = min(100.0, (vmem.percent + swap.percent * 0.5))
            
            return MemoryMetrics(
                total_gb=vmem.total / (1024**3),
                available_gb=vmem.available / (1024**3),
                used_gb=vmem.used / (1024**3),
                usage_percent=vmem.percent,
                cached_gb=getattr(vmem, 'cached', 0) / (1024**3),
                buffers_gb=getattr(vmem, 'buffers', 0) / (1024**3),
                shared_gb=getattr(vmem, 'shared', 0) / (1024**3),
                swap_total_gb=swap.total / (1024**3),
                swap_used_gb=swap.used / (1024**3),
                swap_usage_percent=swap.percent,
                memory_pressure_score=memory_pressure,
                top_memory_processes=top_processes
            )
            
        except Exception as e:
            self.logger.error(f"Error collecting memory metrics: {e}")
            return MemoryMetrics()
    
    async def collect_disk_metrics(self) -> DiskMetrics:
        """Collect comprehensive disk metrics."""
        try:
            partitions = {}
            
            # Disk usage for all partitions
            for partition in psutil.disk_partitions():
                try:
                    usage = psutil.disk_usage(partition.mountpoint)
                    partitions[partition.mountpoint] = {
                        'device': partition.device,
                        'filesystem': partition.fstype,
                        'total_gb': usage.total / (1024**3),
                        'used_gb': usage.used / (1024**3),
                        'free_gb': usage.free / (1024**3),
                        'usage_percent': (usage.used / usage.total) * 100 if usage.total > 0 else 0
                    }
                except (PermissionError, FileNotFoundError):
                    continue
            
            # Disk I/O statistics
            disk_io_before = psutil.disk_io_counters()
            await asyncio.sleep(1)  # Wait 1 second for I/O measurement
            disk_io_after = psutil.disk_io_counters()
            
            read_bytes_per_sec = 0.0
            write_bytes_per_sec = 0.0
            read_iops = 0.0
            write_iops = 0.0
            
            if disk_io_before and disk_io_after:
                read_bytes_per_sec = disk_io_after.read_bytes - disk_io_before.read_bytes
                write_bytes_per_sec = disk_io_after.write_bytes - disk_io_before.write_bytes
                read_iops = disk_io_after.read_count - disk_io_before.read_count
                write_iops = disk_io_after.write_count - disk_io_before.write_count
            
            # Top I/O processes (simplified)
            top_io_processes = []
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    io_counters = proc.io_counters()
                    if io_counters and (io_counters.read_bytes + io_counters.write_bytes) > 1024*1024:  # >1MB
                        top_io_processes.append({
                            'pid': proc.info['pid'],
                            'name': proc.info['name'],
                            'read_mb': io_counters.read_bytes / (1024*1024),
                            'write_mb': io_counters.write_bytes / (1024*1024)
                        })
                except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
                    continue
            
            # Sort by total I/O and take top 10
            top_io_processes.sort(key=lambda x: x['read_mb'] + x['write_mb'], reverse=True)
            top_io_processes = top_io_processes[:10]
            
            return DiskMetrics(
                partitions=partitions,
                read_bytes_per_sec=read_bytes_per_sec,
                write_bytes_per_sec=write_bytes_per_sec,
                read_iops=read_iops,
                write_iops=write_iops,
                top_io_processes=top_io_processes
            )
            
        except Exception as e:
            self.logger.error(f"Error collecting disk metrics: {e}")
            return DiskMetrics()
    
    async def collect_network_metrics(self) -> NetworkMetrics:
        """Collect comprehensive network metrics."""
        try:
            interfaces = {}
            
            # Network I/O for all interfaces
            net_io_before = psutil.net_io_counters(pernic=True)
            await asyncio.sleep(1)  # Wait 1 second for network measurement
            net_io_after = psutil.net_io_counters(pernic=True)
            
            total_bytes_sent_per_sec = 0.0
            total_bytes_recv_per_sec = 0.0
            total_packets_sent_per_sec = 0.0
            total_packets_recv_per_sec = 0.0
            total_errors = 0
            total_drops = 0
            
            for interface, stats_after in net_io_after.items():
                if interface in net_io_before:
                    stats_before = net_io_before[interface]
                    
                    bytes_sent_per_sec = stats_after.bytes_sent - stats_before.bytes_sent
                    bytes_recv_per_sec = stats_after.bytes_recv - stats_before.bytes_recv
                    packets_sent_per_sec = stats_after.packets_sent - stats_before.packets_sent
                    packets_recv_per_sec = stats_after.packets_recv - stats_before.packets_recv
                    
                    interfaces[interface] = {
                        'bytes_sent_per_sec': bytes_sent_per_sec,
                        'bytes_recv_per_sec': bytes_recv_per_sec,
                        'packets_sent_per_sec': packets_sent_per_sec,
                        'packets_recv_per_sec': packets_recv_per_sec,
                        'errors_in': stats_after.errin,
                        'errors_out': stats_after.errout,
                        'drops_in': stats_after.dropin,
                        'drops_out': stats_after.dropout
                    }
                    
                    total_bytes_sent_per_sec += bytes_sent_per_sec
                    total_bytes_recv_per_sec += bytes_recv_per_sec
                    total_packets_sent_per_sec += packets_sent_per_sec
                    total_packets_recv_per_sec += packets_recv_per_sec
                    total_errors += stats_after.errin + stats_after.errout
                    total_drops += stats_after.dropin + stats_after.dropout
            
            # Network connections
            tcp_connections = defaultdict(int)
            udp_connections = 0
            
            try:
                for conn in psutil.net_connections():
                    if conn.type == psutil.SOCK_STREAM:  # TCP
                        tcp_connections[conn.status] += 1
                    elif conn.type == psutil.SOCK_DGRAM:  # UDP
                        udp_connections += 1
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                # May not have permission to access all connections
                pass
            
            return NetworkMetrics(
                interfaces=interfaces,
                total_bytes_sent_per_sec=total_bytes_sent_per_sec,
                total_bytes_recv_per_sec=total_bytes_recv_per_sec,
                total_packets_sent_per_sec=total_packets_sent_per_sec,
                total_packets_recv_per_sec=total_packets_recv_per_sec,
                total_errors_in=total_errors,
                total_errors_out=total_errors,
                total_drops_in=total_drops,
                total_drops_out=total_drops,
                tcp_connections=dict(tcp_connections),
                udp_connections=udp_connections
            )
            
        except Exception as e:
            self.logger.error(f"Error collecting network metrics: {e}")
            return NetworkMetrics()
    
    async def collect_process_metrics(self) -> ProcessMetrics:
        """Collect comprehensive process metrics."""
        try:
            total_processes = 0
            running_processes = 0
            sleeping_processes = 0
            zombie_processes = 0
            total_threads = 0
            total_fds = 0
            
            high_cpu_processes = []
            high_memory_processes = []
            
            for proc in psutil.process_iter(['pid', 'name', 'status', 'cpu_percent', 
                                           'memory_percent', 'num_threads', 'num_fds']):
                try:
                    total_processes += 1
                    
                    status = proc.info['status']
                    if status == psutil.STATUS_RUNNING:
                        running_processes += 1
                    elif status == psutil.STATUS_SLEEPING:
                        sleeping_processes += 1
                    elif status == psutil.STATUS_ZOMBIE:
                        zombie_processes += 1
                    
                    if proc.info['num_threads']:
                        total_threads += proc.info['num_threads']
                    
                    if proc.info['num_fds']:
                        total_fds += proc.info['num_fds']
                    
                    # High CPU processes
                    if proc.info['cpu_percent'] and proc.info['cpu_percent'] > 5.0:
                        high_cpu_processes.append({
                            'pid': proc.info['pid'],
                            'name': proc.info['name'],
                            'cpu_percent': proc.info['cpu_percent']
                        })
                    
                    # High memory processes
                    if proc.info['memory_percent'] and proc.info['memory_percent'] > 2.0:
                        high_memory_processes.append({
                            'pid': proc.info['pid'],
                            'name': proc.info['name'],
                            'memory_percent': proc.info['memory_percent']
                        })
                        
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            # Sort and limit high resource processes
            high_cpu_processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
            high_memory_processes.sort(key=lambda x: x['memory_percent'], reverse=True)
            
            # Get system-wide file descriptor limit
            try:
                with open('/proc/sys/fs/file-max', 'r') as f:
                    max_fds = int(f.read().strip())
            except (FileNotFoundError, ValueError, PermissionError):
                max_fds = 1024 * 1024  # Default estimate
            
            return ProcessMetrics(
                total_processes=total_processes,
                running_processes=running_processes,
                sleeping_processes=sleeping_processes,
                zombie_processes=zombie_processes,
                high_cpu_processes=high_cpu_processes[:10],
                high_memory_processes=high_memory_processes[:10],
                total_threads=total_threads,
                total_file_descriptors=total_fds,
                max_file_descriptors=max_fds
            )
            
        except Exception as e:
            self.logger.error(f"Error collecting process metrics: {e}")
            return ProcessMetrics()
    
    async def collect_all_metrics(self) -> Dict[str, Any]:
        """Collect all system metrics."""
        try:
            start_time = time.time()
            
            # Collect all metrics in parallel
            cpu_metrics, memory_metrics, disk_metrics, network_metrics, process_metrics = await asyncio.gather(
                self.collect_cpu_metrics(),
                self.collect_memory_metrics(),
                self.collect_disk_metrics(),
                self.collect_network_metrics(),
                self.collect_process_metrics(),
                return_exceptions=True
            )
            
            # Handle any exceptions
            if isinstance(cpu_metrics, Exception):
                self.logger.error(f"CPU metrics collection failed: {cpu_metrics}")
                cpu_metrics = CPUMetrics()
            
            if isinstance(memory_metrics, Exception):
                self.logger.error(f"Memory metrics collection failed: {memory_metrics}")
                memory_metrics = MemoryMetrics()
            
            if isinstance(disk_metrics, Exception):
                self.logger.error(f"Disk metrics collection failed: {disk_metrics}")
                disk_metrics = DiskMetrics()
            
            if isinstance(network_metrics, Exception):
                self.logger.error(f"Network metrics collection failed: {network_metrics}")
                network_metrics = NetworkMetrics()
            
            if isinstance(process_metrics, Exception):
                self.logger.error(f"Process metrics collection failed: {process_metrics}")
                process_metrics = ProcessMetrics()
            
            collection_time = time.time() - start_time
            
            metrics = {
                'timestamp': datetime.utcnow(),
                'collection_time_ms': collection_time * 1000,
                'cpu': cpu_metrics,
                'memory': memory_metrics,
                'disk': disk_metrics,
                'network': network_metrics,
                'process': process_metrics
            }
            
            # Store current metrics and history
            self.current_metrics = metrics
            self.metrics_history.append(metrics)
            
            # Update Prometheus metrics
            self._update_prometheus_metrics(metrics)
            
            # Store trend data for predictions
            self._update_trend_data(metrics)
            
            self.last_collection_time = datetime.utcnow()
            
            self.logger.debug(f"System metrics collected in {collection_time:.2f}s")
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error collecting system metrics: {e}")
            return {}
    
    def _update_prometheus_metrics(self, metrics: Dict[str, Any]):
        """Update Prometheus metrics with system data."""
        try:
            cpu_metrics = metrics['cpu']
            memory_metrics = metrics['memory']
            disk_metrics = metrics['disk']
            network_metrics = metrics['network']
            
            # CPU metrics
            enterprise_metrics.system_cpu_usage.labels(
                core='all',
                instance='data-platform-1'
            ).set(cpu_metrics.overall_usage_percent)
            
            # Memory metrics
            enterprise_metrics.system_memory_usage.labels(
                type='used',
                instance='data-platform-1'
            ).set(memory_metrics.used_gb * 1024 * 1024 * 1024)  # Convert to bytes
            
            enterprise_metrics.system_memory_usage.labels(
                type='available',
                instance='data-platform-1'
            ).set(memory_metrics.available_gb * 1024 * 1024 * 1024)
            
            # Disk metrics
            for mountpoint, partition_data in disk_metrics.partitions.items():
                enterprise_metrics.system_disk_usage.labels(
                    device=partition_data['device'],
                    mountpoint=mountpoint,
                    instance='data-platform-1'
                ).set(partition_data['usage_percent'])
            
            # Network metrics
            enterprise_metrics.system_network_bytes.labels(
                interface='all',
                direction='sent',
                instance='data-platform-1'
            ).inc(network_metrics.total_bytes_sent_per_sec)
            
            enterprise_metrics.system_network_bytes.labels(
                interface='all',
                direction='received',
                instance='data-platform-1'
            ).inc(network_metrics.total_bytes_recv_per_sec)
            
        except Exception as e:
            self.logger.error(f"Error updating Prometheus metrics: {e}")
    
    def _update_trend_data(self, metrics: Dict[str, Any]):
        """Update trend data for predictive analysis."""
        try:
            timestamp = datetime.utcnow()
            
            # Store key metrics for trend analysis
            self.trend_data['cpu_usage'].append({
                'timestamp': timestamp,
                'value': metrics['cpu'].overall_usage_percent
            })
            
            self.trend_data['memory_usage'].append({
                'timestamp': timestamp,
                'value': metrics['memory'].usage_percent
            })
            
            # Disk usage trends
            for mountpoint, partition_data in metrics['disk'].partitions.items():
                key = f'disk_usage_{mountpoint}'
                self.trend_data[key].append({
                    'timestamp': timestamp,
                    'value': partition_data['usage_percent']
                })
            
            # Network usage trends
            total_network_bytes = (metrics['network'].total_bytes_sent_per_sec + 
                                 metrics['network'].total_bytes_recv_per_sec)
            self.trend_data['network_bytes'].append({
                'timestamp': timestamp,
                'value': total_network_bytes
            })
            
        except Exception as e:
            self.logger.error(f"Error updating trend data: {e}")
    
    async def assess_system_health(self) -> SystemHealth:
        """Assess overall system health and performance."""
        try:
            if not self.current_metrics:
                return SystemHealth()
            
            cpu_metrics = self.current_metrics['cpu']
            memory_metrics = self.current_metrics['memory']
            disk_metrics = self.current_metrics['disk']
            network_metrics = self.current_metrics['network']
            process_metrics = self.current_metrics['process']
            
            health = SystemHealth()
            
            # CPU health assessment
            cpu_usage = cpu_metrics.overall_usage_percent
            if cpu_usage >= self.alert_thresholds['cpu']['critical_percent']:
                health.cpu_status = ResourceStatus.CRITICAL
            elif cpu_usage >= self.alert_thresholds['cpu']['error_percent']:
                health.cpu_status = ResourceStatus.OVERLOADED
            elif cpu_usage >= self.alert_thresholds['cpu']['warning_percent']:
                health.cpu_status = ResourceStatus.WARNING
            else:
                health.cpu_status = ResourceStatus.OPTIMAL
            
            # Memory health assessment
            memory_usage = memory_metrics.usage_percent
            swap_usage = memory_metrics.swap_usage_percent
            
            if (memory_usage >= self.alert_thresholds['memory']['critical_percent'] or
                swap_usage >= self.alert_thresholds['memory']['swap_critical_percent']):
                health.memory_status = ResourceStatus.CRITICAL
            elif (memory_usage >= self.alert_thresholds['memory']['error_percent'] or
                  swap_usage >= self.alert_thresholds['memory']['swap_error_percent']):
                health.memory_status = ResourceStatus.OVERLOADED
            elif (memory_usage >= self.alert_thresholds['memory']['warning_percent'] or
                  swap_usage >= self.alert_thresholds['memory']['swap_warning_percent']):
                health.memory_status = ResourceStatus.WARNING
            else:
                health.memory_status = ResourceStatus.OPTIMAL
            
            # Disk health assessment
            max_disk_usage = 0.0
            for partition_data in disk_metrics.partitions.values():
                max_disk_usage = max(max_disk_usage, partition_data['usage_percent'])
            
            if max_disk_usage >= self.alert_thresholds['disk']['usage_critical_percent']:
                health.disk_status = ResourceStatus.CRITICAL
            elif max_disk_usage >= self.alert_thresholds['disk']['usage_error_percent']:
                health.disk_status = ResourceStatus.OVERLOADED
            elif max_disk_usage >= self.alert_thresholds['disk']['usage_warning_percent']:
                health.disk_status = ResourceStatus.WARNING
            else:
                health.disk_status = ResourceStatus.OPTIMAL
            
            # Network health assessment
            total_errors = network_metrics.total_errors_in + network_metrics.total_errors_out
            total_packets = (network_metrics.total_packets_sent_per_sec + 
                           network_metrics.total_packets_recv_per_sec)
            
            error_rate = (total_errors / total_packets * 100) if total_packets > 0 else 0
            
            if error_rate >= self.alert_thresholds['network']['error_rate_critical_percent']:
                health.network_status = ResourceStatus.CRITICAL
            elif error_rate >= self.alert_thresholds['network']['error_rate_error_percent']:
                health.network_status = ResourceStatus.OVERLOADED
            elif error_rate >= self.alert_thresholds['network']['error_rate_warning_percent']:
                health.network_status = ResourceStatus.WARNING
            else:
                health.network_status = ResourceStatus.OPTIMAL
            
            # Overall health score calculation
            status_scores = {
                ResourceStatus.OPTIMAL: 100,
                ResourceStatus.WARNING: 75,
                ResourceStatus.OVERLOADED: 50,
                ResourceStatus.CRITICAL: 25
            }
            
            resource_scores = [
                status_scores[health.cpu_status],
                status_scores[health.memory_status],
                status_scores[health.disk_status],
                status_scores[health.network_status]
            ]
            
            health.health_score = sum(resource_scores) / len(resource_scores)
            
            # Determine overall status
            if any(status == ResourceStatus.CRITICAL for status in 
                   [health.cpu_status, health.memory_status, health.disk_status, health.network_status]):
                health.overall_status = ResourceStatus.CRITICAL
            elif any(status == ResourceStatus.OVERLOADED for status in 
                     [health.cpu_status, health.memory_status, health.disk_status, health.network_status]):
                health.overall_status = ResourceStatus.OVERLOADED
            elif any(status == ResourceStatus.WARNING for status in 
                     [health.cpu_status, health.memory_status, health.disk_status, health.network_status]):
                health.overall_status = ResourceStatus.WARNING
            else:
                health.overall_status = ResourceStatus.OPTIMAL
            
            # Performance indices
            health.performance_index = max(0, 100 - cpu_usage - (memory_usage * 0.5) - (max_disk_usage * 0.3))
            health.stability_index = max(0, 100 - (process_metrics.zombie_processes * 5) - (error_rate * 10))
            health.efficiency_index = self._calculate_efficiency_index()
            
            # Uptime
            boot_time = datetime.fromtimestamp(psutil.boot_time())
            health.uptime_seconds = (datetime.utcnow() - boot_time).total_seconds()
            health.boot_time = boot_time
            
            # Predictive analysis
            health.predicted_issues = await self._predict_issues()
            health.capacity_forecast = self._forecast_capacity()
            
            # Store health history
            self.health_history.append(health)
            
            return health
            
        except Exception as e:
            self.logger.error(f"Error assessing system health: {e}")
            return SystemHealth()
    
    def _calculate_efficiency_index(self) -> float:
        """Calculate system efficiency index."""
        try:
            if not self.current_metrics:
                return 100.0
            
            cpu_metrics = self.current_metrics['cpu']
            memory_metrics = self.current_metrics['memory']
            
            # Efficiency based on resource utilization balance
            cpu_usage = cpu_metrics.overall_usage_percent
            memory_usage = memory_metrics.usage_percent
            
            # Ideal is balanced usage around 60-70%
            cpu_efficiency = max(0, 100 - abs(cpu_usage - 65) * 2)
            memory_efficiency = max(0, 100 - abs(memory_usage - 65) * 2)
            
            return (cpu_efficiency + memory_efficiency) / 2
            
        except Exception:
            return 100.0
    
    async def _predict_issues(self) -> List[str]:
        """Predict potential system issues based on trends."""
        predicted_issues = []
        
        try:
            # Need at least 30 minutes of data for predictions
            if len(self.trend_data['cpu_usage']) < 30:
                return predicted_issues
            
            # CPU trend analysis
            cpu_trend = list(self.trend_data['cpu_usage'])[-30:]  # Last 30 minutes
            cpu_values = [point['value'] for point in cpu_trend]
            
            # Simple linear trend detection
            if len(cpu_values) >= 10:
                recent_avg = sum(cpu_values[-5:]) / 5
                earlier_avg = sum(cpu_values[:5]) / 5
                
                if recent_avg > earlier_avg + 20:  # Increasing trend
                    predicted_issues.append("CPU usage trending upward - potential overload in next hour")
            
            # Memory trend analysis
            memory_trend = list(self.trend_data['memory_usage'])[-30:]
            memory_values = [point['value'] for point in memory_trend]
            
            if len(memory_values) >= 10:
                recent_avg = sum(memory_values[-5:]) / 5
                earlier_avg = sum(memory_values[:5]) / 5
                
                if recent_avg > earlier_avg + 15:  # Memory increasing
                    predicted_issues.append("Memory usage trending upward - possible memory leak")
            
            # Disk usage trend analysis (slower moving)
            for key in self.trend_data.keys():
                if key.startswith('disk_usage_') and len(self.trend_data[key]) >= 60:  # 1 hour
                    disk_trend = list(self.trend_data[key])[-60:]
                    disk_values = [point['value'] for point in disk_trend]
                    
                    recent_avg = sum(disk_values[-10:]) / 10
                    earlier_avg = sum(disk_values[:10]) / 10
                    
                    if recent_avg > earlier_avg + 5:  # Disk filling up
                        mountpoint = key.replace('disk_usage_', '')
                        predicted_issues.append(f"Disk usage trending upward on {mountpoint}")
                        
        except Exception as e:
            self.logger.error(f"Error predicting issues: {e}")
        
        return predicted_issues
    
    def _forecast_capacity(self) -> Dict[str, float]:
        """Forecast days until capacity limits are reached."""
        forecast = {}
        
        try:
            # CPU capacity forecast
            if len(self.trend_data['cpu_usage']) >= 60:  # 1 hour minimum
                cpu_trend = list(self.trend_data['cpu_usage'])[-60:]
                cpu_values = [point['value'] for point in cpu_trend]
                
                # Simple linear regression for growth rate
                if len(cpu_values) >= 10:
                    recent_growth = (cpu_values[-1] - cpu_values[-10]) / 10  # Per minute growth
                    if recent_growth > 0:
                        minutes_to_100 = (100 - cpu_values[-1]) / recent_growth
                        forecast['cpu_days'] = minutes_to_100 / (24 * 60)
            
            # Memory capacity forecast
            if len(self.trend_data['memory_usage']) >= 60:
                memory_trend = list(self.trend_data['memory_usage'])[-60:]
                memory_values = [point['value'] for point in memory_trend]
                
                if len(memory_values) >= 10:
                    recent_growth = (memory_values[-1] - memory_values[-10]) / 10
                    if recent_growth > 0:
                        minutes_to_100 = (100 - memory_values[-1]) / recent_growth
                        forecast['memory_days'] = minutes_to_100 / (24 * 60)
            
            # Disk capacity forecasts
            for key in self.trend_data.keys():
                if key.startswith('disk_usage_') and len(self.trend_data[key]) >= 120:  # 2 hours
                    disk_trend = list(self.trend_data[key])[-120:]
                    disk_values = [point['value'] for point in disk_trend]
                    
                    if len(disk_values) >= 20:
                        recent_growth = (disk_values[-1] - disk_values[-20]) / 20
                        if recent_growth > 0:
                            minutes_to_100 = (100 - disk_values[-1]) / recent_growth
                            mountpoint = key.replace('disk_usage_', '')
                            forecast[f'disk_{mountpoint}_days'] = minutes_to_100 / (24 * 60)
                            
        except Exception as e:
            self.logger.error(f"Error forecasting capacity: {e}")
        
        return forecast
    
    async def detect_resource_alerts(self) -> List[ResourceAlert]:
        """Detect system resource alerts based on thresholds."""
        alerts = []
        
        try:
            if not self.current_metrics:
                return alerts
            
            current_time = datetime.utcnow()
            cpu_metrics = self.current_metrics['cpu']
            memory_metrics = self.current_metrics['memory']
            disk_metrics = self.current_metrics['disk']
            network_metrics = self.current_metrics['network']
            process_metrics = self.current_metrics['process']
            
            # CPU alerts
            cpu_usage = cpu_metrics.overall_usage_percent
            if cpu_usage >= self.alert_thresholds['cpu']['critical_percent']:
                alert = ResourceAlert(
                    alert_id=str(uuid.uuid4()),
                    alert_type="cpu_critical",
                    severity=AlertSeverity.CRITICAL,
                    resource_type=ResourceType.CPU,
                    metric_name="cpu_usage_percent",
                    current_value=cpu_usage,
                    threshold_value=self.alert_thresholds['cpu']['critical_percent'],
                    message=f"Critical CPU usage: {cpu_usage:.1f}%",
                    recommendation="Scale up resources or optimize CPU-intensive processes",
                    business_impact="High - affects $27.8M platform performance",
                    affected_processes=[proc['name'] for proc in cpu_metrics.top_cpu_processes[:3]]
                )
                alerts.append(alert)
            
            elif cpu_usage >= self.alert_thresholds['cpu']['error_percent']:
                alert = ResourceAlert(
                    alert_id=str(uuid.uuid4()),
                    alert_type="cpu_high",
                    severity=AlertSeverity.ERROR,
                    resource_type=ResourceType.CPU,
                    metric_name="cpu_usage_percent",
                    current_value=cpu_usage,
                    threshold_value=self.alert_thresholds['cpu']['error_percent'],
                    message=f"High CPU usage: {cpu_usage:.1f}%",
                    recommendation="Monitor CPU-intensive processes and consider optimization"
                )
                alerts.append(alert)
            
            # Memory alerts
            memory_usage = memory_metrics.usage_percent
            if memory_usage >= self.alert_thresholds['memory']['critical_percent']:
                alert = ResourceAlert(
                    alert_id=str(uuid.uuid4()),
                    alert_type="memory_critical",
                    severity=AlertSeverity.CRITICAL,
                    resource_type=ResourceType.MEMORY,
                    metric_name="memory_usage_percent",
                    current_value=memory_usage,
                    threshold_value=self.alert_thresholds['memory']['critical_percent'],
                    message=f"Critical memory usage: {memory_usage:.1f}%",
                    recommendation="Immediate action required - scale memory or restart services",
                    business_impact="Critical - potential system instability",
                    affected_processes=[proc['name'] for proc in memory_metrics.top_memory_processes[:3]]
                )
                alerts.append(alert)
            
            # Swap usage alert
            if memory_metrics.swap_usage_percent >= self.alert_thresholds['memory']['swap_error_percent']:
                alert = ResourceAlert(
                    alert_id=str(uuid.uuid4()),
                    alert_type="swap_high",
                    severity=AlertSeverity.ERROR,
                    resource_type=ResourceType.MEMORY,
                    metric_name="swap_usage_percent",
                    current_value=memory_metrics.swap_usage_percent,
                    threshold_value=self.alert_thresholds['memory']['swap_error_percent'],
                    message=f"High swap usage: {memory_metrics.swap_usage_percent:.1f}%",
                    recommendation="Add more RAM or optimize memory usage"
                )
                alerts.append(alert)
            
            # Disk alerts
            for mountpoint, partition_data in disk_metrics.partitions.items():
                disk_usage = partition_data['usage_percent']
                if disk_usage >= self.alert_thresholds['disk']['usage_critical_percent']:
                    alert = ResourceAlert(
                        alert_id=str(uuid.uuid4()),
                        alert_type="disk_critical",
                        severity=AlertSeverity.CRITICAL,
                        resource_type=ResourceType.DISK,
                        metric_name="disk_usage_percent",
                        current_value=disk_usage,
                        threshold_value=self.alert_thresholds['disk']['usage_critical_percent'],
                        message=f"Critical disk space on {mountpoint}: {disk_usage:.1f}% full",
                        recommendation=f"Immediate cleanup required on {mountpoint}",
                        business_impact="High - potential data write failures"
                    )
                    alerts.append(alert)
            
            # Process alerts
            if process_metrics.zombie_processes >= self.alert_thresholds['process']['zombie_error_count']:
                alert = ResourceAlert(
                    alert_id=str(uuid.uuid4()),
                    alert_type="zombie_processes",
                    severity=AlertSeverity.ERROR,
                    resource_type=ResourceType.PROCESS,
                    metric_name="zombie_processes",
                    current_value=process_metrics.zombie_processes,
                    threshold_value=self.alert_thresholds['process']['zombie_error_count'],
                    message=f"High zombie process count: {process_metrics.zombie_processes}",
                    recommendation="Investigate parent processes and restart if necessary"
                )
                alerts.append(alert)
            
            # File descriptor alert
            if process_metrics.max_file_descriptors > 0:
                fd_usage_percent = (process_metrics.total_file_descriptors / 
                                  process_metrics.max_file_descriptors) * 100
                
                if fd_usage_percent >= self.alert_thresholds['process']['fd_usage_error_percent']:
                    alert = ResourceAlert(
                        alert_id=str(uuid.uuid4()),
                        alert_type="file_descriptors_high",
                        severity=AlertSeverity.ERROR,
                        resource_type=ResourceType.PROCESS,
                        metric_name="file_descriptor_usage_percent",
                        current_value=fd_usage_percent,
                        threshold_value=self.alert_thresholds['process']['fd_usage_error_percent'],
                        message=f"High file descriptor usage: {fd_usage_percent:.1f}%",
                        recommendation="Check for file descriptor leaks in applications"
                    )
                    alerts.append(alert)
            
            # Store new alerts
            for alert in alerts:
                self.active_alerts[alert.alert_id] = alert
                self.alert_history.append(alert)
            
        except Exception as e:
            self.logger.error(f"Error detecting resource alerts: {e}")
        
        return alerts
    
    async def start_monitoring(self, interval_seconds: int = 60):
        """Start continuous system monitoring."""
        self.monitoring_active = True
        self.logger.info(f"Starting system resource monitoring with {interval_seconds}s interval")
        
        while self.monitoring_active:
            try:
                start_time = time.time()
                
                # Collect all metrics
                await self.collect_all_metrics()
                
                # Assess system health
                health = await self.assess_system_health()
                
                # Check for alerts
                alerts = await self.detect_resource_alerts()
                
                if alerts:
                    self.logger.warning(f"Detected {len(alerts)} system resource alerts")
                    for alert in alerts:
                        self.logger.warning(f"Alert: {alert.message}")
                
                collection_time = time.time() - start_time
                self.logger.debug(f"System monitoring cycle completed in {collection_time:.2f}s")
                
                # Wait for next interval
                await asyncio.sleep(max(0, interval_seconds - collection_time))
                
            except Exception as e:
                self.logger.error(f"Error in system monitoring loop: {e}")
                await asyncio.sleep(interval_seconds)
    
    def stop_monitoring(self):
        """Stop system monitoring."""
        self.monitoring_active = False
        self.logger.info("System resource monitoring stopped")
    
    def get_monitoring_summary(self) -> Dict[str, Any]:
        """Get comprehensive monitoring summary."""
        try:
            current_health = self.health_history[-1] if self.health_history else SystemHealth()
            active_alerts = list(self.active_alerts.values())
            
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "monitoring_active": self.monitoring_active,
                "last_collection": self.last_collection_time.isoformat(),
                "system_info": self.system_info,
                "current_health": {
                    "overall_status": current_health.overall_status.value,
                    "health_score": current_health.health_score,
                    "cpu_status": current_health.cpu_status.value,
                    "memory_status": current_health.memory_status.value,
                    "disk_status": current_health.disk_status.value,
                    "network_status": current_health.network_status.value,
                    "uptime_hours": current_health.uptime_seconds / 3600,
                    "predicted_issues": current_health.predicted_issues,
                    "capacity_forecast": current_health.capacity_forecast
                },
                "metrics_summary": {
                    "cpu_usage_percent": self.current_metrics.get('cpu', CPUMetrics()).overall_usage_percent,
                    "memory_usage_percent": self.current_metrics.get('memory', MemoryMetrics()).usage_percent,
                    "swap_usage_percent": self.current_metrics.get('memory', MemoryMetrics()).swap_usage_percent,
                    "disk_usage_max_percent": max([p['usage_percent'] for p in 
                                                  self.current_metrics.get('disk', DiskMetrics()).partitions.values()], default=0),
                    "network_errors_per_sec": (self.current_metrics.get('network', NetworkMetrics()).total_errors_in + 
                                              self.current_metrics.get('network', NetworkMetrics()).total_errors_out)
                },
                "alerts": {
                    "total_active": len(active_alerts),
                    "critical": len([a for a in active_alerts if a.severity == AlertSeverity.CRITICAL]),
                    "error": len([a for a in active_alerts if a.severity == AlertSeverity.ERROR]),
                    "warning": len([a for a in active_alerts if a.severity == AlertSeverity.WARNING]),
                    "recent_alerts": [
                        {
                            "type": alert.alert_type,
                            "severity": alert.severity.value,
                            "message": alert.message,
                            "timestamp": alert.timestamp.isoformat()
                        }
                        for alert in sorted(active_alerts, key=lambda x: x.timestamp, reverse=True)[:5]
                    ]
                },
                "performance": {
                    "performance_index": current_health.performance_index,
                    "stability_index": current_health.stability_index,
                    "efficiency_index": current_health.efficiency_index
                },
                "data_collection": {
                    "metrics_history_size": len(self.metrics_history),
                    "health_history_size": len(self.health_history),
                    "alert_history_size": len(self.alert_history),
                    "trend_data_points": sum(len(trend) for trend in self.trend_data.values())
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error generating monitoring summary: {e}")
            return {"error": str(e)}


# Global system monitor instance
system_monitor = ComprehensiveSystemMonitor()


# Async context manager for resource monitoring
class ResourceMonitoringContext:
    """Context manager for monitoring specific operations."""
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.start_time = None
        self.start_metrics = None
    
    async def __aenter__(self):
        self.start_time = time.time()
        self.start_metrics = await system_monitor.collect_all_metrics()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        end_metrics = await system_monitor.collect_all_metrics()
        duration = time.time() - self.start_time
        
        # Calculate resource usage during operation
        if self.start_metrics and end_metrics:
            cpu_delta = (end_metrics['cpu'].overall_usage_percent - 
                        self.start_metrics['cpu'].overall_usage_percent)
            memory_delta = (end_metrics['memory'].usage_percent - 
                           self.start_metrics['memory'].usage_percent)
            
            logger.info(f"Operation '{self.operation_name}' completed in {duration:.2f}s - "
                       f"CPU delta: {cpu_delta:+.1f}%, Memory delta: {memory_delta:+.1f}%")


__all__ = [
    'ComprehensiveSystemMonitor',
    'system_monitor',
    'ResourceMonitoringContext',
    'ResourceType',
    'AlertSeverity',
    'ResourceStatus',
    'CPUMetrics',
    'MemoryMetrics',
    'DiskMetrics',
    'NetworkMetrics',
    'ProcessMetrics',
    'SystemHealth',
    'ResourceAlert'
]
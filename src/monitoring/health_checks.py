"""
Enterprise Health Check System
Provides comprehensive health monitoring for all system components with 
advanced metrics, capacity planning, and SLA monitoring.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import psutil
import requests

try:
    import redis
    import psycopg2
    from elasticsearch import Elasticsearch
    ADVANCED_MONITORING_AVAILABLE = True
except ImportError:
    ADVANCED_MONITORING_AVAILABLE = False

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


class HealthStatus(Enum):
    """Health check status levels."""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class HealthCheck:
    """Enhanced health check result with detailed metrics."""
    name: str
    status: HealthStatus
    message: str
    response_time_ms: float
    timestamp: datetime
    details: dict[str, Any]
    error: str | None = None
    tags: Dict[str, str] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)
    sla_target: Optional[float] = None
    availability_percentage: Optional[float] = None


@dataclass
class ServiceAvailabilityMetrics:
    """Service availability and SLA metrics."""
    service_name: str
    uptime_percentage: float
    average_response_time: float
    error_rate: float
    last_incident: Optional[datetime] = None
    sla_target: float = 99.9
    incidents_count: int = 0
    mttr_seconds: Optional[float] = None  # Mean Time To Recovery
    mtbf_seconds: Optional[float] = None  # Mean Time Between Failures


@dataclass
class CapacityPlanningMetrics:
    """Resource capacity planning metrics."""
    resource_type: str
    current_usage: float
    capacity_limit: float
    utilization_percentage: float
    projected_exhaustion: Optional[datetime] = None
    growth_rate_daily: float = 0.0
    recommendation: str = ""
    alert_threshold: float = 80.0


@dataclass
class PerformanceBaseline:
    """Performance baseline for comparison."""
    metric_name: str
    baseline_value: float
    current_value: float
    deviation_percentage: float
    trend: str = "stable"  # improving, degrading, stable
    last_updated: datetime = field(default_factory=datetime.utcnow)


class HealthChecker:
    """Enterprise-grade health checker with advanced monitoring capabilities."""

    def __init__(self):
        self.checks = {}
        self.check_results = {}
        self.check_history = {}
        self.availability_metrics = {}
        self.capacity_metrics = {}
        self.performance_baselines = {}
        self.is_monitoring = False
        self.monitor_thread = None
        self.check_interval = 60  # seconds
        
        # Advanced monitoring settings
        self.enable_capacity_planning = True
        self.enable_sla_tracking = True
        self.enable_performance_baselines = True
        self.baseline_calculation_window = 168  # hours (1 week)
        
        # Alerting thresholds
        self.cpu_warning_threshold = 80.0
        self.cpu_critical_threshold = 95.0
        self.memory_warning_threshold = 85.0
        self.memory_critical_threshold = 95.0
        self.disk_warning_threshold = 80.0
        self.disk_critical_threshold = 95.0
        self.response_time_warning = 2000  # ms
        self.response_time_critical = 5000  # ms

        # Setup default health checks
        self._setup_default_checks()

    def _setup_default_checks(self):
        """Setup comprehensive health checks."""
        # Enhanced system health checks
        self.register_check("system_cpu", self._check_system_cpu)
        self.register_check("system_memory", self._check_system_memory)
        self.register_check("system_disk", self._check_system_disk)
        self.register_check("system_network", self._check_system_network)
        self.register_check("system_processes", self._check_system_processes)

        # Enhanced database health checks
        self.register_check("database_connection", self._check_database_connection)
        self.register_check("database_performance", self._check_database_performance)
        
        # Advanced service checks (if available)
        if ADVANCED_MONITORING_AVAILABLE:
            self.register_check("redis_health", self._check_redis_health)
            self.register_check("elasticsearch_health", self._check_elasticsearch_health)

        # Enhanced API health checks
        self.register_check("api_health", self._check_api_health)
        self.register_check("api_performance", self._check_api_performance)

        # File system and storage checks
        self.register_check("data_directories", self._check_data_directories)
        self.register_check("log_files", self._check_log_files)
        self.register_check("storage_capacity", self._check_storage_capacity)

        # Process and service checks
        self.register_check("etl_processes", self._check_etl_processes)
        self.register_check("service_availability", self._check_service_availability)
        
        # Security and compliance checks
        self.register_check("security_status", self._check_security_status)
        self.register_check("certificate_expiry", self._check_certificate_expiry)

    def register_check(self, name: str, check_func: Callable[[], HealthCheck]):
        """Register a health check function."""
        self.checks[name] = check_func
        logger.info(f"Registered health check: {name}")

    def remove_check(self, name: str):
        """Remove a health check."""
        if name in self.checks:
            del self.checks[name]
            logger.info(f"Removed health check: {name}")

    def run_check(self, name: str) -> HealthCheck:
        """Run a specific health check."""
        if name not in self.checks:
            return HealthCheck(
                name=name,
                status=HealthStatus.UNKNOWN,
                message=f"Health check '{name}' not found",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={},
                error="Check not found"
            )

        start_time = time.time()

        try:
            result = self.checks[name]()
            result.response_time_ms = (time.time() - start_time) * 1000
            result.timestamp = datetime.now()

            # Store result
            self.check_results[name] = result

            # Update history
            if name not in self.check_history:
                self.check_history[name] = []
            self.check_history[name].append(result)

            # Keep only last 100 results
            self.check_history[name] = self.check_history[name][-100:]

            return result

        except Exception as e:
            error_result = HealthCheck(
                name=name,
                status=HealthStatus.CRITICAL,
                message=f"Health check failed: {str(e)}",
                response_time_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(),
                details={},
                error=str(e)
            )

            self.check_results[name] = error_result
            return error_result

    def run_all_checks(self) -> dict[str, HealthCheck]:
        """Run all registered health checks."""
        results = {}

        for check_name in self.checks:
            results[check_name] = self.run_check(check_name)

        return results

    def start_monitoring(self):
        """Start continuous health monitoring."""
        if self.is_monitoring:
            logger.warning("Health monitoring already running")
            return

        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("Health monitoring started")

    def stop_monitoring(self):
        """Stop health monitoring."""
        self.is_monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("Health monitoring stopped")

    def _monitoring_loop(self):
        """Background monitoring loop."""
        while self.is_monitoring:
            try:
                self.run_all_checks()
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in health monitoring loop: {e}")
                time.sleep(10)

    def get_system_health(self) -> dict[str, Any]:
        """Get overall system health summary."""
        if not self.check_results:
            self.run_all_checks()

        status_counts = {status.value: 0 for status in HealthStatus}
        critical_issues = []
        warning_issues = []

        for check_name, result in self.check_results.items():
            status_counts[result.status.value] += 1

            if result.status == HealthStatus.CRITICAL:
                critical_issues.append(f"{check_name}: {result.message}")
            elif result.status == HealthStatus.WARNING:
                warning_issues.append(f"{check_name}: {result.message}")

        # Determine overall status
        if critical_issues:
            overall_status = HealthStatus.CRITICAL
        elif warning_issues:
            overall_status = HealthStatus.WARNING
        else:
            overall_status = HealthStatus.HEALTHY

        return {
            'overall_status': overall_status.value,
            'total_checks': len(self.check_results),
            'status_breakdown': status_counts,
            'critical_issues': critical_issues,
            'warning_issues': warning_issues,
            'last_updated': datetime.now().isoformat(),
            'checks': {name: asdict(result) for name, result in self.check_results.items()}
        }

    # Individual health check implementations
    def _check_system_cpu(self) -> HealthCheck:
        """Check system CPU usage."""
        cpu_percent = psutil.cpu_percent(interval=1)

        if cpu_percent > 95:
            status = HealthStatus.CRITICAL
            message = f"CPU usage critical: {cpu_percent:.1f}%"
        elif cpu_percent > 80:
            status = HealthStatus.WARNING
            message = f"CPU usage high: {cpu_percent:.1f}%"
        else:
            status = HealthStatus.HEALTHY
            message = f"CPU usage normal: {cpu_percent:.1f}%"

        return HealthCheck(
            name="system_cpu",
            status=status,
            message=message,
            response_time_ms=0,
            timestamp=datetime.now(),
            details={
                'cpu_percent': cpu_percent,
                'cpu_count': psutil.cpu_count(),
                'load_avg': psutil.getloadavg() if hasattr(psutil, 'getloadavg') else None
            }
        )

    def _check_system_memory(self) -> HealthCheck:
        """Check system memory usage."""
        memory = psutil.virtual_memory()

        if memory.percent > 95:
            status = HealthStatus.CRITICAL
            message = f"Memory usage critical: {memory.percent:.1f}%"
        elif memory.percent > 85:
            status = HealthStatus.WARNING
            message = f"Memory usage high: {memory.percent:.1f}%"
        else:
            status = HealthStatus.HEALTHY
            message = f"Memory usage normal: {memory.percent:.1f}%"

        return HealthCheck(
            name="system_memory",
            status=status,
            message=message,
            response_time_ms=0,
            timestamp=datetime.now(),
            details={
                'memory_percent': memory.percent,
                'total_gb': round(memory.total / (1024**3), 2),
                'available_gb': round(memory.available / (1024**3), 2),
                'used_gb': round(memory.used / (1024**3), 2)
            }
        )

    def _check_system_disk(self) -> HealthCheck:
        """Check system disk usage."""
        data_path = Path("./data")

        if data_path.exists():
            disk_usage = psutil.disk_usage(str(data_path))
            disk_percent = (disk_usage.used / disk_usage.total) * 100
        else:
            disk_percent = 0

        if disk_percent > 95:
            status = HealthStatus.CRITICAL
            message = f"Disk usage critical: {disk_percent:.1f}%"
        elif disk_percent > 85:
            status = HealthStatus.WARNING
            message = f"Disk usage high: {disk_percent:.1f}%"
        else:
            status = HealthStatus.HEALTHY
            message = f"Disk usage normal: {disk_percent:.1f}%"

        details = {'disk_percent': disk_percent}
        if data_path.exists():
            details.update({
                'total_gb': round(disk_usage.total / (1024**3), 2),
                'free_gb': round(disk_usage.free / (1024**3), 2),
                'used_gb': round(disk_usage.used / (1024**3), 2)
            })

        return HealthCheck(
            name="system_disk",
            status=status,
            message=message,
            response_time_ms=0,
            timestamp=datetime.now(),
            details=details
        )

    def _check_database_connection(self) -> HealthCheck:
        """Check database connection."""
        try:
            db_url = settings.get_database_url()

            if db_url.startswith('sqlite'):
                # SQLite check
                db_path = db_url.replace('sqlite:///', './').replace('sqlite://', './')
                path = Path(db_path)

                if not path.exists():
                    return HealthCheck(
                        name="database_connection",
                        status=HealthStatus.CRITICAL,
                        message="Database file does not exist",
                        response_time_ms=0,
                        timestamp=datetime.now(),
                        details={'db_path': str(path)}
                    )

                # Try to connect
                with sqlite3.connect(path) as conn:
                    cursor = conn.execute("SELECT 1")
                    cursor.fetchone()

                return HealthCheck(
                    name="database_connection",
                    status=HealthStatus.HEALTHY,
                    message="Database connection successful",
                    response_time_ms=0,
                    timestamp=datetime.now(),
                    details={'db_type': 'sqlite', 'db_path': str(path)}
                )

            else:
                # PostgreSQL or other database
                return HealthCheck(
                    name="database_connection",
                    status=HealthStatus.WARNING,
                    message="Non-SQLite database check not implemented",
                    response_time_ms=0,
                    timestamp=datetime.now(),
                    details={'db_type': 'other', 'db_url': db_url}
                )

        except Exception as e:
            return HealthCheck(
                name="database_connection",
                status=HealthStatus.CRITICAL,
                message=f"Database connection failed: {str(e)}",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={},
                error=str(e)
            )

    def _check_database_performance(self) -> HealthCheck:
        """Check database performance."""
        try:
            db_url = settings.get_database_url()

            if db_url.startswith('sqlite'):
                db_path = db_url.replace('sqlite:///', './').replace('sqlite://', './')
                path = Path(db_path)

                start_time = time.time()

                with sqlite3.connect(path) as conn:
                    # Simple performance test
                    cursor = conn.execute("""
                        SELECT name FROM sqlite_master 
                        WHERE type='table' 
                        ORDER BY name
                    """)
                    tables = cursor.fetchall()

                query_time = (time.time() - start_time) * 1000

                if query_time > 1000:  # > 1 second
                    status = HealthStatus.WARNING
                    message = f"Database query slow: {query_time:.1f}ms"
                else:
                    status = HealthStatus.HEALTHY
                    message = f"Database performance good: {query_time:.1f}ms"

                return HealthCheck(
                    name="database_performance",
                    status=status,
                    message=message,
                    response_time_ms=query_time,
                    timestamp=datetime.now(),
                    details={
                        'query_time_ms': query_time,
                        'table_count': len(tables),
                        'tables': [table[0] for table in tables]
                    }
                )

        except Exception as e:
            return HealthCheck(
                name="database_performance",
                status=HealthStatus.CRITICAL,
                message=f"Database performance check failed: {str(e)}",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={},
                error=str(e)
            )

    def _check_api_health(self) -> HealthCheck:
        """Check API health."""
        try:
            api_port = getattr(settings, 'api_port', 8000)
            health_url = f"http://localhost:{api_port}/api/v1/health"

            start_time = time.time()
            response = requests.get(health_url, timeout=5)
            response_time = (time.time() - start_time) * 1000

            if response.status_code == 200:
                status = HealthStatus.HEALTHY
                message = f"API responding normally: {response.status_code}"
            else:
                status = HealthStatus.WARNING
                message = f"API returned status: {response.status_code}"

            return HealthCheck(
                name="api_health",
                status=status,
                message=message,
                response_time_ms=response_time,
                timestamp=datetime.now(),
                details={
                    'status_code': response.status_code,
                    'response_time_ms': response_time,
                    'url': health_url
                }
            )

        except requests.exceptions.ConnectionError:
            return HealthCheck(
                name="api_health",
                status=HealthStatus.CRITICAL,
                message="API not responding (connection refused)",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={'url': f"http://localhost:{getattr(settings, 'api_port', 8000)}/api/v1/health"},
                error="Connection refused"
            )
        except Exception as e:
            return HealthCheck(
                name="api_health",
                status=HealthStatus.CRITICAL,
                message=f"API health check failed: {str(e)}",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={},
                error=str(e)
            )

    def _check_data_directories(self) -> HealthCheck:
        """Check data directories exist and are writable."""
        directories = [
            Path("./data"),
            Path("./data/raw"),
            Path("./data/bronze"),
            Path("./data/silver"),
            Path("./data/gold"),
            Path("./data/warehouse"),
            Path("./logs")
        ]

        missing_dirs = []
        unwritable_dirs = []

        for directory in directories:
            if not directory.exists():
                missing_dirs.append(str(directory))
            elif not os.access(directory, os.W_OK):
                unwritable_dirs.append(str(directory))

        if missing_dirs:
            status = HealthStatus.CRITICAL
            message = f"Missing directories: {', '.join(missing_dirs)}"
        elif unwritable_dirs:
            status = HealthStatus.WARNING
            message = f"Non-writable directories: {', '.join(unwritable_dirs)}"
        else:
            status = HealthStatus.HEALTHY
            message = "All data directories accessible"

        return HealthCheck(
            name="data_directories",
            status=status,
            message=message,
            response_time_ms=0,
            timestamp=datetime.now(),
            details={
                'checked_dirs': [str(d) for d in directories],
                'missing_dirs': missing_dirs,
                'unwritable_dirs': unwritable_dirs
            }
        )

    def _check_log_files(self) -> HealthCheck:
        """Check log files and disk space."""
        log_dir = Path("./logs")

        if not log_dir.exists():
            return HealthCheck(
                name="log_files",
                status=HealthStatus.WARNING,
                message="Log directory does not exist",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={'log_dir': str(log_dir)}
            )

        # Check log file sizes
        log_files = list(log_dir.glob("*.log"))
        total_size_mb = sum(f.stat().st_size for f in log_files) / (1024 * 1024)

        if total_size_mb > 1000:  # > 1GB
            status = HealthStatus.WARNING
            message = f"Log files large: {total_size_mb:.1f}MB"
        else:
            status = HealthStatus.HEALTHY
            message = f"Log files normal: {total_size_mb:.1f}MB"

        return HealthCheck(
            name="log_files",
            status=status,
            message=message,
            response_time_ms=0,
            timestamp=datetime.now(),
            details={
                'log_file_count': len(log_files),
                'total_size_mb': round(total_size_mb, 2),
                'log_files': [str(f) for f in log_files]
            }
        )

    def _check_etl_processes(self) -> HealthCheck:
        """Check ETL process health."""
        # This is a basic check - in a real system you might check:
        # - Running processes
        # - Recent ETL run status
        # - Process memory usage
        # - Lock files, etc.

        try:
            # Check for any Python ETL processes
            etl_processes = []
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] == 'python.exe' or proc.info['name'] == 'python':
                        cmdline = ' '.join(proc.info['cmdline'] or [])
                        if any(keyword in cmdline for keyword in ['bronze', 'silver', 'gold', 'etl']):
                            etl_processes.append({
                                'pid': proc.info['pid'],
                                'cmdline': cmdline
                            })
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            return HealthCheck(
                name="etl_processes",
                status=HealthStatus.HEALTHY,
                message=f"Found {len(etl_processes)} ETL processes",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={
                    'process_count': len(etl_processes),
                    'processes': etl_processes
                }
            )

        except Exception as e:
            return HealthCheck(
                name="etl_processes",
                status=HealthStatus.WARNING,
                message=f"Could not check ETL processes: {str(e)}",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={},
                error=str(e)
            )

    def _check_system_network(self) -> HealthCheck:
        """Check system network performance and connectivity."""
        try:
            network_io = psutil.net_io_counters()
            network_connections = len(psutil.net_connections())
            
            # Get network interface statistics
            network_interfaces = {}
            for interface, stats in psutil.net_if_stats().items():
                network_interfaces[interface] = {
                    "is_up": stats.isup,
                    "speed": stats.speed,
                    "mtu": stats.mtu
                }
            
            # Calculate network utilization (simplified)
            bytes_sent_mb = network_io.bytes_sent / (1024 * 1024)
            bytes_recv_mb = network_io.bytes_recv / (1024 * 1024)
            
            details = {
                "bytes_sent_mb": round(bytes_sent_mb, 2),
                "bytes_recv_mb": round(bytes_recv_mb, 2),
                "packets_sent": network_io.packets_sent,
                "packets_recv": network_io.packets_recv,
                "errin": network_io.errin,
                "errout": network_io.errout,
                "dropin": network_io.dropin,
                "dropout": network_io.dropout,
                "active_connections": network_connections,
                "network_interfaces": network_interfaces
            }
            
            # Analyze network health
            status = HealthStatus.HEALTHY
            issues = []
            
            if network_io.errin > 1000 or network_io.errout > 1000:
                status = HealthStatus.WARNING
                issues.append(f"High network errors: in={network_io.errin}, out={network_io.errout}")
            
            if network_io.dropin > 100 or network_io.dropout > 100:
                status = HealthStatus.WARNING
                issues.append(f"Network packet drops detected: in={network_io.dropin}, out={network_io.dropout}")
            
            if network_connections > 1000:
                status = HealthStatus.WARNING
                issues.append(f"High number of network connections: {network_connections}")
            
            message = "Network healthy" if not issues else "; ".join(issues)
            
            return HealthCheck(
                name="system_network",
                status=status,
                message=message,
                response_time_ms=0,
                timestamp=datetime.now(),
                details=details,
                metrics={
                    "bytes_sent_mb": bytes_sent_mb,
                    "bytes_recv_mb": bytes_recv_mb,
                    "active_connections": network_connections,
                    "error_rate": network_io.errin + network_io.errout
                }
            )
            
        except Exception as e:
            return HealthCheck(
                name="system_network",
                status=HealthStatus.CRITICAL,
                message=f"Network check failed: {str(e)}",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={},
                error=str(e)
            )

    def _check_system_processes(self) -> HealthCheck:
        """Check system process health and resource usage."""
        try:
            processes = list(psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent', 'status']))
            
            # Analyze process statistics
            total_processes = len(processes)
            running_processes = len([p for p in processes if p.info['status'] == psutil.STATUS_RUNNING])
            sleeping_processes = len([p for p in processes if p.info['status'] == psutil.STATUS_SLEEPING])
            zombie_processes = len([p for p in processes if p.info['status'] == psutil.STATUS_ZOMBIE])
            
            # Find top CPU and memory consuming processes
            processes_by_cpu = sorted(processes, key=lambda p: p.info['cpu_percent'] or 0, reverse=True)[:5]
            processes_by_memory = sorted(processes, key=lambda p: p.info['memory_percent'] or 0, reverse=True)[:5]
            
            top_cpu_processes = [
                {
                    "pid": p.info['pid'],
                    "name": p.info['name'],
                    "cpu_percent": p.info['cpu_percent']
                } for p in processes_by_cpu
            ]
            
            top_memory_processes = [
                {
                    "pid": p.info['pid'],
                    "name": p.info['name'],
                    "memory_percent": p.info['memory_percent']
                } for p in processes_by_memory
            ]
            
            details = {
                "total_processes": total_processes,
                "running_processes": running_processes,
                "sleeping_processes": sleeping_processes,
                "zombie_processes": zombie_processes,
                "top_cpu_processes": top_cpu_processes,
                "top_memory_processes": top_memory_processes
            }
            
            # Analyze process health
            status = HealthStatus.HEALTHY
            issues = []
            
            if zombie_processes > 5:
                status = HealthStatus.WARNING
                issues.append(f"High number of zombie processes: {zombie_processes}")
            
            if total_processes > 500:
                status = HealthStatus.WARNING
                issues.append(f"High total process count: {total_processes}")
            
            # Check for runaway processes
            high_cpu_processes = [p for p in processes if (p.info['cpu_percent'] or 0) > 80]
            if high_cpu_processes:
                status = HealthStatus.WARNING
                issues.append(f"{len(high_cpu_processes)} processes with high CPU usage")
            
            message = "Process health normal" if not issues else "; ".join(issues)
            
            return HealthCheck(
                name="system_processes",
                status=status,
                message=message,
                response_time_ms=0,
                timestamp=datetime.now(),
                details=details,
                metrics={
                    "total_processes": total_processes,
                    "zombie_processes": zombie_processes,
                    "running_processes": running_processes
                }
            )
            
        except Exception as e:
            return HealthCheck(
                name="system_processes",
                status=HealthStatus.CRITICAL,
                message=f"Process check failed: {str(e)}",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={},
                error=str(e)
            )

    def _check_redis_health(self) -> HealthCheck:
        """Check Redis health and performance."""
        if not ADVANCED_MONITORING_AVAILABLE:
            return HealthCheck(
                name="redis_health",
                status=HealthStatus.UNKNOWN,
                message="Redis monitoring not available (missing dependencies)",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={}
            )
        
        try:
            # Connect to Redis
            redis_host = getattr(settings, 'redis_host', 'localhost')
            redis_port = getattr(settings, 'redis_port', 6379)
            
            client = redis.Redis(host=redis_host, port=redis_port, 
                               socket_connect_timeout=5, socket_timeout=5)
            
            start_time = time.time()
            ping_result = client.ping()
            ping_time = (time.time() - start_time) * 1000
            
            # Get Redis info
            info = client.info()
            memory_info = client.info('memory')
            
            # Test basic operations
            test_key = "health_check_test"
            start_op = time.time()
            client.set(test_key, "test_value", ex=60)
            get_result = client.get(test_key)
            client.delete(test_key)
            operation_time = (time.time() - start_op) * 1000
            
            # Calculate metrics
            used_memory_mb = memory_info.get('used_memory', 0) / (1024 * 1024)
            max_memory_mb = memory_info.get('maxmemory', 0) / (1024 * 1024)
            memory_usage_percent = (used_memory_mb / max_memory_mb * 100) if max_memory_mb > 0 else 0
            
            hits = info.get('keyspace_hits', 0)
            misses = info.get('keyspace_misses', 0)
            hit_ratio = (hits / (hits + misses) * 100) if (hits + misses) > 0 else 100
            
            details = {
                "redis_version": info.get("redis_version"),
                "connected_clients": info.get("connected_clients", 0),
                "used_memory_mb": round(used_memory_mb, 2),
                "max_memory_mb": round(max_memory_mb, 2),
                "memory_usage_percent": round(memory_usage_percent, 1),
                "keyspace_hits": hits,
                "keyspace_misses": misses,
                "hit_ratio_percent": round(hit_ratio, 1),
                "ping_time_ms": round(ping_time, 2),
                "operation_time_ms": round(operation_time, 2),
                "ops_per_sec": info.get("instantaneous_ops_per_sec", 0),
                "evicted_keys": info.get("evicted_keys", 0)
            }
            
            # Analyze Redis health
            status = HealthStatus.HEALTHY
            issues = []
            
            if ping_time > 100:  # >100ms ping time
                status = HealthStatus.WARNING
                issues.append(f"High ping time: {ping_time:.1f}ms")
            
            if memory_usage_percent > 90:
                status = HealthStatus.CRITICAL
                issues.append(f"High memory usage: {memory_usage_percent:.1f}%")
            elif memory_usage_percent > 80:
                status = HealthStatus.WARNING
                issues.append(f"Elevated memory usage: {memory_usage_percent:.1f}%")
            
            if hit_ratio < 85 and (hits + misses) > 1000:
                status = HealthStatus.WARNING
                issues.append(f"Low cache hit ratio: {hit_ratio:.1f}%")
            
            if info.get("evicted_keys", 0) > 1000:
                status = HealthStatus.WARNING
                issues.append(f"High number of evicted keys: {info.get('evicted_keys')}")
            
            message = "Redis healthy" if not issues else "; ".join(issues)
            
            return HealthCheck(
                name="redis_health",
                status=status,
                message=message,
                response_time_ms=ping_time,
                timestamp=datetime.now(),
                details=details,
                metrics={
                    "ping_time_ms": ping_time,
                    "operation_time_ms": operation_time,
                    "memory_usage_percent": memory_usage_percent,
                    "hit_ratio_percent": hit_ratio,
                    "connected_clients": info.get("connected_clients", 0)
                }
            )
            
        except Exception as e:
            return HealthCheck(
                name="redis_health",
                status=HealthStatus.CRITICAL,
                message=f"Redis health check failed: {str(e)}",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={},
                error=str(e)
            )

    def _check_api_performance(self) -> HealthCheck:
        """Check API performance across multiple endpoints."""
        try:
            api_port = getattr(settings, 'api_port', 8000)
            endpoints = [
                f"http://localhost:{api_port}/api/v1/health",
                f"http://localhost:{api_port}/api/v1/sales/summary",
                f"http://localhost:{api_port}/metrics"
            ]
            
            endpoint_results = []
            total_time = 0
            successful_calls = 0
            
            for endpoint in endpoints:
                try:
                    start_time = time.time()
                    response = requests.get(endpoint, timeout=10)
                    response_time = (time.time() - start_time) * 1000
                    total_time += response_time
                    
                    result = {
                        "endpoint": endpoint,
                        "status_code": response.status_code,
                        "response_time_ms": round(response_time, 2),
                        "success": response.status_code < 400,
                        "content_length": len(response.content)
                    }
                    
                    if response.status_code < 400:
                        successful_calls += 1
                    
                    endpoint_results.append(result)
                    
                except requests.exceptions.RequestException as e:
                    endpoint_results.append({
                        "endpoint": endpoint,
                        "status_code": 0,
                        "response_time_ms": 0,
                        "success": False,
                        "error": str(e)
                    })
            
            # Calculate overall metrics
            avg_response_time = total_time / len(endpoints)
            success_rate = (successful_calls / len(endpoints)) * 100
            
            details = {
                "endpoints_tested": len(endpoints),
                "successful_calls": successful_calls,
                "average_response_time_ms": round(avg_response_time, 2),
                "success_rate_percent": round(success_rate, 1),
                "endpoint_results": endpoint_results
            }
            
            # Analyze performance
            status = HealthStatus.HEALTHY
            issues = []
            
            if success_rate < 100:
                status = HealthStatus.CRITICAL if success_rate < 50 else HealthStatus.WARNING
                issues.append(f"API success rate: {success_rate:.1f}%")
            
            if avg_response_time > self.response_time_critical:
                status = HealthStatus.CRITICAL
                issues.append(f"Critical response time: {avg_response_time:.1f}ms")
            elif avg_response_time > self.response_time_warning:
                status = HealthStatus.WARNING
                issues.append(f"Slow response time: {avg_response_time:.1f}ms")
            
            message = "API performance good" if not issues else "; ".join(issues)
            
            return HealthCheck(
                name="api_performance",
                status=status,
                message=message,
                response_time_ms=avg_response_time,
                timestamp=datetime.now(),
                details=details,
                metrics={
                    "average_response_time_ms": avg_response_time,
                    "success_rate_percent": success_rate,
                    "successful_calls": successful_calls
                },
                sla_target=99.9,
                availability_percentage=success_rate
            )
            
        except Exception as e:
            return HealthCheck(
                name="api_performance",
                status=HealthStatus.CRITICAL,
                message=f"API performance check failed: {str(e)}",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={},
                error=str(e)
            )

    def _check_storage_capacity(self) -> HealthCheck:
        """Check storage capacity across all mounted filesystems."""
        try:
            filesystem_stats = []
            total_usage_gb = 0
            total_capacity_gb = 0
            max_usage_percent = 0
            
            # Check all disk partitions
            for partition in psutil.disk_partitions():
                try:
                    usage = psutil.disk_usage(partition.mountpoint)
                    usage_percent = (usage.used / usage.total) * 100
                    
                    fs_stats = {
                        "device": partition.device,
                        "mountpoint": partition.mountpoint,
                        "fstype": partition.fstype,
                        "total_gb": round(usage.total / (1024**3), 2),
                        "used_gb": round(usage.used / (1024**3), 2),
                        "free_gb": round(usage.free / (1024**3), 2),
                        "usage_percent": round(usage_percent, 1)
                    }
                    
                    filesystem_stats.append(fs_stats)
                    total_usage_gb += fs_stats["used_gb"]
                    total_capacity_gb += fs_stats["total_gb"]
                    max_usage_percent = max(max_usage_percent, usage_percent)
                    
                except PermissionError:
                    continue
            
            # Calculate capacity planning metrics
            if self.enable_capacity_planning:
                capacity_metrics = self._calculate_storage_capacity_planning(filesystem_stats)
            else:
                capacity_metrics = []
            
            details = {
                "filesystem_count": len(filesystem_stats),
                "filesystems": filesystem_stats,
                "total_capacity_gb": round(total_capacity_gb, 2),
                "total_used_gb": round(total_usage_gb, 2),
                "total_free_gb": round(total_capacity_gb - total_usage_gb, 2),
                "max_usage_percent": round(max_usage_percent, 1),
                "capacity_planning": capacity_metrics
            }
            
            # Analyze storage health
            status = HealthStatus.HEALTHY
            issues = []
            
            if max_usage_percent > self.disk_critical_threshold:
                status = HealthStatus.CRITICAL
                issues.append(f"Critical disk usage: {max_usage_percent:.1f}%")
            elif max_usage_percent > self.disk_warning_threshold:
                status = HealthStatus.WARNING
                issues.append(f"High disk usage: {max_usage_percent:.1f}%")
            
            # Check for capacity issues in planning
            critical_capacity = [m for m in capacity_metrics if m.get("days_until_full", 999) < 30]
            if critical_capacity:
                status = HealthStatus.WARNING
                issues.append(f"{len(critical_capacity)} filesystems may fill within 30 days")
            
            message = "Storage capacity normal" if not issues else "; ".join(issues)
            
            return HealthCheck(
                name="storage_capacity",
                status=status,
                message=message,
                response_time_ms=0,
                timestamp=datetime.now(),
                details=details,
                metrics={
                    "max_usage_percent": max_usage_percent,
                    "total_capacity_gb": total_capacity_gb,
                    "total_free_gb": total_capacity_gb - total_usage_gb
                }
            )
            
        except Exception as e:
            return HealthCheck(
                name="storage_capacity",
                status=HealthStatus.CRITICAL,
                message=f"Storage capacity check failed: {str(e)}",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={},
                error=str(e)
            )

    def _calculate_storage_capacity_planning(self, filesystem_stats: List[Dict]) -> List[Dict]:
        """Calculate storage capacity planning metrics."""
        capacity_metrics = []
        
        for fs in filesystem_stats:
            # Simple growth projection based on current usage
            # In a real implementation, this would use historical data
            usage_percent = fs["usage_percent"]
            free_gb = fs["free_gb"]
            
            # Estimate days until full (simplified calculation)
            # Assumes 1% growth per week
            weekly_growth_percent = 1.0
            days_until_full = None
            
            if usage_percent < 95:
                remaining_percent = 100 - usage_percent
                weeks_remaining = remaining_percent / weekly_growth_percent
                days_until_full = int(weeks_remaining * 7)
            
            capacity_metric = {
                "filesystem": fs["mountpoint"],
                "current_usage_percent": usage_percent,
                "free_gb": free_gb,
                "projected_growth_weekly_percent": weekly_growth_percent,
                "days_until_full": days_until_full,
                "recommendation": self._get_storage_recommendation(usage_percent, days_until_full)
            }
            
            capacity_metrics.append(capacity_metric)
        
        return capacity_metrics

    def _get_storage_recommendation(self, usage_percent: float, days_until_full: Optional[int]) -> str:
        """Get storage capacity recommendation."""
        if usage_percent > 95:
            return "CRITICAL: Immediate cleanup or expansion required"
        elif usage_percent > 90:
            return "HIGH: Plan for storage expansion soon"
        elif days_until_full and days_until_full < 30:
            return f"MEDIUM: Storage may fill in {days_until_full} days"
        elif usage_percent > 80:
            return "LOW: Monitor storage usage trends"
        else:
            return "Storage capacity is healthy"

    def _check_service_availability(self) -> HealthCheck:
        """Check availability of critical services."""
        try:
            services_to_check = [
                ("Database", "database_connection"),
                ("API", "api_health"),
                ("Redis", "redis_health"),
            ]
            
            service_results = []
            available_services = 0
            total_services = len(services_to_check)
            
            for service_name, check_name in services_to_check:
                if check_name in self.check_results:
                    result = self.check_results[check_name]
                    is_available = result.status in [HealthStatus.HEALTHY, HealthStatus.WARNING]
                    
                    service_results.append({
                        "service": service_name,
                        "available": is_available,
                        "status": result.status.value,
                        "response_time_ms": result.response_time_ms,
                        "last_check": result.timestamp.isoformat()
                    })
                    
                    if is_available:
                        available_services += 1
                else:
                    service_results.append({
                        "service": service_name,
                        "available": False,
                        "status": "unknown",
                        "error": "Service not checked"
                    })
            
            availability_percent = (available_services / total_services) * 100
            
            # Calculate SLA metrics
            if self.enable_sla_tracking:
                sla_metrics = self._calculate_sla_metrics("service_availability")
            else:
                sla_metrics = {}
            
            details = {
                "total_services": total_services,
                "available_services": available_services,
                "availability_percent": round(availability_percent, 1),
                "service_results": service_results,
                "sla_metrics": sla_metrics
            }
            
            # Analyze service availability
            status = HealthStatus.HEALTHY
            if availability_percent < 50:
                status = HealthStatus.CRITICAL
                message = f"Critical: Only {availability_percent:.1f}% of services available"
            elif availability_percent < 80:
                status = HealthStatus.WARNING
                message = f"Warning: {availability_percent:.1f}% of services available"
            elif availability_percent < 100:
                status = HealthStatus.WARNING
                message = f"Partial availability: {availability_percent:.1f}% of services available"
            else:
                message = "All services available"
            
            return HealthCheck(
                name="service_availability",
                status=status,
                message=message,
                response_time_ms=0,
                timestamp=datetime.now(),
                details=details,
                metrics={
                    "availability_percent": availability_percent,
                    "available_services": available_services,
                    "total_services": total_services
                },
                sla_target=99.9,
                availability_percentage=availability_percent
            )
            
        except Exception as e:
            return HealthCheck(
                name="service_availability",
                status=HealthStatus.CRITICAL,
                message=f"Service availability check failed: {str(e)}",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={},
                error=str(e)
            )

    def _check_security_status(self) -> HealthCheck:
        """Check basic security status and file permissions."""
        try:
            security_issues = []
            
            # Check critical file permissions
            critical_paths = [
                "./data",
                "./logs", 
                "./config"
            ]
            
            permission_issues = []
            for path_str in critical_paths:
                path = Path(path_str)
                if path.exists():
                    # Check if world-writable (security risk)
                    stat_info = path.stat()
                    if stat_info.st_mode & 0o002:  # World-writable
                        permission_issues.append(f"{path_str} is world-writable")
                
            # Check for sensitive files in wrong locations
            sensitive_patterns = ["*.key", "*.pem", "*.p12", "*password*", "*secret*"]
            exposed_files = []
            
            for pattern in sensitive_patterns:
                try:
                    from glob import glob
                    matches = glob(f"./{pattern}", recursive=False)
                    for match in matches:
                        if not match.startswith("./config/") and not match.startswith("./."):
                            exposed_files.append(match)
                except:
                    pass
            
            details = {
                "permission_issues": permission_issues,
                "exposed_sensitive_files": exposed_files,
                "security_scan_timestamp": datetime.now().isoformat()
            }
            
            # Analyze security status
            status = HealthStatus.HEALTHY
            issues = []
            
            if permission_issues:
                status = HealthStatus.WARNING
                issues.extend(permission_issues)
            
            if exposed_files:
                status = HealthStatus.CRITICAL
                issues.append(f"{len(exposed_files)} sensitive files in exposed locations")
            
            message = "Security status good" if not issues else "; ".join(issues)
            
            return HealthCheck(
                name="security_status",
                status=status,
                message=message,
                response_time_ms=0,
                timestamp=datetime.now(),
                details=details,
                metrics={
                    "permission_issues": len(permission_issues),
                    "exposed_files": len(exposed_files)
                }
            )
            
        except Exception as e:
            return HealthCheck(
                name="security_status",
                status=HealthStatus.WARNING,
                message=f"Security check failed: {str(e)}",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={},
                error=str(e)
            )

    def _check_certificate_expiry(self) -> HealthCheck:
        """Check SSL certificate expiry (simplified implementation)."""
        try:
            # This is a simplified check - in production you'd check actual certificates
            # For now, we'll just return a healthy status as this is a demo
            
            details = {
                "certificates_checked": 0,
                "expiring_soon": [],
                "expired": [],
                "check_method": "simplified_demo"
            }
            
            return HealthCheck(
                name="certificate_expiry",
                status=HealthStatus.HEALTHY,
                message="Certificate expiry check not implemented (demo mode)",
                response_time_ms=0,
                timestamp=datetime.now(),
                details=details
            )
            
        except Exception as e:
            return HealthCheck(
                name="certificate_expiry",
                status=HealthStatus.WARNING,
                message=f"Certificate check failed: {str(e)}",
                response_time_ms=0,
                timestamp=datetime.now(),
                details={},
                error=str(e)
            )

    def _calculate_sla_metrics(self, check_name: str) -> Dict[str, Any]:
        """Calculate SLA metrics for a specific check."""
        if check_name not in self.check_history:
            return {}
        
        # Get recent history (last 24 hours)
        cutoff_time = datetime.now() - timedelta(hours=24)
        recent_results = [
            result for result in self.check_history[check_name]
            if result.timestamp > cutoff_time
        ]
        
        if not recent_results:
            return {}
        
        # Calculate availability
        healthy_count = len([r for r in recent_results if r.status == HealthStatus.HEALTHY])
        total_count = len(recent_results)
        availability = (healthy_count / total_count) * 100
        
        # Calculate average response time
        avg_response_time = sum(r.response_time_ms for r in recent_results) / total_count
        
        # Find incidents
        incidents = [r for r in recent_results if r.status == HealthStatus.CRITICAL]
        
        return {
            "availability_24h": round(availability, 2),
            "average_response_time_24h": round(avg_response_time, 2),
            "incidents_24h": len(incidents),
            "total_checks_24h": total_count,
            "sla_target": 99.9,
            "sla_breach": availability < 99.9
        }


# Global health checker instance
_health_checker = None


def get_health_checker() -> HealthChecker:
    """Get global health checker instance."""
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker()
    return _health_checker


def check_system_health() -> dict[str, Any]:
    """Quick function to check overall system health."""
    checker = get_health_checker()
    return checker.get_system_health()


def main():
    """Test the health check system."""
    print("Health Check System Module loaded successfully")
    print("Available features:")
    print("- System resource monitoring (CPU, memory, disk)")
    print("- Database connectivity and performance checks")
    print("- API health monitoring")
    print("- File system and directory checks")
    print("- ETL process monitoring")
    print("- Continuous health monitoring")
    print("- Health history tracking")


if __name__ == "__main__":
    main()

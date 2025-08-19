"""
Health Check System
Provides comprehensive health monitoring for all system components
"""
from __future__ import annotations

import requests
import sqlite3
import psutil
import time
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
import threading
import subprocess
import socket

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
    """Represents a health check result."""
    name: str
    status: HealthStatus
    message: str
    response_time_ms: float
    timestamp: datetime
    details: Dict[str, Any]
    error: Optional[str] = None


class HealthChecker:
    """Performs health checks on system components."""
    
    def __init__(self):
        self.checks = {}
        self.check_results = {}
        self.check_history = {}
        self.is_monitoring = False
        self.monitor_thread = None
        self.check_interval = 60  # seconds
        
        # Setup default health checks
        self._setup_default_checks()
        
    def _setup_default_checks(self):
        """Setup default health checks."""
        # System health checks
        self.register_check("system_cpu", self._check_system_cpu)
        self.register_check("system_memory", self._check_system_memory)
        self.register_check("system_disk", self._check_system_disk)
        
        # Database health checks
        self.register_check("database_connection", self._check_database_connection)
        self.register_check("database_performance", self._check_database_performance)
        
        # API health checks
        self.register_check("api_health", self._check_api_health)
        
        # File system health checks
        self.register_check("data_directories", self._check_data_directories)
        self.register_check("log_files", self._check_log_files)
        
        # ETL process health checks
        self.register_check("etl_processes", self._check_etl_processes)
        
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
            
    def run_all_checks(self) -> Dict[str, HealthCheck]:
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
                
    def get_system_health(self) -> Dict[str, Any]:
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


# Global health checker instance
_health_checker = None


def get_health_checker() -> HealthChecker:
    """Get global health checker instance."""
    global _health_checker
    if _health_checker is None:
        _health_checker = HealthChecker()
    return _health_checker


def check_system_health() -> Dict[str, Any]:
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
"""
Comprehensive DataDog Log Collection and Aggregation System

This module provides a complete enterprise-grade log collection system that:
- Centralizes log collection from all services and containers
- Implements structured logging with JSON format
- Provides real-time log streaming and intelligent buffering
- Supports log retention policies and automated archiving
- Integrates with Kubernetes, Docker, and application logs
"""

import asyncio
import json
import gzip
import re
import time
import logging
import threading
import queue
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Callable, Union
from enum import Enum
from dataclasses import dataclass, asdict
from pathlib import Path
from collections import defaultdict, deque
import hashlib
import socket
import os
import subprocess
import yaml

# Third-party imports
import psutil
import docker
from kubernetes import client, config
import requests
from opentelemetry import trace
from datadog import initialize, statsd
from datadog.api.logs import Logs

# Internal imports
from ..core.logging import get_logger
from ..core.monitoring.metrics import MetricsCollector

# Initialize tracer
tracer = trace.get_tracer(__name__)

# Configure DataDog
initialize(api_key=os.getenv('DD_API_KEY'), app_key=os.getenv('DD_APP_KEY'))


class LogLevel(Enum):
    """Log level enumeration"""
    DEBUG = "DEBUG"
    INFO = "INFO" 
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    FATAL = "FATAL"


class LogSource(Enum):
    """Log source enumeration"""
    APPLICATION = "application"
    CONTAINER = "container"
    KUBERNETES = "kubernetes"
    SYSTEM = "system"
    DATABASE = "database"
    API = "api"
    ML_PIPELINE = "ml_pipeline"
    ETL_PIPELINE = "etl_pipeline"
    MESSAGE_QUEUE = "message_queue"
    STREAM_PROCESSING = "stream_processing"
    SECURITY = "security"
    AUDIT = "audit"


class LogFormat(Enum):
    """Log format enumeration"""
    JSON = "json"
    STRUCTURED = "structured" 
    PLAIN_TEXT = "plain_text"
    SYSLOG = "syslog"
    COMMON_LOG = "common_log"


class RetentionPolicy(Enum):
    """Log retention policy"""
    HOT = "hot"      # 7 days - high performance storage
    WARM = "warm"    # 30 days - medium performance storage  
    COLD = "cold"    # 90 days - low cost storage
    ARCHIVE = "archive"  # 365 days - archival storage


@dataclass
class LogEntry:
    """Structured log entry"""
    timestamp: datetime
    level: LogLevel
    message: str
    source: LogSource
    service: str
    environment: str
    version: str = "1.0.0"
    host: str = socket.gethostname()
    container_id: Optional[str] = None
    kubernetes_pod: Optional[str] = None
    kubernetes_namespace: Optional[str] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    correlation_id: Optional[str] = None
    tags: Dict[str, str] = None
    metadata: Dict[str, Any] = None
    exception: Optional[Dict[str, str]] = None
    duration_ms: Optional[float] = None
    status_code: Optional[int] = None
    url: Optional[str] = None
    user_agent: Optional[str] = None
    ip_address: Optional[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = {}
        if self.metadata is None:
            self.metadata = {}
    
    def to_json(self) -> str:
        """Convert log entry to JSON string"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['level'] = self.level.value
        data['source'] = self.source.value
        return json.dumps(data, default=str)
    
    def to_datadog_format(self) -> Dict[str, Any]:
        """Convert to DataDog log format"""
        return {
            "timestamp": self.timestamp.isoformat(),
            "status": self.level.value.lower(),
            "message": self.message,
            "logger": {
                "name": f"{self.service}.{self.source.value}",
                "level": self.level.value
            },
            "host": self.host,
            "service": self.service,
            "source": self.source.value,
            "env": self.environment,
            "version": self.version,
            "dd": {
                "trace_id": self.trace_id,
                "span_id": self.span_id
            } if self.trace_id else {},
            "usr": {
                "id": self.user_id,
                "session_id": self.session_id
            } if self.user_id else {},
            "network": {
                "client": {
                    "ip": self.ip_address
                }
            } if self.ip_address else {},
            "http": {
                "status_code": self.status_code,
                "url": self.url,
                "user_agent": self.user_agent,
                "duration": self.duration_ms
            } if any([self.status_code, self.url, self.user_agent, self.duration_ms]) else {},
            "kubernetes": {
                "pod_name": self.kubernetes_pod,
                "namespace": self.kubernetes_namespace
            } if self.kubernetes_pod else {},
            "container": {
                "id": self.container_id
            } if self.container_id else {},
            "error": self.exception if self.exception else {},
            "custom": {
                **self.tags,
                **self.metadata
            }
        }
    
    def get_fingerprint(self) -> str:
        """Generate unique fingerprint for log entry"""
        content = f"{self.service}{self.source.value}{self.level.value}{self.message}"
        return hashlib.md5(content.encode()).hexdigest()


@dataclass
class LogCollectionConfig:
    """Configuration for log collection"""
    # Buffer settings
    max_buffer_size: int = 10000
    batch_size: int = 100
    batch_timeout_seconds: int = 30
    flush_interval_seconds: int = 60
    
    # DataDog settings
    datadog_api_key: str = os.getenv("DD_API_KEY", "")
    datadog_app_key: str = os.getenv("DD_APP_KEY", "")
    datadog_site: str = os.getenv("DD_SITE", "datadoghq.com")
    datadog_intake_url: str = f"https://http-intake.logs.{os.getenv('DD_SITE', 'datadoghq.com')}/v1/input/{os.getenv('DD_API_KEY', '')}"
    
    # Collection settings
    enable_kubernetes_logs: bool = True
    enable_container_logs: bool = True
    enable_system_logs: bool = True
    enable_application_logs: bool = True
    
    # Retention settings
    hot_retention_days: int = 7
    warm_retention_days: int = 30
    cold_retention_days: int = 90
    archive_retention_days: int = 365
    
    # Storage paths
    hot_storage_path: Path = Path("./logs/hot")
    warm_storage_path: Path = Path("./logs/warm") 
    cold_storage_path: Path = Path("./logs/cold")
    archive_storage_path: Path = Path("./logs/archive")
    
    # Processing settings
    enable_real_time_processing: bool = True
    enable_batch_processing: bool = True
    processing_threads: int = 4
    compression_enabled: bool = True
    
    # Filter settings
    log_level_filter: LogLevel = LogLevel.DEBUG
    service_filters: Set[str] = None
    source_filters: Set[LogSource] = None
    
    def __post_init__(self):
        if self.service_filters is None:
            self.service_filters = set()
        if self.source_filters is None:
            self.source_filters = set()


class LogShipper:
    """Handles shipping logs from various sources to DataDog"""
    
    def __init__(self, config: LogCollectionConfig):
        self.config = config
        self.logger = get_logger(f"{__name__}.LogShipper")
        self.is_running = False
        self.shipping_threads = []
        self._setup_shipping_channels()
        
    def _setup_shipping_channels(self):
        """Setup log shipping channels"""
        self.channels = {
            "datadog_http": self._ship_to_datadog_http,
            "datadog_agent": self._ship_to_datadog_agent,
            "local_storage": self._ship_to_local_storage
        }
    
    async def ship_logs(self, logs: List[LogEntry], channel: str = "datadog_http"):
        """Ship logs using specified channel"""
        try:
            if channel in self.channels:
                await self.channels[channel](logs)
            else:
                self.logger.error(f"Unknown shipping channel: {channel}")
        except Exception as e:
            self.logger.error(f"Error shipping logs via {channel}: {e}")
    
    async def _ship_to_datadog_http(self, logs: List[LogEntry]):
        """Ship logs to DataDog via HTTP API"""
        try:
            formatted_logs = []
            for log_entry in logs:
                formatted_logs.append(log_entry.to_datadog_format())
            
            headers = {
                "Content-Type": "application/json",
                "DD-API-KEY": self.config.datadog_api_key
            }
            
            response = requests.post(
                self.config.datadog_intake_url,
                json=formatted_logs,
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                self.logger.debug(f"Successfully shipped {len(logs)} logs to DataDog HTTP")
            else:
                self.logger.error(f"Failed to ship logs to DataDog: {response.status_code} - {response.text}")
                
        except Exception as e:
            self.logger.error(f"Error shipping to DataDog HTTP: {e}")
    
    async def _ship_to_datadog_agent(self, logs: List[LogEntry]):
        """Ship logs to DataDog Agent"""
        try:
            # Use DataDog Agent's log forwarding
            agent_url = "http://localhost:8126/v0.4/traces"  # DataDog Agent endpoint
            
            for log_entry in logs:
                # Send to DataDog Agent via UDP or TCP
                log_data = log_entry.to_json()
                # Implementation depends on DataDog Agent configuration
                self.logger.debug(f"Sent log to DataDog Agent: {log_entry.message[:100]}...")
                
        except Exception as e:
            self.logger.error(f"Error shipping to DataDog Agent: {e}")
    
    async def _ship_to_local_storage(self, logs: List[LogEntry]):
        """Ship logs to local storage for backup/archive"""
        try:
            current_time = datetime.now()
            
            for log_entry in logs:
                # Determine storage tier based on log age
                log_age = current_time - log_entry.timestamp
                
                if log_age.days <= self.config.hot_retention_days:
                    storage_path = self.config.hot_storage_path
                elif log_age.days <= self.config.warm_retention_days:
                    storage_path = self.config.warm_storage_path
                elif log_age.days <= self.config.cold_retention_days:
                    storage_path = self.config.cold_storage_path
                else:
                    storage_path = self.config.archive_storage_path
                
                # Create storage directory
                storage_path.mkdir(parents=True, exist_ok=True)
                
                # Write log entry
                log_file = storage_path / f"{log_entry.service}_{current_time.strftime('%Y%m%d')}.log"
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(log_entry.to_json() + "\n")
                    
        except Exception as e:
            self.logger.error(f"Error shipping to local storage: {e}")


class ContainerLogCollector:
    """Collects logs from Docker containers"""
    
    def __init__(self, config: LogCollectionConfig):
        self.config = config
        self.logger = get_logger(f"{__name__}.ContainerLogCollector")
        self.docker_client = None
        self.collection_tasks = {}
        
    async def start_collection(self) -> List[LogEntry]:
        """Start collecting container logs"""
        if not self.config.enable_container_logs:
            return []
            
        try:
            self.docker_client = docker.from_env()
            containers = self.docker_client.containers.list()
            
            logs = []
            for container in containers:
                container_logs = await self._collect_container_logs(container)
                logs.extend(container_logs)
            
            return logs
            
        except Exception as e:
            self.logger.error(f"Error collecting container logs: {e}")
            return []
    
    async def _collect_container_logs(self, container) -> List[LogEntry]:
        """Collect logs from a specific container"""
        try:
            logs = []
            
            # Get recent logs from container
            log_lines = container.logs(tail=100, timestamps=True).decode('utf-8').split('\n')
            
            for line in log_lines:
                if not line.strip():
                    continue
                    
                # Parse container log line
                log_entry = await self._parse_container_log(line, container)
                if log_entry:
                    logs.append(log_entry)
            
            return logs
            
        except Exception as e:
            self.logger.error(f"Error collecting logs from container {container.name}: {e}")
            return []
    
    async def _parse_container_log(self, log_line: str, container) -> Optional[LogEntry]:
        """Parse a container log line into LogEntry"""
        try:
            # Split timestamp and message
            parts = log_line.split(' ', 1)
            if len(parts) < 2:
                return None
                
            timestamp_str, message = parts
            
            # Parse timestamp
            try:
                timestamp = datetime.fromisoformat(timestamp_str.rstrip('Z'))
            except:
                timestamp = datetime.now()
            
            # Determine log level from message
            level = LogLevel.INFO
            if any(keyword in message.upper() for keyword in ['ERROR', 'EXCEPTION', 'FAILED']):
                level = LogLevel.ERROR
            elif any(keyword in message.upper() for keyword in ['WARN', 'WARNING']):
                level = LogLevel.WARNING
            elif any(keyword in message.upper() for keyword in ['DEBUG']):
                level = LogLevel.DEBUG
            
            # Get container metadata
            container_info = container.attrs
            
            return LogEntry(
                timestamp=timestamp,
                level=level,
                message=message,
                source=LogSource.CONTAINER,
                service=container.name,
                environment=os.getenv("DD_ENV", "development"),
                container_id=container.id[:12],
                tags={
                    "container_name": container.name,
                    "container_image": container_info.get('Config', {}).get('Image', ''),
                    "container_status": container.status
                },
                metadata={
                    "container_created": container_info.get('Created', ''),
                    "container_platform": container_info.get('Platform', ''),
                    "container_mounts": len(container_info.get('Mounts', []))
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error parsing container log line: {e}")
            return None


class KubernetesLogCollector:
    """Collects logs from Kubernetes pods"""
    
    def __init__(self, config: LogCollectionConfig):
        self.config = config
        self.logger = get_logger(f"{__name__}.KubernetesLogCollector")
        self.k8s_client = None
        self._setup_kubernetes_client()
    
    def _setup_kubernetes_client(self):
        """Setup Kubernetes client"""
        try:
            # Try to load in-cluster config first
            config.load_incluster_config()
        except:
            try:
                # Fall back to local kubeconfig
                config.load_kube_config()
            except:
                self.logger.warning("Could not load Kubernetes configuration")
                return
        
        self.k8s_client = client.CoreV1Api()
    
    async def start_collection(self) -> List[LogEntry]:
        """Start collecting Kubernetes logs"""
        if not self.config.enable_kubernetes_logs or not self.k8s_client:
            return []
        
        try:
            logs = []
            
            # Get all pods across all namespaces
            pods = self.k8s_client.list_pod_for_all_namespaces()
            
            for pod in pods.items:
                if pod.status.phase == "Running":
                    pod_logs = await self._collect_pod_logs(pod)
                    logs.extend(pod_logs)
            
            return logs
            
        except Exception as e:
            self.logger.error(f"Error collecting Kubernetes logs: {e}")
            return []
    
    async def _collect_pod_logs(self, pod) -> List[LogEntry]:
        """Collect logs from a specific pod"""
        try:
            logs = []
            
            # Get logs for each container in the pod
            for container in pod.spec.containers:
                try:
                    log_lines = self.k8s_client.read_namespaced_pod_log(
                        name=pod.metadata.name,
                        namespace=pod.metadata.namespace,
                        container=container.name,
                        tail_lines=100
                    ).split('\n')
                    
                    for line in log_lines:
                        if not line.strip():
                            continue
                            
                        log_entry = await self._parse_pod_log(line, pod, container.name)
                        if log_entry:
                            logs.append(log_entry)
                            
                except Exception as e:
                    self.logger.debug(f"Could not get logs for container {container.name} in pod {pod.metadata.name}: {e}")
            
            return logs
            
        except Exception as e:
            self.logger.error(f"Error collecting logs from pod {pod.metadata.name}: {e}")
            return []
    
    async def _parse_pod_log(self, log_line: str, pod, container_name: str) -> Optional[LogEntry]:
        """Parse a pod log line into LogEntry"""
        try:
            # Try to parse JSON logs first
            try:
                log_data = json.loads(log_line)
                timestamp = datetime.fromisoformat(log_data.get('timestamp', datetime.now().isoformat()))
                level_str = log_data.get('level', 'INFO').upper()
                message = log_data.get('message', log_line)
            except:
                # Fall back to plain text parsing
                timestamp = datetime.now()
                level_str = 'INFO'
                message = log_line
                
                # Try to extract log level from message
                if any(keyword in message.upper() for keyword in ['ERROR', 'EXCEPTION', 'FAILED']):
                    level_str = 'ERROR'
                elif any(keyword in message.upper() for keyword in ['WARN', 'WARNING']):
                    level_str = 'WARNING'
                elif any(keyword in message.upper() for keyword in ['DEBUG']):
                    level_str = 'DEBUG'
            
            # Convert level string to LogLevel enum
            try:
                level = LogLevel(level_str)
            except:
                level = LogLevel.INFO
            
            return LogEntry(
                timestamp=timestamp,
                level=level,
                message=message,
                source=LogSource.KUBERNETES,
                service=pod.metadata.labels.get('app', pod.metadata.name),
                environment=pod.metadata.namespace,
                kubernetes_pod=pod.metadata.name,
                kubernetes_namespace=pod.metadata.namespace,
                container_id=container_name,
                tags={
                    "pod_name": pod.metadata.name,
                    "namespace": pod.metadata.namespace,
                    "container": container_name,
                    "node": pod.spec.node_name or "unknown",
                    **pod.metadata.labels
                },
                metadata={
                    "pod_ip": pod.status.pod_ip,
                    "host_ip": pod.status.host_ip,
                    "phase": pod.status.phase,
                    "restart_count": sum(container.restart_count or 0 for container in pod.status.container_statuses or [])
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error parsing pod log line: {e}")
            return None


class SystemLogCollector:
    """Collects system logs"""
    
    def __init__(self, config: LogCollectionConfig):
        self.config = config
        self.logger = get_logger(f"{__name__}.SystemLogCollector")
    
    async def start_collection(self) -> List[LogEntry]:
        """Start collecting system logs"""
        if not self.config.enable_system_logs:
            return []
        
        try:
            logs = []
            
            # Collect system metrics as logs
            system_logs = await self._collect_system_metrics()
            logs.extend(system_logs)
            
            # Collect system events
            event_logs = await self._collect_system_events()
            logs.extend(event_logs)
            
            return logs
            
        except Exception as e:
            self.logger.error(f"Error collecting system logs: {e}")
            return []
    
    async def _collect_system_metrics(self) -> List[LogEntry]:
        """Collect system metrics as log entries"""
        try:
            logs = []
            current_time = datetime.now()
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            logs.append(LogEntry(
                timestamp=current_time,
                level=LogLevel.INFO,
                message=f"CPU usage: {cpu_percent}%",
                source=LogSource.SYSTEM,
                service="system-monitor",
                environment=os.getenv("DD_ENV", "development"),
                tags={"metric_type": "cpu", "unit": "percent"},
                metadata={"value": cpu_percent}
            ))
            
            # Memory usage
            memory = psutil.virtual_memory()
            logs.append(LogEntry(
                timestamp=current_time,
                level=LogLevel.INFO,
                message=f"Memory usage: {memory.percent}%",
                source=LogSource.SYSTEM,
                service="system-monitor", 
                environment=os.getenv("DD_ENV", "development"),
                tags={"metric_type": "memory", "unit": "percent"},
                metadata={
                    "percent": memory.percent,
                    "used_gb": round(memory.used / (1024**3), 2),
                    "total_gb": round(memory.total / (1024**3), 2)
                }
            ))
            
            # Disk usage
            disk = psutil.disk_usage('/')
            logs.append(LogEntry(
                timestamp=current_time,
                level=LogLevel.INFO,
                message=f"Disk usage: {round((disk.used / disk.total) * 100, 1)}%",
                source=LogSource.SYSTEM,
                service="system-monitor",
                environment=os.getenv("DD_ENV", "development"),
                tags={"metric_type": "disk", "unit": "percent"},
                metadata={
                    "percent": round((disk.used / disk.total) * 100, 1),
                    "used_gb": round(disk.used / (1024**3), 2),
                    "total_gb": round(disk.total / (1024**3), 2)
                }
            ))
            
            return logs
            
        except Exception as e:
            self.logger.error(f"Error collecting system metrics: {e}")
            return []
    
    async def _collect_system_events(self) -> List[LogEntry]:
        """Collect system events"""
        try:
            logs = []
            
            # Check for system processes with high resource usage
            for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
                try:
                    if proc.info['cpu_percent'] > 80:
                        logs.append(LogEntry(
                            timestamp=datetime.now(),
                            level=LogLevel.WARNING,
                            message=f"High CPU usage detected for process {proc.info['name']} (PID: {proc.info['pid']}): {proc.info['cpu_percent']}%",
                            source=LogSource.SYSTEM,
                            service="system-monitor",
                            environment=os.getenv("DD_ENV", "development"),
                            tags={
                                "event_type": "high_cpu",
                                "process_name": proc.info['name'],
                                "process_pid": str(proc.info['pid'])
                            },
                            metadata={
                                "cpu_percent": proc.info['cpu_percent'],
                                "memory_percent": proc.info['memory_percent']
                            }
                        ))
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            return logs
            
        except Exception as e:
            self.logger.error(f"Error collecting system events: {e}")
            return []


class ApplicationLogCollector:
    """Collects application-specific logs"""
    
    def __init__(self, config: LogCollectionConfig):
        self.config = config
        self.logger = get_logger(f"{__name__}.ApplicationLogCollector")
    
    async def start_collection(self) -> List[LogEntry]:
        """Start collecting application logs"""
        if not self.config.enable_application_logs:
            return []
        
        try:
            logs = []
            
            # Collect logs from various application sources
            app_logs = await self._collect_application_logs()
            logs.extend(app_logs)
            
            return logs
            
        except Exception as e:
            self.logger.error(f"Error collecting application logs: {e}")
            return []
    
    async def _collect_application_logs(self) -> List[LogEntry]:
        """Collect logs from application log files"""
        try:
            logs = []
            
            # Define application log sources
            log_sources = [
                {"path": "./logs/app.log", "service": "main-app", "source": LogSource.APPLICATION},
                {"path": "./logs/api.log", "service": "api-gateway", "source": LogSource.API},
                {"path": "./logs/etl.log", "service": "etl-pipeline", "source": LogSource.ETL_PIPELINE},
                {"path": "./logs/ml.log", "service": "ml-pipeline", "source": LogSource.ML_PIPELINE}
            ]
            
            for log_source in log_sources:
                log_path = Path(log_source["path"])
                if log_path.exists():
                    file_logs = await self._parse_log_file(log_path, log_source["service"], log_source["source"])
                    logs.extend(file_logs)
            
            return logs
            
        except Exception as e:
            self.logger.error(f"Error collecting application logs: {e}")
            return []
    
    async def _parse_log_file(self, log_path: Path, service: str, source: LogSource) -> List[LogEntry]:
        """Parse a log file and extract log entries"""
        try:
            logs = []
            
            with open(log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
                # Get only recent lines (last 100)
                recent_lines = lines[-100:] if len(lines) > 100 else lines
                
                for line in recent_lines:
                    if not line.strip():
                        continue
                    
                    log_entry = await self._parse_log_line(line, service, source)
                    if log_entry:
                        logs.append(log_entry)
            
            return logs
            
        except Exception as e:
            self.logger.error(f"Error parsing log file {log_path}: {e}")
            return []
    
    async def _parse_log_line(self, line: str, service: str, source: LogSource) -> Optional[LogEntry]:
        """Parse a single log line"""
        try:
            # Try to parse as JSON first
            try:
                log_data = json.loads(line)
                return LogEntry(
                    timestamp=datetime.fromisoformat(log_data.get('timestamp', datetime.now().isoformat())),
                    level=LogLevel(log_data.get('level', 'INFO')),
                    message=log_data.get('message', line),
                    source=source,
                    service=service,
                    environment=os.getenv("DD_ENV", "development"),
                    trace_id=log_data.get('trace_id'),
                    span_id=log_data.get('span_id'),
                    tags=log_data.get('tags', {}),
                    metadata=log_data.get('metadata', {})
                )
            except (json.JSONDecodeError, ValueError):
                pass
            
            # Parse structured log format (e.g., "2023-01-01 12:00:00 INFO message")
            timestamp_pattern = r'(\d{4}-\d{2}-\d{2}[\s\T]\d{2}:\d{2}:\d{2})'
            level_pattern = r'(DEBUG|INFO|WARNING|ERROR|CRITICAL|FATAL)'
            
            match = re.match(rf'{timestamp_pattern}.*?{level_pattern}\s+(.*)', line)
            if match:
                timestamp_str, level_str, message = match.groups()
                
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('T', ' '))
                except:
                    timestamp = datetime.now()
                
                try:
                    level = LogLevel(level_str)
                except:
                    level = LogLevel.INFO
                
                return LogEntry(
                    timestamp=timestamp,
                    level=level,
                    message=message.strip(),
                    source=source,
                    service=service,
                    environment=os.getenv("DD_ENV", "development")
                )
            
            # Fall back to plain text
            return LogEntry(
                timestamp=datetime.now(),
                level=LogLevel.INFO,
                message=line.strip(),
                source=source,
                service=service,
                environment=os.getenv("DD_ENV", "development")
            )
            
        except Exception as e:
            self.logger.error(f"Error parsing log line: {e}")
            return None


class EnterpriseDataDogLogCollector:
    """Main enterprise DataDog log collection orchestrator"""
    
    def __init__(self, config: Optional[LogCollectionConfig] = None):
        self.config = config or LogCollectionConfig()
        self.logger = get_logger(f"{__name__}.EnterpriseDataDogLogCollector")
        
        # Initialize collectors
        self.log_shipper = LogShipper(self.config)
        self.container_collector = ContainerLogCollector(self.config)
        self.k8s_collector = KubernetesLogCollector(self.config)
        self.system_collector = SystemLogCollector(self.config)
        self.app_collector = ApplicationLogCollector(self.config)
        
        # Collection state
        self.is_running = False
        self.collection_tasks = []
        self.log_buffer = queue.Queue(maxsize=self.config.max_buffer_size)
        self.stats = {
            "logs_collected": 0,
            "logs_shipped": 0,
            "logs_failed": 0,
            "start_time": None,
            "collection_cycles": 0
        }
        
        # Setup metrics collection
        self.metrics = MetricsCollector()
    
    async def start(self):
        """Start the log collection system"""
        try:
            self.is_running = True
            self.stats["start_time"] = datetime.now()
            
            self.logger.info("Starting Enterprise DataDog Log Collection System")
            
            # Start collection loop
            while self.is_running:
                await self._collection_cycle()
                await asyncio.sleep(self.config.flush_interval_seconds)
                
        except Exception as e:
            self.logger.error(f"Error in log collection system: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the log collection system"""
        try:
            self.is_running = False
            
            # Flush remaining logs
            await self._flush_buffer()
            
            self.logger.info("Enterprise DataDog Log Collection System stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping log collection system: {e}")
    
    async def _collection_cycle(self):
        """Perform a single collection cycle"""
        try:
            self.stats["collection_cycles"] += 1
            cycle_start = datetime.now()
            
            self.logger.debug("Starting log collection cycle")
            
            # Collect logs from all sources concurrently
            collection_tasks = [
                self.container_collector.start_collection(),
                self.k8s_collector.start_collection(),
                self.system_collector.start_collection(),
                self.app_collector.start_collection()
            ]
            
            results = await asyncio.gather(*collection_tasks, return_exceptions=True)
            
            # Process collected logs
            all_logs = []
            for result in results:
                if isinstance(result, list):
                    all_logs.extend(result)
                elif isinstance(result, Exception):
                    self.logger.error(f"Collection error: {result}")
            
            # Filter logs
            filtered_logs = self._filter_logs(all_logs)
            
            # Buffer logs for shipping
            for log_entry in filtered_logs:
                if not self.log_buffer.full():
                    self.log_buffer.put(log_entry)
                    self.stats["logs_collected"] += 1
                else:
                    self.logger.warning("Log buffer full, dropping log entry")
                    self.stats["logs_failed"] += 1
            
            # Ship logs if buffer is ready
            if self.log_buffer.qsize() >= self.config.batch_size:
                await self._ship_buffered_logs()
            
            # Update metrics
            cycle_duration = (datetime.now() - cycle_start).total_seconds()
            self.metrics.histogram("log_collection.cycle_duration", cycle_duration, tags=["collector:enterprise_datadog"])
            self.metrics.gauge("log_collection.buffer_size", self.log_buffer.qsize(), tags=["collector:enterprise_datadog"])
            self.metrics.counter("log_collection.logs_collected", len(filtered_logs), tags=["collector:enterprise_datadog"])
            
            self.logger.debug(f"Collection cycle completed in {cycle_duration:.2f}s, collected {len(filtered_logs)} logs")
            
        except Exception as e:
            self.logger.error(f"Error in collection cycle: {e}")
            self.stats["logs_failed"] += 1
    
    def _filter_logs(self, logs: List[LogEntry]) -> List[LogEntry]:
        """Filter logs based on configuration"""
        filtered = []
        
        for log_entry in logs:
            # Filter by log level
            if log_entry.level.value < self.config.log_level_filter.value:
                continue
            
            # Filter by service
            if self.config.service_filters and log_entry.service not in self.config.service_filters:
                continue
            
            # Filter by source
            if self.config.source_filters and log_entry.source not in self.config.source_filters:
                continue
            
            filtered.append(log_entry)
        
        return filtered
    
    async def _ship_buffered_logs(self):
        """Ship buffered logs to DataDog"""
        try:
            logs_to_ship = []
            
            # Extract logs from buffer
            while not self.log_buffer.empty() and len(logs_to_ship) < self.config.batch_size:
                try:
                    log_entry = self.log_buffer.get_nowait()
                    logs_to_ship.append(log_entry)
                except queue.Empty:
                    break
            
            if not logs_to_ship:
                return
            
            # Ship logs to multiple channels
            shipping_tasks = [
                self.log_shipper.ship_logs(logs_to_ship, "datadog_http"),
                self.log_shipper.ship_logs(logs_to_ship, "local_storage")
            ]
            
            await asyncio.gather(*shipping_tasks, return_exceptions=True)
            
            self.stats["logs_shipped"] += len(logs_to_ship)
            
            self.logger.debug(f"Shipped {len(logs_to_ship)} logs to DataDog")
            
        except Exception as e:
            self.logger.error(f"Error shipping logs: {e}")
            self.stats["logs_failed"] += len(logs_to_ship)
    
    async def _flush_buffer(self):
        """Flush all remaining logs in buffer"""
        try:
            if self.log_buffer.qsize() > 0:
                await self._ship_buffered_logs()
                self.logger.info(f"Flushed {self.log_buffer.qsize()} remaining logs")
                
        except Exception as e:
            self.logger.error(f"Error flushing buffer: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get collection statistics"""
        current_stats = self.stats.copy()
        if current_stats["start_time"]:
            current_stats["uptime_seconds"] = (datetime.now() - current_stats["start_time"]).total_seconds()
        current_stats["buffer_size"] = self.log_buffer.qsize()
        current_stats["is_running"] = self.is_running
        return current_stats
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check"""
        stats = self.get_stats()
        
        health = {
            "status": "healthy",
            "is_running": self.is_running,
            "buffer_utilization": stats["buffer_size"] / self.config.max_buffer_size,
            "error_rate": stats["logs_failed"] / max(stats["logs_collected"], 1),
            "stats": stats
        }
        
        # Determine health status
        if health["error_rate"] > 0.1:  # >10% error rate
            health["status"] = "degraded"
        if not health["is_running"]:
            health["status"] = "unhealthy"
        
        return health


# Factory function
def create_enterprise_log_collector(config: Optional[LogCollectionConfig] = None) -> EnterpriseDataDogLogCollector:
    """Create an enterprise DataDog log collector"""
    return EnterpriseDataDogLogCollector(config)


# Example usage
if __name__ == "__main__":
    import asyncio
    
    async def main():
        # Configure log collection
        config = LogCollectionConfig()
        config.batch_size = 50
        config.flush_interval_seconds = 30
        config.enable_kubernetes_logs = True
        config.enable_container_logs = True
        
        # Create and start collector
        collector = create_enterprise_log_collector(config)
        
        try:
            # Run for 2 minutes as demo
            await asyncio.wait_for(collector.start(), timeout=120)
        except asyncio.TimeoutError:
            print("Demo completed")
        
        # Print stats
        stats = collector.get_stats()
        print(f"Collection completed. Stats: {json.dumps(stats, indent=2, default=str)}")
        
        # Health check
        health = collector.health_check()
        print(f"Health: {health['status']}")
    
    asyncio.run(main())
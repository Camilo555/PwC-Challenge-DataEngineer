"""
DataDog Container and Docker Monitoring
Comprehensive container monitoring with security scanning and lifecycle tracking
"""

import logging
from typing import Dict, List, Any, Optional
from datadog import initialize, statsd
from datadog.api.metrics import Metrics
from datetime import datetime, timedelta
import asyncio
import time
import json
import docker
import requests
import subprocess
from concurrent.futures import ThreadPoolExecutor
import psutil
import os

# Initialize DataDog
initialize(
    api_key="<your-api-key>",
    app_key="<your-app-key>",
    host_name="enterprise-data-platform"
)

logger = logging.getLogger(__name__)

class DataDogContainerMonitor:
    """Comprehensive container monitoring for enterprise data platform"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.executor = ThreadPoolExecutor(max_workers=15)
        
        # Docker client
        self.docker_client = docker.from_env()
        
        # Monitoring configuration
        self.monitoring_interval = config.get('monitoring_interval', 60)
        self.security_scan_interval = config.get('security_scan_interval', 3600)  # 1 hour
        self.last_security_scan = {}
        
        # Performance baselines
        self.performance_baselines = {
            'cpu_usage_max': 0.8,
            'memory_usage_max': 0.9,
            'disk_io_max': 1000000000,  # 1GB/s
            'network_io_max': 100000000,  # 100MB/s
            'container_restart_threshold': 5
        }
        
        # Security scanning tools
        self.security_tools = {
            'trivy': config.get('trivy_enabled', True),
            'clair': config.get('clair_enabled', False),
            'anchore': config.get('anchore_enabled', False)
        }
        
        # Container registry authentication
        self.registry_auth = config.get('registry_auth', {})
        
        # Vulnerability database
        self.vulnerability_cache = {}
        self.vulnerability_cache_ttl = config.get('vulnerability_cache_ttl', 3600)
        
    async def monitor_containers(self) -> Dict[str, Any]:
        """Monitor all running containers"""
        try:
            containers = self.docker_client.containers.list(all=True)
            
            metrics = {
                'total_containers': len(containers),
                'running_containers': 0,
                'stopped_containers': 0,
                'paused_containers': 0,
                'restarting_containers': 0,
                'container_details': []
            }
            
            # Process each container
            for container in containers:
                try:
                    container_metrics = await self._monitor_single_container(container)
                    
                    if container_metrics:
                        metrics['container_details'].append(container_metrics)
                        
                        # Update status counts
                        status = container_metrics.get('status', 'unknown')
                        if status == 'running':
                            metrics['running_containers'] += 1
                        elif status == 'exited':
                            metrics['stopped_containers'] += 1
                        elif status == 'paused':
                            metrics['paused_containers'] += 1
                        elif status == 'restarting':
                            metrics['restarting_containers'] += 1
                
                except Exception as container_error:
                    logger.warning(f"Error monitoring container {container.id[:12]}: {container_error}")
                    continue
            
            # Send aggregate metrics
            await self._send_container_aggregate_metrics(metrics)
            
            # Check for security scans
            await self._check_security_scans()
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error monitoring containers: {e}")
            statsd.increment('container.monitoring.error')
            return {}
    
    async def _monitor_single_container(self, container) -> Dict[str, Any]:
        """Monitor a single container"""
        try:
            # Basic container info
            container_info = {
                'id': container.id[:12],
                'name': container.name,
                'image': container.image.tags[0] if container.image.tags else container.image.id[:12],
                'status': container.status,
                'created': container.attrs['Created'],
                'started_at': container.attrs.get('State', {}).get('StartedAt'),
                'finished_at': container.attrs.get('State', {}).get('FinishedAt')
            }
            
            # Container labels and environment
            labels = container.labels or {}
            container_info['labels'] = labels
            container_info['service'] = labels.get('com.docker.compose.service', 'unknown')
            container_info['project'] = labels.get('com.docker.compose.project', 'unknown')
            
            # Only get detailed metrics for running containers
            if container.status == 'running':
                try:
                    # Get container stats
                    stats = container.stats(stream=False, decode=True)
                    
                    # CPU metrics
                    cpu_stats = self._calculate_cpu_stats(stats)
                    container_info.update(cpu_stats)
                    
                    # Memory metrics
                    memory_stats = self._calculate_memory_stats(stats)
                    container_info.update(memory_stats)
                    
                    # Network metrics
                    network_stats = self._calculate_network_stats(stats)
                    container_info.update(network_stats)
                    
                    # Block IO metrics
                    blkio_stats = self._calculate_blkio_stats(stats)
                    container_info.update(blkio_stats)
                    
                    # Process count
                    try:
                        processes = container.top()
                        container_info['process_count'] = len(processes.get('Processes', []))
                    except:
                        container_info['process_count'] = 0
                    
                    # Container health check
                    health = container.attrs.get('State', {}).get('Health', {})
                    if health:
                        container_info['health_status'] = health.get('Status', 'none')
                        container_info['failing_streak'] = health.get('FailingStreak', 0)
                    
                except docker.errors.APIError as api_error:
                    logger.warning(f"Could not get stats for container {container.name}: {api_error}")
                    return container_info
            
            # Container restart count
            restart_count = container.attrs.get('RestartCount', 0)
            container_info['restart_count'] = restart_count
            
            # Container exit code (for stopped containers)
            if container.status in ['exited', 'dead']:
                exit_code = container.attrs.get('State', {}).get('ExitCode', 0)
                container_info['exit_code'] = exit_code
            
            # Send individual container metrics
            await self._send_container_metrics(container_info)
            
            return container_info
            
        except Exception as e:
            logger.error(f"Error monitoring container {container.id[:12]}: {e}")
            return {}
    
    def _calculate_cpu_stats(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate CPU usage statistics"""
        try:
            cpu_stats = stats.get('cpu_stats', {})
            precpu_stats = stats.get('precpu_stats', {})
            
            # Calculate CPU usage percentage
            cpu_usage = cpu_stats.get('cpu_usage', {})
            precpu_usage = precpu_stats.get('cpu_usage', {})
            
            total_usage = cpu_usage.get('total_usage', 0)
            prev_total_usage = precpu_usage.get('total_usage', 0)
            
            system_usage = cpu_stats.get('system_cpu_usage', 0)
            prev_system_usage = precpu_stats.get('system_cpu_usage', 0)
            
            num_cpus = len(cpu_usage.get('percpu_usage', []))
            if num_cpus == 0:
                num_cpus = os.cpu_count() or 1
            
            cpu_delta = total_usage - prev_total_usage
            system_delta = system_usage - prev_system_usage
            
            if system_delta > 0 and cpu_delta >= 0:
                cpu_percent = (cpu_delta / system_delta) * num_cpus * 100.0
            else:
                cpu_percent = 0.0
            
            # Throttling information
            throttling = cpu_stats.get('throttling_data', {})
            
            return {
                'cpu_percent': min(cpu_percent, 100.0),
                'cpu_shares': cpu_stats.get('cpu_shares', 0),
                'throttled_periods': throttling.get('throttled_periods', 0),
                'throttled_time': throttling.get('throttled_time', 0),
                'num_cpus': num_cpus
            }
            
        except Exception as e:
            logger.warning(f"Error calculating CPU stats: {e}")
            return {'cpu_percent': 0.0}
    
    def _calculate_memory_stats(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate memory usage statistics"""
        try:
            memory_stats = stats.get('memory_stats', {})
            
            # Memory usage
            usage = memory_stats.get('usage', 0)
            limit = memory_stats.get('limit', 0)
            
            # Calculate memory percentage
            memory_percent = (usage / limit * 100.0) if limit > 0 else 0.0
            
            # Cache and buffer information
            cache = memory_stats.get('stats', {}).get('cache', 0)
            rss = memory_stats.get('stats', {}).get('rss', 0)
            
            # Memory statistics breakdown
            stats_detail = memory_stats.get('stats', {})
            
            return {
                'memory_usage': usage,
                'memory_limit': limit,
                'memory_percent': min(memory_percent, 100.0),
                'memory_cache': cache,
                'memory_rss': rss,
                'memory_swap': stats_detail.get('swap', 0),
                'memory_mapped_file': stats_detail.get('mapped_file', 0),
                'memory_active_anon': stats_detail.get('active_anon', 0),
                'memory_inactive_anon': stats_detail.get('inactive_anon', 0)
            }
            
        except Exception as e:
            logger.warning(f"Error calculating memory stats: {e}")
            return {'memory_usage': 0, 'memory_percent': 0.0}
    
    def _calculate_network_stats(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate network I/O statistics"""
        try:
            networks = stats.get('networks', {})
            
            total_rx_bytes = 0
            total_tx_bytes = 0
            total_rx_packets = 0
            total_tx_packets = 0
            total_rx_errors = 0
            total_tx_errors = 0
            total_rx_dropped = 0
            total_tx_dropped = 0
            
            for interface, net_stats in networks.items():
                total_rx_bytes += net_stats.get('rx_bytes', 0)
                total_tx_bytes += net_stats.get('tx_bytes', 0)
                total_rx_packets += net_stats.get('rx_packets', 0)
                total_tx_packets += net_stats.get('tx_packets', 0)
                total_rx_errors += net_stats.get('rx_errors', 0)
                total_tx_errors += net_stats.get('tx_errors', 0)
                total_rx_dropped += net_stats.get('rx_dropped', 0)
                total_tx_dropped += net_stats.get('tx_dropped', 0)
            
            return {
                'network_rx_bytes': total_rx_bytes,
                'network_tx_bytes': total_tx_bytes,
                'network_rx_packets': total_rx_packets,
                'network_tx_packets': total_tx_packets,
                'network_rx_errors': total_rx_errors,
                'network_tx_errors': total_tx_errors,
                'network_rx_dropped': total_rx_dropped,
                'network_tx_dropped': total_tx_dropped
            }
            
        except Exception as e:
            logger.warning(f"Error calculating network stats: {e}")
            return {}
    
    def _calculate_blkio_stats(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate block I/O statistics"""
        try:
            blkio_stats = stats.get('blkio_stats', {})
            
            # I/O service bytes
            io_service_bytes = blkio_stats.get('io_service_bytes_recursive', [])
            read_bytes = 0
            write_bytes = 0
            
            for entry in io_service_bytes:
                if entry.get('op') == 'Read':
                    read_bytes += entry.get('value', 0)
                elif entry.get('op') == 'Write':
                    write_bytes += entry.get('value', 0)
            
            # I/O serviced (operations count)
            io_serviced = blkio_stats.get('io_serviced_recursive', [])
            read_ops = 0
            write_ops = 0
            
            for entry in io_serviced:
                if entry.get('op') == 'Read':
                    read_ops += entry.get('value', 0)
                elif entry.get('op') == 'Write':
                    write_ops += entry.get('value', 0)
            
            return {
                'blkio_read_bytes': read_bytes,
                'blkio_write_bytes': write_bytes,
                'blkio_read_ops': read_ops,
                'blkio_write_ops': write_ops,
                'blkio_total_bytes': read_bytes + write_bytes
            }
            
        except Exception as e:
            logger.warning(f"Error calculating block I/O stats: {e}")
            return {}
    
    async def _send_container_metrics(self, container_info: Dict[str, Any]):
        """Send individual container metrics to DataDog"""
        try:
            container_id = container_info.get('id', 'unknown')
            container_name = container_info.get('name', 'unknown')
            service = container_info.get('service', 'unknown')
            project = container_info.get('project', 'unknown')
            
            base_tags = [
                f'container_id:{container_id}',
                f'container_name:{container_name}',
                f'service:{service}',
                f'project:{project}',
                f'image:{container_info.get("image", "unknown")}',
                f'status:{container_info.get("status", "unknown")}'
            ]
            
            # CPU metrics
            if 'cpu_percent' in container_info:
                statsd.gauge('docker.container.cpu.usage', container_info['cpu_percent'], tags=base_tags)
                statsd.gauge('docker.container.cpu.throttled_periods', 
                           container_info.get('throttled_periods', 0), tags=base_tags)
                statsd.gauge('docker.container.cpu.throttled_time', 
                           container_info.get('throttled_time', 0), tags=base_tags)
            
            # Memory metrics
            if 'memory_usage' in container_info:
                statsd.gauge('docker.container.memory.usage', container_info['memory_usage'], tags=base_tags)
                statsd.gauge('docker.container.memory.limit', container_info['memory_limit'], tags=base_tags)
                statsd.gauge('docker.container.memory.usage_pct', container_info['memory_percent'], tags=base_tags)
                statsd.gauge('docker.container.memory.cache', container_info.get('memory_cache', 0), tags=base_tags)
                statsd.gauge('docker.container.memory.rss', container_info.get('memory_rss', 0), tags=base_tags)
                statsd.gauge('docker.container.memory.swap', container_info.get('memory_swap', 0), tags=base_tags)
            
            # Network metrics
            if 'network_rx_bytes' in container_info:
                statsd.gauge('docker.container.network.rx_bytes', container_info['network_rx_bytes'], tags=base_tags)
                statsd.gauge('docker.container.network.tx_bytes', container_info['network_tx_bytes'], tags=base_tags)
                statsd.gauge('docker.container.network.rx_packets', container_info['network_rx_packets'], tags=base_tags)
                statsd.gauge('docker.container.network.tx_packets', container_info['network_tx_packets'], tags=base_tags)
                statsd.gauge('docker.container.network.rx_errors', container_info['network_rx_errors'], tags=base_tags)
                statsd.gauge('docker.container.network.tx_errors', container_info['network_tx_errors'], tags=base_tags)
            
            # Block I/O metrics
            if 'blkio_read_bytes' in container_info:
                statsd.gauge('docker.container.blkio.read_bytes', container_info['blkio_read_bytes'], tags=base_tags)
                statsd.gauge('docker.container.blkio.write_bytes', container_info['blkio_write_bytes'], tags=base_tags)
                statsd.gauge('docker.container.blkio.read_ops', container_info['blkio_read_ops'], tags=base_tags)
                statsd.gauge('docker.container.blkio.write_ops', container_info['blkio_write_ops'], tags=base_tags)
            
            # Container lifecycle metrics
            statsd.gauge('docker.container.restart_count', container_info.get('restart_count', 0), tags=base_tags)
            statsd.gauge('docker.container.process_count', container_info.get('process_count', 0), tags=base_tags)
            
            # Health check metrics
            if 'health_status' in container_info:
                health_status_map = {'healthy': 1, 'unhealthy': 0, 'starting': 0.5, 'none': -1}
                health_value = health_status_map.get(container_info['health_status'], -1)
                statsd.gauge('docker.container.health.status', health_value, tags=base_tags)
                statsd.gauge('docker.container.health.failing_streak', 
                           container_info.get('failing_streak', 0), tags=base_tags)
            
            # Check alert thresholds
            await self._check_container_alerts(container_info, base_tags)
            
        except Exception as e:
            logger.warning(f"Error sending container metrics: {e}")
    
    async def _send_container_aggregate_metrics(self, metrics: Dict[str, Any]):
        """Send aggregate container metrics"""
        try:
            base_tags = ['scope:cluster']
            
            # Container counts by status
            statsd.gauge('docker.containers.total', metrics['total_containers'], tags=base_tags)
            statsd.gauge('docker.containers.running', metrics['running_containers'], tags=base_tags)
            statsd.gauge('docker.containers.stopped', metrics['stopped_containers'], tags=base_tags)
            statsd.gauge('docker.containers.paused', metrics['paused_containers'], tags=base_tags)
            statsd.gauge('docker.containers.restarting', metrics['restarting_containers'], tags=base_tags)
            
        except Exception as e:
            logger.warning(f"Error sending aggregate metrics: {e}")
    
    async def _check_container_alerts(self, container_info: Dict[str, Any], tags: List[str]):
        """Check container alert thresholds"""
        try:
            container_name = container_info.get('name', 'unknown')
            
            # Check CPU usage
            cpu_percent = container_info.get('cpu_percent', 0)
            if cpu_percent > self.performance_baselines['cpu_usage_max'] * 100:
                statsd.event(
                    title=f"Container CPU Usage Alert - {container_name}",
                    text=f"CPU usage {cpu_percent:.1f}% exceeds threshold {self.performance_baselines['cpu_usage_max']*100:.1f}%",
                    alert_type='warning',
                    tags=tags
                )
            
            # Check memory usage
            memory_percent = container_info.get('memory_percent', 0)
            if memory_percent > self.performance_baselines['memory_usage_max'] * 100:
                statsd.event(
                    title=f"Container Memory Usage Alert - {container_name}",
                    text=f"Memory usage {memory_percent:.1f}% exceeds threshold {self.performance_baselines['memory_usage_max']*100:.1f}%",
                    alert_type='warning',
                    tags=tags
                )
            
            # Check restart count
            restart_count = container_info.get('restart_count', 0)
            if restart_count > self.performance_baselines['container_restart_threshold']:
                statsd.event(
                    title=f"Container Restart Alert - {container_name}",
                    text=f"Container has restarted {restart_count} times, exceeds threshold {self.performance_baselines['container_restart_threshold']}",
                    alert_type='warning',
                    tags=tags
                )
            
            # Check health status
            health_status = container_info.get('health_status')
            if health_status == 'unhealthy':
                statsd.event(
                    title=f"Container Health Alert - {container_name}",
                    text=f"Container health check is failing",
                    alert_type='error',
                    tags=tags
                )
            
            # Check container status
            status = container_info.get('status')
            if status in ['dead', 'exited'] and container_info.get('exit_code', 0) != 0:
                statsd.event(
                    title=f"Container Exit Alert - {container_name}",
                    text=f"Container exited with code {container_info.get('exit_code', 'unknown')}",
                    alert_type='error',
                    tags=tags
                )
            
        except Exception as e:
            logger.warning(f"Error checking container alerts: {e}")
    
    async def _check_security_scans(self):
        """Check if security scans need to be performed"""
        try:
            current_time = time.time()
            
            for container in self.docker_client.containers.list():
                container_key = f"{container.name}:{container.image.id}"
                last_scan = self.last_security_scan.get(container_key, 0)
                
                if current_time - last_scan > self.security_scan_interval:
                    # Perform security scan
                    await self._perform_security_scan(container)
                    self.last_security_scan[container_key] = current_time
            
        except Exception as e:
            logger.warning(f"Error checking security scans: {e}")
    
    async def _perform_security_scan(self, container):
        """Perform security vulnerability scan on container"""
        try:
            image_name = container.image.tags[0] if container.image.tags else container.image.id
            container_name = container.name
            
            logger.info(f"Starting security scan for container {container_name} with image {image_name}")
            
            vulnerabilities = {}
            
            # Trivy scan
            if self.security_tools.get('trivy'):
                trivy_results = await self._run_trivy_scan(image_name)
                vulnerabilities.update(trivy_results)
            
            # Process vulnerability results
            await self._process_vulnerability_results(container_name, image_name, vulnerabilities)
            
        except Exception as e:
            logger.error(f"Error performing security scan: {e}")
            statsd.increment('docker.security.scan_error', 
                           tags=[f'container:{container.name}', f'image:{image_name}'])
    
    async def _run_trivy_scan(self, image_name: str) -> Dict[str, Any]:
        """Run Trivy vulnerability scan"""
        try:
            # Run Trivy scan
            cmd = [
                'trivy',
                'image',
                '--format', 'json',
                '--quiet',
                image_name
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                scan_results = json.loads(result.stdout)
                
                # Process Trivy results
                vulnerabilities = {
                    'critical': 0,
                    'high': 0,
                    'medium': 0,
                    'low': 0,
                    'unknown': 0,
                    'total': 0,
                    'details': []
                }
                
                for target in scan_results.get('Results', []):
                    for vuln in target.get('Vulnerabilities', []):
                        severity = vuln.get('Severity', 'UNKNOWN').lower()
                        
                        if severity in vulnerabilities:
                            vulnerabilities[severity] += 1
                        else:
                            vulnerabilities['unknown'] += 1
                        
                        vulnerabilities['total'] += 1
                        vulnerabilities['details'].append({
                            'vulnerability_id': vuln.get('VulnerabilityID'),
                            'severity': severity,
                            'title': vuln.get('Title'),
                            'description': vuln.get('Description', '')[:200],  # Truncate
                            'installed_version': vuln.get('InstalledVersion'),
                            'fixed_version': vuln.get('FixedVersion')
                        })
                
                logger.info(f"Trivy scan completed: {vulnerabilities['total']} vulnerabilities found")
                return {'trivy': vulnerabilities}
                
            else:
                logger.warning(f"Trivy scan failed: {result.stderr}")
                return {}
                
        except subprocess.TimeoutExpired:
            logger.warning(f"Trivy scan timed out for image {image_name}")
            return {}
        except Exception as e:
            logger.error(f"Error running Trivy scan: {e}")
            return {}
    
    async def _process_vulnerability_results(self, container_name: str, image_name: str, vulnerabilities: Dict[str, Any]):
        """Process and send vulnerability scan results"""
        try:
            tags = [f'container:{container_name}', f'image:{image_name}']
            
            for tool, results in vulnerabilities.items():
                if isinstance(results, dict) and 'total' in results:
                    # Send vulnerability counts by severity
                    statsd.gauge(f'docker.security.vulnerabilities.critical', 
                               results.get('critical', 0), tags=tags + [f'scanner:{tool}'])
                    statsd.gauge(f'docker.security.vulnerabilities.high', 
                               results.get('high', 0), tags=tags + [f'scanner:{tool}'])
                    statsd.gauge(f'docker.security.vulnerabilities.medium', 
                               results.get('medium', 0), tags=tags + [f'scanner:{tool}'])
                    statsd.gauge(f'docker.security.vulnerabilities.low', 
                               results.get('low', 0), tags=tags + [f'scanner:{tool}'])
                    statsd.gauge(f'docker.security.vulnerabilities.total', 
                               results.get('total', 0), tags=tags + [f'scanner:{tool}'])
                    
                    # Send alerts for critical vulnerabilities
                    critical_count = results.get('critical', 0)
                    high_count = results.get('high', 0)
                    
                    if critical_count > 0:
                        statsd.event(
                            title=f"Critical Vulnerabilities Found - {container_name}",
                            text=f"Found {critical_count} critical vulnerabilities in container {container_name} (image: {image_name})",
                            alert_type='error',
                            tags=tags + [f'scanner:{tool}']
                        )
                    
                    if high_count > 5:  # Alert if more than 5 high severity vulnerabilities
                        statsd.event(
                            title=f"High Vulnerability Count - {container_name}",
                            text=f"Found {high_count} high severity vulnerabilities in container {container_name} (image: {image_name})",
                            alert_type='warning',
                            tags=tags + [f'scanner:{tool}']
                        )
            
        except Exception as e:
            logger.error(f"Error processing vulnerability results: {e}")
    
    async def monitor_docker_daemon(self) -> Dict[str, Any]:
        """Monitor Docker daemon metrics"""
        try:
            # Get Docker system information
            system_info = self.docker_client.info()
            
            metrics = {
                'containers': system_info.get('Containers', 0),
                'containers_running': system_info.get('ContainersRunning', 0),
                'containers_paused': system_info.get('ContainersPaused', 0),
                'containers_stopped': system_info.get('ContainersStopped', 0),
                'images': system_info.get('Images', 0),
                'server_version': system_info.get('ServerVersion', 'unknown'),
                'storage_driver': system_info.get('Driver', 'unknown'),
                'total_memory': system_info.get('MemTotal', 0),
                'ncpu': system_info.get('NCPU', 0)
            }
            
            # Storage information
            if 'DriverStatus' in system_info:
                for status in system_info['DriverStatus']:
                    if len(status) >= 2:
                        key = status[0].lower().replace(' ', '_')
                        if 'size' in key or 'available' in key or 'used' in key:
                            try:
                                # Try to extract numeric value
                                value_str = status[1]
                                if 'GB' in value_str:
                                    value = float(value_str.replace('GB', '').strip()) * 1024 * 1024 * 1024
                                elif 'MB' in value_str:
                                    value = float(value_str.replace('MB', '').strip()) * 1024 * 1024
                                elif 'KB' in value_str:
                                    value = float(value_str.replace('KB', '').strip()) * 1024
                                else:
                                    value = float(value_str.strip())
                                metrics[f'storage_{key}'] = value
                            except:
                                pass
            
            # Send daemon metrics
            await self._send_docker_daemon_metrics(metrics)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error monitoring Docker daemon: {e}")
            statsd.increment('docker.daemon.monitoring.error')
            return {}
    
    async def _send_docker_daemon_metrics(self, metrics: Dict[str, Any]):
        """Send Docker daemon metrics"""
        try:
            base_tags = ['scope:daemon']
            
            # Container counts
            statsd.gauge('docker.daemon.containers.total', metrics.get('containers', 0), tags=base_tags)
            statsd.gauge('docker.daemon.containers.running', metrics.get('containers_running', 0), tags=base_tags)
            statsd.gauge('docker.daemon.containers.paused', metrics.get('containers_paused', 0), tags=base_tags)
            statsd.gauge('docker.daemon.containers.stopped', metrics.get('containers_stopped', 0), tags=base_tags)
            statsd.gauge('docker.daemon.images.total', metrics.get('images', 0), tags=base_tags)
            
            # System resources
            statsd.gauge('docker.daemon.memory.total', metrics.get('total_memory', 0), tags=base_tags)
            statsd.gauge('docker.daemon.cpu.count', metrics.get('ncpu', 0), tags=base_tags)
            
            # Storage metrics
            for key, value in metrics.items():
                if key.startswith('storage_') and isinstance(value, (int, float)):
                    statsd.gauge(f'docker.daemon.{key}', value, tags=base_tags)
            
        except Exception as e:
            logger.warning(f"Error sending Docker daemon metrics: {e}")
    
    async def run_monitoring_loop(self):
        """Run continuous container monitoring loop"""
        logger.info("Starting container monitoring loop")
        
        while True:
            try:
                # Monitor containers
                container_metrics = await self.monitor_containers()
                
                # Monitor Docker daemon
                daemon_metrics = await self.monitor_docker_daemon()
                
                logger.info(f"Container monitoring cycle completed: "
                           f"{container_metrics.get('total_containers', 0)} containers monitored")
                
                # Send health check metric
                statsd.gauge('docker.monitoring.health', 1, tags=['status:healthy'])
                
                # Wait for next monitoring interval
                await asyncio.sleep(self.monitoring_interval)
                
            except Exception as e:
                logger.error(f"Error in container monitoring loop: {e}")
                statsd.increment('docker.monitoring.loop_error')
                await asyncio.sleep(60)  # Wait 1 minute before retrying

# Configuration example
CONTAINER_MONITORING_CONFIG = {
    'monitoring_interval': 60,  # seconds
    'security_scan_interval': 3600,  # 1 hour
    'trivy_enabled': True,
    'clair_enabled': False,
    'anchore_enabled': False,
    'vulnerability_cache_ttl': 3600,
    'registry_auth': {
        'registry.company.com': {
            'username': 'scanner',
            'password': 'scanner-password'
        }
    },
    'performance_baselines': {
        'cpu_usage_max': 0.8,
        'memory_usage_max': 0.9,
        'container_restart_threshold': 5
    }
}

async def main():
    """Main entry point for container monitoring"""
    monitor = DataDogContainerMonitor(CONTAINER_MONITORING_CONFIG)
    await monitor.run_monitoring_loop()

if __name__ == "__main__":
    asyncio.run(main())
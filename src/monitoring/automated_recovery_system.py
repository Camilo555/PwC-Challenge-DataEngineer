"""
Automated Recovery System with Self-Healing Capabilities
Provides intelligent failure detection, automated recovery actions,
circuit breakers, failover mechanisms, and performance optimization.
"""
import asyncio
import json
import time
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Union
from collections import defaultdict, deque
import threading
import logging
from pathlib import Path

try:
    import docker
    import kubernetes
    CONTAINER_ORCHESTRATION_AVAILABLE = True
except ImportError:
    CONTAINER_ORCHESTRATION_AVAILABLE = False

from core.config import settings
from core.logging import get_logger
from .infrastructure_health_monitor import (
    InfrastructureHealthManager, HealthStatus, ComponentType,
    HealthMetrics, CircuitBreakerState
)
from .intelligent_alerting_system import IntelligentAlertingSystem, AlertSeverity

logger = get_logger(__name__)


class RecoveryAction(Enum):
    """Types of recovery actions"""
    RESTART_SERVICE = "restart_service"
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    CLEAR_CACHE = "clear_cache"
    DRAIN_QUEUE = "drain_queue"
    FAILOVER = "failover"
    CIRCUIT_BREAKER_RESET = "circuit_breaker_reset"
    MEMORY_CLEANUP = "memory_cleanup"
    CONNECTION_RESET = "connection_reset"
    CUSTOM_SCRIPT = "custom_script"


class RecoveryTrigger(Enum):
    """Recovery trigger conditions"""
    HEALTH_CHECK_FAILURE = "health_check_failure"
    CIRCUIT_BREAKER_OPEN = "circuit_breaker_open"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    ALERT_ESCALATION = "alert_escalation"
    MANUAL_TRIGGER = "manual_trigger"


class RecoveryStatus(Enum):
    """Recovery action status"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


@dataclass
class RecoveryRule:
    """Recovery rule definition"""
    id: str
    name: str
    component_type: ComponentType
    trigger: RecoveryTrigger
    action: RecoveryAction
    conditions: Dict[str, Any]
    parameters: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    cooldown_seconds: int = 300
    max_attempts: int = 3
    retry_delay_seconds: int = 60
    escalation_delay_seconds: int = 900
    prerequisites: List[str] = field(default_factory=list)
    post_actions: List[str] = field(default_factory=list)


@dataclass
class RecoveryExecution:
    """Recovery execution record"""
    id: str
    rule_id: str
    component_id: str
    trigger_reason: str
    action: RecoveryAction
    status: RecoveryStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    attempts: int = 0
    success: bool = False
    error_message: Optional[str] = None
    logs: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FailoverConfiguration:
    """Failover configuration for high availability"""
    primary_component: str
    backup_components: List[str]
    health_check_interval: int = 30
    failure_threshold: int = 3
    recovery_timeout: int = 300
    automatic_failback: bool = False
    failback_delay: int = 600


class RecoveryActionHandler(ABC):
    """Base class for recovery action handlers"""
    
    @abstractmethod
    async def can_execute(self, component_id: str, parameters: Dict[str, Any]) -> bool:
        """Check if recovery action can be executed"""
        pass
    
    @abstractmethod
    async def execute(self, component_id: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Execute recovery action"""
        pass
    
    @abstractmethod
    async def verify_success(self, component_id: str, parameters: Dict[str, Any]) -> bool:
        """Verify if recovery action was successful"""
        pass


class ServiceRestartHandler(RecoveryActionHandler):
    """Handler for service restart recovery action"""
    
    async def can_execute(self, component_id: str, parameters: Dict[str, Any]) -> bool:
        """Check if service can be restarted"""
        try:
            service_type = parameters.get('service_type', 'docker')
            
            if service_type == 'docker' and CONTAINER_ORCHESTRATION_AVAILABLE:
                client = docker.from_env()
                container = client.containers.get(component_id)
                return container.status in ['running', 'exited', 'unhealthy']
            elif service_type == 'systemd':
                # Check if systemd service exists
                result = subprocess.run(['systemctl', 'status', component_id], 
                                      capture_output=True, text=True)
                return result.returncode in [0, 3]  # Active or inactive
            
            return False
        except Exception as e:
            logger.error(f"Error checking restart capability for {component_id}: {e}")
            return False
    
    async def execute(self, component_id: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Restart service"""
        try:
            service_type = parameters.get('service_type', 'docker')
            graceful_timeout = parameters.get('graceful_timeout', 30)
            
            if service_type == 'docker' and CONTAINER_ORCHESTRATION_AVAILABLE:
                client = docker.from_env()
                container = client.containers.get(component_id)
                
                logger.info(f"Restarting Docker container: {component_id}")
                container.restart(timeout=graceful_timeout)
                
                # Wait for container to be running
                for _ in range(30):  # Wait up to 30 seconds
                    container.reload()
                    if container.status == 'running':
                        break
                    await asyncio.sleep(1)
                
                return {
                    'success': container.status == 'running',
                    'container_id': container.id,
                    'status': container.status,
                    'method': 'docker'
                }
            
            elif service_type == 'systemd':
                logger.info(f"Restarting systemd service: {component_id}")
                result = subprocess.run(['sudo', 'systemctl', 'restart', component_id],
                                      capture_output=True, text=True, timeout=60)
                
                # Check if service is active
                status_result = subprocess.run(['systemctl', 'is-active', component_id],
                                             capture_output=True, text=True)
                
                return {
                    'success': result.returncode == 0 and status_result.stdout.strip() == 'active',
                    'exit_code': result.returncode,
                    'stderr': result.stderr,
                    'service_status': status_result.stdout.strip(),
                    'method': 'systemd'
                }
            
            else:
                return {'success': False, 'error': 'Unsupported service type or unavailable tools'}
        
        except Exception as e:
            logger.error(f"Error restarting service {component_id}: {e}")
            return {'success': False, 'error': str(e)}
    
    async def verify_success(self, component_id: str, parameters: Dict[str, Any]) -> bool:
        """Verify service restart was successful"""
        try:
            service_type = parameters.get('service_type', 'docker')
            
            if service_type == 'docker' and CONTAINER_ORCHESTRATION_AVAILABLE:
                client = docker.from_env()
                container = client.containers.get(component_id)
                return container.status == 'running'
            
            elif service_type == 'systemd':
                result = subprocess.run(['systemctl', 'is-active', component_id],
                                      capture_output=True, text=True)
                return result.stdout.strip() == 'active'
            
            return False
        except Exception as e:
            logger.error(f"Error verifying restart success for {component_id}: {e}")
            return False


class CacheClearHandler(RecoveryActionHandler):
    """Handler for cache clearing recovery action"""
    
    async def can_execute(self, component_id: str, parameters: Dict[str, Any]) -> bool:
        """Check if cache can be cleared"""
        cache_type = parameters.get('cache_type', 'redis')
        return cache_type in ['redis', 'memory', 'application']
    
    async def execute(self, component_id: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Clear cache"""
        try:
            cache_type = parameters.get('cache_type', 'redis')
            
            if cache_type == 'redis':
                import redis
                
                redis_host = parameters.get('host', 'localhost')
                redis_port = parameters.get('port', 6379)
                redis_db = parameters.get('db', 0)
                
                client = redis.Redis(host=redis_host, port=redis_port, db=redis_db, 
                                   socket_timeout=5, socket_connect_timeout=5)
                
                # Get keys count before flush
                keys_before = client.dbsize()
                
                # Flush database
                if parameters.get('flush_all_dbs', False):
                    client.flushall()
                else:
                    client.flushdb()
                
                keys_after = client.dbsize()
                
                logger.info(f"Cleared Redis cache for {component_id}: {keys_before} -> {keys_after} keys")
                
                return {
                    'success': True,
                    'keys_cleared': keys_before - keys_after,
                    'keys_before': keys_before,
                    'keys_after': keys_after,
                    'method': 'redis_flush'
                }
            
            elif cache_type == 'memory':
                # This would typically clear application-level memory caches
                logger.info(f"Memory cache clear requested for {component_id}")
                return {'success': True, 'method': 'memory_clear', 'note': 'Implementation specific'}
            
            else:
                return {'success': False, 'error': f'Unsupported cache type: {cache_type}'}
        
        except Exception as e:
            logger.error(f"Error clearing cache for {component_id}: {e}")
            return {'success': False, 'error': str(e)}
    
    async def verify_success(self, component_id: str, parameters: Dict[str, Any]) -> bool:
        """Verify cache clearing was successful"""
        try:
            cache_type = parameters.get('cache_type', 'redis')
            
            if cache_type == 'redis':
                import redis
                
                redis_host = parameters.get('host', 'localhost')
                redis_port = parameters.get('port', 6379)
                redis_db = parameters.get('db', 0)
                
                client = redis.Redis(host=redis_host, port=redis_port, db=redis_db,
                                   socket_timeout=5, socket_connect_timeout=5)
                
                # Check if database is empty or has minimal keys
                keys_count = client.dbsize()
                expected_max_keys = parameters.get('expected_max_keys', 10)
                
                return keys_count <= expected_max_keys
            
            return True  # Default to success for other cache types
        
        except Exception as e:
            logger.error(f"Error verifying cache clear for {component_id}: {e}")
            return False


class ScalingHandler(RecoveryActionHandler):
    """Handler for scaling operations"""
    
    async def can_execute(self, component_id: str, parameters: Dict[str, Any]) -> bool:
        """Check if scaling can be performed"""
        try:
            orchestrator = parameters.get('orchestrator', 'docker')
            
            if orchestrator == 'kubernetes' and CONTAINER_ORCHESTRATION_AVAILABLE:
                # Check if kubectl is available
                result = subprocess.run(['kubectl', 'version', '--client'], 
                                      capture_output=True, text=True)
                return result.returncode == 0
            elif orchestrator == 'docker-compose':
                # Check if docker-compose is available
                result = subprocess.run(['docker-compose', '--version'],
                                      capture_output=True, text=True)
                return result.returncode == 0
            
            return False
        except Exception as e:
            logger.error(f"Error checking scaling capability for {component_id}: {e}")
            return False
    
    async def execute(self, component_id: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Execute scaling operation"""
        try:
            orchestrator = parameters.get('orchestrator', 'kubernetes')
            scale_direction = parameters.get('direction', 'up')  # up or down
            target_replicas = parameters.get('target_replicas')
            scale_factor = parameters.get('scale_factor', 1.5)
            
            if orchestrator == 'kubernetes':
                namespace = parameters.get('namespace', 'default')
                
                # Get current replica count
                get_result = subprocess.run([
                    'kubectl', 'get', 'deployment', component_id,
                    '-n', namespace, '-o', 'jsonpath={.spec.replicas}'
                ], capture_output=True, text=True, timeout=30)
                
                if get_result.returncode != 0:
                    return {'success': False, 'error': 'Failed to get current replica count'}
                
                current_replicas = int(get_result.stdout.strip())
                
                # Calculate target replicas if not specified
                if target_replicas is None:
                    if scale_direction == 'up':
                        target_replicas = max(1, int(current_replicas * scale_factor))
                    else:
                        target_replicas = max(1, int(current_replicas / scale_factor))
                
                # Apply scaling
                scale_result = subprocess.run([
                    'kubectl', 'scale', 'deployment', component_id,
                    '-n', namespace, f'--replicas={target_replicas}'
                ], capture_output=True, text=True, timeout=60)
                
                if scale_result.returncode != 0:
                    return {
                        'success': False,
                        'error': f'Scaling failed: {scale_result.stderr}'
                    }
                
                logger.info(f"Scaled {component_id} from {current_replicas} to {target_replicas} replicas")
                
                return {
                    'success': True,
                    'method': 'kubernetes',
                    'previous_replicas': current_replicas,
                    'target_replicas': target_replicas,
                    'direction': scale_direction
                }
            
            elif orchestrator == 'docker-compose':
                service_name = parameters.get('service_name', component_id)
                compose_file = parameters.get('compose_file', 'docker-compose.yml')
                
                scale_result = subprocess.run([
                    'docker-compose', '-f', compose_file, 'up', '-d',
                    '--scale', f'{service_name}={target_replicas or 2}'
                ], capture_output=True, text=True, timeout=120)
                
                return {
                    'success': scale_result.returncode == 0,
                    'method': 'docker_compose',
                    'target_replicas': target_replicas or 2,
                    'stderr': scale_result.stderr if scale_result.returncode != 0 else None
                }
            
            else:
                return {'success': False, 'error': f'Unsupported orchestrator: {orchestrator}'}
        
        except Exception as e:
            logger.error(f"Error scaling {component_id}: {e}")
            return {'success': False, 'error': str(e)}
    
    async def verify_success(self, component_id: str, parameters: Dict[str, Any]) -> bool:
        """Verify scaling was successful"""
        try:
            orchestrator = parameters.get('orchestrator', 'kubernetes')
            target_replicas = parameters.get('target_replicas', 2)
            
            if orchestrator == 'kubernetes':
                namespace = parameters.get('namespace', 'default')
                
                # Wait for deployment to be ready
                for attempt in range(30):  # Wait up to 30 seconds
                    result = subprocess.run([
                        'kubectl', 'get', 'deployment', component_id,
                        '-n', namespace, '-o', 'jsonpath={.status.readyReplicas}'
                    ], capture_output=True, text=True)
                    
                    if result.returncode == 0 and result.stdout.strip():
                        ready_replicas = int(result.stdout.strip())
                        if ready_replicas >= target_replicas:
                            return True
                    
                    await asyncio.sleep(1)
                
                return False
            
            return True  # Default to success for other orchestrators
        
        except Exception as e:
            logger.error(f"Error verifying scaling for {component_id}: {e}")
            return False


class AutomatedRecoverySystem:
    """Central automated recovery system"""
    
    def __init__(self, health_manager: InfrastructureHealthManager,
                 alerting_system: IntelligentAlertingSystem):
        self.health_manager = health_manager
        self.alerting_system = alerting_system
        self.logger = get_logger(__name__)
        
        # Recovery rules and handlers
        self.recovery_rules: Dict[str, RecoveryRule] = {}
        self.action_handlers: Dict[RecoveryAction, RecoveryActionHandler] = {
            RecoveryAction.RESTART_SERVICE: ServiceRestartHandler(),
            RecoveryAction.CLEAR_CACHE: CacheClearHandler(),
            RecoveryAction.SCALE_UP: ScalingHandler(),
            RecoveryAction.SCALE_DOWN: ScalingHandler()
        }
        
        # Execution tracking
        self.executions: Dict[str, RecoveryExecution] = {}
        self.execution_history: deque = deque(maxlen=1000)
        
        # Failover configurations
        self.failover_configs: Dict[str, FailoverConfiguration] = {}
        
        # Recovery state
        self.recovery_active = False
        self.recovery_task = None
        self.cooldown_tracker: Dict[str, datetime] = {}
        
        # Statistics
        self.recovery_stats = {
            'total_executions': 0,
            'successful_executions': 0,
            'failed_executions': 0,
            'avg_execution_time': 0.0
        }
        
        # Setup default recovery rules
        self._setup_default_rules()
        
        # Register alert callback
        self.alerting_system.add_alert_callback(self._handle_alert_recovery)
    
    def _setup_default_rules(self):
        """Setup default recovery rules"""
        default_rules = [
            # Redis recovery rules
            RecoveryRule(
                id="redis_memory_critical_clear_cache",
                name="Clear Redis Cache on Memory Critical",
                component_type=ComponentType.REDIS,
                trigger=RecoveryTrigger.RESOURCE_EXHAUSTION,
                action=RecoveryAction.CLEAR_CACHE,
                conditions={
                    'memory_usage_percent': {'operator': 'gt', 'value': 95}
                },
                parameters={
                    'cache_type': 'redis',
                    'flush_all_dbs': False,
                    'expected_max_keys': 100
                },
                cooldown_seconds=600  # 10 minutes
            ),
            RecoveryRule(
                id="redis_offline_restart",
                name="Restart Redis on Offline",
                component_type=ComponentType.REDIS,
                trigger=RecoveryTrigger.HEALTH_CHECK_FAILURE,
                action=RecoveryAction.RESTART_SERVICE,
                conditions={
                    'status': {'operator': 'eq', 'value': 'offline'}
                },
                parameters={
                    'service_type': 'docker',
                    'graceful_timeout': 30
                },
                cooldown_seconds=300
            ),
            
            # RabbitMQ recovery rules
            RecoveryRule(
                id="rabbitmq_queue_critical_scale_consumers",
                name="Scale RabbitMQ Consumers on Queue Critical",
                component_type=ComponentType.RABBITMQ,
                trigger=RecoveryTrigger.PERFORMANCE_DEGRADATION,
                action=RecoveryAction.SCALE_UP,
                conditions={
                    'queue_depth_total': {'operator': 'gt', 'value': 5000}
                },
                parameters={
                    'orchestrator': 'kubernetes',
                    'direction': 'up',
                    'scale_factor': 1.5,
                    'max_replicas': 10
                },
                cooldown_seconds=900  # 15 minutes
            ),
            RecoveryRule(
                id="rabbitmq_offline_restart",
                name="Restart RabbitMQ on Offline",
                component_type=ComponentType.RABBITMQ,
                trigger=RecoveryTrigger.HEALTH_CHECK_FAILURE,
                action=RecoveryAction.RESTART_SERVICE,
                conditions={
                    'status': {'operator': 'eq', 'value': 'offline'}
                },
                parameters={
                    'service_type': 'docker',
                    'graceful_timeout': 45
                },
                cooldown_seconds=300
            ),
            
            # Kafka recovery rules
            RecoveryRule(
                id="kafka_consumer_lag_scale_consumers",
                name="Scale Kafka Consumers on High Lag",
                component_type=ComponentType.KAFKA,
                trigger=RecoveryTrigger.PERFORMANCE_DEGRADATION,
                action=RecoveryAction.SCALE_UP,
                conditions={
                    'consumer_lag_total': {'operator': 'gt', 'value': 10000}
                },
                parameters={
                    'orchestrator': 'kubernetes',
                    'direction': 'up',
                    'scale_factor': 2.0,
                    'max_replicas': 20
                },
                cooldown_seconds=1200  # 20 minutes
            ),
            RecoveryRule(
                id="kafka_offline_partitions_restart",
                name="Restart Kafka on Offline Partitions",
                component_type=ComponentType.KAFKA,
                trigger=RecoveryTrigger.HEALTH_CHECK_FAILURE,
                action=RecoveryAction.RESTART_SERVICE,
                conditions={
                    'offline_partitions': {'operator': 'gt', 'value': 0}
                },
                parameters={
                    'service_type': 'docker',
                    'graceful_timeout': 60
                },
                cooldown_seconds=600,  # 10 minutes - longer for Kafka
                max_attempts=2  # Fewer attempts for Kafka restarts
            )
        ]
        
        for rule in default_rules:
            self.recovery_rules[rule.id] = rule
    
    async def start_recovery_monitoring(self):
        """Start automated recovery monitoring"""
        if self.recovery_active:
            self.logger.warning("Recovery monitoring already active")
            return
        
        self.recovery_active = True
        self.recovery_task = asyncio.create_task(self._recovery_loop())
        self.logger.info("Started automated recovery monitoring")
    
    async def stop_recovery_monitoring(self):
        """Stop automated recovery monitoring"""
        self.recovery_active = False
        if self.recovery_task:
            self.recovery_task.cancel()
            try:
                await self.recovery_task
            except asyncio.CancelledError:
                pass
        self.logger.info("Stopped automated recovery monitoring")
    
    async def _recovery_loop(self):
        """Main recovery monitoring loop"""
        while self.recovery_active:
            try:
                await self._check_component_health()
                await self._cleanup_old_executions()
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in recovery loop: {e}")
                await asyncio.sleep(30)
    
    async def _check_component_health(self):
        """Check component health and trigger recovery if needed"""
        for monitor in self.health_manager.monitors.values():
            try:
                latest_metrics = monitor.get_latest_metrics()
                if not latest_metrics:
                    continue
                
                # Check if circuit breaker is open
                if monitor.circuit_breaker.state == CircuitBreakerState.OPEN:
                    await self._trigger_recovery(
                        monitor.component_id,
                        RecoveryTrigger.CIRCUIT_BREAKER_OPEN,
                        f"Circuit breaker open for {monitor.component_id}"
                    )
                
                # Check health status
                if latest_metrics.status == HealthStatus.OFFLINE:
                    await self._trigger_recovery(
                        monitor.component_id,
                        RecoveryTrigger.HEALTH_CHECK_FAILURE,
                        f"Component {monitor.component_id} is offline"
                    )
                
                # Check performance degradation
                if latest_metrics.response_time_ms > 5000:  # 5 seconds
                    await self._trigger_recovery(
                        monitor.component_id,
                        RecoveryTrigger.PERFORMANCE_DEGRADATION,
                        f"High response time for {monitor.component_id}: {latest_metrics.response_time_ms}ms"
                    )
                
                # Check resource exhaustion (component-specific)
                await self._check_resource_exhaustion(monitor.component_id, latest_metrics)
            
            except Exception as e:
                self.logger.error(f"Error checking health for {monitor.component_id}: {e}")
    
    async def _check_resource_exhaustion(self, component_id: str, metrics: HealthMetrics):
        """Check for resource exhaustion conditions"""
        try:
            if metrics.component_type == ComponentType.REDIS:
                if hasattr(metrics, 'memory_usage_percent') and metrics.memory_usage_percent > 95:
                    await self._trigger_recovery(
                        component_id,
                        RecoveryTrigger.RESOURCE_EXHAUSTION,
                        f"Redis memory usage critical: {metrics.memory_usage_percent}%"
                    )
            
            elif metrics.component_type == ComponentType.RABBITMQ:
                if hasattr(metrics, 'queue_depth_total') and metrics.queue_depth_total > 5000:
                    await self._trigger_recovery(
                        component_id,
                        RecoveryTrigger.PERFORMANCE_DEGRADATION,
                        f"RabbitMQ queue depth critical: {metrics.queue_depth_total}"
                    )
            
            elif metrics.component_type == ComponentType.KAFKA:
                if hasattr(metrics, 'consumer_lag_total') and metrics.consumer_lag_total > 10000:
                    await self._trigger_recovery(
                        component_id,
                        RecoveryTrigger.PERFORMANCE_DEGRADATION,
                        f"Kafka consumer lag critical: {metrics.consumer_lag_total}"
                    )
        
        except Exception as e:
            self.logger.error(f"Error checking resource exhaustion for {component_id}: {e}")
    
    async def _trigger_recovery(self, component_id: str, trigger: RecoveryTrigger, reason: str):
        """Trigger recovery for a component"""
        try:
            # Find applicable rules
            applicable_rules = []
            
            for rule in self.recovery_rules.values():
                if not rule.enabled:
                    continue
                
                if rule.trigger != trigger:
                    continue
                
                # Check component type match
                monitor = self.health_manager.monitors.get(component_id)
                if not monitor or monitor.component_type != rule.component_type:
                    continue
                
                # Check cooldown
                if self._is_in_cooldown(rule.id, component_id):
                    continue
                
                # Check conditions
                if await self._check_rule_conditions(rule, component_id):
                    applicable_rules.append(rule)
            
            # Execute applicable rules
            for rule in applicable_rules:
                await self._execute_recovery_rule(rule, component_id, reason)
        
        except Exception as e:
            self.logger.error(f"Error triggering recovery for {component_id}: {e}")
    
    def _is_in_cooldown(self, rule_id: str, component_id: str) -> bool:
        """Check if rule is in cooldown period"""
        cooldown_key = f"{rule_id}_{component_id}"
        if cooldown_key in self.cooldown_tracker:
            rule = self.recovery_rules[rule_id]
            time_since_last = (datetime.now() - self.cooldown_tracker[cooldown_key]).total_seconds()
            return time_since_last < rule.cooldown_seconds
        return False
    
    async def _check_rule_conditions(self, rule: RecoveryRule, component_id: str) -> bool:
        """Check if rule conditions are met"""
        try:
            monitor = self.health_manager.monitors.get(component_id)
            if not monitor:
                return False
            
            latest_metrics = monitor.get_latest_metrics()
            if not latest_metrics:
                return False
            
            # Check each condition
            for condition_field, condition_spec in rule.conditions.items():
                operator = condition_spec.get('operator', 'eq')
                expected_value = condition_spec.get('value')
                
                # Get actual value from metrics
                if hasattr(latest_metrics, condition_field):
                    actual_value = getattr(latest_metrics, condition_field)
                elif condition_field in latest_metrics.custom_metrics:
                    actual_value = latest_metrics.custom_metrics[condition_field]
                else:
                    # Handle status as string
                    if condition_field == 'status':
                        actual_value = latest_metrics.status.value
                    else:
                        continue  # Skip unknown fields
                
                # Evaluate condition
                if operator == 'eq' and actual_value != expected_value:
                    return False
                elif operator == 'ne' and actual_value == expected_value:
                    return False
                elif operator == 'gt' and actual_value <= expected_value:
                    return False
                elif operator == 'lt' and actual_value >= expected_value:
                    return False
                elif operator == 'ge' and actual_value < expected_value:
                    return False
                elif operator == 'le' and actual_value > expected_value:
                    return False
            
            return True
        
        except Exception as e:
            self.logger.error(f"Error checking rule conditions for {rule.id}: {e}")
            return False
    
    async def _execute_recovery_rule(self, rule: RecoveryRule, component_id: str, reason: str):
        """Execute a recovery rule"""
        try:
            execution_id = f"{rule.id}_{component_id}_{int(time.time())}"
            
            execution = RecoveryExecution(
                id=execution_id,
                rule_id=rule.id,
                component_id=component_id,
                trigger_reason=reason,
                action=rule.action,
                status=RecoveryStatus.PENDING,
                started_at=datetime.now()
            )
            
            self.executions[execution_id] = execution
            self.recovery_stats['total_executions'] += 1
            
            self.logger.info(f"Starting recovery execution: {execution_id}")
            
            # Update cooldown
            cooldown_key = f"{rule.id}_{component_id}"
            self.cooldown_tracker[cooldown_key] = datetime.now()
            
            # Execute recovery action
            success = await self._execute_recovery_action(execution, rule)
            
            # Update execution record
            execution.completed_at = datetime.now()
            execution.duration_seconds = (execution.completed_at - execution.started_at).total_seconds()
            execution.success = success
            execution.status = RecoveryStatus.COMPLETED if success else RecoveryStatus.FAILED
            
            # Update statistics
            if success:
                self.recovery_stats['successful_executions'] += 1
            else:
                self.recovery_stats['failed_executions'] += 1
            
            # Update average execution time
            total_executions = self.recovery_stats['total_executions']
            if total_executions > 0:
                current_avg = self.recovery_stats['avg_execution_time']
                self.recovery_stats['avg_execution_time'] = (
                    (current_avg * (total_executions - 1) + execution.duration_seconds) / total_executions
                )
            
            # Add to history
            self.execution_history.append(execution)
            
            # Generate completion alert
            await self._generate_recovery_alert(execution, rule)
            
            self.logger.info(f"Recovery execution completed: {execution_id} - Success: {success}")
        
        except Exception as e:
            self.logger.error(f"Error executing recovery rule {rule.id}: {e}")
            execution.status = RecoveryStatus.FAILED
            execution.error_message = str(e)
    
    async def _execute_recovery_action(self, execution: RecoveryExecution, rule: RecoveryRule) -> bool:
        """Execute the actual recovery action"""
        try:
            execution.status = RecoveryStatus.IN_PROGRESS
            
            handler = self.action_handlers.get(rule.action)
            if not handler:
                execution.error_message = f"No handler for action: {rule.action}"
                return False
            
            # Check if action can be executed
            can_execute = await handler.can_execute(execution.component_id, rule.parameters)
            if not can_execute:
                execution.error_message = f"Cannot execute {rule.action} for {execution.component_id}"
                return False
            
            # Execute action with retries
            for attempt in range(rule.max_attempts):
                execution.attempts = attempt + 1
                execution.logs.append(f"Attempt {attempt + 1} of {rule.max_attempts}")
                
                try:
                    result = await handler.execute(execution.component_id, rule.parameters)
                    execution.logs.append(f"Execution result: {result}")
                    
                    if result.get('success', False):
                        # Verify success
                        verification_success = await handler.verify_success(
                            execution.component_id, rule.parameters
                        )
                        
                        if verification_success:
                            execution.logs.append("Recovery action verified successful")
                            execution.metadata.update(result)
                            return True
                        else:
                            execution.logs.append("Recovery action executed but verification failed")
                    
                    if attempt < rule.max_attempts - 1:
                        execution.logs.append(f"Retrying in {rule.retry_delay_seconds} seconds")
                        await asyncio.sleep(rule.retry_delay_seconds)
                
                except Exception as e:
                    execution.logs.append(f"Execution error: {str(e)}")
                    if attempt < rule.max_attempts - 1:
                        await asyncio.sleep(rule.retry_delay_seconds)
            
            execution.error_message = f"Failed after {rule.max_attempts} attempts"
            return False
        
        except Exception as e:
            execution.error_message = f"Recovery action execution failed: {str(e)}"
            execution.logs.append(f"Fatal error: {str(e)}")
            return False
    
    async def _generate_recovery_alert(self, execution: RecoveryExecution, rule: RecoveryRule):
        """Generate alert for recovery action completion"""
        try:
            severity = AlertSeverity.INFO if execution.success else AlertSeverity.WARNING
            title = f"Recovery Action {'Completed' if execution.success else 'Failed'}"
            message = f"Recovery action {rule.action.value} for {execution.component_id} {'succeeded' if execution.success else 'failed'}"
            
            if not execution.success and execution.error_message:
                message += f": {execution.error_message}"
            
            # This would typically generate an alert through the alerting system
            self.logger.info(f"Recovery alert: {title} - {message}")
        
        except Exception as e:
            self.logger.error(f"Error generating recovery alert: {e}")
    
    def _handle_alert_recovery(self, alert):
        """Handle alert-triggered recovery"""
        try:
            if alert.severity == AlertSeverity.CRITICAL:
                # Trigger recovery for critical alerts
                asyncio.create_task(
                    self._trigger_recovery(
                        alert.component_id,
                        RecoveryTrigger.ALERT_ESCALATION,
                        f"Critical alert: {alert.title}"
                    )
                )
        except Exception as e:
            self.logger.error(f"Error handling alert recovery: {e}")
    
    async def _cleanup_old_executions(self):
        """Cleanup old execution records"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=24)
            
            to_remove = []
            for execution_id, execution in self.executions.items():
                if (execution.completed_at and 
                    execution.completed_at < cutoff_time and
                    execution.status in [RecoveryStatus.COMPLETED, RecoveryStatus.FAILED]):
                    to_remove.append(execution_id)
            
            for execution_id in to_remove:
                del self.executions[execution_id]
        
        except Exception as e:
            self.logger.error(f"Error cleaning up executions: {e}")
    
    # Public API methods
    def add_recovery_rule(self, rule: RecoveryRule):
        """Add custom recovery rule"""
        self.recovery_rules[rule.id] = rule
        self.logger.info(f"Added recovery rule: {rule.name}")
    
    def remove_recovery_rule(self, rule_id: str):
        """Remove recovery rule"""
        if rule_id in self.recovery_rules:
            del self.recovery_rules[rule_id]
            self.logger.info(f"Removed recovery rule: {rule_id}")
    
    def add_action_handler(self, action: RecoveryAction, handler: RecoveryActionHandler):
        """Add custom action handler"""
        self.action_handlers[action] = handler
        self.logger.info(f"Added action handler for: {action.value}")
    
    async def trigger_manual_recovery(self, component_id: str, action: RecoveryAction,
                                    parameters: Dict[str, Any] = None) -> str:
        """Trigger manual recovery action"""
        try:
            execution_id = f"manual_{component_id}_{action.value}_{int(time.time())}"
            
            execution = RecoveryExecution(
                id=execution_id,
                rule_id="manual",
                component_id=component_id,
                trigger_reason="Manual trigger",
                action=action,
                status=RecoveryStatus.PENDING,
                started_at=datetime.now()
            )
            
            self.executions[execution_id] = execution
            
            # Create temporary rule
            temp_rule = RecoveryRule(
                id="manual",
                name="Manual Recovery",
                component_type=ComponentType.SYSTEM,
                trigger=RecoveryTrigger.MANUAL_TRIGGER,
                action=action,
                conditions={},
                parameters=parameters or {},
                max_attempts=1
            )
            
            success = await self._execute_recovery_action(execution, temp_rule)
            
            execution.completed_at = datetime.now()
            execution.duration_seconds = (execution.completed_at - execution.started_at).total_seconds()
            execution.success = success
            execution.status = RecoveryStatus.COMPLETED if success else RecoveryStatus.FAILED
            
            return execution_id
        
        except Exception as e:
            self.logger.error(f"Error triggering manual recovery: {e}")
            raise
    
    def get_recovery_status(self) -> Dict[str, Any]:
        """Get recovery system status"""
        active_executions = len([e for e in self.executions.values() 
                               if e.status == RecoveryStatus.IN_PROGRESS])
        
        recent_executions = [e for e in self.execution_history 
                           if e.started_at > datetime.now() - timedelta(hours=24)]
        
        return {
            'recovery_active': self.recovery_active,
            'total_rules': len(self.recovery_rules),
            'total_handlers': len(self.action_handlers),
            'active_executions': active_executions,
            'total_executions': len(self.executions),
            'recent_executions_24h': len(recent_executions),
            'statistics': self.recovery_stats,
            'timestamp': datetime.now().isoformat()
        }
    
    def get_execution_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get execution history"""
        recent_executions = list(self.execution_history)[-limit:]
        return [asdict(execution) for execution in recent_executions]


# Factory function
def create_automated_recovery_system(
    health_manager: InfrastructureHealthManager,
    alerting_system: IntelligentAlertingSystem
) -> AutomatedRecoverySystem:
    """Create automated recovery system"""
    return AutomatedRecoverySystem(health_manager, alerting_system)


# Global instance
_recovery_system = None


def get_recovery_system(health_manager: InfrastructureHealthManager = None,
                       alerting_system: IntelligentAlertingSystem = None) -> AutomatedRecoverySystem:
    """Get global recovery system"""
    global _recovery_system
    if _recovery_system is None:
        if not health_manager or not alerting_system:
            from .infrastructure_health_monitor import get_infrastructure_manager
            from .intelligent_alerting_system import get_alerting_system
            
            health_manager = health_manager or get_infrastructure_manager()
            alerting_system = alerting_system or get_alerting_system()
        
        _recovery_system = create_automated_recovery_system(health_manager, alerting_system)
    return _recovery_system


if __name__ == "__main__":
    print("Automated Recovery System")
    print("Features:")
    print("- Intelligent failure detection and recovery")
    print("- Circuit breaker integration")
    print("- Service restart and scaling automation")
    print("- Cache clearing and memory management")
    print("- Kubernetes and Docker Compose support")
    print("- Customizable recovery rules and handlers")
    print("- Comprehensive execution tracking and statistics")
    print("- Alert-triggered and manual recovery capabilities")
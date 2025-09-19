"""
Rate Limiting Orchestrator
Enterprise orchestration layer for comprehensive rate limiting and throttling systems.
Coordinates multiple rate limiting strategies, monitoring, and adaptive responses.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from core.logging import get_logger
from monitoring.system_resource_monitor import SystemResourceMonitor

logger = get_logger(__name__)


class ThreatLevel(Enum):
    """Security threat levels for adaptive rate limiting."""
    LOW = 1
    MODERATE = 2
    HIGH = 3
    CRITICAL = 4
    EMERGENCY = 5


class ResponseStrategy(Enum):
    """Response strategies for rate limiting violations."""
    BLOCK = "block"
    QUEUE = "queue"
    THROTTLE = "throttle"
    CHALLENGE = "challenge"
    MONITOR = "monitor"


@dataclass
class RateLimitEvent:
    """Rate limiting event for monitoring and analysis."""
    timestamp: float
    client_id: str
    endpoint: str
    event_type: str  # 'violation', 'warning', 'anomaly'
    severity: ThreatLevel
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AdaptiveConfig:
    """Configuration for adaptive rate limiting responses."""
    base_multiplier: float = 1.0
    threat_multipliers: Dict[ThreatLevel, float] = field(default_factory=lambda: {
        ThreatLevel.LOW: 1.0,
        ThreatLevel.MODERATE: 0.8,
        ThreatLevel.HIGH: 0.6,
        ThreatLevel.CRITICAL: 0.4,
        ThreatLevel.EMERGENCY: 0.2
    })
    system_load_thresholds: Dict[str, float] = field(default_factory=lambda: {
        'cpu_high': 80.0,
        'memory_high': 85.0,
        'response_time_high': 2.0
    })


class SecurityThreatDetector:
    """Advanced threat detection for rate limiting scenarios."""

    def __init__(self):
        self.client_patterns: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            'request_history': deque(maxlen=1000),
            'violation_count': 0,
            'last_violation': 0,
            'behavior_score': 1.0,
            'threat_level': ThreatLevel.LOW
        })

        self.global_patterns = {
            'total_violations': 0,
            'violation_rate': deque(maxlen=100),
            'active_threats': set(),
            'blocked_ips': set()
        }

    def analyze_request_pattern(self, client_id: str, endpoint: str, timestamp: float) -> ThreatLevel:
        """Analyze request pattern to determine threat level."""
        client_data = self.client_patterns[client_id]
        client_data['request_history'].append({
            'timestamp': timestamp,
            'endpoint': endpoint
        })

        # Analyze request frequency
        recent_requests = [
            req for req in client_data['request_history']
            if timestamp - req['timestamp'] < 60  # Last minute
        ]

        request_rate = len(recent_requests)

        # Analyze endpoint diversity
        endpoints = set(req['endpoint'] for req in recent_requests)
        endpoint_diversity = len(endpoints)

        # Determine threat level
        if request_rate > 1000:  # Very high frequency
            threat_level = ThreatLevel.CRITICAL
        elif request_rate > 500:
            threat_level = ThreatLevel.HIGH
        elif request_rate > 100:
            threat_level = ThreatLevel.MODERATE
        elif endpoint_diversity == 1 and request_rate > 50:  # Focused attack
            threat_level = ThreatLevel.HIGH
        else:
            threat_level = ThreatLevel.LOW

        client_data['threat_level'] = threat_level
        return threat_level

    def record_violation(self, client_id: str, violation_type: str, severity: ThreatLevel):
        """Record a rate limiting violation."""
        client_data = self.client_patterns[client_id]
        client_data['violation_count'] += 1
        client_data['last_violation'] = time.time()

        # Update behavior score (lower is worse)
        score_reduction = {
            ThreatLevel.LOW: 0.1,
            ThreatLevel.MODERATE: 0.2,
            ThreatLevel.HIGH: 0.3,
            ThreatLevel.CRITICAL: 0.5,
            ThreatLevel.EMERGENCY: 0.8
        }

        client_data['behavior_score'] = max(
            0.1,
            client_data['behavior_score'] - score_reduction.get(severity, 0.1)
        )

        # Update global patterns
        self.global_patterns['total_violations'] += 1
        self.global_patterns['violation_rate'].append(time.time())

        if severity in [ThreatLevel.CRITICAL, ThreatLevel.EMERGENCY]:
            self.global_patterns['active_threats'].add(client_id)

    def get_client_threat_level(self, client_id: str) -> ThreatLevel:
        """Get current threat level for a client."""
        return self.client_patterns[client_id]['threat_level']

    def get_client_behavior_score(self, client_id: str) -> float:
        """Get behavior score for a client (0.1-1.0, lower is worse)."""
        return self.client_patterns[client_id]['behavior_score']

    def should_block_client(self, client_id: str) -> bool:
        """Determine if client should be temporarily blocked."""
        client_data = self.client_patterns[client_id]

        # Block if behavior score is very low or threat level is critical
        return (
            client_data['behavior_score'] < 0.3 or
            client_data['threat_level'] in [ThreatLevel.CRITICAL, ThreatLevel.EMERGENCY] or
            client_data['violation_count'] > 100
        )


class RateLimitingOrchestrator:
    """
    Enterprise orchestrator for comprehensive rate limiting and throttling.
    Coordinates multiple rate limiting strategies, monitoring, and adaptive responses.
    """

    def __init__(self, adaptive_config: Optional[AdaptiveConfig] = None):
        self.adaptive_config = adaptive_config or AdaptiveConfig()
        self.threat_detector = SecurityThreatDetector()
        self.resource_monitor = SystemResourceMonitor()

        # Event tracking
        self.events: deque = deque(maxlen=10000)
        self.metrics = {
            'total_requests': 0,
            'blocked_requests': 0,
            'throttled_requests': 0,
            'queued_requests': 0,
            'challenged_requests': 0,
            'adaptive_adjustments': 0,
            'threats_detected': 0,
            'false_positives': 0
        }

        # Background tasks
        self._monitoring_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        """Start the orchestrator background tasks."""
        if not self._running:
            self._running = True
            self._monitoring_task = asyncio.create_task(self._monitoring_loop())
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info("Rate limiting orchestrator started")

    async def stop(self):
        """Stop the orchestrator background tasks."""
        self._running = False

        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        logger.info("Rate limiting orchestrator stopped")

    async def evaluate_request(
        self,
        client_id: str,
        endpoint: str,
        current_rate_limit_result: Tuple[bool, Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, ResponseStrategy, Dict[str, Any]]:
        """
        Evaluate a request and determine the appropriate response strategy.

        Args:
            client_id: Unique client identifier
            endpoint: API endpoint being accessed
            current_rate_limit_result: Result from existing rate limiting
            metadata: Additional request metadata

        Returns:
            Tuple of (should_allow, response_strategy, enhanced_metadata)
        """
        self.metrics['total_requests'] += 1
        timestamp = time.time()
        metadata = metadata or {}

        # Analyze threat level
        threat_level = self.threat_detector.analyze_request_pattern(
            client_id, endpoint, timestamp
        )

        # Get system load
        system_metrics = await self.resource_monitor.get_metrics()
        system_load_factor = await self._calculate_system_load_factor(system_metrics)

        # Get adaptive multiplier
        adaptive_multiplier = self._get_adaptive_multiplier(threat_level, system_load_factor)

        # Determine base response from existing rate limiting
        is_allowed, base_metadata = current_rate_limit_result

        # Apply orchestration logic
        if not is_allowed:
            # Original rate limiting blocked the request
            response_strategy = await self._determine_violation_response(
                client_id, endpoint, threat_level, metadata
            )

            # Record violation
            self.threat_detector.record_violation(
                client_id, 'rate_limit_violation', threat_level
            )

            enhanced_metadata = {
                **base_metadata,
                'threat_level': threat_level.name,
                'adaptive_multiplier': adaptive_multiplier,
                'system_load_factor': system_load_factor,
                'response_strategy': response_strategy.value,
                'behavior_score': self.threat_detector.get_client_behavior_score(client_id),
                'orchestrator_processed': True
            }

            # Update metrics based on strategy
            if response_strategy == ResponseStrategy.BLOCK:
                self.metrics['blocked_requests'] += 1
            elif response_strategy == ResponseStrategy.THROTTLE:
                self.metrics['throttled_requests'] += 1
            elif response_strategy == ResponseStrategy.QUEUE:
                self.metrics['queued_requests'] += 1
            elif response_strategy == ResponseStrategy.CHALLENGE:
                self.metrics['challenged_requests'] += 1

            # Record event
            await self._record_event(RateLimitEvent(
                timestamp=timestamp,
                client_id=client_id,
                endpoint=endpoint,
                event_type='violation',
                severity=threat_level,
                metadata=enhanced_metadata
            ))

            return False, response_strategy, enhanced_metadata

        else:
            # Request is allowed, but check for proactive measures
            if threat_level in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]:
                # High threat but within limits - add monitoring
                enhanced_metadata = {
                    **base_metadata,
                    'threat_level': threat_level.name,
                    'monitoring_enabled': True,
                    'adaptive_multiplier': adaptive_multiplier,
                    'behavior_score': self.threat_detector.get_client_behavior_score(client_id),
                    'orchestrator_processed': True
                }

                await self._record_event(RateLimitEvent(
                    timestamp=timestamp,
                    client_id=client_id,
                    endpoint=endpoint,
                    event_type='warning',
                    severity=threat_level,
                    metadata=enhanced_metadata
                ))

                return True, ResponseStrategy.MONITOR, enhanced_metadata

            # Normal request processing
            enhanced_metadata = {
                **base_metadata,
                'threat_level': threat_level.name,
                'adaptive_multiplier': adaptive_multiplier,
                'orchestrator_processed': True
            }

            return True, ResponseStrategy.MONITOR, enhanced_metadata

    async def _determine_violation_response(
        self,
        client_id: str,
        endpoint: str,
        threat_level: ThreatLevel,
        metadata: Dict[str, Any]
    ) -> ResponseStrategy:
        """Determine the appropriate response strategy for a violation."""

        # Check if client should be blocked
        if self.threat_detector.should_block_client(client_id):
            return ResponseStrategy.BLOCK

        # Strategy based on threat level
        if threat_level == ThreatLevel.EMERGENCY:
            return ResponseStrategy.BLOCK
        elif threat_level == ThreatLevel.CRITICAL:
            return ResponseStrategy.CHALLENGE
        elif threat_level == ThreatLevel.HIGH:
            return ResponseStrategy.THROTTLE
        elif threat_level == ThreatLevel.MODERATE:
            return ResponseStrategy.QUEUE
        else:
            return ResponseStrategy.THROTTLE

    def _get_adaptive_multiplier(self, threat_level: ThreatLevel, system_load_factor: float) -> float:
        """Calculate adaptive multiplier based on threat level and system load."""
        base_multiplier = self.adaptive_config.base_multiplier
        threat_multiplier = self.adaptive_config.threat_multipliers.get(threat_level, 1.0)

        # Combine threat and system load factors
        adaptive_multiplier = base_multiplier * threat_multiplier * system_load_factor

        if adaptive_multiplier != base_multiplier:
            self.metrics['adaptive_adjustments'] += 1

        return max(0.1, min(2.0, adaptive_multiplier))  # Clamp between 0.1 and 2.0

    async def _calculate_system_load_factor(self, system_metrics: Dict[str, Any]) -> float:
        """Calculate system load factor for adaptive rate limiting."""
        cpu_usage = system_metrics.get('cpu_usage_percent', 0)
        memory_usage = system_metrics.get('memory_usage_percent', 0)

        # Get response time from recent requests (if available)
        recent_response_time = system_metrics.get('avg_response_time_ms', 0) / 1000.0

        thresholds = self.adaptive_config.system_load_thresholds

        # Calculate load factors
        cpu_factor = 1.0
        if cpu_usage > thresholds['cpu_high']:
            cpu_factor = max(0.5, 1.0 - ((cpu_usage - thresholds['cpu_high']) / 20.0))

        memory_factor = 1.0
        if memory_usage > thresholds['memory_high']:
            memory_factor = max(0.5, 1.0 - ((memory_usage - thresholds['memory_high']) / 15.0))

        response_time_factor = 1.0
        if recent_response_time > thresholds['response_time_high']:
            response_time_factor = max(0.3, 1.0 - ((recent_response_time - thresholds['response_time_high']) / 3.0))

        # Return the most restrictive factor
        return min(cpu_factor, memory_factor, response_time_factor)

    async def _record_event(self, event: RateLimitEvent):
        """Record a rate limiting event for monitoring."""
        self.events.append(event)

        # Log significant events
        if event.severity in [ThreatLevel.HIGH, ThreatLevel.CRITICAL, ThreatLevel.EMERGENCY]:
            logger.warning(
                f"Rate limiting event: {event.event_type} for {event.client_id} on {event.endpoint}",
                extra={
                    'client_id': event.client_id,
                    'endpoint': event.endpoint,
                    'event_type': event.event_type,
                    'severity': event.severity.name,
                    'metadata': event.metadata
                }
            )

    async def _monitoring_loop(self):
        """Background monitoring loop for threat analysis."""
        while self._running:
            try:
                await asyncio.sleep(30)  # Monitor every 30 seconds
                await self._analyze_global_patterns()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")

    async def _cleanup_loop(self):
        """Background cleanup loop for old data."""
        while self._running:
            try:
                await asyncio.sleep(300)  # Cleanup every 5 minutes
                await self._cleanup_old_data()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup loop error: {e}")

    async def _analyze_global_patterns(self):
        """Analyze global patterns for system-wide threats."""
        current_time = time.time()

        # Analyze violation rate
        recent_violations = [
            ts for ts in self.threat_detector.global_patterns['violation_rate']
            if current_time - ts < 300  # Last 5 minutes
        ]

        violation_rate = len(recent_violations) / 5.0  # Violations per minute

        # Check for DDoS patterns
        if violation_rate > 50:  # More than 50 violations per minute
            logger.warning(
                f"High violation rate detected: {violation_rate:.1f} violations/minute",
                extra={'violation_rate': violation_rate, 'total_violations': len(recent_violations)}
            )
            self.metrics['threats_detected'] += 1

        # Analyze active threats
        active_threats = len(self.threat_detector.global_patterns['active_threats'])
        if active_threats > 10:
            logger.warning(
                f"Multiple active threats detected: {active_threats} clients",
                extra={'active_threats': active_threats}
            )

    async def _cleanup_old_data(self):
        """Clean up old data to prevent memory leaks."""
        current_time = time.time()
        cutoff_time = current_time - 3600  # 1 hour ago

        # Clean up client patterns
        clients_to_remove = []
        for client_id, data in self.threat_detector.client_patterns.items():
            if data['last_violation'] and data['last_violation'] < cutoff_time:
                # Remove clients with no recent activity
                if len(data['request_history']) == 0:
                    clients_to_remove.append(client_id)

        for client_id in clients_to_remove:
            del self.threat_detector.client_patterns[client_id]

        logger.debug(f"Cleaned up {len(clients_to_remove)} inactive client records")

    async def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive orchestrator metrics."""
        system_metrics = await self.resource_monitor.get_metrics()

        return {
            **self.metrics,
            'active_clients': len(self.threat_detector.client_patterns),
            'active_threats': len(self.threat_detector.global_patterns['active_threats']),
            'events_recorded': len(self.events),
            'system_cpu_usage': system_metrics.get('cpu_usage_percent', 0),
            'system_memory_usage': system_metrics.get('memory_usage_percent', 0),
            'global_violation_count': self.threat_detector.global_patterns['total_violations']
        }

    async def get_client_status(self, client_id: str) -> Dict[str, Any]:
        """Get detailed status for a specific client."""
        client_data = self.threat_detector.client_patterns[client_id]

        return {
            'client_id': client_id,
            'threat_level': client_data['threat_level'].name,
            'behavior_score': client_data['behavior_score'],
            'violation_count': client_data['violation_count'],
            'last_violation': client_data['last_violation'],
            'request_count': len(client_data['request_history']),
            'should_block': self.threat_detector.should_block_client(client_id),
            'recent_requests': list(client_data['request_history'])[-10:]  # Last 10 requests
        }

    async def reset_client_status(self, client_id: str):
        """Reset threat status for a specific client."""
        if client_id in self.threat_detector.client_patterns:
            client_data = self.threat_detector.client_patterns[client_id]
            client_data['violation_count'] = 0
            client_data['behavior_score'] = 1.0
            client_data['threat_level'] = ThreatLevel.LOW
            client_data['request_history'].clear()

            # Remove from active threats
            self.threat_detector.global_patterns['active_threats'].discard(client_id)

            logger.info(f"Reset threat status for client {client_id}")


# Global orchestrator instance
_rate_limiting_orchestrator: Optional[RateLimitingOrchestrator] = None


async def get_rate_limiting_orchestrator(
    adaptive_config: Optional[AdaptiveConfig] = None
) -> RateLimitingOrchestrator:
    """Get or create the global rate limiting orchestrator."""
    global _rate_limiting_orchestrator

    if _rate_limiting_orchestrator is None:
        _rate_limiting_orchestrator = RateLimitingOrchestrator(adaptive_config)
        await _rate_limiting_orchestrator.start()

    return _rate_limiting_orchestrator


async def shutdown_rate_limiting_orchestrator():
    """Shutdown the global rate limiting orchestrator."""
    global _rate_limiting_orchestrator

    if _rate_limiting_orchestrator:
        await _rate_limiting_orchestrator.stop()
        _rate_limiting_orchestrator = None
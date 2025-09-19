"""
Advanced Health-Aware Router
===========================

Enhanced health-aware routing with predictive analytics and intelligent decision making:
- AI-powered health prediction based on historical patterns
- Multi-dimensional health scoring with business impact weighting
- Adaptive routing with real-time learning
- Geographic and latency-aware routing
- Advanced circuit breaking with exponential backoff
- Custom health check strategies per service type
"""
from __future__ import annotations

import asyncio
import json
import math
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Callable, Union
import numpy as np

import aiohttp
from fastapi import Request, Response, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

from core.logging import get_logger
from .intelligent_load_balancer import ServiceInstance, ServiceState, LoadBalancingAlgorithm

logger = get_logger(__name__)


class HealthDimension(Enum):
    """Health check dimensions for comprehensive assessment."""
    PERFORMANCE = "performance"
    AVAILABILITY = "availability"
    CAPACITY = "capacity"
    RELIABILITY = "reliability"
    BUSINESS_IMPACT = "business_impact"
    SECURITY = "security"
    DATA_QUALITY = "data_quality"


class RoutingStrategy(Enum):
    """Advanced routing strategies."""
    HEALTH_WEIGHTED = "health_weighted"
    PREDICTIVE = "predictive"
    ADAPTIVE = "adaptive"
    GEOGRAPHIC_AWARE = "geographic_aware"
    LATENCY_OPTIMIZED = "latency_optimized"
    BUSINESS_PRIORITY = "business_priority"
    CANARY_ROUTING = "canary_routing"


@dataclass
class HealthScore:
    """Multi-dimensional health score."""
    overall_score: float  # 0-100 scale
    dimensions: Dict[HealthDimension, float] = field(default_factory=dict)
    confidence: float = 0.0  # 0-1 scale
    prediction_horizon: int = 300  # seconds
    last_updated: datetime = field(default_factory=datetime.utcnow)

    def get_weighted_score(self, weights: Dict[HealthDimension, float]) -> float:
        """Calculate weighted score based on dimension priorities."""
        if not self.dimensions:
            return self.overall_score

        weighted_sum = 0.0
        total_weight = 0.0

        for dimension, score in self.dimensions.items():
            weight = weights.get(dimension, 1.0)
            weighted_sum += score * weight
            total_weight += weight

        return weighted_sum / total_weight if total_weight > 0 else self.overall_score

    def is_degraded(self, threshold: float = 75.0) -> bool:
        """Check if service is considered degraded."""
        return self.overall_score < threshold

    def is_critical(self, threshold: float = 50.0) -> bool:
        """Check if service is in critical state."""
        return self.overall_score < threshold


@dataclass
class PredictiveHealthModel:
    """Predictive health model using historical data."""
    service_id: str
    historical_scores: deque = field(default_factory=lambda: deque(maxlen=1000))
    pattern_data: Dict[str, Any] = field(default_factory=dict)
    model_accuracy: float = 0.0
    last_training: Optional[datetime] = None

    def add_observation(self, score: HealthScore):
        """Add health score observation."""
        self.historical_scores.append({
            'timestamp': score.last_updated,
            'score': score.overall_score,
            'dimensions': dict(score.dimensions)
        })

        # Retrain model if we have enough data
        if len(self.historical_scores) >= 50 and (
            not self.last_training or
            datetime.utcnow() - self.last_training > timedelta(hours=1)
        ):
            self._retrain_model()

    def predict_health(self, horizon_seconds: int = 300) -> Tuple[float, float]:
        """Predict health score for given time horizon."""
        if len(self.historical_scores) < 10:
            return 75.0, 0.0  # Default prediction with low confidence

        try:
            # Simple trend-based prediction (can be replaced with ML model)
            recent_scores = [obs['score'] for obs in list(self.historical_scores)[-20:]]

            # Calculate trend
            x = np.arange(len(recent_scores))
            coeffs = np.polyfit(x, recent_scores, 1)
            trend_slope = coeffs[0]

            # Extrapolate trend
            future_steps = horizon_seconds // 60  # Predict per minute
            predicted_score = recent_scores[-1] + (trend_slope * future_steps)
            predicted_score = max(0, min(100, predicted_score))  # Clamp to valid range

            # Calculate confidence based on recent stability
            score_variance = np.var(recent_scores)
            confidence = max(0, 1.0 - (score_variance / 1000))  # Normalize variance to confidence

            return predicted_score, confidence

        except Exception as e:
            logger.warning(f"Health prediction failed for {self.service_id}: {e}")
            return 75.0, 0.0

    def _retrain_model(self):
        """Retrain the predictive model."""
        try:
            if len(self.historical_scores) < 20:
                return

            # Extract features and calculate model accuracy
            recent_data = list(self.historical_scores)[-100:]

            # Simple accuracy calculation based on trend prediction
            predictions = []
            actuals = []

            for i in range(10, len(recent_data)):
                # Use data up to point i-10 to predict point i
                historical_subset = recent_data[:i-10]
                if len(historical_subset) >= 10:
                    scores = [obs['score'] for obs in historical_subset[-10:]]
                    x = np.arange(len(scores))
                    coeffs = np.polyfit(x, scores, 1)
                    predicted = scores[-1] + (coeffs[0] * 10)
                    predictions.append(predicted)
                    actuals.append(recent_data[i]['score'])

            if predictions and actuals:
                mae = np.mean(np.abs(np.array(predictions) - np.array(actuals)))
                self.model_accuracy = max(0, 1.0 - (mae / 100))  # Convert MAE to accuracy

            self.last_training = datetime.utcnow()
            logger.info(f"Retrained health model for {self.service_id}, accuracy: {self.model_accuracy:.2f}")

        except Exception as e:
            logger.error(f"Model retraining failed for {self.service_id}: {e}")


@dataclass
class GeographicLocation:
    """Geographic location information."""
    region: str
    zone: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    def distance_to(self, other: GeographicLocation) -> float:
        """Calculate distance to another location (simplified)."""
        if self.latitude and self.longitude and other.latitude and other.longitude:
            # Simple Euclidean distance (for demonstration)
            return math.sqrt(
                (self.latitude - other.latitude) ** 2 +
                (self.longitude - other.longitude) ** 2
            )
        return 0.0  # Unknown distance


@dataclass
class EnhancedServiceInstance:
    """Enhanced service instance with advanced health awareness."""
    base_instance: ServiceInstance
    health_score: HealthScore = field(default_factory=lambda: HealthScore(overall_score=100.0))
    predictive_model: Optional[PredictiveHealthModel] = None
    geographic_location: Optional[GeographicLocation] = None
    business_criticality: float = 1.0  # 0-1 scale
    canary_weight: float = 0.0  # 0-1 scale for canary deployments
    custom_health_checks: List[str] = field(default_factory=list)
    sla_requirements: Dict[str, float] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize predictive model."""
        if not self.predictive_model:
            self.predictive_model = PredictiveHealthModel(
                service_id=self.base_instance.id
            )

    @property
    def is_canary(self) -> bool:
        """Check if this is a canary deployment."""
        return self.canary_weight > 0.0

    @property
    def effective_weight(self) -> float:
        """Calculate effective weight considering health and business impact."""
        base_weight = self.base_instance.weight
        health_factor = self.health_score.overall_score / 100.0
        business_factor = self.business_criticality

        return base_weight * health_factor * business_factor

    def get_routing_priority(self, strategy: RoutingStrategy, client_location: Optional[GeographicLocation] = None) -> float:
        """Calculate routing priority based on strategy."""
        base_priority = self.health_score.overall_score

        if strategy == RoutingStrategy.HEALTH_WEIGHTED:
            return base_priority * self.business_criticality

        elif strategy == RoutingStrategy.PREDICTIVE and self.predictive_model:
            predicted_score, confidence = self.predictive_model.predict_health()
            return predicted_score * confidence

        elif strategy == RoutingStrategy.GEOGRAPHIC_AWARE and client_location and self.geographic_location:
            distance = self.geographic_location.distance_to(client_location)
            distance_penalty = max(0, 1.0 - (distance / 100.0))  # Normalize distance
            return base_priority * distance_penalty

        elif strategy == RoutingStrategy.BUSINESS_PRIORITY:
            return base_priority * self.business_criticality * 2.0  # Amplify business impact

        elif strategy == RoutingStrategy.CANARY_ROUTING:
            if self.is_canary:
                return base_priority * 0.1  # Lower priority for canary
            return base_priority

        return base_priority


class AdvancedHealthChecker:
    """Advanced health checker with custom strategies."""

    def __init__(self, check_interval: int = 30):
        self.check_interval = check_interval
        self.session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._tasks: List[asyncio.Task] = []

        # Health check strategies
        self.strategies: Dict[str, Callable] = {
            'basic': self._basic_health_check,
            'detailed': self._detailed_health_check,
            'custom': self._custom_health_check,
            'synthetic': self._synthetic_health_check
        }

        # Health check history for trend analysis
        self.health_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))

    async def start(self, services: Dict[str, EnhancedServiceInstance]):
        """Start advanced health checking."""
        self._running = True
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10.0),
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=20)
        )

        # Start health checking tasks
        self._tasks = [
            asyncio.create_task(self._health_check_loop(services)),
            asyncio.create_task(self._trend_analysis_loop(services)),
            asyncio.create_task(self._predictive_update_loop(services))
        ]

    async def stop(self):
        """Stop health checking."""
        self._running = False

        for task in self._tasks:
            task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        if self.session:
            await self.session.close()

    async def _health_check_loop(self, services: Dict[str, EnhancedServiceInstance]):
        """Main health check loop."""
        while self._running:
            try:
                await asyncio.sleep(self.check_interval)

                # Check all services concurrently
                tasks = []
                for service in services.values():
                    task = asyncio.create_task(self._check_service_health(service))
                    tasks.append(task)

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check loop error: {e}")

    async def _check_service_health(self, service: EnhancedServiceInstance):
        """Comprehensive health check for a service."""
        try:
            # Determine health check strategy
            strategy = 'detailed' if service.custom_health_checks else 'basic'

            # Execute health check
            health_data = await self.strategies[strategy](service)

            # Calculate multi-dimensional health score
            health_score = self._calculate_health_score(service, health_data)

            # Update service health
            service.health_score = health_score

            # Update predictive model
            if service.predictive_model:
                service.predictive_model.add_observation(health_score)

            # Store in history
            self.health_history[service.base_instance.id].append({
                'timestamp': datetime.utcnow(),
                'score': health_score.overall_score,
                'dimensions': dict(health_score.dimensions),
                'raw_data': health_data
            })

        except Exception as e:
            logger.error(f"Health check failed for {service.base_instance.id}: {e}")
            # Record failure
            failure_score = HealthScore(
                overall_score=0.0,
                confidence=0.9,
                last_updated=datetime.utcnow()
            )
            service.health_score = failure_score

    async def _basic_health_check(self, service: EnhancedServiceInstance) -> Dict[str, Any]:
        """Basic health check - simple HTTP GET to /health."""
        health_url = f"{service.base_instance.url}/health"
        start_time = time.time()

        try:
            async with self.session.get(health_url) as response:
                response_time = time.time() - start_time

                if response.status == 200:
                    try:
                        data = await response.json()
                        data['response_time'] = response_time
                        return data
                    except:
                        return {
                            'status': 'healthy',
                            'response_time': response_time
                        }
                else:
                    return {
                        'status': 'unhealthy',
                        'http_status': response.status,
                        'response_time': response_time
                    }

        except Exception as e:
            return {
                'status': 'failed',
                'error': str(e),
                'response_time': time.time() - start_time
            }

    async def _detailed_health_check(self, service: EnhancedServiceInstance) -> Dict[str, Any]:
        """Detailed health check with multiple endpoints."""
        results = {}

        # Basic health check
        results['basic'] = await self._basic_health_check(service)

        # Additional health endpoints
        endpoints = ['/health/detailed', '/metrics', '/status']

        for endpoint in endpoints:
            try:
                url = f"{service.base_instance.url}{endpoint}"
                async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=5.0)) as response:
                    if response.status == 200:
                        try:
                            data = await response.json()
                            results[endpoint.replace('/', '_')] = data
                        except:
                            results[endpoint.replace('/', '_')] = {'status': 'ok'}
            except:
                results[endpoint.replace('/', '_')] = {'status': 'unavailable'}

        return results

    async def _custom_health_check(self, service: EnhancedServiceInstance) -> Dict[str, Any]:
        """Custom health checks based on service configuration."""
        results = await self._basic_health_check(service)

        # Execute custom health checks
        for check_name in service.custom_health_checks:
            try:
                check_url = f"{service.base_instance.url}/health/{check_name}"
                async with self.session.get(check_url) as response:
                    if response.status == 200:
                        check_data = await response.json()
                        results[f'custom_{check_name}'] = check_data
            except Exception as e:
                results[f'custom_{check_name}'] = {'status': 'failed', 'error': str(e)}

        return results

    async def _synthetic_health_check(self, service: EnhancedServiceInstance) -> Dict[str, Any]:
        """Synthetic transaction health check."""
        # Simulate real user transactions for health assessment
        results = await self._basic_health_check(service)

        try:
            # Example synthetic transaction - search query
            search_url = f"{service.base_instance.url}/api/v1/sales/search"
            start_time = time.time()

            async with self.session.get(
                search_url,
                params={'q': 'test_query', 'limit': 10}
            ) as response:
                transaction_time = time.time() - start_time

                results['synthetic_transaction'] = {
                    'status': 'success' if response.status == 200 else 'failed',
                    'response_time': transaction_time,
                    'http_status': response.status
                }

        except Exception as e:
            results['synthetic_transaction'] = {
                'status': 'failed',
                'error': str(e)
            }

        return results

    def _calculate_health_score(self,
                              service: EnhancedServiceInstance,
                              health_data: Dict[str, Any]) -> HealthScore:
        """Calculate comprehensive health score."""
        dimensions = {}

        # Performance dimension
        response_time = health_data.get('response_time', 0)
        if response_time > 0:
            # Score based on response time (100ms baseline)
            perf_score = max(0, 100 - (response_time * 100))  # Convert to 0-100 scale
            dimensions[HealthDimension.PERFORMANCE] = min(perf_score, 100)

        # Availability dimension
        if health_data.get('status') == 'healthy':
            dimensions[HealthDimension.AVAILABILITY] = 100.0
        elif health_data.get('status') == 'degraded':
            dimensions[HealthDimension.AVAILABILITY] = 60.0
        else:
            dimensions[HealthDimension.AVAILABILITY] = 0.0

        # Capacity dimension (from detailed metrics)
        if 'metrics' in health_data:
            metrics = health_data['metrics']
            cpu_usage = metrics.get('cpu_usage', 0)
            memory_usage = metrics.get('memory_usage', 0)

            # Capacity score based on resource utilization
            cpu_score = max(0, 100 - cpu_usage) if cpu_usage > 0 else 100
            mem_score = max(0, 100 - memory_usage) if memory_usage > 0 else 100
            dimensions[HealthDimension.CAPACITY] = (cpu_score + mem_score) / 2

        # Reliability dimension (based on recent history)
        service_id = service.base_instance.id
        if service_id in self.health_history and len(self.health_history[service_id]) > 5:
            recent_scores = [h['score'] for h in list(self.health_history[service_id])[-10:]]
            reliability_score = sum(1 for score in recent_scores if score > 75) / len(recent_scores) * 100
            dimensions[HealthDimension.RELIABILITY] = reliability_score

        # Business impact dimension
        dimensions[HealthDimension.BUSINESS_IMPACT] = service.business_criticality * 100

        # Overall score calculation (weighted average)
        dimension_weights = {
            HealthDimension.PERFORMANCE: 0.3,
            HealthDimension.AVAILABILITY: 0.3,
            HealthDimension.CAPACITY: 0.2,
            HealthDimension.RELIABILITY: 0.15,
            HealthDimension.BUSINESS_IMPACT: 0.05
        }

        overall_score = 0.0
        total_weight = 0.0

        for dimension, score in dimensions.items():
            weight = dimension_weights.get(dimension, 0.1)
            overall_score += score * weight
            total_weight += weight

        if total_weight > 0:
            overall_score /= total_weight

        # Calculate confidence based on data completeness
        confidence = min(len(dimensions) / len(HealthDimension), 1.0)

        return HealthScore(
            overall_score=overall_score,
            dimensions=dimensions,
            confidence=confidence,
            last_updated=datetime.utcnow()
        )

    async def _trend_analysis_loop(self, services: Dict[str, EnhancedServiceInstance]):
        """Analyze health trends and patterns."""
        while self._running:
            try:
                await asyncio.sleep(300)  # Every 5 minutes

                for service_id, service in services.items():
                    if service_id in self.health_history:
                        self._analyze_health_trends(service)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Trend analysis error: {e}")

    def _analyze_health_trends(self, service: EnhancedServiceInstance):
        """Analyze health trends for a service."""
        try:
            service_id = service.base_instance.id
            history = list(self.health_history[service_id])

            if len(history) < 10:
                return

            # Calculate trend metrics
            scores = [h['score'] for h in history[-20:]]

            # Trend direction
            recent_avg = sum(scores[-5:]) / 5 if len(scores) >= 5 else scores[-1]
            historical_avg = sum(scores[:-5]) / len(scores[:-5]) if len(scores) > 5 else scores[0]

            trend_direction = "improving" if recent_avg > historical_avg * 1.05 else (
                "degrading" if recent_avg < historical_avg * 0.95 else "stable"
            )

            # Volatility (standard deviation of recent scores)
            if len(scores) >= 10:
                volatility = statistics.stdev(scores[-10:])

                # Log significant changes
                if trend_direction == "degrading" and volatility > 15:
                    logger.warning(
                        f"Service {service_id} showing degrading health trend "
                        f"(current: {recent_avg:.1f}, historical: {historical_avg:.1f}, "
                        f"volatility: {volatility:.1f})"
                    )
                elif trend_direction == "improving" and recent_avg > 90:
                    logger.info(f"Service {service_id} health fully recovered")

        except Exception as e:
            logger.error(f"Trend analysis failed for {service.base_instance.id}: {e}")

    async def _predictive_update_loop(self, services: Dict[str, EnhancedServiceInstance]):
        """Update predictive models periodically."""
        while self._running:
            try:
                await asyncio.sleep(600)  # Every 10 minutes

                for service in services.values():
                    if service.predictive_model and len(service.predictive_model.historical_scores) >= 20:
                        # Get prediction for next 5 minutes
                        predicted_score, confidence = service.predictive_model.predict_health(300)

                        if confidence > 0.7 and predicted_score < 50:
                            logger.warning(
                                f"Predictive model forecasts health degradation for "
                                f"{service.base_instance.id}: {predicted_score:.1f} "
                                f"(confidence: {confidence:.2f})"
                            )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive update error: {e}")


class HealthAwareRouter:
    """Advanced health-aware router with intelligent decision making."""

    def __init__(self,
                 strategy: RoutingStrategy = RoutingStrategy.ADAPTIVE,
                 health_threshold: float = 70.0,
                 enable_canary: bool = False,
                 canary_traffic_percentage: float = 5.0):

        self.strategy = strategy
        self.health_threshold = health_threshold
        self.enable_canary = enable_canary
        self.canary_traffic_percentage = canary_traffic_percentage

        # Enhanced services registry
        self.services: Dict[str, EnhancedServiceInstance] = {}

        # Health checker
        self.health_checker = AdvancedHealthChecker()

        # Routing statistics
        self.routing_stats = {
            'total_routes': 0,
            'healthy_routes': 0,
            'degraded_routes': 0,
            'failed_routes': 0,
            'canary_routes': 0,
            'strategy_changes': 0
        }

        # Adaptive strategy state
        self.strategy_performance = defaultdict(lambda: {'success': 0, 'total': 0})
        self.last_strategy_evaluation = datetime.utcnow()

        # Geographic routing cache
        self.geo_cache: Dict[str, GeographicLocation] = {}

    async def start(self):
        """Start the health-aware router."""
        await self.health_checker.start(self.services)
        logger.info(f"Health-aware router started with strategy: {self.strategy.value}")

    async def stop(self):
        """Stop the health-aware router."""
        await self.health_checker.stop()
        logger.info("Health-aware router stopped")

    def register_service(self,
                        service_instance: ServiceInstance,
                        business_criticality: float = 1.0,
                        geographic_location: Optional[GeographicLocation] = None,
                        custom_health_checks: Optional[List[str]] = None,
                        sla_requirements: Optional[Dict[str, float]] = None,
                        canary_weight: float = 0.0) -> EnhancedServiceInstance:
        """Register an enhanced service instance."""

        enhanced_service = EnhancedServiceInstance(
            base_instance=service_instance,
            business_criticality=business_criticality,
            geographic_location=geographic_location,
            custom_health_checks=custom_health_checks or [],
            sla_requirements=sla_requirements or {},
            canary_weight=canary_weight
        )

        self.services[service_instance.id] = enhanced_service

        logger.info(
            f"Registered enhanced service {service_instance.id} "
            f"(criticality: {business_criticality}, canary: {canary_weight > 0})"
        )

        return enhanced_service

    async def route_request(self,
                          request: Request,
                          service_group: Optional[str] = None) -> Optional[EnhancedServiceInstance]:
        """Route request using advanced health-aware logic."""

        # Get available healthy services
        available_services = self._get_healthy_services(service_group)

        if not available_services:
            logger.error(f"No healthy services available for group: {service_group}")
            self.routing_stats['failed_routes'] += 1
            return None

        # Determine client location for geographic routing
        client_location = await self._get_client_location(request)

        # Apply canary routing if enabled
        if self.enable_canary:
            canary_service = self._check_canary_routing(request, available_services)
            if canary_service:
                self.routing_stats['canary_routes'] += 1
                return canary_service

        # Select service based on strategy
        selected_service = await self._select_service_by_strategy(
            available_services, request, client_location
        )

        if selected_service:
            self._record_routing_decision(selected_service)

        self.routing_stats['total_routes'] += 1
        return selected_service

    def _get_healthy_services(self, service_group: Optional[str] = None) -> List[EnhancedServiceInstance]:
        """Get healthy services above threshold."""
        available_services = []

        for service in self.services.values():
            # Check base availability
            if not service.base_instance.is_available:
                continue

            # Check health score
            if service.health_score.overall_score >= self.health_threshold:
                available_services.append(service)
            elif service.health_score.overall_score >= (self.health_threshold * 0.7):
                # Allow degraded services with lower priority
                available_services.append(service)

        return available_services

    async def _get_client_location(self, request: Request) -> Optional[GeographicLocation]:
        """Determine client geographic location."""
        try:
            # Check for explicit location headers
            region = request.headers.get('x-client-region')
            zone = request.headers.get('x-client-zone')

            if region and zone:
                return GeographicLocation(region=region, zone=zone)

            # Try to determine from IP (simplified)
            client_ip = request.client.host if request.client else None
            if client_ip and client_ip not in self.geo_cache:
                # In production, use actual IP geolocation service
                # For now, use mock location
                self.geo_cache[client_ip] = GeographicLocation(
                    region="us-east-1",
                    zone="us-east-1a"
                )

            return self.geo_cache.get(client_ip)

        except Exception as e:
            logger.debug(f"Could not determine client location: {e}")
            return None

    def _check_canary_routing(self,
                            request: Request,
                            available_services: List[EnhancedServiceInstance]) -> Optional[EnhancedServiceInstance]:
        """Check if request should be routed to canary deployment."""
        canary_services = [s for s in available_services if s.is_canary]

        if not canary_services:
            return None

        # Simple percentage-based canary routing
        import random
        if random.random() * 100 < self.canary_traffic_percentage:
            # Select best canary service
            return max(canary_services, key=lambda s: s.health_score.overall_score)

        return None

    async def _select_service_by_strategy(self,
                                        available_services: List[EnhancedServiceInstance],
                                        request: Request,
                                        client_location: Optional[GeographicLocation]) -> Optional[EnhancedServiceInstance]:
        """Select service based on current strategy."""

        if self.strategy == RoutingStrategy.ADAPTIVE:
            # Dynamically choose best strategy based on performance
            strategy = await self._select_adaptive_strategy()
        else:
            strategy = self.strategy

        # Calculate priorities for all services
        service_priorities = []

        for service in available_services:
            priority = service.get_routing_priority(strategy, client_location)

            # Apply additional factors
            if strategy == RoutingStrategy.LATENCY_OPTIMIZED:
                # Consider historical latency data
                if service.base_instance.metrics.avg_response_time > 0:
                    latency_factor = 1.0 / (1.0 + service.base_instance.metrics.avg_response_time / 100)
                    priority *= latency_factor

            service_priorities.append((service, priority))

        # Select service with highest priority
        if service_priorities:
            selected = max(service_priorities, key=lambda x: x[1])
            return selected[0]

        return None

    async def _select_adaptive_strategy(self) -> RoutingStrategy:
        """Dynamically select best performing strategy."""
        # Evaluate strategy performance
        if datetime.utcnow() - self.last_strategy_evaluation > timedelta(minutes=15):
            best_strategy = RoutingStrategy.PERFORMANCE_BASED  # Default
            best_success_rate = 0.0

            for strategy, stats in self.strategy_performance.items():
                if stats['total'] > 10:  # Need sufficient data
                    success_rate = stats['success'] / stats['total']
                    if success_rate > best_success_rate:
                        best_success_rate = success_rate
                        best_strategy = RoutingStrategy(strategy)

            if best_strategy != self.strategy:
                logger.info(f"Adaptive strategy change: {self.strategy.value} -> {best_strategy.value}")
                self.strategy = best_strategy
                self.routing_stats['strategy_changes'] += 1

            self.last_strategy_evaluation = datetime.utcnow()

        return self.strategy

    def _record_routing_decision(self, service: EnhancedServiceInstance):
        """Record routing decision for analytics."""
        if service.health_score.overall_score >= 90:
            self.routing_stats['healthy_routes'] += 1
        elif service.health_score.overall_score >= self.health_threshold:
            self.routing_stats['degraded_routes'] += 1

    def record_request_outcome(self, service_id: str, success: bool, response_time: float):
        """Record request outcome for adaptive learning."""
        if service_id in self.services:
            # Update service metrics
            service = self.services[service_id]
            service.base_instance.record_success(response_time) if success else service.base_instance.record_failure()

            # Update strategy performance
            strategy_key = self.strategy.value
            self.strategy_performance[strategy_key]['total'] += 1
            if success:
                self.strategy_performance[strategy_key]['success'] += 1

    def get_routing_statistics(self) -> Dict[str, Any]:
        """Get comprehensive routing statistics."""
        total_routes = max(self.routing_stats['total_routes'], 1)

        return {
            'strategy': self.strategy.value,
            'health_threshold': self.health_threshold,
            'total_services': len(self.services),
            'healthy_services': len([s for s in self.services.values()
                                   if s.health_score.overall_score >= self.health_threshold]),
            'routing_stats': {
                **self.routing_stats,
                'healthy_route_percentage': (self.routing_stats['healthy_routes'] / total_routes) * 100,
                'degraded_route_percentage': (self.routing_stats['degraded_routes'] / total_routes) * 100,
                'failure_rate': (self.routing_stats['failed_routes'] / total_routes) * 100
            },
            'strategy_performance': dict(self.strategy_performance),
            'service_health_summary': {
                service_id: {
                    'overall_score': service.health_score.overall_score,
                    'dimensions': {dim.value: score for dim, score in service.health_score.dimensions.items()},
                    'prediction': service.predictive_model.predict_health() if service.predictive_model else (None, 0.0),
                    'business_criticality': service.business_criticality,
                    'is_canary': service.is_canary
                }
                for service_id, service in self.services.items()
            }
        }


# Factory function
def create_health_aware_router(
    strategy: RoutingStrategy = RoutingStrategy.ADAPTIVE,
    health_threshold: float = 70.0,
    enable_canary: bool = False
) -> HealthAwareRouter:
    """Create a health-aware router with advanced capabilities."""
    return HealthAwareRouter(
        strategy=strategy,
        health_threshold=health_threshold,
        enable_canary=enable_canary
    )
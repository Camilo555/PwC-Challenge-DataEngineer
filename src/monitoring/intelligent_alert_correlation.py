"""
Intelligent Alert Correlation and Noise Reduction System
Advanced alert management with ML-powered correlation, noise reduction,
and intelligent escalation for enterprise monitoring.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
import uuid
import re

import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from core.logging import get_logger

logger = get_logger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4
    EMERGENCY = 5


class AlertStatus(Enum):
    """Alert lifecycle status."""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged" 
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"
    CORRELATED = "correlated"
    ESCALATED = "escalated"


class CorrelationType(Enum):
    """Types of alert correlations."""
    DUPLICATE = "duplicate"
    ROOT_CAUSE = "root_cause"
    CASCADING = "cascading"
    TEMPORAL = "temporal"
    SERVICE_RELATED = "service_related"
    INFRASTRUCTURE = "infrastructure"
    BUSINESS_IMPACT = "business_impact"


@dataclass
class Alert:
    """Individual alert with metadata and correlation info."""
    id: str
    title: str
    description: str
    severity: AlertSeverity
    status: AlertStatus
    timestamp: datetime
    
    # Classification
    component: str
    service: str
    resource_type: str
    
    # Context
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)
    
    # Business context
    business_impact: Optional[str] = None
    business_value: Optional[str] = None
    escalation_level: Optional[str] = None
    
    # Correlation metadata
    correlation_id: Optional[str] = None
    parent_alert_id: Optional[str] = None
    child_alert_ids: Set[str] = field(default_factory=set)
    correlation_type: Optional[CorrelationType] = None
    correlation_confidence: float = 0.0
    
    # Lifecycle tracking
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    escalated_at: Optional[datetime] = None
    suppressed_until: Optional[datetime] = None
    
    # Noise reduction
    occurrence_count: int = 1
    first_occurrence: datetime = field(default_factory=datetime.utcnow)
    last_occurrence: datetime = field(default_factory=datetime.utcnow)
    noise_score: float = 0.0
    
    # Context similarity vectors (for ML correlation)
    text_vector: Optional[np.ndarray] = None
    feature_vector: Optional[np.ndarray] = None


@dataclass
class AlertCorrelation:
    """Alert correlation relationship."""
    correlation_id: str
    primary_alert_id: str
    related_alert_ids: Set[str]
    correlation_type: CorrelationType
    confidence_score: float
    created_at: datetime
    business_impact_score: float
    
    # Root cause analysis
    root_cause_hypothesis: Optional[str] = None
    recommended_actions: List[str] = field(default_factory=list)
    
    # Temporal analysis
    time_window_ms: int = 0
    sequence_pattern: List[str] = field(default_factory=list)


@dataclass
class NoisePattern:
    """Noise pattern definition for alert suppression."""
    pattern_id: str
    pattern_name: str
    regex_patterns: List[str]
    component_filters: List[str]
    time_window_minutes: int
    max_occurrences: int
    confidence_threshold: float
    
    # Learning from historical data
    false_positive_rate: float = 0.0
    true_positive_rate: float = 1.0
    last_updated: datetime = field(default_factory=datetime.utcnow)


class IntelligentAlertCorrelator:
    """Advanced alert correlation and noise reduction system."""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        
        # Alert storage and tracking
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: deque[Alert] = deque(maxlen=50000)
        self.correlations: Dict[str, AlertCorrelation] = {}
        self.correlation_history: deque[AlertCorrelation] = deque(maxlen=10000)
        
        # Noise reduction
        self.noise_patterns: Dict[str, NoisePattern] = {}
        self.suppressed_alerts: Dict[str, Set[str]] = defaultdict(set)  # pattern_id -> alert_ids
        
        # ML models and vectorizers
        self.text_vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words='english',
            ngram_range=(1, 2)
        )
        self.is_fitted = False
        
        # Correlation configuration
        self.correlation_config = self._initialize_correlation_config()
        self.noise_config = self._initialize_noise_patterns()
        
        # Performance tracking
        self.processing_stats = {
            'alerts_processed': 0,
            'correlations_created': 0,
            'alerts_suppressed': 0,
            'false_positive_rate': 0.0,
            'processing_time_ms': []
        }
        
    def _initialize_correlation_config(self) -> Dict[str, Any]:
        """Initialize correlation configuration parameters."""
        return {
            'temporal_window_seconds': 300,    # 5 minutes for temporal correlation
            'similarity_threshold': 0.7,       # Cosine similarity threshold
            'cluster_eps': 0.3,                # DBSCAN epsilon for clustering
            'cluster_min_samples': 2,          # Minimum samples for cluster
            'business_impact_weight': 2.0,     # Weight for business impact correlation
            'cascade_detection_window': 600,   # 10 minutes for cascade detection
            'duplicate_threshold': 0.9,        # Very high similarity for duplicates
            'confidence_threshold': 0.6,       # Minimum confidence for correlation
            'max_correlation_depth': 3,        # Maximum levels in correlation hierarchy
        }
    
    def _initialize_noise_patterns(self) -> Dict[str, NoisePattern]:
        """Initialize common noise patterns for suppression."""
        patterns = {}
        
        # High-frequency low-impact patterns
        patterns['disk_space_flapping'] = NoisePattern(
            pattern_id='disk_space_flapping',
            pattern_name='Disk Space Flapping',
            regex_patterns=[
                r'.*disk.*space.*(warning|elevated).*',
                r'.*disk.*usage.*80.*percent.*'
            ],
            component_filters=['infrastructure', 'disk'],
            time_window_minutes=15,
            max_occurrences=5,
            confidence_threshold=0.8
        )
        
        patterns['memory_usage_oscillation'] = NoisePattern(
            pattern_id='memory_usage_oscillation',
            pattern_name='Memory Usage Oscillation',
            regex_patterns=[
                r'.*memory.*usage.*(warning|elevated).*',
                r'.*memory.*usage.*80.*percent.*'
            ],
            component_filters=['infrastructure', 'memory'],
            time_window_minutes=10,
            max_occurrences=4,
            confidence_threshold=0.75
        )
        
        patterns['network_error_bursts'] = NoisePattern(
            pattern_id='network_error_bursts',
            pattern_name='Network Error Bursts',
            regex_patterns=[
                r'.*network.*error.*rate.*',
                r'.*packet.*drop.*'
            ],
            component_filters=['infrastructure', 'network'],
            time_window_minutes=5,
            max_occurrences=3,
            confidence_threshold=0.85
        )
        
        patterns['api_response_time_spikes'] = NoisePattern(
            pattern_id='api_response_time_spikes',
            pattern_name='API Response Time Spikes',
            regex_patterns=[
                r'.*API.*response.*time.*(elevated|high).*',
                r'.*response.*time.*approaching.*'
            ],
            component_filters=['api-performance'],
            time_window_minutes=10,
            max_occurrences=3,
            confidence_threshold=0.7
        )
        
        patterns['cache_hit_rate_fluctuation'] = NoisePattern(
            pattern_id='cache_hit_rate_fluctuation',
            pattern_name='Cache Hit Rate Fluctuation',
            regex_patterns=[
                r'.*cache.*hit.*rate.*below.*95.*',
                r'.*cache.*efficiency.*degraded.*'
            ],
            component_filters=['cache-performance'],
            time_window_minutes=20,
            max_occurrences=4,
            confidence_threshold=0.8
        )
        
        return patterns
    
    async def process_alert(self, alert_data: Dict[str, Any]) -> Optional[Alert]:
        """Process incoming alert with correlation and noise reduction."""
        
        start_time = time.time()
        
        try:
            # Create alert object
            alert = self._create_alert_from_data(alert_data)
            
            # Check for noise patterns first
            if await self._is_noise_alert(alert):
                self._suppress_alert(alert)
                self.processing_stats['alerts_suppressed'] += 1
                return None
            
            # Add to active alerts
            self.active_alerts[alert.id] = alert
            self.alert_history.append(alert)
            
            # Perform correlation analysis
            correlations = await self._correlate_alert(alert)
            
            # Apply business impact scoring
            await self._calculate_business_impact_score(alert, correlations)
            
            # Determine escalation if needed
            await self._evaluate_escalation(alert, correlations)
            
            self.processing_stats['alerts_processed'] += 1
            processing_time = (time.time() - start_time) * 1000
            self.processing_stats['processing_time_ms'].append(processing_time)
            
            self.logger.info(f"Processed alert {alert.id} in {processing_time:.1f}ms")
            return alert
            
        except Exception as e:
            self.logger.error(f"Error processing alert: {e}")
            return None
    
    def _create_alert_from_data(self, alert_data: Dict[str, Any]) -> Alert:
        """Create Alert object from raw alert data."""
        
        # Parse severity
        severity_map = {
            'info': AlertSeverity.INFO,
            'warning': AlertSeverity.WARNING,
            'error': AlertSeverity.ERROR,
            'critical': AlertSeverity.CRITICAL,
            'emergency': AlertSeverity.EMERGENCY
        }
        
        severity_str = alert_data.get('severity', 'info').lower()
        severity = severity_map.get(severity_str, AlertSeverity.INFO)
        
        # Extract timestamp
        timestamp = datetime.utcnow()
        if 'timestamp' in alert_data:
            if isinstance(alert_data['timestamp'], str):
                timestamp = datetime.fromisoformat(alert_data['timestamp'].replace('Z', '+00:00'))
            elif isinstance(alert_data['timestamp'], (int, float)):
                timestamp = datetime.fromtimestamp(alert_data['timestamp'])
        
        alert = Alert(
            id=alert_data.get('id', str(uuid.uuid4())),
            title=alert_data.get('title', alert_data.get('summary', 'Unknown Alert')),
            description=alert_data.get('description', alert_data.get('message', '')),
            severity=severity,
            status=AlertStatus.ACTIVE,
            timestamp=timestamp,
            component=alert_data.get('component', 'unknown'),
            service=alert_data.get('service', 'unknown'),
            resource_type=alert_data.get('resource_type', 'unknown'),
            labels=alert_data.get('labels', {}),
            annotations=alert_data.get('annotations', {}),
            metrics=alert_data.get('metrics', {}),
            business_impact=alert_data.get('business_impact'),
            business_value=alert_data.get('business_value'),
            escalation_level=alert_data.get('escalation_level')
        )
        
        return alert
    
    async def _is_noise_alert(self, alert: Alert) -> bool:
        """Check if alert matches noise patterns and should be suppressed."""
        
        for pattern_id, pattern in self.noise_patterns.items():
            if self._matches_noise_pattern(alert, pattern):
                # Check if we're within the time window and occurrence limit
                cutoff_time = datetime.utcnow() - timedelta(minutes=pattern.time_window_minutes)
                
                # Count recent occurrences of this pattern
                recent_count = 0
                for suppressed_alert_id in self.suppressed_alerts[pattern_id]:
                    if suppressed_alert_id in self.alert_history:
                        suppressed_alert = next(
                            (a for a in self.alert_history if a.id == suppressed_alert_id), None
                        )
                        if suppressed_alert and suppressed_alert.timestamp > cutoff_time:
                            recent_count += 1
                
                # Check current active alerts for similar patterns
                similar_active = 0
                for active_alert in self.active_alerts.values():
                    if (active_alert.timestamp > cutoff_time and 
                        self._matches_noise_pattern(active_alert, pattern)):
                        similar_active += 1
                
                total_recent = recent_count + similar_active
                
                if total_recent >= pattern.max_occurrences:
                    # Calculate noise confidence
                    noise_confidence = min(1.0, total_recent / pattern.max_occurrences)
                    alert.noise_score = noise_confidence
                    
                    if noise_confidence >= pattern.confidence_threshold:
                        self.logger.info(
                            f"Alert {alert.id} suppressed by noise pattern {pattern_id} "
                            f"(confidence: {noise_confidence:.2f})"
                        )
                        return True
        
        return False
    
    def _matches_noise_pattern(self, alert: Alert, pattern: NoisePattern) -> bool:
        """Check if alert matches a specific noise pattern."""
        
        # Check component filters
        if pattern.component_filters and alert.component not in pattern.component_filters:
            return False
        
        # Check regex patterns against title and description
        alert_text = f"{alert.title} {alert.description}".lower()
        
        for regex_pattern in pattern.regex_patterns:
            if re.search(regex_pattern, alert_text, re.IGNORECASE):
                return True
        
        return False
    
    def _suppress_alert(self, alert: Alert):
        """Suppress alert and update noise tracking."""
        alert.status = AlertStatus.SUPPRESSED
        alert.suppressed_until = datetime.utcnow() + timedelta(hours=1)  # Default suppression period
        
        # Find matching pattern and add to suppressed alerts
        for pattern_id, pattern in self.noise_patterns.items():
            if self._matches_noise_pattern(alert, pattern):
                self.suppressed_alerts[pattern_id].add(alert.id)
                break
    
    async def _correlate_alert(self, alert: Alert) -> List[AlertCorrelation]:
        """Perform comprehensive alert correlation analysis."""
        
        correlations = []
        
        # Prepare alert for ML correlation
        await self._prepare_alert_features(alert)
        
        # Check for different correlation types
        correlations.extend(await self._find_duplicate_correlations(alert))
        correlations.extend(await self._find_temporal_correlations(alert))
        correlations.extend(await self._find_service_correlations(alert))
        correlations.extend(await self._find_infrastructure_correlations(alert))
        correlations.extend(await self._find_cascading_correlations(alert))
        
        # Store correlations
        for correlation in correlations:
            self.correlations[correlation.correlation_id] = correlation
            self.correlation_history.append(correlation)
            self.processing_stats['correlations_created'] += 1
        
        return correlations
    
    async def _prepare_alert_features(self, alert: Alert):
        """Prepare alert features for ML-based correlation."""
        
        # Create text representation for TF-IDF
        alert_text = f"{alert.title} {alert.description} {alert.component} {alert.service}"
        
        # Fit vectorizer if not already fitted
        if not self.is_fitted and len(self.alert_history) > 10:
            all_texts = [
                f"{a.title} {a.description} {a.component} {a.service}" 
                for a in list(self.alert_history)[-100:]  # Use recent history
            ]
            self.text_vectorizer.fit(all_texts)
            self.is_fitted = True
        
        # Generate text vector
        if self.is_fitted:
            try:
                text_matrix = self.text_vectorizer.transform([alert_text])
                alert.text_vector = text_matrix.toarray()[0]
            except Exception as e:
                self.logger.warning(f"Error creating text vector: {e}")
        
        # Create feature vector
        features = [
            alert.severity.value,
            hash(alert.component) % 1000 / 1000.0,  # Normalized hash
            hash(alert.service) % 1000 / 1000.0,
            len(alert.description) / 1000.0,  # Normalized length
            len(alert.labels),
            1.0 if alert.business_impact else 0.0
        ]
        alert.feature_vector = np.array(features)
    
    async def _find_duplicate_correlations(self, alert: Alert) -> List[AlertCorrelation]:
        """Find potential duplicate alerts using high similarity threshold."""
        
        correlations = []
        
        # Look for very similar alerts in recent history
        cutoff_time = datetime.utcnow() - timedelta(minutes=30)
        recent_alerts = [
            a for a in self.active_alerts.values() 
            if a.id != alert.id and a.timestamp > cutoff_time
        ]
        
        for candidate in recent_alerts:
            similarity = self._calculate_alert_similarity(alert, candidate)
            
            if similarity >= self.correlation_config['duplicate_threshold']:
                correlation = AlertCorrelation(
                    correlation_id=str(uuid.uuid4()),
                    primary_alert_id=candidate.id,  # Older alert is primary
                    related_alert_ids={alert.id},
                    correlation_type=CorrelationType.DUPLICATE,
                    confidence_score=similarity,
                    created_at=datetime.utcnow(),
                    business_impact_score=0.0  # Duplicates don't add impact
                )
                
                correlation.recommended_actions = [
                    "Suppress duplicate alert",
                    "Consolidate alert information",
                    "Update occurrence count"
                ]
                
                correlations.append(correlation)
                
                # Update original alert
                candidate.occurrence_count += 1
                candidate.last_occurrence = alert.timestamp
        
        return correlations
    
    async def _find_temporal_correlations(self, alert: Alert) -> List[AlertCorrelation]:
        """Find temporally correlated alerts within time window."""
        
        correlations = []
        time_window = timedelta(seconds=self.correlation_config['temporal_window_seconds'])
        window_start = alert.timestamp - time_window
        window_end = alert.timestamp + time_window
        
        # Find alerts in temporal window
        temporal_candidates = [
            a for a in self.active_alerts.values()
            if (a.id != alert.id and 
                window_start <= a.timestamp <= window_end)
        ]
        
        if len(temporal_candidates) >= 2:  # Need multiple alerts for correlation
            # Group by similarity
            similarities = []
            for candidate in temporal_candidates:
                sim = self._calculate_alert_similarity(alert, candidate)
                if sim >= self.correlation_config['similarity_threshold']:
                    similarities.append((candidate, sim))
            
            if len(similarities) >= 2:
                correlation = AlertCorrelation(
                    correlation_id=str(uuid.uuid4()),
                    primary_alert_id=alert.id,
                    related_alert_ids={a.id for a, _ in similarities},
                    correlation_type=CorrelationType.TEMPORAL,
                    confidence_score=np.mean([sim for _, sim in similarities]),
                    created_at=datetime.utcnow(),
                    business_impact_score=0.5,  # Moderate impact
                    time_window_ms=int(time_window.total_seconds() * 1000)
                )
                
                correlation.recommended_actions = [
                    "Investigate temporal correlation pattern",
                    "Check for common root cause",
                    "Monitor for recurring pattern"
                ]
                
                correlations.append(correlation)
        
        return correlations
    
    async def _find_service_correlations(self, alert: Alert) -> List[AlertCorrelation]:
        """Find correlations based on service relationships."""
        
        correlations = []
        
        # Service dependency map (simplified - would be from service mesh in production)
        service_dependencies = {
            'api-service': ['postgres', 'redis-cache', 'rabbitmq'],
            'etl-service': ['postgres', 'kafka', 'spark-master'],
            'ml-service': ['api-service', 'postgres', 'model-store'],
            'dashboard': ['api-service', 'analytics-service']
        }
        
        related_services = service_dependencies.get(alert.service, [])
        if alert.service in ['postgres', 'redis-cache', 'kafka', 'rabbitmq']:
            # Infrastructure services affect many others
            related_services = [s for s, deps in service_dependencies.items() if alert.service in deps]
        
        # Find alerts in related services
        cutoff_time = datetime.utcnow() - timedelta(minutes=15)
        service_alerts = [
            a for a in self.active_alerts.values()
            if (a.id != alert.id and 
                a.service in related_services and 
                a.timestamp > cutoff_time)
        ]
        
        if service_alerts:
            # Calculate business impact based on service criticality
            impact_multiplier = {
                'api-service': 1.0,
                'postgres': 0.9,
                'etl-service': 0.7,
                'ml-service': 0.8
            }
            
            impact_score = impact_multiplier.get(alert.service, 0.5)
            
            correlation = AlertCorrelation(
                correlation_id=str(uuid.uuid4()),
                primary_alert_id=alert.id,
                related_alert_ids={a.id for a in service_alerts},
                correlation_type=CorrelationType.SERVICE_RELATED,
                confidence_score=0.8,  # High confidence for service dependencies
                created_at=datetime.utcnow(),
                business_impact_score=impact_score
            )
            
            correlation.recommended_actions = [
                f"Check {alert.service} service health",
                "Review service dependency chain",
                "Consider cascading impact mitigation"
            ]
            
            correlations.append(correlation)
        
        return correlations
    
    async def _find_infrastructure_correlations(self, alert: Alert) -> List[AlertCorrelation]:
        """Find correlations based on infrastructure relationships."""
        
        correlations = []
        
        if alert.resource_type in ['cpu', 'memory', 'disk', 'network']:
            # Find other infrastructure alerts on the same instance
            cutoff_time = datetime.utcnow() - timedelta(minutes=10)
            
            same_instance_alerts = [
                a for a in self.active_alerts.values()
                if (a.id != alert.id and
                    a.labels.get('instance') == alert.labels.get('instance') and
                    a.timestamp > cutoff_time and
                    a.component == 'infrastructure')
            ]
            
            if len(same_instance_alerts) >= 2:
                correlation = AlertCorrelation(
                    correlation_id=str(uuid.uuid4()),
                    primary_alert_id=alert.id,
                    related_alert_ids={a.id for a in same_instance_alerts},
                    correlation_type=CorrelationType.INFRASTRUCTURE,
                    confidence_score=0.85,
                    created_at=datetime.utcnow(),
                    business_impact_score=0.7
                )
                
                correlation.root_cause_hypothesis = f"Infrastructure issues on {alert.labels.get('instance', 'unknown')}"
                correlation.recommended_actions = [
                    "Investigate instance health",
                    "Check system resource contention",
                    "Consider instance restart or scaling"
                ]
                
                correlations.append(correlation)
        
        return correlations
    
    async def _find_cascading_correlations(self, alert: Alert) -> List[AlertCorrelation]:
        """Find cascading failure patterns using sequence analysis."""
        
        correlations = []
        
        # Look for cascading patterns in alert sequence
        cascade_window = timedelta(minutes=self.correlation_config['cascade_detection_window'] / 60)
        window_alerts = [
            a for a in self.alert_history
            if (alert.timestamp - cascade_window <= a.timestamp <= alert.timestamp and
                a.id != alert.id)
        ]
        
        # Sort by timestamp to analyze sequence
        window_alerts.sort(key=lambda x: x.timestamp)
        
        if len(window_alerts) >= 3:  # Need sufficient alerts for cascade detection
            # Look for severity escalation pattern
            severity_progression = [a.severity.value for a in window_alerts]
            severity_progression.append(alert.severity.value)
            
            # Check if severity is generally increasing (cascading failure)
            increasing_trend = sum(
                1 for i in range(1, len(severity_progression))
                if severity_progression[i] >= severity_progression[i-1]
            )
            
            cascade_confidence = increasing_trend / (len(severity_progression) - 1)
            
            if cascade_confidence >= 0.7:
                correlation = AlertCorrelation(
                    correlation_id=str(uuid.uuid4()),
                    primary_alert_id=window_alerts[0].id,  # First alert in sequence
                    related_alert_ids={a.id for a in window_alerts[1:]} | {alert.id},
                    correlation_type=CorrelationType.CASCADING,
                    confidence_score=cascade_confidence,
                    created_at=datetime.utcnow(),
                    business_impact_score=1.5,  # High impact for cascading failures
                    sequence_pattern=[a.component for a in window_alerts] + [alert.component]
                )
                
                correlation.root_cause_hypothesis = f"Cascading failure starting from {window_alerts[0].component}"
                correlation.recommended_actions = [
                    "Identify and address root cause",
                    "Implement circuit breakers",
                    "Consider service isolation",
                    "Emergency incident response may be needed"
                ]
                
                correlations.append(correlation)
        
        return correlations
    
    def _calculate_alert_similarity(self, alert1: Alert, alert2: Alert) -> float:
        """Calculate similarity score between two alerts."""
        
        similarity_scores = []
        
        # Text similarity using TF-IDF vectors
        if alert1.text_vector is not None and alert2.text_vector is not None:
            text_sim = cosine_similarity([alert1.text_vector], [alert2.text_vector])[0][0]
            similarity_scores.append(text_sim * 0.4)  # 40% weight
        
        # Feature vector similarity
        if alert1.feature_vector is not None and alert2.feature_vector is not None:
            feature_sim = cosine_similarity([alert1.feature_vector], [alert2.feature_vector])[0][0]
            similarity_scores.append(feature_sim * 0.3)  # 30% weight
        
        # Component/service similarity
        component_match = 1.0 if alert1.component == alert2.component else 0.0
        service_match = 1.0 if alert1.service == alert2.service else 0.0
        similarity_scores.append((component_match + service_match) * 0.2)  # 20% weight
        
        # Severity similarity
        severity_diff = abs(alert1.severity.value - alert2.severity.value)
        severity_sim = max(0, 1.0 - (severity_diff / 4.0))  # Max diff is 4
        similarity_scores.append(severity_sim * 0.1)  # 10% weight
        
        return sum(similarity_scores) if similarity_scores else 0.0
    
    async def _calculate_business_impact_score(self, alert: Alert, correlations: List[AlertCorrelation]):
        """Calculate business impact score based on alert and correlations."""
        
        base_impact = {
            AlertSeverity.INFO: 0.1,
            AlertSeverity.WARNING: 0.3,
            AlertSeverity.ERROR: 0.6,
            AlertSeverity.CRITICAL: 0.9,
            AlertSeverity.EMERGENCY: 1.0
        }
        
        impact_score = base_impact[alert.severity]
        
        # Business value multiplier
        if alert.business_value:
            try:
                value_float = float(alert.business_value.replace('M', ''))
                impact_score *= min(2.0, value_float / 10.0)  # Cap at 2x multiplier
            except (ValueError, AttributeError):
                pass
        
        # Component criticality multiplier
        critical_components = {
            'api-performance': 1.5,
            'database': 1.4,
            'security': 1.3,
            'infrastructure': 1.2
        }
        
        component_multiplier = critical_components.get(alert.component, 1.0)
        impact_score *= component_multiplier
        
        # Correlation impact
        for correlation in correlations:
            if correlation.correlation_type == CorrelationType.CASCADING:
                impact_score *= 1.5
            elif correlation.correlation_type == CorrelationType.SERVICE_RELATED:
                impact_score *= 1.2
        
        # Store impact score in alert
        alert.annotations['business_impact_score'] = str(min(10.0, impact_score))
    
    async def _evaluate_escalation(self, alert: Alert, correlations: List[AlertCorrelation]):
        """Evaluate if alert requires escalation."""
        
        # Business impact thresholds for escalation
        impact_score = float(alert.annotations.get('business_impact_score', '0'))
        
        escalation_needed = False
        escalation_reason = []
        
        # Severity-based escalation
        if alert.severity in [AlertSeverity.CRITICAL, AlertSeverity.EMERGENCY]:
            escalation_needed = True
            escalation_reason.append("Critical/Emergency severity")
        
        # Business impact escalation
        if impact_score >= 5.0:
            escalation_needed = True
            escalation_reason.append(f"High business impact score: {impact_score}")
        
        # Correlation-based escalation
        cascading_correlations = [c for c in correlations if c.correlation_type == CorrelationType.CASCADING]
        if cascading_correlations:
            escalation_needed = True
            escalation_reason.append("Cascading failure detected")
        
        # Multi-service impact
        service_correlations = [c for c in correlations if c.correlation_type == CorrelationType.SERVICE_RELATED]
        if len(service_correlations) >= 2:
            escalation_needed = True
            escalation_reason.append("Multi-service impact")
        
        if escalation_needed:
            alert.status = AlertStatus.ESCALATED
            alert.escalated_at = datetime.utcnow()
            alert.annotations['escalation_reason'] = '; '.join(escalation_reason)
            
            self.logger.warning(f"Alert {alert.id} escalated: {'; '.join(escalation_reason)}")
    
    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an alert and update correlated alerts."""
        
        if alert_id not in self.active_alerts:
            return False
        
        alert = self.active_alerts[alert_id]
        alert.status = AlertStatus.ACKNOWLEDGED
        alert.acknowledged_at = datetime.utcnow()
        alert.annotations['acknowledged_by'] = acknowledged_by
        
        # Acknowledge related alerts in correlations
        for correlation in self.correlations.values():
            if alert_id in [correlation.primary_alert_id] + list(correlation.related_alert_ids):
                for related_id in correlation.related_alert_ids:
                    if (related_id in self.active_alerts and 
                        self.active_alerts[related_id].status == AlertStatus.ACTIVE):
                        self.active_alerts[related_id].status = AlertStatus.ACKNOWLEDGED
                        self.active_alerts[related_id].acknowledged_at = datetime.utcnow()
                        self.active_alerts[related_id].annotations['acknowledged_by'] = f"correlation-{acknowledged_by}"
        
        return True
    
    async def resolve_alert(self, alert_id: str, resolved_by: str) -> bool:
        """Resolve an alert and update correlated alerts."""
        
        if alert_id not in self.active_alerts:
            return False
        
        alert = self.active_alerts[alert_id]
        alert.status = AlertStatus.RESOLVED
        alert.resolved_at = datetime.utcnow()
        alert.annotations['resolved_by'] = resolved_by
        
        # Check if correlated alerts should also be resolved
        for correlation in self.correlations.values():
            if (correlation.primary_alert_id == alert_id and 
                correlation.correlation_type == CorrelationType.ROOT_CAUSE):
                # Resolve child alerts for root cause correlations
                for related_id in correlation.related_alert_ids:
                    if related_id in self.active_alerts:
                        await self.resolve_alert(related_id, f"root-cause-{resolved_by}")
        
        # Move to history and remove from active
        del self.active_alerts[alert_id]
        
        return True
    
    def get_correlation_dashboard_data(self) -> Dict[str, Any]:
        """Get data for correlation dashboard."""
        
        # Active correlations summary
        active_correlations = {
            correlation_type.value: len([
                c for c in self.correlations.values() 
                if c.correlation_type == correlation_type
            ])
            for correlation_type in CorrelationType
        }
        
        # Top correlated alerts
        correlation_counts = defaultdict(int)
        for correlation in self.correlations.values():
            correlation_counts[correlation.primary_alert_id] += 1
            for related_id in correlation.related_alert_ids:
                correlation_counts[related_id] += 1
        
        top_correlated = sorted(correlation_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Noise reduction stats
        noise_stats = {}
        for pattern_id, suppressed_ids in self.suppressed_alerts.items():
            pattern = self.noise_patterns.get(pattern_id)
            if pattern:
                noise_stats[pattern.pattern_name] = {
                    'suppressed_count': len(suppressed_ids),
                    'pattern_confidence': pattern.confidence_threshold,
                    'false_positive_rate': pattern.false_positive_rate
                }
        
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'summary': {
                'total_active_alerts': len(self.active_alerts),
                'total_correlations': len(self.correlations),
                'alerts_suppressed': self.processing_stats['alerts_suppressed'],
                'correlation_rate': (self.processing_stats['correlations_created'] / 
                                   max(1, self.processing_stats['alerts_processed'])) * 100
            },
            'correlation_types': active_correlations,
            'top_correlated_alerts': [
                {
                    'alert_id': alert_id,
                    'correlation_count': count,
                    'alert_title': self.active_alerts.get(alert_id, Alert('', '', '', AlertSeverity.INFO, AlertStatus.ACTIVE, datetime.utcnow(), '', '', '')).title
                }
                for alert_id, count in top_correlated[:5]
            ],
            'noise_patterns': noise_stats,
            'performance': {
                'avg_processing_time_ms': np.mean(self.processing_stats['processing_time_ms'][-100:]) if self.processing_stats['processing_time_ms'] else 0,
                'alerts_processed_total': self.processing_stats['alerts_processed'],
                'false_positive_rate': self.processing_stats['false_positive_rate']
            },
            'escalation_stats': {
                'total_escalated': len([a for a in self.active_alerts.values() if a.status == AlertStatus.ESCALATED]),
                'critical_alerts': len([a for a in self.active_alerts.values() if a.severity in [AlertSeverity.CRITICAL, AlertSeverity.EMERGENCY]]),
                'business_impact_high': len([a for a in self.active_alerts.values() if float(a.annotations.get('business_impact_score', '0')) >= 5.0])
            }
        }
    
    async def optimize_noise_patterns(self):
        """Optimize noise patterns based on historical data."""
        
        # Analyze suppressed alerts for pattern effectiveness
        for pattern_id, pattern in self.noise_patterns.items():
            suppressed_ids = self.suppressed_alerts.get(pattern_id, set())
            
            if len(suppressed_ids) >= 10:  # Need sufficient data
                # Calculate pattern effectiveness metrics
                total_suppressed = len(suppressed_ids)
                
                # Estimate false positives (simplified - would use feedback in production)
                estimated_false_positives = max(0, int(total_suppressed * 0.05))  # Assume 5% FP rate
                
                pattern.false_positive_rate = estimated_false_positives / total_suppressed
                pattern.true_positive_rate = 1.0 - pattern.false_positive_rate
                pattern.last_updated = datetime.utcnow()
                
                # Adjust confidence threshold based on performance
                if pattern.false_positive_rate > 0.1:  # Too many false positives
                    pattern.confidence_threshold = min(1.0, pattern.confidence_threshold + 0.1)
                elif pattern.false_positive_rate < 0.02:  # Very low FP rate
                    pattern.confidence_threshold = max(0.5, pattern.confidence_threshold - 0.05)
        
        self.logger.info("Noise pattern optimization completed")


# Global alert correlator instance
alert_correlator = IntelligentAlertCorrelator()


# Async context manager for alert processing
class AlertProcessingContext:
    """Context manager for alert processing with correlation."""
    
    def __init__(self, alert_data: Dict[str, Any]):
        self.alert_data = alert_data
        self.processed_alert = None
        
    async def __aenter__(self):
        self.processed_alert = await alert_correlator.process_alert(self.alert_data)
        return self.processed_alert
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logger.error(f"Error in alert processing context: {exc_val}")


__all__ = [
    'IntelligentAlertCorrelator',
    'alert_correlator',
    'AlertProcessingContext',
    'Alert',
    'AlertCorrelation',
    'NoisePattern',
    'AlertSeverity',
    'AlertStatus',
    'CorrelationType'
]
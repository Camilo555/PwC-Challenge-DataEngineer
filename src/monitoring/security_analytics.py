"""
Security Analytics and Reporting System
Advanced analytics for security posture trending, attack pattern analysis,
user behavior analytics, and automated compliance reporting.
"""
import asyncio
import json
import statistics
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import threading
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

from core.logging import get_logger
from monitoring.security_metrics import (
    SecurityMetricsCollector, SecurityEventSeverity, SecurityMetricType,
    get_security_metrics_collector, SecurityMetricEvent
)
from monitoring.compliance_dashboard import ComplianceDashboard, get_compliance_dashboard
from monitoring.security_alerting import SecurityAlertingSystem, get_security_alerting_system
from core.security.compliance_framework import ComplianceFramework


logger = get_logger(__name__)


class AnalyticsTimeframe(Enum):
    """Analytics timeframes"""
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"


class ReportFormat(Enum):
    """Report output formats"""
    JSON = "json"
    PDF = "pdf"
    HTML = "html"
    CSV = "csv"
    EXCEL = "excel"


class ThreatIntelligenceSource(Enum):
    """Threat intelligence sources"""
    INTERNAL = "internal"
    MITRE_ATT_CK = "mitre_attack"
    NIST_CVE = "nist_cve"
    THREAT_FEEDS = "threat_feeds"
    OSINT = "osint"


@dataclass
class SecurityTrend:
    """Security trend analysis result"""
    metric_name: str
    timeframe: AnalyticsTimeframe
    trend_direction: str  # increasing, decreasing, stable
    trend_strength: float  # 0-1
    current_value: float
    previous_value: float
    percentage_change: float
    data_points: List[Tuple[datetime, float]]
    confidence_interval: Tuple[float, float]
    prediction_next_period: Optional[float] = None


@dataclass
class AttackPattern:
    """Identified attack pattern"""
    pattern_id: str
    pattern_name: str
    description: str
    tactics: List[str]  # MITRE ATT&CK tactics
    techniques: List[str]  # MITRE ATT&CK techniques
    confidence_score: float
    first_observed: datetime
    last_observed: datetime
    frequency: int
    affected_assets: List[str]
    indicators: List[Dict[str, Any]]
    severity: SecurityEventSeverity
    attribution: Optional[str] = None
    mitigation_recommendations: List[str] = field(default_factory=list)


@dataclass
class UserBehaviorProfile:
    """User behavior analysis profile"""
    user_id: str
    risk_score: float
    behavior_baseline: Dict[str, float]
    anomaly_indicators: List[str]
    
    # Activity patterns
    typical_access_times: List[int]  # Hours of day
    typical_resources: List[str]
    typical_locations: List[str]
    
    # Risk factors
    failed_login_attempts: int
    privilege_escalation_attempts: int
    unusual_data_access: int
    suspicious_ip_usage: int
    
    # Temporal analysis
    profile_created: datetime
    last_updated: datetime
    observation_period_days: int
    
    # Machine learning features
    ml_features: Dict[str, float] = field(default_factory=dict)
    anomaly_score: Optional[float] = None


@dataclass
class ComplianceReport:
    """Comprehensive compliance report"""
    report_id: str
    framework: ComplianceFramework
    report_type: str
    generated_at: datetime
    period_start: datetime
    period_end: datetime
    
    # Summary metrics
    overall_compliance_score: float
    total_controls_assessed: int
    controls_compliant: int
    controls_non_compliant: int
    
    # Detailed findings
    violations: List[Dict[str, Any]]
    remediation_items: List[Dict[str, Any]]
    risk_assessment: Dict[str, Any]
    
    # Trends and forecasting
    compliance_trend: SecurityTrend
    projected_compliance: Optional[float] = None
    
    # Recommendations
    priority_actions: List[str] = field(default_factory=list)
    resource_requirements: Dict[str, Any] = field(default_factory=dict)


class SecurityPostureAnalyzer:
    """Analyze overall security posture and trends"""
    
    def __init__(self, security_metrics: SecurityMetricsCollector):
        self.security_metrics = security_metrics
        self.logger = get_logger(__name__)
        
        # Historical data for trend analysis
        self.historical_scores: Dict[str, List[Tuple[datetime, float]]] = defaultdict(list)
        self._lock = threading.RLock()
    
    def analyze_security_posture_trends(
        self,
        timeframe: AnalyticsTimeframe,
        periods_back: int = 12
    ) -> Dict[str, SecurityTrend]:
        """Analyze security posture trends over time"""
        
        end_date = datetime.now()
        
        # Calculate time delta based on timeframe
        if timeframe == AnalyticsTimeframe.HOURLY:
            delta = timedelta(hours=1)
        elif timeframe == AnalyticsTimeframe.DAILY:
            delta = timedelta(days=1)
        elif timeframe == AnalyticsTimeframe.WEEKLY:
            delta = timedelta(weeks=1)
        elif timeframe == AnalyticsTimeframe.MONTHLY:
            delta = timedelta(days=30)
        else:
            delta = timedelta(days=1)
        
        start_date = end_date - (delta * periods_back)
        
        trends = {}
        
        # Analyze threat detection trends
        threat_trend = self._analyze_metric_trend(
            "threat_detection_rate",
            timeframe,
            start_date,
            end_date,
            self._get_threat_detection_rate
        )
        trends["threat_detection"] = threat_trend
        
        # Analyze DLP violation trends
        dlp_trend = self._analyze_metric_trend(
            "dlp_violation_rate",
            timeframe,
            start_date,
            end_date,
            self._get_dlp_violation_rate
        )
        trends["dlp_violations"] = dlp_trend
        
        # Analyze compliance score trends
        compliance_trend = self._analyze_metric_trend(
            "compliance_score",
            timeframe,
            start_date,
            end_date,
            self._get_compliance_score
        )
        trends["compliance"] = compliance_trend
        
        # Analyze access control trends
        access_trend = self._analyze_metric_trend(
            "access_approval_rate",
            timeframe,
            start_date,
            end_date,
            self._get_access_approval_rate
        )
        trends["access_control"] = access_trend
        
        return trends
    
    def _analyze_metric_trend(
        self,
        metric_name: str,
        timeframe: AnalyticsTimeframe,
        start_date: datetime,
        end_date: datetime,
        value_getter: callable
    ) -> SecurityTrend:
        """Analyze trend for a specific metric"""
        
        # Get data points
        data_points = []
        current_date = start_date
        
        while current_date <= end_date:
            value = value_getter(current_date)
            data_points.append((current_date, value))
            
            # Move to next period
            if timeframe == AnalyticsTimeframe.HOURLY:
                current_date += timedelta(hours=1)
            elif timeframe == AnalyticsTimeframe.DAILY:
                current_date += timedelta(days=1)
            elif timeframe == AnalyticsTimeframe.WEEKLY:
                current_date += timedelta(weeks=1)
            else:
                current_date += timedelta(days=30)
        
        if len(data_points) < 2:
            return SecurityTrend(
                metric_name=metric_name,
                timeframe=timeframe,
                trend_direction="stable",
                trend_strength=0.0,
                current_value=data_points[0][1] if data_points else 0.0,
                previous_value=0.0,
                percentage_change=0.0,
                data_points=data_points,
                confidence_interval=(0.0, 0.0)
            )
        
        # Calculate trend
        values = [point[1] for point in data_points]
        current_value = values[-1]
        previous_value = values[0]
        percentage_change = ((current_value - previous_value) / max(previous_value, 0.001)) * 100
        
        # Linear regression for trend direction and strength
        x = np.arange(len(values))
        y = np.array(values)
        
        # Calculate correlation coefficient for trend strength
        correlation = np.corrcoef(x, y)[0, 1] if len(values) > 1 else 0.0
        trend_strength = abs(correlation)
        
        # Determine trend direction
        if correlation > 0.1:
            trend_direction = "increasing"
        elif correlation < -0.1:
            trend_direction = "decreasing"
        else:
            trend_direction = "stable"
        
        # Calculate confidence interval (simple standard deviation approach)
        std_dev = np.std(values)
        confidence_interval = (current_value - std_dev, current_value + std_dev)
        
        # Simple prediction for next period
        if len(values) >= 3:
            recent_trend = (values[-1] - values[-3]) / 2
            prediction_next_period = current_value + recent_trend
        else:
            prediction_next_period = None
        
        return SecurityTrend(
            metric_name=metric_name,
            timeframe=timeframe,
            trend_direction=trend_direction,
            trend_strength=trend_strength,
            current_value=current_value,
            previous_value=previous_value,
            percentage_change=percentage_change,
            data_points=data_points,
            confidence_interval=confidence_interval,
            prediction_next_period=prediction_next_period
        )
    
    def _get_threat_detection_rate(self, date: datetime) -> float:
        """Get threat detection rate for a specific date"""
        # This would integrate with actual metrics
        # For now, return simulated data
        base_rate = 15.0  # Base threats per hour
        noise = np.random.normal(0, 3)  # Add some realistic variation
        return max(0, base_rate + noise)
    
    def _get_dlp_violation_rate(self, date: datetime) -> float:
        """Get DLP violation rate for a specific date"""
        base_rate = 5.0  # Base violations per day
        noise = np.random.normal(0, 1)
        return max(0, base_rate + noise)
    
    def _get_compliance_score(self, date: datetime) -> float:
        """Get compliance score for a specific date"""
        base_score = 87.5  # Base compliance score
        noise = np.random.normal(0, 2)
        return max(0, min(100, base_score + noise))
    
    def _get_access_approval_rate(self, date: datetime) -> float:
        """Get access approval rate for a specific date"""
        base_rate = 0.92  # 92% approval rate
        noise = np.random.normal(0, 0.02)
        return max(0, min(1, base_rate + noise))
    
    def forecast_security_metrics(
        self,
        metric_name: str,
        periods_ahead: int = 7
    ) -> List[Tuple[datetime, float, float]]:
        """Forecast security metrics using simple time series prediction"""
        
        # Get historical data for the metric
        trend = self.analyze_security_posture_trends(
            AnalyticsTimeframe.DAILY, periods_back=30
        ).get(metric_name)
        
        if not trend or len(trend.data_points) < 7:
            return []
        
        # Simple forecasting using moving average and trend
        values = [point[1] for point in trend.data_points]
        last_date = trend.data_points[-1][0]
        
        # Calculate recent trend
        recent_values = values[-7:]  # Last week
        trend_slope = (recent_values[-1] - recent_values[0]) / 6
        
        # Calculate forecast
        forecasts = []
        for i in range(1, periods_ahead + 1):
            forecast_date = last_date + timedelta(days=i)
            
            # Simple linear trend with moving average base
            base_value = np.mean(recent_values)
            trend_component = trend_slope * i
            forecast_value = base_value + trend_component
            
            # Add confidence interval (simplified)
            std_dev = np.std(recent_values)
            confidence_interval = std_dev * 1.96  # 95% confidence
            
            forecasts.append((forecast_date, forecast_value, confidence_interval))
        
        return forecasts


class AttackPatternAnalyzer:
    """Analyze and identify attack patterns from security events"""
    
    def __init__(self, security_metrics: SecurityMetricsCollector):
        self.security_metrics = security_metrics
        self.logger = get_logger(__name__)
        self.known_patterns: Dict[str, AttackPattern] = {}
        
        # MITRE ATT&CK framework mapping
        self.mitre_tactics = [
            "reconnaissance", "resource_development", "initial_access",
            "execution", "persistence", "privilege_escalation", "defense_evasion",
            "credential_access", "discovery", "lateral_movement", "collection",
            "command_and_control", "exfiltration", "impact"
        ]
    
    def analyze_attack_patterns(
        self,
        time_window_hours: int = 24
    ) -> List[AttackPattern]:
        """Analyze recent events for attack patterns"""
        
        cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
        
        # Get recent security events
        recent_events = [
            event for event in self.security_metrics.security_events
            if event.timestamp >= cutoff_time
        ]
        
        if not recent_events:
            return []
        
        patterns = []
        
        # Pattern 1: Brute force attacks
        brute_force_pattern = self._detect_brute_force_pattern(recent_events)
        if brute_force_pattern:
            patterns.append(brute_force_pattern)
        
        # Pattern 2: Data exfiltration attempts
        exfiltration_pattern = self._detect_data_exfiltration_pattern(recent_events)
        if exfiltration_pattern:
            patterns.append(exfiltration_pattern)
        
        # Pattern 3: Privilege escalation sequences
        privilege_escalation_pattern = self._detect_privilege_escalation_pattern(recent_events)
        if privilege_escalation_pattern:
            patterns.append(privilege_escalation_pattern)
        
        # Pattern 4: Lateral movement
        lateral_movement_pattern = self._detect_lateral_movement_pattern(recent_events)
        if lateral_movement_pattern:
            patterns.append(lateral_movement_pattern)
        
        # Pattern 5: Suspicious user behavior
        suspicious_behavior_pattern = self._detect_suspicious_behavior_pattern(recent_events)
        if suspicious_behavior_pattern:
            patterns.append(suspicious_behavior_pattern)
        
        return patterns
    
    def _detect_brute_force_pattern(self, events: List[SecurityMetricEvent]) -> Optional[AttackPattern]:
        """Detect brute force attack patterns"""
        
        # Look for multiple failed authentication attempts from same IP
        failed_auths = defaultdict(list)
        
        for event in events:
            if (event.metric_type == SecurityMetricType.AUTHENTICATION and 
                event.outcome == "failure" and 
                event.source_ip):
                failed_auths[event.source_ip].append(event)
        
        # Check for IPs with high failure rates
        suspicious_ips = []
        for ip, failures in failed_auths.items():
            if len(failures) >= 10:  # 10+ failures
                time_span = (max(f.timestamp for f in failures) - 
                           min(f.timestamp for f in failures)).total_seconds() / 3600
                if time_span <= 1:  # Within 1 hour
                    suspicious_ips.append((ip, failures))
        
        if not suspicious_ips:
            return None
        
        # Create attack pattern
        all_failures = []
        affected_assets = set()
        
        for ip, failures in suspicious_ips:
            all_failures.extend(failures)
            affected_assets.update(f.resource_id for f in failures)
        
        return AttackPattern(
            pattern_id=f"brute_force_{int(datetime.now().timestamp())}",
            pattern_name="Brute Force Authentication Attack",
            description=f"Multiple failed login attempts detected from {len(suspicious_ips)} IP address(es)",
            tactics=["credential_access"],
            techniques=["T1110.001"],  # Password Guessing
            confidence_score=0.8,
            first_observed=min(f.timestamp for f in all_failures),
            last_observed=max(f.timestamp for f in all_failures),
            frequency=len(all_failures),
            affected_assets=list(affected_assets),
            indicators=[
                {"type": "ip_address", "value": ip, "failure_count": len(failures)}
                for ip, failures in suspicious_ips
            ],
            severity=SecurityEventSeverity.HIGH,
            mitigation_recommendations=[
                "Implement account lockout policies",
                "Enable multi-factor authentication",
                "Monitor and block suspicious IP addresses",
                "Implement CAPTCHA for failed login attempts"
            ]
        )
    
    def _detect_data_exfiltration_pattern(self, events: List[SecurityMetricEvent]) -> Optional[AttackPattern]:
        """Detect data exfiltration patterns"""
        
        # Look for unusual data access patterns followed by DLP violations
        data_access_events = []
        dlp_violations = []
        
        for event in events:
            if event.metric_type == SecurityMetricType.DATA_ACCESS:
                data_access_events.append(event)
            elif event.metric_type == SecurityMetricType.DLP_VIOLATION:
                dlp_violations.append(event)
        
        if not dlp_violations or not data_access_events:
            return None
        
        # Correlate data access with DLP violations by user and time
        suspicious_sequences = []
        
        for dlp_event in dlp_violations:
            # Look for data access events from same user within 1 hour before DLP violation
            related_access = [
                event for event in data_access_events
                if (event.user_id == dlp_event.user_id and
                    event.timestamp <= dlp_event.timestamp and
                    (dlp_event.timestamp - event.timestamp).total_seconds() <= 3600)
            ]
            
            if len(related_access) >= 3:  # Multiple data accesses before DLP violation
                suspicious_sequences.append((dlp_event, related_access))
        
        if not suspicious_sequences:
            return None
        
        all_events = []
        affected_users = set()
        affected_assets = set()
        
        for dlp_event, access_events in suspicious_sequences:
            all_events.extend(access_events)
            all_events.append(dlp_event)
            affected_users.add(dlp_event.user_id)
            affected_assets.add(dlp_event.resource_id)
        
        return AttackPattern(
            pattern_id=f"data_exfiltration_{int(datetime.now().timestamp())}",
            pattern_name="Potential Data Exfiltration",
            description=f"Suspicious data access patterns followed by DLP violations from {len(affected_users)} user(s)",
            tactics=["collection", "exfiltration"],
            techniques=["T1005", "T1041"],  # Data from Local System, Exfiltration Over C2 Channel
            confidence_score=0.7,
            first_observed=min(e.timestamp for e in all_events),
            last_observed=max(e.timestamp for e in all_events),
            frequency=len(all_events),
            affected_assets=list(affected_assets),
            indicators=[
                {"type": "user_id", "value": user, "violation_count": 1}
                for user in affected_users
            ],
            severity=SecurityEventSeverity.CRITICAL,
            mitigation_recommendations=[
                "Review user access permissions",
                "Implement data loss prevention controls",
                "Monitor large data transfers",
                "Conduct user behavior analysis"
            ]
        )
    
    def _detect_privilege_escalation_pattern(self, events: List[SecurityMetricEvent]) -> Optional[AttackPattern]:
        """Detect privilege escalation patterns"""
        
        # Look for access attempts to higher-privilege resources
        access_events = [
            event for event in events
            if event.metric_type == SecurityMetricType.ACCESS_CONTROL
        ]
        
        if not access_events:
            return None
        
        # Group by user and look for escalating access attempts
        user_access = defaultdict(list)
        for event in access_events:
            if event.user_id:
                user_access[event.user_id].append(event)
        
        suspicious_users = []
        for user_id, user_events in user_access.items():
            # Look for mix of denied and approved requests with high-risk scores
            high_risk_events = [
                event for event in user_events
                if event.details.get('risk_score', 0) > 7
            ]
            
            denied_events = [
                event for event in user_events
                if event.outcome == "denied"
            ]
            
            if len(high_risk_events) >= 3 and len(denied_events) >= 2:
                suspicious_users.append((user_id, user_events))
        
        if not suspicious_users:
            return None
        
        all_events = []
        affected_users = set()
        affected_resources = set()
        
        for user_id, user_events in suspicious_users:
            all_events.extend(user_events)
            affected_users.add(user_id)
            affected_resources.update(event.resource_id for event in user_events)
        
        return AttackPattern(
            pattern_id=f"privilege_escalation_{int(datetime.now().timestamp())}",
            pattern_name="Privilege Escalation Attempt",
            description=f"Suspicious privilege escalation attempts from {len(affected_users)} user(s)",
            tactics=["privilege_escalation"],
            techniques=["T1078"],  # Valid Accounts
            confidence_score=0.6,
            first_observed=min(e.timestamp for e in all_events),
            last_observed=max(e.timestamp for e in all_events),
            frequency=len(all_events),
            affected_assets=list(affected_resources),
            indicators=[
                {"type": "user_id", "value": user, "high_risk_attempts": len([e for e in events if e.details.get('risk_score', 0) > 7])}
                for user, events in suspicious_users
            ],
            severity=SecurityEventSeverity.HIGH,
            mitigation_recommendations=[
                "Review user privilege levels",
                "Implement just-in-time access",
                "Monitor privileged account usage",
                "Require approval for privilege escalation"
            ]
        )
    
    def _detect_lateral_movement_pattern(self, events: List[SecurityMetricEvent]) -> Optional[AttackPattern]:
        """Detect lateral movement patterns"""
        
        # Look for access attempts across multiple resources by same user/IP
        access_events = [
            event for event in events
            if event.metric_type == SecurityMetricType.ACCESS_CONTROL and
            event.outcome in ["approved", "denied"]
        ]
        
        if not access_events:
            return None
        
        # Group by source IP and look for access to multiple resources
        ip_access = defaultdict(list)
        for event in access_events:
            if event.source_ip:
                ip_access[event.source_ip].append(event)
        
        suspicious_ips = []
        for ip, ip_events in ip_access.items():
            unique_resources = len(set(event.resource_id for event in ip_events))
            unique_users = len(set(event.user_id for event in ip_events if event.user_id))
            
            # Multiple resources and/or users from same IP
            if unique_resources >= 5 or unique_users >= 3:
                time_span = (max(e.timestamp for e in ip_events) - 
                           min(e.timestamp for e in ip_events)).total_seconds() / 3600
                
                if time_span <= 2:  # Within 2 hours
                    suspicious_ips.append((ip, ip_events, unique_resources, unique_users))
        
        if not suspicious_ips:
            return None
        
        all_events = []
        affected_assets = set()
        
        for ip, ip_events, resources, users in suspicious_ips:
            all_events.extend(ip_events)
            affected_assets.update(event.resource_id for event in ip_events)
        
        return AttackPattern(
            pattern_id=f"lateral_movement_{int(datetime.now().timestamp())}",
            pattern_name="Lateral Movement Activity",
            description=f"Suspicious lateral movement detected from {len(suspicious_ips)} IP address(es)",
            tactics=["lateral_movement"],
            techniques=["T1021"],  # Remote Services
            confidence_score=0.7,
            first_observed=min(e.timestamp for e in all_events),
            last_observed=max(e.timestamp for e in all_events),
            frequency=len(all_events),
            affected_assets=list(affected_assets),
            indicators=[
                {"type": "ip_address", "value": ip, "resources_accessed": resources, "users": users}
                for ip, _, resources, users in suspicious_ips
            ],
            severity=SecurityEventSeverity.HIGH,
            mitigation_recommendations=[
                "Implement network segmentation",
                "Monitor east-west network traffic",
                "Restrict lateral movement paths",
                "Use zero-trust network principles"
            ]
        )
    
    def _detect_suspicious_behavior_pattern(self, events: List[SecurityMetricEvent]) -> Optional[AttackPattern]:
        """Detect general suspicious behavior patterns"""
        
        # Look for users with multiple different types of security events
        user_events = defaultdict(list)
        for event in events:
            if event.user_id:
                user_events[event.user_id].append(event)
        
        suspicious_users = []
        for user_id, user_event_list in user_events.items():
            # Check for diversity of security events
            event_types = set(event.metric_type for event in user_event_list)
            severity_events = [e for e in user_event_list if e.severity in [SecurityEventSeverity.HIGH, SecurityEventSeverity.CRITICAL]]
            
            if len(event_types) >= 3 and len(severity_events) >= 2:
                suspicious_users.append((user_id, user_event_list))
        
        if not suspicious_users:
            return None
        
        all_events = []
        affected_users = set()
        affected_resources = set()
        
        for user_id, user_event_list in suspicious_users:
            all_events.extend(user_event_list)
            affected_users.add(user_id)
            affected_resources.update(event.resource_id for event in user_event_list)
        
        return AttackPattern(
            pattern_id=f"suspicious_behavior_{int(datetime.now().timestamp())}",
            pattern_name="Suspicious User Behavior",
            description=f"Multiple types of security events from {len(affected_users)} user(s)",
            tactics=["reconnaissance", "initial_access"],
            techniques=["T1589"],  # Gather Victim Identity Information
            confidence_score=0.5,
            first_observed=min(e.timestamp for e in all_events),
            last_observed=max(e.timestamp for e in all_events),
            frequency=len(all_events),
            affected_assets=list(affected_resources),
            indicators=[
                {"type": "user_id", "value": user, "event_types": len(set(e.metric_type for e in events))}
                for user, events in suspicious_users
            ],
            severity=SecurityEventSeverity.MEDIUM,
            mitigation_recommendations=[
                "Conduct user behavior analysis",
                "Review user training and awareness",
                "Implement user activity monitoring",
                "Consider access review"
            ]
        )


class UserBehaviorAnalyzer:
    """Analyze user behavior for insider threat detection"""
    
    def __init__(self, security_metrics: SecurityMetricsCollector):
        self.security_metrics = security_metrics
        self.logger = get_logger(__name__)
        self.user_profiles: Dict[str, UserBehaviorProfile] = {}
        self.ml_models = {}
        self._initialize_ml_models()
    
    def _initialize_ml_models(self):
        """Initialize machine learning models for behavior analysis"""
        # Isolation Forest for anomaly detection
        self.ml_models['anomaly_detector'] = IsolationForest(
            contamination=0.1,  # Expect 10% anomalies
            random_state=42
        )
        
        # Feature scaler
        self.ml_models['scaler'] = StandardScaler()
    
    def analyze_user_behavior(
        self,
        user_id: str,
        analysis_period_days: int = 30
    ) -> UserBehaviorProfile:
        """Analyze behavior for a specific user"""
        
        cutoff_time = datetime.now() - timedelta(days=analysis_period_days)
        
        # Get user events
        user_events = [
            event for event in self.security_metrics.security_events
            if event.user_id == user_id and event.timestamp >= cutoff_time
        ]
        
        if not user_events:
            # Return default profile for new/inactive users
            return UserBehaviorProfile(
                user_id=user_id,
                risk_score=0.5,  # Neutral risk
                behavior_baseline={},
                anomaly_indicators=[],
                typical_access_times=[],
                typical_resources=[],
                typical_locations=[],
                failed_login_attempts=0,
                privilege_escalation_attempts=0,
                unusual_data_access=0,
                suspicious_ip_usage=0,
                profile_created=datetime.now(),
                last_updated=datetime.now(),
                observation_period_days=0
            )
        
        # Analyze access patterns
        access_times = [event.timestamp.hour for event in user_events]
        typical_access_times = self._find_typical_hours(access_times)
        
        resources = [event.resource_id for event in user_events if event.resource_id]
        typical_resources = list(set(resources))
        
        locations = [event.source_ip for event in user_events if event.source_ip]
        typical_locations = list(set(locations))
        
        # Count risk indicators
        failed_logins = len([
            e for e in user_events
            if e.metric_type == SecurityMetricType.AUTHENTICATION and e.outcome == "failure"
        ])
        
        privilege_escalations = len([
            e for e in user_events
            if e.metric_type == SecurityMetricType.ACCESS_CONTROL and 
            e.details.get('risk_score', 0) > 8
        ])
        
        data_access = len([
            e for e in user_events
            if e.metric_type == SecurityMetricType.DATA_ACCESS
        ])
        
        suspicious_ips = len(set([
            e.source_ip for e in user_events
            if e.source_ip and self._is_suspicious_ip(e.source_ip)
        ]))
        
        # Calculate behavior baseline
        behavior_baseline = {
            'avg_events_per_day': len(user_events) / analysis_period_days,
            'avg_access_time': statistics.mean(access_times) if access_times else 12,
            'resource_diversity': len(set(resources)),
            'location_diversity': len(set(locations)),
            'failure_rate': failed_logins / max(len(user_events), 1)
        }
        
        # Extract ML features
        ml_features = self._extract_ml_features(user_events, behavior_baseline)
        
        # Calculate anomaly score
        anomaly_score = self._calculate_anomaly_score(ml_features)
        
        # Identify anomaly indicators
        anomaly_indicators = self._identify_anomaly_indicators(
            user_events, behavior_baseline, ml_features
        )
        
        # Calculate overall risk score
        risk_score = self._calculate_user_risk_score(
            failed_logins, privilege_escalations, data_access,
            suspicious_ips, anomaly_score, anomaly_indicators
        )
        
        profile = UserBehaviorProfile(
            user_id=user_id,
            risk_score=risk_score,
            behavior_baseline=behavior_baseline,
            anomaly_indicators=anomaly_indicators,
            typical_access_times=typical_access_times,
            typical_resources=typical_resources,
            typical_locations=typical_locations,
            failed_login_attempts=failed_logins,
            privilege_escalation_attempts=privilege_escalations,
            unusual_data_access=data_access,
            suspicious_ip_usage=suspicious_ips,
            profile_created=self.user_profiles.get(user_id, UserBehaviorProfile(
                user_id=user_id, risk_score=0, behavior_baseline={},
                anomaly_indicators=[], typical_access_times=[], typical_resources=[],
                typical_locations=[], failed_login_attempts=0, privilege_escalation_attempts=0,
                unusual_data_access=0, suspicious_ip_usage=0, profile_created=datetime.now(),
                last_updated=datetime.now(), observation_period_days=0
            )).profile_created,
            last_updated=datetime.now(),
            observation_period_days=analysis_period_days,
            ml_features=ml_features,
            anomaly_score=anomaly_score
        )
        
        # Store profile
        self.user_profiles[user_id] = profile
        
        return profile
    
    def _find_typical_hours(self, access_times: List[int]) -> List[int]:
        """Find typical access hours for user"""
        if not access_times:
            return []
        
        # Count access by hour
        hour_counts = Counter(access_times)
        
        # Find hours with significant activity (>10% of total)
        total_accesses = len(access_times)
        threshold = max(1, total_accesses * 0.1)
        
        typical_hours = [
            hour for hour, count in hour_counts.items()
            if count >= threshold
        ]
        
        return sorted(typical_hours)
    
    def _is_suspicious_ip(self, ip_address: str) -> bool:
        """Check if IP address is suspicious"""
        # Simplified check - in reality would use threat intelligence
        # Check for private IP ranges
        private_ranges = ['10.', '192.168.', '172.']
        
        # If it's not a private IP, consider it potentially suspicious
        return not any(ip_address.startswith(prefix) for prefix in private_ranges)
    
    def _extract_ml_features(
        self,
        user_events: List[SecurityMetricEvent],
        baseline: Dict[str, float]
    ) -> Dict[str, float]:
        """Extract features for ML analysis"""
        
        features = {}
        
        # Temporal features
        hours = [event.timestamp.hour for event in user_events]
        features['hour_std'] = np.std(hours) if hours else 0
        features['hour_range'] = (max(hours) - min(hours)) if hours else 0
        
        # Activity features
        features['total_events'] = len(user_events)
        features['events_per_day'] = baseline.get('avg_events_per_day', 0)
        features['resource_diversity'] = baseline.get('resource_diversity', 0)
        features['location_diversity'] = baseline.get('location_diversity', 0)
        
        # Risk features
        features['failure_rate'] = baseline.get('failure_rate', 0)
        features['high_severity_events'] = len([
            e for e in user_events
            if e.severity in [SecurityEventSeverity.HIGH, SecurityEventSeverity.CRITICAL]
        ])
        
        # Event type diversity
        event_types = set(event.metric_type.value for event in user_events)
        features['event_type_diversity'] = len(event_types)
        
        return features
    
    def _calculate_anomaly_score(self, ml_features: Dict[str, float]) -> Optional[float]:
        """Calculate ML-based anomaly score"""
        
        if not ml_features:
            return None
        
        try:
            # Convert features to array
            feature_values = list(ml_features.values())
            feature_array = np.array(feature_values).reshape(1, -1)
            
            # Scale features
            scaled_features = self.ml_models['scaler'].fit_transform(feature_array)
            
            # Get anomaly score (lower scores indicate anomalies)
            anomaly_score = self.ml_models['anomaly_detector'].decision_function(scaled_features)[0]
            
            # Convert to 0-1 scale (higher = more anomalous)
            normalized_score = max(0, min(1, (1 - anomaly_score) / 2))
            
            return normalized_score
            
        except Exception as e:
            self.logger.error(f"Error calculating anomaly score: {e}")
            return None
    
    def _identify_anomaly_indicators(
        self,
        user_events: List[SecurityMetricEvent],
        baseline: Dict[str, float],
        ml_features: Dict[str, float]
    ) -> List[str]:
        """Identify specific anomaly indicators"""
        
        indicators = []
        
        # High failure rate
        if baseline.get('failure_rate', 0) > 0.2:
            indicators.append("high_authentication_failure_rate")
        
        # Unusual access times
        hours = [event.timestamp.hour for event in user_events]
        night_accesses = len([h for h in hours if h < 6 or h > 22])
        if night_accesses > len(hours) * 0.3:
            indicators.append("unusual_access_times")
        
        # High resource diversity
        if ml_features.get('resource_diversity', 0) > 20:
            indicators.append("high_resource_diversity")
        
        # Multiple locations
        if ml_features.get('location_diversity', 0) > 5:
            indicators.append("multiple_access_locations")
        
        # High-severity events
        if ml_features.get('high_severity_events', 0) > 5:
            indicators.append("frequent_high_severity_events")
        
        # Event type diversity
        if ml_features.get('event_type_diversity', 0) >= 4:
            indicators.append("diverse_security_events")
        
        return indicators
    
    def _calculate_user_risk_score(
        self,
        failed_logins: int,
        privilege_escalations: int,
        data_access: int,
        suspicious_ips: int,
        anomaly_score: Optional[float],
        anomaly_indicators: List[str]
    ) -> float:
        """Calculate overall user risk score (0-10)"""
        
        risk_score = 0.0
        
        # Failed login risk
        if failed_logins > 10:
            risk_score += 2.0
        elif failed_logins > 5:
            risk_score += 1.0
        
        # Privilege escalation risk
        risk_score += min(privilege_escalations * 0.5, 2.0)
        
        # Data access risk
        if data_access > 50:
            risk_score += 1.0
        
        # Suspicious IP risk
        risk_score += min(suspicious_ips * 0.5, 1.5)
        
        # ML anomaly score contribution
        if anomaly_score:
            risk_score += anomaly_score * 2.0
        
        # Anomaly indicators
        risk_score += len(anomaly_indicators) * 0.3
        
        return min(risk_score, 10.0)
    
    def get_high_risk_users(self, risk_threshold: float = 7.0) -> List[UserBehaviorProfile]:
        """Get users with high risk scores"""
        
        return [
            profile for profile in self.user_profiles.values()
            if profile.risk_score >= risk_threshold
        ]
    
    def compare_user_behaviors(
        self,
        user_id1: str,
        user_id2: str
    ) -> Dict[str, Any]:
        """Compare behavior profiles between two users"""
        
        profile1 = self.user_profiles.get(user_id1)
        profile2 = self.user_profiles.get(user_id2)
        
        if not profile1 or not profile2:
            return {"error": "One or both user profiles not found"}
        
        comparison = {
            'users': [user_id1, user_id2],
            'risk_scores': [profile1.risk_score, profile2.risk_score],
            'risk_difference': abs(profile1.risk_score - profile2.risk_score),
            'similar_access_times': len(set(profile1.typical_access_times) & 
                                      set(profile2.typical_access_times)),
            'common_resources': len(set(profile1.typical_resources) & 
                                  set(profile2.typical_resources)),
            'common_locations': len(set(profile1.typical_locations) & 
                                  set(profile2.typical_locations)),
            'unique_anomalies': {
                user_id1: list(set(profile1.anomaly_indicators) - 
                             set(profile2.anomaly_indicators)),
                user_id2: list(set(profile2.anomaly_indicators) - 
                             set(profile1.anomaly_indicators))
            }
        }
        
        return comparison


class SecurityReportGenerator:
    """Generate comprehensive security reports"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.security_metrics = get_security_metrics_collector()
        self.compliance_dashboard = get_compliance_dashboard()
        self.alerting_system = get_security_alerting_system()
        
        # Initialize analyzers
        self.posture_analyzer = SecurityPostureAnalyzer(self.security_metrics)
        self.attack_analyzer = AttackPatternAnalyzer(self.security_metrics)
        self.behavior_analyzer = UserBehaviorAnalyzer(self.security_metrics)
    
    async def generate_executive_summary_report(
        self,
        report_period_days: int = 30,
        format_type: ReportFormat = ReportFormat.JSON
    ) -> str:
        """Generate executive summary report"""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=report_period_days)
        
        # Collect data from various sources
        security_posture = self.posture_analyzer.analyze_security_posture_trends(
            AnalyticsTimeframe.DAILY, report_period_days
        )
        
        attack_patterns = self.attack_analyzer.analyze_attack_patterns(
            time_window_hours=report_period_days * 24
        )
        
        high_risk_users = self.behavior_analyzer.get_high_risk_users()
        
        alerting_stats = self.alerting_system.get_alert_statistics(
            hours=report_period_days * 24
        )
        
        compliance_data = await self.compliance_dashboard.get_dashboard_data()
        
        # Build executive summary
        report_data = {
            'report_type': 'Executive Security Summary',
            'generated_at': datetime.now().isoformat(),
            'period': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'days': report_period_days
            },
            'key_metrics': {
                'overall_security_score': self._calculate_overall_security_score(
                    security_posture, compliance_data, alerting_stats
                ),
                'total_security_events': sum(
                    len(trend.data_points) for trend in security_posture.values()
                ),
                'active_alerts': alerting_stats['active_alerts'],
                'compliance_score': compliance_data['overview']['overall_compliance_score'],
                'high_risk_users': len(high_risk_users),
                'attack_patterns_detected': len(attack_patterns)
            },
            'trends': {
                trend_name: {
                    'direction': trend.trend_direction,
                    'strength': trend.trend_strength,
                    'current_value': trend.current_value,
                    'change_percentage': trend.percentage_change
                }
                for trend_name, trend in security_posture.items()
            },
            'top_threats': [
                {
                    'pattern_name': pattern.pattern_name,
                    'severity': pattern.severity.value,
                    'confidence': pattern.confidence_score,
                    'affected_assets': len(pattern.affected_assets)
                }
                for pattern in sorted(attack_patterns, key=lambda p: p.confidence_score, reverse=True)[:5]
            ],
            'compliance_status': {
                framework: {
                    'score': data['overall_score'],
                    'status': data['status'],
                    'violations': data['active_violations']
                }
                for framework, data in compliance_data['frameworks'].items()
            },
            'recommendations': self._generate_executive_recommendations(
                security_posture, attack_patterns, compliance_data, alerting_stats
            )
        }
        
        if format_type == ReportFormat.JSON:
            return json.dumps(report_data, indent=2, default=str)
        elif format_type == ReportFormat.HTML:
            return self._generate_html_report(report_data)
        else:
            return json.dumps(report_data, default=str)
    
    async def generate_detailed_security_report(
        self,
        report_period_days: int = 7,
        include_user_analysis: bool = True,
        include_attack_details: bool = True,
        format_type: ReportFormat = ReportFormat.JSON
    ) -> str:
        """Generate detailed security analysis report"""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=report_period_days)
        
        # Detailed analysis
        security_trends = self.posture_analyzer.analyze_security_posture_trends(
            AnalyticsTimeframe.DAILY, report_period_days
        )
        
        attack_patterns = self.attack_analyzer.analyze_attack_patterns(
            time_window_hours=report_period_days * 24
        )
        
        alert_stats = self.alerting_system.get_alert_statistics(
            hours=report_period_days * 24
        )
        
        report_data = {
            'report_type': 'Detailed Security Analysis',
            'generated_at': datetime.now().isoformat(),
            'period': {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'days': report_period_days
            },
            'security_trends': {
                trend_name: {
                    'metric_name': trend.metric_name,
                    'timeframe': trend.timeframe.value,
                    'trend_direction': trend.trend_direction,
                    'trend_strength': trend.trend_strength,
                    'current_value': trend.current_value,
                    'previous_value': trend.previous_value,
                    'percentage_change': trend.percentage_change,
                    'confidence_interval': trend.confidence_interval,
                    'prediction_next_period': trend.prediction_next_period,
                    'data_points_count': len(trend.data_points)
                }
                for trend_name, trend in security_trends.items()
            },
            'alerting_analysis': {
                'total_alerts': alert_stats.get('recent_alerts', 0),
                'active_alerts': alert_stats.get('active_alerts', 0),
                'severity_breakdown': alert_stats.get('severity_breakdown', {}),
                'average_resolution_time_minutes': alert_stats.get('avg_resolution_time_minutes', 0),
                'escalation_rate': alert_stats.get('escalation_rate', 0),
                'notification_success_rate': alert_stats.get('notification_success_rate', 1.0)
            }
        }
        
        if include_attack_details:
            report_data['attack_patterns'] = [
                {
                    'pattern_id': pattern.pattern_id,
                    'pattern_name': pattern.pattern_name,
                    'description': pattern.description,
                    'tactics': pattern.tactics,
                    'techniques': pattern.techniques,
                    'confidence_score': pattern.confidence_score,
                    'severity': pattern.severity.value,
                    'first_observed': pattern.first_observed.isoformat(),
                    'last_observed': pattern.last_observed.isoformat(),
                    'frequency': pattern.frequency,
                    'affected_assets_count': len(pattern.affected_assets),
                    'indicators_count': len(pattern.indicators),
                    'mitigation_recommendations': pattern.mitigation_recommendations
                }
                for pattern in attack_patterns
            ]
        
        if include_user_analysis:
            high_risk_users = self.behavior_analyzer.get_high_risk_users()
            report_data['user_behavior_analysis'] = {
                'high_risk_users_count': len(high_risk_users),
                'high_risk_users': [
                    {
                        'user_id': profile.user_id,
                        'risk_score': profile.risk_score,
                        'anomaly_indicators': profile.anomaly_indicators,
                        'failed_login_attempts': profile.failed_login_attempts,
                        'privilege_escalation_attempts': profile.privilege_escalation_attempts,
                        'observation_period_days': profile.observation_period_days
                    }
                    for profile in high_risk_users[:10]  # Top 10 high-risk users
                ]
            }
        
        if format_type == ReportFormat.JSON:
            return json.dumps(report_data, indent=2, default=str)
        elif format_type == ReportFormat.CSV:
            return self._generate_csv_report(report_data)
        else:
            return json.dumps(report_data, default=str)
    
    async def generate_compliance_report(
        self,
        framework: ComplianceFramework,
        report_period_days: int = 90,
        format_type: ReportFormat = ReportFormat.JSON
    ) -> ComplianceReport:
        """Generate compliance-specific report"""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=report_period_days)
        
        # Get compliance dashboard data
        dashboard_data = await self.compliance_dashboard.get_dashboard_data()
        framework_data = dashboard_data['frameworks'].get(framework.value, {})
        
        # Generate detailed compliance report
        detailed_report = self.compliance_dashboard.generate_compliance_report(
            framework, start_date, end_date, "json"
        )
        
        detailed_data = json.loads(detailed_report)
        
        # Analyze compliance trends
        compliance_trend = SecurityTrend(
            metric_name=f"{framework.value}_compliance_score",
            timeframe=AnalyticsTimeframe.DAILY,
            trend_direction="stable",  # Would be calculated from historical data
            trend_strength=0.1,
            current_value=framework_data.get('overall_score', 0),
            previous_value=framework_data.get('overall_score', 0),
            percentage_change=0.0,
            data_points=[],
            confidence_interval=(0, 0)
        )
        
        report = ComplianceReport(
            report_id=f"compliance_{framework.value}_{int(datetime.now().timestamp())}",
            framework=framework,
            report_type="Compliance Assessment",
            generated_at=datetime.now(),
            period_start=start_date,
            period_end=end_date,
            overall_compliance_score=framework_data.get('overall_score', 0),
            total_controls_assessed=framework_data.get('total_checks', 0),
            controls_compliant=framework_data.get('passed_checks', 0),
            controls_non_compliant=framework_data.get('failed_checks', 0),
            violations=detailed_data['detailed_violations'],
            remediation_items=[
                {
                    'violation_id': v['violation_id'],
                    'priority': 'high' if v['severity'] == 'critical' else 'medium',
                    'estimated_effort': 'TBD',
                    'deadline': v['remediation_deadline']
                }
                for v in detailed_data['detailed_violations']
                if v['status'] != 'resolved'
            ],
            risk_assessment={
                'overall_risk_level': self._assess_compliance_risk_level(framework_data),
                'key_risk_areas': self._identify_key_risk_areas(detailed_data),
                'risk_mitigation_priority': self._prioritize_risk_mitigation(detailed_data)
            },
            compliance_trend=compliance_trend,
            priority_actions=self._generate_compliance_actions(framework, detailed_data),
            resource_requirements=self._estimate_resource_requirements(detailed_data)
        )
        
        return report
    
    def _calculate_overall_security_score(
        self,
        security_posture: Dict[str, SecurityTrend],
        compliance_data: Dict[str, Any],
        alerting_stats: Dict[str, Any]
    ) -> float:
        """Calculate overall security score (0-100)"""
        
        scores = []
        
        # Security posture scores
        for trend in security_posture.values():
            if trend.current_value:
                scores.append(min(100, trend.current_value))
        
        # Compliance score
        compliance_score = compliance_data['overview']['overall_compliance_score']
        scores.append(compliance_score)
        
        # Alerting effectiveness score
        alert_score = 100 - (alerting_stats.get('active_alerts', 0) * 5)  # Penalty for active alerts
        scores.append(max(0, alert_score))
        
        return statistics.mean(scores) if scores else 50.0
    
    def _generate_executive_recommendations(
        self,
        security_posture: Dict[str, SecurityTrend],
        attack_patterns: List[AttackPattern],
        compliance_data: Dict[str, Any],
        alerting_stats: Dict[str, Any]
    ) -> List[str]:
        """Generate executive-level recommendations"""
        
        recommendations = []
        
        # Security posture recommendations
        for trend_name, trend in security_posture.items():
            if trend.trend_direction == "increasing" and "violation" in trend_name:
                recommendations.append(f"Address increasing {trend_name} trend")
            elif trend.trend_strength > 0.7 and trend.trend_direction == "decreasing" and "score" in trend_name:
                recommendations.append(f"Investigate declining {trend_name}")
        
        # Attack pattern recommendations
        critical_patterns = [p for p in attack_patterns if p.severity == SecurityEventSeverity.CRITICAL]
        if critical_patterns:
            recommendations.append(f"Immediate attention required for {len(critical_patterns)} critical attack patterns")
        
        # Compliance recommendations
        non_compliant_frameworks = [
            fw for fw, data in compliance_data['frameworks'].items()
            if data['overall_score'] < 80
        ]
        if non_compliant_frameworks:
            recommendations.append(f"Improve compliance for {len(non_compliant_frameworks)} frameworks")
        
        # Alerting recommendations
        if alerting_stats['active_alerts'] > 10:
            recommendations.append("High number of active alerts requires attention")
        
        if not recommendations:
            recommendations.append("Security posture is within acceptable parameters")
        
        return recommendations
    
    def _assess_compliance_risk_level(self, framework_data: Dict[str, Any]) -> str:
        """Assess compliance risk level"""
        score = framework_data.get('overall_score', 0)
        violations = framework_data.get('active_violations', 0)
        
        if score < 70 or violations > 10:
            return "high"
        elif score < 85 or violations > 5:
            return "medium"
        else:
            return "low"
    
    def _identify_key_risk_areas(self, detailed_data: Dict[str, Any]) -> List[str]:
        """Identify key compliance risk areas"""
        violations_by_type = detailed_data.get('violations_by_type', {})
        
        # Sort by frequency
        sorted_types = sorted(violations_by_type.items(), key=lambda x: x[1], reverse=True)
        
        return [violation_type for violation_type, count in sorted_types[:5]]
    
    def _prioritize_risk_mitigation(self, detailed_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Prioritize risk mitigation actions"""
        violations = detailed_data.get('detailed_violations', [])
        
        # Sort by severity and age
        priority_violations = sorted(
            violations,
            key=lambda v: (
                {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}.get(v['severity'], 0),
                -v.get('days_to_resolve', 0)
            ),
            reverse=True
        )
        
        return [
            {
                'violation_type': v['violation_type'],
                'severity': v['severity'],
                'priority_score': idx + 1
            }
            for idx, v in enumerate(priority_violations[:10])
        ]
    
    def _generate_compliance_actions(
        self,
        framework: ComplianceFramework,
        detailed_data: Dict[str, Any]
    ) -> List[str]:
        """Generate compliance-specific actions"""
        
        actions = []
        
        violations_by_type = detailed_data.get('violations_by_type', {})
        
        # Framework-specific actions
        if framework == ComplianceFramework.GDPR:
            if 'data_processing' in violations_by_type:
                actions.append("Review and update data processing agreements")
            if 'consent_management' in violations_by_type:
                actions.append("Implement proper consent management system")
        
        elif framework == ComplianceFramework.PCI_DSS:
            if 'cardholder_data' in violations_by_type:
                actions.append("Secure cardholder data storage and transmission")
            if 'access_control' in violations_by_type:
                actions.append("Implement strong access control measures")
        
        elif framework == ComplianceFramework.HIPAA:
            if 'phi_protection' in violations_by_type:
                actions.append("Strengthen PHI protection controls")
            if 'audit_trails' in violations_by_type:
                actions.append("Improve audit trail mechanisms")
        
        # General actions
        overdue_count = len([v for v in detailed_data.get('detailed_violations', []) if v.get('days_to_resolve', 0) > 0])
        if overdue_count > 0:
            actions.append(f"Address {overdue_count} overdue remediation items")
        
        return actions
    
    def _estimate_resource_requirements(self, detailed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Estimate resource requirements for compliance improvement"""
        
        violations = detailed_data.get('detailed_violations', [])
        
        return {
            'estimated_hours': len(violations) * 8,  # 8 hours per violation
            'personnel_required': max(1, len(violations) // 10),  # 1 person per 10 violations
            'estimated_cost': len(violations) * 1000,  # $1000 per violation
            'timeline_weeks': max(4, len(violations) // 5)  # 1 week per 5 violations, minimum 4 weeks
        }
    
    def _generate_html_report(self, report_data: Dict[str, Any]) -> str:
        """Generate HTML formatted report"""
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{report_data['report_type']}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f4f4f4; padding: 20px; }}
                .section {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; }}
                .metric {{ display: inline-block; margin: 10px; padding: 10px; background-color: #e9ecef; }}
                .high-risk {{ color: #dc3545; }}
                .medium-risk {{ color: #fd7e14; }}
                .low-risk {{ color: #28a745; }}
                table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f8f9fa; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>{report_data['report_type']}</h1>
                <p>Generated: {report_data['generated_at']}</p>
                <p>Period: {report_data['period']['start_date']} to {report_data['period']['end_date']}</p>
            </div>
            
            <div class="section">
                <h2>Key Security Metrics</h2>
                <div class="metric">
                    <strong>Overall Security Score:</strong> 
                    {report_data['key_metrics']['overall_security_score']:.1f}/100
                </div>
                <div class="metric">
                    <strong>Active Alerts:</strong> 
                    {report_data['key_metrics']['active_alerts']}
                </div>
                <div class="metric">
                    <strong>Compliance Score:</strong> 
                    {report_data['key_metrics']['compliance_score']:.1f}%
                </div>
            </div>
            
            <div class="section">
                <h2>Security Trends</h2>
                <table>
                    <tr><th>Metric</th><th>Direction</th><th>Current Value</th><th>Change %</th></tr>
        """
        
        for trend_name, trend in report_data.get('trends', {}).items():
            direction_class = "low-risk" if trend['direction'] == "stable" else "medium-risk"
            html_content += f"""
                    <tr>
                        <td>{trend_name}</td>
                        <td class="{direction_class}">{trend['direction']}</td>
                        <td>{trend['current_value']:.2f}</td>
                        <td>{trend['change_percentage']:.1f}%</td>
                    </tr>
            """
        
        html_content += """
                </table>
            </div>
            
            <div class="section">
                <h2>Recommendations</h2>
                <ul>
        """
        
        for recommendation in report_data.get('recommendations', []):
            html_content += f"<li>{recommendation}</li>"
        
        html_content += """
                </ul>
            </div>
        </body>
        </html>
        """
        
        return html_content
    
    def _generate_csv_report(self, report_data: Dict[str, Any]) -> str:
        """Generate CSV formatted report"""
        
        import csv
        import io
        
        output = io.StringIO()
        
        # Write header
        output.write(f"Report Type,{report_data['report_type']}\n")
        output.write(f"Generated At,{report_data['generated_at']}\n")
        output.write(f"Period,{report_data['period']['start_date']} to {report_data['period']['end_date']}\n")
        output.write("\n")
        
        # Key metrics
        output.write("Key Metrics\n")
        for metric, value in report_data.get('key_metrics', {}).items():
            output.write(f"{metric},{value}\n")
        output.write("\n")
        
        # Security trends
        output.write("Security Trends\n")
        output.write("Metric,Direction,Current Value,Change Percentage\n")
        for trend_name, trend in report_data.get('trends', {}).items():
            output.write(f"{trend_name},{trend['direction']},{trend['current_value']},{trend['change_percentage']}\n")
        
        return output.getvalue()


# Global analytics instance
_security_analytics: Optional[SecurityReportGenerator] = None

def get_security_analytics() -> SecurityReportGenerator:
    """Get global security analytics instance"""
    global _security_analytics
    if _security_analytics is None:
        _security_analytics = SecurityReportGenerator()
    return _security_analytics
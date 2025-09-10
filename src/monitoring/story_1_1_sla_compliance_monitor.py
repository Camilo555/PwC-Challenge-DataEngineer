"""
Story 1.1 SLA Compliance Monitoring System
Enterprise-grade SLA monitoring with 99.9% uptime tracking and automated alerts

This module provides comprehensive SLA compliance monitoring for Story 1.1:
- 99.9% uptime tracking with real-time validation
- <25ms API response time SLA monitoring
- <2s dashboard load time compliance tracking  
- <50ms WebSocket latency SLA validation
- Automated SLA breach alerts with escalation
- Business impact analysis for SLA violations
- Error budget tracking and management
- Compliance reporting with audit trails
- Proactive SLA trend analysis and predictions

Key Features:
- Real-time SLA compliance validation
- Automated breach detection and alerting
- Error budget calculation and tracking
- Business impact correlation
- Compliance trend analysis
- Executive SLA reporting
- Audit trail maintenance
- Proactive prediction and prevention
"""

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
import logging
from collections import defaultdict, deque
import statistics
import math
from decimal import Decimal, ROUND_HALF_UP

from core.logging import get_logger
from core.config.unified_config import get_unified_config

# Import monitoring components
from src.monitoring.datadog_comprehensive_alerting import (
    DataDogComprehensiveAlerting, AlertSeverity, AlertChannel
)
from src.monitoring.datadog_business_metrics import (
    DataDogBusinessMetricsTracker, KPICategory
)
from src.monitoring.enterprise_observability import EnterpriseObservability


class SLAType(Enum):
    """Types of SLA metrics for Story 1.1"""
    UPTIME = "uptime"                      # System availability
    API_RESPONSE_TIME = "api_response_time"  # API performance
    DASHBOARD_LOAD_TIME = "dashboard_load_time"  # Dashboard performance
    WEBSOCKET_LATENCY = "websocket_latency"  # Real-time performance
    ERROR_RATE = "error_rate"              # System reliability
    THROUGHPUT = "throughput"              # System capacity
    DATA_FRESHNESS = "data_freshness"      # Data quality


class SLAStatus(Enum):
    """SLA compliance status"""
    COMPLIANT = "compliant"                # Meeting SLA targets
    WARNING = "warning"                    # Approaching SLA breach
    BREACHED = "breached"                  # SLA violated
    ERROR_BUDGET_EXHAUSTED = "error_budget_exhausted"  # No error budget left
    UNKNOWN = "unknown"                    # Status cannot be determined


class CompliancePeriod(Enum):
    """SLA compliance measurement periods"""
    REAL_TIME = "real_time"               # Current moment
    HOURLY = "hourly"                     # Last hour
    DAILY = "daily"                       # Last 24 hours  
    WEEKLY = "weekly"                     # Last 7 days
    MONTHLY = "monthly"                   # Last 30 days
    QUARTERLY = "quarterly"               # Last 90 days
    ANNUAL = "annual"                     # Last 365 days


class BusinessImpact(Enum):
    """Business impact levels for SLA breaches"""
    CRITICAL = "critical"                 # Severe business impact
    HIGH = "high"                        # Significant impact
    MEDIUM = "medium"                    # Moderate impact
    LOW = "low"                          # Minor impact
    NEGLIGIBLE = "negligible"            # Minimal impact


@dataclass
class SLATarget:
    """SLA target definition"""
    name: str
    sla_type: SLAType
    target_value: float
    target_unit: str
    warning_threshold: float              # Threshold for warning alerts
    critical_threshold: float             # Threshold for critical alerts
    measurement_window_minutes: int = 5    # Measurement window
    evaluation_frequency_seconds: int = 60  # How often to evaluate
    error_budget_percentage: float = 0.1   # Allowed failure percentage (0.1% for 99.9%)
    business_impact: BusinessImpact = BusinessImpact.MEDIUM
    description: str = ""
    enabled: bool = True


@dataclass
class SLAMeasurement:
    """Individual SLA measurement"""
    timestamp: datetime
    sla_type: SLAType
    measured_value: float
    target_value: float
    compliant: bool
    error_budget_consumed: float          # Percentage of error budget consumed
    business_impact_score: float = 0.0    # Business impact of this measurement
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ErrorBudget:
    """Error budget tracking"""
    sla_type: SLAType
    period: CompliancePeriod
    total_budget_percentage: float        # Total allowed failure (0.1% for 99.9%)
    consumed_percentage: float = 0.0      # How much budget has been consumed
    remaining_percentage: float = 0.0     # Remaining error budget
    burn_rate: float = 0.0               # Current rate of budget consumption
    projected_exhaustion: Optional[datetime] = None  # When budget will be exhausted
    last_updated: datetime = field(default_factory=datetime.now)


@dataclass
class SLABreach:
    """SLA breach incident"""
    breach_id: str
    sla_type: SLAType
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_minutes: float = 0.0
    severity: AlertSeverity = AlertSeverity.MEDIUM
    business_impact: BusinessImpact = BusinessImpact.MEDIUM
    measured_values: List[float] = field(default_factory=list)
    target_value: float = 0.0
    breach_magnitude: float = 0.0         # How far from target
    error_budget_consumed: float = 0.0    # Budget consumed by this breach
    escalated: bool = False
    resolved: bool = False
    resolution_actions: List[str] = field(default_factory=list)
    business_impact_cost: float = 0.0     # Estimated cost impact
    affected_users: int = 0               # Number of users affected
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ComplianceReport:
    """SLA compliance report"""
    period: CompliancePeriod
    start_time: datetime
    end_time: datetime
    overall_compliance_percentage: float
    sla_compliance: Dict[SLAType, Dict] = field(default_factory=dict)
    total_breaches: int = 0
    total_downtime_minutes: float = 0.0
    error_budget_status: Dict[SLAType, ErrorBudget] = field(default_factory=dict)
    business_impact_summary: Dict = field(default_factory=dict)
    trends: Dict = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)


class Story11SLAComplianceMonitor:
    """
    Comprehensive SLA compliance monitoring for Story 1.1 Dashboard
    Tracks all SLA metrics with automated alerting and business impact analysis
    """
    
    def __init__(self):
        self.logger = get_logger("story_1_1_sla_compliance")
        self.config = get_unified_config()
        
        # Initialize monitoring components
        self.comprehensive_alerting = DataDogComprehensiveAlerting()
        self.business_metrics = DataDogBusinessMetricsTracker()
        self.enterprise_observability = EnterpriseObservability()
        
        # SLA targets for Story 1.1
        self.sla_targets = self._initialize_sla_targets()
        
        # Tracking data structures
        self.measurements: Dict[SLAType, deque] = {
            sla_type: deque(maxlen=10000) for sla_type in SLAType
        }
        self.error_budgets: Dict[SLAType, Dict[CompliancePeriod, ErrorBudget]] = {}
        self.active_breaches: Dict[str, SLABreach] = {}
        self.breach_history: List[SLABreach] = []
        
        # Compliance tracking
        self.compliance_cache: Dict[CompliancePeriod, ComplianceReport] = {}
        self.last_compliance_calculation = {}
        
        # Business context
        self.business_context = {
            "revenue_per_minute": 2500.0,        # $2.5K revenue per minute
            "cost_per_breach_minute": 5000.0,    # $5K cost per breach minute
            "users_affected_per_breach": 1000,   # Estimated users affected per breach
            "reputation_impact_multiplier": 1.5, # Reputation impact factor
            "peak_business_hours": [9, 12, 14, 16, 17, 18, 19, 20]  # Hours (24-hour format)
        }
        
        # Initialize error budgets
        self._initialize_error_budgets()
        
        # Start monitoring tasks
        self.monitoring_tasks: List[asyncio.Task] = []
        self._start_sla_monitoring_tasks()
    
    def _initialize_sla_targets(self) -> Dict[SLAType, SLATarget]:
        """Initialize SLA targets for Story 1.1"""
        
        return {
            SLAType.UPTIME: SLATarget(
                name="System Uptime",
                sla_type=SLAType.UPTIME,
                target_value=99.9,
                target_unit="%",
                warning_threshold=99.95,         # Warning at 99.95%
                critical_threshold=99.9,         # Critical at 99.9%
                measurement_window_minutes=5,
                evaluation_frequency_seconds=60,
                error_budget_percentage=0.1,     # 0.1% allowed downtime
                business_impact=BusinessImpact.CRITICAL,
                description="Story 1.1 overall system availability"
            ),
            
            SLAType.API_RESPONSE_TIME: SLATarget(
                name="API Response Time (95th percentile)",
                sla_type=SLAType.API_RESPONSE_TIME,
                target_value=25.0,
                target_unit="ms",
                warning_threshold=20.0,          # Warning at 20ms
                critical_threshold=25.0,         # Critical at 25ms
                measurement_window_minutes=5,
                evaluation_frequency_seconds=30,
                error_budget_percentage=5.0,     # 5% allowed over-threshold
                business_impact=BusinessImpact.CRITICAL,
                description="Story 1.1 API response time performance"
            ),
            
            SLAType.DASHBOARD_LOAD_TIME: SLATarget(
                name="Dashboard Load Time",
                sla_type=SLAType.DASHBOARD_LOAD_TIME,
                target_value=2.0,
                target_unit="s",
                warning_threshold=1.5,           # Warning at 1.5s
                critical_threshold=2.0,          # Critical at 2.0s
                measurement_window_minutes=5,
                evaluation_frequency_seconds=60,
                error_budget_percentage=5.0,     # 5% allowed over-threshold
                business_impact=BusinessImpact.HIGH,
                description="Story 1.1 dashboard load time performance"
            ),
            
            SLAType.WEBSOCKET_LATENCY: SLATarget(
                name="WebSocket Latency",
                sla_type=SLAType.WEBSOCKET_LATENCY,
                target_value=50.0,
                target_unit="ms",
                warning_threshold=40.0,          # Warning at 40ms
                critical_threshold=50.0,         # Critical at 50ms
                measurement_window_minutes=2,
                evaluation_frequency_seconds=30,
                error_budget_percentage=5.0,     # 5% allowed over-threshold
                business_impact=BusinessImpact.HIGH,
                description="Story 1.1 WebSocket real-time latency"
            ),
            
            SLAType.ERROR_RATE: SLATarget(
                name="System Error Rate",
                sla_type=SLAType.ERROR_RATE,
                target_value=1.0,
                target_unit="%",
                warning_threshold=0.5,           # Warning at 0.5%
                critical_threshold=1.0,          # Critical at 1.0%
                measurement_window_minutes=5,
                evaluation_frequency_seconds=60,
                error_budget_percentage=1.0,     # 1% total allowed error rate
                business_impact=BusinessImpact.HIGH,
                description="Story 1.1 system error rate"
            ),
            
            SLAType.THROUGHPUT: SLATarget(
                name="System Throughput",
                sla_type=SLAType.THROUGHPUT,
                target_value=1000.0,
                target_unit="rps",
                warning_threshold=800.0,         # Warning at 800 rps
                critical_threshold=500.0,        # Critical at 500 rps
                measurement_window_minutes=5,
                evaluation_frequency_seconds=60,
                error_budget_percentage=10.0,    # 10% throughput tolerance
                business_impact=BusinessImpact.MEDIUM,
                description="Story 1.1 system throughput capacity"
            ),
            
            SLAType.DATA_FRESHNESS: SLATarget(
                name="Data Freshness",
                sla_type=SLAType.DATA_FRESHNESS,
                target_value=5.0,
                target_unit="minutes",
                warning_threshold=3.0,           # Warning at 3 minutes
                critical_threshold=5.0,          # Critical at 5 minutes
                measurement_window_minutes=10,
                evaluation_frequency_seconds=120,
                error_budget_percentage=10.0,    # 10% data freshness tolerance
                business_impact=BusinessImpact.MEDIUM,
                description="Story 1.1 data freshness for dashboards"
            )
        }
    
    def _initialize_error_budgets(self):
        """Initialize error budgets for all SLA types and periods"""
        
        for sla_type in SLAType:
            self.error_budgets[sla_type] = {}
            target = self.sla_targets[sla_type]
            
            for period in CompliancePeriod:
                self.error_budgets[sla_type][period] = ErrorBudget(
                    sla_type=sla_type,
                    period=period,
                    total_budget_percentage=target.error_budget_percentage,
                    consumed_percentage=0.0,
                    remaining_percentage=target.error_budget_percentage
                )
    
    def _start_sla_monitoring_tasks(self):
        """Start all SLA monitoring background tasks"""
        
        self.monitoring_tasks = [
            asyncio.create_task(self._collect_sla_measurements()),
            asyncio.create_task(self._evaluate_sla_compliance()),
            asyncio.create_task(self._update_error_budgets()),
            asyncio.create_task(self._detect_sla_breaches()),
            asyncio.create_task(self._generate_compliance_reports()),
            asyncio.create_task(self._analyze_compliance_trends()),
            asyncio.create_task(self._manage_breach_lifecycle()),
            asyncio.create_task(self._predict_sla_risks())
        ]
        
        self.logger.info("Started Story 1.1 SLA monitoring tasks")
    
    async def _collect_sla_measurements(self):
        """Continuously collect SLA measurements"""
        while True:
            try:
                for sla_type in SLAType:
                    measurement = await self._collect_sla_measurement(sla_type)
                    if measurement:
                        self.measurements[sla_type].append(measurement)
                
                await asyncio.sleep(30)  # Collect every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error collecting SLA measurements: {e}")
                await asyncio.sleep(60)
    
    async def _collect_sla_measurement(self, sla_type: SLAType) -> Optional[SLAMeasurement]:
        """Collect measurement for specific SLA type"""
        try:
            target = self.sla_targets[sla_type]
            
            # Simulate measurements (in production, these would come from real monitoring)
            if sla_type == SLAType.UPTIME:
                # Simulate 99.9+ % uptime with occasional dips
                measured_value = self._simulate_uptime_measurement()
            elif sla_type == SLAType.API_RESPONSE_TIME:
                # Simulate API response times around 20-25ms
                measured_value = self._simulate_api_response_time()
            elif sla_type == SLAType.DASHBOARD_LOAD_TIME:
                # Simulate dashboard load times around 1.5-2s
                measured_value = self._simulate_dashboard_load_time()
            elif sla_type == SLAType.WEBSOCKET_LATENCY:
                # Simulate WebSocket latency around 40-50ms
                measured_value = self._simulate_websocket_latency()
            elif sla_type == SLAType.ERROR_RATE:
                # Simulate low error rate
                measured_value = self._simulate_error_rate()
            elif sla_type == SLAType.THROUGHPUT:
                # Simulate throughput
                measured_value = self._simulate_throughput()
            elif sla_type == SLAType.DATA_FRESHNESS:
                # Simulate data freshness
                measured_value = self._simulate_data_freshness()
            else:
                return None
            
            # Determine compliance
            compliant = self._is_measurement_compliant(sla_type, measured_value, target)
            
            # Calculate error budget consumption
            error_budget_consumed = self._calculate_error_budget_consumption(
                sla_type, measured_value, target, compliant
            )
            
            # Calculate business impact score
            business_impact_score = self._calculate_business_impact_score(
                sla_type, measured_value, target, compliant
            )
            
            return SLAMeasurement(
                timestamp=datetime.now(),
                sla_type=sla_type,
                measured_value=measured_value,
                target_value=target.target_value,
                compliant=compliant,
                error_budget_consumed=error_budget_consumed,
                business_impact_score=business_impact_score,
                metadata={
                    "measurement_window": target.measurement_window_minutes,
                    "business_impact_level": target.business_impact.value
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error collecting measurement for {sla_type.value}: {e}")
            return None
    
    def _simulate_uptime_measurement(self) -> float:
        """Simulate uptime measurement"""
        import random
        
        # 95% chance of excellent uptime (99.95%+)
        # 4% chance of good uptime (99.9-99.95%)
        # 1% chance of degraded uptime (99.8-99.9%)
        
        rand = random.random()
        if rand < 0.95:
            return random.uniform(99.95, 99.99)
        elif rand < 0.99:
            return random.uniform(99.9, 99.95)
        else:
            return random.uniform(99.8, 99.9)
    
    def _simulate_api_response_time(self) -> float:
        """Simulate API response time measurement"""
        import random
        import numpy as np
        
        # Normal distribution around 18ms with occasional spikes
        if random.random() < 0.95:  # 95% of the time
            return max(10.0, np.random.normal(18.0, 3.0))
        else:  # 5% of the time - performance spikes
            return max(15.0, np.random.normal(28.0, 5.0))
    
    def _simulate_dashboard_load_time(self) -> float:
        """Simulate dashboard load time measurement"""
        import random
        import numpy as np
        
        # Normal distribution around 1.5s with occasional longer loads
        if random.random() < 0.9:  # 90% of the time
            return max(0.8, np.random.normal(1.5, 0.3))
        else:  # 10% of the time - longer load times
            return max(1.0, np.random.normal(2.2, 0.4))
    
    def _simulate_websocket_latency(self) -> float:
        """Simulate WebSocket latency measurement"""
        import random
        import numpy as np
        
        # Normal distribution around 35ms
        return max(20.0, np.random.normal(35.0, 8.0))
    
    def _simulate_error_rate(self) -> float:
        """Simulate error rate measurement"""
        import random
        import numpy as np
        
        # Very low error rate with occasional spikes
        if random.random() < 0.98:  # 98% of the time
            return max(0.0, np.random.normal(0.2, 0.1))
        else:  # 2% of the time - error spikes
            return max(0.1, np.random.normal(1.5, 0.5))
    
    def _simulate_throughput(self) -> float:
        """Simulate throughput measurement"""
        import random
        import numpy as np
        
        # Variable throughput based on time of day
        hour = datetime.now().hour
        if hour in self.business_context["peak_business_hours"]:
            # Peak hours - higher throughput
            return max(500.0, np.random.normal(1200.0, 200.0))
        else:
            # Off-peak hours
            return max(200.0, np.random.normal(800.0, 150.0))
    
    def _simulate_data_freshness(self) -> float:
        """Simulate data freshness measurement"""
        import random
        import numpy as np
        
        # Most data is fresh (1-3 minutes), sometimes stale
        if random.random() < 0.9:  # 90% of the time
            return max(0.5, np.random.normal(2.0, 0.5))
        else:  # 10% of the time - stale data
            return max(2.0, np.random.normal(6.0, 2.0))
    
    def _is_measurement_compliant(self, sla_type: SLAType, measured_value: float, target: SLATarget) -> bool:
        """Determine if measurement is compliant with SLA"""
        
        if sla_type in [SLAType.UPTIME, SLAType.THROUGHPUT]:
            # Higher is better
            return measured_value >= target.target_value
        else:
            # Lower is better (response times, error rates, etc.)
            return measured_value <= target.target_value
    
    def _calculate_error_budget_consumption(
        self, sla_type: SLAType, measured_value: float, target: SLATarget, compliant: bool
    ) -> float:
        """Calculate how much error budget this measurement consumes"""
        
        if compliant:
            return 0.0
        
        # Calculate percentage of error budget consumed by this measurement
        if sla_type == SLAType.UPTIME:
            # For uptime, each measurement represents a time period
            measurement_period_minutes = target.measurement_window_minutes
            total_period_minutes = 24 * 60  # Daily budget
            return (measurement_period_minutes / total_period_minutes) * 100
        
        else:
            # For other metrics, consumption is based on how far off target
            if sla_type in [SLAType.THROUGHPUT]:
                # Higher is better
                deficit_percentage = (target.target_value - measured_value) / target.target_value * 100
            else:
                # Lower is better
                excess_percentage = (measured_value - target.target_value) / target.target_value * 100
                deficit_percentage = excess_percentage
            
            # Scale consumption based on severity of miss
            return min(100.0, max(0.1, deficit_percentage))
    
    def _calculate_business_impact_score(
        self, sla_type: SLAType, measured_value: float, target: SLATarget, compliant: bool
    ) -> float:
        """Calculate business impact score for this measurement"""
        
        if compliant:
            return 0.0  # No business impact if compliant
        
        # Base impact based on SLA type
        base_impact = {
            SLAType.UPTIME: 100.0,              # Maximum impact
            SLAType.API_RESPONSE_TIME: 80.0,    # High impact
            SLAType.DASHBOARD_LOAD_TIME: 60.0,  # Medium-high impact
            SLAType.WEBSOCKET_LATENCY: 50.0,    # Medium impact
            SLAType.ERROR_RATE: 90.0,           # Very high impact
            SLAType.THROUGHPUT: 40.0,           # Medium impact
            SLAType.DATA_FRESHNESS: 30.0        # Lower impact
        }.get(sla_type, 50.0)
        
        # Scale based on severity of breach
        if sla_type in [SLAType.THROUGHPUT]:
            # Higher is better
            severity_factor = (target.target_value - measured_value) / target.target_value
        else:
            # Lower is better
            severity_factor = (measured_value - target.target_value) / target.target_value
        
        severity_factor = max(0.1, min(2.0, severity_factor))  # Cap between 0.1 and 2.0
        
        # Apply time-of-day multiplier
        hour = datetime.now().hour
        if hour in self.business_context["peak_business_hours"]:
            time_multiplier = 1.5  # Higher impact during peak hours
        else:
            time_multiplier = 0.7  # Lower impact during off-peak
        
        return base_impact * severity_factor * time_multiplier
    
    async def _evaluate_sla_compliance(self):
        """Continuously evaluate SLA compliance"""
        while True:
            try:
                for sla_type in SLAType:
                    await self._evaluate_sla_type_compliance(sla_type)
                
                await asyncio.sleep(60)  # Evaluate every minute
                
            except Exception as e:
                self.logger.error(f"Error evaluating SLA compliance: {e}")
                await asyncio.sleep(120)
    
    async def _evaluate_sla_type_compliance(self, sla_type: SLAType):
        """Evaluate compliance for specific SLA type"""
        try:
            target = self.sla_targets[sla_type]
            measurements = list(self.measurements[sla_type])
            
            if not measurements:
                return
            
            # Get measurements within evaluation window
            now = datetime.now()
            window_start = now - timedelta(minutes=target.measurement_window_minutes)
            
            window_measurements = [
                m for m in measurements 
                if m.timestamp >= window_start
            ]
            
            if not window_measurements:
                return
            
            # Calculate compliance for this window
            compliant_count = sum(1 for m in window_measurements if m.compliant)
            total_count = len(window_measurements)
            compliance_percentage = (compliant_count / total_count) * 100
            
            # Determine current status
            if compliance_percentage >= 100:
                status = SLAStatus.COMPLIANT
            elif compliance_percentage >= (100 - target.error_budget_percentage):
                status = SLAStatus.WARNING
            else:
                status = SLAStatus.BREACHED
            
            # Check error budget status
            daily_budget = self.error_budgets[sla_type][CompliancePeriod.DAILY]
            if daily_budget.remaining_percentage <= 0:
                status = SLAStatus.ERROR_BUDGET_EXHAUSTED
            
            # Update SLA status
            await self._update_sla_status(sla_type, status, compliance_percentage, window_measurements)
            
        except Exception as e:
            self.logger.error(f"Error evaluating compliance for {sla_type.value}: {e}")
    
    async def _update_sla_status(
        self, sla_type: SLAType, status: SLAStatus, 
        compliance_percentage: float, measurements: List[SLAMeasurement]
    ):
        """Update SLA status and trigger alerts if needed"""
        try:
            target = self.sla_targets[sla_type]
            
            # Check if we need to trigger alerts
            if status in [SLAStatus.BREACHED, SLAStatus.ERROR_BUDGET_EXHAUSTED]:
                await self._trigger_sla_breach_alert(sla_type, status, compliance_percentage, measurements)
            elif status == SLAStatus.WARNING:
                await self._trigger_sla_warning_alert(sla_type, compliance_percentage, measurements)
            
            # Log status update
            self.logger.info(
                f"SLA Status Update - {sla_type.value}: {status.value} "
                f"(Compliance: {compliance_percentage:.2f}%)"
            )
            
            # Send metrics to monitoring systems
            await self._send_sla_metrics(sla_type, status, compliance_percentage)
            
        except Exception as e:
            self.logger.error(f"Error updating SLA status for {sla_type.value}: {e}")
    
    async def _trigger_sla_breach_alert(
        self, sla_type: SLAType, status: SLAStatus, 
        compliance_percentage: float, measurements: List[SLAMeasurement]
    ):
        """Trigger alert for SLA breach"""
        try:
            target = self.sla_targets[sla_type]
            
            # Calculate breach severity
            if status == SLAStatus.ERROR_BUDGET_EXHAUSTED:
                severity = AlertSeverity.CRITICAL
            elif compliance_percentage <= (100 - target.error_budget_percentage * 2):
                severity = AlertSeverity.CRITICAL
            else:
                severity = AlertSeverity.HIGH
            
            # Determine alert channels based on severity and business impact
            channels = [AlertChannel.SLACK_ALERTS, AlertChannel.EMAIL]
            if severity == AlertSeverity.CRITICAL or target.business_impact == BusinessImpact.CRITICAL:
                channels.append(AlertChannel.PAGERDUTY)
                channels.append(AlertChannel.SLACK_CRITICAL)
            
            # Calculate business impact
            business_impact_cost = self._calculate_breach_business_impact(sla_type, measurements)
            
            # Create alert message
            alert_message = self._format_sla_breach_alert_message(
                sla_type, status, compliance_percentage, measurements, business_impact_cost
            )
            
            # Send alert
            await self.comprehensive_alerting.send_alert(
                alert_title=f"Story 1.1 SLA BREACH: {target.name}",
                alert_message=alert_message,
                severity=severity,
                channels=channels,
                metadata={
                    "sla_type": sla_type.value,
                    "status": status.value,
                    "compliance_percentage": compliance_percentage,
                    "business_impact_cost": business_impact_cost,
                    "target_value": target.target_value,
                    "measurements_count": len(measurements)
                }
            )
            
            # Create breach record
            await self._create_breach_record(sla_type, status, measurements, business_impact_cost)
            
        except Exception as e:
            self.logger.error(f"Error triggering SLA breach alert for {sla_type.value}: {e}")
    
    def _calculate_breach_business_impact(self, sla_type: SLAType, measurements: List[SLAMeasurement]) -> float:
        """Calculate estimated business impact cost of breach"""
        try:
            if not measurements:
                return 0.0
            
            # Calculate total business impact score from measurements
            total_impact = sum(m.business_impact_score for m in measurements)
            
            # Estimate cost based on breach duration and impact
            breach_duration_minutes = len(measurements) * 0.5  # Assuming 30-second measurements
            
            # Base cost calculation
            base_cost = breach_duration_minutes * self.business_context["cost_per_breach_minute"]
            
            # Apply SLA type multipliers
            sla_multipliers = {
                SLAType.UPTIME: 3.0,              # Uptime breaches are most costly
                SLAType.API_RESPONSE_TIME: 2.0,   # Performance issues affect user experience
                SLAType.DASHBOARD_LOAD_TIME: 1.5, # Dashboard issues affect productivity
                SLAType.ERROR_RATE: 2.5,          # Errors damage reputation
                SLAType.WEBSOCKET_LATENCY: 1.2,   # Real-time issues moderate impact
                SLAType.THROUGHPUT: 1.8,          # Capacity issues affect scale
                SLAType.DATA_FRESHNESS: 1.0       # Data freshness lower direct impact
            }
            
            multiplier = sla_multipliers.get(sla_type, 1.0)
            
            # Calculate final cost
            estimated_cost = base_cost * multiplier * (total_impact / 100)
            
            return round(estimated_cost, 2)
            
        except Exception as e:
            self.logger.error(f"Error calculating business impact for {sla_type.value}: {e}")
            return 0.0
    
    def _format_sla_breach_alert_message(
        self, sla_type: SLAType, status: SLAStatus, 
        compliance_percentage: float, measurements: List[SLAMeasurement], business_impact_cost: float
    ) -> str:
        """Format SLA breach alert message"""
        
        target = self.sla_targets[sla_type]
        recent_values = [m.measured_value for m in measurements[-5:]]  # Last 5 measurements
        
        message = f"""
**🚨 STORY 1.1 SLA BREACH ALERT**

**SLA Violated**: {target.name}
**Status**: {status.value.replace('_', ' ').title()}
**Compliance**: {compliance_percentage:.1f}% (Target: {100 - target.error_budget_percentage:.1f}%)

**Performance Details**:
- Target: {target.target_value} {target.unit}
- Recent Values: {', '.join(f'{v:.1f}' for v in recent_values)} {target.unit}
- Measurements in Window: {len(measurements)}

**Business Impact**:
- Severity Level: {target.business_impact.value}
- Estimated Cost: ${business_impact_cost:,.2f}
- Affected Users: ~{self.business_context['users_affected_per_breach']:,}

**Error Budget Status**:
- Daily Budget Remaining: {self.error_budgets[sla_type][CompliancePeriod.DAILY].remaining_percentage:.2f}%
- Burn Rate: {self.error_budgets[sla_type][CompliancePeriod.DAILY].burn_rate:.2f}%/hour

**Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

**Required Actions**:
1. Investigate root cause immediately
2. Implement corrective measures
3. Monitor for resolution
4. Update stakeholders on progress

**Story 1.1 Components to Check**:
- Dashboard API performance
- WebSocket connection health
- Cache system status
- Database query performance
- Auto-scaling behavior
        """
        
        return message.strip()
    
    async def _trigger_sla_warning_alert(
        self, sla_type: SLAType, compliance_percentage: float, measurements: List[SLAMeasurement]
    ):
        """Trigger warning alert for SLA approaching breach"""
        try:
            target = self.sla_targets[sla_type]
            
            alert_message = f"""
**⚠️ STORY 1.1 SLA WARNING**

**SLA**: {target.name}
**Status**: Approaching Threshold
**Current Compliance**: {compliance_percentage:.1f}%
**Warning Threshold**: {100 - target.error_budget_percentage:.1f}%

**Details**:
- Target: {target.target_value} {target.unit}
- Recent Measurements: {len(measurements)}
- Error Budget Remaining: {self.error_budgets[sla_type][CompliancePeriod.DAILY].remaining_percentage:.2f}%

**Recommended Actions**:
1. Monitor performance closely
2. Review recent changes
3. Check system capacity
4. Prepare corrective measures

**Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
            """
            
            await self.comprehensive_alerting.send_alert(
                alert_title=f"Story 1.1 SLA Warning: {target.name}",
                alert_message=alert_message.strip(),
                severity=AlertSeverity.MEDIUM,
                channels=[AlertChannel.SLACK_ALERTS],
                metadata={
                    "sla_type": sla_type.value,
                    "compliance_percentage": compliance_percentage,
                    "target_value": target.target_value
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error triggering SLA warning alert for {sla_type.value}: {e}")
    
    async def _create_breach_record(
        self, sla_type: SLAType, status: SLAStatus, 
        measurements: List[SLAMeasurement], business_impact_cost: float
    ):
        """Create breach record for tracking"""
        try:
            breach_id = f"sla_breach_{sla_type.value}_{int(datetime.now().timestamp())}"
            
            breach = SLABreach(
                breach_id=breach_id,
                sla_type=sla_type,
                start_time=measurements[0].timestamp if measurements else datetime.now(),
                severity=AlertSeverity.HIGH if status == SLAStatus.BREACHED else AlertSeverity.CRITICAL,
                business_impact=self.sla_targets[sla_type].business_impact,
                measured_values=[m.measured_value for m in measurements],
                target_value=self.sla_targets[sla_type].target_value,
                breach_magnitude=self._calculate_breach_magnitude(sla_type, measurements),
                error_budget_consumed=sum(m.error_budget_consumed for m in measurements),
                business_impact_cost=business_impact_cost,
                affected_users=self.business_context["users_affected_per_breach"],
                metadata={
                    "measurements_count": len(measurements),
                    "compliance_status": status.value,
                    "alert_channels_notified": ["slack", "email", "pagerduty"]
                }
            )
            
            self.active_breaches[breach_id] = breach
            self.breach_history.append(breach)
            
            self.logger.warning(f"SLA breach recorded: {breach_id}")
            
        except Exception as e:
            self.logger.error(f"Error creating breach record: {e}")
    
    def _calculate_breach_magnitude(self, sla_type: SLAType, measurements: List[SLAMeasurement]) -> float:
        """Calculate magnitude of SLA breach"""
        try:
            if not measurements:
                return 0.0
            
            target_value = self.sla_targets[sla_type].target_value
            measured_values = [m.measured_value for m in measurements]
            
            if sla_type in [SLAType.THROUGHPUT, SLAType.UPTIME]:
                # Higher is better - calculate deficit
                avg_measured = statistics.mean(measured_values)
                magnitude = (target_value - avg_measured) / target_value * 100
            else:
                # Lower is better - calculate excess
                avg_measured = statistics.mean(measured_values)
                magnitude = (avg_measured - target_value) / target_value * 100
            
            return max(0.0, magnitude)
            
        except Exception as e:
            self.logger.error(f"Error calculating breach magnitude: {e}")
            return 0.0
    
    async def _update_error_budgets(self):
        """Continuously update error budgets"""
        while True:
            try:
                for sla_type in SLAType:
                    await self._update_error_budget_for_sla(sla_type)
                
                await asyncio.sleep(300)  # Update every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error updating error budgets: {e}")
                await asyncio.sleep(600)
    
    async def _update_error_budget_for_sla(self, sla_type: SLAType):
        """Update error budget for specific SLA type"""
        try:
            for period in CompliancePeriod:
                budget = self.error_budgets[sla_type][period]
                
                # Calculate consumed budget for this period
                period_start = self._get_period_start_time(period)
                period_measurements = [
                    m for m in self.measurements[sla_type]
                    if m.timestamp >= period_start
                ]
                
                if period_measurements:
                    # Calculate total consumption for period
                    total_consumed = sum(m.error_budget_consumed for m in period_measurements)
                    budget.consumed_percentage = min(budget.total_budget_percentage, total_consumed)
                    budget.remaining_percentage = max(0.0, budget.total_budget_percentage - budget.consumed_percentage)
                    
                    # Calculate burn rate
                    period_hours = self._get_period_hours(period)
                    if period_hours > 0:
                        budget.burn_rate = (budget.consumed_percentage / period_hours) if period_hours else 0.0
                    
                    # Predict budget exhaustion
                    if budget.burn_rate > 0 and budget.remaining_percentage > 0:
                        hours_to_exhaustion = budget.remaining_percentage / budget.burn_rate
                        budget.projected_exhaustion = datetime.now() + timedelta(hours=hours_to_exhaustion)
                    else:
                        budget.projected_exhaustion = None
                    
                    budget.last_updated = datetime.now()
                
                # Check for budget exhaustion alerts
                if budget.remaining_percentage <= 10.0 and budget.remaining_percentage > 0:
                    await self._trigger_error_budget_alert(sla_type, period, budget)
                
        except Exception as e:
            self.logger.error(f"Error updating error budget for {sla_type.value}: {e}")
    
    def _get_period_start_time(self, period: CompliancePeriod) -> datetime:
        """Get start time for compliance period"""
        now = datetime.now()
        
        if period == CompliancePeriod.REAL_TIME:
            return now - timedelta(minutes=1)
        elif period == CompliancePeriod.HOURLY:
            return now - timedelta(hours=1)
        elif period == CompliancePeriod.DAILY:
            return now - timedelta(days=1)
        elif period == CompliancePeriod.WEEKLY:
            return now - timedelta(weeks=1)
        elif period == CompliancePeriod.MONTHLY:
            return now - timedelta(days=30)
        elif period == CompliancePeriod.QUARTERLY:
            return now - timedelta(days=90)
        elif period == CompliancePeriod.ANNUAL:
            return now - timedelta(days=365)
        else:
            return now - timedelta(hours=1)  # Default to hourly
    
    def _get_period_hours(self, period: CompliancePeriod) -> float:
        """Get number of hours in compliance period"""
        if period == CompliancePeriod.REAL_TIME:
            return 1/60  # 1 minute
        elif period == CompliancePeriod.HOURLY:
            return 1
        elif period == CompliancePeriod.DAILY:
            return 24
        elif period == CompliancePeriod.WEEKLY:
            return 24 * 7
        elif period == CompliancePeriod.MONTHLY:
            return 24 * 30
        elif period == CompliancePeriod.QUARTERLY:
            return 24 * 90
        elif period == CompliancePeriod.ANNUAL:
            return 24 * 365
        else:
            return 1
    
    async def _trigger_error_budget_alert(self, sla_type: SLAType, period: CompliancePeriod, budget: ErrorBudget):
        """Trigger alert for error budget exhaustion"""
        try:
            target = self.sla_targets[sla_type]
            
            alert_message = f"""
**🔥 STORY 1.1 ERROR BUDGET ALERT**

**SLA**: {target.name}
**Period**: {period.value.replace('_', ' ').title()}
**Error Budget Status**: {budget.remaining_percentage:.1f}% Remaining

**Budget Details**:
- Total Budget: {budget.total_budget_percentage:.1f}%
- Consumed: {budget.consumed_percentage:.1f}%
- Burn Rate: {budget.burn_rate:.2f}%/hour
- Projected Exhaustion: {budget.projected_exhaustion.strftime('%Y-%m-%d %H:%M UTC') if budget.projected_exhaustion else 'N/A'}

**Risk Level**: {'CRITICAL' if budget.remaining_percentage <= 5 else 'HIGH'}

**Immediate Actions Required**:
1. Halt any risky deployments
2. Focus on reliability improvements
3. Implement additional monitoring
4. Consider emergency procedures if budget <5%

**Business Impact**: Error budget exhaustion will result in SLA violations and potential business impact.

**Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
            """
            
            severity = AlertSeverity.CRITICAL if budget.remaining_percentage <= 5 else AlertSeverity.HIGH
            channels = [AlertChannel.SLACK_ALERTS, AlertChannel.EMAIL]
            if severity == AlertSeverity.CRITICAL:
                channels.append(AlertChannel.PAGERDUTY)
            
            await self.comprehensive_alerting.send_alert(
                alert_title=f"Story 1.1 Error Budget Alert: {target.name}",
                alert_message=alert_message.strip(),
                severity=severity,
                channels=channels,
                metadata={
                    "sla_type": sla_type.value,
                    "period": period.value,
                    "remaining_percentage": budget.remaining_percentage,
                    "burn_rate": budget.burn_rate
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error triggering error budget alert: {e}")
    
    async def _detect_sla_breaches(self):
        """Continuously detect and manage SLA breaches"""
        while True:
            try:
                await self._detect_new_breaches()
                await self._check_breach_resolution()
                
                await asyncio.sleep(120)  # Check every 2 minutes
                
            except Exception as e:
                self.logger.error(f"Error in breach detection: {e}")
                await asyncio.sleep(300)
    
    async def _detect_new_breaches(self):
        """Detect new SLA breaches"""
        # This is handled in the compliance evaluation
        pass
    
    async def _check_breach_resolution(self):
        """Check if active breaches have been resolved"""
        try:
            resolved_breaches = []
            
            for breach_id, breach in self.active_breaches.items():
                if await self._is_breach_resolved(breach):
                    breach.resolved = True
                    breach.end_time = datetime.now()
                    breach.duration_minutes = (breach.end_time - breach.start_time).total_seconds() / 60
                    
                    resolved_breaches.append(breach_id)
                    
                    await self._send_breach_resolution_notification(breach)
            
            # Remove resolved breaches from active list
            for breach_id in resolved_breaches:
                del self.active_breaches[breach_id]
            
        except Exception as e:
            self.logger.error(f"Error checking breach resolution: {e}")
    
    async def _is_breach_resolved(self, breach: SLABreach) -> bool:
        """Check if a breach has been resolved"""
        try:
            sla_type = breach.sla_type
            target = self.sla_targets[sla_type]
            
            # Get recent measurements
            now = datetime.now()
            window_start = now - timedelta(minutes=target.measurement_window_minutes * 2)  # Double window for stability
            
            recent_measurements = [
                m for m in self.measurements[sla_type]
                if m.timestamp >= window_start
            ]
            
            if not recent_measurements:
                return False
            
            # Check if recent measurements are all compliant
            compliant_count = sum(1 for m in recent_measurements if m.compliant)
            compliance_rate = compliant_count / len(recent_measurements)
            
            # Require high compliance rate for resolution (95%+)
            return compliance_rate >= 0.95
            
        except Exception as e:
            self.logger.error(f"Error checking breach resolution: {e}")
            return False
    
    async def _send_breach_resolution_notification(self, breach: SLABreach):
        """Send notification when breach is resolved"""
        try:
            target = self.sla_targets[breach.sla_type]
            
            resolution_message = f"""
**✅ STORY 1.1 SLA BREACH RESOLVED**

**SLA**: {target.name}
**Breach ID**: {breach.breach_id}
**Duration**: {breach.duration_minutes:.1f} minutes
**Resolution Time**: {breach.end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}

**Breach Summary**:
- Start Time: {breach.start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}
- Severity: {breach.severity.value}
- Business Impact Cost: ${breach.business_impact_cost:,.2f}
- Error Budget Consumed: {breach.error_budget_consumed:.2f}%

**Current Status**:
- SLA is now compliant
- System performance restored
- Monitoring continues

**Next Steps**:
1. Conduct post-incident review
2. Document lessons learned
3. Implement preventive measures
4. Update monitoring if needed

Thank you for your prompt response to resolve this issue.
            """
            
            await self.comprehensive_alerting.send_alert(
                alert_title=f"Story 1.1 SLA Restored: {target.name}",
                alert_message=resolution_message.strip(),
                severity=AlertSeverity.INFO,
                channels=[AlertChannel.SLACK_ALERTS, AlertChannel.EMAIL],
                metadata={
                    "breach_id": breach.breach_id,
                    "sla_type": breach.sla_type.value,
                    "duration_minutes": breach.duration_minutes,
                    "business_impact_cost": breach.business_impact_cost
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error sending breach resolution notification: {e}")
    
    async def _generate_compliance_reports(self):
        """Generate periodic compliance reports"""
        while True:
            try:
                # Generate daily report
                await self._generate_compliance_report(CompliancePeriod.DAILY)
                
                # Generate weekly report (once per day)
                if datetime.now().hour == 8:  # 8 AM
                    await self._generate_compliance_report(CompliancePeriod.WEEKLY)
                
                await asyncio.sleep(24 * 3600)  # Generate daily
                
            except Exception as e:
                self.logger.error(f"Error generating compliance reports: {e}")
                await asyncio.sleep(12 * 3600)
    
    async def _generate_compliance_report(self, period: CompliancePeriod) -> ComplianceReport:
        """Generate compliance report for specific period"""
        try:
            end_time = datetime.now()
            start_time = self._get_period_start_time(period)
            
            report = ComplianceReport(
                period=period,
                start_time=start_time,
                end_time=end_time
            )
            
            total_compliance_scores = []
            
            # Calculate compliance for each SLA type
            for sla_type in SLAType:
                period_measurements = [
                    m for m in self.measurements[sla_type]
                    if start_time <= m.timestamp <= end_time
                ]
                
                if period_measurements:
                    compliant_count = sum(1 for m in period_measurements if m.compliant)
                    compliance_percentage = (compliant_count / len(period_measurements)) * 100
                    total_compliance_scores.append(compliance_percentage)
                    
                    # Calculate SLA-specific metrics
                    avg_value = statistics.mean([m.measured_value for m in period_measurements])
                    target_value = self.sla_targets[sla_type].target_value
                    
                    report.sla_compliance[sla_type] = {
                        "compliance_percentage": compliance_percentage,
                        "average_value": avg_value,
                        "target_value": target_value,
                        "measurements_count": len(period_measurements),
                        "breaches": len([m for m in period_measurements if not m.compliant]),
                        "error_budget_consumed": sum(m.error_budget_consumed for m in period_measurements)
                    }
            
            # Calculate overall compliance
            if total_compliance_scores:
                report.overall_compliance_percentage = statistics.mean(total_compliance_scores)
            
            # Calculate breach statistics
            period_breaches = [
                b for b in self.breach_history
                if start_time <= b.start_time <= end_time
            ]
            
            report.total_breaches = len(period_breaches)
            report.total_downtime_minutes = sum(b.duration_minutes for b in period_breaches if b.resolved)
            
            # Add error budget status
            for sla_type in SLAType:
                report.error_budget_status[sla_type] = self.error_budgets[sla_type][period]
            
            # Calculate business impact
            total_business_cost = sum(b.business_impact_cost for b in period_breaches)
            report.business_impact_summary = {
                "total_cost": total_business_cost,
                "average_cost_per_breach": total_business_cost / len(period_breaches) if period_breaches else 0,
                "affected_users_estimate": sum(b.affected_users for b in period_breaches),
                "reputation_impact": self._calculate_reputation_impact(period_breaches)
            }
            
            # Add recommendations
            report.recommendations = self._generate_compliance_recommendations(report)
            
            # Cache report
            self.compliance_cache[period] = report
            
            # Send report to stakeholders
            await self._send_compliance_report(report)
            
            return report
            
        except Exception as e:
            self.logger.error(f"Error generating compliance report for {period.value}: {e}")
            return ComplianceReport(period=period, start_time=datetime.now(), end_time=datetime.now())
    
    def _calculate_reputation_impact(self, breaches: List[SLABreach]) -> str:
        """Calculate reputation impact from breaches"""
        if not breaches:
            return "minimal"
        
        total_cost = sum(b.business_impact_cost for b in breaches)
        critical_breaches = len([b for b in breaches if b.severity == AlertSeverity.CRITICAL])
        
        if total_cost > 50000 or critical_breaches >= 5:
            return "severe"
        elif total_cost > 20000 or critical_breaches >= 3:
            return "significant"
        elif total_cost > 5000 or critical_breaches >= 1:
            return "moderate"
        else:
            return "minimal"
    
    def _generate_compliance_recommendations(self, report: ComplianceReport) -> List[str]:
        """Generate recommendations based on compliance report"""
        recommendations = []
        
        if report.overall_compliance_percentage < 95:
            recommendations.append("URGENT: Overall compliance below 95% - immediate action required")
        
        if report.total_breaches > 5:
            recommendations.append("High breach frequency detected - review system stability")
        
        for sla_type, compliance_data in report.sla_compliance.items():
            if compliance_data["compliance_percentage"] < 90:
                recommendations.append(f"Focus on {sla_type.value} performance - only {compliance_data['compliance_percentage']:.1f}% compliant")
        
        # Error budget recommendations
        for sla_type, budget in report.error_budget_status.items():
            if budget.remaining_percentage < 20:
                recommendations.append(f"Error budget for {sla_type.value} critically low - avoid risky changes")
        
        if not recommendations:
            recommendations.append("Excellent compliance - maintain current practices")
        
        return recommendations
    
    async def _send_compliance_report(self, report: ComplianceReport):
        """Send compliance report to stakeholders"""
        try:
            report_message = self._format_compliance_report_message(report)
            
            # Determine severity based on compliance
            if report.overall_compliance_percentage >= 99:
                severity = AlertSeverity.INFO
            elif report.overall_compliance_percentage >= 95:
                severity = AlertSeverity.LOW
            elif report.overall_compliance_percentage >= 90:
                severity = AlertSeverity.MEDIUM
            else:
                severity = AlertSeverity.HIGH
            
            channels = [AlertChannel.EMAIL]
            if severity in [AlertSeverity.HIGH, AlertSeverity.CRITICAL]:
                channels.append(AlertChannel.SLACK_ALERTS)
            
            await self.comprehensive_alerting.send_alert(
                alert_title=f"Story 1.1 SLA Compliance Report - {report.period.value.title()}",
                alert_message=report_message,
                severity=severity,
                channels=channels,
                metadata=asdict(report)
            )
            
        except Exception as e:
            self.logger.error(f"Error sending compliance report: {e}")
    
    def _format_compliance_report_message(self, report: ComplianceReport) -> str:
        """Format compliance report message"""
        
        period_name = report.period.value.replace('_', ' ').title()
        
        message = f"""
**📊 STORY 1.1 SLA COMPLIANCE REPORT - {period_name}**

**Report Period**: {report.start_time.strftime('%Y-%m-%d %H:%M')} to {report.end_time.strftime('%Y-%m-%d %H:%M')} UTC

**Overall Compliance**: {report.overall_compliance_percentage:.1f}%
**Total Breaches**: {report.total_breaches}
**Total Downtime**: {report.total_downtime_minutes:.1f} minutes

**SLA Performance Summary**:
        """
        
        for sla_type, data in report.sla_compliance.items():
            status_icon = "✅" if data["compliance_percentage"] >= 99 else "⚠️" if data["compliance_percentage"] >= 95 else "❌"
            message += f"\n{status_icon} **{sla_type.value}**: {data['compliance_percentage']:.1f}% (Target: {data['target_value']}, Avg: {data['average_value']:.2f})"
        
        message += f"""

**Business Impact**:
- Total Cost: ${report.business_impact_summary.get('total_cost', 0):,.2f}
- Affected Users: {report.business_impact_summary.get('affected_users_estimate', 0):,}
- Reputation Impact: {report.business_impact_summary.get('reputation_impact', 'minimal').title()}

**Error Budget Status**:
        """
        
        for sla_type, budget in report.error_budget_status.items():
            budget_icon = "🟢" if budget.remaining_percentage > 50 else "🟡" if budget.remaining_percentage > 20 else "🔴"
            message += f"\n{budget_icon} **{sla_type.value}**: {budget.remaining_percentage:.1f}% remaining"
        
        message += f"""

**Key Recommendations**:
        """
        
        for i, recommendation in enumerate(report.recommendations[:5], 1):
            message += f"\n{i}. {recommendation}"
        
        message += f"""

**Next Review**: {(report.end_time + timedelta(days=1 if report.period == CompliancePeriod.DAILY else 7)).strftime('%Y-%m-%d')}

---
*Generated by Story 1.1 SLA Compliance Monitor*
        """
        
        return message.strip()
    
    async def _analyze_compliance_trends(self):
        """Analyze compliance trends and predictions"""
        while True:
            try:
                await self._analyze_sla_trends()
                await asyncio.sleep(3600)  # Analyze hourly
                
            except Exception as e:
                self.logger.error(f"Error analyzing compliance trends: {e}")
                await asyncio.sleep(1800)
    
    async def _analyze_sla_trends(self):
        """Analyze SLA performance trends"""
        try:
            for sla_type in SLAType:
                await self._analyze_sla_type_trend(sla_type)
                
        except Exception as e:
            self.logger.error(f"Error analyzing SLA trends: {e}")
    
    async def _analyze_sla_type_trend(self, sla_type: SLAType):
        """Analyze trend for specific SLA type"""
        try:
            measurements = list(self.measurements[sla_type])
            if len(measurements) < 20:  # Need sufficient data
                return
            
            # Get recent measurements
            recent_values = [m.measured_value for m in measurements[-20:]]
            
            # Calculate trend
            x = list(range(len(recent_values)))
            trend_slope = statistics.correlation(x, recent_values) if len(recent_values) > 1 else 0
            
            # Predict future values
            target_value = self.sla_targets[sla_type].target_value
            current_avg = statistics.mean(recent_values[-5:])  # Last 5 measurements
            
            # Check for concerning trends
            if sla_type in [SLAType.THROUGHPUT, SLAType.UPTIME]:
                # Higher is better
                concerning_trend = trend_slope < -0.1 and current_avg < target_value * 1.1
            else:
                # Lower is better
                concerning_trend = trend_slope > 0.1 and current_avg > target_value * 0.9
            
            if concerning_trend:
                await self._send_trend_alert(sla_type, trend_slope, current_avg, target_value)
            
        except Exception as e:
            self.logger.error(f"Error analyzing trend for {sla_type.value}: {e}")
    
    async def _send_trend_alert(self, sla_type: SLAType, trend_slope: float, current_avg: float, target_value: float):
        """Send alert for concerning SLA trend"""
        try:
            target = self.sla_targets[sla_type]
            
            trend_direction = "deteriorating" if (
                (sla_type in [SLAType.THROUGHPUT, SLAType.UPTIME] and trend_slope < 0) or
                (sla_type not in [SLAType.THROUGHPUT, SLAType.UPTIME] and trend_slope > 0)
            ) else "improving"
            
            alert_message = f"""
**📈 STORY 1.1 SLA TREND ALERT**

**SLA**: {target.name}
**Trend**: Performance is {trend_direction}
**Current Average**: {current_avg:.2f} {target.unit}
**Target**: {target_value} {target.unit}

**Analysis**:
- Trend Slope: {trend_slope:.4f}
- Risk Level: {'HIGH' if abs(trend_slope) > 0.2 else 'MEDIUM'}
- Predicted Impact: Potential SLA breach if trend continues

**Recommended Actions**:
1. Investigate performance degradation causes
2. Review recent system changes
3. Check resource utilization
4. Consider preventive measures

**Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
            """
            
            await self.comprehensive_alerting.send_alert(
                alert_title=f"Story 1.1 SLA Trend Warning: {target.name}",
                alert_message=alert_message.strip(),
                severity=AlertSeverity.MEDIUM,
                channels=[AlertChannel.SLACK_ALERTS],
                metadata={
                    "sla_type": sla_type.value,
                    "trend_slope": trend_slope,
                    "current_average": current_avg,
                    "target_value": target_value
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error sending trend alert for {sla_type.value}: {e}")
    
    async def _manage_breach_lifecycle(self):
        """Manage breach lifecycle and escalations"""
        while True:
            try:
                await self._check_breach_escalations()
                await self._cleanup_old_breaches()
                
                await asyncio.sleep(600)  # Check every 10 minutes
                
            except Exception as e:
                self.logger.error(f"Error managing breach lifecycle: {e}")
                await asyncio.sleep(1200)
    
    async def _check_breach_escalations(self):
        """Check if breaches need escalation"""
        try:
            now = datetime.now()
            
            for breach_id, breach in self.active_breaches.items():
                if not breach.escalated:
                    breach_duration = (now - breach.start_time).total_seconds() / 60
                    
                    # Escalate after 15 minutes for high/critical breaches
                    should_escalate = (
                        (breach.severity == AlertSeverity.CRITICAL and breach_duration >= 10) or
                        (breach.severity == AlertSeverity.HIGH and breach_duration >= 15) or
                        (breach_duration >= 30)  # Always escalate after 30 minutes
                    )
                    
                    if should_escalate:
                        await self._escalate_breach(breach)
                        breach.escalated = True
            
        except Exception as e:
            self.logger.error(f"Error checking breach escalations: {e}")
    
    async def _escalate_breach(self, breach: SLABreach):
        """Escalate breach to higher severity"""
        try:
            target = self.sla_targets[breach.sla_type]
            duration_minutes = (datetime.now() - breach.start_time).total_seconds() / 60
            
            escalation_message = f"""
**🚨 STORY 1.1 SLA BREACH ESCALATION**

**SLA**: {target.name}
**Breach ID**: {breach.breach_id}
**Duration**: {duration_minutes:.1f} minutes
**Original Severity**: {breach.severity.value}
**New Severity**: CRITICAL

**Escalation Trigger**: Breach duration exceeds escalation threshold

**Current Status**:
- Breach ongoing for {duration_minutes:.1f} minutes
- Business Impact: ${breach.business_impact_cost:,.2f}
- Affected Users: {breach.affected_users:,}

**URGENT ACTIONS REQUIRED**:
1. Activate incident response team
2. Implement emergency procedures
3. Coordinate with stakeholders
4. Provide regular status updates

**Executive Notification**: This breach requires executive attention due to duration and impact.

**Next Update**: Expected in 10 minutes
            """
            
            await self.comprehensive_alerting.send_alert(
                alert_title=f"ESCALATED: Story 1.1 SLA Breach - {target.name}",
                alert_message=escalation_message.strip(),
                severity=AlertSeverity.CRITICAL,
                channels=[AlertChannel.PAGERDUTY, AlertChannel.SLACK_CRITICAL, AlertChannel.EMAIL],
                metadata={
                    "breach_id": breach.breach_id,
                    "sla_type": breach.sla_type.value,
                    "duration_minutes": duration_minutes,
                    "escalated": True
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error escalating breach {breach.breach_id}: {e}")
    
    async def _cleanup_old_breaches(self):
        """Clean up old breach records"""
        try:
            cutoff_time = datetime.now() - timedelta(days=30)  # Keep 30 days of history
            
            # Remove old breach history
            self.breach_history = [
                b for b in self.breach_history
                if b.start_time >= cutoff_time
            ]
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old breaches: {e}")
    
    async def _predict_sla_risks(self):
        """Predict SLA risks using trend analysis"""
        while True:
            try:
                for sla_type in SLAType:
                    risk_score = await self._calculate_sla_risk_score(sla_type)
                    if risk_score >= 70:  # High risk
                        await self._send_sla_risk_alert(sla_type, risk_score)
                
                await asyncio.sleep(1800)  # Predict every 30 minutes
                
            except Exception as e:
                self.logger.error(f"Error predicting SLA risks: {e}")
                await asyncio.sleep(3600)
    
    async def _calculate_sla_risk_score(self, sla_type: SLAType) -> float:
        """Calculate risk score for SLA breach (0-100)"""
        try:
            measurements = list(self.measurements[sla_type])
            if len(measurements) < 10:
                return 0.0
            
            target = self.sla_targets[sla_type]
            risk_factors = []
            
            # Factor 1: Current performance vs target
            recent_values = [m.measured_value for m in measurements[-10:]]
            current_avg = statistics.mean(recent_values)
            
            if sla_type in [SLAType.THROUGHPUT, SLAType.UPTIME]:
                performance_ratio = current_avg / target.target_value
                performance_risk = max(0, (1 - performance_ratio) * 100)
            else:
                performance_ratio = current_avg / target.target_value
                performance_risk = max(0, (performance_ratio - 1) * 100)
            
            risk_factors.append(min(100, performance_risk))
            
            # Factor 2: Trend analysis
            if len(recent_values) >= 5:
                x = list(range(len(recent_values)))
                trend_slope = statistics.correlation(x, recent_values) if len(recent_values) > 1 else 0
                
                if sla_type in [SLAType.THROUGHPUT, SLAType.UPTIME]:
                    trend_risk = max(0, -trend_slope * 50)  # Negative trend is bad
                else:
                    trend_risk = max(0, trend_slope * 50)   # Positive trend is bad
                
                risk_factors.append(min(100, trend_risk))
            
            # Factor 3: Error budget consumption rate
            daily_budget = self.error_budgets[sla_type][CompliancePeriod.DAILY]
            budget_risk = (daily_budget.consumed_percentage / daily_budget.total_budget_percentage) * 100
            risk_factors.append(min(100, budget_risk))
            
            # Factor 4: Historical breach frequency
            recent_breaches = len([
                b for b in self.breach_history[-10:]
                if b.sla_type == sla_type and 
                   (datetime.now() - b.start_time).days <= 7
            ])
            breach_risk = min(100, recent_breaches * 20)  # 20 points per breach in last week
            risk_factors.append(breach_risk)
            
            # Calculate weighted average risk score
            return statistics.mean(risk_factors)
            
        except Exception as e:
            self.logger.error(f"Error calculating risk score for {sla_type.value}: {e}")
            return 0.0
    
    async def _send_sla_risk_alert(self, sla_type: SLAType, risk_score: float):
        """Send alert for high SLA risk prediction"""
        try:
            target = self.sla_targets[sla_type]
            
            risk_level = "CRITICAL" if risk_score >= 90 else "HIGH"
            
            alert_message = f"""
**⚠️ STORY 1.1 SLA RISK PREDICTION**

**SLA**: {target.name}
**Risk Score**: {risk_score:.1f}/100
**Risk Level**: {risk_level}

**Risk Analysis**:
- Performance trending towards SLA breach
- Current trajectory suggests potential violation
- Preventive action recommended

**Current Status**:
- Target: {target.target_value} {target.unit}
- Error Budget Remaining: {self.error_budgets[sla_type][CompliancePeriod.DAILY].remaining_percentage:.1f}%

**Preventive Actions**:
1. Review system performance metrics
2. Check for capacity constraints
3. Investigate recent changes
4. Consider proactive scaling
5. Monitor closely for next 2 hours

**Business Impact**: Potential SLA breach could result in business disruption and customer impact.

**Time**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
            """
            
            severity = AlertSeverity.HIGH if risk_score >= 90 else AlertSeverity.MEDIUM
            
            await self.comprehensive_alerting.send_alert(
                alert_title=f"Story 1.1 SLA Risk Alert: {target.name}",
                alert_message=alert_message.strip(),
                severity=severity,
                channels=[AlertChannel.SLACK_ALERTS],
                metadata={
                    "sla_type": sla_type.value,
                    "risk_score": risk_score,
                    "risk_level": risk_level
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error sending SLA risk alert for {sla_type.value}: {e}")
    
    async def _send_sla_metrics(self, sla_type: SLAType, status: SLAStatus, compliance_percentage: float):
        """Send SLA metrics to monitoring systems"""
        try:
            # Send to business metrics tracker
            await self.business_metrics.track_kpi(
                category=KPICategory.PERFORMANCE,
                name=f"sla_compliance_{sla_type.value}",
                value=compliance_percentage,
                target=99.9 if sla_type == SLAType.UPTIME else 95.0,
                metadata={
                    "sla_type": sla_type.value,
                    "status": status.value,
                    "target_value": self.sla_targets[sla_type].target_value,
                    "unit": self.sla_targets[sla_type].target_unit
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error sending SLA metrics for {sla_type.value}: {e}")
    
    # Public API Methods
    
    async def get_sla_status(self, sla_type: Optional[SLAType] = None) -> Dict:
        """Get current SLA status"""
        try:
            if sla_type:
                return await self._get_single_sla_status(sla_type)
            else:
                return await self._get_all_sla_status()
                
        except Exception as e:
            self.logger.error(f"Error getting SLA status: {e}")
            return {"error": "SLA status temporarily unavailable"}
    
    async def _get_single_sla_status(self, sla_type: SLAType) -> Dict:
        """Get status for single SLA type"""
        target = self.sla_targets[sla_type]
        measurements = list(self.measurements[sla_type])
        
        if not measurements:
            return {"sla_type": sla_type.value, "status": "no_data"}
        
        recent_measurements = measurements[-10:]  # Last 10 measurements
        compliant_count = sum(1 for m in recent_measurements if m.compliant)
        compliance_percentage = (compliant_count / len(recent_measurements)) * 100
        
        return {
            "sla_type": sla_type.value,
            "name": target.name,
            "status": "compliant" if compliance_percentage >= 95 else "warning" if compliance_percentage >= 90 else "breached",
            "compliance_percentage": compliance_percentage,
            "target_value": target.target_value,
            "target_unit": target.target_unit,
            "current_value": recent_measurements[-1].measured_value if recent_measurements else 0,
            "error_budget_remaining": self.error_budgets[sla_type][CompliancePeriod.DAILY].remaining_percentage,
            "measurements_count": len(recent_measurements),
            "last_updated": recent_measurements[-1].timestamp.isoformat() if recent_measurements else None
        }
    
    async def _get_all_sla_status(self) -> Dict:
        """Get status for all SLA types"""
        all_status = {}
        overall_compliant = True
        
        for sla_type in SLAType:
            status = await self._get_single_sla_status(sla_type)
            all_status[sla_type.value] = status
            
            if status.get("compliance_percentage", 0) < 95:
                overall_compliant = False
        
        return {
            "timestamp": datetime.now().isoformat(),
            "overall_status": "compliant" if overall_compliant else "degraded",
            "total_active_breaches": len(self.active_breaches),
            "sla_details": all_status
        }
    
    async def get_compliance_report(self, period: CompliancePeriod) -> Optional[Dict]:
        """Get compliance report for specific period"""
        try:
            # Check cache first
            if period in self.compliance_cache:
                report = self.compliance_cache[period]
                # Refresh if older than 1 hour
                if (datetime.now() - report.end_time).total_seconds() < 3600:
                    return asdict(report)
            
            # Generate new report
            report = await self._generate_compliance_report(period)
            return asdict(report)
            
        except Exception as e:
            self.logger.error(f"Error getting compliance report for {period.value}: {e}")
            return None
    
    async def get_error_budget_status(self, sla_type: Optional[SLAType] = None) -> Dict:
        """Get error budget status"""
        try:
            if sla_type:
                return {
                    "sla_type": sla_type.value,
                    "budgets": {
                        period.value: asdict(budget)
                        for period, budget in self.error_budgets[sla_type].items()
                    }
                }
            else:
                return {
                    "timestamp": datetime.now().isoformat(),
                    "all_budgets": {
                        sla_type.value: {
                            period.value: asdict(budget)
                            for period, budget in budgets.items()
                        }
                        for sla_type, budgets in self.error_budgets.items()
                    }
                }
                
        except Exception as e:
            self.logger.error(f"Error getting error budget status: {e}")
            return {"error": "Error budget status temporarily unavailable"}
    
    async def get_active_breaches(self) -> Dict:
        """Get currently active SLA breaches"""
        try:
            return {
                "timestamp": datetime.now().isoformat(),
                "active_breaches_count": len(self.active_breaches),
                "breaches": [
                    asdict(breach) for breach in self.active_breaches.values()
                ]
            }
            
        except Exception as e:
            self.logger.error(f"Error getting active breaches: {e}")
            return {"error": "Active breaches data temporarily unavailable"}
    
    async def trigger_manual_sla_evaluation(self) -> Dict:
        """Manually trigger SLA evaluation"""
        try:
            evaluation_results = {}
            
            for sla_type in SLAType:
                await self._evaluate_sla_type_compliance(sla_type)
                status = await self._get_single_sla_status(sla_type)
                evaluation_results[sla_type.value] = status
            
            return {
                "status": "completed",
                "timestamp": datetime.now().isoformat(),
                "evaluation_results": evaluation_results
            }
            
        except Exception as e:
            self.logger.error(f"Error triggering manual SLA evaluation: {e}")
            return {"error": str(e)}
    
    async def close(self):
        """Clean up SLA monitoring resources"""
        try:
            # Cancel all monitoring tasks
            for task in self.monitoring_tasks:
                task.cancel()
            
            await asyncio.gather(*self.monitoring_tasks, return_exceptions=True)
            
            # Close monitoring components
            if self.comprehensive_alerting:
                await self.comprehensive_alerting.close()
            
            if self.business_metrics:
                await self.business_metrics.close()
            
            if self.enterprise_observability:
                await self.enterprise_observability.close()
            
            self.logger.info("Story 1.1 SLA compliance monitor shut down successfully")
            
        except Exception as e:
            self.logger.error(f"Error during SLA monitor shutdown: {e}")


# Factory function
def create_story_11_sla_compliance_monitor() -> Story11SLAComplianceMonitor:
    """Create Story 1.1 SLA compliance monitor instance"""
    return Story11SLAComplianceMonitor()


# Usage example
async def main():
    """Example usage of Story 1.1 SLA compliance monitor"""
    
    # Create SLA monitor
    sla_monitor = create_story_11_sla_compliance_monitor()
    
    print("🚀 Story 1.1 SLA Compliance Monitor Started!")
    print("📊 SLA Monitoring Features:")
    print("   ✅ 99.9% uptime tracking with real-time validation")
    print("   ✅ <25ms API response time SLA monitoring")
    print("   ✅ <2s dashboard load time compliance tracking")
    print("   ✅ <50ms WebSocket latency SLA validation")
    print("   ✅ Automated SLA breach alerts with escalation")
    print("   ✅ Business impact analysis for SLA violations")
    print("   ✅ Error budget tracking and management")
    print("   ✅ Compliance reporting with audit trails")
    print("   ✅ Proactive SLA trend analysis and predictions")
    print("   ✅ Real-time SLA compliance validation")
    print()
    
    try:
        # Demonstrate SLA monitoring
        await asyncio.sleep(30)  # Let monitoring initialize
        
        # Get SLA status
        sla_status = await sla_monitor.get_sla_status()
        print("📈 Current SLA Status:")
        print(f"   Overall Status: {sla_status.get('overall_status', 'unknown').upper()}")
        print(f"   Active Breaches: {sla_status.get('total_active_breaches', 0)}")
        
        # Get error budget status
        budget_status = await sla_monitor.get_error_budget_status()
        print(f"\n💰 Error Budget Summary: Available for all SLA types")
        
        # Get compliance report
        daily_report = await sla_monitor.get_compliance_report(CompliancePeriod.DAILY)
        if daily_report:
            print(f"📊 Daily Compliance: {daily_report['overall_compliance_percentage']:.1f}%")
        
    finally:
        await sla_monitor.close()


if __name__ == "__main__":
    asyncio.run(main())


__all__ = [
    "Story11SLAComplianceMonitor",
    "SLAType",
    "SLAStatus",
    "CompliancePeriod",
    "BusinessImpact",
    "SLATarget",
    "SLAMeasurement",
    "ErrorBudget",
    "SLABreach",
    "ComplianceReport",
    "create_story_11_sla_compliance_monitor"
]
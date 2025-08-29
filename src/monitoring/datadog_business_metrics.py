"""
DataDog Business Metrics and KPI Tracking
Provides comprehensive business intelligence and KPI monitoring with DataDog
"""

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union, Callable
from enum import Enum
from dataclasses import dataclass, asdict
from contextlib import asynccontextmanager

from ddtrace import tracer

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring, MetricType
from monitoring.datadog_apm_middleware import add_custom_tags, add_custom_metric

logger = get_logger(__name__)


class KPICategory(Enum):
    """Categories for business KPIs."""
    FINANCIAL = "financial"
    OPERATIONAL = "operational"
    CUSTOMER = "customer"
    PRODUCT = "product"
    MARKETING = "marketing"
    DATA_QUALITY = "data_quality"
    TECHNICAL = "technical"


class AlertLevel(Enum):
    """Alert levels for KPI thresholds."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class KPIDefinition:
    """Definition of a business KPI."""
    name: str
    description: str
    category: KPICategory
    unit: str
    calculation_method: str
    target_value: Optional[float] = None
    warning_threshold: Optional[float] = None
    critical_threshold: Optional[float] = None
    higher_is_better: bool = True
    tags: Optional[Dict[str, str]] = None


@dataclass
class KPIValue:
    """A KPI measurement value."""
    kpi_name: str
    value: float
    timestamp: datetime
    tags: Optional[Dict[str, str]] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class BusinessEvent:
    """Business event for tracking significant occurrences."""
    event_type: str
    event_name: str
    value: Optional[float] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


class DataDogBusinessMetricsTracker:
    """
    Comprehensive business metrics and KPI tracking with DataDog
    
    Features:
    - KPI definition and tracking
    - Business event monitoring
    - Real-time business intelligence
    - Automated alerting on KPI thresholds
    - Revenue and financial metrics
    - Customer behavior analytics
    - Product usage analytics
    - Operational efficiency metrics
    """
    
    def __init__(self, service_name: str = "business-intelligence", 
                 datadog_monitoring: Optional[DatadogMonitoring] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # KPI registry and values
        self.kpi_definitions: Dict[str, KPIDefinition] = {}
        self.kpi_values: Dict[str, List[KPIValue]] = {}
        self.business_events: List[BusinessEvent] = []
        
        # Real-time calculations cache
        self.realtime_calculations: Dict[str, Any] = {}
        
        # Initialize standard business KPIs
        self._initialize_standard_kpis()
    
    def _initialize_standard_kpis(self):
        """Initialize standard business KPIs."""
        
        standard_kpis = [
            # Financial KPIs
            KPIDefinition(
                name="revenue_total",
                description="Total revenue generated",
                category=KPICategory.FINANCIAL,
                unit="currency",
                calculation_method="sum",
                higher_is_better=True
            ),
            KPIDefinition(
                name="revenue_per_customer",
                description="Average revenue per customer",
                category=KPICategory.FINANCIAL,
                unit="currency",
                calculation_method="average",
                target_value=1000.0,
                warning_threshold=800.0,
                critical_threshold=500.0,
                higher_is_better=True
            ),
            KPIDefinition(
                name="customer_acquisition_cost",
                description="Cost to acquire a new customer",
                category=KPICategory.MARKETING,
                unit="currency",
                calculation_method="average",
                target_value=100.0,
                warning_threshold=150.0,
                critical_threshold=200.0,
                higher_is_better=False
            ),
            KPIDefinition(
                name="customer_lifetime_value",
                description="Predicted lifetime value of a customer",
                category=KPICategory.CUSTOMER,
                unit="currency",
                calculation_method="average",
                target_value=5000.0,
                warning_threshold=3000.0,
                critical_threshold=2000.0,
                higher_is_better=True
            ),
            
            # Operational KPIs
            KPIDefinition(
                name="api_response_time_p95",
                description="95th percentile API response time",
                category=KPICategory.TECHNICAL,
                unit="milliseconds",
                calculation_method="percentile",
                target_value=200.0,
                warning_threshold=500.0,
                critical_threshold=1000.0,
                higher_is_better=False
            ),
            KPIDefinition(
                name="system_uptime",
                description="System uptime percentage",
                category=KPICategory.TECHNICAL,
                unit="percentage",
                calculation_method="percentage",
                target_value=99.9,
                warning_threshold=99.0,
                critical_threshold=95.0,
                higher_is_better=True
            ),
            KPIDefinition(
                name="error_rate",
                description="Overall system error rate",
                category=KPICategory.TECHNICAL,
                unit="percentage",
                calculation_method="percentage",
                target_value=0.1,
                warning_threshold=1.0,
                critical_threshold=5.0,
                higher_is_better=False
            ),
            
            # Customer KPIs
            KPIDefinition(
                name="active_users_daily",
                description="Daily active users",
                category=KPICategory.CUSTOMER,
                unit="count",
                calculation_method="unique_count",
                target_value=10000.0,
                warning_threshold=8000.0,
                critical_threshold=5000.0,
                higher_is_better=True
            ),
            KPIDefinition(
                name="customer_satisfaction_score",
                description="Customer satisfaction score",
                category=KPICategory.CUSTOMER,
                unit="score",
                calculation_method="average",
                target_value=4.5,
                warning_threshold=4.0,
                critical_threshold=3.5,
                higher_is_better=True
            ),
            KPIDefinition(
                name="customer_churn_rate",
                description="Monthly customer churn rate",
                category=KPICategory.CUSTOMER,
                unit="percentage",
                calculation_method="percentage",
                target_value=2.0,
                warning_threshold=5.0,
                critical_threshold=10.0,
                higher_is_better=False
            ),
            
            # Data Quality KPIs
            KPIDefinition(
                name="data_quality_score",
                description="Overall data quality score",
                category=KPICategory.DATA_QUALITY,
                unit="score",
                calculation_method="average",
                target_value=95.0,
                warning_threshold=90.0,
                critical_threshold=85.0,
                higher_is_better=True
            ),
            KPIDefinition(
                name="etl_success_rate",
                description="ETL pipeline success rate",
                category=KPICategory.DATA_QUALITY,
                unit="percentage",
                calculation_method="percentage",
                target_value=99.0,
                warning_threshold=95.0,
                critical_threshold=90.0,
                higher_is_better=True
            ),
            
            # Product KPIs
            KPIDefinition(
                name="feature_adoption_rate",
                description="Rate of new feature adoption",
                category=KPICategory.PRODUCT,
                unit="percentage",
                calculation_method="percentage",
                target_value=25.0,
                warning_threshold=15.0,
                critical_threshold=10.0,
                higher_is_better=True
            ),
            KPIDefinition(
                name="user_engagement_score",
                description="User engagement score",
                category=KPICategory.PRODUCT,
                unit="score",
                calculation_method="average",
                target_value=7.5,
                warning_threshold=6.0,
                critical_threshold=4.0,
                higher_is_better=True
            )
        ]
        
        for kpi in standard_kpis:
            self.register_kpi(kpi)
        
        self.logger.info(f"Initialized {len(standard_kpis)} standard business KPIs")
    
    def register_kpi(self, kpi_definition: KPIDefinition):
        """Register a new KPI definition."""
        
        self.kpi_definitions[kpi_definition.name] = kpi_definition
        
        if kpi_definition.name not in self.kpi_values:
            self.kpi_values[kpi_definition.name] = []
        
        self.logger.info(f"Registered KPI: {kpi_definition.name} ({kpi_definition.category.value})")
    
    async def track_kpi(self, kpi_name: str, value: float, 
                       tags: Optional[Dict[str, str]] = None,
                       metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Track a KPI value."""
        
        if kpi_name not in self.kpi_definitions:
            self.logger.warning(f"Unknown KPI: {kpi_name}")
            return False
        
        kpi_def = self.kpi_definitions[kpi_name]
        kpi_value = KPIValue(
            kpi_name=kpi_name,
            value=value,
            timestamp=datetime.utcnow(),
            tags=tags,
            metadata=metadata
        )
        
        # Store value
        self.kpi_values[kpi_name].append(kpi_value)
        
        # Keep only last 1000 values per KPI
        if len(self.kpi_values[kpi_name]) > 1000:
            self.kpi_values[kpi_name] = self.kpi_values[kpi_name][-1000:]
        
        # Send to DataDog
        if self.datadog_monitoring:
            await self._send_kpi_to_datadog(kpi_def, kpi_value)
        
        # Check thresholds and alert if needed
        await self._check_kpi_thresholds(kpi_def, kpi_value)
        
        # Add to current span if available
        add_custom_metric(f"business.kpi.{kpi_name}", value, tags)
        
        self.logger.debug(f"Tracked KPI {kpi_name}: {value} {kpi_def.unit}")
        
        return True
    
    async def _send_kpi_to_datadog(self, kpi_def: KPIDefinition, kpi_value: KPIValue):
        """Send KPI value to DataDog."""
        
        try:
            # Prepare tags
            dd_tags = [
                f"kpi:{kpi_def.name}",
                f"category:{kpi_def.category.value}",
                f"unit:{kpi_def.unit}"
            ]
            
            if kpi_def.tags:
                dd_tags.extend([f"{k}:{v}" for k, v in kpi_def.tags.items()])
            
            if kpi_value.tags:
                dd_tags.extend([f"{k}:{v}" for k, v in kpi_value.tags.items()])
            
            # Send as gauge metric
            self.datadog_monitoring.gauge(
                f"business.kpi.{kpi_def.name}",
                kpi_value.value,
                tags=dd_tags
            )
            
            # Send category summary
            self.datadog_monitoring.counter(
                f"business.kpi.category.{kpi_def.category.value}",
                tags=dd_tags
            )
            
        except Exception as e:
            self.logger.error(f"Failed to send KPI to DataDog: {str(e)}")
    
    async def _check_kpi_thresholds(self, kpi_def: KPIDefinition, kpi_value: KPIValue):
        """Check KPI thresholds and generate alerts."""
        
        if not (kpi_def.warning_threshold or kpi_def.critical_threshold):
            return
        
        value = kpi_value.value
        alert_level = None
        message = ""
        
        if kpi_def.critical_threshold is not None:
            if kpi_def.higher_is_better:
                if value <= kpi_def.critical_threshold:
                    alert_level = AlertLevel.CRITICAL
                    message = f"KPI {kpi_def.name} is critically low: {value} {kpi_def.unit} (threshold: {kpi_def.critical_threshold})"
            else:
                if value >= kpi_def.critical_threshold:
                    alert_level = AlertLevel.CRITICAL
                    message = f"KPI {kpi_def.name} is critically high: {value} {kpi_def.unit} (threshold: {kpi_def.critical_threshold})"
        
        if alert_level is None and kpi_def.warning_threshold is not None:
            if kpi_def.higher_is_better:
                if value <= kpi_def.warning_threshold:
                    alert_level = AlertLevel.WARNING
                    message = f"KPI {kpi_def.name} is below warning threshold: {value} {kpi_def.unit} (threshold: {kpi_def.warning_threshold})"
            else:
                if value >= kpi_def.warning_threshold:
                    alert_level = AlertLevel.WARNING
                    message = f"KPI {kpi_def.name} is above warning threshold: {value} {kpi_def.unit} (threshold: {kpi_def.warning_threshold})"
        
        if alert_level:
            await self._send_kpi_alert(kpi_def, kpi_value, alert_level, message)
    
    async def _send_kpi_alert(self, kpi_def: KPIDefinition, kpi_value: KPIValue, 
                            alert_level: AlertLevel, message: str):
        """Send KPI threshold alert."""
        
        try:
            if self.datadog_monitoring:
                tags = [
                    f"kpi:{kpi_def.name}",
                    f"category:{kpi_def.category.value}",
                    f"alert_level:{alert_level.value}"
                ]
                
                self.datadog_monitoring.counter(
                    "business.kpi.alerts",
                    tags=tags
                )
            
            self.logger.warning(f"KPI Alert ({alert_level.value}): {message}")
            
        except Exception as e:
            self.logger.error(f"Failed to send KPI alert: {str(e)}")
    
    # Business Event Tracking
    
    async def track_business_event(self, event: BusinessEvent) -> bool:
        """Track a business event."""
        
        try:
            # Store event
            self.business_events.append(event)
            
            # Keep only last 10000 events
            if len(self.business_events) > 10000:
                self.business_events = self.business_events[-10000:]
            
            # Send to DataDog
            if self.datadog_monitoring:
                await self._send_business_event_to_datadog(event)
            
            # Add to current span
            add_custom_tags({
                f"business.event.{event.event_type}": event.event_name,
                "business.event.timestamp": event.timestamp.isoformat()
            })
            
            if event.value is not None:
                add_custom_metric(f"business.event.{event.event_type}.value", event.value)
            
            self.logger.debug(f"Tracked business event: {event.event_type}.{event.event_name}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to track business event: {str(e)}")
            return False
    
    async def _send_business_event_to_datadog(self, event: BusinessEvent):
        """Send business event to DataDog."""
        
        try:
            tags = [
                f"event_type:{event.event_type}",
                f"event_name:{event.event_name}"
            ]
            
            if event.user_id:
                tags.append(f"user_id:{event.user_id}")
            if event.session_id:
                tags.append(f"session_id:{event.session_id}")
            
            # Track event occurrence
            self.datadog_monitoring.counter(
                f"business.events.{event.event_type}",
                tags=tags
            )
            
            # Track event value if present
            if event.value is not None:
                self.datadog_monitoring.histogram(
                    f"business.events.{event.event_type}.value",
                    event.value,
                    tags=tags
                )
                
        except Exception as e:
            self.logger.error(f"Failed to send business event to DataDog: {str(e)}")
    
    # Convenience methods for common business events
    
    async def track_revenue(self, amount: float, currency: str = "USD",
                          customer_id: Optional[str] = None,
                          product_id: Optional[str] = None,
                          transaction_id: Optional[str] = None):
        """Track revenue event."""
        
        await self.track_business_event(BusinessEvent(
            event_type="revenue",
            event_name="transaction_completed",
            value=amount,
            user_id=customer_id,
            properties={
                "currency": currency,
                "product_id": product_id,
                "transaction_id": transaction_id
            }
        ))
        
        # Update revenue KPI
        await self.track_kpi("revenue_total", amount, tags={"currency": currency})
    
    async def track_user_activity(self, user_id: str, activity_type: str,
                                session_id: Optional[str] = None,
                                properties: Optional[Dict[str, Any]] = None):
        """Track user activity event."""
        
        await self.track_business_event(BusinessEvent(
            event_type="user_activity",
            event_name=activity_type,
            user_id=user_id,
            session_id=session_id,
            properties=properties
        ))
    
    async def track_feature_usage(self, feature_name: str, user_id: Optional[str] = None,
                                usage_value: Optional[float] = None,
                                properties: Optional[Dict[str, Any]] = None):
        """Track feature usage event."""
        
        await self.track_business_event(BusinessEvent(
            event_type="feature_usage",
            event_name=feature_name,
            value=usage_value,
            user_id=user_id,
            properties=properties
        ))
    
    async def track_customer_acquisition(self, customer_id: str, acquisition_channel: str,
                                       acquisition_cost: Optional[float] = None):
        """Track customer acquisition event."""
        
        await self.track_business_event(BusinessEvent(
            event_type="customer_acquisition",
            event_name="new_customer",
            value=acquisition_cost,
            user_id=customer_id,
            properties={"acquisition_channel": acquisition_channel}
        ))
        
        if acquisition_cost:
            await self.track_kpi("customer_acquisition_cost", acquisition_cost,
                               tags={"channel": acquisition_channel})
    
    async def track_error_occurrence(self, error_type: str, severity: str,
                                   component: Optional[str] = None,
                                   user_id: Optional[str] = None):
        """Track error occurrence event."""
        
        await self.track_business_event(BusinessEvent(
            event_type="error",
            event_name=error_type,
            user_id=user_id,
            properties={
                "severity": severity,
                "component": component
            }
        ))
    
    # Real-time KPI Calculations
    
    async def calculate_realtime_kpis(self) -> Dict[str, Any]:
        """Calculate real-time KPI values."""
        
        try:
            now = datetime.utcnow()
            last_hour = now - timedelta(hours=1)
            last_day = now - timedelta(days=1)
            
            calculations = {}
            
            # Calculate KPI summaries
            for kpi_name, values in self.kpi_values.items():
                if not values:
                    continue
                
                kpi_def = self.kpi_definitions.get(kpi_name)
                if not kpi_def:
                    continue
                
                # Filter recent values
                recent_values = [v for v in values if v.timestamp >= last_hour]
                daily_values = [v for v in values if v.timestamp >= last_day]
                
                if recent_values:
                    latest_value = max(recent_values, key=lambda v: v.timestamp).value
                    hourly_avg = sum(v.value for v in recent_values) / len(recent_values)
                else:
                    latest_value = None
                    hourly_avg = None
                
                if daily_values:
                    daily_avg = sum(v.value for v in daily_values) / len(daily_values)
                    daily_min = min(v.value for v in daily_values)
                    daily_max = max(v.value for v in daily_values)
                else:
                    daily_avg = daily_min = daily_max = None
                
                # Determine status
                status = "ok"
                if latest_value is not None:
                    if kpi_def.critical_threshold is not None:
                        if kpi_def.higher_is_better:
                            if latest_value <= kpi_def.critical_threshold:
                                status = "critical"
                        else:
                            if latest_value >= kpi_def.critical_threshold:
                                status = "critical"
                    
                    if status == "ok" and kpi_def.warning_threshold is not None:
                        if kpi_def.higher_is_better:
                            if latest_value <= kpi_def.warning_threshold:
                                status = "warning"
                        else:
                            if latest_value >= kpi_def.warning_threshold:
                                status = "warning"
                
                calculations[kpi_name] = {
                    "latest_value": latest_value,
                    "hourly_avg": hourly_avg,
                    "daily_avg": daily_avg,
                    "daily_min": daily_min,
                    "daily_max": daily_max,
                    "status": status,
                    "unit": kpi_def.unit,
                    "category": kpi_def.category.value,
                    "target_value": kpi_def.target_value,
                    "data_points_hour": len(recent_values),
                    "data_points_day": len(daily_values)
                }
            
            # Calculate business event summaries
            event_summaries = {}
            recent_events = [e for e in self.business_events if e.timestamp >= last_hour]
            daily_events = [e for e in self.business_events if e.timestamp >= last_day]
            
            # Group events by type
            for event in daily_events:
                if event.event_type not in event_summaries:
                    event_summaries[event.event_type] = {
                        "hourly_count": 0,
                        "daily_count": 0,
                        "unique_events": set(),
                        "total_value": 0,
                        "avg_value": None
                    }
                
                event_summaries[event.event_type]["daily_count"] += 1
                event_summaries[event.event_type]["unique_events"].add(event.event_name)
                
                if event.value is not None:
                    event_summaries[event.event_type]["total_value"] += event.value
                
                if event.timestamp >= last_hour:
                    event_summaries[event.event_type]["hourly_count"] += 1
            
            # Calculate averages
            for event_type, summary in event_summaries.items():
                if summary["daily_count"] > 0 and summary["total_value"] > 0:
                    summary["avg_value"] = summary["total_value"] / summary["daily_count"]
                summary["unique_events"] = len(summary["unique_events"])
            
            self.realtime_calculations = {
                "timestamp": now.isoformat(),
                "kpis": calculations,
                "events": event_summaries,
                "summary": {
                    "total_kpis": len(calculations),
                    "kpis_with_data": len([k for k in calculations.values() if k["latest_value"] is not None]),
                    "kpis_critical": len([k for k in calculations.values() if k["status"] == "critical"]),
                    "kpis_warning": len([k for k in calculations.values() if k["status"] == "warning"]),
                    "total_events_hourly": sum(s["hourly_count"] for s in event_summaries.values()),
                    "total_events_daily": sum(s["daily_count"] for s in event_summaries.values()),
                    "unique_event_types": len(event_summaries)
                }
            }
            
            # Send summary metrics to DataDog
            if self.datadog_monitoring:
                await self._send_realtime_summary_to_datadog(self.realtime_calculations)
            
            return self.realtime_calculations
            
        except Exception as e:
            self.logger.error(f"Failed to calculate real-time KPIs: {str(e)}")
            return {}
    
    async def _send_realtime_summary_to_datadog(self, calculations: Dict[str, Any]):
        """Send real-time calculation summary to DataDog."""
        
        try:
            summary = calculations["summary"]
            
            # Send summary metrics
            self.datadog_monitoring.gauge("business.kpis.total", summary["total_kpis"])
            self.datadog_monitoring.gauge("business.kpis.with_data", summary["kpis_with_data"])
            self.datadog_monitoring.gauge("business.kpis.critical", summary["kpis_critical"])
            self.datadog_monitoring.gauge("business.kpis.warning", summary["kpis_warning"])
            self.datadog_monitoring.gauge("business.events.hourly", summary["total_events_hourly"])
            self.datadog_monitoring.gauge("business.events.daily", summary["total_events_daily"])
            
            # Send KPI health score
            total_monitored = summary["kpis_with_data"]
            if total_monitored > 0:
                healthy_kpis = total_monitored - summary["kpis_critical"] - summary["kpis_warning"]
                health_score = (healthy_kpis / total_monitored) * 100
                self.datadog_monitoring.gauge("business.kpis.health_score", health_score)
            
        except Exception as e:
            self.logger.error(f"Failed to send real-time summary to DataDog: {str(e)}")
    
    # Analytics and Reporting
    
    def get_kpi_dashboard_data(self) -> Dict[str, Any]:
        """Get KPI dashboard data."""
        
        dashboard_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "service_name": self.service_name,
            "kpi_definitions": {
                name: {
                    "name": kpi.name,
                    "description": kpi.description,
                    "category": kpi.category.value,
                    "unit": kpi.unit,
                    "target_value": kpi.target_value,
                    "warning_threshold": kpi.warning_threshold,
                    "critical_threshold": kpi.critical_threshold,
                    "higher_is_better": kpi.higher_is_better
                }
                for name, kpi in self.kpi_definitions.items()
            },
            "realtime_calculations": self.realtime_calculations,
            "statistics": {
                "total_kpis_defined": len(self.kpi_definitions),
                "total_kpi_values": sum(len(values) for values in self.kpi_values.values()),
                "total_business_events": len(self.business_events),
                "categories": list(set(kpi.category.value for kpi in self.kpi_definitions.values()))
            }
        }
        
        return dashboard_data


# Global business metrics tracker
_business_metrics_tracker: Optional[DataDogBusinessMetricsTracker] = None


def get_business_metrics_tracker(service_name: str = "business-intelligence",
                               datadog_monitoring: Optional[DatadogMonitoring] = None) -> DataDogBusinessMetricsTracker:
    """Get or create business metrics tracker."""
    global _business_metrics_tracker
    
    if _business_metrics_tracker is None:
        _business_metrics_tracker = DataDogBusinessMetricsTracker(service_name, datadog_monitoring)
    
    return _business_metrics_tracker


# Convenience functions for common business metrics

async def track_kpi_value(kpi_name: str, value: float, tags: Optional[Dict[str, str]] = None):
    """Convenience function for tracking KPI values."""
    tracker = get_business_metrics_tracker()
    return await tracker.track_kpi(kpi_name, value, tags)


async def track_revenue_event(amount: float, currency: str = "USD", customer_id: Optional[str] = None):
    """Convenience function for tracking revenue."""
    tracker = get_business_metrics_tracker()
    return await tracker.track_revenue(amount, currency, customer_id)


async def track_user_event(user_id: str, activity_type: str, session_id: Optional[str] = None):
    """Convenience function for tracking user activity."""
    tracker = get_business_metrics_tracker()
    return await tracker.track_user_activity(user_id, activity_type, session_id)


async def track_feature_event(feature_name: str, user_id: Optional[str] = None, usage_value: Optional[float] = None):
    """Convenience function for tracking feature usage."""
    tracker = get_business_metrics_tracker()
    return await tracker.track_feature_usage(feature_name, user_id, usage_value)


# Context manager for business operation tracking
@asynccontextmanager
async def track_business_operation(operation_name: str, category: str,
                                 expected_duration_seconds: Optional[float] = None):
    """Context manager for tracking business operations."""
    
    start_time = time.time()
    tracker = get_business_metrics_tracker()
    
    # Track operation start
    await tracker.track_business_event(BusinessEvent(
        event_type="operation",
        event_name=f"{operation_name}_started",
        properties={"category": category}
    ))
    
    try:
        yield tracker
        
        # Calculate duration and track success
        duration = time.time() - start_time
        
        await tracker.track_business_event(BusinessEvent(
            event_type="operation",
            event_name=f"{operation_name}_completed",
            value=duration,
            properties={"category": category, "status": "success"}
        ))
        
        # Track duration KPI if expected duration provided
        if expected_duration_seconds:
            performance_ratio = duration / expected_duration_seconds
            await tracker.track_kpi(
                f"operation_{operation_name}_performance",
                performance_ratio,
                tags={"category": category}
            )
        
    except Exception as e:
        # Track operation failure
        duration = time.time() - start_time
        
        await tracker.track_business_event(BusinessEvent(
            event_type="operation",
            event_name=f"{operation_name}_failed",
            value=duration,
            properties={
                "category": category,
                "status": "error",
                "error_type": type(e).__name__,
                "error_message": str(e)
            }
        ))
        
        raise
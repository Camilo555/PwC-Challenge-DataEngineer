"""
DataDog Post-Incident Analysis and Reporting System
Comprehensive post-incident analysis, root cause analysis, and continuous improvement
with automated reporting and knowledge base integration
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Set, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict, deque
import statistics
import hashlib
from concurrent.futures import ThreadPoolExecutor

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.datadog_incident_management import (
    DataDogIncidentManagement, Incident, IncidentStatus, IncidentSeverity
)

logger = get_logger(__name__)


class AnalysisType(Enum):
    """Types of post-incident analysis"""
    ROOT_CAUSE_ANALYSIS = "root_cause_analysis"
    TIMELINE_ANALYSIS = "timeline_analysis"
    IMPACT_ANALYSIS = "impact_analysis"
    RESPONSE_ANALYSIS = "response_analysis"
    PREVENTION_ANALYSIS = "prevention_analysis"
    COST_ANALYSIS = "cost_analysis"


class RootCauseCategory(Enum):
    """Root cause categories"""
    INFRASTRUCTURE = "infrastructure"
    SOFTWARE_DEFECT = "software_defect"
    CONFIGURATION_ERROR = "configuration_error"
    PROCESS_FAILURE = "process_failure"
    HUMAN_ERROR = "human_error"
    EXTERNAL_DEPENDENCY = "external_dependency"
    CAPACITY_ISSUES = "capacity_issues"
    SECURITY_INCIDENT = "security_incident"
    UNKNOWN = "unknown"


class ImprovementPriority(Enum):
    """Priority levels for improvement actions"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ReportFormat(Enum):
    """Report output formats"""
    JSON = "json"
    HTML = "html"
    PDF = "pdf"
    MARKDOWN = "markdown"
    EXECUTIVE_SUMMARY = "executive_summary"


@dataclass
class IncidentImpact:
    """Comprehensive incident impact analysis"""
    incident_id: str
    
    # Financial impact
    revenue_loss: float = 0.0
    recovery_cost: float = 0.0
    productivity_loss: float = 0.0
    reputation_damage_cost: float = 0.0
    total_financial_impact: float = 0.0
    
    # Customer impact
    affected_customers: int = 0
    customer_complaints: int = 0
    churn_risk_customers: int = 0
    support_tickets_created: int = 0
    
    # Business impact
    transactions_lost: int = 0
    service_degradation_minutes: float = 0.0
    sla_breaches: int = 0
    compliance_violations: List[str] = field(default_factory=list)
    
    # Technical impact
    systems_affected: List[str] = field(default_factory=list)
    data_loss_gb: float = 0.0
    downtime_minutes: float = 0.0
    performance_degradation_percent: float = 0.0
    
    # Operational impact
    escalations_required: int = 0
    staff_hours_spent: float = 0.0
    external_vendor_costs: float = 0.0


@dataclass
class RootCauseAnalysis:
    """Root cause analysis data structure"""
    incident_id: str
    analysis_timestamp: datetime
    analyst: str
    
    # Primary root cause
    primary_root_cause: str
    root_cause_category: RootCauseCategory
    confidence_level: float  # 0.0 to 1.0
    
    # Contributing factors
    contributing_factors: List[str] = field(default_factory=list)
    timeline_factors: List[Dict[str, Any]] = field(default_factory=list)
    
    # Analysis methodology
    analysis_methods_used: List[str] = field(default_factory=list)
    evidence_collected: List[Dict[str, Any]] = field(default_factory=list)
    interviews_conducted: List[str] = field(default_factory=list)
    
    # Technical analysis
    log_analysis_summary: Optional[str] = None
    metric_analysis_summary: Optional[str] = None
    trace_analysis_summary: Optional[str] = None
    
    # Why analysis (5 Whys)
    why_analysis: List[str] = field(default_factory=list)
    
    # Lessons learned
    lessons_learned: List[str] = field(default_factory=list)
    knowledge_gaps_identified: List[str] = field(default_factory=list)


@dataclass
class ImprovementAction:
    """Improvement action item"""
    action_id: str
    incident_id: str
    title: str
    description: str
    category: str
    priority: ImprovementPriority
    
    # Assignment and tracking
    assigned_to: Optional[str] = None
    assigned_team: Optional[str] = None
    due_date: Optional[datetime] = None
    estimated_effort_hours: Optional[float] = None
    
    # Implementation details
    implementation_plan: Optional[str] = None
    acceptance_criteria: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    
    # Status tracking
    status: str = "open"  # open, in_progress, completed, cancelled
    progress_percentage: float = 0.0
    completion_date: Optional[datetime] = None
    
    # Impact and validation
    expected_impact: Optional[str] = None
    success_metrics: List[str] = field(default_factory=list)
    validation_criteria: List[str] = field(default_factory=list)
    
    # Cost-benefit analysis
    implementation_cost: Optional[float] = None
    expected_savings: Optional[float] = None
    roi_estimate: Optional[float] = None


@dataclass
class PostIncidentReport:
    """Comprehensive post-incident report"""
    report_id: str
    incident_id: str
    report_timestamp: datetime
    report_author: str
    
    # Incident summary
    incident_summary: Dict[str, Any]
    incident_impact: IncidentImpact
    root_cause_analysis: RootCauseAnalysis
    
    # Timeline and response analysis
    detailed_timeline: List[Dict[str, Any]]
    response_effectiveness: Dict[str, Any]
    communication_analysis: Dict[str, Any]
    
    # Improvement recommendations
    improvement_actions: List[ImprovementAction]
    process_improvements: List[str] = field(default_factory=list)
    technology_improvements: List[str] = field(default_factory=list)
    training_recommendations: List[str] = field(default_factory=list)
    
    # Metrics and KPIs
    response_metrics: Dict[str, float]
    comparison_to_similar_incidents: Dict[str, Any]
    trend_analysis: Dict[str, Any]
    
    # Executive summary
    executive_summary: str = ""
    key_findings: List[str] = field(default_factory=list)
    critical_actions: List[str] = field(default_factory=list)
    
    # Compliance and regulatory
    compliance_impact: List[str] = field(default_factory=list)
    regulatory_notifications_required: List[str] = field(default_factory=list)
    
    # Report metadata
    report_status: str = "draft"  # draft, review, approved, published
    reviewers: List[str] = field(default_factory=list)
    approval_date: Optional[datetime] = None


class DataDogPostIncidentAnalysis:
    """
    Comprehensive DataDog Post-Incident Analysis and Reporting System
    
    Features:
    - Automated incident data collection and analysis
    - Root cause analysis with multiple methodologies
    - Comprehensive impact assessment (financial, customer, technical)
    - Timeline reconstruction and response analysis
    - Automated improvement action generation
    - Trend analysis and pattern recognition
    - Knowledge base integration and learning
    - Multi-format report generation
    - Executive and technical report variants
    - Compliance and regulatory reporting
    - Continuous improvement tracking
    - ROI analysis for improvement actions
    """
    
    def __init__(self, service_name: str = "post-incident-analysis",
                 datadog_monitoring: Optional[DatadogMonitoring] = None,
                 incident_management: Optional[DataDogIncidentManagement] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.incident_management = incident_management
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Analysis data storage
        self.incident_analyses: Dict[str, RootCauseAnalysis] = {}
        self.incident_impacts: Dict[str, IncidentImpact] = {}
        self.post_incident_reports: Dict[str, PostIncidentReport] = {}
        self.improvement_actions: Dict[str, ImprovementAction] = {}
        
        # Knowledge base and learning
        self.incident_patterns: Dict[str, List[str]] = {}
        self.root_cause_patterns: Dict[RootCauseCategory, List[str]] = defaultdict(list)
        self.improvement_effectiveness: Dict[str, Dict[str, Any]] = {}
        
        # Analysis templates and methodologies
        self.analysis_templates = {}
        self.rca_methodologies = [
            "5_whys", "fishbone_diagram", "fault_tree_analysis",
            "timeline_analysis", "change_analysis"
        ]
        
        # Reporting configuration
        self.report_templates = {}
        self.executive_kpis = [
            "mttr", "mtbf", "customer_impact", "revenue_impact",
            "improvement_completion_rate", "recurrence_rate"
        ]
        
        # Performance metrics
        self.analysis_metrics = {
            "total_analyses_completed": 0,
            "average_analysis_time_hours": 0.0,
            "root_cause_identification_rate": 0.0,
            "improvement_action_completion_rate": 0.0,
            "recurrence_prevention_rate": 0.0
        }
        
        # Initialize system
        self._initialize_analysis_templates()
        self._initialize_report_templates()
        
        # Start background services
        asyncio.create_task(self._start_analysis_services())
        
        self.logger.info(f"Post-incident analysis system initialized for {service_name}")
    
    def _initialize_analysis_templates(self):
        """Initialize analysis templates and methodologies"""
        
        try:
            # Root cause analysis template
            self.analysis_templates["root_cause_analysis"] = {
                "required_fields": [
                    "primary_root_cause", "root_cause_category", "confidence_level"
                ],
                "optional_fields": [
                    "contributing_factors", "timeline_factors", "why_analysis"
                ],
                "analysis_steps": [
                    "data_collection", "timeline_reconstruction", "hypothesis_formation",
                    "evidence_analysis", "root_cause_identification", "validation"
                ]
            }
            
            # Impact analysis template
            self.analysis_templates["impact_analysis"] = {
                "financial_factors": [
                    "revenue_loss", "recovery_cost", "productivity_loss", "reputation_damage"
                ],
                "customer_factors": [
                    "affected_customers", "customer_complaints", "churn_risk"
                ],
                "technical_factors": [
                    "systems_affected", "downtime_minutes", "performance_degradation"
                ]
            }
            
            # Timeline analysis template
            self.analysis_templates["timeline_analysis"] = {
                "key_events": [
                    "incident_detection", "first_response", "escalation_points",
                    "major_actions", "resolution_actions", "recovery_completion"
                ],
                "analysis_points": [
                    "detection_delay", "response_time", "escalation_effectiveness",
                    "communication_quality", "resolution_efficiency"
                ]
            }
            
            self.logger.info("Analysis templates initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize analysis templates: {str(e)}")
    
    def _initialize_report_templates(self):
        """Initialize report templates"""
        
        try:
            # Executive summary template
            self.report_templates["executive_summary"] = {
                "sections": [
                    "incident_overview", "business_impact", "root_cause_summary",
                    "immediate_actions", "improvement_plan", "prevention_measures"
                ],
                "key_metrics": [
                    "total_downtime", "customers_affected", "revenue_impact",
                    "resolution_time", "improvement_actions_count"
                ]
            }
            
            # Technical report template
            self.report_templates["technical_report"] = {
                "sections": [
                    "incident_details", "technical_timeline", "system_analysis",
                    "root_cause_analysis", "technical_improvements", "monitoring_enhancements"
                ],
                "technical_metrics": [
                    "error_rates", "performance_metrics", "system_utilization",
                    "alert_effectiveness", "monitoring_coverage"
                ]
            }
            
            # Compliance report template
            self.report_templates["compliance_report"] = {
                "sections": [
                    "incident_classification", "regulatory_impact", "notification_requirements",
                    "compliance_violations", "remediation_actions", "process_improvements"
                ],
                "compliance_frameworks": [
                    "SOX", "GDPR", "PCI_DSS", "HIPAA", "SOC2"
                ]
            }
            
            self.logger.info("Report templates initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize report templates: {str(e)}")
    
    async def _start_analysis_services(self):
        """Start background analysis services"""
        
        try:
            # Incident analysis monitoring
            asyncio.create_task(self._incident_analysis_service())
            
            # Improvement action tracking
            asyncio.create_task(self._improvement_tracking_service())
            
            # Pattern recognition and learning
            asyncio.create_task(self._pattern_recognition_service())
            
            # Report generation automation
            asyncio.create_task(self._automated_reporting_service())
            
            # Analytics and metrics calculation
            asyncio.create_task(self._analytics_service())
            
            self.logger.info("Post-incident analysis services started")
            
        except Exception as e:
            self.logger.error(f"Failed to start analysis services: {str(e)}")
    
    async def _incident_analysis_service(self):
        """Background service for monitoring incidents and triggering analysis"""
        
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                
                if self.incident_management:
                    # Get recently resolved incidents
                    resolved_incidents = self._get_resolved_incidents_needing_analysis()
                    
                    for incident in resolved_incidents:
                        await self._initiate_post_incident_analysis(incident)
                
            except Exception as e:
                self.logger.error(f"Error in incident analysis service: {str(e)}")
                await asyncio.sleep(300)
    
    def _get_resolved_incidents_needing_analysis(self) -> List[Dict[str, Any]]:
        """Get resolved incidents that need post-incident analysis"""
        
        try:
            if not self.incident_management:
                return []
            
            all_incidents = []
            for incident_id, incident in self.incident_management.incidents.items():
                if (incident.status == IncidentStatus.RESOLVED and
                    incident_id not in self.post_incident_reports and
                    incident.resolved_at and
                    (datetime.utcnow() - incident.resolved_at).total_seconds() > 3600):  # 1 hour after resolution
                    
                    all_incidents.append(incident.to_dict())
            
            return all_incidents
            
        except Exception as e:
            self.logger.error(f"Failed to get resolved incidents: {str(e)}")
            return []
    
    async def _initiate_post_incident_analysis(self, incident: Dict[str, Any]):
        """Initiate post-incident analysis for resolved incident"""
        
        try:
            incident_id = incident.get("incident_id")
            if not incident_id:
                return
            
            self.logger.info(f"Initiating post-incident analysis for {incident_id}")
            
            # Collect incident data
            incident_data = await self._collect_incident_data(incident_id)
            
            # Perform impact analysis
            impact_analysis = await self._perform_impact_analysis(incident_data)
            self.incident_impacts[incident_id] = impact_analysis
            
            # Initiate root cause analysis
            rca = await self._initiate_root_cause_analysis(incident_data)
            self.incident_analyses[incident_id] = rca
            
            # Generate improvement actions
            improvements = await self._generate_improvement_actions(incident_data, rca, impact_analysis)
            for improvement in improvements:
                self.improvement_actions[improvement.action_id] = improvement
            
            # Create post-incident report
            report = await self._create_post_incident_report(incident_data, rca, impact_analysis, improvements)
            self.post_incident_reports[incident_id] = report
            
            # Update metrics
            self.analysis_metrics["total_analyses_completed"] += 1
            
            if self.datadog_monitoring:
                self.datadog_monitoring.counter(
                    "post_incident.analysis_initiated",
                    tags=[f"incident_id:{incident_id}", f"severity:{incident.get('severity')}"]
                )
            
        except Exception as e:
            self.logger.error(f"Failed to initiate post-incident analysis: {str(e)}")
    
    async def _collect_incident_data(self, incident_id: str) -> Dict[str, Any]:
        """Collect comprehensive incident data for analysis"""
        
        try:
            incident_data = {
                "incident_id": incident_id,
                "collection_timestamp": datetime.utcnow().isoformat()
            }
            
            if self.incident_management and incident_id in self.incident_management.incidents:
                incident = self.incident_management.incidents[incident_id]
                incident_data["incident_details"] = incident.to_dict()
                
                # Collect related alerts
                incident_data["related_alerts"] = incident.alerts
                
                # Collect timeline events
                incident_data["timeline"] = incident.timeline
                
                # Collect response actions
                incident_data["response_actions"] = incident.response_actions_taken
            
            # Collect metrics data (simulated for this example)
            incident_data["metrics"] = await self._collect_incident_metrics(incident_id)
            
            # Collect log data (simulated)
            incident_data["logs"] = await self._collect_incident_logs(incident_id)
            
            # Collect trace data (simulated)
            incident_data["traces"] = await self._collect_incident_traces(incident_id)
            
            return incident_data
            
        except Exception as e:
            self.logger.error(f"Failed to collect incident data: {str(e)}")
            return {"incident_id": incident_id, "error": str(e)}
    
    async def _collect_incident_metrics(self, incident_id: str) -> Dict[str, Any]:
        """Collect metrics data related to incident"""
        
        try:
            # In production, this would query actual DataDog metrics
            # Simulate metrics collection
            import random
            
            return {
                "cpu_utilization": random.uniform(30, 95),
                "memory_utilization": random.uniform(40, 90),
                "error_rate": random.uniform(0, 25),
                "response_time_p95": random.uniform(100, 3000),
                "throughput": random.uniform(100, 1000),
                "database_connections": random.uniform(50, 200),
                "cache_hit_ratio": random.uniform(60, 95)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to collect incident metrics: {str(e)}")
            return {}
    
    async def _collect_incident_logs(self, incident_id: str) -> List[Dict[str, Any]]:
        """Collect log data related to incident"""
        
        try:
            # In production, this would query actual log data
            # Simulate log collection
            logs = []
            
            for i in range(10):
                logs.append({
                    "timestamp": (datetime.utcnow() - timedelta(minutes=i*5)).isoformat(),
                    "level": random.choice(["ERROR", "WARN", "INFO"]),
                    "service": random.choice(["api", "database", "cache"]),
                    "message": f"Simulated log message {i}",
                    "correlation_id": f"corr_{incident_id}_{i}"
                })
            
            return logs
            
        except Exception as e:
            self.logger.error(f"Failed to collect incident logs: {str(e)}")
            return []
    
    async def _collect_incident_traces(self, incident_id: str) -> List[Dict[str, Any]]:
        """Collect trace data related to incident"""
        
        try:
            # In production, this would query actual trace data
            # Simulate trace collection
            traces = []
            
            for i in range(5):
                traces.append({
                    "trace_id": f"trace_{incident_id}_{i}",
                    "span_count": random.randint(5, 50),
                    "duration_ms": random.uniform(100, 5000),
                    "error_count": random.randint(0, 10),
                    "service_count": random.randint(2, 8)
                })
            
            return traces
            
        except Exception as e:
            self.logger.error(f"Failed to collect incident traces: {str(e)}")
            return []
    
    async def _perform_impact_analysis(self, incident_data: Dict[str, Any]) -> IncidentImpact:
        """Perform comprehensive impact analysis"""
        
        try:
            incident_id = incident_data.get("incident_id")
            incident_details = incident_data.get("incident_details", {})
            
            # Calculate financial impact
            severity = incident_details.get("severity", "medium")
            downtime_minutes = 0.0
            
            if incident_details.get("resolved_at") and incident_details.get("created_at"):
                resolved_time = datetime.fromisoformat(incident_details["resolved_at"])
                created_time = datetime.fromisoformat(incident_details["created_at"])
                downtime_minutes = (resolved_time - created_time).total_seconds() / 60
            
            # Revenue impact calculation (simplified)
            revenue_per_minute = self._get_revenue_per_minute(severity)
            revenue_loss = downtime_minutes * revenue_per_minute
            
            # Recovery cost estimation
            staff_cost_per_hour = 150  # Average cost
            staff_hours = downtime_minutes / 60 * self._get_staff_count(severity)
            recovery_cost = staff_hours * staff_cost_per_hour
            
            # Customer impact estimation
            customer_base = 10000  # Total customers
            impact_percentage = self._get_customer_impact_percentage(severity)
            affected_customers = int(customer_base * impact_percentage)
            
            impact = IncidentImpact(
                incident_id=incident_id,
                revenue_loss=revenue_loss,
                recovery_cost=recovery_cost,
                productivity_loss=recovery_cost * 0.5,  # 50% of recovery cost
                total_financial_impact=revenue_loss + recovery_cost,
                affected_customers=affected_customers,
                customer_complaints=int(affected_customers * 0.1),  # 10% complain
                churn_risk_customers=int(affected_customers * 0.05),  # 5% churn risk
                systems_affected=self._extract_affected_systems(incident_data),
                downtime_minutes=downtime_minutes,
                performance_degradation_percent=self._calculate_performance_degradation(incident_data),
                escalations_required=incident_details.get("escalation_count", 0),
                staff_hours_spent=staff_hours
            )
            
            self.logger.info(f"Impact analysis completed for {incident_id}: ${impact.total_financial_impact:.2f} total impact")
            
            return impact
            
        except Exception as e:
            self.logger.error(f"Failed to perform impact analysis: {str(e)}")
            return IncidentImpact(incident_id=incident_data.get("incident_id", "unknown"))
    
    def _get_revenue_per_minute(self, severity: str) -> float:
        """Get estimated revenue per minute based on severity"""
        
        revenue_rates = {
            "sev1": 1000.0,  # $1000/minute for critical systems
            "sev2": 500.0,   # $500/minute for major systems
            "sev3": 100.0,   # $100/minute for minor systems
            "sev4": 25.0     # $25/minute for low impact
        }
        
        return revenue_rates.get(severity, 100.0)
    
    def _get_staff_count(self, severity: str) -> int:
        """Get estimated staff count involved based on severity"""
        
        staff_counts = {
            "sev1": 10,  # 10 people for critical incidents
            "sev2": 6,   # 6 people for major incidents
            "sev3": 3,   # 3 people for minor incidents
            "sev4": 1    # 1 person for low impact
        }
        
        return staff_counts.get(severity, 3)
    
    def _get_customer_impact_percentage(self, severity: str) -> float:
        """Get percentage of customers affected based on severity"""
        
        impact_percentages = {
            "sev1": 0.8,   # 80% of customers affected
            "sev2": 0.4,   # 40% of customers affected
            "sev3": 0.1,   # 10% of customers affected
            "sev4": 0.01   # 1% of customers affected
        }
        
        return impact_percentages.get(severity, 0.1)
    
    def _extract_affected_systems(self, incident_data: Dict[str, Any]) -> List[str]:
        """Extract list of affected systems from incident data"""
        
        try:
            systems = set()
            
            # Extract from incident context
            incident_details = incident_data.get("incident_details", {})
            context = incident_details.get("context", {})
            if "affected_services" in context:
                systems.update(context["affected_services"])
            
            # Extract from alerts
            alerts = incident_data.get("related_alerts", [])
            for alert in alerts[:5]:  # Limit to prevent too many systems
                systems.add(f"alert_system_{alert}")
            
            # Extract from logs
            logs = incident_data.get("logs", [])
            for log in logs[:10]:
                if "service" in log:
                    systems.add(log["service"])
            
            return list(systems)
            
        except Exception as e:
            self.logger.error(f"Failed to extract affected systems: {str(e)}")
            return ["unknown"]
    
    def _calculate_performance_degradation(self, incident_data: Dict[str, Any]) -> float:
        """Calculate performance degradation percentage"""
        
        try:
            metrics = incident_data.get("metrics", {})
            
            # Simple degradation calculation based on response time
            response_time = metrics.get("response_time_p95", 500)
            baseline_response_time = 200  # Baseline 200ms
            
            degradation = max(0, (response_time - baseline_response_time) / baseline_response_time * 100)
            return min(degradation, 100)  # Cap at 100%
            
        except Exception as e:
            self.logger.error(f"Failed to calculate performance degradation: {str(e)}")
            return 0.0
    
    async def _initiate_root_cause_analysis(self, incident_data: Dict[str, Any]) -> RootCauseAnalysis:
        """Initiate comprehensive root cause analysis"""
        
        try:
            incident_id = incident_data.get("incident_id")
            
            # Perform automated analysis
            rca = RootCauseAnalysis(
                incident_id=incident_id,
                analysis_timestamp=datetime.utcnow(),
                analyst="automated_system",
                primary_root_cause="",
                root_cause_category=RootCauseCategory.UNKNOWN,
                confidence_level=0.0
            )
            
            # Analyze timeline for patterns
            timeline_analysis = await self._analyze_timeline_patterns(incident_data)
            rca.timeline_factors = timeline_analysis
            
            # Perform automated root cause identification
            root_cause_result = await self._identify_root_cause(incident_data)
            rca.primary_root_cause = root_cause_result["primary_cause"]
            rca.root_cause_category = root_cause_result["category"]
            rca.confidence_level = root_cause_result["confidence"]
            rca.contributing_factors = root_cause_result["contributing_factors"]
            
            # Perform 5 Whys analysis
            rca.why_analysis = await self._perform_5_whys_analysis(incident_data, root_cause_result)
            
            # Collect evidence
            rca.evidence_collected = await self._collect_analysis_evidence(incident_data)
            
            # Generate lessons learned
            rca.lessons_learned = await self._generate_lessons_learned(incident_data, root_cause_result)
            
            # Set analysis methods used
            rca.analysis_methods_used = [
                "timeline_analysis", "pattern_matching", "5_whys", "evidence_correlation"
            ]
            
            self.logger.info(f"Root cause analysis completed for {incident_id}: {rca.primary_root_cause}")
            
            return rca
            
        except Exception as e:
            self.logger.error(f"Failed to initiate root cause analysis: {str(e)}")
            return RootCauseAnalysis(
                incident_id=incident_data.get("incident_id", "unknown"),
                analysis_timestamp=datetime.utcnow(),
                analyst="error",
                primary_root_cause="Analysis failed",
                root_cause_category=RootCauseCategory.UNKNOWN,
                confidence_level=0.0
            )
    
    async def _analyze_timeline_patterns(self, incident_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze timeline for patterns and key events"""
        
        try:
            timeline = incident_data.get("incident_details", {}).get("timeline", [])
            patterns = []
            
            # Analyze timeline events
            for i, event in enumerate(timeline):
                pattern = {
                    "sequence": i,
                    "event_type": event.get("event_type", "unknown"),
                    "timestamp": event.get("timestamp"),
                    "description": event.get("description", ""),
                    "significance": "normal"
                }
                
                # Identify significant events
                if "escalat" in event.get("description", "").lower():
                    pattern["significance"] = "escalation"
                elif "resolv" in event.get("description", "").lower():
                    pattern["significance"] = "resolution"
                elif "fail" in event.get("description", "").lower():
                    pattern["significance"] = "failure"
                
                patterns.append(pattern)
            
            return patterns
            
        except Exception as e:
            self.logger.error(f"Failed to analyze timeline patterns: {str(e)}")
            return []
    
    async def _identify_root_cause(self, incident_data: Dict[str, Any]) -> Dict[str, Any]:
        """Identify root cause using automated analysis"""
        
        try:
            metrics = incident_data.get("metrics", {})
            logs = incident_data.get("logs", [])
            incident_details = incident_data.get("incident_details", {})
            
            # Simple rule-based root cause identification
            root_cause = "Unknown cause"
            category = RootCauseCategory.UNKNOWN
            confidence = 0.5
            contributing_factors = []
            
            # Analyze metrics for patterns
            if metrics.get("cpu_utilization", 0) > 90:
                root_cause = "High CPU utilization causing performance degradation"
                category = RootCauseCategory.CAPACITY_ISSUES
                confidence = 0.8
                contributing_factors.append("CPU threshold exceeded")
            
            elif metrics.get("memory_utilization", 0) > 85:
                root_cause = "Memory exhaustion leading to system instability"
                category = RootCauseCategory.CAPACITY_ISSUES
                confidence = 0.8
                contributing_factors.append("Memory threshold exceeded")
            
            elif metrics.get("error_rate", 0) > 10:
                root_cause = "High error rate indicating application or service failure"
                category = RootCauseCategory.SOFTWARE_DEFECT
                confidence = 0.7
                contributing_factors.append("Error rate spike detected")
            
            elif metrics.get("response_time_p95", 0) > 2000:
                root_cause = "Performance degradation due to slow response times"
                category = RootCauseCategory.INFRASTRUCTURE
                confidence = 0.6
                contributing_factors.append("Response time degradation")
            
            # Analyze logs for error patterns
            error_logs = [log for log in logs if log.get("level") == "ERROR"]
            if len(error_logs) > 5:
                contributing_factors.append(f"Multiple error logs detected ({len(error_logs)} errors)")
                if category == RootCauseCategory.UNKNOWN:
                    root_cause = "Multiple application errors indicating software issues"
                    category = RootCauseCategory.SOFTWARE_DEFECT
                    confidence = 0.6
            
            # Analyze incident characteristics
            severity = incident_details.get("severity", "medium")
            if severity == "sev1" and category == RootCauseCategory.UNKNOWN:
                root_cause = "Critical system failure requiring immediate attention"
                category = RootCauseCategory.INFRASTRUCTURE
                confidence = 0.5
            
            return {
                "primary_cause": root_cause,
                "category": category,
                "confidence": confidence,
                "contributing_factors": contributing_factors
            }
            
        except Exception as e:
            self.logger.error(f"Failed to identify root cause: {str(e)}")
            return {
                "primary_cause": "Analysis failed",
                "category": RootCauseCategory.UNKNOWN,
                "confidence": 0.0,
                "contributing_factors": []
            }
    
    async def _perform_5_whys_analysis(self, incident_data: Dict[str, Any], root_cause_result: Dict[str, Any]) -> List[str]:
        """Perform 5 Whys analysis"""
        
        try:
            primary_cause = root_cause_result.get("primary_cause", "Unknown")
            whys = []
            
            # Generate 5 Whys based on root cause category
            category = root_cause_result.get("category", RootCauseCategory.UNKNOWN)
            
            if category == RootCauseCategory.CAPACITY_ISSUES:
                whys = [
                    f"Why did the system experience capacity issues? {primary_cause}",
                    "Why wasn't the capacity limit detected earlier? Monitoring thresholds were not properly configured",
                    "Why weren't the monitoring thresholds configured? Lack of baseline capacity planning",
                    "Why wasn't baseline capacity planning performed? Process gap in capacity management",
                    "Why was there a process gap? Insufficient capacity management procedures"
                ]
            
            elif category == RootCauseCategory.SOFTWARE_DEFECT:
                whys = [
                    f"Why did the software defect occur? {primary_cause}",
                    "Why wasn't the defect caught in testing? Insufficient test coverage",
                    "Why was test coverage insufficient? Testing process gaps",
                    "Why were there testing process gaps? Inadequate quality assurance procedures",
                    "Why were QA procedures inadequate? Need for improved development lifecycle"
                ]
            
            elif category == RootCauseCategory.INFRASTRUCTURE:
                whys = [
                    f"Why did the infrastructure fail? {primary_cause}",
                    "Why wasn't the infrastructure issue prevented? Lack of predictive monitoring",
                    "Why wasn't predictive monitoring in place? Infrastructure monitoring gaps",
                    "Why were there monitoring gaps? Insufficient infrastructure visibility",
                    "Why was infrastructure visibility insufficient? Need for enhanced monitoring strategy"
                ]
            
            else:
                whys = [
                    f"Why did this incident occur? {primary_cause}",
                    "Why wasn't this prevented? Insufficient preventive measures",
                    "Why weren't preventive measures in place? Process improvement needed",
                    "Why wasn't the process improved earlier? Lack of continuous improvement",
                    "Why wasn't continuous improvement implemented? Need for systematic improvement process"
                ]
            
            return whys
            
        except Exception as e:
            self.logger.error(f"Failed to perform 5 Whys analysis: {str(e)}")
            return ["Analysis could not be completed due to error"]
    
    async def _collect_analysis_evidence(self, incident_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Collect evidence for root cause analysis"""
        
        try:
            evidence = []
            
            # Metrics evidence
            metrics = incident_data.get("metrics", {})
            for metric, value in metrics.items():
                evidence.append({
                    "type": "metric",
                    "source": "datadog_metrics",
                    "metric": metric,
                    "value": value,
                    "significance": "high" if self._is_anomalous_metric(metric, value) else "normal"
                })
            
            # Log evidence
            logs = incident_data.get("logs", [])
            error_logs = [log for log in logs if log.get("level") == "ERROR"]
            for log in error_logs[:5]:  # Top 5 error logs
                evidence.append({
                    "type": "log",
                    "source": "application_logs",
                    "timestamp": log.get("timestamp"),
                    "message": log.get("message"),
                    "service": log.get("service"),
                    "significance": "high"
                })
            
            # Timeline evidence
            timeline = incident_data.get("incident_details", {}).get("timeline", [])
            for event in timeline:
                if event.get("event_type") in ["escalation_step_executed", "response_action_executed"]:
                    evidence.append({
                        "type": "timeline_event",
                        "source": "incident_management",
                        "event_type": event.get("event_type"),
                        "description": event.get("description"),
                        "timestamp": event.get("timestamp"),
                        "significance": "medium"
                    })
            
            return evidence
            
        except Exception as e:
            self.logger.error(f"Failed to collect analysis evidence: {str(e)}")
            return []
    
    def _is_anomalous_metric(self, metric: str, value: float) -> bool:
        """Determine if metric value is anomalous"""
        
        thresholds = {
            "cpu_utilization": 80,
            "memory_utilization": 80,
            "error_rate": 5,
            "response_time_p95": 1000
        }
        
        return value > thresholds.get(metric, float('inf'))
    
    async def _generate_lessons_learned(self, incident_data: Dict[str, Any], root_cause_result: Dict[str, Any]) -> List[str]:
        """Generate lessons learned from the incident"""
        
        try:
            lessons = []
            
            category = root_cause_result.get("category", RootCauseCategory.UNKNOWN)
            
            # Category-specific lessons
            if category == RootCauseCategory.CAPACITY_ISSUES:
                lessons.extend([
                    "Implement proactive capacity monitoring and alerting",
                    "Establish capacity planning processes with growth projections",
                    "Set up automated scaling policies for critical systems"
                ])
            
            elif category == RootCauseCategory.SOFTWARE_DEFECT:
                lessons.extend([
                    "Enhance testing coverage for critical code paths",
                    "Implement automated testing in CI/CD pipeline",
                    "Establish code review processes for quality assurance"
                ])
            
            elif category == RootCauseCategory.INFRASTRUCTURE:
                lessons.extend([
                    "Improve infrastructure monitoring and observability",
                    "Implement redundancy and failover mechanisms",
                    "Establish infrastructure health checks and validation"
                ])
            
            # General lessons based on incident characteristics
            incident_details = incident_data.get("incident_details", {})
            
            if incident_details.get("time_to_acknowledge", 0) > 15:
                lessons.append("Improve alert notification and escalation procedures")
            
            if incident_details.get("time_to_resolve", 0) > 60:
                lessons.append("Develop faster incident response and resolution procedures")
            
            if incident_details.get("escalation_count", 0) > 2:
                lessons.append("Enhance first-line response capabilities to reduce escalations")
            
            # Communication lessons
            lessons.append("Establish clear communication channels for incident response")
            lessons.append("Document incident response procedures and runbooks")
            
            return lessons
            
        except Exception as e:
            self.logger.error(f"Failed to generate lessons learned: {str(e)}")
            return ["Unable to generate lessons learned due to analysis error"]
    
    async def _generate_improvement_actions(self, incident_data: Dict[str, Any], 
                                          rca: RootCauseAnalysis, 
                                          impact: IncidentImpact) -> List[ImprovementAction]:
        """Generate improvement actions based on analysis"""
        
        try:
            actions = []
            incident_id = incident_data.get("incident_id")
            
            # High-priority actions based on root cause
            if rca.root_cause_category == RootCauseCategory.CAPACITY_ISSUES:
                actions.append(ImprovementAction(
                    action_id=f"imp_{incident_id}_capacity_monitoring",
                    incident_id=incident_id,
                    title="Implement Advanced Capacity Monitoring",
                    description="Deploy predictive capacity monitoring with automated alerting",
                    category="monitoring",
                    priority=ImprovementPriority.HIGH,
                    estimated_effort_hours=40,
                    due_date=datetime.utcnow() + timedelta(days=30),
                    implementation_plan="Install capacity monitoring tools, configure alerts, establish baselines",
                    acceptance_criteria=["Capacity alerts configured", "Baseline metrics established", "Automated reporting enabled"],
                    expected_impact="Prevent capacity-related incidents",
                    implementation_cost=15000,
                    expected_savings=impact.total_financial_impact * 0.8
                ))
            
            elif rca.root_cause_category == RootCauseCategory.SOFTWARE_DEFECT:
                actions.append(ImprovementAction(
                    action_id=f"imp_{incident_id}_testing_enhancement",
                    incident_id=incident_id,
                    title="Enhanced Testing and Quality Assurance",
                    description="Improve testing coverage and implement additional quality gates",
                    category="quality_assurance",
                    priority=ImprovementPriority.HIGH,
                    estimated_effort_hours=80,
                    due_date=datetime.utcnow() + timedelta(days=45),
                    implementation_plan="Audit current testing, implement new test cases, enhance CI/CD pipeline",
                    acceptance_criteria=["Increased test coverage to >90%", "Automated quality gates", "Code review process enhanced"],
                    expected_impact="Reduce software defect incidents by 70%",
                    implementation_cost=25000,
                    expected_savings=impact.total_financial_impact * 0.7
                ))
            
            # Medium-priority actions based on impact
            if impact.downtime_minutes > 60:
                actions.append(ImprovementAction(
                    action_id=f"imp_{incident_id}_response_time",
                    incident_id=incident_id,
                    title="Improve Incident Response Time",
                    description="Enhance incident response procedures and automation",
                    category="incident_response",
                    priority=ImprovementPriority.MEDIUM,
                    estimated_effort_hours=20,
                    due_date=datetime.utcnow() + timedelta(days=21),
                    implementation_plan="Review response procedures, implement automation, train team",
                    acceptance_criteria=["Response time < 5 minutes", "Automated initial response", "Team training completed"],
                    expected_impact="Reduce average incident response time by 50%"
                ))
            
            # Documentation and knowledge sharing actions
            actions.append(ImprovementAction(
                action_id=f"imp_{incident_id}_documentation",
                incident_id=incident_id,
                title="Update Incident Response Documentation",
                description="Update runbooks and procedures based on lessons learned",
                category="documentation",
                priority=ImprovementPriority.LOW,
                estimated_effort_hours=8,
                due_date=datetime.utcnow() + timedelta(days=14),
                implementation_plan="Review current documentation, incorporate lessons learned, distribute updates",
                acceptance_criteria=["Runbooks updated", "Procedures documented", "Team awareness confirmed"],
                expected_impact="Improve future incident response effectiveness"
            ))
            
            # Monitoring enhancements
            if "monitoring" not in [a.category for a in actions]:
                actions.append(ImprovementAction(
                    action_id=f"imp_{incident_id}_monitoring",
                    incident_id=incident_id,
                    title="Enhance System Monitoring",
                    description="Improve monitoring coverage and alert quality",
                    category="monitoring",
                    priority=ImprovementPriority.MEDIUM,
                    estimated_effort_hours=30,
                    due_date=datetime.utcnow() + timedelta(days=30),
                    implementation_plan="Audit current monitoring, identify gaps, implement improvements",
                    acceptance_criteria=["Monitoring gaps addressed", "Alert quality improved", "Coverage increased"],
                    expected_impact="Earlier detection of similar issues"
                ))
            
            return actions
            
        except Exception as e:
            self.logger.error(f"Failed to generate improvement actions: {str(e)}")
            return []
    
    async def _create_post_incident_report(self, incident_data: Dict[str, Any], 
                                         rca: RootCauseAnalysis, 
                                         impact: IncidentImpact, 
                                         improvements: List[ImprovementAction]) -> PostIncidentReport:
        """Create comprehensive post-incident report"""
        
        try:
            incident_id = incident_data.get("incident_id")
            incident_details = incident_data.get("incident_details", {})
            
            # Create detailed timeline
            timeline = incident_details.get("timeline", [])
            detailed_timeline = []
            
            for event in timeline:
                detailed_timeline.append({
                    "timestamp": event.get("timestamp"),
                    "event_type": event.get("event_type"),
                    "description": event.get("description"),
                    "metadata": event.get("metadata", {}),
                    "significance": self._assess_event_significance(event)
                })
            
            # Analyze response effectiveness
            response_effectiveness = await self._analyze_response_effectiveness(incident_data)
            
            # Generate executive summary
            executive_summary = await self._generate_executive_summary(incident_details, impact, rca)
            
            # Create comprehensive report
            report = PostIncidentReport(
                report_id=f"pir_{incident_id}_{int(time.time())}",
                incident_id=incident_id,
                report_timestamp=datetime.utcnow(),
                report_author="automated_analysis_system",
                incident_summary=incident_details,
                incident_impact=impact,
                root_cause_analysis=rca,
                detailed_timeline=detailed_timeline,
                response_effectiveness=response_effectiveness,
                communication_analysis=await self._analyze_communication_effectiveness(incident_data),
                improvement_actions=improvements,
                response_metrics=await self._calculate_response_metrics(incident_data),
                comparison_to_similar_incidents=await self._compare_to_similar_incidents(incident_id, rca),
                trend_analysis=await self._perform_trend_analysis(incident_id),
                executive_summary=executive_summary,
                key_findings=await self._extract_key_findings(rca, impact),
                critical_actions=[action.title for action in improvements if action.priority in [ImprovementPriority.CRITICAL, ImprovementPriority.HIGH]],
                compliance_impact=await self._assess_compliance_impact(incident_details, impact),
                report_status="draft"
            )
            
            self.logger.info(f"Post-incident report created for {incident_id}")
            
            return report
            
        except Exception as e:
            self.logger.error(f"Failed to create post-incident report: {str(e)}")
            return PostIncidentReport(
                report_id=f"error_{incident_data.get('incident_id', 'unknown')}",
                incident_id=incident_data.get("incident_id", "unknown"),
                report_timestamp=datetime.utcnow(),
                report_author="error",
                incident_summary={},
                incident_impact=impact,
                root_cause_analysis=rca,
                detailed_timeline=[],
                response_effectiveness={},
                communication_analysis={},
                improvement_actions=[],
                response_metrics={},
                comparison_to_similar_incidents={},
                trend_analysis={}
            )
    
    def _assess_event_significance(self, event: Dict[str, Any]) -> str:
        """Assess significance of timeline event"""
        
        event_type = event.get("event_type", "")
        description = event.get("description", "").lower()
        
        if event_type in ["incident_created", "incident_resolved"]:
            return "critical"
        elif event_type in ["escalation_step_executed", "response_action_executed"]:
            return "high"
        elif "critical" in description or "urgent" in description:
            return "high"
        elif event_type in ["incident_acknowledged", "alert_correlated"]:
            return "medium"
        else:
            return "low"
    
    async def _analyze_response_effectiveness(self, incident_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze effectiveness of incident response"""
        
        try:
            incident_details = incident_data.get("incident_details", {})
            
            # Calculate response times
            time_to_acknowledge = incident_details.get("time_to_acknowledge", 0)
            time_to_resolve = incident_details.get("time_to_resolve", 0)
            
            # Assess response quality
            response_actions = incident_details.get("response_actions_taken", [])
            successful_actions = len([a for a in response_actions if a.get("success", False)])
            
            effectiveness = {
                "overall_score": 0,
                "response_time_score": 0,
                "action_effectiveness_score": 0,
                "communication_score": 0,
                "areas_for_improvement": []
            }
            
            # Response time scoring
            if time_to_acknowledge <= 5:
                effectiveness["response_time_score"] = 100
            elif time_to_acknowledge <= 15:
                effectiveness["response_time_score"] = 80
            elif time_to_acknowledge <= 30:
                effectiveness["response_time_score"] = 60
            else:
                effectiveness["response_time_score"] = 40
                effectiveness["areas_for_improvement"].append("Improve initial response time")
            
            # Action effectiveness scoring
            if response_actions:
                action_success_rate = successful_actions / len(response_actions)
                effectiveness["action_effectiveness_score"] = int(action_success_rate * 100)
                
                if action_success_rate < 0.8:
                    effectiveness["areas_for_improvement"].append("Improve response action success rate")
            else:
                effectiveness["action_effectiveness_score"] = 50
                effectiveness["areas_for_improvement"].append("Implement automated response actions")
            
            # Communication scoring (simplified)
            escalation_count = incident_details.get("escalation_count", 0)
            if escalation_count <= 1:
                effectiveness["communication_score"] = 100
            elif escalation_count <= 3:
                effectiveness["communication_score"] = 80
            else:
                effectiveness["communication_score"] = 60
                effectiveness["areas_for_improvement"].append("Improve communication and escalation")
            
            # Overall score
            effectiveness["overall_score"] = int(statistics.mean([
                effectiveness["response_time_score"],
                effectiveness["action_effectiveness_score"],
                effectiveness["communication_score"]
            ]))
            
            return effectiveness
            
        except Exception as e:
            self.logger.error(f"Failed to analyze response effectiveness: {str(e)}")
            return {"overall_score": 0, "error": str(e)}
    
    async def _analyze_communication_effectiveness(self, incident_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze communication effectiveness during incident"""
        
        try:
            incident_details = incident_data.get("incident_details", {})
            timeline = incident_details.get("timeline", [])
            
            # Count communication events
            communication_events = [
                event for event in timeline
                if "notification" in event.get("event_type", "") or
                   "escalation" in event.get("event_type", "")
            ]
            
            notification_count = incident_details.get("notification_count", 0)
            escalation_count = incident_details.get("escalation_count", 0)
            
            return {
                "communication_events": len(communication_events),
                "total_notifications": notification_count,
                "escalation_count": escalation_count,
                "communication_timeline": [
                    {
                        "timestamp": event.get("timestamp"),
                        "type": event.get("event_type"),
                        "description": event.get("description")
                    }
                    for event in communication_events
                ],
                "effectiveness_score": max(0, 100 - (escalation_count * 10)),  # Deduct 10 points per escalation
                "recommendations": [
                    "Establish clearer communication protocols",
                    "Implement automated status updates",
                    "Define escalation criteria more clearly"
                ] if escalation_count > 2 else []
            }
            
        except Exception as e:
            self.logger.error(f"Failed to analyze communication effectiveness: {str(e)}")
            return {"error": str(e)}
    
    async def _calculate_response_metrics(self, incident_data: Dict[str, Any]) -> Dict[str, float]:
        """Calculate key response metrics"""
        
        try:
            incident_details = incident_data.get("incident_details", {})
            
            return {
                "time_to_detect_minutes": 0.0,  # Would calculate from actual detection time
                "time_to_acknowledge_minutes": incident_details.get("time_to_acknowledge", 0),
                "time_to_resolve_minutes": incident_details.get("time_to_resolve", 0),
                "escalation_count": incident_details.get("escalation_count", 0),
                "notification_count": incident_details.get("notification_count", 0),
                "response_actions_count": len(incident_details.get("response_actions_taken", [])),
                "response_success_rate": self._calculate_response_success_rate(incident_details),
                "customer_communication_delay_minutes": 0.0  # Would calculate from communication timeline
            }
            
        except Exception as e:
            self.logger.error(f"Failed to calculate response metrics: {str(e)}")
            return {}
    
    def _calculate_response_success_rate(self, incident_details: Dict[str, Any]) -> float:
        """Calculate response action success rate"""
        
        try:
            response_actions = incident_details.get("response_actions_taken", [])
            if not response_actions:
                return 0.0
            
            successful_actions = len([a for a in response_actions if a.get("success", False)])
            return successful_actions / len(response_actions)
            
        except Exception as e:
            self.logger.error(f"Failed to calculate response success rate: {str(e)}")
            return 0.0
    
    async def _compare_to_similar_incidents(self, incident_id: str, rca: RootCauseAnalysis) -> Dict[str, Any]:
        """Compare incident to similar historical incidents"""
        
        try:
            # Find similar incidents by root cause category
            similar_incidents = []
            
            for existing_rca in self.incident_analyses.values():
                if (existing_rca.incident_id != incident_id and
                    existing_rca.root_cause_category == rca.root_cause_category):
                    similar_incidents.append(existing_rca.incident_id)
            
            if not similar_incidents:
                return {
                    "similar_incidents_found": 0,
                    "comparison_data": {},
                    "patterns": [],
                    "recommendations": ["First incident of this type - establish monitoring for patterns"]
                }
            
            # Calculate comparison metrics
            comparison_data = {
                "similar_incidents_count": len(similar_incidents),
                "recurrence_rate": len(similar_incidents) / max(1, len(self.incident_analyses)),
                "pattern_detected": len(similar_incidents) > 2,
                "recommendations": []
            }
            
            if comparison_data["pattern_detected"]:
                comparison_data["recommendations"].extend([
                    "Pattern of similar incidents detected - implement systematic prevention",
                    "Review and enhance monitoring for this incident type",
                    "Consider automation to prevent recurrence"
                ])
            
            return comparison_data
            
        except Exception as e:
            self.logger.error(f"Failed to compare to similar incidents: {str(e)}")
            return {"error": str(e)}
    
    async def _perform_trend_analysis(self, incident_id: str) -> Dict[str, Any]:
        """Perform trend analysis for incident patterns"""
        
        try:
            # Analyze incident trends over time
            recent_incidents = []
            cutoff_date = datetime.utcnow() - timedelta(days=30)
            
            for analysis in self.incident_analyses.values():
                if analysis.analysis_timestamp >= cutoff_date:
                    recent_incidents.append(analysis)
            
            if not recent_incidents:
                return {"trend_data": {}, "recommendations": []}
            
            # Group by root cause category
            category_trends = defaultdict(list)
            for analysis in recent_incidents:
                category_trends[analysis.root_cause_category.value].append(analysis)
            
            # Calculate trends
            trend_data = {}
            for category, incidents in category_trends.items():
                trend_data[category] = {
                    "count": len(incidents),
                    "trend": "increasing" if len(incidents) > 2 else "stable",
                    "severity": "high" if len(incidents) > 5 else "medium" if len(incidents) > 2 else "low"
                }
            
            recommendations = []
            for category, data in trend_data.items():
                if data["count"] > 3:
                    recommendations.append(f"Focus improvement efforts on {category} incidents")
            
            return {
                "trend_data": trend_data,
                "total_recent_incidents": len(recent_incidents),
                "recommendations": recommendations
            }
            
        except Exception as e:
            self.logger.error(f"Failed to perform trend analysis: {str(e)}")
            return {"error": str(e)}
    
    async def _generate_executive_summary(self, incident_details: Dict[str, Any], 
                                        impact: IncidentImpact, 
                                        rca: RootCauseAnalysis) -> str:
        """Generate executive summary for the incident"""
        
        try:
            summary_parts = []
            
            # Incident overview
            severity = incident_details.get("severity", "unknown")
            downtime = impact.downtime_minutes
            
            summary_parts.append(f"A {severity} incident occurred resulting in {downtime:.0f} minutes of system disruption.")
            
            # Business impact
            if impact.total_financial_impact > 0:
                summary_parts.append(f"The incident resulted in an estimated ${impact.total_financial_impact:,.2f} in total business impact, affecting {impact.affected_customers:,} customers.")
            
            # Root cause
            summary_parts.append(f"Root cause analysis identified: {rca.primary_root_cause}")
            
            # Response effectiveness
            response_time = incident_details.get("time_to_acknowledge", 0)
            resolution_time = incident_details.get("time_to_resolve", 0)
            
            summary_parts.append(f"The incident was acknowledged within {response_time:.0f} minutes and resolved in {resolution_time:.0f} minutes.")
            
            # Key actions
            summary_parts.append("Key improvement actions have been identified and assigned to prevent recurrence and enhance system resilience.")
            
            return " ".join(summary_parts)
            
        except Exception as e:
            self.logger.error(f"Failed to generate executive summary: {str(e)}")
            return "Executive summary could not be generated due to analysis error."
    
    async def _extract_key_findings(self, rca: RootCauseAnalysis, impact: IncidentImpact) -> List[str]:
        """Extract key findings from analysis"""
        
        try:
            findings = []
            
            # Root cause finding
            findings.append(f"Primary root cause: {rca.primary_root_cause}")
            
            # Impact findings
            if impact.total_financial_impact > 10000:
                findings.append(f"Significant financial impact of ${impact.total_financial_impact:,.2f}")
            
            if impact.affected_customers > 1000:
                findings.append(f"Large customer impact with {impact.affected_customers:,} customers affected")
            
            if impact.downtime_minutes > 60:
                findings.append(f"Extended downtime of {impact.downtime_minutes:.0f} minutes")
            
            # Contributing factor findings
            if rca.contributing_factors:
                findings.append(f"Multiple contributing factors identified: {', '.join(rca.contributing_factors[:3])}")
            
            # System findings
            if len(impact.systems_affected) > 3:
                findings.append(f"Multiple systems affected: {len(impact.systems_affected)} systems impacted")
            
            return findings
            
        except Exception as e:
            self.logger.error(f"Failed to extract key findings: {str(e)}")
            return ["Key findings could not be extracted due to analysis error"]
    
    async def _assess_compliance_impact(self, incident_details: Dict[str, Any], impact: IncidentImpact) -> List[str]:
        """Assess compliance implications of the incident"""
        
        try:
            compliance_impacts = []
            
            # Check for data loss
            if impact.data_loss_gb > 0:
                compliance_impacts.extend(["GDPR - Data loss incident", "SOC2 - Data integrity violation"])
            
            # Check for extended downtime
            if impact.downtime_minutes > 240:  # 4 hours
                compliance_impacts.append("SLA breach - Extended service unavailability")
            
            # Check for customer data exposure (simplified check)
            if impact.affected_customers > 5000:
                compliance_impacts.append("Potential GDPR notification requirement - Large customer impact")
            
            # Check for financial systems impact
            if "payment" in str(impact.systems_affected).lower():
                compliance_impacts.append("PCI DSS - Payment system impact")
            
            # Check severity for SOX implications
            if incident_details.get("severity") in ["sev1", "critical"]:
                compliance_impacts.append("SOX - Critical system failure affecting financial reporting")
            
            return compliance_impacts
            
        except Exception as e:
            self.logger.error(f"Failed to assess compliance impact: {str(e)}")
            return []
    
    # Continue with remaining service methods...
    
    async def _improvement_tracking_service(self):
        """Background service for tracking improvement action progress"""
        
        while True:
            try:
                await asyncio.sleep(3600)  # Check hourly
                await self._update_improvement_progress()
            except Exception as e:
                self.logger.error(f"Error in improvement tracking service: {str(e)}")
                await asyncio.sleep(3600)
    
    async def _pattern_recognition_service(self):
        """Background service for pattern recognition and learning"""
        
        while True:
            try:
                await asyncio.sleep(7200)  # Run every 2 hours
                await self._analyze_incident_patterns()
            except Exception as e:
                self.logger.error(f"Error in pattern recognition service: {str(e)}")
                await asyncio.sleep(7200)
    
    async def _automated_reporting_service(self):
        """Background service for automated report generation"""
        
        while True:
            try:
                await asyncio.sleep(86400)  # Daily
                await self._generate_automated_reports()
            except Exception as e:
                self.logger.error(f"Error in automated reporting service: {str(e)}")
                await asyncio.sleep(86400)
    
    async def _analytics_service(self):
        """Background service for analytics and metrics calculation"""
        
        while True:
            try:
                await asyncio.sleep(1800)  # Every 30 minutes
                await self._calculate_analytics_metrics()
            except Exception as e:
                self.logger.error(f"Error in analytics service: {str(e)}")
                await asyncio.sleep(1800)
    
    # Placeholder implementations for remaining service methods
    async def _update_improvement_progress(self): pass
    async def _analyze_incident_patterns(self): pass
    async def _generate_automated_reports(self): pass
    async def _calculate_analytics_metrics(self): pass
    
    # Public API methods
    
    def get_post_incident_report(self, incident_id: str, format_type: ReportFormat = ReportFormat.JSON) -> Optional[str]:
        """Get post-incident report in specified format"""
        
        try:
            report = self.post_incident_reports.get(incident_id)
            if not report:
                return None
            
            if format_type == ReportFormat.JSON:
                return json.dumps(asdict(report), indent=2, default=str)
            elif format_type == ReportFormat.EXECUTIVE_SUMMARY:
                return self._generate_executive_report_format(report)
            else:
                return json.dumps(asdict(report), default=str)
                
        except Exception as e:
            self.logger.error(f"Failed to get post-incident report: {str(e)}")
            return None
    
    def _generate_executive_report_format(self, report: PostIncidentReport) -> str:
        """Generate executive-friendly report format"""
        
        try:
            return f"""
EXECUTIVE POST-INCIDENT REPORT
==============================

Incident ID: {report.incident_id}
Date: {report.report_timestamp.strftime('%Y-%m-%d %H:%M:%S')}
Author: {report.report_author}

EXECUTIVE SUMMARY
{report.executive_summary}

KEY FINDINGS
{chr(10).join(f"• {finding}" for finding in report.key_findings)}

BUSINESS IMPACT
• Total Financial Impact: ${report.incident_impact.total_financial_impact:,.2f}
• Customers Affected: {report.incident_impact.affected_customers:,}
• Downtime: {report.incident_impact.downtime_minutes:.0f} minutes
• Revenue Loss: ${report.incident_impact.revenue_loss:,.2f}

ROOT CAUSE
{report.root_cause_analysis.primary_root_cause}
Confidence Level: {report.root_cause_analysis.confidence_level:.0%}

CRITICAL ACTIONS
{chr(10).join(f"• {action}" for action in report.critical_actions)}

IMPROVEMENT ACTIONS: {len(report.improvement_actions)} total
• Critical Priority: {len([a for a in report.improvement_actions if a.priority == ImprovementPriority.CRITICAL])}
• High Priority: {len([a for a in report.improvement_actions if a.priority == ImprovementPriority.HIGH])}

COMPLIANCE IMPACT
{chr(10).join(f"• {impact}" for impact in report.compliance_impact)}

Report Status: {report.report_status.upper()}
"""
            
        except Exception as e:
            self.logger.error(f"Failed to generate executive report format: {str(e)}")
            return "Error generating executive report format"
    
    def get_analysis_summary(self) -> Dict[str, Any]:
        """Get summary of post-incident analysis system"""
        
        try:
            current_time = datetime.utcnow()
            
            return {
                "timestamp": current_time.isoformat(),
                "service_name": self.service_name,
                "system_status": "operational",
                "analysis_statistics": {
                    "total_analyses_completed": len(self.incident_analyses),
                    "total_reports_generated": len(self.post_incident_reports),
                    "total_improvement_actions": len(self.improvement_actions),
                    "pending_actions": len([a for a in self.improvement_actions.values() if a.status == "open"]),
                    "completed_actions": len([a for a in self.improvement_actions.values() if a.status == "completed"])
                },
                "root_cause_distribution": {
                    category.value: len([rca for rca in self.incident_analyses.values() if rca.root_cause_category == category])
                    for category in RootCauseCategory
                },
                "improvement_priorities": {
                    priority.value: len([a for a in self.improvement_actions.values() if a.priority == priority])
                    for priority in ImprovementPriority
                },
                "performance_metrics": self.analysis_metrics,
                "recent_activity": {
                    "analyses_last_7_days": len([
                        rca for rca in self.incident_analyses.values()
                        if rca.analysis_timestamp >= current_time - timedelta(days=7)
                    ]),
                    "reports_last_7_days": len([
                        report for report in self.post_incident_reports.values()
                        if report.report_timestamp >= current_time - timedelta(days=7)
                    ])
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get analysis summary: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}


# Global instance
_post_incident_analysis: Optional[DataDogPostIncidentAnalysis] = None


def get_post_incident_analysis(
    service_name: str = "post-incident-analysis",
    datadog_monitoring: Optional[DatadogMonitoring] = None,
    incident_management: Optional[DataDogIncidentManagement] = None
) -> DataDogPostIncidentAnalysis:
    """Get or create post-incident analysis instance"""
    global _post_incident_analysis
    
    if _post_incident_analysis is None:
        _post_incident_analysis = DataDogPostIncidentAnalysis(
            service_name, datadog_monitoring, incident_management
        )
    
    return _post_incident_analysis


def get_post_incident_summary() -> Dict[str, Any]:
    """Get post-incident analysis summary"""
    analysis = get_post_incident_analysis()
    return analysis.get_analysis_summary()
"""
Data Quality Message Handlers

This module provides specialized message handlers for data quality operations including:
- Data validation and profiling messages
- Data quality monitoring alerts
- Data lineage tracking events
- Data governance notifications
- Compliance and audit messages
"""

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Set
import threading

from .enterprise_rabbitmq_manager import (
    EnterpriseRabbitMQManager, EnterpriseMessage, MessageMetadata,
    QueueType, MessagePriority, get_rabbitmq_manager
)
from .message_patterns import WorkQueue, PublisherSubscriber, TopicRouter
from core.logging import get_logger


class DataQualityStatus(Enum):
    """Data quality status levels"""
    EXCELLENT = "excellent"
    GOOD = "good"
    WARNING = "warning"
    CRITICAL = "critical"
    FAILED = "failed"


class ValidationSeverity(Enum):
    """Validation issue severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class DataQualityMetricType(Enum):
    """Types of data quality metrics"""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"
    RELEVANCE = "relevance"


@dataclass
class DataQualityRule:
    """Data quality rule definition"""
    rule_id: str
    rule_name: str
    rule_type: str
    description: str
    severity: ValidationSeverity
    expression: str  # SQL or Python expression
    threshold_value: Optional[float] = None
    threshold_operator: Optional[str] = None  # >, <, >=, <=, ==, !=
    enabled: bool = True
    tags: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "rule_type": self.rule_type,
            "description": self.description,
            "severity": self.severity.value,
            "expression": self.expression,
            "threshold_value": self.threshold_value,
            "threshold_operator": self.threshold_operator,
            "enabled": self.enabled,
            "tags": self.tags
        }


@dataclass
class DataQualityValidationRequest:
    """Data quality validation request"""
    request_id: str = field(default_factory=lambda: f"dq_val_{uuid.uuid4().hex[:8]}")
    dataset_name: str = ""
    dataset_path: str = ""
    table_name: Optional[str] = None
    validation_rules: List[DataQualityRule] = field(default_factory=list)
    validation_scope: str = "full"  # full, sample, incremental
    sample_size: Optional[int] = None
    
    # Execution options
    async_execution: bool = False
    timeout_minutes: int = 30
    retry_on_failure: bool = True
    max_retries: int = 3
    
    # Notification options
    notify_on_completion: bool = True
    notify_on_failure: bool = True
    notification_channels: List[str] = field(default_factory=list)
    
    # Metadata
    submitted_by: Optional[str] = None
    business_context: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "dataset_name": self.dataset_name,
            "dataset_path": self.dataset_path,
            "table_name": self.table_name,
            "validation_rules": [rule.to_dict() for rule in self.validation_rules],
            "validation_scope": self.validation_scope,
            "sample_size": self.sample_size,
            "async_execution": self.async_execution,
            "timeout_minutes": self.timeout_minutes,
            "retry_on_failure": self.retry_on_failure,
            "max_retries": self.max_retries,
            "notify_on_completion": self.notify_on_completion,
            "notify_on_failure": self.notify_on_failure,
            "notification_channels": self.notification_channels,
            "submitted_by": self.submitted_by,
            "business_context": self.business_context,
            "tags": self.tags
        }


@dataclass
class DataQualityValidationResult:
    """Data quality validation result"""
    request_id: str
    dataset_name: str
    overall_status: DataQualityStatus
    overall_score: float  # 0.0 to 1.0
    
    # Rule results
    rules_executed: int = 0
    rules_passed: int = 0
    rules_failed: int = 0
    rules_warnings: int = 0
    
    # Detailed results
    rule_results: List[Dict[str, Any]] = field(default_factory=list)
    metric_results: Dict[str, Any] = field(default_factory=dict)
    
    # Execution metadata
    execution_time_seconds: Optional[float] = None
    records_validated: Optional[int] = None
    validation_timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Issues and recommendations
    critical_issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "request_id": self.request_id,
            "dataset_name": self.dataset_name,
            "overall_status": self.overall_status.value,
            "overall_score": self.overall_score,
            "rules_executed": self.rules_executed,
            "rules_passed": self.rules_passed,
            "rules_failed": self.rules_failed,
            "rules_warnings": self.rules_warnings,
            "rule_results": self.rule_results,
            "metric_results": self.metric_results,
            "execution_time_seconds": self.execution_time_seconds,
            "records_validated": self.records_validated,
            "validation_timestamp": self.validation_timestamp.isoformat(),
            "critical_issues": self.critical_issues,
            "warnings": self.warnings,
            "recommendations": self.recommendations
        }


@dataclass
class DataLineageEvent:
    """Data lineage tracking event"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""  # read, write, transform, delete
    source_dataset: Optional[str] = None
    target_dataset: Optional[str] = None
    transformation_name: Optional[str] = None
    transformation_type: Optional[str] = None  # etl, ml_training, aggregation, etc.
    
    # Data details
    columns_read: List[str] = field(default_factory=list)
    columns_written: List[str] = field(default_factory=list)
    records_processed: Optional[int] = None
    
    # Execution context
    pipeline_name: Optional[str] = None
    pipeline_run_id: Optional[str] = None
    user_id: Optional[str] = None
    application_name: Optional[str] = None
    
    # Metadata
    timestamp: datetime = field(default_factory=datetime.utcnow)
    execution_time_ms: Optional[float] = None
    success: bool = True
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source_dataset": self.source_dataset,
            "target_dataset": self.target_dataset,
            "transformation_name": self.transformation_name,
            "transformation_type": self.transformation_type,
            "columns_read": self.columns_read,
            "columns_written": self.columns_written,
            "records_processed": self.records_processed,
            "pipeline_name": self.pipeline_name,
            "pipeline_run_id": self.pipeline_run_id,
            "user_id": self.user_id,
            "application_name": self.application_name,
            "timestamp": self.timestamp.isoformat(),
            "execution_time_ms": self.execution_time_ms,
            "success": self.success,
            "error_message": self.error_message
        }


@dataclass
class DataGovernanceEvent:
    """Data governance and compliance event"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""  # access_granted, access_denied, policy_violation, etc.
    
    # Subject and object
    user_id: Optional[str] = None
    user_role: Optional[str] = None
    dataset_name: Optional[str] = None
    table_name: Optional[str] = None
    column_names: List[str] = field(default_factory=list)
    
    # Policy and compliance
    policy_name: Optional[str] = None
    policy_version: Optional[str] = None
    compliance_framework: Optional[str] = None  # GDPR, CCPA, HIPAA, etc.
    
    # Event details
    action_attempted: Optional[str] = None
    access_granted: bool = True
    reason: Optional[str] = None
    risk_level: str = "low"  # low, medium, high, critical
    
    # Context
    application_name: Optional[str] = None
    ip_address: Optional[str] = None
    session_id: Optional[str] = None
    
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "user_id": self.user_id,
            "user_role": self.user_role,
            "dataset_name": self.dataset_name,
            "table_name": self.table_name,
            "column_names": self.column_names,
            "policy_name": self.policy_name,
            "policy_version": self.policy_version,
            "compliance_framework": self.compliance_framework,
            "action_attempted": self.action_attempted,
            "access_granted": self.access_granted,
            "reason": self.reason,
            "risk_level": self.risk_level,
            "application_name": self.application_name,
            "ip_address": self.ip_address,
            "session_id": self.session_id,
            "timestamp": self.timestamp.isoformat()
        }


class DataQualityMessageHandler:
    """
    Comprehensive data quality message handler for validation, monitoring,
    lineage tracking, and governance events
    """
    
    def __init__(self, rabbitmq_manager: Optional[EnterpriseRabbitMQManager] = None):
        self.rabbitmq = rabbitmq_manager or get_rabbitmq_manager()
        self.logger = get_logger(__name__)
        
        # Message patterns
        self.work_queue = WorkQueue(self.rabbitmq)
        self.publisher = PublisherSubscriber(self.rabbitmq)
        self.topic_router = TopicRouter(self.rabbitmq)
        
        # Tracking and storage
        self.validation_jobs: Dict[str, Dict[str, Any]] = {}
        self.validation_history: Dict[str, Dict[str, Any]] = {}
        self.lineage_graph: Dict[str, Set[str]] = {}  # dataset -> dependencies
        self.governance_policies: Dict[str, Dict[str, Any]] = {}
        
        # Setup handlers
        self._setup_handlers()
        
    def _setup_handlers(self):
        """Setup message handlers for data quality operations"""
        
        # Data validation handlers
        self.work_queue.register_handler("dq_validation_request", self.handle_validation_request)
        self.work_queue.register_handler("dq_profiling_request", self.handle_profiling_request)
        
        # Data monitoring handlers
        self.work_queue.register_handler("dq_monitoring_alert", self.handle_monitoring_alert)
        self.work_queue.register_handler("dq_threshold_check", self.handle_threshold_check)
        
        # Lineage tracking handlers
        self.work_queue.register_handler("data_lineage_event", self.handle_lineage_event)
        
        # Governance handlers
        self.work_queue.register_handler("governance_event", self.handle_governance_event)
        self.work_queue.register_handler("compliance_audit", self.handle_compliance_audit)
        
        self.logger.info("Data quality message handlers setup completed")
    
    def start_dq_workers(self, num_workers: int = 3):
        """Start data quality worker processes"""
        self.work_queue.start_workers(num_workers, QueueType.DATA_QUALITY_VALIDATION.value)
        self.work_queue.start_workers(num_workers, QueueType.DATA_QUALITY_MONITORING.value)
        self.work_queue.start_workers(num_workers, QueueType.DATA_LINEAGE.value)
        
        self.logger.info(f"Started {num_workers} data quality workers for each queue type")
    
    # Data Validation Operations
    
    def submit_validation_request(
        self,
        validation_request: DataQualityValidationRequest,
        priority: MessagePriority = MessagePriority.NORMAL
    ) -> str:
        """Submit data quality validation request"""
        try:
            job_id = self.work_queue.add_task(
                task_type="dq_validation_request",
                task_data=validation_request.to_dict(),
                priority=priority
            )
            
            # Track validation job
            self.validation_jobs[job_id] = {
                "type": "validation",
                "status": "pending",
                "submitted_at": datetime.utcnow(),
                "request": validation_request.to_dict()
            }
            
            self.logger.info(f"Submitted data validation job {job_id}")
            return job_id
            
        except Exception as e:
            self.logger.error(f"Failed to submit validation request: {e}")
            raise
    
    async def submit_validation_request_async(
        self,
        validation_request: DataQualityValidationRequest,
        priority: MessagePriority = MessagePriority.NORMAL
    ) -> str:
        """Submit data quality validation request asynchronously"""
        try:
            job_id = await self.work_queue.add_task_async(
                task_type="dq_validation_request",
                task_data=validation_request.to_dict(),
                priority=priority
            )
            
            # Track validation job
            self.validation_jobs[job_id] = {
                "type": "validation",
                "status": "pending",
                "submitted_at": datetime.utcnow(),
                "request": validation_request.to_dict()
            }
            
            self.logger.info(f"Submitted async data validation job {job_id}")
            return job_id
            
        except Exception as e:
            self.logger.error(f"Failed to submit async validation request: {e}")
            raise
    
    def handle_validation_request(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle data quality validation request"""
        try:
            validation_data = task_data.get("data", {})
            request_id = validation_data.get("request_id")
            dataset_name = validation_data.get("dataset_name")
            
            self.logger.info(f"Processing data validation request {request_id} for dataset {dataset_name}")
            
            # Update job status
            if request_id in self.validation_jobs:
                self.validation_jobs[request_id]["status"] = "running"
                self.validation_jobs[request_id]["started_at"] = datetime.utcnow()
            
            start_time = time.time()
            
            # Execute validation
            validation_result = self._execute_validation(validation_data)
            
            execution_time = time.time() - start_time
            validation_result.execution_time_seconds = execution_time
            
            # Update job tracking
            if request_id in self.validation_jobs:
                self.validation_jobs[request_id]["status"] = "completed"
                self.validation_jobs[request_id]["completed_at"] = datetime.utcnow()
                self.validation_jobs[request_id]["result"] = validation_result.to_dict()
                
                # Move to history
                self.validation_history[request_id] = self.validation_jobs.pop(request_id)
            
            # Publish validation completion event
            self.publisher.publish(
                event_type="dq_validation_completed",
                data=validation_result.to_dict(),
                priority=MessagePriority.HIGH if validation_result.overall_status in [
                    DataQualityStatus.CRITICAL, DataQualityStatus.FAILED
                ] else MessagePriority.NORMAL
            )
            
            # Send alerts if critical issues found
            if validation_result.critical_issues:
                self._send_data_quality_alert(validation_result)
            
            # Send notifications if requested
            if validation_data.get("notify_on_completion", True):
                self._send_validation_notification(validation_data, validation_result)
            
            return validation_result.to_dict()
            
        except Exception as e:
            self.logger.error(f"Error handling validation request: {e}")
            
            # Update job status
            request_id = task_data.get("data", {}).get("request_id")
            if request_id and request_id in self.validation_jobs:
                self.validation_jobs[request_id]["status"] = "failed"
                self.validation_jobs[request_id]["error"] = str(e)
            
            raise
    
    def _execute_validation(self, validation_data: Dict[str, Any]) -> DataQualityValidationResult:
        """Execute data quality validation"""
        try:
            request_id = validation_data.get("request_id")
            dataset_name = validation_data.get("dataset_name")
            validation_rules = validation_data.get("validation_rules", [])
            
            # Simulate validation execution
            rule_results = []
            rules_passed = 0
            rules_failed = 0
            rules_warnings = 0
            critical_issues = []
            warnings = []
            recommendations = []
            
            for rule_data in validation_rules:
                rule = DataQualityRule(**rule_data)
                
                # Simulate rule execution
                result = self._execute_validation_rule(rule, validation_data)
                rule_results.append(result)
                
                if result["status"] == "passed":
                    rules_passed += 1
                elif result["status"] == "failed":
                    rules_failed += 1
                    if rule.severity == ValidationSeverity.CRITICAL:
                        critical_issues.append(result["message"])
                elif result["status"] == "warning":
                    rules_warnings += 1
                    warnings.append(result["message"])
            
            # Calculate overall metrics
            total_rules = len(validation_rules)
            overall_score = rules_passed / total_rules if total_rules > 0 else 1.0
            
            # Determine overall status
            if rules_failed > 0 and any(rule["severity"] == "critical" for rule in validation_rules if not rule.get("passed", True)):
                overall_status = DataQualityStatus.CRITICAL
            elif rules_failed > 0:
                overall_status = DataQualityStatus.FAILED
            elif rules_warnings > 0:
                overall_status = DataQualityStatus.WARNING
            elif overall_score >= 0.95:
                overall_status = DataQualityStatus.EXCELLENT
            else:
                overall_status = DataQualityStatus.GOOD
            
            # Generate recommendations
            if overall_score < 0.8:
                recommendations.append("Consider reviewing data ingestion processes")
            if rules_warnings > 0:
                recommendations.append("Monitor warned rules for potential future issues")
            if rules_failed > 0:
                recommendations.append("Address failed validation rules immediately")
            
            return DataQualityValidationResult(
                request_id=request_id,
                dataset_name=dataset_name,
                overall_status=overall_status,
                overall_score=overall_score,
                rules_executed=total_rules,
                rules_passed=rules_passed,
                rules_failed=rules_failed,
                rules_warnings=rules_warnings,
                rule_results=rule_results,
                records_validated=10000,  # Simulated
                critical_issues=critical_issues,
                warnings=warnings,
                recommendations=recommendations
            )
            
        except Exception as e:
            self.logger.error(f"Error executing validation: {e}")
            raise
    
    def _execute_validation_rule(self, rule: DataQualityRule, validation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single validation rule"""
        try:
            # Simulate rule execution
            rule_id = rule.rule_id
            
            # Generate deterministic but varied results based on rule_id
            hash_val = hash(rule_id) % 100
            
            if hash_val < 10:  # 10% failure rate
                status = "failed"
                message = f"Rule {rule.rule_name} failed: threshold not met"
                value = 0.3 + (hash_val / 100)
            elif hash_val < 25:  # 15% warning rate
                status = "warning"
                message = f"Rule {rule.rule_name} warning: approaching threshold"
                value = 0.7 + (hash_val / 100)
            else:  # 75% pass rate
                status = "passed"
                message = f"Rule {rule.rule_name} passed successfully"
                value = 0.85 + (hash_val / 1000)
            
            return {
                "rule_id": rule.rule_id,
                "rule_name": rule.rule_name,
                "status": status,
                "message": message,
                "value": value,
                "threshold": rule.threshold_value,
                "severity": rule.severity.value,
                "execution_time_ms": 100 + (hash_val * 2)  # Simulated execution time
            }
            
        except Exception as e:
            return {
                "rule_id": rule.rule_id,
                "rule_name": rule.rule_name,
                "status": "error",
                "message": f"Rule execution error: {str(e)}",
                "value": None,
                "severity": rule.severity.value,
                "execution_time_ms": 0
            }
    
    def _send_data_quality_alert(self, validation_result: DataQualityValidationResult):
        """Send data quality alert for critical issues"""
        try:
            alert_data = {
                "alert_type": "data_quality_critical",
                "dataset_name": validation_result.dataset_name,
                "overall_status": validation_result.overall_status.value,
                "overall_score": validation_result.overall_score,
                "critical_issues": validation_result.critical_issues,
                "validation_timestamp": validation_result.validation_timestamp.isoformat(),
                "alert_timestamp": datetime.utcnow().isoformat()
            }
            
            self.work_queue.add_task(
                task_type="send_alert",
                task_data=alert_data,
                priority=MessagePriority.CRITICAL
            )
            
        except Exception as e:
            self.logger.error(f"Failed to send data quality alert: {e}")
    
    def _send_validation_notification(
        self,
        validation_data: Dict[str, Any],
        validation_result: DataQualityValidationResult
    ):
        """Send validation completion notification"""
        try:
            notification_data = {
                "type": "dq_validation_notification",
                "request_id": validation_result.request_id,
                "dataset_name": validation_result.dataset_name,
                "overall_status": validation_result.overall_status.value,
                "overall_score": validation_result.overall_score,
                "rules_executed": validation_result.rules_executed,
                "rules_failed": validation_result.rules_failed,
                "execution_time_seconds": validation_result.execution_time_seconds,
                "critical_issues_count": len(validation_result.critical_issues)
            }
            
            self.work_queue.add_task(
                task_type="send_notification",
                task_data=notification_data,
                priority=MessagePriority.NORMAL
            )
            
        except Exception as e:
            self.logger.error(f"Failed to send validation notification: {e}")
    
    # Data Profiling Operations
    
    def handle_profiling_request(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle data profiling request"""
        try:
            profiling_data = task_data.get("data", {})
            dataset_name = profiling_data.get("dataset_name")
            
            self.logger.info(f"Processing data profiling request for dataset {dataset_name}")
            
            # Simulate data profiling
            profile_results = {
                "dataset_name": dataset_name,
                "row_count": 50000,
                "column_count": 25,
                "data_size_mb": 125.5,
                "null_percentage": 2.3,
                "duplicate_percentage": 0.8,
                "unique_columns": ["id", "customer_id", "transaction_id"],
                "categorical_columns": ["category", "region", "status"],
                "numerical_columns": ["amount", "quantity", "price"],
                "datetime_columns": ["created_at", "updated_at"],
                "profiling_timestamp": datetime.utcnow().isoformat(),
                "column_profiles": self._generate_column_profiles()
            }
            
            # Publish profiling completion event
            self.publisher.publish(
                event_type="dq_profiling_completed",
                data=profile_results,
                priority=MessagePriority.NORMAL
            )
            
            return profile_results
            
        except Exception as e:
            self.logger.error(f"Error handling profiling request: {e}")
            raise
    
    def _generate_column_profiles(self) -> List[Dict[str, Any]]:
        """Generate mock column profiles"""
        columns = [
            {"name": "customer_id", "type": "integer", "null_count": 0, "unique_count": 45000, "min": 1, "max": 50000},
            {"name": "amount", "type": "float", "null_count": 150, "unique_count": 48500, "min": 0.01, "max": 9999.99, "mean": 245.67},
            {"name": "category", "type": "string", "null_count": 25, "unique_count": 12, "most_frequent": "electronics"},
            {"name": "created_at", "type": "datetime", "null_count": 0, "min": "2023-01-01", "max": "2024-08-27"}
        ]
        return columns
    
    # Data Monitoring Operations
    
    def handle_monitoring_alert(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle data quality monitoring alert"""
        try:
            alert_data = task_data.get("data", {})
            alert_type = alert_data.get("alert_type")
            dataset_name = alert_data.get("dataset_name")
            
            self.logger.warning(f"Processing data quality alert {alert_type} for dataset {dataset_name}")
            
            # Process alert based on type
            if alert_type == "threshold_breach":
                return self._handle_threshold_breach_alert(alert_data)
            elif alert_type == "anomaly_detected":
                return self._handle_anomaly_alert(alert_data)
            elif alert_type == "data_freshness":
                return self._handle_freshness_alert(alert_data)
            else:
                return self._handle_generic_alert(alert_data)
                
        except Exception as e:
            self.logger.error(f"Error handling monitoring alert: {e}")
            raise
    
    def _handle_threshold_breach_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle threshold breach alert"""
        metric_name = alert_data.get("metric_name")
        current_value = alert_data.get("current_value")
        threshold_value = alert_data.get("threshold_value")
        
        # Escalate if critical
        if alert_data.get("severity") == "critical":
            self.work_queue.add_task(
                task_type="escalate_alert",
                task_data=alert_data,
                priority=MessagePriority.CRITICAL
            )
        
        return {
            "alert_processed": True,
            "action": "threshold_breach_handled",
            "escalated": alert_data.get("severity") == "critical"
        }
    
    def _handle_anomaly_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle anomaly detection alert"""
        return {
            "alert_processed": True,
            "action": "anomaly_investigation_triggered"
        }
    
    def _handle_freshness_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle data freshness alert"""
        return {
            "alert_processed": True,
            "action": "freshness_check_initiated"
        }
    
    def _handle_generic_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle generic alert"""
        return {
            "alert_processed": True,
            "action": "generic_alert_logged"
        }
    
    def handle_threshold_check(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle threshold check request"""
        try:
            check_data = task_data.get("data", {})
            metric_name = check_data.get("metric_name")
            current_value = check_data.get("current_value")
            threshold_value = check_data.get("threshold_value")
            operator = check_data.get("operator", ">")
            
            # Perform threshold check
            threshold_breached = False
            
            if operator == ">":
                threshold_breached = current_value > threshold_value
            elif operator == "<":
                threshold_breached = current_value < threshold_value
            elif operator == ">=":
                threshold_breached = current_value >= threshold_value
            elif operator == "<=":
                threshold_breached = current_value <= threshold_value
            elif operator == "==":
                threshold_breached = current_value == threshold_value
            elif operator == "!=":
                threshold_breached = current_value != threshold_value
            
            result = {
                "metric_name": metric_name,
                "current_value": current_value,
                "threshold_value": threshold_value,
                "operator": operator,
                "threshold_breached": threshold_breached,
                "check_timestamp": datetime.utcnow().isoformat()
            }
            
            # Trigger alert if threshold breached
            if threshold_breached:
                alert_data = {
                    **check_data,
                    "alert_type": "threshold_breach",
                    "breach_detected": True
                }
                
                self.work_queue.add_task(
                    task_type="dq_monitoring_alert",
                    task_data=alert_data,
                    priority=MessagePriority.HIGH
                )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error handling threshold check: {e}")
            raise
    
    # Data Lineage Operations
    
    def publish_lineage_event(
        self,
        lineage_event: DataLineageEvent,
        priority: MessagePriority = MessagePriority.NORMAL
    ) -> bool:
        """Publish data lineage event"""
        try:
            return self.topic_router.publish(
                routing_key=f"lineage.{lineage_event.event_type}",
                data=lineage_event.to_dict(),
                priority=priority
            )
            
        except Exception as e:
            self.logger.error(f"Failed to publish lineage event: {e}")
            return False
    
    def handle_lineage_event(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle data lineage event"""
        try:
            lineage_data = task_data.get("data", {})
            event_type = lineage_data.get("event_type")
            source_dataset = lineage_data.get("source_dataset")
            target_dataset = lineage_data.get("target_dataset")
            
            self.logger.info(f"Processing lineage event {event_type}: {source_dataset} -> {target_dataset}")
            
            # Update lineage graph
            if source_dataset and target_dataset:
                if target_dataset not in self.lineage_graph:
                    self.lineage_graph[target_dataset] = set()
                self.lineage_graph[target_dataset].add(source_dataset)
            
            # Process different event types
            if event_type == "transformation":
                return self._handle_transformation_lineage(lineage_data)
            elif event_type == "read":
                return self._handle_read_lineage(lineage_data)
            elif event_type == "write":
                return self._handle_write_lineage(lineage_data)
            else:
                return self._handle_generic_lineage(lineage_data)
                
        except Exception as e:
            self.logger.error(f"Error handling lineage event: {e}")
            raise
    
    def _handle_transformation_lineage(self, lineage_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle transformation lineage event"""
        transformation_name = lineage_data.get("transformation_name")
        
        return {
            "lineage_processed": True,
            "event_type": "transformation",
            "transformation_tracked": transformation_name
        }
    
    def _handle_read_lineage(self, lineage_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle read lineage event"""
        return {
            "lineage_processed": True,
            "event_type": "read",
            "access_tracked": True
        }
    
    def _handle_write_lineage(self, lineage_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle write lineage event"""
        return {
            "lineage_processed": True,
            "event_type": "write",
            "output_tracked": True
        }
    
    def _handle_generic_lineage(self, lineage_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle generic lineage event"""
        return {
            "lineage_processed": True,
            "event_type": "generic",
            "event_logged": True
        }
    
    # Data Governance Operations
    
    def publish_governance_event(
        self,
        governance_event: DataGovernanceEvent,
        priority: MessagePriority = MessagePriority.HIGH
    ) -> bool:
        """Publish data governance event"""
        try:
            return self.publisher.publish(
                event_type="governance_event",
                data=governance_event.to_dict(),
                priority=priority
            )
            
        except Exception as e:
            self.logger.error(f"Failed to publish governance event: {e}")
            return False
    
    def handle_governance_event(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle data governance event"""
        try:
            governance_data = task_data.get("data", {})
            event_type = governance_data.get("event_type")
            user_id = governance_data.get("user_id")
            dataset_name = governance_data.get("dataset_name")
            
            self.logger.info(f"Processing governance event {event_type} for user {user_id}")
            
            # Process different governance events
            if event_type == "access_request":
                return self._handle_access_request(governance_data)
            elif event_type == "policy_violation":
                return self._handle_policy_violation(governance_data)
            elif event_type == "compliance_check":
                return self._handle_compliance_check(governance_data)
            else:
                return self._handle_generic_governance(governance_data)
                
        except Exception as e:
            self.logger.error(f"Error handling governance event: {e}")
            raise
    
    def _handle_access_request(self, governance_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle data access request"""
        user_id = governance_data.get("user_id")
        dataset_name = governance_data.get("dataset_name")
        
        # Simulate access decision
        access_granted = hash(f"{user_id}_{dataset_name}") % 10 != 0  # 90% approval rate
        
        return {
            "governance_processed": True,
            "event_type": "access_request",
            "access_granted": access_granted,
            "decision_reason": "Policy evaluation completed"
        }
    
    def _handle_policy_violation(self, governance_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle policy violation"""
        violation_type = governance_data.get("violation_type", "unknown")
        
        # Escalate high-risk violations
        if governance_data.get("risk_level") in ["high", "critical"]:
            self.work_queue.add_task(
                task_type="escalate_violation",
                task_data=governance_data,
                priority=MessagePriority.CRITICAL
            )
        
        return {
            "governance_processed": True,
            "event_type": "policy_violation",
            "violation_logged": True,
            "escalated": governance_data.get("risk_level") in ["high", "critical"]
        }
    
    def _handle_compliance_check(self, governance_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle compliance check"""
        framework = governance_data.get("compliance_framework")
        
        return {
            "governance_processed": True,
            "event_type": "compliance_check",
            "framework": framework,
            "check_completed": True
        }
    
    def _handle_generic_governance(self, governance_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle generic governance event"""
        return {
            "governance_processed": True,
            "event_type": "generic",
            "event_logged": True
        }
    
    def handle_compliance_audit(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle compliance audit request"""
        try:
            audit_data = task_data.get("data", {})
            audit_type = audit_data.get("audit_type")
            timeframe_days = audit_data.get("timeframe_days", 30)
            
            self.logger.info(f"Processing compliance audit: {audit_type}")
            
            # Simulate audit execution
            audit_results = {
                "audit_type": audit_type,
                "timeframe_days": timeframe_days,
                "events_audited": 15000,
                "violations_found": 23,
                "high_risk_violations": 3,
                "compliance_score": 0.92,
                "audit_timestamp": datetime.utcnow().isoformat(),
                "recommendations": [
                    "Review access policies for high-risk datasets",
                    "Implement additional monitoring for PII access",
                    "Update user training on data governance policies"
                ]
            }
            
            # Publish audit completion
            self.publisher.publish(
                event_type="compliance_audit_completed",
                data=audit_results,
                priority=MessagePriority.HIGH
            )
            
            return audit_results
            
        except Exception as e:
            self.logger.error(f"Error handling compliance audit: {e}")
            raise
    
    # Utility Methods
    
    def get_validation_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of validation job"""
        if job_id in self.validation_jobs:
            return self.validation_jobs[job_id]
        elif job_id in self.validation_history:
            return self.validation_history[job_id]
        else:
            return None
    
    def get_dataset_lineage(self, dataset_name: str) -> Dict[str, Any]:
        """Get lineage information for dataset"""
        dependencies = list(self.lineage_graph.get(dataset_name, set()))
        downstream = [ds for ds, deps in self.lineage_graph.items() if dataset_name in deps]
        
        return {
            "dataset_name": dataset_name,
            "upstream_dependencies": dependencies,
            "downstream_datasets": downstream
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get data quality handler metrics"""
        return {
            "active_validation_jobs": len(self.validation_jobs),
            "completed_validation_jobs": len(self.validation_history),
            "datasets_in_lineage": len(self.lineage_graph),
            "governance_policies": len(self.governance_policies)
        }


# Global data quality message handler instance
_dq_message_handler: Optional[DataQualityMessageHandler] = None


def get_dq_message_handler() -> DataQualityMessageHandler:
    """Get or create global data quality message handler instance"""
    global _dq_message_handler
    if _dq_message_handler is None:
        _dq_message_handler = DataQualityMessageHandler()
    return _dq_message_handler


def set_dq_message_handler(handler: DataQualityMessageHandler):
    """Set global data quality message handler instance"""
    global _dq_message_handler
    _dq_message_handler = handler
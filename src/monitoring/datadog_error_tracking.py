"""
DataDog Error Tracking and Exception Analysis System
Provides comprehensive error monitoring, exception analysis, and automated issue detection
"""

import asyncio
import hashlib
import json
import sys
import time
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum

# DataDog imports
from ddtrace import tracer
from ddtrace.ext import errors

from core.config import get_settings
from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.datadog_apm_middleware import add_custom_tags, add_custom_metric
from monitoring.datadog_distributed_tracing import get_distributed_tracer, TraceContext

logger = get_logger(__name__)
settings = get_settings()


class ErrorSeverity(Enum):
    """Error severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Error categories."""
    APPLICATION = "application"
    SYSTEM = "system"
    NETWORK = "network"
    DATABASE = "database"
    EXTERNAL_API = "external_api"
    VALIDATION = "validation"
    BUSINESS_LOGIC = "business_logic"
    SECURITY = "security"
    PERFORMANCE = "performance"
    INFRASTRUCTURE = "infrastructure"


class ErrorResolution(Enum):
    """Error resolution status."""
    OPEN = "open"
    INVESTIGATING = "investigating"
    RESOLVED = "resolved"
    IGNORED = "ignored"
    ESCALATED = "escalated"


@dataclass
class ErrorContext:
    """Comprehensive error context information."""
    service_name: str
    environment: str
    timestamp: datetime
    trace_context: Optional[TraceContext] = None
    user_context: Optional[Dict[str, Any]] = None
    request_context: Optional[Dict[str, Any]] = None
    system_context: Optional[Dict[str, Any]] = None
    business_context: Optional[Dict[str, Any]] = None
    custom_context: Optional[Dict[str, Any]] = None


@dataclass
class ErrorFingerprint:
    """Unique error fingerprint for deduplication."""
    error_hash: str
    error_type: str
    error_message_pattern: str
    stack_trace_hash: str
    location: str  # file:line or function name


@dataclass
class ErrorOccurrence:
    """Individual error occurrence."""
    occurrence_id: str
    fingerprint: ErrorFingerprint
    error_type: str
    error_message: str
    stack_trace: List[str]
    timestamp: datetime
    context: ErrorContext
    severity: ErrorSeverity = ErrorSeverity.MEDIUM
    category: ErrorCategory = ErrorCategory.APPLICATION
    tags: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ErrorGroup:
    """Grouped errors with similar fingerprints."""
    group_id: str
    fingerprint: ErrorFingerprint
    first_occurrence: datetime
    last_occurrence: datetime
    occurrence_count: int = 0
    occurrences: List[str] = field(default_factory=list)  # occurrence_ids
    severity: ErrorSeverity = ErrorSeverity.MEDIUM
    category: ErrorCategory = ErrorCategory.APPLICATION
    resolution_status: ErrorResolution = ErrorResolution.OPEN
    affected_users: Set[str] = field(default_factory=set)
    affected_services: Set[str] = field(default_factory=set)
    resolution_notes: Optional[str] = None
    trends: Dict[str, Any] = field(default_factory=dict)


class DataDogErrorTracker:
    """
    Comprehensive error tracking and analysis system
    
    Features:
    - Automatic exception capture and analysis
    - Error deduplication and grouping
    - Context-aware error reporting
    - Real-time error alerting
    - Error trend analysis
    - Performance impact assessment
    - Automated error categorization
    - Root cause analysis assistance
    """
    
    def __init__(self, service_name: str = "error-tracking", 
                 datadog_monitoring: Optional[DatadogMonitoring] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Error storage
        self.error_occurrences: Dict[str, ErrorOccurrence] = {}
        self.error_groups: Dict[str, ErrorGroup] = {}
        self.error_fingerprints: Dict[str, str] = {}  # fingerprint_hash -> group_id
        
        # Error statistics
        self.error_stats = {
            "total_errors": 0,
            "unique_errors": 0,
            "resolved_errors": 0,
            "critical_errors": 0,
            "errors_by_category": {},
            "errors_by_service": {},
            "error_rate_per_minute": 0.0
        }
        
        # Configuration
        self.config = {
            "auto_categorize": True,
            "auto_severity_detection": True,
            "enable_stack_trace_analysis": True,
            "enable_context_correlation": True,
            "max_occurrences_per_group": 1000,
            "error_retention_days": 30
        }
        
        # Pattern recognition
        self.error_patterns: Dict[str, Dict[str, Any]] = {}
        self.severity_keywords = {
            ErrorSeverity.CRITICAL: ["fatal", "critical", "outofmemory", "stackoverflow", "deadlock"],
            ErrorSeverity.HIGH: ["error", "exception", "failed", "timeout", "connection"],
            ErrorSeverity.MEDIUM: ["warning", "invalid", "missing", "unexpected"],
            ErrorSeverity.LOW: ["info", "debug", "notice"]
        }
        
        # Start background tasks
        asyncio.create_task(self._cleanup_old_errors())
    
    def create_error_fingerprint(self, error_type: str, error_message: str, 
                                stack_trace: List[str]) -> ErrorFingerprint:
        """Create unique fingerprint for error deduplication."""
        
        # Normalize error message (remove dynamic parts)
        normalized_message = self._normalize_error_message(error_message)
        
        # Create hash from error type and normalized message
        message_hash = hashlib.md5(f"{error_type}:{normalized_message}".encode()).hexdigest()[:8]
        
        # Create stack trace hash (top 3 frames)
        relevant_frames = stack_trace[:3] if stack_trace else []
        stack_content = "\n".join(frame for frame in relevant_frames if "site-packages" not in frame)
        stack_hash = hashlib.md5(stack_content.encode()).hexdigest()[:8]
        
        # Extract location from stack trace
        location = "unknown"
        if stack_trace:
            for frame in stack_trace:
                if "site-packages" not in frame and "File \"" in frame:
                    parts = frame.split("\"")
                    if len(parts) >= 2:
                        location = parts[1].split("/")[-1] if "/" in parts[1] else parts[1]
                        if "line" in frame:
                            line_part = frame.split("line")[1].split(",")[0].strip()
                            location += f":{line_part}"
                        break
        
        error_hash = f"{message_hash}-{stack_hash}"
        
        return ErrorFingerprint(
            error_hash=error_hash,
            error_type=error_type,
            error_message_pattern=normalized_message,
            stack_trace_hash=stack_hash,
            location=location
        )
    
    def _normalize_error_message(self, message: str) -> str:
        """Normalize error message by removing dynamic parts."""
        
        import re
        
        # Remove common dynamic patterns
        patterns = [
            r'\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\b',  # Timestamps
            r'\b\d+\.\d+\.\d+\.\d+\b',  # IP addresses
            r'\b[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\b',  # UUIDs
            r'\b\d+\b(?=\s*(seconds?|minutes?|hours?|days?|bytes?|MB|GB))',  # Numbers with units
            r"'[^']*'",  # Single quoted strings
            r'"[^"]*"',  # Double quoted strings
            r'\bfile\s*:\s*[^\s]+',  # File paths
        ]
        
        normalized = message
        for pattern in patterns:
            normalized = re.sub(pattern, '<PLACEHOLDER>', normalized, flags=re.IGNORECASE)
        
        # Remove extra whitespace
        normalized = ' '.join(normalized.split())
        
        return normalized
    
    def _detect_error_severity(self, error_type: str, error_message: str, 
                              context: ErrorContext) -> ErrorSeverity:
        """Automatically detect error severity."""
        
        if not self.config["auto_severity_detection"]:
            return ErrorSeverity.MEDIUM
        
        error_text = f"{error_type} {error_message}".lower()
        
        # Check severity keywords
        for severity, keywords in self.severity_keywords.items():
            if any(keyword in error_text for keyword in keywords):
                return severity
        
        # Context-based severity detection
        if context.system_context:
            cpu_usage = context.system_context.get("cpu_percent", 0)
            memory_usage = context.system_context.get("memory_percent", 0)
            
            if cpu_usage > 90 or memory_usage > 95:
                return ErrorSeverity.CRITICAL
            elif cpu_usage > 80 or memory_usage > 90:
                return ErrorSeverity.HIGH
        
        # Check for known critical error types
        critical_types = ["OutOfMemoryError", "StackOverflowError", "SystemError", "KeyboardInterrupt"]
        if any(critical_type in error_type for critical_type in critical_types):
            return ErrorSeverity.CRITICAL
        
        return ErrorSeverity.MEDIUM
    
    def _detect_error_category(self, error_type: str, error_message: str, 
                              stack_trace: List[str]) -> ErrorCategory:
        """Automatically detect error category."""
        
        if not self.config["auto_categorize"]:
            return ErrorCategory.APPLICATION
        
        error_text = f"{error_type} {error_message}".lower()
        stack_text = "\n".join(stack_trace).lower()
        
        # Category detection patterns
        category_patterns = {
            ErrorCategory.DATABASE: ["database", "sql", "connection", "psycopg", "sqlalchemy", "cursor"],
            ErrorCategory.NETWORK: ["network", "connection", "timeout", "socket", "http", "requests", "urllib"],
            ErrorCategory.EXTERNAL_API: ["api", "client", "response", "request", "httpx", "aiohttp"],
            ErrorCategory.VALIDATION: ["validation", "invalid", "schema", "pydantic", "marshmallow"],
            ErrorCategory.SECURITY: ["permission", "unauthorized", "forbidden", "authentication", "security"],
            ErrorCategory.PERFORMANCE: ["timeout", "memory", "slow", "performance", "bottleneck"],
            ErrorCategory.SYSTEM: ["system", "os", "file", "disk", "process", "subprocess"],
            ErrorCategory.INFRASTRUCTURE: ["kubernetes", "docker", "container", "deployment", "infrastructure"]
        }
        
        # Check patterns in error text and stack trace
        for category, patterns in category_patterns.items():
            if any(pattern in error_text or pattern in stack_text for pattern in patterns):
                return category
        
        return ErrorCategory.APPLICATION
    
    async def capture_error(self, exception: Exception, 
                          context: Optional[ErrorContext] = None,
                          tags: Optional[Dict[str, Any]] = None,
                          metadata: Optional[Dict[str, Any]] = None) -> str:
        """Capture and process an error occurrence."""
        
        try:
            # Generate occurrence ID
            occurrence_id = str(uuid.uuid4())
            
            # Extract error information
            error_type = type(exception).__name__
            error_message = str(exception)
            stack_trace = traceback.format_exception(type(exception), exception, exception.__traceback__)
            
            # Create or get context
            if not context:
                context = await self._create_default_context()
            
            # Create error fingerprint
            fingerprint = self.create_error_fingerprint(error_type, error_message, stack_trace)
            
            # Auto-detect severity and category
            severity = self._detect_error_severity(error_type, error_message, context)
            category = self._detect_error_category(error_type, error_message, stack_trace)
            
            # Create error occurrence
            error_occurrence = ErrorOccurrence(
                occurrence_id=occurrence_id,
                fingerprint=fingerprint,
                error_type=error_type,
                error_message=error_message,
                stack_trace=stack_trace,
                timestamp=datetime.utcnow(),
                context=context,
                severity=severity,
                category=category,
                tags=tags or {},
                metadata=metadata or {}
            )
            
            # Store occurrence
            self.error_occurrences[occurrence_id] = error_occurrence
            
            # Group error
            group_id = await self._group_error(error_occurrence)
            
            # Update statistics
            self._update_error_statistics(error_occurrence)
            
            # Send to DataDog APM
            await self._send_error_to_datadog_apm(error_occurrence, group_id)
            
            # Send metrics to DataDog
            if self.datadog_monitoring:
                await self._send_error_metrics(error_occurrence, group_id)
            
            # Log error occurrence
            self.logger.error(
                f"Error captured: {error_type} - {error_message[:100]}",
                extra={
                    "occurrence_id": occurrence_id,
                    "group_id": group_id,
                    "fingerprint": fingerprint.error_hash,
                    "severity": severity.value,
                    "category": category.value
                }
            )
            
            return occurrence_id
            
        except Exception as e:
            self.logger.error(f"Failed to capture error: {str(e)}")
            return ""
    
    async def _create_default_context(self) -> ErrorContext:
        """Create default error context from current environment."""
        
        # Get trace context
        distributed_tracer = get_distributed_tracer()
        trace_context = distributed_tracer.get_current_trace_context()
        
        # Get system context
        import psutil
        try:
            process = psutil.Process()
            system_context = {
                "cpu_percent": process.cpu_percent(),
                "memory_percent": process.memory_percent(),
                "memory_mb": process.memory_info().rss / (1024 * 1024),
                "threads": process.num_threads(),
                "pid": process.pid
            }
        except:
            system_context = {}
        
        return ErrorContext(
            service_name=self.service_name,
            environment=getattr(settings, 'ENVIRONMENT', 'development'),
            timestamp=datetime.utcnow(),
            trace_context=trace_context,
            system_context=system_context
        )
    
    async def _group_error(self, error_occurrence: ErrorOccurrence) -> str:
        """Group error occurrence with similar errors."""
        
        fingerprint_hash = error_occurrence.fingerprint.error_hash
        
        # Check if group already exists
        if fingerprint_hash in self.error_fingerprints:
            group_id = self.error_fingerprints[fingerprint_hash]
            group = self.error_groups[group_id]
            
            # Update group
            group.occurrence_count += 1
            group.last_occurrence = error_occurrence.timestamp
            
            # Add occurrence to group (limit size)
            if len(group.occurrences) < self.config["max_occurrences_per_group"]:
                group.occurrences.append(error_occurrence.occurrence_id)
            
            # Update affected entities
            if error_occurrence.context.user_context and error_occurrence.context.user_context.get("user_id"):
                group.affected_users.add(error_occurrence.context.user_context["user_id"])
            group.affected_services.add(error_occurrence.context.service_name)
            
            # Update severity if higher
            if error_occurrence.severity.value > group.severity.value:
                group.severity = error_occurrence.severity
            
        else:
            # Create new group
            group_id = str(uuid.uuid4())
            
            group = ErrorGroup(
                group_id=group_id,
                fingerprint=error_occurrence.fingerprint,
                first_occurrence=error_occurrence.timestamp,
                last_occurrence=error_occurrence.timestamp,
                occurrence_count=1,
                occurrences=[error_occurrence.occurrence_id],
                severity=error_occurrence.severity,
                category=error_occurrence.category
            )
            
            # Add affected entities
            if error_occurrence.context.user_context and error_occurrence.context.user_context.get("user_id"):
                group.affected_users.add(error_occurrence.context.user_context["user_id"])
            group.affected_services.add(error_occurrence.context.service_name)
            
            # Store group
            self.error_groups[group_id] = group
            self.error_fingerprints[fingerprint_hash] = group_id
        
        return group_id
    
    def _update_error_statistics(self, error_occurrence: ErrorOccurrence):
        """Update error statistics."""
        
        self.error_stats["total_errors"] += 1
        
        # Update category stats
        category = error_occurrence.category.value
        if category not in self.error_stats["errors_by_category"]:
            self.error_stats["errors_by_category"][category] = 0
        self.error_stats["errors_by_category"][category] += 1
        
        # Update service stats
        service = error_occurrence.context.service_name
        if service not in self.error_stats["errors_by_service"]:
            self.error_stats["errors_by_service"][service] = 0
        self.error_stats["errors_by_service"][service] += 1
        
        # Update unique errors count
        self.error_stats["unique_errors"] = len(self.error_groups)
        
        # Update critical errors count
        if error_occurrence.severity == ErrorSeverity.CRITICAL:
            self.error_stats["critical_errors"] += 1
    
    async def _send_error_to_datadog_apm(self, error_occurrence: ErrorOccurrence, group_id: str):
        """Send error to DataDog APM."""
        
        try:
            current_span = tracer.current_span()
            if current_span:
                # Create exception for the span
                exception = Exception(error_occurrence.error_message)
                exception.__class__.__name__ = error_occurrence.error_type
                
                current_span.set_error(exception)
                
                # Add error metadata
                current_span.set_tag("error.occurrence_id", error_occurrence.occurrence_id)
                current_span.set_tag("error.group_id", group_id)
                current_span.set_tag("error.fingerprint", error_occurrence.fingerprint.error_hash)
                current_span.set_tag("error.severity", error_occurrence.severity.value)
                current_span.set_tag("error.category", error_occurrence.category.value)
                current_span.set_tag("error.location", error_occurrence.fingerprint.location)
                
                # Add context tags
                if error_occurrence.context.trace_context:
                    current_span.set_tag("error.correlation_id", error_occurrence.context.trace_context.correlation_id)
                    if error_occurrence.context.trace_context.user_id:
                        current_span.set_tag("error.user_id", error_occurrence.context.trace_context.user_id)
                
                # Add custom tags
                for key, value in error_occurrence.tags.items():
                    current_span.set_tag(f"error.custom.{key}", str(value))
            
        except Exception as e:
            self.logger.warning(f"Failed to send error to DataDog APM: {str(e)}")
    
    async def _send_error_metrics(self, error_occurrence: ErrorOccurrence, group_id: str):
        """Send error metrics to DataDog."""
        
        try:
            tags = [
                f"error_type:{error_occurrence.error_type}",
                f"severity:{error_occurrence.severity.value}",
                f"category:{error_occurrence.category.value}",
                f"service:{error_occurrence.context.service_name}",
                f"environment:{error_occurrence.context.environment}"
            ]
            
            # Add context tags
            if error_occurrence.context.trace_context:
                if error_occurrence.context.trace_context.user_id:
                    tags.append(f"user_id:{error_occurrence.context.trace_context.user_id}")
                if error_occurrence.context.trace_context.pipeline_id:
                    tags.append(f"pipeline_id:{error_occurrence.context.trace_context.pipeline_id}")
            
            # Send error occurrence metrics
            self.datadog_monitoring.counter("errors.occurrences.total", tags=tags)
            
            # Send error group metrics
            group = self.error_groups[group_id]
            group_tags = tags + [f"group_id:{group_id}"]
            self.datadog_monitoring.gauge("errors.groups.occurrence_count", group.occurrence_count, tags=group_tags)
            
            # Send severity-specific metrics
            if error_occurrence.severity == ErrorSeverity.CRITICAL:
                self.datadog_monitoring.counter("errors.critical.total", tags=tags)
            
            # Send category-specific metrics
            self.datadog_monitoring.counter(f"errors.category.{error_occurrence.category.value}.total", tags=tags)
            
        except Exception as e:
            self.logger.warning(f"Failed to send error metrics: {str(e)}")
    
    async def _cleanup_old_errors(self):
        """Background task to clean up old error data."""
        
        while True:
            try:
                cutoff_time = datetime.utcnow() - timedelta(days=self.config["error_retention_days"])
                
                # Clean up old occurrences
                old_occurrences = [
                    occ_id for occ_id, occurrence in self.error_occurrences.items()
                    if occurrence.timestamp < cutoff_time
                ]
                
                for occ_id in old_occurrences:
                    del self.error_occurrences[occ_id]
                
                # Clean up empty groups
                empty_groups = []
                for group_id, group in self.error_groups.items():
                    # Remove old occurrences from group
                    group.occurrences = [
                        occ_id for occ_id in group.occurrences
                        if occ_id in self.error_occurrences
                    ]
                    
                    # Mark group for deletion if empty
                    if not group.occurrences:
                        empty_groups.append(group_id)
                
                for group_id in empty_groups:
                    group = self.error_groups[group_id]
                    del self.error_groups[group_id]
                    # Remove from fingerprints mapping
                    if group.fingerprint.error_hash in self.error_fingerprints:
                        del self.error_fingerprints[group.fingerprint.error_hash]
                
                if old_occurrences or empty_groups:
                    self.logger.info(f"Cleaned up {len(old_occurrences)} old occurrences and {len(empty_groups)} empty groups")
                
                # Sleep for 1 hour before next cleanup
                await asyncio.sleep(3600)
                
            except Exception as e:
                self.logger.error(f"Error during cleanup: {str(e)}")
                await asyncio.sleep(3600)
    
    # Error Analysis and Reporting
    
    def analyze_error_trends(self, hours: int = 24) -> Dict[str, Any]:
        """Analyze error trends over time."""
        
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        # Filter recent errors
        recent_occurrences = [
            occ for occ in self.error_occurrences.values()
            if occ.timestamp >= cutoff_time
        ]
        
        if not recent_occurrences:
            return {"analysis": "no_data", "time_window_hours": hours}
        
        # Group by hour
        hourly_counts = {}
        severity_trends = {severity.value: [] for severity in ErrorSeverity}
        category_trends = {category.value: [] for category in ErrorCategory}
        
        for i in range(hours):
            hour_start = cutoff_time + timedelta(hours=i)
            hour_end = hour_start + timedelta(hours=1)
            
            hour_errors = [
                occ for occ in recent_occurrences
                if hour_start <= occ.timestamp < hour_end
            ]
            
            hourly_counts[hour_start.isoformat()] = len(hour_errors)
            
            # Count by severity
            for severity in ErrorSeverity:
                count = sum(1 for occ in hour_errors if occ.severity == severity)
                severity_trends[severity.value].append(count)
            
            # Count by category
            for category in ErrorCategory:
                count = sum(1 for occ in hour_errors if occ.category == category)
                category_trends[category.value].append(count)
        
        # Calculate trends
        total_errors = len(recent_occurrences)
        unique_errors = len(set(occ.fingerprint.error_hash for occ in recent_occurrences))
        
        # Most common errors
        error_counts = {}
        for occ in recent_occurrences:
            hash_key = occ.fingerprint.error_hash
            if hash_key not in error_counts:
                error_counts[hash_key] = {"count": 0, "error_type": occ.error_type, "message": occ.error_message[:100]}
            error_counts[hash_key]["count"] += 1
        
        top_errors = sorted(error_counts.items(), key=lambda x: x[1]["count"], reverse=True)[:10]
        
        return {
            "time_window_hours": hours,
            "total_errors": total_errors,
            "unique_errors": unique_errors,
            "error_rate_per_hour": total_errors / hours,
            "hourly_distribution": hourly_counts,
            "severity_trends": severity_trends,
            "category_trends": category_trends,
            "top_errors": [
                {
                    "fingerprint": hash_key,
                    "count": data["count"],
                    "error_type": data["error_type"],
                    "message": data["message"]
                }
                for hash_key, data in top_errors
            ],
            "analysis_timestamp": datetime.utcnow().isoformat()
        }
    
    def get_error_group_details(self, group_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about an error group."""
        
        if group_id not in self.error_groups:
            return None
        
        group = self.error_groups[group_id]
        
        # Get sample occurrences
        sample_occurrences = []
        for occ_id in group.occurrences[:5]:  # Get first 5 occurrences
            if occ_id in self.error_occurrences:
                occ = self.error_occurrences[occ_id]
                sample_occurrences.append({
                    "occurrence_id": occ_id,
                    "timestamp": occ.timestamp.isoformat(),
                    "context": {
                        "service": occ.context.service_name,
                        "user_id": occ.context.trace_context.user_id if occ.context.trace_context else None,
                        "trace_id": occ.context.trace_context.trace_id if occ.context.trace_context else None
                    }
                })
        
        # Calculate trends
        recent_occurrences = [
            self.error_occurrences[occ_id] for occ_id in group.occurrences
            if occ_id in self.error_occurrences and
            self.error_occurrences[occ_id].timestamp >= datetime.utcnow() - timedelta(hours=24)
        ]
        
        return {
            "group_id": group_id,
            "fingerprint": {
                "error_hash": group.fingerprint.error_hash,
                "error_type": group.fingerprint.error_type,
                "message_pattern": group.fingerprint.error_message_pattern,
                "location": group.fingerprint.location
            },
            "statistics": {
                "total_occurrences": group.occurrence_count,
                "recent_occurrences_24h": len(recent_occurrences),
                "first_occurrence": group.first_occurrence.isoformat(),
                "last_occurrence": group.last_occurrence.isoformat(),
                "affected_users": len(group.affected_users),
                "affected_services": list(group.affected_services)
            },
            "classification": {
                "severity": group.severity.value,
                "category": group.category.value,
                "resolution_status": group.resolution_status.value
            },
            "sample_occurrences": sample_occurrences,
            "resolution_notes": group.resolution_notes
        }
    
    def get_error_dashboard_data(self) -> Dict[str, Any]:
        """Get comprehensive error dashboard data."""
        
        current_time = datetime.utcnow()
        
        # Recent error activity (last 24 hours)
        recent_errors = [
            occ for occ in self.error_occurrences.values()
            if (current_time - occ.timestamp).total_seconds() < 86400
        ]
        
        # Critical errors needing attention
        critical_groups = [
            group for group in self.error_groups.values()
            if group.severity == ErrorSeverity.CRITICAL and 
            group.resolution_status == ErrorResolution.OPEN
        ]
        
        # Top error groups by frequency
        top_groups = sorted(
            self.error_groups.values(),
            key=lambda g: g.occurrence_count,
            reverse=True
        )[:10]
        
        return {
            "timestamp": current_time.isoformat(),
            "service_name": self.service_name,
            "overview": {
                "total_error_occurrences": len(self.error_occurrences),
                "total_error_groups": len(self.error_groups),
                "recent_errors_24h": len(recent_errors),
                "critical_open_groups": len(critical_groups),
                "error_rate_24h": len(recent_errors) / 24,
                **self.error_stats
            },
            "recent_activity": [
                {
                    "occurrence_id": occ.occurrence_id,
                    "timestamp": occ.timestamp.isoformat(),
                    "error_type": occ.error_type,
                    "severity": occ.severity.value,
                    "category": occ.category.value,
                    "service": occ.context.service_name
                }
                for occ in sorted(recent_errors, key=lambda o: o.timestamp, reverse=True)[:20]
            ],
            "critical_groups": [
                {
                    "group_id": group.group_id,
                    "error_type": group.fingerprint.error_type,
                    "occurrence_count": group.occurrence_count,
                    "last_occurrence": group.last_occurrence.isoformat(),
                    "affected_users": len(group.affected_users),
                    "affected_services": list(group.affected_services)
                }
                for group in critical_groups
            ],
            "top_error_groups": [
                {
                    "group_id": group.group_id,
                    "error_type": group.fingerprint.error_type,
                    "occurrence_count": group.occurrence_count,
                    "severity": group.severity.value,
                    "category": group.category.value,
                    "resolution_status": group.resolution_status.value
                }
                for group in top_groups
            ],
            "trends": self.analyze_error_trends(24)
        }


# Global error tracker instance
_error_tracker: Optional[DataDogErrorTracker] = None


def get_error_tracker(service_name: str = "error-tracking",
                     datadog_monitoring: Optional[DatadogMonitoring] = None) -> DataDogErrorTracker:
    """Get or create error tracker."""
    global _error_tracker
    
    if _error_tracker is None:
        _error_tracker = DataDogErrorTracker(service_name, datadog_monitoring)
    
    return _error_tracker


# Convenience functions and decorators

async def capture_exception(exception: Exception, 
                           tags: Optional[Dict[str, Any]] = None,
                           metadata: Optional[Dict[str, Any]] = None) -> str:
    """Convenience function for capturing exceptions."""
    tracker = get_error_tracker()
    return await tracker.capture_error(exception, tags=tags, metadata=metadata)


@contextmanager
def error_tracking_context(operation_name: str, 
                          additional_context: Optional[Dict[str, Any]] = None):
    """Context manager for automatic error tracking."""
    
    tracker = get_error_tracker()
    
    try:
        yield
    except Exception as e:
        # Create context with operation name
        context = asyncio.create_task(tracker._create_default_context()).result()
        if context.custom_context is None:
            context.custom_context = {}
        
        context.custom_context["operation_name"] = operation_name
        if additional_context:
            context.custom_context.update(additional_context)
        
        # Capture error
        asyncio.create_task(tracker.capture_error(e, context=context))
        raise


def track_errors(operation_name: Optional[str] = None):
    """Decorator for automatic error tracking."""
    
    def decorator(func: Callable) -> Callable:
        op_name = operation_name or f"{func.__module__}.{func.__name__}"
        
        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    await capture_exception(e, metadata={"operation": op_name})
                    raise
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    asyncio.create_task(capture_exception(e, metadata={"operation": op_name}))
                    raise
            return sync_wrapper
    
    return decorator


def get_error_trends(hours: int = 24) -> Dict[str, Any]:
    """Get error trends analysis."""
    tracker = get_error_tracker()
    return tracker.analyze_error_trends(hours)


def get_error_dashboard() -> Dict[str, Any]:
    """Get error dashboard data."""
    tracker = get_error_tracker()
    return tracker.get_error_dashboard_data()


def resolve_error_group(group_id: str, resolution_notes: Optional[str] = None) -> bool:
    """Mark an error group as resolved."""
    tracker = get_error_tracker()
    
    if group_id in tracker.error_groups:
        group = tracker.error_groups[group_id]
        group.resolution_status = ErrorResolution.RESOLVED
        group.resolution_notes = resolution_notes
        
        tracker.error_stats["resolved_errors"] += 1
        return True
    
    return False
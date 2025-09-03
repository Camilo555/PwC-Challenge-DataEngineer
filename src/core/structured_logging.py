"""
Advanced Structured Logging System
Provides comprehensive logging with correlation IDs, performance metrics,
security event logging, and business intelligence tracking.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from typing import Any

from core.config import settings
from core.logging import get_logger


class LogLevel(Enum):
    """Enhanced log levels for different event types."""

    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    SECURITY = "SECURITY"
    AUDIT = "AUDIT"
    PERFORMANCE = "PERFORMANCE"
    BUSINESS = "BUSINESS"


class EventCategory(Enum):
    """Event categories for structured logging."""

    SYSTEM = "system"
    APPLICATION = "application"
    SECURITY = "security"
    BUSINESS = "business"
    PERFORMANCE = "performance"
    AUDIT = "audit"
    ETL = "etl"
    API = "api"
    DATABASE = "database"
    CACHE = "cache"
    EXTERNAL_API = "external_api"
    USER_ACTION = "user_action"
    ERROR = "error"
    MONITORING = "monitoring"


@dataclass
class LogContext:
    """Enhanced logging context with correlation tracking."""

    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str | None = None
    session_id: str | None = None
    request_id: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    operation: str | None = None
    component: str | None = None
    environment: str = field(default_factory=lambda: getattr(settings, "environment", "unknown"))
    service_name: str = "pwc-data-engineering"
    version: str = "1.0.0"
    additional_context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert context to dictionary for logging."""
        return {
            "correlation_id": self.correlation_id,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "request_id": self.request_id,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "operation": self.operation,
            "component": self.component,
            "environment": self.environment,
            "service_name": self.service_name,
            "version": self.version,
            **self.additional_context,
        }


@dataclass
class SecurityEvent:
    """Security event for audit logging."""

    event_type: str
    severity: str
    user_id: str | None = None
    ip_address: str | None = None
    user_agent: str | None = None
    resource: str | None = None
    action: str | None = None
    success: bool = True
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "event_type": self.event_type,
            "severity": self.severity,
            "user_id": self.user_id,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "resource": self.resource,
            "action": self.action,
            "success": self.success,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class PerformanceEvent:
    """Performance event for performance logging."""

    operation: str
    duration_ms: float
    component: str
    resource_usage: dict[str, float] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "operation": self.operation,
            "duration_ms": self.duration_ms,
            "component": self.component,
            "resource_usage": self.resource_usage or {},
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class BusinessEvent:
    """Business event for business metrics logging."""

    event_type: str
    entity_type: str
    entity_id: str
    action: str
    value: float | None = None
    currency: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "event_type": self.event_type,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "action": self.action,
            "value": self.value,
            "currency": self.currency,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class AuditEvent:
    """Audit event for compliance and tracking."""

    action: str
    resource: str
    user_id: str | None = None
    result: str = "success"
    before_state: dict[str, Any] | None = None
    after_state: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "action": self.action,
            "resource": self.resource,
            "user_id": self.user_id,
            "result": self.result,
            "before_state": self.before_state,
            "after_state": self.after_state,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
        }


class StructuredJSONFormatter(logging.Formatter):
    """Advanced JSON formatter for structured logging with enhanced context."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as enhanced JSON with context."""
        # Base log data
        log_data = {
            "@timestamp": datetime.utcnow().isoformat() + "Z",
            "@version": "1",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "thread": record.threadName,
            "process": record.processName,
            "pathname": record.pathname,
        }

        # Add context if available
        context = getattr(record, "context", None)
        if context and isinstance(context, LogContext):
            log_data.update(context.to_dict())

        # Add category and event type
        log_data["category"] = getattr(record, "category", EventCategory.APPLICATION.value)
        log_data["event_type"] = getattr(record, "event_type", "log")

        # Add performance metrics if available
        performance_event = getattr(record, "performance_event", None)
        if performance_event:
            log_data["performance"] = performance_event.to_dict()

        # Add security context if available
        security_event = getattr(record, "security_event", None)
        if security_event:
            log_data["security"] = security_event.to_dict()

        # Add business context if available
        business_event = getattr(record, "business_event", None)
        if business_event:
            log_data["business"] = business_event.to_dict()

        # Add audit context if available
        audit_event = getattr(record, "audit_event", None)
        if audit_event:
            log_data["audit"] = audit_event.to_dict()

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = {
                "class": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info),
            }

        # Add extra fields (excluding standard logging fields)
        excluded_fields = {
            "name",
            "msg",
            "args",
            "created",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "thread",
            "threadName",
            "exc_info",
            "exc_text",
            "stack_info",
            "getMessage",
            "message",
            "context",
            "category",
            "event_type",
            "performance_event",
            "security_event",
            "business_event",
            "audit_event",
        }

        for key, value in record.__dict__.items():
            if key not in excluded_fields:
                log_data[key] = value

        # Add host information
        log_data["host"] = {
            "hostname": os.getenv("HOSTNAME", "unknown"),
            "ip": os.getenv("HOST_IP", "unknown"),
        }

        # Add tags
        tags = getattr(record, "tags", [])
        if tags:
            log_data["tags"] = tags

        return json.dumps(log_data, default=str, ensure_ascii=False)


class LogMetricsCollector:
    """Collects logging metrics for monitoring."""

    def __init__(self):
        self.metrics = defaultdict(int)
        self.error_rates = defaultdict(lambda: deque(maxlen=100))
        self.performance_metrics = defaultdict(list)
        self.security_events = deque(maxlen=1000)
        self.business_events = deque(maxlen=1000)
        self.audit_events = deque(maxlen=1000)
        self.lock = threading.Lock()
        self.last_reset = datetime.now()

    def record_log_event(self, level: str, category: str, duration_ms: float | None = None):
        """Record logging event for metrics."""
        with self.lock:
            self.metrics[f"{level.lower()}_count"] += 1
            self.metrics[f"{category}_count"] += 1

            if level in ["ERROR", "CRITICAL"]:
                self.error_rates[category].append(datetime.now())

            if duration_ms is not None:
                self.performance_metrics[category].append(duration_ms)

    def record_security_event(self, event: SecurityEvent):
        """Record security event."""
        with self.lock:
            self.security_events.append(event)
            self.metrics["security_events_total"] += 1
            self.metrics[f"security_{event.severity.lower()}_count"] += 1

    def record_business_event(self, event: BusinessEvent):
        """Record business event."""
        with self.lock:
            self.business_events.append(event)
            self.metrics["business_events_total"] += 1
            self.metrics[f"business_{event.event_type}_count"] += 1

    def record_audit_event(self, event: AuditEvent):
        """Record audit event."""
        with self.lock:
            self.audit_events.append(event)
            self.metrics["audit_events_total"] += 1
            self.metrics[f"audit_{event.result}_count"] += 1

    def get_metrics(self) -> dict[str, Any]:
        """Get collected metrics."""
        with self.lock:
            now = datetime.now()
            cutoff = now - timedelta(minutes=5)

            # Calculate error rates (last 5 minutes)
            error_rates = {}
            for category, events in self.error_rates.items():
                recent_errors = [e for e in events if e > cutoff]
                error_rates[f"{category}_error_rate_5m"] = len(recent_errors)

            # Calculate performance metrics
            perf_metrics = {}
            for category, durations in self.performance_metrics.items():
                if durations:
                    perf_metrics[f"{category}_avg_duration_ms"] = sum(durations) / len(durations)
                    perf_metrics[f"{category}_max_duration_ms"] = max(durations)
                    perf_metrics[f"{category}_min_duration_ms"] = min(durations)

            # Get recent security events
            recent_security = [e for e in self.security_events if e.timestamp > cutoff]
            security_summary = {
                "total_events_5m": len(recent_security),
                "critical_events_5m": len([e for e in recent_security if e.severity == "critical"]),
                "failed_events_5m": len([e for e in recent_security if not e.success]),
            }

            # Get recent business events
            recent_business = [e for e in self.business_events if e.timestamp > cutoff]
            business_summary = {
                "total_events_5m": len(recent_business),
                "revenue_events_5m": len([e for e in recent_business if e.value]),
                "avg_transaction_value": sum(e.value for e in recent_business if e.value)
                / max(len([e for e in recent_business if e.value]), 1),
            }

            return {
                "counts": dict(self.metrics),
                "error_rates": error_rates,
                "performance": perf_metrics,
                "security_summary": security_summary,
                "business_summary": business_summary,
                "collection_period_minutes": (now - self.last_reset).total_seconds() / 60,
            }

    def reset_metrics(self):
        """Reset metrics collection."""
        with self.lock:
            self.metrics.clear()
            self.performance_metrics.clear()
            self.last_reset = datetime.now()


# Thread-local storage for context
_context_storage = threading.local()

# Global metrics collector
_metrics_collector = LogMetricsCollector()


def set_log_context(context: LogContext):
    """Set logging context for current thread."""
    _context_storage.context = context


def get_log_context() -> LogContext | None:
    """Get current logging context."""
    return getattr(_context_storage, "context", None)


def clear_log_context():
    """Clear current logging context."""
    if hasattr(_context_storage, "context"):
        delattr(_context_storage, "context")


@contextmanager
def log_context(context: LogContext):
    """Context manager for setting logging context."""
    old_context = get_log_context()
    set_log_context(context)
    try:
        yield context
    finally:
        if old_context:
            set_log_context(old_context)
        else:
            clear_log_context()


def timed_operation(operation_name: str, component: str = None):
    """Decorator for timing operations and logging performance."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            context = get_log_context() or LogContext()
            context.operation = operation_name
            if component:
                context.component = component

            logger = StructuredLogger.get_logger(func.__module__)

            with log_context(context):
                try:
                    result = func(*args, **kwargs)
                    duration_ms = (time.time() - start_time) * 1000

                    # Log performance event
                    perf_event = PerformanceEvent(
                        operation=operation_name,
                        duration_ms=duration_ms,
                        component=component or func.__module__,
                    )

                    logger.log_performance(perf_event)

                    return result

                except Exception as e:
                    duration_ms = (time.time() - start_time) * 1000

                    logger.error(
                        f"Operation {operation_name} failed after {duration_ms:.2f}ms: {str(e)}",
                        exc_info=True,
                        extra={
                            "category": EventCategory.ERROR.value,
                            "event_type": "operation_failed",
                            "operation": operation_name,
                        },
                    )

                    _metrics_collector.record_log_event(
                        "ERROR", EventCategory.ERROR.value, duration_ms
                    )

                    raise

        return wrapper

    return decorator


class StructuredLogger:
    """Enhanced structured logger with support for different event types."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.metrics_collector = _metrics_collector

    @classmethod
    def get_logger(cls, name: str) -> StructuredLogger:
        """Get structured logger for the given name."""
        base_logger = get_logger(name)
        return cls(base_logger)

    def _log_with_context(self, level: int, msg: str, **extra):
        """Log with current context."""
        context = get_log_context()
        if context:
            extra["context"] = context

        self.logger.log(level, msg, extra=extra)

    def debug(self, msg: str, **kwargs):
        """Log debug message."""
        self._log_with_context(logging.DEBUG, msg, **kwargs)
        self.metrics_collector.record_log_event(
            "DEBUG", kwargs.get("category", EventCategory.APPLICATION.value)
        )

    def info(self, msg: str, **kwargs):
        """Log info message."""
        self._log_with_context(logging.INFO, msg, **kwargs)
        self.metrics_collector.record_log_event(
            "INFO", kwargs.get("category", EventCategory.APPLICATION.value)
        )

    def warning(self, msg: str, **kwargs):
        """Log warning message."""
        self._log_with_context(logging.WARNING, msg, **kwargs)
        self.metrics_collector.record_log_event(
            "WARNING", kwargs.get("category", EventCategory.APPLICATION.value)
        )

    def error(self, msg: str, exc_info: bool = False, **kwargs):
        """Log error message."""
        if exc_info:
            kwargs["exc_info"] = True
        self._log_with_context(logging.ERROR, msg, **kwargs)
        self.metrics_collector.record_log_event(
            "ERROR", kwargs.get("category", EventCategory.APPLICATION.value)
        )

    def critical(self, msg: str, exc_info: bool = False, **kwargs):
        """Log critical message."""
        if exc_info:
            kwargs["exc_info"] = True
        self._log_with_context(logging.CRITICAL, msg, **kwargs)
        self.metrics_collector.record_log_event(
            "CRITICAL", kwargs.get("category", EventCategory.APPLICATION.value)
        )

    def log_security(self, event: SecurityEvent, message: str = None):
        """Log security event."""
        msg = message or f"Security event: {event.event_type}"

        self.logger.log(
            logging.WARNING,
            msg,
            extra={
                "category": EventCategory.SECURITY.value,
                "event_type": "security_event",
                "security_event": event,
                "context": get_log_context(),
            },
        )

        self.metrics_collector.record_security_event(event)

    def log_business(self, event: BusinessEvent, message: str = None):
        """Log business event."""
        msg = message or f"Business event: {event.event_type}"

        self.logger.log(
            logging.INFO,
            msg,
            extra={
                "category": EventCategory.BUSINESS.value,
                "event_type": "business_event",
                "business_event": event,
                "context": get_log_context(),
            },
        )

        self.metrics_collector.record_business_event(event)

    def log_performance(self, event: PerformanceEvent, message: str = None):
        """Log performance event."""
        msg = message or f"Performance: {event.operation} took {event.duration_ms:.2f}ms"

        self.logger.log(
            logging.DEBUG,
            msg,
            extra={
                "category": EventCategory.PERFORMANCE.value,
                "event_type": "performance_event",
                "performance_event": event,
                "context": get_log_context(),
            },
        )

        self.metrics_collector.record_log_event(
            "DEBUG", EventCategory.PERFORMANCE.value, event.duration_ms
        )

    def log_audit(self, event: AuditEvent, message: str = None):
        """Log audit event."""
        msg = message or f"Audit: {event.action} on {event.resource}"

        self.logger.log(
            logging.INFO,
            msg,
            extra={
                "category": EventCategory.AUDIT.value,
                "event_type": "audit_event",
                "audit_event": event,
                "context": get_log_context(),
            },
        )

        self.metrics_collector.record_audit_event(event)

    def log_etl_stage(
        self,
        stage: str,
        records_processed: int,
        duration_ms: float,
        quality_score: float = None,
        errors: int = 0,
    ):
        """Log ETL stage completion."""
        perf_event = PerformanceEvent(
            operation=f"etl_{stage}",
            duration_ms=duration_ms,
            component="etl",
            metadata={
                "records_processed": records_processed,
                "quality_score": quality_score,
                "errors": errors,
                "records_per_second": records_processed / (duration_ms / 1000)
                if duration_ms > 0
                else 0,
            },
        )

        self.log_performance(
            perf_event,
            f"ETL {stage} stage completed: {records_processed} records in {duration_ms:.2f}ms",
        )

    def log_api_request(
        self, method: str, endpoint: str, status_code: int, duration_ms: float, user_id: str = None
    ):
        """Log API request."""
        context = get_log_context() or LogContext()
        context.user_id = user_id

        with log_context(context):
            perf_event = PerformanceEvent(
                operation=f"api_{method.lower()}",
                duration_ms=duration_ms,
                component="api",
                metadata={
                    "endpoint": endpoint,
                    "method": method,
                    "status_code": status_code,
                    "user_id": user_id,
                },
            )

            if status_code >= 400:
                self.error(
                    f"API {method} {endpoint} failed with {status_code} in {duration_ms:.2f}ms",
                    category=EventCategory.API.value,
                    event_type="api_error",
                    performance_event=perf_event,
                )
            else:
                self.log_performance(
                    perf_event,
                    f"API {method} {endpoint} completed with {status_code} in {duration_ms:.2f}ms",
                )

    def log_database_operation(
        self,
        operation: str,
        table: str,
        duration_ms: float,
        rows_affected: int = None,
        success: bool = True,
    ):
        """Log database operation."""
        perf_event = PerformanceEvent(
            operation=f"db_{operation.lower()}",
            duration_ms=duration_ms,
            component="database",
            metadata={
                "operation": operation,
                "table": table,
                "rows_affected": rows_affected,
                "success": success,
            },
        )

        if success:
            self.log_performance(
                perf_event, f"Database {operation} on {table} completed in {duration_ms:.2f}ms"
            )
        else:
            self.error(
                f"Database {operation} on {table} failed after {duration_ms:.2f}ms",
                category=EventCategory.DATABASE.value,
                event_type="database_error",
                performance_event=perf_event,
            )


# Logging setup functions
def setup_structured_logging(name: str, enable_json: bool = True) -> StructuredLogger:
    """Set up structured logging for a component."""
    base_logger = get_logger(name)

    if enable_json:
        # Add structured formatter
        for handler in base_logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setFormatter(StructuredJSONFormatter())

    return StructuredLogger(base_logger)


def get_logging_metrics() -> dict[str, Any]:
    """Get current logging metrics."""
    return _metrics_collector.get_metrics()


def reset_logging_metrics():
    """Reset logging metrics."""
    _metrics_collector.reset_metrics()


# Convenience functions for common logging scenarios
def log_user_action(user_id: str, action: str, resource: str, success: bool = True, **metadata):
    """Log user action for audit purposes."""
    logger = StructuredLogger.get_logger("user_actions")

    context = LogContext(user_id=user_id, operation=action)

    with log_context(context):
        audit_event = AuditEvent(
            action=action,
            resource=resource,
            user_id=user_id,
            result="success" if success else "failure",
            metadata=metadata,
        )

        logger.log_audit(audit_event)


def log_security_incident(
    event_type: str,
    severity: str,
    details: dict[str, Any],
    user_id: str = None,
    ip_address: str = None,
):
    """Log security incident."""
    logger = StructuredLogger.get_logger("security")

    security_event = SecurityEvent(
        event_type=event_type,
        severity=severity,
        user_id=user_id,
        ip_address=ip_address,
        details=details,
    )

    logger.log_security(security_event)


def log_business_transaction(
    transaction_type: str,
    entity_type: str,
    entity_id: str,
    value: float,
    currency: str = "USD",
    **metadata,
):
    """Log business transaction."""
    logger = StructuredLogger.get_logger("business")

    business_event = BusinessEvent(
        event_type=transaction_type,
        entity_type=entity_type,
        entity_id=entity_id,
        action="transaction",
        value=value,
        currency=currency,
        metadata=metadata,
    )

    logger.log_business(business_event)


# Default structured logger
default_structured_logger = setup_structured_logging(__name__)


if __name__ == "__main__":
    # Test structured logging
    logger = setup_structured_logging("test")

    # Test with context
    context = LogContext(user_id="test_user", operation="test_operation")

    with log_context(context):
        logger.info("Test message with context")

        # Test performance logging
        @timed_operation("test_function", "test_component")
        def test_function():
            time.sleep(0.1)
            return "result"

        result = test_function()

        # Test security logging
        security_event = SecurityEvent(
            event_type="authentication_failure",
            severity="high",
            user_id="test_user",
            ip_address="192.168.1.1",
        )
        logger.log_security(security_event)

        # Test business logging
        business_event = BusinessEvent(
            event_type="purchase",
            entity_type="customer",
            entity_id="cust_123",
            action="buy",
            value=99.99,
            currency="USD",
        )
        logger.log_business(business_event)

    print("Logging metrics:", get_logging_metrics())

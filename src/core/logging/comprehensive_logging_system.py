"""
Comprehensive Logging System
Enterprise-grade logging configuration with structured logging,
log aggregation, monitoring integration, and adaptive log levels.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import logging
import logging.config
import logging.handlers
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import structlog
from pythonjsonlogger import jsonlogger

from core.config.centralized_config_manager import get_config_manager


class LogLevel(Enum):
    """Enhanced log levels with business context."""
    TRACE = 5       # Detailed tracing for debugging
    DEBUG = 10      # Debug information
    INFO = 20       # General information
    SUCCESS = 25    # Success operations
    WARNING = 30    # Warning conditions
    ERROR = 40      # Error conditions
    CRITICAL = 50   # Critical errors
    SECURITY = 60   # Security-related events
    AUDIT = 70      # Audit trail events


class LogCategory(Enum):
    """Log categories for organization and filtering."""
    APPLICATION = "application"
    SECURITY = "security"
    AUDIT = "audit"
    PERFORMANCE = "performance"
    BUSINESS = "business"
    SYSTEM = "system"
    API = "api"
    DATABASE = "database"
    EXTERNAL = "external"
    ERROR = "error"


@dataclass
class LogConfig:
    """Comprehensive logging configuration."""
    level: LogLevel = LogLevel.INFO
    format_type: str = "structured"  # "structured", "json", "text"
    include_timestamp: bool = True
    include_correlation_id: bool = True
    include_user_context: bool = True
    include_request_context: bool = True
    include_performance_metrics: bool = True

    # File logging
    enable_file_logging: bool = True
    log_directory: str = "./logs"
    max_file_size: int = 100 * 1024 * 1024  # 100MB
    backup_count: int = 10
    compress_rotated: bool = True

    # Console logging
    enable_console_logging: bool = True
    console_format: str = "colored"

    # Remote logging
    enable_remote_logging: bool = False
    remote_endpoint: Optional[str] = None
    remote_api_key: Optional[str] = None

    # Monitoring integration
    enable_metrics_collection: bool = True
    enable_error_tracking: bool = True
    enable_performance_tracking: bool = True

    # Filtering and sampling
    log_sampling_rate: float = 1.0  # 1.0 = log everything, 0.1 = log 10%
    sensitive_fields: Set[str] = field(default_factory=lambda: {
        'password', 'token', 'secret', 'key', 'auth', 'credential'
    })

    # Adaptive logging
    enable_adaptive_levels: bool = True
    adaptive_threshold_errors_per_minute: int = 10
    adaptive_increase_duration_minutes: int = 15


class SensitiveDataFilter:
    """Filter to sanitize sensitive information from logs."""

    def __init__(self, sensitive_fields: Set[str]):
        """Initialize with set of sensitive field names."""
        self.sensitive_fields = {field.lower() for field in sensitive_fields}
        self.replacement_text = "***REDACTED***"

    def filter_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively filter sensitive data from dictionary."""
        if not isinstance(data, dict):
            return data

        filtered = {}
        for key, value in data.items():
            key_lower = key.lower()

            if any(sensitive in key_lower for sensitive in self.sensitive_fields):
                filtered[key] = self.replacement_text
            elif isinstance(value, dict):
                filtered[key] = self.filter_dict(value)
            elif isinstance(value, list):
                filtered[key] = [
                    self.filter_dict(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                filtered[key] = value

        return filtered

    def filter_string(self, text: str) -> str:
        """Filter sensitive data from string content."""
        # Simple pattern matching for common sensitive patterns
        import re

        # Credit card numbers
        text = re.sub(r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',
                     'XXXX-XXXX-XXXX-XXXX', text)

        # Email addresses (partial redaction)
        text = re.sub(r'\b([a-zA-Z0-9._%+-]+)@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b',
                     r'\1***@\2', text)

        # API keys (common patterns)
        text = re.sub(r'\b[a-zA-Z0-9]{32,}\b', '***API_KEY***', text)

        return text


class CorrelationContextFilter(logging.Filter):
    """Filter to add correlation context to log records."""

    def filter(self, record):
        """Add correlation context to log record."""
        # Add correlation ID from thread-local storage or context
        correlation_id = getattr(threading.current_thread(), 'correlation_id', None)
        if correlation_id:
            record.correlation_id = correlation_id

        # Add user context
        user_context = getattr(threading.current_thread(), 'user_context', {})
        if user_context:
            record.user_id = user_context.get('user_id')
            record.user_role = user_context.get('role')

        # Add request context
        request_context = getattr(threading.current_thread(), 'request_context', {})
        if request_context:
            record.request_id = request_context.get('request_id')
            record.endpoint = request_context.get('endpoint')
            record.method = request_context.get('method')

        return True


class PerformanceMetricsFilter(logging.Filter):
    """Filter to add performance metrics to log records."""

    def __init__(self):
        """Initialize performance tracking."""
        super().__init__()
        self.request_start_times = {}

    def filter(self, record):
        """Add performance metrics to log record."""
        # Add timestamp
        record.log_timestamp = datetime.utcnow().isoformat()

        # Add process and thread info
        record.process_id = os.getpid()
        record.thread_id = threading.get_ident()

        # Add memory usage if available
        try:
            import psutil
            process = psutil.Process()
            record.memory_usage_mb = process.memory_info().rss / 1024 / 1024
            record.cpu_percent = process.cpu_percent()
        except ImportError:
            pass

        return True


class AdaptiveLogLevelManager:
    """Manages adaptive log levels based on system conditions."""

    def __init__(self, config: LogConfig):
        """Initialize adaptive log level manager."""
        self.config = config
        self.error_counts = {}
        self.original_levels = {}
        self.increased_levels = {}
        self.last_reset_time = time.time()

        # Start background monitoring if enabled
        if config.enable_adaptive_levels:
            self._start_monitoring()

    def _start_monitoring(self):
        """Start background monitoring for adaptive log levels."""
        def monitor_loop():
            while True:
                time.sleep(60)  # Check every minute
                self._check_and_adjust_levels()

        monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        monitor_thread.start()

    def record_error(self, logger_name: str):
        """Record an error for adaptive level calculation."""
        current_time = time.time()

        if logger_name not in self.error_counts:
            self.error_counts[logger_name] = []

        # Add current error timestamp
        self.error_counts[logger_name].append(current_time)

        # Remove errors older than 1 minute
        cutoff_time = current_time - 60
        self.error_counts[logger_name] = [
            timestamp for timestamp in self.error_counts[logger_name]
            if timestamp > cutoff_time
        ]

    def _check_and_adjust_levels(self):
        """Check error rates and adjust log levels accordingly."""
        current_time = time.time()
        threshold = self.config.adaptive_threshold_errors_per_minute

        for logger_name, error_timestamps in self.error_counts.items():
            errors_per_minute = len(error_timestamps)

            logger = logging.getLogger(logger_name)

            if errors_per_minute >= threshold:
                # Increase log level for this logger
                if logger_name not in self.increased_levels:
                    self.original_levels[logger_name] = logger.level
                    logger.setLevel(logging.DEBUG)
                    self.increased_levels[logger_name] = current_time

                    structlog.get_logger().info(
                        "Adaptive logging: Increased log level for logger",
                        logger_name=logger_name,
                        errors_per_minute=errors_per_minute,
                        new_level="DEBUG"
                    )

        # Reset levels that have been increased for long enough
        duration_seconds = self.config.adaptive_increase_duration_minutes * 60
        loggers_to_reset = []

        for logger_name, increase_time in self.increased_levels.items():
            if current_time - increase_time > duration_seconds:
                loggers_to_reset.append(logger_name)

        for logger_name in loggers_to_reset:
            logger = logging.getLogger(logger_name)
            original_level = self.original_levels.get(logger_name, logging.INFO)
            logger.setLevel(original_level)

            del self.increased_levels[logger_name]
            del self.original_levels[logger_name]

            structlog.get_logger().info(
                "Adaptive logging: Reset log level for logger",
                logger_name=logger_name,
                reset_level=logging.getLevelName(original_level)
            )


class ComprehensiveLogManager:
    """
    Enterprise logging manager with comprehensive features:
    - Structured logging with JSON output
    - Sensitive data filtering
    - Correlation ID tracking
    - Performance metrics
    - Adaptive log levels
    - Log aggregation and monitoring
    """

    def __init__(self, config: Optional[LogConfig] = None):
        """Initialize comprehensive logging manager."""
        self.config = config or LogConfig()
        self.sensitive_filter = SensitiveDataFilter(self.config.sensitive_fields)
        self.adaptive_manager = AdaptiveLogLevelManager(self.config)
        self.log_metrics = {
            'total_logs': 0,
            'errors_logged': 0,
            'warnings_logged': 0,
            'security_events': 0,
            'audit_events': 0,
            'performance_logs': 0
        }

        # Setup logging configuration
        self._setup_logging()

        # Setup structlog
        self._setup_structlog()

        # Create log directories
        self._ensure_log_directories()

    def _setup_logging(self):
        """Setup standard Python logging configuration."""
        # Create custom log levels
        logging.addLevelName(LogLevel.TRACE.value, "TRACE")
        logging.addLevelName(LogLevel.SUCCESS.value, "SUCCESS")
        logging.addLevelName(LogLevel.SECURITY.value, "SECURITY")
        logging.addLevelName(LogLevel.AUDIT.value, "AUDIT")

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(self.config.level.value)

        # Clear existing handlers
        root_logger.handlers.clear()

        # Add console handler if enabled
        if self.config.enable_console_logging:
            self._add_console_handler(root_logger)

        # Add file handlers if enabled
        if self.config.enable_file_logging:
            self._add_file_handlers(root_logger)

        # Add remote handler if enabled
        if self.config.enable_remote_logging and self.config.remote_endpoint:
            self._add_remote_handler(root_logger)

    def _add_console_handler(self, logger: logging.Logger):
        """Add console handler with appropriate formatting."""
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.config.level.value)

        if self.config.format_type == "json":
            formatter = jsonlogger.JsonFormatter(
                '%(asctime)s %(name)s %(levelname)s %(message)s'
            )
        else:
            # Colored console output
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )

        console_handler.setFormatter(formatter)

        # Add filters
        console_handler.addFilter(CorrelationContextFilter())
        console_handler.addFilter(PerformanceMetricsFilter())

        logger.addHandler(console_handler)

    def _add_file_handlers(self, logger: logging.Logger):
        """Add file handlers for different log categories."""
        log_dir = Path(self.config.log_directory)

        # Main application log
        app_handler = logging.handlers.RotatingFileHandler(
            log_dir / "application.log",
            maxBytes=self.config.max_file_size,
            backupCount=self.config.backup_count
        )

        # Error log
        error_handler = logging.handlers.RotatingFileHandler(
            log_dir / "errors.log",
            maxBytes=self.config.max_file_size,
            backupCount=self.config.backup_count
        )
        error_handler.setLevel(logging.ERROR)

        # Security log
        security_handler = logging.handlers.RotatingFileHandler(
            log_dir / "security.log",
            maxBytes=self.config.max_file_size,
            backupCount=self.config.backup_count
        )
        security_handler.setLevel(LogLevel.SECURITY.value)

        # Audit log
        audit_handler = logging.handlers.RotatingFileHandler(
            log_dir / "audit.log",
            maxBytes=self.config.max_file_size,
            backupCount=self.config.backup_count
        )
        audit_handler.setLevel(LogLevel.AUDIT.value)

        # Configure formatters
        if self.config.format_type == "json":
            formatter = jsonlogger.JsonFormatter(
                '%(asctime)s %(name)s %(levelname)s %(message)s'
            )
        else:
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )

        # Apply formatters and filters
        for handler in [app_handler, error_handler, security_handler, audit_handler]:
            handler.setFormatter(formatter)
            handler.addFilter(CorrelationContextFilter())
            handler.addFilter(PerformanceMetricsFilter())
            logger.addHandler(handler)

    def _add_remote_handler(self, logger: logging.Logger):
        """Add remote logging handler for log aggregation."""
        # This would integrate with services like ELK stack, Splunk, etc.
        # Implementation depends on specific remote logging service
        pass

    def _setup_structlog(self):
        """Setup structlog for structured logging."""
        processors = [
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
        ]

        if self.config.format_type == "json":
            processors.append(structlog.processors.JSONRenderer())
        else:
            processors.append(structlog.dev.ConsoleRenderer())

        structlog.configure(
            processors=processors,
            wrapper_class=structlog.stdlib.BoundLogger,
            logger_factory=structlog.stdlib.LoggerFactory(),
            context_class=dict,
            cache_logger_on_first_use=True,
        )

    def _ensure_log_directories(self):
        """Ensure log directories exist."""
        log_dir = Path(self.config.log_directory)
        log_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories for different log types
        for subdir in ["application", "security", "audit", "performance", "errors"]:
            (log_dir / subdir).mkdir(exist_ok=True)

    def get_logger(
        self,
        name: str,
        category: LogCategory = LogCategory.APPLICATION,
        context: Optional[Dict[str, Any]] = None
    ) -> structlog.BoundLogger:
        """
        Get a logger with specified category and context.

        Args:
            name: Logger name
            category: Log category for organization
            context: Additional context to bind to logger

        Returns:
            Configured structlog BoundLogger
        """
        logger = structlog.get_logger(name)

        # Bind category and any additional context
        bound_context = {"category": category.value}
        if context:
            bound_context.update(self.sensitive_filter.filter_dict(context))

        return logger.bind(**bound_context)

    def log_security_event(
        self,
        event_type: str,
        description: str,
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        additional_context: Optional[Dict[str, Any]] = None
    ):
        """Log security-related events with standardized format."""
        context = {
            "event_type": event_type,
            "user_id": user_id,
            "ip_address": ip_address,
            "timestamp": datetime.utcnow().isoformat()
        }

        if additional_context:
            context.update(self.sensitive_filter.filter_dict(additional_context))

        logger = self.get_logger("security", LogCategory.SECURITY, context)
        logger.log(LogLevel.SECURITY.value, description)

        self.log_metrics['security_events'] += 1

    def log_audit_event(
        self,
        action: str,
        resource: str,
        user_id: Optional[str] = None,
        result: str = "success",
        additional_context: Optional[Dict[str, Any]] = None
    ):
        """Log audit trail events with standardized format."""
        context = {
            "action": action,
            "resource": resource,
            "user_id": user_id,
            "result": result,
            "timestamp": datetime.utcnow().isoformat()
        }

        if additional_context:
            context.update(self.sensitive_filter.filter_dict(additional_context))

        logger = self.get_logger("audit", LogCategory.AUDIT, context)
        logger.log(LogLevel.AUDIT.value, f"Audit: {action} on {resource}")

        self.log_metrics['audit_events'] += 1

    def log_performance_metrics(
        self,
        operation: str,
        duration_ms: float,
        success: bool = True,
        additional_metrics: Optional[Dict[str, Any]] = None
    ):
        """Log performance metrics with standardized format."""
        context = {
            "operation": operation,
            "duration_ms": duration_ms,
            "success": success,
            "timestamp": datetime.utcnow().isoformat()
        }

        if additional_metrics:
            context.update(additional_metrics)

        logger = self.get_logger("performance", LogCategory.PERFORMANCE, context)

        if duration_ms > 1000:  # Log slow operations as warnings
            logger.warning(f"Slow operation: {operation} took {duration_ms:.2f}ms")
        else:
            logger.info(f"Performance: {operation} completed in {duration_ms:.2f}ms")

        self.log_metrics['performance_logs'] += 1

    def log_business_event(
        self,
        event_name: str,
        event_data: Dict[str, Any],
        user_id: Optional[str] = None,
        correlation_id: Optional[str] = None
    ):
        """Log business-level events for analytics and monitoring."""
        context = {
            "event_name": event_name,
            "user_id": user_id,
            "correlation_id": correlation_id,
            "timestamp": datetime.utcnow().isoformat()
        }

        # Filter sensitive data from event data
        filtered_data = self.sensitive_filter.filter_dict(event_data)
        context.update(filtered_data)

        logger = self.get_logger("business", LogCategory.BUSINESS, context)
        logger.info(f"Business event: {event_name}")

    def get_log_metrics(self) -> Dict[str, Any]:
        """Get comprehensive logging metrics."""
        return {
            **self.log_metrics,
            "adaptive_adjustments": len(self.adaptive_manager.increased_levels),
            "error_rate_threshold": self.config.adaptive_threshold_errors_per_minute,
            "total_loggers": len(logging.Logger.manager.loggerDict),
            "config": {
                "level": self.config.level.name,
                "format_type": self.config.format_type,
                "file_logging": self.config.enable_file_logging,
                "console_logging": self.config.enable_console_logging,
                "remote_logging": self.config.enable_remote_logging,
                "adaptive_levels": self.config.enable_adaptive_levels
            }
        }

    def configure_from_config_manager(self):
        """Configure logging from centralized configuration manager."""
        config_manager = get_config_manager()

        # Update configuration from centralized config
        log_config = {
            "level": config_manager.get("logging.level", "INFO"),
            "format_type": config_manager.get("logging.format", "structured"),
            "log_directory": config_manager.get("logging.directory", "./logs"),
            "enable_file_logging": config_manager.get("logging.file_enabled", True),
            "enable_console_logging": config_manager.get("logging.console_enabled", True),
            "enable_adaptive_levels": config_manager.get("logging.adaptive_enabled", True),
            "log_sampling_rate": config_manager.get("logging.sampling_rate", 1.0)
        }

        # Apply configuration updates
        for key, value in log_config.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)

        # Reconfigure logging with new settings
        self._setup_logging()


# Global log manager instance
_log_manager: Optional[ComprehensiveLogManager] = None


def get_log_manager(config: Optional[LogConfig] = None) -> ComprehensiveLogManager:
    """Get or create the global log manager."""
    global _log_manager
    if _log_manager is None:
        _log_manager = ComprehensiveLogManager(config)
    return _log_manager


def get_logger(
    name: str,
    category: LogCategory = LogCategory.APPLICATION,
    context: Optional[Dict[str, Any]] = None
) -> structlog.BoundLogger:
    """Convenience function to get a configured logger."""
    return get_log_manager().get_logger(name, category, context)


def log_security_event(
    event_type: str,
    description: str,
    user_id: Optional[str] = None,
    ip_address: Optional[str] = None,
    **kwargs
):
    """Convenience function to log security events."""
    get_log_manager().log_security_event(
        event_type, description, user_id, ip_address, kwargs
    )


def log_audit_event(
    action: str,
    resource: str,
    user_id: Optional[str] = None,
    result: str = "success",
    **kwargs
):
    """Convenience function to log audit events."""
    get_log_manager().log_audit_event(
        action, resource, user_id, result, kwargs
    )


def log_performance(
    operation: str,
    duration_ms: float,
    success: bool = True,
    **kwargs
):
    """Convenience function to log performance metrics."""
    get_log_manager().log_performance_metrics(
        operation, duration_ms, success, kwargs
    )


# Performance monitoring decorator
def log_performance_decorator(operation_name: Optional[str] = None):
    """Decorator to automatically log performance metrics."""
    def decorator(func):
        import functools

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            operation = operation_name or f"{func.__module__}.{func.__name__}"
            success = True

            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                success = False
                raise
            finally:
                duration_ms = (time.time() - start_time) * 1000
                log_performance(operation, duration_ms, success)

        return wrapper
    return decorator


# Export key classes and functions
__all__ = [
    'ComprehensiveLogManager',
    'LogConfig',
    'LogLevel',
    'LogCategory',
    'SensitiveDataFilter',
    'AdaptiveLogLevelManager',
    'get_log_manager',
    'get_logger',
    'log_security_event',
    'log_audit_event',
    'log_performance',
    'log_performance_decorator'
]
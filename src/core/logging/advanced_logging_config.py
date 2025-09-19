"""
Advanced Logging Configuration
Provides comprehensive structured logging with performance monitoring,
distributed tracing, and centralized log management.
"""
from __future__ import annotations

import json
import logging
import logging.config
import sys
import time
import traceback
import uuid
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable
from threading import Lock

from core.config.base_config import BaseConfig

config = BaseConfig()


class LogLevel:
    """Standard log levels with numeric values."""
    TRACE = 5
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


class StructuredFormatter(logging.Formatter):
    """Enhanced structured JSON formatter with performance metrics."""
    
    def __init__(self, include_context: bool = True, include_performance: bool = True):
        super().__init__()
        self.include_context = include_context
        self.include_performance = include_performance
        self.hostname = self._get_hostname()
    
    def _get_hostname(self) -> str:
        """Get hostname for log context."""
        try:
            import socket
            return socket.gethostname()
        except Exception:
            return "unknown"
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        # Base log structure
        log_entry = {
            "@timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "@version": "1",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "thread": record.thread,
            "thread_name": record.threadName,
            "process": record.process,
            "process_name": record.processName,
            "host": self.hostname,
            "environment": getattr(config, 'environment', 'unknown')
        }
        
        # Add source location
        if record.pathname:
            log_entry["source"] = {
                "file": record.filename,
                "line": record.lineno,
                "function": record.funcName,
                "path": record.pathname
            }
        
        # Add exception information
        if record.exc_info:
            log_entry["exception"] = {
                "class": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info)
            }
        
        # Add stack trace if available
        if record.stack_info:
            log_entry["stack_trace"] = record.stack_info
        
        # Add custom fields from extra
        for key, value in record.__dict__.items():
            if key not in {
                'name', 'msg', 'args', 'created', 'filename', 'funcName',
                'levelname', 'levelno', 'lineno', 'module', 'msecs', 'pathname',
                'process', 'processName', 'relativeCreated', 'thread', 'threadName',
                'exc_info', 'exc_text', 'stack_info', 'getMessage', 'message'
            }:
                # Handle complex objects
                try:
                    if isinstance(value, (dict, list, str, int, float, bool, type(None))):
                        log_entry[key] = value
                    else:
                        log_entry[key] = str(value)
                except Exception:
                    log_entry[key] = f"<unserializable: {type(value).__name__}>"
        
        # Add performance context if available
        if self.include_performance and hasattr(record, 'performance'):
            log_entry["performance"] = record.performance
        
        # Add distributed tracing context if available
        if self.include_context:
            if hasattr(record, 'trace_id'):
                log_entry["trace_id"] = record.trace_id
            if hasattr(record, 'span_id'):
                log_entry["span_id"] = record.span_id
            if hasattr(record, 'correlation_id'):
                log_entry["correlation_id"] = record.correlation_id
        
        return json.dumps(log_entry, default=self._json_serializer)
    
    def _json_serializer(self, obj: Any) -> str:
        """Custom JSON serializer for complex objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Exception):
            return str(obj)
        elif hasattr(obj, '__dict__'):
            return str(obj)
        else:
            return f"<unserializable: {type(obj).__name__}>"


class PerformanceFilter(logging.Filter):
    """Filter to add performance metrics to log records."""
    
    def __init__(self):
        super().__init__()
        self.start_times: Dict[str, float] = {}
        self.lock = Lock()
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add performance metrics to the log record."""
        current_time = time.time()
        
        # Add timestamp
        record.timestamp_ms = int(current_time * 1000)
        
        # Add request processing time if available
        if hasattr(record, 'request_id'):
            with self.lock:
                if record.request_id not in self.start_times:
                    self.start_times[record.request_id] = current_time
                    record.request_start = True
                else:
                    elapsed = current_time - self.start_times[record.request_id]
                    record.request_duration_ms = elapsed * 1000
                    # Clean up to prevent memory leak
                    del self.start_times[record.request_id]
        
        return True


class SecurityFilter(logging.Filter):
    """Filter to redact sensitive information from logs."""
    
    def __init__(self):
        super().__init__()
        # Patterns for sensitive data
        self.sensitive_patterns = [
            (r'password["\s]*[:=]["\s]*([^"\\s,}]+)', 'password'),
            (r'token["\s]*[:=]["\s]*([^"\\s,}]+)', 'token'),
            (r'key["\s]*[:=]["\s]*([^"\\s,}]+)', 'key'),
            (r'secret["\s]*[:=]["\s]*([^"\\s,}]+)', 'secret'),
            (r'authorization["\s]*[:=]["\s]*([^"\\s,}]+)', 'authorization'),
            (r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b', 'credit_card'),  # Credit card
            (r'\b\d{3}-\d{2}-\d{4}\b', 'ssn'),  # SSN
        ]
        
        import re
        self.compiled_patterns = [
            (re.compile(pattern, re.IGNORECASE), name) 
            for pattern, name in self.sensitive_patterns
        ]
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Redact sensitive information from log message."""
        if hasattr(record, 'message'):
            message = record.message
        else:
            message = record.getMessage()
        
        # Redact sensitive information
        for pattern, name in self.compiled_patterns:
            message = pattern.sub(f'[REDACTED_{name.upper()}]', message)
        
        # Update the record
        record.msg = message
        record.args = ()  # Clear args to prevent re-formatting
        
        return True


class MetricsCollector:
    """Collect and aggregate logging metrics."""
    
    def __init__(self):
        self.metrics = defaultdict(int)
        self.performance_metrics = defaultdict(list)
        self.error_counts = defaultdict(int)
        self.last_reset = time.time()
        self.lock = Lock()
    
    def record_log_event(self, level: str, logger_name: str, duration_ms: float = 0):
        """Record a log event for metrics."""
        with self.lock:
            self.metrics[f"log_events_{level.lower()}"] += 1
            self.metrics[f"logger_{logger_name}"] += 1
            
            if duration_ms > 0:
                self.performance_metrics[logger_name].append(duration_ms)
            
            if level in ['ERROR', 'CRITICAL']:
                self.error_counts[logger_name] += 1
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get current metrics summary."""
        with self.lock:
            avg_durations = {}
            for logger, durations in self.performance_metrics.items():
                if durations:
                    avg_durations[f"avg_duration_{logger}"] = sum(durations) / len(durations)
            
            return {
                "period_start": self.last_reset,
                "period_duration": time.time() - self.last_reset,
                "log_counts": dict(self.metrics),
                "error_counts": dict(self.error_counts),
                "average_durations": avg_durations,
                "total_log_events": sum(self.metrics.values())
            }
    
    def reset_metrics(self):
        """Reset metrics collection."""
        with self.lock:
            self.metrics.clear()
            self.performance_metrics.clear()
            self.error_counts.clear()
            self.last_reset = time.time()


class MetricsHandler(logging.Handler):
    """Handler that collects metrics from log events."""
    
    def __init__(self, metrics_collector: MetricsCollector):
        super().__init__()
        self.metrics_collector = metrics_collector
    
    def emit(self, record: logging.LogRecord):
        """Process log record and collect metrics."""
        try:
            duration = getattr(record, 'request_duration_ms', 0)
            self.metrics_collector.record_log_event(
                record.levelname,
                record.name,
                duration
            )
        except Exception:
            # Don't let metrics collection interfere with logging
            pass


class AdvancedLoggingConfig:
    """Advanced logging configuration manager."""
    
    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        self.config_dict = config_dict or self._get_default_config()
        self.metrics_collector = MetricsCollector()
        self.performance_filter = PerformanceFilter()
        self.security_filter = SecurityFilter()
        self.metrics_handler = MetricsHandler(self.metrics_collector)
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default logging configuration."""
        log_level = getattr(config, 'log_level', 'INFO').upper()
        log_format = getattr(config, 'log_format', 'json')
        log_file_path = getattr(config, 'log_file_path', None)
        
        config_dict = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'structured': {
                    '()': StructuredFormatter,
                    'include_context': True,
                    'include_performance': True,
                },
                'simple': {
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S'
                },
                'detailed': {
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S'
                }
            },
            'filters': {
                'performance': {
                    '()': PerformanceFilter,
                },
                'security': {
                    '()': SecurityFilter,
                }
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': log_level,
                    'formatter': 'structured' if log_format == 'json' else 'detailed',
                    'filters': ['performance', 'security'],
                    'stream': sys.stdout
                },
                'metrics': {
                    '()': MetricsHandler,
                    'metrics_collector': self.metrics_collector,
                    'level': 'DEBUG'
                }
            },
            'loggers': {
                # Application loggers
                'api': {
                    'level': log_level,
                    'handlers': ['console', 'metrics'],
                    'propagate': False
                },
                'core': {
                    'level': log_level,
                    'handlers': ['console', 'metrics'],
                    'propagate': False
                },
                'etl': {
                    'level': log_level,
                    'handlers': ['console', 'metrics'],
                    'propagate': False
                },
                'analytics': {
                    'level': log_level,
                    'handlers': ['console', 'metrics'],
                    'propagate': False
                },
                # Third-party loggers
                'uvicorn': {
                    'level': 'INFO',
                    'handlers': ['console'],
                    'propagate': False
                },
                'sqlalchemy': {
                    'level': 'WARNING',
                    'handlers': ['console'],
                    'propagate': False
                },
                'pyspark': {
                    'level': 'WARNING',
                    'handlers': ['console'],
                    'propagate': False
                }
            },
            'root': {
                'level': log_level,
                'handlers': ['console', 'metrics']
            }
        }
        
        # Add file handler if log file path is specified
        if log_file_path:
            log_path = Path(log_file_path)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            
            config_dict['handlers']['file'] = {
                'class': 'logging.handlers.RotatingFileHandler',
                'level': log_level,
                'formatter': 'structured',
                'filters': ['performance', 'security'],
                'filename': str(log_path),
                'maxBytes': 100 * 1024 * 1024,  # 100MB
                'backupCount': 10,
                'encoding': 'utf8'
            }
            
            # Add file handler to all loggers
            for logger_config in config_dict['loggers'].values():
                if 'handlers' in logger_config:
                    logger_config['handlers'].append('file')
            config_dict['root']['handlers'].append('file')
        
        return config_dict
    
    def setup_logging(self):
        """Setup logging configuration."""
        # Apply configuration
        logging.config.dictConfig(self.config_dict)
        
        # Add trace level
        logging.addLevelName(LogLevel.TRACE, 'TRACE')
        
        # Add trace method to logger
        def trace(self, message, *args, **kwargs):
            if self.isEnabledFor(LogLevel.TRACE):
                self._log(LogLevel.TRACE, message, args, **kwargs)
        
        logging.Logger.trace = trace
    
    def get_logger(self, name: str) -> logging.Logger:
        """Get configured logger instance."""
        return logging.getLogger(name)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current logging metrics."""
        return self.metrics_collector.get_metrics_summary()
    
    def reset_metrics(self):
        """Reset logging metrics."""
        self.metrics_collector.reset_metrics()
    
    def add_context_to_logs(self, **context):
        """Add context to all future log messages in current thread."""
        # This would typically use thread-local storage or async context
        # For now, we'll use a simple approach
        old_factory = logging.getLogRecordFactory()
        
        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            for key, value in context.items():
                setattr(record, key, value)
            return record
        
        logging.setLogRecordFactory(record_factory)
        
        return lambda: logging.setLogRecordFactory(old_factory)


# Performance logging decorators
def log_performance(logger_name: Optional[str] = None, level: str = 'INFO'):
    """Decorator to log function performance."""
    def decorator(func: Callable) -> Callable:
        logger = logging.getLogger(logger_name or func.__module__)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            function_name = f"{func.__module__}.{func.__name__}"
            
            try:
                result = func(*args, **kwargs)
                duration = (time.time() - start_time) * 1000
                
                logger.log(
                    getattr(logging, level.upper()),
                    f"Function {function_name} completed successfully",
                    extra={
                        "function": function_name,
                        "duration_ms": duration,
                        "performance": {
                            "function_name": function_name,
                            "duration_ms": duration,
                            "status": "success"
                        }
                    }
                )
                return result
            except Exception as e:
                duration = (time.time() - start_time) * 1000
                logger.error(
                    f"Function {function_name} failed",
                    extra={
                        "function": function_name,
                        "duration_ms": duration,
                        "error": str(e),
                        "performance": {
                            "function_name": function_name,
                            "duration_ms": duration,
                            "status": "error",
                            "error": str(e)
                        }
                    },
                    exc_info=True
                )
                raise
        
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            function_name = f"{func.__module__}.{func.__name__}"
            
            try:
                result = await func(*args, **kwargs)
                duration = (time.time() - start_time) * 1000
                
                logger.log(
                    getattr(logging, level.upper()),
                    f"Async function {function_name} completed successfully",
                    extra={
                        "function": function_name,
                        "duration_ms": duration,
                        "performance": {
                            "function_name": function_name,
                            "duration_ms": duration,
                            "status": "success",
                            "async": True
                        }
                    }
                )
                return result
            except Exception as e:
                duration = (time.time() - start_time) * 1000
                logger.error(
                    f"Async function {function_name} failed",
                    extra={
                        "function": function_name,
                        "duration_ms": duration,
                        "error": str(e),
                        "performance": {
                            "function_name": function_name,
                            "duration_ms": duration,
                            "status": "error",
                            "error": str(e),
                            "async": True
                        }
                    },
                    exc_info=True
                )
                raise
        
        # Return appropriate wrapper based on function type
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


@contextmanager
def log_context(**context):
    """Context manager to add context to logs within a block."""
    # Generate a unique context ID
    context_id = str(uuid.uuid4())
    context['context_id'] = context_id
    
    old_factory = logging.getLogRecordFactory()
    
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        for key, value in context.items():
            setattr(record, key, value)
        return record
    
    try:
        logging.setLogRecordFactory(record_factory)
        yield context_id
    finally:
        logging.setLogRecordFactory(old_factory)


# Global logging configuration instance
_logging_config: Optional[AdvancedLoggingConfig] = None


def get_advanced_logging_config() -> AdvancedLoggingConfig:
    """Get or create global advanced logging configuration."""
    global _logging_config
    if _logging_config is None:
        _logging_config = AdvancedLoggingConfig()
        _logging_config.setup_logging()
    return _logging_config


def get_performance_logger(name: str) -> logging.Logger:
    """Get logger configured for performance monitoring."""
    config = get_advanced_logging_config()
    return config.get_logger(name)


def get_logging_metrics() -> Dict[str, Any]:
    """Get current logging metrics."""
    config = get_advanced_logging_config()
    return config.get_metrics()


# Initialize logging on import (disabled for testing)
# get_advanced_logging_config()
"""
Production-Ready Structured Logging with PII Scrubbing
Provides enterprise-grade logging with automatic PII detection and correlation ID propagation.
Designed for ETL pipelines and API applications with security and compliance requirements.
"""

import json
import logging
import re
import sys
import threading
from datetime import datetime
from typing import Any, Dict, Optional, List, Union
from pathlib import Path
from contextvars import ContextVar
from dataclasses import dataclass, asdict
import hashlib
import uuid

# Optional structured logging dependencies
try:
    import structlog
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False

try:
    from pythonjsonlogger import jsonlogger
    JSON_LOGGER_AVAILABLE = True
except ImportError:
    JSON_LOGGER_AVAILABLE = False


# Context variables for correlation IDs
correlation_id_context: ContextVar[Optional[str]] = ContextVar('correlation_id', default=None)
trace_id_context: ContextVar[Optional[str]] = ContextVar('trace_id', default=None)
user_id_context: ContextVar[Optional[str]] = ContextVar('user_id', default=None)


@dataclass
class LogLevel:
    """Log level constants."""
    DEBUG = 'DEBUG'
    INFO = 'INFO'
    WARNING = 'WARNING'
    ERROR = 'ERROR'
    CRITICAL = 'CRITICAL'


class PIIDetector:
    """
    Advanced PII detection and scrubbing with configurable patterns.
    Supports common PII types and custom patterns.
    """
    
    # Comprehensive PII detection patterns
    DEFAULT_PATTERNS = {
        'email': {
            'pattern': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'replacement': '[EMAIL_REDACTED]',
            'description': 'Email addresses'
        },
        'phone_us': {
            'pattern': r'\b(?:\+?1[-.\s]?)?(?:\(?[0-9]{3}\)?[-.\s]?)?[0-9]{3}[-.\s]?[0-9]{4}\b',
            'replacement': '[PHONE_REDACTED]',
            'description': 'US phone numbers'
        },
        'phone_uk': {
            'pattern': r'\b(?:\+44[-.\s]?)?(?:\(?0\)?[-.\s]?)?[1-9][0-9]{8,9}\b',
            'replacement': '[PHONE_REDACTED]',
            'description': 'UK phone numbers'
        },
        'ssn': {
            'pattern': r'\b\d{3}-\d{2}-\d{4}\b',
            'replacement': '[SSN_REDACTED]',
            'description': 'US Social Security Numbers'
        },
        'credit_card': {
            'pattern': r'\b(?:\d{4}[-\s]?){3}\d{4}\b',
            'replacement': '[CARD_REDACTED]',
            'description': 'Credit card numbers'
        },
        'credit_card_short': {
            'pattern': r'\b\d{13,19}\b',
            'replacement': '[CARD_REDACTED]',
            'description': 'Credit card numbers (condensed)'
        },
        'ip_address': {
            'pattern': r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b',
            'replacement': '[IP_REDACTED]',
            'description': 'IPv4 addresses'
        },
        'password': {
            'pattern': r'(?i)(?:password|pwd|pass|secret|key|token)\s*[:=]\s*["\']?([^"\'\s]+)["\']?',
            'replacement': r'\g<0>[PASSWORD_REDACTED]',
            'description': 'Passwords and secrets'
        },
        'api_key': {
            'pattern': r'(?i)(?:api[_-]?key|access[_-]?token|bearer\s+|authorization:\s*bearer\s+)["\']?([a-zA-Z0-9._-]{20,})["\']?',
            'replacement': r'\g<0>[API_KEY_REDACTED]',
            'description': 'API keys and access tokens'
        },
        'jwt_token': {
            'pattern': r'eyJ[A-Za-z0-9_=-]+\.eyJ[A-Za-z0-9_=-]+\.?[A-Za-z0-9_=-]*',
            'replacement': '[JWT_REDACTED]',
            'description': 'JWT tokens'
        },
        'uuid': {
            'pattern': r'\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b',
            'replacement': '[UUID_REDACTED]',
            'description': 'UUIDs (when used as sensitive identifiers)'
        }
    }
    
    def __init__(self, custom_patterns: Optional[Dict[str, Dict[str, str]]] = None, 
                 enabled_patterns: Optional[List[str]] = None):
        """
        Initialize PII detector with optional custom patterns.
        
        Args:
            custom_patterns: Dictionary of custom PII patterns
            enabled_patterns: List of pattern names to enable (default: all)
        """
        self.patterns = self.DEFAULT_PATTERNS.copy()
        
        if custom_patterns:
            self.patterns.update(custom_patterns)
        
        # Filter enabled patterns
        if enabled_patterns:
            self.patterns = {k: v for k, v in self.patterns.items() if k in enabled_patterns}
        
        # Compile regex patterns for better performance
        self._compiled_patterns = {}
        for name, config in self.patterns.items():
            try:
                self._compiled_patterns[name] = {
                    'regex': re.compile(config['pattern'], re.IGNORECASE | re.MULTILINE),
                    'replacement': config['replacement'],
                    'description': config.get('description', f'Pattern: {name}')
                }
            except re.error as e:
                # Log pattern compilation error but continue
                print(f"Warning: Failed to compile PII pattern '{name}': {e}")
    
    def scrub_text(self, text: str, preserve_structure: bool = True) -> str:
        """
        Scrub PII from text while optionally preserving structure.
        
        Args:
            text: Input text to scrub
            preserve_structure: If True, maintain text length/structure where possible
            
        Returns:
            Scrubbed text with PII removed/masked
        """
        if not text or not isinstance(text, str):
            return text
        
        scrubbed = text
        
        for name, config in self._compiled_patterns.items():
            try:
                if preserve_structure and 'REDACTED' in config['replacement']:
                    # For structure preservation, replace with similar-length masked text
                    def mask_match(match):
                        original = match.group(0)
                        if len(original) <= 4:
                            return '*' * len(original)
                        else:
                            # Show first and last character, mask middle
                            return f"{original[0]}{'*' * (len(original) - 2)}{original[-1]}"
                    
                    scrubbed = config['regex'].sub(mask_match, scrubbed)
                else:
                    scrubbed = config['regex'].sub(config['replacement'], scrubbed)
            except Exception as e:
                # Continue if individual pattern fails
                print(f"Warning: PII scrubbing pattern '{name}' failed: {e}")
                continue
        
        return scrubbed
    
    def scrub_dict(self, data: Dict[str, Any], deep_scrub: bool = True) -> Dict[str, Any]:
        """
        Recursively scrub PII from dictionary structures.
        
        Args:
            data: Dictionary to scrub
            deep_scrub: If True, recursively scrub nested structures
            
        Returns:
            Dictionary with PII scrubbed
        """
        if not isinstance(data, dict):
            return data
        
        # Sensitive key patterns that should be completely redacted
        sensitive_keys = {
            'password', 'pwd', 'secret', 'token', 'key', 'auth', 'authorization',
            'api_key', 'access_token', 'refresh_token', 'bearer', 'credential',
            'ssn', 'social_security', 'credit_card', 'card_number', 'cvv', 'pin'
        }
        
        scrubbed = {}
        
        for key, value in data.items():
            key_lower = str(key).lower()
            
            # Check if key name suggests sensitive data
            is_sensitive_key = any(sensitive in key_lower for sensitive in sensitive_keys)
            
            if is_sensitive_key:
                scrubbed[key] = '[REDACTED]'
            elif isinstance(value, str):
                scrubbed[key] = self.scrub_text(value)
            elif isinstance(value, dict) and deep_scrub:
                scrubbed[key] = self.scrub_dict(value, deep_scrub)
            elif isinstance(value, list) and deep_scrub:
                scrubbed[key] = self._scrub_list(value, deep_scrub)
            else:
                scrubbed[key] = value
        
        return scrubbed
    
    def _scrub_list(self, data: List[Any], deep_scrub: bool = True) -> List[Any]:
        """Scrub PII from list structures."""
        scrubbed = []
        
        for item in data:
            if isinstance(item, str):
                scrubbed.append(self.scrub_text(item))
            elif isinstance(item, dict) and deep_scrub:
                scrubbed.append(self.scrub_dict(item, deep_scrub))
            elif isinstance(item, list) and deep_scrub:
                scrubbed.append(self._scrub_list(item, deep_scrub))
            else:
                scrubbed.append(item)
        
        return scrubbed
    
    def add_custom_pattern(self, name: str, pattern: str, replacement: str, 
                          description: Optional[str] = None) -> None:
        """Add a custom PII detection pattern."""
        try:
            self.patterns[name] = {
                'pattern': pattern,
                'replacement': replacement,
                'description': description or f'Custom pattern: {name}'
            }
            
            self._compiled_patterns[name] = {
                'regex': re.compile(pattern, re.IGNORECASE | re.MULTILINE),
                'replacement': replacement,
                'description': self.patterns[name]['description']
            }
        except re.error as e:
            raise ValueError(f"Invalid regex pattern '{pattern}': {e}")


class CorrelationIdManager:
    """
    Manages correlation IDs for request/operation tracing.
    Thread-safe and supports nested operations.
    """
    
    def __init__(self):
        self._local = threading.local()
    
    def generate_correlation_id(self) -> str:
        """Generate a new correlation ID."""
        return str(uuid.uuid4())
    
    def generate_trace_id(self) -> str:
        """Generate a new trace ID (for distributed tracing)."""
        return hashlib.sha256(f"{uuid.uuid4()}{datetime.utcnow().isoformat()}".encode()).hexdigest()[:16]
    
    def set_correlation_id(self, correlation_id: str) -> None:
        """Set correlation ID in context."""
        correlation_id_context.set(correlation_id)
    
    def set_trace_id(self, trace_id: str) -> None:
        """Set trace ID in context."""
        trace_id_context.set(trace_id)
    
    def set_user_id(self, user_id: str) -> None:
        """Set user ID in context."""
        user_id_context.set(user_id)
    
    def get_correlation_id(self) -> Optional[str]:
        """Get current correlation ID."""
        return correlation_id_context.get()
    
    def get_trace_id(self) -> Optional[str]:
        """Get current trace ID."""
        return trace_id_context.get()
    
    def get_user_id(self) -> Optional[str]:
        """Get current user ID."""
        return user_id_context.get()
    
    def get_context_dict(self) -> Dict[str, str]:
        """Get all context values as dictionary."""
        context = {}
        
        if correlation_id := self.get_correlation_id():
            context['correlation_id'] = correlation_id
        
        if trace_id := self.get_trace_id():
            context['trace_id'] = trace_id
        
        if user_id := self.get_user_id():
            context['user_id'] = user_id
        
        return context


class ETLLogContext:
    """
    Context manager for ETL operation logging with automatic metrics collection.
    """
    
    def __init__(self, operation: str, dataset: str, layer: str, 
                 correlation_manager: Optional[CorrelationIdManager] = None,
                 logger: Optional[logging.Logger] = None,
                 **extra_context):
        """
        Initialize ETL logging context.
        
        Args:
            operation: Name of the ETL operation (e.g., 'extract', 'transform', 'load')
            dataset: Dataset being processed
            layer: Data layer (bronze, silver, gold)
            correlation_manager: Correlation ID manager instance
            logger: Logger instance to use
            **extra_context: Additional context to include in logs
        """
        self.operation = operation
        self.dataset = dataset
        self.layer = layer
        self.extra_context = extra_context
        self.start_time = None
        self.correlation_manager = correlation_manager or CorrelationIdManager()
        self.logger = logger or get_structured_logger(f"etl.{layer}.{dataset}")
        
        # Generate correlation ID if not present
        if not self.correlation_manager.get_correlation_id():
            self.correlation_manager.set_correlation_id(self.correlation_manager.generate_correlation_id())
    
    def __enter__(self):
        """Enter the context and log operation start."""
        self.start_time = datetime.utcnow()
        
        context = {
            'operation': self.operation,
            'dataset': self.dataset,
            'layer': self.layer,
            'status': 'started',
            'timestamp': self.start_time.isoformat(),
            **self.correlation_manager.get_context_dict(),
            **self.extra_context
        }
        
        self.logger.info('ETL operation started', extra=context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context and log operation completion."""
        end_time = datetime.utcnow()
        duration_ms = int((end_time - self.start_time).total_seconds() * 1000)
        
        context = {
            'operation': self.operation,
            'dataset': self.dataset,
            'layer': self.layer,
            'duration_ms': duration_ms,
            'timestamp': end_time.isoformat(),
            **self.correlation_manager.get_context_dict(),
            **self.extra_context
        }
        
        if exc_type is None:
            context['status'] = 'completed'
            self.logger.info('ETL operation completed successfully', extra=context)
        else:
            context.update({
                'status': 'failed',
                'error_type': exc_type.__name__,
                'error_message': str(exc_val)
            })
            self.logger.error('ETL operation failed', extra=context)
    
    def log_progress(self, message: str, **metrics) -> None:
        """Log progress update with metrics."""
        context = {
            'operation': self.operation,
            'dataset': self.dataset,
            'layer': self.layer,
            'status': 'progress',
            **self.correlation_manager.get_context_dict(),
            **metrics
        }
        
        self.logger.info(message, extra=context)
    
    def log_data_quality_check(self, check_name: str, passed: bool, 
                              details: Optional[Dict[str, Any]] = None) -> None:
        """Log data quality check results."""
        context = {
            'operation': self.operation,
            'dataset': self.dataset,
            'layer': self.layer,
            'dq_check_name': check_name,
            'dq_passed': passed,
            'dq_details': details or {},
            **self.correlation_manager.get_context_dict()
        }
        
        level = logging.INFO if passed else logging.WARNING
        message = f"Data quality check '{check_name}' {'passed' if passed else 'failed'}"
        
        self.logger.log(level, message, extra=context)


class SecureFormatter(logging.Formatter):
    """
    Custom formatter that automatically scrubs PII and adds context information.
    """
    
    def __init__(self, fmt=None, datefmt=None, pii_detector: Optional[PIIDetector] = None,
                 correlation_manager: Optional[CorrelationIdManager] = None):
        super().__init__(fmt, datefmt)
        self.pii_detector = pii_detector or PIIDetector()
        self.correlation_manager = correlation_manager or CorrelationIdManager()
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with PII scrubbing and context injection."""
        
        # Add correlation context to record
        context = self.correlation_manager.get_context_dict()
        for key, value in context.items():
            setattr(record, key, value)
        
        # Add standard fields
        record.timestamp = datetime.utcnow().isoformat()
        record.level = record.levelname
        record.logger_name = record.name
        
        # Format the base message
        formatted = super().format(record)
        
        # Scrub PII from the formatted message
        return self.pii_detector.scrub_text(formatted)


class SecureJSONFormatter(SecureFormatter):
    """
    JSON formatter with PII scrubbing and structured context.
    """
    
    def __init__(self, pii_detector: Optional[PIIDetector] = None,
                 correlation_manager: Optional[CorrelationIdManager] = None,
                 extra_fields: Optional[List[str]] = None):
        super().__init__(pii_detector=pii_detector, correlation_manager=correlation_manager)
        self.extra_fields = extra_fields or []
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as scrubbed JSON."""
        
        # Build log entry dictionary
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger_name': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
        }
        
        # Add correlation context
        log_entry.update(self.correlation_manager.get_context_dict())
        
        # Add exception information if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields from record
        for field in self.extra_fields:
            if hasattr(record, field):
                log_entry[field] = getattr(record, field)
        
        # Add any extra attributes from record.__dict__
        for key, value in record.__dict__.items():
            if key not in log_entry and not key.startswith('_'):
                log_entry[key] = value
        
        # Scrub PII from the entire log entry
        scrubbed_entry = self.pii_detector.scrub_dict(log_entry)
        
        return json.dumps(scrubbed_entry, default=str)


def setup_secure_logging(
    level: str = 'INFO',
    log_file: Optional[str] = None,
    max_file_size: int = 50 * 1024 * 1024,  # 50MB
    backup_count: int = 5,
    json_format: bool = True,
    pii_detector: Optional[PIIDetector] = None,
    correlation_manager: Optional[CorrelationIdManager] = None,
    extra_fields: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Setup secure logging configuration with PII scrubbing and correlation IDs.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for file logging
        max_file_size: Maximum size for log files before rotation
        backup_count: Number of backup files to keep
        json_format: Whether to use JSON formatting
        pii_detector: Custom PII detector instance
        correlation_manager: Custom correlation manager instance
        extra_fields: Additional fields to include in JSON logs
    
    Returns:
        Dictionary with configured components
    """
    
    # Initialize components
    if not pii_detector:
        pii_detector = PIIDetector()
    
    if not correlation_manager:
        correlation_manager = CorrelationIdManager()
    
    # Set log level
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create formatter
    if json_format:
        formatter = SecureJSONFormatter(
            pii_detector=pii_detector,
            correlation_manager=correlation_manager,
            extra_fields=extra_fields
        )
    else:
        format_string = (
            '%(timestamp)s [%(level)s] %(logger_name)s:%(funcName)s:%(lineno)d '
            '%(correlation_id)s - %(message)s'
        )
        formatter = SecureFormatter(
            fmt=format_string,
            pii_detector=pii_detector,
            correlation_manager=correlation_manager
        )
    
    # Setup handlers
    handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)
    
    # File handler with rotation
    if log_file:
        from logging.handlers import RotatingFileHandler
        
        # Ensure log directory exists
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_file_size,
            backupCount=backup_count
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add new handlers
    for handler in handlers:
        root_logger.addHandler(handler)
    
    # Suppress noisy third-party loggers
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    
    return {
        'pii_detector': pii_detector,
        'correlation_manager': correlation_manager,
        'handlers': handlers,
        'log_level': log_level
    }


def get_structured_logger(name: str, **context) -> logging.Logger:
    """
    Get a logger instance with optional context binding.
    
    Args:
        name: Logger name
        **context: Additional context to bind to the logger
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Add context as extra attributes if needed
    if context:
        # Create a wrapper that includes context in log calls
        class ContextLogger:
            def __init__(self, base_logger, context_data):
                self._logger = base_logger
                self._context = context_data
            
            def _log_with_context(self, level, msg, *args, **kwargs):
                extra = kwargs.get('extra', {})
                extra.update(self._context)
                kwargs['extra'] = extra
                return self._logger.log(level, msg, *args, **kwargs)
            
            def debug(self, msg, *args, **kwargs):
                return self._log_with_context(logging.DEBUG, msg, *args, **kwargs)
            
            def info(self, msg, *args, **kwargs):
                return self._log_with_context(logging.INFO, msg, *args, **kwargs)
            
            def warning(self, msg, *args, **kwargs):
                return self._log_with_context(logging.WARNING, msg, *args, **kwargs)
            
            def error(self, msg, *args, **kwargs):
                return self._log_with_context(logging.ERROR, msg, *args, **kwargs)
            
            def critical(self, msg, *args, **kwargs):
                return self._log_with_context(logging.CRITICAL, msg, *args, **kwargs)
        
        return ContextLogger(logger, context)
    
    return logger


# Convenience functions for common logging patterns

def log_etl_metrics(
    logger: logging.Logger,
    operation: str,
    input_count: int,
    output_count: int,
    duration_ms: int,
    **additional_metrics
) -> None:
    """
    Log ETL processing metrics with standardized format.
    
    Args:
        logger: Logger instance
        operation: ETL operation name
        input_count: Number of input records
        output_count: Number of output records
        duration_ms: Processing duration in milliseconds
        **additional_metrics: Additional metrics to log
    """
    
    records_per_second = int(output_count / (duration_ms / 1000)) if duration_ms > 0 else 0
    data_quality_ratio = (output_count / input_count * 100) if input_count > 0 else 0
    
    metrics = {
        'etl_operation': operation,
        'input_record_count': input_count,
        'output_record_count': output_count,
        'duration_ms': duration_ms,
        'records_per_second': records_per_second,
        'data_quality_ratio': round(data_quality_ratio, 2),
        **additional_metrics
    }
    
    logger.info(f'ETL operation {operation} completed', extra=metrics)


def log_data_quality_summary(
    logger: logging.Logger,
    dataset: str,
    total_checks: int,
    passed_checks: int,
    failed_checks: int,
    critical_failures: int = 0,
    **check_details
) -> None:
    """
    Log data quality check summary.
    
    Args:
        logger: Logger instance
        dataset: Dataset name
        total_checks: Total number of checks performed
        passed_checks: Number of checks that passed
        failed_checks: Number of checks that failed
        critical_failures: Number of critical failures
        **check_details: Detailed check results
    """
    
    pass_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
    
    summary = {
        'dq_dataset': dataset,
        'dq_total_checks': total_checks,
        'dq_passed_checks': passed_checks,
        'dq_failed_checks': failed_checks,
        'dq_critical_failures': critical_failures,
        'dq_pass_rate': round(pass_rate, 2),
        'dq_check_details': check_details
    }
    
    level = logging.ERROR if critical_failures > 0 else (logging.WARNING if failed_checks > 0 else logging.INFO)
    
    logger.log(
        level,
        f'Data quality summary for {dataset}: {passed_checks}/{total_checks} checks passed',
        extra=summary
    )
"""
Comprehensive Logging Configuration
===================================

Advanced logging system with structured logging, multiple handlers,
filters, formatters, and integration with monitoring systems.

Features:
- Advanced JSON and structured formatters with security redaction
- Performance monitoring and metrics collection filters
- Database, security, and metrics handlers
- Distributed tracing and correlation ID support
- Compliance and audit trail capabilities
- Business metrics extraction
- Intelligent sampling and filtering
"""

import os
from typing import Optional, Dict, Any

# Import from existing modules
from .log_config import LoggingConfig, setup_logging, configure_logging

# Import new advanced modules
from .formatters import JSONFormatter, StructuredFormatter, ColoredFormatter
from .handlers import DatabaseLogHandler, MetricsLogHandler, SecurityLogHandler
from .filters import (
    PerformanceFilter,
    SecurityFilter,
    EnvironmentFilter,
    SamplingFilter,
    CorrelationFilter,
    BusinessMetricsFilter
)

# Import advanced logging config if available
try:
    from .advanced_logging_config import (
        AdvancedLoggingConfig,
        get_advanced_logging_config,
        get_performance_logger,
        get_logging_metrics,
        log_performance,
        log_context
    )
    ADVANCED_AVAILABLE = True
except ImportError:
    ADVANCED_AVAILABLE = False


def get_logger(name: str):
    """Get a logger instance with the given name."""
    import logging
    return logging.getLogger(name)


def initialize_enterprise_logging(
    environment: str = None,
    log_level: str = "INFO",
    log_format: str = "json",
    enable_performance_monitoring: bool = True,
    enable_security_logging: bool = True,
    enable_database_logging: bool = False,
    enable_metrics_collection: bool = True,
    custom_config: Optional[Dict[str, Any]] = None
):
    """
    Initialize enterprise logging system with comprehensive features.

    Args:
        environment: Deployment environment (development/staging/production)
        log_level: Logging level (DEBUG/INFO/WARNING/ERROR/CRITICAL)
        log_format: Log format (json/structured/simple/colored)
        enable_performance_monitoring: Enable performance tracking
        enable_security_logging: Enable security event logging
        enable_database_logging: Enable database log storage
        enable_metrics_collection: Enable metrics extraction
        custom_config: Custom configuration overrides

    Returns:
        Configured logging instance
    """
    # Determine environment
    if environment is None:
        environment = os.getenv('ENVIRONMENT', 'development')

    # Use advanced config if available, otherwise fall back to basic config
    if ADVANCED_AVAILABLE and custom_config:
        config = AdvancedLoggingConfig(custom_config)
        config.setup_logging()
        return config
    else:
        # Use basic configuration
        handlers = ['console', 'file']

        if enable_database_logging:
            handlers.append('database')
        if enable_security_logging:
            handlers.append('security')
        if enable_metrics_collection:
            handlers.append('metrics')

        basic_config = LoggingConfig(
            environment=environment,
            default_level=log_level,
            log_format=log_format,
            performance_logging=enable_performance_monitoring,
            security_logging=enable_security_logging,
            handlers=handlers
        )

        setup_logging(basic_config)
        return basic_config


__all__ = [
    # Core configuration
    'LoggingConfig',
    'setup_logging',
    'configure_logging',
    'initialize_enterprise_logging',
    'get_logger',

    # Formatters
    'JSONFormatter',
    'StructuredFormatter',
    'ColoredFormatter',

    # Handlers
    'DatabaseLogHandler',
    'MetricsLogHandler',
    'SecurityLogHandler',

    # Filters
    'PerformanceFilter',
    'SecurityFilter',
    'EnvironmentFilter',
    'SamplingFilter',
    'CorrelationFilter',
    'BusinessMetricsFilter',
]

# Add advanced features if available
if ADVANCED_AVAILABLE:
    __all__.extend([
        'AdvancedLoggingConfig',
        'get_advanced_logging_config',
        'get_performance_logger',
        'get_logging_metrics',
        'log_performance',
        'log_context'
    ])
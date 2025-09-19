"""
Logging Configuration System
============================

Comprehensive logging configuration with support for multiple environments,
structured logging, and integration with monitoring systems.
"""

import logging
import logging.config
import sys
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum

from pydantic import BaseModel, Field


class LogLevel(str, Enum):
    """Logging levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogFormat(str, Enum):
    """Log output formats."""
    JSON = "json"
    STRUCTURED = "structured"
    SIMPLE = "simple"
    COLORED = "colored"


@dataclass
class HandlerConfig:
    """Configuration for a logging handler."""
    name: str
    type: str  # console, file, rotating_file, database, syslog
    level: LogLevel = LogLevel.INFO
    format: LogFormat = LogFormat.JSON
    filename: Optional[str] = None
    max_bytes: int = 10_000_000  # 10MB
    backup_count: int = 5
    encoding: str = 'utf-8'
    filters: List[str] = field(default_factory=list)
    extra_config: Dict[str, Any] = field(default_factory=dict)


class LoggingConfig(BaseModel):
    """Comprehensive logging configuration."""

    # Global settings
    version: int = Field(default=1)
    disable_existing_loggers: bool = Field(default=False)

    # Log levels
    root_level: LogLevel = Field(default=LogLevel.INFO)
    default_level: LogLevel = Field(default=LogLevel.INFO)

    # Output settings
    log_format: LogFormat = Field(default=LogFormat.JSON)
    include_timestamp: bool = Field(default=True)
    include_level: bool = Field(default=True)
    include_logger_name: bool = Field(default=True)
    include_process_info: bool = Field(default=True)
    include_thread_info: bool = Field(default=False)
    include_trace_info: bool = Field(default=True)

    # File logging
    log_directory: str = Field(default="logs")
    log_filename: str = Field(default="application.log")
    max_file_size: int = Field(default=10_000_000)  # 10MB
    backup_count: int = Field(default=5)

    # Console logging
    console_enabled: bool = Field(default=True)
    console_level: LogLevel = Field(default=LogLevel.INFO)
    console_format: LogFormat = Field(default=LogFormat.COLORED)

    # Structured logging
    structured_logging: bool = Field(default=True)
    correlation_id_header: str = Field(default="x-correlation-id")

    # Performance logging
    performance_logging: bool = Field(default=True)
    slow_query_threshold: float = Field(default=1.0)  # seconds

    # Security logging
    security_logging: bool = Field(default=True)
    audit_logging: bool = Field(default=True)

    # External integrations
    datadog_enabled: bool = Field(default=False)
    datadog_api_key: Optional[str] = Field(default=None)
    elasticsearch_enabled: bool = Field(default=False)
    elasticsearch_url: Optional[str] = Field(default=None)
    syslog_enabled: bool = Field(default=False)
    syslog_host: str = Field(default="localhost")
    syslog_port: int = Field(default=514)

    # Logger-specific levels
    logger_levels: Dict[str, LogLevel] = Field(default_factory=dict)

    # Filters and handlers
    handlers: List[str] = Field(default_factory=lambda: ["console", "file"])
    filters: Dict[str, Any] = Field(default_factory=dict)

    # Environment-specific settings
    environment: str = Field(default="development")
    debug_mode: bool = Field(default=False)

    class Config:
        use_enum_values = True


def get_logging_config_dict(config: LoggingConfig) -> Dict[str, Any]:
    """Convert LoggingConfig to logging.config dictionary format."""

    # Ensure log directory exists
    log_dir = Path(config.log_directory)
    log_dir.mkdir(exist_ok=True)

    log_file_path = log_dir / config.log_filename

    # Base configuration structure
    logging_dict = {
        'version': config.version,
        'disable_existing_loggers': config.disable_existing_loggers,
        'formatters': _get_formatters(config),
        'filters': _get_filters(config),
        'handlers': _get_handlers(config, str(log_file_path)),
        'loggers': _get_loggers(config),
        'root': {
            'level': config.root_level.value,
            'handlers': config.handlers
        }
    }

    return logging_dict


def _get_formatters(config: LoggingConfig) -> Dict[str, Any]:
    """Get formatter configurations."""
    formatters = {}

    # JSON formatter
    formatters['json'] = {
        'class': 'src.core.logging.formatters.JSONFormatter',
        'include_timestamp': config.include_timestamp,
        'include_level': config.include_level,
        'include_logger_name': config.include_logger_name,
        'include_process_info': config.include_process_info,
        'include_thread_info': config.include_thread_info,
        'include_trace_info': config.include_trace_info
    }

    # Structured formatter
    formatters['structured'] = {
        'class': 'src.core.logging.formatters.StructuredFormatter',
        'format': '[%(asctime)s] %(levelname)-8s %(name)s: %(message)s',
        'datefmt': '%Y-%m-%d %H:%M:%S'
    }

    # Simple formatter
    formatters['simple'] = {
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        'datefmt': '%Y-%m-%d %H:%M:%S'
    }

    # Colored formatter (for console)
    formatters['colored'] = {
        'class': 'src.core.logging.formatters.ColoredFormatter',
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        'datefmt': '%Y-%m-%d %H:%M:%S'
    }

    return formatters


def _get_filters(config: LoggingConfig) -> Dict[str, Any]:
    """Get filter configurations."""
    filters = {}

    # Performance filter
    if config.performance_logging:
        filters['performance'] = {
            'class': 'src.core.logging.filters.PerformanceFilter',
            'slow_threshold': config.slow_query_threshold
        }

    # Security filter
    if config.security_logging:
        filters['security'] = {
            'class': 'src.core.logging.filters.SecurityFilter'
        }

    # Environment filter
    filters['environment'] = {
        'class': 'src.core.logging.filters.EnvironmentFilter',
        'environment': config.environment
    }

    return filters


def _get_handlers(config: LoggingConfig, log_file_path: str) -> Dict[str, Any]:
    """Get handler configurations."""
    handlers = {}

    # Console handler
    if config.console_enabled and 'console' in config.handlers:
        handlers['console'] = {
            'class': 'logging.StreamHandler',
            'level': config.console_level.value,
            'formatter': config.console_format.value,
            'stream': 'ext://sys.stdout'
        }

    # File handler
    if 'file' in config.handlers:
        handlers['file'] = {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': config.default_level.value,
            'formatter': config.log_format.value,
            'filename': log_file_path,
            'maxBytes': config.max_file_size,
            'backupCount': config.backup_count,
            'encoding': 'utf-8'
        }

    # Security handler
    if config.security_logging and 'security' in config.handlers:
        security_log_path = Path(config.log_directory) / 'security.log'
        handlers['security'] = {
            'class': 'src.core.logging.handlers.SecurityLogHandler',
            'level': 'WARNING',
            'formatter': 'json',
            'filename': str(security_log_path),
            'maxBytes': config.max_file_size,
            'backupCount': config.backup_count
        }

    # Database handler (if configured)
    if 'database' in config.handlers:
        handlers['database'] = {
            'class': 'src.core.logging.handlers.DatabaseLogHandler',
            'level': config.default_level.value,
            'formatter': 'json'
        }

    # Metrics handler
    if 'metrics' in config.handlers:
        handlers['metrics'] = {
            'class': 'src.core.logging.handlers.MetricsLogHandler',
            'level': 'INFO',
            'formatter': 'json'
        }

    # Datadog handler
    if config.datadog_enabled and config.datadog_api_key:
        handlers['datadog'] = {
            'class': 'datadog.DogStatsdLogHandler',
            'level': config.default_level.value,
            'formatter': 'json'
        }

    # Elasticsearch handler
    if config.elasticsearch_enabled and config.elasticsearch_url:
        handlers['elasticsearch'] = {
            'class': 'src.core.logging.handlers.ElasticsearchHandler',
            'level': config.default_level.value,
            'formatter': 'json',
            'elasticsearch_url': config.elasticsearch_url
        }

    # Syslog handler
    if config.syslog_enabled:
        handlers['syslog'] = {
            'class': 'logging.handlers.SysLogHandler',
            'level': config.default_level.value,
            'formatter': 'structured',
            'address': (config.syslog_host, config.syslog_port)
        }

    return handlers


def _get_loggers(config: LoggingConfig) -> Dict[str, Any]:
    """Get logger configurations."""
    loggers = {}

    # Application loggers
    app_loggers = [
        'src.api',
        'src.core',
        'src.etl',
        'src.monitoring',
        'src.domain',
        'uvicorn',
        'sqlalchemy',
        'fastapi'
    ]

    for logger_name in app_loggers:
        level = config.logger_levels.get(logger_name, config.default_level)
        loggers[logger_name] = {
            'level': level.value if hasattr(level, 'value') else level,
            'handlers': config.handlers,
            'propagate': False
        }

    # Security logger
    if config.security_logging:
        loggers['security'] = {
            'level': 'WARNING',
            'handlers': ['security', 'file'],
            'propagate': False
        }

    # Performance logger
    if config.performance_logging:
        loggers['performance'] = {
            'level': 'INFO',
            'handlers': ['file', 'metrics'],
            'propagate': False,
            'filters': ['performance']
        }

    # Audit logger
    if config.audit_logging:
        loggers['audit'] = {
            'level': 'INFO',
            'handlers': ['file', 'database'],
            'propagate': False
        }

    return loggers


def setup_logging(config: Optional[LoggingConfig] = None) -> None:
    """Setup logging configuration."""
    if config is None:
        config = get_default_config()

    logging_dict = get_logging_config_dict(config)
    logging.config.dictConfig(logging_dict)

    # Set up root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(config.root_level.value)

    # Log configuration setup
    logger = logging.getLogger(__name__)
    logger.info(
        "Logging configured",
        extra={
            'environment': config.environment,
            'log_level': config.default_level.value,
            'handlers': config.handlers,
            'structured_logging': config.structured_logging
        }
    )


def get_default_config() -> LoggingConfig:
    """Get default logging configuration."""
    environment = os.getenv('ENVIRONMENT', 'development')
    debug_mode = environment == 'development'

    return LoggingConfig(
        environment=environment,
        debug_mode=debug_mode,
        root_level=LogLevel.DEBUG if debug_mode else LogLevel.INFO,
        console_level=LogLevel.DEBUG if debug_mode else LogLevel.INFO,
        console_format=LogFormat.COLORED if debug_mode else LogFormat.JSON,
        structured_logging=True,
        performance_logging=True,
        security_logging=True,
        audit_logging=True,
        handlers=['console', 'file'],
        logger_levels={
            'uvicorn': LogLevel.INFO,
            'sqlalchemy.engine': LogLevel.WARNING,
            'fastapi': LogLevel.INFO
        }
    )


def configure_logging(
    level: LogLevel = LogLevel.INFO,
    format_type: LogFormat = LogFormat.JSON,
    console_enabled: bool = True,
    file_enabled: bool = True,
    log_directory: str = "logs",
    **kwargs
) -> None:
    """Simplified logging configuration."""

    handlers = []
    if console_enabled:
        handlers.append('console')
    if file_enabled:
        handlers.append('file')

    config = LoggingConfig(
        default_level=level,
        log_format=format_type,
        console_enabled=console_enabled,
        log_directory=log_directory,
        handlers=handlers,
        **kwargs
    )

    setup_logging(config)


# Auto-configure logging on import in development (disabled for testing)
# if os.getenv('ENVIRONMENT', 'development') == 'development':
#     configure_logging(
#         level=LogLevel.DEBUG,
#         format_type=LogFormat.COLORED,
#         console_enabled=True,
#         file_enabled=False
#     )
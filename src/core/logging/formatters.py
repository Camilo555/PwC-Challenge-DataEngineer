"""
Advanced Logging Formatters
==========================

Custom formatters for structured logging with enhanced features including
performance metrics, security redaction, and distributed tracing support.
"""

import json
import logging
import os
import socket
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4
import re


class JSONFormatter(logging.Formatter):
    """
    Advanced JSON formatter with enhanced structured logging capabilities.

    Features:
    - Comprehensive metadata (host, process, thread info)
    - Performance metrics integration
    - Distributed tracing support
    - Security-aware field redaction
    - Custom field extraction and formatting
    """

    def __init__(
        self,
        include_timestamp: bool = True,
        include_level: bool = True,
        include_logger_name: bool = True,
        include_process_info: bool = True,
        include_thread_info: bool = True,
        include_trace_info: bool = True,
        include_host_info: bool = True,
        redact_sensitive: bool = True,
        custom_fields: Optional[List[str]] = None
    ):
        super().__init__()
        self.include_timestamp = include_timestamp
        self.include_level = include_level
        self.include_logger_name = include_logger_name
        self.include_process_info = include_process_info
        self.include_thread_info = include_thread_info
        self.include_trace_info = include_trace_info
        self.include_host_info = include_host_info
        self.redact_sensitive = redact_sensitive
        self.custom_fields = custom_fields or []

        # Cache host information
        self.hostname = self._get_hostname()
        self.service_name = os.getenv('SERVICE_NAME', 'pwc-data-platform')
        self.version = os.getenv('APP_VERSION', '1.0.0')
        self.environment = os.getenv('ENVIRONMENT', 'unknown')

        # Sensitive data patterns for redaction
        self.sensitive_patterns = [
            (re.compile(r'"password"\s*:\s*"[^"]*"', re.IGNORECASE), '"password": "[REDACTED]"'),
            (re.compile(r'"token"\s*:\s*"[^"]*"', re.IGNORECASE), '"token": "[REDACTED]"'),
            (re.compile(r'"api_key"\s*:\s*"[^"]*"', re.IGNORECASE), '"api_key": "[REDACTED]"'),
            (re.compile(r'"secret"\s*:\s*"[^"]*"', re.IGNORECASE), '"secret": "[REDACTED]"'),
            (re.compile(r'"authorization"\s*:\s*"[^"]*"', re.IGNORECASE), '"authorization": "[REDACTED]"'),
            (re.compile(r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b'), '[CREDIT_CARD_REDACTED]'),
            (re.compile(r'\b\d{3}-\d{2}-\d{4}\b'), '[SSN_REDACTED]')
        ]

    def _get_hostname(self) -> str:
        """Get hostname with fallback."""
        try:
            return socket.gethostname()
        except Exception:
            return os.getenv('HOSTNAME', 'unknown')

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as comprehensive JSON structure."""
        # Start with core log entry structure
        log_entry: Dict[str, Any] = {}

        # Timestamp
        if self.include_timestamp:
            log_entry["@timestamp"] = datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat()

        # Version and metadata
        log_entry["@version"] = "2"
        log_entry["@metadata"] = {
            "beat": self.service_name,
            "version": self.version
        }

        # Log level and message
        if self.include_level:
            log_entry["level"] = record.levelname
            log_entry["level_num"] = record.levelno

        if self.include_logger_name:
            log_entry["logger"] = record.name
            log_entry["category"] = self._get_category(record.name)

        log_entry["message"] = record.getMessage()

        # Process and thread information
        if self.include_process_info:
            log_entry["process"] = {
                "pid": record.process,
                "name": record.processName or "unknown"
            }

        if self.include_thread_info:
            log_entry["thread"] = {
                "id": record.thread,
                "name": record.threadName or "unknown"
            }

        # Host and service information
        if self.include_host_info:
            log_entry["host"] = {
                "hostname": self.hostname,
                "os": sys.platform
            }
            log_entry["service"] = {
                "name": self.service_name,
                "version": self.version,
                "environment": self.environment
            }

        # Source code location
        if self.include_trace_info and record.pathname:
            log_entry["source"] = {
                "file": record.filename,
                "line": record.lineno,
                "function": record.funcName,
                "path": record.pathname,
                "module": record.module
            }

        # Exception information with enhanced details
        if record.exc_info:
            exc_type, exc_value, exc_traceback = record.exc_info
            log_entry["error"] = {
                "type": exc_type.__name__ if exc_type else "Unknown",
                "message": str(exc_value) if exc_value else "",
                "stack_trace": self.formatException(record.exc_info),
                "frames": self._extract_traceback_frames(exc_traceback) if exc_traceback else []
            }

        # Stack information
        if record.stack_info:
            log_entry["stack_info"] = record.stack_info

        # Performance metrics
        self._add_performance_metrics(record, log_entry)

        # Distributed tracing information
        self._add_tracing_context(record, log_entry)

        # Request/response context
        self._add_request_context(record, log_entry)

        # Custom fields from record
        self._add_custom_fields(record, log_entry)

        # Business context
        self._add_business_context(record, log_entry)

        # Serialize and optionally redact sensitive information
        json_output = json.dumps(log_entry, default=self._json_serializer)

        if self.redact_sensitive:
            json_output = self._redact_sensitive_data(json_output)

        return json_output

    def _get_category(self, logger_name: str) -> str:
        """Extract category from logger name."""
        if 'api' in logger_name:
            return 'api'
        elif 'etl' in logger_name:
            return 'etl'
        elif 'core' in logger_name:
            return 'core'
        elif 'monitoring' in logger_name:
            return 'monitoring'
        elif 'security' in logger_name:
            return 'security'
        else:
            return 'application'

    def _extract_traceback_frames(self, tb) -> List[Dict[str, Any]]:
        """Extract structured traceback frame information."""
        frames = []
        while tb:
            frame = {
                "filename": tb.tb_frame.f_code.co_filename,
                "function": tb.tb_frame.f_code.co_name,
                "line_number": tb.tb_lineno,
                "locals": self._safe_dict(tb.tb_frame.f_locals)
            }
            frames.append(frame)
            tb = tb.tb_next

        return frames

    def _safe_dict(self, d: Dict, max_depth: int = 2) -> Dict[str, Any]:
        """Safely convert dictionary with depth limit."""
        if max_depth <= 0:
            return {"<max_depth_reached>": True}

        safe = {}
        for k, v in d.items():
            try:
                if isinstance(v, (str, int, float, bool, type(None))):
                    safe[k] = v
                elif isinstance(v, dict) and max_depth > 1:
                    safe[k] = self._safe_dict(v, max_depth - 1)
                elif isinstance(v, (list, tuple)):
                    safe[k] = str(v)[:200]  # Truncate long lists
                else:
                    safe[k] = str(v)[:100]  # Truncate long strings
            except Exception:
                safe[k] = "<unserializable>"

        return safe

    def _add_performance_metrics(self, record: logging.LogRecord, log_entry: Dict[str, Any]):
        """Add performance metrics if available."""
        performance_data = {}

        # Duration from record
        if hasattr(record, 'duration_ms'):
            performance_data['duration_ms'] = record.duration_ms
            performance_data['duration_category'] = self._categorize_duration(record.duration_ms)

        # Memory usage
        if hasattr(record, 'memory_usage'):
            performance_data['memory_usage_mb'] = record.memory_usage

        # Database metrics
        if hasattr(record, 'db_query_time'):
            performance_data['db_query_time_ms'] = record.db_query_time

        # Request size
        if hasattr(record, 'request_size'):
            performance_data['request_size_bytes'] = record.request_size

        # Response size
        if hasattr(record, 'response_size'):
            performance_data['response_size_bytes'] = record.response_size

        if performance_data:
            log_entry['performance'] = performance_data

    def _categorize_duration(self, duration_ms: float) -> str:
        """Categorize duration for easier filtering."""
        if duration_ms < 100:
            return 'fast'
        elif duration_ms < 1000:
            return 'normal'
        elif duration_ms < 5000:
            return 'slow'
        else:
            return 'very_slow'

    def _add_tracing_context(self, record: logging.LogRecord, log_entry: Dict[str, Any]):
        """Add distributed tracing context."""
        tracing_data = {}

        # Trace and span IDs
        if hasattr(record, 'trace_id'):
            tracing_data['trace_id'] = record.trace_id

        if hasattr(record, 'span_id'):
            tracing_data['span_id'] = record.span_id

        if hasattr(record, 'parent_span_id'):
            tracing_data['parent_span_id'] = record.parent_span_id

        # Correlation ID
        if hasattr(record, 'correlation_id'):
            tracing_data['correlation_id'] = record.correlation_id

        # Request ID
        if hasattr(record, 'request_id'):
            tracing_data['request_id'] = record.request_id

        # Session ID
        if hasattr(record, 'session_id'):
            tracing_data['session_id'] = record.session_id

        if tracing_data:
            log_entry['tracing'] = tracing_data

    def _add_request_context(self, record: logging.LogRecord, log_entry: Dict[str, Any]):
        """Add HTTP request/response context."""
        request_data = {}

        # HTTP method and URL
        if hasattr(record, 'http_method'):
            request_data['method'] = record.http_method

        if hasattr(record, 'http_url'):
            request_data['url'] = record.http_url

        if hasattr(record, 'http_path'):
            request_data['path'] = record.http_path

        # Status code
        if hasattr(record, 'http_status_code'):
            request_data['status_code'] = record.http_status_code
            request_data['status_category'] = self._categorize_status_code(record.http_status_code)

        # User agent
        if hasattr(record, 'user_agent'):
            request_data['user_agent'] = record.user_agent

        # IP address
        if hasattr(record, 'client_ip'):
            request_data['client_ip'] = record.client_ip

        # Headers (selected)
        if hasattr(record, 'http_headers'):
            request_data['headers'] = self._filter_headers(record.http_headers)

        if request_data:
            log_entry['http'] = request_data

    def _categorize_status_code(self, status_code: int) -> str:
        """Categorize HTTP status code."""
        if 200 <= status_code < 300:
            return 'success'
        elif 300 <= status_code < 400:
            return 'redirect'
        elif 400 <= status_code < 500:
            return 'client_error'
        elif 500 <= status_code < 600:
            return 'server_error'
        else:
            return 'unknown'

    def _filter_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Filter headers to exclude sensitive information."""
        safe_headers = {}
        allowed_headers = {
            'content-type', 'content-length', 'user-agent', 'accept',
            'accept-language', 'accept-encoding', 'connection', 'host',
            'x-forwarded-for', 'x-real-ip', 'x-correlation-id'
        }

        for key, value in headers.items():
            if key.lower() in allowed_headers:
                safe_headers[key] = value

        return safe_headers

    def _add_custom_fields(self, record: logging.LogRecord, log_entry: Dict[str, Any]):
        """Add custom fields from record."""
        # Standard fields to exclude
        excluded_fields = {
            'name', 'msg', 'args', 'created', 'filename', 'funcName',
            'levelname', 'levelno', 'lineno', 'module', 'msecs', 'pathname',
            'process', 'processName', 'relativeCreated', 'thread', 'threadName',
            'exc_info', 'exc_text', 'stack_info', 'getMessage', 'message'
        }

        # Add custom fields
        for field in self.custom_fields:
            if hasattr(record, field):
                log_entry[field] = getattr(record, field)

        # Add all extra fields from record
        for key, value in record.__dict__.items():
            if key not in excluded_fields and not key.startswith('_'):
                try:
                    # Attempt to serialize the value
                    json.dumps(value, default=self._json_serializer)
                    log_entry[key] = value
                except (TypeError, ValueError):
                    log_entry[key] = str(value)[:500]  # Truncate long values

    def _add_business_context(self, record: logging.LogRecord, log_entry: Dict[str, Any]):
        """Add business context information."""
        business_data = {}

        # User information
        if hasattr(record, 'user_id'):
            business_data['user_id'] = record.user_id

        if hasattr(record, 'username'):
            business_data['username'] = record.username

        if hasattr(record, 'organization_id'):
            business_data['organization_id'] = record.organization_id

        # Entity information
        if hasattr(record, 'entity_id'):
            business_data['entity_id'] = record.entity_id

        if hasattr(record, 'entity_type'):
            business_data['entity_type'] = record.entity_type

        # Transaction information
        if hasattr(record, 'transaction_id'):
            business_data['transaction_id'] = record.transaction_id

        if hasattr(record, 'operation'):
            business_data['operation'] = record.operation

        if business_data:
            log_entry['business'] = business_data

    def _redact_sensitive_data(self, json_output: str) -> str:
        """Redact sensitive information from JSON output."""
        for pattern, replacement in self.sensitive_patterns:
            json_output = pattern.sub(replacement, json_output)

        return json_output

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


class ColoredFormatter(logging.Formatter):
    """
    Colored console formatter with enhanced visual formatting.
    """

    # Color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
    }

    # Style codes
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    RESET = '\033[0m'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.colored = sys.stdout.isatty()  # Only use colors in terminals

    def format(self, record: logging.LogRecord) -> str:
        """Format record with colors and enhanced information."""
        if not self.colored:
            return super().format(record)

        # Make a copy to avoid modifying the original
        record_copy = logging.makeLogRecord(record.__dict__)

        # Add colors to level name
        level_color = self.COLORS.get(record.levelname, '')
        record_copy.levelname = f"{level_color}{self.BOLD}{record.levelname:<8}{self.RESET}"

        # Add colors to logger name
        record_copy.name = f"\033[94m{record.name}{self.RESET}"

        # Format message with performance indicators
        message = record.getMessage()
        if hasattr(record, 'duration_ms') and record.duration_ms > 1000:
            message = f"{message} {self.COLORS['WARNING']}[SLOW: {record.duration_ms:.0f}ms]{self.RESET}"

        record_copy.msg = message
        record_copy.args = ()

        # Format with base formatter
        formatted = super().format(record_copy)

        # Add extra context information
        extras = []
        if hasattr(record, 'correlation_id'):
            extras.append(f"correlation_id={record.correlation_id}")

        if hasattr(record, 'user_id'):
            extras.append(f"user_id={record.user_id}")

        if extras:
            formatted += f" {self.COLORS['DEBUG']}[{', '.join(extras)}]{self.RESET}"

        return formatted


class StructuredFormatter(logging.Formatter):
    """
    Human-readable structured formatter for development.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format record with structured information."""
        lines = [super().format(record)]

        # Add structured information
        if hasattr(record, 'duration_ms'):
            lines.append(f"  ⏱️  Duration: {record.duration_ms:.2f}ms")

        if hasattr(record, 'http_status_code'):
            lines.append(f"  🌐 HTTP: {record.http_method} {record.http_path} → {record.http_status_code}")

        if hasattr(record, 'user_id'):
            lines.append(f"  👤 User: {record.user_id}")

        if hasattr(record, 'correlation_id'):
            lines.append(f"  🔗 Correlation: {record.correlation_id}")

        if hasattr(record, 'memory_usage'):
            lines.append(f"  💾 Memory: {record.memory_usage:.1f}MB")

        # Add exception details
        if record.exc_info:
            lines.extend(['  ❌ Exception:'] + ['    ' + line for line in self.formatException(record.exc_info).split('\n')])

        return '\n'.join(lines)
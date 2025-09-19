"""
Advanced Logging Filters
=======================

Sophisticated logging filters for performance monitoring, security,
compliance, and intelligent log processing.
"""

import logging
import os
import re
import time
from collections import defaultdict, deque
from threading import Lock
from typing import Any, Dict, List, Optional, Set, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class FilterConfig:
    """Base configuration for logging filters."""
    enabled: bool = True
    log_filter_decisions: bool = False


class PerformanceFilter(logging.Filter):
    """
    Performance monitoring filter that tracks timing metrics
    and identifies slow operations.
    """

    def __init__(
        self,
        slow_threshold: float = 1.0,
        very_slow_threshold: float = 5.0,
        track_memory: bool = True,
        track_cpu: bool = False,
        sample_rate: float = 1.0
    ):
        super().__init__()
        self.slow_threshold = slow_threshold * 1000  # Convert to ms
        self.very_slow_threshold = very_slow_threshold * 1000
        self.track_memory = track_memory
        self.track_cpu = track_cpu
        self.sample_rate = sample_rate

        # Performance tracking
        self.request_times: Dict[str, float] = {}
        self.lock = Lock()

        # Statistics
        self.stats = defaultdict(list)
        self.slow_operations = deque(maxlen=100)

        # Import optional dependencies
        self.psutil = None
        if track_memory or track_cpu:
            try:
                import psutil
                self.psutil = psutil
                self.process = psutil.Process()
            except ImportError:
                self.psutil = None

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Add performance metrics to log record.
        """
        # Sample rate check
        if self.sample_rate < 1.0:
            import random
            if random.random() > self.sample_rate:
                return True

        current_time = time.time()

        # Add timestamp
        record.timestamp_ms = int(current_time * 1000)

        # Track memory usage
        if self.track_memory and self.psutil:
            try:
                memory_info = self.process.memory_info()
                record.memory_usage = memory_info.rss / 1024 / 1024  # MB
                record.memory_percent = self.process.memory_percent()
            except Exception:
                pass

        # Track CPU usage
        if self.track_cpu and self.psutil:
            try:
                record.cpu_percent = self.process.cpu_percent()
            except Exception:
                pass

        # Request timing tracking
        self._track_request_timing(record, current_time)

        # Add performance categorization
        self._categorize_performance(record)

        return True

    def _track_request_timing(self, record: logging.LogRecord, current_time: float):
        """Track request start/end times."""
        request_id = getattr(record, 'request_id', None)
        if not request_id:
            return

        with self.lock:
            if hasattr(record, 'request_start') and record.request_start:
                # Start timing
                self.request_times[request_id] = current_time
                record.request_phase = 'start'
            elif request_id in self.request_times:
                # End timing
                start_time = self.request_times.pop(request_id)
                duration = (current_time - start_time) * 1000  # ms

                record.request_duration_ms = duration
                record.request_phase = 'complete'

                # Track statistics
                self.stats[record.name].append(duration)

                # Track slow operations
                if duration > self.slow_threshold:
                    slow_op = {
                        'timestamp': datetime.now(),
                        'logger': record.name,
                        'function': getattr(record, 'funcName', 'unknown'),
                        'duration_ms': duration,
                        'message': record.getMessage()
                    }
                    self.slow_operations.append(slow_op)

    def _categorize_performance(self, record: logging.LogRecord):
        """Categorize performance level."""
        duration = getattr(record, 'request_duration_ms', None)
        if duration is None:
            return

        if duration > self.very_slow_threshold:
            record.performance_category = 'very_slow'
            record.performance_alert = True
        elif duration > self.slow_threshold:
            record.performance_category = 'slow'
            record.performance_warning = True
        elif duration < 100:
            record.performance_category = 'fast'
        else:
            record.performance_category = 'normal'

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        with self.lock:
            stats = {}
            for logger_name, durations in self.stats.items():
                if durations:
                    stats[logger_name] = {
                        'count': len(durations),
                        'avg_duration_ms': sum(durations) / len(durations),
                        'min_duration_ms': min(durations),
                        'max_duration_ms': max(durations),
                        'slow_requests': len([d for d in durations if d > self.slow_threshold])
                    }

            return {
                'logger_stats': stats,
                'recent_slow_operations': list(self.slow_operations),
                'total_requests': sum(len(durations) for durations in self.stats.values())
            }


class SecurityFilter(logging.Filter):
    """
    Security-focused filter that redacts sensitive information,
    detects security events, and maintains audit trails.
    """

    def __init__(
        self,
        redact_patterns: Optional[List[tuple]] = None,
        security_keywords: Optional[Set[str]] = None,
        compliance_mode: bool = False
    ):
        super().__init__()
        self.compliance_mode = compliance_mode

        # Default sensitive patterns (pattern, replacement, category)
        self.redact_patterns = redact_patterns or [
            (re.compile(r'password["\s]*[=:]["\s]*[^"\\s,}]+', re.IGNORECASE),
             'password="[REDACTED]"', 'password'),
            (re.compile(r'token["\s]*[=:]["\s]*[^"\\s,}]+', re.IGNORECASE),
             'token="[REDACTED]"', 'token'),
            (re.compile(r'api[_-]?key["\s]*[=:]["\s]*[^"\\s,}]+', re.IGNORECASE),
             'api_key="[REDACTED]"', 'api_key'),
            (re.compile(r'secret["\s]*[=:]["\s]*[^"\\s,}]+', re.IGNORECASE),
             'secret="[REDACTED]"', 'secret'),
            (re.compile(r'authorization["\s]*[=:]["\s]*[^"\\s,}]+', re.IGNORECASE),
             'authorization="[REDACTED]"', 'authorization'),
            # Credit card numbers
            (re.compile(r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b'),
             '[CREDIT_CARD_REDACTED]', 'credit_card'),
            # Social Security Numbers
            (re.compile(r'\b\d{3}-\d{2}-\d{4}\b'),
             '[SSN_REDACTED]', 'ssn'),
            # Email addresses (partial redaction)
            (re.compile(r'\b([a-zA-Z0-9._%+-]+)@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b'),
             r'\1***@\2', 'email'),
            # IP addresses (partial redaction)
            (re.compile(r'\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.)\d{1,3}\b'),
             r'\1***', 'ip_address')
        ]

        # Security event keywords
        self.security_keywords = security_keywords or {
            'authentication', 'authorization', 'login', 'logout', 'access_denied',
            'permission', 'unauthorized', 'forbidden', 'security', 'audit',
            'breach', 'attack', 'intrusion', 'vulnerability', 'exploit',
            'malicious', 'suspicious', 'fraud', 'compliance', 'privacy'
        }

        # Security event tracking
        self.security_events = deque(maxlen=1000)
        self.failed_attempts = defaultdict(list)
        self.lock = Lock()

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Apply security filtering and event detection.
        """
        original_message = record.getMessage()

        # Redact sensitive information
        redacted_message = self._redact_sensitive_data(original_message)

        # Detect security events
        security_event = self._detect_security_event(record, original_message)

        # Update record
        record.msg = redacted_message
        record.args = ()

        # Add security metadata
        if security_event:
            record.security_event = security_event
            record.security_level = self._get_security_level(security_event)

            # Track security events
            with self.lock:
                self.security_events.append({
                    'timestamp': datetime.now(),
                    'logger': record.name,
                    'level': record.levelname,
                    'event_type': security_event,
                    'message': redacted_message,
                    'source_ip': getattr(record, 'client_ip', 'unknown'),
                    'user_id': getattr(record, 'user_id', 'unknown')
                })

        # Add compliance metadata
        if self.compliance_mode:
            record.compliance_required = True
            record.data_classification = self._classify_data(original_message)
            record.retention_required = True

        return True

    def _redact_sensitive_data(self, message: str) -> str:
        """Redact sensitive information from message."""
        redacted = message
        redaction_count = 0

        for pattern, replacement, category in self.redact_patterns:
            new_message = pattern.sub(replacement, redacted)
            if new_message != redacted:
                redaction_count += 1
                redacted = new_message

        return redacted

    def _detect_security_event(self, record: logging.LogRecord, message: str) -> Optional[str]:
        """Detect security-related events."""
        message_lower = message.lower()

        # Check for security keywords
        for keyword in self.security_keywords:
            if keyword in message_lower:
                # Classify the security event type
                if any(word in message_lower for word in ['login', 'authentication', 'auth']):
                    if any(word in message_lower for word in ['failed', 'denied', 'invalid']):
                        return 'authentication_failure'
                    else:
                        return 'authentication_success'
                elif any(word in message_lower for word in ['access', 'permission', 'authorization']):
                    if any(word in message_lower for word in ['denied', 'forbidden', 'unauthorized']):
                        return 'authorization_failure'
                    else:
                        return 'authorization_success'
                elif any(word in message_lower for word in ['attack', 'intrusion', 'breach']):
                    return 'security_incident'
                elif any(word in message_lower for word in ['suspicious', 'anomaly']):
                    return 'suspicious_activity'
                else:
                    return 'security_related'

        # Check log level for potential security events
        if record.levelname in ['ERROR', 'CRITICAL']:
            if any(word in message_lower for word in ['connection refused', 'timeout', 'network error']):
                return 'infrastructure_security'

        return None

    def _get_security_level(self, event_type: str) -> str:
        """Get security level for event type."""
        high_severity = {'security_incident', 'authentication_failure', 'authorization_failure'}
        medium_severity = {'suspicious_activity', 'infrastructure_security'}

        if event_type in high_severity:
            return 'high'
        elif event_type in medium_severity:
            return 'medium'
        else:
            return 'low'

    def _classify_data(self, message: str) -> str:
        """Classify data sensitivity level."""
        if any(pattern[0].search(message) for pattern in self.redact_patterns):
            return 'sensitive'
        elif any(keyword in message.lower() for keyword in ['user', 'customer', 'account']):
            return 'personal'
        else:
            return 'public'

    def get_security_summary(self) -> Dict[str, Any]:
        """Get security event summary."""
        with self.lock:
            event_counts = defaultdict(int)
            for event in self.security_events:
                event_counts[event['event_type']] += 1

            return {
                'total_security_events': len(self.security_events),
                'event_type_counts': dict(event_counts),
                'recent_events': list(self.security_events)[-10:],  # Last 10 events
                'high_severity_count': len([e for e in self.security_events
                                          if self._get_security_level(e['event_type']) == 'high'])
            }


class EnvironmentFilter(logging.Filter):
    """
    Environment-aware filter that adds context based on deployment environment.
    """

    def __init__(self, environment: str):
        super().__init__()
        self.environment = environment.lower()
        self.is_production = self.environment == 'production'
        self.is_development = self.environment == 'development'

    def filter(self, record: logging.LogRecord) -> bool:
        """Add environment context to log record."""
        record.environment = self.environment
        record.is_production = self.is_production
        record.is_development = self.is_development

        # Environment-specific filtering
        if self.is_production:
            # In production, filter out debug-level traces
            if record.levelname == 'DEBUG' and 'trace' in record.getMessage().lower():
                return False

            # Add production-specific metadata
            record.production_ready = True
            record.log_retention_days = 90

        elif self.is_development:
            # In development, add extra debugging information
            record.debug_mode = True
            record.full_stack_trace = True

        return True


class SamplingFilter(logging.Filter):
    """
    Intelligent sampling filter that reduces log volume while preserving
    important events and maintaining statistical representativeness.
    """

    def __init__(
        self,
        default_sample_rate: float = 1.0,
        level_sample_rates: Optional[Dict[str, float]] = None,
        burst_protection: bool = True,
        preserve_errors: bool = True
    ):
        super().__init__()
        self.default_sample_rate = default_sample_rate
        self.level_sample_rates = level_sample_rates or {}
        self.burst_protection = burst_protection
        self.preserve_errors = preserve_errors

        # Burst protection
        self.message_counts = defaultdict(int)
        self.last_reset = time.time()
        self.reset_interval = 60  # Reset counts every minute
        self.burst_threshold = 100  # Max messages per logger per minute

        self.lock = Lock()

    def filter(self, record: logging.LogRecord) -> bool:
        """Apply intelligent sampling."""
        # Always preserve errors and critical logs
        if self.preserve_errors and record.levelname in ['ERROR', 'CRITICAL']:
            return True

        # Get sample rate for this level
        sample_rate = self.level_sample_rates.get(record.levelname, self.default_sample_rate)

        # Apply burst protection
        if self.burst_protection:
            with self.lock:
                current_time = time.time()

                # Reset counters periodically
                if current_time - self.last_reset > self.reset_interval:
                    self.message_counts.clear()
                    self.last_reset = current_time

                # Check burst threshold
                logger_count = self.message_counts[record.name]
                if logger_count > self.burst_threshold:
                    sample_rate = min(sample_rate, 0.1)  # Reduce to 10% during burst

                self.message_counts[record.name] += 1

        # Apply sampling
        if sample_rate >= 1.0:
            return True
        elif sample_rate <= 0.0:
            return False
        else:
            import random
            return random.random() < sample_rate


class CorrelationFilter(logging.Filter):
    """
    Filter that maintains correlation IDs and request context across
    distributed operations and async boundaries.
    """

    def __init__(self):
        super().__init__()
        self.context_storage = {}  # Thread-local would be better
        self.lock = Lock()

    def filter(self, record: logging.LogRecord) -> bool:
        """Add correlation context to log record."""
        # Try to get correlation ID from various sources
        correlation_id = self._get_correlation_id(record)

        if correlation_id:
            record.correlation_id = correlation_id

            # Store additional context if available
            with self.lock:
                if correlation_id in self.context_storage:
                    context = self.context_storage[correlation_id]
                    for key, value in context.items():
                        if not hasattr(record, key):
                            setattr(record, key, value)

        return True

    def _get_correlation_id(self, record: logging.LogRecord) -> Optional[str]:
        """Extract correlation ID from various sources."""
        # Check if already set
        if hasattr(record, 'correlation_id'):
            return record.correlation_id

        # Check request context
        if hasattr(record, 'request_id'):
            return record.request_id

        # Check trace context
        if hasattr(record, 'trace_id'):
            return record.trace_id

        # Check environment variables (for some frameworks)
        return os.getenv('CORRELATION_ID')

    def set_correlation_context(self, correlation_id: str, context: Dict[str, Any]):
        """Set context for a correlation ID."""
        with self.lock:
            self.context_storage[correlation_id] = context

    def clear_correlation_context(self, correlation_id: str):
        """Clear context for a correlation ID."""
        with self.lock:
            self.context_storage.pop(correlation_id, None)


class BusinessMetricsFilter(logging.Filter):
    """
    Filter that extracts business metrics and KPIs from log messages
    for real-time business intelligence and monitoring.
    """

    def __init__(self):
        super().__init__()
        self.metrics = defaultdict(list)
        self.business_events = deque(maxlen=1000)
        self.lock = Lock()

    def filter(self, record: logging.LogRecord) -> bool:
        """Extract business metrics from log record."""
        message = record.getMessage()

        # Extract business metrics
        metrics = self._extract_metrics(record, message)

        # Track business events
        business_event = self._identify_business_event(record, message)

        if metrics:
            record.business_metrics = metrics

            with self.lock:
                for metric_name, value in metrics.items():
                    self.metrics[metric_name].append({
                        'timestamp': datetime.now(),
                        'value': value,
                        'logger': record.name
                    })

        if business_event:
            record.business_event = business_event

            with self.lock:
                self.business_events.append({
                    'timestamp': datetime.now(),
                    'event_type': business_event['type'],
                    'value': business_event.get('value'),
                    'logger': record.name,
                    'message': message
                })

        return True

    def _extract_metrics(self, record: logging.LogRecord, message: str) -> Dict[str, Any]:
        """Extract numerical metrics from log message."""
        metrics = {}

        # Common patterns for business metrics
        patterns = {
            'revenue': re.compile(r'revenue[:\s]+\$?([\d,]+\.?\d*)', re.IGNORECASE),
            'transaction_count': re.compile(r'processed\s+(\d+)\s+transactions?', re.IGNORECASE),
            'user_count': re.compile(r'(\d+)\s+users?', re.IGNORECASE),
            'error_rate': re.compile(r'error\s+rate[:\s]+([\d.]+)%?', re.IGNORECASE),
            'response_time': re.compile(r'response\s+time[:\s]+([\d.]+)\s*ms', re.IGNORECASE),
        }

        for metric_name, pattern in patterns.items():
            match = pattern.search(message)
            if match:
                try:
                    value = float(match.group(1).replace(',', ''))
                    metrics[metric_name] = value
                except ValueError:
                    pass

        return metrics

    def _identify_business_event(self, record: logging.LogRecord, message: str) -> Optional[Dict[str, Any]]:
        """Identify business events from log message."""
        message_lower = message.lower()

        # Business event patterns
        if 'user registered' in message_lower:
            return {'type': 'user_registration'}
        elif 'payment processed' in message_lower:
            return {'type': 'payment_processed'}
        elif 'order completed' in message_lower:
            return {'type': 'order_completed'}
        elif 'subscription' in message_lower and 'created' in message_lower:
            return {'type': 'subscription_created'}
        elif 'login' in message_lower and 'successful' in message_lower:
            return {'type': 'user_login'}

        return None

    def get_business_metrics_summary(self) -> Dict[str, Any]:
        """Get business metrics summary."""
        with self.lock:
            summary = {}

            for metric_name, values in self.metrics.items():
                if values:
                    numeric_values = [v['value'] for v in values]
                    summary[metric_name] = {
                        'count': len(numeric_values),
                        'sum': sum(numeric_values),
                        'avg': sum(numeric_values) / len(numeric_values),
                        'min': min(numeric_values),
                        'max': max(numeric_values)
                    }

            event_counts = defaultdict(int)
            for event in self.business_events:
                event_counts[event['event_type']] += 1

            summary['business_events'] = dict(event_counts)

            return summary
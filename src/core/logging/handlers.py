"""
Advanced Logging Handlers
========================

Specialized logging handlers for enterprise features including database logging,
security audit trails, metrics collection, and external system integrations.
"""

import asyncio
import json
import logging
import os
import queue
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Dict, List, Optional, Callable
from urllib.parse import urlparse
import sqlite3
from contextlib import contextmanager


class DatabaseLogHandler(logging.Handler):
    """
    Handler that writes log records to a database for structured querying,
    analytics, and long-term storage.
    """

    def __init__(
        self,
        database_url: Optional[str] = None,
        table_name: str = 'log_records',
        max_batch_size: int = 100,
        flush_interval: float = 5.0,
        create_table: bool = True
    ):
        super().__init__()
        self.database_url = database_url or os.getenv('LOG_DATABASE_URL', 'sqlite:///logs/application.db')
        self.table_name = table_name
        self.max_batch_size = max_batch_size
        self.flush_interval = flush_interval

        # Batch processing
        self.record_queue = queue.Queue()
        self.flush_thread = None
        self.shutdown_event = threading.Event()
        self.lock = Lock()

        # Initialize database
        if create_table:
            self._create_table()

        # Start flush thread
        self._start_flush_thread()

    def _create_table(self):
        """Create log table if it doesn't exist."""
        # For simplicity, using SQLite. In production, this would support multiple databases
        if self.database_url.startswith('sqlite:'):
            db_path = self.database_url.replace('sqlite:///', '').replace('sqlite://', '')
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

            with sqlite3.connect(db_path) as conn:
                conn.execute(f'''
                    CREATE TABLE IF NOT EXISTS {self.table_name} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        level TEXT NOT NULL,
                        logger TEXT NOT NULL,
                        message TEXT NOT NULL,
                        module TEXT,
                        function TEXT,
                        line_number INTEGER,
                        thread_id INTEGER,
                        process_id INTEGER,
                        correlation_id TEXT,
                        user_id TEXT,
                        session_id TEXT,
                        request_id TEXT,
                        duration_ms REAL,
                        status_code INTEGER,
                        error_type TEXT,
                        error_message TEXT,
                        stack_trace TEXT,
                        extra_data TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        INDEX(timestamp),
                        INDEX(level),
                        INDEX(logger),
                        INDEX(correlation_id),
                        INDEX(user_id)
                    )
                ''')

    def _start_flush_thread(self):
        """Start background thread for batch processing."""
        self.flush_thread = Thread(target=self._flush_worker, daemon=True)
        self.flush_thread.start()

    def _flush_worker(self):
        """Background worker that flushes log records to database."""
        records_buffer = []
        last_flush = time.time()

        while not self.shutdown_event.is_set():
            try:
                # Get records from queue with timeout
                try:
                    record = self.record_queue.get(timeout=0.1)
                    records_buffer.append(record)
                except queue.Empty:
                    pass

                # Flush conditions
                current_time = time.time()
                should_flush = (
                    len(records_buffer) >= self.max_batch_size or
                    (records_buffer and (current_time - last_flush) >= self.flush_interval)
                )

                if should_flush and records_buffer:
                    self._flush_records(records_buffer)
                    records_buffer.clear()
                    last_flush = current_time

            except Exception as e:
                # Log error but don't let it crash the worker
                print(f"Error in database log flush worker: {e}")

        # Final flush on shutdown
        if records_buffer:
            self._flush_records(records_buffer)

    def _flush_records(self, records: List[Dict[str, Any]]):
        """Flush batch of records to database."""
        if not records:
            return

        try:
            if self.database_url.startswith('sqlite:'):
                self._flush_to_sqlite(records)
            else:
                # Could extend to support PostgreSQL, MySQL, etc.
                raise NotImplementedError(f"Database type not supported: {self.database_url}")

        except Exception as e:
            print(f"Error flushing records to database: {e}")

    def _flush_to_sqlite(self, records: List[Dict[str, Any]]):
        """Flush records to SQLite database."""
        db_path = self.database_url.replace('sqlite:///', '').replace('sqlite://', '')

        with sqlite3.connect(db_path) as conn:
            insert_sql = f'''
                INSERT INTO {self.table_name} (
                    timestamp, level, logger, message, module, function, line_number,
                    thread_id, process_id, correlation_id, user_id, session_id, request_id,
                    duration_ms, status_code, error_type, error_message, stack_trace, extra_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''

            batch_data = []
            for record in records:
                batch_data.append((
                    record.get('timestamp'),
                    record.get('level'),
                    record.get('logger'),
                    record.get('message'),
                    record.get('module'),
                    record.get('function'),
                    record.get('line_number'),
                    record.get('thread_id'),
                    record.get('process_id'),
                    record.get('correlation_id'),
                    record.get('user_id'),
                    record.get('session_id'),
                    record.get('request_id'),
                    record.get('duration_ms'),
                    record.get('status_code'),
                    record.get('error_type'),
                    record.get('error_message'),
                    record.get('stack_trace'),
                    json.dumps(record.get('extra_data', {}))
                ))

            conn.executemany(insert_sql, batch_data)

    def emit(self, record: logging.LogRecord):
        """Queue log record for database insertion."""
        try:
            # Convert log record to dictionary
            log_data = {
                'timestamp': datetime.fromtimestamp(record.created),
                'level': record.levelname,
                'logger': record.name,
                'message': record.getMessage(),
                'module': record.module,
                'function': record.funcName,
                'line_number': record.lineno,
                'thread_id': record.thread,
                'process_id': record.process,
                'correlation_id': getattr(record, 'correlation_id', None),
                'user_id': getattr(record, 'user_id', None),
                'session_id': getattr(record, 'session_id', None),
                'request_id': getattr(record, 'request_id', None),
                'duration_ms': getattr(record, 'duration_ms', None),
                'status_code': getattr(record, 'status_code', None),
            }

            # Add exception information
            if record.exc_info:
                log_data['error_type'] = record.exc_info[0].__name__
                log_data['error_message'] = str(record.exc_info[1])
                log_data['stack_trace'] = self.formatException(record.exc_info)

            # Add extra data
            extra_data = {}
            excluded_attrs = {
                'name', 'msg', 'args', 'created', 'filename', 'funcName',
                'levelname', 'levelno', 'lineno', 'module', 'msecs', 'pathname',
                'process', 'processName', 'relativeCreated', 'thread', 'threadName',
                'exc_info', 'exc_text', 'stack_info', 'getMessage', 'message'
            }

            for key, value in record.__dict__.items():
                if key not in excluded_attrs and not key.startswith('_'):
                    extra_data[key] = value

            log_data['extra_data'] = extra_data

            # Queue for batch processing
            self.record_queue.put(log_data)

        except Exception:
            self.handleError(record)

    def close(self):
        """Close handler and flush remaining records."""
        if self.flush_thread and self.flush_thread.is_alive():
            self.shutdown_event.set()
            self.flush_thread.join(timeout=5.0)
        super().close()


class SecurityLogHandler(logging.Handler):
    """
    Specialized handler for security events with enhanced audit trails,
    compliance features, and security-specific formatting.
    """

    def __init__(
        self,
        security_log_path: str = 'logs/security.log',
        audit_log_path: str = 'logs/audit.log',
        max_file_size: int = 100 * 1024 * 1024,  # 100MB
        backup_count: int = 10,
        encrypt_logs: bool = False,
        compliance_mode: bool = False
    ):
        super().__init__()
        self.security_log_path = Path(security_log_path)
        self.audit_log_path = Path(audit_log_path)
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        self.encrypt_logs = encrypt_logs
        self.compliance_mode = compliance_mode

        # Ensure log directories exist
        self.security_log_path.parent.mkdir(parents=True, exist_ok=True)
        self.audit_log_path.parent.mkdir(parents=True, exist_ok=True)

        # Security event counters
        self.security_event_counts = defaultdict(int)
        self.failed_login_attempts = defaultdict(list)
        self.lock = Lock()

    def emit(self, record: logging.LogRecord):
        """Handle security log record with enhanced processing."""
        try:
            # Classify security event
            security_classification = self._classify_security_event(record)

            # Create enhanced security log entry
            security_entry = self._create_security_entry(record, security_classification)

            # Write to appropriate log file
            if security_classification['type'] in ['authentication', 'authorization', 'security_incident']:
                self._write_security_log(security_entry)

            if self.compliance_mode or security_classification['audit_required']:
                self._write_audit_log(security_entry)

            # Track security metrics
            self._track_security_metrics(record, security_classification)

        except Exception:
            self.handleError(record)

    def _classify_security_event(self, record: logging.LogRecord) -> Dict[str, Any]:
        """Classify the security event type and severity."""
        message = record.getMessage().lower()
        classification = {
            'type': 'general',
            'severity': 'low',
            'audit_required': False,
            'alert_required': False
        }

        # Authentication events
        if any(word in message for word in ['login', 'authentication', 'auth']):
            classification['type'] = 'authentication'
            if any(word in message for word in ['failed', 'denied', 'invalid']):
                classification['severity'] = 'medium'
                classification['audit_required'] = True
            else:
                classification['severity'] = 'low'

        # Authorization events
        elif any(word in message for word in ['access', 'permission', 'authorization']):
            classification['type'] = 'authorization'
            if any(word in message for word in ['denied', 'forbidden', 'unauthorized']):
                classification['severity'] = 'medium'
                classification['audit_required'] = True

        # Security incidents
        elif any(word in message for word in ['attack', 'intrusion', 'breach', 'vulnerability']):
            classification['type'] = 'security_incident'
            classification['severity'] = 'high'
            classification['audit_required'] = True
            classification['alert_required'] = True

        # Data access events
        elif any(word in message for word in ['data access', 'database', 'query']):
            classification['type'] = 'data_access'
            classification['audit_required'] = self.compliance_mode

        return classification

    def _create_security_entry(self, record: logging.LogRecord, classification: Dict[str, Any]) -> Dict[str, Any]:
        """Create comprehensive security log entry."""
        entry = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'event_id': f"SEC_{int(time.time() * 1000)}_{record.thread}",
            'severity': classification['severity'],
            'event_type': classification['type'],
            'logger': record.name,
            'level': record.levelname,
            'message': record.getMessage(),
            'source': {
                'file': record.filename,
                'function': record.funcName,
                'line': record.lineno
            },
            'process': {
                'pid': record.process,
                'name': record.processName
            },
            'thread': {
                'id': record.thread,
                'name': record.threadName
            }
        }

        # Add security-specific context
        security_context = {}
        for attr in ['user_id', 'session_id', 'client_ip', 'user_agent', 'correlation_id']:
            if hasattr(record, attr):
                security_context[attr] = getattr(record, attr)

        if security_context:
            entry['security_context'] = security_context

        # Add compliance information
        if self.compliance_mode:
            entry['compliance'] = {
                'retention_required': True,
                'data_classification': getattr(record, 'data_classification', 'unclassified'),
                'regulation_type': os.getenv('COMPLIANCE_REGULATION', 'general')
            }

        # Add exception details if present
        if record.exc_info:
            entry['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'stack_trace': self.formatException(record.exc_info)
            }

        return entry

    def _write_security_log(self, entry: Dict[str, Any]):
        """Write entry to security log file."""
        log_line = json.dumps(entry) + '\n'

        # Rotate log if necessary
        if self.security_log_path.exists() and self.security_log_path.stat().st_size > self.max_file_size:
            self._rotate_log_file(self.security_log_path)

        # Write to file
        with open(self.security_log_path, 'a', encoding='utf-8') as f:
            f.write(log_line)

    def _write_audit_log(self, entry: Dict[str, Any]):
        """Write entry to audit log file."""
        # Add audit-specific metadata
        audit_entry = entry.copy()
        audit_entry['audit_trail_id'] = f"AUDIT_{int(time.time() * 1000)}"
        audit_entry['audit_timestamp'] = datetime.utcnow().isoformat()

        log_line = json.dumps(audit_entry) + '\n'

        # Rotate log if necessary
        if self.audit_log_path.exists() and self.audit_log_path.stat().st_size > self.max_file_size:
            self._rotate_log_file(self.audit_log_path)

        # Write to file
        with open(self.audit_log_path, 'a', encoding='utf-8') as f:
            f.write(log_line)

    def _rotate_log_file(self, log_path: Path):
        """Rotate log file when it exceeds max size."""
        for i in range(self.backup_count - 1, 0, -1):
            old_file = log_path.with_suffix(f'{log_path.suffix}.{i}')
            new_file = log_path.with_suffix(f'{log_path.suffix}.{i + 1}')
            if old_file.exists():
                old_file.rename(new_file)

        # Move current log to .1
        if log_path.exists():
            log_path.rename(log_path.with_suffix(f'{log_path.suffix}.1'))

    def _track_security_metrics(self, record: logging.LogRecord, classification: Dict[str, Any]):
        """Track security metrics for monitoring."""
        with self.lock:
            self.security_event_counts[classification['type']] += 1

            # Track failed login attempts
            if classification['type'] == 'authentication' and 'failed' in record.getMessage().lower():
                user_id = getattr(record, 'user_id', 'unknown')
                client_ip = getattr(record, 'client_ip', 'unknown')

                self.failed_login_attempts[user_id].append({
                    'timestamp': datetime.fromtimestamp(record.created),
                    'ip': client_ip,
                    'message': record.getMessage()
                })

                # Keep only recent attempts (last hour)
                cutoff_time = datetime.fromtimestamp(record.created) - timedelta(hours=1)
                self.failed_login_attempts[user_id] = [
                    attempt for attempt in self.failed_login_attempts[user_id]
                    if attempt['timestamp'] > cutoff_time
                ]

    def get_security_summary(self) -> Dict[str, Any]:
        """Get security metrics summary."""
        with self.lock:
            return {
                'event_counts': dict(self.security_event_counts),
                'failed_login_summary': {
                    user: len(attempts) for user, attempts in self.failed_login_attempts.items()
                    if attempts
                },
                'total_security_events': sum(self.security_event_counts.values())
            }


class MetricsLogHandler(logging.Handler):
    """
    Handler that extracts and aggregates metrics from log messages
    for real-time monitoring and alerting.
    """

    def __init__(
        self,
        metrics_backend: str = 'memory',
        flush_interval: float = 60.0,
        metric_extractors: Optional[List[Callable]] = None
    ):
        super().__init__()
        self.metrics_backend = metrics_backend
        self.flush_interval = flush_interval
        self.metric_extractors = metric_extractors or []

        # Metrics storage
        self.counters = defaultdict(int)
        self.gauges = defaultdict(float)
        self.histograms = defaultdict(list)
        self.timers = defaultdict(list)
        self.lock = Lock()

        # Start metrics flush thread
        self.flush_thread = Thread(target=self._flush_metrics_worker, daemon=True)
        self.flush_thread.start()

    def emit(self, record: logging.LogRecord):
        """Extract metrics from log record."""
        try:
            # Extract built-in metrics
            self._extract_builtin_metrics(record)

            # Apply custom metric extractors
            for extractor in self.metric_extractors:
                try:
                    extractor(record, self)
                except Exception:
                    # Don't let custom extractors break logging
                    pass

        except Exception:
            self.handleError(record)

    def _extract_builtin_metrics(self, record: logging.LogRecord):
        """Extract built-in metrics from log record."""
        with self.lock:
            # Count log events by level
            self.counters[f'log_events_{record.levelname.lower()}'] += 1
            self.counters[f'log_events_total'] += 1

            # Count by logger
            self.counters[f'logger_{record.name}'] += 1

            # Track response times
            if hasattr(record, 'duration_ms'):
                self.timers['request_duration'].append(record.duration_ms)

            # Track HTTP status codes
            if hasattr(record, 'status_code'):
                self.counters[f'http_status_{record.status_code}'] += 1
                status_class = f'{record.status_code // 100}xx'
                self.counters[f'http_status_class_{status_class}'] += 1

            # Track memory usage
            if hasattr(record, 'memory_usage'):
                self.gauges['memory_usage_mb'] = record.memory_usage

            # Track error rates
            if record.levelname in ['ERROR', 'CRITICAL']:
                self.counters['error_count'] += 1

    def _flush_metrics_worker(self):
        """Background worker that periodically flushes metrics."""
        while True:
            time.sleep(self.flush_interval)
            try:
                self._flush_metrics()
            except Exception as e:
                print(f"Error flushing metrics: {e}")

    def _flush_metrics(self):
        """Flush metrics to backend."""
        with self.lock:
            # Calculate statistics for histograms and timers
            stats = {}

            for name, values in self.histograms.items():
                if values:
                    stats[f'{name}_count'] = len(values)
                    stats[f'{name}_sum'] = sum(values)
                    stats[f'{name}_avg'] = sum(values) / len(values)
                    stats[f'{name}_min'] = min(values)
                    stats[f'{name}_max'] = max(values)

            for name, values in self.timers.items():
                if values:
                    stats[f'{name}_count'] = len(values)
                    stats[f'{name}_avg'] = sum(values) / len(values)
                    stats[f'{name}_p95'] = self._percentile(values, 95)
                    stats[f'{name}_p99'] = self._percentile(values, 99)

            # Send to metrics backend
            if self.metrics_backend == 'prometheus':
                self._send_to_prometheus(stats)
            elif self.metrics_backend == 'datadog':
                self._send_to_datadog(stats)
            elif self.metrics_backend == 'cloudwatch':
                self._send_to_cloudwatch(stats)

            # Reset counters but keep gauges
            self.counters.clear()
            self.histograms.clear()
            self.timers.clear()

    def _percentile(self, values: List[float], percentile: int) -> float:
        """Calculate percentile value."""
        sorted_values = sorted(values)
        index = int(len(sorted_values) * percentile / 100)
        return sorted_values[min(index, len(sorted_values) - 1)]

    def _send_to_prometheus(self, stats: Dict[str, Any]):
        """Send metrics to Prometheus (placeholder)."""
        # Implementation would use prometheus_client library
        pass

    def _send_to_datadog(self, stats: Dict[str, Any]):
        """Send metrics to Datadog (placeholder)."""
        # Implementation would use datadog library
        pass

    def _send_to_cloudwatch(self, stats: Dict[str, Any]):
        """Send metrics to CloudWatch (placeholder)."""
        # Implementation would use boto3
        pass

    def add_counter(self, name: str, value: int = 1):
        """Add to counter metric."""
        with self.lock:
            self.counters[name] += value

    def set_gauge(self, name: str, value: float):
        """Set gauge metric."""
        with self.lock:
            self.gauges[name] = value

    def add_histogram_value(self, name: str, value: float):
        """Add value to histogram."""
        with self.lock:
            self.histograms[name].append(value)

    def add_timer_value(self, name: str, value: float):
        """Add timing value."""
        with self.lock:
            self.timers[name].append(value)
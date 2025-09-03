
"""
DataDog Log Aggregation System

Comprehensive log aggregation and analysis system that processes logs,
detects patterns, performs anomaly detection, and sends results to DataDog.
"""

import gzip
import json
import logging
import os
import queue
import re
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from core.logging import get_logger

try:
    from opentelemetry import trace
    tracer = trace.get_tracer(__name__)
except ImportError:
    # Fallback for when OpenTelemetry is not available
    class MockSpan:
        def __init__(self):
            self.trace_id = None
            self.span_id = None

    class MockTracer:
        def current_span(self):
            return MockSpan()

    tracer = MockTracer()


# Enums and Data Classes
class LogLevel(Enum):
    """Log level enumeration"""
    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class LogCategory(Enum):
    """Log category for classification"""
    APPLICATION = "APPLICATION"
    SECURITY = "SECURITY"
    ETL = "ETL"
    API = "API"
    DATABASE = "DATABASE"
    SYSTEM = "SYSTEM"
    ML_PIPELINE = "ML_PIPELINE"


class AlertSeverity(Enum):
    """Alert severity levels"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class LogEntry:
    """Structured log entry"""
    timestamp: datetime
    level: LogLevel
    message: str
    logger_name: str
    service: str
    environment: str
    category: LogCategory
    tags: dict[str, str] = field(default_factory=dict)
    trace_id: str | None = None
    span_id: str | None = None
    user_id: str | None = None
    session_id: str | None = None
    request_id: str | None = None
    exception: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    source_host: str | None = None
    source_file: str | None = None
    source_line: int | None = None

    def to_datadog_format(self) -> dict[str, Any]:
        """Convert log entry to DataDog format"""
        return {
            "timestamp": self.timestamp.isoformat(),
            "level": self.level.value.lower(),
            "message": self.message,
            "logger": {"name": self.logger_name},
            "service": self.service,
            "env": self.environment,
            "category": self.category.value.lower(),
            "dd": {
                "trace_id": self.trace_id,
                "span_id": self.span_id
            } if self.trace_id else {},
            "usr": {"id": self.user_id} if self.user_id else {},
            "session_id": self.session_id,
            "request_id": self.request_id,
            "error": self.exception if self.exception else {},
            "custom": {
                **self.metadata,
                **self.tags
            },
            "host": self.source_host,
            "source": {
                "file": self.source_file,
                "line": self.source_line
            } if self.source_file else {}
        }


@dataclass
class LogPattern:
    """Pattern definition for log analysis"""
    name: str
    pattern: str
    description: str
    severity: AlertSeverity
    category: LogCategory = LogCategory.APPLICATION
    threshold: int = 5
    time_window_minutes: int = 15
    enabled: bool = True


@dataclass
class LogAggregationRule:
    """Rule for log aggregation"""
    name: str
    field: str
    aggregation_type: str  # count, sum, avg, max, min
    time_window_minutes: int
    threshold: float | None = None
    enabled: bool = True


@dataclass
class LogAnalysisResult:
    """Result of log analysis"""
    timestamp: datetime
    analysis_type: str
    result: dict[str, Any]
    patterns_found: list[str] = field(default_factory=list)
    anomalies_detected: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)


@dataclass
class LogCollectionConfig:
    """Configuration for log collection and aggregation"""
    # Batching configuration
    batch_size: int = 100
    batch_timeout_seconds: float = 5.0
    max_buffer_size: int = 10000

    # Analysis configuration
    analysis_window_minutes: int = 10
    enable_pattern_detection: bool = True
    enable_anomaly_detection: bool = True
    anomaly_threshold_multiplier: float = 3.0

    # Storage configuration
    archive_path: Path = field(default_factory=lambda: Path("./logs/archive"))
    enable_archiving: bool = True
    archive_compression: bool = True

    # DataDog configuration
    datadog_api_key: str | None = None
    datadog_site: str = "datadoghq.com"
    send_to_datadog: bool = True


class DataDogLogAggregator:
    """
    Advanced DataDog log aggregator with pattern detection,
    anomaly detection, and intelligent analysis.
    """

    def __init__(self, config: LogCollectionConfig | None = None):
        self.config = config or LogCollectionConfig()
        self.logger = get_logger(__name__)

        # Threading and processing
        self.log_buffer: queue.Queue = queue.Queue(maxsize=self.config.max_buffer_size)
        self.processing_thread: threading.Thread | None = None
        self.is_running = False

        # Analysis components
        self.log_patterns: list[LogPattern] = self._initialize_default_patterns()
        self.aggregation_rules: list[LogAggregationRule] = self._initialize_default_aggregation_rules()
        self.recent_logs: deque = deque(maxlen=1000)
        self.aggregated_metrics: defaultdict = defaultdict(lambda: defaultdict(int))
        self.analysis_results: deque = deque(maxlen=100)

        # Statistics
        self.stats = {
            "logs_processed": 0,
            "logs_sent_to_datadog": 0,
            "patterns_detected": 0,
            "anomalies_detected": 0,
            "errors_encountered": 0,
            "start_time": datetime.now(),
        }

        self.logger.info("DataDog Log Aggregator initialized")

    def _initialize_default_patterns(self) -> list[LogPattern]:
        """Initialize default log patterns for detection"""
        return [
            LogPattern(
                name="database_error",
                pattern=r"(?i)(database|connection|sql).*error",
                description="Database connection or query errors",
                severity=AlertSeverity.HIGH,
                category=LogCategory.DATABASE
            ),
            LogPattern(
                name="authentication_failure",
                pattern=r"(?i)(authentication|login|auth).*failed",
                description="Authentication failures",
                severity=AlertSeverity.MEDIUM,
                category=LogCategory.SECURITY
            ),
            LogPattern(
                name="critical_exception",
                pattern=r"(?i)critical|fatal|severe",
                description="Critical system exceptions",
                severity=AlertSeverity.CRITICAL,
                category=LogCategory.SYSTEM
            ),
            LogPattern(
                name="memory_issue",
                pattern=r"(?i)(memory|oom|out of memory)",
                description="Memory-related issues",
                severity=AlertSeverity.HIGH,
                category=LogCategory.SYSTEM
            ),
            LogPattern(
                name="etl_failure",
                pattern=r"(?i)(etl|pipeline).*fail",
                description="ETL or data pipeline failures",
                severity=AlertSeverity.HIGH,
                category=LogCategory.ETL
            )
        ]

    def _initialize_default_aggregation_rules(self) -> list[LogAggregationRule]:
        """Initialize default aggregation rules"""
        return [
            LogAggregationRule(
                name="error_rate",
                field="level",
                aggregation_type="count",
                time_window_minutes=5,
                threshold=10
            ),
            LogAggregationRule(
                name="service_requests",
                field="service",
                aggregation_type="count",
                time_window_minutes=15
            ),
            LogAggregationRule(
                name="user_activity",
                field="user_id",
                aggregation_type="count",
                time_window_minutes=30
            )
        ]

    def start(self):
        """Start the log aggregator"""
        if self.is_running:
            self.logger.warning("DataDog Log Aggregator is already running")
            return

        self.is_running = True
        self.processing_thread = threading.Thread(
            target=self._processing_loop,
            daemon=True,
            name="DataDogLogAggregator"
        )
        self.processing_thread.start()
        self.logger.info("DataDog Log Aggregator started")

    def stop(self):
        """Stop the log aggregator"""
        if not self.is_running:
            return

        self.is_running = False

        # Wait for processing thread to finish
        if self.processing_thread and self.processing_thread.is_alive():
            self.processing_thread.join(timeout=10)

        # Flush remaining logs
        self._flush_buffer()

        self.logger.info("DataDog Log Aggregator stopped")

    def add_log(self, log_entry: LogEntry):
        """Add log entry for processing"""
        try:
            if not self.is_running:
                self.start()

            self.log_buffer.put(log_entry, timeout=1.0)
            self.recent_logs.append(log_entry)

        except queue.Full:
            self.logger.error("Log buffer is full, dropping log entry")
            self.stats["errors_encountered"] += 1
        except Exception as e:
            self.logger.error(f"Error adding log entry: {e}")
            self.stats["errors_encountered"] += 1

    def _processing_loop(self):
        """Main processing loop"""
        batch = []
        last_batch_time = time.time()

        while self.is_running:
            try:
                # Check for batch conditions
                current_time = time.time()
                time_since_last_batch = current_time - last_batch_time

                # Get log entry with timeout
                try:
                    log_entry = self.log_buffer.get(timeout=1.0)
                    batch.append(log_entry)
                except queue.Empty:
                    # Process batch if timeout exceeded
                    if batch and time_since_last_batch >= self.config.batch_timeout_seconds:
                        self._process_log_batch(batch)
                        batch = []
                        last_batch_time = current_time
                    continue

                # Process batch if size threshold reached
                if len(batch) >= self.config.batch_size:
                    self._process_log_batch(batch)
                    batch = []
                    last_batch_time = current_time

                # Periodic analysis
                if time_since_last_batch >= self.config.analysis_window_minutes * 60:
                    self._perform_periodic_analysis()
                    last_batch_time = current_time

            except Exception as e:
                self.logger.error(f"Error in processing loop: {e}")
                self.stats["errors_encountered"] += 1
                time.sleep(1)

        # Process remaining logs
        if batch:
            self._process_log_batch(batch)

    def _process_log_batch(self, batch: list[LogEntry]):
        """Process a batch of log entries"""
        if not batch:
            return

        try:
            # Update statistics
            self.stats["logs_processed"] += len(batch)

            # Perform pattern detection
            if self.config.enable_pattern_detection:
                self._detect_patterns(batch)

            # Perform anomaly detection
            if self.config.enable_anomaly_detection:
                self._detect_anomalies(batch)

            # Update aggregated metrics
            self._update_aggregated_metrics(batch)

            # Send to DataDog
            if self.config.send_to_datadog:
                self._send_to_datadog(batch)

            # Archive logs if enabled
            if self.config.enable_archiving:
                self._archive_logs(batch)

            self.logger.debug(f"Processed batch of {len(batch)} log entries")

        except Exception as e:
            self.logger.error(f"Error processing log batch: {e}")
            self.stats["errors_encountered"] += 1

    def _detect_patterns(self, batch: list[LogEntry]):
        """Detect patterns in log batch"""
        pattern_matches = defaultdict(list)

        for log_entry in batch:
            for pattern in self.log_patterns:
                if not pattern.enabled:
                    continue

                if pattern.category != LogCategory.APPLICATION and pattern.category != log_entry.category:
                    continue

                try:
                    if re.search(pattern.pattern, log_entry.message):
                        pattern_matches[pattern.name].append(log_entry)
                except re.error as e:
                    self.logger.warning(f"Invalid regex pattern '{pattern.pattern}': {e}")
                    continue

        # Check thresholds and trigger alerts
        for pattern_name, matches in pattern_matches.items():
            if len(matches) >= next(p.threshold for p in self.log_patterns if p.name == pattern_name):
                pattern = next(p for p in self.log_patterns if p.name == pattern_name)
                self._trigger_pattern_alert(pattern, len(matches), matches[0])
                self.stats["patterns_detected"] += 1

    def _detect_anomalies(self, batch: list[LogEntry]):
        """Detect anomalies in log patterns"""
        # Simple anomaly detection based on log frequency
        current_time = datetime.now()
        time_window = timedelta(minutes=self.config.analysis_window_minutes)

        # Count logs by service in time window
        service_counts = defaultdict(int)
        for log_entry in self.recent_logs:
            if current_time - log_entry.timestamp <= time_window:
                service_counts[log_entry.service] += 1

        # Calculate baseline (average of recent windows)
        baseline = sum(service_counts.values()) / max(len(service_counts), 1)
        threshold = baseline * self.config.anomaly_threshold_multiplier

        # Detect anomalies
        for service, count in service_counts.items():
            if count > threshold:
                anomaly_logs = [log for log in batch if log.service == service]
                self._trigger_anomaly_alert(
                    "high_frequency",
                    f"High log frequency detected for service {service}",
                    count,
                    anomaly_logs[:5]  # Sample logs
                )
                self.stats["anomalies_detected"] += 1

    def _update_aggregated_metrics(self, batch: list[LogEntry]):
        """Update aggregated metrics from batch"""
        for log_entry in batch:
            # Service metrics
            self.aggregated_metrics["services"][log_entry.service] += 1

            # Level metrics
            self.aggregated_metrics["levels"][log_entry.level.value] += 1

            # Category metrics
            self.aggregated_metrics["categories"][log_entry.category.value] += 1

            # Hourly metrics
            hour_key = log_entry.timestamp.strftime("%Y-%m-%d-%H")
            self.aggregated_metrics["hourly"][hour_key] += 1

    def _send_to_datadog(self, batch: list[LogEntry]):
        """Send log batch to DataDog"""
        try:
            # Convert logs to DataDog format
            dd_logs = []
            for log_entry in batch:
                dd_log = log_entry.to_datadog_format()
                dd_logs.append(dd_log)

            # Send to DataDog Logs API
            # Note: In a real implementation, you would use DataDog's log ingestion endpoint
            # For this example, we'll simulate the API call

            # Simulate API call
            success = self._simulate_datadog_api_call(dd_logs)

            if success:
                self.stats["logs_sent_to_datadog"] += len(batch)
                self.logger.debug(f"Sent {len(batch)} logs to DataDog")
            else:
                self.logger.error(f"Failed to send {len(batch)} logs to DataDog")
                self.stats["errors_encountered"] += 1

        except Exception as e:
            self.logger.error(f"Error sending logs to DataDog: {e}")
            self.stats["errors_encountered"] += 1

    def _simulate_datadog_api_call(self, logs: list[dict[str, Any]]) -> bool:
        """Simulate DataDog API call (replace with actual implementation)"""
        try:
            # In a real implementation, you would use:
            # - DataDog's HTTP Log API
            # - DataDog Agent log forwarding
            # - DataDog Lambda extension

            # For demonstration, we'll just log the attempt
            self.logger.debug(f"Simulating DataDog API call for {len(logs)} logs")

            # Simulate network delay
            import random
            time.sleep(random.uniform(0.1, 0.5))

            # Simulate success/failure
            return random.random() > 0.05  # 95% success rate

        except Exception:
            return False

    def _archive_logs(self, batch: list[LogEntry]):
        """Archive logs for long-term storage"""
        try:
            if not self.config.archive_path.exists():
                self.config.archive_path.mkdir(parents=True, exist_ok=True)

            # Create archive file with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_file = self.config.archive_path / f"logs_{timestamp}.json.gz"

            # Prepare log data for archiving
            archive_data = []
            for log_entry in batch:
                archive_data.append({
                    "timestamp": log_entry.timestamp.isoformat(),
                    "level": log_entry.level.value,
                    "message": log_entry.message,
                    "logger": log_entry.logger_name,
                    "service": log_entry.service,
                    "environment": log_entry.environment,
                    "category": log_entry.category.value,
                    "tags": log_entry.tags,
                    "trace_id": log_entry.trace_id,
                    "span_id": log_entry.span_id,
                    "user_id": log_entry.user_id,
                    "session_id": log_entry.session_id,
                    "request_id": log_entry.request_id,
                    "exception": log_entry.exception,
                    "metadata": log_entry.metadata,
                    "source_host": log_entry.source_host,
                    "source_file": log_entry.source_file,
                    "source_line": log_entry.source_line
                })

            # Write compressed archive
            with gzip.open(archive_file, 'wt', encoding='utf-8') as f:
                json.dump(archive_data, f, indent=2)

            self.logger.debug(f"Archived {len(batch)} logs to {archive_file}")

        except Exception as e:
            self.logger.error(f"Error archiving logs: {e}")

    def _trigger_pattern_alert(self, pattern: LogPattern, count: int, sample_log: LogEntry):
        """Trigger alert for detected pattern"""
        alert_message = (
            f"Pattern '{pattern.name}' detected {count} times in {pattern.time_window_minutes} minutes. "
            f"Description: {pattern.description}. "
            f"Sample log: {sample_log.message[:200]}..."
        )

        # Log the alert
        if pattern.severity == AlertSeverity.CRITICAL:
            self.logger.critical(alert_message)
        elif pattern.severity == AlertSeverity.HIGH:
            self.logger.error(alert_message)
        elif pattern.severity == AlertSeverity.MEDIUM:
            self.logger.warning(alert_message)
        else:
            self.logger.info(alert_message)

        # Create structured alert for further processing
        alert_data = {
            "type": "pattern_detection",
            "pattern_name": pattern.name,
            "severity": pattern.severity.value,
            "count": count,
            "time_window_minutes": pattern.time_window_minutes,
            "description": pattern.description,
            "sample_message": sample_log.message,
            "service": sample_log.service,
            "category": pattern.category.value,
            "timestamp": datetime.now().isoformat()
        }

        # Store alert for analysis
        self.analysis_results.append(
            LogAnalysisResult(
                timestamp=datetime.now(),
                analysis_type="pattern_alert",
                result=alert_data,
                patterns_found=[pattern.name]
            )
        )

        # Send alert to monitoring system (implementation specific)
        self._send_alert_to_monitoring(alert_data)

    def _trigger_anomaly_alert(self, anomaly_type: str, description: str, count: int, logs: list[LogEntry]):
        """Trigger alert for detected anomaly"""
        alert_message = f"Anomaly detected: {description} (Count: {count})"

        self.logger.warning(alert_message)

        # Create structured alert
        alert_data = {
            "type": "anomaly_detection",
            "anomaly_type": anomaly_type,
            "description": description,
            "count": count,
            "sample_logs": [log.message for log in logs[:3]],  # Sample messages
            "services_affected": list({log.service for log in logs}),
            "timestamp": datetime.now().isoformat()
        }

        # Store alert for analysis
        self.analysis_results.append(
            LogAnalysisResult(
                timestamp=datetime.now(),
                analysis_type="anomaly_alert",
                result=alert_data,
                anomalies_detected=[anomaly_type]
            )
        )

        # Send alert to monitoring system
        self._send_alert_to_monitoring(alert_data)

    def _send_alert_to_monitoring(self, alert_data: dict[str, Any]):
        """Send alert to monitoring system"""
        try:
            # In a real implementation, this would send to:
            # - DataDog Events API
            # - Slack/Teams notifications
            # - PagerDuty/OpsGenie
            # - Email notifications

            self.logger.info(f"Alert sent to monitoring: {alert_data['type']} - {alert_data.get('description', 'N/A')}")

        except Exception as e:
            self.logger.error(f"Error sending alert to monitoring: {e}")

    def _perform_periodic_analysis(self):
        """Perform periodic log analysis"""
        try:
            current_time = datetime.now()

            # Analyze recent logs
            if len(self.recent_logs) < 10:
                return

            # Convert deque to list for analysis
            recent_logs = list(self.recent_logs)

            # Calculate statistics
            log_levels_count = defaultdict(int)
            services_count = defaultdict(int)
            categories_count = defaultdict(int)

            for log_entry in recent_logs:
                log_levels_count[log_entry.level.value] += 1
                services_count[log_entry.service] += 1
                categories_count[log_entry.category.value] += 1

            # Create analysis result
            analysis_result = LogAnalysisResult(
                timestamp=current_time,
                analysis_type="periodic_analysis",
                result={
                    "total_logs_analyzed": len(recent_logs),
                    "log_levels_distribution": dict(log_levels_count),
                    "services_distribution": dict(services_count),
                    "categories_distribution": dict(categories_count),
                    "error_rate": (log_levels_count["ERROR"] + log_levels_count["CRITICAL"]) / len(recent_logs) if recent_logs else 0,
                    "top_services": sorted(services_count.items(), key=lambda x: x[1], reverse=True)[:5],
                    "analysis_window_minutes": self.config.analysis_window_minutes
                }
            )

            self.analysis_results.append(analysis_result)

            # Generate recommendations based on analysis
            recommendations = self._generate_recommendations(analysis_result.result)
            analysis_result.recommendations = recommendations

            self.logger.debug(f"Completed periodic analysis of {len(recent_logs)} logs")

        except Exception as e:
            self.logger.error(f"Error in periodic analysis: {e}")

    def _generate_recommendations(self, analysis_data: dict[str, Any]) -> list[str]:
        """Generate recommendations based on analysis"""
        recommendations = []

        # High error rate recommendation
        error_rate = analysis_data.get("error_rate", 0)
        if error_rate > 0.1:  # 10% error rate threshold
            recommendations.append(
                f"High error rate detected ({error_rate:.2%}). Consider investigating error patterns and root causes."
            )

        # Service-specific recommendations
        top_services = analysis_data.get("top_services", [])
        if top_services:
            top_service, top_count = top_services[0]
            total_logs = analysis_data.get("total_logs_analyzed", 1)
            if top_count > total_logs * 0.5:  # Single service generating >50% of logs
                recommendations.append(
                    f"Service '{top_service}' is generating {top_count}/{total_logs} logs. "
                    "Consider reviewing log levels and reducing verbose logging."
                )

        # Category-specific recommendations
        categories = analysis_data.get("categories_distribution", {})
        if categories.get("ERROR", 0) > 10:
            recommendations.append(
                "High number of error-category logs detected. Consider implementing error handling improvements."
            )

        return recommendations

    def _flush_buffer(self):
        """Flush remaining logs in buffer"""
        batch = []

        try:
            while not self.log_buffer.empty():
                log_entry = self.log_buffer.get_nowait()
                batch.append(log_entry)
        except queue.Empty:
            pass

        if batch:
            self._process_log_batch(batch)
            self.logger.info(f"Flushed {len(batch)} remaining logs")

    # Public API methods

    def add_log_pattern(self, pattern: LogPattern):
        """Add custom log pattern for detection"""
        self.log_patterns.append(pattern)
        self.logger.info(f"Added log pattern: {pattern.name}")

    def remove_log_pattern(self, pattern_name: str) -> bool:
        """Remove log pattern by name"""
        for i, pattern in enumerate(self.log_patterns):
            if pattern.name == pattern_name:
                del self.log_patterns[i]
                self.logger.info(f"Removed log pattern: {pattern_name}")
                return True
        return False

    def add_aggregation_rule(self, rule: LogAggregationRule):
        """Add custom aggregation rule"""
        self.aggregation_rules.append(rule)
        self.logger.info(f"Added aggregation rule: {rule.name}")

    def remove_aggregation_rule(self, rule_name: str) -> bool:
        """Remove aggregation rule by name"""
        for i, rule in enumerate(self.aggregation_rules):
            if rule.name == rule_name:
                del self.aggregation_rules[i]
                self.logger.info(f"Removed aggregation rule: {rule_name}")
                return True
        return False

    def get_statistics(self) -> dict[str, Any]:
        """Get aggregator statistics"""
        current_stats = self.stats.copy()
        current_stats["uptime_seconds"] = (datetime.now() - current_stats["start_time"]).total_seconds()
        current_stats["buffer_size"] = self.log_buffer.qsize()
        current_stats["patterns_configured"] = len(self.log_patterns)
        current_stats["aggregation_rules_configured"] = len(self.aggregation_rules)
        current_stats["recent_analysis_results"] = len(self.analysis_results)
        return current_stats

    def get_recent_analysis_results(self, limit: int = 10) -> list[LogAnalysisResult]:
        """Get recent analysis results"""
        return list(self.analysis_results)[-limit:]

    def get_aggregated_metrics(self) -> dict[str, dict[str, Any]]:
        """Get current aggregated metrics"""
        return dict(self.aggregated_metrics)

    def search_logs(self, query: str, limit: int = 100) -> list[LogEntry]:
        """Search recent logs by query"""
        results = []
        query_lower = query.lower()

        for log_entry in reversed(self.recent_logs):
            if len(results) >= limit:
                break

            # Simple text search in message
            if query_lower in log_entry.message.lower():
                results.append(log_entry)

        return results

    def health_check(self) -> dict[str, Any]:
        """Perform health check"""
        health_status = {
            "status": "healthy",
            "is_running": self.is_running,
            "buffer_utilization": self.log_buffer.qsize() / self.config.max_buffer_size,
            "error_rate": self.stats["errors_encountered"] / max(self.stats["logs_processed"], 1),
            "processing_thread_active": self.processing_thread and self.processing_thread.is_alive(),
            "statistics": self.get_statistics(),
            "timestamp": datetime.now().isoformat()
        }

        # Determine overall health status
        if health_status["error_rate"] > 0.05:  # 5% error rate threshold
            health_status["status"] = "degraded"
        if not health_status["is_running"] or not health_status["processing_thread_active"]:
            health_status["status"] = "unhealthy"

        return health_status

    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()


# Factory function
def create_datadog_log_aggregator(config: LogCollectionConfig | None = None) -> DataDogLogAggregator:
    """Create DataDog log aggregator instance"""
    return DataDogLogAggregator(config)


# Integration with existing logging system
class DataDogLogHandler(logging.Handler):
    """Custom logging handler that sends logs to DataDog aggregator"""

    def __init__(self, aggregator: DataDogLogAggregator, service: str):
        super().__init__()
        self.aggregator = aggregator
        self.service = service

    def emit(self, record: logging.LogRecord):
        """Emit log record to DataDog aggregator"""
        try:
            # Convert logging record to LogEntry
            level_map = {
                logging.DEBUG: LogLevel.DEBUG,
                logging.INFO: LogLevel.INFO,
                logging.WARNING: LogLevel.WARNING,
                logging.ERROR: LogLevel.ERROR,
                logging.CRITICAL: LogLevel.CRITICAL
            }

            # Determine category based on logger name
            category = LogCategory.APPLICATION
            if "security" in record.name.lower():
                category = LogCategory.SECURITY
            elif "etl" in record.name.lower() or "pipeline" in record.name.lower():
                category = LogCategory.ETL
            elif "api" in record.name.lower():
                category = LogCategory.API
            elif "ml" in record.name.lower():
                category = LogCategory.ML_PIPELINE
            elif "db" in record.name.lower() or "database" in record.name.lower():
                category = LogCategory.DATABASE

            # Extract trace context if available
            span = tracer.current_span()
            trace_id = str(span.trace_id) if span else None
            span_id = str(span.span_id) if span else None

            # Create LogEntry
            log_entry = LogEntry(
                timestamp=datetime.fromtimestamp(record.created),
                level=level_map.get(record.levelno, LogLevel.INFO),
                message=record.getMessage(),
                logger_name=record.name,
                service=self.service,
                environment=os.getenv("DD_ENV", "development"),
                category=category,
                trace_id=trace_id,
                span_id=span_id,
                source_file=record.pathname,
                source_line=record.lineno,
                exception={
                    "type": record.exc_info[0].__name__ if record.exc_info else None,
                    "message": str(record.exc_info[1]) if record.exc_info else None,
                    "traceback": self.formatException(record.exc_info) if record.exc_info else None
                } if record.exc_info else None
            )

            # Add to aggregator
            self.aggregator.add_log(log_entry)

        except Exception:
            self.handleError(record)


# Example usage and testing
if __name__ == "__main__":
    # Configure logging
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = get_logger(__name__)

    # Test configuration
    config = LogCollectionConfig()
    config.batch_size = 5
    config.batch_timeout_seconds = 2
    config.enable_pattern_detection = True
    config.enable_anomaly_detection = True

    # Create and start aggregator
    with create_datadog_log_aggregator(config) as aggregator:
        # Add custom pattern
        custom_pattern = LogPattern(
            name="test_pattern",
            pattern=r"test.*error",
            description="Test error pattern",
            severity=AlertSeverity.MEDIUM
        )
        aggregator.add_log_pattern(custom_pattern)

        # Test log entries
        test_logs = [
            LogEntry(
                timestamp=datetime.now(),
                level=LogLevel.INFO,
                message="Application started successfully",
                logger_name="test.app",
                service="test-service",
                environment="test",
                category=LogCategory.APPLICATION
            ),
            LogEntry(
                timestamp=datetime.now(),
                level=LogLevel.ERROR,
                message="Database connection failed",
                logger_name="test.db",
                service="test-service",
                environment="test",
                category=LogCategory.DATABASE
            ),
            LogEntry(
                timestamp=datetime.now(),
                level=LogLevel.WARNING,
                message="High memory usage detected",
                logger_name="test.system",
                service="test-service",
                environment="test",
                category=LogCategory.SYSTEM
            )
        ]

        # Add test logs
        for log in test_logs:
            aggregator.add_log(log)
            time.sleep(0.1)

        # Wait for processing
        time.sleep(5)

        # Get statistics
        stats = aggregator.get_statistics()
        print(f"Statistics: {json.dumps(stats, indent=2, default=str)}")

        # Get analysis results
        results = aggregator.get_recent_analysis_results()
        print(f"Analysis results: {len(results)}")

        # Health check
        health = aggregator.health_check()
        print(f"Health: {health['status']}")

    print("DataDog log aggregation test completed")

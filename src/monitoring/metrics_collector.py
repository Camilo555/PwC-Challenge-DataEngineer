"""
Advanced Metrics Collection System
Collects comprehensive business and technical metrics for monitoring
"""
from __future__ import annotations

import time
import psutil
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import json
import sqlite3
from pathlib import Path

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class MetricPoint:
    """Represents a single metric measurement."""
    name: str
    value: float
    timestamp: datetime
    tags: Dict[str, str]
    unit: str = ""
    description: str = ""


@dataclass
class SystemMetrics:
    """System performance metrics."""
    cpu_usage_percent: float
    memory_usage_percent: float
    disk_usage_percent: float
    network_bytes_sent: int
    network_bytes_received: int
    timestamp: datetime


@dataclass
class ETLMetrics:
    """ETL pipeline metrics."""
    pipeline_name: str
    stage: str
    records_processed: int
    records_failed: int
    processing_time_seconds: float
    data_quality_score: float
    timestamp: datetime
    error_count: int = 0
    warnings_count: int = 0


@dataclass
class BusinessMetrics:
    """Business intelligence metrics."""
    total_revenue: float
    transaction_count: int
    unique_customers: int
    avg_order_value: float
    top_products: List[str]
    top_countries: List[str]
    timestamp: datetime


class MetricsCollector:
    """Advanced metrics collection and storage system."""
    
    def __init__(self, storage_path: Optional[Path] = None, retention_days: int = 30):
        self.storage_path = storage_path or Path("./data/metrics/metrics.db")
        self.retention_days = retention_days
        self.metrics_buffer = deque(maxlen=10000)  # In-memory buffer
        self.collection_interval = 60  # seconds
        self.is_collecting = False
        self.collection_thread = None
        
        # Initialize storage
        self._init_storage()
        
        # Metric aggregations
        self.aggregations = defaultdict(list)
        
        # Alert thresholds
        self.thresholds = {
            'cpu_usage_percent': 80.0,
            'memory_usage_percent': 85.0,
            'disk_usage_percent': 90.0,
            'etl_error_rate': 5.0,
            'data_quality_score': 80.0,
            'processing_time_threshold': 300.0  # 5 minutes
        }
        
    def _init_storage(self):
        """Initialize SQLite storage for metrics."""
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.storage_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS system_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    cpu_usage_percent REAL,
                    memory_usage_percent REAL,
                    disk_usage_percent REAL,
                    network_bytes_sent INTEGER,
                    network_bytes_received INTEGER
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS etl_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    pipeline_name TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    records_processed INTEGER,
                    records_failed INTEGER,
                    processing_time_seconds REAL,
                    data_quality_score REAL,
                    error_count INTEGER,
                    warnings_count INTEGER
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS business_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    total_revenue REAL,
                    transaction_count INTEGER,
                    unique_customers INTEGER,
                    avg_order_value REAL,
                    top_products TEXT,
                    top_countries TEXT
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS custom_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    name TEXT NOT NULL,
                    value REAL NOT NULL,
                    tags TEXT,
                    unit TEXT,
                    description TEXT
                )
            """)
            
            # Create indexes for better query performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_system_timestamp ON system_metrics(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_etl_timestamp ON etl_metrics(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_business_timestamp ON business_metrics(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_custom_name_timestamp ON custom_metrics(name, timestamp)")
            
    def start_collection(self):
        """Start automatic metrics collection."""
        if self.is_collecting:
            logger.warning("Metrics collection already running")
            return
            
        self.is_collecting = True
        self.collection_thread = threading.Thread(target=self._collection_loop, daemon=True)
        self.collection_thread.start()
        logger.info("Started automatic metrics collection")
        
    def stop_collection(self):
        """Stop automatic metrics collection."""
        self.is_collecting = False
        if self.collection_thread:
            self.collection_thread.join(timeout=5)
        logger.info("Stopped automatic metrics collection")
        
    def _collection_loop(self):
        """Main collection loop running in background thread."""
        while self.is_collecting:
            try:
                # Collect system metrics
                system_metrics = self.collect_system_metrics()
                self.store_system_metrics(system_metrics)
                
                # Check thresholds and generate alerts
                self._check_system_thresholds(system_metrics)
                
                time.sleep(self.collection_interval)
                
            except Exception as e:
                logger.error(f"Error in metrics collection loop: {e}")
                time.sleep(10)  # Wait before retry
                
    def collect_system_metrics(self) -> SystemMetrics:
        """Collect current system performance metrics."""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            
            # Disk usage (for data directory)
            data_path = Path("./data")
            if data_path.exists():
                disk_usage = psutil.disk_usage(data_path)
                disk_percent = (disk_usage.used / disk_usage.total) * 100
            else:
                disk_percent = 0.0
                
            # Network I/O
            network = psutil.net_io_counters()
            
            return SystemMetrics(
                cpu_usage_percent=cpu_percent,
                memory_usage_percent=memory_percent,
                disk_usage_percent=disk_percent,
                network_bytes_sent=network.bytes_sent,
                network_bytes_received=network.bytes_recv,
                timestamp=datetime.now()
            )
            
        except Exception as e:
            logger.error(f"Failed to collect system metrics: {e}")
            return SystemMetrics(0, 0, 0, 0, 0, datetime.now())
            
    def record_etl_metrics(self, pipeline_name: str, stage: str, 
                          records_processed: int, records_failed: int,
                          processing_time: float, data_quality_score: float,
                          error_count: int = 0, warnings_count: int = 0):
        """Record ETL pipeline metrics."""
        metrics = ETLMetrics(
            pipeline_name=pipeline_name,
            stage=stage,
            records_processed=records_processed,
            records_failed=records_failed,
            processing_time_seconds=processing_time,
            data_quality_score=data_quality_score,
            error_count=error_count,
            warnings_count=warnings_count,
            timestamp=datetime.now()
        )
        
        self.store_etl_metrics(metrics)
        self._check_etl_thresholds(metrics)
        
    def record_business_metrics(self, total_revenue: float, transaction_count: int,
                               unique_customers: int, avg_order_value: float,
                               top_products: List[str], top_countries: List[str]):
        """Record business intelligence metrics."""
        metrics = BusinessMetrics(
            total_revenue=total_revenue,
            transaction_count=transaction_count,
            unique_customers=unique_customers,
            avg_order_value=avg_order_value,
            top_products=top_products,
            top_countries=top_countries,
            timestamp=datetime.now()
        )
        
        self.store_business_metrics(metrics)
        
    def record_custom_metric(self, name: str, value: float, 
                           tags: Optional[Dict[str, str]] = None,
                           unit: str = "", description: str = ""):
        """Record a custom metric."""
        metric = MetricPoint(
            name=name,
            value=value,
            timestamp=datetime.now(),
            tags=tags or {},
            unit=unit,
            description=description
        )
        
        self.metrics_buffer.append(metric)
        self.store_custom_metric(metric)
        
    def store_system_metrics(self, metrics: SystemMetrics):
        """Store system metrics to database."""
        try:
            with sqlite3.connect(self.storage_path) as conn:
                conn.execute("""
                    INSERT INTO system_metrics 
                    (timestamp, cpu_usage_percent, memory_usage_percent, 
                     disk_usage_percent, network_bytes_sent, network_bytes_received)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    metrics.timestamp.isoformat(),
                    metrics.cpu_usage_percent,
                    metrics.memory_usage_percent,
                    metrics.disk_usage_percent,
                    metrics.network_bytes_sent,
                    metrics.network_bytes_received
                ))
        except Exception as e:
            logger.error(f"Failed to store system metrics: {e}")
            
    def store_etl_metrics(self, metrics: ETLMetrics):
        """Store ETL metrics to database."""
        try:
            with sqlite3.connect(self.storage_path) as conn:
                conn.execute("""
                    INSERT INTO etl_metrics 
                    (timestamp, pipeline_name, stage, records_processed, 
                     records_failed, processing_time_seconds, data_quality_score,
                     error_count, warnings_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    metrics.timestamp.isoformat(),
                    metrics.pipeline_name,
                    metrics.stage,
                    metrics.records_processed,
                    metrics.records_failed,
                    metrics.processing_time_seconds,
                    metrics.data_quality_score,
                    metrics.error_count,
                    metrics.warnings_count
                ))
        except Exception as e:
            logger.error(f"Failed to store ETL metrics: {e}")
            
    def store_business_metrics(self, metrics: BusinessMetrics):
        """Store business metrics to database."""
        try:
            with sqlite3.connect(self.storage_path) as conn:
                conn.execute("""
                    INSERT INTO business_metrics 
                    (timestamp, total_revenue, transaction_count, unique_customers,
                     avg_order_value, top_products, top_countries)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    metrics.timestamp.isoformat(),
                    metrics.total_revenue,
                    metrics.transaction_count,
                    metrics.unique_customers,
                    metrics.avg_order_value,
                    json.dumps(metrics.top_products),
                    json.dumps(metrics.top_countries)
                ))
        except Exception as e:
            logger.error(f"Failed to store business metrics: {e}")
            
    def store_custom_metric(self, metric: MetricPoint):
        """Store custom metric to database."""
        try:
            with sqlite3.connect(self.storage_path) as conn:
                conn.execute("""
                    INSERT INTO custom_metrics 
                    (timestamp, name, value, tags, unit, description)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    metric.timestamp.isoformat(),
                    metric.name,
                    metric.value,
                    json.dumps(metric.tags),
                    metric.unit,
                    metric.description
                ))
        except Exception as e:
            logger.error(f"Failed to store custom metric: {e}")
            
    def _check_system_thresholds(self, metrics: SystemMetrics):
        """Check system metrics against thresholds and trigger alerts."""
        alerts = []
        
        if metrics.cpu_usage_percent > self.thresholds['cpu_usage_percent']:
            alerts.append(f"High CPU usage: {metrics.cpu_usage_percent:.1f}%")
            
        if metrics.memory_usage_percent > self.thresholds['memory_usage_percent']:
            alerts.append(f"High memory usage: {metrics.memory_usage_percent:.1f}%")
            
        if metrics.disk_usage_percent > self.thresholds['disk_usage_percent']:
            alerts.append(f"High disk usage: {metrics.disk_usage_percent:.1f}%")
            
        for alert in alerts:
            logger.warning(f"SYSTEM ALERT: {alert}")
            
    def _check_etl_thresholds(self, metrics: ETLMetrics):
        """Check ETL metrics against thresholds and trigger alerts."""
        alerts = []
        
        # Error rate check
        if metrics.records_processed > 0:
            error_rate = (metrics.records_failed / metrics.records_processed) * 100
            if error_rate > self.thresholds['etl_error_rate']:
                alerts.append(f"High ETL error rate: {error_rate:.1f}%")
                
        # Data quality check
        if metrics.data_quality_score < self.thresholds['data_quality_score']:
            alerts.append(f"Low data quality score: {metrics.data_quality_score:.1f}")
            
        # Processing time check
        if metrics.processing_time_seconds > self.thresholds['processing_time_threshold']:
            alerts.append(f"Long processing time: {metrics.processing_time_seconds:.1f}s")
            
        for alert in alerts:
            logger.warning(f"ETL ALERT [{metrics.pipeline_name}:{metrics.stage}]: {alert}")
            
    def get_metrics_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get metrics summary for the last N hours."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        try:
            with sqlite3.connect(self.storage_path) as conn:
                # System metrics summary
                system_cursor = conn.execute("""
                    SELECT AVG(cpu_usage_percent), AVG(memory_usage_percent), 
                           AVG(disk_usage_percent), COUNT(*)
                    FROM system_metrics 
                    WHERE timestamp > ?
                """, (cutoff_time.isoformat(),))
                
                system_row = system_cursor.fetchone()
                system_summary = {
                    'avg_cpu': system_row[0] or 0,
                    'avg_memory': system_row[1] or 0,
                    'avg_disk': system_row[2] or 0,
                    'sample_count': system_row[3] or 0
                }
                
                # ETL metrics summary
                etl_cursor = conn.execute("""
                    SELECT pipeline_name, SUM(records_processed), SUM(records_failed),
                           AVG(data_quality_score), COUNT(*)
                    FROM etl_metrics 
                    WHERE timestamp > ?
                    GROUP BY pipeline_name
                """, (cutoff_time.isoformat(),))
                
                etl_summary = {}
                for row in etl_cursor.fetchall():
                    pipeline_name = row[0]
                    etl_summary[pipeline_name] = {
                        'total_processed': row[1] or 0,
                        'total_failed': row[2] or 0,
                        'avg_quality_score': row[3] or 0,
                        'run_count': row[4] or 0
                    }
                
                return {
                    'period_hours': hours,
                    'system': system_summary,
                    'etl_pipelines': etl_summary,
                    'generated_at': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Failed to generate metrics summary: {e}")
            return {'error': str(e)}
            
    def export_metrics(self, output_file: Path, hours: int = 24) -> bool:
        """Export metrics to JSON file."""
        try:
            summary = self.get_metrics_summary(hours)
            
            with open(output_file, 'w') as f:
                json.dump(summary, f, indent=2)
                
            logger.info(f"Metrics exported to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to export metrics: {e}")
            return False
            
    def cleanup_old_metrics(self):
        """Remove old metrics beyond retention period."""
        cutoff_time = datetime.now() - timedelta(days=self.retention_days)
        
        try:
            with sqlite3.connect(self.storage_path) as conn:
                tables = ['system_metrics', 'etl_metrics', 'business_metrics', 'custom_metrics']
                
                for table in tables:
                    result = conn.execute(f"""
                        DELETE FROM {table} WHERE timestamp < ?
                    """, (cutoff_time.isoformat(),))
                    
                    if result.rowcount > 0:
                        logger.info(f"Cleaned up {result.rowcount} old records from {table}")
                        
        except Exception as e:
            logger.error(f"Failed to cleanup old metrics: {e}")


# Global metrics collector instance
_metrics_collector = None


def get_metrics_collector() -> MetricsCollector:
    """Get global metrics collector instance."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


def record_etl_run(pipeline_name: str, stage: str, records_processed: int, 
                   processing_time: float, **kwargs):
    """Convenience function to record ETL metrics."""
    collector = get_metrics_collector()
    collector.record_etl_metrics(
        pipeline_name=pipeline_name,
        stage=stage,
        records_processed=records_processed,
        records_failed=kwargs.get('records_failed', 0),
        processing_time=processing_time,
        data_quality_score=kwargs.get('data_quality_score', 100.0),
        error_count=kwargs.get('error_count', 0),
        warnings_count=kwargs.get('warnings_count', 0)
    )


def main():
    """Test the metrics collector."""
    print("Metrics Collector Module loaded successfully")
    print("Available features:")
    print("- System metrics (CPU, memory, disk, network)")
    print("- ETL pipeline metrics")
    print("- Business intelligence metrics")
    print("- Custom metrics")
    print("- Automated collection and alerting")
    print("- SQLite storage with retention")
    print("- JSON export capabilities")


if __name__ == "__main__":
    main()
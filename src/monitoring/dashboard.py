"""
Real-time Monitoring Dashboard
Provides web-based dashboard for monitoring metrics and alerts
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from core.logging import get_logger

from .alerting import AlertManager
from .metrics_collector import MetricsCollector

logger = get_logger(__name__)


class DashboardDataProvider:
    """Provides data for monitoring dashboard."""

    def __init__(self, metrics_collector: MetricsCollector, alert_manager: AlertManager):
        self.metrics_collector = metrics_collector
        self.alert_manager = alert_manager
        self.cache = {}
        self.cache_ttl = 30  # seconds
        self.last_cache_update = {}

    def get_system_overview(self) -> dict[str, Any]:
        """Get system overview data."""
        cache_key = "system_overview"

        if self._is_cache_valid(cache_key):
            return self.cache[cache_key]

        try:
            # Get latest system metrics
            latest_metrics = self._get_latest_system_metrics()

            # Get system trends (last 24 hours)
            trends = self._get_system_trends(hours=24)

            # Get active alerts
            active_alerts = self.alert_manager.get_active_alerts()

            overview = {
                'current_metrics': latest_metrics,
                'trends': trends,
                'active_alerts_count': len(active_alerts),
                'critical_alerts_count': len([a for a in active_alerts if a.severity.value == 'critical']),
                'system_status': self._determine_system_status(latest_metrics, active_alerts),
                'last_updated': datetime.now().isoformat()
            }

            self._update_cache(cache_key, overview)
            return overview

        except Exception as e:
            logger.error(f"Failed to get system overview: {e}")
            return {'error': str(e)}

    def get_etl_status(self) -> dict[str, Any]:
        """Get ETL pipeline status."""
        cache_key = "etl_status"

        if self._is_cache_valid(cache_key):
            return self.cache[cache_key]

        try:
            # Get ETL metrics from last 24 hours
            etl_data = self._get_etl_metrics(hours=24)

            # Calculate pipeline statistics
            pipeline_stats = {}
            for pipeline_name, runs in etl_data.items():
                if runs:
                    total_processed = sum(run['records_processed'] for run in runs)
                    total_failed = sum(run['records_failed'] for run in runs)
                    avg_quality = sum(run['data_quality_score'] for run in runs) / len(runs)
                    avg_duration = sum(run['processing_time_seconds'] for run in runs) / len(runs)

                    pipeline_stats[pipeline_name] = {
                        'total_runs': len(runs),
                        'total_processed': total_processed,
                        'total_failed': total_failed,
                        'success_rate': ((total_processed - total_failed) / total_processed * 100) if total_processed > 0 else 0,
                        'avg_quality_score': avg_quality,
                        'avg_duration_seconds': avg_duration,
                        'last_run': max(run['timestamp'] for run in runs),
                        'status': self._determine_pipeline_status(runs[-1]) if runs else 'unknown'
                    }

            status_data = {
                'pipelines': pipeline_stats,
                'summary': {
                    'total_pipelines': len(pipeline_stats),
                    'healthy_pipelines': len([p for p in pipeline_stats.values() if p['status'] == 'healthy']),
                    'failed_pipelines': len([p for p in pipeline_stats.values() if p['status'] == 'failed']),
                    'warning_pipelines': len([p for p in pipeline_stats.values() if p['status'] == 'warning'])
                },
                'last_updated': datetime.now().isoformat()
            }

            self._update_cache(cache_key, status_data)
            return status_data

        except Exception as e:
            logger.error(f"Failed to get ETL status: {e}")
            return {'error': str(e)}

    def get_business_metrics(self) -> dict[str, Any]:
        """Get business metrics dashboard data."""
        cache_key = "business_metrics"

        if self._is_cache_valid(cache_key):
            return self.cache[cache_key]

        try:
            # Get business metrics from database
            business_data = self._get_business_metrics_data(hours=24)

            if not business_data:
                return {
                    'error': 'No business metrics available',
                    'last_updated': datetime.now().isoformat()
                }

            # Calculate KPIs
            latest_data = business_data[-1]  # Most recent
            previous_data = business_data[-2] if len(business_data) > 1 else latest_data

            metrics = {
                'current': {
                    'total_revenue': latest_data['total_revenue'],
                    'transaction_count': latest_data['transaction_count'],
                    'unique_customers': latest_data['unique_customers'],
                    'avg_order_value': latest_data['avg_order_value']
                },
                'trends': {
                    'revenue_change': self._calculate_percentage_change(
                        previous_data['total_revenue'], latest_data['total_revenue']
                    ),
                    'transaction_change': self._calculate_percentage_change(
                        previous_data['transaction_count'], latest_data['transaction_count']
                    ),
                    'customer_change': self._calculate_percentage_change(
                        previous_data['unique_customers'], latest_data['unique_customers']
                    ),
                    'aov_change': self._calculate_percentage_change(
                        previous_data['avg_order_value'], latest_data['avg_order_value']
                    )
                },
                'top_products': json.loads(latest_data['top_products']) if latest_data['top_products'] else [],
                'top_countries': json.loads(latest_data['top_countries']) if latest_data['top_countries'] else [],
                'historical_data': business_data[-24:],  # Last 24 data points
                'last_updated': datetime.now().isoformat()
            }

            self._update_cache(cache_key, metrics)
            return metrics

        except Exception as e:
            logger.error(f"Failed to get business metrics: {e}")
            return {'error': str(e)}

    def get_alerts_data(self) -> dict[str, Any]:
        """Get alerts dashboard data."""
        try:
            # Get recent alerts
            active_alerts = self.alert_manager.get_active_alerts()
            alert_summary = self.alert_manager.get_alert_summary(hours=24)

            # Format alerts for dashboard
            formatted_alerts = []
            for alert in sorted(active_alerts, key=lambda x: x.timestamp, reverse=True)[:20]:
                formatted_alerts.append({
                    'id': alert.id,
                    'title': alert.title,
                    'description': alert.description,
                    'severity': alert.severity.value,
                    'source': alert.source,
                    'timestamp': alert.timestamp.isoformat(),
                    'acknowledged': alert.acknowledged,
                    'resolved': alert.resolved
                })

            return {
                'active_alerts': formatted_alerts,
                'summary': alert_summary,
                'last_updated': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"Failed to get alerts data: {e}")
            return {'error': str(e)}

    def get_performance_metrics(self) -> dict[str, Any]:
        """Get performance metrics for charts."""
        cache_key = "performance_metrics"

        if self._is_cache_valid(cache_key):
            return self.cache[cache_key]

        try:
            # Get system metrics time series
            system_series = self._get_system_metrics_series(hours=6)  # Last 6 hours

            # Get ETL performance time series
            etl_series = self._get_etl_performance_series(hours=24)  # Last 24 hours

            performance_data = {
                'system_metrics': {
                    'timestamps': [point['timestamp'] for point in system_series],
                    'cpu_usage': [point['cpu_usage_percent'] for point in system_series],
                    'memory_usage': [point['memory_usage_percent'] for point in system_series],
                    'disk_usage': [point['disk_usage_percent'] for point in system_series]
                },
                'etl_metrics': {
                    'timestamps': [point['timestamp'] for point in etl_series],
                    'processing_times': [point['processing_time_seconds'] for point in etl_series],
                    'records_processed': [point['records_processed'] for point in etl_series],
                    'quality_scores': [point['data_quality_score'] for point in etl_series]
                },
                'last_updated': datetime.now().isoformat()
            }

            self._update_cache(cache_key, performance_data)
            return performance_data

        except Exception as e:
            logger.error(f"Failed to get performance metrics: {e}")
            return {'error': str(e)}

    def _get_latest_system_metrics(self) -> dict[str, Any]:
        """Get the most recent system metrics."""
        try:
            with sqlite3.connect(self.metrics_collector.storage_path) as conn:
                cursor = conn.execute("""
                    SELECT cpu_usage_percent, memory_usage_percent, disk_usage_percent,
                           network_bytes_sent, network_bytes_received, timestamp
                    FROM system_metrics 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                """)

                row = cursor.fetchone()
                if row:
                    return {
                        'cpu_usage_percent': row[0] or 0,
                        'memory_usage_percent': row[1] or 0,
                        'disk_usage_percent': row[2] or 0,
                        'network_bytes_sent': row[3] or 0,
                        'network_bytes_received': row[4] or 0,
                        'timestamp': row[5]
                    }

        except Exception as e:
            logger.error(f"Failed to get latest system metrics: {e}")

        return {'cpu_usage_percent': 0, 'memory_usage_percent': 0, 'disk_usage_percent': 0}

    def _get_system_trends(self, hours: int) -> dict[str, float]:
        """Get system metric trends."""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        try:
            with sqlite3.connect(self.metrics_collector.storage_path) as conn:
                cursor = conn.execute("""
                    SELECT AVG(cpu_usage_percent), AVG(memory_usage_percent), 
                           AVG(disk_usage_percent), COUNT(*)
                    FROM system_metrics 
                    WHERE timestamp > ?
                """, (cutoff_time.isoformat(),))

                row = cursor.fetchone()
                if row and row[3] > 0:  # Has data
                    return {
                        'avg_cpu': row[0] or 0,
                        'avg_memory': row[1] or 0,
                        'avg_disk': row[2] or 0,
                        'sample_count': row[3]
                    }

        except Exception as e:
            logger.error(f"Failed to get system trends: {e}")

        return {'avg_cpu': 0, 'avg_memory': 0, 'avg_disk': 0, 'sample_count': 0}

    def _get_etl_metrics(self, hours: int) -> dict[str, list[dict]]:
        """Get ETL metrics grouped by pipeline."""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        try:
            with sqlite3.connect(self.metrics_collector.storage_path) as conn:
                cursor = conn.execute("""
                    SELECT pipeline_name, stage, records_processed, records_failed,
                           processing_time_seconds, data_quality_score, timestamp
                    FROM etl_metrics 
                    WHERE timestamp > ?
                    ORDER BY pipeline_name, timestamp
                """, (cutoff_time.isoformat(),))

                etl_data = {}
                for row in cursor.fetchall():
                    pipeline_name = row[0]
                    if pipeline_name not in etl_data:
                        etl_data[pipeline_name] = []

                    etl_data[pipeline_name].append({
                        'stage': row[1],
                        'records_processed': row[2] or 0,
                        'records_failed': row[3] or 0,
                        'processing_time_seconds': row[4] or 0,
                        'data_quality_score': row[5] or 0,
                        'timestamp': row[6]
                    })

                return etl_data

        except Exception as e:
            logger.error(f"Failed to get ETL metrics: {e}")
            return {}

    def _get_business_metrics_data(self, hours: int) -> list[dict]:
        """Get business metrics data."""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        try:
            with sqlite3.connect(self.metrics_collector.storage_path) as conn:
                cursor = conn.execute("""
                    SELECT total_revenue, transaction_count, unique_customers,
                           avg_order_value, top_products, top_countries, timestamp
                    FROM business_metrics 
                    WHERE timestamp > ?
                    ORDER BY timestamp
                """, (cutoff_time.isoformat(),))

                return [{
                    'total_revenue': row[0] or 0,
                    'transaction_count': row[1] or 0,
                    'unique_customers': row[2] or 0,
                    'avg_order_value': row[3] or 0,
                    'top_products': row[4],
                    'top_countries': row[5],
                    'timestamp': row[6]
                } for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"Failed to get business metrics: {e}")
            return []

    def _get_system_metrics_series(self, hours: int) -> list[dict]:
        """Get system metrics time series."""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        try:
            with sqlite3.connect(self.metrics_collector.storage_path) as conn:
                cursor = conn.execute("""
                    SELECT cpu_usage_percent, memory_usage_percent, disk_usage_percent, timestamp
                    FROM system_metrics 
                    WHERE timestamp > ?
                    ORDER BY timestamp
                """, (cutoff_time.isoformat(),))

                return [{
                    'cpu_usage_percent': row[0] or 0,
                    'memory_usage_percent': row[1] or 0,
                    'disk_usage_percent': row[2] or 0,
                    'timestamp': row[3]
                } for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"Failed to get system metrics series: {e}")
            return []

    def _get_etl_performance_series(self, hours: int) -> list[dict]:
        """Get ETL performance time series."""
        cutoff_time = datetime.now() - timedelta(hours=hours)

        try:
            with sqlite3.connect(self.metrics_collector.storage_path) as conn:
                cursor = conn.execute("""
                    SELECT processing_time_seconds, records_processed, data_quality_score, timestamp
                    FROM etl_metrics 
                    WHERE timestamp > ?
                    ORDER BY timestamp
                """, (cutoff_time.isoformat(),))

                return [{
                    'processing_time_seconds': row[0] or 0,
                    'records_processed': row[1] or 0,
                    'data_quality_score': row[2] or 0,
                    'timestamp': row[3]
                } for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"Failed to get ETL performance series: {e}")
            return []

    def _determine_system_status(self, metrics: dict[str, Any], alerts: list) -> str:
        """Determine overall system status."""
        critical_alerts = [a for a in alerts if a.severity.value == 'critical']
        high_alerts = [a for a in alerts if a.severity.value == 'high']

        if critical_alerts:
            return 'critical'
        elif high_alerts or metrics.get('cpu_usage_percent', 0) > 90:
            return 'warning'
        elif metrics.get('memory_usage_percent', 0) > 85 or metrics.get('disk_usage_percent', 0) > 85:
            return 'warning'
        else:
            return 'healthy'

    def _determine_pipeline_status(self, latest_run: dict) -> str:
        """Determine pipeline status from latest run."""
        quality_score = latest_run.get('data_quality_score', 100)
        records_processed = latest_run.get('records_processed', 0)
        records_failed = latest_run.get('records_failed', 0)

        if records_processed == 0:
            return 'failed'

        failure_rate = records_failed / records_processed if records_processed > 0 else 0

        if failure_rate > 0.1 or quality_score < 70:
            return 'failed'
        elif failure_rate > 0.05 or quality_score < 80:
            return 'warning'
        else:
            return 'healthy'

    def _calculate_percentage_change(self, old_value: float, new_value: float) -> float:
        """Calculate percentage change between two values."""
        if old_value == 0:
            return 100.0 if new_value > 0 else 0.0
        return ((new_value - old_value) / old_value) * 100

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache entry is still valid."""
        if cache_key not in self.cache:
            return False

        last_update = self.last_cache_update.get(cache_key, datetime.min)
        return (datetime.now() - last_update).total_seconds() < self.cache_ttl

    def _update_cache(self, cache_key: str, data: Any):
        """Update cache with new data."""
        self.cache[cache_key] = data
        self.last_cache_update[cache_key] = datetime.now()


class MonitoringDashboard:
    """Real-time monitoring dashboard."""

    def __init__(self, metrics_collector: MetricsCollector, alert_manager: AlertManager):
        self.data_provider = DashboardDataProvider(metrics_collector, alert_manager)
        self.is_running = False
        self.update_thread = None
        self.dashboard_data = {}

    def start(self):
        """Start the dashboard data updates."""
        if self.is_running:
            logger.warning("Dashboard already running")
            return

        self.is_running = True
        self.update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self.update_thread.start()
        logger.info("Monitoring dashboard started")

    def stop(self):
        """Stop the dashboard."""
        self.is_running = False
        if self.update_thread:
            self.update_thread.join(timeout=5)
        logger.info("Monitoring dashboard stopped")

    def _update_loop(self):
        """Background loop to update dashboard data."""
        while self.is_running:
            try:
                self._refresh_data()
                time.sleep(30)  # Update every 30 seconds
            except Exception as e:
                logger.error(f"Error updating dashboard data: {e}")
                time.sleep(10)

    def _refresh_data(self):
        """Refresh all dashboard data."""
        self.dashboard_data = {
            'system_overview': self.data_provider.get_system_overview(),
            'etl_status': self.data_provider.get_etl_status(),
            'business_metrics': self.data_provider.get_business_metrics(),
            'alerts': self.data_provider.get_alerts_data(),
            'performance': self.data_provider.get_performance_metrics(),
            'last_updated': datetime.now().isoformat()
        }

    def get_dashboard_data(self) -> dict[str, Any]:
        """Get complete dashboard data."""
        if not self.dashboard_data:
            self._refresh_data()
        return self.dashboard_data

    def export_dashboard_data(self, output_file: Path) -> bool:
        """Export dashboard data to JSON file."""
        try:
            data = self.get_dashboard_data()

            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)

            logger.info(f"Dashboard data exported to {output_file}")
            return True

        except Exception as e:
            logger.error(f"Failed to export dashboard data: {e}")
            return False


def main():
    """Test the dashboard module."""
    print("Monitoring Dashboard Module loaded successfully")
    print("Available features:")
    print("- Real-time system overview")
    print("- ETL pipeline status monitoring")
    print("- Business metrics dashboard")
    print("- Alert management interface")
    print("- Performance metrics visualization")
    print("- Data caching for performance")
    print("- JSON export capabilities")


if __name__ == "__main__":
    main()

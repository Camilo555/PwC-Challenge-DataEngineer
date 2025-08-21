"""
Unit Tests for Monitoring System
Tests metrics collection, health checks, and alerting functionality.
"""
import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
import json

from core.monitoring.metrics import (
    MetricsCollector, MetricType, MetricPoint, ETLJobMetrics,
    track_execution_time, track_async_execution_time
)
from core.monitoring.health_checks import (
    HealthStatus, HealthCheckResult, BaseHealthCheck,
    SystemResourcesHealthCheck, HealthCheckManager
)
from core.monitoring.alerting import (
    AlertSeverity, AlertStatus, Alert, AlertRule,
    BaseAlertChannel, AlertManager
)


class TestMetricsCollector:
    """Test metrics collection functionality."""
    
    def setup_method(self):
        """Setup test metrics collector."""
        self.collector = MetricsCollector(max_history=100)
    
    def test_counter_metrics(self):
        """Test counter metric operations."""
        # Increment counter
        self.collector.increment_counter("test_counter", 5.0)
        self.collector.increment_counter("test_counter", 3.0)
        
        # Check counter value
        key = self.collector._build_metric_key("test_counter", None)
        assert self.collector._counters[key] == 8.0
        
        # Check metric history
        history = self.collector.get_metric_history("test_counter")
        assert len(history) == 2
        assert history[-1].value == 8.0
        assert history[-1].metric_type == MetricType.COUNTER
    
    def test_gauge_metrics(self):
        """Test gauge metric operations."""
        # Set gauge values
        self.collector.set_gauge("test_gauge", 10.5)
        self.collector.set_gauge("test_gauge", 15.7)
        
        # Check gauge value
        key = self.collector._build_metric_key("test_gauge", None)
        assert self.collector._gauges[key] == 15.7
        
        # Check metric history
        history = self.collector.get_metric_history("test_gauge")
        assert len(history) == 2
        assert history[-1].value == 15.7
        assert history[-1].metric_type == MetricType.GAUGE
    
    def test_histogram_metrics(self):
        """Test histogram metric operations."""
        # Record histogram values
        values = [1.0, 2.5, 3.7, 1.2, 4.8]
        for value in values:
            self.collector.record_histogram("test_histogram", value)
        
        # Check histogram history
        history = self.collector.get_metric_history("test_histogram")
        assert len(history) == 5
        
        recorded_values = [point.value for point in history]
        assert recorded_values == values
    
    def test_labels_support(self):
        """Test metric labels functionality."""
        labels1 = {"service": "etl", "env": "prod"}
        labels2 = {"service": "api", "env": "dev"}
        
        # Set metrics with different labels
        self.collector.increment_counter("requests_total", 10, labels1)
        self.collector.increment_counter("requests_total", 5, labels2)
        
        # Verify separate tracking
        key1 = self.collector._build_metric_key("requests_total", labels1)
        key2 = self.collector._build_metric_key("requests_total", labels2)
        
        assert self.collector._counters[key1] == 10
        assert self.collector._counters[key2] == 5
    
    def test_job_metrics(self):
        """Test ETL job metrics tracking."""
        # Start job metrics
        job_metrics = self.collector.start_job_metrics("test_job")
        
        assert job_metrics.job_name == "test_job"
        assert job_metrics.status == "running"
        assert job_metrics.records_processed == 0
        
        # Update job progress
        self.collector.update_job_progress("test_job", records_processed=100, records_failed=5)
        
        job_metrics = self.collector.get_job_metrics("test_job")
        assert job_metrics.records_processed == 100
        assert job_metrics.records_failed == 5
        
        # Finish job
        self.collector.finish_job_metrics("test_job", "completed")
        
        job_metrics = self.collector.get_job_metrics("test_job")
        assert job_metrics.status == "completed"
        assert job_metrics.end_time is not None
        assert job_metrics.duration_seconds > 0
    
    def test_metrics_summary(self):
        """Test metrics summary generation."""
        # Add some metrics
        self.collector.increment_counter("test_counter", 5)
        self.collector.set_gauge("test_gauge", 10.5)
        self.collector.start_job_metrics("test_job")
        
        summary = self.collector.get_metrics_summary()
        
        assert "timestamp" in summary
        assert "counters" in summary
        assert "gauges" in summary
        assert summary["active_jobs"] >= 0
        assert summary["total_jobs"] >= 0
    
    def test_cleanup_old_metrics(self):
        """Test cleanup of old metric data."""
        # Add some metrics
        self.collector.increment_counter("old_metric", 1)
        
        # Simulate old timestamp by modifying the metric point
        key = self.collector._build_metric_key("old_metric", None)
        if key in self.collector._metrics:
            old_point = self.collector._metrics[key][0]
            old_point.timestamp = datetime.utcnow() - timedelta(hours=25)
        
        # Cleanup old metrics (older than 24 hours)
        self.collector.cleanup_old_metrics(timedelta(hours=24))
        
        # Old metrics should be removed
        history = self.collector.get_metric_history("old_metric")
        assert len(history) == 0


class TestDecorators:
    """Test monitoring decorators."""
    
    def setup_method(self):
        """Setup test collector."""
        self.collector = MetricsCollector()
    
    def test_execution_time_decorator(self):
        """Test execution time tracking decorator."""
        @track_execution_time()
        def test_function(duration=0.1):
            import time
            time.sleep(duration)
            return "success"
        
        # Call function
        result = test_function(0.05)
        
        assert result == "success"
        
        # Check if metrics were recorded
        # Note: This test may need adjustment based on actual implementation
    
    @pytest.mark.asyncio
    async def test_async_execution_time_decorator(self):
        """Test async execution time tracking decorator."""
        @track_async_execution_time()
        async def async_test_function(duration=0.1):
            await asyncio.sleep(duration)
            return "async_success"
        
        # Call async function
        result = await async_test_function(0.05)
        
        assert result == "async_success"


class TestHealthChecks:
    """Test health check functionality."""
    
    @pytest.mark.asyncio
    async def test_base_health_check(self):
        """Test base health check functionality."""
        class TestHealthCheck(BaseHealthCheck):
            def __init__(self, should_pass=True):
                super().__init__("test_check")
                self.should_pass = should_pass
            
            async def _check_health(self):
                if self.should_pass:
                    return HealthCheckResult(
                        component=self.name,
                        status=HealthStatus.HEALTHY,
                        message="Test check passed",
                        timestamp=datetime.utcnow()
                    )
                else:
                    raise Exception("Test check failed")
        
        # Test successful check
        check = TestHealthCheck(should_pass=True)
        result = await check.check()
        
        assert result.status == HealthStatus.HEALTHY
        assert result.message == "Test check passed"
        assert result.response_time_ms >= 0
        
        # Test failed check
        check = TestHealthCheck(should_pass=False)
        result = await check.check()
        
        assert result.status == HealthStatus.UNHEALTHY
        assert "Test check failed" in result.message
    
    @pytest.mark.asyncio
    async def test_health_check_timeout(self):
        """Test health check timeout handling."""
        class SlowHealthCheck(BaseHealthCheck):
            async def _check_health(self):
                await asyncio.sleep(2)  # Longer than timeout
                return HealthCheckResult(
                    component=self.name,
                    status=HealthStatus.HEALTHY,
                    message="Should not reach here",
                    timestamp=datetime.utcnow()
                )
        
        check = SlowHealthCheck("slow_check", timeout=0.5)
        result = await check.check()
        
        assert result.status == HealthStatus.UNHEALTHY
        assert "timed out" in result.message.lower()
    
    def test_system_resources_health_check(self):
        """Test system resources health check."""
        # Test with high thresholds (should pass)
        check = SystemResourcesHealthCheck(
            cpu_threshold=99.0,
            memory_threshold=99.0,
            disk_threshold=99.0
        )
        
        # This is a sync test - the actual check is async but we test the class setup
        assert check.name == "system_resources"
        assert check.cpu_threshold == 99.0
        assert check.memory_threshold == 99.0
        assert check.disk_threshold == 99.0


class TestHealthCheckManager:
    """Test health check manager functionality."""
    
    def setup_method(self):
        """Setup test health check manager."""
        self.manager = HealthCheckManager()
    
    @pytest.mark.asyncio
    async def test_manager_registration(self):
        """Test health check registration and management."""
        class MockHealthCheck(BaseHealthCheck):
            async def _check_health(self):
                return HealthCheckResult(
                    component=self.name,
                    status=HealthStatus.HEALTHY,
                    message="Mock check passed",
                    timestamp=datetime.utcnow()
                )
        
        # Register health check
        check = MockHealthCheck("mock_check")
        self.manager.register_health_check(check)
        
        assert "mock_check" in self.manager.health_checks
        assert "mock_check" in self.manager.check_history
        
        # Run single check
        result = await self.manager.check_single("mock_check")
        
        assert result is not None
        assert result.status == HealthStatus.HEALTHY
        assert "mock_check" in self.manager.last_results
        
        # Run all checks
        results = await self.manager.check_all()
        
        assert "mock_check" in results
        assert results["mock_check"].status == HealthStatus.HEALTHY
        
        # Test unregistration
        self.manager.unregister_health_check("mock_check")
        assert "mock_check" not in self.manager.health_checks
    
    def test_overall_status_calculation(self):
        """Test overall health status calculation."""
        # Mock some results
        self.manager.last_results = {
            "check1": HealthCheckResult("check1", HealthStatus.HEALTHY, "OK", datetime.utcnow()),
            "check2": HealthCheckResult("check2", HealthStatus.HEALTHY, "OK", datetime.utcnow()),
            "check3": HealthCheckResult("check3", HealthStatus.DEGRADED, "Warning", datetime.utcnow())
        }
        
        overall = self.manager.get_overall_status()
        assert overall == HealthStatus.DEGRADED  # Should be degraded due to check3
        
        # Test unhealthy scenario
        self.manager.last_results["check4"] = HealthCheckResult(
            "check4", HealthStatus.UNHEALTHY, "Failed", datetime.utcnow()
        )
        
        overall = self.manager.get_overall_status()
        assert overall == HealthStatus.UNHEALTHY  # Should be unhealthy due to check4
    
    def test_health_summary(self):
        """Test health summary generation."""
        # Add mock results
        self.manager.last_results = {
            "check1": HealthCheckResult("check1", HealthStatus.HEALTHY, "OK", datetime.utcnow()),
            "check2": HealthCheckResult("check2", HealthStatus.DEGRADED, "Warning", datetime.utcnow())
        }
        
        summary = self.manager.get_health_summary()
        
        assert summary["overall_status"] == HealthStatus.DEGRADED.value
        assert summary["total_checks"] == 2
        assert summary["status_counts"]["healthy"] == 1
        assert summary["status_counts"]["degraded"] == 1
        assert "components" in summary
        assert "last_check_times" in summary


class TestAlerting:
    """Test alerting functionality."""
    
    def test_alert_creation(self):
        """Test alert creation and serialization."""
        alert = Alert(
            id="test_alert_1",
            title="Test Alert",
            description="This is a test alert",
            severity=AlertSeverity.WARNING,
            source="test_system",
            timestamp=datetime.utcnow(),
            labels={"env": "test", "service": "api"}
        )
        
        assert alert.status == AlertStatus.ACTIVE
        assert alert.severity == AlertSeverity.WARNING
        
        # Test serialization
        alert_dict = alert.to_dict()
        assert alert_dict["id"] == "test_alert_1"
        assert alert_dict["severity"] == "warning"
        assert alert_dict["labels"]["env"] == "test"
    
    def test_alert_rule(self):
        """Test alert rule functionality."""
        # Create test rule
        rule = AlertRule(
            id="cpu_high",
            name="High CPU Usage",
            condition=lambda ctx: ctx.get("cpu_percent", 0) > 80,
            severity=AlertSeverity.WARNING,
            description="CPU usage is above 80%",
            labels={"type": "system"}
        )
        
        # Test condition evaluation
        context_high_cpu = {"cpu_percent": 85}
        context_normal_cpu = {"cpu_percent": 45}
        
        assert rule.condition(context_high_cpu) is True
        assert rule.condition(context_normal_cpu) is False
    
    @pytest.mark.asyncio
    async def test_alert_channels(self):
        """Test alert notification channels."""
        class TestAlertChannel(BaseAlertChannel):
            def __init__(self, name):
                super().__init__(name)
                self.sent_alerts = []
            
            async def send_alert(self, alert):
                self.sent_alerts.append(alert)
                return True
        
        # Create test channel
        channel = TestAlertChannel("test_channel")
        
        # Create test alert
        alert = Alert(
            id="test_alert",
            title="Test Alert",
            description="Test description",
            severity=AlertSeverity.INFO,
            source="test",
            timestamp=datetime.utcnow()
        )
        
        # Send alert
        success = await channel.send_alert(alert)
        
        assert success is True
        assert len(channel.sent_alerts) == 1
        assert channel.sent_alerts[0].id == "test_alert"


class TestAlertManager:
    """Test alert manager functionality."""
    
    def setup_method(self):
        """Setup test alert manager."""
        self.manager = AlertManager()
    
    def test_rule_management(self):
        """Test alert rule management."""
        # Create test rule
        rule = AlertRule(
            id="test_rule",
            name="Test Rule",
            condition=lambda ctx: ctx.get("test_value", 0) > 10,
            severity=AlertSeverity.ERROR,
            description="Test rule description"
        )
        
        # Add rule
        self.manager.add_rule(rule)
        assert "test_rule" in self.manager.rules
        
        # Remove rule
        self.manager.remove_rule("test_rule")
        assert "test_rule" not in self.manager.rules
    
    def test_channel_management(self):
        """Test notification channel management."""
        class MockChannel(BaseAlertChannel):
            async def send_alert(self, alert):
                return True
        
        # Add channel
        channel = MockChannel("test_channel")
        self.manager.add_channel(channel)
        
        assert "test_channel" in self.manager.channels
        
        # Test channel routing
        self.manager.set_channel_routing(AlertSeverity.ERROR, ["test_channel"])
        
        assert AlertSeverity.ERROR in self.manager.channel_routing
        assert "test_channel" in self.manager.channel_routing[AlertSeverity.ERROR]
    
    @pytest.mark.asyncio
    async def test_rule_evaluation(self):
        """Test alert rule evaluation."""
        # Create test rule that should fire
        rule = AlertRule(
            id="test_rule",
            name="Test Alert",
            condition=lambda ctx: ctx.get("error_count", 0) > 5,
            severity=AlertSeverity.WARNING,
            description="Too many errors detected"
        )
        
        self.manager.add_rule(rule)
        
        # Create test context that triggers the rule
        context = {"error_count": 10}
        
        # Evaluate rules
        await self.manager.evaluate_rules(context)
        
        # Check that alert was fired
        assert len(self.manager.active_alerts) > 0
        
        # Check alert details
        fired_alert = list(self.manager.active_alerts.values())[0]
        assert fired_alert.severity == AlertSeverity.WARNING
        assert fired_alert.source == "test_rule"
    
    def test_suppression(self):
        """Test alert suppression functionality."""
        # Test global suppression
        self.manager.suppress_global(True)
        assert self.manager.global_suppression is True
        
        self.manager.suppress_global(False)
        assert self.manager.global_suppression is False
        
        # Test rule suppression
        self.manager.suppress_rule("test_rule", True)
        assert "test_rule" in self.manager.suppressed_rules
        
        self.manager.suppress_rule("test_rule", False)
        assert "test_rule" not in self.manager.suppressed_rules
    
    @pytest.mark.asyncio
    async def test_alert_acknowledgment_resolution(self):
        """Test alert acknowledgment and resolution."""
        # Create and fire an alert
        alert = Alert(
            id="test_alert",
            title="Test Alert",
            description="Test",
            severity=AlertSeverity.INFO,
            source="test",
            timestamp=datetime.utcnow()
        )
        
        self.manager.active_alerts[alert.id] = alert
        
        # Test acknowledgment
        success = await self.manager.acknowledge_alert(alert.id, "test_user")
        assert success is True
        
        active_alert = self.manager.active_alerts[alert.id]
        assert active_alert.status == AlertStatus.ACKNOWLEDGED
        assert active_alert.acknowledged_by == "test_user"
        
        # Test resolution
        success = await self.manager.resolve_alert(alert.id)
        assert success is True
        assert alert.id not in self.manager.active_alerts
    
    def test_alert_summary(self):
        """Test alert summary generation."""
        # Add some test alerts
        alert1 = Alert("alert1", "Alert 1", "Description", AlertSeverity.ERROR, "source1", datetime.utcnow())
        alert2 = Alert("alert2", "Alert 2", "Description", AlertSeverity.WARNING, "source2", datetime.utcnow())
        
        self.manager.active_alerts = {"alert1": alert1, "alert2": alert2}
        
        summary = self.manager.get_alert_summary()
        
        assert summary["total_active_alerts"] == 2
        assert summary["active_by_severity"]["error"] == 1
        assert summary["active_by_severity"]["warning"] == 1
        assert "timestamp" in summary


@pytest.mark.integration
class TestMonitoringIntegration:
    """Integration tests for monitoring components."""
    
    @pytest.mark.asyncio
    async def test_end_to_end_monitoring(self):
        """Test complete monitoring workflow."""
        # Setup components
        collector = MetricsCollector()
        health_manager = HealthCheckManager()
        alert_manager = AlertManager()
        
        # Create mock health check
        class MockHealthCheck(BaseHealthCheck):
            def __init__(self, name, should_fail=False):
                super().__init__(name)
                self.should_fail = should_fail
            
            async def _check_health(self):
                status = HealthStatus.UNHEALTHY if self.should_fail else HealthStatus.HEALTHY
                message = "Failed" if self.should_fail else "OK"
                
                return HealthCheckResult(
                    component=self.name,
                    status=status,
                    message=message,
                    timestamp=datetime.utcnow()
                )
        
        # Register health checks
        health_check1 = MockHealthCheck("service1", should_fail=False)
        health_check2 = MockHealthCheck("service2", should_fail=True)
        
        health_manager.register_health_check(health_check1)
        health_manager.register_health_check(health_check2)
        
        # Create alert rule based on health checks
        rule = AlertRule(
            id="health_check_rule",
            name="Service Health Check Failed",
            condition=lambda ctx: any(
                result.get("status") == "unhealthy" 
                for result in ctx.get("health_results", {}).values()
            ),
            severity=AlertSeverity.ERROR,
            description="One or more services are unhealthy"
        )
        
        alert_manager.add_rule(rule)
        
        # Run health checks
        health_results = await health_manager.check_all()
        
        # Create context for alert evaluation
        context = {
            "health_results": {
                name: {"status": result.status.value}
                for name, result in health_results.items()
            }
        }
        
        # Evaluate alert rules
        await alert_manager.evaluate_rules(context)
        
        # Verify results
        assert len(health_results) == 2
        assert health_results["service1"].status == HealthStatus.HEALTHY
        assert health_results["service2"].status == HealthStatus.UNHEALTHY
        
        # Should have fired an alert due to service2 being unhealthy
        assert len(alert_manager.active_alerts) > 0
        
        fired_alert = list(alert_manager.active_alerts.values())[0]
        assert fired_alert.severity == AlertSeverity.ERROR


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
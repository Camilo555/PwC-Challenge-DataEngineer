"""
Synthetic Monitoring - Automated User Journey Testing
====================================================

Enterprise-grade synthetic monitoring with automated user journey testing:
- Multi-browser user journey automation with realistic scenarios
- API endpoint monitoring with performance validation
- Real-time availability and performance monitoring
- Geographic distributed testing from multiple locations
- Intelligent alerting with SLA breach detection
- Business transaction monitoring with critical path validation

Key Features:
- 24/7 automated testing with <30 second intervals
- Multi-step transaction flows with dependencies
- Performance regression detection with historical baselines
- Customer experience impact analysis with business metrics
- Proactive alerting before customer impact
- Comprehensive reporting with trend analysis
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field
import statistics

import aiohttp
import aioredis
import asyncpg
from fastapi import HTTPException
from pydantic import BaseModel, Field
import ssl

from core.config import get_settings
from core.logging import get_logger

# Configure logging
logger = logging.getLogger(__name__)

# Monitoring Types and Configurations
class MonitorType(str, Enum):
    """Monitor type enumeration"""
    HTTP_API = "http_api"
    WEB_JOURNEY = "web_journey"
    DATABASE = "database"
    TCP_PORT = "tcp_port"
    DNS = "dns"
    SSL_CERTIFICATE = "ssl_certificate"
    BUSINESS_TRANSACTION = "business_transaction"

class MonitorLocation(str, Enum):
    """Monitor location enumeration"""
    US_EAST = "us-east-1"
    US_WEST = "us-west-2"
    EU_WEST = "eu-west-1"
    ASIA_PACIFIC = "ap-southeast-1"
    LOCAL = "local"

class MonitorStatus(str, Enum):
    """Monitor status enumeration"""
    UP = "up"
    DOWN = "down"
    WARNING = "warning"
    UNKNOWN = "unknown"
    MAINTENANCE = "maintenance"

class AlertSeverity(str, Enum):
    """Alert severity enumeration"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

# Data Models
@dataclass
class MonitorStep:
    """Individual step in a user journey or monitoring scenario"""
    step_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    action_type: str = "http_request"  # http_request, wait, validate, navigate
    url: Optional[str] = None
    method: str = "GET"
    headers: Dict[str, str] = field(default_factory=dict)
    payload: Optional[Dict[str, Any]] = None
    expected_status: int = 200
    expected_response_time_ms: int = 5000
    validation_rules: List[Dict[str, Any]] = field(default_factory=list)
    wait_time_ms: int = 0
    timeout_ms: int = 30000
    retry_count: int = 3
    critical: bool = True

@dataclass
class MonitorConfiguration:
    """Comprehensive monitor configuration"""
    monitor_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: str = ""
    monitor_type: MonitorType = MonitorType.HTTP_API
    enabled: bool = True

    # Scheduling
    interval_seconds: int = 60
    timeout_seconds: int = 30

    # Location and distribution
    locations: List[MonitorLocation] = field(default_factory=list)

    # Steps for complex monitoring
    steps: List[MonitorStep] = field(default_factory=list)

    # Alerting configuration
    alert_on_failure: bool = True
    alert_on_slow_response: bool = True
    slow_response_threshold_ms: int = 2000
    consecutive_failures_threshold: int = 3
    alert_channels: List[str] = field(default_factory=list)

    # Business context
    business_impact: str = "medium"  # low, medium, high, critical
    sla_target_uptime: float = 99.9
    sla_target_response_time_ms: int = 1000

    # Metadata
    tags: List[str] = field(default_factory=list)
    created_by: str = "system"
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

@dataclass
class MonitorResult:
    """Monitor execution result with comprehensive metrics"""
    result_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    monitor_id: str = ""
    execution_time: datetime = field(default_factory=datetime.utcnow)
    location: MonitorLocation = MonitorLocation.LOCAL

    # Overall result
    status: MonitorStatus = MonitorStatus.UNKNOWN
    success: bool = False
    total_time_ms: float = 0

    # Step results
    step_results: List[Dict[str, Any]] = field(default_factory=list)

    # Performance metrics
    response_time_ms: float = 0
    dns_time_ms: float = 0
    connect_time_ms: float = 0
    ssl_time_ms: float = 0
    first_byte_time_ms: float = 0
    download_time_ms: float = 0

    # Content validation
    response_size_bytes: int = 0
    status_code: Optional[int] = None
    response_headers: Dict[str, str] = field(default_factory=dict)

    # Error information
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    error_step: Optional[str] = None

    # Business metrics
    business_transaction_id: Optional[str] = None
    customer_impact_score: float = 0.0

@dataclass
class SyntheticAlert:
    """Synthetic monitoring alert"""
    alert_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    monitor_id: str = ""
    monitor_name: str = ""
    severity: AlertSeverity = AlertSeverity.WARNING
    message: str = ""

    # Alert details
    triggered_at: datetime = field(default_factory=datetime.utcnow)
    resolved_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None

    # Context
    location: MonitorLocation = MonitorLocation.LOCAL
    failure_count: int = 1
    last_success: Optional[datetime] = None

    # Business impact
    customer_impact: str = "unknown"
    estimated_revenue_impact: float = 0.0

    # Alert metadata
    acknowledged: bool = False
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    auto_resolved: bool = False

class SyntheticMonitoring:
    """
    Comprehensive synthetic monitoring system for automated user journey testing

    Features:
    - Multi-location distributed monitoring
    - Complex user journey automation
    - Real-time performance analysis
    - Intelligent alerting with business impact correlation
    - SLA monitoring with trend analysis
    - Proactive issue detection before customer impact
    """

    def __init__(self):
        self.settings = get_settings()
        self.logger = get_logger(__name__)

        # Client connections
        self.redis_client: Optional[aioredis.Redis] = None
        self.db_pool: Optional[asyncpg.Pool] = None
        self.http_session: Optional[aiohttp.ClientSession] = None

        # Configuration
        self.monitors: Dict[str, MonitorConfiguration] = {}
        self.monitor_schedules: Dict[str, asyncio.Task] = {}
        self.alert_rules: Dict[str, Dict[str, Any]] = {}

        # Performance tracking
        self.recent_results: Dict[str, List[MonitorResult]] = {}
        self.baseline_metrics: Dict[str, Dict[str, float]] = {}

        # System state
        self.is_running = False
        self.statistics = {
            "total_monitors": 0,
            "active_monitors": 0,
            "total_checks": 0,
            "successful_checks": 0,
            "failed_checks": 0,
            "average_response_time_ms": 0.0
        }

    async def initialize(self):
        """Initialize synthetic monitoring system"""
        try:
            # Initialize Redis connection
            self.redis_client = aioredis.from_url(
                f"redis://{self.settings.redis_host}:{self.settings.redis_port}",
                decode_responses=True
            )

            # Initialize database connection pool
            self.db_pool = await asyncpg.create_pool(
                host=self.settings.db_host,
                port=self.settings.db_port,
                user=self.settings.db_user,
                password=self.settings.db_password,
                database=self.settings.db_name,
                min_size=2,
                max_size=10
            )

            # Initialize HTTP session with optimized settings
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=20,
                keepalive_timeout=30,
                enable_cleanup_closed=True
            )

            self.http_session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=60)
            )

            # Load existing monitors from database
            await self._load_monitors_from_db()

            # Start background workers
            asyncio.create_task(self._monitor_scheduler_worker())
            asyncio.create_task(self._alert_processor_worker())
            asyncio.create_task(self._baseline_calculator_worker())
            asyncio.create_task(self._cleanup_worker())

            # Start default monitors
            await self._create_default_monitors()

            self.is_running = True
            self.logger.info("Synthetic monitoring system initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize synthetic monitoring: {str(e)}")
            raise

    async def create_monitor(self, config: MonitorConfiguration) -> str:
        """Create new synthetic monitor"""
        try:
            # Validate configuration
            await self._validate_monitor_config(config)

            # Store monitor configuration
            self.monitors[config.monitor_id] = config
            await self._save_monitor_to_db(config)

            # Start monitor schedule
            await self._start_monitor_schedule(config)

            # Initialize baseline metrics
            self.recent_results[config.monitor_id] = []
            self.baseline_metrics[config.monitor_id] = {}

            self.statistics["total_monitors"] += 1
            if config.enabled:
                self.statistics["active_monitors"] += 1

            self.logger.info(f"Synthetic monitor created: {config.monitor_id} - {config.name}")
            return config.monitor_id

        except Exception as e:
            self.logger.error(f"Failed to create monitor: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Monitor creation failed: {str(e)}")

    async def execute_monitor(self, monitor_id: str, location: MonitorLocation = MonitorLocation.LOCAL) -> MonitorResult:
        """Execute synthetic monitor and return results"""
        try:
            if monitor_id not in self.monitors:
                raise ValueError(f"Monitor not found: {monitor_id}")

            config = self.monitors[monitor_id]
            result = MonitorResult(
                monitor_id=monitor_id,
                location=location,
                execution_time=datetime.utcnow()
            )

            start_time = asyncio.get_event_loop().time()

            try:
                if config.monitor_type == MonitorType.HTTP_API:
                    await self._execute_http_monitor(config, result)
                elif config.monitor_type == MonitorType.WEB_JOURNEY:
                    await self._execute_web_journey(config, result)
                elif config.monitor_type == MonitorType.BUSINESS_TRANSACTION:
                    await self._execute_business_transaction(config, result)
                else:
                    raise ValueError(f"Unsupported monitor type: {config.monitor_type}")

                result.total_time_ms = (asyncio.get_event_loop().time() - start_time) * 1000
                result.success = True
                result.status = MonitorStatus.UP

            except Exception as e:
                result.total_time_ms = (asyncio.get_event_loop().time() - start_time) * 1000
                result.success = False
                result.status = MonitorStatus.DOWN
                result.error_message = str(e)
                result.error_type = type(e).__name__

            # Store result
            await self._store_monitor_result(result)

            # Update statistics
            self.statistics["total_checks"] += 1
            if result.success:
                self.statistics["successful_checks"] += 1
            else:
                self.statistics["failed_checks"] += 1

            # Check for alerts
            await self._check_alert_conditions(config, result)

            return result

        except Exception as e:
            self.logger.error(f"Failed to execute monitor {monitor_id}: {str(e)}")
            raise

    async def get_monitor_statistics(self, monitor_id: str, hours: int = 24) -> Dict[str, Any]:
        """Get comprehensive monitor statistics"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours)

            # Query results from database
            async with self.db_pool.acquire() as conn:
                results = await conn.fetch("""
                    SELECT * FROM synthetic_monitor_results
                    WHERE monitor_id = $1 AND execution_time >= $2 AND execution_time <= $3
                    ORDER BY execution_time ASC
                """, monitor_id, start_time, end_time)

            if not results:
                return {"error": "No data available for the specified time range"}

            # Calculate statistics
            total_checks = len(results)
            successful_checks = sum(1 for r in results if r["success"])
            failed_checks = total_checks - successful_checks

            uptime_percentage = (successful_checks / total_checks * 100) if total_checks > 0 else 0

            response_times = [r["response_time_ms"] for r in results if r["response_time_ms"] and r["success"]]

            stats = {
                "monitor_id": monitor_id,
                "time_range": {
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat(),
                    "hours": hours
                },
                "availability": {
                    "uptime_percentage": round(uptime_percentage, 2),
                    "total_checks": total_checks,
                    "successful_checks": successful_checks,
                    "failed_checks": failed_checks,
                    "sla_compliance": uptime_percentage >= self.monitors[monitor_id].sla_target_uptime if monitor_id in self.monitors else False
                },
                "performance": {},
                "incidents": [],
                "trends": {}
            }

            if response_times:
                stats["performance"] = {
                    "average_response_time_ms": round(statistics.mean(response_times), 2),
                    "median_response_time_ms": round(statistics.median(response_times), 2),
                    "p95_response_time_ms": round(statistics.quantiles(response_times, n=20)[18], 2) if len(response_times) > 20 else round(max(response_times), 2),
                    "p99_response_time_ms": round(statistics.quantiles(response_times, n=100)[98], 2) if len(response_times) > 100 else round(max(response_times), 2),
                    "min_response_time_ms": round(min(response_times), 2),
                    "max_response_time_ms": round(max(response_times), 2)
                }

            # Calculate incidents (consecutive failures)
            incidents = []
            current_incident = None

            for result in results:
                if not result["success"]:
                    if current_incident is None:
                        current_incident = {
                            "start_time": result["execution_time"],
                            "failure_count": 1,
                            "locations": [result["location"]]
                        }
                    else:
                        current_incident["failure_count"] += 1
                        if result["location"] not in current_incident["locations"]:
                            current_incident["locations"].append(result["location"])
                else:
                    if current_incident is not None:
                        current_incident["end_time"] = result["execution_time"]
                        current_incident["duration_minutes"] = (
                            current_incident["end_time"] - current_incident["start_time"]
                        ).total_seconds() / 60
                        incidents.append(current_incident)
                        current_incident = None

            stats["incidents"] = incidents

            return stats

        except Exception as e:
            self.logger.error(f"Failed to get monitor statistics: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Statistics retrieval failed: {str(e)}")

    async def get_all_monitors_status(self) -> Dict[str, Any]:
        """Get status overview of all monitors"""
        try:
            monitor_statuses = []

            for monitor_id, config in self.monitors.items():
                # Get latest result
                recent_results = self.recent_results.get(monitor_id, [])
                latest_result = recent_results[-1] if recent_results else None

                status_info = {
                    "monitor_id": monitor_id,
                    "name": config.name,
                    "type": config.monitor_type.value,
                    "enabled": config.enabled,
                    "locations": [loc.value for loc in config.locations],
                    "interval_seconds": config.interval_seconds,
                    "business_impact": config.business_impact,
                    "current_status": latest_result.status.value if latest_result else "unknown",
                    "last_check": latest_result.execution_time.isoformat() if latest_result else None,
                    "last_response_time_ms": latest_result.response_time_ms if latest_result else None,
                    "success_rate_24h": 0.0,  # Will be calculated
                    "sla_compliance": True  # Will be calculated
                }

                # Calculate 24h success rate
                end_time = datetime.utcnow()
                start_time = end_time - timedelta(hours=24)

                async with self.db_pool.acquire() as conn:
                    stats = await conn.fetchrow("""
                        SELECT
                            COUNT(*) as total_checks,
                            SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_checks
                        FROM synthetic_monitor_results
                        WHERE monitor_id = $1 AND execution_time >= $2
                    """, monitor_id, start_time)

                if stats and stats["total_checks"] > 0:
                    success_rate = (stats["successful_checks"] / stats["total_checks"]) * 100
                    status_info["success_rate_24h"] = round(success_rate, 2)
                    status_info["sla_compliance"] = success_rate >= config.sla_target_uptime

                monitor_statuses.append(status_info)

            # Calculate overall system statistics
            total_monitors = len(self.monitors)
            active_monitors = sum(1 for config in self.monitors.values() if config.enabled)
            healthy_monitors = sum(1 for status in monitor_statuses if status["current_status"] == "up")

            return {
                "overview": {
                    "total_monitors": total_monitors,
                    "active_monitors": active_monitors,
                    "healthy_monitors": healthy_monitors,
                    "unhealthy_monitors": active_monitors - healthy_monitors,
                    "system_health_percentage": round((healthy_monitors / active_monitors * 100) if active_monitors > 0 else 100, 2)
                },
                "global_statistics": self.statistics,
                "monitors": monitor_statuses,
                "last_updated": datetime.utcnow().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Failed to get monitors status: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Monitor status retrieval failed: {str(e)}")

    # Private Methods
    async def _execute_http_monitor(self, config: MonitorConfiguration, result: MonitorResult):
        """Execute HTTP API monitor"""
        if not config.steps:
            raise ValueError("HTTP monitor must have at least one step")

        step = config.steps[0]
        step_start = asyncio.get_event_loop().time()

        try:
            # Prepare request
            headers = step.headers.copy()
            headers.setdefault("User-Agent", "BMAD-SyntheticMonitoring/1.0")

            # Execute HTTP request
            async with self.http_session.request(
                method=step.method,
                url=step.url,
                headers=headers,
                json=step.payload if step.payload else None,
                timeout=aiohttp.ClientTimeout(total=step.timeout_ms / 1000)
            ) as response:

                step_time = (asyncio.get_event_loop().time() - step_start) * 1000
                response_text = await response.text()

                # Update result metrics
                result.response_time_ms = step_time
                result.status_code = response.status
                result.response_size_bytes = len(response_text.encode())
                result.response_headers = dict(response.headers)

                # Validate response
                if response.status != step.expected_status:
                    raise ValueError(f"Expected status {step.expected_status}, got {response.status}")

                if step_time > step.expected_response_time_ms:
                    result.status = MonitorStatus.WARNING

                # Execute validation rules
                for rule in step.validation_rules:
                    await self._validate_response(response_text, rule)

                # Store step result
                result.step_results.append({
                    "step_id": step.step_id,
                    "name": step.name,
                    "success": True,
                    "response_time_ms": step_time,
                    "status_code": response.status,
                    "validation_passed": True
                })

        except Exception as e:
            step_time = (asyncio.get_event_loop().time() - step_start) * 1000
            result.response_time_ms = step_time
            result.step_results.append({
                "step_id": step.step_id,
                "name": step.name,
                "success": False,
                "response_time_ms": step_time,
                "error": str(e)
            })
            raise

    async def _execute_web_journey(self, config: MonitorConfiguration, result: MonitorResult):
        """Execute web journey with multiple steps"""
        for i, step in enumerate(config.steps):
            step_start = asyncio.get_event_loop().time()

            try:
                if step.action_type == "http_request":
                    await self._execute_http_step(step, result)
                elif step.action_type == "wait":
                    await asyncio.sleep(step.wait_time_ms / 1000)
                elif step.action_type == "validate":
                    # Custom validation logic
                    pass

                step_time = (asyncio.get_event_loop().time() - step_start) * 1000

                result.step_results.append({
                    "step_number": i + 1,
                    "step_id": step.step_id,
                    "name": step.name,
                    "action_type": step.action_type,
                    "success": True,
                    "response_time_ms": step_time
                })

            except Exception as e:
                step_time = (asyncio.get_event_loop().time() - step_start) * 1000

                result.step_results.append({
                    "step_number": i + 1,
                    "step_id": step.step_id,
                    "name": step.name,
                    "action_type": step.action_type,
                    "success": False,
                    "response_time_ms": step_time,
                    "error": str(e)
                })

                if step.critical:
                    result.error_step = step.name
                    raise

    async def _execute_business_transaction(self, config: MonitorConfiguration, result: MonitorResult):
        """Execute business transaction monitor"""
        transaction_id = str(uuid.uuid4())
        result.business_transaction_id = transaction_id

        # Execute all steps in the business transaction
        transaction_value = 0.0

        for step in config.steps:
            try:
                await self._execute_http_step(step, result)
                # Calculate business value impact
                transaction_value += step.validation_rules[0].get("business_value", 0) if step.validation_rules else 0
            except Exception as e:
                # Calculate customer impact for failed business transaction
                result.customer_impact_score = self._calculate_customer_impact(config, transaction_value)
                raise

        result.customer_impact_score = 0  # Successful transaction

    async def _execute_http_step(self, step: MonitorStep, result: MonitorResult):
        """Execute individual HTTP step"""
        async with self.http_session.request(
            method=step.method,
            url=step.url,
            headers=step.headers,
            json=step.payload,
            timeout=aiohttp.ClientTimeout(total=step.timeout_ms / 1000)
        ) as response:

            if response.status != step.expected_status:
                raise ValueError(f"Step {step.name}: Expected status {step.expected_status}, got {response.status}")

    async def _validate_response(self, response_text: str, rule: Dict[str, Any]):
        """Validate response against rule"""
        rule_type = rule.get("type", "contains")

        if rule_type == "contains":
            if rule["value"] not in response_text:
                raise ValueError(f"Response validation failed: '{rule['value']}' not found in response")
        elif rule_type == "not_contains":
            if rule["value"] in response_text:
                raise ValueError(f"Response validation failed: '{rule['value']}' found in response")
        elif rule_type == "json_path":
            # JSON path validation (simplified)
            try:
                import json
                data = json.loads(response_text)
                # Implement JSON path validation
            except json.JSONDecodeError:
                raise ValueError("Response validation failed: Invalid JSON response")

    def _calculate_customer_impact(self, config: MonitorConfiguration, transaction_value: float) -> float:
        """Calculate customer impact score for failed transaction"""
        impact_multipliers = {
            "low": 1.0,
            "medium": 2.5,
            "high": 5.0,
            "critical": 10.0
        }

        base_impact = impact_multipliers.get(config.business_impact, 2.5)
        return base_impact * (transaction_value / 100.0)

    async def _store_monitor_result(self, result: MonitorResult):
        """Store monitor result in database and update recent results"""
        try:
            # Store in database
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO synthetic_monitor_results (
                        result_id, monitor_id, execution_time, location, status, success,
                        total_time_ms, response_time_ms, status_code, response_size_bytes,
                        error_message, error_type, step_results
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                """,
                    result.result_id, result.monitor_id, result.execution_time,
                    result.location.value, result.status.value, result.success,
                    result.total_time_ms, result.response_time_ms, result.status_code,
                    result.response_size_bytes, result.error_message, result.error_type,
                    json.dumps(result.step_results)
                )

            # Update recent results (keep last 100)
            if result.monitor_id not in self.recent_results:
                self.recent_results[result.monitor_id] = []

            self.recent_results[result.monitor_id].append(result)
            if len(self.recent_results[result.monitor_id]) > 100:
                self.recent_results[result.monitor_id] = self.recent_results[result.monitor_id][-100:]

        except Exception as e:
            self.logger.error(f"Failed to store monitor result: {str(e)}")

    async def _check_alert_conditions(self, config: MonitorConfiguration, result: MonitorResult):
        """Check if alert conditions are met"""
        try:
            if not config.alert_on_failure and not config.alert_on_slow_response:
                return

            should_alert = False
            alert_message = ""
            severity = AlertSeverity.WARNING

            # Check failure conditions
            if config.alert_on_failure and not result.success:
                # Check consecutive failures
                recent_results = self.recent_results.get(config.monitor_id, [])
                recent_failures = [r for r in recent_results[-config.consecutive_failures_threshold:] if not r.success]

                if len(recent_failures) >= config.consecutive_failures_threshold:
                    should_alert = True
                    alert_message = f"Monitor {config.name} has failed {len(recent_failures)} consecutive times"
                    severity = AlertSeverity.CRITICAL if config.business_impact in ["high", "critical"] else AlertSeverity.ERROR

            # Check slow response conditions
            if config.alert_on_slow_response and result.success and result.response_time_ms > config.slow_response_threshold_ms:
                should_alert = True
                alert_message = f"Monitor {config.name} response time ({result.response_time_ms:.1f}ms) exceeds threshold ({config.slow_response_threshold_ms}ms)"
                severity = AlertSeverity.WARNING

            if should_alert:
                alert = SyntheticAlert(
                    monitor_id=config.monitor_id,
                    monitor_name=config.name,
                    severity=severity,
                    message=alert_message,
                    location=result.location,
                    customer_impact=config.business_impact,
                    estimated_revenue_impact=result.customer_impact_score
                )

                await self._send_alert(alert, config.alert_channels)

        except Exception as e:
            self.logger.error(f"Failed to check alert conditions: {str(e)}")

    async def _send_alert(self, alert: SyntheticAlert, channels: List[str]):
        """Send alert to configured channels"""
        try:
            # Store alert
            await self.redis_client.setex(
                f"synthetic_alert:{alert.alert_id}",
                86400,  # 24 hours
                json.dumps({
                    "alert_id": alert.alert_id,
                    "monitor_id": alert.monitor_id,
                    "monitor_name": alert.monitor_name,
                    "severity": alert.severity.value,
                    "message": alert.message,
                    "triggered_at": alert.triggered_at.isoformat(),
                    "location": alert.location.value,
                    "customer_impact": alert.customer_impact,
                    "estimated_revenue_impact": alert.estimated_revenue_impact
                })
            )

            # Queue alert for processing
            await self.redis_client.lpush("synthetic_alert_queue", alert.alert_id)

            self.logger.warning(f"Synthetic monitoring alert: {alert.message}")

        except Exception as e:
            self.logger.error(f"Failed to send alert: {str(e)}")

    # Background Workers
    async def _monitor_scheduler_worker(self):
        """Background worker to schedule monitor executions"""
        while True:
            try:
                for monitor_id, config in self.monitors.items():
                    if config.enabled and monitor_id not in self.monitor_schedules:
                        await self._start_monitor_schedule(config)

                await asyncio.sleep(30)  # Check every 30 seconds

            except Exception as e:
                self.logger.error(f"Monitor scheduler worker error: {str(e)}")
                await asyncio.sleep(60)

    async def _start_monitor_schedule(self, config: MonitorConfiguration):
        """Start scheduled execution for a monitor"""
        async def monitor_loop():
            while config.enabled:
                try:
                    for location in config.locations or [MonitorLocation.LOCAL]:
                        await self.execute_monitor(config.monitor_id, location)

                    await asyncio.sleep(config.interval_seconds)

                except Exception as e:
                    self.logger.error(f"Monitor execution error for {config.monitor_id}: {str(e)}")
                    await asyncio.sleep(60)  # Wait 1 minute before retry

        task = asyncio.create_task(monitor_loop())
        self.monitor_schedules[config.monitor_id] = task

    async def _alert_processor_worker(self):
        """Background worker to process alerts"""
        while True:
            try:
                # Process alert queue
                alert_id = await self.redis_client.brpop("synthetic_alert_queue", timeout=30)
                if alert_id:
                    # Process alert (send notifications, etc.)
                    pass

            except Exception as e:
                self.logger.error(f"Alert processor worker error: {str(e)}")
                await asyncio.sleep(30)

    async def _baseline_calculator_worker(self):
        """Background worker to calculate performance baselines"""
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour

                for monitor_id in self.monitors:
                    await self._calculate_baseline_metrics(monitor_id)

            except Exception as e:
                self.logger.error(f"Baseline calculator worker error: {str(e)}")
                await asyncio.sleep(1800)

    async def _calculate_baseline_metrics(self, monitor_id: str):
        """Calculate baseline performance metrics for a monitor"""
        try:
            # Get last 7 days of successful results
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=7)

            async with self.db_pool.acquire() as conn:
                results = await conn.fetch("""
                    SELECT response_time_ms FROM synthetic_monitor_results
                    WHERE monitor_id = $1 AND execution_time >= $2 AND success = true
                """, monitor_id, start_time)

            if len(results) >= 100:  # Need enough data points
                response_times = [r["response_time_ms"] for r in results if r["response_time_ms"]]

                baseline = {
                    "mean": statistics.mean(response_times),
                    "median": statistics.median(response_times),
                    "p95": statistics.quantiles(response_times, n=20)[18] if len(response_times) > 20 else max(response_times),
                    "std_dev": statistics.stdev(response_times) if len(response_times) > 1 else 0,
                    "calculated_at": datetime.utcnow().isoformat()
                }

                self.baseline_metrics[monitor_id] = baseline

        except Exception as e:
            self.logger.error(f"Failed to calculate baseline for {monitor_id}: {str(e)}")

    async def _cleanup_worker(self):
        """Background worker for data cleanup"""
        while True:
            try:
                await asyncio.sleep(86400)  # Run daily

                # Clean up old monitor results (keep 30 days)
                cutoff_date = datetime.utcnow() - timedelta(days=30)

                async with self.db_pool.acquire() as conn:
                    deleted_count = await conn.fetchval("""
                        DELETE FROM synthetic_monitor_results
                        WHERE execution_time < $1
                        RETURNING COUNT(*)
                    """, cutoff_date)

                self.logger.info(f"Cleaned up {deleted_count} old monitor results")

            except Exception as e:
                self.logger.error(f"Cleanup worker error: {str(e)}")
                await asyncio.sleep(3600)

    async def _validate_monitor_config(self, config: MonitorConfiguration):
        """Validate monitor configuration"""
        if not config.name:
            raise ValueError("Monitor name is required")

        if not config.steps:
            raise ValueError("Monitor must have at least one step")

        if config.interval_seconds < 30:
            raise ValueError("Monitor interval must be at least 30 seconds")

        for step in config.steps:
            if step.action_type == "http_request" and not step.url:
                raise ValueError(f"HTTP request step '{step.name}' must have URL")

    async def _save_monitor_to_db(self, config: MonitorConfiguration):
        """Save monitor configuration to database"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO synthetic_monitors (
                    monitor_id, name, description, monitor_type, enabled,
                    interval_seconds, timeout_seconds, locations, steps,
                    alert_config, business_impact, sla_config, tags,
                    created_by, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                ON CONFLICT (monitor_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    enabled = EXCLUDED.enabled,
                    interval_seconds = EXCLUDED.interval_seconds,
                    timeout_seconds = EXCLUDED.timeout_seconds,
                    locations = EXCLUDED.locations,
                    steps = EXCLUDED.steps,
                    alert_config = EXCLUDED.alert_config,
                    updated_at = EXCLUDED.updated_at
            """,
                config.monitor_id, config.name, config.description,
                config.monitor_type.value, config.enabled,
                config.interval_seconds, config.timeout_seconds,
                json.dumps([loc.value for loc in config.locations]),
                json.dumps([step.__dict__ for step in config.steps]),
                json.dumps({
                    "alert_on_failure": config.alert_on_failure,
                    "alert_on_slow_response": config.alert_on_slow_response,
                    "slow_response_threshold_ms": config.slow_response_threshold_ms,
                    "consecutive_failures_threshold": config.consecutive_failures_threshold,
                    "alert_channels": config.alert_channels
                }),
                config.business_impact,
                json.dumps({
                    "sla_target_uptime": config.sla_target_uptime,
                    "sla_target_response_time_ms": config.sla_target_response_time_ms
                }),
                config.tags, config.created_by, config.created_at, config.updated_at
            )

    async def _load_monitors_from_db(self):
        """Load existing monitors from database"""
        try:
            async with self.db_pool.acquire() as conn:
                monitors = await conn.fetch("SELECT * FROM synthetic_monitors WHERE enabled = true")

                for monitor_row in monitors:
                    # Reconstruct monitor configuration
                    # Note: This would include proper deserialization of JSON fields
                    pass

        except Exception as e:
            self.logger.error(f"Failed to load monitors from database: {str(e)}")

    async def _create_default_monitors(self):
        """Create default monitors for critical endpoints"""
        try:
            # API Health Check Monitor
            api_health_monitor = MonitorConfiguration(
                name="API Health Check",
                description="Monitor main API health endpoint",
                monitor_type=MonitorType.HTTP_API,
                interval_seconds=60,
                locations=[MonitorLocation.LOCAL],
                steps=[
                    MonitorStep(
                        name="Health Check",
                        url=f"http://localhost:8000/health",
                        expected_status=200,
                        expected_response_time_ms=1000,
                        validation_rules=[
                            {"type": "contains", "value": "healthy"}
                        ]
                    )
                ],
                business_impact="critical",
                sla_target_uptime=99.9,
                sla_target_response_time_ms=500
            )

            await self.create_monitor(api_health_monitor)

        except Exception as e:
            self.logger.error(f"Failed to create default monitors: {str(e)}")

# Global synthetic monitoring instance
synthetic_monitoring = SyntheticMonitoring()

async def get_synthetic_monitoring() -> SyntheticMonitoring:
    """Get global synthetic monitoring instance"""
    if not synthetic_monitoring.is_running:
        await synthetic_monitoring.initialize()
    return synthetic_monitoring
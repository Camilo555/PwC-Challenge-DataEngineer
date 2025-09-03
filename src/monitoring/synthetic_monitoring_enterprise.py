"""
Enterprise-Grade Synthetic Monitoring System
Provides comprehensive synthetic monitoring with multi-tier health checks,
business transaction monitoring, SLA tracking, and predictive alerting.
"""

import asyncio
import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any
from urllib.parse import urljoin

import aiohttp
import psycopg2

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    # Mock Redis for when it's not available
    class MockRedis:
        def __init__(self, *args, **kwargs):
            pass
        def get(self, key):
            return None
        def set(self, key, value, ex=None):
            return True
        def delete(self, key):
            return True
        def ping(self):
            return True
        def close(self):
            pass
    redis = type('MockRedisModule', (), {'Redis': MockRedis})()

from core.config import settings
from core.distributed_tracing import get_tracing_manager
from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring

logger = get_logger(__name__)


class CheckType(Enum):
    """Types of synthetic checks."""

    HTTP_API = "http_api"
    DATABASE = "database"
    CACHE = "cache"
    MESSAGE_QUEUE = "message_queue"
    BUSINESS_TRANSACTION = "business_transaction"
    DATA_PIPELINE = "data_pipeline"
    EXTERNAL_SERVICE = "external_service"
    SECURITY = "security"


class CheckSeverity(Enum):
    """Severity levels for checks."""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class CheckStatus(Enum):
    """Status of checks."""

    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class SLATarget:
    """SLA target definition."""

    availability: float = 99.9  # Percentage
    response_time_p95: float = 500.0  # ms
    error_rate: float = 0.1  # Percentage
    monitoring_window: int = 86400  # seconds (24 hours)


@dataclass
class CheckResult:
    """Result of a synthetic check."""

    check_name: str
    check_type: CheckType
    status: CheckStatus
    response_time_ms: float
    success: bool
    timestamp: datetime
    error_message: str | None = None
    response_code: int | None = None
    response_size: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    business_impact: str | None = None
    correlation_id: str | None = None


@dataclass
class BusinessTransaction:
    """Definition of a business transaction to monitor."""

    transaction_id: str
    name: str
    description: str
    steps: list[dict[str, Any]]
    sla_target: SLATarget
    critical_for_business: bool = True
    frequency_seconds: int = 300  # 5 minutes
    timeout_seconds: int = 30
    retry_count: int = 2
    dependencies: list[str] = field(default_factory=list)


@dataclass
class SyntheticCheck:
    """Definition of a synthetic check."""

    check_id: str
    name: str
    check_type: CheckType
    severity: CheckSeverity
    frequency_seconds: int
    timeout_seconds: int
    retry_count: int
    enabled: bool = True

    # Check configuration
    endpoint: str | None = None
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    payload: dict[str, Any] | None = None
    expected_status: int = 200
    expected_response_contains: str | None = None

    # Database check config
    database_query: str | None = None
    expected_row_count: int | None = None

    # Custom validation function
    custom_validator: Callable | None = None

    # SLA and alerting
    sla_target: SLATarget | None = None
    alert_on_failure: bool = True
    escalation_minutes: int = 5

    # Tags and metadata
    tags: dict[str, str] = field(default_factory=dict)
    business_service: str | None = None
    team_owner: str | None = None


class SyntheticMonitoringEnterprise:
    """
    Enterprise-grade synthetic monitoring system.

    Features:
    - Multi-tier health checks (infrastructure, application, business)
    - Business transaction monitoring
    - SLA tracking and violation detection
    - Predictive alerting and anomaly detection
    - Circuit breaker patterns
    - Dependency mapping and impact analysis
    - Real-time dashboards and reporting
    """

    def __init__(
        self,
        service_name: str = "synthetic-monitoring",
        datadog_monitoring: DatadogMonitoring | None = None,
    ):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")
        self.tracing_manager = get_tracing_manager()

        # Check registry
        self.checks: dict[str, SyntheticCheck] = {}
        self.business_transactions: dict[str, BusinessTransaction] = {}

        # Results storage and analysis
        self.check_results: dict[str, list[CheckResult]] = {}
        self.sla_violations: list[dict[str, Any]] = []
        self.incident_timeline: list[dict[str, Any]] = []

        # Circuit breakers and throttling
        self.circuit_breakers: dict[str, dict[str, Any]] = {}
        self.rate_limiters: dict[str, dict[str, Any]] = {}

        # Real-time monitoring
        self.running_tasks: dict[str, asyncio.Task] = {}
        self.monitoring_active = False

        # HTTP session for efficiency
        self.http_session: aiohttp.ClientSession | None = None

        # Initialize default checks and transactions
        self._initialize_default_checks()
        self._initialize_business_transactions()

        self.logger.info(f"Enterprise synthetic monitoring initialized for {service_name}")

    async def start_monitoring(self):
        """Start all synthetic monitoring tasks."""
        try:
            self.monitoring_active = True

            # Create HTTP session
            timeout = aiohttp.ClientTimeout(total=30)
            self.http_session = aiohttp.ClientSession(timeout=timeout)

            # Start individual check tasks
            for check_id, check in self.checks.items():
                if check.enabled:
                    task = asyncio.create_task(self._run_check_loop(check))
                    self.running_tasks[check_id] = task
                    self.logger.info(f"Started monitoring task for check: {check_id}")

            # Start business transaction monitoring
            for transaction_id, transaction in self.business_transactions.items():
                task = asyncio.create_task(self._run_transaction_loop(transaction))
                self.running_tasks[f"tx_{transaction_id}"] = task
                self.logger.info(f"Started transaction monitoring: {transaction_id}")

            # Start SLA monitoring task
            sla_task = asyncio.create_task(self._sla_monitoring_loop())
            self.running_tasks["sla_monitor"] = sla_task

            # Start anomaly detection task
            anomaly_task = asyncio.create_task(self._anomaly_detection_loop())
            self.running_tasks["anomaly_detector"] = anomaly_task

            self.logger.info("All synthetic monitoring tasks started")

        except Exception as e:
            self.logger.error(f"Failed to start synthetic monitoring: {str(e)}")
            raise

    async def stop_monitoring(self):
        """Stop all monitoring tasks and cleanup."""
        try:
            self.monitoring_active = False

            # Cancel all running tasks
            for task_id, task in self.running_tasks.items():
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    self.logger.info(f"Stopped monitoring task: {task_id}")

            # Close HTTP session
            if self.http_session:
                await self.http_session.close()
                self.http_session = None

            self.running_tasks.clear()
            self.logger.info("Synthetic monitoring stopped")

        except Exception as e:
            self.logger.error(f"Error stopping synthetic monitoring: {str(e)}")

    def _initialize_default_checks(self):
        """Initialize default system checks."""

        default_checks = [
            # API Health Checks
            SyntheticCheck(
                check_id="api_health",
                name="API Health Check",
                check_type=CheckType.HTTP_API,
                severity=CheckSeverity.CRITICAL,
                frequency_seconds=30,
                timeout_seconds=10,
                retry_count=2,
                endpoint=f"{getattr(settings, 'api_base_url', 'http://localhost:8000')}/health",
                method="GET",
                expected_status=200,
                expected_response_contains="healthy",
                sla_target=SLATarget(availability=99.9, response_time_p95=200.0),
                business_service="core_api",
                team_owner="platform_team",
            ),
            SyntheticCheck(
                check_id="api_auth",
                name="API Authentication Check",
                check_type=CheckType.SECURITY,
                severity=CheckSeverity.HIGH,
                frequency_seconds=60,
                timeout_seconds=15,
                retry_count=1,
                endpoint=f"{getattr(settings, 'api_base_url', 'http://localhost:8000')}/auth/verify",
                method="POST",
                headers={"Authorization": "Bearer test_token"},
                expected_status=401,  # Should fail with test token
                business_service="authentication",
                team_owner="security_team",
            ),
            # Database Checks
            SyntheticCheck(
                check_id="database_connectivity",
                name="Primary Database Connectivity",
                check_type=CheckType.DATABASE,
                severity=CheckSeverity.CRITICAL,
                frequency_seconds=60,
                timeout_seconds=5,
                retry_count=3,
                database_query="SELECT 1 as status",
                expected_row_count=1,
                sla_target=SLATarget(availability=99.95, response_time_p95=100.0),
                business_service="database",
                team_owner="data_team",
            ),
            SyntheticCheck(
                check_id="database_performance",
                name="Database Query Performance",
                check_type=CheckType.DATABASE,
                severity=CheckSeverity.HIGH,
                frequency_seconds=300,
                timeout_seconds=10,
                retry_count=2,
                database_query="SELECT COUNT(*) FROM customers WHERE created_at >= NOW() - INTERVAL '1 hour'",
                business_service="database",
                team_owner="data_team",
            ),
            # Cache Checks
            SyntheticCheck(
                check_id="redis_connectivity",
                name="Redis Cache Connectivity",
                check_type=CheckType.CACHE,
                severity=CheckSeverity.HIGH,
                frequency_seconds=60,
                timeout_seconds=5,
                retry_count=2,
                sla_target=SLATarget(availability=99.5, response_time_p95=50.0),
                business_service="cache",
                team_owner="platform_team",
            ),
            # ETL Pipeline Checks
            SyntheticCheck(
                check_id="etl_pipeline_status",
                name="ETL Pipeline Health",
                check_type=CheckType.DATA_PIPELINE,
                severity=CheckSeverity.HIGH,
                frequency_seconds=300,
                timeout_seconds=30,
                retry_count=1,
                endpoint=f"{getattr(settings, 'api_base_url', 'http://localhost:8000')}/pipeline/status",
                method="GET",
                expected_status=200,
                business_service="data_pipeline",
                team_owner="data_team",
            ),
            # External Service Checks
            SyntheticCheck(
                check_id="external_data_provider",
                name="External Data Provider API",
                check_type=CheckType.EXTERNAL_SERVICE,
                severity=CheckSeverity.MEDIUM,
                frequency_seconds=180,
                timeout_seconds=20,
                retry_count=2,
                endpoint="https://api.example-data-provider.com/health",
                method="GET",
                headers={"User-Agent": "PwC-DataPlatform-Monitor/1.0"},
                expected_status=200,
                sla_target=SLATarget(availability=99.0, response_time_p95=1000.0),
                business_service="external_integration",
                team_owner="integration_team",
            ),
        ]

        for check in default_checks:
            self.checks[check.check_id] = check

        self.logger.info(f"Initialized {len(default_checks)} default synthetic checks")

    def _initialize_business_transactions(self):
        """Initialize critical business transaction monitoring."""

        business_transactions = [
            BusinessTransaction(
                transaction_id="customer_onboarding",
                name="Customer Onboarding Flow",
                description="Complete customer registration and data ingestion flow",
                steps=[
                    {
                        "step": "validate_customer_data",
                        "endpoint": "/api/customers/validate",
                        "method": "POST",
                        "timeout": 5,
                    },
                    {
                        "step": "create_customer",
                        "endpoint": "/api/customers",
                        "method": "POST",
                        "timeout": 10,
                    },
                    {
                        "step": "trigger_data_ingestion",
                        "endpoint": "/api/pipeline/trigger",
                        "method": "POST",
                        "timeout": 15,
                    },
                    {
                        "step": "verify_data_quality",
                        "endpoint": "/api/data-quality/check",
                        "method": "GET",
                        "timeout": 10,
                    },
                ],
                sla_target=SLATarget(
                    availability=99.5,
                    response_time_p95=30000.0,  # 30 seconds
                    error_rate=0.5,
                ),
                critical_for_business=True,
                frequency_seconds=600,  # 10 minutes
                dependencies=["api_health", "database_connectivity"],
            ),
            BusinessTransaction(
                transaction_id="data_processing_pipeline",
                name="End-to-End Data Processing",
                description="Complete data processing from ingestion to gold layer",
                steps=[
                    {
                        "step": "bronze_ingestion",
                        "endpoint": "/api/pipeline/bronze/process",
                        "method": "POST",
                        "timeout": 30,
                    },
                    {
                        "step": "silver_transformation",
                        "endpoint": "/api/pipeline/silver/process",
                        "method": "POST",
                        "timeout": 60,
                    },
                    {
                        "step": "gold_aggregation",
                        "endpoint": "/api/pipeline/gold/process",
                        "method": "POST",
                        "timeout": 45,
                    },
                    {
                        "step": "data_quality_validation",
                        "endpoint": "/api/data-quality/validate",
                        "method": "GET",
                        "timeout": 15,
                    },
                ],
                sla_target=SLATarget(
                    availability=99.0,
                    response_time_p95=180000.0,  # 3 minutes
                    error_rate=1.0,
                ),
                critical_for_business=True,
                frequency_seconds=900,  # 15 minutes
                dependencies=["etl_pipeline_status", "database_connectivity"],
            ),
            BusinessTransaction(
                transaction_id="real_time_analytics",
                name="Real-time Analytics Query",
                description="Real-time analytics dashboard data retrieval",
                steps=[
                    {
                        "step": "authenticate_user",
                        "endpoint": "/api/auth/verify",
                        "method": "POST",
                        "timeout": 3,
                    },
                    {
                        "step": "fetch_dashboard_data",
                        "endpoint": "/api/analytics/dashboard",
                        "method": "GET",
                        "timeout": 5,
                    },
                    {
                        "step": "get_real_time_metrics",
                        "endpoint": "/api/metrics/real-time",
                        "method": "GET",
                        "timeout": 3,
                    },
                ],
                sla_target=SLATarget(
                    availability=99.9,
                    response_time_p95=5000.0,  # 5 seconds
                    error_rate=0.1,
                ),
                critical_for_business=False,
                frequency_seconds=120,  # 2 minutes
                dependencies=["api_health", "database_performance", "redis_connectivity"],
            ),
        ]

        for transaction in business_transactions:
            self.business_transactions[transaction.transaction_id] = transaction

        self.logger.info(f"Initialized {len(business_transactions)} business transactions")

    async def _run_check_loop(self, check: SyntheticCheck):
        """Run monitoring loop for a single check."""
        while self.monitoring_active:
            try:
                start_time = time.time()

                # Execute check with tracing
                with self.tracing_manager.start_span(
                    f"synthetic_check_{check.check_id}", self.tracing_manager.TracingComponent.API
                ) as span:
                    if hasattr(span, "set_attribute"):
                        span.set_attribute("check.id", check.check_id)
                        span.set_attribute("check.type", check.check_type.value)
                        span.set_attribute("check.severity", check.severity.value)

                    result = await self._execute_check(check)

                    # Record result
                    await self._record_check_result(result)

                    # Update circuit breaker
                    self._update_circuit_breaker(check.check_id, result.success)

                # Calculate next execution time
                execution_time = time.time() - start_time
                sleep_time = max(0, check.frequency_seconds - execution_time)

                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in check loop for {check.check_id}: {str(e)}")
                await asyncio.sleep(check.frequency_seconds)

    async def _execute_check(self, check: SyntheticCheck) -> CheckResult:
        """Execute a single synthetic check."""
        start_time = time.time()
        correlation_id = f"{check.check_id}_{int(start_time)}"

        try:
            success = False
            error_message = None
            response_code = None
            response_size = None
            metadata = {}

            # Check circuit breaker
            if self._is_circuit_breaker_open(check.check_id):
                error_message = "Circuit breaker is open"
                status = CheckStatus.CRITICAL
            else:
                # Execute check based on type
                if check.check_type == CheckType.HTTP_API:
                    (
                        success,
                        error_message,
                        response_code,
                        response_size,
                        metadata,
                    ) = await self._execute_http_check(check)

                elif check.check_type == CheckType.DATABASE:
                    success, error_message, metadata = await self._execute_database_check(check)

                elif check.check_type == CheckType.CACHE:
                    success, error_message, metadata = await self._execute_cache_check(check)

                elif check.check_type == CheckType.EXTERNAL_SERVICE:
                    (
                        success,
                        error_message,
                        response_code,
                        response_size,
                        metadata,
                    ) = await self._execute_http_check(check)

                elif check.check_type == CheckType.SECURITY:
                    (
                        success,
                        error_message,
                        response_code,
                        response_size,
                        metadata,
                    ) = await self._execute_http_check(check)

                elif check.check_type == CheckType.DATA_PIPELINE:
                    (
                        success,
                        error_message,
                        response_code,
                        response_size,
                        metadata,
                    ) = await self._execute_http_check(check)

                else:
                    error_message = f"Unsupported check type: {check.check_type}"

                # Determine status
                if success:
                    status = CheckStatus.HEALTHY
                else:
                    status = (
                        CheckStatus.CRITICAL
                        if check.severity in [CheckSeverity.CRITICAL, CheckSeverity.HIGH]
                        else CheckStatus.WARNING
                    )

            response_time_ms = (time.time() - start_time) * 1000

            return CheckResult(
                check_name=check.name,
                check_type=check.check_type,
                status=status,
                response_time_ms=response_time_ms,
                success=success,
                timestamp=datetime.utcnow(),
                error_message=error_message,
                response_code=response_code,
                response_size=response_size,
                metadata=metadata,
                business_impact=check.business_service,
                correlation_id=correlation_id,
            )

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000

            return CheckResult(
                check_name=check.name,
                check_type=check.check_type,
                status=CheckStatus.CRITICAL,
                response_time_ms=response_time_ms,
                success=False,
                timestamp=datetime.utcnow(),
                error_message=str(e),
                business_impact=check.business_service,
                correlation_id=correlation_id,
            )

    async def _execute_http_check(
        self, check: SyntheticCheck
    ) -> tuple[bool, str | None, int | None, int | None, dict[str, Any]]:
        """Execute HTTP-based check."""
        try:
            if not self.http_session:
                return False, "HTTP session not available", None, None, {}

            # Prepare request
            url = check.endpoint
            headers = check.headers.copy()

            # Add tracing headers
            trace_context = self.tracing_manager.get_current_trace_context()
            if trace_context:
                headers.update(
                    {
                        "X-Trace-Id": trace_context.get("trace_id", ""),
                        "X-Span-Id": trace_context.get("span_id", ""),
                    }
                )

            # Execute request with retry logic
            last_error = None
            for attempt in range(check.retry_count + 1):
                try:
                    timeout = aiohttp.ClientTimeout(total=check.timeout_seconds)

                    async with self.http_session.request(
                        method=check.method,
                        url=url,
                        headers=headers,
                        json=check.payload,
                        timeout=timeout,
                    ) as response:
                        response_text = await response.text()
                        response_size = len(response_text)

                        # Check status code
                        if response.status != check.expected_status:
                            if attempt < check.retry_count:
                                await asyncio.sleep(1 * (attempt + 1))  # Exponential backoff
                                continue
                            return (
                                False,
                                f"Expected status {check.expected_status}, got {response.status}",
                                response.status,
                                response_size,
                                {"response_text": response_text[:500]},
                            )

                        # Check response content
                        if check.expected_response_contains:
                            if (
                                check.expected_response_contains.lower()
                                not in response_text.lower()
                            ):
                                return (
                                    False,
                                    f"Response does not contain expected text: {check.expected_response_contains}",
                                    response.status,
                                    response_size,
                                    {"response_text": response_text[:500]},
                                )

                        # Custom validation
                        if check.custom_validator:
                            try:
                                validation_result = await check.custom_validator(
                                    response, response_text
                                )
                                if not validation_result:
                                    return (
                                        False,
                                        "Custom validation failed",
                                        response.status,
                                        response_size,
                                        {"response_text": response_text[:500]},
                                    )
                            except Exception as e:
                                return (
                                    False,
                                    f"Custom validation error: {str(e)}",
                                    response.status,
                                    response_size,
                                    {},
                                )

                        return (
                            True,
                            None,
                            response.status,
                            response_size,
                            {
                                "response_headers": dict(response.headers),
                                "response_time_ms": response.headers.get("X-Response-Time", ""),
                            },
                        )

                except asyncio.TimeoutError:
                    last_error = f"Request timeout after {check.timeout_seconds}s"
                    if attempt < check.retry_count:
                        await asyncio.sleep(1 * (attempt + 1))
                        continue
                except aiohttp.ClientError as e:
                    last_error = f"HTTP client error: {str(e)}"
                    if attempt < check.retry_count:
                        await asyncio.sleep(1 * (attempt + 1))
                        continue

            return False, last_error, None, None, {}

        except Exception as e:
            return False, f"HTTP check error: {str(e)}", None, None, {}

    async def _execute_database_check(
        self, check: SyntheticCheck
    ) -> tuple[bool, str | None, dict[str, Any]]:
        """Execute database connectivity/query check."""
        try:
            # Get database connection details from settings
            db_config = {
                "host": getattr(settings, "database_host", "localhost"),
                "port": getattr(settings, "database_port", 5432),
                "database": getattr(settings, "database_name", "retail_db"),
                "user": getattr(settings, "database_user", "postgres"),
                "password": getattr(settings, "database_password", "postgres"),
            }

            # Execute with timeout
            start_time = time.time()

            for attempt in range(check.retry_count + 1):
                try:
                    # Connect to database
                    conn = psycopg2.connect(
                        host=db_config["host"],
                        port=db_config["port"],
                        database=db_config["database"],
                        user=db_config["user"],
                        password=db_config["password"],
                        connect_timeout=check.timeout_seconds,
                    )

                    cursor = conn.cursor()

                    # Execute query
                    if check.database_query:
                        cursor.execute(check.database_query)
                        results = cursor.fetchall()

                        # Check row count if specified
                        if check.expected_row_count is not None:
                            if len(results) != check.expected_row_count:
                                conn.close()
                                return (
                                    False,
                                    f"Expected {check.expected_row_count} rows, got {len(results)}",
                                    {"row_count": len(results)},
                                )

                    # Close connection
                    conn.close()

                    execution_time = (time.time() - start_time) * 1000
                    return (
                        True,
                        None,
                        {
                            "execution_time_ms": execution_time,
                            "row_count": len(results) if "results" in locals() else 0,
                        },
                    )

                except psycopg2.OperationalError as e:
                    if attempt < check.retry_count:
                        await asyncio.sleep(1 * (attempt + 1))
                        continue
                    return False, f"Database connection error: {str(e)}", {}
                except psycopg2.Error as e:
                    return False, f"Database query error: {str(e)}", {}

            return False, "All retry attempts failed", {}

        except Exception as e:
            return False, f"Database check error: {str(e)}", {}

    async def _execute_cache_check(
        self, check: SyntheticCheck
    ) -> tuple[bool, str | None, dict[str, Any]]:
        """Execute Redis cache connectivity check."""
        try:
            # Get Redis connection details
            redis_host = getattr(settings, "redis_host", "localhost")
            redis_port = getattr(settings, "redis_port", 6379)
            redis_password = getattr(settings, "redis_password", None)

            for attempt in range(check.retry_count + 1):
                try:
                    # Connect to Redis
                    r = redis.Redis(
                        host=redis_host,
                        port=redis_port,
                        password=redis_password,
                        socket_timeout=check.timeout_seconds,
                        socket_connect_timeout=check.timeout_seconds,
                    )

                    # Test connectivity with ping
                    start_time = time.time()
                    pong = r.ping()
                    response_time = (time.time() - start_time) * 1000

                    if not pong:
                        return False, "Redis ping failed", {}

                    # Test set/get operations
                    test_key = f"synthetic_check_{int(time.time())}"
                    test_value = "health_check"

                    r.set(test_key, test_value, ex=60)  # Expires in 60 seconds
                    retrieved_value = r.get(test_key)

                    if retrieved_value.decode() != test_value:
                        return False, "Redis set/get operation failed", {}

                    # Cleanup
                    r.delete(test_key)
                    r.close()

                    return (
                        True,
                        None,
                        {"response_time_ms": response_time, "operation": "ping_set_get"},
                    )

                except redis.ConnectionError as e:
                    if attempt < check.retry_count:
                        await asyncio.sleep(1 * (attempt + 1))
                        continue
                    return False, f"Redis connection error: {str(e)}", {}
                except redis.RedisError as e:
                    return False, f"Redis operation error: {str(e)}", {}

            return False, "All retry attempts failed", {}

        except Exception as e:
            return False, f"Cache check error: {str(e)}", {}

    async def _record_check_result(self, result: CheckResult):
        """Record check result and send to monitoring systems."""
        try:
            # Store result
            if result.check_name not in self.check_results:
                self.check_results[result.check_name] = []

            self.check_results[result.check_name].append(result)

            # Keep only last 1000 results per check
            if len(self.check_results[result.check_name]) > 1000:
                self.check_results[result.check_name] = self.check_results[result.check_name][
                    -1000:
                ]

            # Send to DataDog
            if self.datadog_monitoring:
                await self._send_result_to_datadog(result)

            # Check for SLA violations
            await self._check_sla_violations(result)

            # Log critical failures
            if result.status == CheckStatus.CRITICAL and not result.success:
                self.logger.error(
                    f"Synthetic check CRITICAL failure: {result.check_name} - {result.error_message}"
                )

                # Record incident
                self.incident_timeline.append(
                    {
                        "timestamp": result.timestamp.isoformat(),
                        "type": "check_failure",
                        "check_name": result.check_name,
                        "severity": "critical",
                        "error_message": result.error_message,
                        "correlation_id": result.correlation_id,
                        "business_impact": result.business_impact,
                    }
                )

        except Exception as e:
            self.logger.error(f"Failed to record check result: {str(e)}")

    async def _send_result_to_datadog(self, result: CheckResult):
        """Send check result metrics to DataDog with enhanced monitoring."""
        try:
            # Get check configuration for enhanced tagging
            check_config = self.checks.get(result.check_name.replace(" ", "_").lower())

            tags = [
                f"check_name:{result.check_name}",
                f"check_type:{result.check_type.value}",
                f"status:{result.status.value}",
                f"service:{self.service_name}",
                f"environment:{getattr(settings, 'environment', 'production')}",
                f"region:{getattr(settings, 'aws_region', 'us-east-1')}",
            ]

            if result.business_impact:
                tags.append(f"business_service:{result.business_impact}")

            if result.correlation_id:
                tags.append(f"correlation_id:{result.correlation_id}")

            # Add severity and team information
            if check_config:
                tags.append(f"severity:{check_config.severity.value}")
                if check_config.team_owner:
                    tags.append(f"team:{check_config.team_owner}")
                if check_config.business_service:
                    tags.append(f"business_service:{check_config.business_service}")

            # Core synthetic monitoring metrics
            self.datadog_monitoring.gauge(
                "synthetic.check.success", 1.0 if result.success else 0.0, tags=tags
            )

            self.datadog_monitoring.histogram(
                "synthetic.check.response_time", result.response_time_ms, tags=tags
            )

            # Enhanced metrics for better observability
            self.datadog_monitoring.counter("synthetic.check.execution.count", 1, tags=tags)

            # Response time percentile metrics
            self.datadog_monitoring.distribution(
                "synthetic.check.response_time.distribution", result.response_time_ms, tags=tags
            )

            # Status code metrics with better categorization
            if result.response_code:
                status_category = self._categorize_status_code(result.response_code)
                extended_tags = tags + [
                    f"response_code:{result.response_code}",
                    f"status_category:{status_category}",
                ]

                self.datadog_monitoring.gauge(
                    "synthetic.check.response_code", result.response_code, tags=extended_tags
                )

                # Count by status category
                self.datadog_monitoring.counter(
                    f"synthetic.check.status.{status_category}", 1, tags=tags
                )

            # Response size metrics
            if result.response_size:
                self.datadog_monitoring.histogram(
                    "synthetic.check.response_size", result.response_size, tags=tags
                )

                # Response size categories for better alerting
                size_category = self._categorize_response_size(result.response_size)
                self.datadog_monitoring.gauge(
                    "synthetic.check.response_size.category",
                    1.0,
                    tags=tags + [f"size_category:{size_category}"],
                )

            # Error tracking
            if not result.success and result.error_message:
                error_category = self._categorize_error(result.error_message)
                self.datadog_monitoring.counter(
                    "synthetic.check.error",
                    1,
                    tags=tags
                    + [
                        f"error_category:{error_category}",
                        f"error_type:{self._extract_error_type(result.error_message)}",
                    ],
                )

            # SLA metrics
            if check_config and check_config.sla_target:
                sla_tags = tags + ["sla_monitored:true"]

                # Availability metric
                self.datadog_monitoring.gauge(
                    "synthetic.sla.availability.current",
                    1.0 if result.success else 0.0,
                    tags=sla_tags,
                )

                # Response time SLA compliance
                if result.response_time_ms <= check_config.sla_target.response_time_p95:
                    self.datadog_monitoring.gauge(
                        "synthetic.sla.response_time.compliance", 1.0, tags=sla_tags
                    )
                else:
                    self.datadog_monitoring.gauge(
                        "synthetic.sla.response_time.compliance", 0.0, tags=sla_tags
                    )

            # Business impact metrics
            if result.business_impact:
                business_tags = tags + [f"business_impact:{result.business_impact}"]

                self.datadog_monitoring.gauge(
                    "synthetic.business.impact.health",
                    1.0 if result.success else 0.0,
                    tags=business_tags,
                )

            # Advanced monitoring: Trend detection
            await self._send_trend_metrics(result, tags)

        except Exception as e:
            self.logger.error(f"Failed to send result to DataDog: {str(e)}")

    def _categorize_status_code(self, status_code: int) -> str:
        """Categorize HTTP status codes for better monitoring."""
        if 200 <= status_code < 300:
            return "success"
        elif 300 <= status_code < 400:
            return "redirect"
        elif 400 <= status_code < 500:
            return "client_error"
        elif 500 <= status_code < 600:
            return "server_error"
        else:
            return "unknown"

    def _categorize_response_size(self, size: int) -> str:
        """Categorize response sizes for monitoring."""
        if size < 1024:  # < 1KB
            return "small"
        elif size < 10240:  # < 10KB
            return "medium"
        elif size < 102400:  # < 100KB
            return "large"
        else:
            return "very_large"

    def _categorize_error(self, error_message: str) -> str:
        """Categorize errors for better monitoring and alerting."""
        error_msg_lower = error_message.lower()

        if "timeout" in error_msg_lower or "timed out" in error_msg_lower:
            return "timeout"
        elif "connection" in error_msg_lower:
            return "connection"
        elif "dns" in error_msg_lower or "resolve" in error_msg_lower:
            return "dns"
        elif "ssl" in error_msg_lower or "certificate" in error_msg_lower:
            return "ssl"
        elif "auth" in error_msg_lower or "unauthorized" in error_msg_lower:
            return "authentication"
        elif "database" in error_msg_lower or "sql" in error_msg_lower:
            return "database"
        elif "cache" in error_msg_lower or "redis" in error_msg_lower:
            return "cache"
        elif "validation" in error_msg_lower:
            return "validation"
        else:
            return "other"

    def _extract_error_type(self, error_message: str) -> str:
        """Extract specific error type from error message."""
        error_msg_lower = error_message.lower()

        # Common error patterns
        if "connection refused" in error_msg_lower:
            return "connection_refused"
        elif "connection timeout" in error_msg_lower:
            return "connection_timeout"
        elif "read timeout" in error_msg_lower:
            return "read_timeout"
        elif "name resolution" in error_msg_lower:
            return "dns_resolution"
        elif "certificate verify failed" in error_msg_lower:
            return "ssl_verification"
        elif "permission denied" in error_msg_lower:
            return "permission_denied"
        else:
            return "generic_error"

    async def _send_trend_metrics(self, result: CheckResult, base_tags: list[str]):
        """Send advanced trend analysis metrics to DataDog."""
        try:
            check_name = result.check_name

            if check_name not in self.check_results:
                return

            recent_results = self.check_results[check_name][-20:]  # Last 20 results

            if len(recent_results) < 5:
                return

            # Calculate trend metrics
            response_times = [r.response_time_ms for r in recent_results]
            success_rates = [1.0 if r.success else 0.0 for r in recent_results]

            # Response time trend
            if len(response_times) >= 10:
                first_half_avg = statistics.mean(response_times[: len(response_times) // 2])
                second_half_avg = statistics.mean(response_times[len(response_times) // 2 :])

                if first_half_avg > 0:
                    trend_percentage = ((second_half_avg - first_half_avg) / first_half_avg) * 100

                    self.datadog_monitoring.gauge(
                        "synthetic.trend.response_time.percentage_change",
                        trend_percentage,
                        tags=base_tags,
                    )

                    # Trend direction
                    trend_direction = (
                        "increasing"
                        if trend_percentage > 5
                        else "decreasing"
                        if trend_percentage < -5
                        else "stable"
                    )
                    self.datadog_monitoring.gauge(
                        "synthetic.trend.response_time.direction",
                        1.0
                        if trend_direction == "increasing"
                        else -1.0
                        if trend_direction == "decreasing"
                        else 0.0,
                        tags=base_tags + [f"trend_direction:{trend_direction}"],
                    )

            # Success rate trend
            if len(success_rates) >= 10:
                recent_success_rate = statistics.mean(success_rates[-5:]) * 100
                historical_success_rate = statistics.mean(success_rates[:-5]) * 100

                self.datadog_monitoring.gauge(
                    "synthetic.trend.success_rate.current", recent_success_rate, tags=base_tags
                )

                self.datadog_monitoring.gauge(
                    "synthetic.trend.success_rate.historical",
                    historical_success_rate,
                    tags=base_tags,
                )

                # Success rate degradation alert
                if recent_success_rate < historical_success_rate - 10:  # 10% degradation
                    self.datadog_monitoring.counter(
                        "synthetic.alert.success_rate_degradation", 1, tags=base_tags
                    )

        except Exception as e:
            self.logger.error(f"Failed to send trend metrics: {str(e)}")

    async def _run_transaction_loop(self, transaction: BusinessTransaction):
        """Run monitoring loop for business transactions."""
        while self.monitoring_active:
            try:
                start_time = time.time()

                # Check dependencies first
                if not await self._check_transaction_dependencies(transaction):
                    self.logger.warning(
                        f"Transaction {transaction.transaction_id} dependencies not met, skipping"
                    )
                    await asyncio.sleep(transaction.frequency_seconds)
                    continue

                # Execute transaction with tracing
                with self.tracing_manager.start_span(
                    f"business_transaction_{transaction.transaction_id}",
                    self.tracing_manager.TracingComponent.BUSINESS_LOGIC,
                ) as span:
                    if hasattr(span, "set_attribute"):
                        span.set_attribute("transaction.id", transaction.transaction_id)
                        span.set_attribute("transaction.name", transaction.name)
                        span.set_attribute(
                            "transaction.critical", transaction.critical_for_business
                        )

                    (
                        success,
                        total_time_ms,
                        step_results,
                        error_message,
                    ) = await self._execute_business_transaction(transaction)

                    # Record transaction result
                    await self._record_transaction_result(
                        transaction, success, total_time_ms, step_results, error_message
                    )

                # Calculate next execution time
                execution_time = time.time() - start_time
                sleep_time = max(0, transaction.frequency_seconds - execution_time)

                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(
                    f"Error in transaction loop for {transaction.transaction_id}: {str(e)}"
                )
                await asyncio.sleep(transaction.frequency_seconds)

    async def _execute_business_transaction(
        self, transaction: BusinessTransaction
    ) -> tuple[bool, float, list[dict[str, Any]], str | None]:
        """Execute a complete business transaction."""
        start_time = time.time()
        step_results = []

        try:
            base_url = getattr(settings, "api_base_url", "http://localhost:8000")

            for _i, step in enumerate(transaction.steps):
                step_start_time = time.time()

                try:
                    # Prepare request
                    url = urljoin(base_url, step["endpoint"])
                    method = step.get("method", "GET")
                    timeout = step.get("timeout", 30)

                    # Execute step
                    if not self.http_session:
                        raise Exception("HTTP session not available")

                    step_timeout = aiohttp.ClientTimeout(total=timeout)
                    async with self.http_session.request(
                        method=method, url=url, timeout=step_timeout
                    ) as response:
                        response_text = await response.text()
                        step_duration = (time.time() - step_start_time) * 1000

                        step_success = 200 <= response.status < 300

                        step_result = {
                            "step": step["step"],
                            "success": step_success,
                            "duration_ms": step_duration,
                            "status_code": response.status,
                            "response_size": len(response_text),
                        }

                        step_results.append(step_result)

                        if not step_success:
                            total_time_ms = (time.time() - start_time) * 1000
                            return (
                                False,
                                total_time_ms,
                                step_results,
                                f"Step {step['step']} failed with status {response.status}",
                            )

                except asyncio.TimeoutError:
                    step_duration = (time.time() - step_start_time) * 1000
                    step_results.append(
                        {
                            "step": step["step"],
                            "success": False,
                            "duration_ms": step_duration,
                            "error": "timeout",
                        }
                    )
                    total_time_ms = (time.time() - start_time) * 1000
                    return False, total_time_ms, step_results, f"Step {step['step']} timed out"

                except Exception as e:
                    step_duration = (time.time() - step_start_time) * 1000
                    step_results.append(
                        {
                            "step": step["step"],
                            "success": False,
                            "duration_ms": step_duration,
                            "error": str(e),
                        }
                    )
                    total_time_ms = (time.time() - start_time) * 1000
                    return (
                        False,
                        total_time_ms,
                        step_results,
                        f"Step {step['step']} failed: {str(e)}",
                    )

            total_time_ms = (time.time() - start_time) * 1000
            return True, total_time_ms, step_results, None

        except Exception as e:
            total_time_ms = (time.time() - start_time) * 1000
            return False, total_time_ms, step_results, f"Transaction error: {str(e)}"

    async def _record_transaction_result(
        self,
        transaction: BusinessTransaction,
        success: bool,
        duration_ms: float,
        step_results: list[dict[str, Any]],
        error_message: str | None,
    ):
        """Record business transaction result."""
        try:
            # Send metrics to DataDog
            if self.datadog_monitoring:
                tags = [
                    f"transaction_id:{transaction.transaction_id}",
                    f"transaction_name:{transaction.name}",
                    f"critical:{transaction.critical_for_business}",
                    f"service:{self.service_name}",
                ]

                # Overall transaction metrics
                self.datadog_monitoring.gauge(
                    "synthetic.transaction.success", 1.0 if success else 0.0, tags=tags
                )

                self.datadog_monitoring.histogram(
                    "synthetic.transaction.duration", duration_ms, tags=tags
                )

                # Step-by-step metrics
                for step_result in step_results:
                    step_tags = tags + [f"step:{step_result['step']}"]

                    self.datadog_monitoring.gauge(
                        "synthetic.transaction.step.success",
                        1.0 if step_result["success"] else 0.0,
                        tags=step_tags,
                    )

                    self.datadog_monitoring.histogram(
                        "synthetic.transaction.step.duration",
                        step_result["duration_ms"],
                        tags=step_tags,
                    )

                    if "status_code" in step_result:
                        self.datadog_monitoring.gauge(
                            "synthetic.transaction.step.status_code",
                            step_result["status_code"],
                            tags=step_tags,
                        )

            # Check SLA violations for critical transactions
            if transaction.critical_for_business and transaction.sla_target:
                await self._check_transaction_sla(transaction, success, duration_ms)

            # Log failures
            if not success:
                self.logger.error(
                    f"Business transaction failed: {transaction.name} - {error_message}"
                )

                # Record incident
                self.incident_timeline.append(
                    {
                        "timestamp": datetime.utcnow().isoformat(),
                        "type": "transaction_failure",
                        "transaction_id": transaction.transaction_id,
                        "transaction_name": transaction.name,
                        "duration_ms": duration_ms,
                        "error_message": error_message,
                        "step_results": step_results,
                        "critical_for_business": transaction.critical_for_business,
                    }
                )

        except Exception as e:
            self.logger.error(f"Failed to record transaction result: {str(e)}")

    def get_monitoring_summary(self) -> dict[str, Any]:
        """Get comprehensive monitoring summary."""
        try:
            current_time = datetime.utcnow()

            # Overall statistics
            total_checks = len(self.checks)
            active_checks = len([c for c in self.checks.values() if c.enabled])
            total_transactions = len(self.business_transactions)

            # Health status summary
            health_status = self._calculate_overall_health_status()

            # Recent results summary (last hour)
            cutoff_time = current_time - timedelta(hours=1)
            recent_results = []
            for check_results in self.check_results.values():
                recent_results.extend([r for r in check_results if r.timestamp >= cutoff_time])

            success_rate = (
                (len([r for r in recent_results if r.success]) / len(recent_results)) * 100
                if recent_results
                else 100
            )
            avg_response_time = (
                statistics.mean([r.response_time_ms for r in recent_results])
                if recent_results
                else 0
            )

            # SLA compliance
            sla_compliance = self._calculate_sla_compliance()

            # Circuit breaker status
            circuit_breakers_open = len(
                [cb for cb in self.circuit_breakers.values() if cb.get("state") == "open"]
            )

            return {
                "timestamp": current_time.isoformat(),
                "service_name": self.service_name,
                "monitoring_active": self.monitoring_active,
                "overall_health": health_status,
                "statistics": {
                    "total_checks": total_checks,
                    "active_checks": active_checks,
                    "total_transactions": total_transactions,
                    "success_rate_percent": round(success_rate, 2),
                    "avg_response_time_ms": round(avg_response_time, 2),
                    "recent_incidents": len(
                        [
                            i
                            for i in self.incident_timeline
                            if datetime.fromisoformat(i["timestamp"]) >= cutoff_time
                        ]
                    ),
                },
                "sla_compliance": sla_compliance,
                "circuit_breakers": {
                    "total": len(self.circuit_breakers),
                    "open": circuit_breakers_open,
                    "closed": len(self.circuit_breakers) - circuit_breakers_open,
                },
                "recent_violations": len(
                    [
                        v
                        for v in self.sla_violations
                        if datetime.fromisoformat(v["timestamp"]) >= cutoff_time
                    ]
                ),
                "business_impact": {
                    "critical_services_healthy": self._count_healthy_critical_services(),
                    "critical_transactions_success_rate": self._calculate_critical_transaction_success_rate(),
                },
            }

        except Exception as e:
            self.logger.error(f"Failed to generate monitoring summary: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}

    def _calculate_overall_health_status(self) -> str:
        """Calculate overall system health status."""
        try:
            if not self.check_results:
                return "unknown"

            # Get latest results for each check
            latest_results = {}
            for check_name, results in self.check_results.items():
                if results:
                    latest_results[check_name] = results[-1]

            if not latest_results:
                return "unknown"

            critical_checks = [
                r for r in latest_results.values() if r.status == CheckStatus.CRITICAL
            ]
            warning_checks = [r for r in latest_results.values() if r.status == CheckStatus.WARNING]

            if critical_checks:
                return "critical"
            elif warning_checks:
                return "warning"
            else:
                return "healthy"

        except Exception as e:
            self.logger.error(f"Failed to calculate health status: {str(e)}")
            return "unknown"

    async def _sla_monitoring_loop(self):
        """Background task for SLA monitoring and violation detection."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes

                datetime.utcnow()

                # Check SLA compliance for all checks with SLA targets
                for check_id, check in self.checks.items():
                    if check.sla_target and check_id in self.check_results:
                        await self._evaluate_check_sla(check, self.check_results[check_id])

                # Check business transaction SLAs
                for transaction in self.business_transactions.values():
                    if transaction.sla_target:
                        await self._evaluate_transaction_sla(transaction)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in SLA monitoring loop: {str(e)}")
                await asyncio.sleep(300)

    async def _anomaly_detection_loop(self):
        """Background task for anomaly detection and predictive alerting."""
        while self.monitoring_active:
            try:
                await asyncio.sleep(600)  # Check every 10 minutes

                # Analyze patterns in check results
                for check_name, results in self.check_results.items():
                    if len(results) >= 50:  # Need sufficient data
                        await self._detect_anomalies_in_check(check_name, results[-100:])

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in anomaly detection loop: {str(e)}")
                await asyncio.sleep(600)


# Global instance
_synthetic_monitoring: SyntheticMonitoringEnterprise | None = None


def get_synthetic_monitoring(
    service_name: str = "synthetic-monitoring", datadog_monitoring: DatadogMonitoring | None = None
) -> SyntheticMonitoringEnterprise:
    """Get or create synthetic monitoring instance."""
    global _synthetic_monitoring

    if _synthetic_monitoring is None:
        _synthetic_monitoring = SyntheticMonitoringEnterprise(service_name, datadog_monitoring)

    return _synthetic_monitoring


# Convenience functions
async def start_synthetic_monitoring():
    """Start synthetic monitoring."""
    monitoring = get_synthetic_monitoring()
    await monitoring.start_monitoring()


async def stop_synthetic_monitoring():
    """Stop synthetic monitoring."""
    monitoring = get_synthetic_monitoring()
    await monitoring.stop_monitoring()


def get_monitoring_summary() -> dict[str, Any]:
    """Get monitoring summary."""
    monitoring = get_synthetic_monitoring()
    return monitoring.get_monitoring_summary()

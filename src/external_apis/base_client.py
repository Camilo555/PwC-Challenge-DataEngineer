"""Base API client with retry logic and error handling."""

import asyncio
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

import aiohttp
from aiohttp import ClientTimeout

from core.logging import get_logger

logger = get_logger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open."""
    pass


class CircuitBreaker:
    """Circuit breaker for external API calls."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type[Exception] = Exception,
    ):
        """Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time to wait before attempting recovery
            expected_exception: Exception type that triggers circuit breaker
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.last_failure_time: float | None = None
        self.state = CircuitState.CLOSED

    def _should_attempt_reset(self) -> bool:
        """Check if circuit should attempt to reset."""
        return (
            self.state == CircuitState.OPEN
            and self.last_failure_time is not None
            and time.time() - self.last_failure_time >= self.recovery_timeout
        )

    def record_success(self) -> None:
        """Record successful operation."""
        self.failure_count = 0
        self.state = CircuitState.CLOSED
        logger.debug("Circuit breaker: Success recorded, circuit closed")

    def record_failure(self) -> None:
        """Record failed operation."""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")

    async def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
                logger.info("Circuit breaker attempting recovery (half-open)")
            else:
                raise CircuitBreakerError("Circuit breaker is open")

        try:
            result = await func(*args, **kwargs)
            if self.state == CircuitState.HALF_OPEN:
                self.record_success()
            return result
        except self.expected_exception as e:
            self.record_failure()
            raise e


class BaseAPIClient(ABC):
    """Base class for external API clients with common functionality."""

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit_per_second: float = 1.0,
    ) -> None:
        """
        Initialize base API client.

        Args:
            base_url: Base URL for the API
            api_key: Optional API key for authentication
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            rate_limit_per_second: Rate limit (requests per second)
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = ClientTimeout(total=timeout)
        self.max_retries = max_retries
        self.rate_limit_delay = 1.0 / rate_limit_per_second
        self._last_request_time = 0.0
        self._session: aiohttp.ClientSession | None = None

        # Initialize circuit breaker
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60.0,
            expected_exception=aiohttp.ClientError
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            headers = {"User-Agent": "PwC-DataEngineer-Challenge/1.0"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"

            self._session = aiohttp.ClientSession(
                headers=headers,
                timeout=self.timeout
            )
        return self._session

    async def _enforce_rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time

        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            await asyncio.sleep(sleep_time)

        self._last_request_time = time.time()

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        retry_count: int = 0,
    ) -> dict[str, Any]:
        """
        Make HTTP request with circuit breaker and retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
            retry_count: Current retry attempt

        Returns:
            API response as dictionary

        Raises:
            aiohttp.ClientError: If request fails after all retries
            CircuitBreakerError: If circuit breaker is open
        """
        # Use circuit breaker to protect the request
        return await self.circuit_breaker.call(
            self._make_protected_request,
            method, endpoint, params, data, retry_count
        )

    async def _make_protected_request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        retry_count: int = 0,
    ) -> dict[str, Any]:
        """
        Protected HTTP request implementation.
        """
        await self._enforce_rate_limit()

        session = await self._get_session()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            async with session.request(
                method=method,
                url=url,
                params=params,
                json=data
            ) as response:
                response.raise_for_status()
                result: dict[str, Any] = await response.json()
                return result

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if retry_count < self.max_retries:
                wait_time = 2 ** retry_count  # Exponential backoff
                logger.warning(
                    f"Request failed (attempt {retry_count + 1}/{self.max_retries + 1}), "
                    f"retrying in {wait_time}s: {e}"
                )
                await asyncio.sleep(wait_time)
                return await self._make_protected_request(method, endpoint, params, data, retry_count + 1)
            else:
                logger.error(f"Request failed after {self.max_retries + 1} attempts: {e}")
                raise

    async def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Make GET request."""
        return await self._make_request("GET", endpoint, params=params)

    async def post(
        self,
        endpoint: str,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Make POST request."""
        return await self._make_request("POST", endpoint, params=params, data=data)

    async def close(self) -> None:
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the API is healthy and accessible."""
        pass

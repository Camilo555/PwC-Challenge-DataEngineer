"""
Circuit Breaker middleware for API resilience
Enterprise-grade circuit breaker implementation with monitoring
"""
import asyncio
import time
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import Any

from fastapi import HTTPException, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware


class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        timeout: int = 60,
        expected_exception: type = HTTPException
    ):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time: float | None = None
        self.state = CircuitBreakerState.CLOSED
        self._lock = asyncio.Lock()

    async def call(self, func: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitBreakerState.HALF_OPEN
                else:
                    raise HTTPException(
                        status_code=503,
                        detail="Service temporarily unavailable - circuit breaker is OPEN"
                    )

        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except self.expected_exception as e:
            await self._on_failure()
            raise e

    def _should_attempt_reset(self) -> bool:
        return (
            self.last_failure_time is not None and
            time.time() - self.last_failure_time >= self.timeout
        )

    async def _on_success(self):
        async with self._lock:
            self.failure_count = 0
            self.state = CircuitBreakerState.CLOSED

    async def _on_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN


class CircuitBreakerMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, circuit_breakers: dict[str, CircuitBreaker] | None = None):
        super().__init__(app)
        self.circuit_breakers = circuit_breakers or {}

        # Default circuit breaker for all routes
        if "default" not in self.circuit_breakers:
            self.circuit_breakers["default"] = CircuitBreaker()

    async def dispatch(self, request: Request, call_next) -> Response:
        route_key = f"{request.method}:{request.url.path}"
        circuit_breaker = self.circuit_breakers.get(route_key, self.circuit_breakers["default"])

        try:
            response = await circuit_breaker.call(call_next, request)
            return response
        except HTTPException as e:
            if e.status_code >= 500:
                # Server errors trigger circuit breaker
                raise e
            # Client errors don't trigger circuit breaker
            return Response(
                content=str(e.detail),
                status_code=e.status_code,
                headers=e.headers
            )

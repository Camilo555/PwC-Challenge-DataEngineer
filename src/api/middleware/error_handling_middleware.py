"""
Enhanced Error Handling Middleware
Provides standardized error responses, logging, and monitoring across the application.
"""
from __future__ import annotations

import traceback
import time
from typing import Any, Dict, Optional, Union, List
from datetime import datetime
from enum import Enum

from fastapi import Request, Response, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse

from core.exceptions import (
    BaseApplicationException,
    APIException,
    AuthenticationException,
    AuthorizationException,
    NotFoundException,
    RateLimitException,
    ValidationException,
    DataQualityException,
    DatabaseException,
    ETLException,
    ConfigurationException,
    VectorSearchException
)
from core.logging import get_logger
from core.config.base_config import BaseConfig

logger = get_logger(__name__)
config = BaseConfig()


class ErrorSeverity(Enum):
    """Error severity levels for monitoring and alerting."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Error categories for better organization."""
    CLIENT_ERROR = "client_error"
    SERVER_ERROR = "server_error"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    VALIDATION = "validation"
    BUSINESS_LOGIC = "business_logic"
    INTEGRATION = "integration"
    INFRASTRUCTURE = "infrastructure"


class ErrorMetrics:
    """Error metrics collection for monitoring."""
    
    def __init__(self):
        self.error_counts: Dict[str, int] = {}
        self.error_rates: Dict[str, List[float]] = {}
        self.response_times: List[float] = []
        self.last_reset = time.time()
    
    def record_error(self, error_type: str, response_time: float = 0):
        """Record error occurrence."""
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1
        if response_time > 0:
            self.response_times.append(response_time)
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get error summary for monitoring."""
        return {
            "error_counts": self.error_counts.copy(),
            "total_errors": sum(self.error_counts.values()),
            "avg_response_time": sum(self.response_times) / len(self.response_times) if self.response_times else 0,
            "error_types": list(self.error_counts.keys()),
            "period_start": self.last_reset
        }


class ErrorContext:
    """Context information for error handling."""
    
    def __init__(self, request: Request, start_time: float):
        self.request = request
        self.start_time = start_time
        self.correlation_id = request.headers.get("x-correlation-id")
        self.user_id = getattr(request.state, "user_id", None)
        self.endpoint = f"{request.method}:{request.url.path}"
        self.client_ip = self._get_client_ip(request)
        self.user_agent = request.headers.get("user-agent")
    
    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP address considering proxies."""
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip
        return str(request.client.host) if request.client else "unknown"


class StandardErrorResponse:
    """Standard error response format."""
    
    def __init__(
        self,
        error: Exception,
        context: ErrorContext,
        include_traceback: bool = False
    ):
        self.error = error
        self.context = context
        self.include_traceback = include_traceback
        self.timestamp = datetime.utcnow()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to standard error response dictionary."""
        # Determine error details
        if isinstance(self.error, BaseApplicationException):
            error_code = self.error.error_code
            message = self.error.message
            details = self.error.details
            status_code = getattr(self.error, "status_code", 500)
        elif isinstance(self.error, APIException):
            error_code = self.error.error_code
            message = self.error.message
            details = self.error.details
            status_code = self.error.status_code
        else:
            error_code = type(self.error).__name__
            message = str(self.error)
            details = {}
            status_code = 500
        
        # Build response
        response = {
            "error": {
                "code": error_code,
                "message": message,
                "type": self._get_error_category(self.error).value,
                "severity": self._get_error_severity(status_code).value,
                "timestamp": self.timestamp.isoformat(),
                "request_id": self.context.correlation_id,
                "endpoint": self.context.endpoint
            }
        }
        
        # Add details if present
        if details:
            response["error"]["details"] = details
        
        # Add context in development
        if config.environment.value in ["development", "testing"]:
            response["error"]["context"] = {
                "user_id": self.context.user_id,
                "client_ip": self.context.client_ip,
                "user_agent": self.context.user_agent,
                "processing_time_ms": (time.time() - self.context.start_time) * 1000
            }
            
            if self.include_traceback:
                response["error"]["traceback"] = traceback.format_exc()
        
        # Add help information
        response["error"]["help"] = self._get_help_information(error_code, status_code)
        
        return response
    
    def _get_error_category(self, error: Exception) -> ErrorCategory:
        """Determine error category."""
        if isinstance(error, (AuthenticationException, AuthorizationException)):
            return ErrorCategory.AUTHENTICATION
        elif isinstance(error, ValidationException):
            return ErrorCategory.VALIDATION
        elif isinstance(error, DatabaseException):
            return ErrorCategory.INFRASTRUCTURE
        elif isinstance(error, ETLException):
            return ErrorCategory.BUSINESS_LOGIC
        elif isinstance(error, VectorSearchException):
            return ErrorCategory.INTEGRATION
        elif isinstance(error, BaseApplicationException):
            return ErrorCategory.BUSINESS_LOGIC
        else:
            return ErrorCategory.SERVER_ERROR
    
    def _get_error_severity(self, status_code: int) -> ErrorSeverity:
        """Determine error severity based on status code."""
        if status_code >= 500:
            return ErrorSeverity.HIGH
        elif status_code == 429:  # Rate limiting
            return ErrorSeverity.MEDIUM
        elif status_code in [401, 403]:  # Auth errors
            return ErrorSeverity.MEDIUM
        elif status_code >= 400:
            return ErrorSeverity.LOW
        else:
            return ErrorSeverity.LOW
    
    def _get_help_information(self, error_code: str, status_code: int) -> Dict[str, Any]:
        """Generate help information for the error."""
        help_info = {
            "documentation": f"/docs#errors/{error_code.lower()}",
            "support_contact": "support@pwc-challenge.com"
        }
        
        # Add specific help based on error type
        if status_code == 401:
            help_info["suggestions"] = [
                "Check that your authentication token is valid",
                "Ensure the Authorization header is properly formatted",
                "Verify that your token has not expired"
            ]
        elif status_code == 403:
            help_info["suggestions"] = [
                "Check that you have the required permissions",
                "Contact your administrator if you need additional access",
                "Verify that your role includes the necessary privileges"
            ]
        elif status_code == 404:
            help_info["suggestions"] = [
                "Check that the resource path is correct",
                "Verify that the resource exists",
                "Ensure you have access to the resource"
            ]
        elif status_code == 429:
            help_info["suggestions"] = [
                "Reduce your request rate",
                "Wait before retrying the request",
                "Consider implementing exponential backoff"
            ]
        elif status_code == 422:
            help_info["suggestions"] = [
                "Check the request payload format",
                "Verify all required fields are provided",
                "Ensure field values meet validation requirements"
            ]
        elif status_code >= 500:
            help_info["suggestions"] = [
                "Try the request again later",
                "Contact support if the problem persists",
                "Check system status at /health"
            ]
        
        return help_info


class EnhancedErrorHandlingMiddleware(BaseHTTPMiddleware):
    """Enhanced error handling middleware with monitoring and alerting."""
    
    def __init__(self, app, include_traceback: bool = False):
        super().__init__(app)
        self.include_traceback = include_traceback or config.environment.value in ["development", "testing"]
        self.metrics = ErrorMetrics()
        
        # Error severity thresholds for alerting
        self.alert_thresholds = {
            ErrorSeverity.CRITICAL: 1,    # Alert on first critical error
            ErrorSeverity.HIGH: 5,        # Alert after 5 high-severity errors
            ErrorSeverity.MEDIUM: 20,     # Alert after 20 medium-severity errors
        }
    
    async def dispatch(self, request: Request, call_next):
        """Process request with enhanced error handling."""
        start_time = time.time()
        context = ErrorContext(request, start_time)
        
        try:
            response = await call_next(request)
            return response
            
        except Exception as error:
            return await self._handle_error(error, context)
    
    async def _handle_error(self, error: Exception, context: ErrorContext) -> JSONResponse:
        """Handle error with standardized response and logging."""
        # Create standard error response
        error_response = StandardErrorResponse(
            error=error,
            context=context,
            include_traceback=self.include_traceback
        )
        
        response_dict = error_response.to_dict()
        
        # Determine status code
        if isinstance(error, APIException):
            status_code = error.status_code
        elif isinstance(error, AuthenticationException):
            status_code = 401
        elif isinstance(error, AuthorizationException):
            status_code = 403
        elif isinstance(error, NotFoundException):
            status_code = 404
        elif isinstance(error, RateLimitException):
            status_code = 429
        elif isinstance(error, ValidationException):
            status_code = 422
        elif isinstance(error, BaseApplicationException):
            status_code = 500
        else:
            status_code = 500
        
        # Record metrics
        error_type = type(error).__name__
        response_time = time.time() - context.start_time
        self.metrics.record_error(error_type, response_time)
        
        # Log error with appropriate level
        severity = response_dict["error"]["severity"]
        error_data = {
            "error_code": response_dict["error"]["code"],
            "error_type": error_type,
            "status_code": status_code,
            "endpoint": context.endpoint,
            "user_id": context.user_id,
            "client_ip": context.client_ip,
            "correlation_id": context.correlation_id,
            "processing_time_ms": response_time * 1000,
            "severity": severity
        }
        
        if severity == ErrorSeverity.CRITICAL.value:
            logger.critical(f"Critical error occurred: {error}", extra=error_data)
        elif severity == ErrorSeverity.HIGH.value:
            logger.error(f"High severity error: {error}", extra=error_data)
        elif severity == ErrorSeverity.MEDIUM.value:
            logger.warning(f"Medium severity error: {error}", extra=error_data)
        else:
            logger.info(f"Error occurred: {error}", extra=error_data)
        
        # Check if alerting is needed
        await self._check_alert_thresholds(error_type, severity)
        
        # Create JSON response
        return JSONResponse(
            status_code=status_code,
            content=response_dict,
            headers={
                "X-Error-ID": str(error_response.timestamp.timestamp()),
                "X-Correlation-ID": context.correlation_id or "unknown",
                "X-Error-Category": response_dict["error"]["type"],
                "X-Error-Severity": severity
            }
        )
    
    async def _check_alert_thresholds(self, error_type: str, severity: str):
        """Check if error frequency exceeds alert thresholds."""
        try:
            severity_enum = ErrorSeverity(severity)
            threshold = self.alert_thresholds.get(severity_enum, float('inf'))
            
            current_count = self.metrics.error_counts.get(error_type, 0)
            
            if current_count >= threshold:
                await self._send_alert(error_type, severity, current_count)
                
        except Exception as e:
            logger.error(f"Failed to check alert thresholds: {e}")
    
    async def _send_alert(self, error_type: str, severity: str, count: int):
        """Send alert for high error frequency."""
        alert_data = {
            "alert_type": "high_error_frequency",
            "error_type": error_type,
            "severity": severity,
            "count": count,
            "threshold_exceeded": True,
            "timestamp": datetime.utcnow().isoformat(),
            "environment": config.environment.value
        }
        
        # Log alert (in production, this would integrate with alerting systems)
        logger.critical(f"ALERT: High error frequency detected", extra=alert_data)
        
        # TODO: Integrate with alerting systems (PagerDuty, Slack, etc.)
        # await alerting_service.send_alert(alert_data)
    
    def get_error_metrics(self) -> Dict[str, Any]:
        """Get current error metrics."""
        return self.metrics.get_error_summary()
    
    def reset_metrics(self):
        """Reset error metrics."""
        self.metrics = ErrorMetrics()


# Factory function for creating error handling middleware
def create_error_handling_middleware(include_traceback: bool = None) -> EnhancedErrorHandlingMiddleware:
    """Create error handling middleware with optional configuration."""
    if include_traceback is None:
        include_traceback = config.environment.value in ["development", "testing"]
    
    return EnhancedErrorHandlingMiddleware(None, include_traceback=include_traceback)


# Global middleware instance
_error_middleware: Optional[EnhancedErrorHandlingMiddleware] = None


def get_error_handling_middleware() -> EnhancedErrorHandlingMiddleware:
    """Get or create global error handling middleware instance."""
    global _error_middleware
    if _error_middleware is None:
        _error_middleware = create_error_handling_middleware()
    return _error_middleware
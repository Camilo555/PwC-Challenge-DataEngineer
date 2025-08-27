"""
ML Error Handling and Logging Framework

Comprehensive error handling, logging, and monitoring utilities for ML operations
with proper integration into existing monitoring systems.
"""

import logging
import traceback
import functools
import asyncio
from typing import Dict, List, Optional, Any, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import json
import sys

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.monitoring.metrics import MetricsCollector
from src.data_access.supabase_client import get_supabase_client
from src.monitoring.intelligent_alerting import create_intelligent_alerting_system

logger = get_logger(__name__)
settings = get_settings()


class MLErrorType(Enum):
    """Types of ML errors."""
    DATA_ERROR = "data_error"
    MODEL_ERROR = "model_error"
    FEATURE_ERROR = "feature_error"
    PREDICTION_ERROR = "prediction_error"
    TRAINING_ERROR = "training_error"
    DEPLOYMENT_ERROR = "deployment_error"
    MONITORING_ERROR = "monitoring_error"
    SYSTEM_ERROR = "system_error"


class MLErrorSeverity(Enum):
    """ML error severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class MLError:
    """ML error information."""
    
    error_id: str
    error_type: MLErrorType
    severity: MLErrorSeverity
    message: str
    details: Dict[str, Any]
    timestamp: datetime
    component: str
    operation: str
    user_id: Optional[str] = None
    model_id: Optional[str] = None
    traceback: Optional[str] = None
    context: Dict[str, Any] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "error_id": self.error_id,
            "error_type": self.error_type.value,
            "severity": self.severity.value,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
            "component": self.component,
            "operation": self.operation,
            "user_id": self.user_id,
            "model_id": self.model_id,
            "traceback": self.traceback,
            "context": self.context or {}
        }


class MLErrorHandler:
    """Central ML error handling system."""
    
    def __init__(self):
        self.supabase = get_supabase_client()
        self.metrics_collector = MetricsCollector()
        self.alerting_system = create_intelligent_alerting_system()
        self.error_counts: Dict[str, int] = {}
        
    def handle_error(self, error: MLError) -> None:
        """Handle ML error with logging, storage, and alerting."""
        try:
            # Log error
            self._log_error(error)
            
            # Store error
            self._store_error(error)
            
            # Update metrics
            self._update_error_metrics(error)
            
            # Send alerts if necessary
            asyncio.create_task(self._send_alert_if_needed(error))
            
            # Track error patterns
            self._track_error_patterns(error)
            
        except Exception as e:
            logger.critical(f"Error handling ML error: {str(e)}")
    
    def _log_error(self, error: MLError) -> None:
        """Log error with appropriate level."""
        log_message = f"[{error.error_type.value}] {error.message}"
        
        if error.severity == MLErrorSeverity.CRITICAL:
            logger.critical(log_message, extra={
                "error_id": error.error_id,
                "component": error.component,
                "operation": error.operation,
                "details": error.details
            })
        elif error.severity == MLErrorSeverity.HIGH:
            logger.error(log_message, extra={
                "error_id": error.error_id,
                "component": error.component,
                "operation": error.operation,
                "details": error.details
            })
        elif error.severity == MLErrorSeverity.MEDIUM:
            logger.warning(log_message, extra={
                "error_id": error.error_id,
                "component": error.component,
                "operation": error.operation
            })
        else:
            logger.info(log_message, extra={
                "error_id": error.error_id,
                "component": error.component
            })
    
    def _store_error(self, error: MLError) -> None:
        """Store error in database."""
        try:
            self.supabase.table('ml_errors').insert(error.to_dict()).execute()
        except Exception as e:
            logger.error(f"Failed to store ML error: {str(e)}")
    
    def _update_error_metrics(self, error: MLError) -> None:
        """Update error metrics."""
        try:
            # Increment error counter
            self.metrics_collector.increment_counter(
                "ml_errors_total",
                tags={
                    "error_type": error.error_type.value,
                    "severity": error.severity.value,
                    "component": error.component,
                    "operation": error.operation
                }
            )
            
            # Track error rate
            self.metrics_collector.record_gauge(
                "ml_error_rate",
                self._calculate_error_rate(error.component),
                tags={"component": error.component}
            )
            
        except Exception as e:
            logger.error(f"Failed to update error metrics: {str(e)}")
    
    async def _send_alert_if_needed(self, error: MLError) -> None:
        """Send alert if error severity warrants it."""
        try:
            if error.severity in [MLErrorSeverity.HIGH, MLErrorSeverity.CRITICAL]:
                await self.alerting_system.send_alert(
                    title=f"ML {error.severity.value.upper()} Error",
                    message=f"{error.component} - {error.message}",
                    severity=error.severity.value,
                    channels=["email", "slack"],
                    metadata={
                        "error_id": error.error_id,
                        "component": error.component,
                        "operation": error.operation,
                        "model_id": error.model_id
                    }
                )
        except Exception as e:
            logger.error(f"Failed to send error alert: {str(e)}")
    
    def _track_error_patterns(self, error: MLError) -> None:
        """Track error patterns for analysis."""
        try:
            error_key = f"{error.component}:{error.error_type.value}"
            self.error_counts[error_key] = self.error_counts.get(error_key, 0) + 1
            
            # Check for error storms (too many errors in short time)
            if self.error_counts[error_key] > 10:  # Threshold
                logger.warning(f"Potential error storm detected: {error_key}")
                asyncio.create_task(self._handle_error_storm(error_key, error))
                
        except Exception as e:
            logger.error(f"Failed to track error patterns: {str(e)}")
    
    async def _handle_error_storm(self, error_key: str, error: MLError) -> None:
        """Handle error storm situation."""
        try:
            await self.alerting_system.send_alert(
                title="ML Error Storm Detected",
                message=f"High frequency of errors detected: {error_key}",
                severity="critical",
                channels=["email", "slack", "pagerduty"],
                metadata={
                    "error_key": error_key,
                    "error_count": self.error_counts[error_key],
                    "component": error.component
                }
            )
            
            # Reset counter after alert
            self.error_counts[error_key] = 0
            
        except Exception as e:
            logger.error(f"Failed to handle error storm: {str(e)}")
    
    def _calculate_error_rate(self, component: str) -> float:
        """Calculate error rate for a component."""
        try:
            # Simple implementation - in production this would be more sophisticated
            error_key = f"{component}:total"
            return min(self.error_counts.get(error_key, 0) * 0.01, 1.0)
        except:
            return 0.0
    
    def get_error_statistics(self, time_range: timedelta = timedelta(hours=24)) -> Dict[str, Any]:
        """Get error statistics."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - time_range
            
            # Query error statistics from database
            result = self.supabase.table('ml_errors').select('*').gte(
                'timestamp', start_time.isoformat()
            ).lte('timestamp', end_time.isoformat()).execute()
            
            errors = result.data or []
            
            # Calculate statistics
            stats = {
                "total_errors": len(errors),
                "error_by_type": {},
                "error_by_severity": {},
                "error_by_component": {},
                "error_rate_trend": []
            }
            
            for error in errors:
                # By type
                error_type = error.get('error_type', 'unknown')
                stats["error_by_type"][error_type] = stats["error_by_type"].get(error_type, 0) + 1
                
                # By severity
                severity = error.get('severity', 'unknown')
                stats["error_by_severity"][severity] = stats["error_by_severity"].get(severity, 0) + 1
                
                # By component
                component = error.get('component', 'unknown')
                stats["error_by_component"][component] = stats["error_by_component"].get(component, 0) + 1
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get error statistics: {str(e)}")
            return {"error": str(e)}


# Global error handler instance
_error_handler = MLErrorHandler()


def ml_error_handler(
    component: str,
    operation: str,
    error_type: MLErrorType = MLErrorType.SYSTEM_ERROR,
    severity: MLErrorSeverity = MLErrorSeverity.MEDIUM,
    capture_traceback: bool = True
):
    """Decorator for ML error handling."""
    
    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    error = MLError(
                        error_id=f"error_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}",
                        error_type=error_type,
                        severity=severity,
                        message=str(e),
                        details={"args": str(args), "kwargs": str(kwargs)},
                        timestamp=datetime.utcnow(),
                        component=component,
                        operation=operation,
                        traceback=traceback.format_exc() if capture_traceback else None,
                        context={"function": func.__name__}
                    )
                    
                    _error_handler.handle_error(error)
                    raise
            
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error = MLError(
                        error_id=f"error_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}",
                        error_type=error_type,
                        severity=severity,
                        message=str(e),
                        details={"args": str(args), "kwargs": str(kwargs)},
                        timestamp=datetime.utcnow(),
                        component=component,
                        operation=operation,
                        traceback=traceback.format_exc() if capture_traceback else None,
                        context={"function": func.__name__}
                    )
                    
                    _error_handler.handle_error(error)
                    raise
            
            return sync_wrapper
    
    return decorator


def log_ml_operation(
    component: str,
    operation: str,
    level: str = "info",
    include_timing: bool = True
):
    """Decorator for ML operation logging."""
    
    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start_time = datetime.utcnow()
                
                getattr(logger, level)(
                    f"Starting {component}.{operation}",
                    extra={
                        "component": component,
                        "operation": operation,
                        "function": func.__name__,
                        "start_time": start_time.isoformat()
                    }
                )
                
                try:
                    result = await func(*args, **kwargs)
                    
                    if include_timing:
                        duration = (datetime.utcnow() - start_time).total_seconds()
                        getattr(logger, level)(
                            f"Completed {component}.{operation} in {duration:.2f}s",
                            extra={
                                "component": component,
                                "operation": operation,
                                "duration_seconds": duration
                            }
                        )
                    else:
                        getattr(logger, level)(
                            f"Completed {component}.{operation}",
                            extra={
                                "component": component,
                                "operation": operation
                            }
                        )
                    
                    return result
                    
                except Exception as e:
                    duration = (datetime.utcnow() - start_time).total_seconds()
                    logger.error(
                        f"Failed {component}.{operation} after {duration:.2f}s: {str(e)}",
                        extra={
                            "component": component,
                            "operation": operation,
                            "duration_seconds": duration,
                            "error": str(e)
                        }
                    )
                    raise
            
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start_time = datetime.utcnow()
                
                getattr(logger, level)(
                    f"Starting {component}.{operation}",
                    extra={
                        "component": component,
                        "operation": operation,
                        "function": func.__name__,
                        "start_time": start_time.isoformat()
                    }
                )
                
                try:
                    result = func(*args, **kwargs)
                    
                    if include_timing:
                        duration = (datetime.utcnow() - start_time).total_seconds()
                        getattr(logger, level)(
                            f"Completed {component}.{operation} in {duration:.2f}s",
                            extra={
                                "component": component,
                                "operation": operation,
                                "duration_seconds": duration
                            }
                        )
                    else:
                        getattr(logger, level)(
                            f"Completed {component}.{operation}",
                            extra={
                                "component": component,
                                "operation": operation
                            }
                        )
                    
                    return result
                    
                except Exception as e:
                    duration = (datetime.utcnow() - start_time).total_seconds()
                    logger.error(
                        f"Failed {component}.{operation} after {duration:.2f}s: {str(e)}",
                        extra={
                            "component": component,
                            "operation": operation,
                            "duration_seconds": duration,
                            "error": str(e)
                        }
                    )
                    raise
            
            return sync_wrapper
    
    return decorator


def validate_ml_input(validation_rules: Dict[str, Any]):
    """Decorator for ML input validation."""
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                # Validate inputs based on rules
                for param_name, rules in validation_rules.items():
                    if param_name in kwargs:
                        value = kwargs[param_name]
                        
                        # Type validation
                        if "type" in rules and not isinstance(value, rules["type"]):
                            raise ValueError(f"Parameter {param_name} must be of type {rules['type']}")
                        
                        # Required validation
                        if rules.get("required", False) and value is None:
                            raise ValueError(f"Parameter {param_name} is required")
                        
                        # Range validation for numeric values
                        if "min" in rules and hasattr(value, "__len__") and len(value) < rules["min"]:
                            raise ValueError(f"Parameter {param_name} must have minimum length {rules['min']}")
                        
                        if "max" in rules and hasattr(value, "__len__") and len(value) > rules["max"]:
                            raise ValueError(f"Parameter {param_name} must have maximum length {rules['max']}")
                
                return func(*args, **kwargs)
                
            except Exception as e:
                error = MLError(
                    error_id=f"validation_error_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}",
                    error_type=MLErrorType.DATA_ERROR,
                    severity=MLErrorSeverity.MEDIUM,
                    message=f"Input validation failed: {str(e)}",
                    details={"validation_rules": validation_rules, "kwargs": str(kwargs)},
                    timestamp=datetime.utcnow(),
                    component="validation",
                    operation="input_validation",
                    context={"function": func.__name__}
                )
                
                _error_handler.handle_error(error)
                raise
        
        return wrapper
    
    return decorator


class MLContextLogger:
    """Context manager for ML operation logging."""
    
    def __init__(self, component: str, operation: str, model_id: Optional[str] = None):
        self.component = component
        self.operation = operation
        self.model_id = model_id
        self.start_time = None
        
    def __enter__(self):
        self.start_time = datetime.utcnow()
        logger.info(
            f"Starting {self.component}.{self.operation}",
            extra={
                "component": self.component,
                "operation": self.operation,
                "model_id": self.model_id,
                "start_time": self.start_time.isoformat()
            }
        )
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.utcnow() - self.start_time).total_seconds()
        
        if exc_type is None:
            logger.info(
                f"Completed {self.component}.{self.operation} in {duration:.2f}s",
                extra={
                    "component": self.component,
                    "operation": self.operation,
                    "model_id": self.model_id,
                    "duration_seconds": duration
                }
            )
        else:
            logger.error(
                f"Failed {self.component}.{self.operation} after {duration:.2f}s: {str(exc_val)}",
                extra={
                    "component": self.component,
                    "operation": self.operation,
                    "model_id": self.model_id,
                    "duration_seconds": duration,
                    "error": str(exc_val)
                }
            )
            
            # Create ML error
            error = MLError(
                error_id=f"context_error_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}",
                error_type=MLErrorType.SYSTEM_ERROR,
                severity=MLErrorSeverity.HIGH,
                message=str(exc_val),
                details={
                    "operation_duration": duration,
                    "exception_type": str(exc_type.__name__) if exc_type else None
                },
                timestamp=datetime.utcnow(),
                component=self.component,
                operation=self.operation,
                model_id=self.model_id,
                traceback=traceback.format_exc()
            )
            
            _error_handler.handle_error(error)


# Utility functions
def get_error_handler() -> MLErrorHandler:
    """Get global error handler instance."""
    return _error_handler


def report_ml_error(
    message: str,
    error_type: MLErrorType,
    component: str,
    operation: str,
    severity: MLErrorSeverity = MLErrorSeverity.MEDIUM,
    details: Optional[Dict[str, Any]] = None,
    model_id: Optional[str] = None
) -> None:
    """Manually report an ML error."""
    error = MLError(
        error_id=f"manual_error_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}",
        error_type=error_type,
        severity=severity,
        message=message,
        details=details or {},
        timestamp=datetime.utcnow(),
        component=component,
        operation=operation,
        model_id=model_id
    )
    
    _error_handler.handle_error(error)


def get_ml_error_statistics(hours: int = 24) -> Dict[str, Any]:
    """Get ML error statistics for the specified time range."""
    return _error_handler.get_error_statistics(timedelta(hours=hours))
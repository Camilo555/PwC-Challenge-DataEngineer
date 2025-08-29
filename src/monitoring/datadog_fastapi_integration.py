"""
DataDog FastAPI Integration and APM Setup
Provides comprehensive FastAPI integration with DataDog APM for enterprise data platform
"""

import asyncio
import os
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from fastapi import FastAPI, Request, Response, HTTPException, Depends
from fastapi.middleware.base import BaseHTTPMiddleware
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp

# DataDog imports
from ddtrace import patch_all, tracer, config
from ddtrace.contrib.fastapi import patch as patch_fastapi

from core.config import get_settings
from core.logging import get_logger
from monitoring.datadog_apm_initialization import (
    initialize_datadog_apm, shutdown_datadog_apm, datadog_apm_startup_hook, datadog_apm_shutdown_hook
)
from monitoring.datadog_apm_middleware import DataDogAPMMiddleware, DataDogAPMConfig
from monitoring.datadog_business_metrics import get_business_metrics_tracker
from monitoring.datadog_distributed_tracing import get_distributed_tracer
from monitoring.datadog_error_tracking import get_error_tracker, capture_exception
from monitoring.datadog_profiling import get_datadog_profiler
from monitoring.datadog_integration import create_datadog_monitoring

logger = get_logger(__name__)
settings = get_settings()


class DataDogFastAPIIntegrator:
    """
    Comprehensive DataDog APM integration for FastAPI applications
    
    Features:
    - Automatic APM initialization and shutdown
    - Request/response tracing and metrics
    - Business metrics tracking
    - Error tracking and analysis
    - Distributed tracing correlation
    - Performance profiling
    - Health check endpoints
    - APM dashboard endpoints
    - Custom middleware integration
    """
    
    def __init__(self, app: FastAPI, service_name: str = "fastapi-service"):
        self.app = app
        self.service_name = service_name
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Initialize components
        self.datadog_monitoring = None
        self.business_metrics = None
        self.distributed_tracer = None
        self.error_tracker = None
        self.profiler = None
        
        # Integration status
        self.is_integrated = False
        self.integration_start_time = time.time()
    
    async def setup_datadog_integration(self) -> bool:
        """Setup comprehensive DataDog APM integration."""
        
        try:
            self.logger.info("Setting up DataDog APM integration for FastAPI...")
            
            # Step 1: Initialize DataDog APM
            success = await initialize_datadog_apm(self.service_name)
            if not success:
                self.logger.error("DataDog APM initialization failed")
                return False
            
            # Step 2: Setup DataDog monitoring client
            self.datadog_monitoring = create_datadog_monitoring(
                service_name=self.service_name,
                environment=getattr(settings, 'ENVIRONMENT', 'development')
            )
            
            # Step 3: Initialize business metrics tracking
            self.business_metrics = get_business_metrics_tracker(
                service_name=f"{self.service_name}-business",
                datadog_monitoring=self.datadog_monitoring
            )
            
            # Step 4: Initialize distributed tracing
            self.distributed_tracer = get_distributed_tracer(
                service_name=f"{self.service_name}-tracing",
                datadog_monitoring=self.datadog_monitoring
            )
            
            # Step 5: Initialize error tracking
            self.error_tracker = get_error_tracker(
                service_name=f"{self.service_name}-errors",
                datadog_monitoring=self.datadog_monitoring
            )
            
            # Step 6: Initialize profiler
            self.profiler = get_datadog_profiler(
                service_name=f"{self.service_name}-profiler",
                datadog_monitoring=self.datadog_monitoring
            )
            
            # Step 7: Setup FastAPI middleware
            await self._setup_fastapi_middleware()
            
            # Step 8: Setup APM endpoints
            self._setup_apm_endpoints()
            
            # Step 9: Setup lifecycle hooks
            self._setup_lifecycle_hooks()
            
            self.is_integrated = True
            integration_time = time.time() - self.integration_start_time
            
            self.logger.info(f"DataDog APM integration completed successfully in {integration_time:.2f}s")
            
            # Send integration success metric
            if self.datadog_monitoring:
                await self.datadog_monitoring.counter(
                    "fastapi.integration.success",
                    tags=[f"service:{self.service_name}"]
                )
                await self.datadog_monitoring.histogram(
                    "fastapi.integration.duration",
                    integration_time * 1000,
                    tags=[f"service:{self.service_name}"]
                )
            
            return True
            
        except Exception as e:
            integration_time = time.time() - self.integration_start_time
            self.logger.error(f"DataDog APM integration failed: {str(e)}")
            
            # Send integration failure metric
            if self.datadog_monitoring:
                await self.datadog_monitoring.counter(
                    "fastapi.integration.failure",
                    tags=[
                        f"service:{self.service_name}",
                        f"error_type:{type(e).__name__}"
                    ]
                )
            
            return False
    
    async def _setup_fastapi_middleware(self):
        """Setup FastAPI-specific DataDog middleware."""
        
        # Create APM configuration
        apm_config = DataDogAPMConfig()
        apm_config.service_name = self.service_name
        apm_config.enable_custom_metrics = True
        apm_config.enable_business_metrics = True
        apm_config.enable_error_tracking = True
        apm_config.enable_performance_monitoring = True
        
        # Add DataDog APM middleware
        self.app.add_middleware(DataDogAPMMiddleware, config=apm_config)
        
        # Add custom integration middleware
        self.app.add_middleware(FastAPIDataDogMiddleware, integrator=self)
        
        self.logger.info("FastAPI DataDog middleware configured")
    
    def _setup_apm_endpoints(self):
        """Setup APM monitoring and health check endpoints."""
        
        @self.app.get("/apm/health", include_in_schema=False)
        async def apm_health():
            """APM health check endpoint."""
            try:
                health_data = {
                    "service": self.service_name,
                    "timestamp": datetime.utcnow().isoformat(),
                    "status": "healthy" if self.is_integrated else "unhealthy",
                    "integration_time": time.time() - self.integration_start_time,
                    "components": {
                        "datadog_monitoring": self.datadog_monitoring is not None,
                        "business_metrics": self.business_metrics is not None,
                        "distributed_tracing": self.distributed_tracer is not None,
                        "error_tracking": self.error_tracker is not None,
                        "profiler": self.profiler is not None,
                        "profiler_active": self.profiler.is_active if self.profiler else False
                    }
                }
                
                # Send health check metric
                if self.datadog_monitoring:
                    await self.datadog_monitoring.counter(
                        "fastapi.apm.health.check",
                        tags=[f"service:{self.service_name}", "status:healthy"]
                    )
                
                return health_data
                
            except Exception as e:
                await capture_exception(e, tags={"endpoint": "apm_health"})
                return JSONResponse(
                    status_code=503,
                    content={
                        "service": self.service_name,
                        "status": "error",
                        "error": str(e),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
        
        @self.app.get("/apm/metrics", include_in_schema=False)
        async def apm_metrics():
            """APM metrics summary endpoint."""
            try:
                metrics_data = {
                    "service": self.service_name,
                    "timestamp": datetime.utcnow().isoformat(),
                    "business_metrics": {},
                    "error_metrics": {},
                    "performance_metrics": {},
                    "tracing_metrics": {}
                }
                
                # Get business metrics
                if self.business_metrics:
                    metrics_data["business_metrics"] = await self.business_metrics.calculate_realtime_kpis()
                
                # Get error metrics
                if self.error_tracker:
                    metrics_data["error_metrics"] = self.error_tracker.get_error_dashboard_data()
                
                # Get performance metrics
                if self.profiler:
                    metrics_data["performance_metrics"] = self.profiler.get_system_health_summary()
                
                # Get tracing metrics
                if self.distributed_tracer:
                    metrics_data["tracing_metrics"] = self.distributed_tracer.get_trace_performance_analysis()
                
                return metrics_data
                
            except Exception as e:
                await capture_exception(e, tags={"endpoint": "apm_metrics"})
                return JSONResponse(
                    status_code=500,
                    content={"error": str(e), "timestamp": datetime.utcnow().isoformat()}
                )
        
        @self.app.get("/apm/errors", include_in_schema=False)
        async def apm_errors():
            """APM error tracking endpoint."""
            try:
                if not self.error_tracker:
                    return {"error": "Error tracking not initialized"}
                
                return self.error_tracker.get_error_dashboard_data()
                
            except Exception as e:
                await capture_exception(e, tags={"endpoint": "apm_errors"})
                return JSONResponse(
                    status_code=500,
                    content={"error": str(e), "timestamp": datetime.utcnow().isoformat()}
                )
        
        @self.app.get("/apm/performance", include_in_schema=False)
        async def apm_performance():
            """APM performance analysis endpoint."""
            try:
                if not self.profiler:
                    return {"error": "Profiler not initialized"}
                
                return {
                    "system_health": self.profiler.get_system_health_summary(),
                    "performance_analysis": self.profiler.analyze_performance(),
                    "hotspots": self.profiler.identify_hotspots()
                }
                
            except Exception as e:
                await capture_exception(e, tags={"endpoint": "apm_performance"})
                return JSONResponse(
                    status_code=500,
                    content={"error": str(e), "timestamp": datetime.utcnow().isoformat()}
                )
        
        @self.app.post("/apm/profiling/start", include_in_schema=False)
        async def start_profiling():
            """Start DataDog profiling."""
            try:
                if not self.profiler:
                    return {"error": "Profiler not initialized"}
                
                success = self.profiler.start_profiling()
                return {
                    "success": success,
                    "status": "started" if success else "failed",
                    "timestamp": datetime.utcnow().isoformat()
                }
                
            except Exception as e:
                await capture_exception(e, tags={"endpoint": "start_profiling"})
                return JSONResponse(
                    status_code=500,
                    content={"error": str(e), "timestamp": datetime.utcnow().isoformat()}
                )
        
        @self.app.post("/apm/profiling/stop", include_in_schema=False)
        async def stop_profiling():
            """Stop DataDog profiling."""
            try:
                if not self.profiler:
                    return {"error": "Profiler not initialized"}
                
                success = self.profiler.stop_profiling()
                return {
                    "success": success,
                    "status": "stopped" if success else "failed",
                    "timestamp": datetime.utcnow().isoformat()
                }
                
            except Exception as e:
                await capture_exception(e, tags={"endpoint": "stop_profiling"})
                return JSONResponse(
                    status_code=500,
                    content={"error": str(e), "timestamp": datetime.utcnow().isoformat()}
                )
        
        self.logger.info("APM monitoring endpoints configured")
    
    def _setup_lifecycle_hooks(self):
        """Setup application lifecycle hooks for DataDog APM."""
        
        @self.app.on_event("startup")
        async def startup_apm():
            """APM startup hook."""
            try:
                # Start profiling if enabled
                if self.profiler and getattr(settings, 'DD_PROFILING_ENABLED', 'false').lower() == 'true':
                    self.profiler.start_profiling()
                
                # Send startup metric
                if self.datadog_monitoring:
                    await self.datadog_monitoring.counter(
                        "fastapi.application.startup",
                        tags=[f"service:{self.service_name}"]
                    )
                
                self.logger.info("FastAPI DataDog APM startup completed")
                
            except Exception as e:
                self.logger.error(f"FastAPI DataDog APM startup failed: {str(e)}")
        
        @self.app.on_event("shutdown")
        async def shutdown_apm():
            """APM shutdown hook."""
            try:
                # Send shutdown metric
                if self.datadog_monitoring:
                    await self.datadog_monitoring.counter(
                        "fastapi.application.shutdown",
                        tags=[f"service:{self.service_name}"]
                    )
                
                # Stop profiling
                if self.profiler and self.profiler.is_active:
                    self.profiler.stop_profiling()
                
                # Shutdown DataDog APM
                await shutdown_datadog_apm()
                
                self.logger.info("FastAPI DataDog APM shutdown completed")
                
            except Exception as e:
                self.logger.error(f"FastAPI DataDog APM shutdown failed: {str(e)}")
        
        self.logger.info("APM lifecycle hooks configured")
    
    def get_integration_status(self) -> Dict[str, Any]:
        """Get DataDog integration status."""
        
        return {
            "service_name": self.service_name,
            "is_integrated": self.is_integrated,
            "integration_duration": time.time() - self.integration_start_time,
            "components": {
                "datadog_monitoring": self.datadog_monitoring is not None,
                "business_metrics": self.business_metrics is not None,
                "distributed_tracing": self.distributed_tracer is not None,
                "error_tracking": self.error_tracker is not None,
                "profiler": self.profiler is not None,
                "profiler_active": self.profiler.is_active if self.profiler else False
            },
            "endpoints": [
                "/apm/health",
                "/apm/metrics", 
                "/apm/errors",
                "/apm/performance",
                "/apm/profiling/start",
                "/apm/profiling/stop"
            ]
        }


class FastAPIDataDogMiddleware(BaseHTTPMiddleware):
    """Custom FastAPI middleware for additional DataDog integration."""
    
    def __init__(self, app: ASGIApp, integrator: DataDogFastAPIIntegrator):
        super().__init__(app)
        self.integrator = integrator
        self.logger = get_logger(f"{__name__}.middleware")
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process requests with additional DataDog integration."""
        
        start_time = time.time()
        
        try:
            # Extract trace context from headers
            if self.integrator.distributed_tracer:
                trace_context = self.integrator.distributed_tracer.extract_trace_context(dict(request.headers))
                if trace_context:
                    # Add trace context to request state
                    request.state.trace_context = trace_context
            
            # Process request
            response = await call_next(request)
            
            # Track successful request metrics
            if self.integrator.business_metrics:
                await self._track_request_success(request, response, time.time() - start_time)
            
            # Add trace context to response headers
            if self.integrator.distributed_tracer and hasattr(request.state, 'trace_context'):
                response.headers.update(
                    self.integrator.distributed_tracer.inject_trace_context({})
                )
            
            return response
            
        except Exception as e:
            # Track error
            if self.integrator.error_tracker:
                await self.integrator.error_tracker.capture_error(
                    e,
                    tags={"source": "fastapi_middleware", "endpoint": str(request.url.path)}
                )
            
            # Track failed request metrics
            if self.integrator.business_metrics:
                await self._track_request_failure(request, e, time.time() - start_time)
            
            # Re-raise exception to be handled by FastAPI
            raise
    
    async def _track_request_success(self, request: Request, response: Response, duration: float):
        """Track successful request metrics."""
        
        try:
            # Track API request business event
            await self.integrator.business_metrics.track_business_event({
                "event_type": "api_request",
                "event_name": "request_completed",
                "value": duration * 1000,  # duration in ms
                "properties": {
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                    "success": True
                }
            })
            
            # Update API performance KPIs
            await self.integrator.business_metrics.track_kpi(
                "api_response_time_p95",
                duration * 1000,
                tags={"endpoint": request.url.path, "method": request.method}
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to track request success metrics: {str(e)}")
    
    async def _track_request_failure(self, request: Request, exception: Exception, duration: float):
        """Track failed request metrics."""
        
        try:
            # Track API error business event
            await self.integrator.business_metrics.track_business_event({
                "event_type": "api_error",
                "event_name": "request_failed",
                "value": duration * 1000,
                "properties": {
                    "method": request.method,
                    "path": request.url.path,
                    "error_type": type(exception).__name__,
                    "error_message": str(exception),
                    "success": False
                }
            })
            
            # Update error rate KPI
            await self.integrator.business_metrics.track_kpi(
                "error_rate",
                1.0,  # Count as 1 error
                tags={"endpoint": request.url.path, "method": request.method}
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to track request failure metrics: {str(e)}")


# Integration helper functions

async def setup_fastapi_datadog_integration(app: FastAPI, service_name: str) -> DataDogFastAPIIntegrator:
    """Setup comprehensive DataDog APM integration for FastAPI."""
    
    integrator = DataDogFastAPIIntegrator(app, service_name)
    success = await integrator.setup_datadog_integration()
    
    if not success:
        logger.error("Failed to setup DataDog APM integration for FastAPI")
        raise RuntimeError("DataDog APM integration failed")
    
    logger.info(f"DataDog APM integration successfully setup for FastAPI service: {service_name}")
    return integrator


def create_fastapi_with_datadog(service_name: str, 
                               title: Optional[str] = None,
                               description: Optional[str] = None,
                               version: str = "1.0.0") -> Tuple[FastAPI, DataDogFastAPIIntegrator]:
    """Create FastAPI application with DataDog APM integration."""
    
    # Create FastAPI app
    app = FastAPI(
        title=title or f"{service_name} API",
        description=description or f"API for {service_name} with DataDog APM integration",
        version=version
    )
    
    # Create integrator (setup will be called separately)
    integrator = DataDogFastAPIIntegrator(app, service_name)
    
    return app, integrator


# Decorator for DataDog APM route monitoring
def monitor_endpoint(operation_name: Optional[str] = None,
                    track_business_metrics: bool = True,
                    track_performance: bool = True):
    """Decorator for monitoring FastAPI endpoints with DataDog APM."""
    
    def decorator(func: Callable) -> Callable:
        op_name = operation_name or f"endpoint.{func.__name__}"
        
        async def async_wrapper(*args, **kwargs):
            # Get integrator from app (assuming it's available in context)
            integrator = getattr(func, '_datadog_integrator', None)
            
            with tracer.trace(f"fastapi.endpoint.{op_name}", service="fastapi-service") as span:
                span.set_tag("endpoint.name", func.__name__)
                span.set_tag("endpoint.operation", op_name)
                
                start_time = time.time()
                
                try:
                    result = await func(*args, **kwargs)
                    
                    duration = time.time() - start_time
                    span.set_tag("endpoint.duration_ms", duration * 1000)
                    span.set_tag("endpoint.success", True)
                    
                    # Track business metrics if enabled and integrator available
                    if track_business_metrics and integrator and integrator.business_metrics:
                        await integrator.business_metrics.track_business_event({
                            "event_type": "endpoint_call",
                            "event_name": op_name,
                            "value": duration * 1000,
                            "properties": {"success": True}
                        })
                    
                    return result
                    
                except Exception as e:
                    duration = time.time() - start_time
                    span.set_error(e)
                    span.set_tag("endpoint.duration_ms", duration * 1000)
                    span.set_tag("endpoint.success", False)
                    
                    # Track error if integrator available
                    if integrator and integrator.error_tracker:
                        await integrator.error_tracker.capture_error(
                            e,
                            tags={"endpoint": func.__name__, "operation": op_name}
                        )
                    
                    raise
        
        return async_wrapper
    
    return decorator


# Example usage and configuration
def configure_datadog_for_environment():
    """Configure DataDog based on environment variables."""
    
    environment = os.getenv("ENVIRONMENT", "development")
    
    if environment == "production":
        # Production configuration
        os.environ.setdefault("DD_TRACE_SAMPLE_RATE", "0.1")
        os.environ.setdefault("DD_PROFILING_ENABLED", "true")
        os.environ.setdefault("DD_RUNTIME_METRICS_ENABLED", "true")
        os.environ.setdefault("DD_LOGS_INJECTION", "true")
    elif environment == "staging":
        # Staging configuration
        os.environ.setdefault("DD_TRACE_SAMPLE_RATE", "0.5")
        os.environ.setdefault("DD_PROFILING_ENABLED", "true")
        os.environ.setdefault("DD_RUNTIME_METRICS_ENABLED", "true")
    else:
        # Development configuration
        os.environ.setdefault("DD_TRACE_SAMPLE_RATE", "1.0")
        os.environ.setdefault("DD_PROFILING_ENABLED", "false")
        os.environ.setdefault("DD_RUNTIME_METRICS_ENABLED", "true")
    
    # Common configuration
    os.environ.setdefault("DD_SERVICE", "pwc-data-engineering")
    os.environ.setdefault("DD_ENV", environment)
    os.environ.setdefault("DD_VERSION", "1.0.0")
    os.environ.setdefault("DD_TRACE_ANALYTICS_ENABLED", "true")


# Global integrator for easy access
_fastapi_integrator: Optional[DataDogFastAPIIntegrator] = None


def get_fastapi_integrator() -> Optional[DataDogFastAPIIntegrator]:
    """Get the global FastAPI DataDog integrator."""
    return _fastapi_integrator


def set_fastapi_integrator(integrator: DataDogFastAPIIntegrator):
    """Set the global FastAPI DataDog integrator."""
    global _fastapi_integrator
    _fastapi_integrator = integrator
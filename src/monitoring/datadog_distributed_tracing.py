"""
DataDog Distributed Tracing and Correlation System
Provides comprehensive distributed tracing across microservices, data pipelines, and business operations
"""

import asyncio
import json
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum

# DataDog tracing imports
from ddtrace import tracer, patch_all, config
from ddtrace.context import Context
from ddtrace.ext import SpanTypes, http, errors
from ddtrace.propagation.http import HTTPPropagator
from ddtrace.span import Span

from core.config import get_settings
from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.datadog_apm_middleware import add_custom_tags, add_custom_metric

logger = get_logger(__name__)
settings = get_settings()


class TraceType(Enum):
    """Types of distributed traces."""
    WEB_REQUEST = "web_request"
    ETL_PIPELINE = "etl_pipeline" 
    ML_PIPELINE = "ml_pipeline"
    STREAMING_PIPELINE = "streaming_pipeline"
    BUSINESS_PROCESS = "business_process"
    DATA_QUALITY = "data_quality"
    USER_JOURNEY = "user_journey"
    SYSTEM_OPERATION = "system_operation"


class CorrelationLevel(Enum):
    """Levels of trace correlation."""
    REQUEST = "request"  # Single request traces
    SESSION = "session"  # User session traces
    WORKFLOW = "workflow"  # Business workflow traces
    PIPELINE = "pipeline"  # Data pipeline traces
    SYSTEM = "system"  # System-wide operations


@dataclass
class TraceContext:
    """Comprehensive trace context information."""
    trace_id: str
    span_id: str
    parent_id: Optional[str]
    correlation_id: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    pipeline_id: Optional[str] = None
    workflow_id: Optional[str] = None
    business_context: Optional[Dict[str, Any]] = None
    service_context: Optional[Dict[str, Any]] = None
    custom_context: Optional[Dict[str, Any]] = None


@dataclass
class DistributedTrace:
    """Distributed trace with correlation metadata."""
    trace_id: str
    trace_type: TraceType
    correlation_level: CorrelationLevel
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    spans: List[str] = field(default_factory=list)
    services: List[str] = field(default_factory=list)
    status: str = "active"  # active, completed, failed
    error_count: int = 0
    custom_tags: Dict[str, Any] = field(default_factory=dict)
    correlation_context: Optional[TraceContext] = None


class DataDogDistributedTracer:
    """
    Comprehensive distributed tracing and correlation system
    
    Features:
    - End-to-end request tracing across microservices
    - Correlation ID management and propagation
    - Business workflow tracing
    - Data pipeline correlation
    - Cross-service dependency mapping
    - Performance bottleneck identification
    - Error propagation tracking
    - Custom correlation contexts
    """
    
    def __init__(self, service_name: str = "distributed-tracing", 
                 datadog_monitoring: Optional[DatadogMonitoring] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Trace management
        self.active_traces: Dict[str, DistributedTrace] = {}
        self.trace_correlations: Dict[str, List[str]] = {}  # correlation_id -> trace_ids
        self.service_dependencies: Dict[str, List[str]] = {}
        
        # Context propagation
        self.context_propagator = HTTPPropagator()
        
        # Performance tracking
        self.trace_statistics = {
            "total_traces": 0,
            "completed_traces": 0,
            "failed_traces": 0,
            "avg_duration": 0.0,
            "correlation_success_rate": 0.0
        }
        
        # Correlation patterns
        self.correlation_patterns: Dict[str, Dict[str, Any]] = {}
    
    def generate_correlation_id(self, correlation_level: CorrelationLevel, 
                              context: Optional[Dict[str, Any]] = None) -> str:
        """Generate a correlation ID for distributed tracing."""
        
        timestamp = int(time.time() * 1000)
        unique_part = str(uuid.uuid4())[:8]
        
        if correlation_level == CorrelationLevel.REQUEST:
            prefix = "req"
        elif correlation_level == CorrelationLevel.SESSION:
            prefix = "sess"
        elif correlation_level == CorrelationLevel.WORKFLOW:
            prefix = "wf"
        elif correlation_level == CorrelationLevel.PIPELINE:
            prefix = "pipe"
        else:
            prefix = "sys"
        
        # Include context-specific elements
        if context:
            if context.get("user_id"):
                prefix += f"-{context['user_id'][:8]}"
            elif context.get("pipeline_name"):
                prefix += f"-{context['pipeline_name'][:8]}"
        
        correlation_id = f"{prefix}-{timestamp}-{unique_part}"
        
        # Store correlation pattern
        self.correlation_patterns[correlation_id] = {
            "level": correlation_level.value,
            "created_at": datetime.utcnow(),
            "context": context or {}
        }
        
        return correlation_id
    
    def get_current_trace_context(self) -> Optional[TraceContext]:
        """Get current trace context from active span."""
        
        current_span = tracer.current_span()
        if not current_span:
            return None
        
        # Extract context from span
        trace_id = str(current_span.trace_id)
        span_id = str(current_span.span_id)
        parent_id = str(current_span.parent_id) if current_span.parent_id else None
        
        # Look for correlation ID in span tags
        correlation_id = current_span.get_tag("correlation_id")
        user_id = current_span.get_tag("user.id")
        session_id = current_span.get_tag("session.id")
        request_id = current_span.get_tag("request.id")
        pipeline_id = current_span.get_tag("pipeline.id")
        workflow_id = current_span.get_tag("workflow.id")
        
        # Extract custom contexts
        business_context = {}
        service_context = {}
        custom_context = {}
        
        for key, value in current_span.get_tags().items():
            if key.startswith("business."):
                business_context[key[9:]] = value
            elif key.startswith("service."):
                service_context[key[8:]] = value
            elif key.startswith("custom."):
                custom_context[key[7:]] = value
        
        return TraceContext(
            trace_id=trace_id,
            span_id=span_id,
            parent_id=parent_id,
            correlation_id=correlation_id or trace_id,
            user_id=user_id,
            session_id=session_id,
            request_id=request_id,
            pipeline_id=pipeline_id,
            workflow_id=workflow_id,
            business_context=business_context if business_context else None,
            service_context=service_context if service_context else None,
            custom_context=custom_context if custom_context else None
        )
    
    def inject_trace_context(self, headers: Dict[str, str], 
                           correlation_id: Optional[str] = None,
                           custom_context: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """Inject trace context into HTTP headers for propagation."""
        
        # Get current context
        current_context = self.get_current_trace_context()
        
        # Use provided correlation ID or current one
        if correlation_id:
            final_correlation_id = correlation_id
        elif current_context:
            final_correlation_id = current_context.correlation_id
        else:
            final_correlation_id = self.generate_correlation_id(CorrelationLevel.REQUEST)
        
        # Inject DataDog trace context
        injected_headers = headers.copy()
        self.context_propagator.inject(tracer.current_span_context(), injected_headers)
        
        # Add custom correlation headers
        injected_headers.update({
            "X-Correlation-ID": final_correlation_id,
            "X-Request-ID": str(uuid.uuid4()),
            "X-Trace-Source": self.service_name,
            "X-Timestamp": datetime.utcnow().isoformat()
        })
        
        # Add current context information
        if current_context:
            if current_context.user_id:
                injected_headers["X-User-ID"] = current_context.user_id
            if current_context.session_id:
                injected_headers["X-Session-ID"] = current_context.session_id
            if current_context.pipeline_id:
                injected_headers["X-Pipeline-ID"] = current_context.pipeline_id
            if current_context.workflow_id:
                injected_headers["X-Workflow-ID"] = current_context.workflow_id
        
        # Add custom context
        if custom_context:
            for key, value in custom_context.items():
                injected_headers[f"X-Custom-{key}"] = str(value)
        
        return injected_headers
    
    def extract_trace_context(self, headers: Dict[str, str]) -> Optional[TraceContext]:
        """Extract trace context from HTTP headers."""
        
        try:
            # Extract DataDog trace context
            span_context = self.context_propagator.extract(headers)
            
            # Extract custom headers
            correlation_id = headers.get("X-Correlation-ID")
            request_id = headers.get("X-Request-ID")
            user_id = headers.get("X-User-ID")
            session_id = headers.get("X-Session-ID")
            pipeline_id = headers.get("X-Pipeline-ID")
            workflow_id = headers.get("X-Workflow-ID")
            
            # Extract custom context
            custom_context = {}
            for key, value in headers.items():
                if key.startswith("X-Custom-"):
                    custom_key = key[9:]  # Remove "X-Custom-" prefix
                    custom_context[custom_key] = value
            
            # Build service context
            service_context = {
                "source_service": headers.get("X-Trace-Source"),
                "timestamp": headers.get("X-Timestamp")
            }
            
            if span_context:
                return TraceContext(
                    trace_id=str(span_context.trace_id),
                    span_id=str(span_context.span_id),
                    parent_id=str(span_context.parent_id) if span_context.parent_id else None,
                    correlation_id=correlation_id or str(span_context.trace_id),
                    user_id=user_id,
                    session_id=session_id,
                    request_id=request_id,
                    pipeline_id=pipeline_id,
                    workflow_id=workflow_id,
                    service_context=service_context,
                    custom_context=custom_context if custom_context else None
                )
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Failed to extract trace context: {str(e)}")
            return None
    
    @asynccontextmanager
    async def trace_distributed_operation(self, operation_name: str, trace_type: TraceType,
                                        correlation_level: CorrelationLevel,
                                        correlation_id: Optional[str] = None,
                                        parent_context: Optional[TraceContext] = None,
                                        business_context: Optional[Dict[str, Any]] = None,
                                        service_tags: Optional[Dict[str, str]] = None):
        """Context manager for distributed tracing operations."""
        
        # Generate correlation ID if not provided
        if not correlation_id:
            correlation_id = self.generate_correlation_id(
                correlation_level, 
                business_context
            )
        
        # Create span with parent context if available
        span_context = None
        if parent_context:
            span_context = Context(
                trace_id=int(parent_context.trace_id),
                span_id=int(parent_context.span_id) if parent_context.span_id else None
            )
        
        # Start distributed trace
        with tracer.trace(
            name=f"distributed.{trace_type.value}.{operation_name}",
            service=self.service_name,
            resource=operation_name,
            span_type=SpanTypes.CUSTOM,
            child_of=span_context
        ) as span:
            
            # Set correlation and context tags
            span.set_tag("correlation_id", correlation_id)
            span.set_tag("trace_type", trace_type.value)
            span.set_tag("correlation_level", correlation_level.value)
            span.set_tag("operation_name", operation_name)
            span.set_tag("service.name", self.service_name)
            
            # Set parent context information
            if parent_context:
                if parent_context.user_id:
                    span.set_tag("user.id", parent_context.user_id)
                if parent_context.session_id:
                    span.set_tag("session.id", parent_context.session_id)
                if parent_context.request_id:
                    span.set_tag("request.id", parent_context.request_id)
                if parent_context.pipeline_id:
                    span.set_tag("pipeline.id", parent_context.pipeline_id)
                if parent_context.workflow_id:
                    span.set_tag("workflow.id", parent_context.workflow_id)
            
            # Set business context tags
            if business_context:
                for key, value in business_context.items():
                    span.set_tag(f"business.{key}", str(value))
            
            # Set service tags
            if service_tags:
                for key, value in service_tags.items():
                    span.set_tag(f"service.{key}", value)
            
            # Create distributed trace record
            distributed_trace = DistributedTrace(
                trace_id=str(span.trace_id),
                trace_type=trace_type,
                correlation_level=correlation_level,
                start_time=datetime.utcnow(),
                spans=[str(span.span_id)],
                services=[self.service_name],
                custom_tags={
                    "correlation_id": correlation_id,
                    "operation_name": operation_name
                },
                correlation_context=TraceContext(
                    trace_id=str(span.trace_id),
                    span_id=str(span.span_id),
                    parent_id=str(span.parent_id) if span.parent_id else None,
                    correlation_id=correlation_id,
                    user_id=parent_context.user_id if parent_context else None,
                    session_id=parent_context.session_id if parent_context else None,
                    pipeline_id=parent_context.pipeline_id if parent_context else None,
                    workflow_id=parent_context.workflow_id if parent_context else None,
                    business_context=business_context,
                    service_context={"service_name": self.service_name}
                )
            )
            
            # Store active trace
            self.active_traces[str(span.trace_id)] = distributed_trace
            
            # Update correlation mapping
            if correlation_id not in self.trace_correlations:
                self.trace_correlations[correlation_id] = []
            self.trace_correlations[correlation_id].append(str(span.trace_id))
            
            start_time = time.time()
            
            try:
                yield span, distributed_trace
                
                # Mark trace as completed
                distributed_trace.status = "completed"
                distributed_trace.end_time = datetime.utcnow()
                distributed_trace.duration_seconds = time.time() - start_time
                
                span.set_tag("distributed.success", True)
                
            except Exception as e:
                # Mark trace as failed
                distributed_trace.status = "failed"
                distributed_trace.error_count += 1
                distributed_trace.end_time = datetime.utcnow()
                distributed_trace.duration_seconds = time.time() - start_time
                
                span.set_error(e)
                span.set_tag("distributed.success", False)
                span.set_tag("error.type", type(e).__name__)
                span.set_tag("error.message", str(e))
                
                self.logger.error(f"Distributed operation failed: {operation_name} - {str(e)}")
                raise
                
            finally:
                # Update statistics
                self.trace_statistics["total_traces"] += 1
                if distributed_trace.status == "completed":
                    self.trace_statistics["completed_traces"] += 1
                elif distributed_trace.status == "failed":
                    self.trace_statistics["failed_traces"] += 1
                
                # Calculate average duration
                total_completed = self.trace_statistics["completed_traces"]
                if total_completed > 0:
                    current_avg = self.trace_statistics["avg_duration"]
                    new_duration = distributed_trace.duration_seconds or 0
                    self.trace_statistics["avg_duration"] = (
                        (current_avg * (total_completed - 1) + new_duration) / total_completed
                    )
                
                # Send metrics to DataDog
                if self.datadog_monitoring:
                    await self._send_distributed_trace_metrics(distributed_trace)
                
                # Clean up completed trace (keep for correlation)
                # self.active_traces.pop(str(span.trace_id), None)
    
    async def _send_distributed_trace_metrics(self, trace: DistributedTrace):
        """Send distributed trace metrics to DataDog."""
        
        try:
            tags = [
                f"trace_type:{trace.trace_type.value}",
                f"correlation_level:{trace.correlation_level.value}",
                f"status:{trace.status}",
                f"service:{self.service_name}"
            ]
            
            # Add correlation context tags
            if trace.correlation_context:
                if trace.correlation_context.user_id:
                    tags.append(f"user_id:{trace.correlation_context.user_id}")
                if trace.correlation_context.pipeline_id:
                    tags.append(f"pipeline_id:{trace.correlation_context.pipeline_id}")
                if trace.correlation_context.workflow_id:
                    tags.append(f"workflow_id:{trace.correlation_context.workflow_id}")
            
            # Send trace metrics
            self.datadog_monitoring.counter("distributed.traces.total", tags=tags)
            
            if trace.duration_seconds:
                self.datadog_monitoring.histogram(
                    "distributed.traces.duration",
                    trace.duration_seconds * 1000,  # Convert to milliseconds
                    tags=tags
                )
            
            # Send service count
            self.datadog_monitoring.gauge(
                "distributed.traces.services_involved",
                len(trace.services),
                tags=tags
            )
            
            # Send span count
            self.datadog_monitoring.gauge(
                "distributed.traces.spans_count",
                len(trace.spans),
                tags=tags
            )
            
            # Send error metrics
            if trace.error_count > 0:
                self.datadog_monitoring.counter(
                    "distributed.traces.errors",
                    trace.error_count,
                    tags=tags
                )
            
        except Exception as e:
            self.logger.warning(f"Failed to send distributed trace metrics: {str(e)}")
    
    def add_span_to_trace(self, trace_id: str, span_id: str, service_name: str):
        """Add a span to an existing distributed trace."""
        
        if trace_id in self.active_traces:
            trace = self.active_traces[trace_id]
            trace.spans.append(span_id)
            
            if service_name not in trace.services:
                trace.services.append(service_name)
                
                # Update service dependencies
                if service_name not in self.service_dependencies:
                    self.service_dependencies[service_name] = []
                
                for existing_service in trace.services:
                    if existing_service != service_name:
                        if existing_service not in self.service_dependencies[service_name]:
                            self.service_dependencies[service_name].append(existing_service)
    
    def get_correlated_traces(self, correlation_id: str) -> List[DistributedTrace]:
        """Get all traces correlated by correlation ID."""
        
        trace_ids = self.trace_correlations.get(correlation_id, [])
        return [self.active_traces[trace_id] for trace_id in trace_ids 
                if trace_id in self.active_traces]
    
    def get_user_journey_traces(self, user_id: str, 
                              time_window_hours: int = 24) -> List[DistributedTrace]:
        """Get all traces for a user's journey."""
        
        cutoff_time = datetime.utcnow() - timedelta(hours=time_window_hours)
        user_traces = []
        
        for trace in self.active_traces.values():
            if (trace.correlation_context and 
                trace.correlation_context.user_id == user_id and
                trace.start_time >= cutoff_time):
                user_traces.append(trace)
        
        # Sort by start time
        user_traces.sort(key=lambda t: t.start_time)
        return user_traces
    
    def analyze_service_dependencies(self) -> Dict[str, Any]:
        """Analyze service dependencies from distributed traces."""
        
        dependency_analysis = {
            "services": list(self.service_dependencies.keys()),
            "total_services": len(self.service_dependencies),
            "dependencies": self.service_dependencies.copy(),
            "dependency_graph": {},
            "critical_services": [],
            "isolated_services": []
        }
        
        # Build dependency graph
        for service, deps in self.service_dependencies.items():
            dependency_analysis["dependency_graph"][service] = {
                "depends_on": deps,
                "dependency_count": len(deps),
                "is_critical": len(deps) > 3  # Services with many dependencies
            }
            
            if len(deps) > 3:
                dependency_analysis["critical_services"].append(service)
            elif len(deps) == 0:
                dependency_analysis["isolated_services"].append(service)
        
        return dependency_analysis
    
    def get_trace_performance_analysis(self) -> Dict[str, Any]:
        """Get comprehensive trace performance analysis."""
        
        # Calculate success rates
        total_traces = self.trace_statistics["total_traces"]
        completed_traces = self.trace_statistics["completed_traces"]
        failed_traces = self.trace_statistics["failed_traces"]
        
        success_rate = completed_traces / total_traces if total_traces > 0 else 0
        
        # Analyze active traces
        active_count = len(self.active_traces)
        trace_types = {}
        correlation_levels = {}
        
        for trace in self.active_traces.values():
            # Count trace types
            trace_type = trace.trace_type.value
            trace_types[trace_type] = trace_types.get(trace_type, 0) + 1
            
            # Count correlation levels
            corr_level = trace.correlation_level.value
            correlation_levels[corr_level] = correlation_levels.get(corr_level, 0) + 1
        
        # Calculate correlation success rate
        total_correlations = len(self.trace_correlations)
        successful_correlations = sum(1 for traces in self.trace_correlations.values() 
                                    if len(traces) > 1)
        correlation_success_rate = successful_correlations / total_correlations if total_correlations > 0 else 0
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "trace_statistics": {
                "total_traces": total_traces,
                "completed_traces": completed_traces,
                "failed_traces": failed_traces,
                "success_rate": success_rate,
                "avg_duration_seconds": self.trace_statistics["avg_duration"],
                "active_traces": active_count
            },
            "trace_distribution": {
                "by_type": trace_types,
                "by_correlation_level": correlation_levels
            },
            "correlation_analysis": {
                "total_correlation_ids": total_correlations,
                "successful_correlations": successful_correlations,
                "correlation_success_rate": correlation_success_rate,
                "avg_traces_per_correlation": sum(len(traces) for traces in self.trace_correlations.values()) / total_correlations if total_correlations > 0 else 0
            },
            "service_analysis": self.analyze_service_dependencies()
        }


# Global distributed tracer instance
_distributed_tracer: Optional[DataDogDistributedTracer] = None


def get_distributed_tracer(service_name: str = "distributed-tracing", 
                          datadog_monitoring: Optional[DatadogMonitoring] = None) -> DataDogDistributedTracer:
    """Get or create distributed tracer."""
    global _distributed_tracer
    
    if _distributed_tracer is None:
        _distributed_tracer = DataDogDistributedTracer(service_name, datadog_monitoring)
    
    return _distributed_tracer


# Convenience functions for common distributed tracing patterns

async def trace_web_request(operation_name: str, correlation_id: Optional[str] = None,
                          user_id: Optional[str] = None, session_id: Optional[str] = None):
    """Convenience function for tracing web requests."""
    tracer = get_distributed_tracer()
    
    business_context = {}
    if user_id:
        business_context["user_id"] = user_id
    if session_id:
        business_context["session_id"] = session_id
    
    return tracer.trace_distributed_operation(
        operation_name=operation_name,
        trace_type=TraceType.WEB_REQUEST,
        correlation_level=CorrelationLevel.REQUEST,
        correlation_id=correlation_id,
        business_context=business_context
    )


async def trace_business_workflow(workflow_name: str, workflow_id: str, 
                                user_id: Optional[str] = None):
    """Convenience function for tracing business workflows."""
    tracer = get_distributed_tracer()
    
    business_context = {
        "workflow_name": workflow_name,
        "workflow_id": workflow_id
    }
    if user_id:
        business_context["user_id"] = user_id
    
    return tracer.trace_distributed_operation(
        operation_name=workflow_name,
        trace_type=TraceType.BUSINESS_PROCESS,
        correlation_level=CorrelationLevel.WORKFLOW,
        correlation_id=workflow_id,
        business_context=business_context
    )


async def trace_data_pipeline(pipeline_name: str, pipeline_id: str, layer: str):
    """Convenience function for tracing data pipelines."""
    tracer = get_distributed_tracer()
    
    business_context = {
        "pipeline_name": pipeline_name,
        "pipeline_id": pipeline_id,
        "layer": layer
    }
    
    return tracer.trace_distributed_operation(
        operation_name=f"{layer}_{pipeline_name}",
        trace_type=TraceType.ETL_PIPELINE,
        correlation_level=CorrelationLevel.PIPELINE,
        correlation_id=pipeline_id,
        business_context=business_context
    )


async def trace_ml_operation(operation_name: str, model_id: str, experiment_id: Optional[str] = None):
    """Convenience function for tracing ML operations."""
    tracer = get_distributed_tracer()
    
    business_context = {
        "model_id": model_id,
        "operation_name": operation_name
    }
    if experiment_id:
        business_context["experiment_id"] = experiment_id
    
    correlation_id = experiment_id or model_id
    
    return tracer.trace_distributed_operation(
        operation_name=operation_name,
        trace_type=TraceType.ML_PIPELINE,
        correlation_level=CorrelationLevel.PIPELINE,
        correlation_id=correlation_id,
        business_context=business_context
    )


def propagate_trace_context(headers: Dict[str, str], 
                          custom_context: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
    """Propagate trace context to downstream services."""
    tracer = get_distributed_tracer()
    return tracer.inject_trace_context(headers, custom_context=custom_context)


def extract_trace_context_from_headers(headers: Dict[str, str]) -> Optional[TraceContext]:
    """Extract trace context from incoming headers."""
    tracer = get_distributed_tracer()
    return tracer.extract_trace_context(headers)


def get_current_correlation_id() -> Optional[str]:
    """Get correlation ID from current trace context."""
    tracer = get_distributed_tracer()
    context = tracer.get_current_trace_context()
    return context.correlation_id if context else None


def get_user_journey_analysis(user_id: str, hours: int = 24) -> Dict[str, Any]:
    """Get comprehensive user journey analysis."""
    tracer = get_distributed_tracer()
    user_traces = tracer.get_user_journey_traces(user_id, hours)
    
    if not user_traces:
        return {"user_id": user_id, "traces": [], "analysis": "no_data"}
    
    # Analyze user journey
    total_duration = sum(t.duration_seconds or 0 for t in user_traces)
    error_count = sum(t.error_count for t in user_traces)
    unique_services = set()
    trace_types = set()
    
    for trace in user_traces:
        unique_services.update(trace.services)
        trace_types.add(trace.trace_type.value)
    
    journey_analysis = {
        "user_id": user_id,
        "time_window_hours": hours,
        "total_traces": len(user_traces),
        "total_duration_seconds": total_duration,
        "total_errors": error_count,
        "unique_services": list(unique_services),
        "trace_types": list(trace_types),
        "first_activity": user_traces[0].start_time.isoformat(),
        "last_activity": user_traces[-1].start_time.isoformat(),
        "traces": [
            {
                "trace_id": trace.trace_id,
                "operation": trace.custom_tags.get("operation_name"),
                "trace_type": trace.trace_type.value,
                "start_time": trace.start_time.isoformat(),
                "duration_seconds": trace.duration_seconds,
                "status": trace.status,
                "services": trace.services
            }
            for trace in user_traces
        ]
    }
    
    return journey_analysis


def get_service_dependency_map() -> Dict[str, Any]:
    """Get service dependency mapping."""
    tracer = get_distributed_tracer()
    return tracer.analyze_service_dependencies()


def get_distributed_tracing_health() -> Dict[str, Any]:
    """Get distributed tracing system health."""
    tracer = get_distributed_tracer()
    return tracer.get_trace_performance_analysis()
"""
Automatic Instrumentation for OpenTelemetry
Provides automatic instrumentation for common libraries and frameworks.
"""
from __future__ import annotations

import asyncio
import functools
import inspect
import json
import os
import time
import uuid
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, Union

try:
    from opentelemetry import trace
    from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor
    from opentelemetry.instrumentation.auto_instrumentation import AutoInstrumentor
    from opentelemetry.instrumentation.boto3sqs import Boto3SQSInstrumentor
    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
    from opentelemetry.instrumentation.celery import CeleryInstrumentor
    from opentelemetry.instrumentation.elasticsearch import ElasticsearchInstrumentor
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.instrumentation.kafka import KafkaInstrumentor
    from opentelemetry.instrumentation.logging import LoggingInstrumentor
    from opentelemetry.instrumentation.pandas import PandasInstrumentor
    from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
    from opentelemetry.instrumentation.redis import RedisInstrumentor
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    from opentelemetry.instrumentation.sqlite3 import SQLite3Instrumentor
    from opentelemetry.instrumentation.urllib import URLLibInstrumentor
    from opentelemetry.propagate import extract, inject
    from opentelemetry.trace import Status, StatusCode, get_current_span
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    AutoInstrumentor = None
    get_current_span = lambda: None
    Status = None
    StatusCode = None
    extract = lambda *args: {}
    inject = lambda *args: None

from core.logging import get_logger

logger = get_logger(__name__)


class InstrumentationManager:
    """Manages automatic instrumentation for various libraries."""

    def __init__(self):
        self.instrumented_libraries = set()
        self.instrumentation_config = self._load_config()

    def _load_config(self) -> dict[str, Any]:
        """Load instrumentation configuration from environment."""
        return {
            'fastapi': {
                'enabled': os.getenv('OTEL_INSTRUMENT_FASTAPI', 'true').lower() in ('true', '1', 'yes'),
                'server_request_hook': None,
                'client_request_hook': None,
                'excluded_urls': os.getenv('OTEL_FASTAPI_EXCLUDED_URLS', '').split(',') if os.getenv('OTEL_FASTAPI_EXCLUDED_URLS') else []
            },
            'sqlalchemy': {
                'enabled': os.getenv('OTEL_INSTRUMENT_SQLALCHEMY', 'true').lower() in ('true', '1', 'yes'),
                'enable_commenter': True,
                'commenter_options': {},
            },
            'requests': {
                'enabled': os.getenv('OTEL_INSTRUMENT_REQUESTS', 'true').lower() in ('true', '1', 'yes'),
                'excluded_urls': os.getenv('OTEL_REQUESTS_EXCLUDED_URLS', '').split(',') if os.getenv('OTEL_REQUESTS_EXCLUDED_URLS') else []
            },
            'redis': {
                'enabled': os.getenv('OTEL_INSTRUMENT_REDIS', 'true').lower() in ('true', '1', 'yes'),
            },
            'psycopg2': {
                'enabled': os.getenv('OTEL_INSTRUMENT_PSYCOPG2', 'true').lower() in ('true', '1', 'yes'),
            },
            'sqlite3': {
                'enabled': os.getenv('OTEL_INSTRUMENT_SQLITE3', 'true').lower() in ('true', '1', 'yes'),
            },
            'httpx': {
                'enabled': os.getenv('OTEL_INSTRUMENT_HTTPX', 'true').lower() in ('true', '1', 'yes'),
            },
            'urllib': {
                'enabled': os.getenv('OTEL_INSTRUMENT_URLLIB', 'true').lower() in ('true', '1', 'yes'),
            },
            'logging': {
                'enabled': os.getenv('OTEL_INSTRUMENT_LOGGING', 'true').lower() in ('true', '1', 'yes'),
                'set_logging_format': True,
                'log_level': os.getenv('OTEL_LOGGING_LEVEL', 'INFO').upper()
            }
        }

    def instrument_fastapi(self, app=None) -> bool:
        """Instrument FastAPI application."""
        if not OTEL_AVAILABLE or not self.instrumentation_config['fastapi']['enabled']:
            return False

        if 'fastapi' in self.instrumented_libraries:
            logger.debug("FastAPI already instrumented")
            return True

        try:
            config = self.instrumentation_config['fastapi']

            if app:
                # Instrument specific app instance
                FastAPIInstrumentor.instrument_app(
                    app,
                    server_request_hook=config.get('server_request_hook'),
                    client_request_hook=config.get('client_request_hook'),
                    excluded_urls=config.get('excluded_urls')
                )
            else:
                # Instrument all FastAPI apps
                FastAPIInstrumentor().instrument(
                    server_request_hook=config.get('server_request_hook'),
                    client_request_hook=config.get('client_request_hook'),
                    excluded_urls=config.get('excluded_urls')
                )

            self.instrumented_libraries.add('fastapi')
            logger.info("FastAPI instrumentation enabled")
            return True

        except Exception as e:
            logger.error(f"Failed to instrument FastAPI: {e}")
            return False

    def instrument_sqlalchemy(self, engine=None) -> bool:
        """Instrument SQLAlchemy database operations."""
        if not OTEL_AVAILABLE or not self.instrumentation_config['sqlalchemy']['enabled']:
            return False

        if 'sqlalchemy' in self.instrumented_libraries:
            logger.debug("SQLAlchemy already instrumented")
            return True

        try:
            config = self.instrumentation_config['sqlalchemy']

            if engine:
                # Instrument specific engine
                SQLAlchemyInstrumentor.instrument(
                    engine=engine,
                    enable_commenter=config.get('enable_commenter', True),
                    commenter_options=config.get('commenter_options', {})
                )
            else:
                # Instrument all SQLAlchemy engines
                SQLAlchemyInstrumentor().instrument(
                    enable_commenter=config.get('enable_commenter', True),
                    commenter_options=config.get('commenter_options', {})
                )

            self.instrumented_libraries.add('sqlalchemy')
            logger.info("SQLAlchemy instrumentation enabled")
            return True

        except Exception as e:
            logger.error(f"Failed to instrument SQLAlchemy: {e}")
            return False

    def instrument_requests(self) -> bool:
        """Instrument requests library."""
        if not OTEL_AVAILABLE or not self.instrumentation_config['requests']['enabled']:
            return False

        if 'requests' in self.instrumented_libraries:
            logger.debug("Requests already instrumented")
            return True

        try:
            config = self.instrumentation_config['requests']

            RequestsInstrumentor().instrument(
                excluded_urls=config.get('excluded_urls')
            )

            self.instrumented_libraries.add('requests')
            logger.info("Requests library instrumentation enabled")
            return True

        except Exception as e:
            logger.error(f"Failed to instrument requests: {e}")
            return False

    def instrument_redis(self) -> bool:
        """Instrument Redis operations."""
        if not OTEL_AVAILABLE or not self.instrumentation_config['redis']['enabled']:
            return False

        if 'redis' in self.instrumented_libraries:
            logger.debug("Redis already instrumented")
            return True

        try:
            RedisInstrumentor().instrument()

            self.instrumented_libraries.add('redis')
            logger.info("Redis instrumentation enabled")
            return True

        except Exception as e:
            logger.error(f"Failed to instrument Redis: {e}")
            return False

    def instrument_database_drivers(self) -> bool:
        """Instrument database drivers (psycopg2, sqlite3)."""
        success = True

        # PostgreSQL (psycopg2)
        if OTEL_AVAILABLE and self.instrumentation_config['psycopg2']['enabled']:
            if 'psycopg2' not in self.instrumented_libraries:
                try:
                    Psycopg2Instrumentor().instrument()
                    self.instrumented_libraries.add('psycopg2')
                    logger.info("psycopg2 instrumentation enabled")
                except Exception as e:
                    logger.error(f"Failed to instrument psycopg2: {e}")
                    success = False

        # SQLite3
        if OTEL_AVAILABLE and self.instrumentation_config['sqlite3']['enabled']:
            if 'sqlite3' not in self.instrumented_libraries:
                try:
                    SQLite3Instrumentor().instrument()
                    self.instrumented_libraries.add('sqlite3')
                    logger.info("sqlite3 instrumentation enabled")
                except Exception as e:
                    logger.error(f"Failed to instrument sqlite3: {e}")
                    success = False

        return success

    def instrument_http_clients(self) -> bool:
        """Instrument HTTP client libraries."""
        success = True

        # HTTPX
        if OTEL_AVAILABLE and self.instrumentation_config['httpx']['enabled']:
            if 'httpx' not in self.instrumented_libraries:
                try:
                    HTTPXClientInstrumentor().instrument()
                    self.instrumented_libraries.add('httpx')
                    logger.info("HTTPX instrumentation enabled")
                except Exception as e:
                    logger.error(f"Failed to instrument HTTPX: {e}")
                    success = False

        # urllib
        if OTEL_AVAILABLE and self.instrumentation_config['urllib']['enabled']:
            if 'urllib' not in self.instrumented_libraries:
                try:
                    URLLibInstrumentor().instrument()
                    self.instrumented_libraries.add('urllib')
                    logger.info("urllib instrumentation enabled")
                except Exception as e:
                    logger.error(f"Failed to instrument urllib: {e}")
                    success = False

        return success

    def instrument_logging(self) -> bool:
        """Instrument Python logging to correlate with traces."""
        if not OTEL_AVAILABLE or not self.instrumentation_config['logging']['enabled']:
            return False

        if 'logging' in self.instrumented_libraries:
            logger.debug("Logging already instrumented")
            return True

        try:
            config = self.instrumentation_config['logging']

            LoggingInstrumentor().instrument(
                set_logging_format=config.get('set_logging_format', True),
                logging_level=config.get('log_level', 'INFO')
            )

            self.instrumented_libraries.add('logging')
            logger.info("Logging instrumentation enabled")
            return True

        except Exception as e:
            logger.error(f"Failed to instrument logging: {e}")
            return False

    def auto_instrument_all(self, app=None, engine=None) -> dict[str, bool]:
        """
        Automatically instrument all enabled libraries.
        
        Args:
            app: FastAPI app instance (optional)
            engine: SQLAlchemy engine instance (optional)
            
        Returns:
            Dictionary with instrumentation results
        """
        results = {}

        if not OTEL_AVAILABLE:
            logger.warning("OpenTelemetry not available, skipping instrumentation")
            return results

        # Instrument libraries
        results['fastapi'] = self.instrument_fastapi(app)
        results['sqlalchemy'] = self.instrument_sqlalchemy(engine)
        results['requests'] = self.instrument_requests()
        results['redis'] = self.instrument_redis()
        results['database_drivers'] = self.instrument_database_drivers()
        results['http_clients'] = self.instrument_http_clients()
        results['logging'] = self.instrument_logging()

        successful = sum(1 for success in results.values() if success)
        total = len(results)

        logger.info(f"Auto-instrumentation completed: {successful}/{total} libraries instrumented")

        return results

    def uninstrument_all(self) -> dict[str, bool]:
        """Uninstrument all previously instrumented libraries."""
        results = {}
        
        if not OTEL_AVAILABLE:
            return results
            
        # List of instrumentors to uninstrument
        instrumentors = [
            ('fastapi', FastAPIInstrumentor),
            ('sqlalchemy', SQLAlchemyInstrumentor),
            ('requests', RequestsInstrumentor),
            ('redis', RedisInstrumentor),
            ('psycopg2', Psycopg2Instrumentor),
            ('sqlite3', SQLite3Instrumentor),
            ('httpx', HTTPXClientInstrumentor),
            ('urllib', URLLibInstrumentor),
            ('logging', LoggingInstrumentor),
        ]
        
        for name, instrumentor_class in instrumentors:
            if name in self.instrumented_libraries:
                try:
                    instrumentor_class().uninstrument()
                    self.instrumented_libraries.remove(name)
                    results[name] = True
                    logger.info(f"{name} uninstrumented successfully")
                except Exception as e:
                    logger.error(f"Failed to uninstrument {name}: {e}")
                    results[name] = False
            else:
                results[name] = True  # Not instrumented, so "success"
                
        return results

    def get_instrumented_libraries(self) -> list[str]:
        """Get list of successfully instrumented libraries."""
        return list(self.instrumented_libraries)

    def is_library_instrumented(self, library: str) -> bool:
        """Check if a specific library is instrumented."""
        return library in self.instrumented_libraries


# Global instrumentation manager
_instrumentation_manager: InstrumentationManager | None = None


def get_instrumentation_manager() -> InstrumentationManager:
    """Get or create global instrumentation manager."""
    global _instrumentation_manager

    if _instrumentation_manager is None:
        _instrumentation_manager = InstrumentationManager()

    return _instrumentation_manager


# Convenience functions
def instrument_fastapi(app=None) -> bool:
    """Instrument FastAPI application."""
    manager = get_instrumentation_manager()
    return manager.instrument_fastapi(app)


def instrument_sqlalchemy(engine=None) -> bool:
    """Instrument SQLAlchemy operations."""
    manager = get_instrumentation_manager()
    return manager.instrument_sqlalchemy(engine)


def instrument_requests() -> bool:
    """Instrument requests library."""
    manager = get_instrumentation_manager()
    return manager.instrument_requests()


def instrument_redis() -> bool:
    """Instrument Redis operations."""
    manager = get_instrumentation_manager()
    return manager.instrument_redis()


def auto_instrument_all(app=None, engine=None) -> dict[str, bool]:
    """Auto-instrument all enabled libraries."""
    manager = get_instrumentation_manager()
    return manager.auto_instrument_all(app, engine)


def uninstrument_all() -> dict[str, bool]:
    """Uninstrument all libraries."""
    manager = get_instrumentation_manager()
    return manager.uninstrument_all()


def get_instrumentation_status() -> dict[str, Any]:
    """Get current instrumentation status."""
    manager = get_instrumentation_manager()

    return {
        'otel_available': OTEL_AVAILABLE,
        'instrumented_libraries': manager.get_instrumented_libraries(),
        'configuration': manager.instrumentation_config
    }


class InstrumentationScope:
    """Defines instrumentation scope for different components."""
    
    WEB = "web"
    DATABASE = "database"
    CACHE = "cache"
    MESSAGING = "messaging"
    EXTERNAL_API = "external_api"
    BUSINESS_LOGIC = "business_logic"
    DATA_PROCESSING = "data_processing"
    ML_INFERENCE = "ml_inference"
    AUTHENTICATION = "authentication"
    FILE_IO = "file_io"


class EnterpriseInstrumentation:
    """Enterprise-grade instrumentation for all system components."""
    
    def __init__(self):
        self._instrumented = set()
        self._custom_instrumentors = {}
        self.logger = get_logger(self.__class__.__name__)
        
    def instrument_all_enterprise(self, enable_auto_instrumentation: bool = True) -> bool:
        """Enable comprehensive enterprise instrumentation for all supported components."""
        if not OTEL_AVAILABLE:
            self.logger.warning("OpenTelemetry not available, instrumentation disabled")
            return False
            
        success_count = 0
        total_count = 0
        
        # Auto-instrumentation for common libraries
        if enable_auto_instrumentation:
            instrumentors = [
                ("FastAPI", FastAPIInstrumentor),
                ("Requests", RequestsInstrumentor),
                ("HTTPX", HTTPXClientInstrumentor),
                ("SQLAlchemy", SQLAlchemyInstrumentor),
                ("Psycopg2", Psycopg2Instrumentor),
                ("Redis", RedisInstrumentor),
                ("Elasticsearch", ElasticsearchInstrumentor),
                ("Boto3 SQS", Boto3SQSInstrumentor),
                ("Botocore", BotocoreInstrumentor),
                ("Kafka", KafkaInstrumentor),
                ("Celery", CeleryInstrumentor),
                ("Asyncio", AsyncioInstrumentor),
                ("Logging", LoggingInstrumentor),
                ("Pandas", PandasInstrumentor),
                ("SQLite3", SQLite3Instrumentor),
                ("urllib", URLLibInstrumentor),
            ]
            
            for name, instrumentor_class in instrumentors:
                total_count += 1
                try:
                    if name not in self._instrumented:
                        instrumentor_class().instrument()
                        self._instrumented.add(name)
                        success_count += 1
                        self.logger.info(f"Instrumented {name}")
                except Exception as e:
                    self.logger.warning(f"Failed to instrument {name}: {e}")
        
        self.logger.info(f"Enterprise instrumentation complete: {success_count}/{total_count} successful")
        return success_count > 0
    
    def uninstrument_all_enterprise(self) -> None:
        """Disable all enterprise instrumentation."""
        if not OTEL_AVAILABLE:
            return
            
        try:
            # Uninstrument common libraries
            instrumentors = [
                FastAPIInstrumentor,
                RequestsInstrumentor,
                HTTPXClientInstrumentor,
                SQLAlchemyInstrumentor,
                Psycopg2Instrumentor,
                RedisInstrumentor,
                ElasticsearchInstrumentor,
                Boto3SQSInstrumentor,
                BotocoreInstrumentor,
                KafkaInstrumentor,
                CeleryInstrumentor,
                AsyncioInstrumentor,
                LoggingInstrumentor,
                PandasInstrumentor,
                SQLite3Instrumentor,
                URLLibInstrumentor,
            ]
            
            for instrumentor_class in instrumentors:
                try:
                    instrumentor_class().uninstrument()
                except Exception as e:
                    self.logger.warning(f"Failed to uninstrument {instrumentor_class.__name__}: {e}")
            
            self._instrumented.clear()
            self.logger.info("All enterprise instrumentation disabled")
            
        except Exception as e:
            self.logger.error(f"Error during enterprise uninstrumentation: {e}")


class TraceCorrelation:
    """Advanced trace correlation for distributed systems."""
    
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        
    @staticmethod
    def generate_correlation_id() -> str:
        """Generate a unique correlation ID."""
        return str(uuid.uuid4())
    
    @staticmethod
    def get_trace_context() -> Dict[str, Any]:
        """Get current trace context information."""
        if not OTEL_AVAILABLE:
            return {}
            
        span = get_current_span()
        if span:
            span_context = span.get_span_context()
            return {
                'trace_id': format(span_context.trace_id, '032x'),
                'span_id': format(span_context.span_id, '016x'),
                'trace_flags': span_context.trace_flags,
                'is_valid': span_context.is_valid,
            }
        return {}
    
    @staticmethod
    def inject_trace_context(carrier: Dict[str, str]) -> None:
        """Inject trace context into carrier (e.g., HTTP headers)."""
        if OTEL_AVAILABLE:
            inject(carrier)
    
    @staticmethod
    def extract_trace_context(carrier: Dict[str, str]) -> Dict[str, Any]:
        """Extract trace context from carrier (e.g., HTTP headers)."""
        if OTEL_AVAILABLE:
            return extract(carrier)
        return {}
    
    @staticmethod
    def add_span_attributes(attributes: Dict[str, Any]) -> None:
        """Add attributes to current span."""
        if not OTEL_AVAILABLE:
            return
            
        span = get_current_span()
        if span:
            for key, value in attributes.items():
                if value is not None:
                    span.set_attribute(key, value)
    
    @staticmethod
    def add_span_event(name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Add event to current span."""
        if not OTEL_AVAILABLE:
            return
            
        span = get_current_span()
        if span:
            span.add_event(name, attributes or {})
    
    @staticmethod
    def set_span_error(exception: Exception) -> None:
        """Set span as error with exception details."""
        if not OTEL_AVAILABLE:
            return
            
        span = get_current_span()
        if span:
            span.set_status(Status(StatusCode.ERROR, str(exception)))
            span.set_attribute("error.type", type(exception).__name__)
            span.set_attribute("error.message", str(exception))
            span.add_event("exception", {
                "exception.type": type(exception).__name__,
                "exception.message": str(exception),
            })


def trace_method(scope: str, operation: Optional[str] = None, 
                capture_args: bool = False, capture_result: bool = False):
    """Decorator for tracing methods with comprehensive metadata."""
    def decorator(func: Callable) -> Callable:
        if not OTEL_AVAILABLE:
            return func
            
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            tracer = trace.get_tracer(__name__)
            operation_name = operation or f"{func.__module__}.{func.__qualname__}"
            
            with tracer.start_as_current_span(operation_name) as span:
                try:
                    # Add method metadata
                    span.set_attribute("instrumentation.scope", scope)
                    span.set_attribute("code.function", func.__name__)
                    span.set_attribute("code.namespace", func.__module__)
                    
                    # Capture arguments if requested
                    if capture_args:
                        try:
                            sig = inspect.signature(func)
                            bound_args = sig.bind(*args, **kwargs)
                            bound_args.apply_defaults()
                            
                            for param_name, param_value in bound_args.arguments.items():
                                if param_name != 'self' and not param_name.startswith('_'):
                                    # Convert complex types to string representation
                                    if isinstance(param_value, (dict, list, tuple)):
                                        param_value = json.dumps(param_value, default=str)[:1000]
                                    elif hasattr(param_value, '__dict__'):
                                        param_value = str(param_value)[:1000]
                                    
                                    span.set_attribute(f"args.{param_name}", str(param_value)[:1000])
                        except Exception as e:
                            span.set_attribute("args.capture_error", str(e))
                    
                    # Execute function
                    start_time = time.time()
                    result = func(*args, **kwargs)
                    execution_time = time.time() - start_time
                    
                    # Add performance metrics
                    span.set_attribute("performance.execution_time_ms", execution_time * 1000)
                    
                    # Capture result if requested
                    if capture_result and result is not None:
                        try:
                            if isinstance(result, (dict, list, tuple)):
                                result_str = json.dumps(result, default=str)[:1000]
                            else:
                                result_str = str(result)[:1000]
                            span.set_attribute("result.value", result_str)
                            span.set_attribute("result.type", type(result).__name__)
                        except Exception as e:
                            span.set_attribute("result.capture_error", str(e))
                    
                    span.set_status(Status(StatusCode.OK))
                    return result
                    
                except Exception as e:
                    TraceCorrelation.set_span_error(e)
                    raise
                    
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            tracer = trace.get_tracer(__name__)
            operation_name = operation or f"{func.__module__}.{func.__qualname__}"
            
            with tracer.start_as_current_span(operation_name) as span:
                try:
                    # Add method metadata
                    span.set_attribute("instrumentation.scope", scope)
                    span.set_attribute("code.function", func.__name__)
                    span.set_attribute("code.namespace", func.__module__)
                    span.set_attribute("code.async", True)
                    
                    # Capture arguments if requested
                    if capture_args:
                        try:
                            sig = inspect.signature(func)
                            bound_args = sig.bind(*args, **kwargs)
                            bound_args.apply_defaults()
                            
                            for param_name, param_value in bound_args.arguments.items():
                                if param_name != 'self' and not param_name.startswith('_'):
                                    # Convert complex types to string representation
                                    if isinstance(param_value, (dict, list, tuple)):
                                        param_value = json.dumps(param_value, default=str)[:1000]
                                    elif hasattr(param_value, '__dict__'):
                                        param_value = str(param_value)[:1000]
                                    
                                    span.set_attribute(f"args.{param_name}", str(param_value)[:1000])
                        except Exception as e:
                            span.set_attribute("args.capture_error", str(e))
                    
                    # Execute function
                    start_time = time.time()
                    result = await func(*args, **kwargs)
                    execution_time = time.time() - start_time
                    
                    # Add performance metrics
                    span.set_attribute("performance.execution_time_ms", execution_time * 1000)
                    
                    # Capture result if requested
                    if capture_result and result is not None:
                        try:
                            if isinstance(result, (dict, list, tuple)):
                                result_str = json.dumps(result, default=str)[:1000]
                            else:
                                result_str = str(result)[:1000]
                            span.set_attribute("result.value", result_str)
                            span.set_attribute("result.type", type(result).__name__)
                        except Exception as e:
                            span.set_attribute("result.capture_error", str(e))
                    
                    span.set_status(Status(StatusCode.OK))
                    return result
                    
                except Exception as e:
                    TraceCorrelation.set_span_error(e)
                    raise
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
            
    return decorator


@contextmanager
def trace_context(operation_name: str, scope: str, attributes: Optional[Dict[str, Any]] = None):
    """Context manager for manual span creation."""
    if not OTEL_AVAILABLE:
        yield
        return
        
    tracer = trace.get_tracer(__name__)
    
    with tracer.start_as_current_span(operation_name) as span:
        try:
            span.set_attribute("instrumentation.scope", scope)
            
            if attributes:
                for key, value in attributes.items():
                    if value is not None:
                        span.set_attribute(key, value)
                        
            yield span
            
        except Exception as e:
            TraceCorrelation.set_span_error(e)
            raise


# Global enterprise instrumentation instance
_enterprise_instrumentation: Optional[EnterpriseInstrumentation] = None
_trace_correlation: Optional[TraceCorrelation] = None


def get_enterprise_instrumentation() -> EnterpriseInstrumentation:
    """Get global enterprise instrumentation instance."""
    global _enterprise_instrumentation
    if _enterprise_instrumentation is None:
        _enterprise_instrumentation = EnterpriseInstrumentation()
    return _enterprise_instrumentation


def get_trace_correlation() -> TraceCorrelation:
    """Get global trace correlation instance."""
    global _trace_correlation
    if _trace_correlation is None:
        _trace_correlation = TraceCorrelation()
    return _trace_correlation


def initialize_enterprise_tracing() -> bool:
    """Initialize all enterprise tracing components."""
    try:
        instrumentation = get_enterprise_instrumentation()
        success = instrumentation.instrument_all_enterprise()
        
        if success:
            logger.info("Enterprise tracing initialized successfully")
        else:
            logger.warning("Enterprise tracing initialization had issues")
            
        return success
        
    except Exception as e:
        logger.error(f"Failed to initialize enterprise tracing: {e}")
        return False


def shutdown_enterprise_tracing() -> None:
    """Shutdown all enterprise tracing components."""
    global _enterprise_instrumentation, _trace_correlation
    
    try:
        if _enterprise_instrumentation:
            _enterprise_instrumentation.uninstrument_all_enterprise()
            
        _enterprise_instrumentation = None
        _trace_correlation = None
        
        logger.info("Enterprise tracing shutdown complete")
        
    except Exception as e:
        logger.error(f"Error during enterprise tracing shutdown: {e}")


# Convenience aliases for common tracing patterns
trace_api = functools.partial(trace_method, InstrumentationScope.WEB)
trace_db = functools.partial(trace_method, InstrumentationScope.DATABASE)
trace_cache = functools.partial(trace_method, InstrumentationScope.CACHE)
trace_messaging = functools.partial(trace_method, InstrumentationScope.MESSAGING)
trace_business = functools.partial(trace_method, InstrumentationScope.BUSINESS_LOGIC)
trace_data = functools.partial(trace_method, InstrumentationScope.DATA_PROCESSING)
trace_ml = functools.partial(trace_method, InstrumentationScope.ML_INFERENCE)

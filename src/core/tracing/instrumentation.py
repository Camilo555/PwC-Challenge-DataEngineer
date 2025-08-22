"""
Automatic Instrumentation for OpenTelemetry
Provides automatic instrumentation for common libraries and frameworks.
"""

import os
from typing import Optional, Dict, Any, List

try:
    from opentelemetry.instrumentation.auto_instrumentation import AutoInstrumentor
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    from opentelemetry.instrumentation.redis import RedisInstrumentor
    from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor
    from opentelemetry.instrumentation.sqlite3 import SQLite3Instrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
    from opentelemetry.instrumentation.urllib import URLLibInstrumentor
    from opentelemetry.instrumentation.logging import LoggingInstrumentor
    OTEL_AVAILABLE = True
except ImportError:
    OTEL_AVAILABLE = False
    AutoInstrumentor = None

from core.logging import get_logger

logger = get_logger(__name__)


class InstrumentationManager:
    """Manages automatic instrumentation for various libraries."""
    
    def __init__(self):
        self.instrumented_libraries = set()
        self.instrumentation_config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
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
    
    def auto_instrument_all(self, app=None, engine=None) -> Dict[str, bool]:
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
    
    def get_instrumented_libraries(self) -> List[str]:
        """Get list of successfully instrumented libraries."""
        return list(self.instrumented_libraries)
    
    def is_library_instrumented(self, library: str) -> bool:
        """Check if a specific library is instrumented."""
        return library in self.instrumented_libraries


# Global instrumentation manager
_instrumentation_manager: Optional[InstrumentationManager] = None


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


def auto_instrument_all(app=None, engine=None) -> Dict[str, bool]:
    """Auto-instrument all enabled libraries."""
    manager = get_instrumentation_manager()
    return manager.auto_instrument_all(app, engine)


def get_instrumentation_status() -> Dict[str, Any]:
    """Get current instrumentation status."""
    manager = get_instrumentation_manager()
    
    return {
        'otel_available': OTEL_AVAILABLE,
        'instrumented_libraries': manager.get_instrumented_libraries(),
        'configuration': manager.instrumentation_config
    }
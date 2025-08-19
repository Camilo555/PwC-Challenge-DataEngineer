"""Dagster resources for the retail data pipeline."""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from dagster import ConfigurableResource, resource
from sqlalchemy import create_engine

from core.config import settings


class DatabaseResource(ConfigurableResource):
    """Database resource for connecting to SQLite or PostgreSQL."""

    connection_string: str = settings.database_url

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """Get database connection context manager."""
        engine = create_engine(self.connection_string)
        conn = engine.connect()
        try:
            yield conn
        finally:
            conn.close()
            engine.dispose()


class SparkResource(ConfigurableResource):
    """Spark resource for distributed data processing."""

    app_name: str = "DagsterRetailETL"
    master: str = settings.spark_master
    driver_memory: str = settings.spark_driver_memory
    executor_memory: str = settings.spark_executor_memory

    @contextmanager
    def get_spark_session(self) -> Generator[Any, None, None]:
        """Get Spark session context manager."""
        from etl.utils.spark import get_spark

        spark = get_spark(self.app_name)
        try:
            yield spark
        finally:
            spark.stop()


class EnrichmentServiceResource(ConfigurableResource):
    """External API enrichment service resource."""

    enable_enrichment: bool = settings.enable_external_enrichment
    batch_size: int = settings.enrichment_batch_size
    currency_api_key: str = settings.currency_api_key or ""

    @contextmanager
    def get_enrichment_service(self) -> Generator[Any, None, None]:
        """Get enrichment service context manager."""
        if not self.enable_enrichment:
            yield None
            return

        from external_apis.enrichment_service import get_enrichment_service

        service = get_enrichment_service()
        try:
            yield service
        finally:
            import asyncio
            asyncio.run(service.close_all_clients())


# Resource instances
database_resource = DatabaseResource()
spark_resource = SparkResource()
enrichment_service_resource = EnrichmentServiceResource()


# Legacy resource definitions for backwards compatibility
@resource
def database_connection():
    """Legacy database connection resource."""
    return database_resource


@resource
def spark_session():
    """Legacy Spark session resource."""
    return spark_resource


@resource
def enrichment_service():
    """Legacy enrichment service resource."""
    return enrichment_service_resource

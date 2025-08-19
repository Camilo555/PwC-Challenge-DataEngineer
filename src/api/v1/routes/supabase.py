"""
Supabase management and health check endpoints.
Provides comprehensive database monitoring and integrity validation.
"""

from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException, status

from core.config import settings
from core.logging import get_logger
from data_access.supabase_client import (
    get_supabase_client,
    health_check_supabase,
)

logger = get_logger(__name__)
router = APIRouter(prefix="/supabase", tags=["supabase"])


@router.get("/health", summary="Comprehensive Supabase Health Check")
async def supabase_health() -> dict[str, Any]:
    """
    Perform comprehensive health check of Supabase connection.

    Returns detailed information about:
    - Database connectivity
    - Table statistics
    - Data integrity checks
    - Performance metrics
    """
    if not settings.is_supabase_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Supabase integration is not enabled"
        )

    try:
        health_report = await health_check_supabase()
        logger.info("Supabase health check completed")
        return health_report

    except Exception as e:
        logger.error(f"Supabase health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Supabase health check failed: {e}"
        ) from e


@router.get("/connection", summary="Test Supabase Connection")
async def test_connection() -> dict[str, Any]:
    """Test basic Supabase connection and return database info."""
    if not settings.is_supabase_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Supabase integration is not enabled"
        )

    try:
        client = get_supabase_client()
        connection_info = await client.test_connection()
        logger.info("Supabase connection test successful")
        return connection_info

    except Exception as e:
        logger.error(f"Supabase connection test failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Connection test failed: {e}"
        ) from e


@router.get("/statistics", summary="Get Table Statistics")
async def get_statistics() -> dict[str, Any]:
    """Get comprehensive statistics for all star schema tables."""
    if not settings.is_supabase_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Supabase integration is not enabled"
        )

    try:
        client = get_supabase_client()
        statistics = await client.get_table_statistics()
        logger.info("Retrieved Supabase table statistics")
        return {
            "tables": statistics,
            "total_tables": len(statistics),
            "database_type": "postgresql",
            "integration": "supabase"
        }

    except Exception as e:
        logger.error(f"Failed to get statistics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Statistics query failed: {e}"
        ) from e


@router.post("/integrity/validate", summary="Validate Data Integrity")
async def validate_integrity() -> dict[str, Any]:
    """
    Perform comprehensive data integrity validation.

    Checks:
    - Referential integrity between fact and dimension tables
    - Data quality constraints
    - Business rule compliance
    """
    if not settings.is_supabase_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Supabase integration is not enabled"
        )

    try:
        client = get_supabase_client()
        integrity_report = await client.validate_data_integrity()
        logger.info(f"Data integrity validation completed: {integrity_report['status']}")

        # Return appropriate HTTP status based on validation results
        if integrity_report["status"] == "failed":
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=integrity_report
            )

        return integrity_report

    except Exception as e:
        logger.error(f"Data integrity validation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Integrity validation failed: {e}"
        ) from e


@router.post("/schema/create", summary="Create Database Schema")
async def create_schema(schema_name: str = "retail_dwh") -> dict[str, Any]:
    """
    Create database schema if it doesn't exist.

    Args:
        schema_name: Name of the schema to create (default: retail_dwh)
    """
    if not settings.is_supabase_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Supabase integration is not enabled"
        )

    try:
        client = get_supabase_client()
        created = await client.create_schema_if_not_exists(schema_name)
        logger.info(f"Schema '{schema_name}' creation completed")

        return {
            "schema_name": schema_name,
            "created": created,
            "message": f"Schema '{schema_name}' is ready"
        }

    except Exception as e:
        logger.error(f"Schema creation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Schema creation failed: {e}"
        ) from e


@router.post("/tables/create", summary="Create All Tables")
async def create_tables() -> dict[str, Any]:
    """Create all star schema tables with indexes."""
    if not settings.is_supabase_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Supabase integration is not enabled"
        )

    try:
        client = get_supabase_client()
        await client.create_tables()
        logger.info("All star schema tables created successfully")

        # Get table statistics after creation
        statistics = await client.get_table_statistics()

        return {
            "message": "All star schema tables created successfully",
            "tables": list(statistics.keys()),
            "table_count": len(statistics),
            "includes_indexes": True
        }

    except Exception as e:
        logger.error(f"Table creation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Table creation failed: {e}"
        ) from e


@router.get("/config", summary="Get Supabase Configuration")
async def get_config() -> dict[str, Any]:
    """Get current Supabase configuration (sanitized)."""
    config = {
        "enabled": settings.is_supabase_enabled,
        "database_type": settings.database_type.value,
        "environment": settings.environment.value,
    }

    if settings.is_supabase_enabled:
        config.update({
            "url": settings.supabase_url or "",
            "schema": settings.supabase_schema,
            "rls_enabled": settings.enable_supabase_rls,
            "has_service_key": settings.supabase_service_key is not None,
        })
    else:
        config["reason"] = "Missing required configuration (url, key, or not using postgresql)"

    return config


@router.post("/backup/create", summary="Create Full Database Backup")
async def create_backup(backup_dir: str = "backups/supabase") -> dict[str, Any]:
    """Create comprehensive backup of all star schema tables."""
    if not settings.is_supabase_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Supabase integration is not enabled"
        )

    try:
        client = get_supabase_client()
        backup_summary = await client.create_full_backup(backup_dir)
        
        logger.info(f"Database backup completed: {backup_summary['status']}")
        
        return {
            "message": "Database backup completed",
            "backup_summary": backup_summary,
            "backup_directory": backup_dir
        }

    except Exception as e:
        logger.error(f"Database backup failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Backup operation failed: {e}"
        ) from e


@router.post("/backup/table/{table_name}", summary="Backup Single Table")
async def backup_table(table_name: str, backup_dir: str = "backups/supabase") -> dict[str, Any]:
    """Create backup of a specific table."""
    if not settings.is_supabase_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Supabase integration is not enabled"
        )

    try:
        from pathlib import Path
        client = get_supabase_client()
        backup_path = Path(backup_dir)
        
        backup_result = await client.backup_table_data(table_name, backup_path)
        
        if backup_result['status'] == 'failed':
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=backup_result['error']
            )
        
        logger.info(f"Table backup completed: {table_name}")
        return backup_result

    except Exception as e:
        logger.error(f"Table backup failed for {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Table backup failed: {e}"
        ) from e


@router.get("/monitoring/connection", summary="Advanced Connection Monitoring")
async def monitor_connection() -> dict[str, Any]:
    """Perform advanced connection monitoring with retry testing."""
    if not settings.is_supabase_enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Supabase integration is not enabled"
        )

    try:
        client = get_supabase_client()
        
        # Test with different retry scenarios
        connection_tests = {
            'basic_connection': await client.test_connection(max_retries=1),
            'resilience_test': await client.test_connection(max_retries=3)
        }
        
        # Get table statistics for monitoring
        table_stats = await client.get_table_statistics()
        
        monitoring_report = {
            'connection_tests': connection_tests,
            'table_statistics': table_stats,
            'monitoring_timestamp': pd.Timestamp.now().isoformat(),
            'connection_health': 'excellent' if connection_tests['basic_connection']['retry_count'] == 0 else 'good'
        }
        
        logger.info("Advanced connection monitoring completed")
        return monitoring_report

    except Exception as e:
        logger.error(f"Connection monitoring failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Connection monitoring failed: {e}"
        ) from e

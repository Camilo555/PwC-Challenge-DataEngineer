"""
Streaming Export API Router
Provides high-performance streaming export endpoints for large data exports.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from core.logging import get_logger
from api.streaming.streaming_response_manager import (
    StreamingResponseManager,
    StreamingConfig,
    ExportFormat,
    create_streaming_manager
)
from api.v1.services.sales_service import SalesService
from api.v1.routes.sales import get_sales_service
from data_access.db import get_async_engine
from sqlalchemy.ext.asyncio import async_sessionmaker

logger = get_logger(__name__)
router = APIRouter(prefix="/export", tags=["streaming-export"])


class StreamingExportRequest(BaseModel):
    """Request model for streaming exports."""
    query: Optional[str] = None
    table: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    format: str = "json"
    filename: Optional[str] = None
    chunk_size: int = Field(1000, ge=100, le=10000)
    compression: bool = True
    bandwidth_limit_mbps: Optional[float] = Field(None, ge=0.1, le=100)


class ExportConfigurationRequest(BaseModel):
    """Request model for export configuration."""
    chunk_size: int = Field(1000, ge=100, le=10000)
    buffer_size_mb: int = Field(50, ge=10, le=500)
    compression_enabled: bool = True
    compression_level: int = Field(6, ge=1, le=9)
    max_bandwidth_mbps: Optional[float] = Field(None, ge=0.1, le=1000)
    max_concurrent_streams: int = Field(10, ge=1, le=50)
    timeout_seconds: int = Field(3600, ge=60, le=14400)


# Global streaming manager instance
_streaming_manager: Optional[StreamingResponseManager] = None


async def get_streaming_manager() -> StreamingResponseManager:
    """Get or create streaming manager instance."""
    global _streaming_manager
    if _streaming_manager is None:
        config = StreamingConfig(
            chunk_size=1000,
            buffer_size_mb=50,
            compression_enabled=True,
            max_concurrent_streams=10
        )
        _streaming_manager = create_streaming_manager(config)
    return _streaming_manager


async def get_async_session() -> AsyncSession:
    """Get async database session."""
    engine = await get_async_engine()
    async_session = async_sessionmaker(engine, expire_on_commit=False)
    async with async_session() as session:
        yield session


@router.post("/stream/sales")
async def stream_sales_export(
    request: StreamingExportRequest,
    http_request: Request,
    sales_service: SalesService = Depends(get_sales_service),
    session: AsyncSession = Depends(get_async_session),
    manager: StreamingResponseManager = Depends(get_streaming_manager)
) -> StreamingResponse:
    """
    Stream sales data export with high performance and memory efficiency.

    Features:
    - Memory-efficient streaming for millions of records
    - Multiple format support (JSON, CSV, Excel, Parquet)
    - Real-time progress tracking
    - Bandwidth throttling and compression
    - Resume capability for interrupted downloads
    """
    try:
        # Validate format
        try:
            export_format = ExportFormat(request.format.lower())
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid export format: {request.format}. Supported: {[f.value for f in ExportFormat]}"
            )

        # Build sales query based on filters
        if request.query:
            # Use custom query (with safety checks)
            if any(dangerous in request.query.upper() for dangerous in ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'TRUNCATE']):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Query contains potentially dangerous operations"
                )
            query = request.query
        else:
            # Build query from filters
            query = await _build_sales_query(request.filters or {})

        # Get client information
        client_ip = http_request.client.host if http_request.client else "unknown"
        user_id = getattr(http_request.state, 'user_id', None)

        # Create streaming response
        return await manager.create_streaming_response(
            query=query,
            session=session,
            format=export_format,
            filename=request.filename,
            user_id=user_id,
            client_ip=client_ip,
            custom_headers={
                "X-Export-Type": "sales",
                "X-Chunk-Size": str(request.chunk_size),
                "X-Compression": str(request.compression).lower()
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating streaming sales export: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create streaming export: {str(e)}"
        )


@router.post("/stream/custom")
async def stream_custom_export(
    request: StreamingExportRequest,
    http_request: Request,
    session: AsyncSession = Depends(get_async_session),
    manager: StreamingResponseManager = Depends(get_streaming_manager)
) -> StreamingResponse:
    """
    Stream custom data export with user-provided SQL query.

    Features:
    - Custom SQL query support with safety validation
    - All standard export formats
    - Performance optimization for large result sets
    - Query timeout and resource management
    """
    try:
        if not request.query:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Query is required for custom export"
            )

        # Validate query safety
        query_upper = request.query.upper().strip()

        # Block dangerous operations
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'TRUNCATE', 'ALTER', 'CREATE']
        if any(keyword in query_upper for keyword in dangerous_keywords):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Query contains dangerous operations that are not allowed"
            )

        # Ensure it's a SELECT query
        if not query_upper.startswith('SELECT'):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only SELECT queries are allowed for export"
            )

        # Validate format
        try:
            export_format = ExportFormat(request.format.lower())
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid export format: {request.format}"
            )

        # Get client information
        client_ip = http_request.client.host if http_request.client else "unknown"
        user_id = getattr(http_request.state, 'user_id', None)

        # Create streaming response
        return await manager.create_streaming_response(
            query=request.query,
            session=session,
            format=export_format,
            filename=request.filename,
            user_id=user_id,
            client_ip=client_ip,
            custom_headers={
                "X-Export-Type": "custom",
                "X-Query-Hash": str(hash(request.query))
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating custom streaming export: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create custom export: {str(e)}"
        )


@router.post("/stream/table/{table_name}")
async def stream_table_export(
    table_name: str,
    format: str = Query("json", description="Export format"),
    filename: Optional[str] = Query(None, description="Download filename"),
    limit: Optional[int] = Query(None, ge=1, le=1000000, description="Record limit"),
    offset: int = Query(0, ge=0, description="Record offset"),
    where_clause: Optional[str] = Query(None, description="WHERE clause conditions"),
    order_by: Optional[str] = Query(None, description="ORDER BY clause"),
    http_request: Request = None,
    session: AsyncSession = Depends(get_async_session),
    manager: StreamingResponseManager = Depends(get_streaming_manager)
) -> StreamingResponse:
    """
    Stream complete table export with optional filtering and ordering.

    Features:
    - Full table exports with optional filtering
    - Custom WHERE and ORDER BY clauses
    - Pagination support with LIMIT/OFFSET
    - All export formats supported
    """
    try:
        # Validate table name (basic security check)
        if not table_name.replace('_', '').replace('-', '').isalnum():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid table name format"
            )

        # Validate format
        try:
            export_format = ExportFormat(format.lower())
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid export format: {format}"
            )

        # Build query
        query_parts = [f"SELECT * FROM {table_name}"]

        if where_clause:
            # Basic validation of WHERE clause
            if any(dangerous in where_clause.upper() for dangerous in ['DROP', 'DELETE', 'UPDATE', 'INSERT']):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="WHERE clause contains dangerous operations"
                )
            query_parts.append(f"WHERE {where_clause}")

        if order_by:
            # Basic validation of ORDER BY clause
            if any(dangerous in order_by.upper() for dangerous in ['DROP', 'DELETE', 'UPDATE', 'INSERT']):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="ORDER BY clause contains dangerous operations"
                )
            query_parts.append(f"ORDER BY {order_by}")

        query = " ".join(query_parts)

        # Add LIMIT/OFFSET if specified (note: this will be overridden by chunking)
        if limit:
            query += f" LIMIT {limit}"
        if offset:
            query += f" OFFSET {offset}"

        # Get client information
        client_ip = http_request.client.host if http_request.client else "unknown"
        user_id = getattr(http_request.state, 'user_id', None)

        # Create streaming response
        return await manager.create_streaming_response(
            query=query,
            session=session,
            format=export_format,
            filename=filename,
            user_id=user_id,
            client_ip=client_ip,
            custom_headers={
                "X-Export-Type": "table",
                "X-Table-Name": table_name,
                "X-Has-Filters": str(bool(where_clause)).lower()
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating table streaming export: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create table export: {str(e)}"
        )


@router.get("/sessions/active")
async def get_active_export_sessions(
    manager: StreamingResponseManager = Depends(get_streaming_manager)
) -> Dict[str, Any]:
    """
    Get information about active streaming export sessions.

    Provides real-time monitoring of:
    - Active export sessions
    - Progress tracking
    - Performance metrics
    - Session management capabilities
    """
    try:
        active_sessions = await manager.get_active_sessions()
        metrics = manager.get_metrics()

        return {
            "active_sessions": active_sessions,
            "session_count": len(active_sessions),
            "performance_metrics": metrics,
            "system_status": {
                "status": "healthy" if metrics["failed_streams"] < metrics["total_streams"] * 0.1 else "warning",
                "capacity_utilization": len(active_sessions) / metrics["config"]["max_concurrent_streams"] * 100,
                "recommendations": _get_performance_recommendations(metrics, active_sessions)
            },
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error getting active export sessions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get active sessions: {str(e)}"
        )


@router.post("/sessions/{session_id}/cancel")
async def cancel_export_session(
    session_id: str,
    manager: StreamingResponseManager = Depends(get_streaming_manager)
) -> Dict[str, Any]:
    """
    Cancel an active streaming export session.

    Provides graceful cancellation of long-running exports with proper cleanup.
    """
    try:
        success = await manager.cancel_session(session_id)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Export session {session_id} not found or already completed"
            )

        return {
            "status": "success",
            "message": f"Export session {session_id} has been cancelled",
            "session_id": session_id,
            "timestamp": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling export session: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel session: {str(e)}"
        )


@router.post("/configuration/update")
async def update_streaming_configuration(
    request: ExportConfigurationRequest,
    manager: StreamingResponseManager = Depends(get_streaming_manager)
) -> Dict[str, Any]:
    """
    Update streaming export configuration for performance tuning.

    Allows dynamic adjustment of:
    - Chunk sizes and buffer settings
    - Compression parameters
    - Bandwidth limiting
    - Concurrency limits
    """
    try:
        # Update configuration
        manager.config.chunk_size = request.chunk_size
        manager.config.buffer_size_mb = request.buffer_size_mb
        manager.config.compression_enabled = request.compression_enabled
        manager.config.compression_level = request.compression_level
        manager.config.max_bandwidth_mbps = request.max_bandwidth_mbps
        manager.config.max_concurrent_streams = request.max_concurrent_streams
        manager.config.timeout_seconds = request.timeout_seconds

        return {
            "status": "success",
            "message": "Streaming configuration updated successfully",
            "new_configuration": {
                "chunk_size": manager.config.chunk_size,
                "buffer_size_mb": manager.config.buffer_size_mb,
                "compression_enabled": manager.config.compression_enabled,
                "compression_level": manager.config.compression_level,
                "max_bandwidth_mbps": manager.config.max_bandwidth_mbps,
                "max_concurrent_streams": manager.config.max_concurrent_streams,
                "timeout_seconds": manager.config.timeout_seconds
            },
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error updating streaming configuration: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update configuration: {str(e)}"
        )


@router.get("/formats/supported")
async def get_supported_formats() -> Dict[str, Any]:
    """
    Get information about supported export formats and their capabilities.
    """
    return {
        "supported_formats": {
            "json": {
                "description": "JavaScript Object Notation - structured data format",
                "mime_type": "application/json",
                "streaming_efficient": True,
                "compression_effective": True,
                "use_cases": ["API integration", "web applications", "data interchange"]
            },
            "ndjson": {
                "description": "Newline-delimited JSON - streaming-optimized JSON",
                "mime_type": "application/x-ndjson",
                "streaming_efficient": True,
                "compression_effective": True,
                "use_cases": ["log processing", "streaming analytics", "real-time data"]
            },
            "csv": {
                "description": "Comma-separated values - tabular data format",
                "mime_type": "text/csv",
                "streaming_efficient": True,
                "compression_effective": True,
                "use_cases": ["Excel import", "data analysis", "reporting"]
            },
            "xlsx": {
                "description": "Excel spreadsheet format",
                "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "streaming_efficient": False,
                "compression_effective": False,
                "use_cases": ["business reporting", "data presentation", "Excel compatibility"]
            },
            "parquet": {
                "description": "Columnar storage format - highly compressed",
                "mime_type": "application/octet-stream",
                "streaming_efficient": False,
                "compression_effective": True,
                "use_cases": ["big data analytics", "data warehousing", "efficient storage"]
            },
            "xml": {
                "description": "Extensible Markup Language - structured document format",
                "mime_type": "application/xml",
                "streaming_efficient": True,
                "compression_effective": True,
                "use_cases": ["system integration", "legacy systems", "document exchange"]
            }
        },
        "recommendations": {
            "streaming_large_datasets": ["ndjson", "csv"],
            "api_integration": ["json", "ndjson"],
            "business_reporting": ["xlsx", "csv"],
            "data_analytics": ["parquet", "csv"],
            "system_integration": ["xml", "json"]
        },
        "performance_notes": {
            "memory_efficient": ["json", "ndjson", "csv", "xml"],
            "cpu_intensive": ["xlsx", "parquet"],
            "highly_compressible": ["json", "xml", "csv"],
            "fastest_processing": ["ndjson", "csv"]
        }
    }


async def _build_sales_query(filters: Dict[str, Any]) -> str:
    """Build SQL query for sales data with filters."""
    base_query = """
        SELECT
            s.invoice_id,
            s.stock_code,
            s.description,
            s.quantity,
            s.invoice_date,
            s.unit_price,
            s.customer_id,
            s.country,
            s.total_amount,
            s.created_at,
            s.updated_at
        FROM sales_transactions s
        WHERE 1=1
    """

    conditions = []

    if filters.get('date_from'):
        conditions.append(f"s.invoice_date >= '{filters['date_from']}'")

    if filters.get('date_to'):
        conditions.append(f"s.invoice_date <= '{filters['date_to']}'")

    if filters.get('country'):
        conditions.append(f"s.country = '{filters['country']}'")

    if filters.get('customer_id'):
        conditions.append(f"s.customer_id = '{filters['customer_id']}'")

    if filters.get('min_amount'):
        conditions.append(f"s.total_amount >= {filters['min_amount']}")

    if filters.get('max_amount'):
        conditions.append(f"s.total_amount <= {filters['max_amount']}")

    if conditions:
        base_query += " AND " + " AND ".join(conditions)

    base_query += " ORDER BY s.invoice_date DESC"

    return base_query


def _get_performance_recommendations(metrics: Dict[str, Any], active_sessions: List[Dict[str, Any]]) -> List[str]:
    """Generate performance recommendations based on current metrics."""
    recommendations = []

    # Check capacity utilization
    capacity_utilization = len(active_sessions) / metrics["config"]["max_concurrent_streams"] * 100
    if capacity_utilization > 80:
        recommendations.append("High capacity utilization - consider increasing max_concurrent_streams")

    # Check failure rate
    if metrics["total_streams"] > 0:
        failure_rate = metrics["failed_streams"] / metrics["total_streams"] * 100
        if failure_rate > 10:
            recommendations.append("High failure rate - check system resources and query complexity")

    # Check for long-running sessions
    long_running_sessions = [s for s in active_sessions
                           if s["started_at"] and
                           (datetime.now() - datetime.fromisoformat(s["started_at"])).seconds > 1800]
    if long_running_sessions:
        recommendations.append(f"{len(long_running_sessions)} sessions running over 30 minutes - consider optimization")

    # Check compression usage
    if not metrics["config"]["compression_enabled"]:
        recommendations.append("Compression disabled - enabling compression can reduce bandwidth usage")

    if not recommendations:
        recommendations.append("System performance is optimal")

    return recommendations
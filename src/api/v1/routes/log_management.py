"""
Log Management API Routes
========================

FastAPI routes for comprehensive ELK Stack log management with:
- Advanced log search and filtering capabilities
- Real-time log streaming and analysis
- Intelligent log correlation and pattern detection
- Automated alert creation based on log patterns
- Log statistics and analytics for monitoring dashboards
- Log retention and compliance management

Endpoints:
- POST /logs/search - Advanced log search with filtering
- GET /logs/statistics - Comprehensive log statistics
- POST /logs/analyze - Log analysis with insights and recommendations
- POST /logs/alerts - Create log-based alerts
- GET /logs/alerts - List active log alerts
- GET /logs/stream - Real-time log streaming
- GET /logs/export - Export logs in various formats
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from fastapi import status, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, validator

from core.auth.jwt import get_current_user
from core.auth.models import User
from monitoring.elk_stack_manager import (
    ELKStackManager,
    LogLevel,
    LogSource,
    LogCategory,
    LogEntry,
    LogSearchQuery,
    LogAnalysisResult,
    get_elk_manager
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter(prefix="/logs", tags=["log-management"])

# Request/Response Models
class LogSearchRequest(BaseModel):
    """Log search request model"""
    query: str = Field("*", description="Search query (supports Elasticsearch query syntax)")
    start_time: Optional[datetime] = Field(None, description="Start time for log search")
    end_time: Optional[datetime] = Field(None, description="End time for log search")
    levels: List[LogLevel] = Field(default_factory=list, description="Log levels to filter")
    sources: List[LogSource] = Field(default_factory=list, description="Log sources to filter")
    categories: List[LogCategory] = Field(default_factory=list, description="Log categories to filter")
    correlation_id: Optional[str] = Field(None, description="Correlation ID filter")
    trace_id: Optional[str] = Field(None, description="Trace ID filter")
    user_id: Optional[str] = Field(None, description="User ID filter")
    service_name: Optional[str] = Field(None, description="Service name filter")
    tags: List[str] = Field(default_factory=list, description="Tags to filter")
    limit: int = Field(100, ge=1, le=1000, description="Maximum number of results")
    offset: int = Field(0, ge=0, description="Result offset for pagination")
    sort_field: str = Field("timestamp", description="Field to sort by")
    sort_order: str = Field("desc", regex="^(asc|desc)$", description="Sort order")

    @validator('start_time', 'end_time')
    def validate_times(cls, v):
        if v and v > datetime.utcnow():
            raise ValueError("Time cannot be in the future")
        return v

class LogAnalysisRequest(BaseModel):
    """Log analysis request model"""
    start_time: datetime = Field(..., description="Analysis start time")
    end_time: datetime = Field(..., description="Analysis end time")
    sources: Optional[List[LogSource]] = Field(None, description="Sources to analyze")
    include_patterns: bool = Field(True, description="Include pattern analysis")
    include_anomalies: bool = Field(True, description="Include anomaly detection")

    @validator('end_time')
    def validate_end_time(cls, v, values):
        if 'start_time' in values and v <= values['start_time']:
            raise ValueError("End time must be after start time")
        return v

class LogAlertRequest(BaseModel):
    """Log alert request model"""
    name: str = Field(..., min_length=1, max_length=100, description="Alert name")
    description: str = Field("", max_length=500, description="Alert description")
    query: str = Field(..., description="Log query for alert")
    threshold: int = Field(..., gt=0, description="Alert threshold (number of matches)")
    time_window_minutes: int = Field(..., gt=0, le=1440, description="Time window in minutes")
    severity: str = Field("medium", regex="^(low|medium|high|critical)$", description="Alert severity")
    enabled: bool = Field(True, description="Whether alert is enabled")
    notification_channels: List[str] = Field(default_factory=list, description="Notification channels")

class LogStreamRequest(BaseModel):
    """Log stream request model"""
    sources: List[LogSource] = Field(default_factory=list, description="Sources to stream")
    levels: List[LogLevel] = Field(default_factory=list, description="Levels to stream")
    real_time: bool = Field(True, description="Enable real-time streaming")
    buffer_size: int = Field(100, ge=10, le=1000, description="Stream buffer size")

class LogSearchResponse(BaseModel):
    """Log search response model"""
    total: int = Field(..., description="Total number of matching logs")
    logs: List[Dict[str, Any]] = Field(..., description="Log entries")
    aggregations: Dict[str, Any] = Field(default_factory=dict, description="Search aggregations")
    took_ms: int = Field(..., description="Search execution time in milliseconds")
    query_info: Dict[str, Any] = Field(default_factory=dict, description="Query execution info")

class LogStatisticsResponse(BaseModel):
    """Log statistics response model"""
    time_range_hours: int = Field(..., description="Time range in hours")
    total_logs: int = Field(..., description="Total number of logs")
    log_levels: Dict[str, int] = Field(..., description="Log count by level")
    log_sources: Dict[str, int] = Field(..., description="Log count by source")
    log_categories: Dict[str, int] = Field(..., description="Log count by category")
    services: Dict[str, int] = Field(..., description="Log count by service")
    timeline: List[Dict[str, Any]] = Field(..., description="Log timeline data")
    response_time_percentiles: Dict[str, float] = Field(..., description="Response time percentiles")
    generated_at: datetime = Field(..., description="Statistics generation timestamp")

# Log Search Endpoints
@router.post("/search", response_model=LogSearchResponse)
async def search_logs(
    request: LogSearchRequest,
    current_user: User = Depends(get_current_user),
    elk_manager: ELKStackManager = Depends(get_elk_manager)
):
    """
    Advanced log search with comprehensive filtering and aggregation

    Supports complex queries with time range filtering, log level selection,
    source filtering, and full-text search across all log fields.
    """
    try:
        # Convert request to search query
        search_query = LogSearchQuery(
            query=request.query,
            start_time=request.start_time,
            end_time=request.end_time,
            levels=request.levels,
            sources=request.sources,
            categories=request.categories,
            correlation_id=request.correlation_id,
            trace_id=request.trace_id,
            user_id=request.user_id,
            service_name=request.service_name,
            tags=request.tags,
            limit=request.limit,
            offset=request.offset,
            sort_field=request.sort_field,
            sort_order=request.sort_order
        )

        # Execute search
        result = await elk_manager.search_logs(search_query)

        # Log search activity
        await elk_manager.log_structured(
            LogLevel.INFO,
            f"Log search executed by user {current_user.id}",
            source=LogSource.API,
            category=LogCategory.ACCESS,
            user_id=current_user.id,
            search_query=request.query,
            result_count=result["total"]
        )

        return LogSearchResponse(
            total=result["total"],
            logs=result["logs"],
            aggregations=result["aggregations"],
            took_ms=result["took_ms"],
            query_info={
                "user_id": current_user.id,
                "timestamp": datetime.utcnow().isoformat(),
                "search_parameters": request.dict()
            }
        )

    except Exception as e:
        logger.error(f"Log search failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Log search failed: {str(e)}")

@router.get("/statistics", response_model=LogStatisticsResponse)
async def get_log_statistics(
    time_range_hours: int = Query(24, ge=1, le=168, description="Time range in hours"),
    current_user: User = Depends(get_current_user),
    elk_manager: ELKStackManager = Depends(get_elk_manager)
):
    """
    Get comprehensive log statistics for monitoring dashboards

    Returns detailed statistics including log counts by level, source, service,
    timeline data, and performance metrics for the specified time range.
    """
    try:
        # Get log statistics
        stats = await elk_manager.get_log_statistics(time_range_hours)

        # Log statistics access
        await elk_manager.log_structured(
            LogLevel.INFO,
            f"Log statistics accessed by user {current_user.id}",
            source=LogSource.API,
            category=LogCategory.ACCESS,
            user_id=current_user.id,
            time_range_hours=time_range_hours
        )

        return LogStatisticsResponse(**stats)

    except Exception as e:
        logger.error(f"Failed to get log statistics: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Log statistics retrieval failed: {str(e)}")

@router.post("/analyze", response_model=Dict[str, Any])
async def analyze_logs(
    request: LogAnalysisRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    elk_manager: ELKStackManager = Depends(get_elk_manager)
):
    """
    Perform comprehensive log analysis with insights and recommendations

    Analyzes logs for the specified time range and provides detailed insights
    including error rates, performance metrics, anomalies, and actionable recommendations.
    """
    try:
        # Validate time range
        if request.end_time - request.start_time > timedelta(days=7):
            raise HTTPException(
                status_code=400,
                detail="Analysis time range cannot exceed 7 days"
            )

        # Execute log analysis
        analysis = await elk_manager.analyze_logs(
            request.start_time,
            request.end_time,
            request.sources
        )

        # Log analysis activity
        await elk_manager.log_structured(
            LogLevel.INFO,
            f"Log analysis performed by user {current_user.id}",
            source=LogSource.API,
            category=LogCategory.BUSINESS,
            user_id=current_user.id,
            analysis_start=request.start_time.isoformat(),
            analysis_end=request.end_time.isoformat(),
            total_logs_analyzed=analysis.total_logs
        )

        return {
            "analysis_id": f"analysis_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            "time_range": {
                "start": request.start_time.isoformat(),
                "end": request.end_time.isoformat(),
                "duration_hours": (request.end_time - request.start_time).total_seconds() / 3600
            },
            "results": {
                "total_logs": analysis.total_logs,
                "error_rate_percent": round(analysis.error_rate, 2),
                "warning_rate_percent": round(analysis.warning_rate, 2),
                "top_errors": analysis.top_errors,
                "top_sources": analysis.top_sources,
                "performance_metrics": analysis.performance_metrics,
                "anomalies": analysis.anomalies,
                "patterns": analysis.patterns,
                "recommendations": analysis.recommendations
            },
            "analysis_metadata": {
                "performed_by": current_user.id,
                "performed_at": datetime.utcnow().isoformat(),
                "analysis_quality": "high" if analysis.total_logs > 1000 else "medium",
                "confidence_level": "high" if analysis.total_logs > 10000 else "medium"
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Log analysis failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Log analysis failed: {str(e)}")

# Log Alert Management
@router.post("/alerts", response_model=Dict[str, str], status_code=status.HTTP_201_CREATED)
async def create_log_alert(
    request: LogAlertRequest,
    current_user: User = Depends(get_current_user),
    elk_manager: ELKStackManager = Depends(get_elk_manager)
):
    """
    Create log-based alert with intelligent pattern detection

    Creates a new alert that monitors log patterns and triggers notifications
    when specified conditions are met within the defined time window.
    """
    try:
        # Prepare alert configuration
        alert_config = {
            "name": request.name,
            "description": request.description,
            "query": request.query,
            "threshold": request.threshold,
            "time_window_minutes": request.time_window_minutes,
            "severity": request.severity,
            "enabled": request.enabled,
            "notification_channels": request.notification_channels,
            "created_by": current_user.id,
            "created_at": datetime.utcnow().isoformat()
        }

        # Create alert
        alert_id = await elk_manager.create_log_alert(alert_config)

        # Log alert creation
        await elk_manager.log_structured(
            LogLevel.INFO,
            f"Log alert created: {request.name}",
            source=LogSource.API,
            category=LogCategory.SECURITY,
            user_id=current_user.id,
            alert_id=alert_id,
            alert_name=request.name,
            alert_severity=request.severity
        )

        return {
            "message": "Log alert created successfully",
            "alert_id": alert_id,
            "alert_name": request.name
        }

    except Exception as e:
        logger.error(f"Failed to create log alert: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Log alert creation failed: {str(e)}")

@router.get("/alerts", response_model=Dict[str, Any])
async def list_log_alerts(
    active_only: bool = Query(True, description="Return only active alerts"),
    current_user: User = Depends(get_current_user),
    elk_manager: ELKStackManager = Depends(get_elk_manager)
):
    """
    List active log alerts with status and statistics

    Returns all log alerts created by the user or shared alerts,
    including alert status, trigger counts, and recent activity.
    """
    try:
        # Get alerts from Redis
        alert_keys = await elk_manager.redis_client.keys("log_alert:*")
        alerts = []

        for key in alert_keys:
            alert_data = await elk_manager.redis_client.get(key)
            if alert_data:
                alert = json.loads(alert_data)
                if not active_only or alert.get("status") == "active":
                    alerts.append(alert)

        # Log alert access
        await elk_manager.log_structured(
            LogLevel.INFO,
            f"Log alerts accessed by user {current_user.id}",
            source=LogSource.API,
            category=LogCategory.ACCESS,
            user_id=current_user.id,
            alert_count=len(alerts)
        )

        return {
            "alerts": alerts,
            "total_count": len(alerts),
            "active_count": len([a for a in alerts if a.get("status") == "active"]),
            "retrieved_at": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id
        }

    except Exception as e:
        logger.error(f"Failed to list log alerts: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Log alert listing failed: {str(e)}")

# Real-time Log Streaming
@router.websocket("/stream")
async def stream_logs(
    websocket: WebSocket,
    sources: Optional[str] = Query(None, description="Comma-separated log sources"),
    levels: Optional[str] = Query(None, description="Comma-separated log levels"),
    elk_manager: ELKStackManager = Depends(get_elk_manager)
):
    """
    WebSocket endpoint for real-time log streaming

    Provides real-time streaming of logs with filtering by source and level.
    Supports continuous streaming with configurable buffer size and refresh rate.
    """
    await websocket.accept()

    try:
        # Parse parameters
        source_list = [LogSource(s.strip()) for s in sources.split(",")] if sources else []
        level_list = [LogLevel(l.strip()) for l in levels.split(",")] if levels else []

        logger.info(f"WebSocket log stream started with sources: {source_list}, levels: {level_list}")

        # Send initial connection confirmation
        await websocket.send_text(json.dumps({
            "type": "connection_established",
            "timestamp": datetime.utcnow().isoformat(),
            "filters": {
                "sources": [s.value for s in source_list],
                "levels": [l.value for l in level_list]
            }
        }))

        # Stream logs in real-time
        last_timestamp = datetime.utcnow()

        while True:
            try:
                # Get recent logs
                current_time = datetime.utcnow()
                search_query = LogSearchQuery(
                    query="*",
                    start_time=last_timestamp,
                    end_time=current_time,
                    sources=source_list,
                    levels=level_list,
                    limit=50,
                    sort_field="timestamp",
                    sort_order="asc"
                )

                result = await elk_manager.search_logs(search_query)

                # Send new logs
                if result["logs"]:
                    await websocket.send_text(json.dumps({
                        "type": "log_batch",
                        "timestamp": current_time.isoformat(),
                        "count": len(result["logs"]),
                        "logs": result["logs"]
                    }))

                last_timestamp = current_time
                await asyncio.sleep(2)  # 2-second intervals

            except WebSocketDisconnect:
                logger.info("WebSocket log stream disconnected")
                break
            except Exception as e:
                logger.error(f"WebSocket stream error: {str(e)}")
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }))
                await asyncio.sleep(5)

    except WebSocketDisconnect:
        logger.info("WebSocket connection closed")
    except Exception as e:
        logger.error(f"WebSocket error: {str(e)}")
        try:
            await websocket.send_text(json.dumps({
                "type": "error",
                "message": "Stream connection error",
                "timestamp": datetime.utcnow().isoformat()
            }))
        except:
            pass

# Log Export Endpoints
@router.get("/export")
async def export_logs(
    format: str = Query("json", regex="^(json|csv|txt)$", description="Export format"),
    start_time: Optional[datetime] = Query(None, description="Start time for export"),
    end_time: Optional[datetime] = Query(None, description="End time for export"),
    sources: Optional[str] = Query(None, description="Comma-separated log sources"),
    levels: Optional[str] = Query(None, description="Comma-separated log levels"),
    limit: int = Query(10000, ge=1, le=100000, description="Maximum logs to export"),
    current_user: User = Depends(get_current_user),
    elk_manager: ELKStackManager = Depends(get_elk_manager)
):
    """
    Export logs in specified format (JSON, CSV, TXT)

    Exports logs matching the specified criteria in the requested format.
    Supports large exports with streaming response for better performance.
    """
    try:
        # Parse parameters
        source_list = [LogSource(s.strip()) for s in sources.split(",")] if sources else []
        level_list = [LogLevel(l.strip()) for l in levels.split(",")] if levels else []

        # Create search query
        search_query = LogSearchQuery(
            query="*",
            start_time=start_time,
            end_time=end_time,
            sources=source_list,
            levels=level_list,
            limit=limit,
            sort_field="timestamp",
            sort_order="desc"
        )

        # Execute search
        result = await elk_manager.search_logs(search_query)

        # Log export activity
        await elk_manager.log_structured(
            LogLevel.INFO,
            f"Log export requested by user {current_user.id}",
            source=LogSource.API,
            category=LogCategory.ACCESS,
            user_id=current_user.id,
            export_format=format,
            log_count=result["total"],
            export_limit=limit
        )

        # Generate export content
        def generate_export():
            if format == "json":
                yield json.dumps({
                    "export_info": {
                        "timestamp": datetime.utcnow().isoformat(),
                        "user_id": current_user.id,
                        "total_logs": result["total"],
                        "exported_logs": len(result["logs"])
                    },
                    "logs": result["logs"]
                }, indent=2)
            elif format == "csv":
                # CSV header
                yield "timestamp,level,source,category,service_name,message,user_id,correlation_id\\n"
                # CSV data
                for log in result["logs"]:
                    row = [
                        log.get("timestamp", ""),
                        log.get("level", ""),
                        log.get("source", ""),
                        log.get("category", ""),
                        log.get("service_name", ""),
                        log.get("message", "").replace('"', '""'),
                        log.get("user_id", ""),
                        log.get("correlation_id", "")
                    ]
                    yield f'"{row[0]}","{row[1]}","{row[2]}","{row[3]}","{row[4]}","{row[5]}","{row[6]}","{row[7]}"\\n'
            else:  # txt format
                for log in result["logs"]:
                    yield f"[{log.get('timestamp', '')}] {log.get('level', 'INFO')} {log.get('source', 'UNKNOWN')} - {log.get('message', '')}\\n"

        # Set content type and filename
        content_type_map = {
            "json": "application/json",
            "csv": "text/csv",
            "txt": "text/plain"
        }

        filename = f"logs_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.{format}"

        return StreamingResponse(
            generate_export(),
            media_type=content_type_map[format],
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        logger.error(f"Log export failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Log export failed: {str(e)}")

# Log Correlation Endpoints
@router.get("/correlate/{correlation_id}", response_model=Dict[str, Any])
async def get_correlated_logs(
    correlation_id: str,
    time_window_hours: int = Query(24, ge=1, le=168, description="Time window in hours"),
    current_user: User = Depends(get_current_user),
    elk_manager: ELKStackManager = Depends(get_elk_manager)
):
    """
    Get all logs correlated by correlation ID

    Retrieves all logs associated with a specific correlation ID,
    providing complete request/response flow visibility.
    """
    try:
        # Create search query for correlation
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=time_window_hours)

        search_query = LogSearchQuery(
            query="*",
            start_time=start_time,
            end_time=end_time,
            correlation_id=correlation_id,
            limit=1000,
            sort_field="timestamp",
            sort_order="asc"
        )

        # Execute search
        result = await elk_manager.search_logs(search_query)

        # Analyze correlation flow
        logs = result["logs"]
        flow_analysis = {
            "total_logs": len(logs),
            "time_span_seconds": 0,
            "services_involved": list(set(log.get("service_name", "unknown") for log in logs)),
            "log_levels": list(set(log.get("level", "INFO") for log in logs)),
            "has_errors": any(log.get("level") in ["ERROR", "CRITICAL", "FATAL"] for log in logs),
            "request_flow": []
        }

        if logs:
            # Calculate time span
            first_log_time = datetime.fromisoformat(logs[0]["timestamp"].replace("Z", "+00:00"))
            last_log_time = datetime.fromisoformat(logs[-1]["timestamp"].replace("Z", "+00:00"))
            flow_analysis["time_span_seconds"] = (last_log_time - first_log_time).total_seconds()

            # Build request flow
            for i, log in enumerate(logs):
                flow_analysis["request_flow"].append({
                    "sequence": i + 1,
                    "timestamp": log["timestamp"],
                    "service": log.get("service_name", "unknown"),
                    "level": log.get("level", "INFO"),
                    "message": log.get("message", ""),
                    "method": log.get("method"),
                    "path": log.get("path"),
                    "status_code": log.get("status_code"),
                    "response_time_ms": log.get("response_time_ms")
                })

        # Log correlation access
        await elk_manager.log_structured(
            LogLevel.INFO,
            f"Log correlation accessed for ID: {correlation_id}",
            source=LogSource.API,
            category=LogCategory.ACCESS,
            user_id=current_user.id,
            correlation_id=correlation_id,
            correlated_logs_count=len(logs)
        )

        return {
            "correlation_id": correlation_id,
            "search_info": {
                "time_window_hours": time_window_hours,
                "search_start": start_time.isoformat(),
                "search_end": end_time.isoformat()
            },
            "flow_analysis": flow_analysis,
            "logs": logs,
            "retrieved_at": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id
        }

    except Exception as e:
        logger.error(f"Log correlation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Log correlation failed: {str(e)}")

# System Health Endpoints
@router.get("/health", response_model=Dict[str, Any])
async def get_log_system_health(
    elk_manager: ELKStackManager = Depends(get_elk_manager)
):
    """
    Get ELK Stack system health status

    Returns health status of Elasticsearch, log ingestion rates,
    and system performance metrics for monitoring purposes.
    """
    try:
        # Check Elasticsearch health
        es_health = await elk_manager.elasticsearch.cluster.health()

        # Get ingestion statistics
        stats = await elk_manager.get_log_statistics(time_range_hours=1)

        return {
            "status": "healthy" if es_health["status"] in ["green", "yellow"] else "unhealthy",
            "elasticsearch": {
                "cluster_status": es_health["status"],
                "active_shards": es_health["active_shards"],
                "number_of_nodes": es_health["number_of_nodes"],
                "number_of_data_nodes": es_health["number_of_data_nodes"]
            },
            "ingestion": {
                "logs_last_hour": stats["total_logs"],
                "buffer_size": len(elk_manager.log_buffer),
                "retention_days": elk_manager.retention_days,
                "is_running": elk_manager.is_running
            },
            "performance": {
                "index_prefix": elk_manager.index_prefix,
                "batch_size": elk_manager.batch_size,
                "flush_interval_seconds": elk_manager.flush_interval
            },
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Log system health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
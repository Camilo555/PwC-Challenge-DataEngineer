"""
Database Management API Router
Provides management endpoints for database partitioning, indexing, and performance optimization.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from datetime import datetime

from core.logging import get_logger
from core.database.partitioning import (
    DatabasePartitioningManager,
    PartitionConfig,
    PartitionStrategy,
    PartitionInterval,
    create_partitioning_manager
)
from data_access.db import get_async_engine

logger = get_logger(__name__)
router = APIRouter(prefix="/database", tags=["database-management"])


class PartitionConfigRequest(BaseModel):
    """Request model for partition configuration."""
    table_name: str
    strategy: str
    partition_key: str
    interval: Optional[str] = None
    retention_periods: int = 24
    auto_create_partitions: bool = True
    auto_drop_old_partitions: bool = True
    partition_prefix: str = ""
    hash_partitions: int = 16
    compression: bool = True
    indexes: List[str] = []
    constraints: List[str] = []
    statistics_target: int = 100


class MaintenanceRequest(BaseModel):
    """Request model for maintenance operations."""
    tables: Optional[List[str]] = None
    force_analyze: bool = False
    cleanup_empty_partitions: bool = True


# Dependency to get partitioning manager
async def get_partitioning_manager() -> DatabasePartitioningManager:
    """Get database partitioning manager instance."""
    engine = await get_async_engine()
    manager = create_partitioning_manager(engine)
    await manager.initialize_partitioning(register_defaults=True)
    return manager


@router.get("/partitions/summary")
async def get_partitioning_summary(
    manager: DatabasePartitioningManager = Depends(get_partitioning_manager)
) -> Dict[str, Any]:
    """
    Get comprehensive summary of database partitioning system.

    Provides:
    - Total partition counts by table
    - Storage utilization metrics
    - Performance metrics
    - System health indicators
    """
    try:
        summary = manager.get_partition_summary()
        performance_metrics = await manager.get_partition_performance_metrics()

        return {
            "partition_system": summary,
            "performance": performance_metrics,
            "timestamp": datetime.now().isoformat(),
            "status": "healthy" if summary["active_partitions"] > 0 else "warning"
        }

    except Exception as e:
        logger.error(f"Error getting partitioning summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get partitioning summary: {str(e)}"
        )


@router.get("/partitions/{table_name}")
async def get_table_partitions(
    table_name: str,
    manager: DatabasePartitioningManager = Depends(get_partitioning_manager)
) -> Dict[str, Any]:
    """
    Get detailed partition information for a specific table.

    Returns:
    - List of all partitions with metadata
    - Performance metrics per partition
    - Optimization recommendations
    """
    try:
        partitions = manager.partition_registry.get(table_name, [])
        if not partitions:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No partitions found for table: {table_name}"
            )

        # Get optimization recommendations
        optimization = await manager.optimize_partition_performance(table_name)

        return {
            "table_name": table_name,
            "partition_count": len(partitions),
            "partitions": [
                {
                    "name": p.name,
                    "strategy": p.strategy.value,
                    "state": p.state.value,
                    "size_mb": round(p.size_bytes / (1024 * 1024), 2) if p.size_bytes else 0,
                    "row_count": p.row_count,
                    "created_at": p.created_at.isoformat() if p.created_at else None,
                    "last_analyzed": p.last_analyzed.isoformat() if p.last_analyzed else None,
                    "bounds": p.bounds
                } for p in partitions
            ],
            "optimization": optimization,
            "timestamp": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting table partitions for {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get table partitions: {str(e)}"
        )


@router.post("/partitions/{table_name}/create")
async def create_table_partitions(
    table_name: str,
    manager: DatabasePartitioningManager = Depends(get_partitioning_manager)
) -> Dict[str, Any]:
    """
    Create partitions for a specific table based on its configuration.

    Automatically creates:
    - Time-based partitions for configured intervals
    - Hash partitions for parallel processing
    - Hybrid partitions for complex scenarios
    """
    try:
        results = await manager.create_partitions_for_table(table_name)

        return {
            "table_name": table_name,
            "operation": "create_partitions",
            "results": results,
            "summary": {
                "created": len(results["created"]),
                "failed": len(results["failed"]),
                "skipped": len(results["skipped"])
            },
            "timestamp": datetime.now().isoformat()
        }

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error creating partitions for {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create partitions: {str(e)}"
        )


@router.post("/partitions/maintenance")
async def run_partition_maintenance(
    request: MaintenanceRequest = MaintenanceRequest(),
    manager: DatabasePartitioningManager = Depends(get_partitioning_manager)
) -> Dict[str, Any]:
    """
    Execute comprehensive partition maintenance operations.

    Features:
    - Automatic partition creation and cleanup
    - Statistics analysis and updates
    - Performance optimization
    - Storage space reclamation
    """
    try:
        maintenance_results = await manager.maintain_partitions()

        # Get updated system status
        summary = manager.get_partition_summary()

        return {
            "operation": "partition_maintenance",
            "results": maintenance_results,
            "system_summary": {
                "total_partitions": summary["total_partitions"],
                "active_partitions": summary["active_partitions"],
                "total_size_gb": round(summary["total_size_gb"], 2)
            },
            "recommendations": [
                "Review partition retention policies if storage is growing rapidly",
                "Monitor query performance after partition changes",
                "Consider archiving old partitions to reduce storage costs"
            ],
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error running partition maintenance: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Partition maintenance failed: {str(e)}"
        )


@router.post("/partitions/config")
async def register_partition_config(
    config_request: PartitionConfigRequest,
    manager: DatabasePartitioningManager = Depends(get_partitioning_manager)
) -> Dict[str, Any]:
    """
    Register a new partition configuration for a table.

    Supports all partitioning strategies:
    - TIME_SERIES: Date/time-based partitioning
    - HASH: Hash-based for parallel processing
    - HYBRID: Combination of time and hash partitioning
    """
    try:
        # Validate strategy and interval
        try:
            strategy = PartitionStrategy(config_request.strategy.lower())
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid partition strategy: {config_request.strategy}"
            )

        interval = None
        if config_request.interval:
            try:
                interval = PartitionInterval(config_request.interval.lower())
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid partition interval: {config_request.interval}"
                )

        # Create partition configuration
        partition_config = PartitionConfig(
            table_name=config_request.table_name,
            strategy=strategy,
            partition_key=config_request.partition_key,
            interval=interval,
            retention_periods=config_request.retention_periods,
            auto_create_partitions=config_request.auto_create_partitions,
            auto_drop_old_partitions=config_request.auto_drop_old_partitions,
            partition_prefix=config_request.partition_prefix,
            hash_partitions=config_request.hash_partitions,
            compression=config_request.compression,
            indexes=config_request.indexes,
            constraints=config_request.constraints,
            statistics_target=config_request.statistics_target
        )

        # Register the configuration
        manager.register_partition_config(partition_config)

        return {
            "operation": "register_partition_config",
            "table_name": config_request.table_name,
            "strategy": strategy.value,
            "status": "success",
            "message": f"Partition configuration registered for table: {config_request.table_name}",
            "next_steps": [
                f"Run POST /database/partitions/{config_request.table_name}/create to create partitions",
                "Monitor partition performance with GET /database/partitions/summary",
                "Schedule regular maintenance with POST /database/partitions/maintenance"
            ],
            "timestamp": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering partition config: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to register partition configuration: {str(e)}"
        )


@router.get("/partitions/performance/{table_name}")
async def get_partition_performance(
    table_name: str,
    manager: DatabasePartitioningManager = Depends(get_partitioning_manager)
) -> Dict[str, Any]:
    """
    Get detailed partition performance analysis for a table.

    Provides:
    - Query execution metrics
    - Partition elimination statistics
    - Performance optimization recommendations
    - Resource utilization analysis
    """
    try:
        optimization = await manager.optimize_partition_performance(table_name)
        performance_metrics = await manager.get_partition_performance_metrics(table_name)

        if "error" in optimization:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=optimization["error"]
            )

        return {
            "table_name": table_name,
            "performance_analysis": optimization,
            "metrics": performance_metrics,
            "health_score": _calculate_partition_health_score(optimization),
            "timestamp": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting partition performance for {table_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get partition performance: {str(e)}"
        )


@router.get("/partitions/defaults")
async def get_default_partition_configs() -> Dict[str, Any]:
    """
    Get information about default partition configurations.

    Shows recommended partitioning strategies for common table types
    in enterprise data warehouses.
    """
    try:
        # Create a temporary manager to get default configs
        engine = await get_async_engine()
        temp_manager = create_partitioning_manager(engine)
        default_configs = temp_manager.get_default_partition_configs()

        return {
            "default_configurations": [
                {
                    "table_name": config.table_name,
                    "strategy": config.strategy.value,
                    "partition_key": config.partition_key,
                    "interval": config.interval.value if config.interval else None,
                    "retention_periods": config.retention_periods,
                    "description": _get_config_description(config),
                    "use_cases": _get_config_use_cases(config)
                }
                for config in default_configs
            ],
            "partitioning_strategies": {
                "TIME_SERIES": "Date/time-based partitioning for chronological data",
                "HASH": "Hash-based partitioning for parallel processing",
                "HYBRID": "Combined time and hash partitioning for complex scenarios",
                "RANGE": "Range-based partitioning for numeric data",
                "LIST": "List-based partitioning for categorical data"
            },
            "intervals": {
                "DAILY": "New partition each day - for high-volume transactional data",
                "WEEKLY": "New partition each week - for moderate volume event data",
                "MONTHLY": "New partition each month - for analytical fact tables",
                "QUARTERLY": "New partition each quarter - for aggregated data",
                "YEARLY": "New partition each year - for historical data"
            }
        }

    except Exception as e:
        logger.error(f"Error getting default partition configs: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get default configurations: {str(e)}"
        )


def _calculate_partition_health_score(optimization: Dict[str, Any]) -> str:
    """Calculate a health score based on partition optimization analysis."""
    recommendations_count = len(optimization.get("recommendations", []))
    analysis = optimization.get("analysis", {})

    # Score based on various factors
    score = 100

    # Deduct for recommendations
    score -= recommendations_count * 10

    # Deduct for poor performance metrics
    if analysis.get("avg_partition_elimination_pct", 100) < 70:
        score -= 20

    if analysis.get("efficiency_rate", 100) < 80:
        score -= 15

    # Deduct for empty partitions
    if analysis.get("empty_partitions", 0) > 0:
        score -= analysis["empty_partitions"] * 5

    # Determine health category
    if score >= 90:
        return "excellent"
    elif score >= 75:
        return "good"
    elif score >= 60:
        return "fair"
    elif score >= 40:
        return "poor"
    else:
        return "critical"


def _get_config_description(config: PartitionConfig) -> str:
    """Get description for a partition configuration."""
    descriptions = {
        "fact_sale": "Main sales fact table with monthly time-series partitioning for optimal query performance",
        "fact_sale_hash": "Hash-partitioned sales table for parallel processing and data loading",
        "etl_processing_log": "ETL process logs with daily partitioning and automatic cleanup",
        "customer_events": "Customer behavior events with weekly partitioning",
        "api_logs": "API access logs with daily partitioning for monitoring and analytics",
        "dim_customer_partitioned": "Large customer dimension table with hash partitioning",
        "sales_transactions_hybrid": "Hybrid partitioned transactions table combining time and hash strategies"
    }
    return descriptions.get(config.table_name, f"Partitioned table using {config.strategy.value} strategy")


def _get_config_use_cases(config: PartitionConfig) -> List[str]:
    """Get use cases for a partition configuration."""
    if config.strategy == PartitionStrategy.TIME_SERIES:
        return [
            "Time-based analytics and reporting",
            "Efficient data archiving and retention",
            "Improved query performance with date filters",
            "Parallel processing of chronological data"
        ]
    elif config.strategy == PartitionStrategy.HASH:
        return [
            "Parallel data processing and loading",
            "Load balancing across multiple partitions",
            "Improved concurrent access performance",
            "Scalable data distribution"
        ]
    elif config.strategy == PartitionStrategy.HYBRID:
        return [
            "Complex analytical workloads",
            "Both time-based and parallel processing benefits",
            "High-performance OLAP operations",
            "Advanced data warehouse scenarios"
        ]
    else:
        return ["Custom partitioning strategy"]
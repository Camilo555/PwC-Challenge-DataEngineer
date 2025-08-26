"""
Enterprise API endpoints for advanced microservices patterns
Provides access to service registry, SAGA orchestrator, and CQRS framework
"""
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from api.gateway.service_registry import get_service_registry, ServiceInstance, ServiceStatus
from api.patterns.saga_orchestrator import get_saga_orchestrator, SagaTransaction, OrderSagaBuilder
from api.patterns.cqrs_framework import get_cqrs_framework
from core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/enterprise", tags=["enterprise"])


# Pydantic models for request/response
class ServiceRegistrationRequest(BaseModel):
    service_name: str
    host: str
    port: int
    protocol: str = "http"
    weight: int = 1
    health_check_url: str = "/health"
    version: str = "1.0.0"
    tags: List[str] = []
    metadata: Dict[str, Any] = {}


class SagaRequest(BaseModel):
    transaction_type: str
    order_data: Dict[str, Any]  # For order processing SAGA


class ServiceMetricsResponse(BaseModel):
    service_name: str
    total_instances: int
    healthy_instances: int
    unhealthy_instances: int


# Service Registry Endpoints
@router.post("/services/register", status_code=status.HTTP_201_CREATED)
async def register_service(request: ServiceRegistrationRequest):
    """Register a new service instance"""
    service_registry = get_service_registry()
    
    service_instance = ServiceInstance(
        service_name=request.service_name,
        instance_id=f"{request.service_name}-{request.host}-{request.port}",
        host=request.host,
        port=request.port,
        protocol=request.protocol,
        weight=request.weight,
        health_check_url=request.health_check_url,
        version=request.version,
        tags=request.tags,
        metadata=request.metadata
    )
    
    success = await service_registry.register_service(service_instance)
    
    if success:
        return {
            "message": "Service registered successfully",
            "instance_id": service_instance.instance_id,
            "service_name": request.service_name
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to register service"
        )


@router.delete("/services/{service_name}/{instance_id}")
async def deregister_service(service_name: str, instance_id: str):
    """Deregister a service instance"""
    service_registry = get_service_registry()
    
    success = await service_registry.deregister_service(service_name, instance_id)
    
    if success:
        return {"message": "Service deregistered successfully"}
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Service instance not found"
        )


@router.get("/services")
async def list_services():
    """List all registered services"""
    service_registry = get_service_registry()
    metrics = await service_registry.get_service_metrics()
    return metrics


@router.get("/services/{service_name}")
async def get_service_details(service_name: str):
    """Get detailed information about a service"""
    service_registry = get_service_registry()
    metrics = await service_registry.get_service_metrics(service_name)
    
    if not metrics:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Service not found"
        )
    
    return metrics


@router.get("/services/{service_name}/healthy")
async def get_healthy_instances(service_name: str):
    """Get healthy instances for a service"""
    service_registry = get_service_registry()
    instances = await service_registry.get_healthy_instances(service_name)
    
    return {
        "service_name": service_name,
        "healthy_instances": len(instances),
        "instances": [instance.to_dict() for instance in instances]
    }


# SAGA Orchestrator Endpoints
@router.post("/sagas/start", status_code=status.HTTP_201_CREATED)
async def start_saga(request: SagaRequest):
    """Start a new SAGA transaction"""
    saga_orchestrator = get_saga_orchestrator()
    
    if request.transaction_type == "order_processing":
        saga = OrderSagaBuilder.create_order_saga(request.order_data)
        saga_id = await saga_orchestrator.start_saga(saga)
        
        return {
            "saga_id": saga_id,
            "transaction_type": request.transaction_type,
            "status": "started",
            "steps": len(saga.steps)
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown transaction type: {request.transaction_type}"
        )


@router.get("/sagas/{saga_id}")
async def get_saga_status(saga_id: str):
    """Get status of a SAGA transaction"""
    saga_orchestrator = get_saga_orchestrator()
    saga_status = await saga_orchestrator.get_saga_status(saga_id)
    
    if not saga_status:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="SAGA not found"
        )
    
    return saga_status


@router.get("/sagas")
async def list_active_sagas():
    """List all active SAGA transactions"""
    saga_orchestrator = get_saga_orchestrator()
    sagas = await saga_orchestrator.get_all_active_sagas()
    
    return {
        "total_active_sagas": len(sagas),
        "sagas": sagas
    }


@router.post("/sagas/{saga_id}/cancel")
async def cancel_saga(saga_id: str):
    """Cancel a running SAGA transaction"""
    saga_orchestrator = get_saga_orchestrator()
    success = await saga_orchestrator.cancel_saga(saga_id)
    
    if success:
        return {"message": "SAGA cancellation initiated"}
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot cancel SAGA (not found or already completed)"
        )


# CQRS Framework Endpoints
@router.get("/cqrs/metrics")
async def get_cqrs_metrics():
    """Get CQRS framework metrics"""
    cqrs_framework = get_cqrs_framework()
    metrics = await cqrs_framework.get_metrics()
    return metrics


# Overall Enterprise Metrics
@router.get("/metrics")
async def get_enterprise_metrics():
    """Get comprehensive enterprise metrics"""
    service_registry = get_service_registry()
    saga_orchestrator = get_saga_orchestrator()
    cqrs_framework = get_cqrs_framework()
    
    service_metrics = await service_registry.get_service_metrics()
    active_sagas = await saga_orchestrator.get_all_active_sagas()
    cqrs_metrics = await cqrs_framework.get_metrics()
    
    return {
        "service_registry": service_metrics,
        "saga_orchestrator": {
            "active_sagas": len(active_sagas),
            "sagas_by_status": {}  # Could add more detailed stats
        },
        "cqrs_framework": cqrs_metrics,
        "timestamp": import('time').time()
    }


# Health Check for Enterprise Components
@router.get("/health")
async def enterprise_health_check():
    """Health check for enterprise components"""
    try:
        service_registry = get_service_registry()
        saga_orchestrator = get_saga_orchestrator()
        cqrs_framework = get_cqrs_framework()
        
        # Basic connectivity checks
        service_count = len(service_registry.services)
        active_saga_count = len(saga_orchestrator.active_sagas)
        
        return {
            "status": "healthy",
            "components": {
                "service_registry": {
                    "status": "healthy",
                    "registered_services": service_count
                },
                "saga_orchestrator": {
                    "status": "healthy",
                    "active_sagas": active_saga_count
                },
                "cqrs_framework": {
                    "status": "healthy",
                    "handlers_registered": True
                }
            },
            "timestamp": import('time').time()
        }
    except Exception as e:
        logger.error(f"Enterprise health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Enterprise components unhealthy"
        )
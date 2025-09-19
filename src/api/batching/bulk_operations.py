"""
Advanced Request Batching System for Bulk Operations

High-performance batching system for handling multiple API requests
in a single call with intelligent request grouping and parallel processing.
"""

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

from core.logging import get_logger

logger = get_logger(__name__)


class BatchRequestType(Enum):
    """Types of batch requests."""
    READ = "read"
    WRITE = "write"
    MIXED = "mixed"


class BatchExecutionMode(Enum):
    """Batch execution modes."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    OPTIMIZED = "optimized"


@dataclass
class BatchRequestItem:
    """Individual request within a batch."""
    id: str
    method: str
    path: str
    body: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    query_params: Optional[Dict[str, Any]] = None
    depends_on: Optional[List[str]] = None  # Request IDs this depends on
    priority: int = 1
    timeout: Optional[float] = None
    

@dataclass
class BatchResponseItem:
    """Individual response within a batch."""
    id: str
    status_code: int
    body: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    error: Optional[str] = None
    execution_time_ms: float = 0.0


class BatchRequest(BaseModel):
    """Batch request model."""
    requests: List[Dict[str, Any]] = Field(..., min_items=1, max_items=100)
    execution_mode: BatchExecutionMode = BatchExecutionMode.OPTIMIZED
    max_parallel: int = Field(10, ge=1, le=50)
    timeout_seconds: float = Field(30.0, gt=0, le=300)
    fail_fast: bool = Field(True)
    return_partial_results: bool = Field(True)
    
    @validator('requests')
    def validate_requests(cls, v):
        """Validate batch requests."""
        request_ids = set()
        for req in v:
            if 'id' not in req:
                req['id'] = str(uuid.uuid4())
            
            if req['id'] in request_ids:
                raise ValueError(f"Duplicate request ID: {req['id']}")
            request_ids.add(req['id'])
            
            if 'method' not in req or 'path' not in req:
                raise ValueError("Each request must have 'method' and 'path'")
        
        return v


class BatchResponse(BaseModel):
    """Batch response model."""
    responses: List[Dict[str, Any]]
    batch_id: str
    total_requests: int
    successful_requests: int
    failed_requests: int
    total_execution_time_ms: float
    execution_mode: BatchExecutionMode
    

class DependencyGraph:
    """Manages request dependencies for batch execution."""
    
    def __init__(self, requests: List[BatchRequestItem]):
        self.requests = {req.id: req for req in requests}
        self.graph = {req.id: set(req.depends_on or []) for req in requests}
        self.completed = set()
        self.in_progress = set()
    
    def get_ready_requests(self) -> List[str]:
        """Get request IDs that are ready to execute."""
        ready = []
        for req_id, dependencies in self.graph.items():
            if (req_id not in self.completed and 
                req_id not in self.in_progress and 
                dependencies.issubset(self.completed)):
                ready.append(req_id)
        return ready
    
    def mark_in_progress(self, request_id: str):
        """Mark a request as in progress."""
        self.in_progress.add(request_id)
    
    def mark_completed(self, request_id: str):
        """Mark a request as completed."""
        self.in_progress.discard(request_id)
        self.completed.add(request_id)
    
    def has_pending_requests(self) -> bool:
        """Check if there are pending requests."""
        return len(self.completed) < len(self.requests)
    
    def get_execution_order(self) -> List[List[str]]:
        """Get topologically sorted execution order."""
        levels = []
        temp_completed = set()
        temp_graph = {k: v.copy() for k, v in self.graph.items()}
        
        while temp_graph:
            # Find requests with no dependencies
            current_level = []
            for req_id, deps in temp_graph.items():
                if deps.issubset(temp_completed):
                    current_level.append(req_id)
            
            if not current_level:
                # Circular dependency detected
                remaining_requests = list(temp_graph.keys())
                logger.error(f"Circular dependency detected in requests: {remaining_requests}")
                # Break the cycle by picking the first request
                current_level = [remaining_requests[0]]
            
            levels.append(current_level)
            
            # Remove processed requests
            for req_id in current_level:
                temp_completed.add(req_id)
                temp_graph.pop(req_id, None)
        
        return levels


class BatchProcessor:
    """Processes batch requests with various execution strategies."""
    
    def __init__(self, 
                 max_workers: int = 20,
                 default_timeout: float = 30.0):
        self.max_workers = max_workers
        self.default_timeout = default_timeout
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Statistics
        self.stats = {
            "total_batches": 0,
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "avg_batch_size": 0.0,
            "avg_execution_time": 0.0
        }
    
    async def process_batch(self, 
                          batch_request: BatchRequest,
                          request_handler: Callable[[BatchRequestItem], BatchResponseItem]) -> BatchResponse:
        """Process a batch of requests."""
        start_time = time.time()
        batch_id = str(uuid.uuid4())
        
        logger.info(f"Processing batch {batch_id} with {len(batch_request.requests)} requests")
        
        # Parse requests
        request_items = []
        for req_data in batch_request.requests:
            request_item = BatchRequestItem(
                id=req_data.get('id', str(uuid.uuid4())),
                method=req_data['method'],
                path=req_data['path'],
                body=req_data.get('body'),
                headers=req_data.get('headers'),
                query_params=req_data.get('query_params'),
                depends_on=req_data.get('depends_on', []),
                priority=req_data.get('priority', 1),
                timeout=req_data.get('timeout')
            )
            request_items.append(request_item)
        
        # Execute batch based on mode
        if batch_request.execution_mode == BatchExecutionMode.SEQUENTIAL:
            responses = await self._execute_sequential(request_items, request_handler, batch_request)
        elif batch_request.execution_mode == BatchExecutionMode.PARALLEL:
            responses = await self._execute_parallel(request_items, request_handler, batch_request)
        else:  # OPTIMIZED
            responses = await self._execute_optimized(request_items, request_handler, batch_request)
        
        # Calculate statistics
        total_time = time.time() - start_time
        successful = sum(1 for r in responses if r.status_code < 400)
        failed = len(responses) - successful
        
        # Update global stats
        self.stats["total_batches"] += 1
        self.stats["total_requests"] += len(responses)
        self.stats["successful_requests"] += successful
        self.stats["failed_requests"] += failed
        
        if self.stats["avg_batch_size"] == 0:
            self.stats["avg_batch_size"] = len(responses)
        else:
            alpha = 0.1
            self.stats["avg_batch_size"] = (
                alpha * len(responses) + (1 - alpha) * self.stats["avg_batch_size"]
            )
        
        if self.stats["avg_execution_time"] == 0:
            self.stats["avg_execution_time"] = total_time
        else:
            alpha = 0.1
            self.stats["avg_execution_time"] = (
                alpha * total_time + (1 - alpha) * self.stats["avg_execution_time"]
            )
        
        # Create response
        return BatchResponse(
            responses=[{
                "id": r.id,
                "status": r.status_code,
                "body": r.body,
                "headers": r.headers or {},
                "error": r.error,
                "execution_time_ms": r.execution_time_ms
            } for r in responses],
            batch_id=batch_id,
            total_requests=len(request_items),
            successful_requests=successful,
            failed_requests=failed,
            total_execution_time_ms=total_time * 1000,
            execution_mode=batch_request.execution_mode
        )
    
    async def _execute_sequential(self, 
                                requests: List[BatchRequestItem],
                                handler: Callable,
                                batch_request: BatchRequest) -> List[BatchResponseItem]:
        """Execute requests sequentially."""
        responses = []
        
        # Sort by priority
        sorted_requests = sorted(requests, key=lambda r: r.priority, reverse=True)
        
        for request_item in sorted_requests:
            try:
                response = await self._execute_single_request(request_item, handler)
                responses.append(response)
                
                # Check if we should fail fast
                if batch_request.fail_fast and response.status_code >= 400:
                    logger.warning(f"Batch execution failed fast at request {request_item.id}")
                    break
                    
            except Exception as e:
                logger.error(f"Error executing request {request_item.id}: {e}")
                responses.append(BatchResponseItem(
                    id=request_item.id,
                    status_code=500,
                    error=str(e)
                ))
                
                if batch_request.fail_fast:
                    break
        
        return responses
    
    async def _execute_parallel(self,
                              requests: List[BatchRequestItem],
                              handler: Callable,
                              batch_request: BatchRequest) -> List[BatchResponseItem]:
        """Execute requests in parallel."""
        responses = []
        
        # Create semaphore to limit concurrency
        semaphore = asyncio.Semaphore(batch_request.max_parallel)
        
        async def execute_with_semaphore(request_item: BatchRequestItem) -> BatchResponseItem:
            async with semaphore:
                return await self._execute_single_request(request_item, handler)
        
        # Execute all requests concurrently
        tasks = [execute_with_semaphore(req) for req in requests]
        
        try:
            # Wait for all tasks to complete or timeout
            responses = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=batch_request.timeout_seconds
            )
            
            # Handle exceptions
            final_responses = []
            for i, response in enumerate(responses):
                if isinstance(response, Exception):
                    final_responses.append(BatchResponseItem(
                        id=requests[i].id,
                        status_code=500,
                        error=str(response)
                    ))
                else:
                    final_responses.append(response)
            
            return final_responses
            
        except asyncio.TimeoutError:
            logger.error(f"Batch execution timed out after {batch_request.timeout_seconds}s")
            return [BatchResponseItem(
                id=req.id,
                status_code=408,
                error="Request timeout"
            ) for req in requests]
    
    async def _execute_optimized(self,
                               requests: List[BatchRequestItem],
                               handler: Callable,
                               batch_request: BatchRequest) -> List[BatchResponseItem]:
        """Execute requests with dependency awareness and optimization."""
        # Build dependency graph
        dep_graph = DependencyGraph(requests)
        execution_levels = dep_graph.get_execution_order()
        
        responses = {}
        
        for level in execution_levels:
            # Execute requests in this level in parallel
            level_requests = [req for req in requests if req.id in level]
            level_responses = await self._execute_parallel(level_requests, handler, batch_request)
            
            # Store responses
            for response in level_responses:
                responses[response.id] = response
                
                # Check for failures in fail_fast mode
                if batch_request.fail_fast and response.status_code >= 400:
                    logger.warning(f"Optimized batch execution failed fast at request {response.id}")
                    # Return partial results if requested
                    if batch_request.return_partial_results:
                        return list(responses.values())
                    else:
                        raise HTTPException(
                            status_code=500,
                            detail=f"Batch failed at request {response.id}: {response.error}"
                        )
        
        # Return responses in original request order
        return [responses[req.id] for req in requests if req.id in responses]
    
    async def _execute_single_request(self,
                                    request_item: BatchRequestItem,
                                    handler: Callable) -> BatchResponseItem:
        """Execute a single request within the batch."""
        start_time = time.time()
        
        try:
            response = await handler(request_item)
            response.execution_time_ms = (time.time() - start_time) * 1000
            return response
            
        except Exception as e:
            logger.error(f"Error in single request {request_item.id}: {e}")
            return BatchResponseItem(
                id=request_item.id,
                status_code=500,
                error=str(e),
                execution_time_ms=(time.time() - start_time) * 1000
            )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get batch processor statistics."""
        return self.stats.copy()


# Global batch processor
_batch_processor: Optional[BatchProcessor] = None


def get_batch_processor() -> BatchProcessor:
    """Get or create global batch processor."""
    global _batch_processor
    if _batch_processor is None:
        _batch_processor = BatchProcessor()
    return _batch_processor


# Batch request optimization utilities
class BatchOptimizer:
    """Optimizes batch requests for better performance."""
    
    @staticmethod
    def group_similar_requests(requests: List[BatchRequestItem]) -> Dict[str, List[BatchRequestItem]]:
        """Group similar requests together for potential optimization."""
        groups = {}
        
        for request in requests:
            # Group by method and path pattern
            key = f"{request.method}:{request.path.split('?')[0]}"
            if key not in groups:
                groups[key] = []
            groups[key].append(request)
        
        return groups
    
    @staticmethod
    def detect_bulk_operations(requests: List[BatchRequestItem]) -> List[Tuple[str, List[BatchRequestItem]]]:
        """Detect requests that can be converted to bulk operations."""
        bulk_candidates = []
        groups = BatchOptimizer.group_similar_requests(requests)
        
        for group_key, group_requests in groups.items():
            if len(group_requests) > 1:
                # Check if these can be bulk processed
                method, path = group_key.split(':', 1)
                
                # Common bulk operation patterns
                if (method == "POST" and any(pattern in path for pattern in ['/create', '/insert', '/add']) or
                    method == "PUT" and any(pattern in path for pattern in ['/update', '/modify']) or
                    method == "DELETE" and any(pattern in path for pattern in ['/delete', '/remove'])):
                    
                    bulk_candidates.append((group_key, group_requests))
        
        return bulk_candidates


# FastAPI integration
async def handle_batch_request(batch_data: Dict[str, Any], 
                             request_handler: Callable) -> Dict[str, Any]:
    """Handle a batch request and return the response."""
    try:
        batch_request = BatchRequest(**batch_data)
        processor = get_batch_processor()
        
        response = await processor.process_batch(batch_request, request_handler)
        return response.dict()
        
    except Exception as e:
        logger.error(f"Batch request handling failed: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Invalid batch request: {str(e)}"
        )


# Middleware for automatic batch detection
class BatchDetectionMiddleware:
    """Middleware to detect potential batch operations."""
    
    def __init__(self, detection_window: float = 1.0, min_requests: int = 3):
        self.detection_window = detection_window
        self.min_requests = min_requests
        self.request_buffer = {}
        
    def add_request(self, request: Request) -> bool:
        """Add request to buffer and check if batching is beneficial."""
        now = time.time()
        client_id = self._get_client_id(request)
        
        # Clean old requests
        self._clean_old_requests(now)
        
        # Add current request
        if client_id not in self.request_buffer:
            self.request_buffer[client_id] = []
        
        self.request_buffer[client_id].append({
            'timestamp': now,
            'method': request.method,
            'path': str(request.url.path),
            'query': str(request.url.query)
        })
        
        # Check if batching would be beneficial
        recent_requests = [
            r for r in self.request_buffer[client_id]
            if now - r['timestamp'] <= self.detection_window
        ]
        
        return len(recent_requests) >= self.min_requests
    
    def _get_client_id(self, request: Request) -> str:
        """Get client identifier for batching detection."""
        # Use IP + User-Agent as client ID
        client_ip = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "")[:50]
        return f"{client_ip}:{hash(user_agent)}"
    
    def _clean_old_requests(self, now: float):
        """Clean old requests from buffer."""
        cutoff = now - self.detection_window * 2
        
        for client_id in list(self.request_buffer.keys()):
            self.request_buffer[client_id] = [
                r for r in self.request_buffer[client_id]
                if r['timestamp'] > cutoff
            ]
            
            if not self.request_buffer[client_id]:
                del self.request_buffer[client_id]
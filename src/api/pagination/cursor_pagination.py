"""
Advanced Cursor-Based Pagination System

High-performance pagination implementation using cursor-based navigation
for optimal database query performance and consistent results.
"""

import base64
import json
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union, Generic, TypeVar
from dataclasses import dataclass, asdict
from urllib.parse import urlencode

from sqlalchemy import desc, asc, and_, or_, text
from sqlalchemy.orm import Query
from sqlalchemy.sql import Select
from pydantic import BaseModel, Field, validator

from core.logging import get_logger

logger = get_logger(__name__)

T = TypeVar('T')


class SortDirection(Enum):
    """Sort direction options."""
    ASC = "asc"
    DESC = "desc"


class CursorType(Enum):
    """Cursor encoding types."""
    TIMESTAMP = "timestamp"
    ID = "id"
    COMPOSITE = "composite"
    OFFSET = "offset"


@dataclass
class CursorData:
    """Cursor data structure."""
    type: CursorType
    values: Dict[str, Any]
    timestamp: datetime
    
    def encode(self) -> str:
        """Encode cursor data to base64 string."""
        data = {
            "type": self.type.value,
            "values": self.values,
            "timestamp": self.timestamp.isoformat()
        }
        json_str = json.dumps(data, default=str)
        return base64.b64encode(json_str.encode()).decode()
    
    @classmethod
    def decode(cls, cursor: str) -> 'CursorData':
        """Decode base64 cursor string to cursor data."""
        try:
            json_str = base64.b64decode(cursor.encode()).decode()
            data = json.loads(json_str)
            
            return cls(
                type=CursorType(data["type"]),
                values=data["values"],
                timestamp=datetime.fromisoformat(data["timestamp"])
            )
        except Exception as e:
            logger.error(f"Failed to decode cursor: {e}")
            raise ValueError(f"Invalid cursor format: {e}")


class PaginationRequest(BaseModel):
    """Pagination request parameters."""
    first: Optional[int] = Field(None, ge=1, le=1000, description="Number of items to fetch forward")
    last: Optional[int] = Field(None, ge=1, le=1000, description="Number of items to fetch backward")
    after: Optional[str] = Field(None, description="Cursor for forward pagination")
    before: Optional[str] = Field(None, description="Cursor for backward pagination")
    sort_by: Optional[str] = Field("id", description="Field to sort by")
    sort_direction: SortDirection = Field(SortDirection.ASC, description="Sort direction")
    
    @validator('first', 'last')
    def validate_pagination_params(cls, v, values):
        """Validate pagination parameters."""
        if 'first' in values and 'last' in values:
            if values.get('first') and v:
                raise ValueError("Cannot specify both 'first' and 'last'")
        return v
    
    @validator('after', 'before')
    def validate_cursor_params(cls, v, values):
        """Validate cursor parameters."""
        if 'after' in values and 'before' in values:
            if values.get('after') and v:
                raise ValueError("Cannot specify both 'after' and 'before'")
        return v
    
    @property
    def limit(self) -> int:
        """Get the effective limit."""
        return self.first or self.last or 50
    
    @property
    def is_forward(self) -> bool:
        """Check if this is forward pagination."""
        return self.first is not None or self.after is not None
    
    @property
    def is_backward(self) -> bool:
        """Check if this is backward pagination."""
        return self.last is not None or self.before is not None


@dataclass
class PageInfo:
    """Page information for pagination."""
    has_previous_page: bool
    has_next_page: bool
    start_cursor: Optional[str]
    end_cursor: Optional[str]
    total_count: Optional[int] = None


@dataclass 
class PaginatedResult(Generic[T]):
    """Paginated result container."""
    edges: List[Dict[str, Any]]
    page_info: PageInfo
    total_count: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "edges": self.edges,
            "pageInfo": asdict(self.page_info),
            "totalCount": self.total_count
        }


class CursorPaginator:
    """Advanced cursor-based paginator."""
    
    def __init__(self, 
                 default_limit: int = 50,
                 max_limit: int = 1000,
                 enable_total_count: bool = True,
                 cursor_fields: Optional[List[str]] = None):
        self.default_limit = default_limit
        self.max_limit = max_limit
        self.enable_total_count = enable_total_count
        self.cursor_fields = cursor_fields or ["id"]
    
    async def paginate(self, 
                      query: Union[Query, Select],
                      request: PaginationRequest,
                      cursor_field: Optional[str] = None,
                      transform_fn: Optional[callable] = None) -> PaginatedResult:
        """
        Paginate query results using cursor-based pagination.
        
        Args:
            query: SQLAlchemy Query or Select statement
            request: Pagination request parameters
            cursor_field: Field to use for cursor (default: 'id')
            transform_fn: Function to transform each result item
        
        Returns:
            PaginatedResult with edges and page info
        """
        
        cursor_field = cursor_field or self.cursor_fields[0]
        limit = min(request.limit, self.max_limit)
        
        # Apply cursor filtering
        filtered_query = await self._apply_cursor_filter(query, request, cursor_field)
        
        # Apply sorting
        sorted_query = self._apply_sorting(filtered_query, request.sort_by, request.sort_direction)
        
        # Fetch one extra item to determine if there are more pages
        items_query = sorted_query.limit(limit + 1)
        
        # Execute query
        if hasattr(items_query, 'execute'):
            # Raw SQL or select statement
            result = await items_query.execute()
            items = result.fetchall()
        else:
            # SQLAlchemy ORM query
            items = items_query.all()
        
        # Check if there are more items
        has_more = len(items) > limit
        if has_more:
            items = items[:limit]
        
        # Transform items if function provided
        if transform_fn:
            items = [transform_fn(item) for item in items]
        
        # Create edges with cursors
        edges = []
        for item in items:
            cursor_data = self._create_cursor_data(item, cursor_field)
            cursor = cursor_data.encode()
            
            edge = {
                "node": item if not hasattr(item, '__dict__') else self._serialize_item(item),
                "cursor": cursor
            }
            edges.append(edge)
        
        # Determine page info
        page_info = await self._create_page_info(
            query, request, edges, has_more, cursor_field
        )
        
        # Get total count if enabled
        total_count = None
        if self.enable_total_count:
            total_count = await self._get_total_count(query)
        
        return PaginatedResult(
            edges=edges,
            page_info=page_info,
            total_count=total_count
        )
    
    async def _apply_cursor_filter(self, query: Union[Query, Select], 
                                 request: PaginationRequest, 
                                 cursor_field: str) -> Union[Query, Select]:
        """Apply cursor-based filtering to query."""
        
        if request.after:
            # Forward pagination: get items after cursor
            cursor_data = CursorData.decode(request.after)
            cursor_value = cursor_data.values.get(cursor_field)
            
            if cursor_value:
                if request.sort_direction == SortDirection.ASC:
                    query = query.filter(getattr(query.column_descriptions[0]['entity'], cursor_field) > cursor_value)
                else:
                    query = query.filter(getattr(query.column_descriptions[0]['entity'], cursor_field) < cursor_value)
        
        elif request.before:
            # Backward pagination: get items before cursor
            cursor_data = CursorData.decode(request.before)
            cursor_value = cursor_data.values.get(cursor_field)
            
            if cursor_value:
                if request.sort_direction == SortDirection.ASC:
                    query = query.filter(getattr(query.column_descriptions[0]['entity'], cursor_field) < cursor_value)
                else:
                    query = query.filter(getattr(query.column_descriptions[0]['entity'], cursor_field) > cursor_value)
        
        return query
    
    def _apply_sorting(self, query: Union[Query, Select], 
                      sort_by: str, 
                      sort_direction: SortDirection) -> Union[Query, Select]:
        """Apply sorting to query."""
        
        try:
            # Get the sort column
            if hasattr(query, 'column_descriptions') and query.column_descriptions:
                entity = query.column_descriptions[0]['entity']
                sort_column = getattr(entity, sort_by)
            else:
                # For raw select statements, assume sort_by is a column name
                sort_column = text(sort_by)
            
            # Apply sort direction
            if sort_direction == SortDirection.DESC:
                query = query.order_by(desc(sort_column))
            else:
                query = query.order_by(asc(sort_column))
                
        except AttributeError:
            logger.warning(f"Could not sort by field '{sort_by}', using default sorting")
            # Fallback to default sorting
            if hasattr(query, 'order_by'):
                if sort_direction == SortDirection.DESC:
                    query = query.order_by(desc('id'))
                else:
                    query = query.order_by(asc('id'))
        
        return query
    
    def _create_cursor_data(self, item: Any, cursor_field: str) -> CursorData:
        """Create cursor data for an item."""
        
        # Extract cursor value
        if hasattr(item, cursor_field):
            cursor_value = getattr(item, cursor_field)
        elif isinstance(item, dict):
            cursor_value = item.get(cursor_field)
        else:
            # Try to access by index if it's a tuple/list
            try:
                cursor_value = item[0] if hasattr(item, '__getitem__') else str(item)
            except (IndexError, TypeError):
                cursor_value = str(item)
        
        # Determine cursor type
        if cursor_field in ['created_at', 'updated_at', 'timestamp']:
            cursor_type = CursorType.TIMESTAMP
        elif len(self.cursor_fields) > 1:
            cursor_type = CursorType.COMPOSITE
        else:
            cursor_type = CursorType.ID
        
        return CursorData(
            type=cursor_type,
            values={cursor_field: cursor_value},
            timestamp=datetime.utcnow()
        )
    
    async def _create_page_info(self, 
                              query: Union[Query, Select],
                              request: PaginationRequest,
                              edges: List[Dict[str, Any]],
                              has_more: bool,
                              cursor_field: str) -> PageInfo:
        """Create page info for pagination."""
        
        start_cursor = edges[0]["cursor"] if edges else None
        end_cursor = edges[-1]["cursor"] if edges else None
        
        # Determine if there are previous/next pages
        if request.is_forward:
            has_next_page = has_more
            has_previous_page = bool(request.after)  # If we have 'after', there's a previous page
        else:
            has_previous_page = has_more
            has_next_page = bool(request.before)  # If we have 'before', there's a next page
        
        return PageInfo(
            has_previous_page=has_previous_page,
            has_next_page=has_next_page,
            start_cursor=start_cursor,
            end_cursor=end_cursor
        )
    
    async def _get_total_count(self, query: Union[Query, Select]) -> Optional[int]:
        """Get total count for the query."""
        try:
            if hasattr(query, 'count'):
                return query.count()
            else:
                # For select statements, we need to wrap in a count query
                # This is a simplified implementation
                count_query = query.alias().count()
                result = await count_query.execute()
                return result.scalar()
                
        except Exception as e:
            logger.warning(f"Could not get total count: {e}")
            return None
    
    def _serialize_item(self, item: Any) -> Dict[str, Any]:
        """Serialize an item to dictionary."""
        if hasattr(item, '__dict__'):
            # SQLAlchemy model
            result = {}
            for key, value in item.__dict__.items():
                if not key.startswith('_'):
                    if isinstance(value, datetime):
                        result[key] = value.isoformat()
                    else:
                        result[key] = value
            return result
        elif isinstance(item, dict):
            return item
        else:
            return {"value": str(item)}


class OptimizedPaginator(CursorPaginator):
    """Optimized cursor paginator with advanced features."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query_cache = {}
        self.performance_metrics = {
            "queries_executed": 0,
            "avg_query_time": 0.0,
            "cache_hits": 0
        }
    
    async def paginate_with_cache(self, 
                                 query: Union[Query, Select],
                                 request: PaginationRequest,
                                 cache_key: Optional[str] = None,
                                 cache_ttl: int = 300,
                                 **kwargs) -> PaginatedResult:
        """Paginate with query result caching."""
        
        if cache_key and cache_key in self.query_cache:
            cache_entry = self.query_cache[cache_key]
            if (datetime.utcnow() - cache_entry['timestamp']).seconds < cache_ttl:
                self.performance_metrics["cache_hits"] += 1
                logger.debug(f"Using cached pagination result for key: {cache_key}")
                return cache_entry['result']
        
        # Execute pagination
        import time
        start_time = time.time()
        
        result = await self.paginate(query, request, **kwargs)
        
        query_time = time.time() - start_time
        self.performance_metrics["queries_executed"] += 1
        
        # Update average query time
        if self.performance_metrics["avg_query_time"] == 0:
            self.performance_metrics["avg_query_time"] = query_time
        else:
            alpha = 0.1
            self.performance_metrics["avg_query_time"] = (
                alpha * query_time + 
                (1 - alpha) * self.performance_metrics["avg_query_time"]
            )
        
        # Cache result if cache key provided
        if cache_key:
            self.query_cache[cache_key] = {
                'result': result,
                'timestamp': datetime.utcnow()
            }
        
        return result
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get pagination performance statistics."""
        return {
            "queries_executed": self.performance_metrics["queries_executed"],
            "avg_query_time_ms": self.performance_metrics["avg_query_time"] * 1000,
            "cache_hits": self.performance_metrics["cache_hits"],
            "cache_size": len(self.query_cache),
            "cache_hit_ratio": (
                self.performance_metrics["cache_hits"] / 
                max(self.performance_metrics["queries_executed"], 1)
            )
        }


# Factory functions for easy creation
def create_cursor_paginator(default_limit: int = 50, 
                          max_limit: int = 1000,
                          enable_total_count: bool = True) -> CursorPaginator:
    """Create a standard cursor paginator."""
    return CursorPaginator(
        default_limit=default_limit,
        max_limit=max_limit,
        enable_total_count=enable_total_count
    )


def create_optimized_paginator(default_limit: int = 50,
                             max_limit: int = 1000,
                             enable_total_count: bool = True) -> OptimizedPaginator:
    """Create an optimized cursor paginator with caching."""
    return OptimizedPaginator(
        default_limit=default_limit,
        max_limit=max_limit,
        enable_total_count=enable_total_count
    )


# Global paginator instance
_global_paginator: Optional[OptimizedPaginator] = None


def get_paginator() -> OptimizedPaginator:
    """Get or create global paginator instance."""
    global _global_paginator
    if _global_paginator is None:
        _global_paginator = create_optimized_paginator()
    return _global_paginator


# Utility functions
def encode_cursor(cursor_field: str, cursor_value: Any) -> str:
    """Encode a simple cursor."""
    cursor_data = CursorData(
        type=CursorType.ID,
        values={cursor_field: cursor_value},
        timestamp=datetime.utcnow()
    )
    return cursor_data.encode()


def decode_cursor(cursor: str) -> Dict[str, Any]:
    """Decode cursor to values dictionary."""
    cursor_data = CursorData.decode(cursor)
    return cursor_data.values


# FastAPI dependency for pagination
def pagination_params(
    first: Optional[int] = None,
    last: Optional[int] = None,
    after: Optional[str] = None,
    before: Optional[str] = None,
    sort_by: str = "id",
    sort_direction: SortDirection = SortDirection.ASC
) -> PaginationRequest:
    """FastAPI dependency for pagination parameters."""
    return PaginationRequest(
        first=first,
        last=last,
        after=after,
        before=before,
        sort_by=sort_by,
        sort_direction=sort_direction
    )
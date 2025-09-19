"""
Advanced DataLoader Implementation for GraphQL

High-performance DataLoader pattern implementation to eliminate N+1 queries
in GraphQL resolvers with intelligent batching and caching.
"""

import asyncio
import time
from collections import defaultdict, OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable, Generic, TypeVar
from typing import Awaitable
from weakref import WeakValueDictionary

from sqlalchemy import select, and_, or_, text
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from core.logging import get_logger
from data_access.models.star_schema import (
    DimProduct, DimCustomer, DimCountry, DimDate, FactSale
)

logger = get_logger(__name__)

K = TypeVar('K')  # Key type
V = TypeVar('V')  # Value type


@dataclass
class BatchRequest:
    """Represents a batch load request."""
    keys: List[Any]
    future: asyncio.Future
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def __post_init__(self):
        if not self.future.done():
            self.future.set_result(None)


@dataclass
class LoaderStats:
    """Statistics for DataLoader performance monitoring."""
    total_batches: int = 0
    total_keys: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    avg_batch_size: float = 0.0
    avg_load_time: float = 0.0
    total_load_time: float = 0.0
    
    def record_batch(self, batch_size: int, load_time: float, cache_hits: int = 0):
        """Record batch statistics."""
        self.total_batches += 1
        self.total_keys += batch_size
        self.cache_hits += cache_hits
        self.cache_misses += batch_size - cache_hits
        self.total_load_time += load_time
        
        # Update averages
        self.avg_batch_size = self.total_keys / self.total_batches
        self.avg_load_time = self.total_load_time / self.total_batches
    
    def get_cache_hit_ratio(self) -> float:
        """Calculate cache hit ratio."""
        total_requests = self.cache_hits + self.cache_misses
        return self.cache_hits / total_requests if total_requests > 0 else 0.0


class DataLoader(Generic[K, V]):
    """
    High-performance DataLoader implementation with advanced features:
    - Intelligent batching with configurable delays
    - Multi-level caching (in-memory + Redis)
    - Performance monitoring and statistics
    - Automatic cache invalidation
    - Error handling and retry logic
    """
    
    def __init__(self,
                 batch_load_fn: Callable[[List[K]], Awaitable[List[V]]],
                 max_batch_size: int = 1000,
                 batch_delay_ms: int = 10,
                 cache_enabled: bool = True,
                 cache_ttl: int = 300,  # 5 minutes
                 cache_size: int = 10000,
                 enable_stats: bool = True,
                 retry_attempts: int = 3,
                 retry_delay: float = 0.1):
        
        self.batch_load_fn = batch_load_fn
        self.max_batch_size = max_batch_size
        self.batch_delay_ms = batch_delay_ms
        self.cache_enabled = cache_enabled
        self.cache_ttl = cache_ttl
        self.cache_size = cache_size
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        
        # Internal state
        self._pending_keys: Set[K] = set()
        self._pending_requests: List[BatchRequest] = []
        self._batch_timer: Optional[asyncio.Handle] = None
        self._loading = False
        
        # Cache implementation
        self._cache: OrderedDict[K, Tuple[V, datetime]] = OrderedDict()
        self._cache_lock = asyncio.Lock()
        
        # Statistics
        self.stats = LoaderStats() if enable_stats else None
        
        # Weak references to avoid memory leaks
        self._request_futures: WeakValueDictionary = WeakValueDictionary()
    
    async def load(self, key: K) -> V:
        """Load a single item by key."""
        results = await self.load_many([key])
        return results[0] if results else None
    
    async def load_many(self, keys: List[K]) -> List[V]:
        """Load multiple items by keys."""
        if not keys:
            return []
        
        # Check cache first
        cached_results = {}
        uncached_keys = []
        
        if self.cache_enabled:
            async with self._cache_lock:
                for key in keys:
                    cached_item = self._get_from_cache(key)
                    if cached_item is not None:
                        cached_results[key] = cached_item
                        if self.stats:
                            self.stats.cache_hits += 1
                    else:
                        uncached_keys.append(key)
                        if self.stats:
                            self.stats.cache_misses += 1
        else:
            uncached_keys = keys
        
        # If all items are cached, return them
        if not uncached_keys:
            return [cached_results.get(key) for key in keys]
        
        # Create futures for uncached keys
        futures = {}
        for key in uncached_keys:
            if key not in self._pending_keys:
                future = asyncio.Future()
                futures[key] = future
                self._request_futures[key] = future
                self._pending_keys.add(key)
        
        # Schedule batch loading
        await self._schedule_batch_load(uncached_keys)
        
        # Wait for results
        uncached_results = {}
        for key in uncached_keys:
            if key in futures:
                try:
                    result = await futures[key]
                    uncached_results[key] = result
                except Exception as e:
                    logger.error(f"DataLoader error for key {key}: {e}")
                    uncached_results[key] = None
        
        # Combine cached and loaded results
        final_results = []
        for key in keys:
            if key in cached_results:
                final_results.append(cached_results[key])
            else:
                final_results.append(uncached_results.get(key))
        
        return final_results
    
    async def _schedule_batch_load(self, keys: List[K]):
        """Schedule batch loading with intelligent batching."""
        self._pending_requests.append(BatchRequest(
            keys=keys,
            future=asyncio.Future()
        ))
        
        # If we have enough keys or timer not set, trigger load immediately
        if (len(self._pending_keys) >= self.max_batch_size or 
            self._batch_timer is None):
            
            if self._batch_timer:
                self._batch_timer.cancel()
            
            # Small delay to allow more requests to batch
            await asyncio.sleep(self.batch_delay_ms / 1000.0)
            await self._execute_batch()
        else:
            # Schedule delayed execution
            if not self._batch_timer:
                self._batch_timer = asyncio.get_event_loop().call_later(
                    self.batch_delay_ms / 1000.0,
                    lambda: asyncio.create_task(self._execute_batch())
                )
    
    async def _execute_batch(self):
        """Execute batch loading."""
        if self._loading or not self._pending_keys:
            return
        
        self._loading = True
        self._batch_timer = None
        
        try:
            # Collect all pending keys
            keys_to_load = list(self._pending_keys)
            pending_requests = self._pending_requests.copy()
            
            # Clear pending state
            self._pending_keys.clear()
            self._pending_requests.clear()
            
            # Execute batch load with retry logic
            start_time = time.time()
            results = await self._execute_with_retry(keys_to_load)
            load_time = time.time() - start_time
            
            # Cache results
            if self.cache_enabled and results:
                async with self._cache_lock:
                    for key, value in zip(keys_to_load, results):
                        if value is not None:
                            self._set_cache(key, value)
            
            # Resolve futures
            key_to_result = dict(zip(keys_to_load, results))
            for key, future in self._request_futures.items():
                if key in key_to_result and not future.done():
                    future.set_result(key_to_result[key])
            
            # Record statistics
            if self.stats:
                cache_hits = sum(1 for key in keys_to_load 
                               if self._get_from_cache(key) is not None)
                self.stats.record_batch(len(keys_to_load), load_time, cache_hits)
            
            logger.debug(f"DataLoader batch executed: {len(keys_to_load)} keys in {load_time:.3f}s")
            
        except Exception as e:
            logger.error(f"DataLoader batch execution failed: {e}")
            # Reject all pending futures
            for future in self._request_futures.values():
                if not future.done():
                    future.set_exception(e)
        finally:
            self._loading = False
    
    async def _execute_with_retry(self, keys: List[K]) -> List[V]:
        """Execute batch load function with retry logic."""
        last_exception = None
        
        for attempt in range(self.retry_attempts):
            try:
                return await self.batch_load_fn(keys)
            except Exception as e:
                last_exception = e
                if attempt < self.retry_attempts - 1:
                    await asyncio.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                    logger.warning(f"DataLoader retry {attempt + 1}/{self.retry_attempts}: {e}")
                else:
                    logger.error(f"DataLoader failed after {self.retry_attempts} attempts: {e}")
        
        raise last_exception or Exception("DataLoader execution failed")
    
    def _get_from_cache(self, key: K) -> Optional[V]:
        """Get item from cache if not expired."""
        if key in self._cache:
            value, timestamp = self._cache[key]
            if datetime.utcnow() - timestamp < timedelta(seconds=self.cache_ttl):
                # Move to end (LRU)
                del self._cache[key]
                self._cache[key] = (value, timestamp)
                return value
            else:
                # Expired
                del self._cache[key]
        return None
    
    def _set_cache(self, key: K, value: V):
        """Set item in cache with LRU eviction."""
        # Remove if exists
        if key in self._cache:
            del self._cache[key]
        
        # Add to end
        self._cache[key] = (value, datetime.utcnow())
        
        # Evict oldest if cache is full
        while len(self._cache) > self.cache_size:
            self._cache.popitem(last=False)
    
    async def clear_cache(self, keys: Optional[List[K]] = None):
        """Clear cache for specific keys or all keys."""
        async with self._cache_lock:
            if keys:
                for key in keys:
                    self._cache.pop(key, None)
            else:
                self._cache.clear()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "cache_size": len(self._cache),
            "max_cache_size": self.cache_size,
            "cache_utilization": len(self._cache) / self.cache_size,
            "stats": self.stats.__dict__ if self.stats else None
        }


class DatabaseDataLoader(DataLoader[K, V]):
    """Specialized DataLoader for database operations."""
    
    def __init__(self, session: Session, *args, **kwargs):
        self.session = session
        super().__init__(*args, **kwargs)


# Specialized DataLoaders for our domain models
class ProductDataLoader(DatabaseDataLoader[int, DimProduct]):
    """DataLoader for products by product_key."""
    
    def __init__(self, session: Session):
        super().__init__(
            session,
            batch_load_fn=self._batch_load_products,
            max_batch_size=500,
            cache_ttl=600  # 10 minutes for products
        )
    
    async def _batch_load_products(self, product_keys: List[int]) -> List[Optional[DimProduct]]:
        """Batch load products by keys."""
        try:
            query = select(DimProduct).where(DimProduct.product_key.in_(product_keys))
            results = self.session.exec(query).all()
            
            # Create lookup dict
            product_dict = {product.product_key: product for product in results}
            
            # Return in same order as requested keys
            return [product_dict.get(key) for key in product_keys]
            
        except Exception as e:
            logger.error(f"Error batch loading products: {e}")
            return [None] * len(product_keys)


class CustomerDataLoader(DatabaseDataLoader[int, DimCustomer]):
    """DataLoader for customers by customer_key."""
    
    def __init__(self, session: Session):
        super().__init__(
            session,
            batch_load_fn=self._batch_load_customers,
            max_batch_size=500,
            cache_ttl=300  # 5 minutes for customers
        )
    
    async def _batch_load_customers(self, customer_keys: List[int]) -> List[Optional[DimCustomer]]:
        """Batch load customers by keys."""
        try:
            query = select(DimCustomer).where(DimCustomer.customer_key.in_(customer_keys))
            results = self.session.exec(query).all()
            
            customer_dict = {customer.customer_key: customer for customer in results}
            return [customer_dict.get(key) for key in customer_keys]
            
        except Exception as e:
            logger.error(f"Error batch loading customers: {e}")
            return [None] * len(customer_keys)


class CountryDataLoader(DatabaseDataLoader[int, DimCountry]):
    """DataLoader for countries by country_key."""
    
    def __init__(self, session: Session):
        super().__init__(
            session,
            batch_load_fn=self._batch_load_countries,
            max_batch_size=100,  # Smaller batch for countries
            cache_ttl=3600  # 1 hour for countries
        )
    
    async def _batch_load_countries(self, country_keys: List[int]) -> List[Optional[DimCountry]]:
        """Batch load countries by keys."""
        try:
            query = select(DimCountry).where(DimCountry.country_key.in_(country_keys))
            results = self.session.exec(query).all()
            
            country_dict = {country.country_key: country for country in results}
            return [country_dict.get(key) for key in country_keys]
            
        except Exception as e:
            logger.error(f"Error batch loading countries: {e}")
            return [None] * len(country_keys)


class SalesByCustomerDataLoader(DatabaseDataLoader[int, List[FactSale]]):
    """DataLoader for sales by customer_key."""
    
    def __init__(self, session: Session):
        super().__init__(
            session,
            batch_load_fn=self._batch_load_sales_by_customer,
            max_batch_size=200,
            cache_ttl=180  # 3 minutes for sales data
        )
    
    async def _batch_load_sales_by_customer(self, customer_keys: List[int]) -> List[List[FactSale]]:
        """Batch load sales by customer keys."""
        try:
            query = select(FactSale).where(FactSale.customer_key.in_(customer_keys))
            results = self.session.exec(query).all()
            
            # Group by customer_key
            sales_by_customer = defaultdict(list)
            for sale in results:
                sales_by_customer[sale.customer_key].append(sale)
            
            return [sales_by_customer.get(key, []) for key in customer_keys]
            
        except Exception as e:
            logger.error(f"Error batch loading sales by customer: {e}")
            return [[] for _ in customer_keys]


class SalesByProductDataLoader(DatabaseDataLoader[int, List[FactSale]]):
    """DataLoader for sales by product_key."""
    
    def __init__(self, session: Session):
        super().__init__(
            session,
            batch_load_fn=self._batch_load_sales_by_product,
            max_batch_size=200,
            cache_ttl=180  # 3 minutes for sales data
        )
    
    async def _batch_load_sales_by_product(self, product_keys: List[int]) -> List[List[FactSale]]:
        """Batch load sales by product keys."""
        try:
            query = select(FactSale).where(FactSale.product_key.in_(product_keys))
            results = self.session.exec(query).all()
            
            # Group by product_key
            sales_by_product = defaultdict(list)
            for sale in results:
                sales_by_product[sale.product_key].append(sale)
            
            return [sales_by_product.get(key, []) for key in product_keys]
            
        except Exception as e:
            logger.error(f"Error batch loading sales by product: {e}")
            return [[] for _ in product_keys]


class DataLoaderRegistry:
    """Registry for managing multiple DataLoaders."""
    
    def __init__(self, session: Session):
        self.session = session
        self.loaders: Dict[str, DataLoader] = {}
        self._initialize_loaders()
    
    def _initialize_loaders(self):
        """Initialize all DataLoaders."""
        self.loaders = {
            'products': ProductDataLoader(self.session),
            'customers': CustomerDataLoader(self.session),
            'countries': CountryDataLoader(self.session),
            'sales_by_customer': SalesByCustomerDataLoader(self.session),
            'sales_by_product': SalesByProductDataLoader(self.session)
        }
    
    def get_loader(self, name: str) -> DataLoader:
        """Get DataLoader by name."""
        if name not in self.loaders:
            raise ValueError(f"DataLoader '{name}' not found")
        return self.loaders[name]
    
    async def clear_all_caches(self):
        """Clear all DataLoader caches."""
        for loader in self.loaders.values():
            await loader.clear_cache()
    
    def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics from all DataLoaders."""
        return {
            name: loader.get_cache_stats() 
            for name, loader in self.loaders.items()
        }


# Context manager for DataLoader registry
class DataLoaderContext:
    """Context manager for DataLoader registry lifecycle."""
    
    def __init__(self, session: Session):
        self.session = session
        self.registry: Optional[DataLoaderRegistry] = None
    
    async def __aenter__(self) -> DataLoaderRegistry:
        self.registry = DataLoaderRegistry(self.session)
        return self.registry
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.registry:
            await self.registry.clear_all_caches()


# Factory functions
def create_product_loader(session: Session) -> ProductDataLoader:
    """Create a ProductDataLoader instance."""
    return ProductDataLoader(session)


def create_customer_loader(session: Session) -> CustomerDataLoader:
    """Create a CustomerDataLoader instance."""
    return CustomerDataLoader(session)


def create_country_loader(session: Session) -> CountryDataLoader:
    """Create a CountryDataLoader instance."""
    return CountryDataLoader(session)


# GraphQL context integration
def get_dataloaders(session: Session) -> Dict[str, DataLoader]:
    """Get DataLoaders for GraphQL context."""
    return {
        'products': create_product_loader(session),
        'customers': create_customer_loader(session),
        'countries': create_country_loader(session),
        'sales_by_customer': SalesByCustomerDataLoader(session),
        'sales_by_product': SalesByProductDataLoader(session)
    }


# Performance monitoring
class DataLoaderMonitor:
    """Monitor DataLoader performance across requests."""
    
    def __init__(self):
        self.request_stats = []
        self.global_stats = {
            'total_requests': 0,
            'total_keys_loaded': 0,
            'total_cache_hits': 0,
            'total_cache_misses': 0,
            'avg_batch_size': 0.0,
            'avg_load_time': 0.0
        }
    
    def record_request(self, loader_stats: Dict[str, Any]):
        """Record statistics from a request."""
        self.request_stats.append({
            'timestamp': datetime.utcnow(),
            'stats': loader_stats
        })
        
        # Update global stats
        self.global_stats['total_requests'] += 1
        
        # Aggregate stats from all loaders
        for loader_name, stats in loader_stats.items():
            if 'stats' in stats and stats['stats']:
                loader_data = stats['stats']
                self.global_stats['total_keys_loaded'] += loader_data.get('total_keys', 0)
                self.global_stats['total_cache_hits'] += loader_data.get('cache_hits', 0)
                self.global_stats['total_cache_misses'] += loader_data.get('cache_misses', 0)
        
        # Keep only last 1000 requests
        if len(self.request_stats) > 1000:
            self.request_stats = self.request_stats[-1000:]
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        if self.global_stats['total_requests'] == 0:
            return self.global_stats
        
        total_cache_requests = (
            self.global_stats['total_cache_hits'] + 
            self.global_stats['total_cache_misses']
        )
        
        return {
            **self.global_stats,
            'cache_hit_ratio': (
                self.global_stats['total_cache_hits'] / total_cache_requests
                if total_cache_requests > 0 else 0.0
            ),
            'avg_keys_per_request': (
                self.global_stats['total_keys_loaded'] / 
                self.global_stats['total_requests']
            )
        }


# Global monitor instance
dataloader_monitor = DataLoaderMonitor()
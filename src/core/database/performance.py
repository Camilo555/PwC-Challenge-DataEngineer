"""
Database Performance Optimization

Comprehensive database performance monitoring, query optimization,
and maintenance utilities for the retail data warehouse.
"""

import time
import asyncio
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

from sqlalchemy import Engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

from core.logging import get_logger

logger = get_logger(__name__)


class QueryComplexity(str, Enum):
    """Query complexity levels."""
    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"


@dataclass
class QueryPerformanceMetric:
    """Performance metrics for a database query."""
    query_id: str
    sql: str
    execution_time_ms: float
    complexity: QueryComplexity
    affected_rows: int = 0
    execution_plan: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'query_id': self.query_id,
            'sql': self.sql[:200] + '...' if len(self.sql) > 200 else self.sql,
            'execution_time_ms': self.execution_time_ms,
            'complexity': self.complexity.value,
            'affected_rows': self.affected_rows,
            'timestamp': self.timestamp.isoformat(),
            'has_execution_plan': self.execution_plan is not None
        }


@dataclass
class TableStatistics:
    """Database table statistics."""
    table_name: str
    row_count: int
    size_mb: float
    index_count: int
    last_analyzed: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'table_name': self.table_name,
            'row_count': self.row_count,
            'size_mb': self.size_mb,
            'index_count': self.index_count,
            'last_analyzed': self.last_analyzed.isoformat() if self.last_analyzed else None
        }


class DatabasePerformanceOptimizer:
    """
    Advanced database performance optimizer with monitoring and maintenance capabilities.
    
    This class provides comprehensive performance optimization features:
    - Query performance monitoring
    - Automatic statistics updates
    - Index effectiveness analysis
    - Maintenance recommendations
    """
    
    def __init__(self, engine: Engine):
        self.engine = engine
        self.query_metrics: List[QueryPerformanceMetric] = []
        self.table_stats: Dict[str, TableStatistics] = {}
        self._last_maintenance = datetime.utcnow()
        
    async def monitor_query_performance(self, sql: str, query_id: Optional[str] = None) -> QueryPerformanceMetric:
        """
        Execute and monitor query performance.
        
        Args:
            sql: SQL query to execute
            query_id: Optional query identifier
            
        Returns:
            Performance metrics for the query
        """
        query_id = query_id or f"query_{int(time.time())}"
        
        start_time = time.time()
        affected_rows = 0
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                affected_rows = result.rowcount if hasattr(result, 'rowcount') else 0
                
                # Fetch results to ensure complete execution
                if result.returns_rows:
                    list(result)
                    
        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            raise
        
        execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        complexity = self._analyze_query_complexity(sql)
        
        metric = QueryPerformanceMetric(
            query_id=query_id,
            sql=sql,
            execution_time_ms=execution_time,
            complexity=complexity,
            affected_rows=affected_rows
        )
        
        self.query_metrics.append(metric)
        
        # Keep only last 1000 metrics to avoid memory growth
        if len(self.query_metrics) > 1000:
            self.query_metrics = self.query_metrics[-1000:]
        
        logger.debug(f"Query {query_id} executed in {execution_time:.2f}ms")
        
        return metric
    
    def _analyze_query_complexity(self, sql: str) -> QueryComplexity:
        """Analyze query complexity based on SQL structure."""
        sql_lower = sql.lower()
        
        # Count complexity indicators
        complexity_score = 0
        
        # JOIN operations
        join_count = sql_lower.count('join')
        complexity_score += join_count * 2
        
        # Subqueries
        subquery_count = sql_lower.count('select') - 1  # Subtract main SELECT
        complexity_score += subquery_count * 3
        
        # Aggregation functions
        agg_functions = ['sum(', 'count(', 'avg(', 'max(', 'min(', 'group by']
        agg_count = sum(sql_lower.count(func) for func in agg_functions)
        complexity_score += agg_count * 1
        
        # Window functions
        window_count = sql_lower.count('over(')
        complexity_score += window_count * 4
        
        # Sorting
        if 'order by' in sql_lower:
            complexity_score += 1
        
        # Classify complexity
        if complexity_score <= 2:
            return QueryComplexity.SIMPLE
        elif complexity_score <= 8:
            return QueryComplexity.MODERATE
        else:
            return QueryComplexity.COMPLEX
    
    async def collect_table_statistics(self) -> Dict[str, TableStatistics]:
        """Collect comprehensive table statistics."""
        logger.info("Collecting database table statistics...")
        
        # Define key tables to monitor
        key_tables = [
            'fact_sale', 'dim_customer', 'dim_product', 
            'dim_date', 'dim_country', 'dim_invoice'
        ]
        
        stats = {}
        
        for table_name in key_tables:
            try:
                table_stats = await self._get_table_statistics(table_name)
                if table_stats:
                    stats[table_name] = table_stats
                    self.table_stats[table_name] = table_stats
                    
            except Exception as e:
                logger.warning(f"Could not collect statistics for table {table_name}: {e}")
        
        logger.info(f"Collected statistics for {len(stats)} tables")
        return stats
    
    async def _get_table_statistics(self, table_name: str) -> Optional[TableStatistics]:
        """Get statistics for a single table."""
        try:
            with self.engine.connect() as conn:
                # Get row count
                row_count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
                result = conn.execute(text(row_count_query))
                row_count = result.scalar()
                
                # Get index count
                inspector = inspect(self.engine)
                indexes = inspector.get_indexes(table_name)
                index_count = len(indexes)
                
                # Estimate table size (simplified calculation)
                # This is a rough estimate - actual implementation would vary by database
                estimated_size_mb = (row_count * 0.001)  # Rough estimate
                
                return TableStatistics(
                    table_name=table_name,
                    row_count=row_count,
                    size_mb=estimated_size_mb,
                    index_count=index_count,
                    last_analyzed=datetime.utcnow()
                )
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get statistics for table {table_name}: {e}")
            return None
    
    async def run_maintenance_tasks(self) -> Dict[str, Any]:
        """Run comprehensive database maintenance tasks."""
        logger.info("Running database maintenance tasks...")
        
        maintenance_results = {
            'started_at': datetime.utcnow().isoformat(),
            'tasks_completed': [],
            'tasks_failed': [],
            'performance_improvement': {}
        }
        
        try:
            # Task 1: Update table statistics
            await self._update_table_statistics(maintenance_results)
            
            # Task 2: Analyze query performance
            await self._analyze_query_performance_trends(maintenance_results)
            
            # Task 3: Check index effectiveness
            await self._check_index_effectiveness(maintenance_results)
            
            # Task 4: Run database vacuum/optimize
            await self._optimize_database_storage(maintenance_results)
            
            # Task 5: Generate performance recommendations
            await self._generate_recommendations(maintenance_results)
            
            self._last_maintenance = datetime.utcnow()
            maintenance_results['completed_at'] = self._last_maintenance.isoformat()
            
            logger.info(f"Maintenance completed: {len(maintenance_results['tasks_completed'])} tasks successful")
            
        except Exception as e:
            logger.error(f"Maintenance tasks failed: {e}")
            maintenance_results['tasks_failed'].append({
                'task': 'maintenance_execution',
                'error': str(e)
            })
        
        return maintenance_results
    
    async def _update_table_statistics(self, results: Dict[str, Any]) -> None:
        """Update database table statistics."""
        try:
            with self.engine.connect() as conn:
                # Run ANALYZE command for key tables
                analyze_commands = [
                    "ANALYZE fact_sale",
                    "ANALYZE dim_customer",
                    "ANALYZE dim_product", 
                    "ANALYZE dim_date",
                    "ANALYZE dim_country",
                    "ANALYZE dim_invoice"
                ]
                
                for cmd in analyze_commands:
                    try:
                        conn.execute(text(cmd))
                        logger.debug(f"Executed: {cmd}")
                    except SQLAlchemyError as e:
                        logger.warning(f"Failed to execute {cmd}: {e}")
                
                conn.commit()
            
            results['tasks_completed'].append('table_statistics_update')
            
        except Exception as e:
            results['tasks_failed'].append({
                'task': 'table_statistics_update',
                'error': str(e)
            })
    
    async def _analyze_query_performance_trends(self, results: Dict[str, Any]) -> None:
        """Analyze query performance trends."""
        try:
            if not self.query_metrics:
                return
            
            # Calculate performance trends
            recent_metrics = [m for m in self.query_metrics 
                            if m.timestamp > datetime.utcnow() - timedelta(hours=1)]
            
            if recent_metrics:
                avg_execution_time = sum(m.execution_time_ms for m in recent_metrics) / len(recent_metrics)
                slow_queries = [m for m in recent_metrics if m.execution_time_ms > 1000]  # > 1 second
                
                results['performance_improvement']['avg_execution_time_ms'] = avg_execution_time
                results['performance_improvement']['slow_queries_count'] = len(slow_queries)
                results['performance_improvement']['total_queries_analyzed'] = len(recent_metrics)
            
            results['tasks_completed'].append('query_performance_analysis')
            
        except Exception as e:
            results['tasks_failed'].append({
                'task': 'query_performance_analysis',
                'error': str(e)
            })
    
    async def _check_index_effectiveness(self, results: Dict[str, Any]) -> None:
        """Check database index effectiveness."""
        try:
            # This is a simplified check - real implementation would be database-specific
            with self.engine.connect() as conn:
                # Check for unused indexes (simplified)
                inspector = inspect(self.engine)
                
                index_info = {}
                for table_name in ['fact_sale', 'dim_customer', 'dim_product']:
                    try:
                        indexes = inspector.get_indexes(table_name)
                        index_info[table_name] = len(indexes)
                    except:
                        pass
                
                results['performance_improvement']['indexes_by_table'] = index_info
            
            results['tasks_completed'].append('index_effectiveness_check')
            
        except Exception as e:
            results['tasks_failed'].append({
                'task': 'index_effectiveness_check',
                'error': str(e)
            })
    
    async def _optimize_database_storage(self, results: Dict[str, Any]) -> None:
        """Optimize database storage."""
        try:
            with self.engine.connect() as conn:
                # Run VACUUM for SQLite or equivalent maintenance
                try:
                    conn.execute(text("VACUUM"))
                    logger.debug("Database vacuum completed")
                except SQLAlchemyError:
                    # VACUUM might not be supported in all databases
                    logger.debug("VACUUM not supported, skipping storage optimization")
                
                conn.commit()
            
            results['tasks_completed'].append('storage_optimization')
            
        except Exception as e:
            results['tasks_failed'].append({
                'task': 'storage_optimization',
                'error': str(e)
            })
    
    async def _generate_recommendations(self, results: Dict[str, Any]) -> None:
        """Generate performance optimization recommendations."""
        try:
            recommendations = []
            
            # Analyze slow queries
            if self.query_metrics:
                slow_queries = [m for m in self.query_metrics if m.execution_time_ms > 2000]
                if slow_queries:
                    recommendations.append(f"Found {len(slow_queries)} slow queries (>2s). Consider optimizing or adding indexes.")
            
            # Check table sizes
            large_tables = [name for name, stats in self.table_stats.items() 
                          if stats.row_count > 100000]
            if large_tables:
                recommendations.append(f"Large tables detected: {', '.join(large_tables)}. Consider partitioning or archiving.")
            
            # Index recommendations
            low_index_tables = [name for name, stats in self.table_stats.items() 
                              if stats.index_count < 3 and stats.row_count > 10000]
            if low_index_tables:
                recommendations.append(f"Tables with few indexes: {', '.join(low_index_tables)}. Consider adding performance indexes.")
            
            results['performance_improvement']['recommendations'] = recommendations
            results['tasks_completed'].append('recommendation_generation')
            
        except Exception as e:
            results['tasks_failed'].append({
                'task': 'recommendation_generation',
                'error': str(e)
            })
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary."""
        summary = {
            'query_metrics': {
                'total_queries': len(self.query_metrics),
                'avg_execution_time_ms': 0,
                'slow_queries_count': 0,
                'complex_queries_count': 0
            },
            'table_statistics': {name: stats.to_dict() for name, stats in self.table_stats.items()},
            'last_maintenance': self._last_maintenance.isoformat(),
            'recommendations': []
        }
        
        if self.query_metrics:
            summary['query_metrics']['avg_execution_time_ms'] = (
                sum(m.execution_time_ms for m in self.query_metrics) / len(self.query_metrics)
            )
            summary['query_metrics']['slow_queries_count'] = sum(
                1 for m in self.query_metrics if m.execution_time_ms > 1000
            )
            summary['query_metrics']['complex_queries_count'] = sum(
                1 for m in self.query_metrics if m.complexity == QueryComplexity.COMPLEX
            )
        
        return summary
    
    async def benchmark_queries(self) -> Dict[str, Any]:
        """Run benchmark queries to test database performance."""
        logger.info("Running database performance benchmarks...")
        
        benchmark_queries = [
            {
                'name': 'sales_by_date',
                'sql': '''
                    SELECT date_key, COUNT(*) as transaction_count, SUM(total_amount) as revenue
                    FROM fact_sale 
                    WHERE date_key >= 20240101 
                    GROUP BY date_key 
                    ORDER BY date_key
                ''',
                'expected_complexity': QueryComplexity.MODERATE
            },
            {
                'name': 'customer_analytics',
                'sql': '''
                    SELECT c.customer_segment, COUNT(*) as customers, AVG(c.lifetime_value) as avg_ltv
                    FROM dim_customer c
                    WHERE c.is_current = 1
                    GROUP BY c.customer_segment
                    ORDER BY avg_ltv DESC
                ''',
                'expected_complexity': QueryComplexity.MODERATE
            },
            {
                'name': 'product_performance',
                'sql': '''
                    SELECT p.category, COUNT(DISTINCT p.stock_code) as products, 
                           SUM(f.total_amount) as revenue
                    FROM dim_product p
                    JOIN fact_sale f ON p.product_key = f.product_key
                    WHERE p.is_current = 1
                    GROUP BY p.category
                    ORDER BY revenue DESC
                    LIMIT 10
                ''',
                'expected_complexity': QueryComplexity.COMPLEX
            }
        ]
        
        benchmark_results = []
        
        for query_info in benchmark_queries:
            try:
                metric = await self.monitor_query_performance(
                    query_info['sql'], 
                    f"benchmark_{query_info['name']}"
                )
                
                benchmark_results.append({
                    'name': query_info['name'],
                    'execution_time_ms': metric.execution_time_ms,
                    'complexity': metric.complexity.value,
                    'expected_complexity': query_info['expected_complexity'].value,
                    'performance_rating': self._rate_performance(metric.execution_time_ms)
                })
                
            except Exception as e:
                logger.error(f"Benchmark query {query_info['name']} failed: {e}")
                benchmark_results.append({
                    'name': query_info['name'],
                    'error': str(e)
                })
        
        return {
            'benchmark_results': benchmark_results,
            'overall_score': self._calculate_overall_score(benchmark_results),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _rate_performance(self, execution_time_ms: float) -> str:
        """Rate query performance based on execution time."""
        if execution_time_ms < 100:
            return "excellent"
        elif execution_time_ms < 500:
            return "good"
        elif execution_time_ms < 2000:
            return "acceptable"
        else:
            return "poor"
    
    def _calculate_overall_score(self, benchmark_results: List[Dict[str, Any]]) -> float:
        """Calculate overall performance score."""
        if not benchmark_results:
            return 0.0
        
        valid_results = [r for r in benchmark_results if 'error' not in r]
        if not valid_results:
            return 0.0
        
        # Score based on execution times (lower is better)
        total_score = 0
        for result in valid_results:
            time_ms = result.get('execution_time_ms', float('inf'))
            if time_ms < 100:
                total_score += 100
            elif time_ms < 500:
                total_score += 80
            elif time_ms < 2000:
                total_score += 60
            else:
                total_score += 20
        
        return total_score / len(valid_results)
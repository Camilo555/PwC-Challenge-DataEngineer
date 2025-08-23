"""
Intelligent Data Partitioning and Query Optimization System
Provides automatic partitioning strategies and query optimization for different data engines.
"""
import asyncio
import hashlib
import json
import math
import statistics
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import threading

import pandas as pd
import polars as pl
import numpy as np
from concurrent.futures import ThreadPoolExecutor

from core.config.unified_config import get_unified_config
from core.logging import get_logger


class PartitionStrategy(Enum):
    """Different partitioning strategies"""
    TIME_BASED = "time_based"
    HASH_BASED = "hash_based"
    RANGE_BASED = "range_based"
    LIST_BASED = "list_based"
    HYBRID = "hybrid"
    ADAPTIVE = "adaptive"


class QueryType(Enum):
    """Types of queries for optimization"""
    POINT_LOOKUP = "point_lookup"
    RANGE_SCAN = "range_scan"
    AGGREGATION = "aggregation"
    JOIN = "join"
    ANALYTICAL = "analytical"
    STREAMING = "streaming"


@dataclass
class PartitionMetadata:
    """Metadata for a data partition"""
    partition_id: str
    partition_key: str
    partition_value: Any
    row_count: int
    file_size_mb: float
    min_value: Any
    max_value: Any
    created_at: datetime
    last_accessed: datetime
    access_frequency: int
    compression_ratio: float
    data_skew: float


@dataclass
class QueryProfile:
    """Query performance profile"""
    query_id: str
    query_type: QueryType
    execution_time_ms: float
    data_scanned_mb: float
    partitions_accessed: List[str]
    filter_columns: List[str]
    group_by_columns: List[str]
    join_columns: List[str]
    timestamp: datetime
    result_size_mb: float


@dataclass
class PartitioningRecommendation:
    """Partitioning strategy recommendation"""
    strategy: PartitionStrategy
    partition_columns: List[str]
    partition_count: int
    expected_performance_gain: float
    storage_overhead: float
    maintenance_complexity: float
    confidence_score: float
    reasoning: str


class DataProfiler:
    """Analyzes data characteristics for optimal partitioning"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def analyze_data_characteristics(self, data: Union[pd.DataFrame, pl.DataFrame]) -> Dict[str, Any]:
        """Analyze data to understand partitioning opportunities"""
        if isinstance(data, pl.DataFrame):
            data = data.to_pandas()  # Convert for analysis
        
        analysis = {
            'row_count': len(data),
            'column_count': len(data.columns),
            'memory_usage_mb': data.memory_usage(deep=True).sum() / 1024 / 1024,
            'columns': {}
        }
        
        for column in data.columns:
            analysis['columns'][column] = self._analyze_column(data[column])
        
        return analysis
    
    def _analyze_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze individual column characteristics"""
        analysis = {
            'dtype': str(series.dtype),
            'null_count': series.isnull().sum(),
            'null_percentage': series.isnull().sum() / len(series) * 100,
            'unique_count': series.nunique(),
            'cardinality': series.nunique() / len(series),
            'is_temporal': self._is_temporal_column(series),
            'is_categorical': self._is_categorical_column(series),
            'data_skew': self._calculate_data_skew(series),
            'partition_suitability': 0.0
        }
        
        # Calculate partition suitability score
        analysis['partition_suitability'] = self._calculate_partition_suitability(series, analysis)
        
        if pd.api.types.is_numeric_dtype(series):
            analysis.update(self._analyze_numeric_column(series))
        elif pd.api.types.is_string_dtype(series):
            analysis.update(self._analyze_string_column(series))
        elif pd.api.types.is_datetime64_any_dtype(series):
            analysis.update(self._analyze_datetime_column(series))
        
        return analysis
    
    def _is_temporal_column(self, series: pd.Series) -> bool:
        """Check if column represents temporal data"""
        if pd.api.types.is_datetime64_any_dtype(series):
            return True
        
        # Check for date-like strings
        if pd.api.types.is_string_dtype(series):
            sample = series.dropna().head(100)
            try:
                pd.to_datetime(sample, errors='raise')
                return True
            except:
                pass
        
        return False
    
    def _is_categorical_column(self, series: pd.Series) -> bool:
        """Check if column is categorical"""
        if pd.api.types.is_categorical_dtype(series):
            return True
        
        # Low cardinality suggests categorical
        if series.nunique() / len(series) < 0.05 and series.nunique() < 100:
            return True
        
        return False
    
    def _calculate_data_skew(self, series: pd.Series) -> float:
        """Calculate data distribution skew"""
        if not pd.api.types.is_numeric_dtype(series):
            # For non-numeric, calculate frequency skew
            value_counts = series.value_counts()
            if len(value_counts) < 2:
                return 0.0
            
            frequencies = value_counts.values
            return np.std(frequencies) / np.mean(frequencies)
        
        try:
            from scipy import stats
            return abs(stats.skew(series.dropna()))
        except ImportError:
            # Fallback calculation
            clean_series = series.dropna()
            if len(clean_series) < 3:
                return 0.0
            
            mean_val = clean_series.mean()
            median_val = clean_series.median()
            std_val = clean_series.std()
            
            if std_val == 0:
                return 0.0
            
            return abs(3 * (mean_val - median_val) / std_val)
    
    def _calculate_partition_suitability(self, series: pd.Series, analysis: Dict[str, Any]) -> float:
        """Calculate how suitable a column is for partitioning"""
        score = 0.0
        
        # Cardinality score (sweet spot around 10-1000 unique values)
        cardinality = analysis['cardinality']
        unique_count = analysis['unique_count']
        
        if unique_count < 2:
            score += 0.0  # Not suitable for partitioning
        elif 10 <= unique_count <= 1000:
            score += 1.0  # Ideal range
        elif 2 <= unique_count < 10:
            score += 0.6  # Decent but may create hotspots
        elif 1000 < unique_count <= 10000:
            score += 0.4  # May create too many partitions
        else:
            score += 0.1  # Likely too many partitions
        
        # Temporal columns are excellent for partitioning
        if analysis['is_temporal']:
            score += 1.5
        
        # Categorical columns are good for partitioning
        if analysis['is_categorical']:
            score += 1.0
        
        # Low data skew is better for partitioning
        skew = analysis['data_skew']
        if skew < 0.5:
            score += 0.8
        elif skew < 1.0:
            score += 0.5
        elif skew < 2.0:
            score += 0.2
        
        # Penalize high null percentage
        null_percentage = analysis['null_percentage']
        if null_percentage < 5:
            score += 0.5
        elif null_percentage < 20:
            score += 0.2
        else:
            score -= 0.3
        
        return min(score, 5.0)  # Cap at 5.0
    
    def _analyze_numeric_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze numeric column specifics"""
        clean_series = series.dropna()
        if len(clean_series) == 0:
            return {}
        
        return {
            'min_value': clean_series.min(),
            'max_value': clean_series.max(),
            'mean_value': clean_series.mean(),
            'std_value': clean_series.std(),
            'range': clean_series.max() - clean_series.min(),
            'has_outliers': self._detect_outliers(clean_series)
        }
    
    def _analyze_string_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze string column specifics"""
        clean_series = series.dropna()
        if len(clean_series) == 0:
            return {}
        
        lengths = clean_series.str.len()
        value_counts = clean_series.value_counts()
        
        return {
            'avg_length': lengths.mean(),
            'max_length': lengths.max(),
            'min_length': lengths.min(),
            'most_common_values': value_counts.head(10).to_dict(),
            'has_patterns': self._detect_string_patterns(clean_series)
        }
    
    def _analyze_datetime_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze datetime column specifics"""
        clean_series = series.dropna()
        if len(clean_series) == 0:
            return {}
        
        return {
            'min_date': clean_series.min(),
            'max_date': clean_series.max(),
            'date_range_days': (clean_series.max() - clean_series.min()).days,
            'temporal_granularity': self._detect_temporal_granularity(clean_series),
            'seasonality_detected': self._detect_seasonality(clean_series)
        }
    
    def _detect_outliers(self, series: pd.Series) -> bool:
        """Detect if numeric series has outliers"""
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = (series < lower_bound) | (series > upper_bound)
        return outliers.sum() > 0
    
    def _detect_string_patterns(self, series: pd.Series) -> bool:
        """Detect if string series has patterns suitable for partitioning"""
        # Sample some values to check for patterns
        sample = series.head(1000)
        
        # Check for common prefixes (like country codes, categories)
        prefixes = sample.str[:3].value_counts()
        if len(prefixes) < len(sample) * 0.8:  # Many shared prefixes
            return True
        
        # Check for fixed-length patterns
        lengths = sample.str.len()
        if lengths.nunique() <= 3:  # Few distinct lengths
            return True
        
        return False
    
    def _detect_temporal_granularity(self, series: pd.Series) -> str:
        """Detect the granularity of temporal data"""
        if len(series) < 10:
            return "unknown"
        
        # Sort and calculate common intervals
        sorted_series = series.sort_values()
        intervals = sorted_series.diff().dropna()
        
        # Find most common interval
        interval_counts = intervals.value_counts()
        if len(interval_counts) == 0:
            return "unknown"
        
        most_common_interval = interval_counts.index[0]
        
        if most_common_interval <= timedelta(seconds=1):
            return "second"
        elif most_common_interval <= timedelta(minutes=1):
            return "minute"
        elif most_common_interval <= timedelta(hours=1):
            return "hour"
        elif most_common_interval <= timedelta(days=1):
            return "day"
        elif most_common_interval <= timedelta(weeks=1):
            return "week"
        elif most_common_interval <= timedelta(days=31):
            return "month"
        else:
            return "year"
    
    def _detect_seasonality(self, series: pd.Series) -> bool:
        """Detect if temporal series shows seasonality"""
        if len(series) < 100:
            return False
        
        # Simple seasonality detection based on day of week/month patterns
        if hasattr(series, 'dt'):
            day_of_week_var = series.dt.dayofweek.value_counts().var()
            day_of_month_var = series.dt.day.value_counts().var()
            
            # If there's significant variation, likely seasonal
            return day_of_week_var > 10 or day_of_month_var > 100
        
        return False


class PartitioningStrategist:
    """Determines optimal partitioning strategies"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.profiler = DataProfiler()
    
    def recommend_partitioning_strategy(
        self, 
        data: Union[pd.DataFrame, pl.DataFrame],
        query_patterns: List[QueryProfile] = None,
        storage_constraints: Dict[str, Any] = None
    ) -> PartitioningRecommendation:
        """Recommend optimal partitioning strategy"""
        
        # Analyze data characteristics
        data_analysis = self.profiler.analyze_data_characteristics(data)
        
        # Analyze query patterns if available
        query_analysis = self._analyze_query_patterns(query_patterns or [])
        
        # Generate recommendations
        recommendations = self._generate_strategy_recommendations(
            data_analysis, query_analysis, storage_constraints or {}
        )
        
        # Select best recommendation
        best_recommendation = max(recommendations, key=lambda x: x.confidence_score)
        
        self.logger.info(f"Recommended partitioning strategy: {best_recommendation.strategy.value}")
        return best_recommendation
    
    def _analyze_query_patterns(self, query_patterns: List[QueryProfile]) -> Dict[str, Any]:
        """Analyze historical query patterns"""
        if not query_patterns:
            return {'has_patterns': False}
        
        analysis = {
            'has_patterns': True,
            'total_queries': len(query_patterns),
            'avg_execution_time': statistics.mean([q.execution_time_ms for q in query_patterns]),
            'query_types': {},
            'filter_columns': {},
            'group_by_columns': {},
            'join_columns': {},
            'temporal_patterns': {}
        }
        
        # Analyze query type distribution
        for pattern in query_patterns:
            query_type = pattern.query_type.value
            if query_type not in analysis['query_types']:
                analysis['query_types'][query_type] = 0
            analysis['query_types'][query_type] += 1
            
            # Track commonly filtered columns
            for col in pattern.filter_columns:
                if col not in analysis['filter_columns']:
                    analysis['filter_columns'][col] = 0
                analysis['filter_columns'][col] += 1
            
            # Track commonly grouped columns
            for col in pattern.group_by_columns:
                if col not in analysis['group_by_columns']:
                    analysis['group_by_columns'][col] = 0
                analysis['group_by_columns'][col] += 1
        
        return analysis
    
    def _generate_strategy_recommendations(
        self, 
        data_analysis: Dict[str, Any],
        query_analysis: Dict[str, Any],
        storage_constraints: Dict[str, Any]
    ) -> List[PartitioningRecommendation]:
        """Generate multiple partitioning strategy recommendations"""
        
        recommendations = []
        
        # Time-based partitioning recommendation
        time_rec = self._evaluate_time_based_partitioning(data_analysis, query_analysis)
        if time_rec:
            recommendations.append(time_rec)
        
        # Hash-based partitioning recommendation
        hash_rec = self._evaluate_hash_based_partitioning(data_analysis, query_analysis)
        if hash_rec:
            recommendations.append(hash_rec)
        
        # Range-based partitioning recommendation
        range_rec = self._evaluate_range_based_partitioning(data_analysis, query_analysis)
        if range_rec:
            recommendations.append(range_rec)
        
        # Hybrid partitioning recommendation
        hybrid_rec = self._evaluate_hybrid_partitioning(data_analysis, query_analysis)
        if hybrid_rec:
            recommendations.append(hybrid_rec)
        
        return recommendations
    
    def _evaluate_time_based_partitioning(
        self, 
        data_analysis: Dict[str, Any],
        query_analysis: Dict[str, Any]
    ) -> Optional[PartitioningRecommendation]:
        """Evaluate time-based partitioning strategy"""
        
        # Find temporal columns
        temporal_columns = [
            col for col, info in data_analysis['columns'].items()
            if info['is_temporal']
        ]
        
        if not temporal_columns:
            return None
        
        # Select best temporal column
        best_temporal_col = max(
            temporal_columns, 
            key=lambda col: data_analysis['columns'][col]['partition_suitability']
        )
        
        col_info = data_analysis['columns'][best_temporal_col]
        
        # Calculate confidence based on temporal characteristics
        confidence = 0.0
        
        # High confidence if commonly filtered by time
        if query_analysis.get('has_patterns'):
            filter_frequency = query_analysis['filter_columns'].get(best_temporal_col, 0)
            confidence += min(filter_frequency / query_analysis['total_queries'], 0.5)
        
        # Add base confidence for temporal suitability
        confidence += min(col_info['partition_suitability'] / 5.0, 0.4)
        
        # Determine partition granularity
        if 'temporal_granularity' in col_info:
            granularity = col_info['temporal_granularity']
            if granularity in ['day', 'week']:
                confidence += 0.2
            elif granularity in ['hour', 'month']:
                confidence += 0.1
        
        # Estimate partition count based on date range
        partition_count = self._estimate_temporal_partitions(col_info)
        
        return PartitioningRecommendation(
            strategy=PartitionStrategy.TIME_BASED,
            partition_columns=[best_temporal_col],
            partition_count=partition_count,
            expected_performance_gain=confidence * 60,  # Up to 60% improvement
            storage_overhead=15.0,  # Typical overhead for time partitioning
            maintenance_complexity=2.0,  # Low complexity
            confidence_score=confidence,
            reasoning=f"Temporal column '{best_temporal_col}' shows good partitioning characteristics"
        )
    
    def _evaluate_hash_based_partitioning(
        self, 
        data_analysis: Dict[str, Any],
        query_analysis: Dict[str, Any]
    ) -> Optional[PartitioningRecommendation]:
        """Evaluate hash-based partitioning strategy"""
        
        # Find high-cardinality columns suitable for hashing
        suitable_columns = []
        
        for col, info in data_analysis['columns'].items():
            if (info['unique_count'] > 1000 and 
                info['cardinality'] > 0.8 and 
                info['data_skew'] < 1.0):
                suitable_columns.append((col, info))
        
        if not suitable_columns:
            return None
        
        # Select best column for hashing
        best_col, best_info = max(suitable_columns, key=lambda x: x[1]['partition_suitability'])
        
        # Calculate confidence
        confidence = 0.3  # Base confidence for hash partitioning
        
        if best_info['data_skew'] < 0.5:
            confidence += 0.2  # Bonus for low skew
        
        if query_analysis.get('has_patterns'):
            # Hash partitioning good for point lookups
            point_lookups = query_analysis['query_types'].get('point_lookup', 0)
            confidence += min(point_lookups / query_analysis['total_queries'], 0.3)
        
        # Optimal partition count for hash partitioning (power of 2)
        partition_count = 2 ** int(math.log2(min(64, max(4, data_analysis['row_count'] // 100000))))
        
        return PartitioningRecommendation(
            strategy=PartitionStrategy.HASH_BASED,
            partition_columns=[best_col],
            partition_count=partition_count,
            expected_performance_gain=confidence * 40,  # Up to 40% improvement
            storage_overhead=10.0,  # Lower overhead than time-based
            maintenance_complexity=1.0,  # Low maintenance
            confidence_score=confidence,
            reasoning=f"High-cardinality column '{best_col}' suitable for hash partitioning"
        )
    
    def _evaluate_range_based_partitioning(
        self, 
        data_analysis: Dict[str, Any],
        query_analysis: Dict[str, Any]
    ) -> Optional[PartitioningRecommendation]:
        """Evaluate range-based partitioning strategy"""
        
        # Find numeric columns suitable for range partitioning
        suitable_columns = []
        
        for col, info in data_analysis['columns'].items():
            if (info['dtype'] in ['int64', 'float64'] and
                info['partition_suitability'] > 2.0 and
                'range' in info and
                info['range'] > 0):
                suitable_columns.append((col, info))
        
        if not suitable_columns:
            return None
        
        best_col, best_info = max(suitable_columns, key=lambda x: x[1]['partition_suitability'])
        
        # Calculate confidence
        confidence = 0.25  # Base confidence
        
        if query_analysis.get('has_patterns'):
            # Range partitioning good for range scans
            range_scans = query_analysis['query_types'].get('range_scan', 0)
            confidence += min(range_scans / query_analysis['total_queries'], 0.4)
            
            # Check if this column is commonly used in filters
            filter_frequency = query_analysis['filter_columns'].get(best_col, 0)
            confidence += min(filter_frequency / query_analysis['total_queries'], 0.3)
        
        # Determine optimal partition count based on data distribution
        partition_count = min(20, max(4, int(best_info['unique_count'] ** 0.5)))
        
        return PartitioningRecommendation(
            strategy=PartitionStrategy.RANGE_BASED,
            partition_columns=[best_col],
            partition_count=partition_count,
            expected_performance_gain=confidence * 50,  # Up to 50% improvement
            storage_overhead=12.0,
            maintenance_complexity=3.0,  # Medium complexity due to range management
            confidence_score=confidence,
            reasoning=f"Numeric column '{best_col}' shows good range distribution"
        )
    
    def _evaluate_hybrid_partitioning(
        self, 
        data_analysis: Dict[str, Any],
        query_analysis: Dict[str, Any]
    ) -> Optional[PartitioningRecommendation]:
        """Evaluate hybrid partitioning strategy (multiple columns)"""
        
        # Find top 2 columns for hybrid partitioning
        column_scores = [
            (col, info['partition_suitability']) 
            for col, info in data_analysis['columns'].items()
            if info['partition_suitability'] > 1.5
        ]
        
        column_scores.sort(key=lambda x: x[1], reverse=True)
        
        if len(column_scores) < 2:
            return None
        
        primary_col, primary_score = column_scores[0]
        secondary_col, secondary_score = column_scores[1]
        
        # Calculate confidence
        confidence = min((primary_score + secondary_score) / 10.0, 0.8)
        
        if query_analysis.get('has_patterns'):
            # Hybrid works well when both columns are frequently used
            primary_filter_freq = query_analysis['filter_columns'].get(primary_col, 0)
            secondary_filter_freq = query_analysis['filter_columns'].get(secondary_col, 0)
            
            total_queries = query_analysis['total_queries']
            both_used_freq = min(primary_filter_freq, secondary_filter_freq) / total_queries
            confidence += both_used_freq * 0.3
        
        # Estimate partition count (multiplicative)
        primary_partitions = min(10, max(3, int(data_analysis['columns'][primary_col]['unique_count'] ** 0.4)))
        secondary_partitions = min(5, max(2, int(data_analysis['columns'][secondary_col]['unique_count'] ** 0.3)))
        
        return PartitioningRecommendation(
            strategy=PartitionStrategy.HYBRID,
            partition_columns=[primary_col, secondary_col],
            partition_count=primary_partitions * secondary_partitions,
            expected_performance_gain=confidence * 70,  # Up to 70% improvement
            storage_overhead=25.0,  # Higher overhead due to multiple levels
            maintenance_complexity=4.0,  # Higher complexity
            confidence_score=confidence,
            reasoning=f"Hybrid partitioning on '{primary_col}' and '{secondary_col}' for complex queries"
        )
    
    def _estimate_temporal_partitions(self, col_info: Dict[str, Any]) -> int:
        """Estimate number of partitions for temporal column"""
        if 'date_range_days' not in col_info:
            return 12  # Default monthly partitioning
        
        days = col_info['date_range_days']
        
        if days <= 7:
            return max(1, days)  # Daily
        elif days <= 60:
            return max(4, days // 7)  # Weekly
        elif days <= 730:  # 2 years
            return max(12, days // 30)  # Monthly
        elif days <= 3650:  # 10 years
            return max(24, days // 91)  # Quarterly
        else:
            return max(10, days // 365)  # Yearly


class QueryOptimizer:
    """Optimizes queries based on partitioning strategy"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
    
    def optimize_query_plan(
        self, 
        query: Dict[str, Any],
        partition_metadata: List[PartitionMetadata],
        strategy: PartitioningRecommendation
    ) -> Dict[str, Any]:
        """Optimize query execution plan based on partitioning"""
        
        optimizations = {
            'partition_pruning': [],
            'predicate_pushdown': [],
            'join_optimizations': [],
            'aggregation_optimizations': [],
            'estimated_speedup': 1.0
        }
        
        # Partition pruning optimization
        pruning = self._optimize_partition_pruning(query, partition_metadata, strategy)
        optimizations['partition_pruning'] = pruning
        
        # Predicate pushdown
        pushdown = self._optimize_predicate_pushdown(query, strategy)
        optimizations['predicate_pushdown'] = pushdown
        
        # Join optimizations
        join_opts = self._optimize_joins(query, strategy)
        optimizations['join_optimizations'] = join_opts
        
        # Aggregation optimizations
        agg_opts = self._optimize_aggregations(query, strategy)
        optimizations['aggregation_optimizations'] = agg_opts
        
        # Calculate estimated speedup
        optimizations['estimated_speedup'] = self._calculate_estimated_speedup(optimizations)
        
        return optimizations
    
    def _optimize_partition_pruning(
        self, 
        query: Dict[str, Any],
        partition_metadata: List[PartitionMetadata],
        strategy: PartitioningRecommendation
    ) -> List[Dict[str, Any]]:
        """Optimize partition pruning"""
        
        pruning_rules = []
        
        if not query.get('filters'):
            return pruning_rules
        
        for filter_col, filter_value in query['filters'].items():
            if filter_col in strategy.partition_columns:
                # Find relevant partitions
                relevant_partitions = self._find_relevant_partitions(
                    filter_col, filter_value, partition_metadata
                )
                
                pruning_rules.append({
                    'column': filter_col,
                    'filter_value': filter_value,
                    'partitions_to_scan': [p.partition_id for p in relevant_partitions],
                    'partitions_pruned': len(partition_metadata) - len(relevant_partitions),
                    'data_reduction_percent': (1 - len(relevant_partitions) / len(partition_metadata)) * 100
                })
        
        return pruning_rules
    
    def _optimize_predicate_pushdown(
        self, 
        query: Dict[str, Any],
        strategy: PartitioningRecommendation
    ) -> List[Dict[str, Any]]:
        """Optimize predicate pushdown"""
        
        pushdown_rules = []
        
        if not query.get('filters'):
            return pushdown_rules
        
        for filter_col, filter_value in query['filters'].items():
            # Always push down filters on partition columns
            if filter_col in strategy.partition_columns:
                pushdown_rules.append({
                    'column': filter_col,
                    'operation': 'push_to_storage',
                    'expected_benefit': 'high'
                })
            # Push down selective filters
            elif self._is_selective_filter(filter_col, filter_value):
                pushdown_rules.append({
                    'column': filter_col,
                    'operation': 'push_to_storage',
                    'expected_benefit': 'medium'
                })
        
        return pushdown_rules
    
    def _optimize_joins(
        self, 
        query: Dict[str, Any],
        strategy: PartitioningRecommendation
    ) -> List[Dict[str, Any]]:
        """Optimize join operations"""
        
        join_optimizations = []
        
        if not query.get('joins'):
            return join_optimizations
        
        for join in query['joins']:
            # If join key matches partition key, use partition-wise join
            if join['key'] in strategy.partition_columns:
                join_optimizations.append({
                    'join_key': join['key'],
                    'optimization': 'partition_wise_join',
                    'expected_benefit': 'high'
                })
            # Use broadcast join for small tables
            elif join.get('right_table_size_mb', 0) < 100:
                join_optimizations.append({
                    'join_key': join['key'],
                    'optimization': 'broadcast_join',
                    'expected_benefit': 'medium'
                })
        
        return join_optimizations
    
    def _optimize_aggregations(
        self, 
        query: Dict[str, Any],
        strategy: PartitioningRecommendation
    ) -> List[Dict[str, Any]]:
        """Optimize aggregation operations"""
        
        agg_optimizations = []
        
        if not query.get('group_by'):
            return agg_optimizations
        
        group_by_cols = query['group_by']
        
        # If grouping by partition column, use partition-wise aggregation
        partition_cols_in_group_by = set(group_by_cols) & set(strategy.partition_columns)
        
        if partition_cols_in_group_by:
            agg_optimizations.append({
                'columns': list(partition_cols_in_group_by),
                'optimization': 'partition_wise_aggregation',
                'expected_benefit': 'high'
            })
        
        # Pre-aggregation for frequently grouped columns
        if len(group_by_cols) > 1:
            agg_optimizations.append({
                'columns': group_by_cols,
                'optimization': 'pre_aggregation',
                'expected_benefit': 'medium'
            })
        
        return agg_optimizations
    
    def _find_relevant_partitions(
        self, 
        filter_col: str,
        filter_value: Any,
        partition_metadata: List[PartitionMetadata]
    ) -> List[PartitionMetadata]:
        """Find partitions that need to be scanned for a filter"""
        
        relevant = []
        
        for partition in partition_metadata:
            if partition.partition_key == filter_col:
                # For exact match filters
                if isinstance(filter_value, (str, int, float)):
                    if partition.min_value <= filter_value <= partition.max_value:
                        relevant.append(partition)
                # For range filters
                elif isinstance(filter_value, dict) and 'min' in filter_value:
                    if (partition.max_value >= filter_value.get('min', float('-inf')) and
                        partition.min_value <= filter_value.get('max', float('inf'))):
                        relevant.append(partition)
        
        return relevant
    
    def _is_selective_filter(self, filter_col: str, filter_value: Any) -> bool:
        """Check if a filter is selective (reduces data significantly)"""
        # Simple heuristic - in practice, would use statistics
        if isinstance(filter_value, str):
            return len(filter_value) > 2  # Non-trivial string filters
        elif isinstance(filter_value, (int, float)):
            return True  # Numeric filters are usually selective
        elif isinstance(filter_value, dict):
            return True  # Range filters are selective
        
        return False
    
    def _calculate_estimated_speedup(self, optimizations: Dict[str, Any]) -> float:
        """Calculate estimated query speedup from optimizations"""
        
        speedup = 1.0
        
        # Partition pruning speedup
        for pruning in optimizations['partition_pruning']:
            data_reduction = pruning['data_reduction_percent'] / 100
            speedup *= 1 + (data_reduction * 2)  # 2x speedup per % of data pruned
        
        # Predicate pushdown speedup
        high_benefit_pushdowns = len([p for p in optimizations['predicate_pushdown'] if p['expected_benefit'] == 'high'])
        medium_benefit_pushdowns = len([p for p in optimizations['predicate_pushdown'] if p['expected_benefit'] == 'medium'])
        
        speedup *= 1 + (high_benefit_pushdowns * 0.3) + (medium_benefit_pushdowns * 0.15)
        
        # Join optimization speedup
        partition_joins = len([j for j in optimizations['join_optimizations'] if j['optimization'] == 'partition_wise_join'])
        broadcast_joins = len([j for j in optimizations['join_optimizations'] if j['optimization'] == 'broadcast_join'])
        
        speedup *= 1 + (partition_joins * 0.4) + (broadcast_joins * 0.2)
        
        # Aggregation optimization speedup
        partition_aggs = len([a for a in optimizations['aggregation_optimizations'] if a['optimization'] == 'partition_wise_aggregation'])
        pre_aggs = len([a for a in optimizations['aggregation_optimizations'] if a['optimization'] == 'pre_aggregation'])
        
        speedup *= 1 + (partition_aggs * 0.5) + (pre_aggs * 0.25)
        
        return min(speedup, 10.0)  # Cap at 10x speedup


# Global instances
_partitioning_strategist: Optional[PartitioningStrategist] = None
_query_optimizer: Optional[QueryOptimizer] = None


def get_partitioning_strategist() -> PartitioningStrategist:
    """Get global partitioning strategist instance"""
    global _partitioning_strategist
    if _partitioning_strategist is None:
        _partitioning_strategist = PartitioningStrategist()
    return _partitioning_strategist


def get_query_optimizer() -> QueryOptimizer:
    """Get global query optimizer instance"""
    global _query_optimizer
    if _query_optimizer is None:
        _query_optimizer = QueryOptimizer()
    return _query_optimizer


# Decorator for automatic query optimization
def optimize_query(partition_strategy: PartitioningRecommendation = None):
    """Decorator for automatic query optimization"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            optimizer = get_query_optimizer()
            
            # Extract query information from function arguments
            query_info = {
                'function': func.__name__,
                'args': args,
                'kwargs': kwargs
            }
            
            # Apply optimizations if strategy provided
            if partition_strategy:
                optimizations = optimizer.optimize_query_plan(
                    query_info, [], partition_strategy
                )
                
                # Log optimization suggestions
                logger = get_logger(__name__)
                logger.info(f"Query optimizations for {func.__name__}: {optimizations}")
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator
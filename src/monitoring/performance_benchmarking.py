"""
Automated Performance Benchmarking and Regression Detection System
Provides continuous performance monitoring, automated benchmarking, and regression detection.
"""
import asyncio
import json
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, Tuple
import threading
import uuid

import pandas as pd
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from etl.framework.engine_strategy import DataFrameEngineFactory, EngineType


class BenchmarkType(Enum):
    """Types of benchmarks"""
    ETL_PROCESSING = "etl_processing"
    DATABASE_QUERY = "database_query"
    API_RESPONSE = "api_response"
    DATA_VALIDATION = "data_validation"
    FILE_IO = "file_io"
    MEMORY_USAGE = "memory_usage"
    NETWORK_IO = "network_io"


class PerformanceStatus(Enum):
    """Performance status levels"""
    OPTIMAL = "optimal"
    ACCEPTABLE = "acceptable"
    DEGRADED = "degraded"
    CRITICAL = "critical"


@dataclass
class BenchmarkResult:
    """Individual benchmark execution result"""
    id: str
    benchmark_name: str
    benchmark_type: BenchmarkType
    execution_time_ms: float
    memory_usage_mb: float
    cpu_usage_percent: float
    throughput_ops_per_sec: float
    success: bool
    error_message: Optional[str]
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PerformanceBaseline:
    """Performance baseline for comparison"""
    benchmark_name: str
    avg_execution_time_ms: float
    p95_execution_time_ms: float
    p99_execution_time_ms: float
    avg_memory_usage_mb: float
    max_memory_usage_mb: float
    avg_throughput_ops_per_sec: float
    sample_size: int
    created_at: datetime
    last_updated: datetime


@dataclass
class RegressionAlert:
    """Performance regression alert"""
    id: str
    benchmark_name: str
    metric_name: str
    current_value: float
    baseline_value: float
    degradation_percent: float
    severity: PerformanceStatus
    detected_at: datetime
    description: str


class DataGenerator:
    """Generates test data for benchmarking"""
    
    @staticmethod
    def generate_sales_data(num_records: int) -> pd.DataFrame:
        """Generate synthetic sales data for benchmarking"""
        import random
        from datetime import datetime, timedelta
        
        start_date = datetime.now() - timedelta(days=365)
        
        data = {
            'InvoiceNo': [f'INV-{i:06d}' for i in range(num_records)],
            'StockCode': [f'ITEM-{random.randint(1, 1000):04d}' for _ in range(num_records)],
            'Description': [f'Product {random.randint(1, 100)}' for _ in range(num_records)],
            'Quantity': [random.randint(1, 50) for _ in range(num_records)],
            'InvoiceDate': [start_date + timedelta(days=random.randint(0, 365)) for _ in range(num_records)],
            'UnitPrice': [round(random.uniform(1.0, 500.0), 2) for _ in range(num_records)],
            'CustomerID': [f'CUST-{random.randint(1, 1000):04d}' for _ in range(num_records)],
            'Country': [random.choice(['UK', 'Germany', 'France', 'Spain', 'Italy']) for _ in range(num_records)]
        }
        
        return pd.DataFrame(data)
    
    @staticmethod
    def generate_large_dataset(size_mb: int) -> pd.DataFrame:
        """Generate dataset of specific size in MB"""
        # Estimate records needed for target size
        # Rough estimate: ~200 bytes per record for sales data
        estimated_records = (size_mb * 1024 * 1024) // 200
        return DataGenerator.generate_sales_data(estimated_records)


class BenchmarkRunner:
    """Executes performance benchmarks"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.data_generator = DataGenerator()
        
    def benchmark_pandas_processing(self, data_size_mb: int) -> BenchmarkResult:
        """Benchmark Pandas data processing"""
        start_time = time.perf_counter()
        start_memory = self._get_memory_usage()
        
        try:
            # Generate test data
            df = self.data_generator.generate_large_dataset(data_size_mb)
            
            # Perform typical ETL operations
            df['TotalAmount'] = df['Quantity'] * df['UnitPrice']
            df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
            df['Year'] = df['InvoiceDate'].dt.year
            df['Month'] = df['InvoiceDate'].dt.month
            
            # Aggregations
            monthly_sales = df.groupby(['Year', 'Month']).agg({
                'TotalAmount': ['sum', 'mean', 'count'],
                'CustomerID': 'nunique'
            })
            
            customer_stats = df.groupby('CustomerID').agg({
                'TotalAmount': 'sum',
                'InvoiceNo': 'nunique'
            }).sort_values('TotalAmount', ascending=False)
            
            # Filter operations
            high_value_sales = df[df['TotalAmount'] > df['TotalAmount'].quantile(0.9)]
            
            end_time = time.perf_counter()
            end_memory = self._get_memory_usage()
            
            execution_time = (end_time - start_time) * 1000
            memory_delta = end_memory - start_memory
            throughput = len(df) / ((end_time - start_time) or 0.001)  # records per second
            
            return BenchmarkResult(
                id=str(uuid.uuid4()),
                benchmark_name="pandas_etl_processing",
                benchmark_type=BenchmarkType.ETL_PROCESSING,
                execution_time_ms=execution_time,
                memory_usage_mb=memory_delta,
                cpu_usage_percent=0.0,  # Would need psutil for accurate CPU measurement
                throughput_ops_per_sec=throughput,
                success=True,
                error_message=None,
                timestamp=datetime.now(),
                metadata={
                    'data_size_mb': data_size_mb,
                    'records_processed': len(df),
                    'aggregations_performed': 2,
                    'engine': 'pandas'
                }
            )
            
        except Exception as e:
            end_time = time.perf_counter()
            return BenchmarkResult(
                id=str(uuid.uuid4()),
                benchmark_name="pandas_etl_processing",
                benchmark_type=BenchmarkType.ETL_PROCESSING,
                execution_time_ms=(end_time - start_time) * 1000,
                memory_usage_mb=0,
                cpu_usage_percent=0,
                throughput_ops_per_sec=0,
                success=False,
                error_message=str(e),
                timestamp=datetime.now(),
                metadata={'data_size_mb': data_size_mb, 'engine': 'pandas'}
            )
    
    def benchmark_polars_processing(self, data_size_mb: int) -> BenchmarkResult:
        """Benchmark Polars data processing"""
        start_time = time.perf_counter()
        start_memory = self._get_memory_usage()
        
        try:
            # Generate test data and convert to Polars
            df_pandas = self.data_generator.generate_large_dataset(data_size_mb)
            df = pl.from_pandas(df_pandas)
            
            # Perform typical ETL operations with Polars
            df = df.with_columns([
                (pl.col('Quantity') * pl.col('UnitPrice')).alias('TotalAmount')
            ])
            
            df = df.with_columns([
                pl.col('InvoiceDate').str.to_datetime().alias('InvoiceDate')
            ])
            
            df = df.with_columns([
                pl.col('InvoiceDate').dt.year().alias('Year'),
                pl.col('InvoiceDate').dt.month().alias('Month')
            ])
            
            # Aggregations
            monthly_sales = df.group_by(['Year', 'Month']).agg([
                pl.col('TotalAmount').sum().alias('total_sales'),
                pl.col('TotalAmount').mean().alias('avg_sales'),
                pl.col('TotalAmount').count().alias('transaction_count'),
                pl.col('CustomerID').n_unique().alias('unique_customers')
            ])
            
            customer_stats = df.group_by('CustomerID').agg([
                pl.col('TotalAmount').sum().alias('total_spent'),
                pl.col('InvoiceNo').n_unique().alias('unique_orders')
            ]).sort('total_spent', descending=True)
            
            # Filter operations
            total_amount_q90 = df['TotalAmount'].quantile(0.9)
            high_value_sales = df.filter(pl.col('TotalAmount') > total_amount_q90)
            
            end_time = time.perf_counter()
            end_memory = self._get_memory_usage()
            
            execution_time = (end_time - start_time) * 1000
            memory_delta = end_memory - start_memory
            throughput = len(df_pandas) / ((end_time - start_time) or 0.001)
            
            return BenchmarkResult(
                id=str(uuid.uuid4()),
                benchmark_name="polars_etl_processing",
                benchmark_type=BenchmarkType.ETL_PROCESSING,
                execution_time_ms=execution_time,
                memory_usage_mb=memory_delta,
                cpu_usage_percent=0.0,
                throughput_ops_per_sec=throughput,
                success=True,
                error_message=None,
                timestamp=datetime.now(),
                metadata={
                    'data_size_mb': data_size_mb,
                    'records_processed': len(df_pandas),
                    'aggregations_performed': 2,
                    'engine': 'polars'
                }
            )
            
        except Exception as e:
            end_time = time.perf_counter()
            return BenchmarkResult(
                id=str(uuid.uuid4()),
                benchmark_name="polars_etl_processing",
                benchmark_type=BenchmarkType.ETL_PROCESSING,
                execution_time_ms=(end_time - start_time) * 1000,
                memory_usage_mb=0,
                cpu_usage_percent=0,
                throughput_ops_per_sec=0,
                success=False,
                error_message=str(e),
                timestamp=datetime.now(),
                metadata={'data_size_mb': data_size_mb, 'engine': 'polars'}
            )
    
    def benchmark_file_io(self, data_size_mb: int) -> BenchmarkResult:
        """Benchmark file I/O operations"""
        import tempfile
        import os
        
        start_time = time.perf_counter()
        start_memory = self._get_memory_usage()
        
        try:
            # Generate test data
            df = self.data_generator.generate_large_dataset(data_size_mb)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Benchmark CSV write
                csv_path = temp_path / "test_data.csv"
                csv_write_start = time.perf_counter()
                df.to_csv(csv_path, index=False)
                csv_write_time = time.perf_counter() - csv_write_start
                
                # Benchmark CSV read
                csv_read_start = time.perf_counter()
                df_read_csv = pd.read_csv(csv_path)
                csv_read_time = time.perf_counter() - csv_read_start
                
                # Benchmark Parquet write
                parquet_path = temp_path / "test_data.parquet"
                parquet_write_start = time.perf_counter()
                df.to_parquet(parquet_path, index=False)
                parquet_write_time = time.perf_counter() - parquet_write_start
                
                # Benchmark Parquet read
                parquet_read_start = time.perf_counter()
                df_read_parquet = pd.read_parquet(parquet_path)
                parquet_read_time = time.perf_counter() - parquet_read_start
                
                # Get file sizes
                csv_size = csv_path.stat().st_size / 1024 / 1024  # MB
                parquet_size = parquet_path.stat().st_size / 1024 / 1024  # MB
            
            end_time = time.perf_counter()
            end_memory = self._get_memory_usage()
            
            execution_time = (end_time - start_time) * 1000
            memory_delta = end_memory - start_memory
            total_records = len(df) * 4  # read + write for both formats
            throughput = total_records / ((end_time - start_time) or 0.001)
            
            return BenchmarkResult(
                id=str(uuid.uuid4()),
                benchmark_name="file_io_operations",
                benchmark_type=BenchmarkType.FILE_IO,
                execution_time_ms=execution_time,
                memory_usage_mb=memory_delta,
                cpu_usage_percent=0.0,
                throughput_ops_per_sec=throughput,
                success=True,
                error_message=None,
                timestamp=datetime.now(),
                metadata={
                    'data_size_mb': data_size_mb,
                    'records_processed': len(df),
                    'csv_write_time_ms': csv_write_time * 1000,
                    'csv_read_time_ms': csv_read_time * 1000,
                    'parquet_write_time_ms': parquet_write_time * 1000,
                    'parquet_read_time_ms': parquet_read_time * 1000,
                    'csv_size_mb': csv_size,
                    'parquet_size_mb': parquet_size,
                    'compression_ratio': csv_size / parquet_size if parquet_size > 0 else 0
                }
            )
            
        except Exception as e:
            end_time = time.perf_counter()
            return BenchmarkResult(
                id=str(uuid.uuid4()),
                benchmark_name="file_io_operations",
                benchmark_type=BenchmarkType.FILE_IO,
                execution_time_ms=(end_time - start_time) * 1000,
                memory_usage_mb=0,
                cpu_usage_percent=0,
                throughput_ops_per_sec=0,
                success=False,
                error_message=str(e),
                timestamp=datetime.now(),
                metadata={'data_size_mb': data_size_mb}
            )
    
    def benchmark_data_validation(self, num_records: int) -> BenchmarkResult:
        """Benchmark data validation operations"""
        start_time = time.perf_counter()
        start_memory = self._get_memory_usage()
        
        try:
            from core.validation import BusinessRuleValidators, DataQualityValidators
            
            # Generate test data with some invalid records
            df = self.data_generator.generate_sales_data(num_records)
            
            # Introduce some invalid data
            df.loc[::100, 'Quantity'] = -1  # Invalid negative quantities
            df.loc[::150, 'UnitPrice'] = 0   # Invalid zero prices
            df.loc[::200, 'InvoiceNo'] = None  # Missing invoice numbers
            
            validation_results = []
            
            # Validate each record
            for _, row in df.iterrows():
                row_dict = row.to_dict()
                
                # Business rule validations
                try:
                    BusinessRuleValidators.validate_invoice_number(row_dict.get('InvoiceNo'))
                    BusinessRuleValidators.validate_quantity(row_dict.get('Quantity'))
                    BusinessRuleValidators.validate_unit_price(row_dict.get('UnitPrice'))
                    validation_results.append({'valid': True})
                except Exception as e:
                    validation_results.append({'valid': False, 'error': str(e)})
                
                # Data quality checks
                required_fields = ['InvoiceNo', 'StockCode', 'Quantity', 'UnitPrice']
                completeness = DataQualityValidators.check_completeness(row_dict, required_fields)
                
                ranges = {
                    'Quantity': {'min': 1, 'max': 10000},
                    'UnitPrice': {'min': 0.01, 'max': 100000}
                }
                range_check = DataQualityValidators.check_value_ranges(row_dict, ranges)
            
            end_time = time.perf_counter()
            end_memory = self._get_memory_usage()
            
            execution_time = (end_time - start_time) * 1000
            memory_delta = end_memory - start_memory
            throughput = num_records / ((end_time - start_time) or 0.001)
            
            valid_records = sum(1 for r in validation_results if r['valid'])
            
            return BenchmarkResult(
                id=str(uuid.uuid4()),
                benchmark_name="data_validation_operations",
                benchmark_type=BenchmarkType.DATA_VALIDATION,
                execution_time_ms=execution_time,
                memory_usage_mb=memory_delta,
                cpu_usage_percent=0.0,
                throughput_ops_per_sec=throughput,
                success=True,
                error_message=None,
                timestamp=datetime.now(),
                metadata={
                    'records_validated': num_records,
                    'valid_records': valid_records,
                    'invalid_records': num_records - valid_records,
                    'validation_accuracy': valid_records / num_records
                }
            )
            
        except Exception as e:
            end_time = time.perf_counter()
            return BenchmarkResult(
                id=str(uuid.uuid4()),
                benchmark_name="data_validation_operations",
                benchmark_type=BenchmarkType.DATA_VALIDATION,
                execution_time_ms=(end_time - start_time) * 1000,
                memory_usage_mb=0,
                cpu_usage_percent=0,
                throughput_ops_per_sec=0,
                success=False,
                error_message=str(e),
                timestamp=datetime.now(),
                metadata={'records_validated': num_records}
            )
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            return 0.0


class PerformanceRegressionDetector:
    """Detects performance regressions by comparing against baselines"""
    
    def __init__(self, baseline_storage_path: Optional[Path] = None):
        self.baseline_storage_path = baseline_storage_path or Path("performance_baselines.json")
        self.baselines: Dict[str, PerformanceBaseline] = {}
        self.logger = get_logger(__name__)
        self.lock = threading.Lock()
        
        # Load existing baselines
        self._load_baselines()
    
    def _load_baselines(self):
        """Load performance baselines from storage"""
        try:
            if self.baseline_storage_path.exists():
                with open(self.baseline_storage_path, 'r') as f:
                    data = json.load(f)
                    
                for name, baseline_data in data.items():
                    self.baselines[name] = PerformanceBaseline(
                        benchmark_name=baseline_data['benchmark_name'],
                        avg_execution_time_ms=baseline_data['avg_execution_time_ms'],
                        p95_execution_time_ms=baseline_data['p95_execution_time_ms'],
                        p99_execution_time_ms=baseline_data['p99_execution_time_ms'],
                        avg_memory_usage_mb=baseline_data['avg_memory_usage_mb'],
                        max_memory_usage_mb=baseline_data['max_memory_usage_mb'],
                        avg_throughput_ops_per_sec=baseline_data['avg_throughput_ops_per_sec'],
                        sample_size=baseline_data['sample_size'],
                        created_at=datetime.fromisoformat(baseline_data['created_at']),
                        last_updated=datetime.fromisoformat(baseline_data['last_updated'])
                    )
                    
                self.logger.info(f"Loaded {len(self.baselines)} performance baselines")
        except Exception as e:
            self.logger.warning(f"Failed to load baselines: {e}")
    
    def _save_baselines(self):
        """Save performance baselines to storage"""
        try:
            data = {}
            for name, baseline in self.baselines.items():
                data[name] = {
                    'benchmark_name': baseline.benchmark_name,
                    'avg_execution_time_ms': baseline.avg_execution_time_ms,
                    'p95_execution_time_ms': baseline.p95_execution_time_ms,
                    'p99_execution_time_ms': baseline.p99_execution_time_ms,
                    'avg_memory_usage_mb': baseline.avg_memory_usage_mb,
                    'max_memory_usage_mb': baseline.max_memory_usage_mb,
                    'avg_throughput_ops_per_sec': baseline.avg_throughput_ops_per_sec,
                    'sample_size': baseline.sample_size,
                    'created_at': baseline.created_at.isoformat(),
                    'last_updated': baseline.last_updated.isoformat()
                }
            
            with open(self.baseline_storage_path, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            self.logger.error(f"Failed to save baselines: {e}")
    
    def update_baseline(self, results: List[BenchmarkResult]):
        """Update performance baseline with new results"""
        if not results:
            return
        
        benchmark_name = results[0].benchmark_name
        successful_results = [r for r in results if r.success]
        
        if not successful_results:
            self.logger.warning(f"No successful results for {benchmark_name}")
            return
        
        # Calculate statistics
        execution_times = [r.execution_time_ms for r in successful_results]
        memory_usages = [r.memory_usage_mb for r in successful_results]
        throughputs = [r.throughput_ops_per_sec for r in successful_results]
        
        baseline = PerformanceBaseline(
            benchmark_name=benchmark_name,
            avg_execution_time_ms=statistics.mean(execution_times),
            p95_execution_time_ms=self._percentile(execution_times, 95),
            p99_execution_time_ms=self._percentile(execution_times, 99),
            avg_memory_usage_mb=statistics.mean(memory_usages),
            max_memory_usage_mb=max(memory_usages),
            avg_throughput_ops_per_sec=statistics.mean(throughputs),
            sample_size=len(successful_results),
            created_at=datetime.now(),
            last_updated=datetime.now()
        )
        
        with self.lock:
            self.baselines[benchmark_name] = baseline
            self._save_baselines()
        
        self.logger.info(f"Updated baseline for {benchmark_name} with {len(successful_results)} samples")
    
    def detect_regressions(self, result: BenchmarkResult) -> List[RegressionAlert]:
        """Detect performance regressions in a benchmark result"""
        if not result.success:
            return []
        
        with self.lock:
            baseline = self.baselines.get(result.benchmark_name)
        
        if not baseline:
            self.logger.warning(f"No baseline found for {result.benchmark_name}")
            return []
        
        alerts = []
        
        # Check execution time regression
        time_degradation = ((result.execution_time_ms - baseline.avg_execution_time_ms) / 
                           baseline.avg_execution_time_ms * 100)
        
        if time_degradation > 20:  # 20% slower
            severity = PerformanceStatus.CRITICAL if time_degradation > 50 else PerformanceStatus.DEGRADED
            alerts.append(RegressionAlert(
                id=str(uuid.uuid4()),
                benchmark_name=result.benchmark_name,
                metric_name="execution_time_ms",
                current_value=result.execution_time_ms,
                baseline_value=baseline.avg_execution_time_ms,
                degradation_percent=time_degradation,
                severity=severity,
                detected_at=datetime.now(),
                description=f"Execution time increased by {time_degradation:.1f}%"
            ))
        
        # Check memory usage regression
        memory_degradation = ((result.memory_usage_mb - baseline.avg_memory_usage_mb) / 
                             baseline.avg_memory_usage_mb * 100) if baseline.avg_memory_usage_mb > 0 else 0
        
        if memory_degradation > 30:  # 30% more memory
            severity = PerformanceStatus.CRITICAL if memory_degradation > 100 else PerformanceStatus.DEGRADED
            alerts.append(RegressionAlert(
                id=str(uuid.uuid4()),
                benchmark_name=result.benchmark_name,
                metric_name="memory_usage_mb",
                current_value=result.memory_usage_mb,
                baseline_value=baseline.avg_memory_usage_mb,
                degradation_percent=memory_degradation,
                severity=severity,
                detected_at=datetime.now(),
                description=f"Memory usage increased by {memory_degradation:.1f}%"
            ))
        
        # Check throughput regression
        throughput_degradation = ((baseline.avg_throughput_ops_per_sec - result.throughput_ops_per_sec) / 
                                 baseline.avg_throughput_ops_per_sec * 100) if baseline.avg_throughput_ops_per_sec > 0 else 0
        
        if throughput_degradation > 20:  # 20% less throughput
            severity = PerformanceStatus.CRITICAL if throughput_degradation > 50 else PerformanceStatus.DEGRADED
            alerts.append(RegressionAlert(
                id=str(uuid.uuid4()),
                benchmark_name=result.benchmark_name,
                metric_name="throughput_ops_per_sec",
                current_value=result.throughput_ops_per_sec,
                baseline_value=baseline.avg_throughput_ops_per_sec,
                degradation_percent=throughput_degradation,
                severity=severity,
                detected_at=datetime.now(),
                description=f"Throughput decreased by {throughput_degradation:.1f}%"
            ))
        
        return alerts
    
    def _percentile(self, data: List[float], percentile: int) -> float:
        """Calculate percentile value"""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile / 100)
        return sorted_data[min(index, len(sorted_data) - 1)]


class AutomatedBenchmarkSuite:
    """Automated benchmark suite runner"""
    
    def __init__(self):
        self.runner = BenchmarkRunner()
        self.regression_detector = PerformanceRegressionDetector()
        self.logger = get_logger(__name__)
        self.results_history: List[BenchmarkResult] = []
        self.lock = threading.Lock()
    
    def run_comprehensive_benchmark(self, data_sizes: List[int] = None) -> Dict[str, List[BenchmarkResult]]:
        """Run comprehensive benchmark suite"""
        data_sizes = data_sizes or [1, 5, 10, 25, 50]  # MB
        all_results = {}
        
        self.logger.info(f"Starting comprehensive benchmark suite with data sizes: {data_sizes}")
        
        # ETL Processing Benchmarks
        pandas_results = []
        polars_results = []
        
        for size in data_sizes:
            self.logger.info(f"Running ETL benchmarks for {size}MB data")
            
            # Pandas benchmark
            result = self.runner.benchmark_pandas_processing(size)
            pandas_results.append(result)
            
            # Polars benchmark
            result = self.runner.benchmark_polars_processing(size)
            polars_results.append(result)
        
        all_results['pandas_etl'] = pandas_results
        all_results['polars_etl'] = polars_results
        
        # File I/O Benchmarks
        file_io_results = []
        for size in [1, 10, 25]:  # Smaller sizes for file I/O
            result = self.runner.benchmark_file_io(size)
            file_io_results.append(result)
        
        all_results['file_io'] = file_io_results
        
        # Data Validation Benchmarks
        validation_results = []
        for records in [1000, 5000, 10000, 25000]:
            result = self.runner.benchmark_data_validation(records)
            validation_results.append(result)
        
        all_results['data_validation'] = validation_results
        
        # Store results and check for regressions
        with self.lock:
            for category, results in all_results.items():
                self.results_history.extend(results)
                
                # Check for regressions
                for result in results:
                    regressions = self.regression_detector.detect_regressions(result)
                    if regressions:
                        for regression in regressions:
                            self.logger.warning(f"Performance regression detected: {regression.description}")
        
        # Update baselines (run periodically, not every time)
        self._update_baselines_if_needed(all_results)
        
        self.logger.info("Comprehensive benchmark suite completed")
        return all_results
    
    def _update_baselines_if_needed(self, results: Dict[str, List[BenchmarkResult]]):
        """Update baselines if enough new data is available"""
        for category, result_list in results.items():
            if len(result_list) >= 3:  # Minimum samples for baseline
                # Group by benchmark name
                by_benchmark = {}
                for result in result_list:
                    if result.benchmark_name not in by_benchmark:
                        by_benchmark[result.benchmark_name] = []
                    by_benchmark[result.benchmark_name].append(result)
                
                # Update baselines
                for benchmark_name, benchmark_results in by_benchmark.items():
                    self.regression_detector.update_baseline(benchmark_results)
    
    def run_continuous_monitoring(self, interval_minutes: int = 60):
        """Run continuous performance monitoring"""
        self.logger.info(f"Starting continuous performance monitoring (interval: {interval_minutes} minutes)")
        
        while True:
            try:
                # Run lightweight benchmarks
                results = self.run_comprehensive_benchmark([5, 10])  # Small data sizes for continuous monitoring
                
                # Log summary
                total_tests = sum(len(result_list) for result_list in results.values())
                successful_tests = sum(
                    len([r for r in result_list if r.success]) 
                    for result_list in results.values()
                )
                
                self.logger.info(f"Continuous monitoring: {successful_tests}/{total_tests} tests passed")
                
                # Sleep until next run
                time.sleep(interval_minutes * 60)
                
            except Exception as e:
                self.logger.error(f"Error in continuous monitoring: {e}")
                time.sleep(60)  # Wait 1 minute before retry
    
    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report"""
        with self.lock:
            if not self.results_history:
                return {"error": "No benchmark results available"}
        
        # Group results by benchmark name
        by_benchmark = {}
        for result in self.results_history:
            if result.benchmark_name not in by_benchmark:
                by_benchmark[result.benchmark_name] = []
            by_benchmark[result.benchmark_name].append(result)
        
        report = {
            'generated_at': datetime.now(),
            'total_benchmarks_run': len(self.results_history),
            'benchmarks': {}
        }
        
        for benchmark_name, results in by_benchmark.items():
            successful_results = [r for r in results if r.success]
            
            if not successful_results:
                continue
            
            execution_times = [r.execution_time_ms for r in successful_results]
            memory_usages = [r.memory_usage_mb for r in successful_results]
            throughputs = [r.throughput_ops_per_sec for r in successful_results]
            
            report['benchmarks'][benchmark_name] = {
                'total_runs': len(results),
                'successful_runs': len(successful_results),
                'success_rate': len(successful_results) / len(results),
                'avg_execution_time_ms': statistics.mean(execution_times),
                'min_execution_time_ms': min(execution_times),
                'max_execution_time_ms': max(execution_times),
                'p95_execution_time_ms': self._percentile(execution_times, 95),
                'avg_memory_usage_mb': statistics.mean(memory_usages),
                'max_memory_usage_mb': max(memory_usages),
                'avg_throughput_ops_per_sec': statistics.mean(throughputs),
                'last_run': max(r.timestamp for r in results)
            }
        
        return report
    
    def _percentile(self, data: List[float], percentile: int) -> float:
        """Calculate percentile value"""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        index = int(len(sorted_data) * percentile / 100)
        return sorted_data[min(index, len(sorted_data) - 1)]


# Global benchmark suite instance
_benchmark_suite: Optional[AutomatedBenchmarkSuite] = None


def get_benchmark_suite() -> AutomatedBenchmarkSuite:
    """Get global benchmark suite instance"""
    global _benchmark_suite
    if _benchmark_suite is None:
        _benchmark_suite = AutomatedBenchmarkSuite()
    return _benchmark_suite


# Decorator for automatic benchmarking
def benchmark_function(benchmark_name: str = None, data_size_mb: int = 1):
    """Decorator for automatic function benchmarking"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            nonlocal benchmark_name
            if benchmark_name is None:
                benchmark_name = f"{func.__module__}.{func.__name__}"
            
            start_time = time.perf_counter()
            start_memory = 0
            try:
                import psutil
                process = psutil.Process()
                start_memory = process.memory_info().rss / 1024 / 1024
            except ImportError:
                pass
            
            try:
                result = func(*args, **kwargs)
                success = True
                error_message = None
            except Exception as e:
                result = None
                success = False
                error_message = str(e)
                raise
            finally:
                end_time = time.perf_counter()
                end_memory = start_memory
                try:
                    import psutil
                    process = psutil.Process()
                    end_memory = process.memory_info().rss / 1024 / 1024
                except ImportError:
                    pass
                
                execution_time = (end_time - start_time) * 1000
                memory_delta = end_memory - start_memory
                
                benchmark_result = BenchmarkResult(
                    id=str(uuid.uuid4()),
                    benchmark_name=benchmark_name,
                    benchmark_type=BenchmarkType.ETL_PROCESSING,
                    execution_time_ms=execution_time,
                    memory_usage_mb=memory_delta,
                    cpu_usage_percent=0.0,
                    throughput_ops_per_sec=1000 / execution_time if execution_time > 0 else 0,
                    success=success,
                    error_message=error_message,
                    timestamp=datetime.now(),
                    metadata={'function_call': True, 'data_size_mb': data_size_mb}
                )
                
                # Store result in global suite
                suite = get_benchmark_suite()
                with suite.lock:
                    suite.results_history.append(benchmark_result)
                
                # Check for regressions
                regressions = suite.regression_detector.detect_regressions(benchmark_result)
                if regressions:
                    for regression in regressions:
                        suite.logger.warning(f"Performance regression in {func.__name__}: {regression.description}")
            
            return result
        
        return wrapper
    return decorator
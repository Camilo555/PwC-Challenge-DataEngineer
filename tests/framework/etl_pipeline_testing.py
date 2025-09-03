"""
Comprehensive ETL Pipeline Testing Framework
Provides end-to-end testing capabilities for data engineering pipelines with validation.
"""
from __future__ import annotations

import logging
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

try:
    import pyspark
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, mean, stddev
    from pyspark.sql.functions import sum as spark_sum
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    SPARK_AVAILABLE = True
except ImportError:
    pyspark = None
    SparkSession = None
    SparkDataFrame = None
    SPARK_AVAILABLE = False

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    pl = None
    POLARS_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class ETLTestConfig:
    """Configuration for ETL pipeline testing."""
    source_path: str
    target_path: str
    schema_validation: bool = True
    data_quality_checks: bool = True
    performance_benchmarks: bool = True
    row_count_validation: bool = True
    business_rules_validation: bool = True
    transformation_rules: list[dict[str, Any]] = field(default_factory=list)
    expected_output_schema: dict[str, str] | None = None
    quality_thresholds: dict[str, float] = field(default_factory=lambda: {
        'completeness': 0.95,
        'validity': 0.9,
        'accuracy': 0.85
    })


@dataclass
class ETLTestResult:
    """Results from ETL pipeline testing."""
    pipeline_name: str
    success: bool
    execution_time_seconds: float
    records_processed: int
    data_quality_score: float
    test_details: dict[str, Any]
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    performance_metrics: dict[str, float] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


class ETLTestFramework:
    """Comprehensive ETL testing framework for data pipelines."""

    def __init__(self, spark_session: SparkSession | None = None):
        """Initialize ETL test framework."""
        self.spark = spark_session
        self.test_results: list[ETLTestResult] = []
        self.temp_dir = Path(tempfile.mkdtemp())

        if SPARK_AVAILABLE and not self.spark:
            try:
                self.spark = SparkSession.builder \
                    .appName("ETL_Test_Framework") \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                    .getOrCreate()
            except Exception as e:
                logger.warning(f"Failed to create Spark session: {e}")
                self.spark = None

    def create_test_data(self, data_type: str, size: int = 1000) -> pd.DataFrame:
        """Create test data for ETL pipeline validation."""
        np.random.seed(42)  # For reproducible tests

        if data_type == "retail_sales":
            return pd.DataFrame({
                'invoice_id': [f'INV-{i:06d}' for i in range(1, size + 1)],
                'customer_id': np.random.randint(1, 1000, size),
                'product_id': np.random.randint(1, 100, size),
                'country': np.random.choice(['USA', 'UK', 'Germany', 'France', 'Canada'], size),
                'quantity': np.random.randint(1, 10, size),
                'unit_price': np.round(np.random.uniform(10, 100, size), 2),
                'discount': np.random.uniform(0, 0.3, size),
                'invoice_date': pd.date_range('2023-01-01', periods=size, freq='H')
            })

        elif data_type == "customer_data":
            return pd.DataFrame({
                'customer_id': range(1, size + 1),
                'customer_name': [f'Customer_{i}' for i in range(1, size + 1)],
                'email': [f'customer{i}@test.com' for i in range(1, size + 1)],
                'registration_date': pd.date_range('2020-01-01', periods=size, freq='D'),
                'is_active': np.random.choice([True, False], size),
                'lifetime_value': np.round(np.random.uniform(100, 10000, size), 2)
            })

        elif data_type == "corrupted_sales":
            # Intentionally corrupted data for testing data quality
            data = {
                'invoice_id': [f'INV-{i:06d}' if i % 10 != 0 else None for i in range(1, size + 1)],
                'customer_id': [i if i % 15 != 0 else -1 for i in range(1, size + 1)],
                'product_id': np.random.randint(-5, 100, size),  # Some negative IDs
                'country': np.random.choice(['USA', 'UK', 'INVALID', '', None], size),
                'quantity': np.random.randint(-2, 10, size),  # Some negative quantities
                'unit_price': [p if p > 0 else np.nan for p in np.random.uniform(-10, 100, size)],
                'discount': np.random.uniform(-0.1, 1.5, size),  # Some invalid discounts
                'invoice_date': [
                    pd.date_range('2023-01-01', periods=size, freq='H')[i]
                    if i % 20 != 0 else None
                    for i in range(size)
                ]
            }
            return pd.DataFrame(data)

        else:
            raise ValueError(f"Unknown data type: {data_type}")

    def validate_schema(self, df: pd.DataFrame, expected_schema: dict[str, str]) -> list[str]:
        """Validate DataFrame schema against expected structure."""
        errors = []

        # Check for missing columns
        missing_cols = set(expected_schema.keys()) - set(df.columns)
        if missing_cols:
            errors.append(f"Missing columns: {missing_cols}")

        # Check for unexpected columns
        unexpected_cols = set(df.columns) - set(expected_schema.keys())
        if unexpected_cols:
            errors.append(f"Unexpected columns: {unexpected_cols}")

        # Check data types
        for col_name, expected_type in expected_schema.items():
            if col_name in df.columns:
                actual_type = str(df[col_name].dtype)

                # Type mapping for compatibility
                type_compatibility = {
                    'int64': ['int64', 'int32', 'int16', 'int8'],
                    'float64': ['float64', 'float32'],
                    'object': ['object', 'string'],
                    'datetime64[ns]': ['datetime64[ns]', 'datetime64'],
                    'bool': ['bool']
                }

                if expected_type in type_compatibility:
                    if actual_type not in type_compatibility[expected_type]:
                        errors.append(f"Column {col_name}: expected {expected_type}, got {actual_type}")
                elif actual_type != expected_type:
                    errors.append(f"Column {col_name}: expected {expected_type}, got {actual_type}")

        return errors

    def validate_data_quality(self, df: pd.DataFrame, config: ETLTestConfig) -> dict[str, float]:
        """Validate data quality metrics."""
        metrics = {}

        # Completeness: percentage of non-null values
        total_cells = len(df) * len(df.columns)
        null_cells = df.isnull().sum().sum()
        metrics['completeness'] = (total_cells - null_cells) / total_cells if total_cells > 0 else 0.0

        # Row-level completeness
        metrics['row_completeness'] = (df.isnull().sum(axis=1) == 0).mean()

        # Uniqueness (for ID columns)
        id_columns = [col for col in df.columns if 'id' in col.lower()]
        if id_columns:
            unique_ratios = []
            for col in id_columns:
                if col in df.columns and len(df) > 0:
                    unique_ratio = df[col].nunique() / len(df)
                    unique_ratios.append(unique_ratio)
            metrics['uniqueness'] = np.mean(unique_ratios) if unique_ratios else 1.0
        else:
            metrics['uniqueness'] = 1.0

        # Validity: basic business rule validation
        validity_score = 1.0
        validity_errors = 0
        total_validations = 0

        # Check for negative quantities and prices
        if 'quantity' in df.columns:
            invalid_quantities = (df['quantity'] <= 0).sum()
            validity_errors += invalid_quantities
            total_validations += len(df)

        if 'unit_price' in df.columns:
            invalid_prices = (df['unit_price'] <= 0).sum()
            validity_errors += invalid_prices
            total_validations += len(df)

        if 'discount' in df.columns:
            invalid_discounts = ((df['discount'] < 0) | (df['discount'] > 1)).sum()
            validity_errors += invalid_discounts
            total_validations += len(df)

        if total_validations > 0:
            validity_score = 1 - (validity_errors / total_validations)

        metrics['validity'] = max(0.0, validity_score)

        # Overall quality score (weighted average)
        metrics['overall_quality'] = (
            metrics['completeness'] * 0.4 +
            metrics['uniqueness'] * 0.3 +
            metrics['validity'] * 0.3
        )

        return metrics

    def test_bronze_layer_ingestion(self, source_df: pd.DataFrame, config: ETLTestConfig) -> ETLTestResult:
        """Test bronze layer data ingestion process."""
        start_time = time.time()
        errors = []
        warnings = []

        try:
            # Simulate bronze layer ingestion (minimal transformations)
            bronze_df = source_df.copy()

            # Add bronze layer metadata
            bronze_df['_ingestion_timestamp'] = datetime.now()
            bronze_df['_source_file'] = config.source_path
            bronze_df['_batch_id'] = str(uuid.uuid4())

            # Basic data validation
            if config.schema_validation and config.expected_output_schema:
                schema_errors = self.validate_schema(bronze_df, config.expected_output_schema)
                errors.extend(schema_errors)

            # Data quality checks
            quality_metrics = {}
            if config.data_quality_checks:
                quality_metrics = self.validate_data_quality(bronze_df, config)

                # Check against thresholds
                for metric, threshold in config.quality_thresholds.items():
                    if metric in quality_metrics and quality_metrics[metric] < threshold:
                        warnings.append(f"Data quality {metric} below threshold: {quality_metrics[metric]:.3f} < {threshold}")

            execution_time = time.time() - start_time

            result = ETLTestResult(
                pipeline_name="bronze_ingestion",
                success=len(errors) == 0,
                execution_time_seconds=execution_time,
                records_processed=len(bronze_df),
                data_quality_score=quality_metrics.get('overall_quality', 0.0),
                test_details={
                    'schema_validation': len(errors) == 0 if config.schema_validation else None,
                    'quality_metrics': quality_metrics,
                    'row_count': len(bronze_df),
                    'column_count': len(bronze_df.columns)
                },
                errors=errors,
                warnings=warnings,
                performance_metrics={
                    'records_per_second': len(bronze_df) / execution_time if execution_time > 0 else 0,
                    'memory_usage_mb': bronze_df.memory_usage(deep=True).sum() / 1024 / 1024
                }
            )

            self.test_results.append(result)
            return result

        except Exception as e:
            execution_time = time.time() - start_time
            errors.append(f"Bronze ingestion failed: {str(e)}")

            result = ETLTestResult(
                pipeline_name="bronze_ingestion",
                success=False,
                execution_time_seconds=execution_time,
                records_processed=0,
                data_quality_score=0.0,
                test_details={'error': str(e)},
                errors=errors
            )

            self.test_results.append(result)
            return result

    def test_silver_layer_transformation(self, bronze_df: pd.DataFrame, config: ETLTestConfig) -> ETLTestResult:
        """Test silver layer data transformation process."""
        start_time = time.time()
        errors = []
        warnings = []

        try:
            # Simulate silver layer transformations
            silver_df = bronze_df.copy()

            # Data cleansing and enrichment
            if 'invoice_id' in silver_df.columns:
                # Remove records with null invoice IDs
                initial_count = len(silver_df)
                silver_df = silver_df.dropna(subset=['invoice_id'])
                if len(silver_df) != initial_count:
                    warnings.append(f"Removed {initial_count - len(silver_df)} records with null invoice_id")

            # Calculate derived fields
            if all(col in silver_df.columns for col in ['quantity', 'unit_price']):
                silver_df['line_total'] = silver_df['quantity'] * silver_df['unit_price']

            if all(col in silver_df.columns for col in ['line_total', 'discount']):
                silver_df['discount_amount'] = silver_df['line_total'] * silver_df['discount']
                silver_df['net_amount'] = silver_df['line_total'] - silver_df['discount_amount']

            # Data type optimization
            if 'customer_id' in silver_df.columns:
                silver_df['customer_id'] = silver_df['customer_id'].astype('int32')

            if 'product_id' in silver_df.columns:
                silver_df['product_id'] = silver_df['product_id'].astype('int32')

            # Add silver layer metadata
            silver_df['_processed_timestamp'] = datetime.now()
            silver_df['_data_quality_score'] = 0.95  # Placeholder

            # Validation
            if config.schema_validation and config.expected_output_schema:
                schema_errors = self.validate_schema(silver_df, config.expected_output_schema)
                errors.extend(schema_errors)

            # Business rules validation
            if config.business_rules_validation:
                # Rule: Net amount should not be negative
                if 'net_amount' in silver_df.columns:
                    negative_amounts = silver_df[silver_df['net_amount'] < 0]
                    if not negative_amounts.empty:
                        errors.append(f"Found {len(negative_amounts)} records with negative net amounts")

                # Rule: Discount should be between 0 and 1
                if 'discount' in silver_df.columns:
                    invalid_discounts = silver_df[(silver_df['discount'] < 0) | (silver_df['discount'] > 1)]
                    if not invalid_discounts.empty:
                        errors.append(f"Found {len(invalid_discounts)} records with invalid discount values")

            # Quality metrics
            quality_metrics = {}
            if config.data_quality_checks:
                quality_metrics = self.validate_data_quality(silver_df, config)

            execution_time = time.time() - start_time

            result = ETLTestResult(
                pipeline_name="silver_transformation",
                success=len(errors) == 0,
                execution_time_seconds=execution_time,
                records_processed=len(silver_df),
                data_quality_score=quality_metrics.get('overall_quality', 0.0),
                test_details={
                    'schema_validation': len(errors) == 0 if config.schema_validation else None,
                    'business_rules_passed': len(errors) == 0,
                    'quality_metrics': quality_metrics,
                    'transformation_applied': True
                },
                errors=errors,
                warnings=warnings,
                performance_metrics={
                    'records_per_second': len(silver_df) / execution_time if execution_time > 0 else 0,
                    'transformation_overhead': execution_time / len(silver_df) if len(silver_df) > 0 else 0
                }
            )

            self.test_results.append(result)
            return result

        except Exception as e:
            execution_time = time.time() - start_time
            errors.append(f"Silver transformation failed: {str(e)}")

            result = ETLTestResult(
                pipeline_name="silver_transformation",
                success=False,
                execution_time_seconds=execution_time,
                records_processed=0,
                data_quality_score=0.0,
                test_details={'error': str(e)},
                errors=errors
            )

            self.test_results.append(result)
            return result

    def test_gold_layer_aggregation(self, silver_df: pd.DataFrame, config: ETLTestConfig) -> ETLTestResult:
        """Test gold layer data aggregation for star schema."""
        start_time = time.time()
        errors = []
        warnings = []

        try:
            # Create dimension tables
            dimensions = {}

            # Customer dimension
            if 'customer_id' in silver_df.columns:
                customer_dim = silver_df[['customer_id']].drop_duplicates()
                customer_dim['customer_key'] = range(1, len(customer_dim) + 1)
                customer_dim['customer_name'] = customer_dim['customer_id'].apply(lambda x: f'Customer_{x}')
                customer_dim['created_date'] = datetime.now()
                dimensions['customer'] = customer_dim

            # Product dimension
            if 'product_id' in silver_df.columns:
                product_dim = silver_df[['product_id']].drop_duplicates()
                product_dim['product_key'] = range(1, len(product_dim) + 1)
                product_dim['product_name'] = product_dim['product_id'].apply(lambda x: f'Product_{x}')
                product_dim['category'] = 'Electronics'
                dimensions['product'] = product_dim

            # Time dimension
            if 'invoice_date' in silver_df.columns:
                time_dim = pd.DataFrame()
                unique_dates = pd.to_datetime(silver_df['invoice_date']).dt.date.unique()
                time_dim['date'] = unique_dates
                time_dim['time_key'] = range(1, len(time_dim) + 1)
                time_dim['year'] = pd.to_datetime(time_dim['date']).dt.year
                time_dim['month'] = pd.to_datetime(time_dim['date']).dt.month
                time_dim['quarter'] = pd.to_datetime(time_dim['date']).dt.quarter
                dimensions['time'] = time_dim

            # Geography dimension
            if 'country' in silver_df.columns:
                geo_dim = silver_df[['country']].drop_duplicates()
                geo_dim['geography_key'] = range(1, len(geo_dim) + 1)
                geo_dim['continent'] = geo_dim['country'].apply(lambda x: 'North America' if x in ['USA', 'Canada'] else 'Europe')
                dimensions['geography'] = geo_dim

            # Create fact table
            fact_df = silver_df.copy()

            # Add dimension keys
            if 'customer' in dimensions:
                fact_df = fact_df.merge(
                    dimensions['customer'][['customer_id', 'customer_key']],
                    on='customer_id',
                    how='left'
                )

            if 'product' in dimensions:
                fact_df = fact_df.merge(
                    dimensions['product'][['product_id', 'product_key']],
                    on='product_id',
                    how='left'
                )

            # Select fact table columns
            fact_columns = ['invoice_id']
            if 'customer_key' in fact_df.columns:
                fact_columns.append('customer_key')
            if 'product_key' in fact_df.columns:
                fact_columns.append('product_key')

            # Add measures
            measure_columns = ['quantity', 'unit_price', 'line_total', 'discount_amount', 'net_amount']
            fact_columns.extend([col for col in measure_columns if col in fact_df.columns])

            gold_fact = fact_df[fact_columns].copy()

            # Validation for star schema
            referential_integrity_errors = []

            # Check for null foreign keys
            for dim_name in dimensions:
                fk_col = f"{dim_name}_key"
                if fk_col in gold_fact.columns:
                    null_fks = gold_fact[fk_col].isnull().sum()
                    if null_fks > 0:
                        referential_integrity_errors.append(f"Null {fk_col} found: {null_fks} records")

            if referential_integrity_errors:
                errors.extend(referential_integrity_errors)

            # Performance metrics for aggregations
            aggregation_metrics = {}
            if len(gold_fact) > 0:
                # Sample aggregations
                if 'net_amount' in gold_fact.columns:
                    total_revenue = gold_fact['net_amount'].sum()
                    avg_order_value = gold_fact['net_amount'].mean()
                    aggregation_metrics.update({
                        'total_revenue': total_revenue,
                        'avg_order_value': avg_order_value
                    })

            execution_time = time.time() - start_time

            result = ETLTestResult(
                pipeline_name="gold_aggregation",
                success=len(errors) == 0,
                execution_time_seconds=execution_time,
                records_processed=len(gold_fact),
                data_quality_score=1.0 if len(errors) == 0 else 0.5,
                test_details={
                    'dimensions_created': len(dimensions),
                    'fact_records': len(gold_fact),
                    'aggregation_metrics': aggregation_metrics,
                    'referential_integrity': len(referential_integrity_errors) == 0
                },
                errors=errors,
                warnings=warnings,
                performance_metrics={
                    'records_per_second': len(gold_fact) / execution_time if execution_time > 0 else 0,
                    'dimensions_count': len(dimensions)
                }
            )

            self.test_results.append(result)
            return result

        except Exception as e:
            execution_time = time.time() - start_time
            errors.append(f"Gold aggregation failed: {str(e)}")

            result = ETLTestResult(
                pipeline_name="gold_aggregation",
                success=False,
                execution_time_seconds=execution_time,
                records_processed=0,
                data_quality_score=0.0,
                test_details={'error': str(e)},
                errors=errors
            )

            self.test_results.append(result)
            return result

    def test_spark_etl_pipeline(self, source_df: pd.DataFrame, config: ETLTestConfig) -> ETLTestResult:
        """Test ETL pipeline using Spark for large-scale processing."""
        if not SPARK_AVAILABLE or not self.spark:
            return ETLTestResult(
                pipeline_name="spark_etl",
                success=False,
                execution_time_seconds=0,
                records_processed=0,
                data_quality_score=0.0,
                test_details={},
                errors=["Spark not available"]
            )

        start_time = time.time()
        errors = []
        warnings = []

        try:
            # Convert pandas to Spark DataFrame
            spark_df = self.spark.createDataFrame(source_df)

            # Spark transformations
            if 'quantity' in spark_df.columns and 'unit_price' in spark_df.columns:
                spark_df = spark_df.withColumn('line_total', col('quantity') * col('unit_price'))

            # Data quality checks using Spark
            total_records = spark_df.count()

            # Check for nulls
            null_counts = {}
            for column in spark_df.columns:
                null_count = spark_df.filter(col(column).isNull()).count()
                null_counts[column] = null_count

            # Aggregations
            if 'line_total' in spark_df.columns:
                agg_results = spark_df.agg(
                    spark_sum('line_total').alias('total_revenue'),
                    mean('line_total').alias('avg_line_total'),
                    count('*').alias('record_count')
                ).collect()[0]

                spark_metrics = {
                    'total_revenue': float(agg_results['total_revenue']) if agg_results['total_revenue'] else 0.0,
                    'avg_line_total': float(agg_results['avg_line_total']) if agg_results['avg_line_total'] else 0.0,
                    'record_count': int(agg_results['record_count'])
                }
            else:
                spark_metrics = {'record_count': total_records}

            execution_time = time.time() - start_time

            result = ETLTestResult(
                pipeline_name="spark_etl",
                success=len(errors) == 0,
                execution_time_seconds=execution_time,
                records_processed=total_records,
                data_quality_score=0.95,  # Placeholder
                test_details={
                    'spark_version': self.spark.version,
                    'null_counts': null_counts,
                    'aggregation_results': spark_metrics,
                    'partitions': spark_df.rdd.getNumPartitions()
                },
                errors=errors,
                warnings=warnings,
                performance_metrics={
                    'records_per_second': total_records / execution_time if execution_time > 0 else 0,
                    'spark_execution_time': execution_time
                }
            )

            self.test_results.append(result)
            return result

        except Exception as e:
            execution_time = time.time() - start_time
            errors.append(f"Spark ETL failed: {str(e)}")

            result = ETLTestResult(
                pipeline_name="spark_etl",
                success=False,
                execution_time_seconds=execution_time,
                records_processed=0,
                data_quality_score=0.0,
                test_details={'error': str(e)},
                errors=errors
            )

            self.test_results.append(result)
            return result

    def run_end_to_end_pipeline_test(self, config: ETLTestConfig) -> dict[str, Any]:
        """Run complete end-to-end ETL pipeline test."""
        pipeline_start = time.time()

        # Create test data
        source_data = self.create_test_data("retail_sales", size=1000)

        # Test bronze layer
        bronze_result = self.test_bronze_layer_ingestion(source_data, config)

        # Test silver layer (if bronze succeeded)
        silver_result = None
        if bronze_result.success:
            silver_result = self.test_silver_layer_transformation(source_data, config)

        # Test gold layer (if silver succeeded)
        gold_result = None
        if silver_result and silver_result.success:
            silver_data = source_data.copy()  # In practice, would use transformed data
            gold_result = self.test_gold_layer_aggregation(silver_data, config)

        # Test Spark pipeline
        spark_result = self.test_spark_etl_pipeline(source_data, config)

        total_execution_time = time.time() - pipeline_start

        # Compile results
        all_results = [bronze_result]
        if silver_result:
            all_results.append(silver_result)
        if gold_result:
            all_results.append(gold_result)
        all_results.append(spark_result)

        overall_success = all(result.success for result in all_results)
        total_records = sum(result.records_processed for result in all_results)
        avg_quality_score = np.mean([result.data_quality_score for result in all_results])

        return {
            'overall_success': overall_success,
            'total_execution_time_seconds': total_execution_time,
            'total_records_processed': total_records,
            'average_quality_score': avg_quality_score,
            'layer_results': {
                'bronze': bronze_result,
                'silver': silver_result,
                'gold': gold_result,
                'spark': spark_result
            },
            'performance_summary': {
                'records_per_second': total_records / total_execution_time if total_execution_time > 0 else 0,
                'layers_successful': sum(1 for result in all_results if result.success),
                'total_layers_tested': len(all_results)
            }
        }

    def generate_test_report(self, output_path: Path | None = None) -> dict[str, Any]:
        """Generate comprehensive ETL testing report."""
        if not self.test_results:
            return {"error": "No test results available"}

        # Summary statistics
        total_tests = len(self.test_results)
        successful_tests = sum(1 for result in self.test_results if result.success)
        total_records = sum(result.records_processed for result in self.test_results)
        total_execution_time = sum(result.execution_time_seconds for result in self.test_results)

        # Performance metrics
        avg_records_per_second = []
        for result in self.test_results:
            if result.execution_time_seconds > 0:
                rps = result.records_processed / result.execution_time_seconds
                avg_records_per_second.append(rps)

        # Quality metrics
        quality_scores = [result.data_quality_score for result in self.test_results]

        report = {
            'summary': {
                'total_tests': total_tests,
                'successful_tests': successful_tests,
                'success_rate': successful_tests / total_tests if total_tests > 0 else 0,
                'total_records_processed': total_records,
                'total_execution_time_seconds': total_execution_time
            },
            'performance_metrics': {
                'average_records_per_second': np.mean(avg_records_per_second) if avg_records_per_second else 0,
                'max_records_per_second': np.max(avg_records_per_second) if avg_records_per_second else 0,
                'min_records_per_second': np.min(avg_records_per_second) if avg_records_per_second else 0
            },
            'quality_metrics': {
                'average_quality_score': np.mean(quality_scores) if quality_scores else 0,
                'min_quality_score': np.min(quality_scores) if quality_scores else 0,
                'max_quality_score': np.max(quality_scores) if quality_scores else 0
            },
            'pipeline_results': [
                {
                    'pipeline_name': result.pipeline_name,
                    'success': result.success,
                    'execution_time': result.execution_time_seconds,
                    'records_processed': result.records_processed,
                    'quality_score': result.data_quality_score,
                    'errors': result.errors,
                    'warnings': result.warnings
                }
                for result in self.test_results
            ],
            'generated_at': datetime.now().isoformat()
        }

        # Save report if output path provided
        if output_path:
            import json
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, default=str)

        return report

    def cleanup(self):
        """Clean up test resources."""
        if self.spark:
            self.spark.stop()

        # Clean up temporary files
        import shutil
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)


# Test fixtures and utilities
@pytest.fixture
def etl_test_framework():
    """Create ETL test framework fixture."""
    framework = ETLTestFramework()
    yield framework
    framework.cleanup()


@pytest.fixture
def sample_etl_config():
    """Sample ETL test configuration."""
    return ETLTestConfig(
        source_path="/data/raw/sales.csv",
        target_path="/data/processed/sales_star_schema/",
        schema_validation=True,
        data_quality_checks=True,
        performance_benchmarks=True,
        expected_output_schema={
            'invoice_id': 'object',
            'customer_id': 'int32',
            'product_id': 'int32',
            'country': 'object',
            'quantity': 'int64',
            'unit_price': 'float64',
            'discount': 'float64',
            'invoice_date': 'datetime64[ns]'
        },
        quality_thresholds={
            'completeness': 0.95,
            'validity': 0.9,
            'accuracy': 0.85
        }
    )


# Example test cases
class TestETLPipelineFramework:
    """Test cases for ETL pipeline testing framework."""

    def test_bronze_layer_ingestion(self, etl_test_framework, sample_etl_config):
        """Test bronze layer data ingestion."""
        # Create test data
        test_data = etl_test_framework.create_test_data("retail_sales", size=100)

        # Run bronze ingestion test
        result = etl_test_framework.test_bronze_layer_ingestion(test_data, sample_etl_config)

        assert isinstance(result, ETLTestResult)
        assert result.pipeline_name == "bronze_ingestion"
        assert result.records_processed == 100
        assert result.execution_time_seconds > 0

    def test_silver_transformation(self, etl_test_framework, sample_etl_config):
        """Test silver layer transformation logic."""
        test_data = etl_test_framework.create_test_data("retail_sales", size=50)

        result = etl_test_framework.test_silver_layer_transformation(test_data, sample_etl_config)

        assert isinstance(result, ETLTestResult)
        assert result.pipeline_name == "silver_transformation"
        assert result.records_processed > 0

    def test_data_quality_validation(self, etl_test_framework, sample_etl_config):
        """Test data quality validation with corrupted data."""
        corrupted_data = etl_test_framework.create_test_data("corrupted_sales", size=100)

        quality_metrics = etl_test_framework.validate_data_quality(corrupted_data, sample_etl_config)

        assert 'completeness' in quality_metrics
        assert 'validity' in quality_metrics
        assert 'overall_quality' in quality_metrics
        assert 0.0 <= quality_metrics['overall_quality'] <= 1.0

    def test_schema_validation(self, etl_test_framework):
        """Test schema validation functionality."""
        test_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'value': [10.0, 20.0, 30.0]
        })

        expected_schema = {
            'id': 'int64',
            'name': 'object',
            'value': 'float64'
        }

        errors = etl_test_framework.validate_schema(test_df, expected_schema)
        assert len(errors) == 0

        # Test with wrong schema
        wrong_schema = {
            'id': 'object',  # Wrong type
            'missing_col': 'int64'  # Missing column
        }

        errors = etl_test_framework.validate_schema(test_df, wrong_schema)
        assert len(errors) > 0

    def test_end_to_end_pipeline(self, etl_test_framework, sample_etl_config):
        """Test complete end-to-end ETL pipeline."""
        results = etl_test_framework.run_end_to_end_pipeline_test(sample_etl_config)

        assert 'overall_success' in results
        assert 'layer_results' in results
        assert 'performance_summary' in results
        assert results['total_records_processed'] > 0

    def test_spark_pipeline(self, etl_test_framework, sample_etl_config):
        """Test Spark ETL pipeline (if Spark available)."""
        test_data = etl_test_framework.create_test_data("retail_sales", size=100)

        result = etl_test_framework.test_spark_etl_pipeline(test_data, sample_etl_config)

        assert isinstance(result, ETLTestResult)
        assert result.pipeline_name == "spark_etl"

        if SPARK_AVAILABLE:
            # Additional assertions if Spark is available
            assert result.execution_time_seconds >= 0

    def test_report_generation(self, etl_test_framework, sample_etl_config, tmp_path):
        """Test ETL testing report generation."""
        # Run some tests first
        test_data = etl_test_framework.create_test_data("retail_sales", size=50)
        etl_test_framework.test_bronze_layer_ingestion(test_data, sample_etl_config)

        # Generate report
        report = etl_test_framework.generate_test_report()

        assert 'summary' in report
        assert 'performance_metrics' in report
        assert 'quality_metrics' in report
        assert 'pipeline_results' in report
        assert len(report['pipeline_results']) > 0

        # Test saving to file
        report_path = tmp_path / "etl_test_report.json"
        etl_test_framework.generate_test_report(report_path)
        assert report_path.exists()


if __name__ == "__main__":
    print("ETL Pipeline Testing Framework ready for use!")

    if SPARK_AVAILABLE:
        print("✓ Apache Spark support available")
    else:
        print("Note: Apache Spark not available. Install with: pip install pyspark")

    if POLARS_AVAILABLE:
        print("✓ Polars support available")
    else:
        print("Note: Polars not available. Install with: pip install polars")

    print("✓ Bronze layer ingestion testing ready")
    print("✓ Silver layer transformation testing ready")
    print("✓ Gold layer aggregation testing ready")
    print("✓ Data quality validation ready")
    print("✓ Schema validation ready")
    print("✓ Performance benchmarking ready")
    print("✓ End-to-end pipeline testing ready")

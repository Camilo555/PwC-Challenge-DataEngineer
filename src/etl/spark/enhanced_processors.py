"""
Enhanced Spark Processors with Advanced Monitoring and Data Quality
Comprehensive Spark-based ETL processors with built-in monitoring, error handling, and data quality validation.
"""

import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    desc,
    lit,
    percentile_approx,
    regexp_replace,
    row_number,
    trim,
    upper,
    when,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from core.logging import get_logger
from etl.spark.session_manager import SparkSessionManager
from monitoring import AlertSeverity, get_metrics_collector, trigger_custom_alert

logger = get_logger(__name__)

class BaseSparkProcessor(ABC):
    """Base class for all Spark processors with common functionality"""

    def __init__(self, spark_session: SparkSession | None = None):
        self.spark = spark_session or SparkSessionManager().get_or_create_session(self.__class__.__name__)
        self.metrics_collector = get_metrics_collector()
        self.start_time = datetime.now()
        self.processing_stats = {
            'records_read': 0,
            'records_written': 0,
            'records_failed': 0,
            'data_quality_score': 0.0,
            'error_count': 0,
            'warnings_count': 0,
            'processing_time_seconds': 0.0
        }

    @abstractmethod
    def process(self) -> dict[str, Any]:
        """Main processing method to be implemented by subclasses"""
        pass

    def _calculate_data_quality_score(self, df: DataFrame, quality_checks: list[dict]) -> float:
        """Calculate comprehensive data quality score"""
        total_records = df.count()
        if total_records == 0:
            return 0.0

        quality_scores = []

        for check in quality_checks:
            check_type = check.get('type')
            column = check.get('column')
            threshold = check.get('threshold', 0.95)

            if check_type == 'completeness':
                non_null_count = df.filter(col(column).isNotNull()).count()
                score = non_null_count / total_records

            elif check_type == 'uniqueness':
                unique_count = df.select(column).distinct().count()
                score = unique_count / total_records

            elif check_type == 'validity':
                pattern = check.get('pattern')
                if pattern:
                    valid_count = df.filter(col(column).rlike(pattern)).count()
                    score = valid_count / total_records
                else:
                    score = 1.0

            elif check_type == 'range':
                min_val = check.get('min_value')
                max_val = check.get('max_value')
                if min_val is not None and max_val is not None:
                    valid_count = df.filter(
                        (col(column) >= min_val) & (col(column) <= max_val)
                    ).count()
                    score = valid_count / total_records
                else:
                    score = 1.0
            else:
                score = 1.0  # Default perfect score for unknown checks

            quality_scores.append(score)

            # Log quality issues
            if score < threshold:
                logger.warning(f"Data quality issue in {column}: {check_type} score {score:.3f} below threshold {threshold}")
                self.processing_stats['warnings_count'] += 1

        return sum(quality_scores) / len(quality_scores) if quality_scores else 1.0

    def _record_processing_metrics(self, stage: str, result: dict[str, Any]):
        """Record processing metrics for monitoring"""
        processing_time = (datetime.now() - self.start_time).total_seconds()

        self.processing_stats.update({
            'processing_time_seconds': processing_time,
            **result
        })

        self.metrics_collector.record_etl_metrics(
            pipeline_name=f"spark_{stage}_processor",
            stage=stage,
            records_processed=self.processing_stats['records_read'],
            records_failed=self.processing_stats['records_failed'],
            processing_time=processing_time,
            data_quality_score=self.processing_stats['data_quality_score'],
            error_count=self.processing_stats['error_count'],
            warnings_count=self.processing_stats['warnings_count']
        )

    def _save_quality_report(self, df: DataFrame, stage: str, quality_checks: list[dict]):
        """Save detailed data quality report"""
        try:
            report = {
                'timestamp': datetime.now().isoformat(),
                'stage': stage,
                'total_records': df.count(),
                'schema': str(df.schema),
                'quality_checks': quality_checks,
                'data_quality_score': self.processing_stats['data_quality_score'],
                'processing_stats': self.processing_stats
            }

            # Add basic statistics
            numeric_columns = [field.name for field in df.schema.fields
                             if field.dataType in [IntegerType(), DoubleType(), DecimalType()]]

            if numeric_columns:
                stats_df = df.select([col(c) for c in numeric_columns]).describe()
                stats_dict = {}
                for row in stats_df.collect():
                    metric = row['summary']
                    stats_dict[metric] = {col_name: row[col_name] for col_name in numeric_columns}
                report['statistics'] = stats_dict

            # Save report
            report_dir = Path(f"./reports/data_quality/{stage}")
            report_dir.mkdir(parents=True, exist_ok=True)

            report_file = report_dir / f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2)

            logger.info(f"Quality report saved: {report_file}")

        except Exception as e:
            logger.error(f"Failed to save quality report: {e}")
            self.processing_stats['error_count'] += 1

class EnhancedBronzeProcessor(BaseSparkProcessor):
    """Enhanced Bronze layer processor with comprehensive data ingestion and validation"""

    def __init__(self, spark_session: SparkSession | None = None):
        super().__init__(spark_session)
        self.source_configs = self._load_source_configurations()

    def _load_source_configurations(self) -> dict[str, dict]:
        """Load source data configurations"""
        return {
            'retail_transactions': {
                'path': './data/raw/online_retail_II.xlsx',
                'format': 'xlsx',
                'schema': StructType([
                    StructField("InvoiceNo", StringType(), True),
                    StructField("StockCode", StringType(), True),
                    StructField("Description", StringType(), True),
                    StructField("Quantity", IntegerType(), True),
                    StructField("InvoiceDate", StringType(), True),
                    StructField("UnitPrice", DoubleType(), True),
                    StructField("CustomerID", IntegerType(), True),
                    StructField("Country", StringType(), True)
                ]),
                'quality_checks': [
                    {'type': 'completeness', 'column': 'InvoiceNo', 'threshold': 0.99},
                    {'type': 'completeness', 'column': 'StockCode', 'threshold': 0.99},
                    {'type': 'range', 'column': 'Quantity', 'min_value': -1000, 'max_value': 100000},
                    {'type': 'range', 'column': 'UnitPrice', 'min_value': 0, 'max_value': 10000},
                    {'type': 'validity', 'column': 'Country', 'pattern': '^[A-Za-z\\s]+$'}
                ]
            },
            'sample_data': {
                'path': './data/raw/retail_sample.csv',
                'format': 'csv',
                'schema': None,  # Infer schema
                'quality_checks': [
                    {'type': 'completeness', 'column': 'InvoiceNo', 'threshold': 0.95}
                ]
            }
        }

    def process(self) -> dict[str, Any]:
        """Process all bronze layer sources"""
        logger.info("Starting enhanced bronze layer processing")

        results = {}
        total_records_read = 0
        total_records_written = 0
        total_errors = 0

        for source_name, config in self.source_configs.items():
            try:
                logger.info(f"Processing source: {source_name}")

                # Read source data
                df = self._read_source_data(source_name, config)
                if df is None:
                    continue

                source_count = df.count()
                total_records_read += source_count

                # Apply transformations
                df_transformed = self._apply_bronze_transformations(df, source_name)

                # Data quality checks
                quality_score = self._calculate_data_quality_score(
                    df_transformed, config['quality_checks']
                )

                # Save bronze data
                output_path = f"./data/bronze/{source_name}"
                self._save_bronze_data(df_transformed, output_path)

                written_count = df_transformed.count()
                total_records_written += written_count

                # Save quality report
                self._save_quality_report(df_transformed, f"bronze_{source_name}", config['quality_checks'])

                results[source_name] = {
                    'records_read': source_count,
                    'records_written': written_count,
                    'data_quality_score': quality_score
                }

                logger.info(f"Completed processing {source_name}: {written_count} records")

            except Exception as e:
                logger.error(f"Error processing source {source_name}: {e}")
                total_errors += 1
                trigger_custom_alert(
                    title=f"Bronze Processing Error: {source_name}",
                    description=f"Failed to process {source_name}: {str(e)}",
                    severity=AlertSeverity.HIGH,
                    source="spark_bronze"
                )

        # Update processing stats
        self.processing_stats.update({
            'records_read': total_records_read,
            'records_written': total_records_written,
            'records_failed': total_records_read - total_records_written,
            'error_count': total_errors,
            'data_quality_score': sum(r['data_quality_score'] for r in results.values()) / len(results) if results else 0
        })

        # Record metrics
        self._record_processing_metrics('bronze', self.processing_stats)

        return {
            'total_records': total_records_written,
            'sources_processed': len(results),
            'sources_failed': total_errors,
            'data_quality_score': self.processing_stats['data_quality_score'],
            'error_count': total_errors,
            'results_by_source': results
        }

    def _read_source_data(self, source_name: str, config: dict) -> DataFrame | None:
        """Read data from source with error handling"""
        try:
            path = config['path']
            format_type = config['format']
            schema = config.get('schema')

            if not os.path.exists(path):
                logger.warning(f"Source file not found: {path}")
                return None

            if format_type == 'csv':
                df = self.spark.read.csv(path, header=True, inferSchema=True, schema=schema)
            elif format_type == 'xlsx':
                # For Excel files, we need to convert them first or use a different approach
                # For now, let's assume CSV format or use pandas for Excel
                import pandas as pd
                pandas_df = pd.read_excel(path)
                df = self.spark.createDataFrame(pandas_df)
            elif format_type == 'parquet':
                df = self.spark.read.parquet(path)
            elif format_type == 'json':
                df = self.spark.read.json(path)
            else:
                raise ValueError(f"Unsupported format: {format_type}")

            logger.info(f"Successfully read {df.count()} records from {source_name}")
            return df

        except Exception as e:
            logger.error(f"Failed to read source {source_name}: {e}")
            self.processing_stats['error_count'] += 1
            return None

    def _apply_bronze_transformations(self, df: DataFrame, source_name: str) -> DataFrame:
        """Apply bronze layer transformations"""
        # Add metadata columns
        df_with_metadata = df.withColumn("source_system", lit(source_name)) \
                            .withColumn("ingestion_timestamp", current_timestamp()) \
                            .withColumn("batch_id", lit(f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"))

        # Basic data cleaning
        if 'Description' in df.columns:
            df_with_metadata = df_with_metadata.withColumn(
                'Description',
                trim(regexp_replace(col('Description'), r'[^\w\s]', ''))
            )

        if 'Country' in df.columns:
            df_with_metadata = df_with_metadata.withColumn(
                'Country',
                trim(upper(col('Country')))
            )

        return df_with_metadata

    def _save_bronze_data(self, df: DataFrame, output_path: str):
        """Save bronze data with partitioning"""
        try:
            # Create output directory
            Path(output_path).mkdir(parents=True, exist_ok=True)

            # Save as parquet with partitioning if applicable
            if 'Country' in df.columns:
                df.write.mode('overwrite').partitionBy('Country').parquet(output_path)
            else:
                df.write.mode('overwrite').parquet(output_path)

            logger.info(f"Bronze data saved to {output_path}")

        except Exception as e:
            logger.error(f"Failed to save bronze data to {output_path}: {e}")
            self.processing_stats['error_count'] += 1
            raise

class EnhancedSilverProcessor(BaseSparkProcessor):
    """Enhanced Silver layer processor with advanced data cleaning and validation"""

    def process(self) -> dict[str, Any]:
        """Process silver layer with comprehensive data cleaning"""
        logger.info("Starting enhanced silver layer processing")

        try:
            # Read bronze data
            bronze_df = self._read_bronze_data()
            if bronze_df is None:
                raise ValueError("No bronze data found")

            bronze_count = bronze_df.count()
            self.processing_stats['records_read'] = bronze_count

            # Apply silver transformations
            silver_df = self._apply_silver_transformations(bronze_df)

            # Data quality validation
            quality_checks = self._get_silver_quality_checks()
            quality_score = self._calculate_data_quality_score(silver_df, quality_checks)

            # Advanced data profiling
            profiling_results = self._perform_data_profiling(silver_df)

            # Save silver data
            output_path = "./data/silver/sales"
            self._save_silver_data(silver_df, output_path)

            silver_count = silver_df.count()
            self.processing_stats.update({
                'records_written': silver_count,
                'records_failed': max(0, bronze_count - silver_count),
                'data_quality_score': quality_score
            })

            # Save quality report
            self._save_quality_report(silver_df, "silver", quality_checks)

            # Record metrics
            self._record_processing_metrics('silver', self.processing_stats)

            return {
                'total_records': silver_count,
                'data_quality_score': quality_score,
                'error_count': self.processing_stats['error_count'],
                'profiling_results': profiling_results
            }

        except Exception as e:
            logger.error(f"Silver layer processing failed: {e}")
            self.processing_stats['error_count'] += 1
            trigger_custom_alert(
                title="Silver Layer Processing Failed",
                description=f"Error in silver processing: {str(e)}",
                severity=AlertSeverity.HIGH,
                source="spark_silver"
            )
            raise

    def _read_bronze_data(self) -> DataFrame | None:
        """Read bronze layer data"""
        try:
            bronze_paths = [
                "./data/bronze/retail_transactions",
                "./data/bronze/sample_data"
            ]

            dataframes = []
            for path in bronze_paths:
                if os.path.exists(path):
                    df = self.spark.read.parquet(path)
                    dataframes.append(df)
                    logger.info(f"Read bronze data from {path}: {df.count()} records")

            if not dataframes:
                logger.warning("No bronze data found")
                return None

            # Union all bronze dataframes
            combined_df = dataframes[0]
            for df in dataframes[1:]:
                combined_df = combined_df.unionByName(df, allowMissingColumns=True)

            return combined_df

        except Exception as e:
            logger.error(f"Failed to read bronze data: {e}")
            return None

    def _apply_silver_transformations(self, df: DataFrame) -> DataFrame:
        """Apply comprehensive silver layer transformations"""
        logger.info("Applying silver layer transformations")

        # Remove duplicates
        df_deduped = df.dropDuplicates(['InvoiceNo', 'StockCode', 'InvoiceDate'])

        # Data type conversions and validations
        df_typed = df_deduped.withColumn(
            'Quantity',
            when(col('Quantity').isNull() | (col('Quantity') < 0), 0)
            .otherwise(col('Quantity'))
        ).withColumn(
            'UnitPrice',
            when(col('UnitPrice').isNull() | (col('UnitPrice') < 0), 0.0)
            .otherwise(col('UnitPrice'))
        )

        # Calculate derived fields
        df_enhanced = df_typed.withColumn(
            'TotalPrice',
            col('Quantity') * col('UnitPrice')
        ).withColumn(
            'IsValidTransaction',
            (col('Quantity') > 0) & (col('UnitPrice') > 0) & col('CustomerID').isNotNull()
        )

        # Date parsing and validation
        from pyspark.sql.functions import to_timestamp
        df_enhanced = df_enhanced.withColumn(
            'InvoiceDate',
            to_timestamp(col('InvoiceDate'), 'M/d/yyyy H:mm')
        )

        # Add data quality flags
        df_quality = df_enhanced.withColumn(
            'data_quality_flags',
            when(col('Description').isNull(), lit('missing_description'))
            .when(col('CustomerID').isNull(), lit('missing_customer'))
            .when(col('Quantity') <= 0, lit('invalid_quantity'))
            .when(col('UnitPrice') <= 0, lit('invalid_price'))
            .otherwise(lit('valid'))
        )

        # Filter out invalid records (optional - depends on business rules)
        # For now, keep all records but flag them

        return df_quality

    def _get_silver_quality_checks(self) -> list[dict]:
        """Get silver layer quality checks configuration"""
        return [
            {'type': 'completeness', 'column': 'InvoiceNo', 'threshold': 0.99},
            {'type': 'completeness', 'column': 'StockCode', 'threshold': 0.99},
            {'type': 'completeness', 'column': 'CustomerID', 'threshold': 0.80},
            {'type': 'range', 'column': 'Quantity', 'min_value': 0, 'max_value': 100000},
            {'type': 'range', 'column': 'UnitPrice', 'min_value': 0, 'max_value': 10000},
            {'type': 'range', 'column': 'TotalPrice', 'min_value': 0, 'max_value': 1000000}
        ]

    def _perform_data_profiling(self, df: DataFrame) -> dict[str, Any]:
        """Perform comprehensive data profiling"""
        try:
            total_records = df.count()

            # Basic statistics
            stats = {
                'total_records': total_records,
                'valid_transactions': df.filter(col('IsValidTransaction')).count(),
                'unique_customers': df.select('CustomerID').distinct().count(),
                'unique_products': df.select('StockCode').distinct().count(),
                'unique_countries': df.select('Country').distinct().count(),
                'date_range': {
                    'min_date': df.agg(spark_min('InvoiceDate')).collect()[0][0],
                    'max_date': df.agg(spark_max('InvoiceDate')).collect()[0][0]
                }
            }

            # Revenue statistics
            revenue_stats = df.agg(
                spark_sum('TotalPrice').alias('total_revenue'),
                avg('TotalPrice').alias('avg_transaction_value'),
                spark_max('TotalPrice').alias('max_transaction_value'),
                percentile_approx('TotalPrice', 0.5).alias('median_transaction_value')
            ).collect()[0]

            stats['revenue'] = {
                'total_revenue': float(revenue_stats['total_revenue'] or 0),
                'avg_transaction_value': float(revenue_stats['avg_transaction_value'] or 0),
                'max_transaction_value': float(revenue_stats['max_transaction_value'] or 0),
                'median_transaction_value': float(revenue_stats['median_transaction_value'] or 0)
            }

            # Data quality summary
            quality_flags = df.groupBy('data_quality_flags').count().collect()
            stats['data_quality_summary'] = {
                row['data_quality_flags']: row['count'] for row in quality_flags
            }

            return stats

        except Exception as e:
            logger.error(f"Data profiling failed: {e}")
            return {'error': str(e)}

    def _save_silver_data(self, df: DataFrame, output_path: str):
        """Save silver data with optimized partitioning"""
        try:
            Path(output_path).mkdir(parents=True, exist_ok=True)

            # Add year-month partitioning for better query performance
            from pyspark.sql.functions import month, year
            df_partitioned = df.withColumn('year', year('InvoiceDate')) \
                              .withColumn('month', month('InvoiceDate'))

            # Save with partitioning
            df_partitioned.write.mode('overwrite') \
                         .partitionBy('year', 'month') \
                         .parquet(output_path)

            logger.info(f"Silver data saved to {output_path}")

        except Exception as e:
            logger.error(f"Failed to save silver data: {e}")
            self.processing_stats['error_count'] += 1
            raise

class EnhancedGoldProcessor(BaseSparkProcessor):
    """Enhanced Gold layer processor with advanced analytics and aggregations"""

    def process(self) -> dict[str, Any]:
        """Process gold layer with advanced analytics"""
        logger.info("Starting enhanced gold layer processing")

        try:
            # Read silver data
            silver_df = self._read_silver_data()
            if silver_df is None:
                raise ValueError("No silver data found")

            silver_count = silver_df.count()
            self.processing_stats['records_read'] = silver_count

            # Build analytics tables
            analytics_results = self._build_analytics_tables(silver_df)

            # Calculate overall metrics
            total_records_written = sum(
                result.get('records_count', 0) for result in analytics_results.values()
            )

            self.processing_stats.update({
                'records_written': total_records_written,
                'data_quality_score': 1.0  # Gold layer assumes high quality
            })

            # Record metrics
            self._record_processing_metrics('gold', self.processing_stats)

            return {
                'total_records': total_records_written,
                'analytics_tables': list(analytics_results.keys()),
                'data_quality_score': 1.0,
                'error_count': self.processing_stats['error_count'],
                'table_details': analytics_results
            }

        except Exception as e:
            logger.error(f"Gold layer processing failed: {e}")
            self.processing_stats['error_count'] += 1
            trigger_custom_alert(
                title="Gold Layer Processing Failed",
                description=f"Error in gold processing: {str(e)}",
                severity=AlertSeverity.HIGH,
                source="spark_gold"
            )
            raise

    def _read_silver_data(self) -> DataFrame | None:
        """Read silver layer data"""
        try:
            silver_path = "./data/silver/sales"
            if not os.path.exists(silver_path):
                logger.warning("Silver data not found")
                return None

            df = self.spark.read.parquet(silver_path)
            logger.info(f"Read silver data: {df.count()} records")
            return df

        except Exception as e:
            logger.error(f"Failed to read silver data: {e}")
            return None

    def _build_analytics_tables(self, df: DataFrame) -> dict[str, dict[str, Any]]:
        """Build comprehensive analytics tables"""
        results = {}

        # 1. Sales Summary by Country
        results['sales_by_country'] = self._build_sales_by_country(df)

        # 2. Customer Analytics
        results['customer_analytics'] = self._build_customer_analytics(df)

        # 3. Product Performance
        results['product_performance'] = self._build_product_performance(df)

        # 4. Time Series Analytics
        results['time_series_analytics'] = self._build_time_series_analytics(df)

        # 5. RFM Analysis
        results['rfm_analysis'] = self._build_rfm_analysis(df)

        return results

    def _build_sales_by_country(self, df: DataFrame) -> dict[str, Any]:
        """Build sales summary by country"""
        try:
            sales_by_country = df.filter(col('IsValidTransaction')) \
                                .groupBy('Country') \
                                .agg(
                                    spark_sum('TotalPrice').alias('total_revenue'),
                                    count('InvoiceNo').alias('total_transactions'),
                                    avg('TotalPrice').alias('avg_transaction_value'),
                                    spark_sum('Quantity').alias('total_quantity'),
                                    countDistinct('CustomerID').alias('unique_customers')
                                ) \
                                .orderBy(desc('total_revenue'))

            output_path = "./data/gold/sales_by_country"
            Path(output_path).mkdir(parents=True, exist_ok=True)
            sales_by_country.write.mode('overwrite').parquet(output_path)

            record_count = sales_by_country.count()
            logger.info(f"Sales by country table created: {record_count} countries")

            return {
                'records_count': record_count,
                'output_path': output_path,
                'columns': sales_by_country.columns
            }

        except Exception as e:
            logger.error(f"Failed to build sales by country: {e}")
            self.processing_stats['error_count'] += 1
            return {'error': str(e)}

    def _build_customer_analytics(self, df: DataFrame) -> dict[str, Any]:
        """Build customer analytics table"""
        try:
            customer_analytics = df.filter(col('IsValidTransaction') & col('CustomerID').isNotNull()) \
                                  .groupBy('CustomerID', 'Country') \
                                  .agg(
                                      spark_sum('TotalPrice').alias('total_spent'),
                                      count('InvoiceNo').alias('total_orders'),
                                      avg('TotalPrice').alias('avg_order_value'),
                                      spark_sum('Quantity').alias('total_items'),
                                      countDistinct('StockCode').alias('unique_products'),
                                      spark_max('InvoiceDate').alias('last_purchase_date'),
                                      spark_min('InvoiceDate').alias('first_purchase_date')
                                  )

            # Add customer segments based on spending
            from pyspark.sql.functions import when
            customer_analytics = customer_analytics.withColumn(
                'customer_segment',
                when(col('total_spent') > 5000, 'VIP')
                .when(col('total_spent') > 1000, 'Premium')
                .when(col('total_spent') > 100, 'Regular')
                .otherwise('New')
            )

            output_path = "./data/gold/customer_analytics"
            Path(output_path).mkdir(parents=True, exist_ok=True)
            customer_analytics.write.mode('overwrite').partitionBy('customer_segment').parquet(output_path)

            record_count = customer_analytics.count()
            logger.info(f"Customer analytics table created: {record_count} customers")

            return {
                'records_count': record_count,
                'output_path': output_path,
                'columns': customer_analytics.columns
            }

        except Exception as e:
            logger.error(f"Failed to build customer analytics: {e}")
            self.processing_stats['error_count'] += 1
            return {'error': str(e)}

    def _build_product_performance(self, df: DataFrame) -> dict[str, Any]:
        """Build product performance analytics"""
        try:
            product_performance = df.filter(col('IsValidTransaction')) \
                                   .groupBy('StockCode', 'Description') \
                                   .agg(
                                       spark_sum('TotalPrice').alias('total_revenue'),
                                       spark_sum('Quantity').alias('total_quantity_sold'),
                                       count('InvoiceNo').alias('total_transactions'),
                                       countDistinct('CustomerID').alias('unique_customers'),
                                       avg('UnitPrice').alias('avg_unit_price'),
                                       countDistinct('Country').alias('countries_sold')
                                   ) \
                                   .orderBy(desc('total_revenue'))

            # Add product rankings
            window_spec = Window.orderBy(desc('total_revenue'))
            product_performance = product_performance.withColumn(
                'revenue_rank',
                row_number().over(window_spec)
            )

            output_path = "./data/gold/product_performance"
            Path(output_path).mkdir(parents=True, exist_ok=True)
            product_performance.write.mode('overwrite').parquet(output_path)

            record_count = product_performance.count()
            logger.info(f"Product performance table created: {record_count} products")

            return {
                'records_count': record_count,
                'output_path': output_path,
                'columns': product_performance.columns
            }

        except Exception as e:
            logger.error(f"Failed to build product performance: {e}")
            self.processing_stats['error_count'] += 1
            return {'error': str(e)}

    def _build_time_series_analytics(self, df: DataFrame) -> dict[str, Any]:
        """Build time series analytics"""
        try:
            from pyspark.sql.functions import date_trunc, dayofweek

            daily_sales = df.filter(col('IsValidTransaction')) \
                           .withColumn('date', date_trunc('day', 'InvoiceDate')) \
                           .groupBy('date') \
                           .agg(
                               spark_sum('TotalPrice').alias('daily_revenue'),
                               count('InvoiceNo').alias('daily_transactions'),
                               countDistinct('CustomerID').alias('daily_customers'),
                               avg('TotalPrice').alias('avg_transaction_value')
                           ) \
                           .orderBy('date')

            # Add day of week analysis
            daily_sales = daily_sales.withColumn('day_of_week', dayofweek('date'))

            output_path = "./data/gold/time_series_analytics"
            Path(output_path).mkdir(parents=True, exist_ok=True)
            daily_sales.write.mode('overwrite').parquet(output_path)

            record_count = daily_sales.count()
            logger.info(f"Time series analytics table created: {record_count} days")

            return {
                'records_count': record_count,
                'output_path': output_path,
                'columns': daily_sales.columns
            }

        except Exception as e:
            logger.error(f"Failed to build time series analytics: {e}")
            self.processing_stats['error_count'] += 1
            return {'error': str(e)}

    def _build_rfm_analysis(self, df: DataFrame) -> dict[str, Any]:
        """Build RFM (Recency, Frequency, Monetary) analysis"""
        try:
            from pyspark.sql.functions import datediff
            from pyspark.sql.functions import max as spark_max

            # Calculate RFM metrics
            current_date = df.agg(spark_max('InvoiceDate')).collect()[0][0]

            rfm_metrics = df.filter(col('IsValidTransaction') & col('CustomerID').isNotNull()) \
                           .groupBy('CustomerID') \
                           .agg(
                               datediff(lit(current_date), spark_max('InvoiceDate')).alias('recency'),
                               count('InvoiceNo').alias('frequency'),
                               spark_sum('TotalPrice').alias('monetary')
                           )

            # Calculate RFM scores (simplified quintile approach)
            from pyspark.sql.functions import ntile

            rfm_with_scores = rfm_metrics.withColumn(
                'recency_score',
                ntile(5).over(Window.orderBy(col('recency')))
            ).withColumn(
                'frequency_score',
                ntile(5).over(Window.orderBy(desc('frequency')))
            ).withColumn(
                'monetary_score',
                ntile(5).over(Window.orderBy(desc('monetary')))
            )

            # Create RFM segments
            rfm_with_scores = rfm_with_scores.withColumn(
                'rfm_segment',
                when(
                    (col('recency_score') >= 4) & (col('frequency_score') >= 4) & (col('monetary_score') >= 4),
                    'Champions'
                ).when(
                    (col('recency_score') >= 3) & (col('frequency_score') >= 3) & (col('monetary_score') >= 3),
                    'Loyal Customers'
                ).when(
                    (col('recency_score') >= 3) & (col('frequency_score') <= 2),
                    'Potential Loyalists'
                ).when(
                    (col('recency_score') <= 2) & (col('frequency_score') >= 3),
                    'At Risk'
                ).otherwise('Others')
            )

            output_path = "./data/gold/rfm_analysis"
            Path(output_path).mkdir(parents=True, exist_ok=True)
            rfm_with_scores.write.mode('overwrite').partitionBy('rfm_segment').parquet(output_path)

            record_count = rfm_with_scores.count()
            logger.info(f"RFM analysis table created: {record_count} customers")

            return {
                'records_count': record_count,
                'output_path': output_path,
                'columns': rfm_with_scores.columns
            }

        except Exception as e:
            logger.error(f"Failed to build RFM analysis: {e}")
            self.processing_stats['error_count'] += 1
            return {'error': str(e)}

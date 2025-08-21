"""
Star Schema Builder for Gold Layer
Implements complete 5-dimension star schema with 1 fact table using DataFrame API.
"""
import hashlib
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Union
import pandas as pd

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

try:
    from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType
    from pyspark.sql.window import Window
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

from core.logging import get_logger
from etl.transformations.dataframe_ops import DataFrameTransformer

logger = get_logger(__name__)


class StarSchemaBuilder:
    """
    Complete Star Schema Builder with 5 dimensions + 1 fact table.
    Uses DataFrame API exclusively for all transformations.
    """
    
    def __init__(self, engine: str = "pandas"):
        """Initialize with specified processing engine."""
        self.engine = engine.lower()
        self.transformer = DataFrameTransformer(engine)
        
        # Dimension tracking for surrogate key generation
        self.dimension_cache = {
            'date': {},
            'product': {},
            'customer': {},
            'country': {},
            'invoice': {}
        }
    
    def build_complete_star_schema(self, silver_df: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']) -> Dict[str, Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']]:
        """
        Build complete star schema with all 5 dimensions and 1 fact table.
        Returns dict with all star schema tables.
        """
        logger.info(f"Building complete star schema using {self.engine} engine")
        
        schema_tables = {}
        
        # Build all 5 dimension tables
        schema_tables['dim_date'] = self.build_dim_date(silver_df)
        schema_tables['dim_product'] = self.build_dim_product(silver_df)
        schema_tables['dim_customer'] = self.build_dim_customer(silver_df)
        schema_tables['dim_country'] = self.build_dim_country(silver_df)
        schema_tables['dim_invoice'] = self.build_dim_invoice(silver_df)
        
        # Build fact table with foreign keys to all dimensions
        schema_tables['fact_sales'] = self.build_fact_sales(
            silver_df,
            schema_tables['dim_date'],
            schema_tables['dim_product'],
            schema_tables['dim_customer'],
            schema_tables['dim_country'],
            schema_tables['dim_invoice']
        )
        
        logger.info("Star schema build completed successfully")
        return schema_tables
    
    def build_dim_date(self, df: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']) -> Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']:
        """
        Build comprehensive Date dimension with fiscal periods.
        """
        logger.info("Building Date dimension")
        
        if self.engine == "pandas":
            return self._build_dim_date_pandas(df)
        elif self.engine == "polars":
            return self._build_dim_date_polars(df)
        elif self.engine == "spark":
            return self._build_dim_date_spark(df)
    
    def _build_dim_date_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build Date dimension using Pandas."""
        # Extract unique dates
        dates = pd.to_datetime(df['invoice_date']).dt.date.unique()
        
        date_records = []
        for d in dates:
            date_key = self._generate_surrogate_key('date', str(d))
            
            # Calculate fiscal year (assuming April 1st start)
            fiscal_year = d.year if d.month >= 4 else d.year - 1
            fiscal_quarter = ((d.month - 4) % 12) // 3 + 1 if d.month >= 4 else ((d.month + 8) % 12) // 3 + 1
            
            date_records.append({
                'date_key': date_key,
                'date': d,
                'year': d.year,
                'month': d.month,
                'day': d.day,
                'quarter': (d.month - 1) // 3 + 1,
                'fiscal_year': fiscal_year,
                'fiscal_quarter': fiscal_quarter,
                'day_of_week': d.weekday() + 1,
                'day_name': d.strftime('%A'),
                'month_name': d.strftime('%B'),
                'is_weekend': d.weekday() >= 5,
                'is_holiday': self._is_holiday(d),
                'week_of_year': d.isocalendar()[1]
            })
        
        # Add unknown member
        date_records.append(self._get_unknown_date_member())
        
        return pd.DataFrame(date_records)
    
    def _build_dim_date_polars(self, df: 'pl.DataFrame') -> 'pl.DataFrame':
        """Build Date dimension using Polars."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars not available")
            
        # Extract unique dates and build dimension
        unique_dates = df.select(pl.col('invoice_date').cast(pl.Date).unique()).to_pandas()['invoice_date']
        
        date_data = []
        for d in unique_dates:
            if pd.isna(d):
                continue
            date_key = self._generate_surrogate_key('date', str(d))
            fiscal_year = d.year if d.month >= 4 else d.year - 1
            fiscal_quarter = ((d.month - 4) % 12) // 3 + 1 if d.month >= 4 else ((d.month + 8) % 12) // 3 + 1
            
            date_data.append({
                'date_key': date_key,
                'date': d,
                'year': d.year,
                'month': d.month,
                'day': d.day,
                'quarter': (d.month - 1) // 3 + 1,
                'fiscal_year': fiscal_year,
                'fiscal_quarter': fiscal_quarter,
                'day_of_week': d.weekday() + 1,
                'is_weekend': d.weekday() >= 5,
                'is_holiday': self._is_holiday(d)
            })
        
        # Add unknown member
        date_data.append(self._get_unknown_date_member())
        
        return pl.DataFrame(date_data)
    
    def _build_dim_date_spark(self, df: 'SparkDataFrame') -> 'SparkDataFrame':
        """Build Date dimension using Spark."""
        if not SPARK_AVAILABLE:
            raise ImportError("Spark not available")
            
        spark = df.sparkSession
        
        # Extract unique dates using DataFrame API
        unique_dates_df = df.select(F.col('invoice_date').cast('date').alias('date')).distinct()
        
        # Add date dimension attributes using DataFrame API
        dim_date = unique_dates_df.withColumns({
            'date_key': F.sha2(F.col('date').cast('string'), 256),
            'year': F.year('date'),
            'month': F.month('date'),
            'day': F.dayofmonth('date'),
            'quarter': F.quarter('date'),
            'fiscal_year': F.when(F.month('date') >= 4, F.year('date')).otherwise(F.year('date') - 1),
            'fiscal_quarter': F.when(F.month('date') >= 4, 
                                   F.ceil((F.month('date') - 3) / 3)).otherwise(
                                   F.ceil((F.month('date') + 9) / 3)),
            'day_of_week': F.dayofweek('date'),
            'day_name': F.date_format('date', 'EEEE'),
            'month_name': F.date_format('date', 'MMMM'),
            'is_weekend': F.dayofweek('date').isin([1, 7]),  # Sunday=1, Saturday=7
            'is_holiday': F.lit(False),  # Simplified - could be enhanced
            'week_of_year': F.weekofyear('date')
        })
        
        # Add unknown member using DataFrame API
        unknown_date = spark.createDataFrame([self._get_unknown_date_member()])
        return dim_date.union(unknown_date)
    
    def build_dim_product(self, df: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']) -> Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']:
        """Build Product dimension with categories and attributes."""
        logger.info("Building Product dimension")
        
        if self.engine == "pandas":
            return self._build_dim_product_pandas(df)
        elif self.engine == "polars":
            return self._build_dim_product_polars(df)
        elif self.engine == "spark":
            return self._build_dim_product_spark(df)
    
    def _build_dim_product_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build Product dimension using Pandas."""
        # Get unique products using DataFrame operations
        products = df[['stock_code', 'description']].drop_duplicates()
        
        product_records = []
        for _, row in products.iterrows():
            product_key = self._generate_surrogate_key('product', row['stock_code'])
            
            # Extract product category from description (business logic)
            category = self._categorize_product(row['description'])
            brand = self._extract_brand(row['description'])
            
            product_records.append({
                'product_key': product_key,
                'stock_code': row['stock_code'],
                'description': row['description'],
                'category': category,
                'brand': brand,
                'is_active': True,
                'created_date': datetime.utcnow().date(),
                'last_updated': datetime.utcnow()
            })
        
        # Add unknown member
        product_records.append(self._get_unknown_product_member())
        
        return pd.DataFrame(product_records)
    
    def _build_dim_product_polars(self, df: 'pl.DataFrame') -> 'pl.DataFrame':
        """Build Product dimension using Polars."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars not available")
            
        # Get unique products
        products = df.select(['stock_code', 'description']).unique()
        products_pandas = products.to_pandas()
        
        product_data = []
        for _, row in products_pandas.iterrows():
            product_key = self._generate_surrogate_key('product', row['stock_code'])
            category = self._categorize_product(row['description'])
            brand = self._extract_brand(row['description'])
            
            product_data.append({
                'product_key': product_key,
                'stock_code': row['stock_code'],
                'description': row['description'],
                'category': category,
                'brand': brand,
                'is_active': True,
                'created_date': datetime.utcnow().date(),
                'last_updated': datetime.utcnow()
            })
        
        # Add unknown member
        product_data.append(self._get_unknown_product_member())
        
        return pl.DataFrame(product_data)
    
    def _build_dim_product_spark(self, df: 'SparkDataFrame') -> 'SparkDataFrame':
        """Build Product dimension using Spark."""
        if not SPARK_AVAILABLE:
            raise ImportError("Spark not available")
            
        spark = df.sparkSession
        
        # Get unique products using DataFrame API
        products = df.select('stock_code', 'description').distinct()
        
        # Add dimension attributes using DataFrame API
        dim_product = products.withColumns({
            'product_key': F.sha2(F.col('stock_code'), 256),
            'category': self._categorize_product_spark(F.col('description')),
            'brand': self._extract_brand_spark(F.col('description')),
            'is_active': F.lit(True),
            'created_date': F.current_date(),
            'last_updated': F.current_timestamp()
        })
        
        # Add unknown member
        unknown_product = spark.createDataFrame([self._get_unknown_product_member()])
        return dim_product.union(unknown_product)
    
    def build_dim_customer(self, df: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']) -> Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']:
        """Build Customer dimension with segments and lifetime value."""
        logger.info("Building Customer dimension")
        
        if self.engine == "pandas":
            return self._build_dim_customer_pandas(df)
        elif self.engine == "polars":
            return self._build_dim_customer_polars(df)
        elif self.engine == "spark":
            return self._build_dim_customer_spark(df)
    
    def _build_dim_customer_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build Customer dimension using Pandas."""
        # Calculate customer metrics using DataFrame operations
        customer_metrics = df.groupby('customer_id').agg({
            'total_amount': ['sum', 'mean', 'count'],
            'invoice_date': ['min', 'max']
        }).round(2)
        
        customer_metrics.columns = ['lifetime_value', 'avg_order_value', 'order_count', 'first_order_date', 'last_order_date']
        customer_metrics = customer_metrics.reset_index()
        
        customer_records = []
        for _, row in customer_metrics.iterrows():
            customer_key = self._generate_surrogate_key('customer', str(row['customer_id']))
            segment = self._determine_customer_segment(row['lifetime_value'], row['order_count'])
            
            customer_records.append({
                'customer_key': customer_key,
                'customer_id': row['customer_id'],
                'customer_segment': segment,
                'lifetime_value': row['lifetime_value'],
                'avg_order_value': row['avg_order_value'],
                'order_count': row['order_count'],
                'first_order_date': row['first_order_date'],
                'last_order_date': row['last_order_date'],
                'is_active': True,
                'created_date': datetime.utcnow().date()
            })
        
        # Add unknown member
        customer_records.append(self._get_unknown_customer_member())
        
        return pd.DataFrame(customer_records)
    
    def _build_dim_customer_polars(self, df: 'pl.DataFrame') -> 'pl.DataFrame:
        """Build Customer dimension using Polars."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars not available")
            
        # Calculate customer metrics
        customer_metrics = df.group_by('customer_id').agg([
            pl.col('total_amount').sum().alias('lifetime_value'),
            pl.col('total_amount').mean().alias('avg_order_value'),
            pl.col('total_amount').count().alias('order_count'),
            pl.col('invoice_date').min().alias('first_order_date'),
            pl.col('invoice_date').max().alias('last_order_date')
        ])
        
        customer_data = []
        for row in customer_metrics.to_dicts():
            customer_key = self._generate_surrogate_key('customer', str(row['customer_id']))
            segment = self._determine_customer_segment(row['lifetime_value'], row['order_count'])
            
            customer_data.append({
                'customer_key': customer_key,
                'customer_id': row['customer_id'],
                'customer_segment': segment,
                'lifetime_value': row['lifetime_value'],
                'avg_order_value': row['avg_order_value'],
                'order_count': row['order_count'],
                'first_order_date': row['first_order_date'],
                'last_order_date': row['last_order_date'],
                'is_active': True,
                'created_date': datetime.utcnow().date()
            })
        
        # Add unknown member
        customer_data.append(self._get_unknown_customer_member())
        
        return pl.DataFrame(customer_data)
    
    def _build_dim_customer_spark(self, df: 'SparkDataFrame') -> 'SparkDataFrame':
        """Build Customer dimension using Spark."""
        if not SPARK_AVAILABLE:
            raise ImportError("Spark not available")
            
        spark = df.sparkSession
        
        # Calculate customer metrics using DataFrame API
        customer_metrics = df.groupBy('customer_id').agg(
            F.sum('total_amount').alias('lifetime_value'),
            F.mean('total_amount').alias('avg_order_value'),
            F.count('total_amount').alias('order_count'),
            F.min('invoice_date').alias('first_order_date'),
            F.max('invoice_date').alias('last_order_date')
        )
        
        # Add dimension attributes using DataFrame API
        dim_customer = customer_metrics.withColumns({
            'customer_key': F.sha2(F.col('customer_id').cast('string'), 256),
            'customer_segment': self._determine_customer_segment_spark(F.col('lifetime_value'), F.col('order_count')),
            'is_active': F.lit(True),
            'created_date': F.current_date()
        })
        
        # Add unknown member
        unknown_customer = spark.createDataFrame([self._get_unknown_customer_member()])
        return dim_customer.union(unknown_customer)
    
    def build_dim_country(self, df: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']) -> Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']:
        """Build Country dimension with geographic hierarchy."""
        logger.info("Building Country dimension")
        
        if self.engine == "pandas":
            return self._build_dim_country_pandas(df)
        elif self.engine == "polars":
            return self._build_dim_country_polars(df)
        elif self.engine == "spark":
            return self._build_dim_country_spark(df)
    
    def _build_dim_country_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build Country dimension using Pandas."""
        countries = df['country'].unique()
        
        country_records = []
        for country in countries:
            country_key = self._generate_surrogate_key('country', country)
            country_info = self._get_country_info(country)
            
            country_records.append({
                'country_key': country_key,
                'country_name': country,
                'country_code': country_info['code'],
                'region': country_info['region'],
                'continent': country_info['continent'],
                'is_eu': country_info['is_eu'],
                'currency': country_info['currency'],
                'timezone': country_info['timezone']
            })
        
        # Add unknown member
        country_records.append(self._get_unknown_country_member())
        
        return pd.DataFrame(country_records)
    
    def _build_dim_country_polars(self, df: 'pl.DataFrame') -> 'pl.DataFrame':
        """Build Country dimension using Polars."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars not available")
            
        countries = df.select(pl.col('country').unique()).to_pandas()['country']
        
        country_data = []
        for country in countries:
            country_key = self._generate_surrogate_key('country', country)
            country_info = self._get_country_info(country)
            
            country_data.append({
                'country_key': country_key,
                'country_name': country,
                'country_code': country_info['code'],
                'region': country_info['region'],
                'continent': country_info['continent'],
                'is_eu': country_info['is_eu'],
                'currency': country_info['currency'],
                'timezone': country_info['timezone']
            })
        
        # Add unknown member
        country_data.append(self._get_unknown_country_member())
        
        return pl.DataFrame(country_data)
    
    def _build_dim_country_spark(self, df: 'SparkDataFrame') -> 'SparkDataFrame':
        """Build Country dimension using Spark."""
        if not SPARK_AVAILABLE:
            raise ImportError("Spark not available")
            
        spark = df.sparkSession
        
        # Get unique countries using DataFrame API
        countries = df.select('country').distinct()
        
        # Add dimension attributes using DataFrame API and UDFs
        country_info_udf = F.udf(self._get_country_info, StructType([
            StructField("code", StringType()),
            StructField("region", StringType()),
            StructField("continent", StringType()),
            StructField("is_eu", StringType()),
            StructField("currency", StringType()),
            StructField("timezone", StringType())
        ]))
        
        dim_country = countries.withColumn("country_info", country_info_udf(F.col("country"))).select(
            F.sha2(F.col('country'), 256).alias('country_key'),
            F.col('country').alias('country_name'),
            F.col('country_info.code').alias('country_code'),
            F.col('country_info.region').alias('region'),
            F.col('country_info.continent').alias('continent'),
            F.col('country_info.is_eu').cast('boolean').alias('is_eu'),
            F.col('country_info.currency').alias('currency'),
            F.col('country_info.timezone').alias('timezone')
        )
        
        # Add unknown member
        unknown_country = spark.createDataFrame([self._get_unknown_country_member()])
        return dim_country.union(unknown_country)
    
    def build_dim_invoice(self, df: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']) -> Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']:
        """Build Invoice dimension with invoice-level metrics."""
        logger.info("Building Invoice dimension")
        
        if self.engine == "pandas":
            return self._build_dim_invoice_pandas(df)
        elif self.engine == "polars":
            return self._build_dim_invoice_polars(df)
        elif self.engine == "spark":
            return self._build_dim_invoice_spark(df)
    
    def _build_dim_invoice_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build Invoice dimension using Pandas."""
        # Calculate invoice-level metrics using DataFrame operations
        invoice_metrics = df.groupby('invoice_no').agg({
            'total_amount': ['sum', 'count'],
            'invoice_date': 'first',
            'customer_id': 'first',
            'country': 'first'
        })
        
        invoice_metrics.columns = ['invoice_total', 'line_count', 'invoice_date', 'customer_id', 'country']
        invoice_metrics = invoice_metrics.reset_index()
        
        invoice_records = []
        for _, row in invoice_metrics.iterrows():
            invoice_key = self._generate_surrogate_key('invoice', row['invoice_no'])
            
            invoice_records.append({
                'invoice_key': invoice_key,
                'invoice_no': row['invoice_no'],
                'invoice_date': row['invoice_date'],
                'invoice_total': row['invoice_total'],
                'line_count': row['line_count'],
                'customer_id': row['customer_id'],
                'country': row['country'],
                'is_cancelled': row['invoice_no'].startswith('C') if isinstance(row['invoice_no'], str) else False,
                'created_timestamp': datetime.utcnow()
            })
        
        # Add unknown member
        invoice_records.append(self._get_unknown_invoice_member())
        
        return pd.DataFrame(invoice_records)
    
    def _build_dim_invoice_polars(self, df: 'pl.DataFrame') -> 'pl.DataFrame':
        """Build Invoice dimension using Polars."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars not available")
            
        # Calculate invoice metrics
        invoice_metrics = df.group_by('invoice_no').agg([
            pl.col('total_amount').sum().alias('invoice_total'),
            pl.col('total_amount').count().alias('line_count'),
            pl.col('invoice_date').first().alias('invoice_date'),
            pl.col('customer_id').first().alias('customer_id'),
            pl.col('country').first().alias('country')
        ])
        
        invoice_data = []
        for row in invoice_metrics.to_dicts():
            invoice_key = self._generate_surrogate_key('invoice', row['invoice_no'])
            
            invoice_data.append({
                'invoice_key': invoice_key,
                'invoice_no': row['invoice_no'],
                'invoice_date': row['invoice_date'],
                'invoice_total': row['invoice_total'],
                'line_count': row['line_count'],
                'customer_id': row['customer_id'],
                'country': row['country'],
                'is_cancelled': row['invoice_no'].startswith('C') if isinstance(row['invoice_no'], str) else False,
                'created_timestamp': datetime.utcnow()
            })
        
        # Add unknown member
        invoice_data.append(self._get_unknown_invoice_member())
        
        return pl.DataFrame(invoice_data)
    
    def _build_dim_invoice_spark(self, df: 'SparkDataFrame') -> 'SparkDataFrame':
        """Build Invoice dimension using Spark."""
        if not SPARK_AVAILABLE:
            raise ImportError("Spark not available")
            
        spark = df.sparkSession
        
        # Calculate invoice metrics using DataFrame API
        invoice_metrics = df.groupBy('invoice_no').agg(
            F.sum('total_amount').alias('invoice_total'),
            F.count('total_amount').alias('line_count'),
            F.first('invoice_date').alias('invoice_date'),
            F.first('customer_id').alias('customer_id'),
            F.first('country').alias('country')
        )
        
        # Add dimension attributes using DataFrame API
        dim_invoice = invoice_metrics.withColumns({
            'invoice_key': F.sha2(F.col('invoice_no'), 256),
            'is_cancelled': F.col('invoice_no').startswith('C'),
            'created_timestamp': F.current_timestamp()
        })
        
        # Add unknown member
        unknown_invoice = spark.createDataFrame([self._get_unknown_invoice_member()])
        return dim_invoice.union(unknown_invoice)
    
    def build_fact_sales(self, silver_df: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame'],
                        dim_date: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame'],
                        dim_product: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame'],
                        dim_customer: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame'],
                        dim_country: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame'],
                        dim_invoice: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']) -> Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']:
        """
        Build Fact Sales table with foreign keys to all 5 dimensions.
        """
        logger.info("Building Fact Sales table")
        
        if self.engine == "pandas":
            return self._build_fact_sales_pandas(silver_df, dim_date, dim_product, dim_customer, dim_country, dim_invoice)
        elif self.engine == "polars":
            return self._build_fact_sales_polars(silver_df, dim_date, dim_product, dim_customer, dim_country, dim_invoice)
        elif self.engine == "spark":
            return self._build_fact_sales_spark(silver_df, dim_date, dim_product, dim_customer, dim_country, dim_invoice)
    
    def _build_fact_sales_pandas(self, silver_df: pd.DataFrame, dim_date: pd.DataFrame, 
                                dim_product: pd.DataFrame, dim_customer: pd.DataFrame,
                                dim_country: pd.DataFrame, dim_invoice: pd.DataFrame) -> pd.DataFrame:
        """Build Fact Sales using Pandas with proper joins."""
        
        # Start with silver data
        fact_sales = silver_df.copy()
        
        # Add surrogate key for fact table
        fact_sales['sale_id'] = [self._generate_surrogate_key('sale', f"{row['invoice_no']}_{row['stock_code']}_{i}") 
                                for i, (_, row) in enumerate(fact_sales.iterrows())]
        
        # Join with Date dimension using DataFrame API
        fact_sales['join_date'] = pd.to_datetime(fact_sales['invoice_date']).dt.date
        dim_date['join_date'] = dim_date['date']
        fact_sales = fact_sales.merge(dim_date[['date_key', 'join_date']], on='join_date', how='left')
        fact_sales['date_key'] = fact_sales['date_key'].fillna(-1)  # Unknown member
        
        # Join with Product dimension
        fact_sales = fact_sales.merge(dim_product[['product_key', 'stock_code']], on='stock_code', how='left')
        fact_sales['product_key'] = fact_sales['product_key'].fillna(-1)
        
        # Join with Customer dimension
        fact_sales = fact_sales.merge(dim_customer[['customer_key', 'customer_id']], on='customer_id', how='left')
        fact_sales['customer_key'] = fact_sales['customer_key'].fillna(-1)
        
        # Join with Country dimension
        fact_sales = fact_sales.merge(dim_country[['country_key', 'country_name']], 
                                     left_on='country', right_on='country_name', how='left')
        fact_sales['country_key'] = fact_sales['country_key'].fillna(-1)
        
        # Join with Invoice dimension
        fact_sales = fact_sales.merge(dim_invoice[['invoice_key', 'invoice_no']], on='invoice_no', how='left')
        fact_sales['invoice_key'] = fact_sales['invoice_key'].fillna(-1)
        
        # Select final fact table columns
        fact_columns = [
            'sale_id', 'date_key', 'product_key', 'customer_key', 'country_key', 'invoice_key',
            'quantity', 'unit_price', 'total_amount', 'discount_amount', 'tax_amount',
            'profit_amount', 'margin_percentage', 'created_at'
        ]
        
        # Add derived measures
        fact_sales['discount_amount'] = 0.0  # Could be calculated based on business rules
        fact_sales['tax_amount'] = fact_sales['total_amount'] * 0.2  # Example 20% tax
        fact_sales['profit_amount'] = fact_sales['total_amount'] * 0.3  # Example 30% profit margin
        fact_sales['margin_percentage'] = 30.0
        fact_sales['created_at'] = datetime.utcnow()
        
        return fact_sales[fact_columns]
    
    def _build_fact_sales_polars(self, silver_df: 'pl.DataFrame', dim_date: 'pl.DataFrame',
                                dim_product: 'pl.DataFrame', dim_customer: 'pl.DataFrame',
                                dim_country: 'pl.DataFrame', dim_invoice: 'pl.DataFrame') -> 'pl.DataFrame':
        """Build Fact Sales using Polars with proper joins."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars not available")
            
        # Add sale_id
        fact_sales = silver_df.with_row_count('row_idx').with_columns([
            pl.concat_str([pl.col('invoice_no'), pl.col('stock_code'), pl.col('row_idx')], separator='_').alias('sale_key')
        ])
        
        # Join with dimensions using Polars join API
        fact_sales = (fact_sales
            .join(dim_date.select(['date_key', 'date']), 
                  left_on=pl.col('invoice_date').cast(pl.Date), 
                  right_on='date', how='left')
            .join(dim_product.select(['product_key', 'stock_code']), on='stock_code', how='left')
            .join(dim_customer.select(['customer_key', 'customer_id']), on='customer_id', how='left')
            .join(dim_country.select(['country_key', 'country_name']), 
                  left_on='country', right_on='country_name', how='left')
            .join(dim_invoice.select(['invoice_key', 'invoice_no']), on='invoice_no', how='left')
        )
        
        # Add derived measures and finalize
        fact_sales = fact_sales.with_columns([
            pl.col('sale_key').alias('sale_id'),
            pl.col('date_key').fill_null(-1),
            pl.col('product_key').fill_null(-1),
            pl.col('customer_key').fill_null(-1),
            pl.col('country_key').fill_null(-1),
            pl.col('invoice_key').fill_null(-1),
            pl.lit(0.0).alias('discount_amount'),
            (pl.col('total_amount') * 0.2).alias('tax_amount'),
            (pl.col('total_amount') * 0.3).alias('profit_amount'),
            pl.lit(30.0).alias('margin_percentage'),
            pl.lit(datetime.utcnow()).alias('created_at')
        ])
        
        return fact_sales.select([
            'sale_id', 'date_key', 'product_key', 'customer_key', 'country_key', 'invoice_key',
            'quantity', 'unit_price', 'total_amount', 'discount_amount', 'tax_amount',
            'profit_amount', 'margin_percentage', 'created_at'
        ])
    
    def _build_fact_sales_spark(self, silver_df: 'SparkDataFrame', dim_date: 'SparkDataFrame',
                               dim_product: 'SparkDataFrame', dim_customer: 'SparkDataFrame',
                               dim_country: 'SparkDataFrame', dim_invoice: 'SparkDataFrame') -> 'SparkDataFrame':
        """Build Fact Sales using Spark with proper joins."""
        if not SPARK_AVAILABLE:
            raise ImportError("Spark not available")
            
        # Add sale_id using DataFrame API
        fact_sales = silver_df.withColumn('sale_id', 
            F.sha2(F.concat(F.col('invoice_no'), F.lit('_'), F.col('stock_code'), F.lit('_'), F.monotonically_increasing_id()), 256))
        
        # Join with dimensions using DataFrame API
        fact_sales = (fact_sales
            .join(dim_date.select('date_key', 'date'), 
                  fact_sales.invoice_date.cast('date') == dim_date.date, 'left')
            .join(dim_product.select('product_key', 'stock_code'), 'stock_code', 'left')
            .join(dim_customer.select('customer_key', 'customer_id'), 'customer_id', 'left')
            .join(dim_country.select('country_key', 'country_name'), 
                  fact_sales.country == dim_country.country_name, 'left')
            .join(dim_invoice.select('invoice_key', 'invoice_no'), 'invoice_no', 'left')
        )
        
        # Add derived measures using DataFrame API
        fact_sales = fact_sales.withColumns({
            'date_key': F.coalesce(F.col('date_key'), F.lit(-1)),
            'product_key': F.coalesce(F.col('product_key'), F.lit(-1)),
            'customer_key': F.coalesce(F.col('customer_key'), F.lit(-1)),
            'country_key': F.coalesce(F.col('country_key'), F.lit(-1)),
            'invoice_key': F.coalesce(F.col('invoice_key'), F.lit(-1)),
            'discount_amount': F.lit(0.0),
            'tax_amount': F.col('total_amount') * 0.2,
            'profit_amount': F.col('total_amount') * 0.3,
            'margin_percentage': F.lit(30.0),
            'created_at': F.current_timestamp()
        })
        
        return fact_sales.select(
            'sale_id', 'date_key', 'product_key', 'customer_key', 'country_key', 'invoice_key',
            'quantity', 'unit_price', 'total_amount', 'discount_amount', 'tax_amount',
            'profit_amount', 'margin_percentage', 'created_at'
        )
    
    # Helper methods for business logic
    
    def _generate_surrogate_key(self, dimension: str, natural_key: str) -> int:
        """Generate consistent surrogate keys using SHA256 hash."""
        if natural_key in self.dimension_cache[dimension]:
            return self.dimension_cache[dimension][natural_key]
        
        # Use hash of the natural key for consistent surrogate key generation
        hash_value = hashlib.sha256(f"{dimension}_{natural_key}".encode()).hexdigest()
        surrogate_key = int(hash_value[:8], 16)  # Use first 8 hex chars as integer
        
        self.dimension_cache[dimension][natural_key] = surrogate_key
        return surrogate_key
    
    def _categorize_product(self, description: str) -> str:
        """Categorize product based on description."""
        if not description or pd.isna(description):
            return "Unknown"
        
        desc_lower = str(description).lower()
        
        if any(word in desc_lower for word in ['bag', 'handbag', 'tote']):
            return "Bags & Accessories"
        elif any(word in desc_lower for word in ['mug', 'cup', 'bottle']):
            return "Drinkware"
        elif any(word in desc_lower for word in ['card', 'notebook', 'pen']):
            return "Stationery"
        elif any(word in desc_lower for word in ['candle', 'light', 'lamp']):
            return "Home & Garden"
        elif any(word in desc_lower for word in ['toy', 'game', 'doll']):
            return "Toys & Games"
        else:
            return "General Merchandise"
    
    def _extract_brand(self, description: str) -> str:
        """Extract brand from product description."""
        if not description or pd.isna(description):
            return "Unknown"
        
        # Simple brand extraction logic - could be enhanced
        desc_parts = str(description).split()
        if len(desc_parts) > 0:
            return desc_parts[0].upper()
        return "Unknown"
    
    def _determine_customer_segment(self, lifetime_value: float, order_count: int) -> str:
        """Determine customer segment based on lifetime value and order frequency."""
        if lifetime_value >= 1000 and order_count >= 10:
            return "VIP"
        elif lifetime_value >= 500 and order_count >= 5:
            return "Premium"
        elif lifetime_value >= 100 and order_count >= 2:
            return "Regular"
        else:
            return "New"
    
    def _get_country_info(self, country: str) -> Dict[str, str]:
        """Get country information - simplified mapping."""
        country_mapping = {
            "United Kingdom": {"code": "GB", "region": "Western Europe", "continent": "Europe", 
                             "is_eu": "false", "currency": "GBP", "timezone": "GMT"},
            "Germany": {"code": "DE", "region": "Western Europe", "continent": "Europe",
                       "is_eu": "true", "currency": "EUR", "timezone": "CET"},
            "France": {"code": "FR", "region": "Western Europe", "continent": "Europe",
                      "is_eu": "true", "currency": "EUR", "timezone": "CET"},
            "USA": {"code": "US", "region": "North America", "continent": "North America",
                   "is_eu": "false", "currency": "USD", "timezone": "EST"},
        }
        
        return country_mapping.get(country, {
            "code": "XX", "region": "Unknown", "continent": "Unknown",
            "is_eu": "false", "currency": "XXX", "timezone": "UTC"
        })
    
    def _is_holiday(self, date_val: date) -> bool:
        """Check if date is a holiday - simplified logic."""
        # Simple holiday check - could be enhanced with proper holiday calendar
        return (date_val.month == 12 and date_val.day == 25) or \
               (date_val.month == 1 and date_val.day == 1)
    
    # Unknown member definitions for SCD Type 2 support
    
    def _get_unknown_date_member(self) -> Dict[str, Any]:
        """Return unknown member for Date dimension."""
        return {
            'date_key': -1,
            'date': date(1900, 1, 1),
            'year': 1900,
            'month': 1,
            'day': 1,
            'quarter': 1,
            'fiscal_year': 1900,
            'fiscal_quarter': 1,
            'day_of_week': 1,
            'day_name': 'Unknown',
            'month_name': 'Unknown',
            'is_weekend': False,
            'is_holiday': False,
            'week_of_year': 1
        }
    
    def _get_unknown_product_member(self) -> Dict[str, Any]:
        """Return unknown member for Product dimension."""
        return {
            'product_key': -1,
            'stock_code': 'UNKNOWN',
            'description': 'Unknown Product',
            'category': 'Unknown',
            'brand': 'Unknown',
            'is_active': False,
            'created_date': date(1900, 1, 1),
            'last_updated': datetime(1900, 1, 1)
        }
    
    def _get_unknown_customer_member(self) -> Dict[str, Any]:
        """Return unknown member for Customer dimension."""
        return {
            'customer_key': -1,
            'customer_id': 'UNKNOWN',
            'customer_segment': 'Unknown',
            'lifetime_value': 0.0,
            'avg_order_value': 0.0,
            'order_count': 0,
            'first_order_date': date(1900, 1, 1),
            'last_order_date': date(1900, 1, 1),
            'is_active': False,
            'created_date': date(1900, 1, 1)
        }
    
    def _get_unknown_country_member(self) -> Dict[str, Any]:
        """Return unknown member for Country dimension."""
        return {
            'country_key': -1,
            'country_name': 'Unknown',
            'country_code': 'XX',
            'region': 'Unknown',
            'continent': 'Unknown',
            'is_eu': False,
            'currency': 'XXX',
            'timezone': 'UTC'
        }
    
    def _get_unknown_invoice_member(self) -> Dict[str, Any]:
        """Return unknown member for Invoice dimension."""
        return {
            'invoice_key': -1,
            'invoice_no': 'UNKNOWN',
            'invoice_date': date(1900, 1, 1),
            'invoice_total': 0.0,
            'line_count': 0,
            'customer_id': 'UNKNOWN',
            'country': 'Unknown',
            'is_cancelled': False,
            'created_timestamp': datetime(1900, 1, 1)
        }
    
    # Spark UDF helper methods
    
    def _categorize_product_spark(self, description_col):
        """Spark UDF for product categorization."""
        return F.when(description_col.contains("bag"), "Bags & Accessories") \
                .when(description_col.contains("mug"), "Drinkware") \
                .when(description_col.contains("card"), "Stationery") \
                .when(description_col.contains("candle"), "Home & Garden") \
                .when(description_col.contains("toy"), "Toys & Games") \
                .otherwise("General Merchandise")
    
    def _extract_brand_spark(self, description_col):
        """Spark UDF for brand extraction."""
        return F.when(description_col.isNotNull(), 
                     F.upper(F.split(description_col, " ")[0])) \
                .otherwise("Unknown")
    
    def _determine_customer_segment_spark(self, ltv_col, count_col):
        """Spark UDF for customer segmentation."""
        return F.when((ltv_col >= 1000) & (count_col >= 10), "VIP") \
                .when((ltv_col >= 500) & (count_col >= 5), "Premium") \
                .when((ltv_col >= 100) & (count_col >= 2), "Regular") \
                .otherwise("New")
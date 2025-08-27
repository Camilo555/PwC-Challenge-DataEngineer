"""
Real-time Analytics and ML Feature Engineering
Provides comprehensive real-time analytics, machine learning feature engineering, and model serving capabilities
"""
from __future__ import annotations

import json
import uuid
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, expr, lit, struct, when, coalesce,
    window, count, countDistinct, sum as spark_sum, avg, 
    max as spark_max, min as spark_min, stddev, variance,
    lag, lead, first, last, rank, dense_rank, row_number,
    collect_list, array, map_from_arrays, explode,
    udf, pandas_udf, broadcast,
    year, month, day, hour, minute, dayofweek
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    StringType, StructField, StructType, TimestampType,
    DoubleType, IntegerType, BooleanType, LongType, ArrayType, 
    MapType, DecimalType, FloatType
)
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StandardScaler, MinMaxScaler, PCA,
    StringIndexer, OneHotEncoder, Bucketizer, QuantileDiscretizer
)
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from etl.spark.delta_lake_manager import DeltaLakeManager
from monitoring.advanced_metrics import get_metrics_collector
from src.streaming.kafka_manager import KafkaManager, StreamingTopic


class FeatureType(Enum):
    """Types of ML features"""
    NUMERICAL = "numerical"
    CATEGORICAL = "categorical"
    TEMPORAL = "temporal"
    BEHAVIORAL = "behavioral"
    AGGREGATE = "aggregate"
    DERIVED = "derived"
    STATISTICAL = "statistical"


class ModelType(Enum):
    """Types of ML models"""
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"
    ANOMALY_DETECTION = "anomaly_detection"
    RECOMMENDATION = "recommendation"
    TIME_SERIES = "time_series"


class ScalingMethod(Enum):
    """Feature scaling methods"""
    STANDARD = "standard"
    MINMAX = "minmax"
    ROBUST = "robust"
    NONE = "none"


@dataclass
class FeatureDefinition:
    """Definition for a ML feature"""
    name: str
    feature_type: FeatureType
    expression: str
    data_type: str
    scaling_method: ScalingMethod = ScalingMethod.NONE
    window_duration: Optional[str] = None
    description: str = ""
    business_impact: str = "medium"
    computation_cost: str = "low"
    enabled: bool = True


@dataclass
class ModelDefinition:
    """Definition for a ML model"""
    name: str
    model_type: ModelType
    target_column: str
    feature_columns: List[str]
    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    evaluation_metrics: List[str] = field(default_factory=list)
    retrain_frequency: str = "daily"
    prediction_threshold: float = 0.5
    enabled: bool = True


@dataclass
class RealTimeAnalyticsConfig:
    """Configuration for real-time analytics and ML"""
    enable_feature_engineering: bool = True
    enable_model_serving: bool = True
    enable_anomaly_detection: bool = True
    enable_behavioral_analytics: bool = True
    feature_store_enabled: bool = True
    model_refresh_interval: str = "1 hour"
    feature_computation_parallelism: int = 4
    prediction_batch_size: int = 1000
    model_performance_threshold: float = 0.8


class FeatureStore:
    """Real-time feature store for ML features"""
    
    def __init__(self, spark: SparkSession, delta_manager: DeltaLakeManager):
        self.spark = spark
        self.delta_manager = delta_manager
        self.logger = get_logger(__name__)
        
        # Feature registry
        self.features: Dict[str, FeatureDefinition] = {}
        self.feature_groups: Dict[str, List[str]] = {}
        
        # Feature store paths
        self.feature_store_path = Path("./data/feature_store")
        self.feature_store_path.mkdir(parents=True, exist_ok=True)
        
        self._load_builtin_features()
        
    def _load_builtin_features(self):
        """Load built-in feature definitions"""
        
        # Customer behavioral features
        customer_features = [
            FeatureDefinition(
                name="customer_transaction_frequency_7d",
                feature_type=FeatureType.BEHAVIORAL,
                expression="count(*) over (partition by customer_id_clean order by kafka_timestamp range between interval 7 days preceding and current row)",
                data_type="integer",
                window_duration="7 days",
                description="Customer transaction frequency in last 7 days",
                business_impact="high"
            ),
            FeatureDefinition(
                name="customer_avg_order_value_30d",
                feature_type=FeatureType.BEHAVIORAL,
                expression="avg(line_total) over (partition by customer_id_clean order by kafka_timestamp range between interval 30 days preceding and current row)",
                data_type="double",
                scaling_method=ScalingMethod.STANDARD,
                window_duration="30 days",
                description="Customer average order value in last 30 days"
            ),
            FeatureDefinition(
                name="customer_recency_days",
                feature_type=FeatureType.BEHAVIORAL,
                expression="datediff(current_date(), max(date(kafka_timestamp)) over (partition by customer_id_clean order by kafka_timestamp rows between unbounded preceding and 1 preceding))",
                data_type="integer",
                description="Days since customer's last transaction"
            ),
            FeatureDefinition(
                name="customer_monetary_total",
                feature_type=FeatureType.BEHAVIORAL,
                expression="sum(line_total) over (partition by customer_id_clean order by kafka_timestamp rows between unbounded preceding and current row)",
                data_type="double",
                scaling_method=ScalingMethod.MINMAX,
                description="Customer total monetary value"
            )
        ]
        
        # Product performance features
        product_features = [
            FeatureDefinition(
                name="product_popularity_score",
                feature_type=FeatureType.AGGREGATE,
                expression="count(*) over (partition by stock_code order by kafka_timestamp range between interval 1 hour preceding and current row)",
                data_type="integer",
                window_duration="1 hour",
                description="Product popularity in last hour"
            ),
            FeatureDefinition(
                name="product_price_volatility",
                feature_type=FeatureType.STATISTICAL,
                expression="stddev(unit_price) over (partition by stock_code order by kafka_timestamp range between interval 24 hours preceding and current row)",
                data_type="double",
                description="Product price volatility in last 24 hours"
            ),
            FeatureDefinition(
                name="product_return_rate",
                feature_type=FeatureType.BEHAVIORAL,
                expression="avg(case when quantity < 0 then 1.0 else 0.0 end) over (partition by stock_code order by kafka_timestamp range between interval 7 days preceding and current row)",
                data_type="double",
                description="Product return rate in last 7 days"
            )
        ]
        
        # Temporal features
        temporal_features = [
            FeatureDefinition(
                name="hour_of_day",
                feature_type=FeatureType.TEMPORAL,
                expression="hour(kafka_timestamp)",
                data_type="integer",
                description="Hour of the day for the transaction"
            ),
            FeatureDefinition(
                name="day_of_week",
                feature_type=FeatureType.TEMPORAL,
                expression="dayofweek(kafka_timestamp)",
                data_type="integer",
                description="Day of the week (1=Sunday, 7=Saturday)"
            ),
            FeatureDefinition(
                name="is_weekend",
                feature_type=FeatureType.TEMPORAL,
                expression="case when dayofweek(kafka_timestamp) in (1, 7) then 1.0 else 0.0 end",
                data_type="double",
                description="Whether transaction occurred on weekend"
            ),
            FeatureDefinition(
                name="month_of_year",
                feature_type=FeatureType.TEMPORAL,
                expression="month(kafka_timestamp)",
                data_type="integer",
                description="Month of the year for seasonality"
            )
        ]
        
        # Transaction features
        transaction_features = [
            FeatureDefinition(
                name="transaction_amount_zscore",
                feature_type=FeatureType.STATISTICAL,
                expression="(line_total - avg(line_total) over (partition by customer_id_clean)) / (stddev(line_total) over (partition by customer_id_clean) + 0.001)",
                data_type="double",
                description="Z-score of transaction amount for customer"
            ),
            FeatureDefinition(
                name="quantity_price_ratio",
                feature_type=FeatureType.DERIVED,
                expression="quantity / (unit_price + 0.001)",
                data_type="double",
                scaling_method=ScalingMethod.STANDARD,
                description="Ratio of quantity to unit price"
            ),
            FeatureDefinition(
                name="is_bulk_purchase",
                feature_type=FeatureType.DERIVED,
                expression="case when quantity > 10 then 1.0 else 0.0 end",
                data_type="double",
                description="Whether this is a bulk purchase"
            )
        ]
        
        # Register all features
        all_features = customer_features + product_features + temporal_features + transaction_features
        
        for feature in all_features:
            self.features[feature.name] = feature
        
        # Define feature groups
        self.feature_groups = {
            "customer_behavior": [f.name for f in customer_features],
            "product_performance": [f.name for f in product_features],
            "temporal": [f.name for f in temporal_features],
            "transaction": [f.name for f in transaction_features]
        }
        
        self.logger.info(f"Loaded {len(all_features)} built-in features in {len(self.feature_groups)} groups")
    
    def compute_features(
        self, 
        df: DataFrame, 
        feature_groups: List[str] = None
    ) -> DataFrame:
        """Compute features for a DataFrame"""
        try:
            if feature_groups is None:
                feature_groups = list(self.feature_groups.keys())
            
            feature_names = []
            for group in feature_groups:
                if group in self.feature_groups:
                    feature_names.extend(self.feature_groups[group])
            
            # Remove duplicates
            feature_names = list(set(feature_names))
            
            result_df = df
            computed_features = []
            
            for feature_name in feature_names:
                if feature_name not in self.features:
                    continue
                
                feature_def = self.features[feature_name]
                
                if not feature_def.enabled:
                    continue
                
                try:
                    result_df = result_df.withColumn(
                        feature_name,
                        expr(feature_def.expression)
                    )
                    computed_features.append(feature_name)
                    
                except Exception as e:
                    self.logger.error(f"Failed to compute feature {feature_name}: {e}")
            
            # Add feature metadata
            result_df = (
                result_df
                .withColumn("computed_features", lit(json.dumps(computed_features)))
                .withColumn("feature_computation_timestamp", current_timestamp())
            )
            
            self.logger.info(f"Computed {len(computed_features)} features")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Feature computation failed: {e}")
            return df
    
    def save_features_to_store(
        self, 
        df: DataFrame, 
        feature_group: str,
        partition_columns: List[str] = None
    ):
        """Save computed features to feature store"""
        try:
            output_path = self.feature_store_path / feature_group
            
            writer = (
                df
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
            )
            
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            
            writer.save(str(output_path))
            
            self.logger.info(f"Saved features to feature store: {feature_group}")
            
        except Exception as e:
            self.logger.error(f"Failed to save features: {e}")
            raise
    
    def get_features_from_store(
        self, 
        feature_group: str,
        filters: List[str] = None
    ) -> DataFrame:
        """Retrieve features from feature store"""
        try:
            feature_path = self.feature_store_path / feature_group
            
            if not feature_path.exists():
                raise ValueError(f"Feature group {feature_group} not found")
            
            df = self.spark.read.format("delta").load(str(feature_path))
            
            if filters:
                for filter_expr in filters:
                    df = df.filter(filter_expr)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve features: {e}")
            raise
    
    def add_feature_definition(self, feature: FeatureDefinition):
        """Add custom feature definition"""
        self.features[feature.name] = feature
        self.logger.info(f"Added feature definition: {feature.name}")


class ModelRegistry:
    """Registry for ML models and serving"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger(__name__)
        
        # Model registry
        self.models: Dict[str, ModelDefinition] = {}
        self.trained_models: Dict[str, Any] = {}  # Actual model objects
        
        # Model paths
        self.model_store_path = Path("./models")
        self.model_store_path.mkdir(parents=True, exist_ok=True)
        
        self._load_builtin_models()
        
    def _load_builtin_models(self):
        """Load built-in model definitions"""
        
        models = [
            ModelDefinition(
                name="customer_churn_classifier",
                model_type=ModelType.CLASSIFICATION,
                target_column="is_churned",
                feature_columns=[
                    "customer_transaction_frequency_7d",
                    "customer_avg_order_value_30d",
                    "customer_recency_days",
                    "customer_monetary_total"
                ],
                hyperparameters={
                    "numTrees": 100,
                    "maxDepth": 10,
                    "seed": 42
                },
                evaluation_metrics=["accuracy", "precision", "recall", "f1"]
            ),
            ModelDefinition(
                name="transaction_value_predictor",
                model_type=ModelType.REGRESSION,
                target_column="line_total",
                feature_columns=[
                    "quantity",
                    "unit_price",
                    "hour_of_day",
                    "is_weekend",
                    "customer_avg_order_value_30d",
                    "product_popularity_score"
                ],
                hyperparameters={
                    "regParam": 0.01,
                    "elasticNetParam": 0.5
                },
                evaluation_metrics=["rmse", "mae", "r2"]
            ),
            ModelDefinition(
                name="customer_segmentation",
                model_type=ModelType.CLUSTERING,
                target_column="cluster",
                feature_columns=[
                    "customer_transaction_frequency_7d",
                    "customer_avg_order_value_30d",
                    "customer_monetary_total"
                ],
                hyperparameters={
                    "k": 5,
                    "seed": 42
                },
                evaluation_metrics=["silhouette"]
            ),
            ModelDefinition(
                name="anomaly_detector",
                model_type=ModelType.ANOMALY_DETECTION,
                target_column="is_anomaly",
                feature_columns=[
                    "transaction_amount_zscore",
                    "quantity_price_ratio",
                    "hour_of_day",
                    "customer_recency_days"
                ],
                hyperparameters={
                    "contamination": 0.1
                },
                evaluation_metrics=["precision", "recall"]
            )
        ]
        
        for model in models:
            self.models[model.name] = model
        
        self.logger.info(f"Loaded {len(models)} built-in model definitions")
    
    def train_model(self, model_name: str, training_df: DataFrame) -> Any:
        """Train a model"""
        try:
            if model_name not in self.models:
                raise ValueError(f"Model {model_name} not found")
            
            model_def = self.models[model_name]
            
            # Prepare features
            assembler = VectorAssembler(
                inputCols=model_def.feature_columns,
                outputCol="features"
            )
            
            # Create pipeline based on model type
            if model_def.model_type == ModelType.CLASSIFICATION:
                classifier = RandomForestClassifier(
                    featuresCol="features",
                    labelCol=model_def.target_column,
                    **model_def.hyperparameters
                )
                pipeline = Pipeline(stages=[assembler, classifier])
                
            elif model_def.model_type == ModelType.REGRESSION:
                regressor = LinearRegression(
                    featuresCol="features",
                    labelCol=model_def.target_column,
                    **model_def.hyperparameters
                )
                pipeline = Pipeline(stages=[assembler, regressor])
                
            elif model_def.model_type == ModelType.CLUSTERING:
                kmeans = KMeans(
                    featuresCol="features",
                    **model_def.hyperparameters
                )
                pipeline = Pipeline(stages=[assembler, kmeans])
                
            else:
                raise ValueError(f"Unsupported model type: {model_def.model_type}")
            
            # Train the model
            self.logger.info(f"Training model: {model_name}")
            trained_model = pipeline.fit(training_df)
            
            # Store the trained model
            self.trained_models[model_name] = trained_model
            
            # Save model to disk
            model_path = self.model_store_path / model_name
            trained_model.write().overwrite().save(str(model_path))
            
            self.logger.info(f"Model {model_name} trained and saved successfully")
            return trained_model
            
        except Exception as e:
            self.logger.error(f"Model training failed for {model_name}: {e}")
            raise
    
    def predict(self, model_name: str, df: DataFrame) -> DataFrame:
        """Make predictions using a trained model"""
        try:
            if model_name not in self.trained_models:
                raise ValueError(f"Trained model {model_name} not found")
            
            model = self.trained_models[model_name]
            predictions = model.transform(df)
            
            # Add prediction metadata
            predictions = (
                predictions
                .withColumn("model_name", lit(model_name))
                .withColumn("prediction_timestamp", current_timestamp())
            )
            
            return predictions
            
        except Exception as e:
            self.logger.error(f"Prediction failed for {model_name}: {e}")
            raise
    
    def load_model(self, model_name: str) -> Any:
        """Load a saved model"""
        try:
            model_path = self.model_store_path / model_name
            
            if not model_path.exists():
                raise ValueError(f"Model {model_name} not found at {model_path}")
            
            from pyspark.ml import Pipeline
            model = Pipeline.load(str(model_path))
            
            self.trained_models[model_name] = model
            self.logger.info(f"Loaded model: {model_name}")
            
            return model
            
        except Exception as e:
            self.logger.error(f"Failed to load model {model_name}: {e}")
            raise


class RealtimeAnalyticsEngine:
    """Comprehensive real-time analytics and ML engine"""
    
    def __init__(
        self, 
        spark: SparkSession, 
        config: RealTimeAnalyticsConfig = None
    ):
        self.spark = spark
        self.config = config or RealTimeAnalyticsConfig()
        self.logger = get_logger(__name__)
        self.metrics_collector = get_metrics_collector()
        
        # Initialize components
        self.delta_manager = DeltaLakeManager(spark)
        self.feature_store = FeatureStore(spark, self.delta_manager)
        self.model_registry = ModelRegistry(spark)
        self.kafka_manager = KafkaManager()
        
        # Processing metrics
        self.features_computed = 0
        self.predictions_made = 0
        self.anomalies_detected = 0
        
        self.logger.info("Real-time Analytics Engine initialized")
    
    def process_streaming_analytics(
        self,
        silver_stream: DataFrame,
        output_path: str,
        checkpoint_path: str
    ) -> StreamingQuery:
        """Process streaming data with analytics and ML"""
        try:
            self.logger.info("Starting real-time analytics processing")
            
            # Apply feature engineering
            if self.config.enable_feature_engineering:
                featured_stream = self.feature_store.compute_features(
                    silver_stream, 
                    ["customer_behavior", "product_performance", "temporal", "transaction"]
                )
            else:
                featured_stream = silver_stream
            
            # Apply ML models for predictions
            if self.config.enable_model_serving:
                analytics_stream = self._apply_ml_predictions(featured_stream)
            else:
                analytics_stream = featured_stream
            
            # Apply anomaly detection
            if self.config.enable_anomaly_detection:
                analytics_stream = self._apply_anomaly_detection(analytics_stream)
            
            # Apply behavioral analytics
            if self.config.enable_behavioral_analytics:
                analytics_stream = self._apply_behavioral_analytics(analytics_stream)
            
            # Add analytics metadata
            final_stream = self._add_analytics_metadata(analytics_stream)
            
            # Configure output stream
            query = (
                final_stream
                .writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", checkpoint_path)
                .option("mergeSchema", "true")
                .trigger(processingTime="30 seconds")
                .foreachBatch(lambda batch_df, batch_id: 
                             self._process_analytics_batch(batch_df, batch_id, output_path))
                .start()
            )
            
            return query
            
        except Exception as e:
            self.logger.error(f"Failed to start analytics processing: {e}")
            raise
    
    def _apply_ml_predictions(self, df: DataFrame) -> DataFrame:
        """Apply ML model predictions"""
        try:
            # Load required models
            model_names = ["customer_segmentation", "transaction_value_predictor"]
            
            result_df = df
            
            for model_name in model_names:
                try:
                    if model_name not in self.model_registry.trained_models:
                        self.logger.info(f"Loading model: {model_name}")
                        self.model_registry.load_model(model_name)
                    
                    # Make predictions (in a real scenario, this would be optimized)
                    # For now, we'll add mock predictions
                    if model_name == "customer_segmentation":
                        result_df = result_df.withColumn(
                            "predicted_customer_segment",
                            when(col("customer_monetary_total") > 1000, "VIP")
                            .when(col("customer_avg_order_value_30d") > 50, "Premium")
                            .otherwise("Standard")
                        )
                    elif model_name == "transaction_value_predictor":
                        result_df = result_df.withColumn(
                            "predicted_transaction_value",
                            col("quantity") * col("unit_price") * 1.05  # Mock prediction with 5% adjustment
                        )
                    
                except Exception as e:
                    self.logger.error(f"Model prediction failed for {model_name}: {e}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"ML predictions failed: {e}")
            return df
    
    def _apply_anomaly_detection(self, df: DataFrame) -> DataFrame:
        """Apply anomaly detection"""
        try:
            # Simple statistical anomaly detection
            # In production, this would use more sophisticated methods
            
            result_df = (
                df
                # Price anomaly detection
                .withColumn(
                    "is_price_anomaly",
                    abs(col("transaction_amount_zscore")) > 3.0
                )
                # Volume anomaly detection
                .withColumn(
                    "is_volume_anomaly",
                    (col("quantity") > 100) | (col("quantity") < -50)
                )
                # Time-based anomaly detection
                .withColumn(
                    "is_time_anomaly",
                    (col("hour_of_day") < 6) | (col("hour_of_day") > 23)
                )
                # Composite anomaly score
                .withColumn(
                    "anomaly_score",
                    (
                        when(col("is_price_anomaly"), 0.4).otherwise(0.0) +
                        when(col("is_volume_anomaly"), 0.3).otherwise(0.0) +
                        when(col("is_time_anomaly"), 0.2).otherwise(0.0) +
                        when(col("risk_score") > 0.7, 0.3).otherwise(0.0)
                    )
                )
                .withColumn(
                    "is_anomaly",
                    col("anomaly_score") > 0.5
                )
            )
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Anomaly detection failed: {e}")
            return df
    
    def _apply_behavioral_analytics(self, df: DataFrame) -> DataFrame:
        """Apply behavioral analytics"""
        try:
            # Customer behavior patterns
            behavioral_window = (
                Window.partitionBy("customer_id_clean")
                .orderBy(col("kafka_timestamp"))
                .rangeBetween(-86400, Window.currentRow)  # 24-hour window
            )
            
            result_df = (
                df
                # Shopping pattern analysis
                .withColumn(
                    "shopping_pattern",
                    when(col("customer_transaction_frequency_7d") > 5, "frequent")
                    .when(col("customer_avg_order_value_30d") > 100, "high_value")
                    .when(col("is_weekend"), "weekend_shopper")
                    .otherwise("casual")
                )
                # Purchase velocity
                .withColumn(
                    "purchase_velocity",
                    count("*").over(behavioral_window) / 24.0  # Transactions per hour
                )
                # Product affinity (simplified)
                .withColumn(
                    "product_category_preference",
                    when(col("unit_price") > 10, "premium")
                    .when(col("quantity") > 5, "bulk")
                    .otherwise("standard")
                )
                # Customer lifecycle stage
                .withColumn(
                    "lifecycle_stage",
                    when(col("customer_recency_days") <= 7, "active")
                    .when(col("customer_recency_days") <= 30, "regular")
                    .when(col("customer_recency_days") <= 90, "at_risk")
                    .otherwise("dormant")
                )
            )
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Behavioral analytics failed: {e}")
            return df
    
    def _add_analytics_metadata(self, df: DataFrame) -> DataFrame:
        """Add analytics processing metadata"""
        return (
            df
            .withColumn("analytics_id", expr("uuid()"))
            .withColumn("analytics_processed_at", current_timestamp())
            .withColumn("analytics_version", lit("2.0"))
            .withColumn("ml_features_applied", lit(True))
            .withColumn("anomaly_detection_applied", lit(self.config.enable_anomaly_detection))
        )
    
    def _process_analytics_batch(
        self, 
        batch_df: DataFrame, 
        batch_id: int, 
        output_path: str
    ):
        """Process each analytics batch"""
        try:
            if batch_df.isEmpty():
                return
            
            batch_count = batch_df.count()
            self.logger.info(f"Processing analytics batch {batch_id}: {batch_count} records")
            
            # Write to Delta Lake
            (
                batch_df
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("processing_date", "predicted_customer_segment")
                .save(output_path)
            )
            
            # Save features to feature store if enabled
            if self.config.feature_store_enabled:
                self.feature_store.save_features_to_store(
                    batch_df,
                    "realtime_features",
                    ["processing_date"]
                )
            
            # Update metrics
            self.features_computed += batch_count
            predictions_count = batch_df.filter(col("predicted_customer_segment").isNotNull()).count()
            self.predictions_made += predictions_count
            anomalies_count = batch_df.filter(col("is_anomaly")).count()
            self.anomalies_detected += anomalies_count
            
            # Send analytics metrics
            if self.metrics_collector:
                self.metrics_collector.increment_counter(
                    "analytics_records_processed",
                    {"batch_id": str(batch_id)}
                )
                self.metrics_collector.gauge(
                    "anomaly_detection_rate",
                    float(anomalies_count / batch_count) if batch_count > 0 else 0,
                    {"batch_id": str(batch_id)}
                )
            
            # Send anomaly alerts if any detected
            if anomalies_count > 0:
                self._send_anomaly_alerts(batch_df.filter(col("is_anomaly")), batch_id)
            
            self.logger.info(
                f"Analytics batch {batch_id} processed - "
                f"Records: {batch_count}, Predictions: {predictions_count}, Anomalies: {anomalies_count}"
            )
            
        except Exception as e:
            self.logger.error(f"Analytics batch {batch_id} processing failed: {e}")
            raise
    
    def _send_anomaly_alerts(self, anomalies_df: DataFrame, batch_id: int):
        """Send alerts for detected anomalies"""
        try:
            anomalies = anomalies_df.select(
                "analytics_id", "customer_id_clean", "line_total", 
                "anomaly_score", "is_price_anomaly", "is_volume_anomaly"
            ).collect()
            
            for anomaly in anomalies:
                alert_data = {
                    "alert_type": "anomaly_detection",
                    "analytics_id": anomaly["analytics_id"],
                    "customer_id": anomaly["customer_id_clean"],
                    "transaction_value": float(anomaly["line_total"]),
                    "anomaly_score": float(anomaly["anomaly_score"]),
                    "anomaly_types": {
                        "price_anomaly": bool(anomaly["is_price_anomaly"]),
                        "volume_anomaly": bool(anomaly["is_volume_anomaly"])
                    },
                    "severity": "high" if anomaly["anomaly_score"] > 0.8 else "medium",
                    "batch_id": batch_id
                }
                
                self.kafka_manager.produce_data_quality_alert(alert_data)
            
            self.logger.info(f"Sent {len(anomalies)} anomaly alerts for batch {batch_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to send anomaly alerts: {e}")
    
    def get_analytics_metrics(self) -> Dict[str, Any]:
        """Get analytics processing metrics"""
        return {
            "analytics_metrics": {
                "features_computed": self.features_computed,
                "predictions_made": self.predictions_made,
                "anomalies_detected": self.anomalies_detected,
                "feature_store_enabled": self.config.feature_store_enabled,
                "models_loaded": len(self.model_registry.trained_models)
            },
            "feature_store_stats": {
                "total_features": len(self.feature_store.features),
                "feature_groups": len(self.feature_store.feature_groups)
            },
            "model_registry_stats": {
                "model_definitions": len(self.model_registry.models),
                "trained_models": len(self.model_registry.trained_models)
            },
            "timestamp": datetime.now().isoformat()
        }


# Factory functions
def create_analytics_config(**kwargs) -> RealTimeAnalyticsConfig:
    """Create analytics configuration"""
    return RealTimeAnalyticsConfig(**kwargs)


def create_realtime_analytics_engine(
    spark: SparkSession,
    config: RealTimeAnalyticsConfig = None
) -> RealtimeAnalyticsEngine:
    """Create real-time analytics engine instance"""
    return RealtimeAnalyticsEngine(spark, config)


# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("RealtimeAnalyticsEngine")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    try:
        print("Testing Real-time Analytics Engine...")
        
        # Create configuration
        config = create_analytics_config(
            enable_feature_engineering=True,
            enable_model_serving=True,
            enable_anomaly_detection=True,
            feature_store_enabled=True
        )
        
        # Create analytics engine
        engine = create_realtime_analytics_engine(spark, config)
        
        # Get metrics
        metrics = engine.get_analytics_metrics()
        print(f"✅ Created analytics engine with {metrics['feature_store_stats']['total_features']} features")
        print(f"   Feature groups: {metrics['feature_store_stats']['feature_groups']}")
        print(f"   Model definitions: {metrics['model_registry_stats']['model_definitions']}")
        
        print("✅ Real-time Analytics Engine testing completed")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

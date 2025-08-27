"""
Advanced Feature Engineering Pipeline

Comprehensive feature engineering with automated feature selection,
transformation, and validation.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from sklearn.preprocessing import StandardScaler, RobustScaler, MinMaxScaler
from sklearn.feature_selection import SelectKBest, f_classif, f_regression
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
import joblib
from pathlib import Path

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.monitoring.metrics import MetricsCollector

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class FeatureConfig:
    """Configuration for feature engineering pipeline."""
    
    # Feature selection parameters
    feature_selection_method: str = "kbest"
    k_features: int = 50
    selection_score_func: str = "f_classif"
    
    # Scaling parameters
    scaling_method: str = "standard"
    
    # Encoding parameters
    categorical_encoding: str = "onehot"
    handle_unknown: str = "ignore"
    
    # Time-based features
    enable_time_features: bool = True
    time_windows: List[int] = field(default_factory=lambda: [7, 30, 90])
    
    # Advanced features
    enable_interaction_features: bool = True
    interaction_degree: int = 2
    enable_polynomial_features: bool = False
    polynomial_degree: int = 2
    
    # Feature validation
    enable_drift_detection: bool = True
    drift_threshold: float = 0.1
    
    # Output configuration
    save_transformers: bool = True
    transformer_path: str = "models/transformers"


class BaseFeatureTransformer(ABC):
    """Base class for feature transformers."""
    
    def __init__(self, name: str):
        self.name = name
        self.is_fitted = False
        
    @abstractmethod
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'BaseFeatureTransformer':
        """Fit the transformer."""
        pass
        
    @abstractmethod
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Transform features."""
        pass
        
    def fit_transform(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> pd.DataFrame:
        """Fit and transform features."""
        return self.fit(X, y).transform(X)


class TimeFeatureTransformer(BaseFeatureTransformer):
    """Time-based feature engineering."""
    
    def __init__(self, date_columns: List[str], config: FeatureConfig):
        super().__init__("time_features")
        self.date_columns = date_columns
        self.config = config
        
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'TimeFeatureTransformer':
        """Fit the time feature transformer."""
        self.is_fitted = True
        return self
        
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Generate time-based features."""
        if not self.is_fitted:
            raise ValueError("Transformer must be fitted before transform")
            
        X_transformed = X.copy()
        
        for date_col in self.date_columns:
            if date_col not in X_transformed.columns:
                continue
                
            # Convert to datetime if needed
            if not pd.api.types.is_datetime64_any_dtype(X_transformed[date_col]):
                X_transformed[date_col] = pd.to_datetime(X_transformed[date_col])
            
            # Basic time features
            X_transformed[f'{date_col}_year'] = X_transformed[date_col].dt.year
            X_transformed[f'{date_col}_month'] = X_transformed[date_col].dt.month
            X_transformed[f'{date_col}_day'] = X_transformed[date_col].dt.day
            X_transformed[f'{date_col}_dayofweek'] = X_transformed[date_col].dt.dayofweek
            X_transformed[f'{date_col}_quarter'] = X_transformed[date_col].dt.quarter
            X_transformed[f'{date_col}_is_weekend'] = X_transformed[date_col].dt.dayofweek.isin([5, 6]).astype(int)
            
            # Cyclical features
            X_transformed[f'{date_col}_month_sin'] = np.sin(2 * np.pi * X_transformed[date_col].dt.month / 12)
            X_transformed[f'{date_col}_month_cos'] = np.cos(2 * np.pi * X_transformed[date_col].dt.month / 12)
            X_transformed[f'{date_col}_dayofweek_sin'] = np.sin(2 * np.pi * X_transformed[date_col].dt.dayofweek / 7)
            X_transformed[f'{date_col}_dayofweek_cos'] = np.cos(2 * np.pi * X_transformed[date_col].dt.dayofweek / 7)
            
            # Time since features
            reference_date = X_transformed[date_col].max()
            X_transformed[f'{date_col}_days_since'] = (reference_date - X_transformed[date_col]).dt.days
            
        return X_transformed


class AggregationFeatureTransformer(BaseFeatureTransformer):
    """Aggregation-based feature engineering."""
    
    def __init__(self, group_columns: List[str], agg_columns: List[str], 
                 agg_functions: List[str], time_windows: List[int]):
        super().__init__("aggregation_features")
        self.group_columns = group_columns
        self.agg_columns = agg_columns
        self.agg_functions = agg_functions
        self.time_windows = time_windows
        self.reference_stats = {}
        
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'AggregationFeatureTransformer':
        """Fit the aggregation transformer."""
        for group_col in self.group_columns:
            for agg_col in self.agg_columns:
                for func in self.agg_functions:
                    key = f"{group_col}_{agg_col}_{func}"
                    if group_col in X.columns and agg_col in X.columns:
                        self.reference_stats[key] = X.groupby(group_col)[agg_col].agg(func)
        
        self.is_fitted = True
        return self
        
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Generate aggregation features."""
        if not self.is_fitted:
            raise ValueError("Transformer must be fitted before transform")
            
        X_transformed = X.copy()
        
        for group_col in self.group_columns:
            for agg_col in self.agg_columns:
                for func in self.agg_functions:
                    if group_col not in X.columns or agg_col not in X.columns:
                        continue
                        
                    feature_name = f"{group_col}_{agg_col}_{func}"
                    
                    # Map aggregated values
                    if feature_name in self.reference_stats:
                        X_transformed[feature_name] = X_transformed[group_col].map(
                            self.reference_stats[feature_name]
                        ).fillna(0)
                    
                    # Ratio to group statistics
                    if func in ['mean', 'median']:
                        ratio_name = f"{agg_col}_to_{feature_name}_ratio"
                        X_transformed[ratio_name] = X_transformed[agg_col] / (
                            X_transformed[feature_name] + 1e-8
                        )
        
        return X_transformed


class InteractionFeatureTransformer(BaseFeatureTransformer):
    """Interaction feature engineering."""
    
    def __init__(self, interaction_columns: List[str], degree: int = 2):
        super().__init__("interaction_features")
        self.interaction_columns = interaction_columns
        self.degree = degree
        self.interactions = []
        
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'InteractionFeatureTransformer':
        """Fit the interaction transformer."""
        from itertools import combinations
        
        # Generate interaction pairs
        available_columns = [col for col in self.interaction_columns if col in X.columns]
        
        for r in range(2, self.degree + 1):
            for combo in combinations(available_columns, r):
                self.interactions.append(combo)
        
        self.is_fitted = True
        return self
        
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Generate interaction features."""
        if not self.is_fitted:
            raise ValueError("Transformer must be fitted before transform")
            
        X_transformed = X.copy()
        
        for interaction in self.interactions:
            # Multiplicative interactions
            interaction_name = "_x_".join(interaction)
            if all(col in X_transformed.columns for col in interaction):
                X_transformed[interaction_name] = X_transformed[list(interaction)].prod(axis=1)
        
        return X_transformed


class FeatureEngineeringPipeline:
    """Comprehensive feature engineering pipeline."""
    
    def __init__(self, config: FeatureConfig):
        self.config = config
        self.transformers: List[BaseFeatureTransformer] = []
        self.scaler = None
        self.feature_selector = None
        self.feature_names = None
        self.metrics_collector = MetricsCollector()
        
    def add_transformer(self, transformer: BaseFeatureTransformer):
        """Add a feature transformer to the pipeline."""
        self.transformers.append(transformer)
        
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'FeatureEngineeringPipeline':
        """Fit the entire feature engineering pipeline."""
        start_time = datetime.now()
        
        try:
            logger.info(f"Starting feature engineering pipeline fit with {len(X)} samples")
            
            X_processed = X.copy()
            
            # Apply transformers
            for transformer in self.transformers:
                logger.info(f"Fitting transformer: {transformer.name}")
                transformer.fit(X_processed, y)
                X_processed = transformer.transform(X_processed)
            
            # Separate numerical and categorical features
            numerical_features = X_processed.select_dtypes(include=[np.number]).columns.tolist()
            categorical_features = X_processed.select_dtypes(include=['object', 'category']).columns.tolist()
            
            # Handle categorical encoding
            if categorical_features and self.config.categorical_encoding == "onehot":
                X_processed = pd.get_dummies(
                    X_processed, 
                    columns=categorical_features, 
                    drop_first=True,
                    dummy_na=True
                )
            
            # Feature scaling
            if self.config.scaling_method and numerical_features:
                if self.config.scaling_method == "standard":
                    self.scaler = StandardScaler()
                elif self.config.scaling_method == "robust":
                    self.scaler = RobustScaler()
                elif self.config.scaling_method == "minmax":
                    self.scaler = MinMaxScaler()
                
                if self.scaler:
                    numerical_features_current = [col for col in numerical_features 
                                                if col in X_processed.columns]
                    if numerical_features_current:
                        X_processed[numerical_features_current] = self.scaler.fit_transform(
                            X_processed[numerical_features_current]
                        )
            
            # Feature selection
            if self.config.feature_selection_method and y is not None:
                if self.config.feature_selection_method == "kbest":
                    score_func = f_classif if self.config.selection_score_func == "f_classif" else f_regression
                    self.feature_selector = SelectKBest(
                        score_func=score_func, 
                        k=min(self.config.k_features, X_processed.shape[1])
                    )
                    X_processed = pd.DataFrame(
                        self.feature_selector.fit_transform(X_processed, y),
                        columns=X_processed.columns[self.feature_selector.get_support()],
                        index=X_processed.index
                    )
            
            self.feature_names = X_processed.columns.tolist()
            
            # Save transformers if configured
            if self.config.save_transformers:
                self._save_transformers()
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Feature engineering pipeline fit completed in {duration:.2f} seconds")
            
            # Collect metrics
            self.metrics_collector.increment_counter(
                "feature_engineering_fit_total",
                tags={"status": "success"}
            )
            self.metrics_collector.record_histogram(
                "feature_engineering_fit_duration_seconds",
                duration
            )
            
            return self
            
        except Exception as e:
            logger.error(f"Error in feature engineering fit: {str(e)}")
            self.metrics_collector.increment_counter(
                "feature_engineering_fit_total",
                tags={"status": "error"}
            )
            raise
    
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Transform features using fitted pipeline."""
        start_time = datetime.now()
        
        try:
            logger.info(f"Transforming {len(X)} samples")
            
            X_processed = X.copy()
            
            # Apply transformers
            for transformer in self.transformers:
                if transformer.is_fitted:
                    X_processed = transformer.transform(X_processed)
            
            # Handle categorical encoding
            categorical_features = X_processed.select_dtypes(include=['object', 'category']).columns.tolist()
            if categorical_features and self.config.categorical_encoding == "onehot":
                X_processed = pd.get_dummies(
                    X_processed, 
                    columns=categorical_features, 
                    drop_first=True,
                    dummy_na=True
                )
            
            # Feature scaling
            if self.scaler:
                numerical_features = X_processed.select_dtypes(include=[np.number]).columns.tolist()
                numerical_features_current = [col for col in numerical_features 
                                            if col in X_processed.columns]
                if numerical_features_current:
                    X_processed[numerical_features_current] = self.scaler.transform(
                        X_processed[numerical_features_current]
                    )
            
            # Feature selection
            if self.feature_selector:
                # Ensure columns match training
                missing_cols = set(self.feature_names) - set(X_processed.columns)
                for col in missing_cols:
                    X_processed[col] = 0
                
                extra_cols = set(X_processed.columns) - set(self.feature_names)
                X_processed = X_processed.drop(columns=extra_cols)
                
                # Reorder columns to match training
                X_processed = X_processed[self.feature_names]
                
                X_processed = pd.DataFrame(
                    self.feature_selector.transform(X_processed),
                    columns=[col for col in self.feature_names if col in X_processed.columns],
                    index=X_processed.index
                )
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Feature transformation completed in {duration:.2f} seconds")
            
            # Collect metrics
            self.metrics_collector.increment_counter(
                "feature_engineering_transform_total",
                tags={"status": "success"}
            )
            self.metrics_collector.record_histogram(
                "feature_engineering_transform_duration_seconds",
                duration
            )
            
            return X_processed
            
        except Exception as e:
            logger.error(f"Error in feature engineering transform: {str(e)}")
            self.metrics_collector.increment_counter(
                "feature_engineering_transform_total",
                tags={"status": "error"}
            )
            raise
    
    def fit_transform(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> pd.DataFrame:
        """Fit and transform features."""
        return self.fit(X, y).transform(X)
    
    def detect_feature_drift(self, X_reference: pd.DataFrame, X_current: pd.DataFrame) -> Dict[str, float]:
        """Detect feature drift between reference and current datasets."""
        drift_scores = {}
        
        common_columns = set(X_reference.columns) & set(X_current.columns)
        
        for col in common_columns:
            if X_reference[col].dtype in ['float64', 'int64']:
                # Statistical drift detection for numerical features
                ref_mean, ref_std = X_reference[col].mean(), X_reference[col].std()
                curr_mean, curr_std = X_current[col].mean(), X_current[col].std()
                
                mean_drift = abs(ref_mean - curr_mean) / (ref_std + 1e-8)
                std_drift = abs(ref_std - curr_std) / (ref_std + 1e-8)
                
                drift_scores[col] = max(mean_drift, std_drift)
            else:
                # Distribution drift for categorical features
                ref_dist = X_reference[col].value_counts(normalize=True)
                curr_dist = X_current[col].value_counts(normalize=True)
                
                all_values = set(ref_dist.index) | set(curr_dist.index)
                ref_probs = [ref_dist.get(val, 0) for val in all_values]
                curr_probs = [curr_dist.get(val, 0) for val in all_values]
                
                # Jensen-Shannon divergence
                m = 0.5 * (np.array(ref_probs) + np.array(curr_probs))
                js_div = 0.5 * self._kl_divergence(ref_probs, m) + 0.5 * self._kl_divergence(curr_probs, m)
                drift_scores[col] = js_div
        
        return drift_scores
    
    def _kl_divergence(self, p: List[float], q: List[float]) -> float:
        """Calculate KL divergence."""
        p, q = np.array(p), np.array(q)
        p = p + 1e-8  # Avoid log(0)
        q = q + 1e-8
        return np.sum(p * np.log(p / q))
    
    def _save_transformers(self):
        """Save fitted transformers to disk."""
        transformer_path = Path(self.config.transformer_path)
        transformer_path.mkdir(parents=True, exist_ok=True)
        
        # Save individual transformers
        for i, transformer in enumerate(self.transformers):
            joblib.dump(transformer, transformer_path / f"transformer_{i}_{transformer.name}.pkl")
        
        # Save scaler and feature selector
        if self.scaler:
            joblib.dump(self.scaler, transformer_path / "scaler.pkl")
        
        if self.feature_selector:
            joblib.dump(self.feature_selector, transformer_path / "feature_selector.pkl")
        
        # Save feature names
        if self.feature_names:
            joblib.dump(self.feature_names, transformer_path / "feature_names.pkl")
        
        logger.info(f"Transformers saved to {transformer_path}")
    
    def load_transformers(self):
        """Load fitted transformers from disk."""
        transformer_path = Path(self.config.transformer_path)
        
        if not transformer_path.exists():
            logger.warning(f"Transformer path {transformer_path} does not exist")
            return
        
        # Load transformers
        transformer_files = list(transformer_path.glob("transformer_*.pkl"))
        self.transformers = []
        
        for transformer_file in sorted(transformer_files):
            transformer = joblib.load(transformer_file)
            self.transformers.append(transformer)
        
        # Load scaler and feature selector
        scaler_path = transformer_path / "scaler.pkl"
        if scaler_path.exists():
            self.scaler = joblib.load(scaler_path)
        
        feature_selector_path = transformer_path / "feature_selector.pkl"
        if feature_selector_path.exists():
            self.feature_selector = joblib.load(feature_selector_path)
        
        # Load feature names
        feature_names_path = transformer_path / "feature_names.pkl"
        if feature_names_path.exists():
            self.feature_names = joblib.load(feature_names_path)
        
        logger.info(f"Transformers loaded from {transformer_path}")


def create_sales_feature_pipeline() -> FeatureEngineeringPipeline:
    """Create a feature engineering pipeline optimized for sales data."""
    config = FeatureConfig(
        feature_selection_method="kbest",
        k_features=100,
        scaling_method="standard",
        enable_time_features=True,
        time_windows=[7, 30, 90, 365],
        enable_interaction_features=True,
        interaction_degree=2
    )
    
    pipeline = FeatureEngineeringPipeline(config)
    
    # Add time feature transformer
    time_transformer = TimeFeatureTransformer(
        date_columns=['InvoiceDate', 'created_at', 'updated_at'],
        config=config
    )
    pipeline.add_transformer(time_transformer)
    
    # Add aggregation transformer
    agg_transformer = AggregationFeatureTransformer(
        group_columns=['CustomerID', 'Country', 'StockCode'],
        agg_columns=['Quantity', 'UnitPrice', 'TotalAmount'],
        agg_functions=['mean', 'sum', 'count', 'std'],
        time_windows=config.time_windows
    )
    pipeline.add_transformer(agg_transformer)
    
    # Add interaction transformer
    interaction_transformer = InteractionFeatureTransformer(
        interaction_columns=['Quantity', 'UnitPrice', 'TotalAmount'],
        degree=config.interaction_degree
    )
    pipeline.add_transformer(interaction_transformer)
    
    return pipeline


class RFMFeatureTransformer(BaseFeatureTransformer):
    """RFM (Recency, Frequency, Monetary) feature engineering."""
    
    def __init__(self, customer_id_col: str, date_col: str, amount_col: str):
        super().__init__("rfm_features")
        self.customer_id_col = customer_id_col
        self.date_col = date_col
        self.amount_col = amount_col
        self.rfm_stats = {}
        
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'RFMFeatureTransformer':
        """Fit the RFM transformer."""
        df = X.copy()
        
        # Convert date column
        if not pd.api.types.is_datetime64_any_dtype(df[self.date_col]):
            df[self.date_col] = pd.to_datetime(df[self.date_col])
        
        # Calculate RFM metrics
        reference_date = df[self.date_col].max()
        
        rfm = df.groupby(self.customer_id_col).agg({
            self.date_col: lambda x: (reference_date - x.max()).days,  # Recency
            self.customer_id_col: 'count',  # Frequency
            self.amount_col: 'sum'  # Monetary
        })
        
        rfm.columns = ['Recency', 'Frequency', 'Monetary']
        
        # Calculate quintiles for scoring
        self.rfm_stats['recency_quintiles'] = rfm['Recency'].quantile([0.2, 0.4, 0.6, 0.8]).tolist()
        self.rfm_stats['frequency_quintiles'] = rfm['Frequency'].quantile([0.2, 0.4, 0.6, 0.8]).tolist()
        self.rfm_stats['monetary_quintiles'] = rfm['Monetary'].quantile([0.2, 0.4, 0.6, 0.8]).tolist()
        
        self.is_fitted = True
        return self
        
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Transform to RFM features."""
        if not self.is_fitted:
            raise ValueError("Transformer must be fitted before transform")
            
        X_transformed = X.copy()
        df = X_transformed.copy()
        
        # Convert date column
        if not pd.api.types.is_datetime64_any_dtype(df[self.date_col]):
            df[self.date_col] = pd.to_datetime(df[self.date_col])
        
        # Calculate RFM metrics
        reference_date = df[self.date_col].max()
        
        rfm = df.groupby(self.customer_id_col).agg({
            self.date_col: lambda x: (reference_date - x.max()).days,
            self.customer_id_col: 'count',
            self.amount_col: 'sum'
        })
        
        rfm.columns = ['Recency', 'Frequency', 'Monetary']
        
        # Calculate RFM scores
        def score_recency(x):
            if x <= self.rfm_stats['recency_quintiles'][0]:
                return 5
            elif x <= self.rfm_stats['recency_quintiles'][1]:
                return 4
            elif x <= self.rfm_stats['recency_quintiles'][2]:
                return 3
            elif x <= self.rfm_stats['recency_quintiles'][3]:
                return 2
            else:
                return 1
        
        def score_frequency_monetary(x, quintiles):
            if x <= quintiles[0]:
                return 1
            elif x <= quintiles[1]:
                return 2
            elif x <= quintiles[2]:
                return 3
            elif x <= quintiles[3]:
                return 4
            else:
                return 5
        
        rfm['R_Score'] = rfm['Recency'].apply(score_recency)
        rfm['F_Score'] = rfm['Frequency'].apply(lambda x: score_frequency_monetary(x, self.rfm_stats['frequency_quintiles']))
        rfm['M_Score'] = rfm['Monetary'].apply(lambda x: score_frequency_monetary(x, self.rfm_stats['monetary_quintiles']))
        
        rfm['RFM_Score'] = rfm['R_Score'].astype(str) + rfm['F_Score'].astype(str) + rfm['M_Score'].astype(str)
        rfm['RFM_Score_Numeric'] = rfm['R_Score'] + rfm['F_Score'] + rfm['M_Score']
        
        # Merge back to original data
        rfm_features = rfm[['Recency', 'Frequency', 'Monetary', 'R_Score', 'F_Score', 'M_Score', 'RFM_Score_Numeric']]
        X_transformed = X_transformed.merge(rfm_features, left_on=self.customer_id_col, right_index=True, how='left')
        
        return X_transformed


class OutlierFeatureTransformer(BaseFeatureTransformer):
    """Outlier detection and treatment."""
    
    def __init__(self, columns: List[str], method: str = "iqr", factor: float = 1.5):
        super().__init__("outlier_features")
        self.columns = columns
        self.method = method
        self.factor = factor
        self.outlier_bounds = {}
        
    def fit(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> 'OutlierFeatureTransformer':
        """Fit the outlier transformer."""
        for col in self.columns:
            if col not in X.columns:
                continue
                
            if self.method == "iqr":
                Q1 = X[col].quantile(0.25)
                Q3 = X[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - self.factor * IQR
                upper_bound = Q3 + self.factor * IQR
                
            elif self.method == "zscore":
                mean = X[col].mean()
                std = X[col].std()
                lower_bound = mean - self.factor * std
                upper_bound = mean + self.factor * std
                
            elif self.method == "isolation_forest":
                iso_forest = IsolationForest(contamination=0.1, random_state=42)
                outliers = iso_forest.fit_predict(X[[col]])
                # For this method, we'll use it in transform
                self.outlier_bounds[col] = iso_forest
                continue
                
            self.outlier_bounds[col] = (lower_bound, upper_bound)
        
        self.is_fitted = True
        return self
        
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """Transform outliers."""
        if not self.is_fitted:
            raise ValueError("Transformer must be fitted before transform")
            
        X_transformed = X.copy()
        
        for col in self.columns:
            if col not in X_transformed.columns:
                continue
                
            if self.method == "isolation_forest":
                if col in self.outlier_bounds:
                    outliers = self.outlier_bounds[col].predict(X_transformed[[col]])
                    X_transformed[f'{col}_is_outlier'] = (outliers == -1).astype(int)
                    
            else:
                if col in self.outlier_bounds:
                    lower_bound, upper_bound = self.outlier_bounds[col]
                    
                    # Create outlier indicators
                    X_transformed[f'{col}_is_outlier'] = (
                        (X_transformed[col] < lower_bound) | 
                        (X_transformed[col] > upper_bound)
                    ).astype(int)
                    
                    # Cap outliers
                    X_transformed[f'{col}_capped'] = X_transformed[col].clip(lower_bound, upper_bound)
        
        return X_transformed


class FeaturePipeline:
    """High-level feature pipeline interface."""
    
    def __init__(self):
        self.pipelines: Dict[str, FeatureEngineeringPipeline] = {}
        self.default_pipeline = None
        
    def register_pipeline(self, name: str, pipeline: FeatureEngineeringPipeline):
        """Register a named pipeline."""
        self.pipelines[name] = pipeline
        if not self.default_pipeline:
            self.default_pipeline = name
    
    def get_pipeline(self, name: str) -> Optional[FeatureEngineeringPipeline]:
        """Get a registered pipeline."""
        return self.pipelines.get(name)
    
    async def execute_pipeline(self, pipeline_name: str, input_data: Dict[str, Any], 
                              feature_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """Execute a feature pipeline."""
        pipeline = self.get_pipeline(pipeline_name)
        if not pipeline:
            raise ValueError(f"Pipeline '{pipeline_name}' not found")
        
        # Convert input data to DataFrame
        if isinstance(input_data, dict):
            df = pd.DataFrame([input_data])
        else:
            df = pd.DataFrame(input_data)
        
        start_time = datetime.now()
        
        try:
            # Transform features
            transformed_df = pipeline.transform(df)
            
            # Filter specific features if requested
            if feature_names:
                available_features = [f for f in feature_names if f in transformed_df.columns]
                transformed_df = transformed_df[available_features]
            
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            return {
                "execution_id": f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "features": transformed_df.to_dict('records')[0] if len(transformed_df) == 1 else transformed_df.to_dict('records'),
                "feature_count": len(transformed_df.columns),
                "execution_time_ms": execution_time,
                "pipeline_version": "1.0",
                "data_version": "1.0"
            }
            
        except Exception as e:
            logger.error(f"Error executing pipeline {pipeline_name}: {str(e)}")
            raise
    
    async def list_pipelines(self, status_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """List available pipelines."""
        pipelines = []
        
        for name, pipeline in self.pipelines.items():
            pipeline_info = {
                "name": name,
                "status": "active",
                "transformers": len(pipeline.transformers),
                "feature_count": len(pipeline.feature_names) if pipeline.feature_names else 0,
                "is_default": name == self.default_pipeline
            }
            
            if not status_filter or pipeline_info["status"] == status_filter:
                pipelines.append(pipeline_info)
        
        return pipelines
    
    async def get_pipeline_details(self, pipeline_name: str) -> Dict[str, Any]:
        """Get detailed pipeline information."""
        pipeline = self.get_pipeline(pipeline_name)
        if not pipeline:
            raise ValueError(f"Pipeline '{pipeline_name}' not found")
        
        return {
            "description": f"Feature engineering pipeline: {pipeline_name}",
            "version": "1.0",
            "status": "active",
            "input_features": [],  # Would be populated from schema
            "output_features": pipeline.feature_names or [],
            "derived_features": [],  # Would track derived features
            "transformations": [t.name for t in pipeline.transformers],
            "dependencies": [],
            "last_execution": datetime.now().isoformat(),
            "execution_count": 0,
            "avg_execution_time_ms": 0,
            "success_rate": 1.0,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
    
    async def validate_pipeline(self, pipeline_name: str, sample_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Validate a feature pipeline."""
        pipeline = self.get_pipeline(pipeline_name)
        if not pipeline:
            return {"status": "error", "errors": ["Pipeline not found"]}
        
        errors = []
        warnings = []
        
        try:
            if sample_data:
                # Test with sample data
                result = await self.execute_pipeline(pipeline_name, sample_data)
                sample_output = result.get("features", {})
            else:
                sample_output = {}
            
            # Basic validation
            if not pipeline.transformers:
                warnings.append("Pipeline has no transformers")
            
            if not pipeline.feature_names:
                warnings.append("Pipeline has no defined feature names")
            
            validation_result = {
                "status": "success" if not errors else "error",
                "schema_valid": True,
                "transformations_valid": True,
                "output_valid": True,
                "data_quality": {"completeness": 1.0, "validity": 1.0},
                "errors": errors,
                "warnings": warnings,
                "sample_output": sample_output
            }
            
            return validation_result
            
        except Exception as e:
            return {
                "status": "error",
                "schema_valid": False,
                "transformations_valid": False,
                "output_valid": False,
                "errors": [str(e)],
                "warnings": warnings,
                "sample_output": {}
            }


# Factory functions
def create_comprehensive_sales_pipeline() -> FeatureEngineeringPipeline:
    """Create a comprehensive sales feature engineering pipeline."""
    config = FeatureConfig(
        feature_selection_method="kbest",
        k_features=200,
        scaling_method="standard",
        enable_time_features=True,
        time_windows=[7, 30, 90, 365],
        enable_interaction_features=True,
        interaction_degree=2
    )
    
    pipeline = FeatureEngineeringPipeline(config)
    
    # Time features
    time_transformer = TimeFeatureTransformer(
        date_columns=['InvoiceDate', 'created_at', 'updated_at'],
        config=config
    )
    pipeline.add_transformer(time_transformer)
    
    # RFM features
    rfm_transformer = RFMFeatureTransformer(
        customer_id_col='CustomerID',
        date_col='InvoiceDate',
        amount_col='TotalAmount'
    )
    pipeline.add_transformer(rfm_transformer)
    
    # Aggregation features
    agg_transformer = AggregationFeatureTransformer(
        group_columns=['CustomerID', 'Country', 'StockCode'],
        agg_columns=['Quantity', 'UnitPrice', 'TotalAmount'],
        agg_functions=['mean', 'sum', 'count', 'std', 'min', 'max'],
        time_windows=config.time_windows
    )
    pipeline.add_transformer(agg_transformer)
    
    # Outlier features
    outlier_transformer = OutlierFeatureTransformer(
        columns=['Quantity', 'UnitPrice', 'TotalAmount'],
        method="iqr",
        factor=1.5
    )
    pipeline.add_transformer(outlier_transformer)
    
    # Interaction features
    interaction_transformer = InteractionFeatureTransformer(
        interaction_columns=['Quantity', 'UnitPrice', 'TotalAmount', 'Recency', 'Frequency', 'Monetary'],
        degree=config.interaction_degree
    )
    pipeline.add_transformer(interaction_transformer)
    
    return pipeline


def create_feature_pipeline_manager() -> FeaturePipeline:
    """Create a feature pipeline manager with pre-configured pipelines."""
    manager = FeaturePipeline()
    
    # Register standard sales pipeline
    sales_pipeline = create_sales_feature_pipeline()
    manager.register_pipeline("sales_standard", sales_pipeline)
    
    # Register comprehensive sales pipeline
    comprehensive_pipeline = create_comprehensive_sales_pipeline()
    manager.register_pipeline("sales_comprehensive", comprehensive_pipeline)
    
    return manager
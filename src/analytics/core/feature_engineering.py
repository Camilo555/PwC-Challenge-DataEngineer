"""
Advanced Feature Engineering for ML Analytics
Enterprise-grade feature engineering with automated feature selection and transformation
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
import json
from pathlib import Path

import numpy as np
import pandas as pd
from pydantic import BaseModel

from .base import BaseMLComponent, MLConfig, DataQualityChecker, PerformanceProfiler


class FeatureConfig(BaseModel):
    """Configuration for feature engineering"""
    auto_feature_selection: bool = True
    feature_selection_threshold: float = 0.05
    max_features: int = 1000
    encoding_strategy: str = "target"  # target, onehot, label
    scaling_strategy: str = "standard"  # standard, minmax, robust
    handle_missing: str = "median"  # median, mean, mode, drop
    create_interactions: bool = True
    max_interaction_depth: int = 2
    temporal_features: bool = True
    text_features: bool = False


class FeatureMetadata(BaseModel):
    """Metadata for features"""
    feature_name: str
    feature_type: str
    data_type: str
    importance_score: Optional[float] = None
    correlation_with_target: Optional[float] = None
    missing_ratio: float
    unique_values: int
    created_at: datetime
    transformation_applied: List[str] = []
    
    class Config:
        arbitrary_types_allowed = True


class FeatureStore:
    """Feature store for managing ML features"""
    
    def __init__(self, storage_path: str = "features/"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.features_metadata: Dict[str, FeatureMetadata] = {}
        self.feature_groups: Dict[str, List[str]] = {}
        
    def store_features(self, 
                      features: pd.DataFrame, 
                      feature_group: str,
                      metadata: Optional[Dict[str, FeatureMetadata]] = None) -> None:
        """Store features in the feature store"""
        
        # Save features data
        feature_path = self.storage_path / f"{feature_group}.parquet"
        features.to_parquet(feature_path)
        
        # Save metadata
        if metadata:
            metadata_path = self.storage_path / f"{feature_group}_metadata.json"
            serializable_metadata = {
                k: v.dict() for k, v in metadata.items()
            }
            
            with open(metadata_path, 'w') as f:
                json.dump(serializable_metadata, f, indent=2)
        
        # Update feature groups registry
        self.feature_groups[feature_group] = list(features.columns)
        
        # Update registry
        registry_path = self.storage_path / "feature_registry.json"
        with open(registry_path, 'w') as f:
            json.dump(self.feature_groups, f, indent=2)
    
    def load_features(self, feature_group: str) -> Tuple[pd.DataFrame, Optional[Dict[str, FeatureMetadata]]]:
        """Load features from the feature store"""
        
        # Load features data
        feature_path = self.storage_path / f"{feature_group}.parquet"
        if not feature_path.exists():
            raise FileNotFoundError(f"Feature group '{feature_group}' not found")
        
        features = pd.read_parquet(feature_path)
        
        # Load metadata
        metadata = None
        metadata_path = self.storage_path / f"{feature_group}_metadata.json"
        if metadata_path.exists():
            with open(metadata_path, 'r') as f:
                metadata_dict = json.load(f)
            
            metadata = {
                k: FeatureMetadata(**v) for k, v in metadata_dict.items()
            }
        
        return features, metadata
    
    def list_feature_groups(self) -> List[str]:
        """List all available feature groups"""
        registry_path = self.storage_path / "feature_registry.json"
        if not registry_path.exists():
            return []
        
        with open(registry_path, 'r') as f:
            feature_groups = json.load(f)
        
        return list(feature_groups.keys())
    
    def get_feature_info(self, feature_group: str) -> Dict[str, Any]:
        """Get information about a feature group"""
        if feature_group not in self.feature_groups:
            # Try to load from registry
            registry_path = self.storage_path / "feature_registry.json"
            if registry_path.exists():
                with open(registry_path, 'r') as f:
                    self.feature_groups = json.load(f)
        
        if feature_group not in self.feature_groups:
            raise ValueError(f"Feature group '{feature_group}' not found")
        
        feature_path = self.storage_path / f"{feature_group}.parquet"
        feature_size = feature_path.stat().st_size if feature_path.exists() else 0
        
        return {
            "feature_group": feature_group,
            "feature_count": len(self.feature_groups[feature_group]),
            "features": self.feature_groups[feature_group],
            "storage_size_mb": feature_size / (1024 * 1024),
            "last_modified": datetime.fromtimestamp(feature_path.stat().st_mtime) if feature_path.exists() else None
        }


class FeatureEngineer(BaseMLComponent):
    """Advanced feature engineering with automated feature creation and selection"""
    
    def __init__(self, config: Optional[FeatureConfig] = None, ml_config: Optional[MLConfig] = None):
        super().__init__(ml_config)
        self.feature_config = config or FeatureConfig()
        self.feature_transformers = {}
        self.feature_importances = {}
        self.selected_features = []
        self.feature_metadata: Dict[str, FeatureMetadata] = {}
        self.quality_checker = DataQualityChecker()
        self.profiler = PerformanceProfiler()
        
    @PerformanceProfiler().profile_function("feature_engineering_fit")
    def fit(self, data: pd.DataFrame, target: Optional[pd.Series] = None, **kwargs) -> 'FeatureEngineer':
        """Fit feature engineering pipeline"""
        self.validate_data(data)
        
        self.logger.info(f"Starting feature engineering for {data.shape[0]} samples, {data.shape[1]} features")
        
        # Store original columns
        self.original_features = list(data.columns)
        
        # Generate quality report
        quality_report = self.quality_checker.generate_quality_report(data)
        self.update_metadata(quality_report=quality_report)
        
        # Create feature metadata
        self._create_feature_metadata(data, target)
        
        # Fit transformers
        self._fit_missing_value_handlers(data)
        self._fit_scalers(data)
        self._fit_encoders(data)
        
        if target is not None:
            self._calculate_feature_importance(data, target)
            
            if self.feature_config.auto_feature_selection:
                self._select_features(data, target)
        
        self._is_fitted = True
        self.logger.info(f"Feature engineering fitted successfully")
        
        return self
    
    @PerformanceProfiler().profile_function("feature_engineering_transform")
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform data using fitted feature engineering pipeline"""
        if not self.is_fitted:
            raise ValueError("FeatureEngineer must be fitted before transform")
        
        self.validate_data(data)
        
        # Start with copy of data
        transformed_data = data.copy()
        
        # Apply transformations
        transformed_data = self._handle_missing_values(transformed_data)
        transformed_data = self._encode_categorical_features(transformed_data)
        transformed_data = self._create_temporal_features(transformed_data)
        transformed_data = self._create_interaction_features(transformed_data)
        transformed_data = self._scale_features(transformed_data)
        
        # Select features if enabled
        if self.feature_config.auto_feature_selection and self.selected_features:
            available_features = [f for f in self.selected_features if f in transformed_data.columns]
            transformed_data = transformed_data[available_features]
        
        self.logger.info(f"Transformed data shape: {transformed_data.shape}")
        
        return transformed_data
    
    def _create_feature_metadata(self, data: pd.DataFrame, target: Optional[pd.Series]) -> None:
        """Create metadata for all features"""
        for column in data.columns:
            series = data[column]
            
            # Calculate correlation with target if available
            correlation = None
            if target is not None and pd.api.types.is_numeric_dtype(series):
                try:
                    correlation = series.corr(target)
                except:
                    correlation = None
            
            metadata = FeatureMetadata(
                feature_name=column,
                feature_type=self._get_feature_type(series),
                data_type=str(series.dtype),
                correlation_with_target=correlation,
                missing_ratio=series.isnull().sum() / len(series),
                unique_values=series.nunique(),
                created_at=datetime.utcnow()
            )
            
            self.feature_metadata[column] = metadata
    
    def _get_feature_type(self, series: pd.Series) -> str:
        """Determine feature type"""
        if pd.api.types.is_numeric_dtype(series):
            return "numerical"
        elif pd.api.types.is_categorical_dtype(series) or series.dtype == 'object':
            return "categorical"
        elif pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"
        elif pd.api.types.is_bool_dtype(series):
            return "boolean"
        else:
            return "unknown"
    
    def _fit_missing_value_handlers(self, data: pd.DataFrame) -> None:
        """Fit missing value handlers"""
        from sklearn.impute import SimpleImputer
        
        self.feature_transformers['missing_handlers'] = {}
        
        for column in data.columns:
            if data[column].isnull().sum() > 0:
                feature_type = self.feature_metadata[column].feature_type
                
                if feature_type == "numerical":
                    strategy = self.feature_config.handle_missing if self.feature_config.handle_missing in ['mean', 'median'] else 'median'
                    imputer = SimpleImputer(strategy=strategy)
                elif feature_type == "categorical":
                    imputer = SimpleImputer(strategy='most_frequent')
                else:
                    imputer = SimpleImputer(strategy='constant', fill_value='missing')
                
                imputer.fit(data[[column]])
                self.feature_transformers['missing_handlers'][column] = imputer
    
    def _fit_scalers(self, data: pd.DataFrame) -> None:
        """Fit feature scalers"""
        from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler
        
        numerical_columns = [col for col in data.columns 
                           if self.feature_metadata[col].feature_type == "numerical"]
        
        if not numerical_columns:
            return
        
        if self.feature_config.scaling_strategy == "standard":
            scaler = StandardScaler()
        elif self.feature_config.scaling_strategy == "minmax":
            scaler = MinMaxScaler()
        elif self.feature_config.scaling_strategy == "robust":
            scaler = RobustScaler()
        else:
            scaler = StandardScaler()
        
        scaler.fit(data[numerical_columns])
        self.feature_transformers['scaler'] = scaler
        self.feature_transformers['numerical_columns'] = numerical_columns
    
    def _fit_encoders(self, data: pd.DataFrame) -> None:
        """Fit categorical encoders"""
        from sklearn.preprocessing import LabelEncoder, OneHotEncoder
        
        categorical_columns = [col for col in data.columns 
                             if self.feature_metadata[col].feature_type == "categorical"]
        
        if not categorical_columns:
            return
        
        self.feature_transformers['encoders'] = {}
        self.feature_transformers['categorical_columns'] = categorical_columns
        
        for column in categorical_columns:
            if self.feature_config.encoding_strategy == "label":
                encoder = LabelEncoder()
                encoder.fit(data[column].fillna('missing'))
            elif self.feature_config.encoding_strategy == "onehot":
                encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
                encoder.fit(data[[column]].fillna('missing'))
            else:
                # Target encoding - placeholder for now
                encoder = LabelEncoder()
                encoder.fit(data[column].fillna('missing'))
            
            self.feature_transformers['encoders'][column] = encoder
    
    def _handle_missing_values(self, data: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in data"""
        if 'missing_handlers' not in self.feature_transformers:
            return data
        
        result_data = data.copy()
        
        for column, imputer in self.feature_transformers['missing_handlers'].items():
            if column in result_data.columns:
                result_data[column] = imputer.transform(result_data[[column]]).flatten()
        
        return result_data
    
    def _encode_categorical_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Encode categorical features"""
        if 'encoders' not in self.feature_transformers:
            return data
        
        result_data = data.copy()
        
        for column, encoder in self.feature_transformers['encoders'].items():
            if column in result_data.columns:
                if self.feature_config.encoding_strategy == "onehot":
                    encoded = encoder.transform(result_data[[column]].fillna('missing'))
                    feature_names = [f"{column}_{cat}" for cat in encoder.categories_[0]]
                    encoded_df = pd.DataFrame(encoded, columns=feature_names, index=result_data.index)
                    result_data = pd.concat([result_data.drop(column, axis=1), encoded_df], axis=1)
                else:
                    result_data[column] = encoder.transform(result_data[column].fillna('missing'))
        
        return result_data
    
    def _create_temporal_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Create temporal features from datetime columns"""
        if not self.feature_config.temporal_features:
            return data
        
        result_data = data.copy()
        datetime_columns = [col for col in data.columns 
                           if pd.api.types.is_datetime64_any_dtype(data[col])]
        
        for column in datetime_columns:
            if column in result_data.columns:
                dt_series = pd.to_datetime(result_data[column])
                
                # Extract temporal components
                result_data[f"{column}_year"] = dt_series.dt.year
                result_data[f"{column}_month"] = dt_series.dt.month
                result_data[f"{column}_day"] = dt_series.dt.day
                result_data[f"{column}_dayofweek"] = dt_series.dt.dayofweek
                result_data[f"{column}_hour"] = dt_series.dt.hour
                result_data[f"{column}_quarter"] = dt_series.dt.quarter
                result_data[f"{column}_is_weekend"] = (dt_series.dt.dayofweek >= 5).astype(int)
                
                # Calculate time since reference
                if len(dt_series.dropna()) > 0:
                    reference_date = dt_series.min()
                    result_data[f"{column}_days_since"] = (dt_series - reference_date).dt.days
        
        return result_data
    
    def _create_interaction_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Create interaction features between numerical columns"""
        if not self.feature_config.create_interactions:
            return data
        
        result_data = data.copy()
        numerical_columns = [col for col in result_data.columns 
                           if pd.api.types.is_numeric_dtype(result_data[col])]
        
        # Limit number of features to avoid explosion
        max_interactions = min(len(numerical_columns), 10)
        selected_columns = numerical_columns[:max_interactions]
        
        # Create pairwise interactions
        for i, col1 in enumerate(selected_columns):
            for col2 in selected_columns[i+1:]:
                if col1 != col2:
                    # Multiplicative interaction
                    interaction_name = f"{col1}_x_{col2}"
                    result_data[interaction_name] = result_data[col1] * result_data[col2]
                    
                    # Ratio interaction (avoid division by zero)
                    ratio_name = f"{col1}_div_{col2}"
                    denominator = result_data[col2].replace(0, np.finfo(float).eps)
                    result_data[ratio_name] = result_data[col1] / denominator
        
        return result_data
    
    def _scale_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Scale numerical features"""
        if 'scaler' not in self.feature_transformers:
            return data
        
        result_data = data.copy()
        scaler = self.feature_transformers['scaler']
        numerical_columns = self.feature_transformers['numerical_columns']
        
        # Only scale columns that exist in the data
        available_numerical = [col for col in numerical_columns if col in result_data.columns]
        
        if available_numerical:
            scaled_data = scaler.transform(result_data[available_numerical])
            result_data[available_numerical] = scaled_data
        
        return result_data
    
    def _calculate_feature_importance(self, data: pd.DataFrame, target: pd.Series) -> None:
        """Calculate feature importance using various methods"""
        try:
            from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
            from sklearn.feature_selection import mutual_info_regression, mutual_info_classif
        except ImportError:
            self.logger.warning("Scikit-learn not available for feature importance calculation")
            return
        
        # Prepare data
        processed_data = self._handle_missing_values(data)
        processed_data = self._encode_categorical_features(processed_data)
        
        # Select only numerical features for importance calculation
        numerical_features = []
        for col in processed_data.columns:
            if pd.api.types.is_numeric_dtype(processed_data[col]):
                numerical_features.append(col)
        
        if not numerical_features:
            return
        
        X = processed_data[numerical_features]
        y = target
        
        # Remove rows with missing target values
        mask = ~y.isnull()
        X = X[mask]
        y = y[mask]
        
        if len(X) == 0:
            return
        
        # Determine if classification or regression
        is_classification = y.dtype == 'object' or len(y.unique()) < 20
        
        try:
            # Random Forest importance
            if is_classification:
                rf = RandomForestClassifier(n_estimators=100, random_state=self.config.random_state)
                rf.fit(X, y)
                rf_importance = dict(zip(numerical_features, rf.feature_importances_))
                
                # Mutual information
                mi_scores = mutual_info_classif(X, y, random_state=self.config.random_state)
                mi_importance = dict(zip(numerical_features, mi_scores))
            else:
                rf = RandomForestRegressor(n_estimators=100, random_state=self.config.random_state)
                rf.fit(X, y)
                rf_importance = dict(zip(numerical_features, rf.feature_importances_))
                
                # Mutual information
                mi_scores = mutual_info_regression(X, y, random_state=self.config.random_state)
                mi_importance = dict(zip(numerical_features, mi_scores))
            
            # Store importance scores
            self.feature_importances['random_forest'] = rf_importance
            self.feature_importances['mutual_info'] = mi_importance
            
            # Update feature metadata with importance scores
            for feature in numerical_features:
                if feature in self.feature_metadata:
                    avg_importance = (rf_importance.get(feature, 0) + mi_importance.get(feature, 0)) / 2
                    self.feature_metadata[feature].importance_score = avg_importance
            
        except Exception as e:
            self.logger.warning(f"Failed to calculate feature importance: {e}")
    
    def _select_features(self, data: pd.DataFrame, target: pd.Series) -> None:
        """Select important features based on importance scores"""
        if not self.feature_importances:
            self.selected_features = list(data.columns)
            return
        
        # Combine importance scores
        all_features = set()
        for importance_dict in self.feature_importances.values():
            all_features.update(importance_dict.keys())
        
        feature_scores = {}
        for feature in all_features:
            scores = [imp_dict.get(feature, 0) for imp_dict in self.feature_importances.values()]
            feature_scores[feature] = np.mean(scores)
        
        # Sort by importance
        sorted_features = sorted(feature_scores.items(), key=lambda x: x[1], reverse=True)
        
        # Select top features
        threshold = self.feature_config.feature_selection_threshold
        max_features = self.feature_config.max_features
        
        selected = []
        for feature, score in sorted_features:
            if score >= threshold and len(selected) < max_features:
                selected.append(feature)
        
        # Add original categorical features that weren't in importance calculation
        for col in data.columns:
            if col not in feature_scores and col not in selected:
                if self.feature_metadata[col].feature_type == "categorical":
                    selected.append(col)
        
        self.selected_features = selected
        self.logger.info(f"Selected {len(selected)} features out of {len(data.columns)}")
    
    def get_feature_importance(self, method: str = "random_forest") -> Dict[str, float]:
        """Get feature importance scores"""
        return self.feature_importances.get(method, {})
    
    def get_feature_metadata(self, feature_name: Optional[str] = None) -> Union[FeatureMetadata, Dict[str, FeatureMetadata]]:
        """Get feature metadata"""
        if feature_name:
            return self.feature_metadata.get(feature_name)
        return self.feature_metadata
    
    def create_feature_report(self) -> Dict[str, Any]:
        """Create comprehensive feature engineering report"""
        report = {
            "feature_engineering_summary": {
                "total_original_features": len(self.original_features) if hasattr(self, 'original_features') else 0,
                "total_engineered_features": len(self.feature_metadata),
                "selected_features": len(self.selected_features),
                "config": self.feature_config.dict()
            },
            "feature_metadata": {
                name: metadata.dict() for name, metadata in self.feature_metadata.items()
            },
            "feature_importance": self.feature_importances,
            "selected_features": self.selected_features,
            "transformers_fitted": list(self.feature_transformers.keys())
        }
        
        return report
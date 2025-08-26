"""
Base ML Analytics Components
Core infrastructure for machine learning analytics platform
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Optional, Union
from pathlib import Path

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from core.logging import get_logger


class MLConfig(BaseModel):
    """Configuration for ML components"""
    model_storage_path: str = "models/"
    feature_storage_path: str = "features/"
    max_memory_usage: float = 0.8
    enable_gpu: bool = False
    random_state: int = 42
    
    class Config:
        arbitrary_types_allowed = True


class ModelMetrics(BaseModel):
    """Model performance metrics"""
    accuracy: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    mse: Optional[float] = None
    rmse: Optional[float] = None
    mae: Optional[float] = None
    r2_score: Optional[float] = None
    auc_roc: Optional[float] = None
    training_time: Optional[float] = None
    inference_time: Optional[float] = None
    memory_usage: Optional[float] = None
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary"""
        return {k: v for k, v in self.dict().items() if v is not None}


class BaseMLComponent(ABC):
    """Base class for all ML components"""
    
    def __init__(self, config: Optional[MLConfig] = None):
        self.config = config or MLConfig()
        self.logger = get_logger(self.__class__.__name__)
        self._is_fitted = False
        self._metadata: Dict[str, Any] = {}
        
    @property
    def is_fitted(self) -> bool:
        """Check if component is fitted/trained"""
        return self._is_fitted
    
    @property 
    def metadata(self) -> Dict[str, Any]:
        """Get component metadata"""
        return self._metadata.copy()
    
    def update_metadata(self, **kwargs) -> None:
        """Update component metadata"""
        self._metadata.update(kwargs)
        self._metadata["last_updated"] = datetime.utcnow().isoformat()
    
    @abstractmethod
    def fit(self, data: Union[pd.DataFrame, np.ndarray], **kwargs) -> 'BaseMLComponent':
        """Fit the component to data"""
        pass
    
    @abstractmethod
    def transform(self, data: Union[pd.DataFrame, np.ndarray]) -> Union[pd.DataFrame, np.ndarray]:
        """Transform data using fitted component"""
        pass
    
    def fit_transform(self, data: Union[pd.DataFrame, np.ndarray], **kwargs) -> Union[pd.DataFrame, np.ndarray]:
        """Fit component and transform data"""
        return self.fit(data, **kwargs).transform(data)
    
    def save(self, path: Union[str, Path]) -> None:
        """Save component to disk"""
        import pickle
        
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, 'wb') as f:
            pickle.dump(self, f)
        
        self.logger.info(f"Component saved to {path}")
    
    @classmethod
    def load(cls, path: Union[str, Path]) -> 'BaseMLComponent':
        """Load component from disk"""
        import pickle
        
        with open(path, 'rb') as f:
            component = pickle.load(f)
        
        logger = get_logger(cls.__name__)
        logger.info(f"Component loaded from {path}")
        
        return component
    
    def validate_data(self, data: Union[pd.DataFrame, np.ndarray]) -> None:
        """Validate input data"""
        if data is None:
            raise ValueError("Data cannot be None")
        
        if isinstance(data, pd.DataFrame):
            if data.empty:
                raise ValueError("DataFrame cannot be empty")
        elif isinstance(data, np.ndarray):
            if data.size == 0:
                raise ValueError("Array cannot be empty")
        else:
            raise TypeError("Data must be pandas DataFrame or numpy array")


class DataQualityChecker:
    """Data quality validation for ML components"""
    
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
    
    def check_missing_values(self, data: pd.DataFrame, threshold: float = 0.5) -> Dict[str, Any]:
        """Check for missing values"""
        missing_stats = {}
        
        for column in data.columns:
            missing_count = data[column].isnull().sum()
            missing_ratio = missing_count / len(data)
            
            missing_stats[column] = {
                "missing_count": int(missing_count),
                "missing_ratio": float(missing_ratio),
                "exceeds_threshold": missing_ratio > threshold
            }
        
        return missing_stats
    
    def check_data_types(self, data: pd.DataFrame) -> Dict[str, str]:
        """Check data types"""
        return {col: str(dtype) for col, dtype in data.dtypes.items()}
    
    def check_outliers(self, data: pd.DataFrame, method: str = "iqr") -> Dict[str, Any]:
        """Detect outliers in numerical columns"""
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        outlier_stats = {}
        
        for column in numeric_columns:
            if method == "iqr":
                Q1 = data[column].quantile(0.25)
                Q3 = data[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = data[(data[column] < lower_bound) | (data[column] > upper_bound)]
                outlier_count = len(outliers)
                
            elif method == "zscore":
                from scipy import stats
                z_scores = np.abs(stats.zscore(data[column].dropna()))
                outliers = data[z_scores > 3]
                outlier_count = len(outliers)
            
            outlier_stats[column] = {
                "outlier_count": outlier_count,
                "outlier_ratio": outlier_count / len(data),
                "method": method
            }
        
        return outlier_stats
    
    def generate_quality_report(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive data quality report"""
        report = {
            "dataset_info": {
                "shape": data.shape,
                "memory_usage": data.memory_usage(deep=True).sum(),
                "created_at": datetime.utcnow().isoformat()
            },
            "missing_values": self.check_missing_values(data),
            "data_types": self.check_data_types(data),
            "outliers": self.check_outliers(data),
            "summary_statistics": {}
        }
        
        # Add summary statistics for numeric columns
        numeric_data = data.select_dtypes(include=[np.number])
        if not numeric_data.empty:
            report["summary_statistics"] = numeric_data.describe().to_dict()
        
        return report


class MLExperiment:
    """Track ML experiments and model versions"""
    
    def __init__(self, experiment_name: str):
        self.experiment_name = experiment_name
        self.runs: Dict[str, Dict[str, Any]] = {}
        self.logger = get_logger(f"{self.__class__.__name__}_{experiment_name}")
    
    def start_run(self, run_id: str) -> Dict[str, Any]:
        """Start a new experiment run"""
        run_data = {
            "run_id": run_id,
            "experiment_name": self.experiment_name,
            "start_time": datetime.utcnow().isoformat(),
            "parameters": {},
            "metrics": {},
            "artifacts": {},
            "status": "running"
        }
        
        self.runs[run_id] = run_data
        self.logger.info(f"Started experiment run: {run_id}")
        
        return run_data
    
    def log_parameter(self, run_id: str, key: str, value: Any) -> None:
        """Log experiment parameter"""
        if run_id in self.runs:
            self.runs[run_id]["parameters"][key] = value
    
    def log_metric(self, run_id: str, key: str, value: float) -> None:
        """Log experiment metric"""
        if run_id in self.runs:
            self.runs[run_id]["metrics"][key] = value
    
    def log_artifact(self, run_id: str, key: str, path: str) -> None:
        """Log experiment artifact"""
        if run_id in self.runs:
            self.runs[run_id]["artifacts"][key] = path
    
    def end_run(self, run_id: str, status: str = "completed") -> None:
        """End experiment run"""
        if run_id in self.runs:
            self.runs[run_id]["end_time"] = datetime.utcnow().isoformat()
            self.runs[run_id]["status"] = status
            self.logger.info(f"Ended experiment run: {run_id} with status: {status}")
    
    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get experiment run data"""
        return self.runs.get(run_id)
    
    def get_best_run(self, metric: str, ascending: bool = False) -> Optional[Dict[str, Any]]:
        """Get best run based on metric"""
        valid_runs = [run for run in self.runs.values() 
                     if run["status"] == "completed" and metric in run["metrics"]]
        
        if not valid_runs:
            return None
        
        best_run = min(valid_runs, key=lambda x: x["metrics"][metric]) if ascending else \
                   max(valid_runs, key=lambda x: x["metrics"][metric])
        
        return best_run


class PerformanceProfiler:
    """Performance profiling for ML components"""
    
    def __init__(self):
        self.profiles: Dict[str, Dict[str, Any]] = {}
        self.logger = get_logger(self.__class__.__name__)
    
    def profile_function(self, func_name: str):
        """Decorator to profile function performance"""
        def decorator(func):
            def wrapper(*args, **kwargs):
                import time
                import psutil
                import os
                
                process = psutil.Process(os.getpid())
                
                # Before execution
                start_time = time.time()
                start_memory = process.memory_info().rss / 1024 / 1024  # MB
                start_cpu = process.cpu_percent()
                
                try:
                    result = func(*args, **kwargs)
                    status = "success"
                    error = None
                except Exception as e:
                    result = None
                    status = "error"
                    error = str(e)
                    raise
                finally:
                    # After execution
                    end_time = time.time()
                    end_memory = process.memory_info().rss / 1024 / 1024  # MB
                    end_cpu = process.cpu_percent()
                    
                    # Store profile data
                    profile_data = {
                        "function_name": func_name,
                        "execution_time": end_time - start_time,
                        "memory_delta": end_memory - start_memory,
                        "cpu_usage": end_cpu - start_cpu,
                        "peak_memory": end_memory,
                        "status": status,
                        "error": error,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                    self.profiles[func_name] = profile_data
                    
                    self.logger.debug(f"Profiled {func_name}: "
                                    f"time={profile_data['execution_time']:.3f}s, "
                                    f"memory={profile_data['memory_delta']:.1f}MB")
                
                return result
            
            return wrapper
        return decorator
    
    def get_profile_summary(self) -> Dict[str, Any]:
        """Get performance profile summary"""
        if not self.profiles:
            return {"message": "No profiles recorded"}
        
        summary = {
            "total_functions": len(self.profiles),
            "total_execution_time": sum(p["execution_time"] for p in self.profiles.values()),
            "average_memory_usage": np.mean([p["memory_delta"] for p in self.profiles.values()]),
            "functions": list(self.profiles.keys()),
            "detailed_profiles": self.profiles
        }
        
        return summary
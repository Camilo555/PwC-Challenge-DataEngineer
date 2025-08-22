"""
Base Transformation Framework
Provides abstract base classes and strategy pattern for ETL transformations.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Protocol, Union
from enum import Enum
import pandas as pd
from dataclasses import dataclass


class TransformationResult:
    """Result of a transformation operation."""
    
    def __init__(self, data: Any, metadata: Optional[Dict] = None, success: bool = True):
        self.data = data
        self.metadata = metadata or {}
        self.success = success
        self.error_message: Optional[str] = None


class TransformationStrategy(ABC):
    """Abstract base class for transformation strategies."""
    
    @abstractmethod
    def transform(self, data: Any, **kwargs) -> TransformationResult:
        """
        Transform input data according to the strategy.
        
        Args:
            data: Input data to transform
            **kwargs: Additional parameters for transformation
            
        Returns:
            TransformationResult with transformed data
        """
        pass
    
    @abstractmethod
    def validate_input(self, data: Any) -> bool:
        """
        Validate input data before transformation.
        
        Args:
            data: Input data to validate
            
        Returns:
            True if data is valid for this transformation
        """
        pass


class TransformationEngine:
    """Engine for executing transformation strategies."""
    
    def __init__(self):
        self.strategies: Dict[str, TransformationStrategy] = {}
    
    def register_strategy(self, name: str, strategy: TransformationStrategy):
        """Register a transformation strategy."""
        self.strategies[name] = strategy
    
    def execute_strategy(self, strategy_name: str, data: Any, **kwargs) -> TransformationResult:
        """Execute a registered transformation strategy."""
        if strategy_name not in self.strategies:
            result = TransformationResult(data, success=False)
            result.error_message = f"Strategy '{strategy_name}' not found"
            return result
        
        strategy = self.strategies[strategy_name]
        
        if not strategy.validate_input(data):
            result = TransformationResult(data, success=False)
            result.error_message = f"Input validation failed for strategy '{strategy_name}'"
            return result
        
        try:
            return strategy.transform(data, **kwargs)
        except Exception as e:
            result = TransformationResult(data, success=False)
            result.error_message = str(e)
            return result
    
    def list_strategies(self) -> List[str]:
        """List all registered strategies."""
        return list(self.strategies.keys())


class DataFrameTransformationStrategy(TransformationStrategy):
    """Base class for DataFrame-specific transformations."""
    
    def validate_input(self, data: Any) -> bool:
        """Validate that input is a pandas DataFrame."""
        return isinstance(data, pd.DataFrame)


class BasicCleaningStrategy(DataFrameTransformationStrategy):
    """Basic data cleaning transformation strategy."""
    
    def transform(self, data: pd.DataFrame, **kwargs) -> TransformationResult:
        """Apply basic data cleaning operations."""
        # Remove duplicates
        cleaned_data = data.drop_duplicates()
        
        # Fill null values with defaults if specified
        fill_values = kwargs.get('fill_values', {})
        if fill_values:
            cleaned_data = cleaned_data.fillna(fill_values)
        
        # Drop columns with too many nulls
        threshold = kwargs.get('null_threshold', 0.5)
        null_ratios = cleaned_data.isnull().sum() / len(cleaned_data)
        columns_to_drop = null_ratios[null_ratios > threshold].index.tolist()
        
        if columns_to_drop:
            cleaned_data = cleaned_data.drop(columns=columns_to_drop)
        
        metadata = {
            'original_rows': len(data),
            'cleaned_rows': len(cleaned_data),
            'dropped_columns': columns_to_drop,
            'duplicates_removed': len(data) - len(cleaned_data)
        }
        
        return TransformationResult(cleaned_data, metadata)


# Example usage and factory functions
def create_transformation_engine() -> TransformationEngine:
    """Create a transformation engine with default strategies."""
    engine = TransformationEngine()
    
    # Register default strategies
    engine.register_strategy('basic_cleaning', BasicCleaningStrategy())
    
    return engine
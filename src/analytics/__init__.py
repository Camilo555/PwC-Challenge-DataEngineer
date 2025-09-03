"""
ML-powered Analytics and Insights Platform
Enterprise-grade machine learning analytics with automated insights generation
"""

from .core.feature_engineering import FeatureEngineer
from .core.model_manager import ModelManager
from .insights.generator import InsightsGenerator
from .ml.predictor import MLPredictor

__all__ = ["FeatureEngineer", "ModelManager", "MLPredictor", "InsightsGenerator"]

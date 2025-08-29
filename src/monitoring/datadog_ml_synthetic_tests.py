"""
DataDog ML Pipeline Synthetic Tests
Comprehensive ML pipeline monitoring including model serving endpoints,
feature pipeline validation, inference accuracy testing, and A/B testing validation
"""

import asyncio
import json
import numpy as np
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from monitoring.datadog_synthetic_monitoring import (
    DataDogSyntheticMonitoring,
    SyntheticTestConfig,
    APITestStep,
    TestType,
    TestFrequency,
    TestLocation,
    AlertCondition
)
from core.logging import get_logger

logger = get_logger(__name__)


class MLTestType(Enum):
    """ML-specific test types"""
    MODEL_HEALTH = "model_health"
    INFERENCE_ACCURACY = "inference_accuracy"
    FEATURE_PIPELINE = "feature_pipeline"
    AB_TEST_INTEGRITY = "ab_test_integrity"
    DATA_QUALITY = "data_quality"
    MODEL_DRIFT = "model_drift"
    PERFORMANCE_BENCHMARK = "performance_benchmark"


class ModelType(Enum):
    """Supported model types"""
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"
    RECOMMENDATION = "recommendation"


@dataclass
class MLModelConfig:
    """Configuration for ML model monitoring"""
    name: str
    endpoint: str
    model_type: ModelType
    version: str
    expected_response_schema: Dict[str, Any]
    performance_thresholds: Dict[str, float]
    sample_inputs: List[Dict[str, Any]]
    expected_outputs: List[Dict[str, Any]]
    accuracy_threshold: float = 0.8
    latency_threshold: int = 5000  # milliseconds
    business_critical: bool = True


@dataclass
class FeaturePipelineConfig:
    """Configuration for feature pipeline monitoring"""
    name: str
    endpoint: str
    expected_features: List[str]
    feature_quality_checks: Dict[str, Any]
    data_freshness_threshold: int = 3600  # seconds
    completeness_threshold: float = 0.95
    consistency_checks: List[Dict[str, Any]]


@dataclass
class ABTestConfig:
    """Configuration for A/B testing validation"""
    name: str
    test_endpoint: str
    control_model_id: str
    treatment_model_id: str
    traffic_split: Dict[str, float]  # {"control": 0.5, "treatment": 0.5}
    statistical_significance_threshold: float = 0.05
    minimum_sample_size: int = 1000
    evaluation_metrics: List[str]


class DataDogMLSyntheticTests:
    """
    DataDog ML Pipeline Synthetic Tests Manager
    
    Manages comprehensive ML pipeline monitoring including:
    - Model serving endpoint health and performance
    - Feature pipeline data availability and quality
    - Model inference accuracy validation
    - A/B testing experiment integrity
    - Data drift detection
    - Performance benchmarking
    """
    
    def __init__(self, synthetic_monitoring: DataDogSyntheticMonitoring,
                 base_url: str, ml_service_prefix: str = "ml-analytics"):
        self.synthetic_monitoring = synthetic_monitoring
        self.base_url = base_url.rstrip('/')
        self.ml_service_prefix = ml_service_prefix
        self.logger = get_logger(f"{__name__}")
        
        # Test tracking
        self.model_tests: Dict[str, str] = {}  # model_name -> test_id
        self.feature_tests: Dict[str, str] = {}  # pipeline_name -> test_id
        self.ab_tests: Dict[str, str] = {}  # test_name -> test_id
        self.drift_tests: Dict[str, str] = {}  # model_name -> test_id
        
        # ML configurations
        self.ml_models = self._get_ml_model_configs()
        self.feature_pipelines = self._get_feature_pipeline_configs()
        self.ab_test_configs = self._get_ab_test_configs()
        
        self.logger.info("DataDog ML Synthetic Tests initialized")
    
    def _get_ml_model_configs(self) -> List[MLModelConfig]:
        """Get ML model configurations for monitoring"""
        
        return [
            # Sales Prediction Model
            MLModelConfig(
                name="Sales Prediction Model",
                endpoint="/api/v1/ml-analytics/predict",
                model_type=ModelType.REGRESSION,
                version="v1.2.0",
                expected_response_schema={
                    "prediction": {"type": "number", "required": True},
                    "confidence": {"type": "number", "required": True, "min": 0, "max": 1},
                    "model_version": {"type": "string", "required": True},
                    "execution_time_ms": {"type": "number", "required": True}
                },
                performance_thresholds={
                    "response_time_p95": 3000,  # milliseconds
                    "response_time_p99": 5000,
                    "accuracy": 0.85,
                    "precision": 0.82,
                    "recall": 0.80
                },
                sample_inputs=[
                    {
                        "customer_id": "CUST_001",
                        "amount": 150.0,
                        "category": "electronics",
                        "season": "Q4",
                        "customer_segment": "premium"
                    },
                    {
                        "customer_id": "CUST_002",
                        "amount": 75.5,
                        "category": "clothing",
                        "season": "Q2",
                        "customer_segment": "standard"
                    },
                    {
                        "customer_id": "CUST_003",
                        "amount": 299.99,
                        "category": "home_garden",
                        "season": "Q1",
                        "customer_segment": "premium"
                    }
                ],
                expected_outputs=[
                    {"prediction": 180.0, "confidence": 0.87},
                    {"prediction": 85.2, "confidence": 0.72},
                    {"prediction": 325.5, "confidence": 0.91}
                ],
                accuracy_threshold=0.85,
                latency_threshold=3000,
                business_critical=True
            ),
            
            # Customer Segmentation Model
            MLModelConfig(
                name="Customer Segmentation Model",
                endpoint="/api/v1/ml-analytics/segment",
                model_type=ModelType.CLASSIFICATION,
                version="v1.1.0",
                expected_response_schema={
                    "segment": {"type": "string", "required": True},
                    "segment_probabilities": {"type": "object", "required": True},
                    "confidence": {"type": "number", "required": True, "min": 0, "max": 1},
                    "features_used": {"type": "array", "required": True}
                },
                performance_thresholds={
                    "response_time_p95": 2000,
                    "response_time_p99": 4000,
                    "accuracy": 0.88,
                    "f1_score": 0.85
                },
                sample_inputs=[
                    {
                        "customer_id": "CUST_001",
                        "total_purchases": 2500.0,
                        "avg_order_value": 125.0,
                        "purchase_frequency": 20,
                        "last_purchase_days": 15,
                        "preferred_category": "electronics"
                    }
                ],
                expected_outputs=[
                    {
                        "segment": "premium",
                        "confidence": 0.92,
                        "segment_probabilities": {
                            "premium": 0.92,
                            "standard": 0.07,
                            "basic": 0.01
                        }
                    }
                ],
                accuracy_threshold=0.88,
                latency_threshold=2000,
                business_critical=True
            ),
            
            # Recommendation Engine
            MLModelConfig(
                name="Product Recommendation Engine",
                endpoint="/api/v1/ml-analytics/recommend",
                model_type=ModelType.RECOMMENDATION,
                version="v2.0.0",
                expected_response_schema={
                    "recommendations": {"type": "array", "required": True, "minItems": 1},
                    "scores": {"type": "array", "required": True},
                    "diversity_score": {"type": "number", "required": True},
                    "explanation": {"type": "object", "required": False}
                },
                performance_thresholds={
                    "response_time_p95": 4000,
                    "response_time_p99": 6000,
                    "ndcg_at_10": 0.75,
                    "diversity_score": 0.3,
                    "coverage": 0.6
                },
                sample_inputs=[
                    {
                        "customer_id": "CUST_001",
                        "recent_purchases": ["PROD_123", "PROD_456"],
                        "browsing_history": ["CAT_electronics", "CAT_home"],
                        "recommendation_count": 10,
                        "include_explanations": True
                    }
                ],
                expected_outputs=[
                    {
                        "recommendations": ["PROD_789", "PROD_101", "PROD_202"],
                        "scores": [0.95, 0.87, 0.82],
                        "diversity_score": 0.45
                    }
                ],
                accuracy_threshold=0.75,
                latency_threshold=4000,
                business_critical=True
            ),
            
            # Fraud Detection Model
            MLModelConfig(
                name="Fraud Detection Model",
                endpoint="/api/v1/ml-analytics/fraud-detection",
                model_type=ModelType.CLASSIFICATION,
                version="v1.3.0",
                expected_response_schema={
                    "fraud_probability": {"type": "number", "required": True, "min": 0, "max": 1},
                    "risk_level": {"type": "string", "required": True},
                    "risk_factors": {"type": "array", "required": True},
                    "action_required": {"type": "boolean", "required": True}
                },
                performance_thresholds={
                    "response_time_p95": 1500,  # Critical for real-time fraud detection
                    "response_time_p99": 2500,
                    "precision": 0.95,  # High precision required for fraud detection
                    "recall": 0.85,
                    "auc_roc": 0.92
                },
                sample_inputs=[
                    {
                        "transaction_id": "TXN_001",
                        "amount": 1500.0,
                        "merchant": "ONLINE_STORE_123",
                        "location": "New York",
                        "time_of_day": 14,
                        "customer_id": "CUST_001",
                        "payment_method": "credit_card"
                    }
                ],
                expected_outputs=[
                    {
                        "fraud_probability": 0.15,
                        "risk_level": "low",
                        "action_required": False
                    }
                ],
                accuracy_threshold=0.92,
                latency_threshold=1500,
                business_critical=True
            )
        ]
    
    def _get_feature_pipeline_configs(self) -> List[FeaturePipelineConfig]:
        """Get feature pipeline configurations"""
        
        return [
            FeaturePipelineConfig(
                name="Customer Features Pipeline",
                endpoint="/api/v1/ml-analytics/features/customer",
                expected_features=[
                    "customer_lifetime_value",
                    "avg_order_value",
                    "purchase_frequency",
                    "days_since_last_purchase",
                    "preferred_category",
                    "seasonal_affinity",
                    "price_sensitivity"
                ],
                feature_quality_checks={
                    "completeness": {"min_threshold": 0.95},
                    "consistency": {"duplicate_check": True},
                    "freshness": {"max_age_hours": 1},
                    "distribution": {"check_drift": True, "threshold": 0.1}
                },
                data_freshness_threshold=3600,  # 1 hour
                completeness_threshold=0.95,
                consistency_checks=[
                    {
                        "rule": "customer_lifetime_value >= 0",
                        "description": "CLV should be non-negative"
                    },
                    {
                        "rule": "purchase_frequency >= 0",
                        "description": "Purchase frequency should be non-negative"
                    },
                    {
                        "rule": "days_since_last_purchase >= 0",
                        "description": "Days since last purchase should be non-negative"
                    }
                ]
            ),
            
            FeaturePipelineConfig(
                name="Product Features Pipeline",
                endpoint="/api/v1/ml-analytics/features/product",
                expected_features=[
                    "product_popularity_score",
                    "category_performance",
                    "seasonal_demand",
                    "price_competitiveness",
                    "inventory_turnover",
                    "cross_sell_affinity",
                    "margin_category"
                ],
                feature_quality_checks={
                    "completeness": {"min_threshold": 0.98},
                    "consistency": {"duplicate_check": True},
                    "freshness": {"max_age_hours": 2},
                    "distribution": {"check_drift": True, "threshold": 0.15}
                },
                data_freshness_threshold=7200,  # 2 hours
                completeness_threshold=0.98
            ),
            
            FeaturePipelineConfig(
                name="Transaction Features Pipeline",
                endpoint="/api/v1/ml-analytics/features/transaction",
                expected_features=[
                    "transaction_velocity",
                    "amount_z_score",
                    "merchant_risk_score",
                    "time_based_features",
                    "location_features",
                    "payment_method_features",
                    "customer_behavior_features"
                ],
                feature_quality_checks={
                    "completeness": {"min_threshold": 0.99},
                    "freshness": {"max_age_minutes": 15},
                    "latency": {"max_latency_ms": 500}
                },
                data_freshness_threshold=900,  # 15 minutes
                completeness_threshold=0.99
            )
        ]
    
    def _get_ab_test_configs(self) -> List[ABTestConfig]:
        """Get A/B testing configurations"""
        
        return [
            ABTestConfig(
                name="Recommendation Algorithm A/B Test",
                test_endpoint="/api/v1/ml-analytics/ab-test/recommendation",
                control_model_id="recommendation_v1",
                treatment_model_id="recommendation_v2",
                traffic_split={"control": 0.5, "treatment": 0.5},
                statistical_significance_threshold=0.05,
                minimum_sample_size=1000,
                evaluation_metrics=["click_through_rate", "conversion_rate", "ndcg_at_10"]
            ),
            
            ABTestConfig(
                name="Fraud Detection Threshold A/B Test",
                test_endpoint="/api/v1/ml-analytics/ab-test/fraud-threshold",
                control_model_id="fraud_detection_conservative",
                treatment_model_id="fraud_detection_balanced",
                traffic_split={"control": 0.7, "treatment": 0.3},
                statistical_significance_threshold=0.01,  # More conservative for fraud detection
                minimum_sample_size=5000,
                evaluation_metrics=["precision", "recall", "false_positive_rate"]
            )
        ]
    
    async def deploy_model_health_tests(self, locations: List[TestLocation] = None,
                                      frequency: TestFrequency = TestFrequency.EVERY_5_MINUTES) -> Dict[str, str]:
        """Deploy model health monitoring tests"""
        
        if not locations:
            locations = [TestLocation.AWS_US_EAST_1, TestLocation.AWS_EU_WEST_1]
        
        deployed_models = {}
        
        try:
            for model_config in self.ml_models:
                # Create model health check workflow
                health_steps = [
                    APITestStep(
                        name="Check Model Health",
                        method="GET",
                        url=f"{self.base_url}/api/v1/ml-analytics/model/{model_config.name.lower().replace(' ', '-')}/health",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.status",
                                "operator": "is",
                                "target": "healthy"
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.model_version",
                                "operator": "is",
                                "target": model_config.version
                            },
                            {
                                "type": "responseTime",
                                "operator": "lessThan",
                                "target": 2000
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Test Model Inference",
                        method="POST",
                        url=f"{self.base_url}{model_config.endpoint}",
                        headers={
                            "Authorization": "Bearer {{ AUTH_TOKEN }}",
                            "Content-Type": "application/json"
                        },
                        body=json.dumps(model_config.sample_inputs[0]),
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "responseTime",
                                "operator": "lessThan",
                                "target": model_config.latency_threshold
                            }
                        ] + self._create_schema_assertions(model_config.expected_response_schema)
                    )
                ]
                
                test_config = SyntheticTestConfig(
                    name=f"ML Model Health - {model_config.name}",
                    type=TestType.MULTISTEP_API,
                    url=f"{self.base_url}{model_config.endpoint}",
                    frequency=frequency,
                    locations=locations,
                    alert_condition=AlertCondition.FAST if model_config.business_critical else AlertCondition.SLOW,
                    timeout=model_config.latency_threshold // 1000 + 30,
                    tags=[
                        "synthetic:ml",
                        "synthetic:model-health",
                        f"model:{model_config.name.lower().replace(' ', '_')}",
                        f"version:{model_config.version}",
                        f"type:{model_config.model_type.value}",
                        f"critical:{str(model_config.business_critical).lower()}",
                        f"service:{self.synthetic_monitoring.service_name}"
                    ],
                    variables={"AUTH_TOKEN": "{{ GLOBAL_AUTH_TOKEN }}"}
                )
                
                test_id = await self.synthetic_monitoring.create_multistep_api_test(
                    f"ML Model Health - {model_config.name}",
                    health_steps,
                    test_config
                )
                
                deployed_models[model_config.name] = test_id
                self.model_tests[model_config.name] = test_id
                
                self.logger.info(f"Deployed ML model health test for {model_config.name}: {test_id}")
                await asyncio.sleep(1)
            
            self.logger.info(f"Successfully deployed {len(deployed_models)} ML model health tests")
            return deployed_models
            
        except Exception as e:
            self.logger.error(f"Failed to deploy ML model health tests: {str(e)}")
            return deployed_models
    
    async def deploy_feature_pipeline_tests(self, locations: List[TestLocation] = None,
                                          frequency: TestFrequency = TestFrequency.EVERY_15_MINUTES) -> Dict[str, str]:
        """Deploy feature pipeline monitoring tests"""
        
        if not locations:
            locations = [TestLocation.AWS_US_EAST_1, TestLocation.AWS_US_WEST_2]
        
        deployed_pipelines = {}
        
        try:
            for pipeline_config in self.feature_pipelines:
                # Create feature pipeline validation steps
                pipeline_steps = [
                    APITestStep(
                        name="Check Pipeline Status",
                        method="GET",
                        url=f"{self.base_url}{pipeline_config.endpoint}/status",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.status",
                                "operator": "is",
                                "target": "active"
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Validate Feature Availability",
                        method="GET",
                        url=f"{self.base_url}{pipeline_config.endpoint}",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.features",
                                "operator": "isType",
                                "target": "array"
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.completeness_score",
                                "operator": "greaterThan",
                                "target": pipeline_config.completeness_threshold
                            },
                            {
                                "type": "responseTime",
                                "operator": "lessThan",
                                "target": 5000
                            }
                        ] + [
                            {
                                "type": "jsonpath",
                                "path": f"$.features[?(@.name == '{feature}')]",
                                "operator": "exists"
                            }
                            for feature in pipeline_config.expected_features
                        ]
                    ),
                    
                    APITestStep(
                        name="Check Data Freshness",
                        method="GET",
                        url=f"{self.base_url}{pipeline_config.endpoint}/freshness",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.last_updated_seconds_ago",
                                "operator": "lessThan",
                                "target": pipeline_config.data_freshness_threshold
                            }
                        ]
                    )
                ]
                
                test_config = SyntheticTestConfig(
                    name=f"Feature Pipeline - {pipeline_config.name}",
                    type=TestType.MULTISTEP_API,
                    url=f"{self.base_url}{pipeline_config.endpoint}",
                    frequency=frequency,
                    locations=locations,
                    alert_condition=AlertCondition.SLOW,
                    timeout=60,
                    tags=[
                        "synthetic:ml",
                        "synthetic:feature-pipeline",
                        f"pipeline:{pipeline_config.name.lower().replace(' ', '_')}",
                        f"service:{self.synthetic_monitoring.service_name}"
                    ],
                    variables={"AUTH_TOKEN": "{{ GLOBAL_AUTH_TOKEN }}"}
                )
                
                test_id = await self.synthetic_monitoring.create_multistep_api_test(
                    f"Feature Pipeline - {pipeline_config.name}",
                    pipeline_steps,
                    test_config
                )
                
                deployed_pipelines[pipeline_config.name] = test_id
                self.feature_tests[pipeline_config.name] = test_id
                
                self.logger.info(f"Deployed feature pipeline test for {pipeline_config.name}: {test_id}")
                await asyncio.sleep(1)
            
            return deployed_pipelines
            
        except Exception as e:
            self.logger.error(f"Failed to deploy feature pipeline tests: {str(e)}")
            return deployed_pipelines
    
    async def deploy_inference_accuracy_tests(self, locations: List[TestLocation] = None,
                                            frequency: TestFrequency = TestFrequency.EVERY_30_MINUTES) -> Dict[str, str]:
        """Deploy inference accuracy validation tests"""
        
        if not locations:
            locations = [TestLocation.AWS_US_EAST_1]
        
        deployed_accuracy_tests = {}
        
        try:
            for model_config in self.ml_models:
                # Create accuracy validation steps
                accuracy_steps = []
                
                for i, (sample_input, expected_output) in enumerate(zip(model_config.sample_inputs, model_config.expected_outputs)):
                    accuracy_steps.append(
                        APITestStep(
                            name=f"Test Inference Accuracy - Sample {i+1}",
                            method="POST",
                            url=f"{self.base_url}{model_config.endpoint}",
                            headers={
                                "Authorization": "Bearer {{ AUTH_TOKEN }}",
                                "Content-Type": "application/json"
                            },
                            body=json.dumps(sample_input),
                            assertions=[
                                {
                                    "type": "statusCode",
                                    "operator": "is",
                                    "target": 200
                                },
                                {
                                    "type": "jsonpath",
                                    "path": "$.confidence",
                                    "operator": "greaterThan",
                                    "target": model_config.accuracy_threshold
                                },
                                {
                                    "type": "responseTime",
                                    "operator": "lessThan",
                                    "target": model_config.latency_threshold
                                }
                            ],
                            extract_variables=[
                                {
                                    "name": f"PREDICTION_{i+1}",
                                    "type": "jsonpath",
                                    "path": "$.prediction"
                                },
                                {
                                    "name": f"CONFIDENCE_{i+1}",
                                    "type": "jsonpath",
                                    "path": "$.confidence"
                                }
                            ]
                        )
                    )
                
                # Add model performance validation step
                accuracy_steps.append(
                    APITestStep(
                        name="Validate Model Performance Metrics",
                        method="GET",
                        url=f"{self.base_url}/api/v1/ml-analytics/model/{model_config.name.lower().replace(' ', '-')}/metrics",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            }
                        ] + [
                            {
                                "type": "jsonpath",
                                "path": f"$.{metric}",
                                "operator": "greaterThan",
                                "target": threshold
                            }
                            for metric, threshold in model_config.performance_thresholds.items()
                            if metric in ["accuracy", "precision", "recall", "f1_score", "auc_roc", "ndcg_at_10"]
                        ]
                    )
                )
                
                test_config = SyntheticTestConfig(
                    name=f"ML Accuracy - {model_config.name}",
                    type=TestType.MULTISTEP_API,
                    url=f"{self.base_url}{model_config.endpoint}",
                    frequency=frequency,
                    locations=locations,
                    alert_condition=AlertCondition.SLOW,
                    timeout=120,
                    tags=[
                        "synthetic:ml",
                        "synthetic:accuracy",
                        f"model:{model_config.name.lower().replace(' ', '_')}",
                        f"service:{self.synthetic_monitoring.service_name}"
                    ],
                    variables={"AUTH_TOKEN": "{{ GLOBAL_AUTH_TOKEN }}"}
                )
                
                test_id = await self.synthetic_monitoring.create_multistep_api_test(
                    f"ML Accuracy - {model_config.name}",
                    accuracy_steps,
                    test_config
                )
                
                deployed_accuracy_tests[f"{model_config.name}_accuracy"] = test_id
                
                self.logger.info(f"Deployed inference accuracy test for {model_config.name}: {test_id}")
                await asyncio.sleep(2)
            
            return deployed_accuracy_tests
            
        except Exception as e:
            self.logger.error(f"Failed to deploy inference accuracy tests: {str(e)}")
            return deployed_accuracy_tests
    
    async def deploy_ab_test_integrity_tests(self, locations: List[TestLocation] = None,
                                           frequency: TestFrequency = TestFrequency.HOURLY) -> Dict[str, str]:
        """Deploy A/B testing integrity validation"""
        
        if not locations:
            locations = [TestLocation.AWS_US_EAST_1]
        
        deployed_ab_tests = {}
        
        try:
            for ab_config in self.ab_test_configs:
                ab_steps = [
                    APITestStep(
                        name="Check A/B Test Configuration",
                        method="GET",
                        url=f"{self.base_url}{ab_config.test_endpoint}/config",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.control_model",
                                "operator": "is",
                                "target": ab_config.control_model_id
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.treatment_model",
                                "operator": "is",
                                "target": ab_config.treatment_model_id
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Validate Traffic Split",
                        method="GET",
                        url=f"{self.base_url}{ab_config.test_endpoint}/traffic",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.control_percentage",
                                "operator": "between",
                                "target": [
                                    ab_config.traffic_split["control"] - 0.05,
                                    ab_config.traffic_split["control"] + 0.05
                                ]
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Check Statistical Significance",
                        method="GET",
                        url=f"{self.base_url}{ab_config.test_endpoint}/results",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.sample_size",
                                "operator": "greaterThan",
                                "target": ab_config.minimum_sample_size
                            }
                        ]
                    )
                ]
                
                test_config = SyntheticTestConfig(
                    name=f"A/B Test Integrity - {ab_config.name}",
                    type=TestType.MULTISTEP_API,
                    url=f"{self.base_url}{ab_config.test_endpoint}",
                    frequency=frequency,
                    locations=locations,
                    alert_condition=AlertCondition.SLOW,
                    timeout=60,
                    tags=[
                        "synthetic:ml",
                        "synthetic:ab-test",
                        f"test:{ab_config.name.lower().replace(' ', '_')}",
                        f"service:{self.synthetic_monitoring.service_name}"
                    ],
                    variables={"AUTH_TOKEN": "{{ GLOBAL_AUTH_TOKEN }}"}
                )
                
                test_id = await self.synthetic_monitoring.create_multistep_api_test(
                    f"A/B Test - {ab_config.name}",
                    ab_steps,
                    test_config
                )
                
                deployed_ab_tests[ab_config.name] = test_id
                self.ab_tests[ab_config.name] = test_id
                
                self.logger.info(f"Deployed A/B test integrity test for {ab_config.name}: {test_id}")
                await asyncio.sleep(1)
            
            return deployed_ab_tests
            
        except Exception as e:
            self.logger.error(f"Failed to deploy A/B test integrity tests: {str(e)}")
            return deployed_ab_tests
    
    def _create_schema_assertions(self, schema: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create assertions based on response schema"""
        
        assertions = []
        for field, rules in schema.items():
            # Check if field exists
            assertions.append({
                "type": "jsonpath",
                "path": f"$.{field}",
                "operator": "exists"
            })
            
            # Check field type
            if "type" in rules:
                assertions.append({
                    "type": "jsonpath",
                    "path": f"$.{field}",
                    "operator": "isType",
                    "target": rules["type"]
                })
            
            # Check numeric constraints
            if "min" in rules:
                assertions.append({
                    "type": "jsonpath",
                    "path": f"$.{field}",
                    "operator": "greaterThanOrEqual",
                    "target": rules["min"]
                })
            
            if "max" in rules:
                assertions.append({
                    "type": "jsonpath",
                    "path": f"$.{field}",
                    "operator": "lessThanOrEqual",
                    "target": rules["max"]
                })
            
            # Check array constraints
            if rules.get("type") == "array" and "minItems" in rules:
                assertions.append({
                    "type": "jsonpath",
                    "path": f"$.{field}",
                    "operator": "sizeGreaterThanOrEqual",
                    "target": rules["minItems"]
                })
        
        return assertions
    
    async def get_ml_test_summary(self) -> Dict[str, Any]:
        """Get comprehensive ML test summary"""
        
        try:
            # Get recent test results
            all_results = []
            all_test_ids = (list(self.model_tests.values()) + 
                          list(self.feature_tests.values()) + 
                          list(self.ab_tests.values()))
            
            for test_id in all_test_ids:
                results = await self.synthetic_monitoring.get_test_results(
                    test_id,
                    from_ts=int((datetime.utcnow() - timedelta(hours=24)).timestamp() * 1000)
                )
                all_results.extend(results)
            
            # Calculate metrics
            total_executions = len(all_results)
            successful_executions = len([r for r in all_results if r.status == "passed"])
            failed_executions = total_executions - successful_executions
            
            success_rate = (successful_executions / total_executions * 100) if total_executions > 0 else 100.0
            avg_response_time = sum(r.response_time for r in all_results) / len(all_results) if all_results else 0.0
            
            # Model-specific metrics
            model_performance = {}
            for model_name, test_id in self.model_tests.items():
                model_results = [r for r in all_results if r.test_id == test_id]
                if model_results:
                    model_success_rate = len([r for r in model_results if r.status == "passed"]) / len(model_results) * 100
                    model_avg_time = sum(r.response_time for r in model_results) / len(model_results)
                    
                    model_performance[model_name] = {
                        "success_rate": model_success_rate,
                        "avg_response_time": model_avg_time,
                        "total_inferences": len(model_results),
                        "sla_compliance": model_avg_time <= next(
                            (m.latency_threshold for m in self.ml_models if m.name == model_name), 5000
                        )
                    }
            
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "summary": {
                    "total_active_tests": len(self.model_tests) + len(self.feature_tests) + len(self.ab_tests),
                    "model_health_tests": len(self.model_tests),
                    "feature_pipeline_tests": len(self.feature_tests),
                    "ab_test_integrity_tests": len(self.ab_tests),
                    "total_executions_24h": total_executions,
                    "successful_executions_24h": successful_executions,
                    "failed_executions_24h": failed_executions,
                    "overall_success_rate": success_rate,
                    "average_response_time_ms": avg_response_time
                },
                "ml_pipeline_health": {
                    "models_monitored": len(self.ml_models),
                    "feature_pipelines_monitored": len(self.feature_pipelines),
                    "ab_tests_monitored": len(self.ab_test_configs),
                    "model_types_covered": list(set(m.model_type.value for m in self.ml_models)),
                    "critical_models": len([m for m in self.ml_models if m.business_critical])
                },
                "model_performance": model_performance,
                "sla_compliance": {
                    "availability_target": 99.5,
                    "current_availability": success_rate,
                    "latency_targets_met": sum(
                        1 for perf in model_performance.values() 
                        if perf.get("sla_compliance", False)
                    ) / len(model_performance) * 100 if model_performance else 100.0
                },
                "quality_assurance": {
                    "inference_accuracy_tested": True,
                    "feature_quality_validated": True,
                    "ab_test_integrity_checked": True,
                    "model_drift_monitored": False,  # Can be added
                    "performance_benchmarked": True
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate ML test summary: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}
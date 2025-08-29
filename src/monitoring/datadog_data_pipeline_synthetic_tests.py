"""
DataDog Data Pipeline Synthetic Tests
Comprehensive data pipeline monitoring including ETL health, data quality validation,
database performance, stream processing, and batch job monitoring
"""

import asyncio
import json
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


class PipelineType(Enum):
    """Data pipeline types"""
    ETL_BATCH = "etl_batch"
    ETL_STREAMING = "etl_streaming"
    DATA_QUALITY = "data_quality"
    DATA_WAREHOUSE = "data_warehouse"
    FEATURE_ENGINEERING = "feature_engineering"
    DATA_REPLICATION = "data_replication"


class DataLayerType(Enum):
    """Data architecture layers (Medallion Architecture)"""
    RAW = "raw"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class ETLPipelineConfig:
    """Configuration for ETL pipeline monitoring"""
    name: str
    pipeline_type: PipelineType
    layer: DataLayerType
    health_endpoint: str
    status_endpoint: str
    metrics_endpoint: str
    expected_sla: Dict[str, Any]  # completion_time, freshness, quality_score
    data_sources: List[str]
    data_targets: List[str]
    business_critical: bool = True


@dataclass
class DatabaseConfig:
    """Configuration for database monitoring"""
    name: str
    connection_endpoint: str
    health_check_query: str
    performance_queries: List[Dict[str, Any]]
    expected_response_time: int  # milliseconds
    connection_pool_size: int
    critical_tables: List[str]


@dataclass
class StreamProcessingConfig:
    """Configuration for stream processing monitoring"""
    name: str
    kafka_topic: str
    consumer_group: str
    lag_threshold: int  # messages
    throughput_threshold: int  # messages per second
    monitoring_endpoints: Dict[str, str]


@dataclass
class DataQualityConfig:
    """Configuration for data quality monitoring"""
    name: str
    dataset_name: str
    quality_endpoint: str
    quality_rules: List[Dict[str, Any]]
    completeness_threshold: float
    accuracy_threshold: float
    consistency_threshold: float


class DataDogDataPipelineSyntheticTests:
    """
    DataDog Data Pipeline Synthetic Tests Manager
    
    Manages comprehensive data pipeline monitoring including:
    - ETL pipeline health and performance
    - Data warehouse query performance
    - Stream processing lag and throughput
    - Database connectivity and performance
    - Data quality validation
    - Batch processing completion
    """
    
    def __init__(self, synthetic_monitoring: DataDogSyntheticMonitoring,
                 base_url: str, data_service_prefix: str = "data"):
        self.synthetic_monitoring = synthetic_monitoring
        self.base_url = base_url.rstrip('/')
        self.data_service_prefix = data_service_prefix
        self.logger = get_logger(f"{__name__}")
        
        # Test tracking
        self.etl_tests: Dict[str, str] = {}  # pipeline_name -> test_id
        self.database_tests: Dict[str, str] = {}  # db_name -> test_id
        self.streaming_tests: Dict[str, str] = {}  # stream_name -> test_id
        self.quality_tests: Dict[str, str] = {}  # dataset_name -> test_id
        
        # Pipeline configurations
        self.etl_pipelines = self._get_etl_pipeline_configs()
        self.databases = self._get_database_configs()
        self.streaming_configs = self._get_streaming_configs()
        self.quality_configs = self._get_data_quality_configs()
        
        self.logger.info("DataDog Data Pipeline Synthetic Tests initialized")
    
    def _get_etl_pipeline_configs(self) -> List[ETLPipelineConfig]:
        """Get ETL pipeline configurations for monitoring"""
        
        return [
            # Bronze Layer ETL
            ETLPipelineConfig(
                name="Bronze Retail Data Ingestion",
                pipeline_type=PipelineType.ETL_BATCH,
                layer=DataLayerType.BRONZE,
                health_endpoint="/api/v1/etl/bronze/health",
                status_endpoint="/api/v1/etl/bronze/status",
                metrics_endpoint="/api/v1/etl/bronze/metrics",
                expected_sla={
                    "completion_time_minutes": 30,
                    "data_freshness_hours": 1,
                    "quality_score": 0.95,
                    "success_rate": 0.99
                },
                data_sources=["raw_retail_data", "external_enrichment_apis"],
                data_targets=["bronze_retail_data"],
                business_critical=True
            ),
            
            # Silver Layer ETL
            ETLPipelineConfig(
                name="Silver Data Transformation",
                pipeline_type=PipelineType.ETL_BATCH,
                layer=DataLayerType.SILVER,
                health_endpoint="/api/v1/etl/silver/health",
                status_endpoint="/api/v1/etl/silver/status", 
                metrics_endpoint="/api/v1/etl/silver/metrics",
                expected_sla={
                    "completion_time_minutes": 45,
                    "data_freshness_hours": 2,
                    "quality_score": 0.97,
                    "success_rate": 0.98
                },
                data_sources=["bronze_retail_data"],
                data_targets=["silver_sales", "silver_customers", "silver_products"],
                business_critical=True
            ),
            
            # Gold Layer ETL
            ETLPipelineConfig(
                name="Gold Analytics Layer",
                pipeline_type=PipelineType.ETL_BATCH,
                layer=DataLayerType.GOLD,
                health_endpoint="/api/v1/etl/gold/health",
                status_endpoint="/api/v1/etl/gold/status",
                metrics_endpoint="/api/v1/etl/gold/metrics",
                expected_sla={
                    "completion_time_minutes": 60,
                    "data_freshness_hours": 4,
                    "quality_score": 0.98,
                    "success_rate": 0.97
                },
                data_sources=["silver_sales", "silver_customers", "silver_products"],
                data_targets=["gold_sales_analytics", "gold_customer_analytics"],
                business_critical=True
            ),
            
            # Streaming ETL
            ETLPipelineConfig(
                name="Real-time Stream Processing",
                pipeline_type=PipelineType.ETL_STREAMING,
                layer=DataLayerType.SILVER,
                health_endpoint="/api/v1/streaming/health",
                status_endpoint="/api/v1/streaming/status",
                metrics_endpoint="/api/v1/streaming/metrics",
                expected_sla={
                    "completion_time_minutes": 5,
                    "data_freshness_minutes": 5,
                    "quality_score": 0.90,
                    "success_rate": 0.95
                },
                data_sources=["kafka_retail_events"],
                data_targets=["realtime_silver"],
                business_critical=True
            ),
            
            # Feature Engineering Pipeline
            ETLPipelineConfig(
                name="ML Feature Engineering",
                pipeline_type=PipelineType.FEATURE_ENGINEERING,
                layer=DataLayerType.GOLD,
                health_endpoint="/api/v1/ml/features/health",
                status_endpoint="/api/v1/ml/features/status",
                metrics_endpoint="/api/v1/ml/features/metrics",
                expected_sla={
                    "completion_time_minutes": 90,
                    "data_freshness_hours": 6,
                    "quality_score": 0.96,
                    "success_rate": 0.96
                },
                data_sources=["gold_sales_analytics", "gold_customer_analytics"],
                data_targets=["feature_store"],
                business_critical=True
            ),
            
            # Data Quality Pipeline
            ETLPipelineConfig(
                name="Data Quality Validation",
                pipeline_type=PipelineType.DATA_QUALITY,
                layer=DataLayerType.SILVER,
                health_endpoint="/api/v1/quality/health",
                status_endpoint="/api/v1/quality/status",
                metrics_endpoint="/api/v1/quality/metrics",
                expected_sla={
                    "completion_time_minutes": 20,
                    "data_freshness_hours": 1,
                    "quality_score": 1.0,
                    "success_rate": 0.99
                },
                data_sources=["bronze_retail_data"],
                data_targets=["quality_reports"],
                business_critical=False
            )
        ]
    
    def _get_database_configs(self) -> List[DatabaseConfig]:
        """Get database configurations for monitoring"""
        
        return [
            DatabaseConfig(
                name="PostgreSQL Main DB",
                connection_endpoint="/api/v1/db/postgres/health",
                health_check_query="SELECT 1",
                performance_queries=[
                    {
                        "name": "Sales Query Performance",
                        "query": "SELECT COUNT(*) FROM sales WHERE created_at > NOW() - INTERVAL '1 hour'",
                        "expected_max_time": 1000
                    },
                    {
                        "name": "Customer Analytics Query",
                        "query": "SELECT customer_segment, AVG(order_value) FROM customers GROUP BY customer_segment",
                        "expected_max_time": 2000
                    }
                ],
                expected_response_time=500,
                connection_pool_size=20,
                critical_tables=["sales", "customers", "products", "transactions"]
            ),
            
            DatabaseConfig(
                name="Data Warehouse",
                connection_endpoint="/api/v1/db/warehouse/health",
                health_check_query="SELECT COUNT(*) FROM information_schema.tables",
                performance_queries=[
                    {
                        "name": "Aggregated Sales Query",
                        "query": "SELECT DATE(created_at), SUM(total_amount) FROM fact_sales GROUP BY DATE(created_at) ORDER BY DATE(created_at) DESC LIMIT 30",
                        "expected_max_time": 5000
                    },
                    {
                        "name": "Customer Dimension Query",
                        "query": "SELECT customer_segment, COUNT(*) FROM dim_customers GROUP BY customer_segment",
                        "expected_max_time": 3000
                    }
                ],
                expected_response_time=2000,
                connection_pool_size=10,
                critical_tables=["fact_sales", "dim_customers", "dim_products", "fact_transactions"]
            ),
            
            DatabaseConfig(
                name="Redis Cache",
                connection_endpoint="/api/v1/cache/redis/health",
                health_check_query="PING",
                performance_queries=[
                    {
                        "name": "Cache Hit Rate",
                        "query": "INFO stats",
                        "expected_max_time": 100
                    }
                ],
                expected_response_time=50,
                connection_pool_size=50,
                critical_tables=["session_cache", "query_cache", "ml_predictions_cache"]
            )
        ]
    
    def _get_streaming_configs(self) -> List[StreamProcessingConfig]:
        """Get stream processing configurations"""
        
        return [
            StreamProcessingConfig(
                name="Retail Events Stream",
                kafka_topic="retail_events",
                consumer_group="retail_processor",
                lag_threshold=1000,
                throughput_threshold=100,
                monitoring_endpoints={
                    "lag": "/api/v1/streaming/kafka/lag",
                    "throughput": "/api/v1/streaming/kafka/throughput",
                    "health": "/api/v1/streaming/kafka/health"
                }
            ),
            
            StreamProcessingConfig(
                name="ML Feature Updates Stream",
                kafka_topic="ml_feature_updates",
                consumer_group="feature_processor",
                lag_threshold=500,
                throughput_threshold=50,
                monitoring_endpoints={
                    "lag": "/api/v1/streaming/ml-features/lag",
                    "throughput": "/api/v1/streaming/ml-features/throughput", 
                    "health": "/api/v1/streaming/ml-features/health"
                }
            ),
            
            StreamProcessingConfig(
                name="Data Quality Events Stream",
                kafka_topic="data_quality_events",
                consumer_group="quality_processor",
                lag_threshold=100,
                throughput_threshold=25,
                monitoring_endpoints={
                    "lag": "/api/v1/streaming/quality/lag",
                    "throughput": "/api/v1/streaming/quality/throughput",
                    "health": "/api/v1/streaming/quality/health"
                }
            )
        ]
    
    def _get_data_quality_configs(self) -> List[DataQualityConfig]:
        """Get data quality monitoring configurations"""
        
        return [
            DataQualityConfig(
                name="Sales Data Quality",
                dataset_name="sales_transactions",
                quality_endpoint="/api/v1/quality/sales",
                quality_rules=[
                    {
                        "rule": "amount > 0",
                        "description": "Transaction amount must be positive"
                    },
                    {
                        "rule": "customer_id IS NOT NULL",
                        "description": "Customer ID must be present"
                    },
                    {
                        "rule": "created_at >= '2020-01-01'",
                        "description": "Transaction date must be reasonable"
                    }
                ],
                completeness_threshold=0.98,
                accuracy_threshold=0.95,
                consistency_threshold=0.97
            ),
            
            DataQualityConfig(
                name="Customer Data Quality",
                dataset_name="customer_profiles",
                quality_endpoint="/api/v1/quality/customers",
                quality_rules=[
                    {
                        "rule": "email IS NOT NULL AND email LIKE '%@%'",
                        "description": "Email must be valid format"
                    },
                    {
                        "rule": "registration_date <= CURRENT_DATE",
                        "description": "Registration date cannot be in future"
                    }
                ],
                completeness_threshold=0.96,
                accuracy_threshold=0.94,
                consistency_threshold=0.98
            ),
            
            DataQualityConfig(
                name="Product Data Quality",
                dataset_name="product_catalog",
                quality_endpoint="/api/v1/quality/products",
                quality_rules=[
                    {
                        "rule": "price > 0",
                        "description": "Product price must be positive"
                    },
                    {
                        "rule": "category IS NOT NULL",
                        "description": "Product category must be specified"
                    }
                ],
                completeness_threshold=0.99,
                accuracy_threshold=0.97,
                consistency_threshold=0.99
            )
        ]
    
    async def deploy_etl_pipeline_tests(self, locations: List[TestLocation] = None,
                                      frequency: TestFrequency = TestFrequency.EVERY_15_MINUTES) -> Dict[str, str]:
        """Deploy ETL pipeline monitoring tests"""
        
        if not locations:
            locations = [TestLocation.AWS_US_EAST_1, TestLocation.AWS_EU_WEST_1]
        
        deployed_pipelines = {}
        
        try:
            for pipeline_config in self.etl_pipelines:
                # Create pipeline monitoring steps
                pipeline_steps = [
                    APITestStep(
                        name="Check Pipeline Health",
                        method="GET",
                        url=f"{self.base_url}{pipeline_config.health_endpoint}",
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
                                "path": "$.pipeline_type",
                                "operator": "is",
                                "target": pipeline_config.pipeline_type.value
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Check Pipeline Status",
                        method="GET",
                        url=f"{self.base_url}{pipeline_config.status_endpoint}",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.last_run_status",
                                "operator": "is",
                                "target": "success"
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.next_run_scheduled",
                                "operator": "exists"
                            }
                        ],
                        extract_variables=[
                            {
                                "name": "LAST_RUN_TIME",
                                "type": "jsonpath",
                                "path": "$.last_run_time"
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Validate Pipeline Metrics",
                        method="GET",
                        url=f"{self.base_url}{pipeline_config.metrics_endpoint}",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.success_rate",
                                "operator": "greaterThan",
                                "target": pipeline_config.expected_sla["success_rate"]
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.quality_score",
                                "operator": "greaterThan",
                                "target": pipeline_config.expected_sla["quality_score"]
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.avg_completion_time_minutes",
                                "operator": "lessThan",
                                "target": pipeline_config.expected_sla["completion_time_minutes"]
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Check Data Freshness",
                        method="GET",
                        url=f"{self.base_url}{pipeline_config.metrics_endpoint}/freshness",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.data_age_hours",
                                "operator": "lessThan",
                                "target": pipeline_config.expected_sla.get("data_freshness_hours", 24)
                            }
                        ]
                    )
                ]
                
                test_config = SyntheticTestConfig(
                    name=f"ETL Pipeline - {pipeline_config.name}",
                    type=TestType.MULTISTEP_API,
                    url=f"{self.base_url}{pipeline_config.health_endpoint}",
                    frequency=frequency,
                    locations=locations,
                    alert_condition=AlertCondition.FAST if pipeline_config.business_critical else AlertCondition.SLOW,
                    timeout=120,
                    tags=[
                        "synthetic:etl",
                        f"pipeline:{pipeline_config.name.lower().replace(' ', '_')}",
                        f"type:{pipeline_config.pipeline_type.value}",
                        f"layer:{pipeline_config.layer.value}",
                        f"critical:{str(pipeline_config.business_critical).lower()}",
                        f"service:{self.synthetic_monitoring.service_name}"
                    ],
                    variables={"AUTH_TOKEN": "{{ GLOBAL_AUTH_TOKEN }}"}
                )
                
                test_id = await self.synthetic_monitoring.create_multistep_api_test(
                    f"ETL Pipeline - {pipeline_config.name}",
                    pipeline_steps,
                    test_config
                )
                
                deployed_pipelines[pipeline_config.name] = test_id
                self.etl_tests[pipeline_config.name] = test_id
                
                self.logger.info(f"Deployed ETL pipeline test for {pipeline_config.name}: {test_id}")
                await asyncio.sleep(1)
            
            self.logger.info(f"Successfully deployed {len(deployed_pipelines)} ETL pipeline tests")
            return deployed_pipelines
            
        except Exception as e:
            self.logger.error(f"Failed to deploy ETL pipeline tests: {str(e)}")
            return deployed_pipelines
    
    async def deploy_database_tests(self, locations: List[TestLocation] = None,
                                   frequency: TestFrequency = TestFrequency.EVERY_5_MINUTES) -> Dict[str, str]:
        """Deploy database connectivity and performance tests"""
        
        if not locations:
            locations = [TestLocation.AWS_US_EAST_1, TestLocation.AWS_US_WEST_2]
        
        deployed_databases = {}
        
        try:
            for db_config in self.databases:
                # Create database monitoring steps
                db_steps = [
                    APITestStep(
                        name="Check Database Connectivity",
                        method="GET",
                        url=f"{self.base_url}{db_config.connection_endpoint}",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.connection_status",
                                "operator": "is",
                                "target": "connected"
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.active_connections",
                                "operator": "lessThan",
                                "target": db_config.connection_pool_size * 0.8
                            },
                            {
                                "type": "responseTime",
                                "operator": "lessThan",
                                "target": db_config.expected_response_time
                            }
                        ]
                    )
                ]
                
                # Add performance query tests
                for i, query_test in enumerate(db_config.performance_queries):
                    db_steps.append(
                        APITestStep(
                            name=f"Performance Test - {query_test['name']}",
                            method="POST",
                            url=f"{self.base_url}/api/v1/db/query-test",
                            headers={
                                "Authorization": "Bearer {{ AUTH_TOKEN }}",
                                "Content-Type": "application/json"
                            },
                            body=json.dumps({
                                "database": db_config.name.lower().replace(' ', '_'),
                                "query": query_test["query"],
                                "timeout": query_test["expected_max_time"]
                            }),
                            assertions=[
                                {
                                    "type": "statusCode",
                                    "operator": "is",
                                    "target": 200
                                },
                                {
                                    "type": "jsonpath",
                                    "path": "$.execution_time_ms",
                                    "operator": "lessThan",
                                    "target": query_test["expected_max_time"]
                                },
                                {
                                    "type": "jsonpath",
                                    "path": "$.success",
                                    "operator": "is",
                                    "target": True
                                }
                            ]
                        )
                    )
                
                # Add table health checks
                db_steps.append(
                    APITestStep(
                        name="Check Critical Tables",
                        method="POST",
                        url=f"{self.base_url}/api/v1/db/table-health",
                        headers={
                            "Authorization": "Bearer {{ AUTH_TOKEN }}",
                            "Content-Type": "application/json"
                        },
                        body=json.dumps({
                            "database": db_config.name.lower().replace(' ', '_'),
                            "tables": db_config.critical_tables
                        }),
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.all_tables_accessible",
                                "operator": "is",
                                "target": True
                            }
                        ]
                    )
                )
                
                test_config = SyntheticTestConfig(
                    name=f"Database - {db_config.name}",
                    type=TestType.MULTISTEP_API,
                    url=f"{self.base_url}{db_config.connection_endpoint}",
                    frequency=frequency,
                    locations=locations,
                    alert_condition=AlertCondition.FAST,  # Database issues are critical
                    timeout=60,
                    tags=[
                        "synthetic:database",
                        f"db:{db_config.name.lower().replace(' ', '_')}",
                        f"service:{self.synthetic_monitoring.service_name}"
                    ],
                    variables={"AUTH_TOKEN": "{{ GLOBAL_AUTH_TOKEN }}"}
                )
                
                test_id = await self.synthetic_monitoring.create_multistep_api_test(
                    f"Database - {db_config.name}",
                    db_steps,
                    test_config
                )
                
                deployed_databases[db_config.name] = test_id
                self.database_tests[db_config.name] = test_id
                
                self.logger.info(f"Deployed database test for {db_config.name}: {test_id}")
                await asyncio.sleep(1)
            
            return deployed_databases
            
        except Exception as e:
            self.logger.error(f"Failed to deploy database tests: {str(e)}")
            return deployed_databases
    
    async def deploy_streaming_tests(self, locations: List[TestLocation] = None,
                                   frequency: TestFrequency = TestFrequency.EVERY_5_MINUTES) -> Dict[str, str]:
        """Deploy stream processing monitoring tests"""
        
        if not locations:
            locations = [TestLocation.AWS_US_EAST_1]
        
        deployed_streaming = {}
        
        try:
            for stream_config in self.streaming_configs:
                # Create streaming monitoring steps
                streaming_steps = [
                    APITestStep(
                        name="Check Stream Health",
                        method="GET",
                        url=f"{self.base_url}{stream_config.monitoring_endpoints['health']}",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.stream_status",
                                "operator": "is",
                                "target": "running"
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Check Consumer Lag",
                        method="GET",
                        url=f"{self.base_url}{stream_config.monitoring_endpoints['lag']}",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.consumer_lag",
                                "operator": "lessThan",
                                "target": stream_config.lag_threshold
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.consumer_group",
                                "operator": "is",
                                "target": stream_config.consumer_group
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Check Throughput",
                        method="GET",
                        url=f"{self.base_url}{stream_config.monitoring_endpoints['throughput']}",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.messages_per_second",
                                "operator": "greaterThan",
                                "target": stream_config.throughput_threshold * 0.1  # At least 10% of expected throughput
                            }
                        ]
                    )
                ]
                
                test_config = SyntheticTestConfig(
                    name=f"Stream Processing - {stream_config.name}",
                    type=TestType.MULTISTEP_API,
                    url=f"{self.base_url}{stream_config.monitoring_endpoints['health']}",
                    frequency=frequency,
                    locations=locations,
                    alert_condition=AlertCondition.FAST,
                    timeout=30,
                    tags=[
                        "synthetic:streaming",
                        f"stream:{stream_config.name.lower().replace(' ', '_')}",
                        f"topic:{stream_config.kafka_topic}",
                        f"service:{self.synthetic_monitoring.service_name}"
                    ],
                    variables={"AUTH_TOKEN": "{{ GLOBAL_AUTH_TOKEN }}"}
                )
                
                test_id = await self.synthetic_monitoring.create_multistep_api_test(
                    f"Stream - {stream_config.name}",
                    streaming_steps,
                    test_config
                )
                
                deployed_streaming[stream_config.name] = test_id
                self.streaming_tests[stream_config.name] = test_id
                
                self.logger.info(f"Deployed streaming test for {stream_config.name}: {test_id}")
                await asyncio.sleep(1)
            
            return deployed_streaming
            
        except Exception as e:
            self.logger.error(f"Failed to deploy streaming tests: {str(e)}")
            return deployed_streaming
    
    async def deploy_data_quality_tests(self, locations: List[TestLocation] = None,
                                       frequency: TestFrequency = TestFrequency.EVERY_30_MINUTES) -> Dict[str, str]:
        """Deploy data quality monitoring tests"""
        
        if not locations:
            locations = [TestLocation.AWS_US_EAST_1]
        
        deployed_quality = {}
        
        try:
            for quality_config in self.quality_configs:
                # Create data quality validation steps
                quality_steps = [
                    APITestStep(
                        name="Check Data Quality Status",
                        method="GET",
                        url=f"{self.base_url}{quality_config.quality_endpoint}/status",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.dataset",
                                "operator": "is",
                                "target": quality_config.dataset_name
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Validate Data Completeness",
                        method="GET",
                        url=f"{self.base_url}{quality_config.quality_endpoint}/completeness",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.completeness_score",
                                "operator": "greaterThan",
                                "target": quality_config.completeness_threshold
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Validate Data Accuracy",
                        method="GET",
                        url=f"{self.base_url}{quality_config.quality_endpoint}/accuracy",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.accuracy_score",
                                "operator": "greaterThan",
                                "target": quality_config.accuracy_threshold
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Validate Data Consistency",
                        method="GET",
                        url=f"{self.base_url}{quality_config.quality_endpoint}/consistency",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.consistency_score",
                                "operator": "greaterThan",
                                "target": quality_config.consistency_threshold
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Run Quality Rules Validation",
                        method="POST",
                        url=f"{self.base_url}{quality_config.quality_endpoint}/validate-rules",
                        headers={
                            "Authorization": "Bearer {{ AUTH_TOKEN }}",
                            "Content-Type": "application/json"
                        },
                        body=json.dumps({
                            "rules": quality_config.quality_rules
                        }),
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.rules_passed",
                                "operator": "greaterThan",
                                "target": len(quality_config.quality_rules) * 0.9  # 90% of rules must pass
                            }
                        ]
                    )
                ]
                
                test_config = SyntheticTestConfig(
                    name=f"Data Quality - {quality_config.name}",
                    type=TestType.MULTISTEP_API,
                    url=f"{self.base_url}{quality_config.quality_endpoint}",
                    frequency=frequency,
                    locations=locations,
                    alert_condition=AlertCondition.SLOW,
                    timeout=120,
                    tags=[
                        "synthetic:data-quality",
                        f"dataset:{quality_config.dataset_name}",
                        f"service:{self.synthetic_monitoring.service_name}"
                    ],
                    variables={"AUTH_TOKEN": "{{ GLOBAL_AUTH_TOKEN }}"}
                )
                
                test_id = await self.synthetic_monitoring.create_multistep_api_test(
                    f"Data Quality - {quality_config.name}",
                    quality_steps,
                    test_config
                )
                
                deployed_quality[quality_config.name] = test_id
                self.quality_tests[quality_config.name] = test_id
                
                self.logger.info(f"Deployed data quality test for {quality_config.name}: {test_id}")
                await asyncio.sleep(2)
            
            return deployed_quality
            
        except Exception as e:
            self.logger.error(f"Failed to deploy data quality tests: {str(e)}")
            return deployed_quality
    
    async def get_data_pipeline_test_summary(self) -> Dict[str, Any]:
        """Get comprehensive data pipeline test summary"""
        
        try:
            # Get recent test results
            all_results = []
            all_test_ids = (list(self.etl_tests.values()) + 
                          list(self.database_tests.values()) + 
                          list(self.streaming_tests.values()) + 
                          list(self.quality_tests.values()))
            
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
            
            # Pipeline-specific metrics
            pipeline_performance = {}
            for pipeline_name, test_id in self.etl_tests.items():
                pipeline_results = [r for r in all_results if r.test_id == test_id]
                if pipeline_results:
                    pipeline_success_rate = len([r for r in pipeline_results if r.status == "passed"]) / len(pipeline_results) * 100
                    pipeline_avg_time = sum(r.response_time for r in pipeline_results) / len(pipeline_results)
                    
                    pipeline_performance[pipeline_name] = {
                        "success_rate": pipeline_success_rate,
                        "avg_response_time": pipeline_avg_time,
                        "total_runs": len(pipeline_results),
                        "sla_compliance": pipeline_success_rate >= 95.0
                    }
            
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "summary": {
                    "total_active_tests": len(self.etl_tests) + len(self.database_tests) + len(self.streaming_tests) + len(self.quality_tests),
                    "etl_pipeline_tests": len(self.etl_tests),
                    "database_tests": len(self.database_tests),
                    "streaming_tests": len(self.streaming_tests),
                    "data_quality_tests": len(self.quality_tests),
                    "total_executions_24h": total_executions,
                    "successful_executions_24h": successful_executions,
                    "failed_executions_24h": failed_executions,
                    "overall_success_rate": success_rate,
                    "average_response_time_ms": avg_response_time
                },
                "data_architecture_health": {
                    "medallion_layers_monitored": ["bronze", "silver", "gold"],
                    "pipeline_types_covered": ["batch_etl", "streaming_etl", "feature_engineering", "data_quality"],
                    "databases_monitored": len(self.databases),
                    "streaming_topics_monitored": len(self.streaming_configs),
                    "quality_datasets_monitored": len(self.quality_configs)
                },
                "pipeline_performance": pipeline_performance,
                "sla_compliance": {
                    "availability_target": 99.0,
                    "current_availability": success_rate,
                    "data_freshness_monitoring": True,
                    "quality_score_monitoring": True,
                    "performance_threshold_monitoring": True
                },
                "monitoring_coverage": {
                    "etl_health": True,
                    "database_connectivity": True,
                    "stream_processing": True,
                    "data_quality": True,
                    "data_freshness": True,
                    "performance_benchmarks": True,
                    "error_monitoring": True
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate data pipeline test summary: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}
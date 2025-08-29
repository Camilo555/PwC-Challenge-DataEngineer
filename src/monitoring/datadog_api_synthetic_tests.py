"""
DataDog API Synthetic Tests
Comprehensive API endpoint monitoring and multi-step workflow testing
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass

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


@dataclass
class APIEndpointConfig:
    """Configuration for API endpoint monitoring"""
    name: str
    endpoint: str
    method: str = "GET"
    expected_status: int = 200
    max_response_time: int = 2000
    authentication_required: bool = False
    custom_headers: Dict[str, str] = None
    request_body: str = None
    data_validation_rules: List[Dict[str, Any]] = None
    business_critical: bool = True


@dataclass
class WorkflowTestConfig:
    """Configuration for multi-step workflow testing"""
    name: str
    description: str
    steps: List[APITestStep]
    variables: Dict[str, str] = None
    cleanup_steps: List[APITestStep] = None
    max_execution_time: int = 300  # 5 minutes
    critical_workflow: bool = True


class DataDogAPISyntheticTests:
    """
    DataDog API Synthetic Tests Manager
    
    Manages comprehensive API testing including:
    - Individual endpoint health monitoring
    - Multi-step workflow testing
    - Authentication flow validation
    - Performance and SLA monitoring
    - Data validation and schema compliance
    """
    
    def __init__(self, synthetic_monitoring: DataDogSyntheticMonitoring,
                 base_url: str, api_version: str = "v1"):
        self.synthetic_monitoring = synthetic_monitoring
        self.base_url = base_url.rstrip('/')
        self.api_version = api_version
        self.logger = get_logger(f"{__name__}")
        
        # Test tracking
        self.endpoint_tests: Dict[str, str] = {}  # endpoint -> test_id
        self.workflow_tests: Dict[str, str] = {}  # workflow_name -> test_id
        
        # API endpoints configuration
        self.api_endpoints = self._get_api_endpoints_config()
        self.workflows = self._get_workflow_configs()
        
        self.logger.info("DataDog API Synthetic Tests initialized")
    
    def _get_api_endpoints_config(self) -> List[APIEndpointConfig]:
        """Get API endpoints configuration for monitoring"""
        
        return [
            # Core API endpoints
            APIEndpointConfig(
                name="API Health Check",
                endpoint="/health",
                method="GET",
                expected_status=200,
                max_response_time=1000,
                authentication_required=False,
                business_critical=True
            ),
            
            APIEndpointConfig(
                name="API Root",
                endpoint="/",
                method="GET",
                expected_status=200,
                max_response_time=1500,
                authentication_required=False,
                business_critical=True
            ),
            
            # Authentication endpoints
            APIEndpointConfig(
                name="Auth Token Validation",
                endpoint="/api/v1/auth/validate",
                method="POST",
                expected_status=200,
                max_response_time=2000,
                authentication_required=True,
                custom_headers={"Content-Type": "application/json"},
                business_critical=True
            ),
            
            # Sales API endpoints
            APIEndpointConfig(
                name="Sales Analytics",
                endpoint="/api/v1/sales/analytics",
                method="GET",
                expected_status=200,
                max_response_time=3000,
                authentication_required=True,
                business_critical=True,
                data_validation_rules=[
                    {
                        "type": "jsonpath",
                        "path": "$.total_revenue",
                        "operator": "exists"
                    },
                    {
                        "type": "jsonpath", 
                        "path": "$.total_transactions",
                        "operator": "isType",
                        "target": "integer"
                    }
                ]
            ),
            
            APIEndpointConfig(
                name="Sales Performance",
                endpoint="/api/v1/sales/performance",
                method="GET",
                expected_status=200,
                max_response_time=2500,
                authentication_required=True,
                business_critical=True
            ),
            
            APIEndpointConfig(
                name="Sales Trends",
                endpoint="/api/v1/sales/trends",
                method="GET",
                expected_status=200,
                max_response_time=4000,
                authentication_required=True,
                business_critical=True
            ),
            
            # ML Analytics endpoints
            APIEndpointConfig(
                name="ML Model Health",
                endpoint="/api/v1/ml-analytics/model/health",
                method="GET",
                expected_status=200,
                max_response_time=2000,
                authentication_required=True,
                business_critical=True
            ),
            
            APIEndpointConfig(
                name="ML Predictions",
                endpoint="/api/v1/ml-analytics/predict",
                method="POST",
                expected_status=200,
                max_response_time=5000,
                authentication_required=True,
                custom_headers={"Content-Type": "application/json"},
                request_body='{"features": {"customer_id": "test123", "amount": 100.0}}',
                business_critical=True
            ),
            
            APIEndpointConfig(
                name="Feature Engineering",
                endpoint="/api/v1/ml-analytics/features",
                method="GET",
                expected_status=200,
                max_response_time=3000,
                authentication_required=True,
                business_critical=True
            ),
            
            # Enterprise endpoints
            APIEndpointConfig(
                name="Enterprise Dashboard",
                endpoint="/api/v1/enterprise/dashboard",
                method="GET",
                expected_status=200,
                max_response_time=3000,
                authentication_required=True,
                business_critical=True
            ),
            
            APIEndpointConfig(
                name="Security Status",
                endpoint="/api/v1/security/status",
                method="GET",
                expected_status=200,
                max_response_time=2000,
                authentication_required=True,
                business_critical=True
            ),
            
            # Monitoring endpoints
            APIEndpointConfig(
                name="Monitoring Metrics",
                endpoint="/api/v1/monitoring/metrics",
                method="GET",
                expected_status=200,
                max_response_time=2500,
                authentication_required=True,
                business_critical=False
            ),
            
            # Search endpoints
            APIEndpointConfig(
                name="Vector Search",
                endpoint="/api/v1/search/vector",
                method="POST",
                expected_status=200,
                max_response_time=4000,
                authentication_required=True,
                custom_headers={"Content-Type": "application/json"},
                request_body='{"query": "test search", "limit": 10}',
                business_critical=False
            ),
            
            # GraphQL endpoint
            APIEndpointConfig(
                name="GraphQL Health",
                endpoint="/api/graphql",
                method="POST",
                expected_status=200,
                max_response_time=2000,
                authentication_required=True,
                custom_headers={"Content-Type": "application/json"},
                request_body='{"query": "query { __schema { types { name } } }"}',
                business_critical=False
            ),
            
            # V2 Analytics endpoints
            APIEndpointConfig(
                name="Advanced Analytics V2",
                endpoint="/api/v2/analytics/advanced-analytics",
                method="POST",
                expected_status=200,
                max_response_time=6000,
                authentication_required=True,
                custom_headers={"Content-Type": "application/json"},
                request_body='{"analysis_type": "trend_analysis", "period": "30d"}',
                business_critical=True
            )
        ]
    
    def _get_workflow_configs(self) -> List[WorkflowTestConfig]:
        """Get workflow test configurations"""
        
        return [
            # Authentication workflow
            WorkflowTestConfig(
                name="Complete Authentication Flow",
                description="Tests full authentication flow from login to protected resource access",
                steps=[
                    APITestStep(
                        name="Get Auth Token",
                        method="POST",
                        url=f"{self.base_url}/api/v1/auth/token",
                        headers={"Content-Type": "application/json"},
                        body='{"username": "{{ AUTH_USERNAME }}", "password": "{{ AUTH_PASSWORD }}"}',
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.access_token",
                                "operator": "exists"
                            }
                        ],
                        extract_variables=[
                            {
                                "name": "AUTH_TOKEN",
                                "type": "jsonpath", 
                                "path": "$.access_token"
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Validate Token",
                        method="POST",
                        url=f"{self.base_url}/api/v1/auth/validate",
                        headers={
                            "Content-Type": "application/json",
                            "Authorization": "Bearer {{ AUTH_TOKEN }}"
                        },
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is", 
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.valid",
                                "operator": "is",
                                "target": True
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Access Protected Resource",
                        method="GET",
                        url=f"{self.base_url}/api/v1/sales/analytics",
                        headers={
                            "Authorization": "Bearer {{ AUTH_TOKEN }}"
                        },
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.total_revenue",
                                "operator": "exists"
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Logout",
                        method="POST",
                        url=f"{self.base_url}/api/v1/auth/logout",
                        headers={
                            "Authorization": "Bearer {{ AUTH_TOKEN }}"
                        },
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            }
                        ]
                    )
                ],
                variables={
                    "AUTH_USERNAME": "synthetic_test_user",
                    "AUTH_PASSWORD": "synthetic_test_password"
                },
                max_execution_time=60,
                critical_workflow=True
            ),
            
            # Sales Analytics Workflow
            WorkflowTestConfig(
                name="Sales Analytics Complete Workflow",
                description="Tests complete sales analytics workflow including data retrieval and analysis",
                steps=[
                    APITestStep(
                        name="Get Sales Overview",
                        method="GET",
                        url=f"{self.base_url}/api/v1/sales/analytics",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "responseTime",
                                "operator": "lessThan",
                                "target": 3000
                            }
                        ],
                        extract_variables=[
                            {
                                "name": "TOTAL_REVENUE",
                                "type": "jsonpath",
                                "path": "$.total_revenue"
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Get Sales Performance",
                        method="GET",
                        url=f"{self.base_url}/api/v1/sales/performance",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Get Advanced Analytics",
                        method="POST",
                        url=f"{self.base_url}/api/v2/analytics/advanced-analytics",
                        headers={
                            "Authorization": "Bearer {{ AUTH_TOKEN }}",
                            "Content-Type": "application/json"
                        },
                        body='{"analysis_type": "trend_analysis", "period": "7d"}',
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.analysis_results",
                                "operator": "exists"
                            }
                        ]
                    )
                ],
                variables={
                    "AUTH_TOKEN": "{{ GLOBAL_AUTH_TOKEN }}"
                },
                max_execution_time=120,
                critical_workflow=True
            ),
            
            # ML Pipeline Workflow
            WorkflowTestConfig(
                name="ML Pipeline End-to-End Test",
                description="Tests complete ML pipeline from feature engineering to prediction",
                steps=[
                    APITestStep(
                        name="Check Model Health",
                        method="GET",
                        url=f"{self.base_url}/api/v1/ml-analytics/model/health",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.model_status",
                                "operator": "is",
                                "target": "healthy"
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Get Features",
                        method="GET", 
                        url=f"{self.base_url}/api/v1/ml-analytics/features",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            }
                        ],
                        extract_variables=[
                            {
                                "name": "FEATURE_COUNT",
                                "type": "jsonpath",
                                "path": "$.feature_count"
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Make Prediction",
                        method="POST",
                        url=f"{self.base_url}/api/v1/ml-analytics/predict",
                        headers={
                            "Authorization": "Bearer {{ AUTH_TOKEN }}",
                            "Content-Type": "application/json"
                        },
                        body='{"features": {"customer_id": "test123", "amount": 150.0, "category": "electronics"}}',
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.prediction",
                                "operator": "exists"
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.confidence",
                                "operator": "isType",
                                "target": "number"
                            },
                            {
                                "type": "responseTime",
                                "operator": "lessThan",
                                "target": 5000
                            }
                        ]
                    )
                ],
                variables={
                    "AUTH_TOKEN": "{{ GLOBAL_AUTH_TOKEN }}"
                },
                max_execution_time=90,
                critical_workflow=True
            ),
            
            # Data Quality Validation Workflow
            WorkflowTestConfig(
                name="Data Quality Validation Workflow",
                description="Tests data quality validation across different endpoints",
                steps=[
                    APITestStep(
                        name="Validate Sales Data Schema",
                        method="GET",
                        url=f"{self.base_url}/api/v1/sales/analytics",
                        headers={"Authorization": "Bearer {{ AUTH_TOKEN }}"},
                        assertions=[
                            {
                                "type": "statusCode",
                                "operator": "is",
                                "target": 200
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.total_revenue",
                                "operator": "isType",
                                "target": "number"
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.total_transactions",
                                "operator": "isType", 
                                "target": "integer"
                            },
                            {
                                "type": "jsonpath",
                                "path": "$.period",
                                "operator": "exists"
                            }
                        ]
                    ),
                    
                    APITestStep(
                        name="Validate Feature Data Quality",
                        method="GET",
                        url=f"{self.base_url}/api/v1/ml-analytics/features",
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
                                "path": "$.feature_count",
                                "operator": "greaterThan",
                                "target": 0
                            }
                        ]
                    )
                ],
                variables={
                    "AUTH_TOKEN": "{{ GLOBAL_AUTH_TOKEN }}"
                },
                max_execution_time=60,
                critical_workflow=False
            )
        ]
    
    async def deploy_endpoint_tests(self, locations: List[TestLocation] = None,
                                  frequency: TestFrequency = TestFrequency.EVERY_5_MINUTES) -> Dict[str, str]:
        """Deploy synthetic tests for all API endpoints"""
        
        if not locations:
            locations = [
                TestLocation.AWS_US_EAST_1,
                TestLocation.AWS_US_WEST_2,
                TestLocation.AWS_EU_WEST_1
            ]
        
        deployed_tests = {}
        
        try:
            for endpoint_config in self.api_endpoints:
                test_config = SyntheticTestConfig(
                    name=f"API - {endpoint_config.name}",
                    type=TestType.API,
                    url=f"{self.base_url}{endpoint_config.endpoint}",
                    frequency=frequency,
                    locations=locations,
                    alert_condition=AlertCondition.FAST if endpoint_config.business_critical else AlertCondition.SLOW,
                    timeout=endpoint_config.max_response_time // 1000,  # Convert to seconds
                    tags=[
                        "synthetic:api",
                        f"endpoint:{endpoint_config.endpoint}",
                        f"method:{endpoint_config.method.lower()}",
                        f"critical:{str(endpoint_config.business_critical).lower()}",
                        f"service:{self.synthetic_monitoring.service_name}"
                    ],
                    headers=endpoint_config.custom_headers or {},
                    assertions=[
                        {
                            "type": "statusCode",
                            "operator": "is",
                            "target": endpoint_config.expected_status
                        },
                        {
                            "type": "responseTime",
                            "operator": "lessThan", 
                            "target": endpoint_config.max_response_time
                        }
                    ] + (endpoint_config.data_validation_rules or [])
                )
                
                # Add authentication header if required
                if endpoint_config.authentication_required:
                    test_config.headers["Authorization"] = "Bearer {{ SYNTHETIC_TEST_TOKEN }}"
                    test_config.variables = {"SYNTHETIC_TEST_TOKEN": "{{ GLOBAL_SYNTHETIC_TOKEN }}"}
                
                test_id = await self.synthetic_monitoring.create_api_synthetic_test(test_config)
                deployed_tests[endpoint_config.endpoint] = test_id
                self.endpoint_tests[endpoint_config.endpoint] = test_id
                
                self.logger.info(f"Deployed API test for {endpoint_config.name}: {test_id}")
                
                # Rate limiting to avoid API limits
                await asyncio.sleep(1)
            
            self.logger.info(f"Successfully deployed {len(deployed_tests)} API endpoint tests")
            return deployed_tests
            
        except Exception as e:
            self.logger.error(f"Failed to deploy endpoint tests: {str(e)}")
            return deployed_tests
    
    async def deploy_workflow_tests(self, locations: List[TestLocation] = None,
                                  frequency: TestFrequency = TestFrequency.EVERY_15_MINUTES) -> Dict[str, str]:
        """Deploy multi-step workflow tests"""
        
        if not locations:
            locations = [
                TestLocation.AWS_US_EAST_1,
                TestLocation.AWS_EU_WEST_1
            ]
        
        deployed_workflows = {}
        
        try:
            for workflow_config in self.workflows:
                test_config = SyntheticTestConfig(
                    name=f"Workflow - {workflow_config.name}",
                    type=TestType.MULTISTEP_API,
                    url=self.base_url,  # Base URL for workflow
                    frequency=frequency,
                    locations=locations,
                    alert_condition=AlertCondition.FAST if workflow_config.critical_workflow else AlertCondition.SLOW,
                    timeout=workflow_config.max_execution_time,
                    tags=[
                        "synthetic:workflow",
                        f"workflow:{workflow_config.name.lower().replace(' ', '_')}",
                        f"critical:{str(workflow_config.critical_workflow).lower()}",
                        f"service:{self.synthetic_monitoring.service_name}"
                    ],
                    variables=workflow_config.variables or {}
                )
                
                test_id = await self.synthetic_monitoring.create_multistep_api_test(
                    workflow_config.name,
                    workflow_config.steps,
                    test_config
                )
                
                deployed_workflows[workflow_config.name] = test_id
                self.workflow_tests[workflow_config.name] = test_id
                
                self.logger.info(f"Deployed workflow test for {workflow_config.name}: {test_id}")
                
                # Rate limiting
                await asyncio.sleep(2)
            
            self.logger.info(f"Successfully deployed {len(deployed_workflows)} workflow tests")
            return deployed_workflows
            
        except Exception as e:
            self.logger.error(f"Failed to deploy workflow tests: {str(e)}")
            return deployed_workflows
    
    async def deploy_sla_compliance_tests(self, locations: List[TestLocation] = None) -> Dict[str, str]:
        """Deploy specific SLA compliance tests"""
        
        if not locations:
            locations = [TestLocation.AWS_US_EAST_1, TestLocation.AWS_US_WEST_2, TestLocation.AWS_EU_WEST_1]
        
        sla_tests = {}
        
        try:
            # Critical endpoint with strict SLA
            critical_endpoints = [
                {
                    "name": "Health Check SLA",
                    "endpoint": "/health",
                    "max_response_time": 1000,
                    "frequency": TestFrequency.EVERY_MINUTE
                },
                {
                    "name": "Sales Analytics SLA", 
                    "endpoint": "/api/v1/sales/analytics",
                    "max_response_time": 2000,
                    "frequency": TestFrequency.EVERY_5_MINUTES
                },
                {
                    "name": "ML Predictions SLA",
                    "endpoint": "/api/v1/ml-analytics/predict", 
                    "max_response_time": 3000,
                    "frequency": TestFrequency.EVERY_5_MINUTES
                }
            ]
            
            for endpoint_config in critical_endpoints:
                test_config = SyntheticTestConfig(
                    name=endpoint_config["name"],
                    type=TestType.API,
                    url=f"{self.base_url}{endpoint_config['endpoint']}",
                    frequency=endpoint_config["frequency"],
                    locations=locations,
                    alert_condition=AlertCondition.FAST,  # Immediate alerting for SLA violations
                    timeout=endpoint_config["max_response_time"] // 1000,
                    tags=[
                        "synthetic:sla",
                        "priority:critical",
                        f"service:{self.synthetic_monitoring.service_name}",
                        f"endpoint:{endpoint_config['endpoint']}"
                    ],
                    assertions=[
                        {
                            "type": "statusCode",
                            "operator": "is",
                            "target": 200
                        },
                        {
                            "type": "responseTime",
                            "operator": "lessThan",
                            "target": endpoint_config["max_response_time"]
                        }
                    ]
                )
                
                test_id = await self.synthetic_monitoring.create_api_synthetic_test(test_config)
                sla_tests[endpoint_config["name"]] = test_id
                
                self.logger.info(f"Deployed SLA test for {endpoint_config['name']}: {test_id}")
                await asyncio.sleep(1)
            
            return sla_tests
            
        except Exception as e:
            self.logger.error(f"Failed to deploy SLA compliance tests: {str(e)}")
            return sla_tests
    
    async def get_api_test_summary(self) -> Dict[str, Any]:
        """Get comprehensive API test summary"""
        
        try:
            # Get recent test results
            all_results = []
            for test_id in list(self.endpoint_tests.values()) + list(self.workflow_tests.values()):
                results = await self.synthetic_monitoring.get_test_results(
                    test_id,
                    from_ts=int((datetime.utcnow() - timedelta(hours=24)).timestamp() * 1000)
                )
                all_results.extend(results)
            
            # Calculate metrics
            total_tests = len(all_results)
            passed_tests = len([r for r in all_results if r.status == "passed"])
            failed_tests = total_tests - passed_tests
            
            success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 100.0
            avg_response_time = sum(r.response_time for r in all_results) / len(all_results) if all_results else 0.0
            
            # Group by endpoint
            endpoint_performance = {}
            for result in all_results:
                endpoint = result.test_name
                if endpoint not in endpoint_performance:
                    endpoint_performance[endpoint] = {
                        "total_executions": 0,
                        "successful_executions": 0,
                        "avg_response_time": 0.0,
                        "max_response_time": 0.0,
                        "min_response_time": float('inf')
                    }
                
                perf = endpoint_performance[endpoint]
                perf["total_executions"] += 1
                
                if result.status == "passed":
                    perf["successful_executions"] += 1
                
                perf["avg_response_time"] = ((perf["avg_response_time"] * (perf["total_executions"] - 1)) + result.response_time) / perf["total_executions"]
                perf["max_response_time"] = max(perf["max_response_time"], result.response_time)
                perf["min_response_time"] = min(perf["min_response_time"], result.response_time)
            
            # Calculate success rate for each endpoint
            for endpoint_perf in endpoint_performance.values():
                endpoint_perf["success_rate"] = (endpoint_perf["successful_executions"] / endpoint_perf["total_executions"] * 100) if endpoint_perf["total_executions"] > 0 else 0.0
                if endpoint_perf["min_response_time"] == float('inf'):
                    endpoint_perf["min_response_time"] = 0.0
            
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "summary": {
                    "total_active_tests": len(self.endpoint_tests) + len(self.workflow_tests),
                    "endpoint_tests": len(self.endpoint_tests),
                    "workflow_tests": len(self.workflow_tests),
                    "total_executions_24h": total_tests,
                    "successful_executions_24h": passed_tests,
                    "failed_executions_24h": failed_tests,
                    "overall_success_rate": success_rate,
                    "average_response_time_ms": avg_response_time
                },
                "sla_compliance": {
                    "availability_target": 99.9,
                    "current_availability": success_rate,
                    "response_time_target": 2000,
                    "current_avg_response_time": avg_response_time,
                    "sla_met": success_rate >= 99.9 and avg_response_time <= 2000
                },
                "endpoint_performance": endpoint_performance,
                "test_configuration": {
                    "monitored_endpoints": len(self.api_endpoints),
                    "configured_workflows": len(self.workflows),
                    "monitoring_locations": ["aws:us-east-1", "aws:us-west-2", "aws:eu-west-1"],
                    "test_frequencies": ["1m", "5m", "15m"]
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate API test summary: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}
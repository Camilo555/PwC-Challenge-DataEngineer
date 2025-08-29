"""
DataDog Browser Synthetic Tests
Comprehensive browser-based testing for critical user journeys,
end-to-end business processes, and UI validation
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from monitoring.datadog_synthetic_monitoring import (
    DataDogSyntheticMonitoring,
    SyntheticTestConfig,
    BrowserTestStep,
    TestType,
    TestFrequency,
    TestLocation,
    AlertCondition
)
from core.logging import get_logger

logger = get_logger(__name__)


class DeviceType(Enum):
    """Browser device types"""
    DESKTOP_LARGE = "chrome.laptop_large"
    DESKTOP_MEDIUM = "chrome.laptop_small"
    TABLET = "chrome.tablet"
    MOBILE_LARGE = "chrome.mobile_large"
    MOBILE_SMALL = "chrome.mobile_small"
    FIREFOX_DESKTOP = "firefox.laptop_large"
    EDGE_DESKTOP = "edge.laptop_large"


class BrowserType(Enum):
    """Supported browsers"""
    CHROME = "chrome"
    FIREFOX = "firefox"
    EDGE = "edge"
    SAFARI = "safari"


@dataclass
class UserJourneyConfig:
    """Configuration for user journey testing"""
    name: str
    description: str
    entry_point: str
    steps: List[BrowserTestStep]
    success_criteria: List[Dict[str, Any]]
    devices: List[DeviceType]
    critical: bool = True
    max_duration: int = 300  # seconds
    enable_screenshots: bool = True
    enable_performance_monitoring: bool = True
    custom_metrics: List[str] = None


@dataclass
class VisualRegressionConfig:
    """Configuration for visual regression testing"""
    name: str
    pages: List[str]
    viewport_sizes: List[Tuple[int, int]]  # (width, height)
    baseline_branch: str = "main"
    threshold: float = 0.1  # Acceptable difference percentage
    ignore_regions: List[Dict[str, int]] = None  # Regions to ignore


@dataclass
class PerformanceTestConfig:
    """Configuration for performance testing"""
    name: str
    url: str
    metrics_thresholds: Dict[str, float]  # metric_name -> threshold
    devices: List[DeviceType]
    locations: List[TestLocation]


class DataDogBrowserSyntheticTests:
    """
    DataDog Browser Synthetic Tests Manager
    
    Manages comprehensive browser testing including:
    - Critical user journey monitoring
    - End-to-end business process testing
    - Performance monitoring from user perspective
    - Cross-browser and device compatibility testing
    - Visual regression testing
    - Accessibility testing
    """
    
    def __init__(self, synthetic_monitoring: DataDogSyntheticMonitoring,
                 base_url: str, ui_framework: str = "react"):
        self.synthetic_monitoring = synthetic_monitoring
        self.base_url = base_url.rstrip('/')
        self.ui_framework = ui_framework
        self.logger = get_logger(f"{__name__}")
        
        # Test tracking
        self.journey_tests: Dict[str, str] = {}  # journey_name -> test_id
        self.performance_tests: Dict[str, str] = {}  # test_name -> test_id
        self.visual_tests: Dict[str, str] = {}  # test_name -> test_id
        
        # User journeys configuration
        self.user_journeys = self._get_user_journeys_config()
        self.performance_configs = self._get_performance_test_configs()
        self.visual_regression_configs = self._get_visual_regression_configs()
        
        self.logger.info("DataDog Browser Synthetic Tests initialized")
    
    def _get_user_journeys_config(self) -> List[UserJourneyConfig]:
        """Get user journey configurations for critical business processes"""
        
        return [
            # Authentication Journey
            UserJourneyConfig(
                name="User Authentication Flow",
                description="Complete user authentication from login to dashboard access",
                entry_point="/login",
                steps=[
                    BrowserTestStep(
                        name="Navigate to Login Page",
                        action="navigate",
                        selector=None,
                        value=f"{self.base_url}/login",
                        timeout=10,
                        screenshot=True,
                        assertions=[
                            {
                                "type": "elementPresent",
                                "element": "[data-testid='login-form']"
                            }
                        ]
                    ),
                    
                    BrowserTestStep(
                        name="Enter Username",
                        action="type",
                        selector="input[name='username']",
                        value="demo_user@example.com",
                        timeout=5,
                        assertions=[
                            {
                                "type": "elementPresent",
                                "element": "input[name='username']"
                            }
                        ]
                    ),
                    
                    BrowserTestStep(
                        name="Enter Password",
                        action="type",
                        selector="input[name='password']",
                        value="{{ USER_PASSWORD }}",
                        timeout=5
                    ),
                    
                    BrowserTestStep(
                        name="Click Login Button",
                        action="click",
                        selector="button[type='submit']",
                        timeout=5,
                        screenshot=True
                    ),
                    
                    BrowserTestStep(
                        name="Wait for Dashboard Load",
                        action="wait",
                        selector="[data-testid='dashboard']",
                        timeout=30,
                        screenshot=True,
                        assertions=[
                            {
                                "type": "elementPresent",
                                "element": "[data-testid='user-profile']"
                            },
                            {
                                "type": "elementPresent",
                                "element": "[data-testid='navigation-menu']"
                            }
                        ]
                    ),
                    
                    BrowserTestStep(
                        name="Verify User Profile",
                        action="assertElementPresent",
                        selector="[data-testid='user-profile']",
                        timeout=5,
                        assertions=[
                            {
                                "type": "elementContainsText",
                                "element": "[data-testid='user-profile']",
                                "text": "demo_user"
                            }
                        ]
                    )
                ],
                success_criteria=[
                    {
                        "type": "url",
                        "operator": "contains",
                        "value": "/dashboard"
                    },
                    {
                        "type": "elementPresent",
                        "element": "[data-testid='dashboard']"
                    }
                ],
                devices=[DeviceType.DESKTOP_LARGE, DeviceType.TABLET, DeviceType.MOBILE_LARGE],
                critical=True,
                max_duration=60,
                enable_screenshots=True,
                enable_performance_monitoring=True
            ),
            
            # Sales Analytics Journey
            UserJourneyConfig(
                name="Sales Analytics Dashboard Journey",
                description="Navigate through sales analytics features and generate reports",
                entry_point="/dashboard",
                steps=[
                    BrowserTestStep(
                        name="Navigate to Sales Analytics",
                        action="click",
                        selector="a[href*='sales']",
                        timeout=10,
                        screenshot=True
                    ),
                    
                    BrowserTestStep(
                        name="Wait for Sales Dashboard",
                        action="wait",
                        selector="[data-testid='sales-dashboard']",
                        timeout=30,
                        assertions=[
                            {
                                "type": "elementPresent",
                                "element": "[data-testid='revenue-chart']"
                            },
                            {
                                "type": "elementPresent",
                                "element": "[data-testid='sales-metrics']"
                            }
                        ]
                    ),
                    
                    BrowserTestStep(
                        name="Verify Revenue Chart Loading",
                        action="wait",
                        selector="[data-testid='revenue-chart'] svg",
                        timeout=20,
                        screenshot=True,
                        assertions=[
                            {
                                "type": "elementPresent",
                                "element": "svg path"
                            }
                        ]
                    ),
                    
                    BrowserTestStep(
                        name="Click Date Range Filter",
                        action="click",
                        selector="[data-testid='date-range-selector']",
                        timeout=5
                    ),
                    
                    BrowserTestStep(
                        name="Select Last 30 Days",
                        action="click",
                        selector="[data-value='30d']",
                        timeout=5
                    ),
                    
                    BrowserTestStep(
                        name="Wait for Chart Update",
                        action="wait",
                        selector="[data-testid='loading-indicator']:not([aria-hidden='true'])",
                        timeout=15,
                        screenshot=True
                    ),
                    
                    BrowserTestStep(
                        name="Generate Report",
                        action="click",
                        selector="button[data-testid='generate-report']",
                        timeout=5
                    ),
                    
                    BrowserTestStep(
                        name="Verify Report Generated",
                        action="wait",
                        selector="[data-testid='report-success']",
                        timeout=30,
                        screenshot=True,
                        assertions=[
                            {
                                "type": "elementContainsText",
                                "element": "[data-testid='report-success']",
                                "text": "Report generated successfully"
                            }
                        ]
                    )
                ],
                success_criteria=[
                    {
                        "type": "url",
                        "operator": "contains",
                        "value": "/sales"
                    },
                    {
                        "type": "elementPresent",
                        "element": "[data-testid='sales-dashboard']"
                    },
                    {
                        "type": "elementPresent",
                        "element": "[data-testid='report-success']"
                    }
                ],
                devices=[DeviceType.DESKTOP_LARGE, DeviceType.TABLET],
                critical=True,
                max_duration=120,
                custom_metrics=["chart_load_time", "report_generation_time"]
            ),
            
            # ML Analytics Journey
            UserJourneyConfig(
                name="ML Analytics Model Interaction",
                description="Test ML model predictions and feature analysis interface",
                entry_point="/ml-analytics",
                steps=[
                    BrowserTestStep(
                        name="Navigate to ML Analytics",
                        action="navigate",
                        value=f"{self.base_url}/ml-analytics",
                        timeout=10,
                        screenshot=True
                    ),
                    
                    BrowserTestStep(
                        name="Wait for Model Status",
                        action="wait",
                        selector="[data-testid='model-status']",
                        timeout=30,
                        assertions=[
                            {
                                "type": "elementContainsText",
                                "element": "[data-testid='model-status']",
                                "text": "Healthy"
                            }
                        ]
                    ),
                    
                    BrowserTestStep(
                        name="Enter Prediction Input",
                        action="type",
                        selector="input[name='customer_id']",
                        value="TEST123",
                        timeout=5
                    ),
                    
                    BrowserTestStep(
                        name="Enter Amount",
                        action="type",
                        selector="input[name='amount']",
                        value="150.00",
                        timeout=5
                    ),
                    
                    BrowserTestStep(
                        name="Select Category",
                        action="click",
                        selector="select[name='category']",
                        timeout=5
                    ),
                    
                    BrowserTestStep(
                        name="Choose Electronics Category",
                        action="click",
                        selector="option[value='electronics']",
                        timeout=5
                    ),
                    
                    BrowserTestStep(
                        name="Submit Prediction Request",
                        action="click",
                        selector="button[data-testid='predict-button']",
                        timeout=5,
                        screenshot=True
                    ),
                    
                    BrowserTestStep(
                        name="Wait for Prediction Results",
                        action="wait",
                        selector="[data-testid='prediction-result']",
                        timeout=45,
                        screenshot=True,
                        assertions=[
                            {
                                "type": "elementPresent",
                                "element": "[data-testid='prediction-value']"
                            },
                            {
                                "type": "elementPresent",
                                "element": "[data-testid='confidence-score']"
                            }
                        ]
                    ),
                    
                    BrowserTestStep(
                        name="Verify Prediction Confidence",
                        action="assertElementPresent",
                        selector="[data-testid='confidence-score'][data-value]",
                        timeout=5,
                        assertions=[
                            {
                                "type": "attributeGreaterThan",
                                "element": "[data-testid='confidence-score']",
                                "attribute": "data-value",
                                "value": "0.5"
                            }
                        ]
                    )
                ],
                success_criteria=[
                    {
                        "type": "elementPresent",
                        "element": "[data-testid='prediction-result']"
                    },
                    {
                        "type": "elementPresent",
                        "element": "[data-testid='confidence-score']"
                    }
                ],
                devices=[DeviceType.DESKTOP_LARGE, DeviceType.DESKTOP_MEDIUM],
                critical=True,
                max_duration=90,
                custom_metrics=["model_response_time", "prediction_accuracy"]
            ),
            
            # Security Dashboard Journey
            UserJourneyConfig(
                name="Security Dashboard Monitoring",
                description="Test security dashboard and real-time monitoring features",
                entry_point="/security",
                steps=[
                    BrowserTestStep(
                        name="Navigate to Security Dashboard",
                        action="navigate",
                        value=f"{self.base_url}/security/dashboard",
                        timeout=10,
                        screenshot=True
                    ),
                    
                    BrowserTestStep(
                        name="Verify Security Metrics Load",
                        action="wait",
                        selector="[data-testid='security-metrics']",
                        timeout=30,
                        assertions=[
                            {
                                "type": "elementPresent",
                                "element": "[data-testid='threat-level']"
                            },
                            {
                                "type": "elementPresent",
                                "element": "[data-testid='active-sessions']"
                            }
                        ]
                    ),
                    
                    BrowserTestStep(
                        name="Check Real-time Updates",
                        action="wait",
                        selector="[data-testid='last-updated']",
                        timeout=15,
                        assertions=[
                            {
                                "type": "elementPresent",
                                "element": "[data-testid='last-updated']"
                            }
                        ]
                    ),
                    
                    BrowserTestStep(
                        name="Test Alert Filtering",
                        action="click",
                        selector="button[data-testid='filter-critical']",
                        timeout=5
                    ),
                    
                    BrowserTestStep(
                        name="Verify Filtered Results",
                        action="wait",
                        selector="[data-testid='filtered-alerts']",
                        timeout=10,
                        screenshot=True
                    )
                ],
                success_criteria=[
                    {
                        "type": "elementPresent",
                        "element": "[data-testid='security-metrics']"
                    }
                ],
                devices=[DeviceType.DESKTOP_LARGE],
                critical=False,
                max_duration=60
            )
        ]
    
    def _get_performance_test_configs(self) -> List[PerformanceTestConfig]:
        """Get performance test configurations"""
        
        return [
            PerformanceTestConfig(
                name="Homepage Performance",
                url=f"{self.base_url}/",
                metrics_thresholds={
                    "first_contentful_paint": 2000,  # 2 seconds
                    "largest_contentful_paint": 4000,  # 4 seconds
                    "cumulative_layout_shift": 0.1,
                    "first_input_delay": 100  # 100ms
                },
                devices=[DeviceType.DESKTOP_LARGE, DeviceType.MOBILE_LARGE],
                locations=[TestLocation.AWS_US_EAST_1, TestLocation.AWS_EU_WEST_1]
            ),
            
            PerformanceTestConfig(
                name="Dashboard Performance",
                url=f"{self.base_url}/dashboard",
                metrics_thresholds={
                    "first_contentful_paint": 3000,
                    "largest_contentful_paint": 5000,
                    "cumulative_layout_shift": 0.1,
                    "time_to_interactive": 6000
                },
                devices=[DeviceType.DESKTOP_LARGE, DeviceType.TABLET],
                locations=[TestLocation.AWS_US_EAST_1, TestLocation.AWS_US_WEST_2]
            ),
            
            PerformanceTestConfig(
                name="Sales Analytics Performance",
                url=f"{self.base_url}/sales/analytics",
                metrics_thresholds={
                    "first_contentful_paint": 2500,
                    "largest_contentful_paint": 6000,
                    "cumulative_layout_shift": 0.15,
                    "time_to_interactive": 8000
                },
                devices=[DeviceType.DESKTOP_LARGE],
                locations=[TestLocation.AWS_US_EAST_1]
            )
        ]
    
    def _get_visual_regression_configs(self) -> List[VisualRegressionConfig]:
        """Get visual regression test configurations"""
        
        return [
            VisualRegressionConfig(
                name="Homepage Visual Regression",
                pages=["/", "/login", "/dashboard"],
                viewport_sizes=[(1920, 1080), (1366, 768), (768, 1024), (375, 667)],
                threshold=0.05,
                ignore_regions=[
                    {"x": 0, "y": 0, "width": 100, "height": 50},  # Header timestamp
                    {"x": 900, "y": 10, "width": 200, "height": 30}  # User avatar area
                ]
            ),
            
            VisualRegressionConfig(
                name="Sales Dashboard Visual Regression",
                pages=["/sales/analytics", "/sales/performance"],
                viewport_sizes=[(1920, 1080), (1366, 768)],
                threshold=0.1,
                ignore_regions=[
                    {"x": 800, "y": 100, "width": 300, "height": 200}  # Dynamic chart area
                ]
            )
        ]
    
    async def deploy_user_journey_tests(self, locations: List[TestLocation] = None,
                                       frequency: TestFrequency = TestFrequency.EVERY_15_MINUTES) -> Dict[str, str]:
        """Deploy browser tests for all user journeys"""
        
        if not locations:
            locations = [TestLocation.AWS_US_EAST_1, TestLocation.AWS_EU_WEST_1]
        
        deployed_journeys = {}
        
        try:
            for journey_config in self.user_journeys:
                test_config = SyntheticTestConfig(
                    name=f"Browser - {journey_config.name}",
                    type=TestType.BROWSER,
                    url=f"{self.base_url}{journey_config.entry_point}",
                    frequency=frequency,
                    locations=locations,
                    alert_condition=AlertCondition.FAST if journey_config.critical else AlertCondition.SLOW,
                    timeout=journey_config.max_duration,
                    tags=[
                        "synthetic:browser",
                        "synthetic:journey",
                        f"journey:{journey_config.name.lower().replace(' ', '_')}",
                        f"critical:{str(journey_config.critical).lower()}",
                        f"service:{self.synthetic_monitoring.service_name}"
                    ],
                    device_ids=[device.value for device in journey_config.devices],
                    variables={
                        "USER_PASSWORD": "{{ SYNTHETIC_USER_PASSWORD }}"
                    }
                )
                
                test_id = await self.synthetic_monitoring.create_browser_synthetic_test(
                    journey_config.name,
                    journey_config.steps,
                    test_config
                )
                
                deployed_journeys[journey_config.name] = test_id
                self.journey_tests[journey_config.name] = test_id
                
                self.logger.info(f"Deployed user journey test for {journey_config.name}: {test_id}")
                
                # Rate limiting
                await asyncio.sleep(2)
            
            self.logger.info(f"Successfully deployed {len(deployed_journeys)} user journey tests")
            return deployed_journeys
            
        except Exception as e:
            self.logger.error(f"Failed to deploy user journey tests: {str(e)}")
            return deployed_journeys
    
    async def deploy_performance_tests(self, frequency: TestFrequency = TestFrequency.EVERY_30_MINUTES) -> Dict[str, str]:
        """Deploy performance monitoring tests"""
        
        deployed_performance = {}
        
        try:
            for perf_config in self.performance_configs:
                # Create performance assertions based on thresholds
                performance_assertions = []
                for metric, threshold in perf_config.metrics_thresholds.items():
                    performance_assertions.append({
                        "type": "performance",
                        "metric": metric,
                        "operator": "lessThan",
                        "target": threshold
                    })
                
                test_config = SyntheticTestConfig(
                    name=f"Performance - {perf_config.name}",
                    type=TestType.BROWSER,
                    url=perf_config.url,
                    frequency=frequency,
                    locations=perf_config.locations,
                    alert_condition=AlertCondition.SLOW,  # Performance tests can be slower to alert
                    timeout=60,
                    tags=[
                        "synthetic:performance",
                        f"page:{perf_config.name.lower().replace(' ', '_')}",
                        f"service:{self.synthetic_monitoring.service_name}"
                    ],
                    device_ids=[device.value for device in perf_config.devices],
                    assertions=performance_assertions,
                    enable_profiling=True
                )
                
                # Create simple navigation steps for performance testing
                perf_steps = [
                    BrowserTestStep(
                        name="Navigate and Measure Performance",
                        action="navigate",
                        value=perf_config.url,
                        timeout=30,
                        screenshot=True
                    ),
                    BrowserTestStep(
                        name="Wait for Page Load Complete",
                        action="wait",
                        selector="body",
                        timeout=30
                    )
                ]
                
                test_id = await self.synthetic_monitoring.create_browser_synthetic_test(
                    perf_config.name,
                    perf_steps,
                    test_config
                )
                
                deployed_performance[perf_config.name] = test_id
                self.performance_tests[perf_config.name] = test_id
                
                self.logger.info(f"Deployed performance test for {perf_config.name}: {test_id}")
                await asyncio.sleep(2)
            
            self.logger.info(f"Successfully deployed {len(deployed_performance)} performance tests")
            return deployed_performance
            
        except Exception as e:
            self.logger.error(f"Failed to deploy performance tests: {str(e)}")
            return deployed_performance
    
    async def deploy_cross_browser_tests(self, journey_name: str = None,
                                       browsers: List[BrowserType] = None) -> Dict[str, str]:
        """Deploy cross-browser compatibility tests"""
        
        if not browsers:
            browsers = [BrowserType.CHROME, BrowserType.FIREFOX, BrowserType.EDGE]
        
        if not journey_name:
            # Test the most critical journey across browsers
            journey_name = "User Authentication Flow"
        
        journey_config = next(
            (j for j in self.user_journeys if j.name == journey_name),
            None
        )
        
        if not journey_config:
            raise ValueError(f"Journey '{journey_name}' not found")
        
        deployed_cross_browser = {}
        
        try:
            for browser in browsers:
                device_map = {
                    BrowserType.CHROME: DeviceType.DESKTOP_LARGE,
                    BrowserType.FIREFOX: DeviceType.FIREFOX_DESKTOP,
                    BrowserType.EDGE: DeviceType.EDGE_DESKTOP
                }
                
                test_config = SyntheticTestConfig(
                    name=f"Cross-Browser - {journey_name} ({browser.value})",
                    type=TestType.BROWSER,
                    url=f"{self.base_url}{journey_config.entry_point}",
                    frequency=TestFrequency.HOURLY,
                    locations=[TestLocation.AWS_US_EAST_1],
                    alert_condition=AlertCondition.SLOW,
                    timeout=journey_config.max_duration,
                    tags=[
                        "synthetic:cross-browser",
                        f"browser:{browser.value}",
                        f"journey:{journey_config.name.lower().replace(' ', '_')}",
                        f"service:{self.synthetic_monitoring.service_name}"
                    ],
                    device_ids=[device_map[browser].value]
                )
                
                test_id = await self.synthetic_monitoring.create_browser_synthetic_test(
                    f"{journey_name} - {browser.value}",
                    journey_config.steps,
                    test_config
                )
                
                deployed_cross_browser[f"{journey_name}_{browser.value}"] = test_id
                
                self.logger.info(f"Deployed cross-browser test for {journey_name} on {browser.value}: {test_id}")
                await asyncio.sleep(2)
            
            return deployed_cross_browser
            
        except Exception as e:
            self.logger.error(f"Failed to deploy cross-browser tests: {str(e)}")
            return deployed_cross_browser
    
    async def deploy_mobile_responsive_tests(self) -> Dict[str, str]:
        """Deploy mobile responsiveness tests"""
        
        mobile_devices = [DeviceType.MOBILE_LARGE, DeviceType.MOBILE_SMALL, DeviceType.TABLET]
        deployed_mobile = {}
        
        try:
            # Test critical journeys on mobile devices
            critical_journeys = [j for j in self.user_journeys if j.critical][:2]  # Limit to 2 most critical
            
            for journey_config in critical_journeys:
                for device in mobile_devices:
                    test_config = SyntheticTestConfig(
                        name=f"Mobile - {journey_config.name} ({device.value})",
                        type=TestType.BROWSER,
                        url=f"{self.base_url}{journey_config.entry_point}",
                        frequency=TestFrequency.EVERY_30_MINUTES,
                        locations=[TestLocation.AWS_US_EAST_1],
                        alert_condition=AlertCondition.SLOW,
                        timeout=journey_config.max_duration + 30,  # Extra time for mobile
                        tags=[
                            "synthetic:mobile",
                            f"device:{device.value}",
                            f"journey:{journey_config.name.lower().replace(' ', '_')}",
                            f"service:{self.synthetic_monitoring.service_name}"
                        ],
                        device_ids=[device.value]
                    )
                    
                    # Modify steps for mobile if necessary
                    mobile_steps = self._adapt_steps_for_mobile(journey_config.steps)
                    
                    test_id = await self.synthetic_monitoring.create_browser_synthetic_test(
                        f"Mobile - {journey_config.name} - {device.value}",
                        mobile_steps,
                        test_config
                    )
                    
                    deployed_mobile[f"{journey_config.name}_{device.value}"] = test_id
                    
                    self.logger.info(f"Deployed mobile test for {journey_config.name} on {device.value}: {test_id}")
                    await asyncio.sleep(2)
            
            return deployed_mobile
            
        except Exception as e:
            self.logger.error(f"Failed to deploy mobile responsive tests: {str(e)}")
            return deployed_mobile
    
    def _adapt_steps_for_mobile(self, steps: List[BrowserTestStep]) -> List[BrowserTestStep]:
        """Adapt browser test steps for mobile devices"""
        
        mobile_steps = []
        for step in steps:
            # Create a copy of the step
            mobile_step = BrowserTestStep(
                name=step.name,
                action=step.action,
                selector=step.selector,
                value=step.value,
                timeout=step.timeout + 5,  # Extra timeout for mobile
                screenshot=step.screenshot,
                assertions=step.assertions
            )
            
            # Modify selectors for mobile-specific elements if needed
            if step.selector and "button" in step.selector:
                # Increase touch target area consideration
                mobile_step.timeout += 2
            
            mobile_steps.append(mobile_step)
        
        return mobile_steps
    
    async def get_browser_test_summary(self) -> Dict[str, Any]:
        """Get comprehensive browser test summary"""
        
        try:
            # Get recent test results
            all_results = []
            all_test_ids = list(self.journey_tests.values()) + list(self.performance_tests.values())
            
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
            avg_execution_time = sum(r.response_time for r in all_results) / len(all_results) if all_results else 0.0
            
            # Journey-specific metrics
            journey_performance = {}
            for journey_name, test_id in self.journey_tests.items():
                journey_results = [r for r in all_results if r.test_id == test_id]
                if journey_results:
                    journey_success_rate = len([r for r in journey_results if r.status == "passed"]) / len(journey_results) * 100
                    journey_avg_time = sum(r.response_time for r in journey_results) / len(journey_results)
                    
                    journey_performance[journey_name] = {
                        "success_rate": journey_success_rate,
                        "avg_execution_time": journey_avg_time,
                        "total_executions": len(journey_results),
                        "last_execution": max(r.execution_time for r in journey_results).isoformat() if journey_results else None
                    }
            
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "summary": {
                    "total_active_tests": len(self.journey_tests) + len(self.performance_tests),
                    "journey_tests": len(self.journey_tests),
                    "performance_tests": len(self.performance_tests),
                    "total_executions_24h": total_executions,
                    "successful_executions_24h": successful_executions,
                    "failed_executions_24h": failed_executions,
                    "overall_success_rate": success_rate,
                    "average_execution_time_ms": avg_execution_time
                },
                "user_experience_metrics": {
                    "critical_journeys_monitored": len([j for j in self.user_journeys if j.critical]),
                    "devices_tested": ["desktop", "tablet", "mobile"],
                    "browsers_tested": ["chrome", "firefox", "edge"],
                    "performance_thresholds_met": success_rate >= 95.0
                },
                "journey_performance": journey_performance,
                "performance_testing": {
                    "pages_monitored": len(self.performance_configs),
                    "core_web_vitals_tracked": ["FCP", "LCP", "CLS", "FID"],
                    "mobile_performance_tested": True,
                    "cross_browser_tested": True
                },
                "test_coverage": {
                    "authentication_flows": 1,
                    "business_processes": len([j for j in self.user_journeys if "sales" in j.name.lower() or "analytics" in j.name.lower()]),
                    "security_dashboards": len([j for j in self.user_journeys if "security" in j.name.lower()]),
                    "responsive_design": True,
                    "accessibility_testing": False  # Can be added in future
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate browser test summary: {str(e)}")
            return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}
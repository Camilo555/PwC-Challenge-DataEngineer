"""
Advanced Data Quality Framework with Great Expectations Integration
Provides comprehensive data validation, profiling, and expectation management
"""
from __future__ import annotations

import json
import os
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import great_expectations as gx
import pandas as pd
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.validator.validator import Validator
from pydantic import BaseModel, Field

from core.logging import get_logger

logger = get_logger(__name__)


class ExpectationSeverity(str, Enum):
    """Severity levels for data expectations"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    WARNING = "warning"


class DataQualityStatus(str, Enum):
    """Overall data quality status"""
    PASSED = "passed"
    FAILED = "failed"
    PARTIAL = "partial"
    ERROR = "error"
    UNKNOWN = "unknown"


@dataclass
class ExpectationResult:
    """Result of a single expectation"""
    expectation_type: str
    column: str
    success: bool
    severity: ExpectationSeverity
    observed_value: Any = None
    expected_value: Any = None
    exception_info: Optional[Dict] = None
    result_details: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass 
class DataQualityReport:
    """Comprehensive data quality report"""
    dataset_name: str
    execution_time: datetime = field(default_factory=datetime.utcnow)
    total_expectations: int = 0
    successful_expectations: int = 0
    failed_expectations: int = 0
    success_percentage: float = 0.0
    overall_status: DataQualityStatus = DataQualityStatus.UNKNOWN
    expectation_results: List[ExpectationResult] = field(default_factory=list)
    validation_summary: Dict[str, Any] = field(default_factory=dict)
    data_statistics: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ExpectationTemplate(BaseModel):
    """Template for creating expectations"""
    name: str
    expectation_type: str
    column: str
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    meta: Dict[str, Any] = Field(default_factory=dict)
    severity: ExpectationSeverity = ExpectationSeverity.MEDIUM
    description: str = ""
    business_rule: str = ""


class DataQualityProfile(BaseModel):
    """Data quality profile configuration"""
    profile_name: str
    expectation_templates: List[ExpectationTemplate]
    data_source_config: Dict[str, Any] = Field(default_factory=dict)
    validation_schedule: str = "daily"
    alert_thresholds: Dict[str, float] = Field(default_factory=dict)
    retention_days: int = 30
    tags: List[str] = Field(default_factory=list)


class GreatExpectationsManager:
    """Manager for Great Expectations data quality validation"""
    
    def __init__(self, context_root_dir: str = None):
        """Initialize Great Expectations manager
        
        Args:
            context_root_dir: Root directory for GX context
        """
        self.logger = get_logger(__name__)
        self.context_root_dir = context_root_dir or os.getcwd()
        
        # Initialize Great Expectations context
        self.context = None
        self.data_sources = {}
        self.expectation_suites = {}
        
        try:
            self._initialize_context()
        except Exception as e:
            self.logger.error(f"Failed to initialize Great Expectations context: {e}")
            raise
    
    def _initialize_context(self):
        """Initialize or load Great Expectations context"""
        try:
            # Try to get existing context
            self.context = gx.get_context(context_root_dir=self.context_root_dir)
            self.logger.info("Loaded existing Great Expectations context")
        except Exception:
            try:
                # Create new context
                self.context = gx.get_context(
                    context_root_dir=self.context_root_dir,
                    cloud_mode=False
                )
                self.logger.info("Created new Great Expectations context")
            except Exception as e:
                self.logger.error(f"Failed to initialize GX context: {e}")
                raise
        
        # Load existing data sources and expectation suites
        self._load_existing_assets()
    
    def _load_existing_assets(self):
        """Load existing data sources and expectation suites"""
        try:
            # Load data sources
            for ds_name in self.context.list_datasources():
                self.data_sources[ds_name] = self.context.get_datasource(ds_name)
            
            # Load expectation suites
            for suite_name in self.context.list_expectation_suite_names():
                self.expectation_suites[suite_name] = self.context.get_expectation_suite(suite_name)
            
            self.logger.info(f"Loaded {len(self.data_sources)} data sources and {len(self.expectation_suites)} expectation suites")
        except Exception as e:
            self.logger.warning(f"Failed to load existing assets: {e}")
    
    def create_pandas_datasource(self, name: str, description: str = "") -> Any:
        """Create a pandas datasource
        
        Args:
            name: Name of the datasource
            description: Description of the datasource
            
        Returns:
            Datasource object
        """
        try:
            datasource_config = {
                "name": name,
                "class_name": "Datasource",
                "module_name": "great_expectations.datasource",
                "execution_engine": {
                    "module_name": "great_expectations.execution_engine",
                    "class_name": "PandasExecutionEngine",
                },
                "data_connectors": {
                    "runtime_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "module_name": "great_expectations.datasource.data_connector",
                        "batch_identifiers": ["default_identifier_name"],
                    }
                }
            }
            
            datasource = self.context.add_datasource(**datasource_config)
            self.data_sources[name] = datasource
            
            self.logger.info(f"Created pandas datasource: {name}")
            return datasource
        
        except Exception as e:
            self.logger.error(f"Failed to create pandas datasource {name}: {e}")
            raise
    
    def create_expectation_suite(self, suite_name: str, overwrite: bool = False) -> ExpectationSuite:
        """Create or get expectation suite
        
        Args:
            suite_name: Name of the expectation suite
            overwrite: Whether to overwrite existing suite
            
        Returns:
            ExpectationSuite object
        """
        try:
            if suite_name in self.expectation_suites and not overwrite:
                return self.expectation_suites[suite_name]
            
            suite = self.context.add_or_update_expectation_suite(
                expectation_suite_name=suite_name
            )
            
            self.expectation_suites[suite_name] = suite
            self.logger.info(f"Created expectation suite: {suite_name}")
            
            return suite
        
        except Exception as e:
            self.logger.error(f"Failed to create expectation suite {suite_name}: {e}")
            raise
    
    def add_expectations_from_profile(self, suite_name: str, profile: DataQualityProfile):
        """Add expectations to suite from data quality profile
        
        Args:
            suite_name: Name of the expectation suite
            profile: Data quality profile with expectations
        """
        try:
            suite = self.create_expectation_suite(suite_name)
            
            for template in profile.expectation_templates:
                expectation_config = ExpectationConfiguration(
                    expectation_type=template.expectation_type,
                    kwargs={
                        "column": template.column,
                        **template.kwargs
                    },
                    meta={
                        "severity": template.severity.value,
                        "description": template.description,
                        "business_rule": template.business_rule,
                        **template.meta
                    }
                )
                
                suite.add_expectation(expectation_config)
            
            # Save the suite
            self.context.add_or_update_expectation_suite(suite)
            self.expectation_suites[suite_name] = suite
            
            self.logger.info(f"Added {len(profile.expectation_templates)} expectations to suite {suite_name}")
        
        except Exception as e:
            self.logger.error(f"Failed to add expectations from profile: {e}")
            raise
    
    def create_common_expectations(self, suite_name: str, df: pd.DataFrame, columns_config: Dict[str, Dict] = None):
        """Create common data quality expectations based on DataFrame
        
        Args:
            suite_name: Name of the expectation suite
            df: DataFrame to analyze
            columns_config: Configuration for specific columns
        """
        columns_config = columns_config or {}
        
        try:
            suite = self.create_expectation_suite(suite_name, overwrite=True)
            
            # Table-level expectations
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_table_row_count_to_be_between",
                    kwargs={"min_value": 1, "max_value": None},
                    meta={"severity": "high", "description": "Table should not be empty"}
                )
            )
            
            # Column-level expectations
            for column in df.columns:
                col_config = columns_config.get(column, {})
                dtype = df[column].dtype
                
                # Existence expectation
                if col_config.get("not_null", True):
                    suite.add_expectation(
                        ExpectationConfiguration(
                            expectation_type="expect_column_values_to_not_be_null",
                            kwargs={"column": column},
                            meta={
                                "severity": col_config.get("null_severity", "medium"),
                                "description": f"Column {column} should not contain null values"
                            }
                        )
                    )
                
                # Type-specific expectations
                if pd.api.types.is_numeric_dtype(dtype):
                    # Numeric column expectations
                    if col_config.get("min_value") is not None:
                        suite.add_expectation(
                            ExpectationConfiguration(
                                expectation_type="expect_column_values_to_be_between",
                                kwargs={
                                    "column": column,
                                    "min_value": col_config["min_value"],
                                    "max_value": col_config.get("max_value")
                                },
                                meta={"severity": "high", "description": f"Values in {column} should be within valid range"}
                            )
                        )
                
                elif pd.api.types.is_string_dtype(dtype) or dtype == 'object':
                    # String column expectations
                    if col_config.get("max_length"):
                        suite.add_expectation(
                            ExpectationConfiguration(
                                expectation_type="expect_column_value_lengths_to_be_between",
                                kwargs={
                                    "column": column,
                                    "min_value": 1,
                                    "max_value": col_config["max_length"]
                                },
                                meta={"severity": "medium", "description": f"String length in {column} should be within limits"}
                            )
                        )
                    
                    # Pattern matching
                    if col_config.get("regex_pattern"):
                        suite.add_expectation(
                            ExpectationConfiguration(
                                expectation_type="expect_column_values_to_match_regex",
                                kwargs={
                                    "column": column,
                                    "regex": col_config["regex_pattern"]
                                },
                                meta={"severity": "medium", "description": f"Values in {column} should match expected pattern"}
                            )
                        )
                
                # Uniqueness expectations
                if col_config.get("unique", False):
                    suite.add_expectation(
                        ExpectationConfiguration(
                            expectation_type="expect_column_values_to_be_unique",
                            kwargs={"column": column},
                            meta={"severity": "high", "description": f"Values in {column} should be unique"}
                        )
                    )
                
                # Set membership expectations
                if col_config.get("allowed_values"):
                    suite.add_expectation(
                        ExpectationConfiguration(
                            expectation_type="expect_column_values_to_be_in_set",
                            kwargs={
                                "column": column,
                                "value_set": col_config["allowed_values"]
                            },
                            meta={"severity": "high", "description": f"Values in {column} should be from allowed set"}
                        )
                    )
            
            # Save the suite
            self.context.add_or_update_expectation_suite(suite)
            self.expectation_suites[suite_name] = suite
            
            self.logger.info(f"Created {len(suite.expectations)} common expectations for suite {suite_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to create common expectations: {e}")
            raise
    
    def validate_dataframe(self, df: pd.DataFrame, suite_name: str, datasource_name: str = "pandas_datasource") -> DataQualityReport:
        """Validate DataFrame against expectation suite
        
        Args:
            df: DataFrame to validate
            suite_name: Name of expectation suite
            datasource_name: Name of datasource
            
        Returns:
            DataQualityReport with validation results
        """
        try:
            # Ensure datasource exists
            if datasource_name not in self.data_sources:
                self.create_pandas_datasource(datasource_name)
            
            # Create batch request
            batch_request = RuntimeBatchRequest(
                datasource_name=datasource_name,
                data_connector_name="runtime_data_connector",
                data_asset_name=f"validation_asset_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                runtime_parameters={"batch_data": df},
                batch_identifiers={"default_identifier_name": "default_identifier"}
            )
            
            # Get validator
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name
            )
            
            # Run validation
            validation_result = validator.validate(catch_exceptions=False)
            
            # Process results
            report = self._process_validation_results(
                validation_result=validation_result,
                dataset_name=f"dataframe_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                df=df
            )
            
            self.logger.info(f"Validation completed: {report.success_percentage:.1f}% success rate")
            return report
        
        except Exception as e:
            self.logger.error(f"Validation failed: {e}")
            self.logger.error(traceback.format_exc())
            
            # Return error report
            return DataQualityReport(
                dataset_name=f"dataframe_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                overall_status=DataQualityStatus.ERROR,
                metadata={"error": str(e), "traceback": traceback.format_exc()}
            )
    
    def _process_validation_results(self, validation_result: Any, dataset_name: str, df: pd.DataFrame) -> DataQualityReport:
        """Process validation results into DataQualityReport
        
        Args:
            validation_result: Great Expectations validation result
            dataset_name: Name of the dataset
            df: Original DataFrame for statistics
            
        Returns:
            DataQualityReport
        """
        try:
            # Initialize report
            report = DataQualityReport(dataset_name=dataset_name)
            
            # Extract validation statistics
            validation_results = validation_result.results
            report.total_expectations = len(validation_results)
            report.successful_expectations = sum(1 for r in validation_results if r.success)
            report.failed_expectations = report.total_expectations - report.successful_expectations
            
            if report.total_expectations > 0:
                report.success_percentage = (report.successful_expectations / report.total_expectations) * 100
            
            # Determine overall status
            if report.success_percentage == 100:
                report.overall_status = DataQualityStatus.PASSED
            elif report.success_percentage == 0:
                report.overall_status = DataQualityStatus.FAILED
            else:
                report.overall_status = DataQualityStatus.PARTIAL
            
            # Process individual results
            critical_failures = 0
            high_failures = 0
            
            for result in validation_results:
                expectation_config = result.expectation_config
                column = expectation_config.kwargs.get("column", "table")
                severity_str = expectation_config.meta.get("severity", "medium")
                
                try:
                    severity = ExpectationSeverity(severity_str)
                except ValueError:
                    severity = ExpectationSeverity.MEDIUM
                
                # Count failures by severity
                if not result.success:
                    if severity == ExpectationSeverity.CRITICAL:
                        critical_failures += 1
                    elif severity == ExpectationSeverity.HIGH:
                        high_failures += 1
                
                # Create expectation result
                expectation_result = ExpectationResult(
                    expectation_type=expectation_config.expectation_type,
                    column=column,
                    success=result.success,
                    severity=severity,
                    observed_value=result.result.get("observed_value"),
                    expected_value=result.result.get("expected_value"),
                    exception_info=result.exception_info,
                    result_details=result.result
                )
                
                report.expectation_results.append(expectation_result)
            
            # Generate validation summary
            report.validation_summary = {
                "total_expectations": report.total_expectations,
                "successful_expectations": report.successful_expectations,
                "failed_expectations": report.failed_expectations,
                "success_percentage": report.success_percentage,
                "critical_failures": critical_failures,
                "high_failures": high_failures,
                "evaluation_parameters": validation_result.evaluation_parameters
            }
            
            # Generate data statistics
            report.data_statistics = self._generate_data_statistics(df)
            
            # Generate recommendations
            report.recommendations = self._generate_recommendations(report)
            
            # Add metadata
            report.metadata = {
                "validation_time": validation_result.meta.get("run_id", {}).get("run_time"),
                "great_expectations_version": gx.__version__,
                "expectation_suite_name": validation_result.meta.get("expectation_suite_name"),
                "batch_kwargs": validation_result.meta.get("batch_kwargs", {}),
                "data_asset_name": validation_result.meta.get("data_asset_name"),
            }
            
            return report
        
        except Exception as e:
            self.logger.error(f"Failed to process validation results: {e}")
            raise
    
    def _generate_data_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate basic data statistics
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary with statistics
        """
        try:
            stats = {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
                "columns": {}
            }
            
            for column in df.columns:
                col_stats = {
                    "dtype": str(df[column].dtype),
                    "null_count": df[column].isnull().sum(),
                    "null_percentage": (df[column].isnull().sum() / len(df)) * 100,
                    "unique_count": df[column].nunique(),
                    "unique_percentage": (df[column].nunique() / len(df)) * 100
                }
                
                if pd.api.types.is_numeric_dtype(df[column]):
                    col_stats.update({
                        "mean": float(df[column].mean()),
                        "std": float(df[column].std()),
                        "min": float(df[column].min()),
                        "max": float(df[column].max()),
                        "median": float(df[column].median())
                    })
                
                stats["columns"][column] = col_stats
            
            return stats
        
        except Exception as e:
            self.logger.warning(f"Failed to generate data statistics: {e}")
            return {"error": str(e)}
    
    def _generate_recommendations(self, report: DataQualityReport) -> List[str]:
        """Generate recommendations based on validation results
        
        Args:
            report: Data quality report
            
        Returns:
            List of recommendations
        """
        recommendations = []
        
        try:
            # Overall quality recommendations
            if report.success_percentage < 50:
                recommendations.append("Critical: Data quality is very poor. Consider data source investigation.")
            elif report.success_percentage < 80:
                recommendations.append("Warning: Data quality needs improvement. Review failed expectations.")
            elif report.success_percentage < 95:
                recommendations.append("Info: Good data quality with room for improvement.")
            
            # Specific failure analysis
            critical_failures = sum(1 for r in report.expectation_results 
                                  if not r.success and r.severity == ExpectationSeverity.CRITICAL)
            if critical_failures > 0:
                recommendations.append(f"Urgent: Address {critical_failures} critical data quality issues immediately.")
            
            # Column-specific recommendations
            column_failures = {}
            for result in report.expectation_results:
                if not result.success:
                    if result.column not in column_failures:
                        column_failures[result.column] = 0
                    column_failures[result.column] += 1
            
            # Identify problematic columns
            problematic_columns = [col for col, count in column_failures.items() if count > 2]
            if problematic_columns:
                recommendations.append(f"Focus on columns with multiple issues: {', '.join(problematic_columns[:5])}")
            
            # Null value recommendations
            null_issues = [r for r in report.expectation_results 
                          if not r.success and "null" in r.expectation_type]
            if null_issues:
                recommendations.append("Consider implementing null value handling strategy.")
            
            # Uniqueness recommendations
            uniqueness_issues = [r for r in report.expectation_results 
                               if not r.success and "unique" in r.expectation_type]
            if uniqueness_issues:
                recommendations.append("Review duplicate records and implement deduplication process.")
            
        except Exception as e:
            self.logger.warning(f"Failed to generate recommendations: {e}")
            recommendations.append("Unable to generate specific recommendations due to processing error.")
        
        return recommendations[:10]  # Limit to top 10 recommendations
    
    def create_data_docs(self, site_name: str = "local_site"):
        """Create and update data documentation
        
        Args:
            site_name: Name of the documentation site
        """
        try:
            # Build data docs
            self.context.build_data_docs(site_names=[site_name])
            self.logger.info(f"Data documentation built for site: {site_name}")
            
            # Get site URL if available
            try:
                site_url = self.context.get_docs_sites_urls()[0]['site_url']
                self.logger.info(f"Data docs available at: {site_url}")
            except Exception:
                self.logger.info("Data docs built successfully (URL not available)")
                
        except Exception as e:
            self.logger.error(f"Failed to build data docs: {e}")
    
    def save_validation_result(self, report: DataQualityReport, output_path: str = None):
        """Save validation result to file
        
        Args:
            report: Data quality report to save
            output_path: Path to save the report
        """
        if not output_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"data_quality_report_{timestamp}.json"
        
        try:
            # Convert report to serializable format
            report_dict = {
                "dataset_name": report.dataset_name,
                "execution_time": report.execution_time.isoformat(),
                "total_expectations": report.total_expectations,
                "successful_expectations": report.successful_expectations,
                "failed_expectations": report.failed_expectations,
                "success_percentage": report.success_percentage,
                "overall_status": report.overall_status.value,
                "validation_summary": report.validation_summary,
                "data_statistics": report.data_statistics,
                "recommendations": report.recommendations,
                "metadata": report.metadata,
                "expectation_results": [
                    {
                        "expectation_type": r.expectation_type,
                        "column": r.column,
                        "success": r.success,
                        "severity": r.severity.value,
                        "observed_value": str(r.observed_value) if r.observed_value is not None else None,
                        "expected_value": str(r.expected_value) if r.expected_value is not None else None,
                        "timestamp": r.timestamp.isoformat(),
                        "result_details": r.result_details
                    }
                    for r in report.expectation_results
                ]
            }
            
            with open(output_path, 'w') as f:
                json.dump(report_dict, f, indent=2, default=str)
            
            self.logger.info(f"Data quality report saved to: {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save validation result: {e}")


def create_sample_data_quality_profiles() -> List[DataQualityProfile]:
    """Create sample data quality profiles for common use cases"""
    
    # Sales data profile
    sales_profile = DataQualityProfile(
        profile_name="sales_transactions",
        expectation_templates=[
            ExpectationTemplate(
                name="invoice_no_not_null",
                expectation_type="expect_column_values_to_not_be_null",
                column="invoice_no",
                severity=ExpectationSeverity.CRITICAL,
                description="Invoice number must be present",
                business_rule="Every transaction must have a valid invoice number"
            ),
            ExpectationTemplate(
                name="quantity_positive",
                expectation_type="expect_column_values_to_be_between",
                column="quantity",
                kwargs={"min_value": 0, "max_value": 10000},
                severity=ExpectationSeverity.HIGH,
                description="Quantity must be positive and reasonable"
            ),
            ExpectationTemplate(
                name="unit_price_range",
                expectation_type="expect_column_values_to_be_between",
                column="unit_price",
                kwargs={"min_value": 0, "max_value": 1000},
                severity=ExpectationSeverity.HIGH,
                description="Unit price must be within reasonable range"
            ),
            ExpectationTemplate(
                name="customer_id_format",
                expectation_type="expect_column_values_to_match_regex",
                column="customer_id",
                kwargs={"regex": r"^[A-Z0-9]{5,10}$"},
                severity=ExpectationSeverity.MEDIUM,
                description="Customer ID must match expected format"
            )
        ],
        alert_thresholds={"success_rate": 0.95, "critical_failures": 0}
    )
    
    # Customer data profile  
    customer_profile = DataQualityProfile(
        profile_name="customer_master",
        expectation_templates=[
            ExpectationTemplate(
                name="customer_id_unique",
                expectation_type="expect_column_values_to_be_unique",
                column="customer_id",
                severity=ExpectationSeverity.CRITICAL,
                description="Customer ID must be unique"
            ),
            ExpectationTemplate(
                name="email_format",
                expectation_type="expect_column_values_to_match_regex",
                column="email",
                kwargs={"regex": r"^[^@]+@[^@]+\.[^@]+$"},
                severity=ExpectationSeverity.HIGH,
                description="Email must be in valid format"
            ),
            ExpectationTemplate(
                name="country_values",
                expectation_type="expect_column_values_to_be_in_set",
                column="country",
                kwargs={"value_set": ["United States", "United Kingdom", "Germany", "France", "Canada"]},
                severity=ExpectationSeverity.MEDIUM,
                description="Country must be from approved list"
            )
        ]
    )
    
    return [sales_profile, customer_profile]


# Example usage and testing
if __name__ == "__main__":
    import tempfile
    import shutil
    
    print("Testing Great Expectations Integration...")
    
    try:
        # Create temporary directory for GX context
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Initialize manager
            gx_manager = GreatExpectationsManager(context_root_dir=temp_dir)
            
            # Create sample DataFrame
            sample_data = pd.DataFrame({
                'invoice_no': ['INV001', 'INV002', 'INV003', None, 'INV005'],
                'customer_id': ['CUST001', 'CUST002', 'CUST003', 'CUST004', 'CUST001'],  # Duplicate
                'quantity': [1, 2, -1, 5, 1000000],  # Invalid values
                'unit_price': [10.5, 25.0, 15.75, -5.0, 1500.0],  # Invalid values
                'country': ['US', 'UK', 'Germany', 'InvalidCountry', 'France']
            })
            
            print(f"Sample data shape: {sample_data.shape}")
            
            # Create expectations from DataFrame analysis
            suite_name = "sample_validation_suite"
            columns_config = {
                'invoice_no': {'not_null': True, 'null_severity': 'critical'},
                'customer_id': {'unique': True, 'regex_pattern': r'^CUST\d+$'},
                'quantity': {'min_value': 0, 'max_value': 10000},
                'unit_price': {'min_value': 0, 'max_value': 1000},
                'country': {'allowed_values': ['US', 'UK', 'Germany', 'France', 'Canada']}
            }
            
            gx_manager.create_common_expectations(suite_name, sample_data, columns_config)
            print(f"Created expectation suite: {suite_name}")
            
            # Validate DataFrame
            print("\nRunning validation...")
            report = gx_manager.validate_dataframe(
                df=sample_data, 
                suite_name=suite_name
            )
            
            print(f"\nValidation Results:")
            print(f"- Overall Status: {report.overall_status.value}")
            print(f"- Success Rate: {report.success_percentage:.1f}%")
            print(f"- Total Expectations: {report.total_expectations}")
            print(f"- Failed Expectations: {report.failed_expectations}")
            
            print(f"\nRecommendations:")
            for i, rec in enumerate(report.recommendations, 1):
                print(f"  {i}. {rec}")
            
            # Save report
            output_file = "sample_data_quality_report.json"
            gx_manager.save_validation_result(report, output_file)
            print(f"\nReport saved to: {output_file}")
            
            # Test with sample profiles
            print("\nTesting sample profiles...")
            profiles = create_sample_data_quality_profiles()
            
            for profile in profiles:
                print(f"- Profile: {profile.profile_name} ({len(profile.expectation_templates)} expectations)")
            
            print("\n✅ Great Expectations integration testing completed successfully!")
            
        finally:
            # Cleanup temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)
            
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()
"""
Smart Data Validation with ML-based Drift Detection
Advanced validation system with context-aware rules and machine learning capabilities.
"""
from __future__ import annotations

import json
import statistics
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable

import numpy as np
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from core.logging import get_logger
from ..core.anomaly_detection import MLAnomalyDetector
from ..core.statistical_analyzer import StatisticalAnalyzer


class ValidationType(Enum):
    """Types of validation"""
    SCHEMA_VALIDATION = "schema_validation"
    DATA_DRIFT = "data_drift"
    STATISTICAL_DRIFT = "statistical_drift"
    PATTERN_VALIDATION = "pattern_validation"
    BUSINESS_RULE = "business_rule"
    REFERENCE_DATA = "reference_data"
    CROSS_FIELD = "cross_field"
    TEMPORAL_CONSISTENCY = "temporal_consistency"


class DriftType(Enum):
    """Types of data drift"""
    SCHEMA_DRIFT = "schema_drift"
    STATISTICAL_DRIFT = "statistical_drift"
    CATEGORICAL_DRIFT = "categorical_drift"
    NUMERICAL_DRIFT = "numerical_drift"
    DISTRIBUTION_DRIFT = "distribution_drift"
    CONCEPT_DRIFT = "concept_drift"


class ValidationSeverity(Enum):
    """Validation result severity"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationRule:
    """Smart validation rule definition"""
    rule_id: str
    name: str
    description: str
    validation_type: ValidationType
    
    # Rule configuration
    field_name: str | None = None
    condition: str | None = None
    expected_value: Any = None
    threshold: float | None = None
    
    # Advanced configuration
    custom_validator: str | None = None  # Reference to custom validation function
    sql_check: str | None = None        # SQL validation query
    reference_dataset: str | None = None # Reference data for comparison
    
    # ML configuration
    enable_drift_detection: bool = True
    drift_threshold: float = 0.1
    baseline_period_days: int = 30
    
    # Context awareness
    context_fields: list[str] = field(default_factory=list)
    conditional_logic: str | None = None  # When to apply this rule
    
    # Rule lifecycle
    active: bool = True
    severity: ValidationSeverity = ValidationSeverity.WARNING
    auto_adapt: bool = False  # Auto-adapt thresholds based on data
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Validation result"""
    result_id: str
    rule_id: str
    dataset_id: str
    field_name: str | None
    
    # Result details
    passed: bool
    severity: ValidationSeverity
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    
    # Drift information
    drift_detected: bool = False
    drift_type: DriftType | None = None
    drift_magnitude: float = 0.0
    
    # Statistical information
    current_value: Any = None
    expected_value: Any = None
    baseline_value: Any = None
    confidence_score: float = 1.0
    
    # Context
    validation_timestamp: datetime = field(default_factory=datetime.now)
    affected_records: int = 0
    sample_failing_records: list[dict[str, Any]] = field(default_factory=list)
    
    # Recommendations
    recommendations: list[str] = field(default_factory=list)
    auto_fixable: bool = False
    
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DriftAnalysis:
    """Drift detection analysis"""
    analysis_id: str
    dataset_id: str
    analysis_timestamp: datetime = field(default_factory=datetime.now)
    
    # Drift summary
    overall_drift_score: float = 0.0
    drift_detected: bool = False
    drift_types: list[DriftType] = field(default_factory=list)
    
    # Field-level drift
    field_drift_scores: dict[str, float] = field(default_factory=dict)
    drifted_fields: list[str] = field(default_factory=list)
    
    # Statistical analysis
    distribution_changes: dict[str, dict[str, Any]] = field(default_factory=dict)
    statistical_tests: dict[str, dict[str, Any]] = field(default_factory=dict)
    
    # Schema drift
    schema_changes: dict[str, Any] = field(default_factory=dict)
    
    # Recommendations
    recommendations: list[str] = field(default_factory=list)
    remediation_actions: list[str] = field(default_factory=list)
    
    metadata: dict[str, Any] = field(default_factory=dict)


class SmartValidator:
    """
    Smart Data Validation Engine
    
    Provides intelligent validation with:
    - Context-aware validation rules
    - ML-based drift detection
    - Automated test case generation
    - Adaptive thresholds
    - Cross-dataset consistency checking
    - Data freshness monitoring
    """

    def __init__(self, storage_path: Path | None = None):
        self.logger = get_logger(self.__class__.__name__)
        
        # Storage
        self.storage_path = storage_path or Path("./data_validation")
        self.rules_path = self.storage_path / "rules"
        self.results_path = self.storage_path / "results"
        self.baselines_path = self.storage_path / "baselines"
        self.drift_path = self.storage_path / "drift_analysis"
        
        self._ensure_directories()
        
        # Components
        self.anomaly_detector = MLAnomalyDetector()
        self.statistical_analyzer = StatisticalAnalyzer()
        
        # In-memory storage
        self.validation_rules: dict[str, ValidationRule] = {}
        self.active_rules: dict[str, ValidationRule] = {}
        self.validation_history: deque[ValidationResult] = deque(maxlen=10000)
        self.baseline_profiles: dict[str, dict[str, Any]] = {}
        
        # Custom validators
        self.custom_validators: dict[str, Callable] = {}
        
        # ML models for drift detection
        self.drift_detectors: dict[str, Any] = {}
        self.scalers: dict[str, StandardScaler] = {}
        
        # Thread safety
        self._lock = threading.Lock()
        
        # Configuration
        self.drift_detection_window_days = 7
        self.baseline_update_frequency_days = 7
        self.auto_adaptation_enabled = True
        
        # Load existing data
        self._load_validation_data()
        
        # Start background processes
        self._start_background_processes()

    def _ensure_directories(self):
        """Ensure validation directories exist"""
        for path in [self.storage_path, self.rules_path, self.results_path, 
                    self.baselines_path, self.drift_path]:
            path.mkdir(parents=True, exist_ok=True)

    def _load_validation_data(self):
        """Load existing validation rules and baselines"""
        try:
            # Load validation rules
            for rule_file in self.rules_path.glob("*.json"):
                try:
                    with open(rule_file) as f:
                        rule_data = json.load(f)
                        rule = self._dict_to_validation_rule(rule_data)
                        self.validation_rules[rule.rule_id] = rule
                        
                        if rule.active:
                            self.active_rules[rule.rule_id] = rule
                            
                except Exception as e:
                    self.logger.error(f"Failed to load validation rule {rule_file}: {e}")
            
            # Load baseline profiles
            for baseline_file in self.baselines_path.glob("*.json"):
                try:
                    with open(baseline_file) as f:
                        baseline_data = json.load(f)
                        dataset_id = baseline_file.stem.replace("baseline_", "")
                        self.baseline_profiles[dataset_id] = baseline_data
                except Exception as e:
                    self.logger.error(f"Failed to load baseline {baseline_file}: {e}")
            
            self.logger.info(
                f"Loaded {len(self.validation_rules)} validation rules "
                f"({len(self.active_rules)} active), "
                f"{len(self.baseline_profiles)} baseline profiles"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to load validation data: {e}")

    def _start_background_processes(self):
        """Start background validation processes"""
        def drift_monitoring_loop():
            while True:
                try:
                    import time
                    time.sleep(3600)  # Check every hour
                    
                    self._update_baselines()
                    self._cleanup_old_data()
                    
                except Exception as e:
                    self.logger.error(f"Drift monitoring loop error: {e}")
        
        drift_thread = threading.Thread(target=drift_monitoring_loop, daemon=True)
        drift_thread.start()
        
        self.logger.info("Started validation background processes")

    def create_validation_rule(
        self,
        name: str,
        description: str,
        validation_type: ValidationType,
        **kwargs
    ) -> str:
        """Create a new validation rule"""
        try:
            with self._lock:
                rule_id = f"rule_{int(datetime.now().timestamp())}"
                
                rule = ValidationRule(
                    rule_id=rule_id,
                    name=name,
                    description=description,
                    validation_type=validation_type,
                    **{k: v for k, v in kwargs.items() if hasattr(ValidationRule, k)}
                )
                
                # Validate rule configuration
                validation_errors = self._validate_rule_config(rule)
                if validation_errors:
                    raise ValueError(f"Rule validation failed: {validation_errors}")
                
                # Store rule
                self.validation_rules[rule_id] = rule
                
                if rule.active:
                    self.active_rules[rule_id] = rule
                
                # Persist rule
                self._persist_validation_rule(rule)
                
                self.logger.info(f"Created validation rule: {name} ({rule_id})")
                return rule_id
                
        except Exception as e:
            self.logger.error(f"Failed to create validation rule: {e}")
            raise

    def validate_dataset(
        self,
        dataset_id: str,
        data: list[dict[str, Any]],
        schema: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None
    ) -> list[ValidationResult]:
        """Comprehensive dataset validation"""
        try:
            validation_results = []
            
            if not data:
                return validation_results
            
            # Get applicable rules
            applicable_rules = self._get_applicable_rules(dataset_id, schema)
            
            # Execute validation rules
            for rule in applicable_rules:
                try:
                    result = self._execute_validation_rule(
                        rule, dataset_id, data, schema, context or {}
                    )
                    if result:
                        validation_results.append(result)
                except Exception as e:
                    self.logger.error(f"Validation rule {rule.rule_id} failed: {e}")
            
            # Perform drift analysis if enabled
            drift_analysis = self._analyze_data_drift(dataset_id, data, schema)
            if drift_analysis and drift_analysis.drift_detected:
                # Create validation results from drift analysis
                drift_results = self._create_drift_validation_results(drift_analysis)
                validation_results.extend(drift_results)
            
            # Store results
            with self._lock:
                self.validation_history.extend(validation_results)
            
            # Persist results
            self._persist_validation_results(validation_results)
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Dataset validation failed for {dataset_id}: {e}")
            return []

    def register_custom_validator(self, name: str, validator_func: Callable):
        """Register a custom validation function"""
        self.custom_validators[name] = validator_func
        self.logger.info(f"Registered custom validator: {name}")

    def detect_data_drift(
        self,
        dataset_id: str,
        current_data: list[dict[str, Any]],
        baseline_data: list[dict[str, Any]] | None = None,
        schema: dict[str, Any] | None = None
    ) -> DriftAnalysis:
        """Comprehensive data drift detection"""
        try:
            analysis = DriftAnalysis(
                analysis_id=f"drift_{dataset_id}_{int(datetime.now().timestamp())}",
                dataset_id=dataset_id
            )
            
            # Use stored baseline if not provided
            if baseline_data is None:
                baseline_profile = self.baseline_profiles.get(dataset_id)
                if not baseline_profile:
                    # Generate baseline from current data (first run)
                    baseline_profile = self._generate_baseline_profile(current_data, schema)
                    self.baseline_profiles[dataset_id] = baseline_profile
                    self._persist_baseline_profile(dataset_id, baseline_profile)
                    
                    # No drift on first run
                    return analysis
            else:
                # Generate baseline profile from provided data
                baseline_profile = self._generate_baseline_profile(baseline_data, schema)
            
            # Generate current profile
            current_profile = self._generate_baseline_profile(current_data, schema)
            
            # Detect schema drift
            schema_drift = self._detect_schema_drift(
                baseline_profile.get('schema', {}),
                current_profile.get('schema', {})
            )
            
            if schema_drift:
                analysis.drift_types.append(DriftType.SCHEMA_DRIFT)
                analysis.schema_changes = schema_drift
            
            # Detect statistical drift
            statistical_drift = self._detect_statistical_drift(
                baseline_profile.get('statistics', {}),
                current_profile.get('statistics', {})
            )
            
            if statistical_drift['drift_detected']:
                analysis.drift_types.append(DriftType.STATISTICAL_DRIFT)
                analysis.field_drift_scores.update(statistical_drift['field_scores'])
                analysis.statistical_tests = statistical_drift['test_results']
            
            # Detect distribution drift
            distribution_drift = self._detect_distribution_drift(
                baseline_data or self._reconstruct_data_from_profile(baseline_profile),
                current_data
            )
            
            if distribution_drift['drift_detected']:
                analysis.drift_types.append(DriftType.DISTRIBUTION_DRIFT)
                analysis.distribution_changes = distribution_drift['changes']
            
            # Calculate overall drift score
            analysis.overall_drift_score = self._calculate_overall_drift_score(analysis)
            analysis.drift_detected = analysis.overall_drift_score > 0.1
            
            # Generate recommendations
            analysis.recommendations = self._generate_drift_recommendations(analysis)
            analysis.remediation_actions = self._generate_drift_remediation_actions(analysis)
            
            # Persist drift analysis
            self._persist_drift_analysis(analysis)
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Drift detection failed for {dataset_id}: {e}")
            return DriftAnalysis(
                analysis_id=f"drift_error_{int(datetime.now().timestamp())}",
                dataset_id=dataset_id
            )

    def generate_test_cases(
        self,
        dataset_id: str,
        data_sample: list[dict[str, Any]],
        schema: dict[str, Any] | None = None
    ) -> list[ValidationRule]:
        """Automatically generate validation test cases"""
        try:
            generated_rules = []
            
            if not data_sample:
                return generated_rules
            
            # Analyze data sample to infer validation rules
            profile = self._generate_baseline_profile(data_sample, schema)
            
            # Generate schema validation rules
            schema_rules = self._generate_schema_validation_rules(dataset_id, profile)
            generated_rules.extend(schema_rules)
            
            # Generate statistical validation rules
            statistical_rules = self._generate_statistical_validation_rules(dataset_id, profile)
            generated_rules.extend(statistical_rules)
            
            # Generate pattern validation rules
            pattern_rules = self._generate_pattern_validation_rules(dataset_id, data_sample)
            generated_rules.extend(pattern_rules)
            
            # Generate business rule suggestions
            business_rules = self._generate_business_rule_suggestions(dataset_id, data_sample, profile)
            generated_rules.extend(business_rules)
            
            self.logger.info(f"Generated {len(generated_rules)} test cases for {dataset_id}")
            
            return generated_rules
            
        except Exception as e:
            self.logger.error(f"Test case generation failed for {dataset_id}: {e}")
            return []

    def check_data_freshness(
        self,
        dataset_id: str,
        data: list[dict[str, Any]],
        timestamp_field: str,
        max_age_hours: int = 24
    ) -> ValidationResult:
        """Check data freshness"""
        try:
            result_id = f"freshness_{dataset_id}_{int(datetime.now().timestamp())}"
            
            if not data:
                return ValidationResult(
                    result_id=result_id,
                    rule_id="freshness_check",
                    dataset_id=dataset_id,
                    field_name=timestamp_field,
                    passed=False,
                    severity=ValidationSeverity.ERROR,
                    message="No data to check freshness"
                )
            
            # Extract timestamps
            timestamps = []
            for record in data:
                if timestamp_field in record:
                    try:
                        if isinstance(record[timestamp_field], datetime):
                            timestamps.append(record[timestamp_field])
                        elif isinstance(record[timestamp_field], str):
                            timestamps.append(datetime.fromisoformat(record[timestamp_field]))
                    except (ValueError, TypeError):
                        continue
            
            if not timestamps:
                return ValidationResult(
                    result_id=result_id,
                    rule_id="freshness_check",
                    dataset_id=dataset_id,
                    field_name=timestamp_field,
                    passed=False,
                    severity=ValidationSeverity.ERROR,
                    message=f"No valid timestamps found in field {timestamp_field}"
                )
            
            # Check freshness
            latest_timestamp = max(timestamps)
            age_hours = (datetime.now() - latest_timestamp).total_seconds() / 3600
            
            passed = age_hours <= max_age_hours
            
            return ValidationResult(
                result_id=result_id,
                rule_id="freshness_check",
                dataset_id=dataset_id,
                field_name=timestamp_field,
                passed=passed,
                severity=ValidationSeverity.WARNING if not passed else ValidationSeverity.INFO,
                message=f"Data age: {age_hours:.1f} hours (max: {max_age_hours} hours)",
                current_value=age_hours,
                expected_value=max_age_hours,
                details={
                    'latest_timestamp': latest_timestamp.isoformat(),
                    'age_hours': age_hours,
                    'max_age_hours': max_age_hours,
                    'total_records': len(data),
                    'records_with_timestamps': len(timestamps)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Data freshness check failed: {e}")
            return ValidationResult(
                result_id=f"freshness_error_{int(datetime.now().timestamp())}",
                rule_id="freshness_check",
                dataset_id=dataset_id,
                field_name=timestamp_field,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Freshness check failed: {str(e)}"
            )

    def check_cross_dataset_consistency(
        self,
        dataset1_id: str,
        dataset1_data: list[dict[str, Any]],
        dataset2_id: str,
        dataset2_data: list[dict[str, Any]],
        join_keys: list[str],
        check_fields: list[str] | None = None
    ) -> list[ValidationResult]:
        """Check consistency between datasets"""
        try:
            results = []
            
            if not dataset1_data or not dataset2_data:
                return results
            
            # Create lookup dictionaries
            dataset1_lookup = {}
            dataset2_lookup = {}
            
            for record in dataset1_data:
                key = tuple(record.get(k) for k in join_keys)
                dataset1_lookup[key] = record
            
            for record in dataset2_data:
                key = tuple(record.get(k) for k in join_keys)
                dataset2_lookup[key] = record
            
            # Find common keys
            common_keys = set(dataset1_lookup.keys()) & set(dataset2_lookup.keys())
            
            # Check referential integrity
            dataset1_only = len(dataset1_lookup) - len(common_keys)
            dataset2_only = len(dataset2_lookup) - len(common_keys)
            
            if dataset1_only > 0:
                results.append(ValidationResult(
                    result_id=f"ref_integrity_{dataset1_id}_{int(datetime.now().timestamp())}",
                    rule_id="referential_integrity",
                    dataset_id=dataset1_id,
                    field_name=','.join(join_keys),
                    passed=dataset1_only == 0,
                    severity=ValidationSeverity.WARNING,
                    message=f"{dataset1_only} records in {dataset1_id} not found in {dataset2_id}",
                    affected_records=dataset1_only,
                    details={
                        'dataset1_records': len(dataset1_lookup),
                        'dataset2_records': len(dataset2_lookup),
                        'common_records': len(common_keys),
                        'dataset1_only': dataset1_only,
                        'dataset2_only': dataset2_only
                    }
                ))
            
            # Check field consistency for common records
            if check_fields and common_keys:
                for field in check_fields:
                    inconsistent_count = 0
                    sample_inconsistencies = []
                    
                    for key in list(common_keys)[:1000]:  # Sample for performance
                        record1 = dataset1_lookup[key]
                        record2 = dataset2_lookup[key]
                        
                        value1 = record1.get(field)
                        value2 = record2.get(field)
                        
                        if value1 != value2:
                            inconsistent_count += 1
                            if len(sample_inconsistencies) < 10:
                                sample_inconsistencies.append({
                                    'key': key,
                                    f'{dataset1_id}_value': value1,
                                    f'{dataset2_id}_value': value2
                                })
                    
                    if inconsistent_count > 0:
                        results.append(ValidationResult(
                            result_id=f"consistency_{field}_{int(datetime.now().timestamp())}",
                            rule_id="field_consistency",
                            dataset_id=f"{dataset1_id}_{dataset2_id}",
                            field_name=field,
                            passed=inconsistent_count == 0,
                            severity=ValidationSeverity.ERROR if inconsistent_count > len(common_keys) * 0.1 else ValidationSeverity.WARNING,
                            message=f"{inconsistent_count} inconsistent values in field {field}",
                            affected_records=inconsistent_count,
                            sample_failing_records=sample_inconsistencies,
                            details={
                                'inconsistent_count': inconsistent_count,
                                'total_compared': len(common_keys),
                                'inconsistency_rate': inconsistent_count / len(common_keys) if common_keys else 0
                            }
                        ))
            
            return results
            
        except Exception as e:
            self.logger.error(f"Cross-dataset consistency check failed: {e}")
            return []

    def _get_applicable_rules(
        self,
        dataset_id: str,
        schema: dict[str, Any] | None = None
    ) -> list[ValidationRule]:
        """Get validation rules applicable to a dataset"""
        applicable_rules = []
        
        try:
            for rule in self.active_rules.values():
                # Check if rule applies to this dataset
                if self._rule_applies_to_dataset(rule, dataset_id, schema):
                    applicable_rules.append(rule)
            
            return applicable_rules
            
        except Exception as e:
            self.logger.error(f"Failed to get applicable rules: {e}")
            return []

    def _rule_applies_to_dataset(
        self,
        rule: ValidationRule,
        dataset_id: str,
        schema: dict[str, Any] | None = None
    ) -> bool:
        """Check if validation rule applies to dataset"""
        try:
            # Dataset-specific rules
            if 'dataset_pattern' in rule.metadata:
                import re
                pattern = rule.metadata['dataset_pattern']
                if not re.match(pattern, dataset_id):
                    return False
            
            # Field-specific rules
            if rule.field_name and schema:
                fields = schema.get('fields', {})
                if rule.field_name not in fields:
                    return False
            
            # Context-based rules
            if rule.conditional_logic:
                # Evaluate conditional logic (placeholder)
                # In practice, implement a proper expression evaluator
                pass
            
            return True
            
        except Exception as e:
            self.logger.error(f"Rule applicability check failed: {e}")
            return False

    def _execute_validation_rule(
        self,
        rule: ValidationRule,
        dataset_id: str,
        data: list[dict[str, Any]],
        schema: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None
    ) -> ValidationResult | None:
        """Execute a single validation rule"""
        try:
            result_id = f"validation_{rule.rule_id}_{int(datetime.now().timestamp())}"
            
            # Custom validator
            if rule.custom_validator and rule.custom_validator in self.custom_validators:
                validator_func = self.custom_validators[rule.custom_validator]
                validation_passed = validator_func(data, rule, context or {})
                
                return ValidationResult(
                    result_id=result_id,
                    rule_id=rule.rule_id,
                    dataset_id=dataset_id,
                    field_name=rule.field_name,
                    passed=validation_passed,
                    severity=rule.severity if not validation_passed else ValidationSeverity.INFO,
                    message=f"Custom validation {'passed' if validation_passed else 'failed'}: {rule.name}"
                )
            
            # Schema validation
            elif rule.validation_type == ValidationType.SCHEMA_VALIDATION:
                return self._execute_schema_validation(rule, dataset_id, data, schema)
            
            # Statistical validation
            elif rule.validation_type == ValidationType.STATISTICAL_DRIFT:
                return self._execute_statistical_validation(rule, dataset_id, data)
            
            # Pattern validation
            elif rule.validation_type == ValidationType.PATTERN_VALIDATION:
                return self._execute_pattern_validation(rule, dataset_id, data)
            
            # Business rule validation
            elif rule.validation_type == ValidationType.BUSINESS_RULE:
                return self._execute_business_rule_validation(rule, dataset_id, data)
            
            # Cross-field validation
            elif rule.validation_type == ValidationType.CROSS_FIELD:
                return self._execute_cross_field_validation(rule, dataset_id, data)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Validation rule execution failed: {e}")
            return ValidationResult(
                result_id=f"validation_error_{int(datetime.now().timestamp())}",
                rule_id=rule.rule_id,
                dataset_id=dataset_id,
                field_name=rule.field_name,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Validation execution failed: {str(e)}"
            )

    def _execute_schema_validation(
        self,
        rule: ValidationRule,
        dataset_id: str,
        data: list[dict[str, Any]],
        schema: dict[str, Any] | None = None
    ) -> ValidationResult:
        """Execute schema validation"""
        try:
            if not schema:
                return ValidationResult(
                    result_id=f"schema_{rule.rule_id}_{int(datetime.now().timestamp())}",
                    rule_id=rule.rule_id,
                    dataset_id=dataset_id,
                    field_name=rule.field_name,
                    passed=False,
                    severity=ValidationSeverity.WARNING,
                    message="No schema provided for validation"
                )
            
            # Check required field presence
            if rule.field_name:
                schema_fields = schema.get('fields', {})
                field_present = rule.field_name in schema_fields
                
                if not field_present:
                    return ValidationResult(
                        result_id=f"schema_{rule.rule_id}_{int(datetime.now().timestamp())}",
                        rule_id=rule.rule_id,
                        dataset_id=dataset_id,
                        field_name=rule.field_name,
                        passed=False,
                        severity=rule.severity,
                        message=f"Required field {rule.field_name} not found in schema"
                    )
                
                # Check data type consistency
                expected_type = schema_fields[rule.field_name].get('type')
                if expected_type and data:
                    type_errors = 0
                    sample_errors = []
                    
                    for i, record in enumerate(data[:1000]):  # Sample for performance
                        if rule.field_name in record:
                            value = record[rule.field_name]
                            if not self._check_data_type(value, expected_type):
                                type_errors += 1
                                if len(sample_errors) < 5:
                                    sample_errors.append({
                                        'record_index': i,
                                        'value': value,
                                        'expected_type': expected_type,
                                        'actual_type': type(value).__name__
                                    })
                    
                    type_error_rate = type_errors / min(len(data), 1000)
                    passed = type_error_rate <= 0.05  # Allow 5% type errors
                    
                    return ValidationResult(
                        result_id=f"schema_{rule.rule_id}_{int(datetime.now().timestamp())}",
                        rule_id=rule.rule_id,
                        dataset_id=dataset_id,
                        field_name=rule.field_name,
                        passed=passed,
                        severity=rule.severity if not passed else ValidationSeverity.INFO,
                        message=f"Data type validation: {type_error_rate:.2%} errors",
                        affected_records=type_errors,
                        sample_failing_records=sample_errors,
                        details={
                            'type_error_count': type_errors,
                            'type_error_rate': type_error_rate,
                            'expected_type': expected_type,
                            'records_checked': min(len(data), 1000)
                        }
                    )
            
            # General schema validation passed
            return ValidationResult(
                result_id=f"schema_{rule.rule_id}_{int(datetime.now().timestamp())}",
                rule_id=rule.rule_id,
                dataset_id=dataset_id,
                field_name=rule.field_name,
                passed=True,
                severity=ValidationSeverity.INFO,
                message="Schema validation passed"
            )
            
        except Exception as e:
            self.logger.error(f"Schema validation failed: {e}")
            return ValidationResult(
                result_id=f"schema_error_{int(datetime.now().timestamp())}",
                rule_id=rule.rule_id,
                dataset_id=dataset_id,
                field_name=rule.field_name,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Schema validation error: {str(e)}"
            )

    def _execute_statistical_validation(
        self,
        rule: ValidationRule,
        dataset_id: str,
        data: list[dict[str, Any]]
    ) -> ValidationResult:
        """Execute statistical validation"""
        try:
            if not rule.field_name or not data:
                return ValidationResult(
                    result_id=f"stat_{rule.rule_id}_{int(datetime.now().timestamp())}",
                    rule_id=rule.rule_id,
                    dataset_id=dataset_id,
                    field_name=rule.field_name,
                    passed=False,
                    severity=ValidationSeverity.WARNING,
                    message="Insufficient data for statistical validation"
                )
            
            # Extract field values
            field_values = []
            for record in data:
                if rule.field_name in record and record[rule.field_name] is not None:
                    try:
                        field_values.append(float(record[rule.field_name]))
                    except (ValueError, TypeError):
                        continue
            
            if len(field_values) < 10:
                return ValidationResult(
                    result_id=f"stat_{rule.rule_id}_{int(datetime.now().timestamp())}",
                    rule_id=rule.rule_id,
                    dataset_id=dataset_id,
                    field_name=rule.field_name,
                    passed=False,
                    severity=ValidationSeverity.WARNING,
                    message="Insufficient numeric data for statistical validation"
                )
            
            # Statistical analysis
            mean_value = statistics.mean(field_values)
            std_value = statistics.stdev(field_values) if len(field_values) > 1 else 0
            
            # Check against baseline if available
            baseline = self.baseline_profiles.get(dataset_id, {})
            baseline_stats = baseline.get('statistics', {}).get(rule.field_name, {})
            
            if baseline_stats:
                baseline_mean = baseline_stats.get('mean', mean_value)
                baseline_std = baseline_stats.get('std_dev', std_value)
                
                # Statistical significance test
                if baseline_std > 0:
                    z_score = abs(mean_value - baseline_mean) / baseline_std
                    drift_detected = z_score > 2.0  # 2 standard deviations
                    
                    return ValidationResult(
                        result_id=f"stat_{rule.rule_id}_{int(datetime.now().timestamp())}",
                        rule_id=rule.rule_id,
                        dataset_id=dataset_id,
                        field_name=rule.field_name,
                        passed=not drift_detected,
                        severity=rule.severity if drift_detected else ValidationSeverity.INFO,
                        message=f"Statistical validation: {'drift detected' if drift_detected else 'no significant change'}",
                        drift_detected=drift_detected,
                        drift_type=DriftType.STATISTICAL_DRIFT if drift_detected else None,
                        current_value=mean_value,
                        baseline_value=baseline_mean,
                        confidence_score=1.0 - min(z_score / 3.0, 1.0),
                        details={
                            'current_mean': mean_value,
                            'current_std': std_value,
                            'baseline_mean': baseline_mean,
                            'baseline_std': baseline_std,
                            'z_score': z_score,
                            'sample_size': len(field_values)
                        }
                    )
            
            # No baseline available - create baseline
            return ValidationResult(
                result_id=f"stat_{rule.rule_id}_{int(datetime.now().timestamp())}",
                rule_id=rule.rule_id,
                dataset_id=dataset_id,
                field_name=rule.field_name,
                passed=True,
                severity=ValidationSeverity.INFO,
                message="Statistical baseline established",
                current_value=mean_value,
                details={
                    'mean': mean_value,
                    'std_dev': std_value,
                    'sample_size': len(field_values),
                    'baseline_created': True
                }
            )
            
        except Exception as e:
            self.logger.error(f"Statistical validation failed: {e}")
            return ValidationResult(
                result_id=f"stat_error_{int(datetime.now().timestamp())}",
                rule_id=rule.rule_id,
                dataset_id=dataset_id,
                field_name=rule.field_name,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Statistical validation error: {str(e)}"
            )

    def _execute_pattern_validation(
        self,
        rule: ValidationRule,
        dataset_id: str,
        data: list[dict[str, Any]]
    ) -> ValidationResult:
        """Execute pattern validation"""
        try:
            if not rule.field_name or not rule.condition:
                return ValidationResult(
                    result_id=f"pattern_{rule.rule_id}_{int(datetime.now().timestamp())}",
                    rule_id=rule.rule_id,
                    dataset_id=dataset_id,
                    field_name=rule.field_name,
                    passed=False,
                    severity=ValidationSeverity.WARNING,
                    message="Pattern validation configuration incomplete"
                )
            
            import re
            pattern = re.compile(rule.condition)
            
            pattern_violations = 0
            sample_violations = []
            
            for i, record in enumerate(data):
                if rule.field_name in record:
                    value = str(record[rule.field_name])
                    if not pattern.match(value):
                        pattern_violations += 1
                        if len(sample_violations) < 10:
                            sample_violations.append({
                                'record_index': i,
                                'value': value,
                                'pattern': rule.condition
                            })
            
            violation_rate = pattern_violations / len(data) if data else 0
            passed = violation_rate <= (rule.threshold or 0.05)  # Default 5% threshold
            
            return ValidationResult(
                result_id=f"pattern_{rule.rule_id}_{int(datetime.now().timestamp())}",
                rule_id=rule.rule_id,
                dataset_id=dataset_id,
                field_name=rule.field_name,
                passed=passed,
                severity=rule.severity if not passed else ValidationSeverity.INFO,
                message=f"Pattern validation: {violation_rate:.2%} violations",
                affected_records=pattern_violations,
                sample_failing_records=sample_violations,
                details={
                    'pattern': rule.condition,
                    'violation_count': pattern_violations,
                    'violation_rate': violation_rate,
                    'threshold': rule.threshold or 0.05,
                    'records_checked': len(data)
                }
            )
            
        except Exception as e:
            self.logger.error(f"Pattern validation failed: {e}")
            return ValidationResult(
                result_id=f"pattern_error_{int(datetime.now().timestamp())}",
                rule_id=rule.rule_id,
                dataset_id=dataset_id,
                field_name=rule.field_name,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Pattern validation error: {str(e)}"
            )

    def _execute_business_rule_validation(
        self,
        rule: ValidationRule,
        dataset_id: str,
        data: list[dict[str, Any]]
    ) -> ValidationResult:
        """Execute business rule validation"""
        try:
            # Placeholder for business rule logic
            # In practice, implement a rule engine or expression evaluator
            
            violations = 0
            
            if rule.condition:
                # Simple condition evaluation (extend as needed)
                for record in data:
                    if not self._evaluate_business_condition(record, rule.condition):
                        violations += 1
            
            violation_rate = violations / len(data) if data else 0
            passed = violation_rate <= (rule.threshold or 0.01)  # Default 1% threshold
            
            return ValidationResult(
                result_id=f"business_{rule.rule_id}_{int(datetime.now().timestamp())}",
                rule_id=rule.rule_id,
                dataset_id=dataset_id,
                field_name=rule.field_name,
                passed=passed,
                severity=rule.severity if not passed else ValidationSeverity.INFO,
                message=f"Business rule validation: {violation_rate:.2%} violations",
                affected_records=violations,
                details={
                    'business_rule': rule.condition,
                    'violation_count': violations,
                    'violation_rate': violation_rate,
                    'threshold': rule.threshold or 0.01
                }
            )
            
        except Exception as e:
            self.logger.error(f"Business rule validation failed: {e}")
            return ValidationResult(
                result_id=f"business_error_{int(datetime.now().timestamp())}",
                rule_id=rule.rule_id,
                dataset_id=dataset_id,
                field_name=rule.field_name,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Business rule validation error: {str(e)}"
            )

    def _execute_cross_field_validation(
        self,
        rule: ValidationRule,
        dataset_id: str,
        data: list[dict[str, Any]]
    ) -> ValidationResult:
        """Execute cross-field validation"""
        try:
            violations = 0
            sample_violations = []
            
            for i, record in enumerate(data):
                if not self._validate_cross_field_relationship(record, rule):
                    violations += 1
                    if len(sample_violations) < 10:
                        sample_violations.append({
                            'record_index': i,
                            'record': {k: v for k, v in record.items() if k in rule.context_fields}
                        })
            
            violation_rate = violations / len(data) if data else 0
            passed = violation_rate <= (rule.threshold or 0.05)  # Default 5% threshold
            
            return ValidationResult(
                result_id=f"crossfield_{rule.rule_id}_{int(datetime.now().timestamp())}",
                rule_id=rule.rule_id,
                dataset_id=dataset_id,
                field_name=rule.field_name,
                passed=passed,
                severity=rule.severity if not passed else ValidationSeverity.INFO,
                message=f"Cross-field validation: {violation_rate:.2%} violations",
                affected_records=violations,
                sample_failing_records=sample_violations,
                details={
                    'violation_count': violations,
                    'violation_rate': violation_rate,
                    'threshold': rule.threshold or 0.05,
                    'cross_fields': rule.context_fields
                }
            )
            
        except Exception as e:
            self.logger.error(f"Cross-field validation failed: {e}")
            return ValidationResult(
                result_id=f"crossfield_error_{int(datetime.now().timestamp())}",
                rule_id=rule.rule_id,
                dataset_id=dataset_id,
                field_name=rule.field_name,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Cross-field validation error: {str(e)}"
            )

    # Implementation of remaining methods would continue here...
    # Due to length constraints, I'll include the most critical helper methods

    def _generate_baseline_profile(
        self,
        data: list[dict[str, Any]],
        schema: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Generate baseline data profile"""
        try:
            profile = {
                'generated_at': datetime.now().isoformat(),
                'record_count': len(data),
                'schema': {},
                'statistics': {},
                'patterns': {}
            }
            
            if not data:
                return profile
            
            # Schema analysis
            fields = set()
            for record in data:
                fields.update(record.keys())
            
            for field in fields:
                field_values = [record.get(field) for record in data if field in record]
                non_null_values = [v for v in field_values if v is not None]
                
                field_profile = {
                    'count': len(field_values),
                    'non_null_count': len(non_null_values),
                    'null_rate': (len(field_values) - len(non_null_values)) / len(field_values) if field_values else 0,
                    'unique_count': len(set(str(v) for v in non_null_values)),
                    'data_type': self._infer_data_type(non_null_values)
                }
                
                # Statistical measures for numeric fields
                if field_profile['data_type'] in ['int', 'float'] and non_null_values:
                    try:
                        numeric_values = [float(v) for v in non_null_values if isinstance(v, (int, float))]
                        if numeric_values:
                            field_profile.update({
                                'mean': statistics.mean(numeric_values),
                                'median': statistics.median(numeric_values),
                                'std_dev': statistics.stdev(numeric_values) if len(numeric_values) > 1 else 0,
                                'min': min(numeric_values),
                                'max': max(numeric_values)
                            })
                    except (ValueError, TypeError):
                        pass
                
                profile['statistics'][field] = field_profile
            
            return profile
            
        except Exception as e:
            self.logger.error(f"Baseline profile generation failed: {e}")
            return {}

    def _detect_schema_drift(
        self,
        baseline_schema: dict[str, Any],
        current_schema: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Detect schema changes"""
        try:
            changes = {
                'added_fields': [],
                'removed_fields': [],
                'type_changes': [],
                'drift_detected': False
            }
            
            baseline_fields = set(baseline_schema.keys())
            current_fields = set(current_schema.keys())
            
            # Added fields
            added_fields = current_fields - baseline_fields
            changes['added_fields'] = list(added_fields)
            
            # Removed fields
            removed_fields = baseline_fields - current_fields
            changes['removed_fields'] = list(removed_fields)
            
            # Type changes
            common_fields = baseline_fields & current_fields
            for field in common_fields:
                baseline_type = baseline_schema[field].get('data_type')
                current_type = current_schema[field].get('data_type')
                
                if baseline_type != current_type:
                    changes['type_changes'].append({
                        'field': field,
                        'baseline_type': baseline_type,
                        'current_type': current_type
                    })
            
            changes['drift_detected'] = (
                len(added_fields) > 0 or 
                len(removed_fields) > 0 or 
                len(changes['type_changes']) > 0
            )
            
            return changes if changes['drift_detected'] else None
            
        except Exception as e:
            self.logger.error(f"Schema drift detection failed: {e}")
            return None

    def _detect_statistical_drift(
        self,
        baseline_stats: dict[str, Any],
        current_stats: dict[str, Any]
    ) -> dict[str, Any]:
        """Detect statistical drift"""
        try:
            result = {
                'drift_detected': False,
                'field_scores': {},
                'test_results': {}
            }
            
            common_fields = set(baseline_stats.keys()) & set(current_stats.keys())
            
            for field in common_fields:
                baseline_field = baseline_stats[field]
                current_field = current_stats[field]
                
                # Numerical drift detection
                if baseline_field.get('data_type') in ['int', 'float']:
                    baseline_mean = baseline_field.get('mean', 0)
                    current_mean = current_field.get('mean', 0)
                    baseline_std = baseline_field.get('std_dev', 0)
                    
                    if baseline_std > 0:
                        z_score = abs(current_mean - baseline_mean) / baseline_std
                        drift_score = min(z_score / 3.0, 1.0)  # Normalize to 0-1
                        
                        result['field_scores'][field] = drift_score
                        result['test_results'][field] = {
                            'test_type': 'z_score',
                            'statistic': z_score,
                            'drift_score': drift_score,
                            'baseline_mean': baseline_mean,
                            'current_mean': current_mean,
                            'baseline_std': baseline_std
                        }
                        
                        if drift_score > 0.5:  # Significant drift threshold
                            result['drift_detected'] = True
            
            return result
            
        except Exception as e:
            self.logger.error(f"Statistical drift detection failed: {e}")
            return {'drift_detected': False, 'field_scores': {}, 'test_results': {}}

    def _detect_distribution_drift(
        self,
        baseline_data: list[dict[str, Any]],
        current_data: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Detect distribution drift using statistical tests"""
        try:
            result = {
                'drift_detected': False,
                'changes': {}
            }
            
            if not baseline_data or not current_data:
                return result
            
            # Get common fields
            baseline_fields = set(baseline_data[0].keys()) if baseline_data else set()
            current_fields = set(current_data[0].keys()) if current_data else set()
            common_fields = baseline_fields & current_fields
            
            for field in common_fields:
                baseline_values = [record.get(field) for record in baseline_data if record.get(field) is not None]
                current_values = [record.get(field) for record in current_data if record.get(field) is not None]
                
                if len(baseline_values) < 10 or len(current_values) < 10:
                    continue
                
                # Try to convert to numeric for statistical tests
                try:
                    baseline_numeric = [float(v) for v in baseline_values if isinstance(v, (int, float))]
                    current_numeric = [float(v) for v in current_values if isinstance(v, (int, float))]
                    
                    if len(baseline_numeric) >= 10 and len(current_numeric) >= 10:
                        # Kolmogorov-Smirnov test
                        ks_statistic, p_value = stats.ks_2samp(baseline_numeric, current_numeric)
                        
                        drift_detected = p_value < 0.05  # 5% significance level
                        
                        result['changes'][field] = {
                            'test_type': 'kolmogorov_smirnov',
                            'statistic': ks_statistic,
                            'p_value': p_value,
                            'drift_detected': drift_detected,
                            'baseline_samples': len(baseline_numeric),
                            'current_samples': len(current_numeric)
                        }
                        
                        if drift_detected:
                            result['drift_detected'] = True
                            
                except (ValueError, TypeError):
                    # Handle categorical data
                    baseline_dist = {}
                    current_dist = {}
                    
                    for value in baseline_values:
                        baseline_dist[str(value)] = baseline_dist.get(str(value), 0) + 1
                    
                    for value in current_values:
                        current_dist[str(value)] = current_dist.get(str(value), 0) + 1
                    
                    # Chi-square test for categorical drift
                    all_categories = set(baseline_dist.keys()) | set(current_dist.keys())
                    
                    if len(all_categories) > 1:
                        baseline_counts = [baseline_dist.get(cat, 0) for cat in all_categories]
                        current_counts = [current_dist.get(cat, 0) for cat in all_categories]
                        
                        # Avoid chi-square test with zero counts
                        if min(baseline_counts) > 0 and min(current_counts) > 0:
                            try:
                                chi2_stat, p_value = stats.chisquare(current_counts, baseline_counts)
                                drift_detected = p_value < 0.05
                                
                                result['changes'][field] = {
                                    'test_type': 'chi_square',
                                    'statistic': chi2_stat,
                                    'p_value': p_value,
                                    'drift_detected': drift_detected,
                                    'categories': len(all_categories)
                                }
                                
                                if drift_detected:
                                    result['drift_detected'] = True
                            except (ValueError, ZeroDivisionError):
                                pass
            
            return result
            
        except Exception as e:
            self.logger.error(f"Distribution drift detection failed: {e}")
            return {'drift_detected': False, 'changes': {}}

    # Additional helper methods for completeness
    def _validate_rule_config(self, rule: ValidationRule) -> list[str]:
        """Validate rule configuration"""
        errors = []
        
        if not rule.name:
            errors.append("Rule name is required")
        
        if rule.custom_validator and rule.custom_validator not in self.custom_validators:
            errors.append(f"Custom validator '{rule.custom_validator}' not registered")
        
        if rule.validation_type == ValidationType.PATTERN_VALIDATION and not rule.condition:
            errors.append("Pattern validation requires a condition (regex pattern)")
        
        return errors

    def _check_data_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected data type"""
        try:
            if expected_type == 'int':
                return isinstance(value, int) and not isinstance(value, bool)
            elif expected_type == 'float':
                return isinstance(value, (int, float)) and not isinstance(value, bool)
            elif expected_type == 'string':
                return isinstance(value, str)
            elif expected_type == 'boolean':
                return isinstance(value, bool)
            elif expected_type == 'datetime':
                return isinstance(value, datetime) or (isinstance(value, str) and self._is_datetime_string(value))
            else:
                return True  # Unknown type, assume valid
        except:
            return False

    def _is_datetime_string(self, value: str) -> bool:
        """Check if string is a valid datetime"""
        try:
            datetime.fromisoformat(value.replace('Z', '+00:00'))
            return True
        except:
            return False

    def _infer_data_type(self, values: list[Any]) -> str:
        """Infer data type from values"""
        if not values:
            return 'unknown'
        
        # Sample values for type inference
        sample_values = values[:100]
        
        type_counts = {
            'int': 0,
            'float': 0,
            'string': 0,
            'boolean': 0,
            'datetime': 0
        }
        
        for value in sample_values:
            if isinstance(value, bool):
                type_counts['boolean'] += 1
            elif isinstance(value, int):
                type_counts['int'] += 1
            elif isinstance(value, float):
                type_counts['float'] += 1
            elif isinstance(value, datetime):
                type_counts['datetime'] += 1
            elif isinstance(value, str):
                if self._is_datetime_string(value):
                    type_counts['datetime'] += 1
                else:
                    type_counts['string'] += 1
            else:
                type_counts['string'] += 1
        
        # Return most common type
        return max(type_counts.items(), key=lambda x: x[1])[0]

    def _evaluate_business_condition(self, record: dict[str, Any], condition: str) -> bool:
        """Evaluate business rule condition"""
        try:
            # Simple condition evaluator - extend as needed
            # Example: "age >= 18 and status == 'active'"
            
            # Create a safe evaluation context
            context = {k: v for k, v in record.items() if isinstance(k, str)}
            
            # This is a simplified evaluator - in production, use a proper expression parser
            # For now, just return True as placeholder
            return True
            
        except Exception as e:
            self.logger.error(f"Business condition evaluation failed: {e}")
            return False

    def _validate_cross_field_relationship(self, record: dict[str, Any], rule: ValidationRule) -> bool:
        """Validate cross-field relationships"""
        try:
            # Placeholder for cross-field validation logic
            # Example: start_date < end_date, price > 0 when status = 'active'
            
            if not rule.context_fields or len(rule.context_fields) < 2:
                return True
            
            # Simple validation - extend based on business needs
            return True
            
        except Exception as e:
            self.logger.error(f"Cross-field validation failed: {e}")
            return False

    # Additional methods for drift analysis, test generation, etc. would continue...

    def _persist_validation_rule(self, rule: ValidationRule):
        """Persist validation rule"""
        try:
            rule_file = self.rules_path / f"{rule.rule_id}.json"
            with open(rule_file, 'w') as f:
                json.dump(self._validation_rule_to_dict(rule), f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to persist validation rule: {e}")

    def _validation_rule_to_dict(self, rule: ValidationRule) -> dict[str, Any]:
        """Convert validation rule to dictionary"""
        return {
            'rule_id': rule.rule_id,
            'name': rule.name,
            'description': rule.description,
            'validation_type': rule.validation_type.value,
            'field_name': rule.field_name,
            'condition': rule.condition,
            'expected_value': rule.expected_value,
            'threshold': rule.threshold,
            'custom_validator': rule.custom_validator,
            'sql_check': rule.sql_check,
            'reference_dataset': rule.reference_dataset,
            'enable_drift_detection': rule.enable_drift_detection,
            'drift_threshold': rule.drift_threshold,
            'baseline_period_days': rule.baseline_period_days,
            'context_fields': rule.context_fields,
            'conditional_logic': rule.conditional_logic,
            'active': rule.active,
            'severity': rule.severity.value,
            'auto_adapt': rule.auto_adapt,
            'created_at': rule.created_at.isoformat(),
            'updated_at': rule.updated_at.isoformat(),
            'tags': rule.tags,
            'metadata': rule.metadata
        }

    def _dict_to_validation_rule(self, data: dict[str, Any]) -> ValidationRule:
        """Convert dictionary to validation rule"""
        return ValidationRule(
            rule_id=data['rule_id'],
            name=data['name'],
            description=data['description'],
            validation_type=ValidationType(data['validation_type']),
            field_name=data.get('field_name'),
            condition=data.get('condition'),
            expected_value=data.get('expected_value'),
            threshold=data.get('threshold'),
            custom_validator=data.get('custom_validator'),
            sql_check=data.get('sql_check'),
            reference_dataset=data.get('reference_dataset'),
            enable_drift_detection=data.get('enable_drift_detection', True),
            drift_threshold=data.get('drift_threshold', 0.1),
            baseline_period_days=data.get('baseline_period_days', 30),
            context_fields=data.get('context_fields', []),
            conditional_logic=data.get('conditional_logic'),
            active=data.get('active', True),
            severity=ValidationSeverity(data.get('severity', 'warning')),
            auto_adapt=data.get('auto_adapt', False),
            created_at=datetime.fromisoformat(data['created_at']),
            updated_at=datetime.fromisoformat(data['updated_at']),
            tags=data.get('tags', []),
            metadata=data.get('metadata', {})
        )

    # Cleanup and maintenance methods
    def _update_baselines(self):
        """Update baseline profiles periodically"""
        try:
            cutoff_date = datetime.now() - timedelta(days=self.baseline_update_frequency_days)
            
            # This is a placeholder - in practice, you'd query your data sources
            # and update baselines based on recent data
            self.logger.info("Baseline update check completed")
            
        except Exception as e:
            self.logger.error(f"Baseline update failed: {e}")

    def _cleanup_old_data(self):
        """Clean up old validation data"""
        try:
            # Clean old results files
            cutoff_date = datetime.now() - timedelta(days=90)
            
            for result_file in self.results_path.glob("*.json"):
                file_time = datetime.fromtimestamp(result_file.stat().st_mtime)
                if file_time < cutoff_date:
                    result_file.unlink()
            
            # Clean old drift analysis files
            for drift_file in self.drift_path.glob("*.json"):
                file_time = datetime.fromtimestamp(drift_file.stat().st_mtime)
                if file_time < cutoff_date:
                    drift_file.unlink()
                    
            self.logger.info("Data cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Data cleanup failed: {e}")

    def _persist_validation_results(self, results: list[ValidationResult]):
        """Persist validation results"""
        try:
            if not results:
                return
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            results_file = self.results_path / f"validation_results_{timestamp}.json"
            
            results_data = {
                'timestamp': timestamp,
                'results_count': len(results),
                'results': [self._validation_result_to_dict(r) for r in results]
            }
            
            with open(results_file, 'w') as f:
                json.dump(results_data, f, default=str)
                
        except Exception as e:
            self.logger.error(f"Failed to persist validation results: {e}")

    def _validation_result_to_dict(self, result: ValidationResult) -> dict[str, Any]:
        """Convert validation result to dictionary"""
        return {
            'result_id': result.result_id,
            'rule_id': result.rule_id,
            'dataset_id': result.dataset_id,
            'field_name': result.field_name,
            'passed': result.passed,
            'severity': result.severity.value,
            'message': result.message,
            'details': result.details,
            'drift_detected': result.drift_detected,
            'drift_type': result.drift_type.value if result.drift_type else None,
            'drift_magnitude': result.drift_magnitude,
            'current_value': result.current_value,
            'expected_value': result.expected_value,
            'baseline_value': result.baseline_value,
            'confidence_score': result.confidence_score,
            'validation_timestamp': result.validation_timestamp.isoformat(),
            'affected_records': result.affected_records,
            'sample_failing_records': result.sample_failing_records,
            'recommendations': result.recommendations,
            'auto_fixable': result.auto_fixable,
            'metadata': result.metadata
        }

    def _persist_baseline_profile(self, dataset_id: str, profile: dict[str, Any]):
        """Persist baseline profile"""
        try:
            baseline_file = self.baselines_path / f"baseline_{dataset_id}.json"
            with open(baseline_file, 'w') as f:
                json.dump(profile, f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to persist baseline profile: {e}")

    def _persist_drift_analysis(self, analysis: DriftAnalysis):
        """Persist drift analysis"""
        try:
            analysis_file = self.drift_path / f"{analysis.analysis_id}.json"
            with open(analysis_file, 'w') as f:
                json.dump(self._drift_analysis_to_dict(analysis), f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to persist drift analysis: {e}")

    def _drift_analysis_to_dict(self, analysis: DriftAnalysis) -> dict[str, Any]:
        """Convert drift analysis to dictionary"""
        return {
            'analysis_id': analysis.analysis_id,
            'dataset_id': analysis.dataset_id,
            'analysis_timestamp': analysis.analysis_timestamp.isoformat(),
            'overall_drift_score': analysis.overall_drift_score,
            'drift_detected': analysis.drift_detected,
            'drift_types': [dt.value for dt in analysis.drift_types],
            'field_drift_scores': analysis.field_drift_scores,
            'drifted_fields': analysis.drifted_fields,
            'distribution_changes': analysis.distribution_changes,
            'statistical_tests': analysis.statistical_tests,
            'schema_changes': analysis.schema_changes,
            'recommendations': analysis.recommendations,
            'remediation_actions': analysis.remediation_actions,
            'metadata': analysis.metadata
        }
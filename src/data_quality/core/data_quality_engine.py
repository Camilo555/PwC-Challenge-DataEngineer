"""
Intelligent Data Quality Engine
Core orchestrator for data quality assessment, monitoring, and remediation.
"""
from __future__ import annotations

import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from core.config import get_settings
from core.logging import get_logger
from monitoring.advanced_metrics import get_metrics_collector
from .anomaly_detection import MLAnomalyDetector
from .profiler import DataProfiler
from .statistical_analyzer import StatisticalAnalyzer


class QualityLevel(Enum):
    """Data quality assessment levels"""
    EXCELLENT = "excellent"  # > 95%
    GOOD = "good"           # 85-95%
    ACCEPTABLE = "acceptable" # 70-85%
    POOR = "poor"           # 50-70%
    CRITICAL = "critical"    # < 50%


class QualityDimension(Enum):
    """Comprehensive data quality dimensions"""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    RELIABILITY = "reliability"
    RELEVANCE = "relevance"
    CONFORMITY = "conformity"


@dataclass
class QualityRule:
    """Data quality rule definition"""
    rule_id: str
    dimension: QualityDimension
    description: str
    condition: str
    threshold: float
    weight: float = 1.0
    critical: bool = False
    active: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityIssue:
    """Data quality issue detected"""
    issue_id: str
    rule_id: str
    dimension: QualityDimension
    severity: str
    description: str
    affected_records: int
    detected_at: datetime
    location: str | None = None
    sample_data: list[dict[str, Any]] = field(default_factory=list)
    remediation_suggestion: str | None = None
    auto_fixable: bool = False


@dataclass
class QualityMetrics:
    """Comprehensive quality metrics"""
    dataset_id: str
    timestamp: datetime
    overall_score: float
    dimension_scores: dict[QualityDimension, float] = field(default_factory=dict)
    total_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    issues_count: int = 0
    critical_issues: int = 0
    quality_level: QualityLevel = QualityLevel.CRITICAL
    execution_time_ms: float = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityReport:
    """Comprehensive data quality report"""
    report_id: str
    dataset_id: str
    timestamp: datetime
    metrics: QualityMetrics
    issues: list[QualityIssue] = field(default_factory=list)
    rules_applied: list[QualityRule] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)
    trend_analysis: dict[str, Any] = field(default_factory=dict)
    data_profile: dict[str, Any] = field(default_factory=dict)
    anomalies: list[dict[str, Any]] = field(default_factory=list)


class DataQualityEngine:
    """
    Intelligent Data Quality Engine
    
    Provides comprehensive data quality assessment with:
    - ML-powered anomaly detection
    - Statistical analysis and profiling
    - Configurable quality rules
    - Automated issue detection and remediation
    - Trend analysis and scoring
    """

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self.settings = get_settings()
        self.logger = get_logger(self.__class__.__name__)
        self.metrics_collector = get_metrics_collector()
        
        # Initialize components
        self.profiler = DataProfiler()
        self.anomaly_detector = MLAnomalyDetector()
        self.statistical_analyzer = StatisticalAnalyzer()
        
        # Quality rules and configuration
        self.quality_rules: dict[str, QualityRule] = {}
        self.quality_history: dict[str, list[QualityMetrics]] = {}
        
        # Threading and async support
        self.executor = ThreadPoolExecutor(max_workers=4)
        self._lock = threading.Lock()
        
        # Load default rules
        self._load_default_rules()
        
        # Initialize storage paths
        self.reports_path = Path("./data_quality/reports")
        self.cache_path = Path("./data_quality/cache")
        self._ensure_directories()

    def _ensure_directories(self):
        """Ensure required directories exist"""
        for path in [self.reports_path, self.cache_path]:
            path.mkdir(parents=True, exist_ok=True)

    def _load_default_rules(self):
        """Load default data quality rules"""
        default_rules = [
            # Completeness Rules
            QualityRule(
                rule_id="completeness_required_fields",
                dimension=QualityDimension.COMPLETENESS,
                description="Required fields must be present",
                condition="null_percentage < 0.05",
                threshold=0.95,
                weight=2.0,
                critical=True
            ),
            QualityRule(
                rule_id="completeness_optional_fields",
                dimension=QualityDimension.COMPLETENESS,
                description="Optional fields completeness check",
                condition="null_percentage < 0.20",
                threshold=0.80,
                weight=1.0
            ),
            
            # Accuracy Rules
            QualityRule(
                rule_id="accuracy_data_types",
                dimension=QualityDimension.ACCURACY,
                description="Data types must match schema",
                condition="type_mismatch_percentage < 0.02",
                threshold=0.98,
                weight=1.5,
                critical=True
            ),
            QualityRule(
                rule_id="accuracy_format_compliance",
                dimension=QualityDimension.ACCURACY,
                description="Data must comply with expected formats",
                condition="format_error_percentage < 0.05",
                threshold=0.95,
                weight=1.5
            ),
            
            # Consistency Rules  
            QualityRule(
                rule_id="consistency_referential_integrity",
                dimension=QualityDimension.CONSISTENCY,
                description="Foreign key relationships must be valid",
                condition="referential_violation_percentage < 0.01",
                threshold=0.99,
                weight=2.0,
                critical=True
            ),
            QualityRule(
                rule_id="consistency_cross_field",
                dimension=QualityDimension.CONSISTENCY,
                description="Related fields must be consistent",
                condition="cross_field_inconsistency < 0.02",
                threshold=0.98,
                weight=1.5
            ),
            
            # Uniqueness Rules
            QualityRule(
                rule_id="uniqueness_primary_key",
                dimension=QualityDimension.UNIQUENESS,
                description="Primary key must be unique",
                condition="duplicate_percentage == 0.0",
                threshold=1.0,
                weight=2.0,
                critical=True
            ),
            QualityRule(
                rule_id="uniqueness_business_key",
                dimension=QualityDimension.UNIQUENESS,
                description="Business key should have minimal duplicates",
                condition="duplicate_percentage < 0.01",
                threshold=0.99,
                weight=1.5
            ),
            
            # Validity Rules
            QualityRule(
                rule_id="validity_range_checks",
                dimension=QualityDimension.VALIDITY,
                description="Numeric values must be within valid ranges",
                condition="out_of_range_percentage < 0.02",
                threshold=0.98,
                weight=1.5
            ),
            QualityRule(
                rule_id="validity_enumeration",
                dimension=QualityDimension.VALIDITY,
                description="Categorical values must be from valid set",
                condition="invalid_category_percentage < 0.01",
                threshold=0.99,
                weight=1.5
            ),
            
            # Timeliness Rules
            QualityRule(
                rule_id="timeliness_freshness",
                dimension=QualityDimension.TIMELINESS,
                description="Data must be within acceptable age",
                condition="stale_data_percentage < 0.10",
                threshold=0.90,
                weight=1.0
            ),
            QualityRule(
                rule_id="timeliness_future_dates",
                dimension=QualityDimension.TIMELINESS,
                description="No unrealistic future dates",
                condition="future_date_percentage < 0.01",
                threshold=0.99,
                weight=1.5
            )
        ]
        
        for rule in default_rules:
            self.quality_rules[rule.rule_id] = rule

    async def assess_quality(
        self, 
        data: list[dict[str, Any]] | dict[str, Any],
        dataset_id: str,
        schema: dict[str, Any] | None = None,
        rules: list[str] | None = None
    ) -> QualityReport:
        """
        Comprehensive data quality assessment
        
        Args:
            data: Data to assess (list of records or single dataset)
            dataset_id: Unique identifier for the dataset
            schema: Optional schema definition
            rules: Optional list of rule IDs to apply (default: all active rules)
        
        Returns:
            Comprehensive quality report
        """
        start_time = datetime.now()
        
        try:
            # Normalize data input
            if isinstance(data, dict):
                data = [data]
            
            # Generate profile
            profile = await self._generate_data_profile(data, schema)
            
            # Detect anomalies
            anomalies = await self._detect_anomalies(data, profile)
            
            # Apply quality rules
            issues = await self._apply_quality_rules(data, profile, rules)
            
            # Calculate metrics
            metrics = self._calculate_quality_metrics(
                dataset_id, data, profile, issues, start_time
            )
            
            # Generate recommendations
            recommendations = self._generate_recommendations(issues, profile, anomalies)
            
            # Perform trend analysis
            trend_analysis = self._analyze_trends(dataset_id, metrics)
            
            # Create report
            report = QualityReport(
                report_id=f"{dataset_id}_{int(start_time.timestamp())}",
                dataset_id=dataset_id,
                timestamp=start_time,
                metrics=metrics,
                issues=issues,
                rules_applied=[r for r in self.quality_rules.values() if r.active],
                recommendations=recommendations,
                trend_analysis=trend_analysis,
                data_profile=profile,
                anomalies=anomalies
            )
            
            # Store report and update history
            await self._store_report(report)
            self._update_quality_history(dataset_id, metrics)
            
            # Record metrics
            self._record_assessment_metrics(report)
            
            return report
            
        except Exception as e:
            self.logger.error(f"Quality assessment failed for {dataset_id}: {e}", exc_info=True)
            raise

    async def _generate_data_profile(
        self, 
        data: list[dict[str, Any]], 
        schema: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Generate comprehensive data profile"""
        try:
            loop = asyncio.get_event_loop()
            profile = await loop.run_in_executor(
                self.executor,
                self.profiler.profile_data,
                data,
                schema
            )
            return profile
        except Exception as e:
            self.logger.error(f"Data profiling failed: {e}")
            return {}

    async def _detect_anomalies(
        self, 
        data: list[dict[str, Any]], 
        profile: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Detect anomalies using ML algorithms"""
        try:
            loop = asyncio.get_event_loop()
            anomalies = await loop.run_in_executor(
                self.executor,
                self.anomaly_detector.detect_anomalies,
                data,
                profile
            )
            return anomalies
        except Exception as e:
            self.logger.error(f"Anomaly detection failed: {e}")
            return []

    async def _apply_quality_rules(
        self,
        data: list[dict[str, Any]],
        profile: dict[str, Any],
        rule_ids: list[str] | None = None
    ) -> list[QualityIssue]:
        """Apply quality rules and detect issues"""
        issues = []
        
        # Determine which rules to apply
        rules_to_apply = []
        if rule_ids:
            rules_to_apply = [
                rule for rule_id, rule in self.quality_rules.items()
                if rule_id in rule_ids and rule.active
            ]
        else:
            rules_to_apply = [rule for rule in self.quality_rules.values() if rule.active]
        
        # Apply rules concurrently
        tasks = []
        for rule in rules_to_apply:
            task = asyncio.create_task(
                self._apply_single_rule(rule, data, profile)
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, list):
                issues.extend(result)
            elif isinstance(result, Exception):
                self.logger.error(f"Rule application failed: {result}")
        
        return issues

    async def _apply_single_rule(
        self,
        rule: QualityRule,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[QualityIssue]:
        """Apply a single quality rule"""
        try:
            loop = asyncio.get_event_loop()
            
            # Execute rule evaluation in thread pool
            evaluation_result = await loop.run_in_executor(
                self.executor,
                self._evaluate_rule,
                rule,
                data,
                profile
            )
            
            return evaluation_result
        except Exception as e:
            self.logger.error(f"Failed to apply rule {rule.rule_id}: {e}")
            return []

    def _evaluate_rule(
        self,
        rule: QualityRule,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[QualityIssue]:
        """Evaluate a quality rule and return issues"""
        issues = []
        
        try:
            # Rule evaluation logic based on dimension
            if rule.dimension == QualityDimension.COMPLETENESS:
                issues.extend(self._evaluate_completeness_rule(rule, data, profile))
            elif rule.dimension == QualityDimension.ACCURACY:
                issues.extend(self._evaluate_accuracy_rule(rule, data, profile))
            elif rule.dimension == QualityDimension.CONSISTENCY:
                issues.extend(self._evaluate_consistency_rule(rule, data, profile))
            elif rule.dimension == QualityDimension.UNIQUENESS:
                issues.extend(self._evaluate_uniqueness_rule(rule, data, profile))
            elif rule.dimension == QualityDimension.VALIDITY:
                issues.extend(self._evaluate_validity_rule(rule, data, profile))
            elif rule.dimension == QualityDimension.TIMELINESS:
                issues.extend(self._evaluate_timeliness_rule(rule, data, profile))
                
        except Exception as e:
            self.logger.error(f"Rule evaluation failed for {rule.rule_id}: {e}")
        
        return issues

    def _evaluate_completeness_rule(
        self, 
        rule: QualityRule,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[QualityIssue]:
        """Evaluate completeness rules"""
        issues = []
        
        field_stats = profile.get('fields', {})
        
        for field_name, stats in field_stats.items():
            null_percentage = stats.get('null_percentage', 0)
            
            # Apply rule threshold
            if rule.rule_id == "completeness_required_fields":
                if stats.get('required', False) and null_percentage > (1 - rule.threshold):
                    issues.append(QualityIssue(
                        issue_id=f"completeness_{field_name}_{int(datetime.now().timestamp())}",
                        rule_id=rule.rule_id,
                        dimension=rule.dimension,
                        severity="critical" if rule.critical else "high",
                        description=f"Field '{field_name}' has {null_percentage:.2%} missing values",
                        affected_records=int(stats.get('null_count', 0)),
                        detected_at=datetime.now(),
                        location=field_name,
                        remediation_suggestion=f"Implement data imputation or validation for {field_name}"
                    ))
            
            elif rule.rule_id == "completeness_optional_fields":
                if not stats.get('required', False) and null_percentage > (1 - rule.threshold):
                    issues.append(QualityIssue(
                        issue_id=f"completeness_optional_{field_name}_{int(datetime.now().timestamp())}",
                        rule_id=rule.rule_id,
                        dimension=rule.dimension,
                        severity="medium",
                        description=f"Optional field '{field_name}' has high missing rate: {null_percentage:.2%}",
                        affected_records=int(stats.get('null_count', 0)),
                        detected_at=datetime.now(),
                        location=field_name,
                        remediation_suggestion=f"Consider reviewing data collection for {field_name}"
                    ))
        
        return issues

    def _evaluate_accuracy_rule(
        self,
        rule: QualityRule,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[QualityIssue]:
        """Evaluate accuracy rules"""
        issues = []
        
        field_stats = profile.get('fields', {})
        
        for field_name, stats in field_stats.items():
            # Type accuracy
            if rule.rule_id == "accuracy_data_types":
                type_error_rate = stats.get('type_error_rate', 0)
                if type_error_rate > (1 - rule.threshold):
                    issues.append(QualityIssue(
                        issue_id=f"accuracy_type_{field_name}_{int(datetime.now().timestamp())}",
                        rule_id=rule.rule_id,
                        dimension=rule.dimension,
                        severity="critical" if rule.critical else "high",
                        description=f"Field '{field_name}' has {type_error_rate:.2%} type mismatches",
                        affected_records=int(stats.get('type_error_count', 0)),
                        detected_at=datetime.now(),
                        location=field_name,
                        remediation_suggestion=f"Review data types and casting logic for {field_name}"
                    ))
            
            # Format accuracy
            elif rule.rule_id == "accuracy_format_compliance":
                format_error_rate = stats.get('format_error_rate', 0)
                if format_error_rate > (1 - rule.threshold):
                    issues.append(QualityIssue(
                        issue_id=f"accuracy_format_{field_name}_{int(datetime.now().timestamp())}",
                        rule_id=rule.rule_id,
                        dimension=rule.dimension,
                        severity="high",
                        description=f"Field '{field_name}' has {format_error_rate:.2%} format violations",
                        affected_records=int(stats.get('format_error_count', 0)),
                        detected_at=datetime.now(),
                        location=field_name,
                        remediation_suggestion=f"Implement format validation for {field_name}"
                    ))
        
        return issues

    def _evaluate_consistency_rule(
        self,
        rule: QualityRule,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[QualityIssue]:
        """Evaluate consistency rules"""
        issues = []
        
        consistency_stats = profile.get('consistency', {})
        
        if rule.rule_id == "consistency_referential_integrity":
            ref_violations = consistency_stats.get('referential_violations', 0)
            total_records = len(data)
            violation_rate = ref_violations / total_records if total_records > 0 else 0
            
            if violation_rate > (1 - rule.threshold):
                issues.append(QualityIssue(
                    issue_id=f"consistency_ref_{int(datetime.now().timestamp())}",
                    rule_id=rule.rule_id,
                    dimension=rule.dimension,
                    severity="critical" if rule.critical else "high",
                    description=f"Referential integrity violations: {violation_rate:.2%}",
                    affected_records=ref_violations,
                    detected_at=datetime.now(),
                    remediation_suggestion="Review foreign key relationships and data integration logic"
                ))
        
        elif rule.rule_id == "consistency_cross_field":
            cross_field_issues = consistency_stats.get('cross_field_inconsistencies', 0)
            total_records = len(data)
            inconsistency_rate = cross_field_issues / total_records if total_records > 0 else 0
            
            if inconsistency_rate > (1 - rule.threshold):
                issues.append(QualityIssue(
                    issue_id=f"consistency_cross_{int(datetime.now().timestamp())}",
                    rule_id=rule.rule_id,
                    dimension=rule.dimension,
                    severity="medium",
                    description=f"Cross-field inconsistencies: {inconsistency_rate:.2%}",
                    affected_records=cross_field_issues,
                    detected_at=datetime.now(),
                    remediation_suggestion="Review business rules for field relationships"
                ))
        
        return issues

    def _evaluate_uniqueness_rule(
        self,
        rule: QualityRule,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[QualityIssue]:
        """Evaluate uniqueness rules"""
        issues = []
        
        uniqueness_stats = profile.get('uniqueness', {})
        
        for key_type, stats in uniqueness_stats.items():
            duplicate_rate = stats.get('duplicate_rate', 0)
            
            if rule.rule_id == "uniqueness_primary_key" and key_type == "primary_key":
                if duplicate_rate > 0:
                    issues.append(QualityIssue(
                        issue_id=f"uniqueness_pk_{int(datetime.now().timestamp())}",
                        rule_id=rule.rule_id,
                        dimension=rule.dimension,
                        severity="critical",
                        description=f"Primary key violations: {duplicate_rate:.2%}",
                        affected_records=int(stats.get('duplicate_count', 0)),
                        detected_at=datetime.now(),
                        remediation_suggestion="Remove or merge duplicate primary key records",
                        auto_fixable=True
                    ))
            
            elif rule.rule_id == "uniqueness_business_key" and key_type == "business_key":
                if duplicate_rate > (1 - rule.threshold):
                    issues.append(QualityIssue(
                        issue_id=f"uniqueness_bk_{int(datetime.now().timestamp())}",
                        rule_id=rule.rule_id,
                        dimension=rule.dimension,
                        severity="high",
                        description=f"Business key duplicates: {duplicate_rate:.2%}",
                        affected_records=int(stats.get('duplicate_count', 0)),
                        detected_at=datetime.now(),
                        remediation_suggestion="Review business key definition and deduplication logic"
                    ))
        
        return issues

    def _evaluate_validity_rule(
        self,
        rule: QualityRule,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[QualityIssue]:
        """Evaluate validity rules"""
        issues = []
        
        field_stats = profile.get('fields', {})
        
        for field_name, stats in field_stats.items():
            if rule.rule_id == "validity_range_checks":
                out_of_range_rate = stats.get('out_of_range_rate', 0)
                if out_of_range_rate > (1 - rule.threshold):
                    issues.append(QualityIssue(
                        issue_id=f"validity_range_{field_name}_{int(datetime.now().timestamp())}",
                        rule_id=rule.rule_id,
                        dimension=rule.dimension,
                        severity="high",
                        description=f"Field '{field_name}' has {out_of_range_rate:.2%} out-of-range values",
                        affected_records=int(stats.get('out_of_range_count', 0)),
                        detected_at=datetime.now(),
                        location=field_name,
                        remediation_suggestion=f"Review valid ranges for {field_name}"
                    ))
            
            elif rule.rule_id == "validity_enumeration":
                invalid_category_rate = stats.get('invalid_category_rate', 0)
                if invalid_category_rate > (1 - rule.threshold):
                    issues.append(QualityIssue(
                        issue_id=f"validity_enum_{field_name}_{int(datetime.now().timestamp())}",
                        rule_id=rule.rule_id,
                        dimension=rule.dimension,
                        severity="medium",
                        description=f"Field '{field_name}' has {invalid_category_rate:.2%} invalid categories",
                        affected_records=int(stats.get('invalid_category_count', 0)),
                        detected_at=datetime.now(),
                        location=field_name,
                        remediation_suggestion=f"Review valid categories for {field_name}"
                    ))
        
        return issues

    def _evaluate_timeliness_rule(
        self,
        rule: QualityRule,
        data: list[dict[str, Any]],
        profile: dict[str, Any]
    ) -> list[QualityIssue]:
        """Evaluate timeliness rules"""
        issues = []
        
        timeliness_stats = profile.get('timeliness', {})
        
        if rule.rule_id == "timeliness_freshness":
            stale_rate = timeliness_stats.get('stale_data_rate', 0)
            if stale_rate > (1 - rule.threshold):
                issues.append(QualityIssue(
                    issue_id=f"timeliness_stale_{int(datetime.now().timestamp())}",
                    rule_id=rule.rule_id,
                    dimension=rule.dimension,
                    severity="medium",
                    description=f"Stale data rate: {stale_rate:.2%}",
                    affected_records=int(timeliness_stats.get('stale_records', 0)),
                    detected_at=datetime.now(),
                    remediation_suggestion="Review data refresh schedules and ETL pipelines"
                ))
        
        elif rule.rule_id == "timeliness_future_dates":
            future_date_rate = timeliness_stats.get('future_date_rate', 0)
            if future_date_rate > (1 - rule.threshold):
                issues.append(QualityIssue(
                    issue_id=f"timeliness_future_{int(datetime.now().timestamp())}",
                    rule_id=rule.rule_id,
                    dimension=rule.dimension,
                    severity="high",
                    description=f"Future date rate: {future_date_rate:.2%}",
                    affected_records=int(timeliness_stats.get('future_date_records', 0)),
                    detected_at=datetime.now(),
                    remediation_suggestion="Review date validation logic and data entry processes"
                ))
        
        return issues

    def _calculate_quality_metrics(
        self,
        dataset_id: str,
        data: list[dict[str, Any]],
        profile: dict[str, Any],
        issues: list[QualityIssue],
        start_time: datetime
    ) -> QualityMetrics:
        """Calculate comprehensive quality metrics"""
        
        total_records = len(data)
        critical_issues = sum(1 for issue in issues if issue.severity == "critical")
        
        # Calculate dimension scores
        dimension_scores = {}
        for dimension in QualityDimension:
            dimension_issues = [i for i in issues if i.dimension == dimension]
            if not dimension_issues:
                dimension_scores[dimension] = 1.0
            else:
                # Weight issues by severity
                severity_weights = {"critical": 1.0, "high": 0.8, "medium": 0.5, "low": 0.2}
                total_impact = sum(severity_weights.get(i.severity, 0.5) for i in dimension_issues)
                max_possible_impact = len(dimension_issues) * 1.0
                dimension_scores[dimension] = max(0.0, 1.0 - (total_impact / max_possible_impact))
        
        # Calculate overall score using weighted dimensions
        dimension_weights = {
            QualityDimension.COMPLETENESS: 0.20,
            QualityDimension.ACCURACY: 0.20,
            QualityDimension.CONSISTENCY: 0.15,
            QualityDimension.UNIQUENESS: 0.15,
            QualityDimension.VALIDITY: 0.15,
            QualityDimension.TIMELINESS: 0.10,
            QualityDimension.RELIABILITY: 0.03,
            QualityDimension.RELEVANCE: 0.01,
            QualityDimension.CONFORMITY: 0.01
        }
        
        overall_score = sum(
            dimension_scores.get(dim, 0.5) * weight
            for dim, weight in dimension_weights.items()
        )
        
        # Determine quality level
        if overall_score >= 0.95:
            quality_level = QualityLevel.EXCELLENT
        elif overall_score >= 0.85:
            quality_level = QualityLevel.GOOD
        elif overall_score >= 0.70:
            quality_level = QualityLevel.ACCEPTABLE
        elif overall_score >= 0.50:
            quality_level = QualityLevel.POOR
        else:
            quality_level = QualityLevel.CRITICAL
        
        execution_time = (datetime.now() - start_time).total_seconds() * 1000
        
        return QualityMetrics(
            dataset_id=dataset_id,
            timestamp=datetime.now(),
            overall_score=overall_score,
            dimension_scores=dimension_scores,
            total_records=total_records,
            processed_records=total_records,
            failed_records=len([i for i in issues if i.severity in ["critical", "high"]]),
            issues_count=len(issues),
            critical_issues=critical_issues,
            quality_level=quality_level,
            execution_time_ms=execution_time,
            metadata={
                'profile_summary': profile.get('summary', {}),
                'rules_applied': len(self.quality_rules),
                'anomalies_detected': len(profile.get('anomalies', []))
            }
        )

    def _generate_recommendations(
        self,
        issues: list[QualityIssue],
        profile: dict[str, Any],
        anomalies: list[dict[str, Any]]
    ) -> list[str]:
        """Generate actionable recommendations"""
        recommendations = []
        
        # Priority recommendations based on critical issues
        critical_issues = [i for i in issues if i.severity == "critical"]
        if critical_issues:
            recommendations.append(
                f"URGENT: Address {len(critical_issues)} critical data quality issues immediately"
            )
        
        # Dimension-specific recommendations
        dimension_issues = {}
        for issue in issues:
            if issue.dimension not in dimension_issues:
                dimension_issues[issue.dimension] = []
            dimension_issues[issue.dimension].append(issue)
        
        for dimension, dim_issues in dimension_issues.items():
            if len(dim_issues) > 3:
                recommendations.append(
                    f"Focus on {dimension.value} improvements - {len(dim_issues)} issues detected"
                )
        
        # Anomaly-based recommendations
        if anomalies:
            recommendations.append(
                f"Investigate {len(anomalies)} statistical anomalies detected"
            )
        
        # Profile-based recommendations
        field_stats = profile.get('fields', {})
        high_null_fields = [
            name for name, stats in field_stats.items()
            if stats.get('null_percentage', 0) > 0.20
        ]
        
        if high_null_fields:
            recommendations.append(
                f"Implement data collection improvements for fields: {', '.join(high_null_fields[:3])}"
            )
        
        # Auto-fixable issues
        auto_fixable = [i for i in issues if i.auto_fixable]
        if auto_fixable:
            recommendations.append(
                f"Consider automated remediation for {len(auto_fixable)} auto-fixable issues"
            )
        
        return recommendations[:10]  # Limit to top 10 recommendations

    def _analyze_trends(self, dataset_id: str, current_metrics: QualityMetrics) -> dict[str, Any]:
        """Analyze quality trends over time"""
        with self._lock:
            history = self.quality_history.get(dataset_id, [])
            
        if len(history) < 2:
            return {"message": "Insufficient historical data for trend analysis"}
        
        # Calculate trends
        recent_scores = [m.overall_score for m in history[-5:]]  # Last 5 assessments
        score_trend = "improving" if recent_scores[-1] > recent_scores[0] else "declining"
        
        avg_score = sum(recent_scores) / len(recent_scores)
        score_variance = sum((s - avg_score) ** 2 for s in recent_scores) / len(recent_scores)
        
        return {
            "score_trend": score_trend,
            "average_score": avg_score,
            "score_variance": score_variance,
            "assessments_count": len(history),
            "last_assessment": history[-1].timestamp.isoformat() if history else None,
            "dimension_trends": {
                dim.value: [
                    m.dimension_scores.get(dim, 0.5) for m in history[-5:]
                ] for dim in QualityDimension
            }
        }

    async def _store_report(self, report: QualityReport):
        """Store quality report"""
        try:
            report_file = self.reports_path / f"{report.report_id}.json"
            
            # Convert to JSON-serializable format
            report_data = {
                "report_id": report.report_id,
                "dataset_id": report.dataset_id,
                "timestamp": report.timestamp.isoformat(),
                "metrics": {
                    "overall_score": report.metrics.overall_score,
                    "quality_level": report.metrics.quality_level.value,
                    "total_records": report.metrics.total_records,
                    "issues_count": report.metrics.issues_count,
                    "critical_issues": report.metrics.critical_issues,
                    "execution_time_ms": report.metrics.execution_time_ms,
                    "dimension_scores": {
                        dim.value: score for dim, score in report.metrics.dimension_scores.items()
                    }
                },
                "issues": [
                    {
                        "issue_id": issue.issue_id,
                        "rule_id": issue.rule_id,
                        "dimension": issue.dimension.value,
                        "severity": issue.severity,
                        "description": issue.description,
                        "affected_records": issue.affected_records,
                        "location": issue.location,
                        "auto_fixable": issue.auto_fixable
                    } for issue in report.issues
                ],
                "recommendations": report.recommendations,
                "trend_analysis": report.trend_analysis,
                "anomalies_count": len(report.anomalies)
            }
            
            with open(report_file, 'w') as f:
                json.dump(report_data, f, indent=2)
                
            self.logger.info(f"Quality report stored: {report_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to store quality report: {e}")

    def _update_quality_history(self, dataset_id: str, metrics: QualityMetrics):
        """Update quality history for trend analysis"""
        with self._lock:
            if dataset_id not in self.quality_history:
                self.quality_history[dataset_id] = []
            
            self.quality_history[dataset_id].append(metrics)
            
            # Keep only last 50 assessments
            if len(self.quality_history[dataset_id]) > 50:
                self.quality_history[dataset_id] = self.quality_history[dataset_id][-50:]

    def _record_assessment_metrics(self, report: QualityReport):
        """Record assessment metrics for monitoring"""
        try:
            labels = {
                "dataset_id": report.dataset_id,
                "quality_level": report.metrics.quality_level.value
            }
            
            self.metrics_collector.set_gauge(
                "data_quality_overall_score",
                report.metrics.overall_score * 100,
                labels
            )
            
            self.metrics_collector.increment_counter(
                "data_quality_assessments_total",
                1,
                labels
            )
            
            self.metrics_collector.set_gauge(
                "data_quality_issues_count",
                report.metrics.issues_count,
                labels
            )
            
            self.metrics_collector.set_gauge(
                "data_quality_critical_issues",
                report.metrics.critical_issues,
                labels
            )
            
            self.metrics_collector.record_histogram(
                "data_quality_assessment_duration_ms",
                report.metrics.execution_time_ms,
                labels
            )
            
        except Exception as e:
            self.logger.error(f"Failed to record assessment metrics: {e}")

    # Configuration and management methods
    def add_quality_rule(self, rule: QualityRule):
        """Add a custom quality rule"""
        self.quality_rules[rule.rule_id] = rule
        self.logger.info(f"Added quality rule: {rule.rule_id}")

    def remove_quality_rule(self, rule_id: str):
        """Remove a quality rule"""
        if rule_id in self.quality_rules:
            del self.quality_rules[rule_id]
            self.logger.info(f"Removed quality rule: {rule_id}")

    def get_quality_summary(self, dataset_id: str | None = None) -> dict[str, Any]:
        """Get quality summary for dataset or all datasets"""
        with self._lock:
            if dataset_id:
                history = self.quality_history.get(dataset_id, [])
                if not history:
                    return {"message": "No quality data available"}
                
                latest = history[-1]
                return {
                    "dataset_id": dataset_id,
                    "latest_score": latest.overall_score,
                    "quality_level": latest.quality_level.value,
                    "last_assessment": latest.timestamp.isoformat(),
                    "total_assessments": len(history)
                }
            else:
                # Summary for all datasets
                summaries = {}
                for ds_id, history in self.quality_history.items():
                    if history:
                        latest = history[-1]
                        summaries[ds_id] = {
                            "latest_score": latest.overall_score,
                            "quality_level": latest.quality_level.value,
                            "assessments_count": len(history)
                        }
                return summaries

    async def cleanup_old_reports(self, days_to_keep: int = 30):
        """Clean up old quality reports"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            for report_file in self.reports_path.glob("*.json"):
                if report_file.stat().st_mtime < cutoff_date.timestamp():
                    report_file.unlink()
                    self.logger.debug(f"Cleaned up old report: {report_file}")
                    
        except Exception as e:
            self.logger.error(f"Failed to cleanup old reports: {e}")
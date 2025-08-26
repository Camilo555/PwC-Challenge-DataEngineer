"""
Advanced Statistical Analyzer
Statistical analysis and hypothesis testing for data quality assessment.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import numpy as np
from scipy import stats
from scipy.stats import chi2_contingency, jarque_bera, normaltest, shapiro

from core.logging import get_logger


class StatisticalTest(Enum):
    """Available statistical tests"""
    NORMALITY_SHAPIRO = "shapiro_wilk"
    NORMALITY_JARQUE_BERA = "jarque_bera" 
    NORMALITY_DAGOSTINO = "dagostino_pearson"
    INDEPENDENCE_CHI2 = "chi2_independence"
    CORRELATION_PEARSON = "pearson_correlation"
    CORRELATION_SPEARMAN = "spearman_correlation"
    STATIONARITY_ADF = "augmented_dickey_fuller"
    OUTLIERS_GRUBBS = "grubbs_test"
    HOMOGENEITY_LEVENE = "levene_test"


class TestResult(Enum):
    """Statistical test results"""
    SIGNIFICANT = "significant"
    NOT_SIGNIFICANT = "not_significant"
    INCONCLUSIVE = "inconclusive"


@dataclass
class StatisticalTestResult:
    """Result of a statistical test"""
    test_name: str
    test_type: StatisticalTest
    statistic: float
    p_value: float
    critical_value: float | None = None
    result: TestResult = TestResult.INCONCLUSIVE
    confidence_level: float = 0.95
    interpretation: str = ""
    recommendations: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DistributionAnalysis:
    """Distribution analysis results"""
    distribution_type: str
    parameters: dict[str, float] = field(default_factory=dict)
    goodness_of_fit: dict[str, float] = field(default_factory=dict)
    descriptive_stats: dict[str, float] = field(default_factory=dict)
    percentiles: dict[str, float] = field(default_factory=dict)
    outliers: list[float] = field(default_factory=list)
    quality_score: float = 0.0


@dataclass
class CorrelationAnalysis:
    """Correlation analysis results"""
    correlation_matrix: dict[str, dict[str, float]] = field(default_factory=dict)
    strong_correlations: list[dict[str, Any]] = field(default_factory=list)
    correlation_clusters: list[list[str]] = field(default_factory=list)
    significance_tests: dict[str, StatisticalTestResult] = field(default_factory=dict)


@dataclass
class TimeSeriesAnalysis:
    """Time series analysis results"""
    trend: str = "unknown"
    seasonality: bool = False
    stationarity: bool = False
    autocorrelation: list[float] = field(default_factory=list)
    partial_autocorrelation: list[float] = field(default_factory=list)
    change_points: list[datetime] = field(default_factory=list)
    anomalous_periods: list[dict[str, Any]] = field(default_factory=list)


class StatisticalAnalyzer:
    """
    Advanced Statistical Analyzer for Data Quality
    
    Provides comprehensive statistical analysis including:
    - Distribution analysis and testing
    - Normality testing
    - Correlation analysis
    - Time series analysis
    - Outlier detection
    - Hypothesis testing
    """

    def __init__(self, confidence_level: float = 0.95):
        self.confidence_level = confidence_level
        self.alpha = 1 - confidence_level
        self.logger = get_logger(self.__class__.__name__)

    def analyze_distribution(
        self, 
        data: list[float], 
        field_name: str = "unknown"
    ) -> DistributionAnalysis:
        """
        Comprehensive distribution analysis
        
        Args:
            data: Numeric data to analyze
            field_name: Name of the field being analyzed
        
        Returns:
            Distribution analysis results
        """
        try:
            if len(data) < 3:
                return DistributionAnalysis(distribution_type="insufficient_data")
            
            # Remove NaN and infinite values
            clean_data = [x for x in data if np.isfinite(x)]
            
            if len(clean_data) < 3:
                return DistributionAnalysis(distribution_type="insufficient_clean_data")
            
            analysis = DistributionAnalysis(distribution_type="unknown")
            
            # Descriptive statistics
            analysis.descriptive_stats = self._calculate_descriptive_stats(clean_data)
            
            # Percentiles
            analysis.percentiles = self._calculate_percentiles(clean_data)
            
            # Outlier detection
            analysis.outliers = self._detect_statistical_outliers(clean_data)
            
            # Distribution identification
            analysis.distribution_type = self._identify_distribution(clean_data)
            
            # Distribution parameters
            analysis.parameters = self._estimate_distribution_parameters(
                clean_data, analysis.distribution_type
            )
            
            # Goodness of fit tests
            analysis.goodness_of_fit = self._perform_goodness_of_fit_tests(clean_data)
            
            # Quality score
            analysis.quality_score = self._calculate_distribution_quality_score(analysis)
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Distribution analysis failed for {field_name}: {e}")
            return DistributionAnalysis(distribution_type="analysis_error")

    def test_normality(
        self, 
        data: list[float], 
        test_method: StatisticalTest = StatisticalTest.NORMALITY_SHAPIRO
    ) -> StatisticalTestResult:
        """
        Test for normality using various methods
        
        Args:
            data: Data to test
            test_method: Statistical test to use
        
        Returns:
            Test result
        """
        try:
            clean_data = [x for x in data if np.isfinite(x)]
            
            if len(clean_data) < 3:
                return StatisticalTestResult(
                    test_name="normality_test",
                    test_type=test_method,
                    statistic=0.0,
                    p_value=1.0,
                    result=TestResult.INCONCLUSIVE,
                    interpretation="Insufficient data for normality testing"
                )
            
            if test_method == StatisticalTest.NORMALITY_SHAPIRO:
                return self._shapiro_wilk_test(clean_data)
            elif test_method == StatisticalTest.NORMALITY_JARQUE_BERA:
                return self._jarque_bera_test(clean_data)
            elif test_method == StatisticalTest.NORMALITY_DAGOSTINO:
                return self._dagostino_pearson_test(clean_data)
            else:
                raise ValueError(f"Unsupported normality test: {test_method}")
                
        except Exception as e:
            self.logger.error(f"Normality test failed: {e}")
            return StatisticalTestResult(
                test_name="normality_test",
                test_type=test_method,
                statistic=0.0,
                p_value=1.0,
                result=TestResult.INCONCLUSIVE,
                interpretation=f"Test failed: {str(e)}"
            )

    def analyze_correlations(
        self, 
        data: dict[str, list[float]]
    ) -> CorrelationAnalysis:
        """
        Comprehensive correlation analysis between multiple variables
        
        Args:
            data: Dictionary of field_name -> values
        
        Returns:
            Correlation analysis results
        """
        try:
            analysis = CorrelationAnalysis()
            
            field_names = list(data.keys())
            
            if len(field_names) < 2:
                return analysis
            
            # Calculate correlation matrix
            for field1 in field_names:
                analysis.correlation_matrix[field1] = {}
                
                for field2 in field_names:
                    if field1 == field2:
                        analysis.correlation_matrix[field1][field2] = 1.0
                    else:
                        correlation = self._calculate_correlation(
                            data[field1], data[field2]
                        )
                        analysis.correlation_matrix[field1][field2] = correlation
            
            # Identify strong correlations
            analysis.strong_correlations = self._identify_strong_correlations(
                analysis.correlation_matrix
            )
            
            # Perform correlation significance tests
            analysis.significance_tests = self._test_correlation_significance(data)
            
            # Identify correlation clusters
            analysis.correlation_clusters = self._identify_correlation_clusters(
                analysis.correlation_matrix
            )
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Correlation analysis failed: {e}")
            return CorrelationAnalysis()

    def analyze_time_series(
        self, 
        timestamps: list[datetime], 
        values: list[float]
    ) -> TimeSeriesAnalysis:
        """
        Time series analysis for temporal data quality assessment
        
        Args:
            timestamps: List of timestamps
            values: Corresponding values
        
        Returns:
            Time series analysis results
        """
        try:
            if len(timestamps) != len(values) or len(timestamps) < 10:
                return TimeSeriesAnalysis()
            
            analysis = TimeSeriesAnalysis()
            
            # Sort by timestamp
            sorted_data = sorted(zip(timestamps, values))
            sorted_timestamps, sorted_values = zip(*sorted_data)
            
            # Trend analysis
            analysis.trend = self._detect_trend(list(sorted_values))
            
            # Stationarity test
            analysis.stationarity = self._test_stationarity(list(sorted_values))
            
            # Seasonality detection
            analysis.seasonality = self._detect_seasonality(
                list(sorted_timestamps), list(sorted_values)
            )
            
            # Change point detection
            analysis.change_points = self._detect_change_points(
                list(sorted_timestamps), list(sorted_values)
            )
            
            # Anomalous periods detection
            analysis.anomalous_periods = self._detect_anomalous_periods(
                list(sorted_timestamps), list(sorted_values)
            )
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Time series analysis failed: {e}")
            return TimeSeriesAnalysis()

    def _calculate_descriptive_stats(self, data: list[float]) -> dict[str, float]:
        """Calculate comprehensive descriptive statistics"""
        try:
            return {
                'count': len(data),
                'mean': statistics.mean(data),
                'median': statistics.median(data),
                'mode': statistics.mode(data) if len(set(data)) < len(data) else None,
                'std_dev': statistics.stdev(data) if len(data) > 1 else 0,
                'variance': statistics.variance(data) if len(data) > 1 else 0,
                'min': min(data),
                'max': max(data),
                'range': max(data) - min(data),
                'skewness': stats.skew(data),
                'kurtosis': stats.kurtosis(data),
                'coefficient_of_variation': statistics.stdev(data) / abs(statistics.mean(data)) 
                    if len(data) > 1 and statistics.mean(data) != 0 else 0
            }
        except Exception as e:
            self.logger.error(f"Descriptive statistics calculation failed: {e}")
            return {}

    def _calculate_percentiles(self, data: list[float]) -> dict[str, float]:
        """Calculate percentiles"""
        try:
            percentiles = {}
            for p in [1, 5, 10, 25, 50, 75, 90, 95, 99]:
                percentiles[f'p{p}'] = np.percentile(data, p)
            return percentiles
        except Exception as e:
            self.logger.error(f"Percentile calculation failed: {e}")
            return {}

    def _detect_statistical_outliers(self, data: list[float]) -> list[float]:
        """Detect outliers using multiple methods"""
        outliers = set()
        
        try:
            # IQR method
            q25 = np.percentile(data, 25)
            q75 = np.percentile(data, 75)
            iqr = q75 - q25
            
            if iqr > 0:
                lower_bound = q25 - 1.5 * iqr
                upper_bound = q75 + 1.5 * iqr
                
                for value in data:
                    if value < lower_bound or value > upper_bound:
                        outliers.add(value)
            
            # Z-score method
            if len(data) > 1:
                mean = statistics.mean(data)
                std_dev = statistics.stdev(data)
                
                if std_dev > 0:
                    for value in data:
                        z_score = abs((value - mean) / std_dev)
                        if z_score > 3:
                            outliers.add(value)
            
            # Modified Z-score method
            median = statistics.median(data)
            mad = statistics.median([abs(x - median) for x in data])
            
            if mad > 0:
                for value in data:
                    modified_z_score = 0.6745 * (value - median) / mad
                    if abs(modified_z_score) > 3.5:
                        outliers.add(value)
            
        except Exception as e:
            self.logger.error(f"Outlier detection failed: {e}")
        
        return list(outliers)

    def _identify_distribution(self, data: list[float]) -> str:
        """Identify the most likely distribution type"""
        try:
            # Simple heuristics for distribution identification
            skewness = stats.skew(data)
            kurtosis = stats.kurtosis(data)
            
            # Test for normality
            if len(data) >= 8:
                _, p_value = shapiro(data)
                if p_value > 0.05:
                    return "normal"
            
            # Check for uniform distribution
            if abs(kurtosis + 1.2) < 0.5:  # Uniform has kurtosis of -1.2
                return "uniform"
            
            # Check for exponential distribution
            if skewness > 1.5:
                return "exponential"
            
            # Check for lognormal distribution
            if skewness > 0.5 and kurtosis > 0:
                # Try log transformation
                try:
                    log_data = [np.log(x) for x in data if x > 0]
                    if len(log_data) > len(data) * 0.9:  # Most values are positive
                        log_skewness = stats.skew(log_data)
                        if abs(log_skewness) < abs(skewness):
                            return "lognormal"
                except:
                    pass
            
            # Default classification based on skewness and kurtosis
            if abs(skewness) < 0.5 and abs(kurtosis) < 0.5:
                return "approximately_normal"
            elif skewness > 0.5:
                return "right_skewed"
            elif skewness < -0.5:
                return "left_skewed"
            else:
                return "unknown"
                
        except Exception as e:
            self.logger.error(f"Distribution identification failed: {e}")
            return "unknown"

    def _estimate_distribution_parameters(
        self, 
        data: list[float], 
        distribution_type: str
    ) -> dict[str, float]:
        """Estimate parameters for identified distribution"""
        try:
            parameters = {}
            
            if distribution_type == "normal" or distribution_type == "approximately_normal":
                parameters['mean'] = statistics.mean(data)
                parameters['std_dev'] = statistics.stdev(data) if len(data) > 1 else 0
            
            elif distribution_type == "lognormal":
                log_data = [np.log(x) for x in data if x > 0]
                if log_data:
                    parameters['log_mean'] = statistics.mean(log_data)
                    parameters['log_std'] = statistics.stdev(log_data) if len(log_data) > 1 else 0
            
            elif distribution_type == "exponential":
                parameters['lambda'] = 1 / statistics.mean(data) if statistics.mean(data) > 0 else 0
            
            elif distribution_type == "uniform":
                parameters['min'] = min(data)
                parameters['max'] = max(data)
            
            return parameters
            
        except Exception as e:
            self.logger.error(f"Parameter estimation failed: {e}")
            return {}

    def _perform_goodness_of_fit_tests(self, data: list[float]) -> dict[str, float]:
        """Perform goodness of fit tests"""
        try:
            tests = {}
            
            # Kolmogorov-Smirnov test against normal distribution
            if len(data) >= 8:
                mean = statistics.mean(data)
                std_dev = statistics.stdev(data) if len(data) > 1 else 1
                
                ks_stat, ks_p = stats.kstest(data, lambda x: stats.norm.cdf(x, mean, std_dev))
                tests['ks_normal_statistic'] = ks_stat
                tests['ks_normal_p_value'] = ks_p
            
            # Anderson-Darling test for normality
            try:
                ad_stat, ad_critical, ad_significance = stats.anderson(data, dist='norm')
                tests['anderson_darling_statistic'] = ad_stat
                tests['anderson_darling_critical_5pct'] = ad_critical[2]  # 5% significance level
            except:
                pass
            
            return tests
            
        except Exception as e:
            self.logger.error(f"Goodness of fit tests failed: {e}")
            return {}

    def _calculate_distribution_quality_score(self, analysis: DistributionAnalysis) -> float:
        """Calculate quality score for distribution analysis"""
        try:
            score = 1.0
            
            # Penalize for outliers
            if analysis.outliers and analysis.descriptive_stats.get('count', 0) > 0:
                outlier_rate = len(analysis.outliers) / analysis.descriptive_stats['count']
                score -= outlier_rate * 0.3
            
            # Penalize for extreme skewness
            skewness = analysis.descriptive_stats.get('skewness', 0)
            if abs(skewness) > 2:
                score -= 0.2
            elif abs(skewness) > 1:
                score -= 0.1
            
            # Penalize for extreme kurtosis
            kurtosis = analysis.descriptive_stats.get('kurtosis', 0)
            if abs(kurtosis) > 3:
                score -= 0.1
            
            return max(0.0, score)
            
        except Exception as e:
            self.logger.error(f"Distribution quality score calculation failed: {e}")
            return 0.5

    def _shapiro_wilk_test(self, data: list[float]) -> StatisticalTestResult:
        """Perform Shapiro-Wilk normality test"""
        try:
            if len(data) < 3 or len(data) > 5000:
                return StatisticalTestResult(
                    test_name="Shapiro-Wilk Normality Test",
                    test_type=StatisticalTest.NORMALITY_SHAPIRO,
                    statistic=0.0,
                    p_value=1.0,
                    result=TestResult.INCONCLUSIVE,
                    interpretation="Sample size outside valid range (3-5000)"
                )
            
            statistic, p_value = shapiro(data)
            
            result = TestResult.SIGNIFICANT if p_value < self.alpha else TestResult.NOT_SIGNIFICANT
            
            interpretation = (
                f"Data {'does not follow' if result == TestResult.SIGNIFICANT else 'follows'} "
                f"a normal distribution (p-value: {p_value:.4f})"
            )
            
            recommendations = []
            if result == TestResult.SIGNIFICANT:
                recommendations.extend([
                    "Consider data transformation (log, square root, etc.)",
                    "Use non-parametric statistical methods",
                    "Investigate potential outliers"
                ])
            
            return StatisticalTestResult(
                test_name="Shapiro-Wilk Normality Test",
                test_type=StatisticalTest.NORMALITY_SHAPIRO,
                statistic=statistic,
                p_value=p_value,
                result=result,
                interpretation=interpretation,
                recommendations=recommendations,
                metadata={'sample_size': len(data)}
            )
            
        except Exception as e:
            self.logger.error(f"Shapiro-Wilk test failed: {e}")
            return StatisticalTestResult(
                test_name="Shapiro-Wilk Normality Test",
                test_type=StatisticalTest.NORMALITY_SHAPIRO,
                statistic=0.0,
                p_value=1.0,
                result=TestResult.INCONCLUSIVE,
                interpretation=f"Test failed: {str(e)}"
            )

    def _jarque_bera_test(self, data: list[float]) -> StatisticalTestResult:
        """Perform Jarque-Bera normality test"""
        try:
            if len(data) < 10:
                return StatisticalTestResult(
                    test_name="Jarque-Bera Normality Test",
                    test_type=StatisticalTest.NORMALITY_JARQUE_BERA,
                    statistic=0.0,
                    p_value=1.0,
                    result=TestResult.INCONCLUSIVE,
                    interpretation="Sample size too small (minimum 10 required)"
                )
            
            statistic, p_value = jarque_bera(data)
            
            result = TestResult.SIGNIFICANT if p_value < self.alpha else TestResult.NOT_SIGNIFICANT
            
            interpretation = (
                f"Data {'does not follow' if result == TestResult.SIGNIFICANT else 'follows'} "
                f"a normal distribution based on skewness and kurtosis (p-value: {p_value:.4f})"
            )
            
            return StatisticalTestResult(
                test_name="Jarque-Bera Normality Test",
                test_type=StatisticalTest.NORMALITY_JARQUE_BERA,
                statistic=statistic,
                p_value=p_value,
                result=result,
                interpretation=interpretation,
                metadata={'sample_size': len(data)}
            )
            
        except Exception as e:
            self.logger.error(f"Jarque-Bera test failed: {e}")
            return StatisticalTestResult(
                test_name="Jarque-Bera Normality Test",
                test_type=StatisticalTest.NORMALITY_JARQUE_BERA,
                statistic=0.0,
                p_value=1.0,
                result=TestResult.INCONCLUSIVE,
                interpretation=f"Test failed: {str(e)}"
            )

    def _dagostino_pearson_test(self, data: list[float]) -> StatisticalTestResult:
        """Perform D'Agostino-Pearson normality test"""
        try:
            if len(data) < 20:
                return StatisticalTestResult(
                    test_name="D'Agostino-Pearson Normality Test",
                    test_type=StatisticalTest.NORMALITY_DAGOSTINO,
                    statistic=0.0,
                    p_value=1.0,
                    result=TestResult.INCONCLUSIVE,
                    interpretation="Sample size too small (minimum 20 required)"
                )
            
            statistic, p_value = normaltest(data)
            
            result = TestResult.SIGNIFICANT if p_value < self.alpha else TestResult.NOT_SIGNIFICANT
            
            interpretation = (
                f"Data {'does not follow' if result == TestResult.SIGNIFICANT else 'follows'} "
                f"a normal distribution (p-value: {p_value:.4f})"
            )
            
            return StatisticalTestResult(
                test_name="D'Agostino-Pearson Normality Test", 
                test_type=StatisticalTest.NORMALITY_DAGOSTINO,
                statistic=statistic,
                p_value=p_value,
                result=result,
                interpretation=interpretation,
                metadata={'sample_size': len(data)}
            )
            
        except Exception as e:
            self.logger.error(f"D'Agostino-Pearson test failed: {e}")
            return StatisticalTestResult(
                test_name="D'Agostino-Pearson Normality Test",
                test_type=StatisticalTest.NORMALITY_DAGOSTINO,
                statistic=0.0,
                p_value=1.0,
                result=TestResult.INCONCLUSIVE,
                interpretation=f"Test failed: {str(e)}"
            )

    def _calculate_correlation(self, data1: list[float], data2: list[float]) -> float:
        """Calculate Pearson correlation coefficient"""
        try:
            # Ensure same length and remove missing values
            clean_pairs = [
                (x, y) for x, y in zip(data1, data2) 
                if np.isfinite(x) and np.isfinite(y)
            ]
            
            if len(clean_pairs) < 3:
                return 0.0
            
            clean_data1, clean_data2 = zip(*clean_pairs)
            
            correlation = np.corrcoef(clean_data1, clean_data2)[0, 1]
            return correlation if np.isfinite(correlation) else 0.0
            
        except Exception as e:
            self.logger.error(f"Correlation calculation failed: {e}")
            return 0.0

    def _identify_strong_correlations(
        self, 
        correlation_matrix: dict[str, dict[str, float]]
    ) -> list[dict[str, Any]]:
        """Identify strong correlations in the matrix"""
        strong_correlations = []
        
        try:
            fields = list(correlation_matrix.keys())
            
            for i, field1 in enumerate(fields):
                for j, field2 in enumerate(fields[i+1:], i+1):
                    correlation = correlation_matrix[field1].get(field2, 0.0)
                    
                    if abs(correlation) > 0.7:  # Strong correlation threshold
                        strong_correlations.append({
                            'field1': field1,
                            'field2': field2,
                            'correlation': correlation,
                            'strength': 'very_strong' if abs(correlation) > 0.9 else 'strong',
                            'direction': 'positive' if correlation > 0 else 'negative'
                        })
            
            # Sort by absolute correlation strength
            strong_correlations.sort(key=lambda x: abs(x['correlation']), reverse=True)
            
        except Exception as e:
            self.logger.error(f"Strong correlation identification failed: {e}")
        
        return strong_correlations

    def _test_correlation_significance(
        self, 
        data: dict[str, list[float]]
    ) -> dict[str, StatisticalTestResult]:
        """Test significance of correlations"""
        significance_tests = {}
        
        try:
            fields = list(data.keys())
            
            for i, field1 in enumerate(fields):
                for j, field2 in enumerate(fields[i+1:], i+1):
                    # Calculate correlation and test significance
                    clean_pairs = [
                        (x, y) for x, y in zip(data[field1], data[field2])
                        if np.isfinite(x) and np.isfinite(y)
                    ]
                    
                    if len(clean_pairs) < 3:
                        continue
                    
                    clean_data1, clean_data2 = zip(*clean_pairs)
                    
                    correlation, p_value = stats.pearsonr(clean_data1, clean_data2)
                    
                    result = TestResult.SIGNIFICANT if p_value < self.alpha else TestResult.NOT_SIGNIFICANT
                    
                    test_key = f"{field1}_vs_{field2}"
                    significance_tests[test_key] = StatisticalTestResult(
                        test_name="Pearson Correlation Test",
                        test_type=StatisticalTest.CORRELATION_PEARSON,
                        statistic=correlation,
                        p_value=p_value,
                        result=result,
                        interpretation=f"Correlation between {field1} and {field2} is "
                                     f"{'significant' if result == TestResult.SIGNIFICANT else 'not significant'}",
                        metadata={'sample_size': len(clean_pairs)}
                    )
        
        except Exception as e:
            self.logger.error(f"Correlation significance testing failed: {e}")
        
        return significance_tests

    def _identify_correlation_clusters(
        self, 
        correlation_matrix: dict[str, dict[str, float]]
    ) -> list[list[str]]:
        """Identify clusters of highly correlated fields"""
        # This is a simplified clustering based on correlation threshold
        # In practice, you might use more sophisticated clustering algorithms
        
        try:
            fields = list(correlation_matrix.keys())
            visited = set()
            clusters = []
            
            for field in fields:
                if field in visited:
                    continue
                
                cluster = [field]
                visited.add(field)
                
                # Find fields highly correlated with this field
                for other_field in fields:
                    if (other_field not in visited and 
                        abs(correlation_matrix[field].get(other_field, 0)) > 0.8):
                        cluster.append(other_field)
                        visited.add(other_field)
                
                if len(cluster) > 1:
                    clusters.append(cluster)
            
            return clusters
            
        except Exception as e:
            self.logger.error(f"Correlation clustering failed: {e}")
            return []

    def _detect_trend(self, values: list[float]) -> str:
        """Detect trend in time series data"""
        try:
            if len(values) < 3:
                return "insufficient_data"
            
            # Simple trend detection using linear regression slope
            x = list(range(len(values)))
            slope, _, r_value, p_value, _ = stats.linregress(x, values)
            
            if p_value < 0.05:  # Significant trend
                if slope > 0:
                    return "increasing"
                elif slope < 0:
                    return "decreasing"
                else:
                    return "stable"
            else:
                return "no_trend"
                
        except Exception as e:
            self.logger.error(f"Trend detection failed: {e}")
            return "unknown"

    def _test_stationarity(self, values: list[float]) -> bool:
        """Test for stationarity using simple variance test"""
        try:
            if len(values) < 20:
                return False
            
            # Split data into two halves and compare variance
            mid = len(values) // 2
            first_half = values[:mid]
            second_half = values[mid:]
            
            if len(first_half) < 2 or len(second_half) < 2:
                return False
            
            var1 = statistics.variance(first_half)
            var2 = statistics.variance(second_half)
            
            # Simple test: if variances are similar, assume stationarity
            if var1 == 0 and var2 == 0:
                return True
            elif var1 == 0 or var2 == 0:
                return False
            
            ratio = max(var1, var2) / min(var1, var2)
            return ratio < 2.0  # Arbitrary threshold
            
        except Exception as e:
            self.logger.error(f"Stationarity test failed: {e}")
            return False

    def _detect_seasonality(
        self, 
        timestamps: list[datetime], 
        values: list[float]
    ) -> bool:
        """Simple seasonality detection"""
        try:
            if len(values) < 24:  # Need at least 2 years of monthly data
                return False
            
            # Extract time components
            months = [ts.month for ts in timestamps]
            
            # Group by month and calculate averages
            monthly_means = {}
            for month, value in zip(months, values):
                if month not in monthly_means:
                    monthly_means[month] = []
                monthly_means[month].append(value)
            
            # Calculate mean for each month
            month_averages = {
                month: statistics.mean(values) 
                for month, values in monthly_means.items() 
                if len(values) > 0
            }
            
            if len(month_averages) < 6:  # Need data from at least 6 different months
                return False
            
            # Check if there's significant variation between months
            overall_mean = statistics.mean(values)
            monthly_deviations = [
                abs(avg - overall_mean) for avg in month_averages.values()
            ]
            
            avg_deviation = statistics.mean(monthly_deviations)
            overall_std = statistics.stdev(values) if len(values) > 1 else 0
            
            # If monthly deviations are larger than overall std, assume seasonality
            return avg_deviation > overall_std * 0.5 if overall_std > 0 else False
            
        except Exception as e:
            self.logger.error(f"Seasonality detection failed: {e}")
            return False

    def _detect_change_points(
        self, 
        timestamps: list[datetime], 
        values: list[float]
    ) -> list[datetime]:
        """Simple change point detection"""
        change_points = []
        
        try:
            if len(values) < 10:
                return change_points
            
            window_size = max(5, len(values) // 10)
            
            for i in range(window_size, len(values) - window_size):
                # Compare means before and after point i
                before = values[i-window_size:i]
                after = values[i:i+window_size]
                
                if len(before) < 2 or len(after) < 2:
                    continue
                
                mean_before = statistics.mean(before)
                mean_after = statistics.mean(after)
                std_before = statistics.stdev(before)
                std_after = statistics.stdev(after)
                
                # Simple threshold-based detection
                if std_before > 0 and std_after > 0:
                    z_score = abs(mean_after - mean_before) / (
                        (std_before + std_after) / 2
                    )
                    
                    if z_score > 2.0:  # Significant change
                        change_points.append(timestamps[i])
            
        except Exception as e:
            self.logger.error(f"Change point detection failed: {e}")
        
        return change_points[:5]  # Limit to 5 most significant change points

    def _detect_anomalous_periods(
        self, 
        timestamps: list[datetime], 
        values: list[float]
    ) -> list[dict[str, Any]]:
        """Detect anomalous time periods"""
        anomalous_periods = []
        
        try:
            if len(values) < 20:
                return anomalous_periods
            
            # Calculate rolling statistics
            window_size = max(5, len(values) // 20)
            
            for i in range(window_size, len(values) - window_size, window_size):
                window = values[i-window_size:i+window_size]
                current = values[i:i+min(window_size, len(values)-i)]
                
                if len(window) < 3 or len(current) < 1:
                    continue
                
                window_mean = statistics.mean(window)
                window_std = statistics.stdev(window) if len(window) > 1 else 0
                current_mean = statistics.mean(current)
                
                if window_std > 0:
                    z_score = abs(current_mean - window_mean) / window_std
                    
                    if z_score > 2.5:  # Anomalous period
                        anomalous_periods.append({
                            'start_time': timestamps[i],
                            'end_time': timestamps[min(i+window_size, len(timestamps)-1)],
                            'z_score': z_score,
                            'severity': 'high' if z_score > 3.0 else 'medium'
                        })
        
        except Exception as e:
            self.logger.error(f"Anomalous period detection failed: {e}")
        
        return anomalous_periods[:5]  # Limit to 5 most anomalous periods
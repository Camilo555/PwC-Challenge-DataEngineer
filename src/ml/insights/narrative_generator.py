"""
Automated Narrative Generator

AI-powered insight generation with natural language explanations
for ML model results and data analysis.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.monitoring.metrics import MetricsCollector

logger = get_logger(__name__)
settings = get_settings()


class InsightType(Enum):
    """Types of insights that can be generated."""
    TREND_ANALYSIS = "trend_analysis"
    ANOMALY_DETECTION = "anomaly_detection"
    PERFORMANCE_SUMMARY = "performance_summary"
    CORRELATION_ANALYSIS = "correlation_analysis"
    FORECASTING_INSIGHTS = "forecasting_insights"
    CUSTOMER_SEGMENTATION = "customer_segmentation"
    RECOMMENDATION = "recommendation"


class ReportLevel(Enum):
    """Report detail levels."""
    EXECUTIVE = "executive"
    OPERATIONAL = "operational"
    TECHNICAL = "technical"


@dataclass
class InsightConfig:
    """Configuration for insight generation."""
    
    report_level: ReportLevel = ReportLevel.OPERATIONAL
    include_visualizations: bool = True
    include_recommendations: bool = True
    max_insights: int = 10
    confidence_threshold: float = 0.7
    language: str = "en"
    tone: str = "professional"  # "professional", "casual", "technical"


@dataclass
class Insight:
    """Individual insight with narrative."""
    
    insight_id: str
    insight_type: InsightType
    title: str
    summary: str
    detailed_explanation: str
    confidence_score: float
    supporting_data: Dict[str, Any]
    recommendations: List[str]
    visualization_config: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)


class NarrativeTemplates:
    """Templates for generating narratives."""
    
    TREND_TEMPLATES = {
        "increasing": "The data shows a {strength} upward trend in {metric} with a {percentage}% increase over the {period}.",
        "decreasing": "There has been a {strength} downward trend in {metric}, declining by {percentage}% over the {period}.",
        "stable": "The {metric} has remained relatively stable over the {period}, with minimal variation.",
        "volatile": "The {metric} exhibits high volatility over the {period} with significant fluctuations."
    }
    
    ANOMALY_TEMPLATES = {
        "high": "An unusual spike was detected in {metric} on {date}, reaching {value} which is {std_devs} standard deviations above normal.",
        "low": "An unusual drop was observed in {metric} on {date}, falling to {value} which is {std_devs} standard deviations below normal.",
        "pattern": "An irregular pattern was identified in {metric} during {period}, deviating from expected seasonal behavior."
    }
    
    PERFORMANCE_TEMPLATES = {
        "excellent": "Performance has been excellent with {metric} achieving {value}, which is {percentage}% above target.",
        "good": "Performance has been satisfactory with {metric} at {value}, meeting {percentage}% of the target.",
        "poor": "Performance has been below expectations with {metric} at {value}, which is {percentage}% below target."
    }


class InsightEngine:
    """Main engine for generating automated insights."""
    
    def __init__(self, config: InsightConfig = None):
        self.config = config or InsightConfig()
        self.metrics_collector = MetricsCollector()
        self.templates = NarrativeTemplates()
        
    async def generate_insights(self, data: pd.DataFrame, 
                              analysis_types: List[str] = None,
                              date_column: str = "date",
                              metric_columns: List[str] = None) -> List[Insight]:
        """Generate insights from data."""
        try:
            analysis_types = analysis_types or ["trends", "anomalies", "performance"]
            metric_columns = metric_columns or self._detect_metric_columns(data)
            
            insights = []
            
            if "trends" in analysis_types:
                trend_insights = await self._analyze_trends(data, date_column, metric_columns)
                insights.extend(trend_insights)
            
            if "anomalies" in analysis_types:
                anomaly_insights = await self._detect_anomalies(data, metric_columns)
                insights.extend(anomaly_insights)
            
            if "performance" in analysis_types:
                performance_insights = await self._analyze_performance(data, metric_columns)
                insights.extend(performance_insights)
            
            if "correlations" in analysis_types:
                correlation_insights = await self._analyze_correlations(data, metric_columns)
                insights.extend(correlation_insights)
            
            # Sort by confidence and limit
            insights.sort(key=lambda x: x.confidence_score, reverse=True)
            insights = insights[:self.config.max_insights]
            
            logger.info(f"Generated {len(insights)} insights")
            
            return insights
            
        except Exception as e:
            logger.error(f"Error generating insights: {str(e)}")
            raise
    
    async def generate_report(self, insights: List[Insight]) -> str:
        """Generate a comprehensive narrative report."""
        try:
            report_sections = []
            
            # Executive Summary
            if self.config.report_level == ReportLevel.EXECUTIVE:
                summary = self._generate_executive_summary(insights)
                report_sections.append(f"## Executive Summary\n{summary}\n")
            
            # Key Findings
            key_findings = self._generate_key_findings(insights)
            report_sections.append(f"## Key Findings\n{key_findings}\n")
            
            # Detailed Analysis
            if self.config.report_level in [ReportLevel.OPERATIONAL, ReportLevel.TECHNICAL]:
                detailed_analysis = self._generate_detailed_analysis(insights)
                report_sections.append(f"## Detailed Analysis\n{detailed_analysis}\n")
            
            # Recommendations
            if self.config.include_recommendations:
                recommendations = self._generate_recommendations(insights)
                report_sections.append(f"## Recommendations\n{recommendations}\n")
            
            # Technical Details
            if self.config.report_level == ReportLevel.TECHNICAL:
                technical_details = self._generate_technical_details(insights)
                report_sections.append(f"## Technical Details\n{technical_details}\n")
            
            report = "\n".join(report_sections)
            report += f"\n\n*Report generated on {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}*"
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating report: {str(e)}")
            raise
    
    def _detect_metric_columns(self, data: pd.DataFrame) -> List[str]:
        """Detect metric columns in the data."""
        numeric_columns = data.select_dtypes(include=[np.number]).columns.tolist()
        
        # Filter out likely ID columns
        metric_columns = []
        for col in numeric_columns:
            if not any(keyword in col.lower() for keyword in ['id', 'key', 'index']):
                if data[col].nunique() > 1:  # Has variation
                    metric_columns.append(col)
        
        return metric_columns
    
    async def _analyze_trends(self, data: pd.DataFrame, date_column: str, 
                            metric_columns: List[str]) -> List[Insight]:
        """Analyze trends in the data."""
        insights = []
        
        try:
            if date_column not in data.columns:
                return insights
            
            # Ensure date column is datetime
            if not pd.api.types.is_datetime64_any_dtype(data[date_column]):
                data[date_column] = pd.to_datetime(data[date_column])
            
            # Sort by date
            data_sorted = data.sort_values(date_column)
            
            for metric in metric_columns:
                if metric not in data.columns:
                    continue
                
                # Calculate trend using linear regression slope
                y = data_sorted[metric].values
                x = np.arange(len(y))
                
                if len(y) < 2:
                    continue
                
                # Simple linear regression
                slope = np.polyfit(x, y, 1)[0]
                correlation = np.corrcoef(x, y)[0, 1] if not np.isnan(np.corrcoef(x, y)[0, 1]) else 0
                
                # Determine trend strength and direction
                relative_slope = abs(slope) / (np.mean(y) + 1e-8)
                
                if abs(correlation) < 0.3:
                    trend_type = "stable"
                    strength = "minimal"
                elif slope > 0:
                    trend_type = "increasing"
                    strength = "strong" if relative_slope > 0.1 else "moderate"
                else:
                    trend_type = "decreasing"
                    strength = "strong" if relative_slope > 0.1 else "moderate"
                
                # Calculate percentage change
                start_value = y[0] if len(y) > 0 else 0
                end_value = y[-1] if len(y) > 0 else 0
                percentage_change = ((end_value - start_value) / (start_value + 1e-8)) * 100
                
                # Generate narrative
                period = f"{len(data_sorted)} data points"
                template = self.templates.TREND_TEMPLATES[trend_type]
                
                narrative = template.format(
                    strength=strength,
                    metric=metric.replace('_', ' ').title(),
                    percentage=abs(round(percentage_change, 1)),
                    period=period
                )
                
                insight = Insight(
                    insight_id=f"trend_{metric}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    insight_type=InsightType.TREND_ANALYSIS,
                    title=f"Trend Analysis: {metric.replace('_', ' ').title()}",
                    summary=narrative,
                    detailed_explanation=f"Statistical analysis reveals a {trend_type} trend with correlation coefficient of {correlation:.3f}. "
                                      f"The slope indicates a {strength} {trend_type} pattern over the analyzed period.",
                    confidence_score=abs(correlation),
                    supporting_data={
                        "metric": metric,
                        "slope": slope,
                        "correlation": correlation,
                        "percentage_change": percentage_change,
                        "trend_type": trend_type,
                        "strength": strength
                    },
                    recommendations=self._generate_trend_recommendations(trend_type, metric, percentage_change)
                )
                
                if insight.confidence_score >= self.config.confidence_threshold:
                    insights.append(insight)
        
        except Exception as e:
            logger.error(f"Error analyzing trends: {str(e)}")
        
        return insights
    
    async def _detect_anomalies(self, data: pd.DataFrame, metric_columns: List[str]) -> List[Insight]:
        """Detect anomalies in the data."""
        insights = []
        
        try:
            for metric in metric_columns:
                if metric not in data.columns:
                    continue
                
                values = data[metric].dropna()
                if len(values) < 10:  # Need sufficient data
                    continue
                
                # Calculate z-scores
                mean_val = values.mean()
                std_val = values.std()
                
                if std_val == 0:
                    continue
                
                z_scores = np.abs((values - mean_val) / std_val)
                
                # Find anomalies (|z-score| > 3)
                anomaly_threshold = 3.0
                anomalies = values[z_scores > anomaly_threshold]
                
                if len(anomalies) > 0:
                    # Get the most extreme anomaly
                    max_anomaly_idx = z_scores.idxmax()
                    anomaly_value = values.loc[max_anomaly_idx]
                    anomaly_z_score = z_scores.loc[max_anomaly_idx]
                    
                    anomaly_type = "high" if anomaly_value > mean_val else "low"
                    
                    narrative = f"Detected {len(anomalies)} anomal{'y' if len(anomalies) == 1 else 'ies'} in {metric.replace('_', ' ')}. "
                    narrative += f"The most significant anomaly shows a value of {anomaly_value:.2f}, which is "
                    narrative += f"{anomaly_z_score:.1f} standard deviations {'above' if anomaly_type == 'high' else 'below'} the mean."
                    
                    insight = Insight(
                        insight_id=f"anomaly_{metric}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        insight_type=InsightType.ANOMALY_DETECTION,
                        title=f"Anomaly Detection: {metric.replace('_', ' ').title()}",
                        summary=narrative,
                        detailed_explanation=f"Statistical analysis using z-score method (threshold: {anomaly_threshold}) "
                                          f"identified {len(anomalies)} outliers. This represents "
                                          f"{(len(anomalies)/len(values)*100):.1f}% of all observations.",
                        confidence_score=min(1.0, anomaly_z_score / 5.0),  # Normalize confidence
                        supporting_data={
                            "metric": metric,
                            "anomaly_count": len(anomalies),
                            "max_anomaly_value": anomaly_value,
                            "max_anomaly_z_score": anomaly_z_score,
                            "anomaly_type": anomaly_type,
                            "mean": mean_val,
                            "std": std_val
                        },
                        recommendations=self._generate_anomaly_recommendations(anomaly_type, metric, len(anomalies))
                    )
                    
                    if insight.confidence_score >= self.config.confidence_threshold:
                        insights.append(insight)
        
        except Exception as e:
            logger.error(f"Error detecting anomalies: {str(e)}")
        
        return insights
    
    async def _analyze_performance(self, data: pd.DataFrame, metric_columns: List[str]) -> List[Insight]:
        """Analyze performance metrics."""
        insights = []
        
        # This would typically compare against targets or benchmarks
        # For now, we'll analyze distribution and basic statistics
        
        try:
            for metric in metric_columns:
                if metric not in data.columns:
                    continue
                
                values = data[metric].dropna()
                if len(values) == 0:
                    continue
                
                # Basic statistics
                mean_val = values.mean()
                median_val = values.median()
                std_val = values.std()
                
                # Performance assessment (simplified)
                cv = std_val / mean_val if mean_val != 0 else 0  # Coefficient of variation
                
                if cv < 0.1:
                    performance_level = "excellent"
                    description = "highly consistent"
                elif cv < 0.3:
                    performance_level = "good"  
                    description = "moderately consistent"
                else:
                    performance_level = "poor"
                    description = "highly variable"
                
                narrative = f"The {metric.replace('_', ' ')} metric shows {description} performance with a coefficient of variation of {cv:.3f}. "
                narrative += f"The average value is {mean_val:.2f} with a standard deviation of {std_val:.2f}."
                
                insight = Insight(
                    insight_id=f"performance_{metric}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    insight_type=InsightType.PERFORMANCE_SUMMARY,
                    title=f"Performance Summary: {metric.replace('_', ' ').title()}",
                    summary=narrative,
                    detailed_explanation=f"Performance analysis based on variability assessment. "
                                      f"Lower coefficient of variation indicates more consistent performance.",
                    confidence_score=0.8,  # Standard confidence for statistical summaries
                    supporting_data={
                        "metric": metric,
                        "mean": mean_val,
                        "median": median_val,
                        "std": std_val,
                        "cv": cv,
                        "performance_level": performance_level,
                        "count": len(values)
                    },
                    recommendations=self._generate_performance_recommendations(performance_level, metric, cv)
                )
                
                insights.append(insight)
        
        except Exception as e:
            logger.error(f"Error analyzing performance: {str(e)}")
        
        return insights
    
    async def _analyze_correlations(self, data: pd.DataFrame, metric_columns: List[str]) -> List[Insight]:
        """Analyze correlations between metrics."""
        insights = []
        
        try:
            if len(metric_columns) < 2:
                return insights
            
            # Calculate correlation matrix
            correlation_matrix = data[metric_columns].corr()
            
            # Find strong correlations (excluding self-correlations)
            strong_correlations = []
            for i in range(len(metric_columns)):
                for j in range(i + 1, len(metric_columns)):
                    corr = correlation_matrix.iloc[i, j]
                    if abs(corr) > 0.7:  # Strong correlation threshold
                        strong_correlations.append({
                            'metric1': metric_columns[i],
                            'metric2': metric_columns[j],
                            'correlation': corr
                        })
            
            if strong_correlations:
                # Create insight for strongest correlation
                strongest = max(strong_correlations, key=lambda x: abs(x['correlation']))
                
                correlation_type = "positive" if strongest['correlation'] > 0 else "negative"
                correlation_strength = "very strong" if abs(strongest['correlation']) > 0.9 else "strong"
                
                narrative = f"Found a {correlation_strength} {correlation_type} correlation "
                narrative += f"({strongest['correlation']:.3f}) between "
                narrative += f"{strongest['metric1'].replace('_', ' ')} and {strongest['metric2'].replace('_', ' ')}."
                
                insight = Insight(
                    insight_id=f"correlation_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    insight_type=InsightType.CORRELATION_ANALYSIS,
                    title="Correlation Analysis",
                    summary=narrative,
                    detailed_explanation=f"Correlation analysis revealed {len(strong_correlations)} "
                                      f"strong correlation{'s' if len(strong_correlations) > 1 else ''} between metrics. "
                                      f"This suggests potential relationships that could be leveraged for predictions.",
                    confidence_score=abs(strongest['correlation']),
                    supporting_data={
                        "strongest_correlation": strongest,
                        "all_strong_correlations": strong_correlations,
                        "correlation_matrix": correlation_matrix.to_dict()
                    },
                    recommendations=self._generate_correlation_recommendations(strongest)
                )
                
                insights.append(insight)
        
        except Exception as e:
            logger.error(f"Error analyzing correlations: {str(e)}")
        
        return insights
    
    def _generate_trend_recommendations(self, trend_type: str, metric: str, 
                                      percentage_change: float) -> List[str]:
        """Generate recommendations based on trend analysis."""
        recommendations = []
        
        if trend_type == "increasing":
            if percentage_change > 20:
                recommendations.append(f"Monitor the rapid growth in {metric} to ensure sustainability")
                recommendations.append("Consider scaling resources to support continued growth")
            else:
                recommendations.append(f"Maintain current strategies driving {metric} improvement")
        
        elif trend_type == "decreasing":
            if abs(percentage_change) > 20:
                recommendations.append(f"Investigate root causes of declining {metric}")
                recommendations.append("Implement corrective actions to reverse the downward trend")
            else:
                recommendations.append(f"Monitor {metric} closely to prevent further decline")
        
        else:  # stable
            recommendations.append(f"Consider opportunities to optimize {metric}")
            recommendations.append("Maintain current performance levels while exploring improvements")
        
        return recommendations
    
    def _generate_anomaly_recommendations(self, anomaly_type: str, metric: str, 
                                        count: int) -> List[str]:
        """Generate recommendations based on anomaly detection."""
        recommendations = []
        
        recommendations.append(f"Investigate the root cause of anomalies in {metric}")
        
        if anomaly_type == "high":
            recommendations.append("Determine if high values represent opportunities or issues")
        else:
            recommendations.append("Address potential issues causing low values")
        
        if count > 1:
            recommendations.append("Review data collection processes for systematic issues")
        
        recommendations.append("Implement monitoring to catch future anomalies early")
        
        return recommendations
    
    def _generate_performance_recommendations(self, performance_level: str, metric: str, 
                                           cv: float) -> List[str]:
        """Generate recommendations based on performance analysis."""
        recommendations = []
        
        if performance_level == "excellent":
            recommendations.append(f"Maintain current processes for {metric}")
            recommendations.append("Document best practices for consistency")
        
        elif performance_level == "good":
            recommendations.append(f"Look for opportunities to reduce variability in {metric}")
            recommendations.append("Analyze top performers to identify improvement strategies")
        
        else:  # poor
            recommendations.append(f"Prioritize reducing variability in {metric}")
            recommendations.append("Implement process improvements to increase consistency")
            recommendations.append("Consider root cause analysis for high variation")
        
        return recommendations
    
    def _generate_correlation_recommendations(self, correlation: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on correlation analysis."""
        recommendations = []
        
        metric1 = correlation['metric1'].replace('_', ' ')
        metric2 = correlation['metric2'].replace('_', ' ')
        
        if correlation['correlation'] > 0:
            recommendations.append(f"Leverage the positive relationship between {metric1} and {metric2}")
            recommendations.append(f"Consider using {metric1} as a leading indicator for {metric2}")
        else:
            recommendations.append(f"Investigate the inverse relationship between {metric1} and {metric2}")
            recommendations.append("Consider if this represents a trade-off that needs management")
        
        recommendations.append("Use this correlation for predictive modeling")
        
        return recommendations
    
    def _generate_executive_summary(self, insights: List[Insight]) -> str:
        """Generate executive summary from insights."""
        if not insights:
            return "No significant insights were identified in the analyzed data."
        
        summary = f"Analysis of the data revealed {len(insights)} key insights. "
        
        # Categorize insights
        categories = {}
        for insight in insights:
            category = insight.insight_type.value
            if category not in categories:
                categories[category] = 0
            categories[category] += 1
        
        category_text = ", ".join([f"{count} {category.replace('_', ' ')}" for category, count in categories.items()])
        summary += f"These include {category_text}. "
        
        # Highlight top insight
        top_insight = max(insights, key=lambda x: x.confidence_score)
        summary += f"The most significant finding: {top_insight.summary}"
        
        return summary
    
    def _generate_key_findings(self, insights: List[Insight]) -> str:
        """Generate key findings section."""
        findings = []
        
        for i, insight in enumerate(insights[:5], 1):  # Top 5 insights
            findings.append(f"{i}. **{insight.title}**: {insight.summary}")
        
        return "\n".join(findings)
    
    def _generate_detailed_analysis(self, insights: List[Insight]) -> str:
        """Generate detailed analysis section."""
        analysis_sections = []
        
        for insight in insights:
            section = f"### {insight.title}\n"
            section += f"{insight.detailed_explanation}\n"
            
            if self.config.report_level == ReportLevel.TECHNICAL:
                section += f"**Confidence Score**: {insight.confidence_score:.3f}\n"
                section += f"**Supporting Data**: {json.dumps(insight.supporting_data, indent=2, default=str)}\n"
            
            analysis_sections.append(section)
        
        return "\n".join(analysis_sections)
    
    def _generate_recommendations(self, insights: List[Insight]) -> str:
        """Generate recommendations section."""
        all_recommendations = []
        
        for insight in insights:
            all_recommendations.extend(insight.recommendations)
        
        # Remove duplicates while preserving order
        unique_recommendations = []
        seen = set()
        for rec in all_recommendations:
            if rec not in seen:
                unique_recommendations.append(rec)
                seen.add(rec)
        
        numbered_recs = [f"{i+1}. {rec}" for i, rec in enumerate(unique_recommendations[:10])]
        
        return "\n".join(numbered_recs)
    
    def _generate_technical_details(self, insights: List[Insight]) -> str:
        """Generate technical details section."""
        details = []
        
        details.append(f"**Analysis Configuration**:")
        details.append(f"- Report Level: {self.config.report_level.value}")
        details.append(f"- Confidence Threshold: {self.config.confidence_threshold}")
        details.append(f"- Max Insights: {self.config.max_insights}")
        
        details.append(f"\n**Insight Statistics**:")
        if insights:
            confidences = [i.confidence_score for i in insights]
            details.append(f"- Average Confidence: {np.mean(confidences):.3f}")
            details.append(f"- Confidence Range: {min(confidences):.3f} - {max(confidences):.3f}")
        
        return "\n".join(details)


# Factory function
def create_insight_engine(config: InsightConfig = None) -> InsightEngine:
    """Create an insight engine with specified configuration."""
    return InsightEngine(config)
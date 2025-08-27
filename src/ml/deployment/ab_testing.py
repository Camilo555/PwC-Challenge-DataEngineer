"""
A/B Testing Framework for ML Models

Comprehensive A/B testing system for evaluating model performance
in production with statistical significance testing.
"""

import logging
import asyncio
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from enum import Enum
import json
import uuid
from scipy import stats
from scipy.stats import chi2_contingency, ttest_ind, mannwhitneyu
import aioredis

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.monitoring.metrics import MetricsCollector
from src.core.caching.redis_cache import get_redis_client
from src.data_access.supabase_client import get_supabase_client

logger = get_logger(__name__)
settings = get_settings()


class ExperimentStatus(Enum):
    """Status of A/B test experiment."""
    DRAFT = "draft"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class SplitStrategy(Enum):
    """Traffic splitting strategy."""
    RANDOM = "random"
    USER_ID = "user_id"
    SESSION_ID = "session_id"
    CUSTOM = "custom"


@dataclass
class ExperimentConfig:
    """Configuration for A/B test experiment."""
    
    experiment_id: str
    name: str
    description: str
    start_date: datetime
    end_date: datetime
    
    # Model configuration
    control_model_id: str
    treatment_model_ids: List[str]
    
    # Traffic splitting
    split_strategy: SplitStrategy = SplitStrategy.RANDOM
    traffic_allocation: Dict[str, float] = field(default_factory=dict)
    
    # Statistical configuration
    primary_metric: str = "accuracy"
    secondary_metrics: List[str] = field(default_factory=list)
    significance_level: float = 0.05
    minimum_sample_size: int = 1000
    minimum_effect_size: float = 0.05
    
    # Monitoring
    check_frequency_hours: int = 24
    early_stopping_enabled: bool = True
    
    # Metadata
    owner: str = "system"
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ExperimentResult:
    """Result of A/B test experiment."""
    
    experiment_id: str
    variant_id: str
    sample_size: int
    metric_values: Dict[str, List[float]]
    metric_stats: Dict[str, Dict[str, float]]
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class StatisticalTest:
    """Statistical test result."""
    
    metric_name: str
    control_mean: float
    treatment_mean: float
    effect_size: float
    p_value: float
    confidence_interval: Tuple[float, float]
    is_significant: bool
    test_type: str
    sample_size_control: int
    sample_size_treatment: int


class TrafficSplitter:
    """Traffic splitter for A/B tests."""
    
    def __init__(self, config: ExperimentConfig):
        self.config = config
        
    def assign_variant(self, user_context: Dict[str, Any]) -> str:
        """Assign user to experiment variant."""
        if self.config.split_strategy == SplitStrategy.RANDOM:
            return self._random_assignment()
        elif self.config.split_strategy == SplitStrategy.USER_ID:
            return self._hash_based_assignment(user_context.get("user_id", ""))
        elif self.config.split_strategy == SplitStrategy.SESSION_ID:
            return self._hash_based_assignment(user_context.get("session_id", ""))
        else:
            return self._random_assignment()
    
    def _random_assignment(self) -> str:
        """Random variant assignment."""
        rand = np.random.random()
        cumulative_prob = 0.0
        
        # Control first
        control_prob = self.config.traffic_allocation.get("control", 0.5)
        if rand < control_prob:
            return "control"
        
        cumulative_prob = control_prob
        
        # Treatment variants
        for i, model_id in enumerate(self.config.treatment_model_ids):
            variant_id = f"treatment_{i}"
            variant_prob = self.config.traffic_allocation.get(variant_id, 0.5 / len(self.config.treatment_model_ids))
            
            if rand < cumulative_prob + variant_prob:
                return variant_id
            
            cumulative_prob += variant_prob
        
        # Default to control
        return "control"
    
    def _hash_based_assignment(self, identifier: str) -> str:
        """Hash-based consistent assignment."""
        if not identifier:
            return self._random_assignment()
        
        # Simple hash-based assignment
        hash_value = hash(f"{self.config.experiment_id}:{identifier}")
        normalized_hash = (hash_value % 10000) / 10000.0
        
        cumulative_prob = 0.0
        
        # Control first
        control_prob = self.config.traffic_allocation.get("control", 0.5)
        if normalized_hash < control_prob:
            return "control"
        
        cumulative_prob = control_prob
        
        # Treatment variants
        for i, model_id in enumerate(self.config.treatment_model_ids):
            variant_id = f"treatment_{i}"
            variant_prob = self.config.traffic_allocation.get(variant_id, 0.5 / len(self.config.treatment_model_ids))
            
            if normalized_hash < cumulative_prob + variant_prob:
                return variant_id
            
            cumulative_prob += variant_prob
        
        # Default to control
        return "control"


class StatisticalAnalyzer:
    """Statistical analysis for A/B tests."""
    
    def __init__(self, significance_level: float = 0.05):
        self.significance_level = significance_level
    
    def analyze_metric(self, control_data: List[float], treatment_data: List[float],
                      metric_name: str, metric_type: str = "continuous") -> StatisticalTest:
        """Analyze a single metric between control and treatment."""
        
        control_array = np.array(control_data)
        treatment_array = np.array(treatment_data)
        
        control_mean = np.mean(control_array)
        treatment_mean = np.mean(treatment_array)
        effect_size = (treatment_mean - control_mean) / control_mean if control_mean != 0 else 0
        
        if metric_type == "continuous":
            # Use t-test for continuous metrics
            statistic, p_value = ttest_ind(control_array, treatment_array, equal_var=False)
            test_type = "Welch's t-test"
            
            # Calculate confidence interval
            pooled_se = np.sqrt(
                (np.var(control_array, ddof=1) / len(control_array)) +
                (np.var(treatment_array, ddof=1) / len(treatment_array))
            )
            
            degrees_freedom = len(control_array) + len(treatment_array) - 2
            t_critical = stats.t.ppf(1 - self.significance_level/2, degrees_freedom)
            
            ci_lower = effect_size - t_critical * pooled_se / control_mean if control_mean != 0 else 0
            ci_upper = effect_size + t_critical * pooled_se / control_mean if control_mean != 0 else 0
            
        elif metric_type == "binary":
            # Use chi-square test for binary metrics
            control_success = np.sum(control_array)
            control_total = len(control_array)
            treatment_success = np.sum(treatment_array)
            treatment_total = len(treatment_array)
            
            contingency_table = [
                [control_success, control_total - control_success],
                [treatment_success, treatment_total - treatment_success]
            ]
            
            chi2, p_value, _, _ = chi2_contingency(contingency_table)
            test_type = "Chi-square test"
            
            # Calculate confidence interval for proportion difference
            p1 = control_success / control_total
            p2 = treatment_success / treatment_total
            
            se_diff = np.sqrt(
                (p1 * (1 - p1) / control_total) + 
                (p2 * (1 - p2) / treatment_total)
            )
            
            z_critical = stats.norm.ppf(1 - self.significance_level/2)
            diff = p2 - p1
            
            ci_lower = diff - z_critical * se_diff
            ci_upper = diff + z_critical * se_diff
            
        else:
            # Use Mann-Whitney U test for non-parametric
            statistic, p_value = mannwhitneyu(control_array, treatment_array, alternative='two-sided')
            test_type = "Mann-Whitney U test"
            
            # Bootstrap confidence interval
            ci_lower, ci_upper = self._bootstrap_ci(control_array, treatment_array)
        
        is_significant = p_value < self.significance_level
        
        return StatisticalTest(
            metric_name=metric_name,
            control_mean=control_mean,
            treatment_mean=treatment_mean,
            effect_size=effect_size,
            p_value=p_value,
            confidence_interval=(ci_lower, ci_upper),
            is_significant=is_significant,
            test_type=test_type,
            sample_size_control=len(control_array),
            sample_size_treatment=len(treatment_array)
        )
    
    def _bootstrap_ci(self, control_data: np.ndarray, treatment_data: np.ndarray,
                     n_bootstrap: int = 1000) -> Tuple[float, float]:
        """Calculate bootstrap confidence interval."""
        bootstrap_effects = []
        
        for _ in range(n_bootstrap):
            # Bootstrap samples
            control_bootstrap = np.random.choice(control_data, size=len(control_data), replace=True)
            treatment_bootstrap = np.random.choice(treatment_data, size=len(treatment_data), replace=True)
            
            # Calculate effect
            control_mean = np.mean(control_bootstrap)
            treatment_mean = np.mean(treatment_bootstrap)
            effect = (treatment_mean - control_mean) / control_mean if control_mean != 0 else 0
            
            bootstrap_effects.append(effect)
        
        # Calculate percentiles
        ci_lower = np.percentile(bootstrap_effects, 100 * self.significance_level / 2)
        ci_upper = np.percentile(bootstrap_effects, 100 * (1 - self.significance_level / 2))
        
        return ci_lower, ci_upper
    
    def calculate_sample_size(self, baseline_rate: float, minimum_effect: float,
                            power: float = 0.8, significance: float = 0.05) -> int:
        """Calculate required sample size for experiment."""
        
        # For proportion-based metrics
        z_alpha = stats.norm.ppf(1 - significance / 2)
        z_beta = stats.norm.ppf(power)
        
        p1 = baseline_rate
        p2 = baseline_rate * (1 + minimum_effect)
        
        pooled_p = (p1 + p2) / 2
        
        numerator = (z_alpha * np.sqrt(2 * pooled_p * (1 - pooled_p)) + 
                    z_beta * np.sqrt(p1 * (1 - p1) + p2 * (1 - p2))) ** 2
        denominator = (p2 - p1) ** 2
        
        n_per_group = int(np.ceil(numerator / denominator))
        
        return n_per_group * 2  # Total sample size


class ExperimentManager:
    """Manager for A/B test experiments."""
    
    def __init__(self):
        self.supabase = get_supabase_client()
        self.redis_client = get_redis_client()
        self.metrics_collector = MetricsCollector()
        self.experiments: Dict[str, ExperimentConfig] = {}
        self.analyzers: Dict[str, StatisticalAnalyzer] = {}
        
    async def create_experiment(self, config: ExperimentConfig) -> bool:
        """Create a new A/B test experiment."""
        try:
            # Store experiment configuration
            experiment_data = {
                "experiment_id": config.experiment_id,
                "name": config.name,
                "description": config.description,
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
                "control_model_id": config.control_model_id,
                "treatment_model_ids": config.treatment_model_ids,
                "split_strategy": config.split_strategy.value,
                "traffic_allocation": config.traffic_allocation,
                "primary_metric": config.primary_metric,
                "secondary_metrics": config.secondary_metrics,
                "significance_level": config.significance_level,
                "minimum_sample_size": config.minimum_sample_size,
                "minimum_effect_size": config.minimum_effect_size,
                "status": ExperimentStatus.DRAFT.value,
                "owner": config.owner,
                "tags": config.tags,
                "created_at": config.created_at.isoformat()
            }
            
            # Store in database
            result = self.supabase.table('ab_experiments').insert(experiment_data).execute()
            
            # Cache in memory and Redis
            self.experiments[config.experiment_id] = config
            self.analyzers[config.experiment_id] = StatisticalAnalyzer(config.significance_level)
            
            await self.redis_client.hset(
                f"experiment:{config.experiment_id}",
                mapping={k: json.dumps(v, default=str) for k, v in experiment_data.items()}
            )
            
            logger.info(f"Experiment created: {config.name} ({config.experiment_id})")
            
            self.metrics_collector.increment_counter(
                "ab_testing_experiments_created_total",
                tags={"experiment_name": config.name}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating experiment: {str(e)}")
            return False
    
    async def start_experiment(self, experiment_id: str) -> bool:
        """Start an A/B test experiment."""
        try:
            # Update status to running
            self.supabase.table('ab_experiments').update({
                "status": ExperimentStatus.RUNNING.value,
                "actual_start_date": datetime.utcnow().isoformat()
            }).eq("experiment_id", experiment_id).execute()
            
            # Update cache
            await self.redis_client.hset(
                f"experiment:{experiment_id}",
                "status",
                json.dumps(ExperimentStatus.RUNNING.value)
            )
            
            logger.info(f"Experiment started: {experiment_id}")
            
            self.metrics_collector.increment_counter(
                "ab_testing_experiments_started_total",
                tags={"experiment_id": experiment_id}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error starting experiment {experiment_id}: {str(e)}")
            return False
    
    async def stop_experiment(self, experiment_id: str) -> bool:
        """Stop an A/B test experiment."""
        try:
            # Update status to completed
            self.supabase.table('ab_experiments').update({
                "status": ExperimentStatus.COMPLETED.value,
                "actual_end_date": datetime.utcnow().isoformat()
            }).eq("experiment_id", experiment_id).execute()
            
            # Update cache
            await self.redis_client.hset(
                f"experiment:{experiment_id}",
                "status",
                json.dumps(ExperimentStatus.COMPLETED.value)
            )
            
            logger.info(f"Experiment stopped: {experiment_id}")
            
            self.metrics_collector.increment_counter(
                "ab_testing_experiments_stopped_total",
                tags={"experiment_id": experiment_id}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error stopping experiment {experiment_id}: {str(e)}")
            return False
    
    async def assign_variant(self, experiment_id: str, user_context: Dict[str, Any]) -> Optional[str]:
        """Assign user to experiment variant."""
        try:
            config = await self._get_experiment_config(experiment_id)
            if not config:
                return None
            
            # Check if experiment is running
            status = await self._get_experiment_status(experiment_id)
            if status != ExperimentStatus.RUNNING:
                return None
            
            # Check date range
            now = datetime.utcnow()
            if now < config.start_date or now > config.end_date:
                return None
            
            # Assign variant
            splitter = TrafficSplitter(config)
            variant = splitter.assign_variant(user_context)
            
            # Log assignment
            assignment_data = {
                "experiment_id": experiment_id,
                "user_id": user_context.get("user_id"),
                "session_id": user_context.get("session_id"),
                "variant": variant,
                "timestamp": now.isoformat(),
                "context": user_context
            }
            
            # Store assignment
            self.supabase.table('ab_assignments').insert(assignment_data).execute()
            
            # Cache assignment
            cache_key = f"assignment:{experiment_id}:{user_context.get('user_id', user_context.get('session_id', 'unknown'))}"
            await self.redis_client.setex(
                cache_key,
                3600,  # 1 hour TTL
                json.dumps({"variant": variant, "timestamp": now.isoformat()})
            )
            
            self.metrics_collector.increment_counter(
                "ab_testing_assignments_total",
                tags={"experiment_id": experiment_id, "variant": variant}
            )
            
            return variant
            
        except Exception as e:
            logger.error(f"Error assigning variant for experiment {experiment_id}: {str(e)}")
            return None
    
    async def record_event(self, experiment_id: str, user_context: Dict[str, Any],
                          event_type: str, event_value: float) -> bool:
        """Record an event for experiment analysis."""
        try:
            # Get user's variant assignment
            variant = await self._get_user_variant(experiment_id, user_context)
            if not variant:
                return False
            
            # Record event
            event_data = {
                "experiment_id": experiment_id,
                "user_id": user_context.get("user_id"),
                "session_id": user_context.get("session_id"),
                "variant": variant,
                "event_type": event_type,
                "event_value": event_value,
                "timestamp": datetime.utcnow().isoformat(),
                "context": user_context
            }
            
            self.supabase.table('ab_events').insert(event_data).execute()
            
            # Update real-time metrics in Redis
            redis_key = f"experiment_metrics:{experiment_id}:{variant}:{event_type}"
            await self.redis_client.lpush(redis_key, event_value)
            await self.redis_client.expire(redis_key, 86400 * 7)  # 7 days TTL
            
            self.metrics_collector.increment_counter(
                "ab_testing_events_recorded_total",
                tags={"experiment_id": experiment_id, "variant": variant, "event_type": event_type}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error recording event for experiment {experiment_id}: {str(e)}")
            return False
    
    async def analyze_experiment(self, experiment_id: str) -> Dict[str, Any]:
        """Analyze experiment results."""
        try:
            config = await self._get_experiment_config(experiment_id)
            if not config:
                return {}
            
            analyzer = self.analyzers.get(experiment_id, StatisticalAnalyzer())
            
            # Get experiment data
            results = {}
            all_metrics = [config.primary_metric] + config.secondary_metrics
            
            for metric in all_metrics:
                # Get data for each variant
                control_data = await self._get_metric_data(experiment_id, "control", metric)
                
                for i, treatment_model_id in enumerate(config.treatment_model_ids):
                    variant_id = f"treatment_{i}"
                    treatment_data = await self._get_metric_data(experiment_id, variant_id, metric)
                    
                    if len(control_data) >= config.minimum_sample_size and len(treatment_data) >= config.minimum_sample_size:
                        # Perform statistical analysis
                        test_result = analyzer.analyze_metric(
                            control_data, treatment_data, metric
                        )
                        
                        results[f"{metric}_{variant_id}"] = {
                            "metric_name": test_result.metric_name,
                            "control_mean": test_result.control_mean,
                            "treatment_mean": test_result.treatment_mean,
                            "effect_size": test_result.effect_size,
                            "p_value": test_result.p_value,
                            "confidence_interval": test_result.confidence_interval,
                            "is_significant": test_result.is_significant,
                            "test_type": test_result.test_type,
                            "sample_size_control": test_result.sample_size_control,
                            "sample_size_treatment": test_result.sample_size_treatment
                        }
            
            # Store analysis results
            analysis_data = {
                "experiment_id": experiment_id,
                "analysis_date": datetime.utcnow().isoformat(),
                "results": results,
                "summary": self._generate_summary(results, config)
            }
            
            self.supabase.table('ab_analyses').insert(analysis_data).execute()
            
            logger.info(f"Experiment analysis completed: {experiment_id}")
            
            self.metrics_collector.increment_counter(
                "ab_testing_analyses_completed_total",
                tags={"experiment_id": experiment_id}
            )
            
            return analysis_data
            
        except Exception as e:
            logger.error(f"Error analyzing experiment {experiment_id}: {str(e)}")
            return {}
    
    async def _get_experiment_config(self, experiment_id: str) -> Optional[ExperimentConfig]:
        """Get experiment configuration."""
        if experiment_id in self.experiments:
            return self.experiments[experiment_id]
        
        # Try Redis cache
        experiment_data = await self.redis_client.hgetall(f"experiment:{experiment_id}")
        if experiment_data:
            # Reconstruct config from Redis data
            # This would need proper deserialization
            pass
        
        # Query database
        result = self.supabase.table('ab_experiments').select('*').eq('experiment_id', experiment_id).execute()
        if result.data:
            # Reconstruct config from database data
            # This would need proper deserialization
            pass
        
        return None
    
    async def _get_experiment_status(self, experiment_id: str) -> Optional[ExperimentStatus]:
        """Get experiment status."""
        # Try Redis first
        status_data = await self.redis_client.hget(f"experiment:{experiment_id}", "status")
        if status_data:
            return ExperimentStatus(json.loads(status_data.decode()))
        
        # Query database
        result = self.supabase.table('ab_experiments').select('status').eq('experiment_id', experiment_id).execute()
        if result.data:
            return ExperimentStatus(result.data[0]['status'])
        
        return None
    
    async def _get_user_variant(self, experiment_id: str, user_context: Dict[str, Any]) -> Optional[str]:
        """Get user's variant assignment."""
        cache_key = f"assignment:{experiment_id}:{user_context.get('user_id', user_context.get('session_id', 'unknown'))}"
        cached_assignment = await self.redis_client.get(cache_key)
        
        if cached_assignment:
            assignment_data = json.loads(cached_assignment)
            return assignment_data.get("variant")
        
        # Query database
        user_id = user_context.get("user_id")
        session_id = user_context.get("session_id")
        
        query = self.supabase.table('ab_assignments').select('variant').eq('experiment_id', experiment_id)
        
        if user_id:
            query = query.eq('user_id', user_id)
        elif session_id:
            query = query.eq('session_id', session_id)
        else:
            return None
        
        result = query.order('timestamp', desc=True).limit(1).execute()
        
        if result.data:
            return result.data[0]['variant']
        
        return None
    
    async def _get_metric_data(self, experiment_id: str, variant: str, metric: str) -> List[float]:
        """Get metric data for analysis."""
        # Try Redis for real-time data
        redis_key = f"experiment_metrics:{experiment_id}:{variant}:{metric}"
        redis_data = await self.redis_client.lrange(redis_key, 0, -1)
        
        if redis_data:
            return [float(value.decode()) for value in redis_data]
        
        # Query database
        result = self.supabase.table('ab_events').select('event_value').eq('experiment_id', experiment_id).eq('variant', variant).eq('event_type', metric).execute()
        
        if result.data:
            return [float(record['event_value']) for record in result.data]
        
        return []
    
    def _generate_summary(self, results: Dict[str, Any], config: ExperimentConfig) -> Dict[str, Any]:
        """Generate experiment summary."""
        summary = {
            "total_variants_tested": len(config.treatment_model_ids) + 1,
            "significant_results": 0,
            "recommendations": []
        }
        
        primary_metric_results = []
        
        for key, result in results.items():
            if config.primary_metric in key and result.get("is_significant"):
                summary["significant_results"] += 1
                primary_metric_results.append((key, result))
        
        # Generate recommendations
        if primary_metric_results:
            best_result = max(primary_metric_results, key=lambda x: x[1]["treatment_mean"])
            summary["recommendations"].append(
                f"Consider adopting {best_result[0]} as it shows significant improvement in {config.primary_metric}"
            )
        else:
            summary["recommendations"].append(
                "No significant improvements detected. Consider running longer or testing different approaches."
            )
        
        return summary
    
    async def get_experiment_status_report(self, experiment_id: str) -> Dict[str, Any]:
        """Get comprehensive experiment status report."""
        try:
            config = await self._get_experiment_config(experiment_id)
            if not config:
                return {"error": "Experiment not found"}
            
            status = await self._get_experiment_status(experiment_id)
            
            # Get sample sizes for each variant
            sample_sizes = {}
            for variant in ["control"] + [f"treatment_{i}" for i in range(len(config.treatment_model_ids))]:
                events_result = self.supabase.table('ab_events').select('user_id', exact=False).eq('experiment_id', experiment_id).eq('variant', variant).execute()
                sample_sizes[variant] = len(set([event.get('user_id') for event in events_result.data if event.get('user_id')]))
            
            total_sample_size = sum(sample_sizes.values())
            
            # Calculate progress
            progress = min(100, (total_sample_size / config.minimum_sample_size) * 100) if config.minimum_sample_size > 0 else 0
            
            report = {
                "experiment_id": experiment_id,
                "name": config.name,
                "status": status.value if status else "unknown",
                "start_date": config.start_date.isoformat(),
                "end_date": config.end_date.isoformat(),
                "progress": {
                    "total_sample_size": total_sample_size,
                    "minimum_required": config.minimum_sample_size,
                    "progress_percentage": progress,
                    "sample_sizes_by_variant": sample_sizes
                },
                "configuration": {
                    "traffic_allocation": config.traffic_allocation,
                    "primary_metric": config.primary_metric,
                    "secondary_metrics": config.secondary_metrics,
                    "significance_level": config.significance_level
                },
                "time_remaining_days": (config.end_date - datetime.utcnow()).days if config.end_date > datetime.utcnow() else 0,
                "is_ready_for_analysis": total_sample_size >= config.minimum_sample_size,
                "last_updated": datetime.utcnow().isoformat()
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Error getting experiment status report: {str(e)}")
            return {"error": str(e)}


class ABTestingFramework:
    """High-level A/B testing framework interface."""
    
    def __init__(self):
        self.experiment_manager = ExperimentManager()
        self.statistical_analyzer = StatisticalAnalyzer()
        
    async def create_model_comparison_experiment(
        self,
        experiment_name: str,
        control_model_id: str,
        treatment_model_ids: List[str],
        primary_metric: str = "accuracy",
        duration_days: int = 14,
        significance_level: float = 0.05,
        minimum_sample_size: int = 1000,
        traffic_allocation: Optional[Dict[str, float]] = None
    ) -> str:
        """Create a model comparison A/B test experiment."""
        
        experiment_id = str(uuid.uuid4())
        
        if not traffic_allocation:
            # Default equal allocation
            num_variants = len(treatment_model_ids) + 1  # +1 for control
            allocation = 1.0 / num_variants
            traffic_allocation = {"control": allocation}
            for i in range(len(treatment_model_ids)):
                traffic_allocation[f"treatment_{i}"] = allocation
        
        config = ExperimentConfig(
            experiment_id=experiment_id,
            name=experiment_name,
            description=f"A/B test comparing {control_model_id} (control) vs {', '.join(treatment_model_ids)} (treatments)",
            start_date=datetime.utcnow(),
            end_date=datetime.utcnow() + timedelta(days=duration_days),
            control_model_id=control_model_id,
            treatment_model_ids=treatment_model_ids,
            split_strategy=SplitStrategy.RANDOM,
            traffic_allocation=traffic_allocation,
            primary_metric=primary_metric,
            significance_level=significance_level,
            minimum_sample_size=minimum_sample_size
        )
        
        success = await self.experiment_manager.create_experiment(config)
        
        if success:
            # Auto-start the experiment
            await self.experiment_manager.start_experiment(experiment_id)
            logger.info(f"Model comparison experiment created and started: {experiment_name} ({experiment_id})")
            return experiment_id
        else:
            raise Exception(f"Failed to create experiment: {experiment_name}")
    
    async def get_model_for_prediction(self, experiment_id: str, user_context: Dict[str, Any]) -> Optional[str]:
        """Get the model ID to use for prediction based on A/B test assignment."""
        try:
            variant = await self.experiment_manager.assign_variant(experiment_id, user_context)
            
            if not variant:
                return None
                
            config = await self.experiment_manager._get_experiment_config(experiment_id)
            if not config:
                return None
            
            if variant == "control":
                return config.control_model_id
            elif variant.startswith("treatment_"):
                treatment_index = int(variant.split("_")[1])
                if treatment_index < len(config.treatment_model_ids):
                    return config.treatment_model_ids[treatment_index]
            
            # Fallback to control
            return config.control_model_id
            
        except Exception as e:
            logger.error(f"Error getting model for prediction: {str(e)}")
            return None
    
    async def record_prediction_outcome(
        self,
        experiment_id: str,
        user_context: Dict[str, Any],
        prediction_accuracy: float,
        prediction_latency: float,
        custom_metrics: Optional[Dict[str, float]] = None
    ) -> bool:
        """Record prediction outcomes for A/B test analysis."""
        try:
            # Record accuracy
            await self.experiment_manager.record_event(
                experiment_id, user_context, "accuracy", prediction_accuracy
            )
            
            # Record latency
            await self.experiment_manager.record_event(
                experiment_id, user_context, "latency", prediction_latency
            )
            
            # Record custom metrics
            if custom_metrics:
                for metric_name, metric_value in custom_metrics.items():
                    await self.experiment_manager.record_event(
                        experiment_id, user_context, metric_name, metric_value
                    )
            
            return True
            
        except Exception as e:
            logger.error(f"Error recording prediction outcome: {str(e)}")
            return False
    
    async def check_experiment_for_early_stopping(self, experiment_id: str) -> Dict[str, Any]:
        """Check if experiment should be stopped early due to significant results."""
        try:
            config = await self.experiment_manager._get_experiment_config(experiment_id)
            if not config or not config.early_stopping_enabled:
                return {"should_stop": False, "reason": "Early stopping disabled"}
            
            # Analyze current results
            analysis = await self.experiment_manager.analyze_experiment(experiment_id)
            
            if not analysis.get("results"):
                return {"should_stop": False, "reason": "Insufficient data"}
            
            # Check for significant results in primary metric
            primary_results = [
                result for key, result in analysis["results"].items()
                if config.primary_metric in key and result.get("is_significant")
            ]
            
            if primary_results:
                # Check if we have sufficient power and effect size
                significant_count = len(primary_results)
                total_comparisons = len(config.treatment_model_ids)
                
                if significant_count / total_comparisons >= 0.5:  # Majority of comparisons are significant
                    return {
                        "should_stop": True,
                        "reason": f"Significant results detected in {significant_count}/{total_comparisons} comparisons",
                        "analysis": analysis
                    }
            
            return {"should_stop": False, "reason": "No significant results yet"}
            
        except Exception as e:
            logger.error(f"Error checking early stopping: {str(e)}")
            return {"should_stop": False, "reason": f"Error: {str(e)}"}
    
    async def get_active_experiments(self) -> List[Dict[str, Any]]:
        """Get list of active experiments."""
        try:
            result = self.experiment_manager.supabase.table('ab_experiments').select('*').eq('status', ExperimentStatus.RUNNING.value).execute()
            
            experiments = []
            for exp_data in result.data or []:
                status_report = await self.experiment_manager.get_experiment_status_report(exp_data['experiment_id'])
                experiments.append(status_report)
            
            return experiments
            
        except Exception as e:
            logger.error(f"Error getting active experiments: {str(e)}")
            return []
    
    async def cleanup_completed_experiments(self, days_old: int = 30) -> int:
        """Clean up old completed experiments."""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_old)
            
            # Get completed experiments older than cutoff
            result = self.experiment_manager.supabase.table('ab_experiments').select('experiment_id').eq('status', ExperimentStatus.COMPLETED.value).lt('actual_end_date', cutoff_date.isoformat()).execute()
            
            cleanup_count = 0
            for exp in result.data or []:
                experiment_id = exp['experiment_id']
                
                # Clean up Redis cache
                pattern = f"*{experiment_id}*"
                keys = await self.experiment_manager.redis_client.keys(pattern)
                if keys:
                    await self.experiment_manager.redis_client.delete(*keys)
                
                cleanup_count += 1
            
            logger.info(f"Cleaned up {cleanup_count} old experiments")
            return cleanup_count
            
        except Exception as e:
            logger.error(f"Error cleaning up experiments: {str(e)}")
            return 0


# Factory functions
def create_ab_testing_framework() -> ABTestingFramework:
    """Create A/B testing framework instance."""
    return ABTestingFramework()


def create_experiment_manager() -> ExperimentManager:
    """Create experiment manager instance."""
    return ExperimentManager()


def create_statistical_analyzer(significance_level: float = 0.05) -> StatisticalAnalyzer:
    """Create statistical analyzer instance."""
    return StatisticalAnalyzer(significance_level)
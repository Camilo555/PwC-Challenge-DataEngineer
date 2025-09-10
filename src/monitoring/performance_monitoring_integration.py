"""
Performance Monitoring Integration for <15ms SLA Validation
Integrates all monitoring components for comprehensive performance validation and business impact tracking
"""

import asyncio
import time
from typing import Dict, Any, Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from core.logging import get_logger
from monitoring.performance_sla_monitor import get_performance_monitor, PerformanceSLAMonitor
from monitoring.fastapi_performance_middleware import FastAPIPerformanceMiddleware, setup_performance_monitoring_middleware
from monitoring.business_impact_monitor import get_business_impact_monitor, BusinessImpactMonitor

logger = get_logger(__name__)


class IntegratedPerformanceMiddleware(BaseHTTPMiddleware):
    """
    Integrated Performance Middleware that combines:
    - Microsecond-precision performance monitoring
    - Business impact correlation
    - Real-time SLA validation
    - Executive dashboard metrics
    """
    
    def __init__(self, app, **kwargs):
        super().__init__(app)
        
        # Initialize monitoring components
        self.performance_monitor = kwargs.get('performance_monitor') or get_performance_monitor()
        self.business_monitor = kwargs.get('business_monitor') or get_business_impact_monitor()
        
        # Configuration
        self.excluded_paths = kwargs.get('excluded_paths', [
            '/health', '/metrics', '/docs', '/redoc', '/openapi.json', '/favicon.ico'
        ])
        
        logger.info("Integrated Performance Middleware initialized with comprehensive monitoring")
    
    async def dispatch(self, request: Request, call_next) -> Response:
        """Process request with integrated performance and business monitoring."""
        
        # Skip monitoring for excluded paths
        if any(excluded in str(request.url.path) for excluded in self.excluded_paths):
            return await call_next(request)
        
        # Start high-precision timing
        start_time = time.perf_counter()
        
        # Extract request metadata
        endpoint = request.url.path
        method = request.method
        user_id = request.headers.get("X-User-ID")
        
        # Identify business story
        story_id = self._identify_business_story(endpoint)
        
        try:
            # Process request
            response = await call_next(request)
            
            # Calculate response time
            end_time = time.perf_counter()
            response_time_seconds = end_time - start_time
            
            # Extract cache information
            cache_hit = "hit" in response.headers.get("X-Cache", "").lower()
            cache_level = self._extract_cache_level(response)
            
            # Record performance measurement
            performance_measurement = self.performance_monitor.record_measurement(
                endpoint=endpoint,
                method=method,
                response_time_seconds=response_time_seconds,
                status_code=response.status_code,
                user_id=user_id,
                cache_hit=cache_hit,
                cache_level=cache_level
            )
            
            # Record business impact if story identified
            if story_id:
                business_impact = self.business_monitor.record_performance_impact(
                    story_id=story_id,
                    endpoint=endpoint,
                    response_time_ms=performance_measurement.response_time_ms,
                    request_volume=1,
                    status_code=response.status_code
                )
                
                # Add business context headers
                if business_impact:
                    response.headers["X-Business-Story"] = story_id
                    response.headers["X-Revenue-Impact"] = f"{business_impact.estimated_revenue_impact:.2f}"
                    response.headers["X-User-Satisfaction"] = f"{business_impact.user_satisfaction_score:.1f}"
            
            # Add performance headers
            response.headers["X-Response-Time-Ms"] = f"{performance_measurement.response_time_ms:.3f}"
            response.headers["X-SLA-Compliant"] = "true" if performance_measurement.is_sla_compliant else "false"
            response.headers["X-Performance-Grade"] = performance_measurement.performance_grade
            
            return response
            
        except Exception as e:
            # Handle errors
            end_time = time.perf_counter()
            response_time_seconds = end_time - start_time
            
            # Record failed performance measurement
            self.performance_monitor.record_measurement(
                endpoint=endpoint,
                method=method,
                response_time_seconds=response_time_seconds,
                status_code=500,
                user_id=user_id
            )
            
            # Record business impact for error
            if story_id:
                self.business_monitor.record_performance_impact(
                    story_id=story_id,
                    endpoint=endpoint,
                    response_time_ms=response_time_seconds * 1000,
                    request_volume=1,
                    status_code=500
                )
            
            logger.error(f"Request processing error: {e}")
            raise
    
    def _identify_business_story(self, endpoint: str) -> Optional[str]:
        """Identify business story from endpoint."""
        story_mapping = {
            "/api/v1/sales": "1",
            "/api/v1/analytics": "1", 
            "/api/v1/datamart": "2",
            "/api/v1/security": "3",
            "/api/v1/auth": "3",
            "/api/v2/analytics": "4",
            "/api/v1/mobile": "4.1",
            "/api/v1/ai": "4.2",
            "/api/v1/llm": "4.2",
            "/api/v1/advanced-security": "6",
            "/api/v1/streaming": "7",
            "/api/v1/ml": "8"
        }
        
        for path_prefix, story_id in story_mapping.items():
            if endpoint.startswith(path_prefix):
                return story_id
        return None
    
    def _extract_cache_level(self, response: Response) -> Optional[str]:
        """Extract cache level from response headers."""
        x_cache = response.headers.get("X-Cache", "").lower()
        
        if "l0" in x_cache or "memory" in x_cache:
            return "L0"
        elif "l1" in x_cache or "redis" in x_cache:
            return "L1"
        elif "l2" in x_cache or "database" in x_cache:
            return "L2"
        elif "l3" in x_cache or "external" in x_cache:
            return "L3"
        
        return None


def setup_comprehensive_performance_monitoring(app: FastAPI, **kwargs) -> Dict[str, Any]:
    """
    Setup comprehensive performance monitoring for FastAPI application.
    
    Returns monitoring components and configuration details.
    """
    
    logger.info("Setting up comprehensive performance monitoring for <15ms SLA validation")
    
    # Initialize monitoring components
    performance_monitor = kwargs.get('performance_monitor') or PerformanceSLAMonitor(
        sla_threshold_ms=kwargs.get('sla_threshold_ms', 15.0),
        sla_target_percentage=kwargs.get('sla_target_percentage', 95.0),
        measurement_window_minutes=kwargs.get('measurement_window_minutes', 5),
        enable_datadog=kwargs.get('enable_datadog', True)
    )
    
    business_monitor = kwargs.get('business_monitor') or BusinessImpactMonitor(
        enable_datadog=kwargs.get('enable_datadog', True)
    )
    
    # Start monitoring
    performance_monitor.start_monitoring()
    
    # Add integrated middleware
    app.add_middleware(
        IntegratedPerformanceMiddleware,
        performance_monitor=performance_monitor,
        business_monitor=business_monitor,
        excluded_paths=kwargs.get('excluded_paths')
    )
    
    # Add monitoring endpoints
    _add_monitoring_endpoints(app, performance_monitor, business_monitor)
    
    monitoring_config = {
        "performance_monitor": performance_monitor,
        "business_monitor": business_monitor,
        "sla_threshold_ms": performance_monitor.sla_threshold_ms,
        "sla_target_percentage": performance_monitor.sla_target_percentage,
        "datadog_enabled": performance_monitor.enable_datadog,
        "monitoring_active": True,
        "business_stories_tracked": len(business_monitor.business_stories),
        "total_business_value": sum(s.business_value_usd for s in business_monitor.business_stories.values())
    }
    
    logger.info(f"Comprehensive performance monitoring configured: "
               f"Target <{monitoring_config['sla_threshold_ms']}ms, "
               f"{monitoring_config['business_stories_tracked']} business stories, "
               f"${monitoring_config['total_business_value']:,.0f} total business value")
    
    return monitoring_config


def _add_monitoring_endpoints(app: FastAPI, 
                             performance_monitor: PerformanceSLAMonitor,
                             business_monitor: BusinessImpactMonitor):
    """Add monitoring endpoints to FastAPI application."""
    
    @app.get("/api/v1/monitoring/performance-summary")
    async def get_performance_summary():
        """Get comprehensive performance monitoring summary."""
        return performance_monitor.get_performance_summary()
    
    @app.get("/api/v1/monitoring/business-dashboard")
    async def get_business_dashboard():
        """Get executive business impact dashboard data."""
        return business_monitor.get_executive_dashboard_data()
    
    @app.get("/api/v1/monitoring/story/{story_id}")
    async def get_story_performance(story_id: str):
        """Get detailed performance report for specific business story."""
        report = business_monitor.get_story_performance_report(story_id)
        if not report:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail=f"Business story '{story_id}' not found")
        return report
    
    @app.get("/api/v1/monitoring/sla-compliance")
    async def get_sla_compliance():
        """Get current SLA compliance metrics."""
        summary = performance_monitor.get_performance_summary()
        current_metrics = summary['current_metrics']
        
        return {
            "sla_threshold_ms": performance_monitor.sla_threshold_ms,
            "sla_target_percentage": performance_monitor.sla_target_percentage,
            "current_compliance_percentage": current_metrics['sla_compliance_percentage'],
            "meets_sla_target": current_metrics['meets_sla_target'],
            "p95_response_time_ms": current_metrics['p95_response_time_ms'],
            "total_requests": current_metrics['total_requests'],
            "compliant_requests": current_metrics['compliant_requests'],
            "cache_hit_rate": summary['cache_performance']['overall_hit_rate'],
            "monitoring_window_minutes": performance_monitor.measurement_window_minutes
        }
    
    @app.get("/api/v1/monitoring/health-check")
    async def monitoring_health_check():
        """Health check for monitoring systems."""
        perf_summary = performance_monitor.get_performance_summary()
        business_dashboard = business_monitor.get_executive_dashboard_data()
        
        return {
            "status": "healthy",
            "performance_monitoring": {
                "active": perf_summary['monitoring_status']['active'],
                "total_measurements": perf_summary['monitoring_status']['total_measurements'],
                "datadog_enabled": perf_summary['monitoring_status']['datadog_enabled']
            },
            "business_monitoring": {
                "stories_monitored": len(business_monitor.business_stories),
                "stories_healthy": business_dashboard['executive_summary']['stories_healthy'],
                "critical_alerts": len(business_dashboard['critical_alerts'])
            },
            "overall_platform_health": business_dashboard['executive_summary']['overall_platform_health'],
            "sla_compliance": business_dashboard['executive_summary']['overall_sla_compliance']
        }
    
    @app.post("/api/v1/monitoring/run-load-test")
    async def trigger_load_test():
        """Trigger comprehensive load testing for SLA validation."""
        try:
            from monitoring.load_testing_sla_validator import run_load_testing_validation
            
            # Run load test asynchronously
            report = await run_load_testing_validation(
                base_url="http://localhost:8000",  # Should be configurable
                config_file=None  # Use default scenarios
            )
            
            return {
                "load_test_triggered": True,
                "report_summary": {
                    "overall_sla_compliance": report['overall_statistics']['overall_sla_compliance'],
                    "total_requests": report['overall_statistics']['total_requests'],
                    "production_ready": report['sla_validation']['production_ready'],
                    "scenarios_tested": report['overall_statistics']['total_scenarios']
                },
                "full_report": report
            }
            
        except Exception as e:
            logger.error(f"Load test execution failed: {e}")
            return {
                "load_test_triggered": False,
                "error": str(e),
                "recommendation": "Check load testing configuration and system availability"
            }
    
    @app.get("/api/v1/monitoring/regression-analysis")
    async def get_regression_analysis():
        """Get performance regression analysis."""
        regressions = performance_monitor.detect_performance_regression()
        
        return {
            "regressions_detected": len(regressions),
            "regressions": regressions,
            "baseline_established": bool(performance_monitor.baseline_percentiles),
            "baseline_metrics": performance_monitor.baseline_percentiles,
            "regression_threshold_percent": performance_monitor.regression_threshold * 100,
            "analysis_timestamp": time.time()
        }


@asynccontextmanager
async def performance_monitoring_lifespan(app: FastAPI):
    """Application lifespan context manager for performance monitoring."""
    
    # Startup
    logger.info("Starting comprehensive performance monitoring")
    
    try:
        # Setup monitoring (this should be called during app initialization)
        # monitoring_config = setup_comprehensive_performance_monitoring(app)
        
        yield
        
    finally:
        # Cleanup
        logger.info("Shutting down performance monitoring")
        
        # Stop monitoring components
        try:
            performance_monitor = get_performance_monitor()
            performance_monitor.stop_monitoring()
        except Exception as e:
            logger.warning(f"Error stopping performance monitor: {e}")


def validate_production_readiness(performance_monitor: PerformanceSLAMonitor,
                                business_monitor: BusinessImpactMonitor) -> Dict[str, Any]:
    """
    Validate production readiness based on comprehensive performance analysis.
    
    Returns detailed readiness assessment for stakeholder review.
    """
    
    # Get current performance data
    perf_summary = performance_monitor.get_performance_summary()
    business_data = business_monitor.get_executive_dashboard_data()
    
    # Readiness criteria
    criteria = {
        "sla_compliance_95_percent": perf_summary['current_metrics']['sla_compliance_percentage'] >= 95.0,
        "p95_response_time_under_15ms": perf_summary['current_metrics']['p95_response_time_ms'] < 15.0,
        "cache_hit_rate_above_95_percent": perf_summary['cache_performance']['overall_hit_rate'] >= 95.0,
        "no_critical_business_stories": business_data['executive_summary']['critical_stories_count'] == 0,
        "overall_roi_above_85_percent": business_data['executive_summary']['overall_roi_percentage'] >= 85.0,
        "revenue_at_risk_minimal": business_data['executive_summary']['current_revenue_at_risk'] < 100000,
        "no_performance_regressions": len(performance_monitor.detect_performance_regression()) == 0
    }
    
    # Calculate readiness score
    criteria_met = sum(1 for met in criteria.values() if met)
    total_criteria = len(criteria)
    readiness_score = (criteria_met / total_criteria) * 100
    
    # Overall assessment
    if readiness_score >= 90:
        readiness_status = "READY"
        confidence_level = "HIGH"
        recommendation = "System meets all production readiness criteria"
    elif readiness_score >= 75:
        readiness_status = "READY_WITH_MONITORING"
        confidence_level = "MEDIUM"
        recommendation = "System ready with enhanced monitoring of identified areas"
    else:
        readiness_status = "NOT_READY"
        confidence_level = "LOW"
        recommendation = "System requires optimization before production deployment"
    
    # Detailed assessment
    failed_criteria = [name for name, met in criteria.items() if not met]
    
    return {
        "readiness_assessment": {
            "status": readiness_status,
            "confidence_level": confidence_level,
            "readiness_score_percentage": readiness_score,
            "criteria_met": criteria_met,
            "total_criteria": total_criteria,
            "recommendation": recommendation
        },
        "criteria_details": criteria,
        "failed_criteria": failed_criteria,
        "current_performance": {
            "sla_compliance": perf_summary['current_metrics']['sla_compliance_percentage'],
            "p95_response_time_ms": perf_summary['current_metrics']['p95_response_time_ms'],
            "cache_hit_rate": perf_summary['cache_performance']['overall_hit_rate'],
            "business_value_protected": business_data['executive_summary']['total_business_value_protected'],
            "revenue_at_risk": business_data['executive_summary']['current_revenue_at_risk']
        },
        "business_impact": {
            "stories_healthy": business_data['executive_summary']['stories_healthy'],
            "stories_total": business_data['executive_summary']['stories_total'],
            "overall_roi": business_data['executive_summary']['overall_roi_percentage'],
            "platform_health": business_data['executive_summary']['overall_platform_health']
        },
        "next_steps": _generate_readiness_next_steps(failed_criteria, perf_summary, business_data),
        "validation_timestamp": datetime.utcnow().isoformat()
    }


def _generate_readiness_next_steps(failed_criteria: List[str], 
                                 perf_summary: Dict, 
                                 business_data: Dict) -> List[str]:
    """Generate actionable next steps for production readiness."""
    next_steps = []
    
    if "sla_compliance_95_percent" in failed_criteria:
        next_steps.append("🎯 Optimize API performance to achieve 95%+ SLA compliance")
        next_steps.append("🔍 Identify and fix performance bottlenecks in slowest endpoints")
    
    if "p95_response_time_under_15ms" in failed_criteria:
        current_p95 = perf_summary['current_metrics']['p95_response_time_ms']
        next_steps.append(f"⚡ Reduce P95 response time from {current_p95:.1f}ms to <15ms")
        next_steps.append("🚀 Implement additional caching layers and query optimization")
    
    if "cache_hit_rate_above_95_percent" in failed_criteria:
        current_rate = perf_summary['cache_performance']['overall_hit_rate']
        next_steps.append(f"💾 Improve cache hit rate from {current_rate:.1f}% to >95%")
        next_steps.append("🔧 Review and optimize caching strategies for critical endpoints")
    
    if "no_critical_business_stories" in failed_criteria:
        critical_count = business_data['executive_summary']['critical_stories_count']
        next_steps.append(f"🚨 Address {critical_count} critical business stories immediately")
        next_steps.append("🎯 Focus on high-value stories with performance issues")
    
    if "overall_roi_above_85_percent" in failed_criteria:
        current_roi = business_data['executive_summary']['overall_roi_percentage']
        next_steps.append(f"📈 Improve overall ROI from {current_roi:.1f}% to >85%")
        next_steps.append("💡 Optimize performance for highest business value endpoints")
    
    if "revenue_at_risk_minimal" in failed_criteria:
        at_risk = business_data['executive_summary']['current_revenue_at_risk']
        next_steps.append(f"💰 Reduce revenue at risk from ${at_risk:,.2f} to <$100K")
    
    # Always include monitoring step
    next_steps.append("📊 Continue comprehensive monitoring during optimization")
    next_steps.append("🔄 Re-run production readiness validation after improvements")
    
    return next_steps


# Export key functions and classes
__all__ = [
    'IntegratedPerformanceMiddleware',
    'setup_comprehensive_performance_monitoring', 
    'performance_monitoring_lifespan',
    'validate_production_readiness'
]
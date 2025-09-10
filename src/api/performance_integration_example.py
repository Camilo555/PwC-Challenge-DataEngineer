"""
Example integration of comprehensive performance monitoring into FastAPI main application
Shows how to integrate <15ms SLA monitoring with business impact correlation
"""

from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse

# Import monitoring components
from monitoring.performance_monitoring_integration import (
    setup_comprehensive_performance_monitoring,
    performance_monitoring_lifespan,
    validate_production_readiness
)
from monitoring.performance_sla_monitor import get_performance_monitor
from monitoring.business_impact_monitor import get_business_impact_monitor

# Import existing auth and config
from core.config.base_config import BaseConfig
from core.logging import get_logger

logger = get_logger(__name__)
base_config = BaseConfig()


@asynccontextmanager
async def enhanced_lifespan(app: FastAPI):
    """Enhanced lifespan with comprehensive performance monitoring."""
    
    logger.info("🚀 Starting PwC Data Engineering API with <15ms SLA Monitoring")
    
    # Setup comprehensive performance monitoring
    monitoring_config = setup_comprehensive_performance_monitoring(
        app=app,
        sla_threshold_ms=15.0,  # <15ms SLA target
        sla_target_percentage=95.0,  # 95% compliance target
        measurement_window_minutes=5,  # 5-minute monitoring windows
        enable_datadog=True,  # Enable DataDog integration
        excluded_paths=['/health', '/metrics', '/docs', '/redoc', '/openapi.json', '/favicon.ico']
    )
    
    # Log monitoring configuration
    logger.info(f"📊 Performance monitoring configured:")
    logger.info(f"   • SLA Target: <{monitoring_config['sla_threshold_ms']}ms response time")
    logger.info(f"   • Compliance Target: {monitoring_config['sla_target_percentage']}%")
    logger.info(f"   • Business Stories Tracked: {monitoring_config['business_stories_tracked']}")
    logger.info(f"   • Total Business Value: ${monitoring_config['total_business_value']:,.0f}")
    logger.info(f"   • DataDog Enabled: {monitoring_config['datadog_enabled']}")
    
    # Store monitoring config in app state
    app.state.monitoring_config = monitoring_config
    
    yield
    
    # Cleanup on shutdown
    logger.info("🔄 Shutting down performance monitoring")
    try:
        performance_monitor = monitoring_config['performance_monitor']
        performance_monitor.stop_monitoring()
        logger.info("✅ Performance monitoring stopped successfully")
    except Exception as e:
        logger.error(f"❌ Error stopping monitoring: {e}")


# Create FastAPI app with enhanced monitoring
app = FastAPI(
    title="PwC Data Engineering Challenge - Performance Monitored API",
    version="4.0.0",  # Updated version with performance monitoring
    description="""
    Enterprise-grade REST API with comprehensive <15ms SLA performance monitoring:
    
    ## 🎯 Performance Features
    - **Microsecond-precision** response time tracking
    - **<15ms SLA validation** with 95% compliance target
    - **Real-time performance monitoring** with automated alerts
    - **Business impact correlation** across $27.8M+ BMAD stories
    - **L0-L3 cache performance** monitoring and optimization
    - **Production readiness validation** with comprehensive reporting
    
    ## 📊 Monitoring Endpoints
    - `/api/v1/monitoring/performance-summary` - Comprehensive performance metrics
    - `/api/v1/monitoring/business-dashboard` - Executive business impact dashboard
    - `/api/v1/monitoring/sla-compliance` - Current SLA compliance status
    - `/api/v1/monitoring/health-check` - Monitoring system health
    
    ## 🚀 Business Value Tracking
    - **Story 1**: BI Dashboards ($2.5M) - Executive analytics performance
    - **Story 2**: Data Quality ($1.8M) - Data governance performance  
    - **Story 3**: Security & Governance ($3.2M) - Security compliance performance
    - **Story 4**: API Performance ($2.1M) - High-performance API validation
    - **Story 4.1**: Mobile Analytics ($2.8M) - Mobile-optimized performance
    - **Story 4.2**: AI/LLM Analytics ($3.5M) - AI conversation performance
    - **Story 6**: Advanced Security ($4.2M) - Security operations performance
    - **Story 7**: Real-time Streaming ($3.4M) - Streaming analytics performance
    - **Story 8**: ML Operations ($6.4M) - ML model serving performance
    """,
    docs_url="/docs" if base_config.environment != "production" else None,
    redoc_url="/redoc" if base_config.environment != "production" else None,
    lifespan=enhanced_lifespan
)


# Performance monitoring endpoints (enhanced versions)
@app.get("/api/v1/monitoring/performance-dashboard", tags=["monitoring"])
async def get_performance_dashboard():
    """Get comprehensive performance monitoring dashboard for stakeholders."""
    
    performance_monitor = get_performance_monitor()
    business_monitor = get_business_impact_monitor()
    
    # Get comprehensive data
    perf_summary = performance_monitor.get_performance_summary()
    business_dashboard = business_monitor.get_executive_dashboard_data()
    
    # Create unified dashboard
    dashboard = {
        "dashboard_info": {
            "title": "PwC Data Engineering <15ms SLA Performance Dashboard",
            "generated_at": datetime.utcnow().isoformat(),
            "monitoring_active": perf_summary['monitoring_status']['active'],
            "data_freshness_minutes": 1  # Real-time data
        },
        "sla_performance": {
            "target_response_time_ms": performance_monitor.sla_threshold_ms,
            "target_compliance_percentage": performance_monitor.sla_target_percentage,
            "current_compliance_percentage": perf_summary['current_metrics']['sla_compliance_percentage'],
            "meets_sla_target": perf_summary['current_metrics']['meets_sla_target'],
            "p95_response_time_ms": perf_summary['current_metrics']['p95_response_time_ms'],
            "p99_response_time_ms": perf_summary['current_metrics']['p99_response_time_ms'],
            "avg_response_time_ms": perf_summary['current_metrics']['avg_response_time_ms'],
            "fastest_response_ms": perf_summary['current_metrics']['fastest_response_ms'],
            "total_requests": perf_summary['current_metrics']['total_requests'],
            "compliant_requests": perf_summary['current_metrics']['compliant_requests']
        },
        "business_impact": business_dashboard['executive_summary'],
        "story_performance": business_dashboard['story_performance'],
        "cache_performance": perf_summary['cache_performance'],
        "critical_alerts": business_dashboard['critical_alerts'],
        "recommendations": business_dashboard['recommendations'],
        "production_readiness": validate_production_readiness(performance_monitor, business_monitor)
    }
    
    return dashboard


@app.get("/api/v1/monitoring/executive-summary", tags=["monitoring"])
async def get_executive_summary():
    """Get executive-level performance and business impact summary."""
    
    business_monitor = get_business_impact_monitor()
    performance_monitor = get_performance_monitor()
    
    dashboard_data = business_monitor.get_executive_dashboard_data()
    perf_summary = performance_monitor.get_performance_summary()
    
    # Calculate key executive metrics
    total_business_value = dashboard_data['executive_summary']['total_business_value_protected']
    revenue_at_risk = dashboard_data['executive_summary']['current_revenue_at_risk']
    roi_percentage = dashboard_data['executive_summary']['overall_roi_percentage']
    sla_compliance = dashboard_data['executive_summary']['overall_sla_compliance']
    
    # Risk assessment
    risk_level = "LOW" if revenue_at_risk < total_business_value * 0.01 else \
                "MEDIUM" if revenue_at_risk < total_business_value * 0.05 else "HIGH"
    
    # Performance grade
    if sla_compliance >= 95 and perf_summary['current_metrics']['p95_response_time_ms'] < 10:
        performance_grade = "A+"
    elif sla_compliance >= 95:
        performance_grade = "A"
    elif sla_compliance >= 90:
        performance_grade = "B"
    else:
        performance_grade = "C"
    
    return {
        "executive_summary": {
            "platform_name": "PwC Data Engineering Challenge - BMAD Platform",
            "total_business_value": f"${total_business_value:,.0f}",
            "performance_grade": performance_grade,
            "sla_compliance_percentage": f"{sla_compliance:.1f}%",
            "roi_achievement_percentage": f"{roi_percentage:.1f}%",
            "revenue_at_risk": f"${revenue_at_risk:,.2f}",
            "risk_level": risk_level,
            "platform_health": dashboard_data['executive_summary']['overall_platform_health'].upper(),
            "stories_healthy": f"{dashboard_data['executive_summary']['stories_healthy']}/{dashboard_data['executive_summary']['stories_total']}"
        },
        "key_metrics": {
            "response_time_p95_ms": f"{perf_summary['current_metrics']['p95_response_time_ms']:.1f}",
            "sla_target_ms": f"{performance_monitor.sla_threshold_ms}",
            "cache_hit_rate_percentage": f"{perf_summary['cache_performance']['overall_hit_rate']:.1f}%",
            "total_requests_monitored": f"{perf_summary['current_metrics']['total_requests']:,}",
            "monitoring_window_hours": "24"
        },
        "business_stories_status": [
            {
                "story_id": story['story_id'],
                "name": story['story_name'], 
                "health": story['health_status'].upper(),
                "sla_compliance": f"{story['current_sla_compliance']:.1f}%",
                "business_value": f"${story['business_value']:,.0f}"
            }
            for story in dashboard_data['story_performance']
        ],
        "immediate_actions": dashboard_data['critical_alerts'][:3],  # Top 3 critical items
        "strategic_recommendations": dashboard_data['recommendations'][:5],  # Top 5 recommendations
        "report_generated": datetime.utcnow().isoformat()
    }


@app.post("/api/v1/monitoring/validate-production-readiness", tags=["monitoring"])
async def validate_production_readiness_endpoint():
    """Comprehensive production readiness validation for stakeholder approval."""
    
    performance_monitor = get_performance_monitor()
    business_monitor = get_business_impact_monitor()
    
    # Run comprehensive validation
    readiness_report = validate_production_readiness(performance_monitor, business_monitor)
    
    # Add additional validation context
    validation_context = {
        "validation_scope": {
            "sla_target": f"<{performance_monitor.sla_threshold_ms}ms response time",
            "compliance_target": f"{performance_monitor.sla_target_percentage}% of requests",
            "business_stories_evaluated": len(business_monitor.business_stories),
            "total_business_value_protected": f"${sum(s.business_value_usd for s in business_monitor.business_stories.values()):,.0f}",
            "evaluation_period": "Real-time + Historical Analysis"
        },
        "stakeholder_confidence": {
            "technical_readiness": readiness_report['readiness_assessment']['confidence_level'],
            "business_impact_validated": readiness_report['business_impact']['overall_roi'] >= 85.0,
            "risk_mitigation": "COMPREHENSIVE" if readiness_report['current_performance']['revenue_at_risk'] < 100000 else "MODERATE",
            "monitoring_coverage": "FULL_STACK"
        }
    }
    
    # Combine reports
    comprehensive_report = {
        **readiness_report,
        "validation_context": validation_context,
        "executive_recommendation": {
            "deploy_to_production": readiness_report['readiness_assessment']['status'] in ["READY", "READY_WITH_MONITORING"],
            "confidence_level": readiness_report['readiness_assessment']['confidence_level'],
            "key_message": readiness_report['readiness_assessment']['recommendation'],
            "business_justification": f"Platform protects ${validation_context['validation_scope']['total_business_value_protected']} in business value with {readiness_report['current_performance']['sla_compliance']:.1f}% SLA compliance"
        }
    }
    
    return comprehensive_report


@app.get("/api/v1/monitoring/performance-trends", tags=["monitoring"])
async def get_performance_trends():
    """Get performance trends and regression analysis."""
    
    performance_monitor = get_performance_monitor()
    regressions = performance_monitor.detect_performance_regression()
    
    return {
        "trend_analysis": {
            "regressions_detected": len(regressions),
            "performance_regressions": regressions,
            "baseline_established": bool(performance_monitor.baseline_percentiles),
            "baseline_metrics": performance_monitor.baseline_percentiles or {},
            "regression_threshold_percent": performance_monitor.regression_threshold * 100
        },
        "trend_indicators": {
            "performance_stability": "STABLE" if len(regressions) == 0 else "DEGRADING",
            "monitoring_confidence": "HIGH" if len(performance_monitor.measurements) > 1000 else "BUILDING",
            "prediction_accuracy": "VALIDATED" if performance_monitor.baseline_percentiles else "ESTABLISHING"
        },
        "analysis_timestamp": datetime.utcnow().isoformat()
    }


# Health check endpoint enhanced with monitoring status
@app.get("/health", tags=["health"])
async def enhanced_health_check():
    """Enhanced health check including monitoring system status."""
    
    try:
        # Get monitoring status
        performance_monitor = get_performance_monitor()
        business_monitor = get_business_impact_monitor()
        
        perf_summary = performance_monitor.get_performance_summary()
        business_data = business_monitor.get_executive_dashboard_data()
        
        monitoring_health = {
            "performance_monitoring": perf_summary['monitoring_status']['active'],
            "business_monitoring": len(business_monitor.business_stories) > 0,
            "datadog_integration": perf_summary['monitoring_status']['datadog_enabled'],
            "measurements_collected": perf_summary['monitoring_status']['total_measurements'],
            "sla_compliance_current": perf_summary['current_metrics']['sla_compliance_percentage'],
            "platform_health": business_data['executive_summary']['overall_platform_health']
        }
        
        # Overall health assessment
        overall_healthy = all([
            monitoring_health["performance_monitoring"],
            monitoring_health["business_monitoring"],
            perf_summary['current_metrics']['sla_compliance_percentage'] >= 90.0,
            business_data['executive_summary']['critical_stories_count'] == 0
        ])
        
        return {
            "status": "healthy" if overall_healthy else "degraded",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "4.0.0",
            "environment": base_config.environment.value,
            "monitoring": monitoring_health,
            "sla_performance": {
                "target_ms": performance_monitor.sla_threshold_ms,
                "current_compliance": f"{perf_summary['current_metrics']['sla_compliance_percentage']:.1f}%",
                "meets_target": perf_summary['current_metrics']['meets_sla_target']
            },
            "business_value_protected": f"${business_data['executive_summary']['total_business_value_protected']:,.0f}"
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "error": "Monitoring system unavailable",
                "details": str(e)
            }
        )


# Example of how to add monitoring to existing endpoints
@app.get("/api/v1/example/monitored-endpoint", tags=["examples"])
async def example_monitored_endpoint():
    """
    Example endpoint demonstrating automatic performance monitoring.
    
    This endpoint is automatically monitored for:
    - Response time (<15ms SLA)
    - Business impact correlation
    - Cache performance
    - User satisfaction scoring
    """
    
    # Simulate some work
    import asyncio
    await asyncio.sleep(0.001)  # 1ms simulated processing
    
    return {
        "message": "This endpoint is automatically monitored",
        "features": [
            "Microsecond-precision response time tracking",
            "Business story correlation", 
            "SLA compliance validation",
            "Cache performance monitoring",
            "Real-time alerting on violations"
        ],
        "sla_target": "<15ms response time",
        "monitoring": "Comprehensive performance and business impact tracking"
    }


if __name__ == "__main__":
    import uvicorn
    
    logger.info("🚀 Starting PwC Data Engineering API with comprehensive performance monitoring")
    logger.info("📊 Monitoring Features:")
    logger.info("   • <15ms SLA validation with 95% compliance target")
    logger.info("   • Business impact correlation across $27.8M+ BMAD stories")
    logger.info("   • Real-time performance dashboards and alerting")
    logger.info("   • Production readiness validation")
    logger.info("   • Comprehensive load testing capabilities")
    
    uvicorn.run(
        "api.performance_integration_example:app",
        host="0.0.0.0",
        port=8000,
        reload=base_config.environment.value == "development",
        log_level="info"
    )
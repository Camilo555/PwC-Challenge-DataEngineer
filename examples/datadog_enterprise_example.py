"""
DataDog Enterprise Platform Usage Example
Demonstrates how to use the comprehensive DataDog enterprise monitoring system
"""

import asyncio
import os
from datetime import datetime, timedelta

# Import the enterprise platform and related components
from src.monitoring.datadog_enterprise_platform import (
    DataDogEnterprisePlatform, 
    PlatformConfiguration, 
    DeploymentEnvironment,
    create_enterprise_platform,
    create_development_config,
    create_production_config
)

# Import specific dashboard and metric types
from src.monitoring.datadog_executive_dashboards import ExecutiveDashboardType
from src.monitoring.datadog_technical_operations_dashboards import TechnicalDashboardType
from src.monitoring.datadog_ml_operations_dashboards import MLDashboardType, DriftType
from src.monitoring.datadog_role_based_access import Role, Permission, ResourceType, AccessLevel


async def main():
    """Main example demonstrating DataDog Enterprise Platform usage"""
    
    print("🚀 DataDog Enterprise Platform Example")
    print("=" * 50)
    
    # Configuration - replace with your actual DataDog API keys
    api_key = os.getenv("DATADOG_API_KEY", "your_api_key_here")
    app_key = os.getenv("DATADOG_APP_KEY", "your_app_key_here")
    
    # Create configuration for development environment
    config = create_development_config(api_key, app_key)
    
    # For production, you would use:
    # config = create_production_config(api_key, app_key, region="us1")
    
    print(f"🔧 Configuration created for {config.environment.value} environment")
    print(f"📊 Service: {config.service_name}")
    print(f"🌍 Region: {config.region}")
    print()
    
    # Initialize and run the enterprise platform
    async with create_enterprise_platform(config) as platform:
        
        print("✅ DataDog Enterprise Platform initialized successfully!")
        print()
        
        # Example 1: Get platform health status
        await example_platform_health(platform)
        print()
        
        # Example 2: Create executive dashboards
        await example_executive_dashboards(platform)
        print()
        
        # Example 3: Track business metrics
        await example_business_metrics(platform)
        print()
        
        # Example 4: ML operations monitoring
        await example_ml_operations(platform)
        print()
        
        # Example 5: Technical operations monitoring
        await example_technical_operations(platform)
        print()
        
        # Example 6: Enterprise alerting
        await example_enterprise_alerting(platform)
        print()
        
        # Example 7: Role-based access control
        await example_rbac_system(platform)
        print()
        
        # Example 8: Dashboard automation
        await example_dashboard_automation(platform)
        print()
        
        # Get final platform summary
        summary = platform.get_platform_summary()
        print("📋 Final Platform Summary:")
        print(f"   • Total Components: {summary['components']['total_components']}")
        print(f"   • Uptime: {summary['uptime']['formatted']}")
        print(f"   • Performance Metrics: {summary['performance']['total_dashboards']} dashboards")
        print()
        
        print("🎉 DataDog Enterprise Platform example completed successfully!")


async def example_platform_health(platform: DataDogEnterprisePlatform):
    """Example: Monitor platform health"""
    
    print("💊 Platform Health Monitoring")
    print("-" * 30)
    
    try:
        # Get comprehensive health status
        health = await platform.get_platform_health()
        
        print(f"Overall Status: {health.overall_status}")
        print(f"Uptime: {health.uptime_percentage:.1f}%")
        print(f"Total Dashboards: {health.total_dashboards}")
        print(f"Total Metrics: {health.total_metrics}")
        print(f"Total Alerts: {health.total_alerts}")
        print(f"Active Users: {health.active_users}")
        
        # Show component status
        print("\nComponent Status:")
        for component, status in health.component_statuses.items():
            status_icon = "✅" if status.status == "healthy" else "❌"
            print(f"   {status_icon} {component.value}: {status.status}")
            
    except Exception as e:
        print(f"Error monitoring platform health: {str(e)}")


async def example_executive_dashboards(platform: DataDogEnterprisePlatform):
    """Example: Create executive dashboards"""
    
    print("📊 Executive Dashboard Creation")
    print("-" * 30)
    
    try:
        from src.monitoring.datadog_enterprise_platform import PlatformComponent
        
        # Get executive dashboard component
        if PlatformComponent.EXECUTIVE_DASHBOARDS in platform.components:
            executive_dashboards = platform.components[PlatformComponent.EXECUTIVE_DASHBOARDS]
            
            # Create CEO overview dashboard
            ceo_dashboard_id = await executive_dashboards.create_executive_dashboard(
                ExecutiveDashboardType.CEO_OVERVIEW,
                custom_title="CEO Executive Overview - Real-time KPIs"
            )
            
            if ceo_dashboard_id:
                print(f"✅ CEO Dashboard created: {ceo_dashboard_id}")
            else:
                print("❌ Failed to create CEO Dashboard")
            
            # Create financial overview dashboard
            finance_dashboard_id = await executive_dashboards.create_executive_dashboard(
                ExecutiveDashboardType.FINANCIAL_OVERVIEW,
                custom_title="Financial Performance Dashboard"
            )
            
            if finance_dashboard_id:
                print(f"✅ Financial Dashboard created: {finance_dashboard_id}")
            else:
                print("❌ Failed to create Financial Dashboard")
                
        else:
            print("Executive dashboards component not available")
            
    except Exception as e:
        print(f"Error creating executive dashboards: {str(e)}")


async def example_business_metrics(platform: DataDogEnterprisePlatform):
    """Example: Track business metrics"""
    
    print("💼 Business Metrics Tracking")
    print("-" * 30)
    
    try:
        from src.monitoring.datadog_enterprise_platform import PlatformComponent
        
        # Get business metrics component
        if PlatformComponent.BUSINESS_METRICS in platform.components:
            business_metrics = platform.components[PlatformComponent.BUSINESS_METRICS]
            
            # Track various business KPIs
            await business_metrics.track_kpi("daily_revenue", 125000.50, tags={"currency": "USD"})
            print("✅ Tracked daily revenue: $125,000.50")
            
            await business_metrics.track_kpi("customer_acquisition_cost", 45.75, tags={"channel": "digital"})
            print("✅ Tracked customer acquisition cost: $45.75")
            
            await business_metrics.track_kpi("customer_lifetime_value", 2150.00, tags={"segment": "premium"})
            print("✅ Tracked customer lifetime value: $2,150.00")
            
            await business_metrics.track_kpi("monthly_active_users", 85000, tags={"platform": "web"})
            print("✅ Tracked monthly active users: 85,000")
            
            await business_metrics.track_kpi("conversion_rate", 3.45, tags={"funnel": "signup"})
            print("✅ Tracked conversion rate: 3.45%")
            
            # Track custom business event
            await business_metrics.track_business_event(
                event_type="product_launch",
                event_data={
                    "product_name": "Enterprise Analytics Pro",
                    "launch_date": datetime.utcnow().isoformat(),
                    "target_revenue": 500000,
                    "expected_customers": 1000
                },
                tags={"category": "product", "priority": "high"}
            )
            print("✅ Tracked product launch event")
            
        else:
            print("Business metrics component not available")
            
    except Exception as e:
        print(f"Error tracking business metrics: {str(e)}")


async def example_ml_operations(platform: DataDogEnterprisePlatform):
    """Example: ML operations monitoring"""
    
    print("🤖 ML Operations Monitoring")
    print("-" * 30)
    
    try:
        from src.monitoring.datadog_enterprise_platform import PlatformComponent
        
        # Get ML operations component
        if PlatformComponent.ML_DASHBOARDS in platform.components:
            ml_ops = platform.components[PlatformComponent.ML_DASHBOARDS]
            
            # Track model performance
            model_metrics = {
                "accuracy": 0.94,
                "precision": 0.91,
                "recall": 0.89,
                "f1_score": 0.90,
                "inference_latency": 145.5  # milliseconds
            }
            
            success = await ml_ops.track_model_performance(
                model_id="customer_churn_v1",
                metrics=model_metrics,
                model_version="1.2.0"
            )
            
            if success:
                print("✅ Tracked model performance for customer_churn_v1")
                for metric, value in model_metrics.items():
                    print(f"   • {metric}: {value}")
            else:
                print("❌ Failed to track model performance")
            
            # Track model drift
            drift_detected = await ml_ops.track_model_drift(
                model_id="customer_churn_v1",
                drift_type=DriftType.DATA_DRIFT,
                drift_score=0.08,
                feature_name="customer_age"
            )
            
            if drift_detected:
                print("✅ Tracked model drift for customer_age feature")
            
            # Track A/B experiment
            experiment_tracked = await ml_ops.track_ab_experiment(
                experiment_id="churn_model_v1_vs_v2",
                variant="model_v2",
                conversion_type="retention_improvement",
                conversion_value=0.12,  # 12% improvement
                user_count=1000
            )
            
            if experiment_tracked:
                print("✅ Tracked A/B experiment results")
            
            # Create ML dashboard
            ml_dashboard_id = await ml_ops.create_ml_dashboard(
                MLDashboardType.MODEL_PERFORMANCE,
                custom_title="ML Model Performance - Production Monitoring"
            )
            
            if ml_dashboard_id:
                print(f"✅ ML Performance Dashboard created: {ml_dashboard_id}")
                
        else:
            print("ML operations component not available")
            
    except Exception as e:
        print(f"Error with ML operations: {str(e)}")


async def example_technical_operations(platform: DataDogEnterprisePlatform):
    """Example: Technical operations monitoring"""
    
    print("⚙️ Technical Operations Monitoring")
    print("-" * 30)
    
    try:
        from src.monitoring.datadog_enterprise_platform import PlatformComponent
        
        # Get technical operations component
        if PlatformComponent.TECHNICAL_DASHBOARDS in platform.components:
            tech_ops = platform.components[PlatformComponent.TECHNICAL_DASHBOARDS]
            
            # Track technical metrics
            await tech_ops.track_technical_metric("cpu_utilization", 65.4, tags={"host": "web-01"})
            print("✅ Tracked CPU utilization: 65.4%")
            
            await tech_ops.track_technical_metric("api_response_time_p95", 247.8, tags={"endpoint": "/api/v1/users"})
            print("✅ Tracked API response time P95: 247.8ms")
            
            await tech_ops.track_technical_metric("database_connection_pool", 78.5, tags={"db": "primary"})
            print("✅ Tracked database connection pool: 78.5%")
            
            # Collect system metrics
            system_metrics = await tech_ops.collect_system_metrics()
            if system_metrics:
                print("✅ Collected system metrics:")
                metrics = system_metrics.get("metrics", {})
                print(f"   • CPU: {metrics.get('cpu_percent', 0):.1f}%")
                print(f"   • Memory: {metrics.get('memory_percent', 0):.1f}%")
                print(f"   • Disk: {metrics.get('disk_percent', 0):.1f}%")
            
            # Perform health checks
            health_results = await tech_ops.perform_health_checks()
            if health_results:
                print("✅ Health checks completed:")
                print(f"   • Overall healthy: {health_results['overall_healthy']}")
                print(f"   • Services checked: {health_results['total_count']}")
                print(f"   • Healthy services: {health_results['healthy_count']}")
            
            # Create infrastructure dashboard
            infra_dashboard_id = await tech_ops.create_technical_dashboard(
                TechnicalDashboardType.INFRASTRUCTURE_OVERVIEW,
                custom_title="Infrastructure Overview - System Performance"
            )
            
            if infra_dashboard_id:
                print(f"✅ Infrastructure Dashboard created: {infra_dashboard_id}")
                
        else:
            print("Technical operations component not available")
            
    except Exception as e:
        print(f"Error with technical operations: {str(e)}")


async def example_enterprise_alerting(platform: DataDogEnterprisePlatform):
    """Example: Enterprise alerting system"""
    
    print("🚨 Enterprise Alerting System")
    print("-" * 30)
    
    try:
        from src.monitoring.datadog_enterprise_platform import PlatformComponent
        
        # Get enterprise alerting component
        if PlatformComponent.ENTERPRISE_ALERTING in platform.components:
            enterprise_alerting = platform.components[PlatformComponent.ENTERPRISE_ALERTING]
            
            # Get alerting summary
            if hasattr(enterprise_alerting, 'get_enterprise_alerting_summary'):
                summary = enterprise_alerting.get_enterprise_alerting_summary()
                
                print("✅ Enterprise Alerting Status:")
                print(f"   • Total Rules: {summary.get('enterprise_rules', {}).get('total', 0)}")
                print(f"   • Security Rules: {summary.get('security_rules', {}).get('total', 0)}")
                print(f"   • Capacity Metrics: {summary.get('capacity_planning', {}).get('total_metrics', 0)}")
                print(f"   • Compliance Violations: {summary.get('compliance', {}).get('total_violations', 0)}")
                
                # Show rules by business impact
                rules_by_impact = summary.get('enterprise_rules', {}).get('by_business_impact', {})
                if rules_by_impact:
                    print("   • Rules by Business Impact:")
                    for impact, count in rules_by_impact.items():
                        print(f"     - {impact}: {count}")
                        
        else:
            print("Enterprise alerting component not available")
            
    except Exception as e:
        print(f"Error with enterprise alerting: {str(e)}")


async def example_rbac_system(platform: DataDogEnterprisePlatform):
    """Example: Role-based access control"""
    
    print("🔐 Role-Based Access Control")
    print("-" * 30)
    
    try:
        from src.monitoring.datadog_enterprise_platform import PlatformComponent
        
        # Get RBAC component
        if PlatformComponent.RBAC_SYSTEM in platform.components:
            rbac = platform.components[PlatformComponent.RBAC_SYSTEM]
            
            # Get RBAC summary
            if hasattr(rbac, 'get_rbac_summary'):
                summary = rbac.get_rbac_summary()
                
                print("✅ RBAC System Status:")
                print(f"   • Total Users: {summary.get('users', {}).get('total_users', 0)}")
                print(f"   • Active Users: {summary.get('users', {}).get('active_users', 0)}")
                print(f"   • Active Sessions: {summary.get('sessions', {}).get('active_sessions', 0)}")
                print(f"   • Pending Requests: {summary.get('access_requests', {}).get('pending_requests', 0)}")
                print(f"   • Audit Entries: {summary.get('audit', {}).get('total_audit_entries', 0)}")
                
                # Show users by role
                users_by_role = summary.get('users', {}).get('users_by_role', {})
                if users_by_role:
                    print("   • Users by Role:")
                    for role, count in users_by_role.items():
                        print(f"     - {role}: {count}")
            
            # Example: Check permissions
            has_dashboard_permission = await rbac.check_permission(
                user_id="admin",
                permission=Permission.DASHBOARD_CREATE
            )
            print(f"   • Admin can create dashboards: {has_dashboard_permission}")
            
            has_sensitive_data_permission = await rbac.check_permission(
                user_id="analyst",
                permission=Permission.DATA_SENSITIVE_VIEW
            )
            print(f"   • Analyst can view sensitive data: {has_sensitive_data_permission}")
                        
        else:
            print("RBAC system not available (disabled in development)")
            
    except Exception as e:
        print(f"Error with RBAC system: {str(e)}")


async def example_dashboard_automation(platform: DataDogEnterprisePlatform):
    """Example: Dashboard automation"""
    
    print("🤖 Dashboard Automation")
    print("-" * 30)
    
    try:
        from src.monitoring.datadog_enterprise_platform import PlatformComponent
        
        # Get dashboard automation component
        if PlatformComponent.DASHBOARD_AUTOMATION in platform.components:
            dashboard_automation = platform.components[PlatformComponent.DASHBOARD_AUTOMATION]
            
            # Get automation summary
            if hasattr(dashboard_automation, 'get_automation_summary'):
                summary = dashboard_automation.get_automation_summary()
                
                print("✅ Dashboard Automation Status:")
                print(f"   • Template Types: {summary.get('templates', {}).get('total', 0)}")
                print(f"   • Total Configurations: {summary.get('configurations', {}).get('total', 0)}")
                print(f"   • Auto-update Enabled: {summary.get('configurations', {}).get('auto_update_enabled', 0)}")
                print(f"   • Scheduled Reports: {summary.get('scheduled_reports', {}).get('total', 0)}")
                
                # Show performance metrics
                performance = summary.get('performance', {})
                print(f"   • Dashboards Created: {performance.get('dashboards_created', 0)}")
                print(f"   • Dashboards Updated: {performance.get('dashboards_updated', 0)}")
                print(f"   • Reports Generated: {performance.get('reports_generated', 0)}")
                
                # Show templates by scope
                templates_by_scope = summary.get('templates', {}).get('by_scope', {})
                if templates_by_scope:
                    print("   • Templates by Scope:")
                    for scope, count in templates_by_scope.items():
                        print(f"     - {scope}: {count}")
                        
        else:
            print("Dashboard automation component not available")
            
    except Exception as e:
        print(f"Error with dashboard automation: {str(e)}")


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
#!/usr/bin/env python3
"""
Monitoring Optimizations Validation Script
Validates comprehensive monitoring setup and optimizations.
"""

import asyncio
import logging
from pathlib import Path
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import subprocess

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

try:
    from monitoring.datadog_comprehensive_alerting import DatadogAlertingOrchestrator
    from monitoring.datadog_enterprise_platform import DatadogEnterprisePlatform
    from monitoring.prometheus_metrics import PrometheusMetricsCollector
    from monitoring.health_checks import HealthCheckManager
    from monitoring.intelligent_alerting import IntelligentAlertingSystem
    from core.monitoring.metrics import MetricsCollector
except ImportError as e:
    logging.warning(f"Import warning: {e}")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MonitoringValidator:
    """Validates monitoring system components and optimizations."""
    
    def __init__(self):
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "validations": [],
            "overall_status": "unknown",
            "recommendations": []
        }
    
    def validate_datadog_setup(self) -> Dict[str, Any]:
        """Validate DataDog monitoring setup."""
        validation = {
            "component": "DataDog Setup",
            "status": "checking",
            "details": []
        }
        
        try:
            # Check DataDog configuration files
            datadog_configs = [
                project_root / "infrastructure/monitoring/datadog",
                project_root / "src/monitoring"
            ]
            
            for config_path in datadog_configs:
                if config_path.exists():
                    validation["details"].append(f"Found DataDog config at {config_path}")
                    validation["status"] = "pass"
                else:
                    validation["details"].append(f"Missing DataDog config at {config_path}")
            
            # Check for DataDog components
            datadog_components = [
                "datadog_comprehensive_alerting.py",
                "datadog_enterprise_platform.py",
                "datadog_synthetic_monitoring.py",
                "datadog_log_visualization.py"
            ]
            
            monitoring_dir = project_root / "src/monitoring"
            for component in datadog_components:
                component_path = monitoring_dir / component
                if component_path.exists():
                    validation["details"].append(f"✓ Found {component}")
                else:
                    validation["details"].append(f"✗ Missing {component}")
                    validation["status"] = "warn"
                    
        except Exception as e:
            validation["status"] = "error"
            validation["details"].append(f"Error validating DataDog setup: {str(e)}")
        
        return validation
    
    def validate_prometheus_setup(self) -> Dict[str, Any]:
        """Validate Prometheus monitoring setup."""
        validation = {
            "component": "Prometheus Setup",
            "status": "checking",
            "details": []
        }
        
        try:
            prometheus_files = [
                "prometheus_metrics.py",
                "prometheus_config.py",
                "prometheus_metrics_exporter.py"
            ]
            
            monitoring_dir = project_root / "src/monitoring"
            found_files = 0
            
            for prom_file in prometheus_files:
                file_path = monitoring_dir / prom_file
                if file_path.exists():
                    validation["details"].append(f"✓ Found {prom_file}")
                    found_files += 1
                else:
                    validation["details"].append(f"✗ Missing {prom_file}")
            
            if found_files >= 2:
                validation["status"] = "pass"
            elif found_files >= 1:
                validation["status"] = "warn"
            else:
                validation["status"] = "error"
                
        except Exception as e:
            validation["status"] = "error"
            validation["details"].append(f"Error validating Prometheus setup: {str(e)}")
        
        return validation
    
    def validate_health_checks(self) -> Dict[str, Any]:
        """Validate health check system."""
        validation = {
            "component": "Health Checks",
            "status": "checking",
            "details": []
        }
        
        try:
            health_check_files = [
                "health_checks.py",
                "advanced_health_checks.py",
                "infrastructure_health_monitor.py"
            ]
            
            monitoring_dir = project_root / "src/monitoring"
            core_monitoring_dir = project_root / "src/core/monitoring"
            
            found_health_checks = 0
            for health_file in health_check_files:
                for check_dir in [monitoring_dir, core_monitoring_dir]:
                    file_path = check_dir / health_file
                    if file_path.exists():
                        validation["details"].append(f"✓ Found {health_file} in {check_dir.name}")
                        found_health_checks += 1
                        break
                else:
                    validation["details"].append(f"✗ Missing {health_file}")
            
            if found_health_checks >= 2:
                validation["status"] = "pass"
            elif found_health_checks >= 1:
                validation["status"] = "warn"
            else:
                validation["status"] = "error"
                
        except Exception as e:
            validation["status"] = "error"
            validation["details"].append(f"Error validating health checks: {str(e)}")
        
        return validation
    
    def validate_alerting_system(self) -> Dict[str, Any]:
        """Validate intelligent alerting system."""
        validation = {
            "component": "Intelligent Alerting",
            "status": "checking",
            "details": []
        }
        
        try:
            alerting_files = [
                "intelligent_alerting.py",
                "intelligent_alerting_system.py",
                "datadog_intelligent_alerting.py",
                "automated_incident_response.py"
            ]
            
            monitoring_dir = project_root / "src/monitoring"
            found_alerting = 0
            
            for alert_file in alerting_files:
                file_path = monitoring_dir / alert_file
                if file_path.exists():
                    validation["details"].append(f"✓ Found {alert_file}")
                    found_alerting += 1
                else:
                    validation["details"].append(f"✗ Missing {alert_file}")
            
            if found_alerting >= 3:
                validation["status"] = "pass"
            elif found_alerting >= 2:
                validation["status"] = "warn"
            else:
                validation["status"] = "error"
                
        except Exception as e:
            validation["status"] = "error"
            validation["details"].append(f"Error validating alerting system: {str(e)}")
        
        return validation
    
    def validate_observability_stack(self) -> Dict[str, Any]:
        """Validate comprehensive observability stack."""
        validation = {
            "component": "Observability Stack",
            "status": "checking",
            "details": []
        }
        
        try:
            observability_components = [
                "enterprise_observability.py",
                "advanced_observability.py",
                "distributed_tracing",
                "correlation.py",
                "instrumentation.py"
            ]
            
            search_dirs = [
                project_root / "src/monitoring",
                project_root / "src/core/tracing"
            ]
            
            found_components = 0
            for component in observability_components:
                for search_dir in search_dirs:
                    if component.endswith(".py"):
                        file_path = search_dir / component
                        if file_path.exists():
                            validation["details"].append(f"✓ Found {component}")
                            found_components += 1
                            break
                    else:
                        # Check for directory
                        dir_path = search_dir / component
                        if dir_path.exists() and dir_path.is_dir():
                            validation["details"].append(f"✓ Found {component} directory")
                            found_components += 1
                            break
                else:
                    validation["details"].append(f"✗ Missing {component}")
            
            if found_components >= 4:
                validation["status"] = "pass"
            elif found_components >= 2:
                validation["status"] = "warn"
            else:
                validation["status"] = "error"
                
        except Exception as e:
            validation["status"] = "error"
            validation["details"].append(f"Error validating observability stack: {str(e)}")
        
        return validation
    
    def generate_recommendations(self, validations: List[Dict[str, Any]]) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []
        
        # Count status types
        pass_count = sum(1 for v in validations if v["status"] == "pass")
        warn_count = sum(1 for v in validations if v["status"] == "warn")
        error_count = sum(1 for v in validations if v["status"] == "error")
        
        if error_count > 0:
            recommendations.append("🔴 Critical: Address failed monitoring components immediately")
            recommendations.append("• Implement missing core monitoring components")
            recommendations.append("• Review error logs and fix configuration issues")
        
        if warn_count > 0:
            recommendations.append("🟡 Warning: Some monitoring components need attention")
            recommendations.append("• Complete partial implementations")
            recommendations.append("• Add missing optional monitoring features")
        
        if pass_count >= len(validations) * 0.8:
            recommendations.append("✅ Good: Monitoring system is well-configured")
            recommendations.append("• Continue monitoring performance and fine-tune alerts")
            recommendations.append("• Consider advanced features like ML-based anomaly detection")
        
        # Specific recommendations
        recommendations.extend([
            "📊 Recommended next steps:",
            "• Set up automated dashboard creation",
            "• Implement SLA tracking and reporting",
            "• Configure cross-service correlation",
            "• Add business metrics monitoring",
            "• Implement chaos engineering validation"
        ])
        
        return recommendations
    
    async def run_validation(self) -> Dict[str, Any]:
        """Run complete monitoring validation."""
        logger.info("Starting monitoring validation...")
        
        validations = [
            self.validate_datadog_setup(),
            self.validate_prometheus_setup(),
            self.validate_health_checks(),
            self.validate_alerting_system(),
            self.validate_observability_stack()
        ]
        
        # Determine overall status
        statuses = [v["status"] for v in validations]
        if "error" in statuses:
            overall_status = "needs_attention"
        elif "warn" in statuses:
            overall_status = "partial"
        else:
            overall_status = "healthy"
        
        self.results.update({
            "validations": validations,
            "overall_status": overall_status,
            "recommendations": self.generate_recommendations(validations),
            "summary": {
                "total_components": len(validations),
                "passing": len([v for v in validations if v["status"] == "pass"]),
                "warnings": len([v for v in validations if v["status"] == "warn"]),
                "errors": len([v for v in validations if v["status"] == "error"])
            }
        })
        
        return self.results

def main():
    """Main validation function."""
    validator = MonitoringValidator()
    
    try:
        # Run validation
        results = asyncio.run(validator.run_validation())
        
        # Print results
        print("\n" + "="*80)
        print("🔍 MONITORING OPTIMIZATIONS VALIDATION REPORT")
        print("="*80)
        
        print(f"\n📊 OVERALL STATUS: {results['overall_status'].upper()}")
        print(f"⏰ Validation Time: {results['timestamp']}")
        
        summary = results['summary']
        print(f"\n📈 SUMMARY:")
        print(f"  Total Components: {summary['total_components']}")
        print(f"  ✅ Passing: {summary['passing']}")
        print(f"  ⚠️  Warnings: {summary['warnings']}")
        print(f"  ❌ Errors: {summary['errors']}")
        
        print(f"\n📋 VALIDATION RESULTS:")
        for validation in results['validations']:
            status_emoji = {"pass": "✅", "warn": "⚠️", "error": "❌"}.get(validation['status'], "❓")
            print(f"\n{status_emoji} {validation['component']} - {validation['status'].upper()}")
            for detail in validation['details']:
                print(f"    {detail}")
        
        print(f"\n💡 RECOMMENDATIONS:")
        for rec in results['recommendations']:
            print(f"  {rec}")
        
        print("\n" + "="*80)
        
        # Save results
        results_file = project_root / "monitoring_validation_report.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"📄 Full report saved to: {results_file}")
        
        # Exit with appropriate code
        if results['overall_status'] == "needs_attention":
            sys.exit(1)
        elif results['overall_status'] == "partial":
            sys.exit(2)
        else:
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Validation failed: {str(e)}")
        print(f"❌ Validation failed: {str(e)}")
        sys.exit(3)

if __name__ == "__main__":
    main()
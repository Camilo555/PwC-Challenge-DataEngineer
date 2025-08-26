#!/usr/bin/env python3
"""
Comprehensive Test Runner for PwC Enterprise Data Platform
Orchestrates all testing phases with detailed reporting and CI/CD integration.
"""
import asyncio
import os
import sys
import subprocess
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tests.framework.comprehensive_test_suite import ComprehensiveTestSuite
from tests.framework.testing_framework import TestReporter, TestType, TestStatus
from core.logging import get_logger

class TestPhase(Enum):
    """Test execution phases"""
    SETUP = "setup"
    UNIT = "unit"
    INTEGRATION = "integration"
    PERFORMANCE = "performance"
    SECURITY = "security"
    E2E = "e2e"
    TEARDOWN = "teardown"

@dataclass
class TestRunConfiguration:
    """Configuration for test execution"""
    phases: List[TestPhase]
    parallel_execution: bool = True
    generate_coverage: bool = True
    output_formats: List[str] = None
    test_timeout_minutes: int = 30
    performance_benchmarks: bool = True
    security_scans: bool = True
    mutation_testing: bool = False
    smoke_tests_only: bool = False
    
    def __post_init__(self):
        if self.output_formats is None:
            self.output_formats = ["json", "html", "junit"]

class ComprehensiveTestRunner:
    """Main test runner orchestrating all test phases"""
    
    def __init__(self, config: TestRunConfiguration = None):
        self.config = config or TestRunConfiguration(
            phases=[TestPhase.UNIT, TestPhase.INTEGRATION, TestPhase.PERFORMANCE, TestPhase.SECURITY, TestPhase.E2E]
        )
        self.logger = get_logger(__name__)
        self.start_time = datetime.now()
        self.phase_results: Dict[TestPhase, Dict[str, Any]] = {}
        
    async def run_all_tests(self) -> Dict[str, Any]:
        """Execute comprehensive test suite with all phases"""
        self.logger.info("🚀 Starting Comprehensive Test Suite Execution")
        
        total_start_time = time.time()
        overall_results = {
            "start_time": self.start_time.isoformat(),
            "configuration": {
                "phases": [phase.value for phase in self.config.phases],
                "parallel_execution": self.config.parallel_execution,
                "generate_coverage": self.config.generate_coverage,
                "output_formats": self.config.output_formats
            },
            "phase_results": {},
            "summary": {},
            "artifacts": []
        }
        
        try:
            # Setup phase
            if TestPhase.SETUP in self.config.phases:
                await self._run_setup_phase()
            
            # Execute test phases in parallel if configured
            if self.config.parallel_execution:
                await self._run_phases_parallel()
            else:
                await self._run_phases_sequential()
            
            # Generate comprehensive reports
            await self._generate_comprehensive_reports()
            
            # Teardown phase
            if TestPhase.TEARDOWN in self.config.phases:
                await self._run_teardown_phase()
            
            # Calculate summary
            overall_results["phase_results"] = self.phase_results
            overall_results["summary"] = self._calculate_overall_summary()
            overall_results["duration_seconds"] = time.time() - total_start_time
            overall_results["end_time"] = datetime.now().isoformat()
            
            self.logger.info(f"✅ Test Suite Completed - Success Rate: {overall_results['summary']['success_rate']:.1f}%")
            
            return overall_results
            
        except Exception as e:
            self.logger.error(f"❌ Test Suite Failed: {e}")
            overall_results["error"] = str(e)
            overall_results["duration_seconds"] = time.time() - total_start_time
            overall_results["end_time"] = datetime.now().isoformat()
            raise
    
    async def _run_setup_phase(self):
        """Setup test environment and dependencies"""
        self.logger.info("🔧 Setup Phase: Preparing Test Environment")
        
        setup_start = time.time()
        setup_tasks = []
        
        # Validate environment
        setup_tasks.append(self._validate_test_environment())
        
        # Setup test databases
        setup_tasks.append(self._setup_test_databases())
        
        # Start test services
        setup_tasks.append(self._start_test_services())
        
        # Wait for all setup tasks
        await asyncio.gather(*setup_tasks)
        
        self.phase_results[TestPhase.SETUP] = {
            "status": "completed",
            "duration_seconds": time.time() - setup_start,
            "tasks_completed": len(setup_tasks)
        }
    
    async def _run_phases_parallel(self):
        """Execute test phases in parallel for faster execution"""
        self.logger.info("⚡ Running Test Phases in Parallel")
        
        phase_tasks = []
        
        if TestPhase.UNIT in self.config.phases:
            phase_tasks.append(self._run_unit_tests_phase())
        
        if TestPhase.INTEGRATION in self.config.phases:
            phase_tasks.append(self._run_integration_tests_phase())
        
        if TestPhase.PERFORMANCE in self.config.phases:
            phase_tasks.append(self._run_performance_tests_phase())
        
        if TestPhase.SECURITY in self.config.phases:
            phase_tasks.append(self._run_security_tests_phase())
        
        if TestPhase.E2E in self.config.phases:
            phase_tasks.append(self._run_e2e_tests_phase())
        
        # Execute all phases concurrently
        await asyncio.gather(*phase_tasks, return_exceptions=True)
    
    async def _run_phases_sequential(self):
        """Execute test phases sequentially"""
        self.logger.info("🔄 Running Test Phases Sequentially")
        
        if TestPhase.UNIT in self.config.phases:
            await self._run_unit_tests_phase()
        
        if TestPhase.INTEGRATION in self.config.phases:
            await self._run_integration_tests_phase()
        
        if TestPhase.PERFORMANCE in self.config.phases:
            await self._run_performance_tests_phase()
        
        if TestPhase.SECURITY in self.config.phases:
            await self._run_security_tests_phase()
        
        if TestPhase.E2E in self.config.phases:
            await self._run_e2e_tests_phase()
    
    async def _run_unit_tests_phase(self):
        """Execute unit tests"""
        self.logger.info("🧪 Unit Tests Phase")
        
        phase_start = time.time()
        
        try:
            # Run pytest unit tests
            unit_result = await self._run_pytest_command([
                "-v", "--tb=short", 
                "--durations=10",
                "-m", "unit",
                "tests/"
            ])
            
            # Run custom unit test suite
            suite = ComprehensiveTestSuite()
            await suite.setup_test_environment()
            await suite.run_unit_tests()
            await suite.teardown_test_environment()
            
            self.phase_results[TestPhase.UNIT] = {
                "status": "completed",
                "duration_seconds": time.time() - phase_start,
                "pytest_result": unit_result,
                "custom_tests": len(suite.api_framework.test_results),
                "passed_tests": len([r for r in suite.api_framework.test_results if r.status == TestStatus.PASSED])
            }
            
        except Exception as e:
            self.phase_results[TestPhase.UNIT] = {
                "status": "failed",
                "error": str(e),
                "duration_seconds": time.time() - phase_start
            }
    
    async def _run_integration_tests_phase(self):
        """Execute integration tests"""
        self.logger.info("🔗 Integration Tests Phase")
        
        phase_start = time.time()
        
        try:
            # Run pytest integration tests
            integration_result = await self._run_pytest_command([
                "-v", "--tb=short",
                "-m", "integration",
                "tests/"
            ])
            
            # Run custom integration test suite
            suite = ComprehensiveTestSuite()
            await suite.setup_test_environment()
            await suite.run_integration_tests()
            await suite.run_api_contract_tests()
            await suite.run_database_tests()
            await suite.teardown_test_environment()
            
            self.phase_results[TestPhase.INTEGRATION] = {
                "status": "completed",
                "duration_seconds": time.time() - phase_start,
                "pytest_result": integration_result,
                "custom_tests": len(suite.api_framework.test_results) + len(suite.db_framework.test_results),
                "passed_tests": len([r for r in suite.api_framework.test_results + suite.db_framework.test_results 
                                   if r.status == TestStatus.PASSED])
            }
            
        except Exception as e:
            self.phase_results[TestPhase.INTEGRATION] = {
                "status": "failed",
                "error": str(e),
                "duration_seconds": time.time() - phase_start
            }
    
    async def _run_performance_tests_phase(self):
        """Execute performance tests"""
        self.logger.info("⚡ Performance Tests Phase")
        
        phase_start = time.time()
        
        try:
            # Run pytest performance tests
            performance_result = await self._run_pytest_command([
                "-v", "--tb=short",
                "-m", "performance",
                "tests/"
            ])
            
            # Run custom performance tests
            suite = ComprehensiveTestSuite()
            await suite.setup_test_environment()
            await suite.run_performance_tests()
            await suite.teardown_test_environment()
            
            # Run load tests with locust
            load_test_result = await self._run_load_tests()
            
            self.phase_results[TestPhase.PERFORMANCE] = {
                "status": "completed",
                "duration_seconds": time.time() - phase_start,
                "pytest_result": performance_result,
                "custom_tests": len(suite.perf_framework.test_results),
                "load_test_result": load_test_result,
                "performance_metrics": suite.perf_framework.performance_metrics
            }
            
        except Exception as e:
            self.phase_results[TestPhase.PERFORMANCE] = {
                "status": "failed",
                "error": str(e),
                "duration_seconds": time.time() - phase_start
            }
    
    async def _run_security_tests_phase(self):
        """Execute security tests"""
        self.logger.info("🔒 Security Tests Phase")
        
        phase_start = time.time()
        
        try:
            # Run pytest security tests
            security_result = await self._run_pytest_command([
                "-v", "--tb=short",
                "-m", "security",
                "tests/"
            ])
            
            # Run custom security tests
            suite = ComprehensiveTestSuite()
            await suite.setup_test_environment()
            await suite.run_security_tests()
            await suite.teardown_test_environment()
            
            # Run additional security scans if enabled
            security_scans = []
            if self.config.security_scans:
                security_scans = await self._run_security_scans()
            
            self.phase_results[TestPhase.SECURITY] = {
                "status": "completed",
                "duration_seconds": time.time() - phase_start,
                "pytest_result": security_result,
                "custom_tests": len([r for r in suite.api_framework.test_results if r.test_type == TestType.SECURITY]),
                "security_scans": security_scans
            }
            
        except Exception as e:
            self.phase_results[TestPhase.SECURITY] = {
                "status": "failed",
                "error": str(e),
                "duration_seconds": time.time() - phase_start
            }
    
    async def _run_e2e_tests_phase(self):
        """Execute end-to-end tests"""
        self.logger.info("🔄 End-to-End Tests Phase")
        
        phase_start = time.time()
        
        try:
            # Run pytest e2e tests
            e2e_result = await self._run_pytest_command([
                "-v", "--tb=short",
                "-m", "e2e",
                "tests/"
            ])
            
            # Run custom e2e tests
            suite = ComprehensiveTestSuite()
            await suite.setup_test_environment()
            await suite.run_end_to_end_tests()
            await suite.run_data_quality_tests()
            await suite.teardown_test_environment()
            
            self.phase_results[TestPhase.E2E] = {
                "status": "completed",
                "duration_seconds": time.time() - phase_start,
                "pytest_result": e2e_result,
                "custom_tests": len([r for r in suite.api_framework.test_results + suite.db_framework.test_results
                                   if r.test_type in [TestType.E2E, TestType.INTEGRATION]])
            }
            
        except Exception as e:
            self.phase_results[TestPhase.E2E] = {
                "status": "failed",
                "error": str(e),
                "duration_seconds": time.time() - phase_start
            }
    
    async def _run_teardown_phase(self):
        """Cleanup test environment"""
        self.logger.info("🧹 Teardown Phase: Cleaning Up Test Environment")
        
        teardown_start = time.time()
        
        # Stop test services
        await self._stop_test_services()
        
        # Cleanup test databases
        await self._cleanup_test_databases()
        
        # Generate final artifacts
        await self._generate_test_artifacts()
        
        self.phase_results[TestPhase.TEARDOWN] = {
            "status": "completed",
            "duration_seconds": time.time() - teardown_start
        }
    
    async def _validate_test_environment(self):
        """Validate test environment setup"""
        self.logger.info("✅ Validating test environment...")
        
        # Check required environment variables
        required_env_vars = [
            "DATABASE_URL", "REDIS_URL", "ENVIRONMENT"
        ]
        
        missing_vars = []
        for var in required_env_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            self.logger.warning(f"Missing environment variables: {missing_vars}")
        
        # Check Python dependencies
        try:
            import pytest, asyncio, httpx, pandas  # noqa
            self.logger.info("✅ Required Python packages available")
        except ImportError as e:
            self.logger.error(f"❌ Missing Python dependencies: {e}")
            raise
    
    async def _setup_test_databases(self):
        """Setup test databases"""
        self.logger.info("🗄️ Setting up test databases...")
        
        # Initialize test database schema
        # This would typically run database migrations for testing
        pass
    
    async def _start_test_services(self):
        """Start required test services"""
        self.logger.info("🚀 Starting test services...")
        
        # Start test API server, Redis, etc. if needed
        # This could use Docker Compose or direct service startup
        pass
    
    async def _stop_test_services(self):
        """Stop test services"""
        self.logger.info("🛑 Stopping test services...")
        pass
    
    async def _cleanup_test_databases(self):
        """Cleanup test databases"""
        self.logger.info("🧹 Cleaning up test databases...")
        pass
    
    async def _run_pytest_command(self, args: List[str]) -> Dict[str, Any]:
        """Run pytest with specified arguments"""
        cmd = ["python", "-m", "pytest"] + args
        
        if self.config.generate_coverage:
            cmd.extend(["--cov=src", "--cov-report=html", "--cov-report=xml"])
        
        # Add timeout
        cmd.extend([f"--timeout={self.config.test_timeout_minutes * 60}"])
        
        self.logger.info(f"Running: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent
            )
            
            return {
                "return_code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "success": result.returncode == 0
            }
        except Exception as e:
            return {
                "return_code": -1,
                "error": str(e),
                "success": False
            }
    
    async def _run_load_tests(self) -> Dict[str, Any]:
        """Run load tests using Locust"""
        self.logger.info("📈 Running load tests...")
        
        try:
            cmd = [
                "python", "-m", "locust",
                "-f", "tests/load/locustfile.py",
                "--headless",
                "--users", "10",
                "--spawn-rate", "2",
                "--run-time", "60s",
                "--host", "http://localhost:8000"
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent
            )
            
            return {
                "return_code": result.returncode,
                "stdout": result.stdout,
                "success": result.returncode == 0
            }
        except Exception as e:
            return {
                "error": str(e),
                "success": False
            }
    
    async def _run_security_scans(self) -> List[Dict[str, Any]]:
        """Run additional security scans"""
        self.logger.info("🔍 Running security scans...")
        
        scans = []
        
        # Run bandit for security issues
        try:
            result = subprocess.run(
                ["python", "-m", "bandit", "-r", "src/", "-f", "json"],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent
            )
            
            scans.append({
                "name": "bandit",
                "return_code": result.returncode,
                "output": result.stdout,
                "success": result.returncode == 0
            })
        except Exception as e:
            scans.append({
                "name": "bandit",
                "error": str(e),
                "success": False
            })
        
        # Run safety for known vulnerabilities
        try:
            result = subprocess.run(
                ["python", "-m", "safety", "check", "--json"],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent
            )
            
            scans.append({
                "name": "safety",
                "return_code": result.returncode,
                "output": result.stdout,
                "success": result.returncode == 0
            })
        except Exception as e:
            scans.append({
                "name": "safety",
                "error": str(e),
                "success": False
            })
        
        return scans
    
    async def _generate_comprehensive_reports(self):
        """Generate comprehensive test reports"""
        self.logger.info("📋 Generating comprehensive test reports...")
        
        # Create reports directory
        reports_dir = Path(__file__).parent.parent / "test_reports"
        reports_dir.mkdir(exist_ok=True)
        
        # Generate summary report
        summary_report = self._generate_summary_report()
        
        for output_format in self.config.output_formats:
            if output_format == "json":
                report_file = reports_dir / f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(report_file, 'w') as f:
                    json.dump(summary_report, f, indent=2, default=str)
                self.logger.info(f"📄 JSON report saved: {report_file}")
            
            elif output_format == "html":
                html_report = self._generate_html_report(summary_report)
                report_file = reports_dir / f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                with open(report_file, 'w') as f:
                    f.write(html_report)
                self.logger.info(f"📄 HTML report saved: {report_file}")
    
    async def _generate_test_artifacts(self):
        """Generate additional test artifacts"""
        self.logger.info("📦 Generating test artifacts...")
        
        # Coverage reports, performance benchmarks, etc.
        pass
    
    def _generate_summary_report(self) -> Dict[str, Any]:
        """Generate comprehensive summary report"""
        return {
            "execution_summary": {
                "start_time": self.start_time.isoformat(),
                "end_time": datetime.now().isoformat(),
                "total_duration_seconds": (datetime.now() - self.start_time).total_seconds(),
                "phases_executed": list(self.phase_results.keys()),
                "overall_status": self._get_overall_status()
            },
            "phase_results": {phase.value: result for phase, result in self.phase_results.items()},
            "summary_statistics": self._calculate_overall_summary(),
            "configuration": {
                "phases": [phase.value for phase in self.config.phases],
                "parallel_execution": self.config.parallel_execution,
                "coverage_enabled": self.config.generate_coverage,
                "security_scans_enabled": self.config.security_scans
            }
        }
    
    def _generate_html_report(self, summary_report: Dict[str, Any]) -> str:
        """Generate HTML report"""
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Comprehensive Test Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
                .phase {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
                .success {{ border-left: 5px solid #4CAF50; }}
                .failure {{ border-left: 5px solid #f44336; }}
                .summary {{ background-color: #e8f5e8; padding: 15px; border-radius: 5px; margin: 20px 0; }}
                table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Comprehensive Test Report</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>Overall Status: <strong>{summary_report['execution_summary']['overall_status']}</strong></p>
            </div>
            
            <div class="summary">
                <h2>Summary Statistics</h2>
                <p>Total Duration: {summary_report['execution_summary']['total_duration_seconds']:.2f} seconds</p>
                <p>Phases Executed: {len(summary_report['phase_results'])}</p>
            </div>
            
            <h2>Phase Results</h2>
        """
        
        for phase_name, phase_result in summary_report['phase_results'].items():
            status_class = "success" if phase_result.get('status') == 'completed' else "failure"
            html_content += f"""
            <div class="phase {status_class}">
                <h3>{phase_name.upper()} Phase</h3>
                <p>Status: {phase_result.get('status', 'unknown')}</p>
                <p>Duration: {phase_result.get('duration_seconds', 0):.2f} seconds</p>
                {f"<p>Error: {phase_result.get('error')}</p>" if phase_result.get('error') else ""}
            </div>
            """
        
        html_content += """
        </body>
        </html>
        """
        
        return html_content
    
    def _calculate_overall_summary(self) -> Dict[str, Any]:
        """Calculate overall test execution summary"""
        total_phases = len(self.phase_results)
        completed_phases = sum(1 for result in self.phase_results.values() 
                             if result.get('status') == 'completed')
        failed_phases = sum(1 for result in self.phase_results.values() 
                          if result.get('status') == 'failed')
        
        return {
            "total_phases": total_phases,
            "completed_phases": completed_phases,
            "failed_phases": failed_phases,
            "success_rate": (completed_phases / total_phases * 100) if total_phases > 0 else 0,
            "total_duration": sum(result.get('duration_seconds', 0) 
                                for result in self.phase_results.values())
        }
    
    def _get_overall_status(self) -> str:
        """Get overall test execution status"""
        if not self.phase_results:
            return "not_started"
        
        if any(result.get('status') == 'failed' for result in self.phase_results.values()):
            return "failed"
        
        if all(result.get('status') == 'completed' for result in self.phase_results.values()):
            return "success"
        
        return "partial"


async def main():
    """Main entry point for test runner"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Comprehensive Test Runner")
    parser.add_argument("--phases", nargs="+", 
                       choices=["unit", "integration", "performance", "security", "e2e"],
                       default=["unit", "integration", "performance", "security", "e2e"],
                       help="Test phases to execute")
    parser.add_argument("--parallel", action="store_true", default=True,
                       help="Run phases in parallel")
    parser.add_argument("--no-coverage", action="store_true", 
                       help="Disable coverage reporting")
    parser.add_argument("--output-formats", nargs="+", 
                       choices=["json", "html", "junit"],
                       default=["json", "html"],
                       help="Output report formats")
    parser.add_argument("--timeout", type=int, default=30,
                       help="Test timeout in minutes")
    parser.add_argument("--smoke-only", action="store_true",
                       help="Run only smoke tests")
    
    args = parser.parse_args()
    
    # Convert phase names to enum values
    phases = [TestPhase(phase) for phase in args.phases]
    
    config = TestRunConfiguration(
        phases=phases,
        parallel_execution=args.parallel,
        generate_coverage=not args.no_coverage,
        output_formats=args.output_formats,
        test_timeout_minutes=args.timeout,
        smoke_tests_only=args.smoke_only
    )
    
    runner = ComprehensiveTestRunner(config)
    
    try:
        results = await runner.run_all_tests()
        
        # Print final summary
        print(f"\n🎯 FINAL RESULTS:")
        print(f"   Overall Status: {results.get('summary', {}).get('success_rate', 0):.1f}% Success")
        print(f"   Total Duration: {results.get('duration_seconds', 0):.2f} seconds")
        print(f"   Phases Executed: {len(results.get('phase_results', {}))}")
        
        # Exit with appropriate code
        success_rate = results.get('summary', {}).get('success_rate', 0)
        exit_code = 0 if success_rate >= 90 else 1
        
        return exit_code
        
    except Exception as e:
        print(f"❌ Test execution failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
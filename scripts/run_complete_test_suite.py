#!/usr/bin/env python3
"""
Complete Test Suite Runner
Executes comprehensive test suite to achieve 95%+ coverage target.
Includes performance validation, mutation testing, and reporting.
"""

from __future__ import annotations

import subprocess
import sys
import time
import json
from pathlib import Path
from typing import Dict, List, Any
import argparse


class TestSuiteRunner:
    """Complete test suite runner with comprehensive reporting."""
    
    def __init__(self):
        self.project_root = Path.cwd()
        self.reports_dir = self.project_root / "reports"
        self.reports_dir.mkdir(exist_ok=True)
        
        self.test_results = {}
        self.coverage_results = {}
        self.performance_results = {}
    
    def run_unit_tests(self) -> Dict[str, Any]:
        """Run unit tests with coverage."""
        print("🧪 Running Unit Tests...")
        
        cmd = [
            sys.executable, "-m", "pytest",
            "tests/unit/",
            "tests/test_coverage_baseline.py",
            "tests/test_comprehensive_coverage_suite.py",
            "-v",
            "--cov=src",
            "--cov-report=html:htmlcov",
            "--cov-report=xml:coverage.xml",
            "--cov-report=term-missing",
            "--junitxml=reports/unit-tests.xml",
            "--tb=short",
            "-m", "unit"
        ]
        
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        end_time = time.time()
        
        return {
            "exit_code": result.returncode,
            "duration": end_time - start_time,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    
    def run_integration_tests(self) -> Dict[str, Any]:
        """Run integration tests."""
        print("🔗 Running Integration Tests...")
        
        cmd = [
            sys.executable, "-m", "pytest",
            "tests/integration/",
            "-v",
            "--junitxml=reports/integration-tests.xml",
            "--tb=short",
            "-m", "integration"
        ]
        
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        end_time = time.time()
        
        return {
            "exit_code": result.returncode,
            "duration": end_time - start_time,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    
    def run_api_tests(self) -> Dict[str, Any]:
        """Run API tests."""
        print("🌐 Running API Tests...")
        
        cmd = [
            sys.executable, "-m", "pytest",
            "tests/api/",
            "-v",
            "--junitxml=reports/api-tests.xml",
            "--tb=short",
            "-m", "api"
        ]
        
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        end_time = time.time()
        
        return {
            "exit_code": result.returncode,
            "duration": end_time - start_time,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    
    def run_performance_tests(self) -> Dict[str, Any]:
        """Run performance tests."""
        print("⚡ Running Performance Tests...")
        
        cmd = [
            sys.executable, "-m", "pytest",
            "tests/performance/",
            "-v",
            "--junitxml=reports/performance-tests.xml",
            "--tb=short",
            "-m", "performance"
        ]
        
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        end_time = time.time()
        
        return {
            "exit_code": result.returncode,
            "duration": end_time - start_time,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    
    def run_property_tests(self) -> Dict[str, Any]:
        """Run property-based tests with Hypothesis."""
        print("🎲 Running Property-Based Tests...")
        
        cmd = [
            sys.executable, "-m", "pytest",
            "tests/property/",
            "-v",
            "--junitxml=reports/property-tests.xml",
            "--tb=short",
            "-m", "property"
        ]
        
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        end_time = time.time()
        
        return {
            "exit_code": result.returncode,
            "duration": end_time - start_time,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    
    def run_mutation_tests(self) -> Dict[str, Any]:
        """Run mutation tests."""
        print("🧬 Running Mutation Tests...")
        
        cmd = [
            sys.executable, "-m", "pytest",
            "tests/mutation/",
            "-v",
            "--junitxml=reports/mutation-tests.xml",
            "--tb=short",
            "-m", "mutation"
        ]
        
        start_time = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True)
        end_time = time.time()
        
        return {
            "exit_code": result.returncode,
            "duration": end_time - start_time,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
    
    def analyze_coverage(self) -> Dict[str, Any]:
        """Analyze test coverage results."""
        print("📊 Analyzing Coverage Results...")
        
        coverage_file = self.project_root / "coverage.xml"
        if not coverage_file.exists():
            return {"error": "Coverage file not found"}
        
        # Parse coverage results (simplified)
        coverage_data = {
            "target_coverage": 95.0,
            "current_coverage": 0.0,
            "files_covered": 0,
            "total_files": 0,
            "lines_covered": 0,
            "total_lines": 0
        }
        
        try:
            import xml.etree.ElementTree as ET
            tree = ET.parse(coverage_file)
            root = tree.getroot()
            
            # Extract coverage data from XML
            for coverage in root.findall('.//coverage'):
                lines_covered = int(coverage.get('lines-covered', 0))
                lines_valid = int(coverage.get('lines-valid', 0))
                
                if lines_valid > 0:
                    coverage_data["current_coverage"] = (lines_covered / lines_valid) * 100
                    coverage_data["lines_covered"] = lines_covered
                    coverage_data["total_lines"] = lines_valid
                break
            
        except Exception as e:
            coverage_data["error"] = str(e)
        
        return coverage_data
    
    def generate_comprehensive_report(self) -> None:
        """Generate comprehensive test report."""
        print("📝 Generating Comprehensive Report...")
        
        report = {
            "test_suite_execution": {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "total_duration": sum(
                    result.get("duration", 0) 
                    for result in self.test_results.values()
                ),
                "tests_run": len(self.test_results),
                "success_rate": sum(
                    1 for result in self.test_results.values() 
                    if result.get("exit_code") == 0
                ) / len(self.test_results) * 100 if self.test_results else 0
            },
            "coverage_analysis": self.coverage_results,
            "test_results": self.test_results,
            "performance_metrics": self.performance_results,
            "recommendations": self.generate_recommendations()
        }
        
        # Save report
        report_file = self.reports_dir / "comprehensive-test-report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Generate HTML summary
        self.generate_html_summary(report)
        
        print(f"📊 Report saved to: {report_file}")
    
    def generate_recommendations(self) -> List[str]:
        """Generate test improvement recommendations."""
        recommendations = []
        
        # Coverage recommendations
        current_coverage = self.coverage_results.get("current_coverage", 0)
        if current_coverage < 95:
            recommendations.append(
                f"Increase test coverage from {current_coverage:.1f}% to 95%+ target"
            )
        
        # Performance recommendations
        failed_tests = sum(
            1 for result in self.test_results.values() 
            if result.get("exit_code") != 0
        )
        if failed_tests > 0:
            recommendations.append(
                f"Fix {failed_tests} failing test suite(s) for production readiness"
            )
        
        # Duration recommendations
        total_duration = sum(
            result.get("duration", 0) 
            for result in self.test_results.values()
        )
        if total_duration > 300:  # 5 minutes
            recommendations.append(
                f"Optimize test execution time (current: {total_duration:.1f}s, target: <300s)"
            )
        
        if not recommendations:
            recommendations.append("🎉 All test quality targets achieved!")
        
        return recommendations
    
    def generate_html_summary(self, report: Dict[str, Any]) -> None:
        """Generate HTML summary report."""
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Test Suite Execution Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background: #2c3e50; color: white; padding: 20px; border-radius: 5px; }}
        .section {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
        .success {{ background: #d4edda; border-color: #c3e6cb; }}
        .warning {{ background: #fff3cd; border-color: #ffeaa7; }}
        .error {{ background: #f8d7da; border-color: #f5c6cb; }}
        .metric {{ display: inline-block; margin: 10px; padding: 10px; background: #f8f9fa; border-radius: 3px; }}
        ul {{ list-style-type: none; padding: 0; }}
        li {{ margin: 5px 0; padding: 5px; background: #f1f2f6; border-radius: 3px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🧪 Test Suite Execution Report</h1>
        <p>Generated: {report['test_suite_execution']['timestamp']}</p>
    </div>
    
    <div class="section">
        <h2>📊 Execution Summary</h2>
        <div class="metric">
            <strong>Total Duration:</strong> {report['test_suite_execution']['total_duration']:.1f}s
        </div>
        <div class="metric">
            <strong>Success Rate:</strong> {report['test_suite_execution']['success_rate']:.1f}%
        </div>
        <div class="metric">
            <strong>Tests Run:</strong> {report['test_suite_execution']['tests_run']}
        </div>
    </div>
    
    <div class="section">
        <h2>📈 Coverage Analysis</h2>
        <div class="metric">
            <strong>Current Coverage:</strong> {report['coverage_analysis'].get('current_coverage', 0):.1f}%
        </div>
        <div class="metric">
            <strong>Target Coverage:</strong> {report['coverage_analysis'].get('target_coverage', 95):.1f}%
        </div>
        <div class="metric">
            <strong>Lines Covered:</strong> {report['coverage_analysis'].get('lines_covered', 0):,}
        </div>
    </div>
    
    <div class="section">
        <h2>🎯 Recommendations</h2>
        <ul>
        {"".join(f"<li>{rec}</li>" for rec in report['recommendations'])}
        </ul>
    </div>
    
    <div class="section">
        <h2>🔍 Test Suite Results</h2>
        <ul>
        {"".join(f"<li class='{'success' if result.get('exit_code') == 0 else 'error'}'><strong>{suite}:</strong> {'✅ PASSED' if result.get('exit_code') == 0 else '❌ FAILED'} ({result.get('duration', 0):.1f}s)</li>" for suite, result in report['test_results'].items())}
        </ul>
    </div>
</body>
</html>
        """
        
        html_file = self.reports_dir / "test-summary.html"
        with open(html_file, 'w') as f:
            f.write(html_content)
        
        print(f"📋 HTML summary saved to: {html_file}")
    
    def run_complete_suite(self) -> None:
        """Run complete test suite."""
        print("🚀 Starting Complete Test Suite Execution")
        print("=" * 60)
        
        start_time = time.time()
        
        # Run all test suites
        test_suites = [
            ("unit_tests", self.run_unit_tests),
            ("integration_tests", self.run_integration_tests),
            ("api_tests", self.run_api_tests),
            ("performance_tests", self.run_performance_tests),
            ("property_tests", self.run_property_tests),
            ("mutation_tests", self.run_mutation_tests)
        ]
        
        for suite_name, suite_func in test_suites:
            try:
                result = suite_func()
                self.test_results[suite_name] = result
                
                status = "✅ PASSED" if result["exit_code"] == 0 else "❌ FAILED"
                duration = result["duration"]
                print(f"{suite_name}: {status} ({duration:.1f}s)")
                
            except Exception as e:
                print(f"❌ {suite_name}: ERROR - {e}")
                self.test_results[suite_name] = {
                    "exit_code": 1,
                    "duration": 0,
                    "error": str(e)
                }
        
        # Analyze coverage
        self.coverage_results = self.analyze_coverage()
        
        # Generate report
        self.generate_comprehensive_report()
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        print("=" * 60)
        print(f"🏁 Test Suite Completed in {total_duration:.1f}s")
        
        # Summary
        passed_suites = sum(
            1 for result in self.test_results.values() 
            if result.get("exit_code") == 0
        )
        total_suites = len(self.test_results)
        success_rate = passed_suites / total_suites * 100 if total_suites > 0 else 0
        
        print(f"📊 Results: {passed_suites}/{total_suites} suites passed ({success_rate:.1f}%)")
        
        current_coverage = self.coverage_results.get("current_coverage", 0)
        print(f"📈 Coverage: {current_coverage:.1f}% (Target: 95%)")
        
        if success_rate >= 80 and current_coverage >= 90:
            print("🎉 Test suite execution successful!")
            return True
        else:
            print("⚠️  Test suite needs improvement")
            return False


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Run complete test suite")
    parser.add_argument(
        "--suite", 
        choices=["unit", "integration", "api", "performance", "property", "mutation", "all"],
        default="all",
        help="Test suite to run"
    )
    parser.add_argument(
        "--coverage-target",
        type=float,
        default=95.0,
        help="Target coverage percentage"
    )
    
    args = parser.parse_args()
    
    runner = TestSuiteRunner()
    
    if args.suite == "all":
        success = runner.run_complete_suite()
        sys.exit(0 if success else 1)
    else:
        # Run specific suite
        suite_map = {
            "unit": runner.run_unit_tests,
            "integration": runner.run_integration_tests,
            "api": runner.run_api_tests,
            "performance": runner.run_performance_tests,
            "property": runner.run_property_tests,
            "mutation": runner.run_mutation_tests
        }
        
        if args.suite in suite_map:
            result = suite_map[args.suite]()
            print(f"Suite {args.suite}: {'PASSED' if result['exit_code'] == 0 else 'FAILED'}")
            sys.exit(result['exit_code'])


if __name__ == "__main__":
    main()
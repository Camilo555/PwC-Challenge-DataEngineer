#!/usr/bin/env python3
"""
Security Test Automation Script
Automated execution and reporting for security tests
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add src to path for imports
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root / "src"))


class SecurityTestRunner:
    """Automated security test execution and reporting"""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.reports_dir = project_root / "reports" / "security"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
        # Security test categories
        self.test_categories = {
            "dlp": {
                "name": "Data Loss Prevention",
                "marker": "dlp",
                "critical": True,
                "timeout": 300
            },
            "compliance": {
                "name": "Compliance Framework", 
                "marker": "compliance",
                "critical": True,
                "timeout": 600
            },
            "access_control": {
                "name": "Access Control & Authorization",
                "marker": "access_control",
                "critical": True,
                "timeout": 300
            },
            "api_security": {
                "name": "API Security",
                "marker": "api_security", 
                "critical": True,
                "timeout": 300
            },
            "penetration": {
                "name": "Penetration Testing",
                "marker": "penetration",
                "critical": False,
                "timeout": 900
            },
            "performance": {
                "name": "Security Performance",
                "marker": "security and performance",
                "critical": False,
                "timeout": 600
            },
            "integration": {
                "name": "Security Integration",
                "marker": "security and integration",
                "critical": True,
                "timeout": 900
            }
        }
    
    def run_security_tests(
        self,
        categories: Optional[List[str]] = None,
        include_penetration: bool = False,
        generate_reports: bool = True,
        fail_fast: bool = False,
        verbose: bool = False
    ) -> Dict[str, Any]:
        """Run security tests with comprehensive reporting"""
        
        print("🔒 Starting Enterprise Security Test Suite")
        print("=" * 60)
        
        start_time = datetime.now()
        results = {
            "start_time": start_time.isoformat(),
            "test_categories": {},
            "summary": {},
            "recommendations": []
        }
        
        # Determine which categories to run
        if categories is None:
            categories_to_run = list(self.test_categories.keys())
        else:
            categories_to_run = [cat for cat in categories if cat in self.test_categories]
        
        # Remove penetration tests unless explicitly included
        if not include_penetration and "penetration" in categories_to_run:
            categories_to_run.remove("penetration")
            print("⚠️  Penetration tests excluded (use --include-penetration to run)")
        
        print(f"📋 Running {len(categories_to_run)} security test categories")
        
        # Run each category
        total_passed = 0
        total_failed = 0
        total_skipped = 0
        critical_failures = []
        
        for category in categories_to_run:
            print(f"\n🔍 Running {self.test_categories[category]['name']} Tests...")
            
            category_result = self._run_category_tests(
                category,
                verbose=verbose,
                fail_fast=fail_fast
            )
            
            results["test_categories"][category] = category_result
            
            # Update totals
            total_passed += category_result["passed"]
            total_failed += category_result["failed"]
            total_skipped += category_result["skipped"]
            
            # Track critical failures
            if self.test_categories[category]["critical"] and category_result["failed"] > 0:
                critical_failures.append({
                    "category": category,
                    "name": self.test_categories[category]["name"],
                    "failures": category_result["failed"]
                })
            
            # Print category summary
            status = "✅ PASS" if category_result["failed"] == 0 else "❌ FAIL"
            print(f"{status} - {category_result['passed']} passed, {category_result['failed']} failed")
        
        # Overall summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        results.update({
            "end_time": end_time.isoformat(),
            "duration_seconds": duration.total_seconds(),
            "summary": {
                "total_passed": total_passed,
                "total_failed": total_failed,
                "total_skipped": total_skipped,
                "success_rate": total_passed / (total_passed + total_failed) if (total_passed + total_failed) > 0 else 0,
                "critical_failures": len(critical_failures),
                "overall_status": "PASS" if len(critical_failures) == 0 else "FAIL"
            },
            "critical_failures": critical_failures
        })
        
        # Generate recommendations
        results["recommendations"] = self._generate_security_recommendations(results)
        
        # Print final summary
        self._print_summary(results)
        
        # Generate reports if requested
        if generate_reports:
            self._generate_reports(results)
        
        # Exit with error code if critical tests failed
        if critical_failures and fail_fast:
            sys.exit(1)
        
        return results
    
    def _run_category_tests(
        self,
        category: str,
        verbose: bool = False,
        fail_fast: bool = False
    ) -> Dict[str, Any]:
        """Run tests for a specific security category"""
        
        category_info = self.test_categories[category]
        marker = category_info["marker"]
        timeout = category_info["timeout"]
        
        # Build pytest command
        cmd = [
            sys.executable, "-m", "pytest",
            f"tests/security/",
            f"-m", marker,
            f"--timeout={timeout}",
            "--tb=short",
            "--json-report",
            f"--json-report-file={self.reports_dir / f'{category}_results.json'}"
        ]
        
        if verbose:
            cmd.append("-v")
        if fail_fast:
            cmd.append("-x")
        
        # Add security-specific options
        cmd.extend([
            "--disable-warnings",
            "--strict-markers"
        ])
        
        # Run tests
        start_time = datetime.now()
        
        try:
            result = subprocess.run(
                cmd,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=timeout + 60  # Add buffer to pytest timeout
            )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Parse results
            return self._parse_pytest_results(category, result, duration)
            
        except subprocess.TimeoutExpired:
            return {
                "category": category,
                "status": "timeout",
                "passed": 0,
                "failed": 1,
                "skipped": 0,
                "duration": timeout,
                "error": f"Tests timed out after {timeout}s"
            }
        except Exception as e:
            return {
                "category": category,
                "status": "error",
                "passed": 0,
                "failed": 1,
                "skipped": 0,
                "duration": 0,
                "error": str(e)
            }
    
    def _parse_pytest_results(self, category: str, result: subprocess.CompletedProcess, duration: float) -> Dict[str, Any]:
        """Parse pytest results from JSON report"""
        
        json_file = self.reports_dir / f"{category}_results.json"
        
        try:
            if json_file.exists():
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                summary = data.get("summary", {})
                
                return {
                    "category": category,
                    "status": "pass" if result.returncode == 0 else "fail",
                    "passed": summary.get("passed", 0),
                    "failed": summary.get("failed", 0),
                    "skipped": summary.get("skipped", 0),
                    "duration": duration,
                    "return_code": result.returncode,
                    "detailed_results": data
                }
            else:
                # Fallback to parsing stdout
                return self._parse_stdout_results(category, result, duration)
                
        except Exception as e:
            return {
                "category": category,
                "status": "error",
                "passed": 0,
                "failed": 1,
                "skipped": 0,
                "duration": duration,
                "error": f"Failed to parse results: {e}"
            }
    
    def _parse_stdout_results(self, category: str, result: subprocess.CompletedProcess, duration: float) -> Dict[str, Any]:
        """Fallback parsing from pytest stdout"""
        
        stdout = result.stdout
        passed = failed = skipped = 0
        
        # Simple regex parsing of pytest output
        import re
        
        if "failed" in stdout:
            failed_match = re.search(r'(\d+) failed', stdout)
            failed = int(failed_match.group(1)) if failed_match else 1
        
        if "passed" in stdout:
            passed_match = re.search(r'(\d+) passed', stdout)
            passed = int(passed_match.group(1)) if passed_match else 0
        
        if "skipped" in stdout:
            skipped_match = re.search(r'(\d+) skipped', stdout)
            skipped = int(skipped_match.group(1)) if skipped_match else 0
        
        return {
            "category": category,
            "status": "pass" if result.returncode == 0 else "fail",
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "duration": duration,
            "return_code": result.returncode,
            "stdout": stdout,
            "stderr": result.stderr
        }
    
    def _generate_security_recommendations(self, results: Dict[str, Any]) -> List[str]:
        """Generate security improvement recommendations"""
        
        recommendations = []
        summary = results["summary"]
        
        # Overall security posture
        if summary["success_rate"] < 0.9:
            recommendations.append(
                f"Security test success rate is {summary['success_rate']:.1%}. "
                "Consider addressing failing tests to improve security posture."
            )
        
        # Critical failures
        if summary["critical_failures"] > 0:
            recommendations.append(
                f"⚠️  {summary['critical_failures']} critical security categories have failures. "
                "These should be addressed immediately as they represent significant security risks."
            )
        
        # Category-specific recommendations
        for category, result in results["test_categories"].items():
            if result["failed"] > 0:
                category_name = self.test_categories[category]["name"]
                
                if category == "dlp":
                    recommendations.append(
                        f"DLP failures detected: Review data classification policies and "
                        "ensure sensitive data detection rules are properly configured."
                    )
                elif category == "compliance":
                    recommendations.append(
                        f"Compliance failures detected: Review {category_name} controls and "
                        "ensure regulatory requirements are being met."
                    )
                elif category == "access_control":
                    recommendations.append(
                        f"Access control failures detected: Review RBAC/ABAC policies and "
                        "authentication mechanisms for proper implementation."
                    )
                elif category == "api_security":
                    recommendations.append(
                        f"API security failures detected: Review input validation, "
                        "authentication, and authorization for API endpoints."
                    )
                elif category == "penetration":
                    recommendations.append(
                        f"Penetration test failures detected: Review security controls "
                        "to address identified vulnerabilities."
                    )
        
        # Performance recommendations
        if "performance" in results["test_categories"]:
            perf_result = results["test_categories"]["performance"]
            if perf_result["failed"] > 0:
                recommendations.append(
                    "Security performance issues detected: Consider optimizing security "
                    "middleware and controls to reduce performance impact."
                )
        
        # General recommendations
        if summary["total_failed"] == 0:
            recommendations.append(
                "🎉 All security tests passing! Consider running penetration tests "
                "and expanding security test coverage for additional assurance."
            )
        
        return recommendations
    
    def _print_summary(self, results: Dict[str, Any]) -> None:
        """Print comprehensive test summary"""
        
        print("\n" + "=" * 60)
        print("🔒 SECURITY TEST SUMMARY")
        print("=" * 60)
        
        summary = results["summary"]
        
        # Overall status
        status_emoji = "✅" if summary["overall_status"] == "PASS" else "❌"
        print(f"{status_emoji} Overall Status: {summary['overall_status']}")
        print(f"⏱️  Duration: {results['duration_seconds']:.1f}s")
        print(f"📊 Success Rate: {summary['success_rate']:.1%}")
        
        # Test counts
        print(f"\n📈 Test Results:")
        print(f"   ✅ Passed: {summary['total_passed']}")
        print(f"   ❌ Failed: {summary['total_failed']}")
        print(f"   ⏭️  Skipped: {summary['total_skipped']}")
        
        # Critical failures
        if summary["critical_failures"] > 0:
            print(f"\n⚠️  CRITICAL FAILURES: {summary['critical_failures']}")
            for failure in results["critical_failures"]:
                print(f"   - {failure['name']}: {failure['failures']} failures")
        
        # Category breakdown
        print(f"\n📋 Category Results:")
        for category, result in results["test_categories"].items():
            category_name = self.test_categories[category]["name"]
            status = "✅" if result["failed"] == 0 else "❌"
            critical = "🔴" if self.test_categories[category]["critical"] else "🟡"
            
            print(f"   {status} {critical} {category_name}: "
                 f"{result['passed']} passed, {result['failed']} failed "
                 f"({result['duration']:.1f}s)")
        
        # Recommendations
        if results["recommendations"]:
            print(f"\n💡 RECOMMENDATIONS:")
            for i, rec in enumerate(results["recommendations"], 1):
                print(f"   {i}. {rec}")
        
        print("=" * 60)
    
    def _generate_reports(self, results: Dict[str, Any]) -> None:
        """Generate comprehensive security test reports"""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # JSON report
        json_report_file = self.reports_dir / f"security_test_report_{timestamp}.json"
        with open(json_report_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        # HTML report
        html_report = self._generate_html_report(results)
        html_report_file = self.reports_dir / f"security_test_report_{timestamp}.html"
        with open(html_report_file, 'w') as f:
            f.write(html_report)
        
        # CSV summary for spreadsheet analysis
        csv_report = self._generate_csv_report(results)
        csv_report_file = self.reports_dir / f"security_test_summary_{timestamp}.csv"
        with open(csv_report_file, 'w') as f:
            f.write(csv_report)
        
        print(f"\n📄 Reports generated:")
        print(f"   JSON: {json_report_file}")
        print(f"   HTML: {html_report_file}")
        print(f"   CSV:  {csv_report_file}")
    
    def _generate_html_report(self, results: Dict[str, Any]) -> str:
        """Generate HTML report"""
        
        summary = results["summary"]
        status_color = "green" if summary["overall_status"] == "PASS" else "red"
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Security Test Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background: #f4f4f4; padding: 20px; border-radius: 5px; }}
                .status-pass {{ color: green; font-weight: bold; }}
                .status-fail {{ color: red; font-weight: bold; }}
                .critical {{ background: #ffebee; padding: 10px; border-radius: 5px; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background: #f4f4f4; }}
                .recommendations {{ background: #e3f2fd; padding: 15px; border-radius: 5px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🔒 Enterprise Security Test Report</h1>
                <p><strong>Generated:</strong> {results['end_time']}</p>
                <p><strong>Duration:</strong> {results['duration_seconds']:.1f} seconds</p>
                <p class="status-{summary['overall_status'].lower()}">
                    <strong>Status:</strong> {summary['overall_status']}
                </p>
            </div>
            
            <h2>📊 Summary</h2>
            <table>
                <tr><td><strong>Success Rate</strong></td><td>{summary['success_rate']:.1%}</td></tr>
                <tr><td><strong>Tests Passed</strong></td><td>{summary['total_passed']}</td></tr>
                <tr><td><strong>Tests Failed</strong></td><td>{summary['total_failed']}</td></tr>
                <tr><td><strong>Tests Skipped</strong></td><td>{summary['total_skipped']}</td></tr>
                <tr><td><strong>Critical Failures</strong></td><td>{summary['critical_failures']}</td></tr>
            </table>
        """
        
        # Category results
        html += "<h2>📋 Category Results</h2><table><tr><th>Category</th><th>Status</th><th>Passed</th><th>Failed</th><th>Duration</th></tr>"
        
        for category, result in results["test_categories"].items():
            category_name = self.test_categories[category]["name"]
            status = "PASS" if result["failed"] == 0 else "FAIL"
            status_class = "status-pass" if result["failed"] == 0 else "status-fail"
            
            html += f"""
            <tr>
                <td>{category_name}</td>
                <td class="{status_class}">{status}</td>
                <td>{result['passed']}</td>
                <td>{result['failed']}</td>
                <td>{result['duration']:.1f}s</td>
            </tr>
            """
        
        html += "</table>"
        
        # Recommendations
        if results["recommendations"]:
            html += '<div class="recommendations"><h2>💡 Recommendations</h2><ul>'
            for rec in results["recommendations"]:
                html += f"<li>{rec}</li>"
            html += "</ul></div>"
        
        html += "</body></html>"
        return html
    
    def _generate_csv_report(self, results: Dict[str, Any]) -> str:
        """Generate CSV summary report"""
        
        csv_lines = [
            "Category,Name,Status,Passed,Failed,Skipped,Duration,Critical"
        ]
        
        for category, result in results["test_categories"].items():
            category_name = self.test_categories[category]["name"]
            status = "PASS" if result["failed"] == 0 else "FAIL"
            critical = "Yes" if self.test_categories[category]["critical"] else "No"
            
            csv_lines.append(
                f"{category},{category_name},{status},{result['passed']},"
                f"{result['failed']},{result['skipped']},{result['duration']:.1f},{critical}"
            )
        
        return "\n".join(csv_lines)


def main():
    """Main entry point for security test runner"""
    
    parser = argparse.ArgumentParser(
        description="Enterprise Security Test Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all security tests
  python scripts/run_security_tests.py
  
  # Run specific categories
  python scripts/run_security_tests.py --categories dlp compliance
  
  # Include penetration tests (use with caution)
  python scripts/run_security_tests.py --include-penetration
  
  # Verbose output with fail-fast
  python scripts/run_security_tests.py --verbose --fail-fast
        """
    )
    
    parser.add_argument(
        "--categories",
        nargs="+",
        choices=["dlp", "compliance", "access_control", "api_security", "penetration", "performance", "integration"],
        help="Specific test categories to run"
    )
    
    parser.add_argument(
        "--include-penetration",
        action="store_true",
        help="Include penetration tests (use with caution)"
    )
    
    parser.add_argument(
        "--no-reports",
        action="store_true",
        help="Skip report generation"
    )
    
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on first failure"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    # Initialize runner
    project_root = Path(__file__).parent.parent
    runner = SecurityTestRunner(project_root)
    
    # Run tests
    try:
        results = runner.run_security_tests(
            categories=args.categories,
            include_penetration=args.include_penetration,
            generate_reports=not args.no_reports,
            fail_fast=args.fail_fast,
            verbose=args.verbose
        )
        
        # Exit with appropriate code
        if results["summary"]["critical_failures"] > 0:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except KeyboardInterrupt:
        print("\n⚠️  Security tests interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Security test runner failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
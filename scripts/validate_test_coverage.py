#!/usr/bin/env python3
"""
Test Coverage Validation Script
Validates that test coverage meets the 95%+ target across all modules.
"""

import subprocess
import sys
import json
from pathlib import Path
from typing import Dict, List, Any
import xml.etree.ElementTree as ET


class CoverageValidator:
    """Validates test coverage meets enterprise standards."""

    def __init__(self):
        self.project_root = Path.cwd()
        self.target_coverage = 95.0
        self.critical_modules = [
            "src/api/main.py",
            "src/monitoring/enterprise_prometheus_metrics.py",
            "src/domain/__init__.py",
            "src/core/database/indexes.py",
            "src/api/middleware/advanced_caching_middleware.py"
        ]

    def run_coverage_analysis(self) -> Dict[str, Any]:
        """Run comprehensive coverage analysis."""
        print("🔍 Running comprehensive test coverage analysis...")

        # Run tests with coverage
        cmd = [
            sys.executable, "-m", "pytest",
            "--cov=src",
            "--cov-report=xml:coverage.xml",
            "--cov-report=term-missing",
            "--cov-report=json:coverage.json",
            "--cov-branch",
            "-q",
            "--tb=no"
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minutes timeout
            )

            if result.returncode != 0:
                print(f"⚠️  Some tests failed, but continuing coverage analysis...")
                print(f"STDERR: {result.stderr}")

            return self._parse_coverage_results()

        except subprocess.TimeoutExpired:
            print("❌ Test execution timed out")
            return {"error": "timeout"}
        except Exception as e:
            print(f"❌ Error running tests: {e}")
            return {"error": str(e)}

    def _parse_coverage_results(self) -> Dict[str, Any]:
        """Parse coverage results from XML and JSON reports."""
        results = {
            "overall_coverage": 0.0,
            "module_coverage": {},
            "critical_modules_coverage": {},
            "missing_lines": {},
            "branch_coverage": 0.0,
            "meets_target": False
        }

        # Parse XML coverage report
        xml_file = self.project_root / "coverage.xml"
        if xml_file.exists():
            results.update(self._parse_xml_coverage(xml_file))

        # Parse JSON coverage report
        json_file = self.project_root / "coverage.json"
        if json_file.exists():
            results.update(self._parse_json_coverage(json_file))

        return results

    def _parse_xml_coverage(self, xml_file: Path) -> Dict[str, Any]:
        """Parse XML coverage report."""
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()

            # Get overall coverage
            overall_coverage = float(root.attrib.get('line-rate', 0)) * 100
            branch_coverage = float(root.attrib.get('branch-rate', 0)) * 100

            # Get module-level coverage
            module_coverage = {}
            for package in root.findall('.//package'):
                package_name = package.attrib.get('name', 'unknown')
                package_coverage = float(package.attrib.get('line-rate', 0)) * 100
                module_coverage[package_name] = package_coverage

            return {
                "overall_coverage": overall_coverage,
                "branch_coverage": branch_coverage,
                "module_coverage": module_coverage,
                "meets_target": overall_coverage >= self.target_coverage
            }

        except Exception as e:
            print(f"⚠️  Error parsing XML coverage: {e}")
            return {}

    def _parse_json_coverage(self, json_file: Path) -> Dict[str, Any]:
        """Parse JSON coverage report for detailed information."""
        try:
            with open(json_file) as f:
                data = json.load(f)

            files_coverage = {}
            missing_lines = {}

            for filename, file_data in data.get('files', {}).items():
                coverage_percent = file_data['summary']['percent_covered']
                files_coverage[filename] = coverage_percent

                # Get missing lines
                missing = file_data.get('missing_lines', [])
                if missing:
                    missing_lines[filename] = missing

            # Check critical modules
            critical_modules_coverage = {}
            for module in self.critical_modules:
                # Find matching file in coverage data
                for filename, coverage in files_coverage.items():
                    if module.replace('src/', '') in filename:
                        critical_modules_coverage[module] = coverage
                        break

            return {
                "files_coverage": files_coverage,
                "missing_lines": missing_lines,
                "critical_modules_coverage": critical_modules_coverage
            }

        except Exception as e:
            print(f"⚠️  Error parsing JSON coverage: {e}")
            return {}

    def generate_coverage_report(self, results: Dict[str, Any]) -> None:
        """Generate comprehensive coverage report."""
        print("\n" + "="*80)
        print("📊 TEST COVERAGE ANALYSIS REPORT")
        print("="*80)

        if "error" in results:
            print(f"❌ Error in coverage analysis: {results['error']}")
            return

        overall_coverage = results.get("overall_coverage", 0)
        branch_coverage = results.get("branch_coverage", 0)
        meets_target = results.get("meets_target", False)

        # Overall coverage status
        status_icon = "✅" if meets_target else "❌"
        print(f"\n{status_icon} Overall Coverage: {overall_coverage:.2f}% (Target: {self.target_coverage}%)")
        print(f"🌿 Branch Coverage: {branch_coverage:.2f}%")

        # Critical modules coverage
        critical_coverage = results.get("critical_modules_coverage", {})
        if critical_coverage:
            print(f"\n🎯 Critical Modules Coverage:")
            for module, coverage in critical_coverage.items():
                status = "✅" if coverage >= self.target_coverage else "⚠️"
                print(f"  {status} {module}: {coverage:.2f}%")

        # Module coverage summary
        module_coverage = results.get("module_coverage", {})
        if module_coverage:
            print(f"\n📂 Module Coverage Summary:")
            sorted_modules = sorted(module_coverage.items(), key=lambda x: x[1])
            for module, coverage in sorted_modules:
                status = "✅" if coverage >= self.target_coverage else "⚠️"
                print(f"  {status} {module}: {coverage:.2f}%")

        # Files with low coverage
        files_coverage = results.get("files_coverage", {})
        low_coverage_files = {
            f: c for f, c in files_coverage.items()
            if c < self.target_coverage
        }

        if low_coverage_files:
            print(f"\n⚠️  Files Below Target Coverage ({self.target_coverage}%):")
            sorted_files = sorted(low_coverage_files.items(), key=lambda x: x[1])
            for filename, coverage in sorted_files[:10]:  # Show top 10
                print(f"  📄 {filename}: {coverage:.2f}%")

        # Missing lines summary
        missing_lines = results.get("missing_lines", {})
        if missing_lines:
            print(f"\n📝 Files with Missing Test Coverage:")
            for filename, lines in list(missing_lines.items())[:5]:  # Show top 5
                lines_str = ", ".join(map(str, lines[:10]))  # Show first 10 lines
                if len(lines) > 10:
                    lines_str += f" ... (+{len(lines) - 10} more)"
                print(f"  📄 {filename}: Lines {lines_str}")

        # Recommendations
        self._generate_recommendations(results)

    def _generate_recommendations(self, results: Dict[str, Any]) -> None:
        """Generate recommendations for improving coverage."""
        print(f"\n💡 RECOMMENDATIONS:")

        overall_coverage = results.get("overall_coverage", 0)

        if overall_coverage < self.target_coverage:
            gap = self.target_coverage - overall_coverage
            print(f"  📈 Increase overall coverage by {gap:.2f} percentage points")

            # Specific recommendations based on analysis
            files_coverage = results.get("files_coverage", {})
            low_coverage_files = [
                f for f, c in files_coverage.items()
                if c < 80  # Files with very low coverage
            ]

            if low_coverage_files:
                print(f"  🎯 Focus on files with <80% coverage:")
                for filename in low_coverage_files[:3]:
                    print(f"     - {filename}")

            print(f"  ✅ Add tests for:")
            print(f"     - Error handling scenarios")
            print(f"     - Edge cases and boundary conditions")
            print(f"     - Integration between components")
            print(f"     - Async functionality and concurrency")

        else:
            print(f"  🎉 Excellent! Coverage target achieved!")
            print(f"  🔄 Maintain coverage with:")
            print(f"     - Continuous integration checks")
            print(f"     - Regular coverage monitoring")
            print(f"     - New feature test requirements")

    def validate_test_quality(self) -> Dict[str, Any]:
        """Validate test quality metrics beyond just coverage."""
        print("\n🔍 Validating test quality metrics...")

        quality_metrics = {
            "test_file_count": 0,
            "test_function_count": 0,
            "async_test_count": 0,
            "integration_test_count": 0,
            "performance_test_count": 0,
            "security_test_count": 0
        }

        # Count test files and analyze test quality
        test_dir = self.project_root / "tests"
        if test_dir.exists():
            test_files = list(test_dir.rglob("test_*.py"))
            quality_metrics["test_file_count"] = len(test_files)

            # Analyze test functions
            for test_file in test_files:
                try:
                    with open(test_file, 'r', encoding='utf-8') as f:
                        content = f.read()

                    # Count test functions
                    quality_metrics["test_function_count"] += content.count("def test_")

                    # Count specific test types
                    quality_metrics["async_test_count"] += content.count("async def test_")
                    quality_metrics["integration_test_count"] += content.count("@pytest.mark.integration")
                    quality_metrics["performance_test_count"] += content.count("@pytest.mark.performance")
                    quality_metrics["security_test_count"] += content.count("@pytest.mark.security")

                except Exception as e:
                    print(f"⚠️  Error analyzing {test_file}: {e}")

        return quality_metrics

    def run_full_validation(self) -> bool:
        """Run full test coverage validation."""
        print("🚀 Starting comprehensive test coverage validation...")

        # Run coverage analysis
        coverage_results = self.run_coverage_analysis()

        # Generate coverage report
        self.generate_coverage_report(coverage_results)

        # Validate test quality
        quality_metrics = self.validate_test_quality()

        print(f"\n📊 TEST SUITE QUALITY METRICS:")
        print(f"  📁 Test files: {quality_metrics['test_file_count']}")
        print(f"  🧪 Test functions: {quality_metrics['test_function_count']}")
        print(f"  ⚡ Async tests: {quality_metrics['async_test_count']}")
        print(f"  🔗 Integration tests: {quality_metrics['integration_test_count']}")
        print(f"  ⏱️  Performance tests: {quality_metrics['performance_test_count']}")
        print(f"  🔒 Security tests: {quality_metrics['security_test_count']}")

        # Final validation
        meets_coverage_target = coverage_results.get("meets_target", False)
        has_sufficient_tests = quality_metrics["test_function_count"] > 100

        overall_success = meets_coverage_target and has_sufficient_tests

        print(f"\n" + "="*80)
        if overall_success:
            print("✅ SUCCESS: Test coverage validation passed!")
            print(f"   Coverage: {coverage_results.get('overall_coverage', 0):.2f}% (≥{self.target_coverage}%)")
            print(f"   Test functions: {quality_metrics['test_function_count']} (≥100)")
        else:
            print("❌ FAILURE: Test coverage validation failed!")
            if not meets_coverage_target:
                print(f"   Coverage below target: {coverage_results.get('overall_coverage', 0):.2f}% < {self.target_coverage}%")
            if not has_sufficient_tests:
                print(f"   Insufficient test functions: {quality_metrics['test_function_count']} < 100")

        print("="*80)

        return overall_success


def main():
    """Main entry point for coverage validation."""
    validator = CoverageValidator()
    success = validator.run_full_validation()

    if success:
        print("\n🎉 All test coverage requirements met! Ready for production deployment.")
        sys.exit(0)
    else:
        print("\n⚠️  Test coverage requirements not met. Please address issues above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
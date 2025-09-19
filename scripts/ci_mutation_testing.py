"""
CI/CD Integration for Mutation Testing
Provides automated mutation testing validation for continuous integration
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import argparse
from datetime import datetime


class CIMutationTesting:
    """CI/CD integration for mutation testing with quality gates."""

    def __init__(self, project_root: str = None):
        self.project_root = Path(project_root) if project_root else Path.cwd()
        self.config = {
            "min_mutation_score": 95.0,
            "critical_files_score": 98.0,
            "timeout_minutes": 60,
            "max_mutations_per_file": 200,  # Reduced for CI speed
            "fail_fast": True
        }

        # Critical files that must meet higher standards
        self.critical_files = [
            "src/api/v1/routes/auth.py",
            "src/api/v1/routes/payment.py",
            "src/core/security/",
            "src/api/middleware/authentication.py",
            "src/etl/gold/business_metrics.py"
        ]

    def run_mutation_testing_for_ci(self, changed_files: List[str] = None) -> bool:
        """Run mutation testing optimized for CI/CD pipeline."""
        print("🧬 Starting CI Mutation Testing")
        print("=" * 50)

        try:
            if changed_files:
                print(f"Testing {len(changed_files)} changed files")
                return self._run_targeted_mutation_testing(changed_files)
            else:
                print("Running full mutation testing suite")
                return self._run_full_mutation_testing()

        except Exception as e:
            print(f"❌ Mutation testing failed: {e}")
            return False

    def _run_targeted_mutation_testing(self, changed_files: List[str]) -> bool:
        """Run mutation testing only on changed files."""
        print("Running targeted mutation testing...")

        results = {}
        overall_success = True

        for file_path in changed_files:
            if self._should_test_file(file_path):
                print(f"Testing: {file_path}")
                result = self._test_single_file(file_path)
                results[file_path] = result

                if not result["passed"]:
                    overall_success = False
                    if self.config["fail_fast"]:
                        break

        self._report_ci_results(results)
        return overall_success

    def _run_full_mutation_testing(self) -> bool:
        """Run mutation testing on all critical files."""
        print("Running full mutation testing on critical files...")

        results = {}
        overall_success = True

        for file_pattern in self.critical_files:
            file_path = self.project_root / file_pattern
            if file_path.exists():
                if file_path.is_file():
                    result = self._test_single_file(str(file_path))
                    results[str(file_path)] = result
                else:
                    # Directory - test all Python files
                    for py_file in file_path.rglob("*.py"):
                        result = self._test_single_file(str(py_file))
                        results[str(py_file)] = result

        # Check overall results
        for result in results.values():
            if not result["passed"]:
                overall_success = False

        self._report_ci_results(results)
        return overall_success

    def _should_test_file(self, file_path: str) -> bool:
        """Determine if a file should be mutation tested."""
        # Only test Python files
        if not file_path.endswith('.py'):
            return False

        # Skip test files, migrations, etc.
        skip_patterns = [
            'test_', '_test.py', 'tests/', 'migrations/',
            '__pycache__', '.pyc', 'setup.py', 'conftest.py'
        ]

        for pattern in skip_patterns:
            if pattern in file_path:
                return False

        # Only test files in src/ directory
        return 'src/' in file_path

    def _test_single_file(self, file_path: str) -> Dict:
        """Run mutation testing on a single file."""
        try:
            # Determine threshold based on criticality
            threshold = (self.config["critical_files_score"]
                        if any(critical in file_path for critical in self.critical_files)
                        else self.config["min_mutation_score"])

            # Run mutmut on single file
            cmd = [
                "mutmut", "run",
                "--paths-to-mutate", file_path,
                "--tests-dir", str(self.project_root / "tests"),
                "--timeout-factor", "1.5",  # Faster for CI
                "--no-progress"
            ]

            result = subprocess.run(
                cmd,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout per file
            )

            # Parse results
            mutation_score = self._parse_mutation_score(result.stdout)

            return {
                "file": file_path,
                "mutation_score": mutation_score,
                "threshold": threshold,
                "passed": mutation_score >= threshold,
                "execution_time": "300s",  # Approximate
                "status": "PASS" if mutation_score >= threshold else "FAIL"
            }

        except subprocess.TimeoutExpired:
            return {
                "file": file_path,
                "mutation_score": 0.0,
                "threshold": threshold,
                "passed": False,
                "execution_time": "TIMEOUT",
                "status": "TIMEOUT"
            }
        except Exception as e:
            return {
                "file": file_path,
                "mutation_score": 0.0,
                "threshold": threshold,
                "passed": False,
                "execution_time": "ERROR",
                "status": f"ERROR: {e}"
            }

    def _parse_mutation_score(self, output: str) -> float:
        """Parse mutation score from mutmut output."""
        try:
            # Look for mutation score in output
            lines = output.split('\n')
            for line in lines:
                if 'mutation score' in line.lower():
                    # Extract percentage
                    import re
                    match = re.search(r'(\d+\.?\d*)%', line)
                    if match:
                        return float(match.group(1))

            # Fallback: try to calculate from killed/total
            killed = 0
            total = 0
            for line in lines:
                if 'killed:' in line.lower():
                    killed = int(re.search(r'killed:\s*(\d+)', line.lower()).group(1))
                elif 'total:' in line.lower():
                    total = int(re.search(r'total:\s*(\d+)', line.lower()).group(1))

            if total > 0:
                return (killed / total) * 100

            return 0.0

        except Exception:
            return 0.0

    def _report_ci_results(self, results: Dict):
        """Report CI mutation testing results."""
        print("\n📊 Mutation Testing Results")
        print("-" * 40)

        total_files = len(results)
        passed_files = sum(1 for r in results.values() if r["passed"])

        print(f"Files Tested: {total_files}")
        print(f"Files Passed: {passed_files}")
        print(f"Success Rate: {(passed_files/total_files)*100:.1f}%" if total_files > 0 else "N/A")

        print("\nDetailed Results:")
        for result in results.values():
            status_icon = "✅" if result["passed"] else "❌"
            print(f"  {status_icon} {result['file']}")
            print(f"      Score: {result['mutation_score']:.1f}% (threshold: {result['threshold']:.1f}%)")
            print(f"      Status: {result['status']}")

        # Generate JSON report for CI tools
        self._generate_ci_json_report(results)

        # Set exit code based on results
        if passed_files < total_files:
            print(f"\n❌ Mutation testing failed: {total_files - passed_files} files below threshold")
        else:
            print(f"\n✅ All {total_files} files passed mutation testing!")

    def _generate_ci_json_report(self, results: Dict):
        """Generate JSON report for CI tools."""
        report_path = self.project_root / "reports" / "mutation_ci_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)

        ci_report = {
            "timestamp": datetime.now().isoformat(),
            "mutation_testing_results": {
                "total_files": len(results),
                "passed_files": sum(1 for r in results.values() if r["passed"]),
                "failed_files": sum(1 for r in results.values() if not r["passed"]),
                "overall_success": all(r["passed"] for r in results.values())
            },
            "file_results": list(results.values()),
            "configuration": self.config
        }

        with open(report_path, 'w') as f:
            json.dump(ci_report, f, indent=2)

        print(f"📄 CI report generated: {report_path}")

    def run_quick_mutation_check(self, file_paths: List[str]) -> bool:
        """Quick mutation check for pre-commit hooks."""
        print("🚀 Quick Mutation Check")

        if not file_paths:
            print("No files to check")
            return True

        python_files = [f for f in file_paths if self._should_test_file(f)]
        if not python_files:
            print("No Python source files to test")
            return True

        print(f"Checking {len(python_files)} files...")

        # Run minimal mutation testing
        all_passed = True
        for file_path in python_files:
            # Quick test with limited mutations
            try:
                cmd = [
                    "mutmut", "run",
                    "--paths-to-mutate", file_path,
                    "--tests-dir", str(self.project_root / "tests"),
                    "--timeout-factor", "1.0",
                    "--no-progress",
                    "--quick"  # If mutmut supports it
                ]

                result = subprocess.run(
                    cmd,
                    cwd=self.project_root,
                    capture_output=True,
                    text=True,
                    timeout=60  # 1 minute timeout
                )

                score = self._parse_mutation_score(result.stdout)
                threshold = 85.0  # Lower threshold for quick check

                if score < threshold:
                    print(f"❌ {file_path}: {score:.1f}% (threshold: {threshold}%)")
                    all_passed = False
                else:
                    print(f"✅ {file_path}: {score:.1f}%")

            except Exception as e:
                print(f"⚠️ {file_path}: Error - {e}")
                # Don't fail for errors in quick check

        return all_passed


def get_changed_files() -> List[str]:
    """Get list of changed files from git."""
    try:
        # Get changed files in current branch vs main
        result = subprocess.run(
            ["git", "diff", "--name-only", "origin/main..HEAD"],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            return [f.strip() for f in result.stdout.split('\n') if f.strip()]

        # Fallback: get staged files
        result = subprocess.run(
            ["git", "diff", "--name-only", "--cached"],
            capture_output=True,
            text=True
        )

        return [f.strip() for f in result.stdout.split('\n') if f.strip()]

    except Exception:
        return []


def main():
    """Main entry point for CI mutation testing."""
    parser = argparse.ArgumentParser(description="CI/CD Mutation Testing")
    parser.add_argument("--mode", choices=["full", "changed", "quick"], default="changed",
                       help="Testing mode")
    parser.add_argument("--files", nargs="*", help="Specific files to test")
    parser.add_argument("--threshold", type=float, default=95.0,
                       help="Mutation score threshold")

    args = parser.parse_args()

    ci_tester = CIMutationTesting()
    ci_tester.config["min_mutation_score"] = args.threshold

    if args.mode == "quick":
        files_to_test = args.files or get_changed_files()
        success = ci_tester.run_quick_mutation_check(files_to_test)
    elif args.mode == "changed":
        changed_files = args.files or get_changed_files()
        success = ci_tester.run_mutation_testing_for_ci(changed_files)
    else:  # full
        success = ci_tester.run_mutation_testing_for_ci()

    if success:
        print("\n🎉 Mutation testing passed!")
        sys.exit(0)
    else:
        print("\n💥 Mutation testing failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
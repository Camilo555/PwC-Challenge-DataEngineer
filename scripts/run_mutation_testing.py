"""
Comprehensive Mutation Testing Framework with mutmut
Validates test effectiveness by introducing code mutations and ensuring tests catch them.
Target: 95%+ mutation test score for critical business logic
"""

import os
import sys
import subprocess
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import logging


@dataclass
class MutationTestResult:
    """Results from mutation testing."""
    module: str
    total_mutants: int
    killed_mutants: int
    survived_mutants: int
    suspicious_mutants: int
    timeout_mutants: int
    mutation_score: float
    execution_time: float
    critical_survivals: List[str]


class MutationTestingFramework:
    """Comprehensive mutation testing framework with business logic focus."""

    def __init__(self, project_root: str = None):
        self.project_root = Path(project_root) if project_root else Path.cwd()
        self.results_dir = self.project_root / "reports" / "mutation"
        self.results_dir.mkdir(parents=True, exist_ok=True)

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.results_dir / "mutation_testing.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

        # Critical modules for business logic (prioritize these)
        self.critical_modules = [
            "src/api/v1/routes/",
            "src/api/graphql/resolvers.py",
            "src/core/database/",
            "src/etl/gold/",
            "src/monitoring/",
            "src/api/middleware/"
        ]

        # Mutation testing configuration
        self.mutation_config = {
            "timeout_factor": 2.0,
            "max_mutations_per_module": 500,
            "min_mutation_score_threshold": 95.0,
            "critical_module_threshold": 98.0,
            "excluded_patterns": [
                "__pycache__",
                ".pytest_cache",
                ".venv",
                "tests/",
                "docs/",
                "terraform/",
                "docker/"
            ]
        }

    def run_full_mutation_testing_suite(self) -> Dict[str, MutationTestResult]:
        """Run comprehensive mutation testing on all critical modules."""
        self.logger.info("Starting comprehensive mutation testing suite...")

        start_time = time.time()
        results = {}

        # Run mutation testing on critical modules first
        for module in self.critical_modules:
            module_path = self.project_root / module
            if module_path.exists():
                self.logger.info(f"Running mutation testing on critical module: {module}")
                result = self._run_module_mutation_testing(str(module_path))
                if result:
                    results[module] = result

        # Generate comprehensive report
        self._generate_mutation_report(results, time.time() - start_time)

        return results

    def _run_module_mutation_testing(self, module_path: str) -> Optional[MutationTestResult]:
        """Run mutation testing on a specific module."""
        try:
            # Prepare mutmut command
            cmd = [
                "mutmut",
                "run",
                "--paths-to-mutate", module_path,
                "--tests-dir", str(self.project_root / "tests"),
                "--runner", "pytest",
                "--timeout-factor", str(self.mutation_config["timeout_factor"]),
                "--no-progress"
            ]

            self.logger.info(f"Executing mutation testing: {' '.join(cmd)}")

            start_time = time.time()
            result = subprocess.run(
                cmd,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            execution_time = time.time() - start_time

            # Parse mutmut results
            return self._parse_mutmut_results(module_path, result, execution_time)

        except subprocess.TimeoutExpired:
            self.logger.error(f"Mutation testing timed out for module: {module_path}")
            return None
        except Exception as e:
            self.logger.error(f"Error running mutation testing on {module_path}: {e}")
            return None

    def _parse_mutmut_results(self, module_path: str, result: subprocess.CompletedProcess, execution_time: float) -> Optional[MutationTestResult]:
        """Parse mutmut results and extract statistics."""
        try:
            # Get mutation results using mutmut results command
            results_cmd = ["mutmut", "results"]
            results_proc = subprocess.run(
                results_cmd,
                cwd=self.project_root,
                capture_output=True,
                text=True
            )

            # Parse the output for statistics
            output_lines = results_proc.stdout.split('\n')

            total_mutants = 0
            killed_mutants = 0
            survived_mutants = 0
            suspicious_mutants = 0
            timeout_mutants = 0

            for line in output_lines:
                if 'Total mutants:' in line:
                    total_mutants = int(line.split(':')[1].strip())
                elif 'Killed:' in line:
                    killed_mutants = int(line.split(':')[1].strip())
                elif 'Survived:' in line:
                    survived_mutants = int(line.split(':')[1].strip())
                elif 'Suspicious:' in line:
                    suspicious_mutants = int(line.split(':')[1].strip())
                elif 'Timeout:' in line:
                    timeout_mutants = int(line.split(':')[1].strip())

            # Calculate mutation score
            if total_mutants > 0:
                mutation_score = (killed_mutants / total_mutants) * 100
            else:
                mutation_score = 0.0

            # Get surviving mutants details
            critical_survivals = self._analyze_surviving_mutants()

            return MutationTestResult(
                module=module_path,
                total_mutants=total_mutants,
                killed_mutants=killed_mutants,
                survived_mutants=survived_mutants,
                suspicious_mutants=suspicious_mutants,
                timeout_mutants=timeout_mutants,
                mutation_score=mutation_score,
                execution_time=execution_time,
                critical_survivals=critical_survivals
            )

        except Exception as e:
            self.logger.error(f"Error parsing mutmut results: {e}")
            return None

    def _analyze_surviving_mutants(self) -> List[str]:
        """Analyze surviving mutants to identify critical issues."""
        critical_survivals = []

        try:
            # Get surviving mutants
            survival_cmd = ["mutmut", "show", "--filter", "survived"]
            survival_proc = subprocess.run(
                survival_cmd,
                cwd=self.project_root,
                capture_output=True,
                text=True
            )

            # Parse surviving mutants and categorize by criticality
            output_lines = survival_proc.stdout.split('\n')
            for line in output_lines:
                if any(critical in line for critical in [
                    "authentication", "authorization", "payment", "security",
                    "validation", "database", "critical", "business"
                ]):
                    critical_survivals.append(line.strip())

        except Exception as e:
            self.logger.warning(f"Could not analyze surviving mutants: {e}")

        return critical_survivals

    def run_targeted_mutation_testing(self, file_path: str) -> Optional[MutationTestResult]:
        """Run mutation testing on a specific file."""
        self.logger.info(f"Running targeted mutation testing on: {file_path}")

        try:
            cmd = [
                "mutmut",
                "run",
                "--paths-to-mutate", file_path,
                "--tests-dir", str(self.project_root / "tests"),
                "--runner", "pytest"
            ]

            start_time = time.time()
            result = subprocess.run(cmd, cwd=self.project_root, capture_output=True, text=True)
            execution_time = time.time() - start_time

            return self._parse_mutmut_results(file_path, result, execution_time)

        except Exception as e:
            self.logger.error(f"Error in targeted mutation testing: {e}")
            return None

    def validate_test_effectiveness(self, mutation_results: Dict[str, MutationTestResult]) -> Dict[str, str]:
        """Validate test effectiveness based on mutation scores."""
        validation_results = {}

        for module, result in mutation_results.items():
            if module in self.critical_modules:
                threshold = self.mutation_config["critical_module_threshold"]
            else:
                threshold = self.mutation_config["min_mutation_score_threshold"]

            if result.mutation_score >= threshold:
                validation_results[module] = "EXCELLENT"
            elif result.mutation_score >= threshold - 5:
                validation_results[module] = "GOOD"
            elif result.mutation_score >= threshold - 10:
                validation_results[module] = "ACCEPTABLE"
            else:
                validation_results[module] = "NEEDS_IMPROVEMENT"

        return validation_results

    def _generate_mutation_report(self, results: Dict[str, MutationTestResult], total_time: float):
        """Generate comprehensive mutation testing report."""
        report_path = self.results_dir / f"mutation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        # Calculate overall statistics
        total_mutants = sum(r.total_mutants for r in results.values())
        total_killed = sum(r.killed_mutants for r in results.values())
        overall_score = (total_killed / total_mutants * 100) if total_mutants > 0 else 0

        report_data = {
            "mutation_testing_summary": {
                "timestamp": datetime.now().isoformat(),
                "total_execution_time_seconds": total_time,
                "overall_mutation_score": overall_score,
                "total_mutants_generated": total_mutants,
                "total_mutants_killed": total_killed,
                "modules_tested": len(results),
                "critical_modules_tested": len([m for m in results.keys() if m in self.critical_modules])
            },
            "module_results": {
                module: {
                    "total_mutants": result.total_mutants,
                    "killed_mutants": result.killed_mutants,
                    "survived_mutants": result.survived_mutants,
                    "mutation_score": result.mutation_score,
                    "execution_time_seconds": result.execution_time,
                    "critical_survivals_count": len(result.critical_survivals),
                    "critical_survivals": result.critical_survivals,
                    "status": "CRITICAL" if module in self.critical_modules else "STANDARD"
                }
                for module, result in results.items()
            },
            "test_effectiveness_analysis": self.validate_test_effectiveness(results),
            "recommendations": self._generate_recommendations(results),
            "configuration": self.mutation_config
        }

        # Write JSON report
        with open(report_path, 'w') as f:
            json.dump(report_data, f, indent=2)

        # Generate human-readable report
        self._generate_human_readable_report(report_data)

        self.logger.info(f"Mutation testing report generated: {report_path}")

    def _generate_human_readable_report(self, report_data: Dict):
        """Generate human-readable mutation testing report."""
        report_path = self.results_dir / f"mutation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

        with open(report_path, 'w') as f:
            f.write("# Comprehensive Mutation Testing Report\n\n")
            f.write(f"**Generated:** {report_data['mutation_testing_summary']['timestamp']}\n\n")

            # Summary section
            summary = report_data['mutation_testing_summary']
            f.write("## Executive Summary\n\n")
            f.write(f"- **Overall Mutation Score:** {summary['overall_mutation_score']:.2f}%\n")
            f.write(f"- **Total Mutants Generated:** {summary['total_mutants_generated']}\n")
            f.write(f"- **Total Mutants Killed:** {summary['total_mutants_killed']}\n")
            f.write(f"- **Execution Time:** {summary['total_execution_time_seconds']:.2f} seconds\n")
            f.write(f"- **Modules Tested:** {summary['modules_tested']}\n\n")

            # Module results
            f.write("## Module Results\n\n")
            for module, result in report_data['module_results'].items():
                f.write(f"### {module}\n\n")
                f.write(f"- **Mutation Score:** {result['mutation_score']:.2f}%\n")
                f.write(f"- **Total Mutants:** {result['total_mutants']}\n")
                f.write(f"- **Killed:** {result['killed_mutants']}\n")
                f.write(f"- **Survived:** {result['survived_mutants']}\n")
                f.write(f"- **Execution Time:** {result['execution_time_seconds']:.2f}s\n")

                if result['critical_survivals']:
                    f.write(f"- **Critical Surviving Mutants:** {result['critical_survivals_count']}\n")
                    for survival in result['critical_survivals'][:5]:  # Show first 5
                        f.write(f"  - {survival}\n")
                f.write("\n")

            # Recommendations
            f.write("## Recommendations\n\n")
            for rec in report_data['recommendations']:
                f.write(f"- {rec}\n")

        self.logger.info(f"Human-readable report generated: {report_path}")

    def _generate_recommendations(self, results: Dict[str, MutationTestResult]) -> List[str]:
        """Generate recommendations based on mutation testing results."""
        recommendations = []

        for module, result in results.items():
            if result.mutation_score < 95:
                recommendations.append(
                    f"Improve test coverage for {module} - current mutation score: {result.mutation_score:.1f}%"
                )

            if result.critical_survivals:
                recommendations.append(
                    f"Review critical surviving mutants in {module} - {len(result.critical_survivals)} critical issues found"
                )

            if result.survived_mutants > result.killed_mutants * 0.1:  # More than 10% survival rate
                recommendations.append(
                    f"Add edge case tests for {module} - high mutant survival rate detected"
                )

        # General recommendations
        overall_score = sum(r.mutation_score for r in results.values()) / len(results) if results else 0
        if overall_score < 95:
            recommendations.extend([
                "Consider adding property-based testing with Hypothesis for better coverage",
                "Review and strengthen assertion statements in existing tests",
                "Add negative test cases and error condition testing",
                "Implement boundary value testing for numerical operations"
            ])

        return recommendations

    def generate_mutation_test_summary(self) -> Dict[str, Any]:
        """Generate a summary of mutation testing capabilities and configuration."""
        return {
            "framework": "mutmut",
            "target_mutation_score": self.mutation_config["min_mutation_score_threshold"],
            "critical_module_threshold": self.mutation_config["critical_module_threshold"],
            "critical_modules": self.critical_modules,
            "mutation_operators": [
                "Arithmetic operators (+, -, *, /, %)",
                "Comparison operators (<, >, <=, >=, ==, !=)",
                "Boolean operators (and, or, not)",
                "Constant values (numbers, strings, booleans)",
                "Index operations",
                "Slice operations",
                "Function calls",
                "Return statements"
            ],
            "test_quality_metrics": [
                "Mutation score (killed mutants / total mutants)",
                "Critical business logic coverage",
                "Edge case detection capability",
                "Error handling validation",
                "Boundary condition testing"
            ],
            "integration_features": [
                "CI/CD pipeline integration",
                "Comprehensive reporting",
                "Critical module prioritization",
                "Business logic focus",
                "Performance monitoring"
            ]
        }


def main():
    """Main entry point for mutation testing."""
    print("🧬 Starting Comprehensive Mutation Testing Framework")
    print("=" * 60)

    framework = MutationTestingFramework()

    # Display configuration
    print("Configuration:")
    config = framework.generate_mutation_test_summary()
    print(f"  Target Mutation Score: {config['target_mutation_score']}%")
    print(f"  Critical Module Threshold: {config['critical_module_threshold']}%")
    print(f"  Critical Modules: {len(config['critical_modules'])}")
    print()

    # Run mutation testing
    try:
        print("Running comprehensive mutation testing suite...")
        results = framework.run_full_mutation_testing_suite()

        if results:
            print("\n🎯 Mutation Testing Results:")
            print("-" * 40)

            total_mutants = sum(r.total_mutants for r in results.values())
            total_killed = sum(r.killed_mutants for r in results.values())
            overall_score = (total_killed / total_mutants * 100) if total_mutants > 0 else 0

            print(f"Overall Mutation Score: {overall_score:.2f}%")
            print(f"Total Mutants: {total_mutants}")
            print(f"Killed Mutants: {total_killed}")
            print(f"Modules Tested: {len(results)}")

            print("\nModule Scores:")
            for module, result in results.items():
                status = "✅" if result.mutation_score >= 95 else "⚠️" if result.mutation_score >= 90 else "❌"
                print(f"  {status} {module}: {result.mutation_score:.1f}%")

            # Determine overall success
            if overall_score >= 95:
                print("\n🎉 EXCELLENT: Mutation testing shows high test effectiveness!")
            elif overall_score >= 90:
                print("\n✅ GOOD: Test effectiveness is solid with room for improvement")
            else:
                print("\n⚠️ NEEDS IMPROVEMENT: Consider strengthening test coverage")

        else:
            print("❌ No mutation testing results generated")

    except Exception as e:
        print(f"❌ Error during mutation testing: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
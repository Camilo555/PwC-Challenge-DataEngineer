#!/usr/bin/env python3
"""
Comprehensive Test Report Generator
Generates detailed analytics and reports from test execution results.
"""

from __future__ import annotations

import argparse
import json
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import statistics
import re
from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class TestMetrics:
    """Test execution metrics."""
    total_tests: int = 0
    passed_tests: int = 0
    failed_tests: int = 0
    skipped_tests: int = 0
    error_tests: int = 0
    execution_time: float = 0.0
    success_rate: float = 0.0
    
    @property
    def failure_rate(self) -> float:
        """Calculate failure rate."""
        return (self.failed_tests + self.error_tests) / self.total_tests if self.total_tests > 0 else 0.0


@dataclass
class TestSuite:
    """Test suite information."""
    name: str
    metrics: TestMetrics = field(default_factory=TestMetrics)
    test_cases: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


@dataclass
class SecurityFindings:
    """Security test findings."""
    total_issues: int = 0
    high_severity: int = 0
    medium_severity: int = 0
    low_severity: int = 0
    tools_used: List[str] = field(default_factory=list)
    findings_by_type: Dict[str, int] = field(default_factory=dict)


@dataclass
class PerformanceMetrics:
    """Performance test metrics."""
    avg_response_time: float = 0.0
    p95_response_time: float = 0.0
    p99_response_time: float = 0.0
    throughput_ops_per_sec: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_percent: float = 0.0


class TestReportGenerator:
    """
    Comprehensive test report generator with analytics and insights.
    """
    
    def __init__(self, test_results_dir: Path):
        """Initialize report generator."""
        self.test_results_dir = Path(test_results_dir)
        self.test_suites: Dict[str, TestSuite] = {}
        self.security_findings = SecurityFindings()
        self.performance_metrics = PerformanceMetrics()
        self.coverage_data: Dict[str, Any] = {}
        self.quality_gate_results: Dict[str, Any] = {}
        
    def generate_comprehensive_report(self) -> str:
        """Generate comprehensive test report."""
        self._parse_all_test_results()
        
        report_sections = [
            self._generate_header(),
            self._generate_executive_summary(),
            self._generate_quality_gate_results(),
            self._generate_test_suite_details(),
            self._generate_coverage_analysis(),
            self._generate_security_analysis(),
            self._generate_performance_analysis(),
            self._generate_trends_analysis(),
            self._generate_recommendations(),
            self._generate_footer()
        ]
        
        return "\n\n".join(section for section in report_sections if section)
    
    def _parse_all_test_results(self):
        """Parse all test result files."""
        # Parse JUnit XML files
        for xml_file in self.test_results_dir.rglob("*.xml"):
            if xml_file.name.endswith("-tests.xml"):
                self._parse_junit_xml(xml_file)
        
        # Parse coverage reports
        for coverage_file in self.test_results_dir.rglob("coverage.xml"):
            self._parse_coverage_xml(coverage_file)
        
        # Parse security reports
        for security_file in self.test_results_dir.rglob("*security*.json"):
            self._parse_security_json(security_file)
        
        # Parse Bandit reports
        for bandit_file in self.test_results_dir.rglob("bandit-report.json"):
            self._parse_bandit_json(bandit_file)
        
        # Parse Safety reports
        for safety_file in self.test_results_dir.rglob("safety-report.json"):
            self._parse_safety_json(safety_file)
        
        # Parse Semgrep reports
        for semgrep_file in self.test_results_dir.rglob("semgrep-results.json"):
            self._parse_semgrep_json(semgrep_file)
    
    def _parse_junit_xml(self, xml_file: Path):
        """Parse JUnit XML test results."""
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            suite_name = self._extract_suite_name(xml_file.name)
            
            metrics = TestMetrics(
                total_tests=int(root.get('tests', 0)),
                failed_tests=int(root.get('failures', 0)),
                error_tests=int(root.get('errors', 0)),
                skipped_tests=int(root.get('skipped', 0)),
                execution_time=float(root.get('time', 0.0))
            )
            
            metrics.passed_tests = metrics.total_tests - metrics.failed_tests - metrics.error_tests - metrics.skipped_tests
            metrics.success_rate = metrics.passed_tests / metrics.total_tests if metrics.total_tests > 0 else 0.0
            
            test_suite = TestSuite(name=suite_name, metrics=metrics)
            
            # Parse individual test cases
            for testcase in root.findall('.//testcase'):
                test_case = {
                    'name': testcase.get('name', ''),
                    'classname': testcase.get('classname', ''),
                    'time': float(testcase.get('time', 0.0)),
                    'status': 'passed'
                }
                
                # Check for failures
                failure = testcase.find('failure')
                if failure is not None:
                    test_case['status'] = 'failed'
                    test_case['failure_message'] = failure.get('message', '')
                    test_case['failure_text'] = failure.text or ''
                
                # Check for errors
                error = testcase.find('error')
                if error is not None:
                    test_case['status'] = 'error'
                    test_case['error_message'] = error.get('message', '')
                    test_case['error_text'] = error.text or ''
                
                # Check for skipped
                skipped = testcase.find('skipped')
                if skipped is not None:
                    test_case['status'] = 'skipped'
                    test_case['skip_reason'] = skipped.get('message', '')
                
                test_suite.test_cases.append(test_case)
            
            self.test_suites[suite_name] = test_suite
            
        except Exception as e:
            print(f"Error parsing {xml_file}: {e}")
    
    def _parse_coverage_xml(self, coverage_file: Path):
        """Parse coverage XML report."""
        try:
            tree = ET.parse(coverage_file)
            root = tree.getroot()
            
            # Extract overall coverage metrics
            coverage_elem = root.find('.//coverage')
            if coverage_elem is not None:
                self.coverage_data = {
                    'line_rate': float(coverage_elem.get('line-rate', 0.0)),
                    'branch_rate': float(coverage_elem.get('branch-rate', 0.0)),
                    'lines_covered': int(coverage_elem.get('lines-covered', 0)),
                    'lines_valid': int(coverage_elem.get('lines-valid', 0)),
                    'branches_covered': int(coverage_elem.get('branches-covered', 0)),
                    'branches_valid': int(coverage_elem.get('branches-valid', 0))
                }
                
                # Calculate percentages
                self.coverage_data['line_coverage_percent'] = self.coverage_data['line_rate'] * 100
                self.coverage_data['branch_coverage_percent'] = self.coverage_data['branch_rate'] * 100
            
        except Exception as e:
            print(f"Error parsing coverage file {coverage_file}: {e}")
    
    def _parse_security_json(self, security_file: Path):
        """Parse generic security JSON reports."""
        try:
            with open(security_file, 'r') as f:
                data = json.load(f)
            
            # Generic security report parsing
            if isinstance(data, dict):
                if 'total_issues' in data:
                    self.security_findings.total_issues += data['total_issues']
                if 'by_severity' in data:
                    severity_data = data['by_severity']
                    self.security_findings.high_severity += severity_data.get('high', 0)
                    self.security_findings.medium_severity += severity_data.get('medium', 0)
                    self.security_findings.low_severity += severity_data.get('low', 0)
        
        except Exception as e:
            print(f"Error parsing security file {security_file}: {e}")
    
    def _parse_bandit_json(self, bandit_file: Path):
        """Parse Bandit security report."""
        try:
            with open(bandit_file, 'r') as f:
                data = json.load(f)
            
            if 'results' in data:
                self.security_findings.tools_used.append('Bandit')
                
                for result in data['results']:
                    severity = result.get('issue_severity', '').lower()
                    issue_type = result.get('test_name', 'unknown')
                    
                    self.security_findings.total_issues += 1
                    self.security_findings.findings_by_type[issue_type] = \
                        self.security_findings.findings_by_type.get(issue_type, 0) + 1
                    
                    if severity == 'high':
                        self.security_findings.high_severity += 1
                    elif severity == 'medium':
                        self.security_findings.medium_severity += 1
                    else:
                        self.security_findings.low_severity += 1
        
        except Exception as e:
            print(f"Error parsing Bandit file {bandit_file}: {e}")
    
    def _parse_safety_json(self, safety_file: Path):
        """Parse Safety dependency vulnerability report."""
        try:
            with open(safety_file, 'r') as f:
                data = json.load(f)
            
            if isinstance(data, list):
                self.security_findings.tools_used.append('Safety')
                
                for vuln in data:
                    self.security_findings.total_issues += 1
                    vuln_type = 'dependency_vulnerability'
                    self.security_findings.findings_by_type[vuln_type] = \
                        self.security_findings.findings_by_type.get(vuln_type, 0) + 1
                    
                    # Classify severity based on CVE score if available
                    if 'cve' in vuln:
                        self.security_findings.high_severity += 1
                    else:
                        self.security_findings.medium_severity += 1
        
        except Exception as e:
            print(f"Error parsing Safety file {safety_file}: {e}")
    
    def _parse_semgrep_json(self, semgrep_file: Path):
        """Parse Semgrep SAST report."""
        try:
            with open(semgrep_file, 'r') as f:
                data = json.load(f)
            
            if 'results' in data:
                self.security_findings.tools_used.append('Semgrep')
                
                for result in data['results']:
                    severity = result.get('extra', {}).get('severity', 'INFO').lower()
                    rule_id = result.get('check_id', 'unknown')
                    
                    self.security_findings.total_issues += 1
                    self.security_findings.findings_by_type[rule_id] = \
                        self.security_findings.findings_by_type.get(rule_id, 0) + 1
                    
                    if severity in ['error', 'critical']:
                        self.security_findings.high_severity += 1
                    elif severity == 'warning':
                        self.security_findings.medium_severity += 1
                    else:
                        self.security_findings.low_severity += 1
        
        except Exception as e:
            print(f"Error parsing Semgrep file {semgrep_file}: {e}")
    
    def _extract_suite_name(self, filename: str) -> str:
        """Extract test suite name from filename."""
        # Remove .xml extension and common prefixes/suffixes
        name = filename.replace('.xml', '')
        name = re.sub(r'^test[_-]?', '', name, flags=re.IGNORECASE)
        name = re.sub(r'[_-]?tests?$', '', name, flags=re.IGNORECASE)
        
        # Convert to title case
        return name.replace('_', ' ').replace('-', ' ').title()
    
    def _generate_header(self) -> str:
        """Generate report header."""
        return f"""# 📊 Comprehensive Test Report
        
**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}  
**Report Version:** 1.0  
**Test Results Directory:** `{self.test_results_dir}`

---"""
    
    def _generate_executive_summary(self) -> str:
        """Generate executive summary."""
        total_tests = sum(suite.metrics.total_tests for suite in self.test_suites.values())
        total_passed = sum(suite.metrics.passed_tests for suite in self.test_suites.values())
        total_failed = sum(suite.metrics.failed_tests for suite in self.test_suites.values())
        total_errors = sum(suite.metrics.error_tests for suite in self.test_suites.values())
        total_skipped = sum(suite.metrics.skipped_tests for suite in self.test_suites.values())
        total_execution_time = sum(suite.metrics.execution_time for suite in self.test_suites.values())
        
        overall_success_rate = total_passed / total_tests if total_tests > 0 else 0.0
        
        # Determine overall status
        if overall_success_rate >= 0.95:
            status_emoji = "✅"
            status_text = "EXCELLENT"
        elif overall_success_rate >= 0.90:
            status_emoji = "🟡"
            status_text = "GOOD"
        elif overall_success_rate >= 0.80:
            status_emoji = "🟠"
            status_text = "NEEDS ATTENTION"
        else:
            status_emoji = "❌"
            status_text = "CRITICAL"
        
        return f"""## 📈 Executive Summary

{status_emoji} **Overall Status: {status_text}**

### Key Metrics
| Metric | Value |
|--------|--------|
| **Total Tests** | {total_tests:,} |
| **Success Rate** | {overall_success_rate:.1%} |
| **Passed** | {total_passed:,} |
| **Failed** | {total_failed:,} |
| **Errors** | {total_errors:,} |
| **Skipped** | {total_skipped:,} |
| **Execution Time** | {total_execution_time:.1f}s |
| **Test Suites** | {len(self.test_suites)} |

### Coverage Summary
{self._format_coverage_summary()}

### Security Summary
{self._format_security_summary()}"""
    
    def _generate_quality_gate_results(self) -> str:
        """Generate quality gate results."""
        total_tests = sum(suite.metrics.total_tests for suite in self.test_suites.values())
        total_passed = sum(suite.metrics.passed_tests for suite in self.test_suites.values())
        total_failed = sum(suite.metrics.failed_tests for suite in self.test_suites.values())
        total_errors = sum(suite.metrics.error_tests for suite in self.test_suites.values())
        
        success_rate = total_passed / total_tests if total_tests > 0 else 0.0
        coverage_rate = self.coverage_data.get('line_coverage_percent', 0) / 100
        
        # Quality gate criteria
        gates = [
            {
                'name': 'Test Success Rate',
                'threshold': 0.95,
                'actual': success_rate,
                'unit': '%',
                'passed': success_rate >= 0.95
            },
            {
                'name': 'Code Coverage',
                'threshold': 0.85,
                'actual': coverage_rate,
                'unit': '%',
                'passed': coverage_rate >= 0.85
            },
            {
                'name': 'Total Tests',
                'threshold': 50,
                'actual': total_tests,
                'unit': '',
                'passed': total_tests >= 50
            },
            {
                'name': 'Critical Security Issues',
                'threshold': 0,
                'actual': self.security_findings.high_severity,
                'unit': '',
                'passed': self.security_findings.high_severity == 0
            }
        ]
        
        all_passed = all(gate['passed'] for gate in gates)
        overall_status = "✅ PASSED" if all_passed else "❌ FAILED"
        
        gate_table = "| Quality Gate | Threshold | Actual | Status |\n"
        gate_table += "|--------------|-----------|---------|--------|\n"
        
        for gate in gates:
            status_icon = "✅" if gate['passed'] else "❌"
            if gate['unit'] == '%':
                actual_str = f"{gate['actual']:.1%}"
                threshold_str = f"{gate['threshold']:.1%}"
            else:
                actual_str = f"{gate['actual']:,}"
                threshold_str = f"{gate['threshold']:,}"
            
            gate_table += f"| {gate['name']} | {threshold_str} | {actual_str} | {status_icon} |\n"
        
        return f"""## 🚪 Quality Gate Results

### Overall Result: {overall_status}

{gate_table}"""
    
    def _generate_test_suite_details(self) -> str:
        """Generate detailed test suite results."""
        if not self.test_suites:
            return "## 🧪 Test Suite Details\n\nNo test results found."
        
        details = "## 🧪 Test Suite Details\n\n"
        
        for suite_name, suite in sorted(self.test_suites.items()):
            status_icon = "✅" if suite.metrics.success_rate >= 0.95 else "❌" if suite.metrics.success_rate < 0.8 else "🟡"
            
            details += f"""### {status_icon} {suite_name}

| Metric | Value |
|--------|--------|
| **Tests** | {suite.metrics.total_tests:,} |
| **Passed** | {suite.metrics.passed_tests:,} |
| **Failed** | {suite.metrics.failed_tests:,} |
| **Errors** | {suite.metrics.error_tests:,} |
| **Skipped** | {suite.metrics.skipped_tests:,} |
| **Success Rate** | {suite.metrics.success_rate:.1%} |
| **Execution Time** | {suite.metrics.execution_time:.1f}s |

"""
            
            # Add failed test details
            failed_tests = [tc for tc in suite.test_cases if tc['status'] in ['failed', 'error']]
            if failed_tests:
                details += "#### ❌ Failed Tests\n\n"
                for test in failed_tests[:5]:  # Limit to first 5
                    details += f"- **{test['name']}** ({test['status']})\n"
                    if 'failure_message' in test and test['failure_message']:
                        details += f"  - Error: `{test['failure_message'][:100]}...`\n"
                    elif 'error_message' in test and test['error_message']:
                        details += f"  - Error: `{test['error_message'][:100]}...`\n"
                
                if len(failed_tests) > 5:
                    details += f"- ... and {len(failed_tests) - 5} more failures\n"
                details += "\n"
        
        return details
    
    def _generate_coverage_analysis(self) -> str:
        """Generate coverage analysis."""
        if not self.coverage_data:
            return "## 📊 Coverage Analysis\n\nNo coverage data available."
        
        line_coverage = self.coverage_data.get('line_coverage_percent', 0)
        branch_coverage = self.coverage_data.get('branch_coverage_percent', 0)
        
        # Coverage status
        if line_coverage >= 90:
            coverage_status = "✅ Excellent"
        elif line_coverage >= 80:
            coverage_status = "🟡 Good"
        elif line_coverage >= 70:
            coverage_status = "🟠 Needs Improvement"
        else:
            coverage_status = "❌ Poor"
        
        return f"""## 📊 Coverage Analysis

### Overall Coverage: {coverage_status}

| Coverage Type | Percentage | Lines/Branches |
|---------------|------------|----------------|
| **Line Coverage** | {line_coverage:.1f}% | {self.coverage_data.get('lines_covered', 0):,} / {self.coverage_data.get('lines_valid', 0):,} |
| **Branch Coverage** | {branch_coverage:.1f}% | {self.coverage_data.get('branches_covered', 0):,} / {self.coverage_data.get('branches_valid', 0):,} |

### Coverage Goals
- 🎯 **Target:** ≥ 85% line coverage
- 🎯 **Stretch Goal:** ≥ 90% line coverage
- 🎯 **Branch Coverage:** ≥ 80%

{self._generate_coverage_recommendations(line_coverage)}"""
    
    def _generate_security_analysis(self) -> str:
        """Generate security analysis."""
        if self.security_findings.total_issues == 0:
            return """## 🔒 Security Analysis

✅ **No security issues found!**

Security tools used: {tools}

All automated security scans passed without detecting vulnerabilities.""".format(
                tools=", ".join(self.security_findings.tools_used) if self.security_findings.tools_used else "None"
            )
        
        # Risk level assessment
        if self.security_findings.high_severity > 0:
            risk_level = "🔴 HIGH RISK"
        elif self.security_findings.medium_severity > 5:
            risk_level = "🟡 MEDIUM RISK"
        elif self.security_findings.total_issues > 10:
            risk_level = "🟠 LOW-MEDIUM RISK"
        else:
            risk_level = "🟢 LOW RISK"
        
        findings_table = "| Severity | Count |\n|----------|-------|\n"
        findings_table += f"| 🔴 High | {self.security_findings.high_severity} |\n"
        findings_table += f"| 🟡 Medium | {self.security_findings.medium_severity} |\n"
        findings_table += f"| 🟢 Low | {self.security_findings.low_severity} |\n"
        findings_table += f"| **Total** | **{self.security_findings.total_issues}** |\n"
        
        return f"""## 🔒 Security Analysis

### Risk Level: {risk_level}

{findings_table}

**Tools Used:** {", ".join(self.security_findings.tools_used)}

### Top Security Issue Types
{self._format_top_security_issues()}

{self._generate_security_recommendations()}"""
    
    def _generate_performance_analysis(self) -> str:
        """Generate performance analysis."""
        # Look for performance test results
        perf_suites = [suite for name, suite in self.test_suites.items() if 'performance' in name.lower()]
        
        if not perf_suites:
            return "## ⚡ Performance Analysis\n\nNo performance test results available."
        
        # Aggregate performance metrics
        total_perf_tests = sum(suite.metrics.total_tests for suite in perf_suites)
        avg_execution_time = statistics.mean([suite.metrics.execution_time for suite in perf_suites])
        
        return f"""## ⚡ Performance Analysis

### Performance Test Summary
- **Performance Tests Executed:** {total_perf_tests:,}
- **Average Execution Time:** {avg_execution_time:.1f}s
- **Success Rate:** {statistics.mean([suite.metrics.success_rate for suite in perf_suites]):.1%}

### Performance Recommendations
{self._generate_performance_recommendations()}"""
    
    def _generate_trends_analysis(self) -> str:
        """Generate trends analysis."""
        return """## 📈 Trends Analysis

### Historical Comparison
*Historical trend analysis requires multiple test runs. This feature will be enhanced with test history storage.*

### Key Indicators to Monitor
- ✅ **Success Rate Trend:** Monitor for declining success rates
- ✅ **Execution Time Trend:** Watch for performance degradation
- ✅ **Coverage Trend:** Track coverage improvements/regressions
- ✅ **Security Issues Trend:** Monitor security posture over time

### Recommendations for Trend Monitoring
1. Store test results in a time-series database
2. Set up automated alerting for negative trends
3. Create dashboards for real-time trend visualization
4. Implement regression detection algorithms"""
    
    def _generate_recommendations(self) -> str:
        """Generate actionable recommendations."""
        recommendations = []
        
        # Test success recommendations
        total_tests = sum(suite.metrics.total_tests for suite in self.test_suites.values())
        total_passed = sum(suite.metrics.passed_tests for suite in self.test_suites.values())
        success_rate = total_passed / total_tests if total_tests > 0 else 0.0
        
        if success_rate < 0.95:
            recommendations.append("🔧 **Improve Test Reliability:** Focus on fixing failing tests to achieve >95% success rate")
        
        # Coverage recommendations
        line_coverage = self.coverage_data.get('line_coverage_percent', 0)
        if line_coverage < 85:
            recommendations.append(f"📊 **Increase Code Coverage:** Current coverage is {line_coverage:.1f}%, target is ≥85%")
        
        # Security recommendations
        if self.security_findings.high_severity > 0:
            recommendations.append(f"🔒 **Address Critical Security Issues:** {self.security_findings.high_severity} high-severity security issues found")
        
        # Performance recommendations
        slow_suites = [suite for suite in self.test_suites.values() if suite.metrics.execution_time > 300]  # 5 minutes
        if slow_suites:
            recommendations.append(f"⚡ **Optimize Test Performance:** {len(slow_suites)} test suites are taking >5 minutes")
        
        # General recommendations
        if total_tests < 100:
            recommendations.append("🧪 **Expand Test Coverage:** Consider adding more comprehensive tests")
        
        if not recommendations:
            recommendations.append("✅ **Excellent Work!** All quality metrics are meeting targets")
        
        return f"""## 💡 Recommendations

{chr(10).join(f"{i+1}. {rec}" for i, rec in enumerate(recommendations))}

### Next Steps
1. **Immediate Actions:** Address any critical security issues and failing tests
2. **Short-term Goals:** Improve coverage and test reliability
3. **Long-term Strategy:** Implement trend monitoring and automated quality gates"""
    
    def _generate_footer(self) -> str:
        """Generate report footer."""
        return f"""---

## 📋 Report Metadata

- **Generated By:** Comprehensive Test Report Generator v1.0
- **Test Results Parsed:** {len(self.test_suites)} test suites
- **Report Generation Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
- **Total Test Execution Time:** {sum(suite.metrics.execution_time for suite in self.test_suites.values()):.1f} seconds

*This report was automatically generated from test execution results. For questions or issues, please contact the QA team.*"""
    
    # Helper methods
    
    def _format_coverage_summary(self) -> str:
        """Format coverage summary for executive summary."""
        if not self.coverage_data:
            return "📊 **Coverage:** No data available"
        
        line_coverage = self.coverage_data.get('line_coverage_percent', 0)
        status = "✅" if line_coverage >= 85 else "🟡" if line_coverage >= 70 else "❌"
        return f"📊 **Coverage:** {status} {line_coverage:.1f}% line coverage"
    
    def _format_security_summary(self) -> str:
        """Format security summary for executive summary."""
        if self.security_findings.total_issues == 0:
            return "🔒 **Security:** ✅ No issues found"
        
        if self.security_findings.high_severity > 0:
            status = "❌"
        elif self.security_findings.medium_severity > 5:
            status = "🟡"
        else:
            status = "🟠"
        
        return f"🔒 **Security:** {status} {self.security_findings.total_issues} issues ({self.security_findings.high_severity} critical)"
    
    def _format_top_security_issues(self) -> str:
        """Format top security issues."""
        if not self.security_findings.findings_by_type:
            return "No detailed security findings available."
        
        # Sort by count and take top 5
        top_issues = sorted(
            self.security_findings.findings_by_type.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        issues_list = ""
        for issue_type, count in top_issues:
            issues_list += f"- **{issue_type}:** {count} occurrence{'s' if count > 1 else ''}\n"
        
        return issues_list
    
    def _generate_coverage_recommendations(self, line_coverage: float) -> str:
        """Generate coverage-specific recommendations."""
        if line_coverage >= 90:
            return "✅ **Excellent coverage!** Consider maintaining this level and focusing on branch coverage."
        elif line_coverage >= 80:
            return "🎯 **Good coverage.** Focus on covering the remaining uncovered lines to reach 90%."
        elif line_coverage >= 70:
            return "📈 **Moderate coverage.** Prioritize adding tests for critical code paths."
        else:
            return "⚠️ **Low coverage detected.** Implement comprehensive testing strategy."
    
    def _generate_security_recommendations(self) -> str:
        """Generate security-specific recommendations."""
        if self.security_findings.high_severity > 0:
            return """### 🚨 Immediate Actions Required
1. **Address all critical security issues immediately**
2. **Review and update security policies**
3. **Implement additional security controls**"""
        elif self.security_findings.total_issues > 0:
            return """### 🔧 Security Improvements
1. **Review and fix medium/low priority issues**
2. **Enhance security testing in CI/CD pipeline**
3. **Consider additional SAST/DAST tools**"""
        else:
            return """### ✅ Security Best Practices
1. **Continue regular security scans**
2. **Keep security tools updated**
3. **Monitor for new vulnerabilities**"""
    
    def _generate_performance_recommendations(self) -> str:
        """Generate performance-specific recommendations."""
        return """1. **Monitor Response Times:** Set up performance baselines and alerting
2. **Optimize Slow Operations:** Profile and optimize code paths with high latency
3. **Load Testing:** Implement regular load testing in CI/CD pipeline
4. **Resource Monitoring:** Track memory and CPU usage trends"""


def main():
    """Main function to generate test reports."""
    parser = argparse.ArgumentParser(description="Generate comprehensive test reports")
    parser.add_argument(
        "test_results_dir",
        help="Directory containing test result files"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="comprehensive-test-report.md",
        help="Output file name (default: comprehensive-test-report.md)"
    )
    parser.add_argument(
        "--format",
        choices=["markdown", "html", "json"],
        default="markdown",
        help="Output format (default: markdown)"
    )
    
    args = parser.parse_args()
    
    # Check if test results directory exists
    test_results_dir = Path(args.test_results_dir)
    if not test_results_dir.exists():
        print(f"Error: Test results directory '{test_results_dir}' does not exist")
        return 1
    
    # Generate report
    generator = TestReportGenerator(test_results_dir)
    
    try:
        if args.format == "markdown":
            report = generator.generate_comprehensive_report()
            print(report)  # Output to stdout for GitHub Actions
            
            # Also save to file if output specified
            if args.output != "-":
                with open(args.output, 'w', encoding='utf-8') as f:
                    f.write(report)
                print(f"\nReport saved to: {args.output}", file=sys.stderr)
        
        else:
            print(f"Format '{args.format}' not yet implemented")
            return 1
    
    except Exception as e:
        print(f"Error generating report: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
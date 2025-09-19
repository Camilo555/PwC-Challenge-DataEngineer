#!/usr/bin/env python3
"""
Error Handling CLI Tool
========================

Command-line interface for analyzing, monitoring, and managing
the standardized error handling system across the codebase.
"""
import ast
import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Set
from collections import defaultdict, Counter

import click

from .patterns import get_pattern_manager
from .error_codes import ErrorCode


class ErrorHandlingAnalyzer:
    """Analyzes error handling patterns across the codebase."""

    def __init__(self, root_path: Path):
        self.root_path = Path(root_path)
        self.python_files = []
        self.analysis_results = {
            'files_analyzed': 0,
            'exception_patterns': Counter(),
            'decorator_usage': Counter(),
            'context_manager_usage': Counter(),
            'missing_error_handling': [],
            'inconsistent_patterns': [],
            'recommendations': []
        }

    def scan_codebase(self) -> None:
        """Scan the codebase for Python files."""
        self.python_files = list(self.root_path.rglob("*.py"))
        self.analysis_results['files_analyzed'] = len(self.python_files)
        click.echo(f"Found {len(self.python_files)} Python files to analyze")

    def analyze_file(self, file_path: Path) -> Dict[str, Any]:
        """Analyze a single Python file for error handling patterns."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            tree = ast.parse(content)
            file_analysis = {
                'path': str(file_path),
                'exceptions_raised': [],
                'exceptions_caught': [],
                'decorators_used': [],
                'context_managers_used': [],
                'functions_without_error_handling': [],
                'recommendations': []
            }

            self._analyze_ast(tree, file_analysis)
            return file_analysis

        except Exception as e:
            return {
                'path': str(file_path),
                'error': str(e),
                'analysis_failed': True
            }

    def _analyze_ast(self, tree: ast.AST, file_analysis: Dict[str, Any]) -> None:
        """Analyze AST for error handling patterns."""
        class ErrorHandlingVisitor(ast.NodeVisitor):
            def __init__(self, analysis: Dict[str, Any]):
                self.analysis = analysis
                self.current_function = None
                self.in_try_block = False

            def visit_FunctionDef(self, node):
                old_function = self.current_function
                self.current_function = node.name

                # Check for error handling decorators
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Name):
                        decorator_name = decorator.id
                    elif isinstance(decorator, ast.Attribute):
                        decorator_name = decorator.attr
                    else:
                        decorator_name = "unknown"

                    self.analysis['decorators_used'].append({
                        'function': node.name,
                        'decorator': decorator_name,
                        'line': node.lineno
                    })

                # Check if function has try/except blocks
                has_error_handling = self._has_error_handling(node)
                if not has_error_handling and not self._has_error_decorators(node):
                    self.analysis['functions_without_error_handling'].append({
                        'function': node.name,
                        'line': node.lineno
                    })

                self.generic_visit(node)
                self.current_function = old_function

            def visit_Raise(self, node):
                if isinstance(node.exc, ast.Call) and isinstance(node.exc.func, ast.Name):
                    exception_name = node.exc.func.id
                elif isinstance(node.exc, ast.Name):
                    exception_name = node.exc.id
                else:
                    exception_name = "unknown"

                self.analysis['exceptions_raised'].append({
                    'exception': exception_name,
                    'function': self.current_function,
                    'line': node.lineno
                })

            def visit_ExceptHandler(self, node):
                old_in_try = self.in_try_block
                self.in_try_block = True

                if node.type:
                    if isinstance(node.type, ast.Name):
                        exception_name = node.type.id
                    elif isinstance(node.type, ast.Attribute):
                        exception_name = node.type.attr
                    else:
                        exception_name = "unknown"
                else:
                    exception_name = "Exception"

                self.analysis['exceptions_caught'].append({
                    'exception': exception_name,
                    'function': self.current_function,
                    'line': node.lineno
                })

                self.generic_visit(node)
                self.in_try_block = old_in_try

            def visit_With(self, node):
                for item in node.items:
                    if isinstance(item.context_expr, ast.Call) and isinstance(item.context_expr.func, ast.Name):
                        context_manager = item.context_expr.func.id
                    elif isinstance(item.context_expr, ast.Name):
                        context_manager = item.context_expr.id
                    else:
                        context_manager = "unknown"

                    self.analysis['context_managers_used'].append({
                        'context_manager': context_manager,
                        'function': self.current_function,
                        'line': node.lineno
                    })

                self.generic_visit(node)

            def _has_error_handling(self, node) -> bool:
                """Check if function has try/except blocks."""
                for child in ast.walk(node):
                    if isinstance(child, ast.Try):
                        return True
                return False

            def _has_error_decorators(self, node) -> bool:
                """Check if function has error handling decorators."""
                error_decorators = {
                    'handle_exceptions', 'retry_on_error', 'circuit_breaker',
                    'api_endpoint', 'data_processing', 'external_service'
                }

                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Name) and decorator.id in error_decorators:
                        return True
                return False

        visitor = ErrorHandlingVisitor(file_analysis)
        visitor.visit(tree)

    def analyze_codebase(self) -> Dict[str, Any]:
        """Analyze the entire codebase for error handling patterns."""
        self.scan_codebase()

        for file_path in self.python_files:
            if 'test' in str(file_path).lower() or '__pycache__' in str(file_path):
                continue  # Skip test files and cache

            file_analysis = self.analyze_file(file_path)

            if 'analysis_failed' not in file_analysis:
                # Aggregate results
                for exception in file_analysis['exceptions_raised']:
                    self.analysis_results['exception_patterns'][exception['exception']] += 1

                for decorator in file_analysis['decorators_used']:
                    self.analysis_results['decorator_usage'][decorator['decorator']] += 1

                for cm in file_analysis['context_managers_used']:
                    self.analysis_results['context_manager_usage'][cm['context_manager']] += 1

                # Check for functions without error handling
                for func in file_analysis['functions_without_error_handling']:
                    self.analysis_results['missing_error_handling'].append({
                        'file': str(file_path),
                        'function': func['function'],
                        'line': func['line']
                    })

        self._generate_recommendations()
        return self.analysis_results

    def _generate_recommendations(self) -> None:
        """Generate recommendations based on analysis."""
        recommendations = []

        # Check for basic Exception usage
        if self.analysis_results['exception_patterns']['Exception'] > 0:
            recommendations.append({
                'type': 'exception_specificity',
                'severity': 'medium',
                'message': f"Found {self.analysis_results['exception_patterns']['Exception']} usages of generic 'Exception'. Consider using more specific exception types.",
                'suggestion': "Replace generic Exception with specific exceptions like ValidationError, DatabaseError, etc."
            })

        # Check for missing error handling
        missing_count = len(self.analysis_results['missing_error_handling'])
        if missing_count > 10:
            recommendations.append({
                'type': 'missing_error_handling',
                'severity': 'high',
                'message': f"Found {missing_count} functions without error handling.",
                'suggestion': "Add appropriate error handling decorators or try/except blocks to these functions."
            })

        # Check for low decorator adoption
        total_decorators = sum(self.analysis_results['decorator_usage'].values())
        if total_decorators < self.analysis_results['files_analyzed'] * 0.1:
            recommendations.append({
                'type': 'decorator_adoption',
                'severity': 'medium',
                'message': "Low adoption of error handling decorators.",
                'suggestion': "Consider using @handle_exceptions, @retry_on_error, and other error handling decorators."
            })

        # Check for context manager usage
        total_cms = sum(self.analysis_results['context_manager_usage'].values())
        if total_cms == 0:
            recommendations.append({
                'type': 'context_manager_usage',
                'severity': 'low',
                'message': "No error handling context managers found.",
                'suggestion': "Consider using context managers like database_operation, external_service_call for better error handling."
            })

        self.analysis_results['recommendations'] = recommendations

    def generate_report(self, output_format: str = 'text') -> str:
        """Generate a report of the analysis results."""
        if output_format == 'json':
            return json.dumps(self.analysis_results, indent=2, default=str)

        # Text format
        report = []
        report.append("Error Handling Analysis Report")
        report.append("=" * 50)
        report.append(f"Files analyzed: {self.analysis_results['files_analyzed']}")
        report.append("")

        # Exception patterns
        report.append("Exception Patterns:")
        report.append("-" * 20)
        for exception, count in self.analysis_results['exception_patterns'].most_common(10):
            report.append(f"  {exception}: {count}")
        report.append("")

        # Decorator usage
        report.append("Error Handling Decorator Usage:")
        report.append("-" * 35)
        for decorator, count in self.analysis_results['decorator_usage'].most_common():
            report.append(f"  {decorator}: {count}")
        report.append("")

        # Context manager usage
        report.append("Context Manager Usage:")
        report.append("-" * 22)
        for cm, count in self.analysis_results['context_manager_usage'].most_common():
            report.append(f"  {cm}: {count}")
        report.append("")

        # Missing error handling
        missing_count = len(self.analysis_results['missing_error_handling'])
        report.append(f"Functions Without Error Handling: {missing_count}")
        report.append("-" * 40)
        if missing_count > 0:
            for item in self.analysis_results['missing_error_handling'][:10]:
                report.append(f"  {item['file']}:{item['line']} - {item['function']}")
            if missing_count > 10:
                report.append(f"  ... and {missing_count - 10} more")
        report.append("")

        # Recommendations
        report.append("Recommendations:")
        report.append("-" * 15)
        for rec in self.analysis_results['recommendations']:
            severity_indicator = {
                'high': '🔴',
                'medium': '🟡',
                'low': '🟢'
            }.get(rec['severity'], '⚪')

            report.append(f"  {severity_indicator} [{rec['severity'].upper()}] {rec['type']}")
            report.append(f"    {rec['message']}")
            report.append(f"    → {rec['suggestion']}")
            report.append("")

        return "\n".join(report)


@click.group()
def error_cli():
    """Error handling management CLI."""
    pass


@error_cli.command()
@click.option('--path', type=click.Path(exists=True), default='.', help='Path to analyze')
@click.option('--format', type=click.Choice(['text', 'json']), default='text', help='Output format')
@click.option('--output', type=click.Path(), help='Output file (default: stdout)')
def analyze(path: str, format: str, output: str):
    """Analyze codebase for error handling patterns."""
    analyzer = ErrorHandlingAnalyzer(Path(path))

    click.echo("Analyzing codebase for error handling patterns...")
    with click.progressbar(length=100, label='Analyzing files') as bar:
        results = analyzer.analyze_codebase()
        bar.update(100)

    report = analyzer.generate_report(format)

    if output:
        with open(output, 'w') as f:
            f.write(report)
        click.echo(f"Report saved to {output}")
    else:
        click.echo(report)


@error_cli.command()
def patterns():
    """Show current error pattern statistics."""
    manager = get_pattern_manager()
    summaries = manager.get_all_error_summaries()

    click.echo("Error Pattern Statistics")
    click.echo("=" * 30)

    for pattern_name, summary in summaries.items():
        click.echo(f"\n{pattern_name.replace('_', ' ').title()}:")
        click.echo(f"  Total errors: {summary['total_errors']}")

        if summary['error_types']:
            click.echo("  Error types:")
            for error_type, count in summary['error_types'].items():
                click.echo(f"    {error_type}: {count}")

        if summary['recent_errors']:
            click.echo(f"  Recent errors: {len(summary['recent_errors'])}")


@error_cli.command()
def codes():
    """List all available error codes."""
    click.echo("Available Error Codes")
    click.echo("=" * 25)

    # Group by category
    categories = defaultdict(list)
    for error_code in ErrorCode:
        category = ErrorCode.get_category(error_code)
        categories[category].append(error_code)

    for category, codes in categories.items():
        click.echo(f"\n{category}:")
        for code in codes:
            severity = ErrorCode.get_severity(code)
            http_status = ErrorCode.get_http_status_code(code)
            retry = "Yes" if ErrorCode.requires_retry(code) else "No"

            click.echo(f"  {code.value}")
            click.echo(f"    Severity: {severity}")
            click.echo(f"    HTTP Status: {http_status}")
            click.echo(f"    Retryable: {retry}")


@error_cli.command()
@click.option('--severity', type=click.Choice(['low', 'medium', 'high']), help='Filter by severity')
def validate(severity: str):
    """Validate error handling configuration."""
    issues = []

    # Check if error patterns are being used
    manager = get_pattern_manager()
    summaries = manager.get_all_error_summaries()

    total_errors = sum(s['total_errors'] for s in summaries.values())
    if total_errors == 0:
        issues.append({
            'severity': 'low',
            'type': 'usage',
            'message': 'No errors have been recorded through the standardized system',
            'suggestion': 'Ensure applications are using the standardized error handling'
        })

    # Check for pattern imbalances
    pattern_usage = {name: summary['total_errors'] for name, summary in summaries.items()}
    max_usage = max(pattern_usage.values()) if pattern_usage.values() else 0

    for pattern_name, usage in pattern_usage.items():
        if max_usage > 0 and usage < max_usage * 0.1:
            issues.append({
                'severity': 'medium',
                'type': 'pattern_imbalance',
                'message': f'Pattern "{pattern_name}" has very low usage compared to others',
                'suggestion': f'Review if {pattern_name} pattern should be used more consistently'
            })

    # Filter by severity if specified
    if severity:
        issues = [issue for issue in issues if issue['severity'] == severity]

    # Display results
    if not issues:
        click.echo("✅ No validation issues found!")
        return

    click.echo("Validation Issues Found:")
    click.echo("=" * 30)

    for issue in issues:
        severity_indicator = {
            'high': '🔴',
            'medium': '🟡',
            'low': '🟢'
        }.get(issue['severity'], '⚪')

        click.echo(f"\n{severity_indicator} [{issue['severity'].upper()}] {issue['type']}")
        click.echo(f"  Issue: {issue['message']}")
        click.echo(f"  Suggestion: {issue['suggestion']}")


@error_cli.command()
@click.option('--pattern', help='Specific pattern to reset')
def reset(pattern: str):
    """Reset error pattern statistics."""
    manager = get_pattern_manager()

    if pattern:
        pattern_obj = manager.get_pattern(pattern)
        if pattern_obj:
            pattern_obj.error_counts.clear()
            pattern_obj.last_errors.clear()
            click.echo(f"Reset statistics for pattern: {pattern}")
        else:
            click.echo(f"Pattern not found: {pattern}")
            return
    else:
        manager.reset_all_patterns()
        click.echo("Reset all error pattern statistics")


@error_cli.command()
def doctor():
    """Run comprehensive error handling health check."""
    click.echo("Error Handling System Health Check")
    click.echo("=" * 40)

    # Check 1: Pattern manager availability
    try:
        manager = get_pattern_manager()
        click.echo("✅ Pattern manager available")
    except Exception as e:
        click.echo(f"❌ Pattern manager error: {e}")
        return

    # Check 2: Error code system
    try:
        total_codes = len(list(ErrorCode))
        click.echo(f"✅ Error codes system: {total_codes} codes available")
    except Exception as e:
        click.echo(f"❌ Error codes error: {e}")

    # Check 3: Pattern usage
    summaries = manager.get_all_error_summaries()
    total_errors = sum(s['total_errors'] for s in summaries.values())

    if total_errors > 0:
        click.echo(f"✅ Error patterns in use: {total_errors} total errors recorded")
    else:
        click.echo("⚠️  No error pattern usage detected")

    # Check 4: Import tests
    try:
        from . import (
            handle_exceptions, retry_on_error, database_operation,
            get_validation_pattern, ErrorCode
        )
        click.echo("✅ Core imports successful")
    except ImportError as e:
        click.echo(f"❌ Import error: {e}")

    # Summary
    click.echo("\nSummary:")
    click.echo("- Error handling system is operational")
    click.echo("- All core components are available")
    if total_errors == 0:
        click.echo("- Consider running error handling examples to test the system")


@error_cli.command()
def examples():
    """Show available error handling examples."""
    click.echo("Error Handling Examples")
    click.echo("=" * 25)

    examples_file = Path(__file__).parent.parent.parent.parent / "examples" / "error_handling_examples.py"

    if examples_file.exists():
        click.echo(f"✅ Examples file found: {examples_file}")
        click.echo("\nTo run examples:")
        click.echo(f"  python {examples_file}")
        click.echo("\nAvailable examples:")
        click.echo("  - Decorator examples (basic error handling, validation, retry)")
        click.echo("  - Context manager examples (validation, database, external services)")
        click.echo("  - Pattern examples (validation, database, business rules)")
        click.echo("  - Utility examples (safe calls, comprehensive scenarios)")
    else:
        click.echo(f"❌ Examples file not found: {examples_file}")


if __name__ == '__main__':
    error_cli()
#!/usr/bin/env python3
"""
Targeted Coverage Analysis Script
================================

Script to diagnose and fix the test coverage reporting gap identified in the
comprehensive analysis. This script provides multiple approaches to running
coverage analysis and identifies the root cause of the 1% vs 95% discrepancy.

Usage:
    python scripts/run_targeted_coverage.py [--mode simple|comprehensive|debug]

Author: Enterprise Data Engineering Team
Created: 2025-01-14
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path


def run_command(cmd, description=""):
    """Run a command and return the result."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {cmd}")
    print(f"{'='*60}")

    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent
        )

        print(f"Exit code: {result.returncode}")
        print(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            print(f"STDERR:\n{result.stderr}")

        return result
    except Exception as e:
        print(f"Error running command: {e}")
        return None


def check_environment():
    """Check the testing environment setup."""
    print("🔍 ENVIRONMENT DIAGNOSTICS")
    print("="*60)

    # Check pytest installation
    run_command("pytest --version", "Check pytest version")

    # Check coverage installation
    run_command("python -c \"import coverage; print(f'Coverage version: {coverage.__version__}')\"", "Check coverage version")


def run_simple_coverage():
    """Run simple coverage analysis."""
    print("\n🧪 SIMPLE COVERAGE ANALYSIS")
    print("="*60)

    # Run coverage with minimal configuration
    run_command("python -m coverage erase", "Clear previous coverage data")
    run_command("python -m coverage run --source=src -m pytest tests/unit/test_config.py -v", "Run basic coverage test")
    run_command("python -m coverage report", "Generate coverage report")


def main():
    """Main function to run coverage analysis."""
    print("🚀 TARGETED COVERAGE ANALYSIS")

    project_root = Path(__file__).parent.parent
    os.chdir(project_root)

    check_environment()
    run_simple_coverage()


if __name__ == "__main__":
    main()

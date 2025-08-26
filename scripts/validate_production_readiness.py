#!/usr/bin/env python3
"""
Production Readiness Validation Script

This script validates that the project is ready for production deployment
by checking security configurations, dependencies, and critical settings.
"""

import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    from core.config import Environment, settings
except ImportError as e:
    print(f"❌ Failed to import settings: {e}")
    print("Make sure all dependencies are installed with: poetry install")
    sys.exit(1)


def validate_security_config() -> list[str]:
    """Validate security configuration."""
    issues = []

    # Check for production environment settings
    if settings.environment == Environment.PRODUCTION:
        if not settings.secret_key:
            issues.append("SECRET_KEY must be set in production")
        elif len(settings.secret_key) < 32:
            issues.append("SECRET_KEY must be at least 32 characters long")

        if not settings.basic_auth_password:
            issues.append("BASIC_AUTH_PASSWORD must be set in production")
        elif len(settings.basic_auth_password) < 12:
            issues.append("BASIC_AUTH_PASSWORD must be at least 12 characters long")

        if not settings.database_url:
            issues.append("DATABASE_URL must be set in production")

    return issues


def validate_external_apis() -> list[str]:
    """Validate external API configuration."""
    issues = []

    if settings.enable_external_enrichment:
        if not settings.currency_api_key:
            issues.append("CURRENCY_API_KEY should be set when external enrichment is enabled")

    return issues


def validate_paths() -> list[str]:
    """Validate required paths exist or can be created."""
    issues = []

    paths_to_check = [
        ("Raw data path", settings.raw_data_path),
        ("Bronze path", settings.bronze_path),
        ("Silver path", settings.silver_path),
        ("Gold path", settings.gold_path),
    ]

    for name, path in paths_to_check:
        try:
            Path(path).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            issues.append(f"Cannot create {name} at {path}: {e}")

    return issues


def validate_database_config() -> list[str]:
    """Validate database configuration."""
    issues = []

    if not settings.database_url:
        issues.append("DATABASE_URL is not configured")
    elif settings.database_url.startswith("sqlite"):
        if settings.environment == Environment.PRODUCTION:
            issues.append("SQLite is not recommended for production use")

    return issues


def validate_dependencies() -> list[str]:
    """Validate critical dependencies are available."""
    issues = []

    required_modules = [
        ("fastapi", "FastAPI web framework"),
        ("pydantic", "Data validation"),
        ("sqlalchemy", "Database ORM"),
        ("pandas", "Data processing"),
        ("aiohttp", "Async HTTP client"),
    ]

    for module_name, description in required_modules:
        try:
            __import__(module_name)
        except ImportError:
            issues.append(f"Missing required module: {module_name} ({description})")

    # Check optional but recommended modules
    optional_modules = [
        ("pyspark", "Spark processing - required for ETL pipeline"),
        ("delta", "Delta Lake - required for medallion architecture"),
        ("typesense", "Vector search - required for search features"),
    ]

    for module_name, description in optional_modules:
        try:
            __import__(module_name)
        except ImportError:
            issues.append(f"Warning: Missing optional module: {module_name} ({description})")

    return issues


def validate_environment_variables() -> list[str]:
    """Validate environment variables are properly set."""
    issues = []

    # Critical environment variables for production
    if settings.environment == Environment.PRODUCTION:
        critical_vars = [
            "DATABASE_URL",
            "SECRET_KEY",
            "BASIC_AUTH_PASSWORD"
        ]

        for var in critical_vars:
            if not os.getenv(var):
                issues.append(f"Missing critical environment variable: {var}")

    return issues


def run_validation() -> tuple[bool, dict[str, list[str]]]:
    """Run all validation checks."""
    print("🔍 Running production readiness validation...")
    print(f"Environment: {settings.environment}")
    print("-" * 50)

    validation_results = {
        "Security Configuration": validate_security_config(),
        "External APIs": validate_external_apis(),
        "File Paths": validate_paths(),
        "Database Configuration": validate_database_config(),
        "Dependencies": validate_dependencies(),
        "Environment Variables": validate_environment_variables(),
    }

    all_passed = True
    total_issues = 0

    for category, issues in validation_results.items():
        if issues:
            total_issues += len(issues)
            if any("Warning:" not in issue for issue in issues):
                all_passed = False

            print(f"\n❌ {category}:")
            for issue in issues:
                if "Warning:" in issue:
                    print(f"  ⚠️  {issue}")
                else:
                    print(f"  ❌ {issue}")
        else:
            print(f"✅ {category}: All checks passed")

    return all_passed, validation_results


def main():
    """Main validation function."""
    try:
        all_passed, results = run_validation()

        print("\n" + "="*50)

        if all_passed:
            print("🎉 Production Readiness: PASSED")
            print("The project is ready for production deployment!")
        else:
            print("❌ Production Readiness: FAILED")
            print("Please address the issues above before deploying to production.")

        # Count total issues
        total_issues = sum(len(issues) for issues in results.values())
        warnings = sum(1 for issues in results.values() for issue in issues if "Warning:" in issue)
        errors = total_issues - warnings

        print(f"\nSummary: {errors} errors, {warnings} warnings")

        if errors > 0:
            sys.exit(1)
        else:
            sys.exit(0)

    except Exception as e:
        print(f"❌ Validation failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

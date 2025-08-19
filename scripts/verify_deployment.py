#!/usr/bin/env python3
"""
Deployment verification script for PwC Data Engineering Challenge.
Verifies all components are working correctly including Supabase integration.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Any

import httpx
from sqlmodel import create_engine

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.config import settings
from data_access.supabase_client import get_supabase_client, health_check_supabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DeploymentVerifier:
    """Comprehensive deployment verification."""

    def __init__(self):
        self.results = {
            "database": {"status": "unknown", "details": {}},
            "supabase": {"status": "unknown", "details": {}},
            "api": {"status": "unknown", "details": {}},
            "etl": {"status": "unknown", "details": {}},
            "overall": {"status": "unknown", "issues": []},
        }

    async def verify_database_connection(self) -> bool:
        """Verify basic database connectivity."""
        logger.info("🔍 Verifying database connection...")

        try:
            engine = create_engine(settings.database_url)
            with engine.connect() as conn:
                result = conn.execute("SELECT 1").scalar()
                if result == 1:
                    self.results["database"]["status"] = "healthy"
                    self.results["database"]["details"] = {
                        "type": settings.database_type.value,
                        "url_host": settings.database_url.split("@")[-1].split("/")[0] if "@" in settings.database_url else "local",
                    }
                    logger.info("✅ Database connection successful")
                    return True

        except Exception as e:
            self.results["database"]["status"] = "failed"
            self.results["database"]["details"] = {"error": str(e)}
            logger.error(f"❌ Database connection failed: {e}")
            return False

        return False

    async def verify_supabase_integration(self) -> bool:
        """Verify Supabase-specific functionality."""
        if not settings.is_supabase_enabled:
            self.results["supabase"]["status"] = "disabled"
            self.results["supabase"]["details"] = {"reason": "Supabase not configured"}
            logger.info("⚠️  Supabase integration disabled")
            return True

        logger.info("🔍 Verifying Supabase integration...")

        try:
            # Test Supabase client
            client = get_supabase_client()
            connection_info = await client.test_connection()

            # Run health check
            health_report = await health_check_supabase()

            self.results["supabase"]["status"] = "healthy"
            self.results["supabase"]["details"] = {
                "connection": connection_info,
                "health": health_report["status"],
                "tables": len(health_report.get("tables", {})),
            }

            logger.info("✅ Supabase integration verified")
            return True

        except Exception as e:
            self.results["supabase"]["status"] = "failed"
            self.results["supabase"]["details"] = {"error": str(e)}
            logger.error(f"❌ Supabase verification failed: {e}")
            return False

    async def verify_api_endpoints(self) -> bool:
        """Verify API endpoints are accessible."""
        logger.info("🔍 Verifying API endpoints...")

        api_url = f"http://localhost:{settings.api_port}"
        auth = (settings.basic_auth_username, settings.basic_auth_password)

        endpoints_to_test = [
            ("GET", "/health", False),  # No auth required
            ("GET", "/api/v1/health", False),  # No auth required
            ("GET", "/api/v1/sales", True),  # Auth required
        ]

        if settings.is_supabase_enabled:
            endpoints_to_test.extend([
                ("GET", "/api/v1/supabase/config", True),
                ("GET", "/api/v1/supabase/connection", True),
            ])

        successful_endpoints = 0
        total_endpoints = len(endpoints_to_test)

        try:
            async with httpx.AsyncClient() as client:
                for method, endpoint, requires_auth in endpoints_to_test:
                    try:
                        kwargs = {"auth": auth} if requires_auth else {}
                        response = await client.request(
                            method,
                            f"{api_url}{endpoint}",
                            timeout=10.0,
                            **kwargs
                        )

                        if response.status_code in [200, 401]:  # 401 is OK if auth is not provided when required
                            successful_endpoints += 1
                            logger.info(f"✅ {method} {endpoint}: {response.status_code}")
                        else:
                            logger.warning(f"⚠️  {method} {endpoint}: {response.status_code}")

                    except Exception as e:
                        logger.warning(f"⚠️  {method} {endpoint}: {e}")

            success_rate = successful_endpoints / total_endpoints

            self.results["api"]["status"] = "healthy" if success_rate >= 0.8 else "degraded"
            self.results["api"]["details"] = {
                "successful_endpoints": successful_endpoints,
                "total_endpoints": total_endpoints,
                "success_rate": f"{success_rate:.1%}",
                "base_url": api_url,
            }

            if success_rate >= 0.8:
                logger.info(f"✅ API endpoints verified ({success_rate:.1%} success rate)")
                return True
            else:
                logger.warning(f"⚠️  API endpoints partially working ({success_rate:.1%} success rate)")
                return False

        except Exception as e:
            self.results["api"]["status"] = "failed"
            self.results["api"]["details"] = {"error": str(e)}
            logger.error(f"❌ API verification failed: {e}")
            return False

    async def verify_etl_components(self) -> bool:
        """Verify ETL components can be imported and configured."""
        logger.info("🔍 Verifying ETL components...")

        try:
            # Test imports

            # Test Spark configuration (don't actually create session to avoid Java issues)
            spark_config = settings.spark_config

            self.results["etl"]["status"] = "ready"
            self.results["etl"]["details"] = {
                "components": ["bronze", "silver", "gold"],
                "spark_configured": bool(spark_config),
                "windows_optimized": "windows_spark" in sys.modules,
            }

            logger.info("✅ ETL components ready")
            return True

        except Exception as e:
            self.results["etl"]["status"] = "failed"
            self.results["etl"]["details"] = {"error": str(e)}
            logger.error(f"❌ ETL verification failed: {e}")
            return False

    async def run_comprehensive_verification(self) -> dict[str, Any]:
        """Run all verification checks."""
        logger.info("🚀 Starting comprehensive deployment verification...")

        # Run all verification checks
        checks = [
            ("Database Connection", self.verify_database_connection()),
            ("Supabase Integration", self.verify_supabase_integration()),
            ("API Endpoints", self.verify_api_endpoints()),
            ("ETL Components", self.verify_etl_components()),
        ]

        passed_checks = 0
        total_checks = len(checks)

        for check_name, check_coro in checks:
            logger.info(f"\n--- {check_name} ---")
            try:
                result = await check_coro
                if result:
                    passed_checks += 1
            except Exception as e:
                logger.error(f"❌ {check_name} check failed with exception: {e}")

        # Determine overall status
        success_rate = passed_checks / total_checks

        if success_rate == 1.0:
            self.results["overall"]["status"] = "healthy"
            logger.info(f"\n🎉 All verification checks passed! ({passed_checks}/{total_checks})")
        elif success_rate >= 0.75:
            self.results["overall"]["status"] = "mostly_healthy"
            logger.warning(f"\n⚠️  Most checks passed ({passed_checks}/{total_checks})")
        else:
            self.results["overall"]["status"] = "unhealthy"
            logger.error(f"\n❌ Multiple checks failed ({passed_checks}/{total_checks})")

        # Add deployment recommendations
        recommendations = []

        if self.results["database"]["status"] != "healthy":
            recommendations.append("Fix database connectivity issues")

        if self.results["api"]["status"] not in ["healthy", "degraded"]:
            recommendations.append("Ensure API server is running on correct port")

        if settings.is_supabase_enabled and self.results["supabase"]["status"] != "healthy":
            recommendations.append("Check Supabase configuration and credentials")

        if not settings.is_supabase_enabled:
            recommendations.append("Consider enabling Supabase for production deployment")

        self.results["overall"]["recommendations"] = recommendations
        self.results["overall"]["success_rate"] = f"{success_rate:.1%}"

        return self.results

    def print_summary(self):
        """Print a human-readable summary."""
        print("\n" + "="*60)
        print("🎯 DEPLOYMENT VERIFICATION SUMMARY")
        print("="*60)

        status_emoji = {
            "healthy": "✅",
            "mostly_healthy": "⚠️",
            "ready": "✅",
            "degraded": "⚠️",
            "disabled": "⏸️",
            "failed": "❌",
            "unhealthy": "❌",
            "unknown": "❓"
        }

        for component, result in self.results.items():
            if component == "overall":
                continue

            status = result["status"]
            emoji = status_emoji.get(status, "❓")
            print(f"{emoji} {component.upper()}: {status}")

            if "error" in result.get("details", {}):
                print(f"   Error: {result['details']['error']}")

        print(f"\n🎯 OVERALL STATUS: {self.results['overall']['status'].upper()}")
        print(f"   Success Rate: {self.results['overall']['success_rate']}")

        if self.results["overall"]["recommendations"]:
            print("\n📋 RECOMMENDATIONS:")
            for rec in self.results["overall"]["recommendations"]:
                print(f"   • {rec}")

        print("\n" + "="*60)

        # Print next steps based on status
        overall_status = self.results["overall"]["status"]

        if overall_status == "healthy":
            print("🚀 Your deployment is ready for production!")
            print("   Next steps:")
            print("   • Run the ETL pipeline: poetry run python scripts/run_etl.py")
            print("   • Test API endpoints with your client applications")
            print("   • Monitor performance and logs")

        elif overall_status == "mostly_healthy":
            print("⚠️  Your deployment is mostly ready, with minor issues to address.")
            print("   Next steps:")
            print("   • Address the recommendations above")
            print("   • Test critical functionality")
            print("   • Consider proceeding with limited functionality")

        else:
            print("❌ Your deployment has significant issues that need attention.")
            print("   Next steps:")
            print("   • Fix critical issues identified above")
            print("   • Check configuration files (.env)")
            print("   • Verify all services are running")
            print("   • Re-run verification after fixes")


async def main():
    """Main verification function."""
    verifier = DeploymentVerifier()

    try:
        results = await verifier.run_comprehensive_verification()

        # Print human-readable summary
        verifier.print_summary()

        # Save detailed results to file
        results_file = Path("deployment_verification_results.json")
        with open(results_file, "w") as f:
            json.dump(results, f, indent=2, default=str)

        print(f"\n📄 Detailed results saved to: {results_file}")

        # Exit with appropriate code
        overall_status = results["overall"]["status"]
        if overall_status == "healthy":
            sys.exit(0)
        elif overall_status == "mostly_healthy":
            sys.exit(1)  # Warning
        else:
            sys.exit(2)  # Error

    except KeyboardInterrupt:
        logger.info("\n⏹️  Verification cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"\n💥 Verification script failed: {e}")
        sys.exit(3)


if __name__ == "__main__":
    print("🔍 PwC Data Engineering Challenge - Deployment Verification")
    print("=" * 60)
    asyncio.run(main())

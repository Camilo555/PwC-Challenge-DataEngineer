#!/usr/bin/env python3
"""
Comprehensive Performance Benchmarking Script
Validates all APIs meet <15ms SLA requirement

This script performs comprehensive performance validation for the $27.8M+ platform
to ensure all critical APIs deliver <15ms response times under realistic load.

Usage:
    python scripts/performance_benchmark.py --target-sla 15 --comprehensive
    python scripts/performance_benchmark.py --story 1.1 --load-test
    python scripts/performance_benchmark.py --validate-production
"""

import asyncio
import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from api.performance.performance_validator import (
    PerformanceValidator,
    PerformanceReport,
    get_performance_validator
)
from core.logging import get_logger

logger = get_logger("performance_benchmark")

class ComprehensivePerformanceBenchmark:
    """Comprehensive performance benchmarking for all stories"""
    
    def __init__(self, sla_target_ms: float = 15.0, base_url: str = "http://localhost:8000"):
        self.sla_target_ms = sla_target_ms
        self.base_url = base_url
        self.validator: Optional[PerformanceValidator] = None
        
        # Story-specific configuration
        self.story_configs = {
            "1.1": {
                "name": "Real-time BI Dashboard",
                "endpoints": [
                    ("/api/v1/dashboard/executive", "GET"),
                    ("/api/v1/dashboard/revenue", "GET"),
                    ("/api/v1/performance/stats", "GET"),
                    ("/api/v1/health/comprehensive", "GET")
                ],
                "priority": "critical",
                "load_test_requests": 100,
                "concurrent_users": 10,
                "expected_cache_hit_rate": 0.95
            },
            "2.2": {
                "name": "API Performance Optimization",
                "endpoints": [
                    ("/api/v1/cache/warm", "POST"),
                    ("/api/v1/scaling/status", "GET"),
                    ("/api/v1/scaling/trigger", "POST")
                ],
                "priority": "high",
                "load_test_requests": 50,
                "concurrent_users": 5,
                "expected_cache_hit_rate": 0.90
            },
            "4.1": {
                "name": "Mobile Analytics Platform",
                "endpoints": [
                    ("/api/v1/mobile/analytics/dashboard", "GET"),
                    ("/api/v1/mobile/analytics/kpis", "GET"),
                    ("/api/v1/mobile/sync/status", "GET")
                ],
                "priority": "high",
                "load_test_requests": 75,
                "concurrent_users": 8,
                "expected_cache_hit_rate": 0.90
            },
            "4.2": {
                "name": "AI/LLM Conversational Analytics",
                "endpoints": [
                    ("/api/v1/ai/query", "POST"),
                    ("/api/v1/ai/insights", "POST"),
                    ("/api/v1/ai/conversation/history", "GET")
                ],
                "priority": "medium",
                "load_test_requests": 25,
                "concurrent_users": 3,
                "expected_cache_hit_rate": 0.85,
                "special_payloads": {
                    "/api/v1/ai/query": {
                        "query": "What were our sales trends in Q4?",
                        "complexity": "simple",
                        "user_tier": "basic"
                    },
                    "/api/v1/ai/insights": {
                        "data_source": "sales",
                        "insight_type": "trend_analysis",
                        "time_range": "last_30_days"
                    }
                }
            },
            "core": {
                "name": "Core Platform APIs", 
                "endpoints": [
                    ("/api/v1/health", "GET"),
                    ("/", "GET"),
                    ("/api/v1/auth/token", "POST")
                ],
                "priority": "critical",
                "load_test_requests": 100,
                "concurrent_users": 15,
                "expected_cache_hit_rate": 0.95,
                "special_payloads": {
                    "/api/v1/auth/token": {
                        "username": "benchmark_user",
                        "password": "benchmark_pass"
                    }
                }
            }
        }
    
    async def initialize(self):
        """Initialize the benchmarking system"""
        self.validator = await get_performance_validator(self.sla_target_ms)
        logger.info(f"Performance benchmark initialized with <{self.sla_target_ms}ms SLA")
    
    async def close(self):
        """Close resources"""
        if self.validator:
            await self.validator.close()
    
    async def benchmark_story(self, story_id: str) -> Dict[str, Any]:
        """Benchmark a specific story's endpoints"""
        
        if story_id not in self.story_configs:
            raise ValueError(f"Unknown story: {story_id}. Available: {list(self.story_configs.keys())}")
        
        story_config = self.story_configs[story_id]
        logger.info(f"🧪 Benchmarking Story {story_id}: {story_config['name']}")
        
        results = {
            "story_id": story_id,
            "story_name": story_config["name"],
            "priority": story_config["priority"],
            "sla_target_ms": self.sla_target_ms,
            "benchmark_timestamp": datetime.now().isoformat(),
            "endpoint_results": {},
            "summary": {}
        }
        
        total_requests = 0
        sla_compliant_requests = 0
        response_times = []
        
        # Test each endpoint
        for endpoint, method in story_config["endpoints"]:
            try:
                logger.info(f"  📊 Testing {method} {endpoint}")
                
                # Get special payload if exists
                payload = story_config.get("special_payloads", {}).get(endpoint)
                
                result = await self.validator.validate_endpoint(
                    base_url=self.base_url,
                    endpoint=endpoint,
                    method=method,
                    payload=payload,
                    measurement_requests=story_config["load_test_requests"],
                    concurrent_users=story_config["concurrent_users"]
                )
                
                results["endpoint_results"][endpoint] = {
                    "method": method,
                    "average_response_time_ms": result.average_response_time_ms,
                    "p50_response_time_ms": result.p50_response_time_ms,
                    "p95_response_time_ms": result.p95_response_time_ms,
                    "p99_response_time_ms": result.p99_response_time_ms,
                    "sla_compliance_rate": result.sla_compliance_rate,
                    "success_rate": result.success_rate,
                    "requests_per_second": result.requests_per_second,
                    "sla_compliant": result.average_response_time_ms <= self.sla_target_ms
                }
                
                # Update totals
                total_requests += result.total_requests
                sla_compliant_requests += int(result.total_requests * result.sla_compliance_rate)
                response_times.append(result.average_response_time_ms)
                
                # Log result
                sla_status = "✅ PASS" if result.average_response_time_ms <= self.sla_target_ms else "❌ FAIL"
                logger.info(f"    {sla_status} Avg: {result.average_response_time_ms:.2f}ms | "
                          f"P95: {result.p95_response_time_ms:.2f}ms | "
                          f"Success: {result.success_rate:.1%}")
                
            except Exception as e:
                logger.error(f"    ❌ ERROR testing {endpoint}: {e}")
                results["endpoint_results"][endpoint] = {
                    "error": str(e),
                    "sla_compliant": False
                }
        
        # Calculate story summary
        if response_times:
            results["summary"] = {
                "total_endpoints_tested": len(story_config["endpoints"]),
                "successful_endpoints": len([r for r in results["endpoint_results"].values() 
                                           if r.get("sla_compliant", False)]),
                "total_requests": total_requests,
                "overall_sla_compliance_rate": sla_compliant_requests / total_requests if total_requests > 0 else 0,
                "average_response_time_ms": sum(response_times) / len(response_times),
                "story_performance_score": self._calculate_story_score(results),
                "production_ready": self._is_story_production_ready(results)
            }
        
        return results
    
    async def benchmark_all_stories(self) -> Dict[str, Any]:
        """Benchmark all stories comprehensively"""
        
        logger.info(f"🚀 Starting comprehensive performance benchmark for all stories")
        logger.info(f"📊 Target SLA: <{self.sla_target_ms}ms response time")
        logger.info(f"🎯 Base URL: {self.base_url}")
        
        overall_results = {
            "benchmark_type": "comprehensive",
            "sla_target_ms": self.sla_target_ms,
            "benchmark_timestamp": datetime.now().isoformat(),
            "story_results": {},
            "overall_summary": {}
        }
        
        # Benchmark each story
        story_summaries = []
        for story_id in self.story_configs.keys():
            try:
                story_result = await self.benchmark_story(story_id)
                overall_results["story_results"][story_id] = story_result
                story_summaries.append(story_result.get("summary", {}))
                
                # Brief summary log
                summary = story_result.get("summary", {})
                if summary:
                    sla_rate = summary.get("overall_sla_compliance_rate", 0) * 100
                    avg_time = summary.get("average_response_time_ms", 0)
                    ready = "✅ READY" if summary.get("production_ready", False) else "❌ NOT READY"
                    logger.info(f"Story {story_id}: {sla_rate:.1f}% SLA compliance, {avg_time:.2f}ms avg - {ready}")
                
            except Exception as e:
                logger.error(f"Failed to benchmark story {story_id}: {e}")
                overall_results["story_results"][story_id] = {"error": str(e)}
        
        # Calculate overall summary
        if story_summaries:
            total_endpoints = sum(s.get("total_endpoints_tested", 0) for s in story_summaries)
            successful_endpoints = sum(s.get("successful_endpoints", 0) for s in story_summaries)
            overall_sla_rates = [s.get("overall_sla_compliance_rate", 0) for s in story_summaries if s.get("overall_sla_compliance_rate")]
            avg_response_times = [s.get("average_response_time_ms", 0) for s in story_summaries if s.get("average_response_time_ms")]
            
            overall_results["overall_summary"] = {
                "total_stories_tested": len(story_summaries),
                "total_endpoints_tested": total_endpoints,
                "successful_endpoints": successful_endpoints,
                "endpoint_success_rate": successful_endpoints / total_endpoints if total_endpoints > 0 else 0,
                "overall_sla_compliance_rate": sum(overall_sla_rates) / len(overall_sla_rates) if overall_sla_rates else 0,
                "platform_average_response_time_ms": sum(avg_response_times) / len(avg_response_times) if avg_response_times else 0,
                "production_ready_stories": len([s for s in story_summaries if s.get("production_ready", False)]),
                "platform_production_ready": all(s.get("production_ready", False) for s in story_summaries),
                "confidence_level": self._calculate_confidence_level(story_summaries)
            }
            
        return overall_results
    
    def _calculate_story_score(self, story_result: Dict[str, Any]) -> float:
        """Calculate performance score for a story (0-100)"""
        
        endpoint_results = story_result.get("endpoint_results", {})
        if not endpoint_results:
            return 0.0
        
        scores = []
        for endpoint, result in endpoint_results.items():
            if "error" in result:
                scores.append(0.0)
                continue
            
            # SLA compliance (50 points)
            sla_score = result.get("sla_compliance_rate", 0) * 50
            
            # Success rate (25 points)  
            success_score = result.get("success_rate", 0) * 25
            
            # Response time quality (25 points)
            avg_time = result.get("average_response_time_ms", float('inf'))
            if avg_time <= 5.0:
                time_score = 25  # Excellent
            elif avg_time <= 10.0:
                time_score = 20  # Good
            elif avg_time <= 15.0:
                time_score = 15  # Acceptable
            elif avg_time <= 25.0:
                time_score = 5   # Poor
            else:
                time_score = 0   # Unacceptable
            
            endpoint_score = sla_score + success_score + time_score
            scores.append(endpoint_score)
        
        return sum(scores) / len(scores) if scores else 0.0
    
    def _is_story_production_ready(self, story_result: Dict[str, Any]) -> bool:
        """Determine if story is production ready"""
        
        summary = story_result.get("summary", {})
        
        # Criteria for production readiness
        sla_compliance = summary.get("overall_sla_compliance_rate", 0) >= 0.95  # 95% SLA compliance
        performance_score = summary.get("story_performance_score", 0) >= 85.0   # 85+ performance score
        avg_response_time = summary.get("average_response_time_ms", float('inf')) <= self.sla_target_ms
        
        return sla_compliance and performance_score and avg_response_time
    
    def _calculate_confidence_level(self, story_summaries: List[Dict[str, Any]]) -> float:
        """Calculate overall confidence level (0.0-1.0)"""
        
        if not story_summaries:
            return 0.0
        
        # Weight by story priority
        priority_weights = {"critical": 1.0, "high": 0.8, "medium": 0.6}
        
        weighted_scores = []
        for summary in story_summaries:
            sla_compliance = summary.get("overall_sla_compliance_rate", 0)
            # Assume high priority if not specified
            weight = priority_weights.get("high", 0.8)  
            weighted_scores.append(sla_compliance * weight)
        
        return sum(weighted_scores) / len(weighted_scores) if weighted_scores else 0.0
    
    def generate_report(self, results: Dict[str, Any]) -> str:
        """Generate detailed performance report"""
        
        lines = [
            "=" * 80,
            "COMPREHENSIVE PERFORMANCE BENCHMARK REPORT",
            f"Generated: {datetime.now().isoformat()}",
            f"SLA Target: <{self.sla_target_ms}ms response time",
            f"Test Environment: {self.base_url}",
            "=" * 80,
            ""
        ]
        
        # Overall summary
        overall = results.get("overall_summary", {})
        if overall:
            lines.extend([
                "🎯 EXECUTIVE SUMMARY",
                "-" * 40,
                f"Total Stories Tested: {overall.get('total_stories_tested', 0)}",
                f"Total Endpoints Tested: {overall.get('total_endpoints_tested', 0)}",
                f"Endpoint Success Rate: {overall.get('endpoint_success_rate', 0):.1%}",
                f"Platform SLA Compliance: {overall.get('overall_sla_compliance_rate', 0):.1%}",
                f"Average Response Time: {overall.get('platform_average_response_time_ms', 0):.2f}ms",
                f"Production Ready Stories: {overall.get('production_ready_stories', 0)}/{overall.get('total_stories_tested', 0)}",
                f"Platform Production Ready: {'✅ YES' if overall.get('platform_production_ready', False) else '❌ NO'}",
                f"Confidence Level: {overall.get('confidence_level', 0):.1%}",
                ""
            ])
        
        # Story details
        story_results = results.get("story_results", {})
        for story_id, story_data in story_results.items():
            if "error" in story_data:
                lines.extend([
                    f"📊 STORY {story_id.upper()}: ERROR",
                    f"   Error: {story_data['error']}",
                    ""
                ])
                continue
            
            story_config = self.story_configs.get(story_id, {})
            summary = story_data.get("summary", {})
            
            lines.extend([
                f"📊 STORY {story_id.upper()}: {story_data.get('story_name', 'Unknown')}",
                f"   Priority: {story_data.get('priority', 'unknown').upper()}",
                f"   SLA Compliance: {summary.get('overall_sla_compliance_rate', 0):.1%}",
                f"   Average Response Time: {summary.get('average_response_time_ms', 0):.2f}ms",
                f"   Performance Score: {summary.get('story_performance_score', 0):.1f}/100",
                f"   Production Ready: {'✅ YES' if summary.get('production_ready', False) else '❌ NO'}",
                ""
            ])
            
            # Endpoint details
            endpoint_results = story_data.get("endpoint_results", {})
            for endpoint, endpoint_data in endpoint_results.items():
                if "error" in endpoint_data:
                    lines.append(f"     ❌ {endpoint}: ERROR - {endpoint_data['error']}")
                else:
                    status = "✅ PASS" if endpoint_data.get("sla_compliant", False) else "❌ FAIL"
                    avg_time = endpoint_data.get("average_response_time_ms", 0)
                    p95_time = endpoint_data.get("p95_response_time_ms", 0)
                    success_rate = endpoint_data.get("success_rate", 0)
                    
                    lines.append(
                        f"     {status} {endpoint}: "
                        f"Avg {avg_time:.2f}ms | P95 {p95_time:.2f}ms | Success {success_rate:.1%}"
                    )
            lines.append("")
        
        lines.extend([
            "=" * 80,
            "RECOMMENDATIONS",
            "-" * 40
        ])
        
        # Generate recommendations
        if overall:
            platform_sla = overall.get("overall_sla_compliance_rate", 0)
            if platform_sla < 0.95:
                lines.append("🚨 CRITICAL: Platform SLA compliance below 95% - immediate optimization required")
            if overall.get("platform_average_response_time_ms", 0) > self.sla_target_ms:
                lines.append("🚨 CRITICAL: Platform average response time exceeds SLA target")
            if not overall.get("platform_production_ready", False):
                lines.append("⚠️  HIGH: Platform not production ready - address failing stories")
        
        lines.extend([
            "",
            "=" * 80,
            f"Report generated by Comprehensive Performance Benchmark",
            f"Target SLA: <{self.sla_target_ms}ms | Production Readiness Assessment",
            "=" * 80
        ])
        
        return "\n".join(lines)

async def main():
    """Main benchmarking function"""
    
    parser = argparse.ArgumentParser(description="Comprehensive Performance Benchmark")
    parser.add_argument("--target-sla", type=float, default=15.0, help="Target SLA in milliseconds")
    parser.add_argument("--base-url", default="http://localhost:8000", help="Base URL for testing")
    parser.add_argument("--story", help="Specific story to benchmark (1.1, 2.2, 4.1, 4.2, core)")
    parser.add_argument("--comprehensive", action="store_true", help="Run comprehensive benchmark of all stories")
    parser.add_argument("--output-file", help="Output file for results")
    parser.add_argument("--validate-production", action="store_true", help="Validate production readiness")
    
    args = parser.parse_args()
    
    # Initialize benchmark
    benchmark = ComprehensivePerformanceBenchmark(
        sla_target_ms=args.target_sla,
        base_url=args.base_url
    )
    
    try:
        await benchmark.initialize()
        
        if args.story:
            # Single story benchmark
            logger.info(f"Running single story benchmark: {args.story}")
            results = await benchmark.benchmark_story(args.story)
            
        elif args.comprehensive or args.validate_production:
            # Comprehensive benchmark
            logger.info("Running comprehensive benchmark for all stories")
            results = await benchmark.benchmark_all_stories()
            
        else:
            # Default to comprehensive
            logger.info("Running comprehensive benchmark (default)")
            results = await benchmark.benchmark_all_stories()
        
        # Generate report
        report = benchmark.generate_report(results)
        
        # Output results
        if args.output_file:
            with open(args.output_file, 'w') as f:
                f.write(report)
            logger.info(f"Report written to {args.output_file}")
        else:
            print(report)
        
        # Exit code based on results
        if args.validate_production:
            overall = results.get("overall_summary", {})
            if overall.get("platform_production_ready", False):
                logger.info("✅ Platform is PRODUCTION READY")
                sys.exit(0)
            else:
                logger.error("❌ Platform is NOT production ready")
                sys.exit(1)
        
    except KeyboardInterrupt:
        logger.info("Benchmark interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        sys.exit(1)
    finally:
        await benchmark.close()

if __name__ == "__main__":
    asyncio.run(main())
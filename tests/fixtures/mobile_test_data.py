"""
Mobile Analytics Test Data Fixtures

Comprehensive test data generation for mobile analytics testing:
- Mock mobile device configurations
- User profile test data
- Dashboard data scenarios
- Analytics payload samples
- Biometric authentication data
- Sync conflict scenarios
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

class DeviceType(str, Enum):
    IOS_PHONE = "ios_phone"
    ANDROID_PHONE = "android_phone"
    IOS_TABLET = "ios_tablet"
    ANDROID_TABLET = "android_tablet"
    WEB_MOBILE = "web_mobile"

class NetworkType(str, Enum):
    WIFI = "wifi"
    CELLULAR_4G = "4g"
    CELLULAR_3G = "3g"
    CELLULAR_2G = "2g"

@dataclass
class MockMobileDevice:
    """Mock mobile device configuration for testing"""
    device_id: str
    device_type: DeviceType
    platform: str
    os_version: str
    app_version: str
    screen_dimensions: Dict[str, int]
    memory_gb: float
    storage_gb: float
    network_type: NetworkType
    biometric_capabilities: List[str]
    performance_tier: float
    
def create_test_mobile_device(
    device_type: DeviceType = DeviceType.IOS_PHONE,
    performance_tier: str = "high"
) -> MockMobileDevice:
    """Create test mobile device configuration"""
    
    device_configs = {
        DeviceType.IOS_PHONE: {
            "platform": "iOS",
            "os_version": "17.2.1",
            "screen_dimensions": {"width": 375, "height": 812},
            "memory_gb": 6.0,
            "storage_gb": 128.0,
            "biometric_capabilities": ["touchid", "faceid"]
        },
        DeviceType.ANDROID_PHONE: {
            "platform": "Android",
            "os_version": "14.0",
            "screen_dimensions": {"width": 393, "height": 851},
            "memory_gb": 8.0,
            "storage_gb": 256.0,
            "biometric_capabilities": ["fingerprint", "face_unlock"]
        },
        DeviceType.IOS_TABLET: {
            "platform": "iPadOS",
            "os_version": "17.2.1",
            "screen_dimensions": {"width": 768, "height": 1024},
            "memory_gb": 8.0,
            "storage_gb": 256.0,
            "biometric_capabilities": ["touchid", "faceid"]
        },
        DeviceType.ANDROID_TABLET: {
            "platform": "Android",
            "os_version": "14.0",
            "screen_dimensions": {"width": 800, "height": 1280},
            "memory_gb": 12.0,
            "storage_gb": 512.0,
            "biometric_capabilities": ["fingerprint"]
        },
        DeviceType.WEB_MOBILE: {
            "platform": "Web",
            "os_version": "Chrome 120",
            "screen_dimensions": {"width": 390, "height": 844},
            "memory_gb": 4.0,
            "storage_gb": 16.0,
            "biometric_capabilities": ["webauthn"]
        }
    }
    
    config = device_configs[device_type]
    
    performance_tiers = {
        "low": 0.6,
        "medium": 0.8,
        "high": 1.0,
        "premium": 1.2
    }
    
    return MockMobileDevice(
        device_id=f"test_device_{device_type.value}_{uuid.uuid4().hex[:8]}",
        device_type=device_type,
        platform=config["platform"],
        os_version=config["os_version"],
        app_version="1.2.3",
        screen_dimensions=config["screen_dimensions"],
        memory_gb=config["memory_gb"],
        storage_gb=config["storage_gb"],
        network_type=NetworkType.WIFI,
        biometric_capabilities=config["biometric_capabilities"],
        performance_tier=performance_tiers.get(performance_tier, 1.0)
    )

def create_test_user_profile(
    user_type: str = "premium",
    preferences: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Create test user profile data"""
    
    base_profile = {
        "user_id": f"test_user_{uuid.uuid4().hex[:8]}",
        "name": "Test User",
        "email": "test.user@example.com",
        "role": "analyst",
        "department": "sales",
        "region": "north_america",
        "timezone": "America/New_York",
        "created_at": (datetime.utcnow() - timedelta(days=30)).isoformat(),
        "last_login": (datetime.utcnow() - timedelta(hours=2)).isoformat(),
        "subscription_type": user_type,
        "permissions": [
            "view_dashboards",
            "view_analytics",
            "export_data"
        ]
    }
    
    # Add user type specific features
    if user_type == "premium":
        base_profile["permissions"].extend([
            "advanced_analytics",
            "custom_reports",
            "ai_insights"
        ])
    elif user_type == "enterprise":
        base_profile["permissions"].extend([
            "advanced_analytics",
            "custom_reports", 
            "ai_insights",
            "admin_functions",
            "manage_users"
        ])
    
    # Merge custom preferences
    if preferences:
        base_profile.update(preferences)
    
    return base_profile

def create_test_dashboard_data(
    data_scenario: str = "normal",
    time_range: str = "7d"
) -> Dict[str, Any]:
    """Create test dashboard data for different scenarios"""
    
    base_time = datetime.utcnow()
    
    scenarios = {
        "normal": {
            "kpis": {
                "revenue": 2450000.50,
                "orders": 1250,
                "growth": 15.5,
                "conversion": 3.2,
                "aov": 1960.0,
                "users": 8500
            },
            "trends": [
                {"date": (base_time - timedelta(days=i)).strftime("%Y-%m-%d"), 
                 "value": 125000 + (i * 2500), 
                 "change": 5.2 + (i * 0.1)}
                for i in range(7)
            ],
            "alerts": [
                {
                    "type": "revenue",
                    "severity": "info", 
                    "message": "Revenue target exceeded by 15%",
                    "timestamp": base_time.isoformat()
                }
            ]
        },
        "high_performance": {
            "kpis": {
                "revenue": 3250000.75,
                "orders": 2100,
                "growth": 25.8,
                "conversion": 4.1,
                "aov": 1547.6,
                "users": 12500
            },
            "trends": [
                {"date": (base_time - timedelta(days=i)).strftime("%Y-%m-%d"),
                 "value": 180000 + (i * 5000),
                 "change": 8.5 + (i * 0.2)}
                for i in range(7)
            ],
            "alerts": [
                {
                    "type": "performance",
                    "severity": "success",
                    "message": "All KPIs exceeding targets",
                    "timestamp": base_time.isoformat()
                }
            ]
        },
        "declining": {
            "kpis": {
                "revenue": 1850000.25,
                "orders": 850,
                "growth": -5.2,
                "conversion": 2.1,
                "aov": 2176.5,
                "users": 6200
            },
            "trends": [
                {"date": (base_time - timedelta(days=i)).strftime("%Y-%m-%d"),
                 "value": 95000 - (i * 2000),
                 "change": -2.1 - (i * 0.3)}
                for i in range(7)
            ],
            "alerts": [
                {
                    "type": "warning",
                    "severity": "warning",
                    "message": "Revenue declining for 3 consecutive days",
                    "timestamp": base_time.isoformat()
                },
                {
                    "type": "conversion", 
                    "severity": "error",
                    "message": "Conversion rate below 2.5% threshold",
                    "timestamp": (base_time - timedelta(hours=1)).isoformat()
                }
            ]
        },
        "empty": {
            "kpis": {},
            "trends": [],
            "alerts": []
        }
    }
    
    data = scenarios.get(data_scenario, scenarios["normal"])
    
    # Add metadata
    data["metadata"] = {
        "generated_at": base_time.isoformat(),
        "scenario": data_scenario,
        "time_range": time_range,
        "data_freshness": "real_time",
        "cache_status": "fresh"
    }
    
    return data

def create_test_analytics_payload(
    dataset: str = "sales_performance",
    size: str = "small"
) -> Dict[str, Any]:
    """Create test analytics payload for mobile consumption"""
    
    base_time = datetime.utcnow()
    
    size_configs = {
        "small": {"data_points": 20, "dimensions": 2, "metrics": 3},
        "medium": {"data_points": 50, "dimensions": 3, "metrics": 5},
        "large": {"data_points": 100, "dimensions": 4, "metrics": 7}
    }
    
    config = size_configs.get(size, size_configs["small"])
    
    # Generate data points
    data_points = []
    for i in range(config["data_points"]):
        data_points.append({
            "date": (base_time - timedelta(days=i)).strftime("%Y-%m-%d"),
            "revenue": round(45000 + (i * 2500.5), 2),
            "orders": 150 + (i * 10),
            "region": ["north", "south", "east", "west"][i % 4],
            "category": ["electronics", "clothing", "home"][i % 3]
        })
    
    return {
        "dataset": dataset,
        "dimensions": ["region", "category", "date"][:config["dimensions"]],
        "metrics": {
            "revenue": round(sum(dp["revenue"] for dp in data_points), 2),
            "orders": sum(dp["orders"] for dp in data_points),
            "avg_order_value": round(
                sum(dp["revenue"] for dp in data_points) / 
                sum(dp["orders"] for dp in data_points), 2
            )
        },
        "aggregation_level": "daily",
        "data_points": data_points,
        "filters_applied": {},
        "generated_at": base_time.isoformat(),
        "size_category": size,
        "mobile_optimized": True
    }

def create_test_biometric_data(
    biometric_type: str = "fingerprint",
    success: bool = True
) -> Dict[str, Any]:
    """Create test biometric authentication data"""
    
    biometric_configs = {
        "fingerprint": {
            "hash_length": 64,
            "hardware_required": True,
            "fallback_available": True,
            "security_level": "high"
        },
        "face_id": {
            "hash_length": 128,
            "hardware_required": True,
            "fallback_available": True,
            "security_level": "very_high"
        },
        "voice": {
            "hash_length": 96,
            "hardware_required": False,
            "fallback_available": True,
            "security_level": "medium"
        },
        "iris": {
            "hash_length": 256,
            "hardware_required": True,
            "fallback_available": False,
            "security_level": "maximum"
        }
    }
    
    config = biometric_configs.get(biometric_type, biometric_configs["fingerprint"])
    
    if success:
        biometric_hash = "a" * config["hash_length"]
        challenge_response = "valid_challenge_response_12345"
    else:
        biometric_hash = "invalid_hash"
        challenge_response = "invalid_challenge"
    
    return {
        "device_id": f"test_device_{uuid.uuid4().hex[:8]}",
        "biometric_hash": biometric_hash,
        "biometric_type": biometric_type,
        "challenge_response": challenge_response,
        "device_info": {
            "platform": "iOS" if biometric_type == "face_id" else "Android",
            "biometric_hardware": config["hardware_required"],
            "security_enclave": config["security_level"] in ["high", "very_high", "maximum"],
            "fallback_methods": ["passcode", "pattern"] if config["fallback_available"] else []
        },
        "timestamp": datetime.utcnow().isoformat(),
        "expected_success": success
    }

def create_test_sync_data(
    conflict_scenario: str = "none",
    change_count: int = 5
) -> Dict[str, Any]:
    """Create test offline sync data with various conflict scenarios"""
    
    base_time = datetime.utcnow()
    
    # Generate local changes
    local_changes = []
    for i in range(change_count):
        change = {
            "id": f"item_{i}",
            "type": "update",
            "field": f"field_{i}",
            "value": f"local_value_{i}",
            "timestamp": (base_time - timedelta(minutes=i*10)).timestamp()
        }
        
        # Add conflicts based on scenario
        if conflict_scenario == "timestamp_conflicts" and i % 2 == 0:
            # Make some changes very old to trigger conflicts
            change["timestamp"] = (base_time - timedelta(hours=2)).timestamp()
        elif conflict_scenario == "value_conflicts" and i % 3 == 0:
            # Add server conflict marker
            change["server_value"] = f"server_value_{i}"
            change["conflict_detected"] = True
            
        local_changes.append(change)
    
    return {
        "device_id": f"test_device_{uuid.uuid4().hex[:8]}",
        "last_sync_timestamp": (base_time - timedelta(hours=1)).isoformat(),
        "local_changes": local_changes,
        "conflict_resolution_strategy": "server_wins" if conflict_scenario != "none" else "auto",
        "sync_scope": ["dashboard", "analytics", "preferences"],
        "expected_conflicts": conflict_scenario != "none",
        "conflict_count": len([c for c in local_changes if c.get("conflict_detected", False)])
    }

def create_test_push_notification_data(
    notification_type: str = "revenue_alert",
    device_count: int = 1
) -> Dict[str, Any]:
    """Create test push notification data"""
    
    notification_templates = {
        "revenue_alert": {
            "title": "Revenue Milestone Reached!",
            "body": "Q4 revenue has exceeded target by 15% - great job!",
            "priority": "high",
            "data": {
                "alert_type": "revenue_milestone",
                "dashboard_url": "/dashboard/executive",
                "achievement": "q4_revenue_target"
            }
        },
        "performance_warning": {
            "title": "Performance Alert",
            "body": "Conversion rate has dropped below 2.5% in the last hour",
            "priority": "high",
            "data": {
                "alert_type": "performance_warning",
                "metric": "conversion_rate",
                "threshold": "2.5%"
            }
        },
        "weekly_report": {
            "title": "Weekly Analytics Report",
            "body": "Your weekly performance summary is ready to view",
            "priority": "normal",
            "data": {
                "report_type": "weekly_summary",
                "report_url": "/reports/weekly",
                "period": "week_ending_2025-01-07"
            }
        },
        "system_maintenance": {
            "title": "Scheduled Maintenance",
            "body": "System maintenance will begin in 1 hour. Please save your work.",
            "priority": "normal",
            "data": {
                "maintenance_type": "scheduled",
                "start_time": (datetime.utcnow() + timedelta(hours=1)).isoformat(),
                "duration": "30 minutes"
            }
        }
    }
    
    template = notification_templates.get(notification_type, notification_templates["revenue_alert"])
    
    # Generate device tokens
    device_tokens = [f"device_token_{i}_{uuid.uuid4().hex[:16]}" for i in range(device_count)]
    
    notification_data = {
        "device_tokens": device_tokens,
        "title": template["title"],
        "body": template["body"],
        "data": template["data"],
        "priority": template["priority"],
        "analytics_tracking": True,
        "scheduled_time": None,  # Immediate delivery
        "notification_type": notification_type,
        "created_at": datetime.utcnow().isoformat()
    }
    
    return notification_data

def create_test_performance_metrics(
    performance_scenario: str = "optimal"
) -> Dict[str, Any]:
    """Create test mobile performance metrics"""
    
    scenarios = {
        "optimal": {
            "response_times": {
                "avg_ms": 12.5,
                "p95_ms": 18.2,
                "p99_ms": 25.4
            },
            "payload_sizes": {
                "avg_kb": 15.2,
                "max_kb": 28.7,
                "compression_ratio": 0.65
            },
            "cache_performance": {
                "hit_rate": 0.87,
                "miss_rate": 0.13,
                "avg_cache_time_ms": 2.1
            }
        },
        "degraded": {
            "response_times": {
                "avg_ms": 45.8,
                "p95_ms": 68.5,
                "p99_ms": 89.2
            },
            "payload_sizes": {
                "avg_kb": 35.6,
                "max_kb": 78.3,
                "compression_ratio": 0.45
            },
            "cache_performance": {
                "hit_rate": 0.62,
                "miss_rate": 0.38,
                "avg_cache_time_ms": 8.7
            }
        },
        "poor": {
            "response_times": {
                "avg_ms": 125.3,
                "p95_ms": 245.8,
                "p99_ms": 456.7
            },
            "payload_sizes": {
                "avg_kb": 89.4,
                "max_kb": 156.8,
                "compression_ratio": 0.25
            },
            "cache_performance": {
                "hit_rate": 0.34,
                "miss_rate": 0.66,
                "avg_cache_time_ms": 25.6
            }
        }
    }
    
    base_metrics = scenarios.get(performance_scenario, scenarios["optimal"])
    
    # Add additional metrics
    base_metrics.update({
        "sync_performance": {
            "avg_sync_time_ms": 245.6 if performance_scenario == "optimal" else 567.8,
            "delta_size_kb": 12.3,
            "conflict_rate": 0.02 if performance_scenario == "optimal" else 0.08
        },
        "network_efficiency": {
            "requests_per_session": 8.5,
            "data_usage_mb": 2.1 if performance_scenario == "optimal" else 4.8,
            "offline_capability": 0.95 if performance_scenario == "optimal" else 0.72
        },
        "timestamp": datetime.utcnow().isoformat(),
        "scenario": performance_scenario
    })
    
    return base_metrics

# Utility functions for test data generation
def create_large_dataset(record_count: int = 1000) -> List[Dict[str, Any]]:
    """Create large dataset for performance testing"""
    
    records = []
    base_time = datetime.utcnow()
    
    for i in range(record_count):
        records.append({
            "id": i,
            "timestamp": (base_time - timedelta(hours=i)).isoformat(),
            "revenue": round(1000 + (i * 12.5), 2),
            "orders": 25 + (i % 50),
            "region": ["north", "south", "east", "west"][i % 4],
            "category": ["A", "B", "C"][i % 3],
            "user_segment": ["premium", "standard", "basic"][i % 3]
        })
    
    return records

def create_error_scenarios() -> Dict[str, Dict[str, Any]]:
    """Create various error scenarios for testing"""
    
    return {
        "network_timeout": {
            "error_type": "network_timeout",
            "message": "Request timeout after 30 seconds",
            "retry_possible": True,
            "recovery_suggestion": "Check network connection and retry"
        },
        "server_error": {
            "error_type": "server_error",
            "message": "Internal server error (500)",
            "retry_possible": True,
            "recovery_suggestion": "Wait a moment and try again"
        },
        "authentication_failed": {
            "error_type": "authentication_failed", 
            "message": "Biometric authentication failed",
            "retry_possible": True,
            "recovery_suggestion": "Try authentication again or use fallback method"
        },
        "data_corruption": {
            "error_type": "data_corruption",
            "message": "Local data corrupted during sync",
            "retry_possible": False,
            "recovery_suggestion": "Reset local cache and re-sync from server"
        },
        "quota_exceeded": {
            "error_type": "quota_exceeded",
            "message": "API rate limit exceeded",
            "retry_possible": True,
            "recovery_suggestion": "Wait before making additional requests"
        }
    }

# Export key functions for use in tests
__all__ = [
    "create_test_mobile_device",
    "create_test_user_profile", 
    "create_test_dashboard_data",
    "create_test_analytics_payload",
    "create_test_biometric_data",
    "create_test_sync_data",
    "create_test_push_notification_data",
    "create_test_performance_metrics",
    "create_large_dataset",
    "create_error_scenarios",
    "DeviceType",
    "NetworkType",
    "MockMobileDevice"
]
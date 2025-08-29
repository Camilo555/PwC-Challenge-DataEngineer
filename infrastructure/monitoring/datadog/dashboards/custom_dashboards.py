"""
DataDog Custom Dashboards and Infrastructure Metrics
Comprehensive dashboard creation and custom infrastructure KPIs
"""

import logging
from typing import Dict, List, Any, Optional
from datadog import initialize, api, statsd
from datadog.api.dashboards import Dashboards
from datadog.api.monitors import Monitors
from datadog.api.service_level_objectives import ServiceLevelObjectives
from datetime import datetime, timedelta
import asyncio
import time
import json
from concurrent.futures import ThreadPoolExecutor
import psutil
import subprocess
import requests
import boto3

# Initialize DataDog
initialize(
    api_key="<your-api-key>",
    app_key="<your-app-key>",
    host_name="enterprise-data-platform"
)

logger = logging.getLogger(__name__)

class DataDogCustomDashboardManager:
    """Comprehensive custom dashboard and metrics manager for enterprise data platform"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.executor = ThreadPoolExecutor(max_workers=10)
        
        # Dashboard configuration
        self.dashboard_templates = self._load_dashboard_templates()
        self.custom_metrics_interval = config.get('custom_metrics_interval', 60)
        
        # Business KPIs configuration
        self.business_kpis = config.get('business_kpis', {})
        
        # Cost monitoring
        self.cost_monitoring_enabled = config.get('cost_monitoring_enabled', True)
        self.aws_cost_explorer = None
        if self.cost_monitoring_enabled:
            try:
                self.aws_cost_explorer = boto3.client('ce', region_name='us-east-1')
            except:
                logger.warning("AWS Cost Explorer not available")
        
        # Capacity planning
        self.capacity_thresholds = config.get('capacity_thresholds', {
            'cpu_utilization': 80,
            'memory_utilization': 85,
            'disk_utilization': 90,
            'network_utilization': 75
        })
        
        # Performance benchmarks
        self.performance_benchmarks = config.get('performance_benchmarks', {})
        
    def _load_dashboard_templates(self) -> Dict[str, Any]:
        """Load dashboard templates"""
        return {
            'infrastructure_overview': self._create_infrastructure_overview_template(),
            'data_platform_operations': self._create_data_platform_operations_template(),
            'security_compliance': self._create_security_compliance_template(),
            'business_metrics': self._create_business_metrics_template(),
            'cost_optimization': self._create_cost_optimization_template(),
            'capacity_planning': self._create_capacity_planning_template(),
            'executive_summary': self._create_executive_summary_template()
        }
    
    def _create_infrastructure_overview_template(self) -> Dict[str, Any]:
        """Create infrastructure overview dashboard template"""
        return {
            "title": "Enterprise Data Platform - Infrastructure Overview",
            "description": "Comprehensive infrastructure monitoring dashboard",
            "layout_type": "ordered",
            "template_variables": [
                {
                    "name": "environment",
                    "default": "*",
                    "prefix": "environment"
                },
                {
                    "name": "service",
                    "default": "*",
                    "prefix": "service"
                }
            ],
            "widgets": [
                {
                    "id": 1,
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:system.cpu.user{environment:$environment} by {host}",
                                "display_type": "line",
                                "style": {
                                    "palette": "dog_classic",
                                    "line_type": "solid",
                                    "line_width": "normal"
                                }
                            }
                        ],
                        "title": "CPU Utilization by Host",
                        "title_size": "16",
                        "title_align": "left",
                        "yaxis": {
                            "min": "0",
                            "max": "100"
                        }
                    }
                },
                {
                    "id": 2,
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:system.mem.pct_usable{environment:$environment} by {host}",
                                "display_type": "line",
                                "style": {
                                    "palette": "warm",
                                    "line_type": "solid",
                                    "line_width": "normal"
                                }
                            }
                        ],
                        "title": "Memory Utilization by Host",
                        "title_size": "16",
                        "title_align": "left"
                    }
                },
                {
                    "id": 3,
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "sum:kubernetes.pods.running{environment:$environment}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Running Pods",
                        "title_size": "16",
                        "title_align": "center",
                        "autoscale": True,
                        "precision": 0
                    }
                },
                {
                    "id": 4,
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "sum:docker.containers.running{environment:$environment}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Running Containers",
                        "title_size": "16",
                        "title_align": "center",
                        "autoscale": True,
                        "precision": 0
                    }
                }
            ]
        }
    
    def _create_data_platform_operations_template(self) -> Dict[str, Any]:
        """Create data platform operations dashboard template"""
        return {
            "title": "Enterprise Data Platform - Operations",
            "description": "Data platform operational metrics and performance",
            "layout_type": "ordered",
            "template_variables": [
                {
                    "name": "environment",
                    "default": "production",
                    "prefix": "environment"
                }
            ],
            "widgets": [
                {
                    "id": 1,
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "sum:etl.pipeline.records_processed{environment:$environment} by {pipeline}",
                                "display_type": "bars",
                                "style": {
                                    "palette": "dog_classic"
                                }
                            }
                        ],
                        "title": "ETL Records Processed",
                        "title_size": "16"
                    }
                },
                {
                    "id": 2,
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:postgresql.connections.active{environment:$environment} by {database}",
                                "display_type": "line"
                            },
                            {
                                "q": "avg:redis.connections.connected_clients{environment:$environment} by {redis}",
                                "display_type": "line"
                            }
                        ],
                        "title": "Database Connections",
                        "title_size": "16"
                    }
                },
                {
                    "id": 3,
                    "definition": {
                        "type": "heatmap",
                        "requests": [
                            {
                                "q": "avg:api.response_time{environment:$environment} by {endpoint}",
                                "style": {
                                    "palette": "dog_classic"
                                }
                            }
                        ],
                        "title": "API Response Times",
                        "title_size": "16"
                    }
                },
                {
                    "id": 4,
                    "definition": {
                        "type": "query_table",
                        "requests": [
                            {
                                "q": "top(sum:kafka.consumer_group.lag{environment:$environment} by {topic,group}, 10, 'mean', 'desc')",
                                "aggregator": "avg",
                                "limit": 10
                            }
                        ],
                        "title": "Top Kafka Consumer Lag",
                        "title_size": "16"
                    }
                }
            ]
        }
    
    def _create_security_compliance_template(self) -> Dict[str, Any]:
        """Create security and compliance dashboard template"""
        return {
            "title": "Enterprise Data Platform - Security & Compliance",
            "description": "Security events and compliance monitoring",
            "layout_type": "ordered",
            "widgets": [
                {
                    "id": 1,
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "sum:security.authentication.failed_logins{*}",
                                "display_type": "bars",
                                "style": {
                                    "palette": "warm"
                                }
                            }
                        ],
                        "title": "Failed Login Attempts",
                        "title_size": "16"
                    }
                },
                {
                    "id": 2,
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "sum:security.vulnerabilities.critical{*}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Critical Vulnerabilities",
                        "title_size": "16",
                        "title_align": "center",
                        "autoscale": True,
                        "conditional_formats": [
                            {
                                "comparator": ">",
                                "value": 0,
                                "palette": "red_on_white"
                            }
                        ]
                    }
                },
                {
                    "id": 3,
                    "definition": {
                        "type": "sunburst",
                        "requests": [
                            {
                                "q": "sum:compliance.control.violations{*} by {framework,control}"
                            }
                        ],
                        "title": "Compliance Violations by Framework",
                        "title_size": "16"
                    }
                },
                {
                    "id": 4,
                    "definition": {
                        "type": "geomap",
                        "requests": [
                            {
                                "q": "sum:security.network.suspicious_connections{*} by {country}"
                            }
                        ],
                        "title": "Suspicious Connections by Country",
                        "title_size": "16"
                    }
                }
            ]
        }
    
    def _create_business_metrics_template(self) -> Dict[str, Any]:
        """Create business metrics dashboard template"""
        return {
            "title": "Enterprise Data Platform - Business Metrics",
            "description": "Business KPIs and performance indicators",
            "layout_type": "ordered",
            "widgets": [
                {
                    "id": 1,
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "sum:business.revenue.daily{*}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Daily Revenue",
                        "title_size": "16",
                        "title_align": "center",
                        "autoscale": True,
                        "custom_unit": "$"
                    }
                },
                {
                    "id": 2,
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "sum:business.users.active{*}",
                                "display_type": "line"
                            }
                        ],
                        "title": "Active Users",
                        "title_size": "16"
                    }
                },
                {
                    "id": 3,
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "avg:business.data_quality.score{*}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Data Quality Score",
                        "title_size": "16",
                        "title_align": "center",
                        "autoscale": True,
                        "custom_unit": "%",
                        "conditional_formats": [
                            {
                                "comparator": ">=",
                                "value": 95,
                                "palette": "green_on_white"
                            },
                            {
                                "comparator": "<",
                                "value": 90,
                                "palette": "red_on_white"
                            }
                        ]
                    }
                }
            ]
        }
    
    def _create_cost_optimization_template(self) -> Dict[str, Any]:
        """Create cost optimization dashboard template"""
        return {
            "title": "Enterprise Data Platform - Cost Optimization",
            "description": "Cost monitoring and optimization metrics",
            "layout_type": "ordered",
            "widgets": [
                {
                    "id": 1,
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "sum:aws.billing.estimated_charges{*} by {service_name}",
                                "display_type": "area"
                            }
                        ],
                        "title": "AWS Costs by Service",
                        "title_size": "16"
                    }
                },
                {
                    "id": 2,
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "sum:infrastructure.cost.monthly{*}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Monthly Infrastructure Cost",
                        "title_size": "16",
                        "title_align": "center",
                        "autoscale": True,
                        "custom_unit": "$"
                    }
                },
                {
                    "id": 3,
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "avg:infrastructure.efficiency.cost_per_transaction{*}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Cost per Transaction",
                        "title_size": "16",
                        "title_align": "center",
                        "autoscale": True,
                        "custom_unit": "$"
                    }
                }
            ]
        }
    
    def _create_capacity_planning_template(self) -> Dict[str, Any]:
        """Create capacity planning dashboard template"""
        return {
            "title": "Enterprise Data Platform - Capacity Planning",
            "description": "Resource utilization and capacity planning metrics",
            "layout_type": "ordered",
            "widgets": [
                {
                    "id": 1,
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:system.cpu.user{*}",
                                "display_type": "line"
                            }
                        ],
                        "title": "CPU Utilization Trend",
                        "title_size": "16",
                        "markers": [
                            {
                                "value": f"y = {self.capacity_thresholds['cpu_utilization']}",
                                "display_type": "error dashed"
                            }
                        ]
                    }
                },
                {
                    "id": 2,
                    "definition": {
                        "type": "forecast",
                        "requests": [
                            {
                                "q": "avg:kubernetes.memory.usage{*}",
                                "display_type": "line"
                            }
                        ],
                        "title": "Memory Usage Forecast",
                        "title_size": "16"
                    }
                },
                {
                    "id": 3,
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "avg:infrastructure.capacity.days_until_full{*}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Days Until Capacity Full",
                        "title_size": "16",
                        "title_align": "center",
                        "autoscale": True,
                        "conditional_formats": [
                            {
                                "comparator": "<=",
                                "value": 30,
                                "palette": "red_on_white"
                            },
                            {
                                "comparator": "<=",
                                "value": 60,
                                "palette": "yellow_on_white"
                            }
                        ]
                    }
                }
            ]
        }
    
    def _create_executive_summary_template(self) -> Dict[str, Any]:
        """Create executive summary dashboard template"""
        return {
            "title": "Enterprise Data Platform - Executive Summary",
            "description": "High-level metrics and KPIs for executives",
            "layout_type": "ordered",
            "widgets": [
                {
                    "id": 1,
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "avg:platform.availability{*}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Platform Availability",
                        "title_size": "20",
                        "title_align": "center",
                        "autoscale": True,
                        "custom_unit": "%",
                        "conditional_formats": [
                            {
                                "comparator": ">=",
                                "value": 99.9,
                                "palette": "green_on_white"
                            }
                        ]
                    }
                },
                {
                    "id": 2,
                    "definition": {
                        "type": "slo",
                        "title": "Data Processing SLA",
                        "title_size": "16",
                        "slo_id": "data_processing_sla",
                        "show_error_budget": True,
                        "view_type": "detail"
                    }
                },
                {
                    "id": 3,
                    "definition": {
                        "type": "note",
                        "content": "## Platform Health Summary\n\n- **Uptime**: 99.95%\n- **Data Quality**: 98.2%\n- **Security Score**: 95%\n- **Cost Efficiency**: +12% vs target",
                        "background_color": "gray",
                        "font_size": "14",
                        "text_align": "left",
                        "show_tick": True,
                        "tick_pos": "50%",
                        "tick_edge": "left"
                    }
                }
            ]
        }
    
    async def create_dashboards(self) -> Dict[str, str]:
        """Create all custom dashboards"""
        try:
            created_dashboards = {}
            
            for dashboard_name, template in self.dashboard_templates.items():
                try:
                    response = Dashboards.create(template)
                    dashboard_id = response['id']
                    dashboard_url = response['url']
                    
                    created_dashboards[dashboard_name] = {
                        'id': dashboard_id,
                        'url': dashboard_url
                    }
                    
                    logger.info(f"Created dashboard: {dashboard_name} (ID: {dashboard_id})")
                    
                except Exception as dashboard_error:
                    logger.error(f"Error creating dashboard {dashboard_name}: {dashboard_error}")
                    continue
            
            return created_dashboards
            
        except Exception as e:
            logger.error(f"Error creating dashboards: {e}")
            return {}
    
    async def collect_custom_infrastructure_metrics(self) -> Dict[str, Any]:
        """Collect custom infrastructure metrics"""
        try:
            metrics = {}
            
            # Platform availability
            availability = await self._calculate_platform_availability()
            metrics['platform_availability'] = availability
            
            # Cost metrics
            if self.cost_monitoring_enabled:
                cost_metrics = await self._collect_cost_metrics()
                metrics.update(cost_metrics)
            
            # Capacity planning metrics
            capacity_metrics = await self._collect_capacity_metrics()
            metrics.update(capacity_metrics)
            
            # Performance benchmarks
            performance_metrics = await self._collect_performance_metrics()
            metrics.update(performance_metrics)
            
            # Business KPIs
            business_metrics = await self._collect_business_kpis()
            metrics.update(business_metrics)
            
            # Send all custom metrics
            await self._send_custom_metrics(metrics)
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error collecting custom infrastructure metrics: {e}")
            return {}
    
    async def _calculate_platform_availability(self) -> float:
        """Calculate overall platform availability"""
        try:
            # Query service health from various components
            services_to_check = [
                'kubernetes.nodes.ready',
                'postgresql.connections.active',
                'redis.connections.connected_clients',
                'elasticsearch.cluster.status'
            ]
            
            # This would typically query your monitoring system
            # For now, we'll simulate based on current system health
            healthy_services = 0
            total_services = len(services_to_check)
            
            # Check if basic services are responding
            try:
                # Check if we can connect to basic system resources
                psutil.cpu_percent()
                psutil.virtual_memory()
                healthy_services += 1
            except:
                pass
            
            # Simulate additional service checks
            healthy_services = total_services  # Assume all healthy for demo
            
            availability = (healthy_services / total_services) * 100
            return min(availability, 100.0)
            
        except Exception as e:
            logger.warning(f"Error calculating platform availability: {e}")
            return 0.0
    
    async def _collect_cost_metrics(self) -> Dict[str, Any]:
        """Collect infrastructure cost metrics"""
        try:
            cost_metrics = {}
            
            if self.aws_cost_explorer:
                # Get AWS costs for the current month
                today = datetime.now()
                start_date = today.replace(day=1).strftime('%Y-%m-%d')
                end_date = today.strftime('%Y-%m-%d')
                
                try:
                    response = self.aws_cost_explorer.get_cost_and_usage(
                        TimePeriod={
                            'Start': start_date,
                            'End': end_date
                        },
                        Granularity='MONTHLY',
                        Metrics=['BlendedCost'],
                        GroupBy=[
                            {
                                'Type': 'DIMENSION',
                                'Key': 'SERVICE'
                            }
                        ]
                    )
                    
                    total_cost = 0
                    service_costs = {}
                    
                    for result in response['ResultsByTime']:
                        for group in result['Groups']:
                            service = group['Keys'][0]
                            cost = float(group['Metrics']['BlendedCost']['Amount'])
                            service_costs[service] = cost
                            total_cost += cost
                    
                    cost_metrics.update({
                        'monthly_cost': total_cost,
                        'service_costs': service_costs
                    })
                    
                except Exception as aws_error:
                    logger.warning(f"Error fetching AWS costs: {aws_error}")
            
            # Calculate cost efficiency metrics
            cost_metrics['cost_per_transaction'] = await self._calculate_cost_per_transaction()
            cost_metrics['cost_trend'] = await self._calculate_cost_trend()
            
            return cost_metrics
            
        except Exception as e:
            logger.warning(f"Error collecting cost metrics: {e}")
            return {}
    
    async def _collect_capacity_metrics(self) -> Dict[str, Any]:
        """Collect capacity planning metrics"""
        try:
            capacity_metrics = {}
            
            # CPU capacity
            cpu_usage = psutil.cpu_percent(interval=1)
            capacity_metrics['cpu_utilization'] = cpu_usage
            
            # Memory capacity
            memory = psutil.virtual_memory()
            capacity_metrics['memory_utilization'] = memory.percent
            
            # Disk capacity
            disk_usage = psutil.disk_usage('/')
            disk_utilization = (disk_usage.used / disk_usage.total) * 100
            capacity_metrics['disk_utilization'] = disk_utilization
            
            # Network capacity (simplified)
            network_stats = psutil.net_io_counters()
            capacity_metrics['network_bytes_sent'] = network_stats.bytes_sent
            capacity_metrics['network_bytes_recv'] = network_stats.bytes_recv
            
            # Predict days until capacity is full
            capacity_metrics['days_until_cpu_full'] = await self._predict_capacity_exhaustion('cpu', cpu_usage)
            capacity_metrics['days_until_memory_full'] = await self._predict_capacity_exhaustion('memory', memory.percent)
            capacity_metrics['days_until_disk_full'] = await self._predict_capacity_exhaustion('disk', disk_utilization)
            
            return capacity_metrics
            
        except Exception as e:
            logger.warning(f"Error collecting capacity metrics: {e}")
            return {}
    
    async def _collect_performance_metrics(self) -> Dict[str, Any]:
        """Collect performance benchmark metrics"""
        try:
            performance_metrics = {}
            
            # System performance
            boot_time = psutil.boot_time()
            uptime_seconds = time.time() - boot_time
            performance_metrics['uptime_hours'] = uptime_seconds / 3600
            
            # Process performance
            process_count = len(psutil.pids())
            performance_metrics['process_count'] = process_count
            
            # Load average (Linux/Unix)
            try:
                load_avg = os.getloadavg()
                performance_metrics['load_average_1m'] = load_avg[0]
                performance_metrics['load_average_5m'] = load_avg[1]
                performance_metrics['load_average_15m'] = load_avg[2]
            except:
                pass
            
            # Benchmark scores (simulated)
            performance_metrics['performance_score'] = await self._calculate_performance_score()
            
            return performance_metrics
            
        except Exception as e:
            logger.warning(f"Error collecting performance metrics: {e}")
            return {}
    
    async def _collect_business_kpis(self) -> Dict[str, Any]:
        """Collect business KPI metrics"""
        try:
            business_metrics = {}
            
            # Simulate business metrics (replace with actual business logic)
            business_metrics.update({
                'daily_revenue': 125000.00,
                'active_users': 15420,
                'data_quality_score': 98.2,
                'customer_satisfaction': 4.7,
                'processing_efficiency': 94.5
            })
            
            # Calculate derived KPIs
            business_metrics['revenue_per_user'] = business_metrics['daily_revenue'] / business_metrics['active_users']
            
            return business_metrics
            
        except Exception as e:
            logger.warning(f"Error collecting business KPIs: {e}")
            return {}
    
    async def _calculate_cost_per_transaction(self) -> float:
        """Calculate cost per transaction"""
        try:
            # This would typically query your transaction volume and costs
            # For now, we'll simulate
            daily_transactions = 1000000  # 1M transactions per day
            daily_cost = 500.00  # $500 per day infrastructure cost
            
            return daily_cost / daily_transactions
            
        except Exception as e:
            logger.warning(f"Error calculating cost per transaction: {e}")
            return 0.0
    
    async def _calculate_cost_trend(self) -> float:
        """Calculate cost trend percentage"""
        try:
            # Compare current month to previous month
            # For now, simulate a 5% increase
            return 5.0
            
        except Exception as e:
            logger.warning(f"Error calculating cost trend: {e}")
            return 0.0
    
    async def _predict_capacity_exhaustion(self, resource_type: str, current_utilization: float) -> int:
        """Predict days until resource capacity is exhausted"""
        try:
            threshold = self.capacity_thresholds.get(f'{resource_type}_utilization', 90)
            
            # Simple linear prediction (in real implementation, use time series analysis)
            if current_utilization >= threshold:
                return 0  # Already at capacity
            
            # Assume 1% growth per week (simplified)
            growth_rate_per_day = 0.14  # ~1% per week
            remaining_capacity = threshold - current_utilization
            
            if growth_rate_per_day <= 0:
                return 999  # No growth
            
            days_until_full = remaining_capacity / growth_rate_per_day
            return max(0, int(days_until_full))
            
        except Exception as e:
            logger.warning(f"Error predicting capacity exhaustion for {resource_type}: {e}")
            return 999
    
    async def _calculate_performance_score(self) -> float:
        """Calculate overall performance score"""
        try:
            # Composite score based on various factors
            cpu_usage = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            
            # Score components (0-100)
            cpu_score = max(0, 100 - cpu_usage)
            memory_score = max(0, 100 - memory.percent)
            
            # Weighted average
            performance_score = (cpu_score * 0.5 + memory_score * 0.5)
            
            return round(performance_score, 1)
            
        except Exception as e:
            logger.warning(f"Error calculating performance score: {e}")
            return 0.0
    
    async def _send_custom_metrics(self, metrics: Dict[str, Any]):
        """Send custom metrics to DataDog"""
        try:
            base_tags = ['scope:custom_infrastructure']
            
            # Platform metrics
            if 'platform_availability' in metrics:
                statsd.gauge('platform.availability', metrics['platform_availability'], tags=base_tags)
            
            # Cost metrics
            if 'monthly_cost' in metrics:
                statsd.gauge('infrastructure.cost.monthly', metrics['monthly_cost'], tags=base_tags)
            if 'cost_per_transaction' in metrics:
                statsd.gauge('infrastructure.efficiency.cost_per_transaction', metrics['cost_per_transaction'], tags=base_tags)
            if 'cost_trend' in metrics:
                statsd.gauge('infrastructure.cost.trend_percent', metrics['cost_trend'], tags=base_tags)
            
            # Capacity metrics
            for metric_name in ['cpu_utilization', 'memory_utilization', 'disk_utilization']:
                if metric_name in metrics:
                    statsd.gauge(f'infrastructure.capacity.{metric_name}', metrics[metric_name], tags=base_tags)
            
            for days_metric in ['days_until_cpu_full', 'days_until_memory_full', 'days_until_disk_full']:
                if days_metric in metrics:
                    statsd.gauge(f'infrastructure.capacity.{days_metric}', metrics[days_metric], tags=base_tags)
            
            # Performance metrics
            if 'performance_score' in metrics:
                statsd.gauge('infrastructure.performance.score', metrics['performance_score'], tags=base_tags)
            if 'uptime_hours' in metrics:
                statsd.gauge('infrastructure.uptime.hours', metrics['uptime_hours'], tags=base_tags)
            
            # Business KPIs
            business_tags = ['scope:business_kpis']
            for kpi_name in ['daily_revenue', 'active_users', 'data_quality_score', 'customer_satisfaction']:
                if kpi_name in metrics:
                    statsd.gauge(f'business.{kpi_name}', metrics[kpi_name], tags=business_tags)
            
        except Exception as e:
            logger.warning(f"Error sending custom metrics: {e}")
    
    async def create_custom_monitors(self) -> List[str]:
        """Create custom monitors for infrastructure"""
        try:
            monitor_ids = []
            
            # Infrastructure availability monitor
            availability_monitor = {
                "type": "metric alert",
                "query": "avg(last_5m):avg:platform.availability{*} < 99",
                "name": "Platform Availability Alert",
                "message": "Platform availability has dropped below 99%",
                "tags": ["team:platform", "severity:critical"],
                "options": {
                    "thresholds": {
                        "critical": 99,
                        "warning": 99.5
                    },
                    "notify_audit": False,
                    "require_full_window": True,
                    "notify_no_data": True,
                    "no_data_timeframe": 10
                }
            }
            
            response = Monitors.create(**availability_monitor)
            monitor_ids.append(response['id'])
            
            # Cost anomaly monitor
            cost_monitor = {
                "type": "metric alert",
                "query": "avg(last_1h):anomalies(avg:infrastructure.cost.monthly{*}, 'basic', 2, direction='above', alert_window='last_15m', interval=60, count_default_zero='true') >= 1",
                "name": "Infrastructure Cost Anomaly",
                "message": "Unusual spike in infrastructure costs detected",
                "tags": ["team:finance", "severity:warning"],
                "options": {
                    "thresholds": {
                        "critical": 1,
                        "critical_recovery": 0
                    },
                    "notify_audit": False,
                    "require_full_window": False
                }
            }
            
            response = Monitors.create(**cost_monitor)
            monitor_ids.append(response['id'])
            
            # Capacity planning monitor
            capacity_monitor = {
                "type": "metric alert",
                "query": "avg(last_15m):avg:infrastructure.capacity.days_until_disk_full{*} < 30",
                "name": "Capacity Planning Alert",
                "message": "Disk capacity will be exhausted within 30 days",
                "tags": ["team:infrastructure", "severity:warning"],
                "options": {
                    "thresholds": {
                        "critical": 7,
                        "warning": 30
                    },
                    "notify_audit": False,
                    "require_full_window": True
                }
            }
            
            response = Monitors.create(**capacity_monitor)
            monitor_ids.append(response['id'])
            
            logger.info(f"Created {len(monitor_ids)} custom monitors")
            return monitor_ids
            
        except Exception as e:
            logger.error(f"Error creating custom monitors: {e}")
            return []
    
    async def run_custom_metrics_collection(self):
        """Run continuous custom metrics collection"""
        logger.info("Starting custom metrics collection loop")
        
        while True:
            try:
                # Collect and send custom metrics
                metrics = await self.collect_custom_infrastructure_metrics()
                
                logger.info(f"Custom metrics collection completed: "
                           f"Platform availability: {metrics.get('platform_availability', 0):.1f}%, "
                           f"Performance score: {metrics.get('performance_score', 0):.1f}")
                
                # Send health check metric
                statsd.gauge('infrastructure.monitoring.health', 1, tags=['status:healthy'])
                
                # Wait for next collection interval
                await asyncio.sleep(self.custom_metrics_interval)
                
            except Exception as e:
                logger.error(f"Error in custom metrics collection loop: {e}")
                statsd.increment('infrastructure.monitoring.loop_error')
                await asyncio.sleep(60)  # Wait 1 minute before retrying

# Configuration example
CUSTOM_DASHBOARD_CONFIG = {
    'custom_metrics_interval': 60,  # seconds
    'cost_monitoring_enabled': True,
    'business_kpis': {
        'revenue_target': 150000,
        'user_growth_target': 0.05,  # 5% monthly growth
        'quality_target': 99.0
    },
    'capacity_thresholds': {
        'cpu_utilization': 80,
        'memory_utilization': 85,
        'disk_utilization': 90,
        'network_utilization': 75
    },
    'performance_benchmarks': {
        'api_response_time_p95': 200,  # ms
        'etl_processing_time': 3600,   # seconds
        'data_freshness_minutes': 15
    }
}

async def main():
    """Main entry point for custom dashboard management"""
    dashboard_manager = DataDogCustomDashboardManager(CUSTOM_DASHBOARD_CONFIG)
    
    # Create dashboards
    dashboards = await dashboard_manager.create_dashboards()
    logger.info(f"Created dashboards: {list(dashboards.keys())}")
    
    # Create monitors
    monitors = await dashboard_manager.create_custom_monitors()
    logger.info(f"Created {len(monitors)} monitors")
    
    # Start metrics collection
    await dashboard_manager.run_custom_metrics_collection()

if __name__ == "__main__":
    asyncio.run(main())
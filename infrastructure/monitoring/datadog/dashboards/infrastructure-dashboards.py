#!/usr/bin/env python3
"""
DataDog Custom Infrastructure Dashboards
Creates comprehensive dashboards for enterprise data platform monitoring
"""

import json
import logging
from typing import Dict, List, Any
from datadog import api, initialize

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InfrastructureDashboards:
    """Create and manage DataDog infrastructure dashboards"""
    
    def __init__(self, api_key: str, app_key: str, site: str = 'datadoghq.com'):
        """Initialize dashboard manager"""
        initialize(api_key=api_key, app_key=app_key, api_host=f'https://api.{site}')
        
    def create_kubernetes_dashboard(self) -> Dict[str, Any]:
        """Create comprehensive Kubernetes monitoring dashboard"""
        dashboard = {
            "title": "Enterprise Data Platform - Kubernetes Infrastructure",
            "description": "Comprehensive Kubernetes cluster monitoring for the enterprise data platform",
            "widgets": [
                # Cluster Overview
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "sum:kubernetes.nodes.ready{cluster_name:enterprise-data-platform}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Ready Nodes",
                        "title_size": "16",
                        "title_align": "left",
                        "precision": 0
                    },
                    "layout": {"x": 0, "y": 0, "width": 2, "height": 2}
                },
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "sum:kubernetes.pods.running{cluster_name:enterprise-data-platform}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Running Pods",
                        "title_size": "16",
                        "title_align": "left",
                        "precision": 0
                    },
                    "layout": {"x": 2, "y": 0, "width": 2, "height": 2}
                },
                # Node Resource Utilization
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:kubernetes.cpu.usage.total{cluster_name:enterprise-data-platform} by {host}",
                                "display_type": "line",
                                "style": {"palette": "dog_classic", "line_type": "solid", "line_width": "normal"}
                            }
                        ],
                        "title": "CPU Usage by Node",
                        "title_size": "16",
                        "title_align": "left",
                        "show_legend": True,
                        "legend_size": "0"
                    },
                    "layout": {"x": 0, "y": 2, "width": 6, "height": 3}
                },
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:kubernetes.memory.usage{cluster_name:enterprise-data-platform} by {host}",
                                "display_type": "line",
                                "style": {"palette": "cool", "line_type": "solid", "line_width": "normal"}
                            }
                        ],
                        "title": "Memory Usage by Node",
                        "title_size": "16",
                        "title_align": "left",
                        "show_legend": True
                    },
                    "layout": {"x": 6, "y": 2, "width": 6, "height": 3}
                },
                # Pod Status Distribution
                {
                    "definition": {
                        "type": "query_table",
                        "requests": [
                            {
                                "q": "sum:kubernetes.pods.running{cluster_name:enterprise-data-platform} by {namespace}",
                                "aggregator": "last",
                                "alias": "Running"
                            },
                            {
                                "q": "sum:kubernetes.pods.pending{cluster_name:enterprise-data-platform} by {namespace}",
                                "aggregator": "last", 
                                "alias": "Pending"
                            },
                            {
                                "q": "sum:kubernetes.pods.failed{cluster_name:enterprise-data-platform} by {namespace}",
                                "aggregator": "last",
                                "alias": "Failed"
                            }
                        ],
                        "title": "Pod Status by Namespace",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 0, "y": 5, "width": 6, "height": 4}
                },
                # Network Traffic
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:kubernetes.network.rx_bytes{cluster_name:enterprise-data-platform} by {pod_name}",
                                "display_type": "line"
                            },
                            {
                                "q": "avg:kubernetes.network.tx_bytes{cluster_name:enterprise-data-platform} by {pod_name}",
                                "display_type": "line"
                            }
                        ],
                        "title": "Network I/O by Pod",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 6, "y": 5, "width": 6, "height": 4}
                }
            ],
            "layout_type": "ordered",
            "is_read_only": False,
            "notify_list": [],
            "template_variables": [
                {
                    "name": "cluster",
                    "prefix": "cluster_name",
                    "default": "enterprise-data-platform"
                },
                {
                    "name": "namespace",
                    "prefix": "kube_namespace",
                    "default": "*"
                }
            ]
        }
        
        try:
            result = api.Dashboard.create(**dashboard)
            logger.info(f"Created Kubernetes dashboard: {result['url']}")
            return result
        except Exception as e:
            logger.error(f"Error creating Kubernetes dashboard: {e}")
            raise
    
    def create_database_dashboard(self) -> Dict[str, Any]:
        """Create database monitoring dashboard"""
        dashboard = {
            "title": "Enterprise Data Platform - Database Infrastructure",
            "description": "Comprehensive database monitoring for PostgreSQL, Redis, and Elasticsearch",
            "widgets": [
                # PostgreSQL Metrics
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "avg:postgresql.connections.active{service:postgresql,env:production}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "PostgreSQL Active Connections",
                        "title_size": "16",
                        "title_align": "left",
                        "precision": 0
                    },
                    "layout": {"x": 0, "y": 0, "width": 2, "height": 2}
                },
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:postgresql.database.size{service:postgresql,env:production} by {db}",
                                "display_type": "line"
                            }
                        ],
                        "title": "PostgreSQL Database Sizes",
                        "title_size": "16",
                        "title_align": "left",
                        "show_legend": True
                    },
                    "layout": {"x": 0, "y": 2, "width": 6, "height": 3}
                },
                # Redis Metrics
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "avg:redis.clients.connected{service:redis,env:production}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Redis Connected Clients",
                        "title_size": "16",
                        "title_align": "left",
                        "precision": 0
                    },
                    "layout": {"x": 2, "y": 0, "width": 2, "height": 2}
                },
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:redis.mem.used{service:redis,env:production} by {redis_host}",
                                "display_type": "line"
                            }
                        ],
                        "title": "Redis Memory Usage",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 6, "y": 2, "width": 6, "height": 3}
                },
                # Elasticsearch Metrics
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "avg:elasticsearch.cluster.status{service:elasticsearch,env:production}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Elasticsearch Cluster Status",
                        "title_size": "16",
                        "title_align": "left",
                        "precision": 0
                    },
                    "layout": {"x": 4, "y": 0, "width": 2, "height": 2}
                },
                # Query Performance
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:postgresql.slow_queries{service:postgresql,env:production}",
                                "display_type": "bars",
                                "style": {"palette": "warm"}
                            }
                        ],
                        "title": "PostgreSQL Slow Queries",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 0, "y": 5, "width": 4, "height": 3}
                },
                {
                    "definition": {
                        "type": "timeseries", 
                        "requests": [
                            {
                                "q": "avg:elasticsearch.search.query.time{service:elasticsearch,env:production}",
                                "display_type": "line"
                            }
                        ],
                        "title": "Elasticsearch Query Time",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 4, "y": 5, "width": 4, "height": 3}
                },
                # Database Health Summary
                {
                    "definition": {
                        "type": "query_table",
                        "requests": [
                            {
                                "q": "avg:postgresql.connections.active{service:postgresql,env:production} by {host}",
                                "aggregator": "last",
                                "alias": "PostgreSQL Connections"
                            },
                            {
                                "q": "avg:redis.clients.connected{service:redis,env:production} by {host}",
                                "aggregator": "last",
                                "alias": "Redis Clients"
                            },
                            {
                                "q": "avg:elasticsearch.cluster.number_of_nodes{service:elasticsearch,env:production} by {host}",
                                "aggregator": "last",
                                "alias": "ES Nodes"
                            }
                        ],
                        "title": "Database Health Summary",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 8, "y": 5, "width": 4, "height": 3}
                }
            ],
            "layout_type": "ordered",
            "template_variables": [
                {
                    "name": "environment",
                    "prefix": "env",
                    "default": "production"
                }
            ]
        }
        
        try:
            result = api.Dashboard.create(**dashboard)
            logger.info(f"Created Database dashboard: {result['url']}")
            return result
        except Exception as e:
            logger.error(f"Error creating Database dashboard: {e}")
            raise
    
    def create_messaging_dashboard(self) -> Dict[str, Any]:
        """Create message queue monitoring dashboard"""
        dashboard = {
            "title": "Enterprise Data Platform - Messaging Infrastructure", 
            "description": "Comprehensive monitoring for RabbitMQ and Kafka message queues",
            "widgets": [
                # RabbitMQ Overview
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "sum:rabbitmq.queue.messages{service:rabbitmq,env:production}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "RabbitMQ Total Messages",
                        "title_size": "16",
                        "title_align": "left",
                        "precision": 0
                    },
                    "layout": {"x": 0, "y": 0, "width": 2, "height": 2}
                },
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:rabbitmq.queue.messages.rate{service:rabbitmq,env:production} by {queue}",
                                "display_type": "line"
                            }
                        ],
                        "title": "RabbitMQ Message Rate by Queue",
                        "title_size": "16",
                        "title_align": "left",
                        "show_legend": True
                    },
                    "layout": {"x": 0, "y": 2, "width": 6, "height": 3}
                },
                # Kafka Overview
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "sum:kafka.broker.messages_in_per_sec{service:kafka,env:production}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Kafka Messages/sec",
                        "title_size": "16",
                        "title_align": "left",
                        "precision": 0
                    },
                    "layout": {"x": 2, "y": 0, "width": 2, "height": 2}
                },
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:kafka.broker.bytes_in_per_sec{service:kafka,env:production} by {broker}",
                                "display_type": "line"
                            }
                        ],
                        "title": "Kafka Bytes In by Broker",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 6, "y": 2, "width": 6, "height": 3}
                },
                # Queue Depth Monitoring
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:rabbitmq.queue.messages{service:rabbitmq,env:production} by {queue}",
                                "display_type": "area"
                            }
                        ],
                        "title": "RabbitMQ Queue Depth",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 0, "y": 5, "width": 6, "height": 3}
                },
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:kafka.log.partition_size{service:kafka,env:production} by {topic}",
                                "display_type": "area"
                            }
                        ],
                        "title": "Kafka Topic Partition Sizes",
                        "title_size": "16", 
                        "title_align": "left"
                    },
                    "layout": {"x": 6, "y": 5, "width": 6, "height": 3}
                },
                # Consumer Lag
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:kafka.consumer.lag{service:kafka,env:production} by {consumer_group}",
                                "display_type": "line",
                                "style": {"palette": "orange"}
                            }
                        ],
                        "title": "Kafka Consumer Lag",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 0, "y": 8, "width": 12, "height": 3}
                }
            ],
            "layout_type": "ordered",
            "template_variables": [
                {
                    "name": "service",
                    "prefix": "service",
                    "default": "*"
                }
            ]
        }
        
        try:
            result = api.Dashboard.create(**dashboard)
            logger.info(f"Created Messaging dashboard: {result['url']}")
            return result
        except Exception as e:
            logger.error(f"Error creating Messaging dashboard: {e}")
            raise
    
    def create_security_dashboard(self) -> Dict[str, Any]:
        """Create security monitoring dashboard"""
        dashboard = {
            "title": "Enterprise Data Platform - Security Monitoring",
            "description": "Comprehensive security monitoring and threat detection dashboard",
            "widgets": [
                # Security Events Overview
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "sum:security.events.count{severity:HIGH,env:production}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "High Severity Security Events",
                        "title_size": "16",
                        "title_align": "left",
                        "precision": 0
                    },
                    "layout": {"x": 0, "y": 0, "width": 2, "height": 2}
                },
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "sum:security.events.count{env:production} by {event_type}",
                                "display_type": "bars"
                            }
                        ],
                        "title": "Security Events by Type",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 0, "y": 2, "width": 6, "height": 3}
                },
                # Container Security
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "avg:container.security.vulnerabilities.critical{env:production}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Critical Vulnerabilities",
                        "title_size": "16",
                        "title_align": "left",
                        "precision": 0
                    },
                    "layout": {"x": 2, "y": 0, "width": 2, "height": 2}
                },
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:container.security.score{env:production} by {container_name}",
                                "display_type": "line"
                            }
                        ],
                        "title": "Container Security Scores",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 6, "y": 2, "width": 6, "height": 3}
                },
                # Compliance Status
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "avg:security.compliance.percentage{framework:SOC2,env:production}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "SOC2 Compliance %",
                        "title_size": "16",
                        "title_align": "left",
                        "precision": 1
                    },
                    "layout": {"x": 4, "y": 0, "width": 2, "height": 2}
                },
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:security.compliance.percentage{env:production} by {framework}",
                                "display_type": "line"
                            }
                        ],
                        "title": "Compliance Status by Framework",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 0, "y": 5, "width": 12, "height": 3}
                },
                # Threat Intelligence
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "sum:security.threat_intelligence.indicators{env:production}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Threat Intel Indicators",
                        "title_size": "16",
                        "title_align": "left",
                        "precision": 0
                    },
                    "layout": {"x": 6, "y": 0, "width": 2, "height": 2}
                }
            ],
            "layout_type": "ordered",
            "template_variables": [
                {
                    "name": "severity",
                    "prefix": "severity",
                    "default": "*"
                }
            ]
        }
        
        try:
            result = api.Dashboard.create(**dashboard)
            logger.info(f"Created Security dashboard: {result['url']}")
            return result
        except Exception as e:
            logger.error(f"Error creating Security dashboard: {e}")
            raise
    
    def create_cost_optimization_dashboard(self) -> Dict[str, Any]:
        """Create cost optimization and capacity planning dashboard"""
        dashboard = {
            "title": "Enterprise Data Platform - Cost Optimization & Capacity Planning",
            "description": "Infrastructure cost optimization and capacity planning metrics",
            "widgets": [
                # Resource Utilization
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:system.cpu.usage{env:production} by {host}",
                                "display_type": "line"
                            }
                        ],
                        "title": "CPU Utilization by Host",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 0, "y": 0, "width": 6, "height": 3}
                },
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:system.mem.pct_usable{env:production} by {host}",
                                "display_type": "line"
                            }
                        ],
                        "title": "Memory Utilization by Host",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 6, "y": 0, "width": 6, "height": 3}
                },
                # Storage Growth
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:system.disk.used{env:production} by {device}",
                                "display_type": "area"
                            }
                        ],
                        "title": "Disk Usage Growth",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 0, "y": 3, "width": 6, "height": 3}
                },
                # Network Bandwidth
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "avg:system.net.bytes_sent{env:production}",
                                "display_type": "line"
                            },
                            {
                                "q": "avg:system.net.bytes_rcvd{env:production}",
                                "display_type": "line"
                            }
                        ],
                        "title": "Network Bandwidth Usage",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 6, "y": 3, "width": 6, "height": 3}
                },
                # Cost Metrics (Custom Business Metrics)
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "sum:infrastructure.cost.monthly{env:production}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Monthly Infrastructure Cost",
                        "title_size": "16",
                        "title_align": "left",
                        "precision": 2,
                        "custom_unit": "$"
                    },
                    "layout": {"x": 0, "y": 6, "width": 3, "height": 2}
                },
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [
                            {
                                "q": "avg:infrastructure.efficiency.score{env:production}",
                                "aggregator": "last"
                            }
                        ],
                        "title": "Resource Efficiency Score",
                        "title_size": "16",
                        "title_align": "left",
                        "precision": 1
                    },
                    "layout": {"x": 3, "y": 6, "width": 3, "height": 2}
                },
                # Capacity Planning
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [
                            {
                                "q": "forecast(avg:postgresql.database.size{env:production}, 'linear', 2592000)",
                                "display_type": "line",
                                "style": {"palette": "purple"}
                            }
                        ],
                        "title": "Database Growth Forecast (30 days)",
                        "title_size": "16",
                        "title_align": "left"
                    },
                    "layout": {"x": 6, "y": 6, "width": 6, "height": 2}
                }
            ],
            "layout_type": "ordered",
            "template_variables": [
                {
                    "name": "service",
                    "prefix": "service",
                    "default": "*"
                }
            ]
        }
        
        try:
            result = api.Dashboard.create(**dashboard)
            logger.info(f"Created Cost Optimization dashboard: {result['url']}")
            return result
        except Exception as e:
            logger.error(f"Error creating Cost Optimization dashboard: {e}")
            raise
    
    def create_all_dashboards(self) -> List[Dict[str, Any]]:
        """Create all infrastructure dashboards"""
        dashboards = []
        
        try:
            dashboards.append(self.create_kubernetes_dashboard())
            dashboards.append(self.create_database_dashboard())
            dashboards.append(self.create_messaging_dashboard())
            dashboards.append(self.create_security_dashboard())
            dashboards.append(self.create_cost_optimization_dashboard())
            
            logger.info(f"Successfully created {len(dashboards)} infrastructure dashboards")
            return dashboards
            
        except Exception as e:
            logger.error(f"Error creating dashboards: {e}")
            raise

def main():
    """Main execution function"""
    config = {
        'api_key': 'your-datadog-api-key',
        'app_key': 'your-datadog-app-key',
        'site': 'datadoghq.com'
    }
    
    dashboard_manager = InfrastructureDashboards(
        config['api_key'], 
        config['app_key'], 
        config['site']
    )
    
    try:
        dashboards = dashboard_manager.create_all_dashboards()
        
        print("Created DataDog Infrastructure Dashboards:")
        for dashboard in dashboards:
            print(f"- {dashboard['title']}: {dashboard['url']}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Dashboard creation failed: {e}")
        return 1

if __name__ == '__main__':
    exit(main())
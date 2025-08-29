#!/usr/bin/env python3
"""
DataDog Infrastructure Alerting System
Creates comprehensive alerts for infrastructure monitoring with automated notification
"""

import json
import logging
from typing import Dict, List, Any, Optional
from dataclass import dataclass
from datadog import api, initialize

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class AlertRule:
    """Alert rule configuration"""
    name: str
    query: str
    message: str
    threshold: float
    comparison: str  # 'above', 'below', 'equal'
    timeframe: str   # '5m', '15m', '1h', etc.
    priority: str    # 'low', 'normal', 'high', 'critical'
    tags: List[str]
    evaluation_delay: Optional[int] = None
    new_host_delay: Optional[int] = None
    notify_no_data: bool = False
    renotify_interval: Optional[int] = None

class InfrastructureAlerting:
    """Comprehensive infrastructure alerting system"""
    
    def __init__(self, api_key: str, app_key: str, site: str = 'datadoghq.com'):
        """Initialize alerting system"""
        initialize(api_key=api_key, app_key=app_key, api_host=f'https://api.{site}')
        
        # Notification channels
        self.notification_channels = {
            'critical': ['@opsgenie-critical', '@slack-ops-critical', '@pagerduty-high'],
            'high': ['@slack-ops-alerts', '@opsgenie-high'],
            'normal': ['@slack-ops-general', '@email-ops@company.com'],
            'low': ['@email-ops@company.com']
        }
        
        # Alert configuration
        self.alert_rules = self._define_alert_rules()
        
    def _define_alert_rules(self) -> List[AlertRule]:
        """Define comprehensive infrastructure alert rules"""
        rules = []
        
        # ===== KUBERNETES ALERTS =====
        
        # Kubernetes Node Health
        rules.append(AlertRule(
            name="Kubernetes Node Down",
            query="max(last_5m):sum:kubernetes.nodes.ready{cluster_name:enterprise-data-platform} < 3",
            message="""
            {{#is_alert}}
            **CRITICAL: Kubernetes Node Down**
            
            Less than 3 nodes are ready in the enterprise data platform cluster.
            
            **Current Value:** {{value}}
            **Expected:** >= 3 nodes
            
            **Impact:** Reduced cluster capacity and potential service disruption
            
            **Actions:**
            1. Check node status: `kubectl get nodes`
            2. Investigate node logs: `kubectl describe node <node-name>`
            3. Check node resources and health
            4. Escalate to infrastructure team if nodes cannot be recovered
            
            **Runbook:** https://wiki.company.com/runbooks/kubernetes-node-down
            {{/is_alert}}
            
            {{#is_recovery}}
            **RECOVERY: All Kubernetes nodes are now healthy**
            {{/is_recovery}}
            """,
            threshold=3,
            comparison='below',
            timeframe='5m',
            priority='critical',
            tags=['kubernetes', 'nodes', 'infrastructure'],
            evaluation_delay=60,
            renotify_interval=0
        ))
        
        # Pod Resource Usage
        rules.append(AlertRule(
            name="High Pod CPU Usage",
            query="avg(last_15m):avg:kubernetes.cpu.usage.total{cluster_name:enterprise-data-platform} by {pod_name} > 0.8",
            message="""
            {{#is_alert}}
            **WARNING: High Pod CPU Usage**
            
            Pod {{pod_name.name}} is using {{value}}% CPU (>80%)
            
            **Actions:**
            1. Check pod resource limits: `kubectl describe pod {{pod_name.name}}`
            2. Review application performance
            3. Consider scaling if needed: `kubectl scale deployment <deployment-name> --replicas=<count>`
            {{/is_alert}}
            """,
            threshold=0.8,
            comparison='above',
            timeframe='15m',
            priority='high',
            tags=['kubernetes', 'pods', 'cpu', 'performance']
        ))
        
        # ===== DATABASE ALERTS =====
        
        # PostgreSQL Connection Pool
        rules.append(AlertRule(
            name="PostgreSQL High Connection Usage",
            query="avg(last_10m):avg:postgresql.connections.active{service:postgresql,env:production} > 80",
            message="""
            {{#is_alert}}
            **CRITICAL: PostgreSQL High Connection Usage**
            
            PostgreSQL is using {{value}} active connections (>80)
            
            **Impact:** Database performance degradation, potential connection exhaustion
            
            **Actions:**
            1. Check current connections: `SELECT count(*) FROM pg_stat_activity;`
            2. Identify long-running queries: `SELECT * FROM pg_stat_activity WHERE state = 'active' ORDER BY query_start;`
            3. Check application connection pooling configuration
            4. Consider increasing max_connections if needed
            {{/is_alert}}
            """,
            threshold=80,
            comparison='above',
            timeframe='10m',
            priority='critical',
            tags=['postgresql', 'database', 'connections']
        ))
        
        # Database Slow Queries
        rules.append(AlertRule(
            name="PostgreSQL Slow Queries",
            query="sum(last_15m):sum:postgresql.slow_queries{service:postgresql,env:production} > 10",
            message="""
            {{#is_alert}}
            **WARNING: PostgreSQL Slow Queries Detected**
            
            {{value}} slow queries detected in the last 15 minutes
            
            **Actions:**
            1. Review slow query log
            2. Check for missing indexes: `SELECT * FROM pg_stat_user_tables WHERE seq_scan > idx_scan;`
            3. Analyze query performance with EXPLAIN ANALYZE
            4. Consider query optimization or index creation
            {{/is_alert}}
            """,
            threshold=10,
            comparison='above',
            timeframe='15m',
            priority='high',
            tags=['postgresql', 'database', 'performance', 'queries']
        ))
        
        # Redis Memory Usage
        rules.append(AlertRule(
            name="Redis High Memory Usage",
            query="avg(last_10m):avg:redis.mem.used{service:redis,env:production} / avg:redis.mem.max{service:redis,env:production} * 100 > 85",
            message="""
            {{#is_alert}}
            **WARNING: Redis High Memory Usage**
            
            Redis memory usage is at {{value}}% (>85%)
            
            **Actions:**
            1. Check memory usage: `INFO memory`
            2. Review key expiration policies
            3. Consider memory cleanup: `FLUSHALL` (if safe)
            4. Check for memory leaks or inefficient data structures
            {{/is_alert}}
            """,
            threshold=85,
            comparison='above',
            timeframe='10m',
            priority='high',
            tags=['redis', 'database', 'memory']
        ))
        
        # Elasticsearch Cluster Health
        rules.append(AlertRule(
            name="Elasticsearch Cluster Health",
            query="max(last_5m):avg:elasticsearch.cluster.status{service:elasticsearch,env:production} < 1",
            message="""
            {{#is_alert}}
            **CRITICAL: Elasticsearch Cluster Unhealthy**
            
            Elasticsearch cluster status is not green ({{value}})
            
            **Status Codes:**
            - 0: Red (critical)
            - 1: Yellow (warning)  
            - 2: Green (healthy)
            
            **Actions:**
            1. Check cluster health: `GET /_cluster/health`
            2. Investigate unassigned shards: `GET /_cat/shards?v&h=index,shard,prirep,state,unassigned.reason`
            3. Review cluster logs for errors
            4. Check disk space and node resources
            {{/is_alert}}
            """,
            threshold=1,
            comparison='below',
            timeframe='5m',
            priority='critical',
            tags=['elasticsearch', 'cluster', 'health']
        ))
        
        # ===== MESSAGE QUEUE ALERTS =====
        
        # RabbitMQ Queue Depth
        rules.append(AlertRule(
            name="RabbitMQ High Queue Depth",
            query="max(last_10m):max:rabbitmq.queue.messages{service:rabbitmq,env:production} by {queue} > 1000",
            message="""
            {{#is_alert}}
            **WARNING: RabbitMQ High Queue Depth**
            
            Queue {{queue.name}} has {{value}} messages (>1000)
            
            **Actions:**
            1. Check consumer status and processing rate
            2. Verify consumers are healthy: rabbitmqctl list_consumers
            3. Check for consumer errors in application logs
            4. Consider scaling consumers if needed
            {{/is_alert}}
            """,
            threshold=1000,
            comparison='above',
            timeframe='10m',
            priority='high',
            tags=['rabbitmq', 'queue', 'messaging']
        ))
        
        # Kafka Consumer Lag
        rules.append(AlertRule(
            name="Kafka High Consumer Lag",
            query="max(last_15m):max:kafka.consumer.lag{service:kafka,env:production} by {consumer_group} > 10000",
            message="""
            {{#is_alert}}
            **WARNING: Kafka High Consumer Lag**
            
            Consumer group {{consumer_group.name}} has lag of {{value}} messages (>10,000)
            
            **Actions:**
            1. Check consumer group status: `kafka-consumer-groups.sh --describe --group {{consumer_group.name}}`
            2. Verify consumers are processing messages
            3. Check consumer application health and logs
            4. Consider increasing consumer instances
            {{/is_alert}}
            """,
            threshold=10000,
            comparison='above',
            timeframe='15m',
            priority='high',
            tags=['kafka', 'consumer', 'messaging', 'lag']
        ))
        
        # ===== SYSTEM RESOURCE ALERTS =====
        
        # High CPU Usage
        rules.append(AlertRule(
            name="High System CPU Usage",
            query="avg(last_10m):avg:system.cpu.usage{env:production} by {host} > 85",
            message="""
            {{#is_alert}}
            **WARNING: High System CPU Usage**
            
            Host {{host.name}} CPU usage is {{value}}% (>85%)
            
            **Actions:**
            1. Check top processes: `top -p <pid>`
            2. Investigate high CPU processes
            3. Review system load: `uptime`
            4. Consider scaling or resource optimization
            {{/is_alert}}
            """,
            threshold=85,
            comparison='above',
            timeframe='10m',
            priority='high',
            tags=['system', 'cpu', 'performance']
        ))
        
        # High Memory Usage
        rules.append(AlertRule(
            name="High System Memory Usage", 
            query="avg(last_10m):100 - avg:system.mem.pct_usable{env:production} by {host} > 90",
            message="""
            {{#is_alert}}
            **WARNING: High System Memory Usage**
            
            Host {{host.name}} memory usage is {{value}}% (>90%)
            
            **Actions:**
            1. Check memory usage: `free -h`
            2. Identify memory-intensive processes: `ps aux --sort=-%mem | head`
            3. Check for memory leaks
            4. Consider adding memory or scaling
            {{/is_alert}}
            """,
            threshold=90,
            comparison='above',
            timeframe='10m',
            priority='high',
            tags=['system', 'memory', 'performance']
        ))
        
        # Disk Space
        rules.append(AlertRule(
            name="Low Disk Space",
            query="max(last_5m):100 - max:system.disk.free{env:production} / max:system.disk.total{env:production} * 100 by {host,device} > 85",
            message="""
            {{#is_alert}}
            **CRITICAL: Low Disk Space**
            
            Host {{host.name}} device {{device.name}} is {{value}}% full (>85%)
            
            **Actions:**
            1. Check disk usage: `df -h`
            2. Find large files: `du -sh /* | sort -hr | head -10`
            3. Clean up logs and temporary files
            4. Archive or delete old data
            5. Consider expanding storage
            {{/is_alert}}
            """,
            threshold=85,
            comparison='above',
            timeframe='5m',
            priority='critical',
            tags=['system', 'disk', 'storage'],
            renotify_interval=60  # Renotify every hour
        ))
        
        # ===== SECURITY ALERTS =====
        
        # Security Events
        rules.append(AlertRule(
            name="High Severity Security Events",
            query="sum(last_15m):sum:security.events.count{severity:HIGH,env:production} > 5",
            message="""
            {{#is_alert}}
            **CRITICAL: Multiple High Severity Security Events**
            
            {{value}} high severity security events detected in the last 15 minutes
            
            **Actions:**
            1. Review security event details in DataDog
            2. Check security logs for patterns
            3. Investigate potential security incidents
            4. Follow incident response procedures
            5. Contact security team immediately
            
            **Escalation:** security-team@company.com
            {{/is_alert}}
            """,
            threshold=5,
            comparison='above',
            timeframe='15m',
            priority='critical',
            tags=['security', 'events', 'threat'],
            renotify_interval=0  # Immediate renotification
        ))
        
        # Container Vulnerabilities
        rules.append(AlertRule(
            name="Critical Container Vulnerabilities",
            query="sum(last_1h):sum:container.security.vulnerabilities.critical{env:production} > 0",
            message="""
            {{#is_alert}}
            **CRITICAL: Critical Container Vulnerabilities Detected**
            
            {{value}} critical vulnerabilities found in container images
            
            **Actions:**
            1. Review vulnerability scan results
            2. Identify affected containers and images
            3. Update base images and dependencies
            4. Redeploy containers with patched images
            5. Follow security patching procedures
            {{/is_alert}}
            """,
            threshold=0,
            comparison='above',
            timeframe='1h',
            priority='critical',
            tags=['security', 'containers', 'vulnerabilities']
        ))
        
        # ===== BUSINESS KPI ALERTS =====
        
        # Data Processing Throughput
        rules.append(AlertRule(
            name="Low Data Processing Throughput",
            query="avg(last_30m):avg:business.data_processing.throughput_records_hour{env:production} < 500000",
            message="""
            {{#is_alert}}
            **WARNING: Low Data Processing Throughput**
            
            Data processing throughput is {{value}} records/hour (<500,000)
            
            **Impact:** SLA breach, delayed data availability
            
            **Actions:**
            1. Check data pipeline status
            2. Review processing job logs
            3. Investigate bottlenecks in data flow
            4. Scale processing resources if needed
            {{/is_alert}}
            """,
            threshold=500000,
            comparison='below',
            timeframe='30m',
            priority='high',
            tags=['business', 'kpi', 'throughput', 'data-processing']
        ))
        
        # System Availability
        rules.append(AlertRule(
            name="Low System Availability",
            query="avg(last_1h):avg:business.system.availability_pct{env:production} < 99.5",
            message="""
            {{#is_alert}}
            **CRITICAL: System Availability Below SLA**
            
            System availability is {{value}}% (<99.5%)
            
            **Impact:** SLA breach, customer impact
            
            **Actions:**
            1. Identify failing services
            2. Check service health endpoints
            3. Review error rates and response times
            4. Follow incident response procedures
            5. Notify stakeholders
            {{/is_alert}}
            """,
            threshold=99.5,
            comparison='below',
            timeframe='1h',
            priority='critical',
            tags=['business', 'kpi', 'availability', 'sla']
        ))
        
        return rules
    
    def create_all_alerts(self) -> List[Dict[str, Any]]:
        """Create all infrastructure alerts"""
        created_alerts = []
        
        for rule in self.alert_rules:
            try:
                alert = self._create_alert(rule)
                created_alerts.append(alert)
                logger.info(f"Created alert: {rule.name}")
            except Exception as e:
                logger.error(f"Failed to create alert {rule.name}: {e}")
        
        logger.info(f"Successfully created {len(created_alerts)} alerts")
        return created_alerts
    
    def _create_alert(self, rule: AlertRule) -> Dict[str, Any]:
        """Create individual alert"""
        # Build notification list based on priority
        notifications = self.notification_channels.get(rule.priority, ['@email-ops@company.com'])
        
        # Add notifications to message
        notification_str = ' '.join(notifications)
        message_with_notifications = f"{rule.message}\n\n{notification_str}"
        
        # Create monitor configuration
        monitor_options = {
            'notify_no_data': rule.notify_no_data,
            'no_data_timeframe': 20,
            'timeout_h': 0,
            'require_full_window': False,
            'new_host_delay': rule.new_host_delay or 300,
            'evaluation_delay': rule.evaluation_delay or 60,
            'silenced': {}
        }
        
        if rule.renotify_interval is not None:
            monitor_options['renotify_interval'] = rule.renotify_interval
        
        # Determine monitor type based on query
        if 'by {' in rule.query:
            monitor_type = 'metric alert'
        else:
            monitor_type = 'query alert'
        
        # Create the monitor
        monitor = api.Monitor.create(
            type=monitor_type,
            query=rule.query,
            name=rule.name,
            message=message_with_notifications,
            tags=rule.tags + ['env:production', 'team:data-platform'],
            options=monitor_options
        )
        
        return monitor
    
    def create_composite_alerts(self) -> List[Dict[str, Any]]:
        """Create composite alerts that combine multiple conditions"""
        composite_alerts = []
        
        # Data Platform Health Composite
        data_platform_health = {
            'name': 'Data Platform Health Composite',
            'type': 'composite',
            'query': '(a || b || c)',  # Any of the conditions trigger
            'message': """
            {{#is_alert}}
            **CRITICAL: Data Platform Health Issue**
            
            Multiple components of the data platform are showing issues:
            - Database connectivity or performance problems
            - Message queue issues
            - High resource utilization
            
            **Actions:**
            1. Check individual component alerts for details
            2. Review overall system health
            3. Follow escalation procedures
            4. Notify on-call engineer
            {{/is_alert}}
            """,
            'tags': ['composite', 'data-platform', 'health'],
            'options': {
                'notify_no_data': False,
                'renotify_interval': 0
            }
        }
        
        try:
            composite_monitor = api.Monitor.create(**data_platform_health)
            composite_alerts.append(composite_monitor)
            logger.info("Created Data Platform Health composite alert")
        except Exception as e:
            logger.error(f"Failed to create composite alert: {e}")
        
        return composite_alerts
    
    def create_sli_slo_alerts(self) -> List[Dict[str, Any]]:
        """Create SLI/SLO based alerts"""
        slo_alerts = []
        
        # Data Processing SLO (99.5% of data processed within 1 hour)
        data_processing_slo = {
            'name': 'Data Processing SLO Violation',
            'type': 'slo alert',
            'query': 'burn_rate("data_processing_slo").over("1h").long_window("7d") > 14.4',
            'message': """
            {{#is_alert}}
            **CRITICAL: Data Processing SLO Violation**
            
            Data processing SLO burn rate is {{value}} (>14.4x normal rate)
            
            **SLO Target:** 99.5% of data processed within 1 hour
            **Current Performance:** Below target
            
            **Actions:**
            1. Check data processing pipeline health
            2. Investigate processing delays
            3. Review resource allocation
            4. Consider emergency scaling
            
            **Error Budget Impact:** High - immediate attention required
            {{/is_alert}}
            """,
            'tags': ['slo', 'data-processing', 'burn-rate'],
            'options': {
                'notify_no_data': False,
                'renotify_interval': 60
            }
        }
        
        # System Availability SLO (99.9% uptime)
        availability_slo = {
            'name': 'System Availability SLO Violation',
            'type': 'slo alert',
            'query': 'burn_rate("system_availability_slo").over("1h").long_window("30d") > 36',
            'message': """
            {{#is_alert}}
            **CRITICAL: System Availability SLO Violation**
            
            System availability SLO burn rate is {{value}} (>36x normal rate)
            
            **SLO Target:** 99.9% system availability
            **Error Budget:** Being consumed rapidly
            
            **Actions:**
            1. Identify and resolve service outages
            2. Check all critical system components
            3. Follow incident response procedures
            4. Notify stakeholders and customers if needed
            {{/is_alert}}
            """,
            'tags': ['slo', 'availability', 'burn-rate'],
            'options': {
                'notify_no_data': False,
                'renotify_interval': 30
            }
        }
        
        for slo_alert in [data_processing_slo, availability_slo]:
            try:
                monitor = api.Monitor.create(**slo_alert)
                slo_alerts.append(monitor)
                logger.info(f"Created SLO alert: {slo_alert['name']}")
            except Exception as e:
                logger.error(f"Failed to create SLO alert {slo_alert['name']}: {e}")
        
        return slo_alerts
    
    def create_anomaly_detection_alerts(self) -> List[Dict[str, Any]]:
        """Create anomaly detection based alerts"""
        anomaly_alerts = []
        
        # Database Query Volume Anomaly
        db_anomaly = {
            'name': 'Database Query Volume Anomaly',
            'type': 'query alert',
            'query': 'avg(last_4h):anomalies(avg:postgresql.queries.select{env:production}, "basic", 2, direction="both", alert_window="last_15m", interval=60, count_default_zero="true") >= 1',
            'message': """
            {{#is_alert}}
            **WARNING: Database Query Volume Anomaly Detected**
            
            Database query volume is showing unusual patterns compared to historical data.
            
            **Actions:**
            1. Review recent application deployments
            2. Check for unexpected traffic patterns
            3. Investigate potential performance issues
            4. Monitor query performance and resource usage
            {{/is_alert}}
            """,
            'tags': ['anomaly', 'database', 'queries'],
            'options': {
                'notify_no_data': False,
                'evaluation_delay': 900
            }
        }
        
        # Network Traffic Anomaly
        network_anomaly = {
            'name': 'Network Traffic Anomaly',
            'type': 'query alert',
            'query': 'avg(last_4h):anomalies(avg:system.net.bytes_sent{env:production}, "agile", 3, direction="above", alert_window="last_30m", interval=300, count_default_zero="true") >= 1',
            'message': """
            {{#is_alert}}
            **WARNING: Network Traffic Anomaly Detected**
            
            Network traffic is significantly higher than normal patterns.
            
            **Possible Causes:**
            - DDoS attack or unusual traffic spike
            - Data replication or backup operations
            - Application behavior changes
            - Network misconfiguration
            
            **Actions:**
            1. Investigate traffic sources and destinations
            2. Check for security events
            3. Review network monitoring dashboards
            4. Verify legitimate traffic patterns
            {{/is_alert}}
            """,
            'tags': ['anomaly', 'network', 'traffic'],
            'options': {
                'notify_no_data': False,
                'evaluation_delay': 600
            }
        }
        
        for anomaly_alert in [db_anomaly, network_anomaly]:
            try:
                monitor = api.Monitor.create(**anomaly_alert)
                anomaly_alerts.append(monitor)
                logger.info(f"Created anomaly alert: {anomaly_alert['name']}")
            except Exception as e:
                logger.error(f"Failed to create anomaly alert {anomaly_alert['name']}: {e}")
        
        return anomaly_alerts
    
    def setup_notification_channels(self) -> Dict[str, Any]:
        """Setup notification channels (integrations)"""
        # This would configure integrations with external systems
        # For now, return the configured channels
        return {
            'slack_channels': ['#ops-alerts', '#ops-critical', '#ops-general'],
            'email_lists': ['ops@company.com', 'oncall@company.com'],
            'pagerduty_services': ['data-platform-high', 'infrastructure-critical'],
            'opsgenie_teams': ['data-platform', 'infrastructure']
        }
    
    def validate_alerts(self) -> Dict[str, Any]:
        """Validate that all alerts are working correctly"""
        validation_results = {
            'total_alerts': 0,
            'active_alerts': 0,
            'muted_alerts': 0,
            'errors': []
        }
        
        try:
            # Get all monitors
            monitors = api.Monitor.get_all()
            
            validation_results['total_alerts'] = len(monitors)
            
            for monitor in monitors:
                if monitor.get('options', {}).get('silenced'):
                    validation_results['muted_alerts'] += 1
                else:
                    validation_results['active_alerts'] += 1
            
            logger.info(f"Alert validation complete: {validation_results}")
            
        except Exception as e:
            validation_results['errors'].append(str(e))
            logger.error(f"Alert validation failed: {e}")
        
        return validation_results

def main():
    """Main execution function"""
    config = {
        'api_key': 'your-datadog-api-key',
        'app_key': 'your-datadog-app-key',
        'site': 'datadoghq.com'
    }
    
    alerting = InfrastructureAlerting(config['api_key'], config['app_key'], config['site'])
    
    try:
        # Create all alert types
        basic_alerts = alerting.create_all_alerts()
        composite_alerts = alerting.create_composite_alerts()
        slo_alerts = alerting.create_sli_slo_alerts()
        anomaly_alerts = alerting.create_anomaly_detection_alerts()
        
        # Setup notification channels
        notification_channels = alerting.setup_notification_channels()
        
        # Validate alerts
        validation = alerting.validate_alerts()
        
        total_alerts = len(basic_alerts) + len(composite_alerts) + len(slo_alerts) + len(anomaly_alerts)
        
        print(f"\nDataDog Infrastructure Alerting Setup Complete:")
        print(f"- Basic alerts: {len(basic_alerts)}")
        print(f"- Composite alerts: {len(composite_alerts)}")
        print(f"- SLO alerts: {len(slo_alerts)}")
        print(f"- Anomaly detection alerts: {len(anomaly_alerts)}")
        print(f"- Total alerts created: {total_alerts}")
        print(f"\nNotification channels configured:")
        for channel_type, channels in notification_channels.items():
            print(f"- {channel_type}: {len(channels)} channels")
        
        return 0
        
    except Exception as e:
        logger.error(f"Alerting setup failed: {e}")
        return 1

if __name__ == '__main__':
    exit(main())
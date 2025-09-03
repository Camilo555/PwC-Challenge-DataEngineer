#!/usr/bin/env python3
"""
Advanced Resource Utilization Analyzer
Analyzes resource usage patterns and provides rightsizing recommendations
"""

import json
import boto3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ResourceAnalyzer:
    """Advanced resource utilization analyzer with ML-powered insights"""
    
    def __init__(self):
        self.cloudwatch = boto3.client('cloudwatch')
        self.ec2 = boto3.client('ec2')
        self.rds = boto3.client('rds')
        self.ce = boto3.client('ce')
        self.sns = boto3.client('sns')
        self.ssm = boto3.client('ssm')
        
        # Configuration from environment
        self.optimization_rules = json.loads(os.environ.get('OPTIMIZATION_RULES', '{}'))
        self.sns_topic = os.environ.get('SNS_TOPIC_ARN')
        self.namespace = os.environ.get('CLOUDWATCH_NAMESPACE', 'Optimization')
        self.analysis_days = int(os.environ.get('ANALYSIS_PERIOD_DAYS', '14'))
        self.cpu_threshold_low = float(os.environ.get('CPU_THRESHOLD_LOW', '20'))
        self.memory_threshold_low = float(os.environ.get('MEMORY_THRESHOLD_LOW', '30'))
        
    def lambda_handler(self, event, context):
        """Main Lambda handler for resource analysis"""
        try:
            logger.info(f"Starting resource analysis for ${var.project_name}-${var.environment}")
            
            # Analyze different resource types
            analysis_results = {
                'timestamp': datetime.utcnow().isoformat(),
                'ec2_analysis': self.analyze_ec2_instances(),
                'rds_analysis': self.analyze_rds_instances(),
                'cost_analysis': self.analyze_cost_trends(),
                'recommendations': []
            }
            
            # Generate recommendations
            recommendations = self.generate_recommendations(analysis_results)
            analysis_results['recommendations'] = recommendations
            
            # Store results
            self.store_analysis_results(analysis_results)
            
            # Send notifications if needed
            if recommendations:
                self.send_notifications(recommendations)
                
            # Update CloudWatch metrics
            self.update_cloudwatch_metrics(analysis_results)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Resource analysis completed successfully',
                    'recommendations_count': len(recommendations),
                    'potential_savings': self.calculate_potential_savings(recommendations)
                })
            }
            
        except Exception as e:
            logger.error(f"Error in resource analysis: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({'error': str(e)})
            }
    
    def analyze_ec2_instances(self) -> Dict[str, Any]:
        """Analyze EC2 instance utilization and performance"""
        logger.info("Analyzing EC2 instances")
        
        try:
            instances_response = self.ec2.describe_instances(
                Filters=[
                    {'Name': 'instance-state-name', 'Values': ['running']},
                    {'Name': 'tag:Project', 'Values': ['${var.project_name}']}
                ]
            )
            
            analysis = {
                'total_instances': 0,
                'underutilized_instances': [],
                'overutilized_instances': [],
                'rightsizing_candidates': [],
                'total_cost_current': 0,
                'potential_savings': 0
            }
            
            for reservation in instances_response['Reservations']:
                for instance in reservation['Instances']:
                    instance_id = instance['InstanceId']
                    instance_type = instance['InstanceType']
                    
                    analysis['total_instances'] += 1
                    
                    # Get utilization metrics
                    cpu_utilization = self.get_cpu_utilization(instance_id)
                    memory_utilization = self.get_memory_utilization(instance_id)
                    network_utilization = self.get_network_utilization(instance_id)
                    
                    # Analyze utilization patterns
                    instance_analysis = {
                        'instance_id': instance_id,
                        'instance_type': instance_type,
                        'cpu_avg': cpu_utilization['average'],
                        'cpu_max': cpu_utilization['maximum'],
                        'memory_avg': memory_utilization['average'],
                        'network_in_avg': network_utilization['in_average'],
                        'network_out_avg': network_utilization['out_average'],
                        'cost_monthly': self.get_instance_monthly_cost(instance_type),
                        'tags': {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
                    }
                    
                    # Categorize based on utilization
                    if (cpu_utilization['average'] < self.cpu_threshold_low and 
                        memory_utilization['average'] < self.memory_threshold_low):
                        analysis['underutilized_instances'].append(instance_analysis)
                        
                        # Generate rightsizing recommendation
                        recommended_type = self.recommend_instance_type(instance_analysis)
                        if recommended_type != instance_type:
                            savings = instance_analysis['cost_monthly'] - self.get_instance_monthly_cost(recommended_type)
                            analysis['rightsizing_candidates'].append({
                                **instance_analysis,
                                'recommended_type': recommended_type,
                                'potential_monthly_savings': savings
                            })
                            analysis['potential_savings'] += savings
                    
                    elif cpu_utilization['average'] > 80 or memory_utilization['average'] > 85:
                        analysis['overutilized_instances'].append(instance_analysis)
                    
                    analysis['total_cost_current'] += instance_analysis['cost_monthly']
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing EC2 instances: {str(e)}")
            return {}
    
    def analyze_rds_instances(self) -> Dict[str, Any]:
        """Analyze RDS instance utilization"""
        logger.info("Analyzing RDS instances")
        
        try:
            db_instances = self.rds.describe_db_instances()
            
            analysis = {
                'total_instances': len(db_instances['DBInstances']),
                'underutilized_instances': [],
                'rightsizing_candidates': [],
                'potential_savings': 0
            }
            
            for db_instance in db_instances['DBInstances']:
                if db_instance['DBInstanceStatus'] != 'available':
                    continue
                    
                db_identifier = db_instance['DBInstanceIdentifier']
                db_class = db_instance['DBInstanceClass']
                
                # Get RDS metrics
                cpu_utilization = self.get_rds_cpu_utilization(db_identifier)
                connection_count = self.get_rds_connections(db_identifier)
                
                instance_analysis = {
                    'db_identifier': db_identifier,
                    'db_class': db_class,
                    'engine': db_instance['Engine'],
                    'cpu_avg': cpu_utilization['average'],
                    'cpu_max': cpu_utilization['maximum'],
                    'connection_avg': connection_count['average'],
                    'cost_monthly': self.get_rds_monthly_cost(db_class),
                    'multi_az': db_instance.get('MultiAZ', False)
                }
                
                if cpu_utilization['average'] < 20 and connection_count['average'] < 10:
                    analysis['underutilized_instances'].append(instance_analysis)
                    
                    # Recommend smaller instance
                    recommended_class = self.recommend_rds_class(instance_analysis)
                    if recommended_class != db_class:
                        savings = instance_analysis['cost_monthly'] - self.get_rds_monthly_cost(recommended_class)
                        analysis['rightsizing_candidates'].append({
                            **instance_analysis,
                            'recommended_class': recommended_class,
                            'potential_monthly_savings': savings
                        })
                        analysis['potential_savings'] += savings
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing RDS instances: {str(e)}")
            return {}
    
    def analyze_cost_trends(self) -> Dict[str, Any]:
        """Analyze cost trends and detect anomalies"""
        logger.info("Analyzing cost trends")
        
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            cost_response = self.ce.get_cost_and_usage(
                TimePeriod={
                    'Start': start_date.strftime('%Y-%m-%d'),
                    'End': end_date.strftime('%Y-%m-%d')
                },
                Granularity='DAILY',
                Metrics=['BlendedCost'],
                GroupBy=[
                    {'Type': 'DIMENSION', 'Key': 'SERVICE'},
                    {'Type': 'TAG', 'Key': 'Environment'}
                ]
            )
            
            analysis = {
                'total_cost_30d': 0,
                'cost_by_service': {},
                'cost_trends': [],
                'anomalies_detected': []
            }
            
            for result in cost_response['ResultsByTime']:
                date = result['TimePeriod']['Start']
                daily_cost = float(result['Total']['BlendedCost']['Amount'])
                analysis['total_cost_30d'] += daily_cost
                
                analysis['cost_trends'].append({
                    'date': date,
                    'cost': daily_cost
                })
                
                # Simple anomaly detection (cost > 2x average)
                if len(analysis['cost_trends']) > 7:
                    avg_cost = sum(t['cost'] for t in analysis['cost_trends'][-7:]) / 7
                    if daily_cost > avg_cost * 2:
                        analysis['anomalies_detected'].append({
                            'date': date,
                            'cost': daily_cost,
                            'average': avg_cost,
                            'anomaly_factor': daily_cost / avg_cost
                        })
                
                # Aggregate by service
                for group in result['Groups']:
                    service = group['Keys'][0] if group['Keys'] else 'Unknown'
                    cost = float(group['Metrics']['BlendedCost']['Amount'])
                    
                    if service not in analysis['cost_by_service']:
                        analysis['cost_by_service'][service] = 0
                    analysis['cost_by_service'][service] += cost
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing cost trends: {str(e)}")
            return {}
    
    def get_cpu_utilization(self, instance_id: str) -> Dict[str, float]:
        """Get CPU utilization metrics for an instance"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=self.analysis_days)
        
        try:
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='CPUUtilization',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1 hour
                Statistics=['Average', 'Maximum']
            )
            
            if response['Datapoints']:
                averages = [dp['Average'] for dp in response['Datapoints']]
                maximums = [dp['Maximum'] for dp in response['Datapoints']]
                return {
                    'average': sum(averages) / len(averages),
                    'maximum': max(maximums)
                }
            
        except Exception as e:
            logger.warning(f"Error getting CPU metrics for {instance_id}: {str(e)}")
        
        return {'average': 0, 'maximum': 0}
    
    def get_memory_utilization(self, instance_id: str) -> Dict[str, float]:
        """Get memory utilization metrics (requires CloudWatch agent)"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=self.analysis_days)
        
        try:
            response = self.cloudwatch.get_metric_statistics(
                Namespace='CWAgent',
                MetricName='mem_used_percent',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Average', 'Maximum']
            )
            
            if response['Datapoints']:
                averages = [dp['Average'] for dp in response['Datapoints']]
                maximums = [dp['Maximum'] for dp in response['Datapoints']]
                return {
                    'average': sum(averages) / len(averages),
                    'maximum': max(maximums)
                }
                
        except Exception as e:
            logger.warning(f"Error getting memory metrics for {instance_id}: {str(e)}")
        
        return {'average': 50, 'maximum': 60}  # Default assumption
    
    def get_network_utilization(self, instance_id: str) -> Dict[str, float]:
        """Get network utilization metrics"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=self.analysis_days)
        
        try:
            # Network In
            net_in_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='NetworkIn',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Average']
            )
            
            # Network Out
            net_out_response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/EC2',
                MetricName='NetworkOut',
                Dimensions=[{'Name': 'InstanceId', 'Value': instance_id}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Average']
            )
            
            net_in_avg = 0
            net_out_avg = 0
            
            if net_in_response['Datapoints']:
                net_in_avg = sum(dp['Average'] for dp in net_in_response['Datapoints']) / len(net_in_response['Datapoints'])
                
            if net_out_response['Datapoints']:
                net_out_avg = sum(dp['Average'] for dp in net_out_response['Datapoints']) / len(net_out_response['Datapoints'])
            
            return {
                'in_average': net_in_avg / (1024 * 1024),  # Convert to MB
                'out_average': net_out_avg / (1024 * 1024)
            }
            
        except Exception as e:
            logger.warning(f"Error getting network metrics for {instance_id}: {str(e)}")
        
        return {'in_average': 0, 'out_average': 0}
    
    def get_rds_cpu_utilization(self, db_identifier: str) -> Dict[str, float]:
        """Get RDS CPU utilization"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=self.analysis_days)
        
        try:
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='CPUUtilization',
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_identifier}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Average', 'Maximum']
            )
            
            if response['Datapoints']:
                averages = [dp['Average'] for dp in response['Datapoints']]
                maximums = [dp['Maximum'] for dp in response['Datapoints']]
                return {
                    'average': sum(averages) / len(averages),
                    'maximum': max(maximums)
                }
                
        except Exception as e:
            logger.warning(f"Error getting RDS CPU metrics for {db_identifier}: {str(e)}")
        
        return {'average': 0, 'maximum': 0}
    
    def get_rds_connections(self, db_identifier: str) -> Dict[str, float]:
        """Get RDS connection count"""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=self.analysis_days)
        
        try:
            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/RDS',
                MetricName='DatabaseConnections',
                Dimensions=[{'Name': 'DBInstanceIdentifier', 'Value': db_identifier}],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Average']
            )
            
            if response['Datapoints']:
                averages = [dp['Average'] for dp in response['Datapoints']]
                return {'average': sum(averages) / len(averages)}
                
        except Exception as e:
            logger.warning(f"Error getting RDS connection metrics for {db_identifier}: {str(e)}")
        
        return {'average': 0}
    
    def recommend_instance_type(self, instance_analysis: Dict[str, Any]) -> str:
        """Recommend optimal instance type based on utilization"""
        current_type = instance_analysis['instance_type']
        cpu_avg = instance_analysis['cpu_avg']
        memory_avg = instance_analysis['memory_avg']
        
        # Simple recommendation logic - in production, use more sophisticated ML models
        if cpu_avg < 10 and memory_avg < 20:
            return 't3.micro'
        elif cpu_avg < 20 and memory_avg < 30:
            return 't3.small'
        elif cpu_avg < 40 and memory_avg < 50:
            return 't3.medium'
        elif cpu_avg < 60:
            return 't3.large'
        
        return current_type  # No change recommended
    
    def recommend_rds_class(self, instance_analysis: Dict[str, Any]) -> str:
        """Recommend optimal RDS instance class"""
        current_class = instance_analysis['db_class']
        cpu_avg = instance_analysis['cpu_avg']
        
        if cpu_avg < 10:
            return 'db.t3.micro'
        elif cpu_avg < 20:
            return 'db.t3.small'
        elif cpu_avg < 40:
            return 'db.t3.medium'
        
        return current_class
    
    def get_instance_monthly_cost(self, instance_type: str) -> float:
        """Get estimated monthly cost for instance type"""
        # Simplified cost calculation - in production, use AWS Pricing API
        costs = {
            't3.micro': 7.5,
            't3.small': 15.0,
            't3.medium': 30.0,
            't3.large': 60.0,
            't3.xlarge': 120.0,
            'm5.large': 70.0,
            'm5.xlarge': 140.0,
            'c5.large': 62.0,
            'r5.large': 91.0
        }
        return costs.get(instance_type, 50.0)  # Default estimate
    
    def get_rds_monthly_cost(self, db_class: str) -> float:
        """Get estimated monthly cost for RDS instance class"""
        costs = {
            'db.t3.micro': 12.0,
            'db.t3.small': 24.0,
            'db.t3.medium': 48.0,
            'db.t3.large': 96.0,
            'db.r5.large': 180.0
        }
        return costs.get(db_class, 60.0)
    
    def generate_recommendations(self, analysis_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate actionable optimization recommendations"""
        recommendations = []
        
        # EC2 Rightsizing Recommendations
        ec2_analysis = analysis_results.get('ec2_analysis', {})
        for candidate in ec2_analysis.get('rightsizing_candidates', []):
            recommendations.append({
                'type': 'ec2_rightsizing',
                'priority': 'high' if candidate['potential_monthly_savings'] > 50 else 'medium',
                'resource_id': candidate['instance_id'],
                'current_type': candidate['instance_type'],
                'recommended_type': candidate['recommended_type'],
                'potential_monthly_savings': candidate['potential_monthly_savings'],
                'reason': f"Low utilization: CPU {candidate['cpu_avg']:.1f}%, Memory {candidate['memory_avg']:.1f}%",
                'action': 'resize_instance'
            })
        
        # RDS Rightsizing Recommendations
        rds_analysis = analysis_results.get('rds_analysis', {})
        for candidate in rds_analysis.get('rightsizing_candidates', []):
            recommendations.append({
                'type': 'rds_rightsizing',
                'priority': 'medium',
                'resource_id': candidate['db_identifier'],
                'current_class': candidate['db_class'],
                'recommended_class': candidate['recommended_class'],
                'potential_monthly_savings': candidate['potential_monthly_savings'],
                'reason': f"Low utilization: CPU {candidate['cpu_avg']:.1f}%",
                'action': 'resize_database'
            })
        
        # Cost Anomaly Alerts
        cost_analysis = analysis_results.get('cost_analysis', {})
        for anomaly in cost_analysis.get('anomalies_detected', []):
            recommendations.append({
                'type': 'cost_anomaly',
                'priority': 'high',
                'date': anomaly['date'],
                'cost': anomaly['cost'],
                'anomaly_factor': anomaly['anomaly_factor'],
                'reason': f"Cost spike detected: {anomaly['anomaly_factor']:.1f}x normal",
                'action': 'investigate_cost_spike'
            })
        
        return recommendations
    
    def calculate_potential_savings(self, recommendations: List[Dict[str, Any]]) -> float:
        """Calculate total potential monthly savings"""
        total_savings = 0
        for rec in recommendations:
            if 'potential_monthly_savings' in rec:
                total_savings += rec['potential_monthly_savings']
        return total_savings
    
    def store_analysis_results(self, results: Dict[str, Any]):
        """Store analysis results in SSM Parameter Store"""
        try:
            self.ssm.put_parameter(
                Name=f"/${var.project_name}/${var.environment}/optimization/latest-analysis",
                Value=json.dumps(results, default=str),
                Type='String',
                Overwrite=True
            )
            logger.info("Analysis results stored successfully")
        except Exception as e:
            logger.error(f"Error storing analysis results: {str(e)}")
    
    def send_notifications(self, recommendations: List[Dict[str, Any]]):
        """Send notifications for high-priority recommendations"""
        try:
            high_priority = [r for r in recommendations if r.get('priority') == 'high']
            if high_priority and self.sns_topic:
                message = f"High-priority optimization recommendations for ${var.project_name}-${var.environment}:\n\n"
                
                for rec in high_priority:
                    savings = rec.get('potential_monthly_savings', 0)
                    message += f"• {rec['type']}: {rec['resource_id']}\n"
                    message += f"  Action: {rec['action']}\n"
                    message += f"  Savings: ${savings:.2f}/month\n"
                    message += f"  Reason: {rec['reason']}\n\n"
                
                self.sns.publish(
                    TopicArn=self.sns_topic,
                    Subject=f"Infrastructure Optimization Alert - ${var.project_name}",
                    Message=message
                )
                logger.info("Notifications sent successfully")
        except Exception as e:
            logger.error(f"Error sending notifications: {str(e)}")
    
    def update_cloudwatch_metrics(self, analysis_results: Dict[str, Any]):
        """Update CloudWatch custom metrics"""
        try:
            timestamp = datetime.utcnow()
            
            # EC2 metrics
            ec2_analysis = analysis_results.get('ec2_analysis', {})
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=[
                    {
                        'MetricName': 'TotalInstances',
                        'Value': ec2_analysis.get('total_instances', 0),
                        'Timestamp': timestamp,
                        'Dimensions': [
                            {'Name': 'ResourceType', 'Value': 'EC2'},
                            {'Name': 'Environment', 'Value': '${var.environment}'}
                        ]
                    },
                    {
                        'MetricName': 'UnderutilizedInstances',
                        'Value': len(ec2_analysis.get('underutilized_instances', [])),
                        'Timestamp': timestamp,
                        'Dimensions': [
                            {'Name': 'ResourceType', 'Value': 'EC2'},
                            {'Name': 'Environment', 'Value': '${var.environment}'}
                        ]
                    },
                    {
                        'MetricName': 'PotentialSavings',
                        'Value': ec2_analysis.get('potential_savings', 0),
                        'Unit': 'Count',
                        'Timestamp': timestamp
                    }
                ]
            )
            
            logger.info("CloudWatch metrics updated successfully")
            
        except Exception as e:
            logger.error(f"Error updating CloudWatch metrics: {str(e)}")

# Create instance for Lambda execution
analyzer = ResourceAnalyzer()

def lambda_handler(event, context):
    """Lambda entry point"""
    return analyzer.lambda_handler(event, context)
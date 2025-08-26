import json
import boto3
from datetime import datetime, timedelta
import os

def lambda_handler(event, context):
    """
    AWS Lambda function for intelligent cost optimization
    """
    
    # Initialize AWS clients
    ec2 = boto3.client('ec2')
    autoscaling = boto3.client('autoscaling')
    rds = boto3.client('rds')
    ce = boto3.client('ce')
    
    # Environment variables
    environment = os.environ.get('ENVIRONMENT', '${environment}')
    cost_center = os.environ.get('COST_CENTER', '${cost_center}')
    auto_shutdown = os.environ.get('AUTO_SHUTDOWN_ENABLED', 'false').lower() == 'true'
    
    optimization_actions = []
    
    try:
        # 1. Analyze cost and usage
        cost_analysis = analyze_costs(ce, environment)
        optimization_actions.append({
            "action": "cost_analysis",
            "status": "completed",
            "details": cost_analysis
        })
        
        # 2. Optimize EC2 instances (only for non-prod environments)
        if environment != 'prod' and auto_shutdown:
            ec2_optimization = optimize_ec2_instances(ec2, environment)
            optimization_actions.extend(ec2_optimization)
        
        # 3. Optimize RDS instances
        if environment != 'prod' and auto_shutdown:
            rds_optimization = optimize_rds_instances(rds, environment)
            optimization_actions.extend(rds_optimization)
        
        # 4. Optimize Auto Scaling Groups
        asg_optimization = optimize_auto_scaling_groups(autoscaling, environment)
        optimization_actions.extend(asg_optimization)
        
        # 5. Generate recommendations
        recommendations = generate_cost_recommendations(cost_analysis)
        optimization_actions.append({
            "action": "recommendations",
            "status": "completed",
            "details": recommendations
        })
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Cost optimization completed successfully',
                'actions_taken': len(optimization_actions),
                'optimization_actions': optimization_actions,
                'timestamp': datetime.now().isoformat()
            })
        }
        
    except Exception as e:
        print(f"Error in cost optimization: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Cost optimization failed',
                'timestamp': datetime.now().isoformat()
            })
        }

def analyze_costs(ce_client, environment):
    """Analyze current costs and usage patterns"""
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    
    try:
        response = ce_client.get_cost_and_usage(
            TimePeriod={
                'Start': start_date.strftime('%Y-%m-%d'),
                'End': end_date.strftime('%Y-%m-%d')
            },
            Granularity='DAILY',
            Metrics=['BlendedCost'],
            GroupBy=[
                {
                    'Type': 'DIMENSION',
                    'Key': 'SERVICE'
                }
            ],
            Filter={
                'Tags': {
                    'Key': 'Environment',
                    'Values': [environment]
                }
            }
        )
        
        total_cost = 0
        service_costs = {}
        
        for result in response['ResultsByTime']:
            for group in result['Groups']:
                service = group['Keys'][0]
                cost = float(group['Metrics']['BlendedCost']['Amount'])
                total_cost += cost
                
                if service not in service_costs:
                    service_costs[service] = 0
                service_costs[service] += cost
        
        # Sort services by cost (descending)
        sorted_services = sorted(service_costs.items(), key=lambda x: x[1], reverse=True)
        
        return {
            'total_cost_30_days': round(total_cost, 2),
            'top_services': sorted_services[:5],
            'analysis_period': f"{start_date} to {end_date}"
        }
        
    except Exception as e:
        print(f"Error analyzing costs: {str(e)}")
        return {'error': str(e)}

def optimize_ec2_instances(ec2_client, environment):
    """Optimize EC2 instances based on usage patterns"""
    
    actions = []
    current_hour = datetime.now().hour
    current_weekday = datetime.now().weekday()
    
    try:
        # Get all running instances for the environment
        response = ec2_client.describe_instances(
            Filters=[
                {
                    'Name': 'instance-state-name',
                    'Values': ['running']
                },
                {
                    'Name': 'tag:Environment',
                    'Values': [environment]
                }
            ]
        )
        
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                instance_id = instance['InstanceId']
                instance_type = instance['InstanceType']
                
                # Check if instance should be stopped (outside business hours for dev/test)
                if environment in ['dev', 'test']:
                    if current_hour < 8 or current_hour > 18 or current_weekday >= 5:  # Weekend or after hours
                        try:
                            ec2_client.stop_instances(InstanceIds=[instance_id])
                            actions.append({
                                "action": "stop_ec2_instance",
                                "instance_id": instance_id,
                                "instance_type": instance_type,
                                "reason": "outside_business_hours",
                                "status": "completed"
                            })
                        except Exception as e:
                            actions.append({
                                "action": "stop_ec2_instance",
                                "instance_id": instance_id,
                                "status": "failed",
                                "error": str(e)
                            })
                
                # Check for right-sizing opportunities
                # This is a simplified example - in practice, you'd analyze CloudWatch metrics
                if 't2.micro' not in instance_type and environment in ['dev', 'test']:
                    actions.append({
                        "action": "rightsizing_recommendation",
                        "instance_id": instance_id,
                        "current_type": instance_type,
                        "recommended_type": "t3.small",
                        "potential_savings": "30-50%",
                        "status": "recommendation"
                    })
        
        return actions
        
    except Exception as e:
        print(f"Error optimizing EC2 instances: {str(e)}")
        return [{"action": "optimize_ec2", "status": "failed", "error": str(e)}]

def optimize_rds_instances(rds_client, environment):
    """Optimize RDS instances"""
    
    actions = []
    
    try:
        response = rds_client.describe_db_instances()
        
        for db_instance in response['DBInstances']:
            db_identifier = db_instance['DBInstanceIdentifier']
            db_class = db_instance['DBInstanceClass']
            db_status = db_instance['DBInstanceStatus']
            
            # Check tags to see if this belongs to our environment
            try:
                tags_response = rds_client.list_tags_for_resource(
                    ResourceName=db_instance['DBInstanceArn']
                )
                
                tags = {tag['Key']: tag['Value'] for tag in tags_response['TagList']}
                
                if tags.get('Environment') == environment:
                    # Stop non-prod databases during off-hours
                    if environment in ['dev', 'test'] and db_status == 'available':
                        current_hour = datetime.now().hour
                        current_weekday = datetime.now().weekday()
                        
                        if current_hour < 8 or current_hour > 18 or current_weekday >= 5:
                            try:
                                rds_client.stop_db_instance(DBInstanceIdentifier=db_identifier)
                                actions.append({
                                    "action": "stop_rds_instance",
                                    "db_identifier": db_identifier,
                                    "db_class": db_class,
                                    "reason": "outside_business_hours",
                                    "status": "completed"
                                })
                            except Exception as e:
                                actions.append({
                                    "action": "stop_rds_instance",
                                    "db_identifier": db_identifier,
                                    "status": "failed",
                                    "error": str(e)
                                })
                    
                    # Recommend right-sizing for oversized instances
                    if 'large' in db_class and environment in ['dev', 'test']:
                        actions.append({
                            "action": "rds_rightsizing_recommendation",
                            "db_identifier": db_identifier,
                            "current_class": db_class,
                            "recommended_class": db_class.replace('large', 'small'),
                            "potential_savings": "40-60%",
                            "status": "recommendation"
                        })
                        
            except Exception as e:
                print(f"Error getting tags for RDS instance {db_identifier}: {str(e)}")
        
        return actions
        
    except Exception as e:
        print(f"Error optimizing RDS instances: {str(e)}")
        return [{"action": "optimize_rds", "status": "failed", "error": str(e)}]

def optimize_auto_scaling_groups(autoscaling_client, environment):
    """Optimize Auto Scaling Groups"""
    
    actions = []
    
    try:
        response = autoscaling_client.describe_auto_scaling_groups()
        
        for asg in response['AutoScalingGroups']:
            asg_name = asg['AutoScalingGroupName']
            
            # Check if ASG belongs to our environment
            tags = {tag['Key']: tag['Value'] for tag in asg.get('Tags', [])}
            
            if tags.get('Environment') == environment:
                current_capacity = asg['DesiredCapacity']
                min_size = asg['MinSize']
                max_size = asg['MaxSize']
                
                # Recommend capacity optimization for non-prod environments
                if environment in ['dev', 'test']:
                    current_hour = datetime.now().hour
                    current_weekday = datetime.now().weekday()
                    
                    # Scale down during off-hours
                    if (current_hour < 8 or current_hour > 18 or current_weekday >= 5) and current_capacity > 1:
                        try:
                            autoscaling_client.update_auto_scaling_group(
                                AutoScalingGroupName=asg_name,
                                DesiredCapacity=1,
                                MinSize=min_size,
                                MaxSize=max_size
                            )
                            actions.append({
                                "action": "scale_down_asg",
                                "asg_name": asg_name,
                                "previous_capacity": current_capacity,
                                "new_capacity": 1,
                                "reason": "outside_business_hours",
                                "status": "completed"
                            })
                        except Exception as e:
                            actions.append({
                                "action": "scale_down_asg",
                                "asg_name": asg_name,
                                "status": "failed",
                                "error": str(e)
                            })
        
        return actions
        
    except Exception as e:
        print(f"Error optimizing Auto Scaling Groups: {str(e)}")
        return [{"action": "optimize_asg", "status": "failed", "error": str(e)}]

def generate_cost_recommendations(cost_analysis):
    """Generate cost optimization recommendations"""
    
    recommendations = []
    
    if 'top_services' in cost_analysis:
        for service, cost in cost_analysis['top_services']:
            if service == 'Amazon Elastic Compute Cloud - Compute':
                recommendations.append({
                    "service": service,
                    "cost": cost,
                    "recommendations": [
                        "Consider using Spot Instances for non-critical workloads",
                        "Implement auto-scheduling for dev/test environments",
                        "Right-size instances based on actual usage",
                        "Use Reserved Instances for steady-state workloads"
                    ]
                })
            elif service == 'Amazon Relational Database Service':
                recommendations.append({
                    "service": service,
                    "cost": cost,
                    "recommendations": [
                        "Stop non-production databases during off-hours",
                        "Use read replicas only when necessary",
                        "Consider Aurora Serverless for variable workloads",
                        "Right-size database instances"
                    ]
                })
            elif service == 'Amazon Simple Storage Service':
                recommendations.append({
                    "service": service,
                    "cost": cost,
                    "recommendations": [
                        "Implement intelligent tiering",
                        "Set up lifecycle policies for old data",
                        "Use compression for large datasets",
                        "Clean up incomplete multipart uploads"
                    ]
                })
    
    return recommendations
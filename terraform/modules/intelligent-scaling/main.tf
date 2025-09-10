# Intelligent Auto-Scaling and Cost Optimization Module
# Implements predictive scaling with ML-driven cost optimization

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
  }
}

# ============================================================================
# LOCAL VARIABLES
# ============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Scaling metrics configuration
  scaling_metrics = {
    cpu_threshold     = 60
    memory_threshold  = 70
    request_threshold = 1000
    latency_threshold = 500  # milliseconds
    queue_threshold   = 50
  }
  
  # Cost optimization rules
  cost_rules = {
    spot_instance_ratio    = 0.7  # 70% spot instances
    reserved_capacity_min  = 0.3  # 30% reserved
    scale_down_cooldown   = 300   # 5 minutes
    scale_up_cooldown     = 60    # 1 minute
  }
}

# ============================================================================
# PREDICTIVE SCALING INFRASTRUCTURE
# ============================================================================

# Lambda function for predictive scaling algorithm
resource "aws_lambda_function" "predictive_scaler" {
  count = var.deploy_to_aws ? 1 : 0
  
  filename         = data.archive_file.predictive_scaler_zip[0].output_path
  function_name    = "${local.name_prefix}-predictive-scaler"
  role            = aws_iam_role.predictive_scaler_role[0].arn
  handler         = "predictive_scaler.lambda_handler"
  runtime         = "python3.11"
  timeout         = 300
  memory_size     = 1024
  
  environment {
    variables = {
      ENVIRONMENT             = var.environment
      CLUSTER_NAME           = var.cluster_name
      PROMETHEUS_ENDPOINT    = var.prometheus_endpoint
      PREDICTION_WINDOW      = "600"  # 10 minutes
      SCALING_FACTOR         = "1.2"
      COST_OPTIMIZATION_MODE = var.cost_optimization_enabled ? "true" : "false"
      MIN_REPLICAS           = tostring(var.min_replicas)
      MAX_REPLICAS           = tostring(var.max_replicas)
    }
  }
  
  layers = [aws_lambda_layer_version.ml_dependencies[0].arn]
  
  tags = var.tags
}

# Lambda layer for ML dependencies
resource "aws_lambda_layer_version" "ml_dependencies" {
  count = var.deploy_to_aws ? 1 : 0
  
  filename            = data.archive_file.ml_layer_zip[0].output_path
  layer_name          = "${local.name_prefix}-ml-dependencies"
  compatible_runtimes = ["python3.11"]
  description         = "ML dependencies for predictive scaling"
  
  source_code_hash = data.archive_file.ml_layer_zip[0].output_base64sha256
}

# Predictive scaler source code
data "archive_file" "predictive_scaler_zip" {
  count = var.deploy_to_aws ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/predictive_scaler.zip"
  
  source {
    content = templatefile("${path.module}/templates/predictive_scaler.py.tpl", {
      cluster_name = var.cluster_name
      environment  = var.environment
    })
    filename = "predictive_scaler.py"
  }
  
  source {
    content = "scikit-learn==1.3.2\nnumpy==1.24.3\npandas==2.1.4\nkubernetes==28.1.0\nprometheus-client==0.19.0"
    filename = "requirements.txt"
  }
}

# ML layer dependencies
data "archive_file" "ml_layer_zip" {
  count = var.deploy_to_aws ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/ml_layer.zip"
  
  source_dir = "${path.module}/layer"
}

# IAM role for predictive scaler
resource "aws_iam_role" "predictive_scaler_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-predictive-scaler-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
  
  tags = var.tags
}

# IAM policy for predictive scaler
resource "aws_iam_role_policy" "predictive_scaler_policy" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-predictive-scaler-policy"
  role = aws_iam_role.predictive_scaler_role[0].id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "eks:DescribeCluster",
          "eks:ListClusters",
          "cloudwatch:GetMetricData",
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = aws_dynamodb_table.scaling_history[0].arn
      }
    ]
  })
}

# DynamoDB table for scaling history and predictions
resource "aws_dynamodb_table" "scaling_history" {
  count = var.deploy_to_aws ? 1 : 0
  
  name           = "${local.name_prefix}-scaling-history"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "timestamp"
  range_key      = "service"
  
  attribute {
    name = "timestamp"
    type = "S"
  }
  
  attribute {
    name = "service"
    type = "S"
  }
  
  attribute {
    name = "date"
    type = "S"
  }
  
  global_secondary_index {
    name     = "DateIndex"
    hash_key = "date"
    
    projection_type = "ALL"
  }
  
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
  
  point_in_time_recovery {
    enabled = var.environment == "prod"
  }
  
  server_side_encryption {
    enabled = true
  }
  
  tags = var.tags
}

# CloudWatch Event Rule for predictive scaling
resource "aws_cloudwatch_event_rule" "predictive_scaling_schedule" {
  count = var.deploy_to_aws ? 1 : 0
  
  name                = "${local.name_prefix}-predictive-scaling"
  description         = "Trigger predictive scaling analysis"
  schedule_expression = "rate(5 minutes)"
  
  tags = var.tags
}

resource "aws_cloudwatch_event_target" "predictive_scaler_target" {
  count = var.deploy_to_aws ? 1 : 0
  
  rule      = aws_cloudwatch_event_rule.predictive_scaling_schedule[0].name
  target_id = "PredictiveScalerTarget"
  arn       = aws_lambda_function.predictive_scaler[0].arn
}

resource "aws_lambda_permission" "allow_cloudwatch_predictive" {
  count = var.deploy_to_aws ? 1 : 0
  
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.predictive_scaler[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.predictive_scaling_schedule[0].arn
}

# ============================================================================
# COST OPTIMIZATION ENGINE
# ============================================================================

# Lambda function for cost optimization
resource "aws_lambda_function" "cost_optimizer" {
  count = var.deploy_to_aws ? 1 : 0
  
  filename         = data.archive_file.cost_optimizer_zip[0].output_path
  function_name    = "${local.name_prefix}-cost-optimizer"
  role            = aws_iam_role.cost_optimizer_role[0].arn
  handler         = "cost_optimizer.lambda_handler"
  runtime         = "python3.11"
  timeout         = 900  # 15 minutes
  memory_size     = 1024
  
  environment {
    variables = {
      ENVIRONMENT                = var.environment
      CLUSTER_NAME              = var.cluster_name
      COST_TARGET_REDUCTION     = "40"  # 40% cost reduction target
      SPOT_INSTANCE_RATIO       = tostring(local.cost_rules.spot_instance_ratio)
      RESERVED_CAPACITY_MIN     = tostring(local.cost_rules.reserved_capacity_min)
      AUTO_SHUTDOWN_ENABLED     = var.auto_shutdown_enabled ? "true" : "false"
      RIGHTSIZING_ENABLED       = "true"
      UNUSED_RESOURCE_DETECTION = "true"
    }
  }
  
  tags = var.tags
}

# Cost optimizer source code
data "archive_file" "cost_optimizer_zip" {
  count = var.deploy_to_aws ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/cost_optimizer.zip"
  
  source {
    content = templatefile("${path.module}/templates/cost_optimizer.py.tpl", {
      cluster_name = var.cluster_name
      environment  = var.environment
    })
    filename = "cost_optimizer.py"
  }
}

# IAM role for cost optimizer
resource "aws_iam_role" "cost_optimizer_role" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-cost-optimizer-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
  
  tags = var.tags
}

# IAM policy for cost optimizer
resource "aws_iam_role_policy" "cost_optimizer_policy" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-cost-optimizer-policy"
  role = aws_iam_role.cost_optimizer_role[0].id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:DescribeInstances",
          "ec2:DescribeInstanceTypes",
          "ec2:ModifyInstanceAttribute",
          "autoscaling:DescribeAutoScalingGroups",
          "autoscaling:UpdateAutoScalingGroup",
          "autoscaling:SetDesiredCapacity",
          "eks:DescribeCluster",
          "eks:DescribeNodegroup",
          "eks:UpdateNodegroupConfig",
          "ce:GetCostAndUsage",
          "ce:GetUsageReport",
          "ce:GetReservationCoverage",
          "ce:GetReservationPurchaseRecommendation",
          "ce:GetReservationUtilization",
          "compute-optimizer:GetRecommendations",
          "compute-optimizer:GetEC2InstanceRecommendations",
          "compute-optimizer:GetAutoScalingGroupRecommendations"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = [
          aws_dynamodb_table.scaling_history[0].arn,
          aws_dynamodb_table.cost_recommendations[0].arn
        ]
      }
    ]
  })
}

# DynamoDB table for cost recommendations
resource "aws_dynamodb_table" "cost_recommendations" {
  count = var.deploy_to_aws ? 1 : 0
  
  name           = "${local.name_prefix}-cost-recommendations"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "resource_id"
  range_key      = "timestamp"
  
  attribute {
    name = "resource_id"
    type = "S"
  }
  
  attribute {
    name = "timestamp"
    type = "S"
  }
  
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
  
  tags = var.tags
}

# CloudWatch Event Rule for cost optimization
resource "aws_cloudwatch_event_rule" "cost_optimization_schedule" {
  count = var.deploy_to_aws ? 1 : 0
  
  name                = "${local.name_prefix}-cost-optimization"
  description         = "Trigger cost optimization analysis"
  schedule_expression = "cron(0 9 * * ? *)"  # Daily at 9 AM
  
  tags = var.tags
}

resource "aws_cloudwatch_event_target" "cost_optimizer_target" {
  count = var.deploy_to_aws ? 1 : 0
  
  rule      = aws_cloudwatch_event_rule.cost_optimization_schedule[0].name
  target_id = "CostOptimizerTarget"
  arn       = aws_lambda_function.cost_optimizer[0].arn
}

resource "aws_lambda_permission" "allow_cloudwatch_cost" {
  count = var.deploy_to_aws ? 1 : 0
  
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.cost_optimizer[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.cost_optimization_schedule[0].arn
}

# ============================================================================
# KUBERNETES CUSTOM RESOURCES
# ============================================================================

# Custom Resource Definition for Predictive Autoscaler
resource "kubernetes_manifest" "predictive_autoscaler_crd" {
  manifest = {
    apiVersion = "apiextensions.k8s.io/v1"
    kind       = "CustomResourceDefinition"
    metadata = {
      name = "predictiveautoscalers.scaling.pwc.io"
    }
    spec = {
      group = "scaling.pwc.io"
      versions = [
        {
          name    = "v1alpha1"
          served  = true
          storage = true
          schema = {
            openAPIV3Schema = {
              type = "object"
              properties = {
                spec = {
                  type = "object"
                  properties = {
                    targetRef = {
                      type = "object"
                      properties = {
                        apiVersion = { type = "string" }
                        kind       = { type = "string" }
                        name       = { type = "string" }
                      }
                    }
                    minReplicas = {
                      type    = "integer"
                      minimum = 1
                    }
                    maxReplicas = {
                      type    = "integer"
                      minimum = 1
                    }
                    predictionWindow = {
                      type = "string"
                    }
                    scaleUpThreshold = {
                      type = "number"
                    }
                    scaleDownThreshold = {
                      type = "number"
                    }
                    metrics = {
                      type = "array"
                      items = {
                        type = "object"
                        properties = {
                          name   = { type = "string" }
                          weight = { type = "number" }
                          source = {
                            type = "object"
                            properties = {
                              prometheus = {
                                type = "object"
                                properties = {
                                  query = { type = "string" }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                    algorithms = {
                      type = "array"
                      items = {
                        type = "object"
                        properties = {
                          name   = { type = "string" }
                          weight = { type = "number" }
                        }
                      }
                    }
                  }
                }
                status = {
                  type = "object"
                  properties = {
                    currentReplicas   = { type = "integer" }
                    desiredReplicas   = { type = "integer" }
                    predictedReplicas = { type = "integer" }
                    lastScaleTime     = { type = "string" }
                    conditions = {
                      type = "array"
                      items = {
                        type = "object"
                        properties = {
                          type               = { type = "string" }
                          status             = { type = "string" }
                          lastTransitionTime = { type = "string" }
                          reason             = { type = "string" }
                          message            = { type = "string" }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      ]
      scope = "Namespaced"
      names = {
        plural   = "predictiveautoscalers"
        singular = "predictiveautoscaler"
        kind     = "PredictiveAutoscaler"
      }
    }
  }
}

# ============================================================================
# MONITORING AND METRICS
# ============================================================================

# CloudWatch Dashboard for Scaling Metrics
resource "aws_cloudwatch_dashboard" "scaling_dashboard" {
  count = var.deploy_to_aws ? 1 : 0
  
  dashboard_name = "${local.name_prefix}-scaling-dashboard"
  
  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        
        properties = {
          metrics = [
            ["AWS/EKS", "cluster_node_count", "ClusterName", var.cluster_name],
            [".", "cluster_pod_count", ".", "."],
            ["Custom/Scaling", "predicted_replicas", "Service", "api"],
            [".", "actual_replicas", ".", "."]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Cluster Scaling Metrics"
          period  = 300
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        
        properties = {
          metrics = [
            ["Custom/CostOptimization", "cost_savings_percentage", "Environment", var.environment],
            [".", "spot_instance_utilization", ".", "."],
            [".", "resource_utilization", ".", "."]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Cost Optimization Metrics"
          period  = 300
        }
      }
    ]
  })
}

# CloudWatch Alarms for Scaling Events
resource "aws_cloudwatch_metric_alarm" "scaling_prediction_accuracy" {
  count = var.deploy_to_aws ? 1 : 0
  
  alarm_name          = "${local.name_prefix}-scaling-prediction-accuracy"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = "3"
  metric_name         = "prediction_accuracy"
  namespace           = "Custom/Scaling"
  period              = "300"
  statistic           = "Average"
  threshold           = "80"
  alarm_description   = "Scaling prediction accuracy below 80%"
  alarm_actions       = [aws_sns_topic.scaling_alerts[0].arn]
  
  dimensions = {
    Service = "predictive-scaler"
  }
  
  tags = var.tags
}

resource "aws_cloudwatch_metric_alarm" "cost_optimization_target" {
  count = var.deploy_to_aws ? 1 : 0
  
  alarm_name          = "${local.name_prefix}-cost-optimization-target"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "cost_savings_percentage"
  namespace           = "Custom/CostOptimization"
  period              = "86400"  # Daily
  statistic           = "Average"
  threshold           = "30"  # Alert if less than 30% cost savings
  alarm_description   = "Cost optimization target not met"
  alarm_actions       = [aws_sns_topic.scaling_alerts[0].arn]
  
  tags = var.tags
}

# SNS Topic for Scaling Alerts
resource "aws_sns_topic" "scaling_alerts" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-scaling-alerts"
  
  tags = var.tags
}

# ============================================================================
# SPOT INSTANCE FLEET MANAGEMENT
# ============================================================================

# Spot Fleet Request for Cost-Optimized Scaling
resource "aws_spot_fleet_request" "worker_fleet" {
  count = var.deploy_to_aws && var.enable_spot_fleet ? 1 : 0
  
  iam_fleet_role                      = aws_iam_role.spot_fleet_role[0].arn
  allocation_strategy                 = "diversified"
  target_capacity                     = var.spot_fleet_target_capacity
  valid_until                        = timeadd(timestamp(), "24h")
  replace_unhealthy_instances        = true
  instance_interruption_behaviour    = "terminate"
  fleet_type                         = "maintain"
  
  launch_specification {
    image_id                    = var.worker_ami_id
    instance_type              = var.spot_instance_types[0]
    key_name                   = var.ec2_key_name
    security_groups            = [var.worker_security_group_id]
    subnet_id                  = var.private_subnet_ids[0]
    associate_public_ip_address = false
    spot_price                 = var.spot_max_price
    
    user_data = base64encode(templatefile("${path.module}/templates/worker_userdata.sh.tpl", {
      cluster_name = var.cluster_name
      environment  = var.environment
    }))
    
    root_block_device {
      volume_type = "gp3"
      volume_size = 50
      encrypted   = true
    }
    
    tags = merge(var.tags, {
      Name = "${local.name_prefix}-spot-worker"
      Type = "spot-instance"
    })
  }
  
  dynamic "launch_specification" {
    for_each = slice(var.spot_instance_types, 1, length(var.spot_instance_types))
    content {
      image_id                    = var.worker_ami_id
      instance_type              = launch_specification.value
      key_name                   = var.ec2_key_name
      security_groups            = [var.worker_security_group_id]
      subnet_id                  = var.private_subnet_ids[count.index % length(var.private_subnet_ids)]
      associate_public_ip_address = false
      spot_price                 = var.spot_max_price
      
      user_data = base64encode(templatefile("${path.module}/templates/worker_userdata.sh.tpl", {
        cluster_name = var.cluster_name
        environment  = var.environment
      }))
      
      root_block_device {
        volume_type = "gp3"
        volume_size = 50
        encrypted   = true
      }
      
      tags = merge(var.tags, {
        Name = "${local.name_prefix}-spot-worker-${launch_specification.value}"
        Type = "spot-instance"
      })
    }
  }
  
  tags = var.tags
}

# IAM role for Spot Fleet
resource "aws_iam_role" "spot_fleet_role" {
  count = var.deploy_to_aws && var.enable_spot_fleet ? 1 : 0
  
  name = "${local.name_prefix}-spot-fleet-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "spotfleet.amazonaws.com"
      }
    }]
  })
  
  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "spot_fleet_policy" {
  count = var.deploy_to_aws && var.enable_spot_fleet ? 1 : 0
  
  role       = aws_iam_role.spot_fleet_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole"
}
# Intelligent Infrastructure Optimization Module
# Advanced resource utilization analysis and automated optimization

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.70"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
    }
  }
}

# ============================================================================
# LOCAL VARIABLES
# ============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Optimization strategies
  optimization_rules = {
    cpu_rightsizing = {
      enabled = var.cpu_rightsizing_enabled
      threshold_low = var.cpu_threshold_low
      threshold_high = var.cpu_threshold_high
      analysis_period_days = var.analysis_period_days
    }
    memory_rightsizing = {
      enabled = var.memory_rightsizing_enabled
      threshold_low = var.memory_threshold_low
      threshold_high = var.memory_threshold_high
      analysis_period_days = var.analysis_period_days
    }
    predictive_scaling = {
      enabled = var.predictive_scaling_enabled
      seasonality_patterns = var.seasonality_patterns
      scaling_buffer = var.scaling_buffer_percentage
    }
    cost_anomaly_detection = {
      enabled = var.anomaly_detection_enabled
      threshold_percentage = var.anomaly_threshold_percentage
      evaluation_periods = var.anomaly_evaluation_periods
    }
  }
  
  # Resource tagging strategy
  resource_tags = merge(var.tags, {
    IntelligentOptimization = "enabled"
    AutomatedRightsizing   = var.cpu_rightsizing_enabled ? "enabled" : "disabled"
    PredictiveScaling      = var.predictive_scaling_enabled ? "enabled" : "disabled"
    CostAnomalyDetection   = var.anomaly_detection_enabled ? "enabled" : "disabled"
    OptimizationLevel      = var.optimization_level
    LastOptimized         = timestamp()
  })
}

# ============================================================================
# AWS INTELLIGENT INFRASTRUCTURE OPTIMIZATION
# ============================================================================

# AWS Lambda for Resource Utilization Analysis
resource "aws_lambda_function" "resource_analyzer" {
  count = var.deploy_to_aws ? 1 : 0
  
  filename         = data.archive_file.resource_analyzer_zip[0].output_path
  function_name    = "${local.name_prefix}-resource-analyzer"
  role            = aws_iam_role.resource_analyzer[0].arn
  handler         = "index.lambda_handler"
  runtime         = "python3.11"
  timeout         = 900
  memory_size     = 1024
  
  environment {
    variables = {
      OPTIMIZATION_RULES    = jsonencode(local.optimization_rules)
      SNS_TOPIC_ARN        = aws_sns_topic.optimization_alerts[0].arn
      CLOUDWATCH_NAMESPACE = "${local.name_prefix}/Optimization"
      ANALYSIS_PERIOD_DAYS = var.analysis_period_days
      CPU_THRESHOLD_LOW    = var.cpu_threshold_low
      MEMORY_THRESHOLD_LOW = var.memory_threshold_low
      COST_THRESHOLD       = var.cost_anomaly_threshold
    }
  }
  
  tags = local.resource_tags
}

# Lambda code for resource analyzer
data "archive_file" "resource_analyzer_zip" {
  count = var.deploy_to_aws ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/resource_analyzer.zip"
  
  source {
    content = templatefile("${path.module}/templates/resource_analyzer.py.tpl", {
      environment = var.environment
      project_name = var.project_name
    })
    filename = "index.py"
  }
  
  source {
    content = file("${path.module}/templates/requirements.txt")
    filename = "requirements.txt"
  }
}

# IAM Role for Resource Analyzer Lambda
resource "aws_iam_role" "resource_analyzer" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-resource-analyzer-role"
  
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
  
  tags = local.resource_tags
}

# IAM Policy for Resource Analyzer
resource "aws_iam_role_policy" "resource_analyzer" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-resource-analyzer-policy"
  role = aws_iam_role.resource_analyzer[0].id
  
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
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:GetMetricData",
          "cloudwatch:PutMetricData",
          "autoscaling:DescribeAutoScalingGroups",
          "autoscaling:UpdateAutoScalingGroup",
          "autoscaling:PutScalingPolicy",
          "ce:GetCostAndUsage",
          "ce:GetUsageReport",
          "ce:GetRightsizingRecommendation",
          "sns:Publish",
          "ssm:GetParameter",
          "ssm:PutParameter",
          "rds:DescribeDBInstances",
          "rds:DescribeDBClusters",
          "rds:ModifyDBInstance",
          "rds:ModifyDBCluster"
        ]
        Resource = "*"
      }
    ]
  })
}

# EventBridge Rule for Scheduled Analysis
resource "aws_cloudwatch_event_rule" "resource_analysis_schedule" {
  count = var.deploy_to_aws ? 1 : 0
  
  name                = "${local.name_prefix}-resource-analysis"
  description         = "Trigger resource utilization analysis"
  schedule_expression = var.analysis_schedule
  
  tags = local.resource_tags
}

resource "aws_cloudwatch_event_target" "resource_analyzer" {
  count = var.deploy_to_aws ? 1 : 0
  
  rule      = aws_cloudwatch_event_rule.resource_analysis_schedule[0].name
  target_id = "ResourceAnalyzerTarget"
  arn       = aws_lambda_function.resource_analyzer[0].arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  count = var.deploy_to_aws ? 1 : 0
  
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.resource_analyzer[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.resource_analysis_schedule[0].arn
}

# ============================================================================
# PREDICTIVE SCALING IMPLEMENTATION
# ============================================================================

# Lambda for Predictive Scaling Engine
resource "aws_lambda_function" "predictive_scaler" {
  count = var.deploy_to_aws && var.predictive_scaling_enabled ? 1 : 0
  
  filename         = data.archive_file.predictive_scaler_zip[0].output_path
  function_name    = "${local.name_prefix}-predictive-scaler"
  role            = aws_iam_role.predictive_scaler[0].arn
  handler         = "index.lambda_handler"
  runtime         = "python3.11"
  timeout         = 900
  memory_size     = 2048
  
  environment {
    variables = {
      SEASONALITY_PATTERNS = jsonencode(var.seasonality_patterns)
      SCALING_BUFFER      = var.scaling_buffer_percentage
      PREDICTION_WINDOW   = var.prediction_window_hours
      ML_MODEL_S3_BUCKET  = aws_s3_bucket.ml_models[0].bucket
      CLOUDWATCH_NAMESPACE = "${local.name_prefix}/PredictiveScaling"
    }
  }
  
  tags = local.resource_tags
}

data "archive_file" "predictive_scaler_zip" {
  count = var.deploy_to_aws && var.predictive_scaling_enabled ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/predictive_scaler.zip"
  
  source {
    content = templatefile("${path.module}/templates/predictive_scaler.py.tpl", {
      environment = var.environment
      project_name = var.project_name
    })
    filename = "index.py"
  }
  
  source {
    content = file("${path.module}/templates/ml_requirements.txt")
    filename = "requirements.txt"
  }
}

# IAM Role for Predictive Scaler
resource "aws_iam_role" "predictive_scaler" {
  count = var.deploy_to_aws && var.predictive_scaling_enabled ? 1 : 0
  
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
  
  tags = local.resource_tags
}

# IAM Policy for Predictive Scaler
resource "aws_iam_role_policy" "predictive_scaler" {
  count = var.deploy_to_aws && var.predictive_scaling_enabled ? 1 : 0
  
  name = "${local.name_prefix}-predictive-scaler-policy"
  role = aws_iam_role.predictive_scaler[0].id
  
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
          "cloudwatch:GetMetricStatistics",
          "cloudwatch:GetMetricData",
          "cloudwatch:PutMetricData",
          "autoscaling:DescribeAutoScalingGroups",
          "autoscaling:PutScalingPolicy",
          "autoscaling:SetDesiredCapacity",
          "s3:GetObject",
          "s3:PutObject",
          "sns:Publish",
          "application-autoscaling:*"
        ]
        Resource = "*"
      }
    ]
  })
}

# S3 Bucket for ML Models
resource "aws_s3_bucket" "ml_models" {
  count = var.deploy_to_aws && var.predictive_scaling_enabled ? 1 : 0
  
  bucket = "${local.name_prefix}-ml-models-${random_string.ml_bucket_suffix.result}"
  
  tags = local.resource_tags
}

resource "aws_s3_bucket_versioning" "ml_models" {
  count = var.deploy_to_aws && var.predictive_scaling_enabled ? 1 : 0
  
  bucket = aws_s3_bucket.ml_models[0].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "random_string" "ml_bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# ============================================================================
# AUTOMATED RESOURCE TAGGING
# ============================================================================

# Lambda for Automated Resource Tagging
resource "aws_lambda_function" "resource_tagger" {
  count = var.deploy_to_aws && var.automated_tagging_enabled ? 1 : 0
  
  filename         = data.archive_file.resource_tagger_zip[0].output_path
  function_name    = "${local.name_prefix}-resource-tagger"
  role            = aws_iam_role.resource_tagger[0].arn
  handler         = "index.lambda_handler"
  runtime         = "python3.11"
  timeout         = 300
  memory_size     = 512
  
  environment {
    variables = {
      TAGGING_RULES       = jsonencode(var.tagging_rules)
      COST_CENTER         = var.cost_center
      BUSINESS_UNIT       = var.business_unit
      PROJECT_NAME        = var.project_name
      ENVIRONMENT         = var.environment
      COMPLIANCE_TAGS     = jsonencode(var.compliance_tags)
      AUTO_TAG_RESOURCES  = jsonencode(var.auto_tag_resource_types)
    }
  }
  
  tags = local.resource_tags
}

data "archive_file" "resource_tagger_zip" {
  count = var.deploy_to_aws && var.automated_tagging_enabled ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/resource_tagger.zip"
  
  source {
    content = templatefile("${path.module}/templates/resource_tagger.py.tpl", {
      environment = var.environment
    })
    filename = "index.py"
  }
}

# IAM Role for Resource Tagger
resource "aws_iam_role" "resource_tagger" {
  count = var.deploy_to_aws && var.automated_tagging_enabled ? 1 : 0
  
  name = "${local.name_prefix}-resource-tagger-role"
  
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
  
  tags = local.resource_tags
}

# IAM Policy for Resource Tagger
resource "aws_iam_role_policy" "resource_tagger" {
  count = var.deploy_to_aws && var.automated_tagging_enabled ? 1 : 0
  
  name = "${local.name_prefix}-resource-tagger-policy"
  role = aws_iam_role.resource_tagger[0].id
  
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
          "ec2:CreateTags",
          "ec2:DescribeTags",
          "ec2:DescribeInstances",
          "ec2:DescribeVolumes",
          "ec2:DescribeSnapshots",
          "rds:AddTagsToResource",
          "rds:ListTagsForResource",
          "rds:DescribeDBInstances",
          "s3:PutBucketTagging",
          "s3:GetBucketTagging",
          "lambda:TagResource",
          "lambda:ListTags",
          "iam:TagRole",
          "iam:TagPolicy",
          "cloudformation:DescribeStacks",
          "tag:GetResources",
          "tag:TagResources",
          "tag:UntagResources"
        ]
        Resource = "*"
      }
    ]
  })
}

# EventBridge Rule for Resource Creation Events
resource "aws_cloudwatch_event_rule" "resource_creation" {
  count = var.deploy_to_aws && var.automated_tagging_enabled ? 1 : 0
  
  name        = "${local.name_prefix}-resource-creation"
  description = "Trigger resource tagging on resource creation"
  
  event_pattern = jsonencode({
    "source": ["aws.ec2", "aws.rds", "aws.s3", "aws.lambda"]
    "detail-type": [
      "EC2 Instance State-change Notification",
      "RDS DB Instance Event",
      "S3 Bucket Notification",
      "AWS API Call via CloudTrail"
    ]
    "detail": {
      "state": ["running", "available"]
    }
  })
  
  tags = local.resource_tags
}

resource "aws_cloudwatch_event_target" "resource_tagger" {
  count = var.deploy_to_aws && var.automated_tagging_enabled ? 1 : 0
  
  rule      = aws_cloudwatch_event_rule.resource_creation[0].name
  target_id = "ResourceTaggerTarget"
  arn       = aws_lambda_function.resource_tagger[0].arn
}

resource "aws_lambda_permission" "allow_eventbridge_tagger" {
  count = var.deploy_to_aws && var.automated_tagging_enabled ? 1 : 0
  
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.resource_tagger[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.resource_creation[0].arn
}

# ============================================================================
# RESOURCE LIFECYCLE MANAGEMENT
# ============================================================================

# Lambda for Lifecycle Management
resource "aws_lambda_function" "lifecycle_manager" {
  count = var.deploy_to_aws && var.lifecycle_management_enabled ? 1 : 0
  
  filename         = data.archive_file.lifecycle_manager_zip[0].output_path
  function_name    = "${local.name_prefix}-lifecycle-manager"
  role            = aws_iam_role.lifecycle_manager[0].arn
  handler         = "index.lambda_handler"
  runtime         = "python3.11"
  timeout         = 900
  memory_size     = 1024
  
  environment {
    variables = {
      LIFECYCLE_POLICIES    = jsonencode(var.lifecycle_policies)
      AUTO_SHUTDOWN_RULES   = jsonencode(var.auto_shutdown_rules)
      RETENTION_POLICIES    = jsonencode(var.retention_policies)
      BACKUP_POLICIES       = jsonencode(var.backup_policies)
      SNS_TOPIC_ARN        = aws_sns_topic.optimization_alerts[0].arn
      DRY_RUN_MODE         = var.lifecycle_dry_run_mode
    }
  }
  
  tags = local.resource_tags
}

data "archive_file" "lifecycle_manager_zip" {
  count = var.deploy_to_aws && var.lifecycle_management_enabled ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/lifecycle_manager.zip"
  
  source {
    content = templatefile("${path.module}/templates/lifecycle_manager.py.tpl", {
      environment = var.environment
    })
    filename = "index.py"
  }
}

# IAM Role for Lifecycle Manager
resource "aws_iam_role" "lifecycle_manager" {
  count = var.deploy_to_aws && var.lifecycle_management_enabled ? 1 : 0
  
  name = "${local.name_prefix}-lifecycle-manager-role"
  
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
  
  tags = local.resource_tags
}

# IAM Policy for Lifecycle Manager
resource "aws_iam_role_policy" "lifecycle_manager" {
  count = var.deploy_to_aws && var.lifecycle_management_enabled ? 1 : 0
  
  name = "${local.name_prefix}-lifecycle-manager-policy"
  role = aws_iam_role.lifecycle_manager[0].id
  
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
          "ec2:StartInstances",
          "ec2:StopInstances",
          "ec2:TerminateInstances",
          "ec2:CreateSnapshot",
          "ec2:DeleteSnapshot",
          "ec2:DescribeSnapshots",
          "rds:DescribeDBInstances",
          "rds:StartDBInstance",
          "rds:StopDBInstance",
          "rds:DeleteDBInstance",
          "rds:CreateDBSnapshot",
          "s3:PutLifecycleConfiguration",
          "s3:GetLifecycleConfiguration",
          "s3:DeleteObject",
          "s3:DeleteObjectVersion",
          "backup:StartBackupJob",
          "backup:DescribeBackupJob",
          "sns:Publish",
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
      }
    ]
  })
}

# ============================================================================
# COST AND PERFORMANCE MONITORING
# ============================================================================

# SNS Topic for Optimization Alerts
resource "aws_sns_topic" "optimization_alerts" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-optimization-alerts"
  
  tags = local.resource_tags
}

resource "aws_sns_topic_subscription" "optimization_email" {
  count = var.deploy_to_aws && var.alert_email != "" ? 1 : 0
  
  topic_arn = aws_sns_topic.optimization_alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# CloudWatch Dashboard for Infrastructure Optimization
resource "aws_cloudwatch_dashboard" "infrastructure_optimization" {
  count = var.deploy_to_aws ? 1 : 0
  
  dashboard_name = "${local.name_prefix}-infrastructure-optimization"
  
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
            ["${local.name_prefix}/Optimization", "ResourceUtilization", "ResourceType", "EC2"],
            ["...", "RDS"],
            ["...", "Lambda"]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Resource Utilization Overview"
          period  = 300
          yAxis = {
            left = {
              min = 0
              max = 100
            }
          }
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        
        properties = {
          metrics = [
            ["${local.name_prefix}/PredictiveScaling", "PredictedLoad"],
            ["${local.name_prefix}/PredictiveScaling", "ActualLoad"]
          ]
          view   = "timeSeries"
          region = var.aws_region
          title  = "Predictive vs Actual Load"
          period = 300
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 24
        height = 6
        
        properties = {
          metrics = [
            ["AWS/Billing", "EstimatedCharges", "Currency", "USD"],
            ["${local.name_prefix}/Optimization", "CostSavings"]
          ]
          view   = "timeSeries"
          region = var.aws_region
          title  = "Cost Optimization Impact"
          period = 3600
        }
      }
    ]
  })
}

# CloudWatch Alarms for Cost and Performance
resource "aws_cloudwatch_metric_alarm" "high_cost_anomaly" {
  count = var.deploy_to_aws ? 1 : 0
  
  alarm_name          = "${local.name_prefix}-high-cost-anomaly"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "EstimatedCharges"
  namespace           = "AWS/Billing"
  period              = "86400"
  statistic           = "Maximum"
  threshold           = var.cost_anomaly_threshold
  alarm_description   = "High cost anomaly detected"
  alarm_actions       = [aws_sns_topic.optimization_alerts[0].arn]
  
  dimensions = {
    Currency = "USD"
  }
  
  tags = local.resource_tags
}

resource "aws_cloudwatch_metric_alarm" "low_utilization" {
  count = var.deploy_to_aws ? 1 : 0
  
  alarm_name          = "${local.name_prefix}-low-utilization"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = "3"
  metric_name         = "ResourceUtilization"
  namespace           = "${local.name_prefix}/Optimization"
  period              = "3600"
  statistic           = "Average"
  threshold           = var.low_utilization_threshold
  alarm_description   = "Consistently low resource utilization detected"
  alarm_actions       = [aws_sns_topic.optimization_alerts[0].arn]
  
  tags = local.resource_tags
}

# ============================================================================
# AZURE INTELLIGENT INFRASTRUCTURE
# ============================================================================

# Azure Logic App for Resource Optimization
resource "azurerm_logic_app_workflow" "resource_optimizer" {
  count = var.deploy_to_azure ? 1 : 0
  
  name                = "${local.name_prefix}-resource-optimizer"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  
  tags = local.resource_tags
}

# Azure Automation Runbook for Resource Management
resource "azurerm_automation_runbook" "resource_lifecycle" {
  count = var.deploy_to_azure && var.lifecycle_management_enabled ? 1 : 0
  
  name                    = "${local.name_prefix}-resource-lifecycle"
  location               = var.azure_location
  resource_group_name    = var.azure_resource_group_name
  automation_account_name = var.azure_automation_account_name
  log_verbose            = "true"
  log_progress           = "true"
  description           = "Automated resource lifecycle management"
  runbook_type          = "PowerShell"
  
  content = templatefile("${path.module}/templates/azure_resource_lifecycle.ps1.tpl", {
    environment      = var.environment
    resource_group   = var.azure_resource_group_name
  })
  
  tags = local.resource_tags
}

# ============================================================================
# GCP INTELLIGENT INFRASTRUCTURE
# ============================================================================

# GCP Cloud Function for Resource Analysis
resource "google_cloudfunctions_function" "resource_optimizer" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name        = "${local.name_prefix}-resource-optimizer"
  description = "Intelligent resource optimization for GCP"
  runtime     = "python39"
  
  available_memory_mb   = 1024
  source_archive_bucket = google_storage_bucket.function_source[0].name
  source_archive_object = google_storage_bucket_object.function_source[0].name
  
  trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.optimization_trigger[0].name
  }
  
  entry_point = "optimize_resources"
  timeout     = 540
  
  environment_variables = {
    PROJECT_ID           = var.gcp_project_id
    ENVIRONMENT          = var.environment
    OPTIMIZATION_LEVEL   = var.optimization_level
    COST_THRESHOLD       = var.cost_anomaly_threshold
  }
  
  labels = local.resource_tags
}

# GCP Storage for Cloud Function Source
resource "google_storage_bucket" "function_source" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name     = "${local.name_prefix}-optimization-functions-${random_string.gcp_bucket_suffix.result}"
  location = var.gcp_region
  
  labels = local.resource_tags
}

resource "google_storage_bucket_object" "function_source" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name   = "resource-optimizer-source.zip"
  bucket = google_storage_bucket.function_source[0].name
  source = data.archive_file.gcp_optimizer_zip[0].output_path
}

data "archive_file" "gcp_optimizer_zip" {
  count = var.deploy_to_gcp ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/gcp_resource_optimizer.zip"
  
  source {
    content = templatefile("${path.module}/templates/gcp_resource_optimizer.py.tpl", {
      project_id = var.gcp_project_id
    })
    filename = "main.py"
  }
  
  source {
    content = "google-cloud-compute==1.14.0\ngoogle-cloud-monitoring==2.15.1\ngoogle-cloud-billing==1.12.0\nnumpy==1.24.3\npandas==2.0.3"
    filename = "requirements.txt"
  }
}

# GCP Pub/Sub Topic for Optimization Triggers
resource "google_pubsub_topic" "optimization_trigger" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name = "${local.name_prefix}-optimization-trigger"
  
  labels = local.resource_tags
}

# GCP Cloud Scheduler for Regular Optimization
resource "google_cloud_scheduler_job" "optimization_schedule" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name     = "${local.name_prefix}-optimization-schedule"
  schedule = var.analysis_schedule
  
  pubsub_target {
    topic_name = google_pubsub_topic.optimization_trigger[0].id
    data       = base64encode(jsonencode({
      action = "analyze_and_optimize"
      config = local.optimization_rules
    }))
  }
}

resource "random_string" "gcp_bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# ============================================================================
# SHARED OUTPUTS
# ============================================================================

output "optimization_dashboard_url" {
  description = "URL for the optimization dashboard"
  value = var.deploy_to_aws ? "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.infrastructure_optimization[0].dashboard_name}" : null
}

output "cost_savings_estimate" {
  description = "Estimated monthly cost savings"
  value = {
    cpu_rightsizing = var.cpu_rightsizing_enabled ? "15-25%" : "0%"
    memory_rightsizing = var.memory_rightsizing_enabled ? "10-20%" : "0%"
    predictive_scaling = var.predictive_scaling_enabled ? "20-30%" : "0%"
    lifecycle_management = var.lifecycle_management_enabled ? "5-15%" : "0%"
  }
}
# Multi-Cloud Cost Optimization Module
# Implements intelligent cost management and governance

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
  }
}

# ============================================================================
# LOCAL VARIABLES
# ============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Cost optimization strategies
  cost_strategies = ["rightsizing", "scheduling", "reserved_instances", "spot_instances", "lifecycle_policies"]
  
  # Resource tagging for cost allocation
  cost_tags = {
    CostCenter       = var.cost_center
    BusinessUnit     = var.business_unit
    Environment      = var.environment
    AutoShutdown     = var.auto_shutdown_enabled
    CostOptimization = "enabled"
    BudgetTracking   = "enabled"
  }
}

# ============================================================================
# AWS COST OPTIMIZATION
# ============================================================================

# AWS Budgets
resource "aws_budgets_budget" "main" {
  count = var.deploy_to_aws ? 1 : 0
  
  name     = "${local.name_prefix}-monthly-budget"
  budget_type = "COST"
  limit_amount = var.monthly_budget_limit
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
  time_period_start = "2024-01-01_00:00"
  
  cost_filters {
    tag {
      key = "Project"
      values = [var.project_name]
    }
  }
  
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                 = var.budget_alert_threshold
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
  
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                 = var.budget_alert_threshold - 20
    threshold_type            = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
  
  tags = merge(var.tags, local.cost_tags)
}

# AWS Cost Anomaly Detection
resource "aws_ce_anomaly_detector" "main" {
  count = var.deploy_to_aws ? 1 : 0
  
  name         = "${local.name_prefix}-anomaly-detector"
  monitor_type = "DIMENSIONAL"
  
  specification = jsonencode({
    Dimension = "SERVICE"
    MatchOptions = ["EQUALS"]
    Values = ["EC2-Instance", "Amazon Relational Database Service", "Amazon Simple Storage Service"]
  })
  
  tags = var.tags
}

# AWS Savings Plans Recommendation
resource "aws_ce_cost_category" "main" {
  count = var.deploy_to_aws ? 1 : 0
  
  name         = "${local.name_prefix}-cost-category"
  rule_version = "CostCategoryExpression.v1"
  
  rule {
    value = "Development"
    rule {
      and {
        tag {
          key           = "Environment"
          values        = ["dev", "test"]
          match_options = ["EQUALS"]
        }
      }
    }
  }
  
  rule {
    value = "Production"
    rule {
      and {
        tag {
          key           = "Environment"
          values        = ["prod", "production"]
          match_options = ["EQUALS"]
        }
      }
    }
  }
  
  tags = var.tags
}

# Auto Scaling Groups for Cost Optimization
resource "aws_autoscaling_schedule" "shutdown_weekends" {
  count = var.deploy_to_aws && var.auto_shutdown_enabled && var.environment != "prod" ? 1 : 0
  
  scheduled_action_name  = "${local.name_prefix}-weekend-shutdown"
  min_size              = 0
  max_size              = 0
  desired_capacity      = 0
  recurrence            = "0 18 * * 5"  # Friday 6 PM
  autoscaling_group_name = var.asg_name
}

resource "aws_autoscaling_schedule" "startup_monday" {
  count = var.deploy_to_aws && var.auto_shutdown_enabled && var.environment != "prod" ? 1 : 0
  
  scheduled_action_name  = "${local.name_prefix}-monday-startup"
  min_size              = var.asg_min_size
  max_size              = var.asg_max_size
  desired_capacity      = var.asg_desired_size
  recurrence            = "0 8 * * 1"   # Monday 8 AM
  autoscaling_group_name = var.asg_name
}

# Lambda function for cost optimization
resource "aws_lambda_function" "cost_optimizer" {
  count = var.deploy_to_aws ? 1 : 0
  
  filename         = data.archive_file.cost_optimizer_zip[0].output_path
  function_name    = "${local.name_prefix}-cost-optimizer"
  role            = aws_iam_role.cost_optimizer[0].arn
  handler         = "index.lambda_handler"
  runtime         = "python3.11"
  timeout         = 300
  
  environment {
    variables = {
      ENVIRONMENT           = var.environment
      COST_CENTER          = var.cost_center
      BUDGET_THRESHOLD     = var.budget_alert_threshold
      AUTO_SHUTDOWN_ENABLED = var.auto_shutdown_enabled
    }
  }
  
  tags = var.tags
}

# Lambda function code
data "archive_file" "cost_optimizer_zip" {
  count = var.deploy_to_aws ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/cost_optimizer.zip"
  
  source {
    content = templatefile("${path.module}/templates/cost_optimizer.py.tpl", {
      environment = var.environment
      cost_center = var.cost_center
    })
    filename = "index.py"
  }
}

# IAM Role for Cost Optimizer Lambda
resource "aws_iam_role" "cost_optimizer" {
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

# IAM Policy for Cost Optimizer
resource "aws_iam_role_policy" "cost_optimizer" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-cost-optimizer-policy"
  role = aws_iam_role.cost_optimizer[0].id
  
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
          "ec2:StopInstances",
          "ec2:StartInstances",
          "ec2:DescribeInstanceTypes",
          "autoscaling:DescribeAutoScalingGroups",
          "autoscaling:UpdateAutoScalingGroup",
          "rds:DescribeDBInstances",
          "rds:StopDBInstance",
          "rds:StartDBInstance",
          "ce:GetCostAndUsage",
          "ce:GetUsageReport"
        ]
        Resource = "*"
      }
    ]
  })
}

# CloudWatch Event Rule for Cost Optimization
resource "aws_cloudwatch_event_rule" "cost_optimization_schedule" {
  count = var.deploy_to_aws ? 1 : 0
  
  name                = "${local.name_prefix}-cost-optimization"
  description         = "Trigger cost optimization lambda"
  schedule_expression = "cron(0 9 * * ? *)"  # Daily at 9 AM
  
  tags = var.tags
}

resource "aws_cloudwatch_event_target" "cost_optimizer" {
  count = var.deploy_to_aws ? 1 : 0
  
  rule      = aws_cloudwatch_event_rule.cost_optimization_schedule[0].name
  target_id = "CostOptimizerTarget"
  arn       = aws_lambda_function.cost_optimizer[0].arn
}

resource "aws_lambda_permission" "allow_cloudwatch" {
  count = var.deploy_to_aws ? 1 : 0
  
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.cost_optimizer[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.cost_optimization_schedule[0].arn
}

# ============================================================================
# AZURE COST OPTIMIZATION
# ============================================================================

# Azure Consumption Budget
resource "azurerm_consumption_budget_subscription" "main" {
  count = var.deploy_to_azure ? 1 : 0
  
  name            = "${local.name_prefix}-monthly-budget"
  subscription_id = data.azurerm_client_config.current.subscription_id
  
  amount     = var.monthly_budget_limit
  time_grain = "Monthly"
  
  time_period {
    start_date = "2024-01-01T00:00:00Z"
  }
  
  notification {
    enabled   = true
    threshold = var.budget_alert_threshold
    operator  = "GreaterThan"
    
    contact_emails = [var.alert_email]
  }
  
  notification {
    enabled   = true
    threshold = var.budget_alert_threshold - 20
    operator  = "GreaterThan"
    threshold_type = "Forecasted"
    
    contact_emails = [var.alert_email]
  }
  
  filter {
    tag {
      name = "Project"
      values = [var.project_name]
    }
  }
}

# Azure Automation Account for Cost Optimization
resource "azurerm_automation_account" "cost_optimization" {
  count = var.deploy_to_azure ? 1 : 0
  
  name                = "${local.name_prefix}-automation"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  sku_name           = "Basic"
  
  identity {
    type = "SystemAssigned"
  }
  
  tags = merge(var.tags, local.cost_tags)
}

# Azure Runbook for Auto Shutdown
resource "azurerm_automation_runbook" "auto_shutdown" {
  count = var.deploy_to_azure && var.auto_shutdown_enabled ? 1 : 0
  
  name                    = "${local.name_prefix}-auto-shutdown"
  location               = var.azure_location
  resource_group_name    = var.azure_resource_group_name
  automation_account_name = azurerm_automation_account.cost_optimization[0].name
  log_verbose            = "true"
  log_progress           = "true"
  description           = "Automated shutdown for cost optimization"
  runbook_type          = "PowerShell"
  
  content = templatefile("${path.module}/templates/azure_auto_shutdown.ps1.tpl", {
    environment      = var.environment
    resource_group   = var.azure_resource_group_name
  })
  
  tags = var.tags
}

# ============================================================================
# GCP COST OPTIMIZATION
# ============================================================================

# GCP Budget
resource "google_billing_budget" "main" {
  count = var.deploy_to_gcp ? 1 : 0
  
  billing_account = var.gcp_billing_account
  display_name    = "${local.name_prefix}-monthly-budget"
  
  budget_filter {
    projects = ["projects/${var.gcp_project_id}"]
    
    labels = {
      "project" = var.project_name
    }
  }
  
  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.monthly_budget_limit)
    }
  }
  
  threshold_rules {
    threshold_percent = var.budget_alert_threshold / 100
    spend_basis      = "CURRENT_SPEND"
  }
  
  threshold_rules {
    threshold_percent = (var.budget_alert_threshold - 20) / 100
    spend_basis      = "FORECASTED_SPEND"
  }
  
  all_updates_rule {
    monitoring_notification_channels = [
      google_monitoring_notification_channel.email[0].name
    ]
    disable_default_iam_recipients = false
  }
}

# GCP Monitoring Notification Channel
resource "google_monitoring_notification_channel" "email" {
  count = var.deploy_to_gcp ? 1 : 0
  
  display_name = "${local.name_prefix} Cost Alerts"
  type         = "email"
  
  labels = {
    email_address = var.alert_email
  }
  
  force_delete = false
}

# GCP Cloud Function for Cost Optimization
resource "google_cloudfunctions_function" "cost_optimizer" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name        = "${local.name_prefix}-cost-optimizer"
  description = "Automated cost optimization for GCP resources"
  runtime     = "python39"
  
  available_memory_mb   = 256
  source_archive_bucket = google_storage_bucket.function_source[0].name
  source_archive_object = google_storage_bucket_object.function_source[0].name
  trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.cost_optimization[0].name
  }
  entry_point = "cost_optimize"
  
  environment_variables = {
    ENVIRONMENT   = var.environment
    COST_CENTER   = var.cost_center
    PROJECT_ID    = var.gcp_project_id
  }
  
  labels = var.tags
}

# GCP Storage for Cloud Function Source
resource "google_storage_bucket" "function_source" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name     = "${local.name_prefix}-function-source-${random_string.bucket_suffix.result}"
  location = var.gcp_region
  
  labels = var.tags
}

resource "google_storage_bucket_object" "function_source" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name   = "cost-optimizer-source.zip"
  bucket = google_storage_bucket.function_source[0].name
  source = data.archive_file.gcp_cost_optimizer_zip[0].output_path
}

data "archive_file" "gcp_cost_optimizer_zip" {
  count = var.deploy_to_gcp ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/gcp_cost_optimizer.zip"
  
  source {
    content = templatefile("${path.module}/templates/gcp_cost_optimizer.py.tpl", {
      project_id = var.gcp_project_id
    })
    filename = "main.py"
  }
  
  source {
    content = "google-cloud-compute==1.14.0\ngoogle-cloud-monitoring==2.15.1"
    filename = "requirements.txt"
  }
}

# GCP Pub/Sub Topic for Cost Optimization
resource "google_pubsub_topic" "cost_optimization" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name = "${local.name_prefix}-cost-optimization"
  
  labels = var.tags
}

# GCP Cloud Scheduler for Cost Optimization
resource "google_cloud_scheduler_job" "cost_optimization" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name     = "${local.name_prefix}-cost-optimization"
  schedule = "0 9 * * *"  # Daily at 9 AM
  
  pubsub_target {
    topic_name = google_pubsub_topic.cost_optimization[0].id
    data       = base64encode(jsonencode({
      action = "optimize_costs"
    }))
  }
}

# ============================================================================
# SHARED RESOURCES
# ============================================================================

resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# Data sources
data "azurerm_client_config" "current" {
  count = var.deploy_to_azure ? 1 : 0
}
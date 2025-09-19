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
# INTELLIGENT RESOURCE SCHEDULING AND RIGHTSIZING
# ============================================================================

# Kubernetes CronJob for intelligent resource scheduling
resource "kubernetes_cron_job_v1" "intelligent_scheduler" {
  metadata {
    name      = "${local.name_prefix}-intelligent-scheduler"
    namespace = var.kubernetes_namespace
    labels = merge(var.tags, {
      component = "cost-optimization"
      tier      = "intelligent-scheduling"
    })
  }
  
  spec {
    schedule          = "*/15 * * * *"  # Every 15 minutes
    concurrency_policy = "Forbid"
    
    job_template {
      metadata {
        labels = merge(var.tags, {
          job_type = "cost-optimization"
          scheduler = "intelligent"
        })
      }
      
      spec {
        template {
          metadata {
            labels = var.tags
          }
          
          spec {
            service_account_name = var.service_account_name
            restart_policy      = "OnFailure"
            
            container {
              name  = "intelligent-scheduler"
              image = "cost-optimizer:latest"
              
              env {
                name  = "COST_REDUCTION_TARGET"
                value = tostring(var.cost_reduction_target_percentage)
              }
              
              env {
                name  = "ENVIRONMENT"
                value = var.environment
              }
              
              env {
                name  = "ENABLE_SPOT_INSTANCES"
                value = tostring(var.enable_spot_instances)
              }
              
              env {
                name  = "SPOT_INSTANCE_PERCENTAGE"
                value = tostring(var.spot_instance_percentage)
              }
              
              env {
                name  = "ENABLE_INTELLIGENT_SCALING"
                value = tostring(var.enable_intelligent_scaling)
              }
              
              command = ["/opt/cost-optimizer/scheduler.py"]
              
              resources {
                requests = {
                  cpu    = "100m"
                  memory = "256Mi"
                }
                limits = {
                  cpu    = "500m"
                  memory = "1Gi"
                }
              }
              
              volume_mount {
                name       = "config"
                mount_path = "/opt/config"
                read_only  = true
              }
              
              volume_mount {
                name       = "metrics"
                mount_path = "/opt/metrics"
              }
            }
            
            volume {
              name = "config"
              config_map {
                name = kubernetes_config_map.cost_optimization_config.metadata[0].name
              }
            }
            
            volume {
              name = "metrics"
              empty_dir {}
            }
          }
        }
        
        backoff_limit = 3
      }
    }
  }
}

# ConfigMap for cost optimization configuration
resource "kubernetes_config_map" "cost_optimization_config" {
  metadata {
    name      = "${local.name_prefix}-cost-optimization-config"
    namespace = var.kubernetes_namespace
    labels    = var.tags
  }
  
  data = {
    "cost-optimization-policy" = jsonencode({
      target_cost_reduction = var.cost_reduction_target_percentage
      
      scheduling_policies = {
        development = {
          auto_shutdown_enabled = true
          shutdown_schedule = {
            weekdays = "18:00"  # 6 PM
            weekends = "all_day"
            holidays = "all_day"
          }
          startup_schedule = {
            weekdays = "08:00"  # 8 AM
            weekends = "disabled"
            holidays = "disabled"
          }
        }
        
        staging = {
          auto_shutdown_enabled = true
          shutdown_schedule = {
            weekdays = "20:00"  # 8 PM
            weekends = "22:00"  # 10 PM
            holidays = "all_day"
          }
          startup_schedule = {
            weekdays = "07:00"  # 7 AM
            weekends = "10:00"  # 10 AM
            holidays = "disabled"
          }
        }
        
        production = {
          auto_shutdown_enabled = false
          intelligent_scaling_enabled = true
          right_sizing_enabled = true
          spot_instance_enabled = var.enable_spot_instances
        }
      }
      
      rightsizing_rules = {
        cpu_utilization_thresholds = {
          underutilized = 20  # Less than 20% for 7 days
          overutilized = 80   # More than 80% for 1 hour
          evaluation_period_days = 7
        }
        
        memory_utilization_thresholds = {
          underutilized = 30  # Less than 30% for 7 days
          overutilized = 85   # More than 85% for 1 hour
          evaluation_period_days = 7
        }
        
        rightsizing_actions = {
          downsize_threshold = 0.5  # Downsize if 50% reduction possible
          upsize_threshold = 1.5    # Upsize if 150% increase needed
          minimum_savings_threshold = 100  # Minimum $100/month savings
        }
      }
      
      spot_instance_strategy = {
        enabled = var.enable_spot_instances
        target_percentage = var.spot_instance_percentage
        
        workload_suitability = {
          batch_processing = 80    # 80% spot instances for batch jobs
          web_applications = 30    # 30% spot instances for web apps
          databases = 0            # 0% spot instances for databases
          stateful_services = 10   # 10% spot instances for stateful services
        }
        
        diversification_strategy = {
          instance_types = 5       # Use at least 5 different instance types
          availability_zones = 3   # Spread across 3 AZs
          allocation_strategy = "diversified"
        }
      }
      
      reserved_instance_recommendations = {
        enabled = var.enable_reserved_instance_recommendations
        coverage_target = var.reserved_instance_coverage
        
        evaluation_criteria = {
          minimum_usage_threshold = 70     # 70% utilization minimum
          commitment_period_months = 12    # 12-month term
          payment_option = "partial_upfront"
        }
        
        recommendation_frequency = "weekly"
        auto_purchase_enabled = false  # Manual approval required
      }
    })
    
    "monitoring-config" = jsonencode({
      cost_metrics = {
        collection_interval_minutes = 15
        retention_days = 90
        
        tracked_metrics = [
          "cost_per_service",
          "cost_per_environment", 
          "cost_per_resource_type",
          "utilization_metrics",
          "savings_achieved",
          "optimization_opportunities"
        ]
        
        anomaly_detection = {
          enabled = true
          sensitivity = "medium"
          notification_threshold = 20  # 20% cost increase
        }
      }
      
      dashboard_config = {
        refresh_interval_seconds = 300
        cost_trend_period_days = 30
        
        widgets = [
          "daily_cost_trend",
          "service_breakdown",
          "optimization_savings",
          "rightsizing_opportunities",
          "reserved_instance_coverage",
          "spot_instance_usage"
        ]
      }
      
      alerting = {
        budget_threshold_alerts = [50, 75, 90, 100]  # Percentage thresholds
        anomaly_alerts = true
        optimization_opportunity_alerts = true
        
        notification_channels = [
          var.alert_email,
          var.slack_webhook_url
        ]
      }
    })
    
    "automation-rules" = jsonencode({
      automated_actions = {
        auto_shutdown = {
          enabled = var.auto_shutdown_enabled
          environments = ["dev", "staging"]
          exclude_tags = ["persistent", "always-on"]
          
          schedule = {
            shutdown_time = "18:00"
            startup_time = "08:00"
            timezone = "UTC"
            skip_weekends = false
          }
          
          safety_checks = {
            check_active_connections = true
            check_running_jobs = true
            minimum_idle_time_minutes = 30
            confirmation_required = var.environment == "prod"
          }
        }
        
        rightsizing = {
          enabled = var.enable_intelligent_scaling
          auto_apply_threshold = 30  # Auto-apply if >30% cost savings
          
          approval_workflow = {
            required_for_production = true
            approval_timeout_hours = 24
            auto_rollback_on_issues = true
          }
          
          rollback_conditions = {
            performance_degradation_threshold = 20  # 20% performance drop
            error_rate_increase_threshold = 5      # 5% error rate increase
            monitoring_period_hours = 24
          }
        }
        
        spot_instance_management = {
          enabled = var.enable_spot_instances
          auto_replacement = true
          
          interruption_handling = {
            graceful_shutdown_seconds = 120
            auto_migration_enabled = true
            fallback_to_on_demand = true
          }
          
          bid_management = {
            strategy = "adaptive"
            max_price_multiplier = 0.7  # Max 70% of on-demand price
            price_history_analysis_days = 7
          }
        }
      }
      
      cost_governance = {
        spending_limits = {
          daily_limit_usd = var.daily_spending_limit
          monthly_limit_usd = var.monthly_budget_limit
          emergency_brake_threshold = 150  # 150% of monthly budget
        }
        
        resource_quotas = {
          max_instances_per_environment = {
            dev = 10
            staging = 20
            prod = 100
          }
          
          max_storage_gb_per_environment = {
            dev = 1000
            staging = 5000
            prod = 50000
          }
          
          max_monthly_cost_per_service = {
            compute = 10000
            storage = 5000
            networking = 2000
            databases = 8000
          }
        }
        
        approval_workflows = {
          high_cost_resources = {
            threshold_usd_per_month = 1000
            approval_required = true
            approvers = ["cost-team", "engineering-leads"]
          }
          
          new_services = {
            cost_estimate_required = true
            business_justification_required = true
            approval_timeout_hours = 48
          }
        }
      }
    })
  }
}

# ============================================================================
# COST ANALYTICS AND REPORTING
# ============================================================================

# Deployment for cost analytics service
resource "kubernetes_deployment" "cost_analytics" {
  metadata {
    name      = "${local.name_prefix}-cost-analytics"
    namespace = var.kubernetes_namespace
    labels = merge(var.tags, {
      component = "cost-analytics"
      tier      = "monitoring"
    })
  }
  
  spec {
    replicas = 1
    
    selector {
      match_labels = {
        app       = "${local.name_prefix}-cost-analytics"
        component = "cost-analytics"
      }
    }
    
    template {
      metadata {
        labels = merge(var.tags, {
          app       = "${local.name_prefix}-cost-analytics"
          component = "cost-analytics"
        })
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "8080"
          "prometheus.io/path"   = "/metrics"
        }
      }
      
      spec {
        service_account_name = var.service_account_name
        
        container {
          name  = "cost-analytics"
          image = "cost-analytics:latest"
          
          port {
            name           = "http"
            container_port = 8080
            protocol       = "TCP"
          }
          
          port {
            name           = "metrics"
            container_port = 9090
            protocol       = "TCP"
          }
          
          env {
            name  = "ENVIRONMENT"
            value = var.environment
          }
          
          env {
            name  = "COST_REDUCTION_TARGET"
            value = tostring(var.cost_reduction_target_percentage)
          }
          
          env {
            name  = "AWS_ENABLED"
            value = tostring(var.deploy_to_aws)
          }
          
          env {
            name  = "AZURE_ENABLED"
            value = tostring(var.deploy_to_azure)
          }
          
          env {
            name  = "GCP_ENABLED"
            value = tostring(var.deploy_to_gcp)
          }
          
          env {
            name = "DATABASE_URL"
            value_from {
              secret_key_ref {
                name = var.database_secret_name
                key  = "url"
              }
            }
          }
          
          resources {
            requests = {
              cpu    = "500m"
              memory = "1Gi"
            }
            limits = {
              cpu    = "2000m"
              memory = "4Gi"
            }
          }
          
          liveness_probe {
            http_get {
              path = "/health"
              port = 8080
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }
          
          readiness_probe {
            http_get {
              path = "/ready"
              port = 8080
            }
            initial_delay_seconds = 10
            period_seconds        = 5
          }
          
          volume_mount {
            name       = "config"
            mount_path = "/app/config"
            read_only  = true
          }
          
          volume_mount {
            name       = "data"
            mount_path = "/app/data"
          }
        }
        
        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.cost_optimization_config.metadata[0].name
          }
        }
        
        volume {
          name = "data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.cost_analytics_data.metadata[0].name
          }
        }
        
        restart_policy = "Always"
      }
    }
  }
}

# PVC for cost analytics data
resource "kubernetes_persistent_volume_claim" "cost_analytics_data" {
  metadata {
    name      = "${local.name_prefix}-cost-analytics-data"
    namespace = var.kubernetes_namespace
    labels    = var.tags
  }
  
  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.storage_class
    
    resources {
      requests = {
        storage = "50Gi"
      }
    }
  }
}

# Service for cost analytics
resource "kubernetes_service" "cost_analytics" {
  metadata {
    name      = "${local.name_prefix}-cost-analytics-service"
    namespace = var.kubernetes_namespace
    labels = merge(var.tags, {
      component = "cost-analytics"
    })
    annotations = {
      "prometheus.io/scrape" = "true"
      "prometheus.io/port"   = "9090"
      "prometheus.io/path"   = "/metrics"
    }
  }
  
  spec {
    selector = {
      app       = "${local.name_prefix}-cost-analytics"
      component = "cost-analytics"
    }
    
    port {
      name        = "http"
      port        = 80
      target_port = 8080
      protocol    = "TCP"
    }
    
    port {
      name        = "metrics"
      port        = 9090
      target_port = 9090
      protocol    = "TCP"
    }
    
    type = "ClusterIP"
  }
}

# ============================================================================
# ADVANCED COST OPTIMIZATION FEATURES
# ============================================================================

# ServiceMonitor for Prometheus integration
resource "kubernetes_manifest" "cost_analytics_service_monitor" {
  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "ServiceMonitor"
    metadata = {
      name      = "${local.name_prefix}-cost-analytics"
      namespace = var.kubernetes_namespace
      labels = merge(var.tags, {
        component = "cost-monitoring"
      })
    }
    spec = {
      selector = {
        matchLabels = {
          component = "cost-analytics"
        }
      }
      endpoints = [{
        port     = "metrics"
        interval = "30s"
        path     = "/metrics"
      }]
    }
  }
}

# PrometheusRule for cost alerting
resource "kubernetes_manifest" "cost_optimization_alerts" {
  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "PrometheusRule"
    metadata = {
      name      = "${local.name_prefix}-cost-alerts"
      namespace = var.kubernetes_namespace
      labels = merge(var.tags, {
        component = "cost-alerting"
      })
    }
    spec = {
      groups = [
        {
          name = "cost-optimization"
          rules = [
            {
              alert = "HighCostIncrease"
              expr  = "increase(cost_total_usd[24h]) > ${var.monthly_budget_limit * 0.1}"
              for   = "15m"
              labels = {
                severity = "warning"
                team     = "cost-optimization"
              }
              annotations = {
                summary     = "Cost increase detected"
                description = "Daily cost increase exceeds 10% of monthly budget"
              }
            },
            {
              alert = "BudgetThresholdExceeded"
              expr  = "cost_monthly_total_usd > ${var.monthly_budget_limit * var.budget_alert_threshold / 100}"
              for   = "5m"
              labels = {
                severity = "critical"
                team     = "cost-optimization"
              }
              annotations = {
                summary     = "Monthly budget threshold exceeded"
                description = "Current monthly costs exceed ${var.budget_alert_threshold}% of budget"
              }
            },
            {
              alert = "UnderutilizedResources"
              expr  = "resource_utilization_cpu_percent < 20 and resource_uptime_hours > 168"
              for   = "1h"
              labels = {
                severity = "info"
                team     = "cost-optimization"
              }
              annotations = {
                summary     = "Underutilized resources detected"
                description = "Resources with <20% CPU utilization for over 1 week"
              }
            },
            {
              alert = "SpotInstanceInterruptionRate"
              expr  = "rate(spot_instance_interruptions_total[1h]) > 0.1"
              for   = "15m"
              labels = {
                severity = "warning"
                team     = "infrastructure"
              }
              annotations = {
                summary     = "High spot instance interruption rate"
                description = "Spot instance interruption rate exceeds 10% per hour"
              }
            }
          ]
        }
      ]
    }
  }
}

# ============================================================================
# COST OPTIMIZATION DASHBOARD
# ============================================================================

resource "kubernetes_config_map" "cost_dashboard" {
  metadata {
    name      = "${local.name_prefix}-cost-dashboard"
    namespace = var.kubernetes_namespace
    labels = merge(var.tags, {
      component = "cost-dashboard"
    })
  }
  
  data = {
    "dashboard.json" = jsonencode({
      dashboard = {
        id    = null
        title = "Cost Optimization Dashboard"
        tags  = ["cost-optimization", var.environment]
        style = "dark"
        timezone = "UTC"
        refresh = "5m"
        
        panels = [
          {
            id    = 1
            title = "Monthly Cost Trend"
            type  = "graph"
            targets = [{
              expr = "cost_monthly_total_usd"
            }]
            yAxes = [{
              label = "USD"
            }]
          },
          {
            id    = 2
            title = "Cost by Service"
            type  = "piechart"
            targets = [{
              expr = "sum by (service) (cost_service_total_usd)"
            }]
          },
          {
            id    = 3
            title = "Savings Achieved"
            type  = "stat"
            targets = [{
              expr = "cost_savings_total_usd"
            }]
            fieldConfig = {
              defaults = {
                color = {
                  mode = "value"
                }
                mappings = []
                thresholds = {
                  steps = [
                    { color = "red", value = 0 },
                    { color = "yellow", value = 1000 },
                    { color = "green", value = 5000 }
                  ]
                }
                unit = "currencyUSD"
              }
            }
          },
          {
            id    = 4
            title = "Resource Utilization"
            type  = "heatmap"
            targets = [{
              expr = "resource_utilization_percent"
            }]
          },
          {
            id    = 5
            title = "Spot Instance Usage"
            type  = "gauge"
            targets = [{
              expr = "spot_instance_percentage"
            }]
            fieldConfig = {
              defaults = {
                max = 100
                min = 0
                thresholds = {
                  steps = [
                    { color = "red", value = 0 },
                    { color = "yellow", value = 20 },
                    { color = "green", value = 40 }
                  ]
                }
                unit = "percent"
              }
            }
          },
          {
            id    = 6
            title = "Rightsizing Opportunities"
            type  = "table"
            targets = [{
              expr = "rightsizing_opportunities"
            }]
          }
        ]
        
        templating = {
          list = [
            {
              name  = "environment"
              type  = "custom"
              query = "dev,staging,prod"
            },
            {
              name  = "service"
              type  = "query"
              query = "label_values(cost_service_total_usd, service)"
            }
          ]
        }
        
        time = {
          from = "now-30d"
          to   = "now"
        }
      }
    })
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
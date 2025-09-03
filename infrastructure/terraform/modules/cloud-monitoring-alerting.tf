# Comprehensive Multi-Cloud Monitoring and Centralized Alerting Module
# Implements unified observability across AWS, Azure, and GCP

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
    datadog = {
      source  = "DataDog/datadog"
      version = "~> 3.30"
    }
  }
}

# Local variables
locals {
  common_tags = var.common_tags
  alert_channels = {
    email    = var.alert_email_addresses
    slack    = var.slack_webhook_urls
    pagerduty = var.pagerduty_integration_keys
  }
  
  # Environment-specific thresholds
  thresholds = {
    development = {
      cpu_warning    = 70
      cpu_critical   = 85
      memory_warning = 75
      memory_critical = 90
      disk_warning   = 80
      disk_critical  = 95
      error_rate_warning = 5
      error_rate_critical = 10
    }
    staging = {
      cpu_warning    = 65
      cpu_critical   = 80
      memory_warning = 70
      memory_critical = 85
      disk_warning   = 75
      disk_critical  = 90
      error_rate_warning = 3
      error_rate_critical = 7
    }
    production = {
      cpu_warning    = 60
      cpu_critical   = 75
      memory_warning = 65
      memory_critical = 80
      disk_warning   = 70
      disk_critical  = 85
      error_rate_warning = 2
      error_rate_critical = 5
    }
  }
  
  current_thresholds = local.thresholds[var.environment]
}

# ============================================================================
# AWS CLOUDWATCH MONITORING
# ============================================================================

# CloudWatch Composite Alarms for sophisticated alerting
resource "aws_cloudwatch_composite_alarm" "application_health" {
  count = var.enable_aws_monitoring ? 1 : 0
  
  alarm_name        = "${var.project_name}-application-health-${var.environment}"
  alarm_description = "Overall application health composite alarm"
  
  alarm_rule = "ALARM(${aws_cloudwatch_metric_alarm.high_cpu_utilization[0].alarm_name}) OR ALARM(${aws_cloudwatch_metric_alarm.high_memory_utilization[0].alarm_name}) OR ALARM(${aws_cloudwatch_metric_alarm.high_error_rate[0].alarm_name})"
  
  actions_enabled = true
  alarm_actions   = [aws_sns_topic.critical_alerts[0].arn]
  ok_actions      = [aws_sns_topic.critical_alerts[0].arn]
  
  depends_on = [
    aws_cloudwatch_metric_alarm.high_cpu_utilization,
    aws_cloudwatch_metric_alarm.high_memory_utilization,
    aws_cloudwatch_metric_alarm.high_error_rate
  ]
  
  tags = merge(local.common_tags, {
    Purpose = "CompositeMonitoring"
    Severity = "Critical"
  })
}

# CPU Utilization Alarm
resource "aws_cloudwatch_metric_alarm" "high_cpu_utilization" {
  count = var.enable_aws_monitoring ? 1 : 0
  
  alarm_name          = "${var.project_name}-high-cpu-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = "300"
  statistic           = "Average"
  threshold           = local.current_thresholds.cpu_critical
  alarm_description   = "This metric monitors EC2 CPU utilization"
  alarm_actions       = [aws_sns_topic.warning_alerts[0].arn]
  ok_actions          = [aws_sns_topic.warning_alerts[0].arn]
  treat_missing_data  = "breaching"
  
  dimensions = {
    InstanceId = var.aws_instance_id
  }
  
  tags = merge(local.common_tags, {
    Purpose = "CPUMonitoring"
    Severity = "Warning"
  })
}

# Memory Utilization Alarm (requires CloudWatch agent)
resource "aws_cloudwatch_metric_alarm" "high_memory_utilization" {
  count = var.enable_aws_monitoring ? 1 : 0
  
  alarm_name          = "${var.project_name}-high-memory-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "mem_used_percent"
  namespace           = "CWAgent"
  period              = "300"
  statistic           = "Average"
  threshold           = local.current_thresholds.memory_critical
  alarm_description   = "This metric monitors memory utilization"
  alarm_actions       = [aws_sns_topic.warning_alerts[0].arn]
  ok_actions          = [aws_sns_topic.warning_alerts[0].arn]
  treat_missing_data  = "notBreaching"
  
  dimensions = {
    InstanceId = var.aws_instance_id
  }
  
  tags = merge(local.common_tags, {
    Purpose = "MemoryMonitoring"
    Severity = "Warning"
  })
}

# Application Error Rate Alarm
resource "aws_cloudwatch_metric_alarm" "high_error_rate" {
  count = var.enable_aws_monitoring ? 1 : 0
  
  alarm_name          = "${var.project_name}-high-error-rate-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  
  metric_query {
    id = "error_rate"
    
    metric {
      metric_name = "4XXError"
      namespace   = "AWS/ApplicationELB"
      period      = "300"
      stat        = "Sum"
      
      dimensions = {
        LoadBalancer = var.aws_load_balancer_name
      }
    }
    
    return_data = true
  }
  
  threshold          = local.current_thresholds.error_rate_critical
  alarm_description  = "This metric monitors application error rate"
  alarm_actions      = [aws_sns_topic.critical_alerts[0].arn]
  ok_actions         = [aws_sns_topic.critical_alerts[0].arn]
  treat_missing_data = "notBreaching"
  
  tags = merge(local.common_tags, {
    Purpose = "ErrorRateMonitoring"
    Severity = "Critical"
  })
}

# Database Performance Monitoring
resource "aws_cloudwatch_metric_alarm" "database_cpu_high" {
  count = var.enable_database_monitoring && var.enable_aws_monitoring ? 1 : 0
  
  alarm_name          = "${var.project_name}-database-cpu-high-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "CPUUtilization"
  namespace           = "AWS/RDS"
  period              = "300"
  statistic           = "Average"
  threshold           = local.current_thresholds.cpu_critical
  alarm_description   = "RDS instance CPU utilization is high"
  alarm_actions       = [aws_sns_topic.critical_alerts[0].arn]
  
  dimensions = {
    DBInstanceIdentifier = var.aws_rds_instance_id
  }
  
  tags = merge(local.common_tags, {
    Purpose = "DatabaseMonitoring"
    Severity = "Critical"
  })
}

# CloudWatch Insights Queries for Advanced Analytics
resource "aws_cloudwatchinsights_query_definition" "error_analysis" {
  count = var.enable_aws_monitoring ? 1 : 0
  
  name = "${var.project_name}-error-analysis"
  
  log_group_names = [
    "/aws/lambda/${var.project_name}",
    "/aws/apigateway/${var.project_name}",
    "/aws/ecs/${var.project_name}"
  ]
  
  query_string = <<EOF
fields @timestamp, @message
| filter @message like /ERROR/
| stats count() by bin(5m)
| sort @timestamp desc
EOF
}

# Custom Metrics for Business KPIs
resource "aws_cloudwatch_metric_alarm" "business_kpi_alert" {
  count = var.enable_business_monitoring && var.enable_aws_monitoring ? 1 : 0
  
  alarm_name          = "${var.project_name}-low-user-engagement-${var.environment}"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = "3"
  metric_name         = "ActiveUsers"
  namespace           = "Custom/BusinessMetrics"
  period              = "900"  # 15 minutes
  statistic           = "Average"
  threshold           = var.min_active_users
  alarm_description   = "Active user count is below threshold"
  alarm_actions       = [aws_sns_topic.business_alerts[0].arn]
  
  tags = merge(local.common_tags, {
    Purpose = "BusinessMetrics"
    Severity = "Warning"
  })
}

# SNS Topics for different alert severities
resource "aws_sns_topic" "critical_alerts" {
  count = var.enable_aws_monitoring ? 1 : 0
  
  name         = "${var.project_name}-critical-alerts-${var.environment}"
  display_name = "Critical System Alerts"
  
  kms_master_key_id = aws_kms_key.monitoring_encryption[0].key_id
  
  tags = merge(local.common_tags, {
    Purpose = "CriticalAlerting"
  })
}

resource "aws_sns_topic" "warning_alerts" {
  count = var.enable_aws_monitoring ? 1 : 0
  
  name         = "${var.project_name}-warning-alerts-${var.environment}"
  display_name = "System Warning Alerts"
  
  kms_master_key_id = aws_kms_key.monitoring_encryption[0].key_id
  
  tags = merge(local.common_tags, {
    Purpose = "WarningAlerting"
  })
}

resource "aws_sns_topic" "business_alerts" {
  count = var.enable_business_monitoring && var.enable_aws_monitoring ? 1 : 0
  
  name         = "${var.project_name}-business-alerts-${var.environment}"
  display_name = "Business Metric Alerts"
  
  kms_master_key_id = aws_kms_key.monitoring_encryption[0].key_id
  
  tags = merge(local.common_tags, {
    Purpose = "BusinessAlerting"
  })
}

# KMS Key for SNS encryption
resource "aws_kms_key" "monitoring_encryption" {
  count = var.enable_aws_monitoring ? 1 : 0
  
  description             = "KMS key for monitoring and alerting encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  
  tags = merge(local.common_tags, {
    Purpose = "MonitoringEncryption"
  })
}

resource "aws_kms_alias" "monitoring_encryption" {
  count         = var.enable_aws_monitoring ? 1 : 0
  name          = "alias/${var.project_name}-monitoring-encryption"
  target_key_id = aws_kms_key.monitoring_encryption[0].key_id
}

# ============================================================================
# AZURE MONITOR INTEGRATION
# ============================================================================

# Azure Application Insights
resource "azurerm_application_insights" "main" {
  count = var.enable_azure_monitoring ? 1 : 0
  
  name                = "${var.project_name}-insights-${var.environment}"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  application_type    = "web"
  retention_in_days   = var.metrics_retention_days
  
  tags = merge(local.common_tags, {
    Purpose = "ApplicationInsights"
  })
}

# Azure Log Analytics Workspace
resource "azurerm_log_analytics_workspace" "main" {
  count = var.enable_azure_monitoring ? 1 : 0
  
  name                = "${var.project_name}-logs-${var.environment}"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  sku                = var.environment == "production" ? "PerGB2018" : "Free"
  retention_in_days   = var.log_retention_days
  
  tags = merge(local.common_tags, {
    Purpose = "LogAnalytics"
  })
}

# Azure Monitor Action Groups
resource "azurerm_monitor_action_group" "critical" {
  count = var.enable_azure_monitoring ? 1 : 0
  
  name                = "${var.project_name}-critical-actions-${var.environment}"
  resource_group_name = var.azure_resource_group_name
  short_name          = "critical"
  
  dynamic "email_receiver" {
    for_each = var.alert_email_addresses
    content {
      name          = "email-${email_receiver.key}"
      email_address = email_receiver.value
    }
  }
  
  dynamic "webhook_receiver" {
    for_each = var.slack_webhook_urls
    content {
      name                    = "slack-${webhook_receiver.key}"
      service_uri            = webhook_receiver.value
      use_common_alert_schema = true
    }
  }
  
  tags = merge(local.common_tags, {
    Purpose = "CriticalActionGroup"
  })
}

# Azure VM Monitoring Alerts
resource "azurerm_monitor_metric_alert" "vm_cpu_high" {
  count = var.enable_azure_monitoring && var.azure_vm_id != "" ? 1 : 0
  
  name                = "${var.project_name}-vm-cpu-high-${var.environment}"
  resource_group_name = var.azure_resource_group_name
  scopes              = [var.azure_vm_id]
  description         = "Virtual Machine CPU usage is high"
  severity            = 2
  frequency           = "PT5M"
  window_size         = "PT15M"
  
  criteria {
    metric_namespace = "Microsoft.Compute/virtualMachines"
    metric_name      = "Percentage CPU"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = local.current_thresholds.cpu_critical
  }
  
  action {
    action_group_id = azurerm_monitor_action_group.critical[0].id
  }
  
  tags = merge(local.common_tags, {
    Purpose = "VMMonitoring"
  })
}

# Azure SQL Database Monitoring
resource "azurerm_monitor_metric_alert" "sql_dtu_high" {
  count = var.enable_azure_monitoring && var.azure_sql_database_id != "" ? 1 : 0
  
  name                = "${var.project_name}-sql-dtu-high-${var.environment}"
  resource_group_name = var.azure_resource_group_name
  scopes              = [var.azure_sql_database_id]
  description         = "SQL Database DTU usage is high"
  severity            = 1
  frequency           = "PT5M"
  window_size         = "PT15M"
  
  criteria {
    metric_namespace = "Microsoft.Sql/servers/databases"
    metric_name      = "dtu_consumption_percent"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = 80
  }
  
  action {
    action_group_id = azurerm_monitor_action_group.critical[0].id
  }
  
  tags = merge(local.common_tags, {
    Purpose = "DatabaseMonitoring"
  })
}

# ============================================================================
# GOOGLE CLOUD MONITORING
# ============================================================================

# Google Cloud Monitoring Notification Channels
resource "google_monitoring_notification_channel" "email" {
  count = var.enable_gcp_monitoring ? length(var.alert_email_addresses) : 0
  
  display_name = "Email Notification ${count.index + 1}"
  type         = "email"
  
  labels = {
    email_address = var.alert_email_addresses[count.index]
  }
  
  project = var.gcp_project_id
}

resource "google_monitoring_notification_channel" "slack" {
  count = var.enable_gcp_monitoring ? length(var.slack_webhook_urls) : 0
  
  display_name = "Slack Notification ${count.index + 1}"
  type         = "slack"
  
  labels = {
    url = var.slack_webhook_urls[count.index]
  }
  
  project = var.gcp_project_id
}

# GCP Compute Engine Monitoring
resource "google_monitoring_alert_policy" "gce_cpu_high" {
  count = var.enable_gcp_monitoring && var.gcp_instance_name != "" ? 1 : 0
  
  display_name = "${var.project_name}-gce-cpu-high-${var.environment}"
  combiner     = "OR"
  
  conditions {
    display_name = "GCE Instance CPU usage"
    
    condition_threshold {
      filter          = "resource.type=\"gce_instance\" AND resource.labels.instance_name=\"${var.gcp_instance_name}\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = local.current_thresholds.cpu_critical
      
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
  
  notification_channels = concat(
    [for nc in google_monitoring_notification_channel.email : nc.name],
    [for nc in google_monitoring_notification_channel.slack : nc.name]
  )
  
  alert_strategy {
    auto_close = "1800s"
  }
  
  project = var.gcp_project_id
}

# GCP Cloud SQL Monitoring
resource "google_monitoring_alert_policy" "cloudsql_cpu_high" {
  count = var.enable_gcp_monitoring && var.gcp_sql_instance_name != "" ? 1 : 0
  
  display_name = "${var.project_name}-cloudsql-cpu-high-${var.environment}"
  combiner     = "OR"
  
  conditions {
    display_name = "Cloud SQL CPU usage"
    
    condition_threshold {
      filter          = "resource.type=\"cloudsql_database\" AND resource.labels.database_id=\"${var.gcp_project_id}:${var.gcp_sql_instance_name}\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = local.current_thresholds.cpu_critical
      
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }
  
  notification_channels = concat(
    [for nc in google_monitoring_notification_channel.email : nc.name],
    [for nc in google_monitoring_notification_channel.slack : nc.name]
  )
  
  project = var.gcp_project_id
}

# ============================================================================
# DATADOG UNIFIED MONITORING (OPTIONAL)
# ============================================================================

# Datadog Monitors for cross-cloud visibility
resource "datadog_monitor" "cross_cloud_cpu" {
  count = var.enable_datadog_monitoring ? 1 : 0
  
  name    = "${var.project_name} - Cross-Cloud CPU Usage"
  type    = "metric alert"
  message = "CPU usage is high across cloud providers @${join(" @", var.datadog_notification_handles)}"
  
  query = "avg(last_5m):( avg:system.cpu.user{environment:${var.environment}} by {host} + avg:system.cpu.system{environment:${var.environment}} by {host} ) > ${local.current_thresholds.cpu_critical}"
  
  monitor_thresholds {
    warning  = local.current_thresholds.cpu_warning
    critical = local.current_thresholds.cpu_critical
  }
  
  notify_no_data    = false
  renotify_interval = 60
  
  tags = ["environment:${var.environment}", "purpose:cross-cloud-monitoring"]
}

resource "datadog_monitor" "cross_cloud_memory" {
  count = var.enable_datadog_monitoring ? 1 : 0
  
  name    = "${var.project_name} - Cross-Cloud Memory Usage"
  type    = "metric alert"
  message = "Memory usage is high across cloud providers @${join(" @", var.datadog_notification_handles)}"
  
  query = "avg(last_5m):avg:system.mem.pct_usable{environment:${var.environment}} by {host} < ${100 - local.current_thresholds.memory_critical}"
  
  monitor_thresholds {
    warning  = 100 - local.current_thresholds.memory_warning
    critical = 100 - local.current_thresholds.memory_critical
  }
  
  notify_no_data    = false
  renotify_interval = 60
  
  tags = ["environment:${var.environment}", "purpose:cross-cloud-monitoring"]
}

# Datadog Dashboard for Cross-Cloud Overview
resource "datadog_dashboard" "cross_cloud_overview" {
  count = var.enable_datadog_monitoring ? 1 : 0
  
  title         = "${var.project_name} - Cross-Cloud Infrastructure Overview"
  description   = "Unified view of infrastructure metrics across AWS, Azure, and GCP"
  layout_type   = "ordered"
  is_read_only  = false
  
  widget {
    group_definition {
      title = "System Metrics"
      
      widget {
        timeseries_definition {
          title = "CPU Usage by Cloud Provider"
          
          request {
            q = "avg:system.cpu.user{environment:${var.environment}} by {cloud_provider}"
            
            style {
              palette = "dog_classic"
            }
          }
        }
      }
      
      widget {
        timeseries_definition {
          title = "Memory Usage by Cloud Provider"
          
          request {
            q = "avg:system.mem.pct_usable{environment:${var.environment}} by {cloud_provider}"
            
            style {
              palette = "dog_classic"
            }
          }
        }
      }
    }
  }
  
  widget {
    group_definition {
      title = "Application Metrics"
      
      widget {
        query_value_definition {
          title = "Total Active Hosts"
          
          request {
            q = "count_nonzero(avg:system.uptime{environment:${var.environment}} by {host})"
            
            aggregator = "last"
          }
          
          autoscale = true
        }
      }
      
      widget {
        toplist_definition {
          title = "Top Hosts by CPU Usage"
          
          request {
            q = "top(avg:system.cpu.user{environment:${var.environment}} by {host}, 10, 'mean', 'desc')"
          }
        }
      }
    }
  }
  
  template_variable {
    name    = "environment"
    prefix  = "environment"
    default = var.environment
  }
  
  template_variable {
    name    = "cloud_provider"
    prefix  = "cloud_provider"
    default = "*"
  }
}

# ============================================================================
# SYNTHETIC MONITORING
# ============================================================================

# AWS CloudWatch Synthetics Canaries
resource "aws_synthetics_canary" "api_health_check" {
  count = var.enable_synthetic_monitoring && var.enable_aws_monitoring ? 1 : 0
  
  name                 = "${var.project_name}-api-health-${var.environment}"
  artifact_s3_location = "s3://${aws_s3_bucket.synthetics_artifacts[0].bucket}/"
  execution_role_arn   = aws_iam_role.synthetics_execution_role[0].arn
  handler              = "apiCanaryBlueprint.handler"
  zip_file            = data.archive_file.synthetics_canary[0].output_path
  runtime_version     = "syn-nodejs-puppeteer-3.9"
  
  schedule {
    expression = var.synthetic_monitoring_frequency
  }
  
  run_config {
    timeout_in_seconds = 60
    memory_in_mb      = 1000
  }
  
  tags = merge(local.common_tags, {
    Purpose = "SyntheticMonitoring"
  })
}

# S3 bucket for synthetics artifacts
resource "aws_s3_bucket" "synthetics_artifacts" {
  count  = var.enable_synthetic_monitoring && var.enable_aws_monitoring ? 1 : 0
  bucket = "${var.project_name}-synthetics-artifacts-${var.environment}-${random_string.bucket_suffix.result}"
  
  tags = merge(local.common_tags, {
    Purpose = "SyntheticsArtifacts"
  })
}

resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# Synthetics canary source code
data "archive_file" "synthetics_canary" {
  count = var.enable_synthetic_monitoring && var.enable_aws_monitoring ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/synthetics_canary.zip"
  
  source {
    content = templatefile("${path.module}/synthetics/api_canary.js", {
      api_endpoint = var.api_endpoint_url
      environment = var.environment
    })
    filename = "nodejs/node_modules/apiCanaryBlueprint.js"
  }
}

# IAM role for synthetics execution
resource "aws_iam_role" "synthetics_execution_role" {
  count = var.enable_synthetic_monitoring && var.enable_aws_monitoring ? 1 : 0
  
  name = "${var.project_name}-synthetics-execution-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
  
  tags = local.common_tags
}

# IAM policy for synthetics execution
resource "aws_iam_role_policy" "synthetics_execution_policy" {
  count = var.enable_synthetic_monitoring && var.enable_aws_monitoring ? 1 : 0
  
  name = "${var.project_name}-synthetics-execution-policy"
  role = aws_iam_role.synthetics_execution_role[0].id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.synthetics_artifacts[0].arn,
          "${aws_s3_bucket.synthetics_artifacts[0].arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:CreateLogGroup"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = "CloudWatchSynthetics"
          }
        }
      }
    ]
  })
}

# Variables
variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment (development, staging, production)"
  type        = string
}

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "enable_aws_monitoring" {
  description = "Enable AWS CloudWatch monitoring"
  type        = bool
  default     = true
}

variable "enable_azure_monitoring" {
  description = "Enable Azure Monitor integration"
  type        = bool
  default     = false
}

variable "enable_gcp_monitoring" {
  description = "Enable Google Cloud Monitoring"
  type        = bool
  default     = false
}

variable "enable_datadog_monitoring" {
  description = "Enable Datadog for unified monitoring"
  type        = bool
  default     = false
}

variable "enable_synthetic_monitoring" {
  description = "Enable synthetic monitoring and health checks"
  type        = bool
  default     = false
}

variable "enable_business_monitoring" {
  description = "Enable business KPI monitoring"
  type        = bool
  default     = false
}

variable "enable_database_monitoring" {
  description = "Enable database performance monitoring"
  type        = bool
  default     = true
}

variable "alert_email_addresses" {
  description = "List of email addresses for alerts"
  type        = list(string)
  default     = []
}

variable "slack_webhook_urls" {
  description = "List of Slack webhook URLs for notifications"
  type        = list(string)
  default     = []
}

variable "pagerduty_integration_keys" {
  description = "List of PagerDuty integration keys"
  type        = list(string)
  default     = []
}

variable "datadog_notification_handles" {
  description = "List of Datadog notification handles"
  type        = list(string)
  default     = []
}

variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = "West US 2"
}

variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = ""
}

variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
  default     = ""
}

variable "aws_instance_id" {
  description = "AWS EC2 instance ID to monitor"
  type        = string
  default     = ""
}

variable "aws_load_balancer_name" {
  description = "AWS Load Balancer name to monitor"
  type        = string
  default     = ""
}

variable "aws_rds_instance_id" {
  description = "AWS RDS instance ID to monitor"
  type        = string
  default     = ""
}

variable "azure_vm_id" {
  description = "Azure VM resource ID to monitor"
  type        = string
  default     = ""
}

variable "azure_sql_database_id" {
  description = "Azure SQL Database resource ID to monitor"
  type        = string
  default     = ""
}

variable "gcp_instance_name" {
  description = "GCP Compute Engine instance name to monitor"
  type        = string
  default     = ""
}

variable "gcp_sql_instance_name" {
  description = "GCP Cloud SQL instance name to monitor"
  type        = string
  default     = ""
}

variable "api_endpoint_url" {
  description = "API endpoint URL for synthetic monitoring"
  type        = string
  default     = ""
}

variable "min_active_users" {
  description = "Minimum number of active users threshold"
  type        = number
  default     = 100
}

variable "metrics_retention_days" {
  description = "Number of days to retain metrics"
  type        = number
  default     = 90
}

variable "log_retention_days" {
  description = "Number of days to retain logs"
  type        = number
  default     = 30
}

variable "synthetic_monitoring_frequency" {
  description = "Frequency for synthetic monitoring (cron expression)"
  type        = string
  default     = "rate(5 minutes)"
}

# Outputs
output "aws_sns_critical_topic_arn" {
  description = "ARN of the AWS SNS critical alerts topic"
  value       = var.enable_aws_monitoring ? aws_sns_topic.critical_alerts[0].arn : null
}

output "azure_log_analytics_workspace_id" {
  description = "ID of the Azure Log Analytics workspace"
  value       = var.enable_azure_monitoring ? azurerm_log_analytics_workspace.main[0].workspace_id : null
}

output "datadog_dashboard_url" {
  description = "URL of the Datadog cross-cloud dashboard"
  value       = var.enable_datadog_monitoring ? datadog_dashboard.cross_cloud_overview[0].url : null
}

output "monitoring_dashboard_urls" {
  description = "URLs for various monitoring dashboards"
  value = {
    aws_cloudwatch = var.enable_aws_monitoring ? "https://console.aws.amazon.com/cloudwatch/home" : null
    azure_monitor  = var.enable_azure_monitoring ? "https://portal.azure.com/#blade/Microsoft_Azure_Monitoring/AzureMonitoringBrowseBlade/overview" : null
    gcp_monitoring = var.enable_gcp_monitoring ? "https://console.cloud.google.com/monitoring" : null
    datadog       = var.enable_datadog_monitoring ? datadog_dashboard.cross_cloud_overview[0].url : null
  }
}